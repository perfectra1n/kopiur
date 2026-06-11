//! Hook execution (ADR §4.8): `SnapshotPolicy.spec.hooks` run **in the
//! controller**, around the mover Job — `beforeSnapshot` (quiesce) to completion
//! before the Job is created, `afterSnapshot` (resume/notify) once the Job is
//! terminal. The mover never runs hooks (it only carries the plan summary for
//! observability).
//!
//! The per-hook dispatch is an **exhaustive `match`** over the externally-tagged
//! [`Hook`] enum (no `_ =>`): a new hook form cannot compile until it is given an
//! execution. Outcomes are two-layered: a kube/API problem is a reconcile
//! [`Error`] (transient, requeued), while the hook itself failing is a
//! *business* outcome ([`HookFailure`]) the Snapshot reconciler turns into a
//! `HooksSucceeded=False` condition + Failed phase (unless the hook opted into
//! `continueOnFailure`).
//!
//! `afterSnapshot` hooks run whether the backup succeeded **or failed**: the
//! canonical pairing is quiesce/resume, and a database left locked because the
//! backup failed would turn one incident into two.

use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{AttachParams, ListParams, ObjectMeta, PostParams};
use kube::{Api, ResourceExt};
use tokio::io::AsyncReadExt;

use kopiur_api::common::PodSelector;
use kopiur_api::snapshot_policy::{Hook, HttpRequestHook, RunJobHook, WorkloadExecHook};

use crate::context::Context;
use crate::error::Result;
use crate::io;
use crate::snapshot_schedule::parse_go_duration;

/// Default per-hook timeout when `timeout` is unset. Generous enough for a
/// quiesce/flush, small enough that a wedged hook can't stall the reconciler
/// worker for long.
pub const DEFAULT_HOOK_TIMEOUT: Duration = Duration::from_secs(300);

/// How often a `runJob` hook's Job is polled for a terminal state.
const JOB_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Bytes of hook stderr / HTTP body kept for the failure message.
const HOOK_OUTPUT_CAP: usize = 1024;

/// Which hook list is running — drives names, condition reasons, and messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPhase {
    /// `hooks.beforeSnapshot` — runs to completion before the mover Job exists.
    Before,
    /// `hooks.afterSnapshot` — runs once the mover Job is terminal (either way).
    After,
}

impl HookPhase {
    /// The spec list name, for messages (`beforeSnapshot` / `afterSnapshot`).
    pub fn list_name(self) -> &'static str {
        match self {
            HookPhase::Before => "beforeSnapshot",
            HookPhase::After => "afterSnapshot",
        }
    }

    /// The condition/Event reason for an aborting failure in this phase.
    pub fn failed_reason(self) -> &'static str {
        match self {
            HookPhase::Before => "PreHookFailed",
            HookPhase::After => "PostHookFailed",
        }
    }

    /// The child-Job name infix for `runJob` hooks (`prehook` / `posthook`).
    fn job_infix(self) -> &'static str {
        match self {
            HookPhase::Before => "prehook",
            HookPhase::After => "posthook",
        }
    }
}

/// An aborting hook failure: which hook, which form, and an actionable why.
#[derive(Debug, Clone)]
pub struct HookFailure {
    /// Zero-based index into the phase's hook list.
    pub index: usize,
    /// The hook form (`Hook::kind_str`).
    pub kind: &'static str,
    /// What failed and why (command stderr / Job state / HTTP status).
    pub message: String,
}

impl HookFailure {
    /// The full what/why/fix message for the condition + Event.
    pub fn condition_message(&self, phase: HookPhase, policy_name: &str) -> String {
        format!(
            "{} hook #{} ({}) failed: {}. Fix the hook in SnapshotPolicy `{policy_name}` \
             spec.hooks.{}, or set continueOnFailure: true on it to proceed despite failures",
            phase.list_name(),
            self.index,
            self.kind,
            self.message,
            phase.list_name(),
        )
    }
}

/// Run a phase's hooks in order. Returns `Ok(None)` when every hook passed (or
/// failed with `continueOnFailure: true` — logged and recorded by the caller's
/// condition, never silently dropped), `Ok(Some(failure))` for the first
/// aborting failure, and `Err` only for cluster-IO problems (transient).
pub async fn run_hooks(
    ctx: &Context,
    namespace: &str,
    owner: &k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference,
    snapshot_name: &str,
    hooks: &[Hook],
    phase: HookPhase,
) -> Result<Option<HookFailure>> {
    for (index, hook) in hooks.iter().enumerate() {
        // Exhaustive: a new Hook form must be executed (and given a
        // continueOnFailure read) before this compiles.
        let (outcome, continue_on_failure) = match hook {
            Hook::WorkloadExec(h) => (
                run_workload_exec(ctx, namespace, h).await?,
                h.continue_on_failure,
            ),
            Hook::RunJob(h) => (
                run_job_hook(ctx, namespace, owner, snapshot_name, phase, index, h).await?,
                h.continue_on_failure,
            ),
            Hook::HttpRequest(h) => (run_http_hook(h).await, h.continue_on_failure),
        };
        if let Err(message) = outcome {
            if continue_on_failure {
                tracing::warn!(
                    snapshot = %snapshot_name,
                    hook = index,
                    kind = hook.kind_str(),
                    phase = phase.list_name(),
                    %message,
                    "hook failed; continuing (continueOnFailure: true)"
                );
                continue;
            }
            return Ok(Some(HookFailure {
                index,
                kind: hook.kind_str(),
                message,
            }));
        }
        tracing::info!(
            snapshot = %snapshot_name,
            hook = index,
            kind = hook.kind_str(),
            phase = phase.list_name(),
            "hook completed"
        );
    }
    Ok(None)
}

/// The effective timeout for a hook (`timeout` parsed, else the default).
fn hook_timeout(timeout: Option<&str>) -> Duration {
    timeout
        .and_then(parse_go_duration)
        .unwrap_or(DEFAULT_HOOK_TIMEOUT)
}

/// Pick the workload pod a `workloadExec` hook execs into: a `Running` pod
/// matching the selector. Pure over the listed pods; returns the actionable
/// hook-failure message when nothing usable matches.
pub fn pick_exec_pod<'a>(
    pods: &'a [Pod],
    selector_query: &str,
    namespace: &str,
) -> std::result::Result<&'a Pod, String> {
    if pods.is_empty() {
        return Err(format!(
            "no pod matches the hook podSelector (`{selector_query}`) in namespace \
             `{namespace}` — the workload must be running for an exec hook; scale it up or \
             fix the selector"
        ));
    }
    pods.iter()
        .find(|p| {
            p.status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .is_some_and(|ph| ph == "Running")
        })
        .ok_or_else(|| {
            format!(
                "no RUNNING pod matches the hook podSelector (`{selector_query}`) in namespace \
                 `{namespace}` ({} matched, none Running) — an exec hook needs a running \
                 container",
                pods.len()
            )
        })
}

/// Exec the hook command in a matched workload pod/container, waiting for its
/// exit status (bounded by the hook timeout). The inner `Err(String)` is the
/// hook failing (non-zero exit, bad selector, timeout); the outer `Err` is kube IO.
async fn run_workload_exec(
    ctx: &Context,
    namespace: &str,
    hook: &WorkloadExecHook,
) -> Result<std::result::Result<(), String>> {
    let PodSelector {
        pod_selector,
        container,
    } = &hook.selector;
    let query = io::label_selector_to_string(pod_selector);
    if query.is_empty() {
        return Ok(Err(
            "the hook podSelector is empty — set matchLabels/matchExpressions identifying the \
             workload pod to exec into"
                .to_string(),
        ));
    }
    let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), namespace);
    let listed = pods.list(&ListParams::default().labels(&query)).await?;
    let pod = match pick_exec_pod(&listed.items, &query, namespace) {
        Ok(p) => p,
        Err(msg) => return Ok(Err(msg)),
    };
    let pod_name = pod.name_any();

    let mut params = AttachParams::default().stdout(false).stderr(true);
    if let Some(c) = container {
        params = params.container(c.clone());
    }
    let timeout = hook_timeout(hook.timeout.as_deref());
    let exec = pods.exec(&pod_name, hook.command.clone(), &params);
    let mut attached = match tokio::time::timeout(timeout, exec).await {
        Ok(Ok(a)) => a,
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => {
            return Ok(Err(format!(
                "exec into pod `{pod_name}` did not start within {timeout:?}"
            )));
        }
    };

    // Drain stderr (bounded) so a chatty command can't stall on a full pipe,
    // then read the exit status from the websocket status channel.
    let mut stderr_buf = Vec::new();
    let drained = async {
        if let Some(mut stderr) = attached.stderr() {
            let _ = (&mut stderr)
                .take(HOOK_OUTPUT_CAP as u64)
                .read_to_end(&mut stderr_buf)
                .await;
        }
        attached.take_status()?.await
    };
    let status = match tokio::time::timeout(timeout, drained).await {
        Ok(s) => s,
        Err(_) => {
            return Ok(Err(format!(
                "command {:?} in pod `{pod_name}` did not finish within {timeout:?} — raise the \
                 hook `timeout` or make the command faster",
                hook.command
            )));
        }
    };
    let stderr_tail = String::from_utf8_lossy(&stderr_buf).trim().to_string();
    match status {
        Some(s) if s.status.as_deref() == Some("Success") => Ok(Ok(())),
        Some(s) => {
            let detail = s.message.unwrap_or_else(|| "non-zero exit".to_string());
            Ok(Err(if stderr_tail.is_empty() {
                format!(
                    "command {:?} in pod `{pod_name}` failed: {detail}",
                    hook.command
                )
            } else {
                format!(
                    "command {:?} in pod `{pod_name}` failed: {detail}; stderr: {stderr_tail}",
                    hook.command
                )
            }))
        }
        None => Ok(Err(format!(
            "exec into pod `{pod_name}` returned no status (connection closed early)"
        ))),
    }
}

/// Materialize the hook's `JobSpec` as a one-shot Job owned by the Snapshot
/// (GC'd with it), then wait for it to finish (bounded by the hook timeout).
async fn run_job_hook(
    ctx: &Context,
    namespace: &str,
    owner: &k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference,
    snapshot_name: &str,
    phase: HookPhase,
    index: usize,
    hook: &RunJobHook,
) -> Result<std::result::Result<(), String>> {
    let name =
        crate::snapshot::capped_name(&format!("{snapshot_name}-{}-{index}", phase.job_infix()));
    let job = Job {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace.to_string()),
            labels: Some(io::child_labels(&[(
                "kopiur.home-operations.com/op",
                "hook",
            )])),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        spec: Some(hook.job_spec.clone()),
        status: None,
    };
    let jobs: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);
    // Create-or-reuse: a requeue between create and terminal must watch the
    // SAME Job, not spawn a second run of a side-effecting hook.
    match jobs.create(&PostParams::default(), &job).await {
        Ok(_) => {}
        Err(kube::Error::Api(e)) if e.code == 409 => {}
        Err(e) => return Err(e.into()),
    }

    let timeout = hook_timeout(hook.timeout.as_deref());
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let current = jobs.get(&name).await?;
        match crate::snapshot::job_terminal_state(&current) {
            Some(true) => return Ok(Ok(())),
            Some(false) => {
                return Ok(Err(format!(
                    "hook Job `{name}` failed — inspect `kubectl logs -n {namespace} \
                     --selector=job-name={name}` for the command's output"
                )));
            }
            None => {
                if tokio::time::Instant::now() >= deadline {
                    return Ok(Err(format!(
                        "hook Job `{name}` did not finish within {timeout:?} — raise the hook \
                         `timeout`, or check why its pod is not completing (`kubectl describe \
                         job -n {namespace} {name}`)"
                    )));
                }
                tokio::time::sleep(JOB_POLL_INTERVAL).await;
            }
        }
    }
}

/// Issue the hook's HTTP request. URL userinfo (`https://user:pass@host/…`) is
/// extracted into Basic auth explicitly — relying on implicit client handling
/// would silently drop credentials. Non-2xx (and transport errors) are hook
/// failures, never reconcile errors: the endpoint is the user's system.
async fn run_http_hook(hook: &HttpRequestHook) -> std::result::Result<(), String> {
    let mut url = reqwest::Url::parse(&hook.url)
        .map_err(|e| format!("hook url {:?} is not a valid URL: {e}", hook.url))?;
    let (user, pass) = (
        url.username().to_string(),
        url.password().map(str::to_string),
    );
    if !user.is_empty() {
        // Move credentials out of the URL and into the Authorization header.
        let _ = url.set_username("");
        let _ = url.set_password(None);
    }
    let method = match hook.method.as_deref() {
        None => reqwest::Method::POST,
        Some(m) => reqwest::Method::from_bytes(m.to_ascii_uppercase().as_bytes())
            .map_err(|_| format!("hook method {m:?} is not a valid HTTP method"))?,
    };
    let timeout = hook_timeout(hook.timeout.as_deref());
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| format!("could not build the hook HTTP client: {e}"))?;
    let mut req = client.request(method.clone(), url.clone());
    if !user.is_empty() {
        req = req.basic_auth(&user, pass.as_deref());
    }
    if let Some(body) = &hook.body {
        req = req.body(body.clone());
    }
    let resp = req.send().await.map_err(|e| {
        format!(
            "{method} {url} failed: {e} — is the endpoint reachable from the operator (and \
             within the hook timeout {timeout:?})?"
        )
    })?;
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body = resp.text().await.unwrap_or_default();
    let snippet: String = body.chars().take(HOOK_OUTPUT_CAP).collect();
    Err(format!("{method} {url} returned {status}: {snippet}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(name: &str, phase: Option<&str>) -> Pod {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": { "name": name },
            "status": phase.map(|p| serde_json::json!({ "phase": p })),
        }))
        .unwrap()
    }

    #[test]
    fn pick_exec_pod_prefers_running_and_messages_are_actionable() {
        // Empty list: names the selector, the namespace, and the fix.
        let err = pick_exec_pod(&[], "app=pg", "billing").unwrap_err();
        assert!(err.contains("app=pg"), "{err}");
        assert!(err.contains("billing"), "{err}");
        assert!(err.contains("scale it up or"), "{err}");

        // A Running pod is selected over a Pending one.
        let pods = vec![
            pod("pending", Some("Pending")),
            pod("running", Some("Running")),
        ];
        assert_eq!(
            pick_exec_pod(&pods, "app=pg", "billing")
                .unwrap()
                .name_any(),
            "running"
        );

        // Matches but none Running: a distinct, actionable message.
        let pods = vec![pod("pending", Some("Pending"))];
        let err = pick_exec_pod(&pods, "app=pg", "billing").unwrap_err();
        assert!(err.contains("none Running"), "{err}");
    }

    #[test]
    fn hook_timeout_parses_or_defaults() {
        assert_eq!(hook_timeout(Some("2m")), Duration::from_secs(120));
        assert_eq!(hook_timeout(None), DEFAULT_HOOK_TIMEOUT);
        // Unparseable falls back to the default (the webhook rejects it at
        // admission; this is the defensive path).
        assert_eq!(hook_timeout(Some("soon")), DEFAULT_HOOK_TIMEOUT);
    }

    #[test]
    fn phase_strings_cover_both_lists() {
        assert_eq!(HookPhase::Before.list_name(), "beforeSnapshot");
        assert_eq!(HookPhase::After.list_name(), "afterSnapshot");
        assert_eq!(HookPhase::Before.failed_reason(), "PreHookFailed");
        assert_eq!(HookPhase::After.failed_reason(), "PostHookFailed");
    }

    #[test]
    fn hook_failure_condition_message_names_hook_and_both_fixes() {
        let f = HookFailure {
            index: 1,
            kind: "WorkloadExec",
            message: "command [\"pg_backup_start\"] in pod `pg-0` failed: non-zero exit".into(),
        };
        let msg = f.condition_message(HookPhase::Before, "postgres-data");
        assert!(
            msg.contains("beforeSnapshot hook #1 (WorkloadExec)"),
            "{msg}"
        );
        assert!(msg.contains("postgres-data"), "{msg}");
        assert!(msg.contains("continueOnFailure: true"), "{msg}");
    }

    #[tokio::test]
    async fn http_hook_rejects_bad_url_and_method_with_actionable_messages() {
        let bad_url = HttpRequestHook {
            url: "not a url".into(),
            method: None,
            body: None,
            timeout: None,
            continue_on_failure: false,
        };
        let err = run_http_hook(&bad_url).await.unwrap_err();
        assert!(err.contains("not a valid URL"), "{err}");

        let bad_method = HttpRequestHook {
            url: "http://example.invalid/notify".into(),
            method: Some("FETCH IT".into()),
            body: None,
            timeout: None,
            continue_on_failure: false,
        };
        let err = run_http_hook(&bad_method).await.unwrap_err();
        assert!(err.contains("not a valid HTTP method"), "{err}");
    }
}
