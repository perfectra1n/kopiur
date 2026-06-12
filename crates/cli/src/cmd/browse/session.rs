//! The in-cluster session transport: a mover `Job` (built with the same pure
//! builders the operator uses) connects to the repository **read-only**, signals
//! readiness, and idles for its TTL; the CLI execs the closed
//! [`SessionCmd`](kopiur_kopia::SessionCmd) surface into it. One warm session
//! per repository — repeated `ls`/`cat` reuse it instantly.

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use kube::api::{Api, AttachParams, DeleteParams, ListParams, LogParams, PostParams};
use tokio::io::AsyncReadExt;

use kopiur_api::common::RepositoryKind;
use kopiur_api::consts::{SESSION_BROWSE, SESSION_LABEL, SESSION_REPO_LABEL};
use kopiur_kopia::SessionCmd;
use kopiur_mover::jobs::{self, JobLimits, MoverJobInputs, VolumeMountSpec};
use kopiur_mover::repo_meta::{
    backend_to_repository_connect, filesystem_repo_mount_source, filesystem_repo_path,
};
use kopiur_mover::workspec::{
    BrowseSessionOp, MoverWorkSpec, Operation, ResolvedIdentity, TargetRef,
};

use super::resolve::{BrowseTarget, RepoHandle, session_creds_secrets};
use crate::context::KubeCtx;
use crate::error::{CliError, classify_kube};

/// The kopia binary inside the mover image (distroless: fixed path, no PATH).
pub const SESSION_KOPIA_BIN: &str = "/usr/local/bin/kopia";
/// The mover binary inside the image, re-invoked by the readinessProbe.
pub const SESSION_MOVER_BIN: &str = "/usr/local/bin/kopiur-mover";
/// The upstream label the Job controller stamps on its pods.
const JOB_NAME_LABEL: &str = "batch.kubernetes.io/job-name";
/// How long `ensure` waits for the session pod to become Ready. Covers image
/// pull + scheduling + a cold object-store connect; the pod's own probe budget
/// (2s × 60) sits inside this.
const READY_WAIT: Duration = Duration::from_secs(300);
/// The env var on the controller Deployment naming the mover image (mirrors
/// `kopiur_controller::config::MOVER_IMAGE_ENV` without a controller dep).
const MOVER_IMAGE_ENV: &str = "KOPIUR_MOVER_IMAGE";

/// FNV-1a 64-bit — a tiny, dependency-free stable hash for deterministic
/// resource names. NOT a security boundary, just name disambiguation.
fn fnv1a64(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Deterministic session Job name for a repository:
/// `kopiur-browse-<name≤24>-<hash8 of kind/ns/name>`. Deterministic so a second
/// CLI invocation finds (and reuses) the warm session; hashed so two
/// repositories whose truncated names collide still get distinct Jobs. Always
/// well under the 63-char limit pod names derive from.
pub fn session_job_name(kind: RepositoryKind, namespace: Option<&str>, name: &str) -> String {
    let key = format!("{kind:?}/{}/{name}", namespace.unwrap_or_default());
    let hash8 = format!("{:016x}", fnv1a64(&key));
    let short: String = name.chars().take(24).collect();
    format!("kopiur-browse-{short}-{}", &hash8[..8])
}

/// The labels stamped on a session Job (and selected on to find it):
/// `session=browse` + `session-repo=<Kind>-<name>`.
pub fn session_labels(kind: RepositoryKind, name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (SESSION_LABEL.to_string(), SESSION_BROWSE.to_string()),
        (
            SESSION_REPO_LABEL.to_string(),
            session_repo_label_value(kind, name),
        ),
    ])
}

/// The `session-repo` label value, `<Kind>-<name>`, clamped to the 63-char
/// label-value limit (the hashed Job name keeps clamped repositories distinct).
pub fn session_repo_label_value(kind: RepositoryKind, name: &str) -> String {
    let kind_str = match kind {
        RepositoryKind::Repository => "Repository",
        RepositoryKind::ClusterRepository => "ClusterRepository",
    };
    let mut v = format!("{kind_str}-{name}");
    v.truncate(63);
    v
}

/// Pull the mover image out of the controller Deployment's
/// `KOPIUR_MOVER_IMAGE` env (any container), so session pods run the exact
/// image the operator stamps into its own movers. Pure.
pub fn mover_image_from_deployments(deployments: &[Deployment]) -> Option<String> {
    deployments.iter().find_map(|d| {
        d.spec
            .as_ref()?
            .template
            .spec
            .as_ref()?
            .containers
            .iter()
            .find_map(|c| {
                c.env.as_ref()?.iter().find_map(|e| {
                    (e.name == MOVER_IMAGE_ENV)
                        .then(|| e.value.clone())
                        .flatten()
                })
            })
    })
}

/// Whether a session Job is already finished/failed/being-deleted (so a new
/// one must replace it rather than be "reused"). Pure.
pub fn job_is_terminal(job: &Job) -> bool {
    if job.metadata.deletion_timestamp.is_some() {
        return true;
    }
    job.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|conds| {
            conds
                .iter()
                .any(|c| (c.type_ == "Complete" || c.type_ == "Failed") && c.status == "True")
        })
}

/// Build the session's mover work spec. Browse ignores the identity/targetRef
/// operationally, but they are filled honestly: the identity names the CLI as
/// the actor, the targetRef names the repository the session holds open. Pure.
pub fn session_work_spec(target: &BrowseTarget, ttl: Duration) -> MoverWorkSpec {
    MoverWorkSpec {
        version: 1,
        operation: Operation::BrowseSession(BrowseSessionOp {
            ttl_seconds: ttl.as_secs(),
        }),
        identity: ResolvedIdentity {
            username: "kubectl-kopiur-browse".into(),
            hostname: target.namespace.clone(),
            source_path: String::new(),
        },
        repository: backend_to_repository_connect(&target.repo.backend),
        target_ref: TargetRef {
            api_version: kopiur_api::consts::API_VERSION.into(),
            kind: match target.repo.kind {
                RepositoryKind::Repository => "Repository".into(),
                RepositoryKind::ClusterRepository => "ClusterRepository".into(),
            },
            name: target.repo.name.clone(),
            namespace: target.namespace.clone(),
        },
        hook_plan: Default::default(),
        options: Default::default(),
        cache: Default::default(),
        throttle: Default::default(),
    }
}

/// A live, Ready session pod the CLI can exec the closed [`SessionCmd`]
/// surface into.
pub struct ExecSession {
    client: kube::Client,
    /// The session Job's (and ConfigMap's) namespace.
    pub namespace: String,
    /// The session Job name.
    pub job_name: String,
    /// The Ready pod execed into.
    pub pod: String,
}

impl ExecSession {
    /// Find-or-create the session Job for the target's repository, then wait
    /// for its pod to become Ready (the read-only connect succeeded).
    pub async fn ensure(
        ctx: &KubeCtx,
        target: &BrowseTarget,
        ttl: Duration,
    ) -> Result<ExecSession, CliError> {
        let ns = target.namespace.clone();
        let jobs_api: Api<Job> = Api::namespaced(ctx.client.clone(), &ns);
        let labels = session_labels(target.repo.kind, &target.repo.name);
        let selector = labels
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");

        let expected_name = session_job_name(
            target.repo.kind,
            target.repo.namespace.as_deref(),
            &target.repo.name,
        );
        let existing = jobs_api
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| classify_kube("list", "Job", "jobs", Some(&ns), None, e))?
            .items
            .into_iter()
            // The session-repo LABEL clamps at 63 chars, so two long repo
            // names can share it — the deterministic NAME disambiguates.
            .find(|j| j.metadata.name.as_deref() == Some(expected_name.as_str()));

        let job_name = match existing {
            Some(job) if !job_is_terminal(&job) => {
                eprintln!("reusing warm browse session {}", job.name_any());
                job.name_any()
            }
            Some(job) => {
                // A finished/expired session under the deterministic name: replace it.
                let name = job.name_any();
                let _ = jobs_api.delete(&name, &DeleteParams::background()).await;
                wait_job_gone(&jobs_api, &name).await?;
                create_session_job(ctx, target, ttl, &labels).await?
            }
            None => create_session_job(ctx, target, ttl, &labels).await?,
        };

        let pod = wait_pod_ready(&ctx.client, &ns, &job_name).await?;
        Ok(ExecSession {
            client: ctx.client.clone(),
            namespace: ns,
            job_name,
            pod,
        })
    }

    /// Exec one session command and capture its full stdout (manifest/list
    /// JSON). Non-zero exits surface kopia's stderr.
    pub async fn exec_capture(&self, cmd: SessionCmd) -> Result<Vec<u8>, CliError> {
        let mut buf = Vec::new();
        self.exec_stream(cmd, &mut buf).await?;
        Ok(buf)
    }

    /// Exec one session command, streaming stdout **byte-for-byte** into
    /// `sink` (the file-content path; binary-safe, no line splitting). Returns
    /// the byte count.
    pub async fn exec_stream(
        &self,
        cmd: SessionCmd,
        sink: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<u64, CliError> {
        let argv = cmd.argv(SESSION_KOPIA_BIN);
        let what = argv[1..].join(" ");
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let mut attached = pods
            .exec(
                &self.pod,
                argv.clone(),
                &AttachParams::default()
                    .stdin(false)
                    .stdout(true)
                    .stderr(true),
            )
            .await
            .map_err(|e| {
                classify_kube(
                    "create",
                    "Pod",
                    "pods/exec",
                    Some(&self.namespace),
                    Some(&self.pod),
                    e,
                )
            })?;

        let mut stdout = attached.stdout().ok_or_else(|| CliError::SessionExec {
            what: what.clone(),
            stderr: "no stdout stream attached".into(),
        })?;
        let mut stderr = attached.stderr().ok_or_else(|| CliError::SessionExec {
            what: what.clone(),
            stderr: "no stderr stream attached".into(),
        })?;
        let status_fut = attached
            .take_status()
            .ok_or_else(|| CliError::SessionExec {
                what: what.clone(),
                stderr: "no status stream attached".into(),
            })?;

        let copy = tokio::io::copy(&mut stdout, sink);
        let read_err = async {
            let mut buf = String::new();
            let _ = stderr.read_to_string(&mut buf).await;
            buf
        };
        let (copied, err_text, status) = tokio::join!(copy, read_err, status_fut);
        let n = copied.map_err(|source| CliError::LocalIo {
            what: format!("streaming `{what}` output"),
            source,
        })?;

        // The apiserver reports the exec result as a V1Status on the error
        // channel; anything but an explicit Success is a failed read.
        let ok = status
            .as_ref()
            .is_some_and(|s| s.status.as_deref() == Some("Success"));
        if ok {
            Ok(n)
        } else {
            let detail = status
                .as_ref()
                .and_then(|s| s.message.clone())
                .unwrap_or_default();
            let stderr_text = if err_text.trim().is_empty() {
                detail
            } else {
                err_text.trim_end().to_string()
            };
            Err(CliError::SessionExec {
                what,
                stderr: stderr_text,
            })
        }
    }
}

/// Create the session Job (FIRST, so it can own the ConfigMap) and its
/// work-spec ConfigMap, returning the Job name.
async fn create_session_job(
    ctx: &KubeCtx,
    target: &BrowseTarget,
    ttl: Duration,
    labels: &BTreeMap<String, String>,
) -> Result<String, CliError> {
    let ns = &target.namespace;
    let name = session_job_name(
        target.repo.kind,
        target.repo.namespace.as_deref(),
        &target.repo.name,
    );
    let creds_secrets = session_creds_secrets(&target.repo, ns)?;
    let work_spec = session_work_spec(target, ttl);
    let image = resolve_mover_image(ctx).await?;

    // The repository volume (filesystem backends) is mounted READ-ONLY — the
    // kopia connect is already read-only; this makes the mount match.
    let repo_volume =
        filesystem_repo_mount_source(&target.repo.backend).map(|source| VolumeMountSpec {
            source,
            mount_path: filesystem_repo_path(&target.repo.backend)
                .expect("a backend with a repo volume has a repo path"),
            read_only: true,
        });

    let inputs = MoverJobInputs {
        name: &name,
        namespace: ns,
        owner: owner_ref_for_repo(&target.repo),
        work_spec: &work_spec,
        image: &image,
        image_pull_policy: None,
        limits: JobLimits {
            backoff_limit: 0,
            // The TTL bounds the session; the deadline backstops a pod that
            // never finishes connecting.
            active_deadline_seconds: Some(ttl.as_secs() as i64 + 120),
            ttl_seconds_after_finished: Some(60),
        },
        resources: None,
        // The hardened baseline every mover gets when no moverDefaults/recipe
        // overlays apply — a read-only session never needs elevation.
        security_context: kopiur_api::common::hardened_security_context(),
        pod_security_context: None,
        node_selector: None,
        tolerations: None,
        affinity: None,
        labels: labels.clone(),
        source_volume: None,
        repo_volume,
        creds_secrets,
        result_configmap: None,
        // The session pod never talks to the kube API: no ServiceAccount
        // (falls back to the namespace default with no extra rights).
        service_account: None,
        passthrough_env: Vec::new(),
        annotations: Default::default(),
        cache_volume: Default::default(),
        readiness_exec: Some(vec![SESSION_MOVER_BIN.to_string(), "ready".to_string()]),
    };

    let job = jobs::build_job(&inputs);
    let jobs_api: Api<Job> = Api::namespaced(ctx.client.clone(), ns);
    eprintln!(
        "starting browse session {name} (repository {})…",
        target.repo.name
    );
    let created = match jobs_api.create(&PostParams::default(), &job).await {
        Ok(created) => created,
        // A concurrent CLI won the race: the session exists — reuse it (the
        // winner also creates the ConfigMap; skip ours).
        Err(kube::Error::Api(ae)) if ae.code == 409 => {
            eprintln!("a concurrent command already started this session; joining it");
            return Ok(name);
        }
        Err(e) => {
            return Err(classify_kube(
                "create",
                "Job",
                "jobs",
                Some(ns),
                Some(&name),
                e,
            ));
        }
    };

    // The ConfigMap is owned by the JOB (not the repository) so `session end`
    // and the Job TTL cascade-delete it.
    let mut cm = jobs::build_config_map(&inputs).map_err(|source| CliError::Serialization {
        what: "browse session work spec",
        source: source.into(),
    })?;
    cm.metadata.owner_references = Some(vec![OwnerReference {
        api_version: "batch/v1".into(),
        kind: "Job".into(),
        name: created.name_any(),
        uid: created.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(false),
    }]);
    let cms: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), ns);
    match cms.create(&PostParams::default(), &cm).await {
        Ok(_) => {}
        // A leftover CM under the deterministic name (e.g. from a session whose
        // Job the TTL controller already reaped): replace it.
        Err(kube::Error::Api(ae)) if ae.code == 409 => {
            let _ = cms.delete(&name, &DeleteParams::default()).await;
            cms.create(&PostParams::default(), &cm).await.map_err(|e| {
                classify_kube(
                    "create",
                    "ConfigMap",
                    "configmaps",
                    Some(ns),
                    Some(&name),
                    e,
                )
            })?;
        }
        Err(e) => {
            return Err(classify_kube(
                "create",
                "ConfigMap",
                "configmaps",
                Some(ns),
                Some(&name),
                e,
            ));
        }
    }
    Ok(created.name_any())
}

/// The session Job's ownerReference: the Repository/ClusterRepository it holds
/// open, so deleting the repository reaps any session.
fn owner_ref_for_repo(repo: &RepoHandle) -> OwnerReference {
    let kind = match repo.kind {
        RepositoryKind::Repository => "Repository",
        RepositoryKind::ClusterRepository => "ClusterRepository",
    };
    jobs::owner_ref(kind, &repo.name, &repo.uid)
}

/// Resolve the mover image from the controller Deployment (label
/// `app.kubernetes.io/name=kopiur,app.kubernetes.io/component=controller`,
/// env `KOPIUR_MOVER_IMAGE`). FAILS rather than degrades:
/// - the pinned [`DEFAULT_MOVER_IMAGE`] fallback would be a 2-versions-stale
///   mover that cannot parse a BrowseSession work spec — a guaranteed-broken
///   session is worse than an actionable error;
/// - the lookup demands EXACTLY ONE matching Deployment cluster-wide, so a
///   spoofed kopiur-labeled Deployment in another namespace cannot silently
///   redirect session pods (and their repository credentials) to an
///   attacker-chosen image — two matches fail closed and name both.
///
/// Deliberately resolve-only: there is no `--image` flag, so a session always
/// runs what the operator runs.
async fn resolve_mover_image(ctx: &KubeCtx) -> Result<String, CliError> {
    let api: Api<Deployment> = Api::all(ctx.client.clone());
    let selector = "app.kubernetes.io/name=kopiur,app.kubernetes.io/component=controller";
    let list = api
        .list(&ListParams::default().labels(selector))
        .await
        .map_err(|e| {
            crate::error::classify_kube("list", "Deployment", "deployments", None, None, e)
        })?;
    match list.items.as_slice() {
        [] => Err(CliError::MoverImageUnresolvable {
            why: format!("no Deployment matches {selector} in any namespace"),
            fix: "is the kopiur operator installed? Sessions run the operator's mover image, \
                  so browsing needs a running install"
                .into(),
        }),
        [only] => mover_image_from_deployments(std::slice::from_ref(only)).ok_or_else(|| {
            CliError::MoverImageUnresolvable {
                why: format!(
                    "the controller Deployment {}/{} has no {MOVER_IMAGE_ENV} env var",
                    only.metadata.namespace.clone().unwrap_or_default(),
                    only.metadata.name.clone().unwrap_or_default()
                ),
                fix: "check the chart/deployment; the operator must know its mover image".into(),
            }
        }),
        many => Err(CliError::MoverImageUnresolvable {
            why: format!(
                "{} Deployments match {selector}: {} — refusing to guess which mover image \
                 to trust",
                many.len(),
                many.iter()
                    .map(|d| format!(
                        "{}/{}",
                        d.metadata.namespace.clone().unwrap_or_default(),
                        d.metadata.name.clone().unwrap_or_default()
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            fix: "remove the impostor/stale Deployment (only one kopiur controller should \
                  exist), or scope your kubeconfig to the real one"
                .into(),
        }),
    }
}

/// Poll until the session Job's pod reports Ready (the mover wrote its
/// readiness marker after the read-only connect), with progress on stderr. On
/// pod failure, surfaces the pod's log tail.
async fn wait_pod_ready(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
) -> Result<String, CliError> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let selector = format!("{JOB_NAME_LABEL}={job_name}");
    let deadline = std::time::Instant::now() + READY_WAIT;
    eprint!("waiting for the session pod to connect (read-only)");
    let result = loop {
        if std::time::Instant::now() > deadline {
            break Err(CliError::SessionNotReady {
                job: job_name.to_string(),
                after: format!("{}s", READY_WAIT.as_secs()),
            });
        }
        let list = pods
            .list(&ListParams::default().labels(&selector))
            .await
            .map_err(|e| classify_kube("list", "Pod", "pods", Some(namespace), None, e));
        let list = match list {
            Ok(l) => l.items,
            Err(e) => break Err(e),
        };
        if let Some(pod) = list.iter().find(|p| pod_is_ready(p)) {
            break Ok(pod.name_any());
        }
        if let Some(failed) = list.iter().find(|p| pod_phase(p) == Some("Failed")) {
            let tail = pod_log_tail(&pods, &failed.name_any()).await;
            break Err(CliError::SessionPodFailed {
                job: job_name.to_string(),
                namespace: namespace.to_string(),
                detail: tail,
            });
        }
        eprint!(".");
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    eprintln!();
    result
}

/// Whether a pod's `Ready` condition is `True`. Pure.
fn pod_is_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|conds| {
            conds
                .iter()
                .any(|c| c.type_ == "Ready" && c.status == "True")
        })
}

/// A pod's phase string, if reported.
fn pod_phase(pod: &Pod) -> Option<&str> {
    pod.status.as_ref().and_then(|s| s.phase.as_deref())
}

/// Best-effort tail of a failed session pod's logs for the error message.
async fn pod_log_tail(pods: &Api<Pod>, pod: &str) -> String {
    match pods
        .logs(
            pod,
            &LogParams {
                tail_lines: Some(15),
                ..Default::default()
            },
        )
        .await
    {
        Ok(text) if !text.trim().is_empty() => text.trim_end().to_string(),
        _ => "(no pod logs available)".to_string(),
    }
}

/// Poll until a Job is fully gone (its replacement can then be created under
/// the same deterministic name).
async fn wait_job_gone(jobs_api: &Api<Job>, name: &str) -> Result<(), CliError> {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        match jobs_api.get_opt(name).await {
            Ok(None) => return Ok(()),
            Ok(Some(_)) if std::time::Instant::now() > deadline => {
                return Err(CliError::SessionNotReady {
                    job: name.to_string(),
                    after: "60s (deleting the previous, expired session)".to_string(),
                });
            }
            Ok(Some(_)) => tokio::time::sleep(Duration::from_millis(500)).await,
            Err(e) => {
                return Err(classify_kube("get", "Job", "jobs", None, Some(name), e));
            }
        }
    }
}

/// Find the session Job for a repository (the `session end` lookup). Returns
/// `None` when no session exists.
pub async fn find_session_job(
    ctx: &KubeCtx,
    namespace: &str,
    kind: RepositoryKind,
    repo_namespace: Option<&str>,
    repo_name: &str,
) -> Result<Option<Job>, CliError> {
    let jobs_api: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);
    let labels = session_labels(kind, repo_name);
    let selector = labels
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");
    let expected = session_job_name(kind, repo_namespace, repo_name);
    let list = jobs_api
        .list(&ListParams::default().labels(&selector))
        .await
        .map_err(|e| classify_kube("list", "Job", "jobs", Some(namespace), None, e))?;
    // Same clamped-label caveat as `ensure`: confirm by deterministic name.
    Ok(list
        .items
        .into_iter()
        .find(|j| j.metadata.name.as_deref() == Some(expected.as_str())))
}

/// Delete a session Job (background propagation reaps its pod and the
/// Job-owned ConfigMap; the ConfigMap is deleted explicitly too for
/// promptness).
pub async fn delete_session(
    ctx: &KubeCtx,
    namespace: &str,
    job_name: &str,
) -> Result<(), CliError> {
    let jobs_api: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);
    jobs_api
        .delete(job_name, &DeleteParams::background())
        .await
        .map_err(|e| classify_kube("delete", "Job", "jobs", Some(namespace), Some(job_name), e))?;
    let cms: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
    let _ = cms.delete(job_name, &DeleteParams::default()).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_job_name_is_deterministic_distinct_and_short() {
        let a = session_job_name(RepositoryKind::Repository, Some("media"), "nas");
        let b = session_job_name(RepositoryKind::Repository, Some("media"), "nas");
        assert_eq!(a, b, "deterministic — a rerun finds the warm session");
        assert!(a.starts_with("kopiur-browse-nas-"), "{a}");

        // Same name, different kind/namespace → different Job.
        let c = session_job_name(RepositoryKind::ClusterRepository, None, "nas");
        let d = session_job_name(RepositoryKind::Repository, Some("other"), "nas");
        assert_ne!(a, c);
        assert_ne!(a, d);
        assert_ne!(c, d);

        // A maximal CRD name stays comfortably under the pod-name budget.
        let long = "x".repeat(253);
        let e = session_job_name(RepositoryKind::Repository, Some("media"), &long);
        assert!(e.len() <= 52, "{} chars: {e}", e.len());
    }

    #[test]
    fn session_labels_carry_the_wire_contract_keys() {
        let labels = session_labels(RepositoryKind::Repository, "nas");
        assert_eq!(labels[SESSION_LABEL], "browse");
        assert_eq!(labels[SESSION_REPO_LABEL], "Repository-nas");
        let labels = session_labels(RepositoryKind::ClusterRepository, "shared");
        assert_eq!(labels[SESSION_REPO_LABEL], "ClusterRepository-shared");
        // Label values are clamped to the k8s 63-char limit.
        let long = session_repo_label_value(RepositoryKind::Repository, &"y".repeat(100));
        assert_eq!(long.len(), 63);
    }

    #[test]
    fn mover_image_resolves_from_the_controller_env_or_none() {
        let dep: Deployment = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": "kopiur" },
            "spec": {
                "selector": { "matchLabels": { "app": "kopiur" } },
                "template": { "spec": { "containers": [{
                    "name": "controller",
                    "image": "ghcr.io/home-operations/kopiur-controller:v9",
                    "env": [
                        { "name": "OTHER", "value": "x" },
                        { "name": "KOPIUR_MOVER_IMAGE", "value": "ghcr.io/home-operations/kopiur-mover:v9" }
                    ]
                }] } }
            }
        }))
        .unwrap();
        assert_eq!(
            mover_image_from_deployments(&[dep]).as_deref(),
            Some("ghcr.io/home-operations/kopiur-mover:v9")
        );
        assert_eq!(mover_image_from_deployments(&[]), None);
    }

    #[test]
    fn terminal_job_detection_covers_complete_failed_and_deleting() {
        let job = |conds: serde_json::Value, deleting: bool| -> Job {
            let mut v = serde_json::json!({
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": { "name": "j" },
                "status": { "conditions": conds }
            });
            if deleting {
                v["metadata"]["deletionTimestamp"] = serde_json::json!("2026-06-11T00:00:00Z");
            }
            serde_json::from_value(v).unwrap()
        };
        assert!(!job_is_terminal(&job(serde_json::json!([]), false)));
        assert!(job_is_terminal(&job(
            serde_json::json!([{ "type": "Complete", "status": "True" }]),
            false
        )));
        assert!(job_is_terminal(&job(
            serde_json::json!([{ "type": "Failed", "status": "True" }]),
            false
        )));
        assert!(job_is_terminal(&job(serde_json::json!([]), true)));
    }

    #[test]
    fn work_spec_pins_the_ttl_and_read_only_browse_operation() {
        use kopiur_api::backend::{Backend, FilesystemBackend};
        use kopiur_api::common::{Encryption, SecretKeyRef};
        let target = BrowseTarget {
            snapshot: "db-1".into(),
            namespace: "media".into(),
            kopia_snapshot_id: "kdead".into(),
            repo: RepoHandle {
                kind: RepositoryKind::Repository,
                name: "nas".into(),
                uid: "u1".into(),
                namespace: Some("media".into()),
                backend: Backend::Filesystem(FilesystemBackend {
                    path: "/repo".into(),
                    volume: None,
                }),
                encryption: Encryption {
                    password_secret_ref: SecretKeyRef {
                        name: "creds".into(),
                        namespace: None,
                        key: None,
                    },
                },
            },
        };
        let ws = session_work_spec(&target, Duration::from_secs(1800));
        match &ws.operation {
            Operation::BrowseSession(op) => assert_eq!(op.ttl_seconds, 1800),
            other => panic!("expected BrowseSession, got {other:?}"),
        }
        assert_eq!(ws.target_ref.kind, "Repository");
        assert_eq!(ws.target_ref.name, "nas");
        assert_eq!(ws.target_ref.namespace, "media");
        // The repository connect mirrors the backend.
        let v = serde_json::to_value(&ws).unwrap();
        assert_eq!(v["repository"]["filesystem"]["path"], "/repo");
    }
}
