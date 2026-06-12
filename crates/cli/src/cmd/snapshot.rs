//! `kubectl kopiur snapshot now` — run a SnapshotPolicy immediately by
//! creating a manual `Snapshot` CR, optionally waiting for the terminal phase
//! (and streaming the mover's logs along the way).

use chrono::{DateTime, Utc};
use kopiur_api::common::FailurePolicy;
use kopiur_api::consts::{CONFIG_LABEL, ORIGIN_LABEL};
use kopiur_api::{Origin, PolicyRef, Snapshot, SnapshotPhase, SnapshotPolicy, SnapshotSpec};
use kube::api::{Api, PostParams};

use crate::CmdOutput;
use crate::cli::SnapshotNowArgs;
use crate::context::KubeCtx;
use crate::error::{CliError, classify_kube};
use crate::output::{OutputFormat, human_bytes};
use crate::wait::{DEFAULT_WAIT_TIMEOUT, wait_for};

/// Build the manual `Snapshot` CR for this invocation. Pure — `now` is
/// injected so names are deterministic under test. Mirrors what a
/// `SnapshotSchedule` stamps on its children (origin + config labels) so the
/// rest of the tooling (`snapshots list --policy`, the controller's
/// `resolve_origin`) treats it uniformly.
pub fn build_snapshot(args: &SnapshotNowArgs, namespace: &str, now: DateTime<Utc>) -> Snapshot {
    let name = args
        .name
        .clone()
        .unwrap_or_else(|| format!("{}-manual-{}", args.policy, now.format("%Y%m%d%H%M%S")));
    let failure_policy = if args.backoff_limit.is_some() || args.active_deadline_seconds.is_some() {
        Some(FailurePolicy {
            backoff_limit: args.backoff_limit,
            active_deadline_seconds: args.active_deadline_seconds,
        })
    } else {
        None
    };
    let mut snapshot = Snapshot::new(
        &name,
        SnapshotSpec {
            policy_ref: Some(PolicyRef {
                name: args.policy.clone(),
                namespace: None,
            }),
            tags: if args.tags.is_empty() {
                None
            } else {
                Some(args.tags.iter().cloned().collect())
            },
            failure_policy,
            deletion_policy: args.deletion_policy.map(Into::into),
            pin: args.pin,
        },
    );
    snapshot.metadata.namespace = Some(namespace.to_string());
    snapshot.metadata.labels = Some(
        [
            (
                ORIGIN_LABEL.to_string(),
                Origin::Manual.label_value().to_string(),
            ),
            (CONFIG_LABEL.to_string(), args.policy.clone()),
        ]
        .into(),
    );
    snapshot
}

/// What a terminal phase means for this command. Exhaustive over
/// [`SnapshotPhase`]: a new phase cannot compile until classified.
pub fn terminal(snapshot: &Snapshot) -> Option<Result<Box<Snapshot>, Box<Snapshot>>> {
    match snapshot.status.as_ref().and_then(|s| s.phase)? {
        SnapshotPhase::Pending | SnapshotPhase::Running => None,
        SnapshotPhase::Succeeded => Some(Ok(Box::new(snapshot.clone()))),
        SnapshotPhase::Failed => Some(Err(Box::new(snapshot.clone()))),
        // A manual Snapshot can only be Deleting if someone deleted it mid-run;
        // surface that as the failure path (the wait also catches the delete
        // event itself). Discovered is unreachable for a CR we just created
        // with a policyRef.
        SnapshotPhase::Deleting | SnapshotPhase::Discovered => {
            Some(Err(Box::new(snapshot.clone())))
        }
    }
}

/// One-line success summary from the terminal object's status.
pub fn success_summary(snapshot: &Snapshot) -> String {
    let status = snapshot.status.as_ref();
    let id = status
        .and_then(|s| s.snapshot.as_ref())
        .map(|i| i.kopia_snapshot_id.as_str())
        .unwrap_or("?");
    let size = status
        .and_then(|s| s.stats.as_ref())
        .and_then(|s| s.size_bytes)
        .map(human_bytes)
        .unwrap_or_else(|| "?".into());
    let duration = status
        .and_then(|s| s.timing.as_ref())
        .and_then(|t| t.duration_seconds)
        .map(|d| format!("{d}s"))
        .unwrap_or_else(|| "?".into());
    let name = snapshot.metadata.name.as_deref().unwrap_or("?");
    format!("snapshot {name} succeeded: kopia id {id}, {size}, took {duration}\n")
}

/// Failure detail from the terminal object's status (what/why + kopia stderr
/// tail + logTail), for stderr.
pub fn failure_detail(snapshot: &Snapshot) -> String {
    let name = snapshot.metadata.name.as_deref().unwrap_or("?");
    let mut out = format!("snapshot {name} failed");
    if let Some(f) = snapshot.status.as_ref().and_then(|s| s.failure.as_ref()) {
        out.push_str(&format!(" ({}): {}", f.kopia_error_class, f.message));
        if let Some(stderr) = &f.stderr_tail {
            out.push_str(&format!("\n--- kopia stderr tail ---\n{stderr}"));
        }
    }
    if let Some(tail) = snapshot.status.as_ref().and_then(|s| s.log_tail.as_ref()) {
        out.push_str(&format!("\n--- log tail ---\n{tail}"));
    }
    out.push('\n');
    out
}

/// Run `snapshot now`.
pub async fn run(
    ctx: &KubeCtx,
    args: &SnapshotNowArgs,
    output: OutputFormat,
    now: DateTime<Utc>,
) -> Result<CmdOutput, CliError> {
    if matches!(ctx.scope, crate::context::Scope::All) {
        return Err(CliError::AllNamespacesNotApplicable {
            command: "snapshot now",
        });
    }
    let ns = ctx.namespace.as_str();

    // Preflight: the recipe must exist (actionable miss beats a webhook 404
    // chain), and a suspended recipe deserves a heads-up (the run proceeds —
    // the operator is authoritative about what suspension means).
    let policies: Api<SnapshotPolicy> = Api::namespaced(ctx.client.clone(), ns);
    let policy = policies.get(&args.policy).await.map_err(|e| {
        classify_kube(
            "get",
            "SnapshotPolicy",
            "snapshotpolicies",
            Some(ns),
            Some(&args.policy),
            e,
        )
    })?;
    if policy.spec.suspend {
        eprintln!(
            "warning: SnapshotPolicy {} is suspended; the operator may not run this snapshot until it is resumed \
             (kubectl kopiur resume policy {})",
            args.policy, args.policy
        );
    }

    let snapshots: Api<Snapshot> = Api::namespaced(ctx.client.clone(), ns);
    let snapshot = build_snapshot(args, ns, now);
    let name = snapshot.metadata.name.clone().expect("name set by builder");
    let created = snapshots
        .create(&PostParams::default(), &snapshot)
        .await
        .map_err(|e| classify_kube("create", "Snapshot", "snapshots", Some(ns), Some(&name), e))?;

    let wait = args.wait || args.logs;
    let created_line = format!("snapshot.{}/{} created\n", kopiur_api::GROUP, name);
    if !wait {
        let text = match output {
            OutputFormat::Table | OutputFormat::Wide => created_line,
            OutputFormat::Yaml => {
                // Via a JSON Value: keeps the cluster's encoding for any
                // externally-tagged enum (see cmd/restore.rs; SnapshotSpec has
                // none today, but the route must not depend on that).
                let value =
                    serde_json::to_value(&created).map_err(|e| CliError::Serialization {
                        what: "created Snapshot",
                        source: e.into(),
                    })?;
                serde_yaml::to_string(&value).map_err(|e| CliError::Serialization {
                    what: "created Snapshot",
                    source: e.into(),
                })?
            }
            OutputFormat::Json => {
                let mut s = serde_json::to_string_pretty(&created).map_err(|e| {
                    CliError::Serialization {
                        what: "created Snapshot",
                        source: e.into(),
                    }
                })?;
                s.push('\n');
                s
            }
            OutputFormat::Name => format!("snapshot.{}/{}\n", kopiur_api::GROUP, name),
        };
        return Ok(CmdOutput { text, exit: 0 });
    }

    // Waiting: progress goes to stderr so stdout stays the result (or the
    // mover logs when --logs).
    eprint!("{created_line}");
    let log_task = if args.logs {
        let ctx_clone = ctx.clone();
        let snap_name = name.clone();
        Some(tokio::spawn(async move {
            crate::cmd::logs::stream_target_logs_when_ready(
                &ctx_clone,
                crate::cmd::logs::LogsTarget::Snapshot,
                &snap_name,
            )
            .await
        }))
    } else {
        None
    };

    let timeout = args.timeout.unwrap_or(DEFAULT_WAIT_TIMEOUT);
    let verdict = wait_for(
        &snapshots,
        &name,
        format!("snapshot {name}"),
        format!(
            "follow it with `kubectl kopiur logs snapshot {name} -n {ns} -f`, or raise --timeout"
        ),
        timeout,
        terminal,
    )
    .await;

    // The streamer self-exits at the terminal phase once the final pod is
    // drained; give it a bounded moment, then abort — dropping the timeout
    // future alone would detach (not stop) the task.
    if let Some(mut task) = log_task
        && tokio::time::timeout(std::time::Duration::from_secs(5), &mut task)
            .await
            .is_err()
    {
        task.abort();
    }

    match verdict? {
        Ok(succeeded) => Ok(CmdOutput {
            text: success_summary(&succeeded),
            exit: 0,
        }),
        Err(failed) => {
            eprint!("{}", failure_detail(&failed));
            Ok(CmdOutput {
                text: String::new(),
                exit: 1,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::DeletionPolicyArg;
    use chrono::TimeZone;

    fn args() -> SnapshotNowArgs {
        SnapshotNowArgs {
            policy: "nightly".into(),
            name: None,
            tags: vec![],
            deletion_policy: None,
            pin: false,
            backoff_limit: None,
            active_deadline_seconds: None,
            wait: false,
            logs: false,
            timeout: None,
        }
    }

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 11, 3, 0, 12).unwrap()
    }

    #[test]
    fn builds_a_minimal_manual_snapshot_with_canonical_labels() {
        let snap = build_snapshot(&args(), "media", at());
        // Round-trip the cluster's way: typed → JSON → typed.
        let wire = serde_json::to_value(&snap).unwrap();
        assert_eq!(wire["metadata"]["name"], "nightly-manual-20260611030012");
        assert_eq!(wire["metadata"]["namespace"], "media");
        assert_eq!(
            wire["metadata"]["labels"]["kopiur.home-operations.com/origin"],
            "manual"
        );
        assert_eq!(
            wire["metadata"]["labels"]["kopiur.home-operations.com/config"],
            "nightly"
        );
        assert_eq!(wire["spec"]["policyRef"]["name"], "nightly");
        // Unset options must be ABSENT on the wire, not null/false noise.
        for key in ["tags", "failurePolicy", "deletionPolicy", "pin"] {
            assert!(wire["spec"].get(key).is_none(), "{key} should be absent");
        }
        let reparsed: Snapshot = serde_json::from_value(wire).unwrap();
        assert_eq!(reparsed.spec, snap.spec);
    }

    #[test]
    fn every_flag_lands_in_the_spec() {
        // The restore-options-dropped bug class: each flag must round-trip.
        let mut a = args();
        a.name = Some("pre-upgrade".into());
        a.tags = vec![("reason".into(), "pre-upgrade".into())];
        a.deletion_policy = Some(DeletionPolicyArg::Retain);
        a.pin = true;
        a.backoff_limit = Some(0);
        a.active_deadline_seconds = Some(120);
        let wire = serde_json::to_value(build_snapshot(&a, "media", at())).unwrap();
        assert_eq!(wire["metadata"]["name"], "pre-upgrade");
        assert_eq!(wire["spec"]["tags"]["reason"], "pre-upgrade");
        assert_eq!(wire["spec"]["deletionPolicy"], "Retain");
        assert_eq!(wire["spec"]["pin"], true);
        assert_eq!(wire["spec"]["failurePolicy"]["backoffLimit"], 0);
        assert_eq!(wire["spec"]["failurePolicy"]["activeDeadlineSeconds"], 120);
    }

    fn with_phase(phase: &str) -> Snapshot {
        let v = serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Snapshot",
            "metadata": { "name": "s", "namespace": "media" },
            "spec": {},
            "status": { "phase": phase }
        });
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn terminal_classification_is_exhaustive_and_correct() {
        assert!(terminal(&with_phase("Pending")).is_none());
        assert!(terminal(&with_phase("Running")).is_none());
        assert!(matches!(terminal(&with_phase("Succeeded")), Some(Ok(_))));
        assert!(matches!(terminal(&with_phase("Failed")), Some(Err(_))));
        assert!(matches!(terminal(&with_phase("Deleting")), Some(Err(_))));
    }

    #[test]
    fn success_summary_reports_id_size_duration() {
        let v = serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Snapshot",
            "metadata": { "name": "s" },
            "spec": {},
            "status": {
                "phase": "Succeeded",
                "snapshot": { "kopiaSnapshotID": "abc123", "identity": { "username": "u", "hostname": "h" } },
                "stats": { "sizeBytes": 1536 },
                "timing": { "durationSeconds": 42 }
            }
        });
        let snap: Snapshot = serde_json::from_value(v).unwrap();
        assert_eq!(
            success_summary(&snap),
            "snapshot s succeeded: kopia id abc123, 1.5 KiB, took 42s\n"
        );
    }

    #[test]
    fn failure_detail_includes_class_message_and_tails() {
        let v = serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Snapshot",
            "metadata": { "name": "s" },
            "spec": {},
            "status": {
                "phase": "Failed",
                "failure": {
                    "kopiaErrorClass": "AuthFailure",
                    "message": "credentials rejected; fix the Secret",
                    "stderrTail": "access denied",
                    "retryRecommended": false
                },
                "logTail": "last lines"
            }
        });
        let snap: Snapshot = serde_json::from_value(v).unwrap();
        let detail = failure_detail(&snap);
        assert!(detail.contains("snapshot s failed (AuthFailure): credentials rejected"));
        assert!(detail.contains("--- kopia stderr tail ---\naccess denied"));
        assert!(detail.contains("--- log tail ---\nlast lines"));
    }
}
