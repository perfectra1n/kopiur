//! The `SnapshotPolicy` reconciler — the *recipe* (ADR §4.4).
//!
//! Responsibilities:
//! 1. Defensive re-validation via `api::validate`.
//! 2. Resolve identity via `api::identity` and pin it to `status.resolved`.
//! 3. Enforce GFS retention by calling `api::retention::select_kept` over the
//!    matching `Snapshot` CRs and deleting those outside the kept set (deletion
//!    goes through the `Snapshot` finalizer path, never a raw snapshot delete).
//!
//! The retention selection is reused verbatim from `api::retention` — this
//! module only adapts `Snapshot` CRs to its `SnapshotLike` trait and decides which
//! CRs to delete, both of which are pure and unit-tested here.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use kube::api::{DeleteParams, ListParams};
use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};

use kopiur_api::common::Retention;
use kopiur_api::retention::{SnapshotLike, select_kept};
use kopiur_api::{Snapshot, SnapshotPolicy, validate};

use crate::consts::CONFIG_LABEL;
use crate::context::Context;
use crate::error::{Error, Result, error_policy_for};
use crate::io;

/// A minimal view of a `Snapshot` for retention selection: its CR name (the id
/// used in delete decisions) and its snapshot end time (the GFS bucketing key).
pub struct SnapshotRetentionView {
    /// CR name — the stable id returned in the kept/delete sets.
    pub name: String,
    /// Snapshot completion time (from `status.snapshot`/`status.timing`).
    pub end_time: DateTime<Utc>,
    /// Whether the `Snapshot` is pinned (`spec.pin`, ADR-0005 §13(c)) — exempt from
    /// GFS retention (never selected for deletion).
    pub pinned: bool,
}

impl SnapshotLike for SnapshotRetentionView {
    fn end_time(&self) -> DateTime<Utc> {
        self.end_time
    }
    fn id(&self) -> &str {
        &self.name
    }
    fn pinned(&self) -> bool {
        self.pinned
    }
}

/// Build a retention view from a `Snapshot` CR, using `status.timing.endTime`
/// (falling back to the CR creation timestamp). Returns `None` if the backup is
/// not in a terminal successful state — only successful snapshots participate in
/// GFS (failures are bounded separately by `failedJobsHistoryLimit`).
pub fn retention_view(b: &Snapshot) -> Option<SnapshotRetentionView> {
    use kopiur_api::SnapshotPhase;
    let status = b.status.as_ref()?;
    if status.phase != Some(SnapshotPhase::Succeeded) {
        return None;
    }
    let end_time = status
        .timing
        .as_ref()
        .and_then(|t| t.end_time.as_deref())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|| {
            // metadata.creationTimestamp is a k8s-openapi `Time` wrapping a
            // jiff `Timestamp`; convert via unix seconds to chrono.
            b.creation_timestamp()
                .and_then(|t| DateTime::<Utc>::from_timestamp(t.0.as_second(), 0))
        })?;
    Some(SnapshotRetentionView {
        name: b.name_any(),
        end_time,
        // A pinned Snapshot is exempt from GFS pruning (ADR-0005 §13(c)).
        pinned: b.spec.pin,
    })
}

/// Decide which `Snapshot` CR names to delete under a GFS `policy`. Wraps
/// `api::retention::select_kept`; returns the `delete` set. Snapshots that are not
/// terminal-successful are ignored entirely (never selected for deletion here).
pub fn backups_to_delete(backups: &[Snapshot], policy: &Retention) -> Vec<String> {
    let views: Vec<SnapshotRetentionView> = backups.iter().filter_map(retention_view).collect();
    select_kept(&views, policy).delete
}

/// Count the most-recent run of consecutive `Failed` backups before the latest
/// `Succeeded` one (the `kopiur_snapshot_consecutive_failures` gauge). Only
/// terminal backups (Succeeded/Failed) count; ordering is by `endTime` (falling
/// back to the CR creation time). Pure. ADR §4.13.
pub fn consecutive_failures(backups: &[Snapshot]) -> i64 {
    use kopiur_api::SnapshotPhase;
    let terminal_time = |b: &Snapshot| -> Option<(DateTime<Utc>, SnapshotPhase)> {
        let status = b.status.as_ref()?;
        let phase = status.phase?;
        if !matches!(phase, SnapshotPhase::Succeeded | SnapshotPhase::Failed) {
            return None;
        }
        let t = status
            .timing
            .as_ref()
            .and_then(|t| t.end_time.as_deref())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .or_else(|| {
                b.creation_timestamp()
                    .and_then(|t| DateTime::<Utc>::from_timestamp(t.0.as_second(), 0))
            })?;
        Some((t, phase))
    };
    let mut terminal: Vec<(DateTime<Utc>, SnapshotPhase)> =
        backups.iter().filter_map(terminal_time).collect();
    // Newest first.
    terminal.sort_by_key(|(t, _)| std::cmp::Reverse(*t));
    let mut n = 0;
    for (_, phase) in terminal {
        match phase {
            SnapshotPhase::Failed => n += 1,
            SnapshotPhase::Succeeded => break,
            // Non-terminal already filtered out.
            _ => {}
        }
    }
    n
}

/// Reconcile a `SnapshotPolicy`.
#[tracing::instrument(skip(config, ctx), fields(kind = "SnapshotPolicy", namespace = %config.namespace().unwrap_or_default(), name = %config.name_any()))]
pub async fn reconcile(config: Arc<SnapshotPolicy>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&config, &ctx).await;
    ctx.metrics
        .record_reconcile("SnapshotPolicy", start.elapsed().as_secs_f64());
    result
}

async fn reconcile_inner(config: &SnapshotPolicy, ctx: &Context) -> Result<Action> {
    let errs = validate::validate_backup_config(&config.spec);
    if let Some(first) = errs.into_iter().next() {
        return Err(Error::Validation(first.to_string()));
    }

    let namespace = config
        .namespace()
        .ok_or_else(|| Error::Invariant("SnapshotPolicy has no namespace".into()))?;
    let name = config.name_any();
    let generation = config.metadata.generation;
    let api: Api<SnapshotPolicy> = Api::namespaced(ctx.client.clone(), &namespace);
    let existing = config
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();

    // §14(e): a suspended SnapshotPolicy is skipped entirely (no identity re-pin, no
    // retention prune). Surface `Ready=False`/`Reconciling=False` so GitOps sees a
    // deliberate pause rather than a hang, then back off long.
    if config.spec.suspend {
        let conditions = io::set_ready(
            &existing,
            generation,
            io::ReadyOutcome::Reconciling,
            "Suspended",
            "SnapshotPolicy is suspended (spec.suspend); skipping retention and backups",
        );
        io::patch_status(
            &api,
            &name,
            serde_json::json!({ "observedGeneration": generation, "conditions": conditions }),
        )
        .await?;
        return Ok(Action::requeue(std::time::Duration::from_secs(300)));
    }

    // 1. Resolve identity (per-source PVC + overrides + the repository's CEL
    //    identityDefaults) and pin status.resolved. Resolving the repository first
    //    means a ClusterRepository's `identityDefaults` (ADR-0004 §5) are applied and
    //    the pinned identity is correct from the start (never re-rendered, ADR §4.2).
    let repo = io::resolve_repository_ref(&ctx.client, &config.spec.repository, &namespace).await?;
    let resolved = resolve_config_identity(config, &namespace, repo.identity_defaults.as_ref())?;
    io::patch_status(&api, &name, serde_json::json!({ "resolved": resolved })).await?;

    // §2 dependent gating: a SnapshotPolicy should not be Ready (and schedules
    // shouldn't fire it productively) until its Repository is Ready. Read readiness
    // from the existing helper and REQUEUE (not error) until then, surfacing a
    // `Reconciling` condition. This makes `kubectl wait` and Flux/Argo health behave.
    if !io::repository_ready(&ctx.client, &config.spec.repository, &namespace).await? {
        let conditions = io::set_ready(
            &existing,
            generation,
            io::ReadyOutcome::Reconciling,
            "RepositoryNotReady",
            "waiting for the referenced Repository to become Ready before reconciling",
        );
        io::patch_status(
            &api,
            &name,
            serde_json::json!({ "observedGeneration": generation, "conditions": conditions }),
        )
        .await?;
        return Ok(Action::requeue(std::time::Duration::from_secs(15)));
    }

    // §3: surface the most recent successful child Snapshot timestamp (backs the
    // LAST-SNAPSHOT column + the staleness alert). Deterministic (the max endTime
    // over this policy's Succeeded Snapshots), so an unchanged value is a no-op patch.
    let last_successful = latest_successful_snapshot(&ctx.client, &namespace, &name).await?;

    // 2. Enforce GFS retention: list this config's Snapshots, decide which to
    //    delete, and delete each (the Snapshot finalizer governs the snapshot).
    if let Some(retention) = config.spec.retention.as_ref() {
        let backup_api: Api<Snapshot> = Api::namespaced(ctx.client.clone(), &namespace);
        let lp = ListParams::default().labels(&format!("{CONFIG_LABEL}={name}"));
        let backups = backup_api.list(&lp).await?.items;
        // Surface the consecutive-failure streak for alerting (ADR §4.13).
        ctx.metrics.set_backup_consecutive_failures(
            &namespace,
            &name,
            consecutive_failures(&backups),
        );
        let to_delete = backups_to_delete(&backups, retention);
        let dp = DeleteParams::default();
        for cr_name in &to_delete {
            match backup_api.delete(cr_name, &dp).await {
                Ok(_) => {
                    tracing::info!(config = %name, backup = %cr_name, "pruned backup (GFS retention)")
                }
                Err(kube::Error::Api(ae)) if ae.code == 404 => {}
                Err(e) => return Err(Error::Kube(e)),
            }
        }
        let active = backups.len().saturating_sub(to_delete.len());
        // Only stamp `lastPruneAt`/`lastPruneDeleted` when a prune actually
        // happened. Writing `now()` on every reconcile made the status differ each
        // pass → resourceVersion bump → watch event → self-triggered reconcile (the
        // same hot-loop class as the repo bug). The bare count write is
        // deterministic, so an unchanged value is a no-op patch at the apiserver.
        //
        // Built from the CRD's own `RetentionSummary` so the field names cannot
        // drift from the structural schema: this used to write the pre-rename
        // `activeBackupCount`, which the apiserver SILENTLY PRUNED (the schema
        // field is `activeSnapshotCount`) — caught by the retention e2e.
        let pruned = !to_delete.is_empty();
        let summary = kopiur_api::snapshot_policy::RetentionSummary {
            active_snapshot_count: Some(active as i64),
            last_prune_at: pruned.then(|| Utc::now().to_rfc3339()),
            last_prune_deleted: pruned.then_some(to_delete.len() as i64),
        };
        io::patch_status(&api, &name, serde_json::json!({ "retention": summary })).await?;
    }

    // Final status: Ready (the policy is reconciled and its repo is Ready), the
    // observedGeneration, and the §3 lastSuccessfulSnapshot. Only set the timestamp
    // when known so we never thrash it with null.
    let conditions = io::set_ready(
        &existing,
        generation,
        io::ReadyOutcome::Ready,
        "Reconciled",
        "SnapshotPolicy reconciled; retention enforced",
    );
    let mut status = serde_json::json!({
        "observedGeneration": generation,
        "conditions": conditions,
    });
    if let Some(ts) = last_successful {
        status["lastSuccessfulSnapshot"] = serde_json::json!(ts);
    }
    io::patch_status(&api, &name, status).await?;

    // §4: first-class verification scheduling. When `spec.verification` is set, the
    // policy reconciler doubles as the verify scheduler (mirroring the Maintenance
    // kernel): it spawns per-slot quick/deep verify Jobs and tracks them. When absent,
    // `verify_step` is a no-op (None) and the steady 300s requeue applies. Otherwise
    // requeue on the shorter of the steady cadence and the verify cadence so a due
    // verification fires on time.
    let steady = std::time::Duration::from_secs(300);
    match crate::verification::verify_step(config, ctx, &repo, &namespace).await? {
        Some(verify_requeue) => Ok(Action::requeue(steady.min(verify_requeue))),
        None => Ok(Action::requeue(steady)),
    }
}

/// The RFC3339 `endTime` of the most recent `Succeeded` `Snapshot` produced by
/// this policy (backs `status.lastSuccessfulSnapshot`, §3), or `None` if there is
/// none yet. Reads the policy's Snapshots via the `CONFIG_LABEL` selector.
async fn latest_successful_snapshot(
    client: &kube::Client,
    namespace: &str,
    config_name: &str,
) -> Result<Option<String>> {
    let api: Api<Snapshot> = Api::namespaced(client.clone(), namespace);
    let lp = ListParams::default().labels(&format!("{CONFIG_LABEL}={config_name}"));
    let backups = api.list(&lp).await?.items;
    Ok(latest_successful_end_time(&backups))
}

/// Pure: the max `status.timing.endTime` over `Succeeded` Snapshots, as RFC3339.
/// Pulled out so the selection is unit-tested without a cluster.
pub fn latest_successful_end_time(backups: &[Snapshot]) -> Option<String> {
    backups
        .iter()
        .filter_map(retention_view)
        .map(|v| v.end_time)
        .max()
        .map(|t| t.to_rfc3339())
}

/// Resolve a `SnapshotPolicy`'s identity into the api `ResolvedIdentity` (reused by
/// the restore reconciler for `fromPolicy` source resolution).
pub fn config_identity(
    config: &SnapshotPolicy,
    namespace: &str,
    defaults: Option<&kopiur_api::IdentityDefaults>,
) -> Result<kopiur_api::common::ResolvedIdentity> {
    let first = config.spec.sources.first();
    let pvc_name = first.and_then(|s| s.pvc.as_ref().map(|p| p.name.clone()));
    let nfs_source_path = first.and_then(|s| s.nfs.as_ref().map(|n| n.path.clone()));
    let source_path_override = first.and_then(|s| s.source_path_override.clone());
    let inputs = kopiur_api::IdentityInputs {
        object_name: &config.name_any(),
        namespace,
        overrides: config.spec.identity.as_ref(),
        defaults,
        labels: config.metadata.labels.as_ref(),
        annotations: config.metadata.annotations.as_ref(),
        pvc_name: pvc_name.as_deref(),
        default_source_path: nfs_source_path.as_deref(),
        source_path_override: source_path_override.as_deref(),
    };
    kopiur_api::resolve_identity(&inputs).map_err(|e| Error::Validation(e.to_string()))
}

/// Resolve the config's identity + per-source paths into a `ResolvedPolicy`
/// status body. Reuses `api::identity::resolve_identity` (tested kernel).
fn resolve_config_identity(
    config: &SnapshotPolicy,
    namespace: &str,
    defaults: Option<&kopiur_api::IdentityDefaults>,
) -> Result<kopiur_api::snapshot_policy::ResolvedPolicy> {
    use kopiur_api::snapshot_policy::{ResolvedPolicy, ResolvedPolicySource};
    let first = config.spec.sources.first();
    let pvc_name = first.and_then(|s| s.pvc.as_ref().map(|p| p.name.clone()));
    let nfs_source_path = first.and_then(|s| s.nfs.as_ref().map(|n| n.path.clone()));
    let source_path_override = first.and_then(|s| s.source_path_override.clone());
    let inputs = kopiur_api::IdentityInputs {
        object_name: &config.name_any(),
        namespace,
        overrides: config.spec.identity.as_ref(),
        defaults,
        labels: config.metadata.labels.as_ref(),
        annotations: config.metadata.annotations.as_ref(),
        pvc_name: pvc_name.as_deref(),
        default_source_path: nfs_source_path.as_deref(),
        source_path_override: source_path_override.as_deref(),
    };
    let identity =
        kopiur_api::resolve_identity(&inputs).map_err(|e| Error::Validation(e.to_string()))?;
    let sources = config
        .spec
        .sources
        .iter()
        .map(|s| ResolvedPolicySource {
            pvc: s.pvc.as_ref().map(|p| format!("{namespace}/{}", p.name)),
            source_path: s
                .source_path_override
                .clone()
                .or_else(|| s.pvc.as_ref().map(|p| format!("/pvc/{}", p.name)))
                .or_else(|| s.nfs.as_ref().map(|n| n.path.clone())),
        })
        .collect();
    Ok(ResolvedPolicy {
        identity: Some(identity),
        sources,
    })
}

/// `error_policy` for the `SnapshotPolicy` controller.
pub fn error_policy(obj: Arc<SnapshotPolicy>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("SnapshotPolicy", obj.as_ref(), err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use kopiur_api::common::ResolvedIdentity;
    use kopiur_api::snapshot::{SnapshotInfo, SnapshotSpec, SnapshotStatus, SnapshotTiming};
    use kopiur_api::{Origin, SnapshotPhase};
    use std::collections::BTreeSet;

    fn at(y: i32, mo: u32, d: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, 2, 0, 0).single().unwrap()
    }

    fn succeeded_backup(name: &str, end: DateTime<Utc>) -> Snapshot {
        let mut b = Snapshot::new(
            name,
            SnapshotSpec {
                policy_ref: None,
                tags: None,
                failure_policy: None,
                deletion_policy: None,
                pin: false,
            },
        );
        b.status = Some(SnapshotStatus {
            phase: Some(SnapshotPhase::Succeeded),
            origin: Some(Origin::Scheduled),
            timing: Some(SnapshotTiming {
                start_time: None,
                end_time: Some(end.to_rfc3339()),
                duration_seconds: None,
            }),
            snapshot: Some(SnapshotInfo {
                kopia_snapshot_id: format!("snap-{name}"),
                identity: ResolvedIdentity {
                    username: "u".into(),
                    hostname: "h".into(),
                    source_path: Some("/d".into()),
                },
            }),
            ..Default::default()
        });
        b
    }

    fn failed_backup(name: &str, end: DateTime<Utc>) -> Snapshot {
        let mut b = succeeded_backup(name, end);
        if let Some(s) = b.status.as_mut() {
            s.phase = Some(SnapshotPhase::Failed);
            s.snapshot = None;
        }
        b
    }

    fn policy(latest: Option<u32>, daily: Option<u32>) -> Retention {
        Retention {
            keep_latest: latest,
            keep_daily: daily,
            ..Default::default()
        }
    }

    #[test]
    fn consecutive_failures_counts_trailing_failures_before_last_success() {
        // Newest→oldest: Fail(26), Fail(25), Succeed(24), Fail(23) → 2.
        let backups = vec![
            failed_backup("f23", at(2026, 5, 23)),
            succeeded_backup("s24", at(2026, 5, 24)),
            failed_backup("f25", at(2026, 5, 25)),
            failed_backup("f26", at(2026, 5, 26)),
        ];
        assert_eq!(consecutive_failures(&backups), 2);
        // All succeeded → 0.
        assert_eq!(
            consecutive_failures(&[succeeded_backup("s", at(2026, 5, 24))]),
            0
        );
        // All failed → counts them all.
        assert_eq!(
            consecutive_failures(&[
                failed_backup("f1", at(2026, 5, 24)),
                failed_backup("f2", at(2026, 5, 25)),
            ]),
            2
        );
        // No terminal backups (e.g. only Running/Pending) → 0.
        assert_eq!(consecutive_failures(&[]), 0);
    }

    #[test]
    fn keeps_newest_deletes_rest_via_retention_kernel() {
        let backups = vec![
            succeeded_backup("d24", at(2026, 5, 24)),
            succeeded_backup("d23", at(2026, 5, 23)),
            succeeded_backup("d22", at(2026, 5, 22)),
        ];
        let del: BTreeSet<String> = backups_to_delete(&backups, &policy(Some(1), None))
            .into_iter()
            .collect();
        assert_eq!(
            del,
            ["d23".to_string(), "d22".to_string()].into_iter().collect()
        );
    }

    #[test]
    fn daily_policy_keeps_one_per_day() {
        let backups = vec![
            succeeded_backup("d24", at(2026, 5, 24)),
            succeeded_backup("d23", at(2026, 5, 23)),
            succeeded_backup("d22", at(2026, 5, 22)),
        ];
        // keepDaily:2 → newest two days kept, oldest deleted.
        let del = backups_to_delete(&backups, &policy(None, Some(2)));
        assert_eq!(del, vec!["d22".to_string()]);
    }

    #[test]
    fn non_terminal_backups_are_ignored() {
        // A Running backup has no end time and is not Succeeded → not a
        // retention candidate, so it is never returned for deletion.
        let mut running = Snapshot::new(
            "running",
            SnapshotSpec {
                policy_ref: None,
                tags: None,
                failure_policy: None,
                deletion_policy: None,
                pin: false,
            },
        );
        running.status = Some(SnapshotStatus {
            phase: Some(SnapshotPhase::Running),
            ..Default::default()
        });
        let succeeded = succeeded_backup("done", at(2026, 5, 24));
        let del = backups_to_delete(&[running, succeeded], &policy(Some(1), None));
        assert!(del.is_empty(), "single succeeded kept, running ignored");
    }

    #[test]
    fn empty_policy_marks_all_succeeded_for_deletion() {
        let backups = vec![
            succeeded_backup("a", at(2026, 5, 24)),
            succeeded_backup("b", at(2026, 5, 23)),
        ];
        let del: BTreeSet<String> = backups_to_delete(&backups, &Retention::default())
            .into_iter()
            .collect();
        assert_eq!(
            del,
            ["a".to_string(), "b".to_string()].into_iter().collect()
        );
    }

    #[test]
    fn pinned_snapshot_is_never_pruned_by_gfs() {
        // §13(c): a pinned Snapshot is exempt — keepLatest:1 would delete the older
        // ones, but the pinned one survives.
        let mut pinned = succeeded_backup("pinned", at(2026, 5, 20));
        pinned.spec.pin = true;
        let backups = vec![
            succeeded_backup("newest", at(2026, 5, 24)),
            pinned,
            succeeded_backup("old", at(2026, 5, 19)),
        ];
        let del: BTreeSet<String> = backups_to_delete(&backups, &policy(Some(1), None))
            .into_iter()
            .collect();
        assert!(del.contains("old"), "unpinned old snapshot is pruned");
        assert!(!del.contains("pinned"), "pinned snapshot is never pruned");
        assert!(!del.contains("newest"));
    }

    #[test]
    fn latest_successful_end_time_is_the_max_succeeded() {
        // §3: the lastSuccessfulSnapshot is the newest Succeeded endTime; failures
        // and an empty set don't count.
        let backups = vec![
            succeeded_backup("a", at(2026, 5, 22)),
            succeeded_backup("b", at(2026, 5, 24)),
            failed_backup("f", at(2026, 5, 25)),
        ];
        assert_eq!(
            latest_successful_end_time(&backups),
            Some(at(2026, 5, 24).to_rfc3339())
        );
        assert_eq!(latest_successful_end_time(&[]), None);
        assert_eq!(
            latest_successful_end_time(&[failed_backup("f", at(2026, 5, 25))]),
            None
        );
    }
}
