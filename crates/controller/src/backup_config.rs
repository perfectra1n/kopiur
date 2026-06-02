//! The `BackupConfig` reconciler — the *recipe* (ADR §4.4).
//!
//! Responsibilities:
//! 1. Defensive re-validation via `api::validate`.
//! 2. Resolve identity via `api::identity` and pin it to `status.resolved`.
//! 3. Enforce GFS retention by calling `api::retention::select_kept` over the
//!    matching `Backup` CRs and deleting those outside the kept set (deletion
//!    goes through the `Backup` finalizer path, never a raw snapshot delete).
//!
//! The retention selection is reused verbatim from `api::retention` — this
//! module only adapts `Backup` CRs to its `BackupLike` trait and decides which
//! CRs to delete, both of which are pure and unit-tested here.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use kube::runtime::controller::Action;
use kube::ResourceExt;

use kopiur_api::common::Retention;
use kopiur_api::retention::{select_kept, BackupLike};
use kopiur_api::{validate, Backup, BackupConfig};

use crate::context::Context;
use crate::error::{error_policy_for, Error, Result};

/// A minimal view of a `Backup` for retention selection: its CR name (the id
/// used in delete decisions) and its snapshot end time (the GFS bucketing key).
pub struct BackupRetentionView {
    /// CR name — the stable id returned in the kept/delete sets.
    pub name: String,
    /// Snapshot completion time (from `status.snapshot`/`status.timing`).
    pub end_time: DateTime<Utc>,
}

impl BackupLike for BackupRetentionView {
    fn end_time(&self) -> DateTime<Utc> {
        self.end_time
    }
    fn id(&self) -> &str {
        &self.name
    }
}

/// Build a retention view from a `Backup` CR, using `status.timing.endTime`
/// (falling back to the CR creation timestamp). Returns `None` if the backup is
/// not in a terminal successful state — only successful snapshots participate in
/// GFS (failures are bounded separately by `failedJobsHistoryLimit`).
pub fn retention_view(b: &Backup) -> Option<BackupRetentionView> {
    use kopiur_api::BackupPhase;
    let status = b.status.as_ref()?;
    if status.phase != Some(BackupPhase::Succeeded) {
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
    Some(BackupRetentionView {
        name: b.name_any(),
        end_time,
    })
}

/// Decide which `Backup` CR names to delete under a GFS `policy`. Wraps
/// `api::retention::select_kept`; returns the `delete` set. Backups that are not
/// terminal-successful are ignored entirely (never selected for deletion here).
pub fn backups_to_delete(backups: &[Backup], policy: &Retention) -> Vec<String> {
    let views: Vec<BackupRetentionView> = backups.iter().filter_map(retention_view).collect();
    select_kept(&views, policy).delete
}

/// Reconcile a `BackupConfig`.
#[tracing::instrument(skip(config, ctx), fields(kind = "BackupConfig", name = %config.name_any()))]
pub async fn reconcile(config: Arc<BackupConfig>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&config, &ctx).await;
    ctx.metrics
        .record_reconcile("BackupConfig", start.elapsed().as_secs_f64());
    result
}

async fn reconcile_inner(config: &BackupConfig, _ctx: &Context) -> Result<Action> {
    let errs = validate::validate_backup_config(&config.spec);
    if let Some(first) = errs.into_iter().next() {
        return Err(Error::Validation(first.to_string()));
    }

    // TODO(M6): resolve identity via api::identity::resolve_identity (the
    // ClusterRepository template + per-source pvc), pin status.resolved; then
    // list owned Backup CRs, call backups_to_delete(&backups, retention), and
    // `delete` each (the Backup finalizer governs the snapshot). The pure
    // selection (backups_to_delete) is tested below.

    Ok(Action::requeue(std::time::Duration::from_secs(300)))
}

/// `error_policy` for the `BackupConfig` controller.
pub fn error_policy(_obj: Arc<BackupConfig>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("BackupConfig", err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use kopiur_api::backup::{BackupSpec, BackupStatus, BackupTiming, SnapshotInfo};
    use kopiur_api::common::ResolvedIdentity;
    use kopiur_api::{BackupPhase, Origin};
    use std::collections::BTreeSet;

    fn at(y: i32, mo: u32, d: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, 2, 0, 0).single().unwrap()
    }

    fn succeeded_backup(name: &str, end: DateTime<Utc>) -> Backup {
        let mut b = Backup::new(
            name,
            BackupSpec {
                config_ref: None,
                tags: None,
                failure_policy: None,
                deletion_policy: None,
            },
        );
        b.status = Some(BackupStatus {
            phase: Some(BackupPhase::Succeeded),
            origin: Some(Origin::Scheduled),
            timing: Some(BackupTiming {
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

    fn policy(latest: Option<u32>, daily: Option<u32>) -> Retention {
        Retention {
            keep_latest: latest,
            keep_daily: daily,
            ..Default::default()
        }
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
        let mut running = Backup::new(
            "running",
            BackupSpec {
                config_ref: None,
                tags: None,
                failure_policy: None,
                deletion_policy: None,
            },
        );
        running.status = Some(BackupStatus {
            phase: Some(BackupPhase::Running),
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
}
