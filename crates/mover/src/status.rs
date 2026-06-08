//! Status types the mover PATCHes onto the `Backup`/`Restore` `.status`
//! subresource, plus the **pure** mapping from kopia results/errors to those
//! types.
//!
//! The pure mapping (`KopiaError → FailureBlock`, `SnapshotCreateResult →
//! `kopiur_api::BackupStats`/`BackupTiming`) is unit-testable with no cluster.
//! The stats/timing types are the CRD's own (not mover-local) so their field
//! names cannot drift from the structural schema — a mismatch is silently pruned
//! by the API server. The actual kube PATCH lives in a thin function gated so
//! tests don't need a client.

use chrono::{DateTime, Utc};
use kopiur_api::backup::SnapshotInfo;
use kopiur_api::common::ResolvedIdentity;
use kopiur_api::restore::RestorePhase;
use kopiur_api::{BackupStats, BackupTiming, PhaseLabel};
use kopiur_kopia::{KopiaError, SnapshotCreateResult};
use serde::{Deserialize, Serialize};

/// Map a kopia create result to the CRD `status.stats` shape (`BackupStats`).
///
/// We reuse the API type rather than a mover-local struct so the field names
/// stay in lockstep with the `Backup` CRD's structural schema. They MUST match:
/// the API server **prunes unknown status fields**, so a drifting name (the old
/// mover-local `totalBytes`/`fileCount`) is silently dropped and `status.stats`
/// lands as `{}` — which is exactly the bug that left `kopiur_backup_size_bytes`
/// empty. kopia's snapshot-create summary reports the snapshot's total size and
/// file count, mapped to `sizeBytes`/`filesNew`.
fn stats_from_result(r: &SnapshotCreateResult) -> BackupStats {
    BackupStats {
        size_bytes: Some(r.total_bytes() as i64),
        files_new: Some(r.file_count() as i64),
        ..Default::default()
    }
}

/// Map a kopia create result to the CRD `status.snapshot` (`SnapshotInfo`).
///
/// MUST be the nested `{ kopiaSnapshotID, identity }` shape, not a flat
/// `snapshotId`: the API server prunes unknown status fields, so a flat field is
/// silently dropped and `status.snapshot` never lands — which is exactly why
/// object-store backups recorded `Succeeded` with no snapshot id. The identity
/// comes from kopia's recorded source (`user@host:path`), which the controller
/// pinned via `--override-source`.
fn snapshot_from_result(r: &SnapshotCreateResult) -> SnapshotInfo {
    SnapshotInfo {
        kopia_snapshot_id: r.id.clone(),
        identity: ResolvedIdentity {
            username: r.source.user_name.clone(),
            hostname: r.source.host.clone(),
            source_path: Some(r.source.path.clone()),
        },
    }
}

/// Map a kopia create result's start/end timestamps to the CRD `status.timing`.
fn timing_from_result(r: &SnapshotCreateResult) -> BackupTiming {
    BackupTiming {
        start_time: Some(r.start_time.to_rfc3339()),
        end_time: Some(r.end_time.to_rfc3339()),
        duration_seconds: Some((r.end_time - r.start_time).num_seconds()),
    }
}

/// A structured terminal-failure block (ADR §4.10): kopia error class, the last
/// stderr lines, and a retry recommendation. Written to `status.failure` before
/// the mover exits non-zero.
///
/// Built directly from a [`kopiur_kopia::KopiaError`]; the class, stderr tail,
/// exit code, and retry hint all carry through:
///
/// ```
/// use kopiur_kopia::{KopiaError, KopiaErrorClass};
/// use kopiur_mover::status::FailureBlock;
///
/// let err = KopiaError::NonZeroExit {
///     args: "repository connect".into(),
///     code: Some(1),
///     class: KopiaErrorClass::AuthFailure,
///     stderr_tail: "invalid repository password".into(),
/// };
/// let fb = FailureBlock::from(&err);
/// assert_eq!(fb.kopia_error_class, "AuthFailure");
/// assert_eq!(fb.exit_code, Some(1));
/// assert_eq!(fb.stderr_tail.as_deref(), Some("invalid repository password"));
/// // A wrong password is not worth a blind retry.
/// assert!(!fb.retry_recommended);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailureBlock {
    /// kopia error class (e.g. `RepositoryUnavailable`, `AuthFailure`).
    pub kopia_error_class: String,
    /// A short human-readable message.
    pub message: String,
    /// The last lines of kopia's stderr, if any were captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_tail: Option<String>,
    /// The process exit code, if one was reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Whether the operator should retry the same operation.
    pub retry_recommended: bool,
}

impl From<&KopiaError> for FailureBlock {
    fn from(err: &KopiaError) -> Self {
        let class = err.class();
        let exit_code = match err {
            KopiaError::NonZeroExit { code, .. } => *code,
            _ => None,
        };
        FailureBlock {
            kopia_error_class: class.as_str().to_string(),
            message: err.to_string(),
            stderr_tail: err.stderr_tail().map(str::to_string),
            exit_code,
            retry_recommended: class.is_retryable(),
        }
    }
}

/// The phase a mover run reports.
///
/// ```
/// use kopiur_mover::status::MoverPhase;
///
/// assert_eq!(MoverPhase::Running.as_str(), "Running");
/// assert_eq!(MoverPhase::Succeeded.as_str(), "Succeeded");
/// assert_eq!(MoverPhase::Failed.as_str(), "Failed");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MoverPhase {
    /// kopia is running.
    Running,
    /// The operation completed successfully.
    Succeeded,
    /// The operation failed terminally.
    Failed,
}

impl MoverPhase {
    /// Stable string form for the CR status `phase` field.
    pub fn as_str(&self) -> &'static str {
        match self {
            MoverPhase::Running => "Running",
            MoverPhase::Succeeded => "Succeeded",
            MoverPhase::Failed => "Failed",
        }
    }
}

/// A status update the mover PATCHes onto the CR. This is the payload shape;
/// the kube call wraps it under `{ "status": ... }`.
///
/// [`StatusUpdate::as_patch_body`] nests the payload under `status` for a
/// status-subresource merge PATCH:
///
/// ```
/// use chrono::{DateTime, Utc};
/// use kopiur_mover::status::StatusUpdate;
///
/// let observed_at: DateTime<Utc> = "2026-06-01T12:00:00Z".parse().unwrap();
/// let update = StatusUpdate::running(observed_at);
/// assert_eq!(update.phase, "Running");
///
/// let body = update.as_patch_body();
/// assert_eq!(body["status"]["phase"], "Running");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusUpdate {
    /// Current phase.
    pub phase: String,
    /// When this update was produced.
    pub observed_at: DateTime<Utc>,
    /// The snapshot (CRD `status.snapshot`), once known. Nested `SnapshotInfo`
    /// (`{ kopiaSnapshotID, identity }`) so the API server doesn't prune it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotInfo>,
    /// Timing, on success (CRD `status.timing`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing: Option<BackupTiming>,
    /// Stats, on success (CRD `status.stats`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<BackupStats>,
    /// Failure block, on terminal failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureBlock>,
}

impl StatusUpdate {
    /// A "running / progress" update with the given timestamp.
    pub fn running(observed_at: DateTime<Utc>) -> Self {
        StatusUpdate {
            phase: MoverPhase::Running.as_str().to_string(),
            observed_at,
            snapshot: None,
            timing: None,
            stats: None,
            failure: None,
        }
    }

    /// A successful backup update from a kopia create result.
    pub fn succeeded_backup(result: &SnapshotCreateResult, observed_at: DateTime<Utc>) -> Self {
        StatusUpdate {
            phase: MoverPhase::Succeeded.as_str().to_string(),
            observed_at,
            snapshot: Some(snapshot_from_result(result)),
            timing: Some(timing_from_result(result)),
            stats: Some(stats_from_result(result)),
            failure: None,
        }
    }

    /// A successful snapshot-delete update (Backup finalizer path) with no stats.
    /// The Backup CRD's terminal success phase is `Succeeded`.
    pub fn succeeded(observed_at: DateTime<Utc>) -> Self {
        StatusUpdate {
            phase: MoverPhase::Succeeded.as_str().to_string(),
            observed_at,
            snapshot: None,
            timing: None,
            stats: None,
            failure: None,
        }
    }

    /// A successful restore update with no stats. The Restore CRD's terminal
    /// success phase is `Completed` — NOT `Succeeded` (the Backup phase). Writing
    /// `Succeeded` here is rejected by the apiserver with a 422 (the enum forbids
    /// it), so the phase string is sourced from [`RestorePhase::Completed`] to
    /// stay locked to the CRD.
    pub fn completed(observed_at: DateTime<Utc>) -> Self {
        StatusUpdate {
            phase: RestorePhase::Completed.label().to_string(),
            observed_at,
            snapshot: None,
            timing: None,
            stats: None,
            failure: None,
        }
    }

    /// A terminal-failure update from a kopia error.
    pub fn failed(err: &KopiaError, observed_at: DateTime<Utc>) -> Self {
        StatusUpdate {
            phase: MoverPhase::Failed.as_str().to_string(),
            observed_at,
            snapshot: None,
            timing: None,
            stats: None,
            failure: Some(FailureBlock::from(err)),
        }
    }

    /// Wrap this update as the `{ "status": ... }` merge-patch body kube
    /// expects for a status subresource PATCH.
    pub fn as_patch_body(&self) -> serde_json::Value {
        serde_json::json!({ "status": self })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_kopia::KopiaErrorClass;

    fn ts() -> DateTime<Utc> {
        "2026-06-01T12:00:00Z".parse().unwrap()
    }

    #[test]
    fn stats_from_create_result_uses_crd_field_names() {
        let json = r#"{
            "id":"x","source":{"host":"h","userName":"u","path":"/p"},
            "startTime":"2026-06-02T03:13:59Z","endTime":"2026-06-02T03:14:00Z",
            "rootEntry":{"name":"p","type":"d","obj":"k1","summ":{"size":100,"files":5,"dirs":2,"numFailed":1}}
        }"#;
        let r: SnapshotCreateResult = serde_json::from_str(json).unwrap();
        let stats = stats_from_result(&r);
        assert_eq!(stats.size_bytes, Some(100));
        assert_eq!(stats.files_new, Some(5));
        // The serialized body MUST use the CRD `status.stats` field names, or the
        // API server prunes them and the stats are lost (regression guard).
        let body = serde_json::to_value(&stats).unwrap();
        assert_eq!(body["sizeBytes"], 100);
        assert_eq!(body["filesNew"], 5);
        assert!(body.get("totalBytes").is_none(), "stale field name leaked");
    }

    #[test]
    fn timing_from_create_result_computes_duration() {
        let json = r#"{
            "id":"x","source":{"host":"h","userName":"u","path":"/p"},
            "startTime":"2026-06-02T03:13:59Z","endTime":"2026-06-02T03:14:00Z",
            "rootEntry":{"name":"p","type":"d","obj":"k1","summ":{"size":1,"files":1}}
        }"#;
        let r: SnapshotCreateResult = serde_json::from_str(json).unwrap();
        let timing = timing_from_result(&r);
        assert_eq!(timing.duration_seconds, Some(1));
    }

    #[test]
    fn failure_block_from_nonzero_exit_retryable() {
        let err = KopiaError::NonZeroExit {
            args: "snapshot create".into(),
            code: Some(1),
            class: KopiaErrorClass::RepositoryUnavailable,
            stderr_tail: "error connecting to repository: dial tcp".into(),
        };
        let fb = FailureBlock::from(&err);
        assert_eq!(fb.kopia_error_class, "RepositoryUnavailable");
        assert_eq!(fb.exit_code, Some(1));
        assert_eq!(
            fb.stderr_tail.as_deref(),
            Some("error connecting to repository: dial tcp")
        );
        assert!(fb.retry_recommended);
    }

    #[test]
    fn failure_block_from_auth_not_retryable() {
        let err = KopiaError::NonZeroExit {
            args: "repository connect".into(),
            code: Some(1),
            class: KopiaErrorClass::AuthFailure,
            stderr_tail: "invalid repository password".into(),
        };
        let fb = FailureBlock::from(&err);
        assert_eq!(fb.kopia_error_class, "AuthFailure");
        assert!(!fb.retry_recommended);
    }

    #[test]
    fn failure_block_from_spawn_error_no_exit_code() {
        let err = KopiaError::Spawn {
            binary: "kopia".into(),
            source: std::io::Error::from(std::io::ErrorKind::NotFound),
        };
        let fb = FailureBlock::from(&err);
        assert_eq!(fb.kopia_error_class, "Unknown");
        assert_eq!(fb.exit_code, None);
        assert_eq!(fb.stderr_tail, None);
        assert!(!fb.retry_recommended);
    }

    #[test]
    fn failure_block_from_timeout_retryable() {
        let err = KopiaError::Timeout {
            args: "snapshot create".into(),
            seconds: 3600,
        };
        let fb = FailureBlock::from(&err);
        assert_eq!(fb.kopia_error_class, "RepositoryUnavailable");
        assert!(fb.retry_recommended);
    }

    #[test]
    fn succeeded_backup_update() {
        let json = r#"{
            "id":"snap1","source":{"host":"h","userName":"u","path":"/p"},
            "startTime":"2026-06-02T03:13:59Z","endTime":"2026-06-02T03:14:00Z",
            "rootEntry":{"name":"p","type":"d","obj":"k1","summ":{"size":42,"files":3}}
        }"#;
        let r: SnapshotCreateResult = serde_json::from_str(json).unwrap();
        let u = StatusUpdate::succeeded_backup(&r, ts());
        assert_eq!(u.phase, "Succeeded");
        // The snapshot MUST serialize as the nested CRD shape
        // `status.snapshot.kopiaSnapshotID`, or the API server prunes it (the bug
        // that left object-store backups Succeeded with no snapshot id).
        let snap = u.snapshot.as_ref().expect("snapshot present");
        assert_eq!(snap.kopia_snapshot_id, "snap1");
        assert_eq!(snap.identity.username, "u");
        assert_eq!(snap.identity.hostname, "h");
        let body = u.as_patch_body();
        assert_eq!(body["status"]["snapshot"]["kopiaSnapshotID"], "snap1");
        assert!(
            body["status"].get("snapshotId").is_none(),
            "flat snapshotId leaked; the API server would prune it"
        );
        assert_eq!(u.stats.as_ref().unwrap().size_bytes, Some(42));
        assert_eq!(u.stats.as_ref().unwrap().files_new, Some(3));
        assert!(u.timing.is_some());
        assert!(u.failure.is_none());
    }

    #[test]
    fn restore_terminal_phase_is_completed_not_succeeded() {
        // Regression: the mover used `succeeded()` ("Succeeded") for restores,
        // but the Restore CRD enum only allows "Completed", so the status PATCH
        // was rejected 422 and every restore flooded the controller logs. The
        // restore terminal phase MUST match RestorePhase::Completed.
        let u = StatusUpdate::completed(ts());
        assert_eq!(u.phase, "Completed");
        assert_eq!(u.phase, RestorePhase::Completed.label());
        assert_ne!(u.phase, MoverPhase::Succeeded.as_str());
        assert!(u.failure.is_none());
        assert!(u.snapshot.is_none());
        assert_eq!(u.as_patch_body()["status"]["phase"], "Completed");
    }

    #[test]
    fn failed_update_carries_block() {
        let err = KopiaError::EmptyOutput {
            context: "snapshot create result".into(),
        };
        let u = StatusUpdate::failed(&err, ts());
        assert_eq!(u.phase, "Failed");
        assert!(u.failure.is_some());
        assert_eq!(u.failure.unwrap().kopia_error_class, "Unknown");
    }

    #[test]
    fn patch_body_wraps_under_status() {
        let u = StatusUpdate::running(ts());
        let body = u.as_patch_body();
        assert_eq!(body["status"]["phase"], "Running");
    }
}
