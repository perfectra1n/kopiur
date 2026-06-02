//! Status types the mover PATCHes onto the `Backup`/`Restore` `.status`
//! subresource, plus the **pure** mapping from kopia results/errors to those
//! types.
//!
//! The pure mapping (`KopiaError → FailureBlock`, `SnapshotCreateResult →
//! StatusStats`) is unit-testable with no cluster. The actual kube PATCH lives
//! in a thin function (`patch_status`) gated so tests don't need a client.

use chrono::{DateTime, Utc};
use kopiur_kopia::{KopiaError, SnapshotCreateResult};
use serde::{Deserialize, Serialize};

/// Aggregate snapshot statistics surfaced on a successful `Backup.status`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusStats {
    /// Total logical bytes in the snapshot.
    pub total_bytes: u64,
    /// Number of files in the snapshot.
    pub file_count: u64,
    /// Number of entries that failed during the walk.
    pub error_count: u64,
}

impl From<&SnapshotCreateResult> for StatusStats {
    fn from(r: &SnapshotCreateResult) -> Self {
        StatusStats {
            total_bytes: r.total_bytes(),
            file_count: r.file_count(),
            error_count: r.error_count(),
        }
    }
}

/// A structured terminal-failure block (ADR §4.10): kopia error class, the last
/// stderr lines, and a retry recommendation. Written to `status.failure` before
/// the mover exits non-zero.
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusUpdate {
    /// Current phase.
    pub phase: String,
    /// When this update was produced.
    pub observed_at: DateTime<Utc>,
    /// The snapshot id, once known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    /// Stats, on success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<StatusStats>,
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
            snapshot_id: None,
            stats: None,
            failure: None,
        }
    }

    /// A successful backup update from a kopia create result.
    pub fn succeeded_backup(result: &SnapshotCreateResult, observed_at: DateTime<Utc>) -> Self {
        StatusUpdate {
            phase: MoverPhase::Succeeded.as_str().to_string(),
            observed_at,
            snapshot_id: Some(result.id.clone()),
            stats: Some(StatusStats::from(result)),
            failure: None,
        }
    }

    /// A successful non-backup update (restore / delete) with no stats.
    pub fn succeeded(observed_at: DateTime<Utc>) -> Self {
        StatusUpdate {
            phase: MoverPhase::Succeeded.as_str().to_string(),
            observed_at,
            snapshot_id: None,
            stats: None,
            failure: None,
        }
    }

    /// A terminal-failure update from a kopia error.
    pub fn failed(err: &KopiaError, observed_at: DateTime<Utc>) -> Self {
        StatusUpdate {
            phase: MoverPhase::Failed.as_str().to_string(),
            observed_at,
            snapshot_id: None,
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
    fn stats_from_create_result() {
        let json = r#"{
            "id":"x","source":{"host":"h","userName":"u","path":"/p"},
            "startTime":"2026-06-02T03:13:59Z","endTime":"2026-06-02T03:14:00Z",
            "rootEntry":{"name":"p","type":"d","obj":"k1","summ":{"size":100,"files":5,"dirs":2,"numFailed":1}}
        }"#;
        let r: SnapshotCreateResult = serde_json::from_str(json).unwrap();
        let stats = StatusStats::from(&r);
        assert_eq!(stats.total_bytes, 100);
        assert_eq!(stats.file_count, 5);
        assert_eq!(stats.error_count, 1);
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
        assert_eq!(u.snapshot_id.as_deref(), Some("snap1"));
        assert_eq!(u.stats.as_ref().unwrap().total_bytes, 42);
        assert!(u.failure.is_none());
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
