//! Status types the mover PATCHes onto the `Snapshot`/`Restore` `.status`
//! subresource, plus the **pure** mapping from kopia results/errors to those
//! types.
//!
//! The pure mapping (`KopiaError → FailureBlock`, `SnapshotCreateResult →
//! `kopiur_api::SnapshotStats`/`SnapshotTiming`) is unit-testable with no cluster.
//! The stats/timing types are the CRD's own (not mover-local) so their field
//! names cannot drift from the structural schema — a mismatch is silently pruned
//! by the API server. The actual kube PATCH lives in a thin function gated so
//! tests don't need a client.

use chrono::{DateTime, Utc};
use kopiur_api::common::ResolvedIdentity;
use kopiur_api::restore::RestorePhase;
use kopiur_api::snapshot::SnapshotInfo;
use kopiur_api::{PhaseLabel, SnapshotStats, SnapshotTiming};
use kopiur_kopia::{KopiaError, SnapshotCreateResult};
use serde::{Deserialize, Serialize};

/// Map a kopia create result to the CRD `status.stats` shape (`SnapshotStats`).
///
/// We reuse the API type rather than a mover-local struct so the field names
/// stay in lockstep with the `Snapshot` CRD's structural schema. They MUST match:
/// the API server **prunes unknown status fields**, so a drifting name (the old
/// mover-local `totalBytes`/`fileCount`) is silently dropped and `status.stats`
/// lands as `{}` — which is exactly the bug that left `kopiur_snapshot_size_bytes`
/// empty. kopia's snapshot-create summary reports the snapshot's total size and
/// file count, mapped to `sizeBytes`/`filesNew`.
fn stats_from_result(r: &SnapshotCreateResult) -> SnapshotStats {
    SnapshotStats {
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
fn timing_from_result(r: &SnapshotCreateResult) -> SnapshotTiming {
    SnapshotTiming {
        start_time: Some(r.start_time.to_rfc3339()),
        end_time: Some(r.end_time.to_rfc3339()),
        duration_seconds: Some((r.end_time - r.start_time).num_seconds()),
    }
}

/// The structured terminal-failure block (ADR §4.10), re-exported from
/// `kopiur-api` so the field names are the CRD's own — the API server prunes a
/// status field the schema doesn't define, which is exactly how the original
/// mover-local `FailureBlock` was silently lost (`SnapshotStatus` had no
/// `failure` property until the API type landed).
pub use kopiur_api::common::FailureBlock;

/// Build a [`FailureBlock`] from a bare kopia error; the class, stderr tail,
/// exit code, and retry hint all carry through. (A free function, not
/// `From<&KopiaError>`: both types are foreign here, so the impl would violate
/// the orphan rule.)
///
/// ```
/// use kopiur_kopia::{KopiaError, KopiaErrorClass};
/// use kopiur_mover::status::failure_block_from_kopia;
///
/// let err = KopiaError::NonZeroExit {
///     args: "repository connect".into(),
///     code: Some(1),
///     class: KopiaErrorClass::AuthFailure,
///     stderr_tail: "invalid repository password".into(),
/// };
/// let fb = failure_block_from_kopia(&err);
/// assert_eq!(fb.kopia_error_class, "AuthFailure");
/// assert_eq!(fb.exit_code, Some(1));
/// assert_eq!(fb.stderr_tail.as_deref(), Some("invalid repository password"));
/// // A wrong password is not worth a blind retry.
/// assert!(!fb.retry_recommended);
/// ```
pub fn failure_block_from_kopia(err: &KopiaError) -> FailureBlock {
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

impl From<&crate::error::MoverError> for FailureBlock {
    /// Map a typed mover failure to the persisted block. The structured error
    /// is stringified **only here**, at the status surface; a kopia-backed
    /// failure carries its stderr tail and exit code through, and the class +
    /// retry hint always come from [`MoverError::kopia_class`]
    /// (`crate::error::MoverError::kopia_class`) so they cannot drift from the
    /// message.
    fn from(err: &crate::error::MoverError) -> Self {
        use crate::error::MoverError;
        let (stderr_tail, exit_code) = match err {
            MoverError::Kopia { source, .. } => (
                source.stderr_tail().map(str::to_string),
                match source {
                    KopiaError::NonZeroExit { code, .. } => *code,
                    KopiaError::Spawn { .. }
                    | KopiaError::Json { .. }
                    | KopiaError::EmptyOutput { .. }
                    | KopiaError::Timeout { .. } => None,
                },
            ),
            MoverError::BootstrapFailed { .. }
            | MoverError::WorkSpecPathMissing
            | MoverError::WorkSpecRead { .. }
            | MoverError::WorkSpecParse { .. }
            | MoverError::CredentialStagingDir { .. }
            | MoverError::CredentialWrite { .. }
            | MoverError::ReadyMarkerWrite { .. }
            | MoverError::VerifyNoSnapshot { .. }
            | MoverError::SuccessExprFalse { .. }
            | MoverError::SuccessExprEval { .. }
            | MoverError::KubeClient { .. }
            | MoverError::StatusPatch { .. }
            | MoverError::ResultSerialize { .. }
            | MoverError::ResultConfigMapPatch { .. }
            | MoverError::Telemetry(_) => (None, None),
        };
        FailureBlock {
            kopia_error_class: err.kopia_class().as_str().to_string(),
            message: err.to_string(),
            stderr_tail,
            exit_code,
            retry_recommended: err.retry_recommended(),
        }
    }
}

/// Truncate to the LAST [`MAX_LOG_TAIL_BYTES`] bytes (the newest output is the
/// actionable part), cutting on a `char` boundary and preferring to start at the
/// first whole line after the cut. Pure.
pub fn capped_tail(s: &str) -> String {
    use kopiur_api::common::MAX_LOG_TAIL_BYTES;
    if s.len() <= MAX_LOG_TAIL_BYTES {
        return s.to_string();
    }
    let mut start = s.len() - MAX_LOG_TAIL_BYTES;
    while !s.is_char_boundary(start) {
        start += 1;
    }
    let tail = &s[start..];
    // Prefer starting on a whole line, as long as that doesn't eat most of the tail.
    match tail.find('\n') {
        Some(nl) if nl + 1 < tail.len() && nl < MAX_LOG_TAIL_BYTES / 8 => {
            tail[nl + 1..].to_string()
        }
        _ => tail.to_string(),
    }
}

/// The `logTail` text for a terminal failure: the actionable message plus the
/// kopia stderr tail (when present), capped. Deterministic given the failure —
/// no timestamps — so a re-patch of the same outcome cannot churn status.
fn failure_log_tail(failure: &FailureBlock) -> String {
    match failure.stderr_tail.as_deref() {
        Some(stderr) if !stderr.is_empty() => {
            capped_tail(&format!("{}\n{}", failure.message, stderr))
        }
        _ => capped_tail(&failure.message),
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
    pub timing: Option<SnapshotTiming>,
    /// Stats, on success (CRD `status.stats`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<SnapshotStats>,
    /// Failure block, on terminal failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureBlock>,
    /// The last lines of the run's output (CRD `status.logTail`), set ONLY by
    /// the terminal constructors — never by `running()` — so it is written once
    /// per terminal transition and cannot churn status. Bounded by
    /// [`kopiur_api::common::MAX_LOG_TAIL_BYTES`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_tail: Option<String>,
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
            log_tail: None,
        }
    }

    /// A successful backup update from a kopia create result. `logTail` carries
    /// the documented `Snapshot created: <id>` line (ADR §3.4).
    pub fn succeeded_backup(result: &SnapshotCreateResult, observed_at: DateTime<Utc>) -> Self {
        StatusUpdate {
            phase: MoverPhase::Succeeded.as_str().to_string(),
            observed_at,
            snapshot: Some(snapshot_from_result(result)),
            timing: Some(timing_from_result(result)),
            stats: Some(stats_from_result(result)),
            failure: None,
            log_tail: Some(format!("Snapshot created: {}", result.id)),
        }
    }

    /// A successful snapshot-delete update (Snapshot finalizer path) with no stats.
    /// The Snapshot CRD's terminal success phase is `Succeeded`.
    pub fn succeeded(observed_at: DateTime<Utc>) -> Self {
        StatusUpdate {
            phase: MoverPhase::Succeeded.as_str().to_string(),
            observed_at,
            snapshot: None,
            timing: None,
            stats: None,
            failure: None,
            log_tail: None,
        }
    }

    /// A successful restore update with no stats. The Restore CRD's terminal
    /// success phase is `Completed` — NOT `Succeeded` (the Snapshot phase). Writing
    /// `Succeeded` here is rejected by the apiserver with a 422 (the enum forbids
    /// it), so the phase string is sourced from [`RestorePhase::Completed`] to
    /// stay locked to the CRD. `snapshot_id` is the kopia snapshot that was
    /// restored, surfaced on `status.logTail`.
    pub fn completed(snapshot_id: &str, observed_at: DateTime<Utc>) -> Self {
        StatusUpdate {
            phase: RestorePhase::Completed.label().to_string(),
            observed_at,
            snapshot: None,
            timing: None,
            stats: None,
            failure: None,
            log_tail: Some(format!("Restore completed: snapshot {snapshot_id}")),
        }
    }

    /// A terminal-failure update from a kopia error. `logTail` mirrors the
    /// failure's message + stderr tail so `kubectl get -o yaml` shows the
    /// actionable text without digging into the (possibly reaped) Job pod.
    pub fn failed(err: &KopiaError, observed_at: DateTime<Utc>) -> Self {
        let failure = failure_block_from_kopia(err);
        StatusUpdate {
            phase: MoverPhase::Failed.as_str().to_string(),
            observed_at,
            snapshot: None,
            timing: None,
            stats: None,
            log_tail: Some(failure_log_tail(&failure)),
            failure: Some(failure),
        }
    }

    /// A terminal-failure update from a typed mover error — same JSON shape as
    /// [`StatusUpdate::failed`], but the message names which operation failed
    /// and non-kopia failures (work spec, credentials, …) are representable.
    pub fn failed_mover(err: &crate::error::MoverError, observed_at: DateTime<Utc>) -> Self {
        let failure = FailureBlock::from(err);
        StatusUpdate {
            phase: MoverPhase::Failed.as_str().to_string(),
            observed_at,
            snapshot: None,
            timing: None,
            stats: None,
            log_tail: Some(failure_log_tail(&failure)),
            failure: Some(failure),
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
        let fb = failure_block_from_kopia(&err);
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
        let fb = failure_block_from_kopia(&err);
        assert_eq!(fb.kopia_error_class, "AuthFailure");
        assert!(!fb.retry_recommended);
    }

    #[test]
    fn failure_block_from_spawn_error_no_exit_code() {
        let err = KopiaError::Spawn {
            binary: "kopia".into(),
            source: std::io::Error::from(std::io::ErrorKind::NotFound),
        };
        let fb = failure_block_from_kopia(&err);
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
        let fb = failure_block_from_kopia(&err);
        assert_eq!(fb.kopia_error_class, "RepositoryUnavailable");
        assert!(fb.retry_recommended);
    }

    #[test]
    fn failure_block_from_mover_kopia_matches_the_kopia_error_path() {
        // The MoverError wrapper must not lose anything the bare-KopiaError
        // mapping carried: class, stderr tail, exit code, retry hint all
        // survive; only the message gains the "which op" prefix.
        use crate::error::{KopiaOp, MoverError};
        let kopia = KopiaError::NonZeroExit {
            args: "snapshot create".into(),
            code: Some(1),
            class: KopiaErrorClass::RepositoryUnavailable,
            stderr_tail: "dial tcp: connection refused".into(),
        };
        let bare = failure_block_from_kopia(&kopia);
        let wrapped = FailureBlock::from(&MoverError::Kopia {
            op: KopiaOp::SnapshotCreate,
            source: kopia,
        });
        assert_eq!(wrapped.kopia_error_class, bare.kopia_error_class);
        assert_eq!(wrapped.stderr_tail, bare.stderr_tail);
        assert_eq!(wrapped.exit_code, bare.exit_code);
        assert_eq!(wrapped.retry_recommended, bare.retry_recommended);
        assert!(wrapped.message.starts_with("snapshot create failed"));
    }

    #[test]
    fn failure_block_from_bootstrap_failed_keeps_class_and_retry_hint() {
        use crate::error::MoverError;
        let fb = FailureBlock::from(&MoverError::BootstrapFailed {
            class: KopiaErrorClass::AuthFailure,
            message: "invalid repository password".into(),
        });
        assert_eq!(fb.kopia_error_class, "AuthFailure");
        assert!(!fb.retry_recommended);
        assert!(fb.stderr_tail.is_none());
    }

    #[test]
    fn failure_block_from_environmental_mover_error_is_unknown_non_retryable() {
        use crate::error::MoverError;
        let fb = FailureBlock::from(&MoverError::WorkSpecPathMissing);
        assert_eq!(fb.kopia_error_class, "Unknown");
        assert!(!fb.retry_recommended);
        assert!(fb.exit_code.is_none());
        assert!(fb.message.contains("KOPIUR_WORK_SPEC_PATH"));
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
        let u = StatusUpdate::completed("k1f1ec0a8", ts());
        assert_eq!(u.phase, "Completed");
        assert_eq!(u.phase, RestorePhase::Completed.label());
        assert_ne!(u.phase, MoverPhase::Succeeded.as_str());
        assert!(u.failure.is_none());
        assert!(u.snapshot.is_none());
        let body = u.as_patch_body();
        assert_eq!(body["status"]["phase"], "Completed");
        // The restored snapshot id is surfaced on logTail (the exact CRD field
        // name — a drifting name would be pruned by the API server).
        assert_eq!(
            body["status"]["logTail"],
            "Restore completed: snapshot k1f1ec0a8"
        );
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

    #[test]
    fn capped_tail_keeps_the_last_bytes_on_char_and_line_boundaries() {
        use kopiur_api::common::MAX_LOG_TAIL_BYTES;
        // Under the cap: passthrough.
        assert_eq!(capped_tail("short"), "short");
        // Exactly at the cap: passthrough.
        let exact = "x".repeat(MAX_LOG_TAIL_BYTES);
        assert_eq!(capped_tail(&exact), exact);
        // Over the cap: keeps the LAST bytes (the newest output), bounded.
        let over = format!("{}{}", "a".repeat(MAX_LOG_TAIL_BYTES), "tail-marker");
        let capped = capped_tail(&over);
        assert!(capped.len() <= MAX_LOG_TAIL_BYTES);
        assert!(capped.ends_with("tail-marker"));
        // Multi-byte char straddling the cut: never panics, stays valid UTF-8.
        let snowmen = "☃".repeat(MAX_LOG_TAIL_BYTES); // 3 bytes each
        let capped = capped_tail(&snowmen);
        assert!(capped.len() <= MAX_LOG_TAIL_BYTES);
        assert!(capped.chars().all(|c| c == '☃'));
        // A newline shortly after the cut: the tail starts on a whole line
        // (a partial first line is dropped when a full one follows close by).
        let payload = format!("fresh line{}", "x".repeat(MAX_LOG_TAIL_BYTES - 60));
        let lines = format!("{}\n{}", "junk".repeat(2000), payload);
        assert!(capped_tail(&lines).starts_with("fresh line"));
    }

    #[test]
    fn terminal_updates_carry_log_tail_with_the_exact_crd_field_name() {
        // Success: the documented `Snapshot created: <id>` line (ADR §3.4),
        // serialized as `logTail` — the CRD's field name. A drifting name is
        // silently pruned by the API server (regression guard).
        let json = r#"{
            "id":"snap1","source":{"host":"h","userName":"u","path":"/p"},
            "startTime":"2026-06-02T03:13:59Z","endTime":"2026-06-02T03:14:00Z",
            "rootEntry":{"name":"p","type":"d","obj":"k1","summ":{"size":42,"files":3}}
        }"#;
        let r: SnapshotCreateResult = serde_json::from_str(json).unwrap();
        let body = StatusUpdate::succeeded_backup(&r, ts()).as_patch_body();
        assert_eq!(body["status"]["logTail"], "Snapshot created: snap1");

        // Failure: logTail carries the actionable message + kopia stderr tail,
        // alongside the structured failure block.
        let err = KopiaError::NonZeroExit {
            args: "repository connect".into(),
            code: Some(1),
            class: KopiaErrorClass::AuthFailure,
            stderr_tail: "invalid repository password".into(),
        };
        let body = StatusUpdate::failed(&err, ts()).as_patch_body();
        let tail = body["status"]["logTail"].as_str().unwrap();
        assert!(tail.contains("invalid repository password"), "{tail}");
        assert_eq!(
            body["status"]["failure"]["kopiaErrorClass"], "AuthFailure",
            "the structured failure block must land under the CRD's field names"
        );

        // Progress updates never set logTail (it is written once, at the
        // terminal transition — the status-churn rule).
        let body = StatusUpdate::running(ts()).as_patch_body();
        assert!(body["status"].get("logTail").is_none());
    }
}
