//! Repository-bootstrap result types + the pure "should we create?" decision.
//!
//! The mover's `BootstrapRepository` operation connects to (or creates) an
//! object-store repository the controller cannot reach in-process (ADR §5.4) and
//! reports the outcome back via a [`BootstrapResult`] written into the work-spec
//! `ConfigMap` (key [`RESULT_CONFIGMAP_KEY`]). The controller — the single writer
//! of the `Repository` status — reads it and patches `phase`/`uniqueId`/
//! `storageStats`, then materializes `origin: discovered` Backup CRs.
//!
//! This module is **pure data + serde** plus the create-gate decision; the kopia
//! subprocess calls and the kube `ConfigMap` PATCH live in `main.rs`.

use kopiur_kopia::{KopiaError, KopiaErrorClass, SnapshotListEntry};
use serde::{Deserialize, Serialize};

use crate::status::FailureBlock;

/// The `ConfigMap` data key the bootstrap result is written under (the mover
/// writes it; the controller reads it — one definition so the contract can't
/// drift, mirroring [`crate::env::WORK_SPEC_PATH`]).
pub const RESULT_CONFIGMAP_KEY: &str = "result.json";

/// Upper bound on snapshot entries returned for materialization. Bounds the
/// `ConfigMap` size (etcd's ~1MB object limit). The snapshot *count* is reported
/// exactly regardless; only the per-entry list for materialization is capped, and
/// the cap is surfaced via [`BootstrapResult::snapshots_truncated`] (never a
/// silent truncation).
pub const MAX_RETURNED_SNAPSHOTS: usize = 1000;

/// The outcome of a bootstrap run, serialized into the work-spec `ConfigMap`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapResult {
    /// Whether connect/create succeeded and the repository is usable.
    pub success: bool,
    /// `true` when this run created a new repository (vs adopting an existing
    /// one). Drives the controller's "created" vs "connected" event.
    #[serde(default)]
    pub created: bool,
    /// The repository's stable kopia unique id (on success).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<String>,
    /// Total snapshots in the repository (authoritative; not affected by the
    /// returned-entries cap).
    #[serde(default)]
    pub snapshot_count: i64,
    /// Snapshot entries for the controller to materialize as discovered Backups.
    /// Empty when `scanCatalog` was off, or capped to [`MAX_RETURNED_SNAPSHOTS`].
    #[serde(default)]
    pub snapshots: Vec<SnapshotListEntry>,
    /// `true` if more than [`MAX_RETURNED_SNAPSHOTS`] existed and the returned
    /// list was capped (so the controller can log that not all were materialized).
    #[serde(default)]
    pub snapshots_truncated: bool,
    /// Structured failure block on `success == false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureBlock>,
}

impl BootstrapResult {
    /// A successful bootstrap outcome.
    pub fn ready(
        created: bool,
        unique_id: Option<String>,
        snapshot_count: i64,
        snapshots: Vec<SnapshotListEntry>,
        snapshots_truncated: bool,
    ) -> Self {
        BootstrapResult {
            success: true,
            created,
            unique_id,
            snapshot_count,
            snapshots,
            snapshots_truncated,
            failure: None,
        }
    }

    /// A terminal-failure outcome carrying the kopia error class + stderr tail.
    pub fn failed(err: &KopiaError) -> Self {
        BootstrapResult {
            success: false,
            created: false,
            unique_id: None,
            snapshot_count: 0,
            snapshots: Vec::new(),
            snapshots_truncated: false,
            failure: Some(FailureBlock::from(err)),
        }
    }
}

/// Whether, after a failed connect, the mover should attempt `repository
/// create`. Pure so it is unit-tested without kopia.
///
/// Create is attempted only when `auto_create` is set AND the failure class does
/// not indicate an *existing* repository:
/// - `AuthFailure` ⇒ a repo exists here that the password can't open — never
///   recreate (would risk a second repo / mask the real wrong-password error).
/// - `Locked` ⇒ a repo exists and is held by another writer — retry, don't create.
/// - everything else (`NotFound`, `RepositoryUnavailable`, `SourceError`,
///   `Unknown`) ⇒ attempt create. kopia's own `create` refuses to overwrite an
///   existing repository (the format blob backstop), so this can never smash
///   data; a genuinely unreachable backend simply fails `create` too, surfacing
///   the real error.
pub fn should_attempt_create(auto_create: bool, class: KopiaErrorClass) -> bool {
    auto_create
        && !matches!(
            class,
            KopiaErrorClass::AuthFailure | KopiaErrorClass::Locked
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_blocked_on_auth_and_lock() {
        // An existing repo we can't open / is locked must never be recreated.
        assert!(!should_attempt_create(true, KopiaErrorClass::AuthFailure));
        assert!(!should_attempt_create(true, KopiaErrorClass::Locked));
    }

    #[test]
    fn create_attempted_for_absent_or_unknown_when_enabled() {
        assert!(should_attempt_create(true, KopiaErrorClass::NotFound));
        assert!(should_attempt_create(
            true,
            KopiaErrorClass::RepositoryUnavailable
        ));
        assert!(should_attempt_create(true, KopiaErrorClass::Unknown));
        assert!(should_attempt_create(true, KopiaErrorClass::SourceError));
    }

    #[test]
    fn create_never_attempted_when_disabled() {
        for class in [
            KopiaErrorClass::NotFound,
            KopiaErrorClass::AuthFailure,
            KopiaErrorClass::Unknown,
            KopiaErrorClass::RepositoryUnavailable,
        ] {
            assert!(!should_attempt_create(false, class));
        }
    }

    #[test]
    fn ready_result_roundtrips_via_serde() {
        let r = BootstrapResult::ready(true, Some("abc".into()), 3, vec![], false);
        let back: BootstrapResult =
            serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back, r);
        assert!(back.success && back.created);
        assert_eq!(back.unique_id.as_deref(), Some("abc"));
        assert_eq!(back.snapshot_count, 3);
    }

    #[test]
    fn failed_result_roundtrips_with_failure_block() {
        let err = KopiaError::EmptyOutput {
            context: "repository status".into(),
        };
        let f = BootstrapResult::failed(&err);
        let back: BootstrapResult =
            serde_json::from_str(&serde_json::to_string(&f).unwrap()).unwrap();
        assert_eq!(back, f);
        assert!(!back.success);
        assert_eq!(back.failure.unwrap().kopia_error_class, "Unknown");
    }
}
