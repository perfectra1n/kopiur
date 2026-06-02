//! The `Restore` reconciler (ADR §4.6, §4.7).
//!
//! Resolves the source (`backupRef` / `fromConfig` / `identity`), pins
//! `status.resolved`, creates a restore mover `Job`, and handles the passive
//! populator mode (a PVC's `spec.dataSourceRef` points at the `Restore`).
//!
//! The source-mode dispatch is an **exhaustive `match`** over the externally
//! tagged `RestoreSource` enum (no `_ =>`), and [`default_on_missing`] /
//! [`populator_state`] are pure decisions, all unit-tested. The pvc-prime
//! handshake IO is a documented partial (`TODO(M6)`).

use std::sync::Arc;

use kube::runtime::controller::Action;
use kube::ResourceExt;

use kopiur_api::{validate, OnMissingSnapshot, Restore, RestoreSource};

use crate::context::Context;
use crate::error::{error_policy_for, Error, Result};

/// Which source mode a restore uses, as a stable string (mirrors
/// `RestoreSource::kind_str`, re-derived through an exhaustive match so a new
/// variant must be handled here too).
pub fn source_mode(source: &RestoreSource) -> &'static str {
    match source {
        RestoreSource::BackupRef(_) => "BackupRef",
        RestoreSource::FromConfig(_) => "FromConfig",
        RestoreSource::Identity(_) => "Identity",
    }
}

/// The default `onMissingSnapshot` for a source mode when the spec doesn't set
/// it (ADR §4.6 / SKILL "Restores fail closed"): `fromConfig` defaults to
/// `Continue` (deploy-or-restore), everything else fails closed (`Fail`).
pub fn default_on_missing(source: &RestoreSource) -> OnMissingSnapshot {
    match source {
        RestoreSource::FromConfig(_) => OnMissingSnapshot::Continue,
        RestoreSource::BackupRef(_) | RestoreSource::Identity(_) => OnMissingSnapshot::Fail,
    }
}

/// Effective `onMissingSnapshot`: explicit spec value wins, else the per-mode
/// default.
pub fn effective_on_missing(
    spec: Option<OnMissingSnapshot>,
    source: &RestoreSource,
) -> OnMissingSnapshot {
    spec.unwrap_or_else(|| default_on_missing(source))
}

/// State of the passive-populator handshake. Pure model of the §4.7 machine so
/// the reconcile loop can dispatch without re-deriving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopulatorState {
    /// No `target` on the spec: this `Restore` is a passive populator source,
    /// awaiting a PVC `dataSourceRef` to claim it.
    AwaitingClaim,
    /// A `target` is set: the operator drives the restore directly.
    DirectTarget,
}

/// Decide the populator state from whether a `target` is present.
pub fn populator_state(has_target: bool) -> PopulatorState {
    if has_target {
        PopulatorState::DirectTarget
    } else {
        PopulatorState::AwaitingClaim
    }
}

/// Reconcile a `Restore`.
#[tracing::instrument(skip(restore, ctx), fields(kind = "Restore", name = %restore.name_any()))]
pub async fn reconcile(restore: Arc<Restore>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&restore, &ctx).await;
    ctx.metrics
        .record_reconcile("Restore", start.elapsed().as_secs_f64());
    result
}

async fn reconcile_inner(restore: &Restore, _ctx: &Context) -> Result<Action> {
    if let Err(e) = validate::validate_restore(&restore.spec) {
        return Err(Error::Validation(e.to_string()));
    }

    let _state = populator_state(restore.spec.target.is_some());
    let _mode = source_mode(&restore.spec.source);

    // TODO(M6): resolve the source to a concrete snapshot id (BackupRef → that
    // Backup's status.snapshot; FromConfig → identity + offset/asOf via kopia
    // snapshot list; Identity → directly), pin status.resolved; honor
    // effective_on_missing (Fail vs Continue); for DirectTarget create a restore
    // mover Job; for AwaitingClaim drive the pvc-prime handshake by watching the
    // target PVC's dataSourceRef. The pure decisions above are tested.

    Ok(Action::requeue(std::time::Duration::from_secs(300)))
}

/// `error_policy` for the `Restore` controller.
pub fn error_policy(_obj: Arc<Restore>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("Restore", err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::common::ObjectRef;
    use kopiur_api::restore::{FromConfig, IdentitySource};

    fn backup_ref() -> RestoreSource {
        RestoreSource::BackupRef(ObjectRef {
            name: "b".into(),
            namespace: None,
        })
    }
    fn from_config() -> RestoreSource {
        RestoreSource::FromConfig(FromConfig {
            name: "cfg".into(),
            namespace: None,
            as_of: None,
            offset: Some(0),
        })
    }
    fn identity() -> RestoreSource {
        RestoreSource::Identity(IdentitySource {
            username: "u".into(),
            hostname: "h".into(),
            source_path: None,
            snapshot_id: None,
            as_of: None,
            offset: None,
        })
    }

    #[test]
    fn from_config_defaults_to_continue_others_fail() {
        assert_eq!(
            default_on_missing(&from_config()),
            OnMissingSnapshot::Continue
        );
        assert_eq!(default_on_missing(&backup_ref()), OnMissingSnapshot::Fail);
        assert_eq!(default_on_missing(&identity()), OnMissingSnapshot::Fail);
    }

    #[test]
    fn explicit_on_missing_overrides_default() {
        // fromConfig would default Continue, but an explicit Fail wins.
        assert_eq!(
            effective_on_missing(Some(OnMissingSnapshot::Fail), &from_config()),
            OnMissingSnapshot::Fail
        );
        // backupRef defaults Fail, explicit Continue wins.
        assert_eq!(
            effective_on_missing(Some(OnMissingSnapshot::Continue), &backup_ref()),
            OnMissingSnapshot::Continue
        );
    }

    #[test]
    fn source_mode_strings_match_each_variant() {
        assert_eq!(source_mode(&backup_ref()), "BackupRef");
        assert_eq!(source_mode(&from_config()), "FromConfig");
        assert_eq!(source_mode(&identity()), "Identity");
    }

    #[test]
    fn populator_state_depends_on_target_presence() {
        assert_eq!(populator_state(false), PopulatorState::AwaitingClaim);
        assert_eq!(populator_state(true), PopulatorState::DirectTarget);
    }
}
