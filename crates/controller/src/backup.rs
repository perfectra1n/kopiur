//! The `Backup` reconciler — the heart of the ADR §5.5 thesis.
//!
//! Two paths:
//! 1. **Normal reconcile** (produced backups): add the `kopia.io/snapshot-cleanup`
//!    finalizer, create a mover `Job` + `ConfigMap` (work spec), watch it to a
//!    terminal state, copy stats/phase into `status`, and reap (owner-ref GC).
//! 2. **Deletion** (finalizer present, `deletionTimestamp` set): run the
//!    EXHAUSTIVE [`plan_deletion`] decision, execute its IO, then remove the
//!    finalizer.
//!
//! [`plan_deletion`] is a pure function over `(DeletionPolicy, annotations)`
//! returning a [`DeletionPlan`]. It is the single most important thing to get
//! right and is exhaustively unit-tested — the `match` has **no** `_ =>` arm, so
//! a new `DeletionPolicy` variant cannot compile until handled (SKILL thesis).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use kube::runtime::controller::Action;
use kube::ResourceExt;

use kopiur_api::{Backup, DeletionPolicy, Origin};

use crate::consts::SKIP_SNAPSHOT_CLEANUP_ANNOTATION;
use crate::context::Context;
use crate::error::{error_policy_for, Error, Result};

/// The decision the deletion handler must execute. Derived purely from the
/// effective `DeletionPolicy` and the object's annotations — no IO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionPlan {
    /// Run `kopia snapshot delete <id>` (via a short Job) then remove the
    /// finalizer. On failure, stay in `phase: Deleting` and back off — the CR
    /// is NOT dropped (ADR §4.5).
    DeleteSnapshot,
    /// Remove the finalizer without contacting the repository (snapshot stays).
    /// Used by `Retain`.
    RetainSnapshot,
    /// Remove the finalizer without contacting the repository, record the
    /// snapshot orphaned, emit `SnapshotOrphaned`, bump the orphan metric. Used
    /// by `Orphan` and by the `skip-snapshot-cleanup` annotation escape hatch.
    OrphanSnapshot,
}

/// Decide what to do on deletion. **Exhaustive** over [`DeletionPolicy`] with no
/// catch-all: a new variant fails to compile until handled here (ADR §5.5).
///
/// The `skip-snapshot-cleanup` annotation is the repo-offline escape hatch and
/// **overrides everything** — even `Delete` — because its entire purpose is "the
/// bucket is gone, just let me remove the CR" (ADR §4.5).
pub fn plan_deletion(
    policy: DeletionPolicy,
    annotations: &BTreeMap<String, String>,
) -> DeletionPlan {
    if annotations.contains_key(SKIP_SNAPSHOT_CLEANUP_ANNOTATION) {
        return DeletionPlan::OrphanSnapshot;
    }
    match policy {
        DeletionPolicy::Delete => DeletionPlan::DeleteSnapshot,
        DeletionPolicy::Retain => DeletionPlan::RetainSnapshot,
        DeletionPolicy::Orphan => DeletionPlan::OrphanSnapshot,
    }
}

/// Compute the effective `DeletionPolicy` for a `Backup`, honoring the
/// origin-aware default (ADR §4.5): discovered backups are forced to `Retain`,
/// produced backups default to `Delete` when unset.
pub fn effective_deletion_policy(
    spec_policy: Option<DeletionPolicy>,
    origin: Origin,
) -> DeletionPolicy {
    match origin {
        // Discovered snapshots are never ours to delete — forced Retain.
        Origin::Discovered => DeletionPolicy::Retain,
        Origin::Scheduled | Origin::Manual => spec_policy.unwrap_or(DeletionPolicy::Delete),
    }
}

/// Resolve a `Backup`'s origin from its status (canonical) or its
/// `kopia.io/origin` label, defaulting to `Manual` when neither is present
/// (a bare `kubectl create`).
pub fn resolve_origin(b: &Backup) -> Origin {
    if let Some(o) = b.status.as_ref().and_then(|s| s.origin) {
        return o;
    }
    match b
        .labels()
        .get(crate::consts::ORIGIN_LABEL)
        .map(String::as_str)
    {
        Some("scheduled") => Origin::Scheduled,
        Some("discovered") => Origin::Discovered,
        _ => Origin::Manual,
    }
}

/// Reconcile a `Backup`.
///
/// IO is intentionally thin here: the decision logic ([`plan_deletion`],
/// [`effective_deletion_policy`], the job builders in [`crate::jobs`]) is pure
/// and unit-tested; this function wires those decisions to the cluster.
#[tracing::instrument(skip(backup, ctx), fields(kind = "Backup", name = %backup.name_any()))]
pub async fn reconcile(backup: Arc<Backup>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&backup, &ctx).await;
    ctx.metrics
        .record_reconcile("Backup", start.elapsed().as_secs_f64());
    result
}

async fn reconcile_inner(backup: &Backup, _ctx: &Context) -> Result<Action> {
    let origin = resolve_origin(backup);
    let _policy = effective_deletion_policy(backup.spec.deletion_policy, origin);

    if backup.metadata.deletion_timestamp.is_some() {
        // Deletion path: compute the plan and (in a full cluster impl) execute
        // its IO, then remove the finalizer. The plan is the tested decision;
        // the cluster wiring (patch finalizer / create delete Job) is the thin
        // part.
        let plan = plan_deletion(_policy, backup.annotations());
        tracing::info!(?plan, "backup deletion plan computed");
        // TODO(M6): execute plan IO against the cluster:
        //  - DeleteSnapshot: create a short SnapshotDelete mover Job; on success
        //    remove the finalizer; on failure set phase=Deleting + condition,
        //    bump kopia_snapshot_deletion_failures_total, and requeue.
        //  - RetainSnapshot: remove the finalizer immediately.
        //  - OrphanSnapshot: emit SnapshotOrphaned event, bump
        //    kopia_orphaned_snapshots_total, remove the finalizer.
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // Normal path: ensure finalizer, create/observe the mover Job, copy status.
    // TODO(M6): patch-add the finalizer if absent; build the work spec from the
    // resolved BackupConfig (jobs::build_config_map / build_job); apply both;
    // watch the owned Job to terminal; copy stats/phase into status; the Job and
    // ConfigMap are reaped by owner-ref GC (§4.10). The construction is exercised
    // by jobs.rs tests; cluster apply is covered by the integration tests.
    Ok(Action::requeue(Duration::from_secs(300)))
}

/// `error_policy` for the `Backup` controller.
pub fn error_policy(_backup: Arc<Backup>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("Backup", err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ann(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // --- plan_deletion: exhaustive over every DeletionPolicy ----------------

    #[test]
    fn delete_policy_plans_snapshot_delete() {
        assert_eq!(
            plan_deletion(DeletionPolicy::Delete, &BTreeMap::new()),
            DeletionPlan::DeleteSnapshot
        );
    }

    #[test]
    fn retain_policy_plans_retain() {
        assert_eq!(
            plan_deletion(DeletionPolicy::Retain, &BTreeMap::new()),
            DeletionPlan::RetainSnapshot
        );
    }

    #[test]
    fn orphan_policy_plans_orphan() {
        assert_eq!(
            plan_deletion(DeletionPolicy::Orphan, &BTreeMap::new()),
            DeletionPlan::OrphanSnapshot
        );
    }

    #[test]
    fn skip_annotation_overrides_delete_to_orphan() {
        // The repo-offline escape hatch: even Delete becomes Orphan so we never
        // contact a dead repository.
        let a = ann(&[(SKIP_SNAPSHOT_CLEANUP_ANNOTATION, "true")]);
        assert_eq!(
            plan_deletion(DeletionPolicy::Delete, &a),
            DeletionPlan::OrphanSnapshot
        );
    }

    #[test]
    fn skip_annotation_overrides_every_policy() {
        let a = ann(&[(SKIP_SNAPSHOT_CLEANUP_ANNOTATION, "")]);
        for p in [
            DeletionPolicy::Delete,
            DeletionPolicy::Retain,
            DeletionPolicy::Orphan,
        ] {
            assert_eq!(plan_deletion(p, &a), DeletionPlan::OrphanSnapshot);
        }
    }

    #[test]
    fn unrelated_annotations_do_not_trigger_skip() {
        let a = ann(&[("kopia.io/other", "x")]);
        assert_eq!(
            plan_deletion(DeletionPolicy::Delete, &a),
            DeletionPlan::DeleteSnapshot
        );
    }

    // --- effective_deletion_policy ------------------------------------------

    #[test]
    fn discovered_is_forced_to_retain_regardless_of_spec() {
        for p in [
            None,
            Some(DeletionPolicy::Delete),
            Some(DeletionPolicy::Orphan),
            Some(DeletionPolicy::Retain),
        ] {
            assert_eq!(
                effective_deletion_policy(p, Origin::Discovered),
                DeletionPolicy::Retain
            );
        }
    }

    #[test]
    fn produced_defaults_to_delete_when_unset() {
        assert_eq!(
            effective_deletion_policy(None, Origin::Scheduled),
            DeletionPolicy::Delete
        );
        assert_eq!(
            effective_deletion_policy(None, Origin::Manual),
            DeletionPolicy::Delete
        );
    }

    #[test]
    fn produced_honors_explicit_spec_policy() {
        assert_eq!(
            effective_deletion_policy(Some(DeletionPolicy::Orphan), Origin::Manual),
            DeletionPolicy::Orphan
        );
        assert_eq!(
            effective_deletion_policy(Some(DeletionPolicy::Retain), Origin::Scheduled),
            DeletionPolicy::Retain
        );
    }
}
