//! The `Snapshot` reconciler — the heart of the ADR §5.5 thesis.
//!
//! Two paths:
//! 1. **Normal reconcile** (produced backups): add the `kopiur.home-operations.com/snapshot-cleanup`
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

use k8s_openapi::api::batch::v1::Job;
use kube::runtime::controller::Action;
use kube::runtime::events::{Event, EventType};
use kube::{Api, Resource, ResourceExt};

use kopiur_api::backend::Backend;
use kopiur_api::common::{NamespaceDeletePolicy, ResolvedIdentity as ApiResolvedIdentity};
use kopiur_api::snapshot::SnapshotPhase;
use kopiur_api::{DeletionPolicy, Origin, Snapshot, SnapshotPolicy};
use kopiur_mover::workspec::{
    MoverOptions, MoverWorkSpec, Operation, RepositoryConnect, ResolvedIdentity as MoverIdentity,
    SnapshotDeleteOp, SnapshotOp, SnapshotPinOp, TargetRef,
};

use crate::config;
use crate::consts::{
    ALLOW_PRIVILEGED_MOVER_ACTION, API_VERSION, CONFIG_LABEL, CREDENTIALS_AVAILABLE_CONDITION,
    CREDENTIALS_PROJECTED_REASON, MISSING_CREDENTIALS_REASON, MOVER_PERMITTED_CONDITION,
    ORIGIN_LABEL, PRIVILEGED_MOVER_NOT_PERMITTED_REASON, SKIP_SNAPSHOT_CLEANUP_ANNOTATION,
    SNAPSHOT_CLEANUP_FINALIZER,
};
use crate::context::Context;
use crate::error::{Error, Result, error_policy_for};
use crate::io::{self, ResolvedRepository};
use crate::jobs::{self, JobLimits, MoverJobInputs, VolumeMountSpec};

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

/// Reshape a per-`Snapshot` deletion plan for the **namespace-deletion** cascade
/// policy (ADR-0005 §5). This is the data-loss-prevention fix: a `kubectl delete ns`
/// must not silently destroy off-site backup history.
///
/// - When the owning namespace is NOT terminating, a single `kubectl delete snapshot`
///   honors the `Snapshot`'s own plan unchanged (`base_plan`).
/// - When the namespace IS terminating, the owning repository's
///   [`NamespaceDeletePolicy`] decides:
///   - `Orphan` (the fail-safe default) → force [`DeletionPlan::OrphanSnapshot`]:
///     remove the finalizer WITHOUT `kopia snapshot delete`, keeping history.
///   - `Delete` → opt-in cascade: fall through to the per-`Snapshot` `base_plan`.
///
/// Pure + exhaustive over [`NamespaceDeletePolicy`] (no `_ =>`), so a new variant
/// cannot compile until handled here (ADR §5.5). The fail-safe path is also taken
/// when the repository can't be resolved (repo already gone) — the caller passes
/// `Orphan` in that case.
pub fn namespace_delete_plan(
    policy: NamespaceDeletePolicy,
    ns_terminating: bool,
    base_plan: DeletionPlan,
) -> DeletionPlan {
    if !ns_terminating {
        return base_plan;
    }
    match policy {
        NamespaceDeletePolicy::Orphan => DeletionPlan::OrphanSnapshot,
        NamespaceDeletePolicy::Delete => base_plan,
    }
}

/// Map a `Snapshot` phase to its kstatus [`io::ReadyOutcome`] (ADR-0005 §2), so
/// `kubectl wait --for=condition=Ready` and Flux/Argo health work uniformly. Pure +
/// exhaustive: a new phase cannot compile until its Ready mapping is decided.
///
/// - `Succeeded`/`Discovered` → `Ready` (the snapshot exists / is catalogued).
/// - `Failed` → `Stalled` (terminal: won't progress without a spec change/retry).
/// - `Pending`/`Running`/`Deleting` → `Reconciling` (in flight).
pub fn snapshot_ready_outcome(phase: SnapshotPhase) -> io::ReadyOutcome {
    match phase {
        SnapshotPhase::Succeeded | SnapshotPhase::Discovered => io::ReadyOutcome::Ready,
        SnapshotPhase::Failed => io::ReadyOutcome::Stalled,
        SnapshotPhase::Pending | SnapshotPhase::Running | SnapshotPhase::Deleting => {
            io::ReadyOutcome::Reconciling
        }
    }
}

/// Build the `(phase, observedGeneration, conditions)` status JSON for a `Snapshot`
/// reaching `phase`, deriving the kstatus Ready/Reconciling/Stalled conditions via
/// [`snapshot_ready_outcome`] + [`io::set_ready`]. Existing conditions (e.g.
/// `CredentialsAvailable`) are preserved by `set_ready`'s upsert.
fn snapshot_ready_status(
    backup: &Snapshot,
    phase: SnapshotPhase,
    reason: &str,
    message: &str,
) -> serde_json::Value {
    use kopiur_api::common::PhaseLabel;
    let existing = backup
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    let generation = backup.meta().generation;
    let conditions = io::set_ready(
        &existing,
        generation,
        snapshot_ready_outcome(phase),
        reason,
        message,
    );
    serde_json::json!({
        "phase": phase.label(),
        "observedGeneration": generation,
        "conditions": conditions,
    })
}

/// Compute the effective `DeletionPolicy` for a `Snapshot`, honoring the
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

/// The kopia-side pin action a `Snapshot` reconcile must take (ADR-0005 §13(c)),
/// derived purely from `spec.pin` (desired) and `status.pinned` (observed). No IO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinAction {
    /// Apply the pin (`kopia snapshot pin --add`): desired `true`, not yet pinned.
    Pin,
    /// Remove the pin (`kopia snapshot pin --remove`): desired `false`, currently pinned.
    Unpin,
    /// Nothing to do — kopia's pin state already matches `spec.pin`.
    NoOp,
}

/// Decide the kopia-side pin action from the desired (`spec.pin`) and observed
/// (`status.pinned`) state. Pure + exhaustive so the decision is unit-tested and a
/// redundant `kopia snapshot pin` is never issued.
///
/// `observed == None` means we've never reconciled the pin: act iff `desired` is
/// `true` (apply it); a never-pinned snapshot with `desired == false` is already in
/// the right state, so `NoOp` (don't spawn an unpin for a pin that was never set).
pub fn pin_decision(desired: bool, observed: Option<bool>) -> PinAction {
    match (desired, observed) {
        (true, Some(true)) => PinAction::NoOp,
        (true, _) => PinAction::Pin,
        (false, Some(true)) => PinAction::Unpin,
        (false, _) => PinAction::NoOp,
    }
}

/// Resolve a `Snapshot`'s origin from its status (canonical) or its
/// `kopiur.home-operations.com/origin` label, defaulting to `Manual` when neither is present
/// (a bare `kubectl create`).
pub fn resolve_origin(b: &Snapshot) -> Origin {
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

/// Reconcile a `Snapshot`.
///
/// IO is intentionally thin here: the decision logic ([`plan_deletion`],
/// [`effective_deletion_policy`], the job builders in [`crate::jobs`]) is pure
/// and unit-tested; this function wires those decisions to the cluster.
#[tracing::instrument(skip(backup, ctx), fields(kind = "Snapshot", namespace = %backup.namespace().unwrap_or_default(), name = %backup.name_any()))]
pub async fn reconcile(backup: Arc<Snapshot>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&backup, &ctx).await;
    ctx.metrics
        .record_reconcile("Snapshot", start.elapsed().as_secs_f64());
    record_backup_status_metrics(&backup, &ctx, result.is_ok()).await;
    result
}

/// Drive the Snapshot's phase + stats gauges. On deletion the phase series is
/// zeroed so `kopiur_resource_phase{...} == 1` alerts clear before the CR is GC'd
/// (OTel sync gauges can't drop a series). Otherwise, on a successful reconcile,
/// the freshest status is re-read — the object handed to `reconcile` is the
/// pre-reconcile watch-cache copy, so reading its status would lag one cycle.
async fn record_backup_status_metrics(backup: &Snapshot, ctx: &Context, ok: bool) {
    let (Some(ns), name) = (backup.namespace(), backup.name_any()) else {
        return;
    };
    if backup.metadata.deletion_timestamp.is_some() {
        ctx.metrics
            .clear_phase::<SnapshotPhase>("Snapshot", &ns, &name);
        return;
    }
    if !ok {
        return;
    }
    let api: Api<Snapshot> = Api::namespaced(ctx.client.clone(), &ns);
    if let Ok(Some(latest)) = api.get_opt(&name).await {
        record_backup_metrics(&latest, ctx);
    }
}

/// Mirror the Snapshot's observed status onto the phase + stats gauges. Idempotent
/// (it `set`s current values), so it is safe to call every reconcile.
fn record_backup_metrics(backup: &Snapshot, ctx: &Context) {
    let (Some(ns), name) = (backup.namespace(), backup.name_any()) else {
        return;
    };
    let Some(status) = backup.status.as_ref() else {
        return;
    };
    if let Some(phase) = status.phase {
        ctx.metrics.set_backup_phase(&ns, &name, phase);
    }
    let size = status.stats.as_ref().and_then(|s| s.size_bytes);
    // Only emit a file count when at least one category is present — otherwise
    // "unknown" would masquerade as a measured zero.
    let files = status.stats.as_ref().and_then(|s| {
        match (s.files_new, s.files_modified, s.files_unchanged) {
            (None, None, None) => None,
            (a, b, c) => Some(a.unwrap_or(0) + b.unwrap_or(0) + c.unwrap_or(0)),
        }
    });
    let duration = status.timing.as_ref().and_then(|t| t.duration_seconds);
    if size.is_some() || files.is_some() || duration.is_some() {
        ctx.metrics
            .set_backup_stats(&ns, &name, size, files, duration);
    }
}

async fn reconcile_inner(backup: &Snapshot, ctx: &Context) -> Result<Action> {
    let origin = resolve_origin(backup);
    let policy = effective_deletion_policy(backup.spec.deletion_policy, origin);
    let namespace = backup
        .namespace()
        .ok_or_else(|| Error::Invariant("Snapshot has no namespace".into()))?;
    let name = backup.name_any();
    let api: Api<Snapshot> = Api::namespaced(ctx.client.clone(), &namespace);

    if backup.metadata.deletion_timestamp.is_some() {
        return handle_deletion(backup, ctx, &api, &namespace, &name, policy).await;
    }

    // Discovered backups are catalog rows, not runs: never spawn a Job. Pin the
    // Discovered phase (with kstatus Ready, ADR-0005 §2) if unset and stop.
    if origin == Origin::Discovered {
        if backup.status.as_ref().and_then(|s| s.phase) != Some(SnapshotPhase::Discovered) {
            let mut status = snapshot_ready_status(
                backup,
                SnapshotPhase::Discovered,
                "Discovered",
                "catalog-materialized snapshot",
            );
            status["origin"] = serde_json::json!("discovered");
            io::patch_status(&api, &name, status).await?;
        }
        return Ok(Action::requeue(Duration::from_secs(600)));
    }

    // Ensure the snapshot-cleanup finalizer before doing any work that creates a
    // snapshot, so a delete during the run still triggers cleanup.
    if io::ensure_finalizer(&api, backup, SNAPSHOT_CLEANUP_FINALIZER).await? {
        // Requeue so the next pass sees the finalizer.
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // If the owned mover Job already reached a terminal state, copy phase/stats
    // into status (controller-as-source-of-truth for phase) and stop running.
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), &namespace);
    if let Some(job) = job_api.get_opt(&name).await? {
        match job_terminal_state(&job) {
            Some(true) => {
                if backup.status.as_ref().and_then(|s| s.phase) != Some(SnapshotPhase::Succeeded) {
                    finalize_succeeded(ctx, backup, &api, &name, &namespace).await?;
                }
                // §13(c): reconcile kopia-side pin state with spec.pin once the
                // snapshot exists. A no-op when already in the desired state.
                return reconcile_pin(backup, ctx, &api, &namespace, &name).await;
            }
            Some(false) => {
                if backup.status.as_ref().and_then(|s| s.phase) != Some(SnapshotPhase::Failed) {
                    io::patch_status(
                        &api,
                        &name,
                        snapshot_ready_status(
                            backup,
                            SnapshotPhase::Failed,
                            "MoverJobFailed",
                            "the backup mover Job failed; see the Job/pod logs",
                        ),
                    )
                    .await?;
                }
                return Ok(Action::requeue(Duration::from_secs(120)));
            }
            None => {
                // Job exists but is still running; mark Running and wait.
                if backup.status.as_ref().and_then(|s| s.phase) != Some(SnapshotPhase::Running) {
                    io::patch_status(
                        &api,
                        &name,
                        snapshot_ready_status(
                            backup,
                            SnapshotPhase::Running,
                            "MoverJobRunning",
                            "the backup mover Job is in flight",
                        ),
                    )
                    .await?;
                }
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
        }
    }

    // No Job yet: resolve the recipe and create the mover Job + ConfigMap.
    let (config, repo) = resolve_recipe(ctx, backup, &namespace).await?;

    // §11: a ReadOnly repository serves restores only — refuse to create a backup
    // Job. Surface a clear condition + Event and stop (not an error: it's a
    // deliberate, terminal-until-spec-change state). Restores remain allowed (the
    // Restore reconciler does not gate on mode).
    if !repo.mode.allows_writes() {
        let conds = backup
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();
        let conditions = io::upsert_condition(
            &conds,
            crate::consts::REPOSITORY_WRITABLE_CONDITION,
            false,
            crate::consts::REPOSITORY_READ_ONLY_REASON,
            &readonly_backup_message(&config.spec.repository.name),
            backup.meta().generation,
        );
        io::patch_status(
            &api,
            &name,
            serde_json::json!({ "phase": "Failed", "conditions": conditions }),
        )
        .await?;
        let _ = ctx
            .recorder
            .publish(
                &Event {
                    type_: EventType::Warning,
                    reason: crate::consts::REPOSITORY_READ_ONLY_REASON.into(),
                    note: Some(readonly_backup_message(&config.spec.repository.name)),
                    action: "RefuseBackupReadOnlyRepository".into(),
                    secondary: None,
                },
                &backup.object_ref(&()),
            )
            .await;
        tracing::warn!(backup = %name, repository = %config.spec.repository.name, "refusing backup: repository is ReadOnly");
        return Ok(Action::await_change());
    }

    let (work_spec, source_volume, repo_volume, _) =
        build_backup_run(backup, &config, &repo, &namespace, &name)?;

    // The mover Job runs in THIS (workload) namespace, where the operator SA does
    // not exist. Mint the least-privilege mover SA + RoleBinding here, then verify
    // the credential Secret(s) the mover loads via envFrom are present — otherwise
    // surface a clear `CredentialsAvailable=False` condition + Warning Event and
    // requeue, instead of launching a Job that hangs (ADR §4.12).
    if let Some(sa) = ctx.mover_service_account.as_deref() {
        io::ensure_mover_rbac(
            &ctx.client,
            &namespace,
            sa,
            &ctx.mover_role_kind,
            &ctx.mover_clusterrole,
        )
        .await?;
    }

    // Resolve the mover's EFFECTIVE security context once: the explicit
    // `securityContext`, or the one inherited from a workload pod via
    // `inheritSecurityContextFrom`. Both the privileged-mover gate and the Job use it,
    // so an inherited root context is gated exactly like an explicit one.
    // The effective container + pod security contexts — explicit, or both inherited
    // from a workload pod via `inheritSecurityContextFrom`.
    let (effective_sc, effective_pod_sc) =
        io::resolve_mover_security_contexts(&ctx.client, &namespace, config.spec.mover.as_ref())
            .await?;
    let privileged_mode = config.spec.mover.as_ref().and_then(|m| m.privileged_mode);

    // Field-wise merge the repository's moverDefaults under the recipe's effective
    // contexts/resources/cache: `hardened ⊂ moverDefaults ⊂ recipe` (ADR-0004 §1/§2).
    // Both the privileged-mover gate below and the Job run on the MERGED result, so an
    // elevation introduced by moverDefaults is gated too, and a partial recipe override
    // can only tighten (never drops the hardened drop:[ALL]/seccomp).
    let resolved_mover = kopiur_api::common::resolve_mover(
        repo.mover_defaults.as_ref(),
        effective_sc.as_ref(),
        effective_pod_sc.as_ref(),
        config
            .spec
            .mover
            .as_ref()
            .and_then(|m| m.resources.as_ref()),
        config.spec.mover.as_ref().and_then(|m| m.cache.as_ref()),
        // Recipe `mover.ttlSecondsAfterFinished` wins over the repo default (§12).
        config
            .spec
            .mover
            .as_ref()
            .and_then(|m| m.ttl_seconds_after_finished),
    );

    // Privileged-mover gate (ADR §4.11/§G16, VolSync-parity): an elevated mover
    // (root/privileged/added caps/`privilegedMode`, container- OR pod-level) requires
    // the workload namespace to opt in via the
    // `kopiur.home-operations.com/privileged-movers` annotation — a tenant there could
    // otherwise reuse the minted mover SA at that privilege. Refuse with a clear
    // `MoverPermitted=False` condition + Event otherwise.
    if kopiur_api::common::requires_privilege_resolved(
        Some(&resolved_mover.security_context),
        resolved_mover.pod_security_context.as_ref(),
        privileged_mode,
    ) && !io::namespace_allows_privileged_movers(&ctx.client, &namespace).await?
    {
        let sa = ctx
            .mover_service_account
            .as_deref()
            .unwrap_or(config::DEFAULT_MOVER_NAME);
        let msg =
            io::privileged_mover_message("SnapshotPolicy", &config.name_any(), &namespace, sa);
        let existing = backup
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();
        let conditions = io::upsert_condition(
            &existing,
            MOVER_PERMITTED_CONDITION,
            false,
            PRIVILEGED_MOVER_NOT_PERMITTED_REASON,
            &msg,
            backup.meta().generation,
        );
        io::patch_status(
            &api,
            &name,
            serde_json::json!({ "phase": "Pending", "conditions": conditions }),
        )
        .await?;
        io::publish_warning_event(
            ctx,
            backup,
            PRIVILEGED_MOVER_NOT_PERMITTED_REASON,
            ALLOW_PRIVILEGED_MOVER_ACTION,
            &msg,
        )
        .await;
        // The missing dependency is the namespace opt-in annotation an admin adds
        // out-of-band (like a missing creds Secret) — Transient, NOT Structural, so
        // it is re-checked on the short transient cadence and the opt-in takes
        // effect within ~30s instead of a 5-minute structural backoff. (A namespace
        // annotation does not enqueue this Snapshot, so the requeue is what picks it
        // up.) Mirrors the `CredentialsAvailable=False` gate above.
        return Err(Error::MissingDependency(msg));
    }
    // Permitted: clear any stale `MoverPermitted=False` from a prior reconcile.
    if let Some(conds) = backup.status.as_ref().map(|s| s.conditions.as_slice())
        && conds
            .iter()
            .any(|c| c.type_ == MOVER_PERMITTED_CONDITION && c.status != "True")
    {
        let conditions = io::upsert_condition(
            conds,
            MOVER_PERMITTED_CONDITION,
            true,
            "Permitted",
            "the mover is permitted in this namespace",
            backup.meta().generation,
        );
        io::patch_status(&api, &name, serde_json::json!({ "conditions": conditions })).await?;
    }

    let owner = io::owner_ref_for(backup, "Snapshot")?;
    // Resolve the credential Secret names the mover loads via envFrom. With
    // `spec.credentialProjection` enabled, the operator copies the repository's
    // Secret(s) into THIS namespace (owned by the Snapshot, GC'd with it) and returns
    // the projected names; otherwise it verifies the user-managed Secret(s) are
    // already present here. Either way a problem surfaces as a clear
    // `CredentialsAvailable=False` condition + Warning Event before we launch a Job
    // that would hang on a missing-Secret envFrom (ADR §4.12).
    let creds = match io::resolve_mover_creds_for(
        &ctx.client,
        &namespace,
        &name,
        &owner,
        &repo,
        config
            .spec
            .credential_projection
            .as_ref()
            .is_some_and(|p| p.enabled),
        io::repo_kind_str(config.spec.repository.kind),
        &config.spec.repository.name,
    )
    .await
    {
        Ok(c) => c,
        Err(Error::MissingDependency(msg)) => {
            let existing = backup
                .status
                .as_ref()
                .map(|s| s.conditions.clone())
                .unwrap_or_default();
            let conditions = io::upsert_condition(
                &existing,
                CREDENTIALS_AVAILABLE_CONDITION,
                false,
                MISSING_CREDENTIALS_REASON,
                &msg,
                backup.meta().generation,
            );
            io::patch_status(
                &api,
                &name,
                serde_json::json!({ "phase": "Pending", "conditions": conditions }),
            )
            .await?;
            io::publish_missing_creds_event(ctx, backup, &msg).await;
            return Err(Error::MissingDependency(msg));
        }
        Err(e) => return Err(e),
    };
    if creds.projected > 0 {
        ctx.metrics
            .inc_secrets_projected(&namespace, creds.projected);
    }
    // Creds are present (or were just projected): clear any stale
    // `CredentialsAvailable=False` from a prior reconcile so a fixed problem stops
    // showing on the object.
    if let Some(conds) = backup.status.as_ref().map(|s| s.conditions.as_slice())
        && conds
            .iter()
            .any(|c| c.type_ == CREDENTIALS_AVAILABLE_CONDITION && c.status != "True")
    {
        let (reason, note) = if creds.projected > 0 {
            (
                CREDENTIALS_PROJECTED_REASON,
                "credential Secret(s) projected into the mover namespace",
            )
        } else {
            (
                "Available",
                "credentials Secret(s) present in the mover namespace",
            )
        };
        let conditions = io::upsert_condition(
            conds,
            CREDENTIALS_AVAILABLE_CONDITION,
            true,
            reason,
            note,
            backup.meta().generation,
        );
        io::patch_status(&api, &name, serde_json::json!({ "conditions": conditions })).await?;
    }
    let creds_secrets = creds.names;

    let labels = run_labels(&config, origin);
    let mut limits = job_limits(backup);
    // moverDefaults.ttlSecondsAfterFinished applies unless the recipe's FailurePolicy
    // already set a TTL (ADR-0005 §12).
    if limits.ttl_seconds_after_finished.is_none() {
        limits.ttl_seconds_after_finished = resolved_mover.ttl_seconds_after_finished;
    }
    // Resolve the cache VOLUME (emptyDir / sized-ephemeral / persistent PVC). A
    // persistent cache PVC is owned by the SnapshotPolicy so a warm cache survives
    // across individual Snapshot runs (ADR §3.1).
    let cache_volume = crate::cache::resolve_cache_volume(
        &ctx.client,
        &namespace,
        io::owner_ref_for(&config, "SnapshotPolicy")?,
        &format!("kopiur-cache-{}", config.name_any()),
        crate::cache::effective_cache(
            &repo,
            config.spec.mover.as_ref().and_then(|m| m.cache.as_ref()),
        )
        .as_ref(),
    )
    .await?;
    let inputs = MoverJobInputs {
        name: &name,
        namespace: &namespace,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: mover_pull_policy(),
        limits,
        resources: resolved_mover.resources.clone(),
        // The fully-merged contexts (hardened ⊂ moverDefaults ⊂ recipe) — the same
        // values the privileged gate above ran on.
        security_context: resolved_mover.security_context.clone(),
        pod_security_context: resolved_mover.pod_security_context.clone(),
        node_selector: resolved_mover.node_selector.clone(),
        tolerations: resolved_mover.tolerations.clone(),
        affinity: resolved_mover.affinity.clone(),
        labels,
        source_volume,
        repo_volume,
        creds_secrets,
        result_configmap: None,
        service_account: ctx.mover_service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
        annotations: Default::default(),
        cache_volume,
    };
    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, &namespace, &name, &cm, &job).await?;

    io::patch_status(
        &api,
        &name,
        serde_json::json!({ "phase": "Running", "origin": origin_str(origin) }),
    )
    .await?;
    tracing::info!(backup = %name, "created mover Job for backup");

    Ok(Action::requeue(Duration::from_secs(30)))
}

/// Execute the deletion plan (the tested [`plan_deletion`] decision) against the
/// cluster, then remove the finalizer when cleanup completes.
async fn handle_deletion(
    backup: &Snapshot,
    ctx: &Context,
    api: &Api<Snapshot>,
    namespace: &str,
    name: &str,
    policy: DeletionPolicy,
) -> Result<Action> {
    // Nothing to clean up if our finalizer isn't present.
    if !backup
        .finalizers()
        .iter()
        .any(|f| f == SNAPSHOT_CLEANUP_FINALIZER)
    {
        return Ok(Action::await_change());
    }

    let base_plan = plan_deletion(policy, backup.annotations());

    // Namespace-deletion cascade (ADR-0005 §5): if the owning namespace is being torn
    // down, the repository's `onNamespaceDelete` decides. Default `Orphan` keeps
    // off-site history (a `kubectl delete ns` must not be a data-loss event); only an
    // explicit `Delete` cascades to the per-Snapshot plan. Resolving the repo policy
    // needs the recipe; if it can't be resolved (repo/recipe already gone), fail safe
    // to `Orphan`. A non-terminating namespace leaves `base_plan` unchanged, so a lone
    // `kubectl delete snapshot` still honors the Snapshot's own deletionPolicy.
    let plan = match io::namespace_is_terminating(&ctx.client, namespace).await {
        Ok(false) => base_plan,
        Ok(true) => {
            let ns_policy = resolve_recipe(ctx, backup, namespace)
                .await
                .map(|(_, repo)| repo.on_namespace_delete)
                .unwrap_or(NamespaceDeletePolicy::Orphan);
            namespace_delete_plan(ns_policy, true, base_plan)
        }
        // Transient read error: don't guess. Fall back to the per-Snapshot plan so a
        // single delete still works; the namespace-cascade case re-evaluates on the
        // next pass once the read succeeds.
        Err(_) => base_plan,
    };
    tracing::info!(?plan, backup = %name, "executing backup deletion plan");

    match plan {
        DeletionPlan::DeleteSnapshot => {
            let snapshot_id = backup
                .status
                .as_ref()
                .and_then(|s| s.snapshot.as_ref())
                .map(|s| s.kopia_snapshot_id.clone());
            match snapshot_id {
                // No snapshot was ever recorded: nothing to delete in the repo.
                None => {
                    io::remove_finalizer(api, backup, SNAPSHOT_CLEANUP_FINALIZER).await?;
                    Ok(Action::await_change())
                }
                Some(id) => delete_snapshot_via_job(backup, ctx, api, namespace, name, &id).await,
            }
        }
        DeletionPlan::RetainSnapshot => {
            io::remove_finalizer(api, backup, SNAPSHOT_CLEANUP_FINALIZER).await?;
            Ok(Action::await_change())
        }
        DeletionPlan::OrphanSnapshot => {
            ctx.metrics.inc_orphaned_snapshot(namespace);
            let _ = ctx
                .recorder
                .publish(
                    &Event {
                        type_: EventType::Normal,
                        reason: "SnapshotOrphaned".into(),
                        note: Some(format!(
                            "snapshot for backup {name} orphaned (policy/escape-hatch); finalizer removed without contacting the repository"
                        )),
                        action: "Orphan".into(),
                        secondary: None,
                    },
                    &backup.object_ref(&()),
                )
                .await;
            io::remove_finalizer(api, backup, SNAPSHOT_CLEANUP_FINALIZER).await?;
            Ok(Action::await_change())
        }
    }
}

/// Drive a SnapshotDelete mover Job for the deletion path. Creates the Job if
/// absent; on terminal success removes the finalizer; on failure records a
/// Deleting phase, bumps the failure metric, and requeues.
async fn delete_snapshot_via_job(
    backup: &Snapshot,
    ctx: &Context,
    api: &Api<Snapshot>,
    namespace: &str,
    name: &str,
    snapshot_id: &str,
) -> Result<Action> {
    let job_name = format!("{name}-delete");
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);

    if let Some(job) = job_api.get_opt(&job_name).await? {
        match job_terminal_state(&job) {
            Some(true) => {
                io::remove_finalizer(api, backup, SNAPSHOT_CLEANUP_FINALIZER).await?;
                tracing::info!(backup = %name, %snapshot_id, "snapshot deleted; finalizer removed");
                return Ok(Action::await_change());
            }
            Some(false) => {
                ctx.metrics.inc_snapshot_deletion_failure(namespace);
                io::patch_status(api, name, serde_json::json!({ "phase": "Deleting" })).await?;
                tracing::warn!(backup = %name, "snapshot delete Job failed; backing off");
                return Ok(Action::requeue(Duration::from_secs(60)));
            }
            None => return Ok(Action::requeue(Duration::from_secs(15))),
        }
    }

    // Create the SnapshotDelete Job. We need the recipe to know how to connect
    // and authenticate to the repository.
    let (config, repo) = resolve_recipe(ctx, backup, namespace).await?;
    let identity = resolve_identity_for(&config, namespace, repo.identity_defaults.as_ref())?;
    let owner = io::owner_ref_for(backup, "Snapshot")?;
    // Resolve (and, when `spec.credentialProjection` is enabled, project) the mover's
    // credential Secret(s) into this namespace before building the Job. Errors
    // propagate as MissingDependency (Transient) — this is the delete path, so we
    // requeue rather than surface a CredentialsAvailable condition.
    let creds = io::resolve_mover_creds_for(
        &ctx.client,
        namespace,
        &job_name,
        &owner,
        &repo,
        config
            .spec
            .credential_projection
            .as_ref()
            .is_some_and(|p| p.enabled),
        io::repo_kind_str(config.spec.repository.kind),
        &config.spec.repository.name,
    )
    .await?;
    if creds.projected > 0 {
        ctx.metrics
            .inc_secrets_projected(namespace, creds.projected);
    }
    let creds_secrets = creds.names;
    let work_spec = MoverWorkSpec {
        version: 1,
        operation: Operation::SnapshotDelete(SnapshotDeleteOp {
            snapshot_id: snapshot_id.to_string(),
        }),
        identity,
        repository: repository_connect(&repo)?,
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "Snapshot".to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
        // A one-shot finalizer delete: kopia's default cache is fine.
        cache: Default::default(),
        throttle: Default::default(),
    };

    let mut labels = run_labels(&config, resolve_origin(backup));
    labels.insert(
        "kopiur.home-operations.com/op".to_string(),
        "snapshot-delete".to_string(),
    );
    let repo_volume =
        io::filesystem_repo_mount_source(&repo.backend).map(|source| VolumeMountSpec {
            source,
            mount_path: io::filesystem_repo_path(&repo.backend).unwrap_or_default(),
            read_only: false,
        });
    // The finalizer delete-Job has no recipe `mover`, but still inherits the
    // repository's moverDefaults (security context, placement) so it can reach a
    // filesystem/NFS repo on a non-65532-owned directory (ADR-0004 §1).
    let resolved_mover = kopiur_api::common::resolve_mover(
        repo.mover_defaults.as_ref(),
        None,
        None,
        None,
        None,
        None,
    );
    let limits = JobLimits {
        ttl_seconds_after_finished: resolved_mover.ttl_seconds_after_finished,
        ..JobLimits::default()
    };
    let inputs = MoverJobInputs {
        name: &job_name,
        namespace,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: mover_pull_policy(),
        limits,
        resources: resolved_mover.resources.clone(),
        security_context: resolved_mover.security_context.clone(),
        pod_security_context: resolved_mover.pod_security_context.clone(),
        node_selector: resolved_mover.node_selector.clone(),
        tolerations: resolved_mover.tolerations.clone(),
        affinity: resolved_mover.affinity.clone(),
        labels,
        source_volume: None,
        repo_volume,
        creds_secrets,
        result_configmap: None,
        service_account: ctx.mover_service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
        annotations: Default::default(),
        // A one-shot finalizer delete: an ephemeral emptyDir cache is fine.
        cache_volume: Default::default(),
    };
    // The SnapshotDelete Job runs in this namespace too: mint the mover SA before
    // launching it (its credential Secret(s) were resolved/projected above).
    if let Some(sa) = ctx.mover_service_account.as_deref() {
        io::ensure_mover_rbac(
            &ctx.client,
            namespace,
            sa,
            &ctx.mover_role_kind,
            &ctx.mover_clusterrole,
        )
        .await?;
    }
    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, namespace, &job_name, &cm, &job).await?;
    io::patch_status(api, name, serde_json::json!({ "phase": "Deleting" })).await?;
    tracing::info!(backup = %name, %snapshot_id, "created SnapshotDelete Job");
    Ok(Action::requeue(Duration::from_secs(15)))
}

/// Reconcile kopia's snapshot-pin state with `Snapshot.spec.pin` (ADR-0005 §13(c)),
/// after the snapshot exists. Issues a `SnapshotPin` mover Job only when the desired
/// (`spec.pin`) and observed (`status.pinned`) state differ (the tested
/// [`pin_decision`]); on the Job's terminal success it records the new observed pin
/// state in `status.pinned`. A `NoOp` (or a not-yet-recorded snapshot id) just
/// returns the standard succeeded-snapshot requeue.
async fn reconcile_pin(
    backup: &Snapshot,
    ctx: &Context,
    api: &Api<Snapshot>,
    namespace: &str,
    name: &str,
) -> Result<Action> {
    let desired = backup.spec.pin;
    let observed = backup.status.as_ref().and_then(|s| s.pinned);
    let steady = Action::requeue(Duration::from_secs(600));
    let action = pin_decision(desired, observed);
    if action == PinAction::NoOp {
        return Ok(steady);
    }
    // Need the kopia snapshot id to pin/unpin; it's recorded once Succeeded.
    let Some(snapshot_id) = backup
        .status
        .as_ref()
        .and_then(|s| s.snapshot.as_ref())
        .map(|s| s.kopia_snapshot_id.clone())
    else {
        // Not resolved yet (e.g. object-store mover hasn't PATCHed the id) — retry.
        return Ok(Action::requeue(Duration::from_secs(30)));
    };

    let job_name = format!("{name}-pin");
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);
    if let Some(job) = job_api.get_opt(&job_name).await? {
        match job_terminal_state(&job) {
            Some(true) => {
                // Record the new observed pin state so the next reconcile is a NoOp.
                io::patch_status(api, name, serde_json::json!({ "pinned": desired })).await?;
                tracing::info!(backup = %name, %snapshot_id, pin = desired, "snapshot pin reconciled");
                return Ok(steady);
            }
            Some(false) => {
                tracing::warn!(backup = %name, "snapshot pin Job failed; backing off");
                return Ok(Action::requeue(Duration::from_secs(120)));
            }
            None => return Ok(Action::requeue(Duration::from_secs(15))),
        }
    }

    // Create the SnapshotPin Job (mirrors the SnapshotDelete one-shot path).
    let (config, repo) = resolve_recipe(ctx, backup, namespace).await?;
    let identity = resolve_identity_for(&config, namespace, repo.identity_defaults.as_ref())?;
    let owner = io::owner_ref_for(backup, "Snapshot")?;
    let creds = io::resolve_mover_creds_for(
        &ctx.client,
        namespace,
        &job_name,
        &owner,
        &repo,
        config
            .spec
            .credential_projection
            .as_ref()
            .is_some_and(|p| p.enabled),
        io::repo_kind_str(config.spec.repository.kind),
        &config.spec.repository.name,
    )
    .await?;
    if creds.projected > 0 {
        ctx.metrics
            .inc_secrets_projected(namespace, creds.projected);
    }
    let creds_secrets = creds.names;
    let work_spec = MoverWorkSpec {
        version: 1,
        operation: Operation::SnapshotPin(SnapshotPinOp {
            snapshot_id: snapshot_id.clone(),
            pin: matches!(action, PinAction::Pin),
        }),
        identity,
        repository: repository_connect(&repo)?,
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "Snapshot".to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
        cache: Default::default(),
        throttle: Default::default(),
    };
    let mut labels = run_labels(&config, resolve_origin(backup));
    labels.insert(
        "kopiur.home-operations.com/op".to_string(),
        "snapshot-pin".to_string(),
    );
    let repo_volume =
        io::filesystem_repo_mount_source(&repo.backend).map(|source| VolumeMountSpec {
            source,
            mount_path: io::filesystem_repo_path(&repo.backend).unwrap_or_default(),
            read_only: false,
        });
    let resolved_mover = kopiur_api::common::resolve_mover(
        repo.mover_defaults.as_ref(),
        None,
        None,
        None,
        None,
        None,
    );
    let limits = JobLimits {
        ttl_seconds_after_finished: resolved_mover.ttl_seconds_after_finished,
        ..JobLimits::default()
    };
    let inputs = MoverJobInputs {
        name: &job_name,
        namespace,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: mover_pull_policy(),
        limits,
        resources: resolved_mover.resources.clone(),
        security_context: resolved_mover.security_context.clone(),
        pod_security_context: resolved_mover.pod_security_context.clone(),
        node_selector: resolved_mover.node_selector.clone(),
        tolerations: resolved_mover.tolerations.clone(),
        affinity: resolved_mover.affinity.clone(),
        labels,
        source_volume: None,
        repo_volume,
        creds_secrets,
        result_configmap: None,
        service_account: ctx.mover_service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
        annotations: Default::default(),
        cache_volume: Default::default(),
    };
    if let Some(sa) = ctx.mover_service_account.as_deref() {
        io::ensure_mover_rbac(
            &ctx.client,
            namespace,
            sa,
            &ctx.mover_role_kind,
            &ctx.mover_clusterrole,
        )
        .await?;
    }
    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, namespace, &job_name, &cm, &job).await?;
    tracing::info!(backup = %name, %snapshot_id, ?action, "created SnapshotPin Job");
    Ok(Action::requeue(Duration::from_secs(15)))
}

/// On a Job's terminal success, pin phase=Succeeded and the resulting kopia
/// snapshot id/identity into status. The controller is the authoritative source
/// of the terminal phase AND (for the filesystem backend) of the snapshot id: it
/// resolves the newest snapshot for the run's identity in-process, so status is
/// complete even when the in-cluster mover cannot PATCH back (best-effort path).
/// The mover still PATCHes stats when it can reach the API server.
async fn finalize_succeeded(
    ctx: &Context,
    backup: &Snapshot,
    api: &Api<Snapshot>,
    name: &str,
    namespace: &str,
) -> Result<()> {
    // Try to resolve the snapshot id authoritatively for the filesystem backend.
    let snapshot = resolve_succeeded_snapshot(ctx, backup, namespace).await;
    // Base status carries the kstatus Ready conditions (ADR-0005 §2) so
    // `kubectl wait --for=condition=Ready` works on a Succeeded Snapshot.
    let mut status = snapshot_ready_status(
        backup,
        SnapshotPhase::Succeeded,
        "SnapshotCreated",
        "the kopia snapshot was created successfully",
    );
    if let Ok(Some((id, identity))) = snapshot {
        status["snapshot"] = serde_json::json!({
            "kopiaSnapshotID": id,
            "identity": identity,
        });
    }
    io::patch_status(api, name, status).await?;
    ctx.metrics
        .set_backup_last_success(namespace, name, chrono::Utc::now().timestamp());
    tracing::info!(backup = %name, "backup Job succeeded; phase=Succeeded");
    Ok(())
}

/// Resolve the newest snapshot matching this backup's identity for the
/// filesystem backend (in-process, ADR §5.4). Returns the snapshot id and a
/// status `identity` JSON body, or `None` when not resolvable in-process.
async fn resolve_succeeded_snapshot(
    ctx: &Context,
    backup: &Snapshot,
    namespace: &str,
) -> Result<Option<(String, serde_json::Value)>> {
    let (config, repo) = resolve_recipe(ctx, backup, namespace).await?;
    let identity = resolve_identity_for(&config, namespace, repo.identity_defaults.as_ref())?;
    match &repo.backend {
        Backend::Filesystem(fs) => {
            let creds = io::repo_credentials(&repo.encryption);
            let password = io::read_repo_password(&ctx.client, namespace, &creds).await?;
            let client = ctx.kopia.build([("KOPIA_PASSWORD".to_string(), password)]);
            client
                .repository_connect(
                    &kopiur_kopia::ConnectSpec::Filesystem {
                        path: fs.path.clone().into(),
                    },
                    kopiur_kopia::CacheTuning::default(),
                )
                .await?;
            // Match the snapshot by its source path (the path we snapshotted),
            // newest first. The pod's recorded user/host differ from our
            // resolved identity (a documented mover-identity follow-up), so we
            // key on the source path which IS authoritative.
            let mut list = client.snapshot_list(None).await?;
            list.sort_by_key(|e| std::cmp::Reverse(e.end_time));
            let matched = list
                .into_iter()
                .find(|e| e.source.path == identity.source_path);
            Ok(matched.map(|e| {
                let id = e.id.clone();
                let body = serde_json::json!({
                    "username": e.source.user_name,
                    "hostname": e.source.host,
                    "sourcePath": e.source.path,
                });
                (id, body)
            }))
        }
        _ => Ok(None),
    }
}

/// Resolve a `Snapshot`'s referenced `SnapshotPolicy` and that config's
/// `Repository`. Cluster references and non-filesystem backends still resolve
/// here; backend-specific behavior is decided downstream.
async fn resolve_recipe(
    ctx: &Context,
    backup: &Snapshot,
    namespace: &str,
) -> Result<(SnapshotPolicy, ResolvedRepository)> {
    let policy_ref = backup
        .spec
        .policy_ref
        .as_ref()
        .ok_or_else(|| Error::Invariant("produced Snapshot has no policyRef".into()))?;
    let cfg_ns = policy_ref.namespace.as_deref().unwrap_or(namespace);
    let cfg_api: Api<SnapshotPolicy> = Api::namespaced(ctx.client.clone(), cfg_ns);
    let config = cfg_api.get_opt(&policy_ref.name).await?.ok_or_else(|| {
        Error::MissingDependency(format!("SnapshotPolicy {cfg_ns}/{}", policy_ref.name))
    })?;

    // Honor `repository.kind`: namespaced `Repository` (cross-ns via
    // `ref.namespace`, defaulting to the config's namespace) vs. cluster-scoped
    // `ClusterRepository` (`Api::all`). The discriminated kind is matched
    // exhaustively in the resolver (ADR §5.5).
    let repo = io::resolve_repository_ref(&ctx.client, &config.spec.repository, cfg_ns).await?;
    Ok((config, repo))
}

/// Build everything a backup run needs: the work spec, the source volume mount
/// (PVC or inline NFS), the repo volume mount (filesystem only), and the
/// credentials Secret name.
type SnapshotRun<'a> = (
    MoverWorkSpec,
    Option<VolumeMountSpec>,
    Option<VolumeMountSpec>,
    Vec<String>,
);
fn build_backup_run(
    _backup: &Snapshot,
    config: &SnapshotPolicy,
    repo: &ResolvedRepository,
    namespace: &str,
    _name: &str,
) -> Result<SnapshotRun<'static>> {
    let identity = resolve_identity_for(config, namespace, repo.identity_defaults.as_ref())?;

    // First source's volume + path drive the mount and the snapshot source path.
    let source = config
        .spec
        .sources
        .first()
        .ok_or_else(|| Error::Invariant("SnapshotPolicy has no sources".into()))?;

    // The mover snapshots whatever is mounted at `source_path`, so the mount path
    // and the kopia source path are the same. PVC: `/pvc/<name>` by default; NFS:
    // the export path by default; either overridable by `sourcePathOverride`.
    let (source_path, source_volume) = match (&source.pvc, &source.nfs) {
        (Some(pvc), None) => {
            let path = source
                .source_path_override
                .clone()
                .unwrap_or_else(|| format!("/pvc/{}", pvc.name));
            (
                path.clone(),
                VolumeMountSpec::pvc(pvc.name.clone(), path, true),
            )
        }
        (None, Some(nfs)) => {
            // The export's server-side path (`nfs.path`) is what the volume is
            // mounted FROM; it is NOT necessarily a valid in-container mount
            // path. An NFSv4 pseudo-root export ("/") must not be mounted at "/"
            // in the container — that mounts over the rootfs and the pod fails to
            // start. Remap a "/" target to a safe path; kopia snapshots there.
            let requested = source
                .source_path_override
                .clone()
                .unwrap_or_else(|| nfs.path.clone());
            let mount_path = if requested == "/" {
                crate::consts::NFS_SOURCE_MOUNT_PATH.to_string()
            } else {
                requested
            };
            (
                mount_path.clone(),
                VolumeMountSpec::nfs(nfs.server.clone(), nfs.path.clone(), mount_path, true),
            )
        }
        // `pvcSelector` (multi-PVC) and the mutually-exclusive cases are rejected
        // at admission by `validate_source`; the single-source mover path requires
        // an explicit `pvc` or `nfs`.
        _ => {
            return Err(Error::Invariant(
                "backup mover path requires exactly one of source.pvc or source.nfs".into(),
            ));
        }
    };

    let creds_secrets = io::mover_creds_secrets(&repo.backend, &repo.encryption);

    // Effective kopia cache budgets: the repository's cacheDefaults overlaid by this
    // recipe's mover.cache (ADR §3.1).
    let cache = crate::cache::cache_tuning(
        crate::cache::effective_cache(
            repo,
            config.spec.mover.as_ref().and_then(|m| m.cache.as_ref()),
        )
        .as_ref(),
    );
    let work_spec = MoverWorkSpec {
        version: 1,
        operation: Operation::Snapshot(SnapshotOp {
            source_path: source_path.clone(),
            tags: tags_for(config),
            // Flattened SnapshotPolicy policy knobs → kopia `policy set` flags
            // (compression / files / errorHandling / upload / extraArgs). Wired so
            // these never stay inert (ADR-0005 §13(b)/§13(f), ADR-0004 §4b).
            policy: policy_args_for(config),
        }),
        identity,
        repository: repository_connect(repo)?,
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "Snapshot".to_string(),
            name: _name.to_string(),
            namespace: namespace.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
        cache,
        // Repository-wide throttle (moverDefaults.throttle, ADR-0005 §13(e)).
        throttle: crate::io::throttle_spec(repo.mover_defaults.as_ref()),
    };

    let source_volume = Some(source_volume);
    let repo_volume =
        io::filesystem_repo_mount_source(&repo.backend).map(|source| VolumeMountSpec {
            source,
            mount_path: io::filesystem_repo_path(&repo.backend).unwrap_or_default(),
            read_only: false,
        });

    Ok((work_spec, source_volume, repo_volume, creds_secrets))
}

/// Resolve identity from a `SnapshotPolicy` (overrides + the repository's CEL
/// `identityDefaults`) into the mover wire identity. Reuses
/// `api::identity::resolve_identity` (the tested kernel). `defaults` is the
/// `ClusterRepository`'s `identityDefaults` (ADR-0004 §5), `None` for a namespaced
/// `Repository`.
fn resolve_identity_for(
    config: &SnapshotPolicy,
    namespace: &str,
    defaults: Option<&kopiur_api::IdentityDefaults>,
) -> Result<MoverIdentity> {
    let first = config.spec.sources.first();
    let pvc_name = first.and_then(|s| s.pvc.as_ref().map(|p| p.name.clone()));
    // A non-PVC NFS source supplies the sourcePath default (the export path).
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
    let resolved: ApiResolvedIdentity =
        kopiur_api::resolve_identity(&inputs).map_err(|e| Error::Validation(e.to_string()))?;
    Ok(MoverIdentity {
        username: resolved.username,
        hostname: resolved.hostname,
        source_path: resolved.source_path.unwrap_or_else(|| "/data".to_string()),
    })
}

/// Public wrapper so the restore reconciler can reuse the backend mapping.
pub(crate) fn repository_connect_pub(repo: &ResolvedRepository) -> Result<RepositoryConnect> {
    repository_connect(repo)
}

/// Public wrapper for the mover image pull policy (reused by restore).
pub(crate) fn mover_pull_policy_pub() -> Option<&'static str> {
    mover_pull_policy()
}

/// Map a `Repository`'s backend to the mover's `RepositoryConnect`.
///
/// Exhaustive over every CRD `Backend` variant — a new backend cannot compile
/// until it is wired through to the mover. Credentials never appear here; they
/// flow to the mover Job as env vars from the referenced Secret (ADR §4.10).
fn repository_connect(repo: &ResolvedRepository) -> Result<RepositoryConnect> {
    Ok(backend_to_repository_connect(&repo.backend))
}

/// Pure `Backend -> RepositoryConnect` translation (no kube types), so it is
/// unit-testable and shared by the backup and restore reconcilers.
pub(crate) fn backend_to_repository_connect(backend: &Backend) -> RepositoryConnect {
    match backend {
        Backend::Filesystem(f) => RepositoryConnect::Filesystem {
            path: f.path.clone(),
        },
        Backend::S3(s) => RepositoryConnect::S3 {
            bucket: s.bucket.clone(),
            endpoint: s.endpoint.clone(),
            prefix: s.prefix.clone(),
            region: s.region.clone(),
            disable_tls: s.tls.as_ref().map(|t| t.disable_tls).unwrap_or(false),
            disable_tls_verification: s
                .tls
                .as_ref()
                .map(|t| t.insecure_skip_verify)
                .unwrap_or(false),
        },
        Backend::Azure(a) => RepositoryConnect::Azure {
            container: a.container.clone(),
            storage_account: a.storage_account.clone(),
            prefix: a.prefix.clone(),
        },
        Backend::Gcs(g) => RepositoryConnect::Gcs {
            bucket: g.bucket.clone(),
            prefix: g.prefix.clone(),
        },
        Backend::B2(b) => RepositoryConnect::B2 {
            bucket: b.bucket.clone(),
            prefix: b.prefix.clone(),
        },
        Backend::Sftp(s) => RepositoryConnect::Sftp {
            host: s.host.clone(),
            path: s.path.clone(),
            port: s.port,
            username: s.username.clone(),
            keyfile: None,
        },
        Backend::WebDav(w) => RepositoryConnect::WebDav { url: w.url.clone() },
        Backend::Rclone(r) => RepositoryConnect::Rclone {
            remote_path: r.remote_path.clone(),
        },
    }
}

/// Actionable message for a backup refused because its repository is `ReadOnly`
/// (§11): what / why / how-to-fix. Pure so the text is unit-asserted.
fn readonly_backup_message(repo_name: &str) -> String {
    format!(
        "refusing to create a backup: repository `{repo_name}` is `mode: ReadOnly` (ADR-0005 §11), \
         which serves restores only. Set the repository's `spec.mode: ReadWrite` to allow backups, \
         or target a different repository."
    )
}

/// Snapshot tags from the config + run metadata.
fn tags_for(config: &SnapshotPolicy) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    tags.insert("kopiur:config".to_string(), config.name_any());
    tags
}

/// Resolve the kopia `policy set` knobs the mover applies before snapshotting
/// (compression / files / errorHandling / upload / extraArgs). Delegates to the
/// single tested mapping so these flattened `SnapshotPolicy` fields are never
/// inert (ADR-0005 §13(b)/§13(f), ADR-0004 §4b).
fn policy_args_for(config: &SnapshotPolicy) -> kopiur_mover::workspec::PolicyArgsSpec {
    kopiur_mover::workspec::PolicyArgsSpec::from_policy(&config.spec)
}

/// Labels applied to the mover Job/ConfigMap and any child objects.
fn run_labels(config: &SnapshotPolicy, origin: Origin) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(ORIGIN_LABEL.to_string(), origin_str(origin).to_string());
    labels.insert(CONFIG_LABEL.to_string(), config.name_any());
    labels
}

fn origin_str(origin: Origin) -> &'static str {
    match origin {
        Origin::Scheduled => "scheduled",
        Origin::Manual => "manual",
        Origin::Discovered => "discovered",
    }
}

/// Job limits from the backup's `failurePolicy`, falling back to ADR defaults.
fn job_limits(backup: &Snapshot) -> JobLimits {
    match &backup.spec.failure_policy {
        Some(fp) => JobLimits {
            backoff_limit: fp.backoff_limit.unwrap_or(2),
            active_deadline_seconds: fp.active_deadline_seconds,
            ..JobLimits::default()
        },
        None => JobLimits::default(),
    }
}

/// `IfNotPresent` when running against a locally-loaded mover image (kind e2e),
/// else `None` (cluster default). Controlled by the same env that picks the
/// image so the two stay consistent.
fn mover_pull_policy() -> Option<&'static str> {
    if std::env::var(crate::config::MOVER_IMAGE_ENV).is_ok() {
        Some("IfNotPresent")
    } else {
        None
    }
}

/// Whether a Job reached a terminal state: `Some(true)` complete, `Some(false)`
/// failed, `None` still running.
pub(crate) fn job_terminal_state(job: &Job) -> Option<bool> {
    let status = job.status.as_ref()?;
    let conditions = status.conditions.as_ref();
    if let Some(conds) = conditions {
        for c in conds {
            if c.status == "True" {
                match c.type_.as_str() {
                    "Complete" => return Some(true),
                    "Failed" => return Some(false),
                    _ => {}
                }
            }
        }
    }
    // Fall back to counts when conditions aren't populated yet.
    if status.succeeded.unwrap_or(0) >= 1 {
        return Some(true);
    }
    None
}

/// `error_policy` for the `Snapshot` controller.
pub fn error_policy(_backup: Arc<Snapshot>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("Snapshot", err, &ctx)
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

    // --- §11 ReadOnly gate: ReadOnly refuses backups; restores allowed elsewhere ---

    #[test]
    fn readonly_repository_refuses_backup_writes() {
        use kopiur_api::common::RepositoryMode;
        // The pure gate the reconciler branches on.
        assert!(!RepositoryMode::ReadOnly.allows_writes());
        assert!(RepositoryMode::ReadWrite.allows_writes());
        // The refusal message names the repo, the mode, and how to fix.
        let msg = readonly_backup_message("nas");
        assert!(msg.contains("nas"));
        assert!(msg.contains("ReadOnly"));
        assert!(msg.contains("ReadWrite"));
    }

    // --- §13(c) pin decision (pure: spec.pin vs observed → pin/unpin/noop) ---

    #[test]
    fn pin_decision_covers_every_case() {
        // Desired pinned, never reconciled → apply the pin.
        assert_eq!(pin_decision(true, None), PinAction::Pin);
        // Desired pinned, observed unpinned → apply.
        assert_eq!(pin_decision(true, Some(false)), PinAction::Pin);
        // Desired pinned, already pinned → no-op (never issue a redundant pin).
        assert_eq!(pin_decision(true, Some(true)), PinAction::NoOp);
        // Desired unpinned, currently pinned → remove.
        assert_eq!(pin_decision(false, Some(true)), PinAction::Unpin);
        // Desired unpinned, never pinned / observed unpinned → no-op.
        assert_eq!(pin_decision(false, None), PinAction::NoOp);
        assert_eq!(pin_decision(false, Some(false)), PinAction::NoOp);
    }

    // --- §2 phase → Ready mapping ---

    #[test]
    fn snapshot_ready_outcome_maps_every_phase() {
        use kopiur_api::snapshot::SnapshotPhase;
        assert_eq!(
            snapshot_ready_outcome(SnapshotPhase::Succeeded),
            io::ReadyOutcome::Ready
        );
        assert_eq!(
            snapshot_ready_outcome(SnapshotPhase::Discovered),
            io::ReadyOutcome::Ready
        );
        assert_eq!(
            snapshot_ready_outcome(SnapshotPhase::Failed),
            io::ReadyOutcome::Stalled
        );
        for p in [
            SnapshotPhase::Pending,
            SnapshotPhase::Running,
            SnapshotPhase::Deleting,
        ] {
            assert_eq!(snapshot_ready_outcome(p), io::ReadyOutcome::Reconciling);
        }
    }

    #[test]
    fn policy_args_for_threads_flattened_knobs() {
        // §13(b)/§13(f) end-to-end at the controller seam: the SnapshotPolicy policy
        // knobs become non-empty work-spec policy args.
        use kopiur_api::snapshot_policy::{Compression, ErrorHandling};
        let mut sp = sample_policy();
        sp.spec.compression = Some(Compression {
            compressor: Some("zstd".into()),
            never_compress: vec![],
        });
        sp.spec.error_handling = Some(ErrorHandling {
            ignore_file_errors: true,
            ..Default::default()
        });
        let p = policy_args_for(&sp);
        assert!(!p.is_empty());
        assert_eq!(p.compression.as_deref(), Some("zstd"));
        assert_eq!(p.ignore_file_errors, Some(true));
    }

    fn sample_policy() -> kopiur_api::SnapshotPolicy {
        kopiur_api::SnapshotPolicy::new(
            "pg",
            kopiur_api::SnapshotPolicySpec {
                repository: kopiur_api::common::RepositoryRef {
                    kind: Default::default(),
                    name: "r".into(),
                    namespace: None,
                },
                identity: None,
                sources: vec![],
                copy_method: Default::default(),
                volume_snapshot_class_name: None,
                group_by: None,
                retention: None,
                default_deletion_policy: None,
                compression: None,
                files: None,
                extra_args: vec![],
                error_handling: None,
                upload: None,
                verification: None,
                suspend: false,
                hooks: None,
                mover: None,
                credential_projection: None,
            },
        )
    }

    // --- backend_to_repository_connect: every CRD Backend variant must map to a
    // mover RepositoryConnect (no silent reject). A new Backend variant fails to
    // compile in the mapping until handled. ---

    #[test]
    fn every_backend_maps_to_a_repository_connect() {
        use kopiur_api::backend::{
            AzureBackend, B2Backend, FilesystemBackend, GcsBackend, RcloneBackend, S3Backend,
            SftpBackend, WebDavBackend,
        };
        let cases = vec![
            Backend::Filesystem(FilesystemBackend {
                path: "/repo".into(),
                volume: None,
            }),
            Backend::S3(S3Backend {
                bucket: "b".into(),
                prefix: None,
                endpoint: None,
                region: None,
                auth: None,
                tls: None,
            }),
            Backend::Azure(AzureBackend {
                container: "c".into(),
                prefix: None,
                storage_account: Some("acct".into()),
                auth: None,
            }),
            Backend::Gcs(GcsBackend {
                bucket: "b".into(),
                prefix: None,
                auth: None,
            }),
            Backend::B2(B2Backend {
                bucket: "b".into(),
                prefix: None,
                auth: None,
            }),
            Backend::Sftp(SftpBackend {
                host: "h".into(),
                path: "/r".into(),
                port: Some(22),
                username: Some("u".into()),
                auth: None,
            }),
            Backend::WebDav(WebDavBackend {
                url: "https://dav".into(),
                auth: None,
            }),
            Backend::Rclone(RcloneBackend {
                remote_path: "r:bucket".into(),
                config_secret_ref: None,
            }),
        ];
        // Each maps without panicking and converts cleanly to a kopia ConnectSpec
        // whose discriminant matches the backend kind.
        for backend in cases {
            let rc = backend_to_repository_connect(&backend);
            let spec = rc.to_connect_spec();
            let want = match backend.kind_str() {
                "WebDav" => "webdav",
                other => &other.to_ascii_lowercase(),
            };
            assert_eq!(spec.kind_str(), want, "backend {}", backend.kind_str());
        }
    }

    // --- build_backup_run: the source volume (PVC vs inline NFS) glue ----------

    fn resolved_s3_repo() -> io::ResolvedRepository {
        use kopiur_api::backend::S3Backend;
        use kopiur_api::common::{Encryption, SecretKeyRef};
        io::ResolvedRepository {
            // An object-store repo so there is no repo volume to mount — isolates
            // the SOURCE-volume assertion.
            backend: Backend::S3(S3Backend {
                bucket: "b".into(),
                prefix: None,
                endpoint: None,
                region: None,
                auth: None,
                tls: None,
            }),
            encryption: Encryption {
                password_secret_ref: SecretKeyRef {
                    name: "creds".into(),
                    namespace: None,
                    key: Some("KOPIA_PASSWORD".into()),
                },
            },
            repo_namespace: Some("media-ns".into()),
            mover_defaults: None,
            identity_defaults: None,
            on_namespace_delete: Default::default(),
            mode: Default::default(),
            credential_projection_allowed: false,
        }
    }

    fn config_with_source(
        name: &str,
        source: kopiur_api::snapshot_policy::Source,
    ) -> SnapshotPolicy {
        use kopiur_api::common::{RepositoryKind, RepositoryRef};
        use kopiur_api::snapshot_policy::SnapshotPolicySpec;
        SnapshotPolicy::new(
            name,
            SnapshotPolicySpec {
                repository: RepositoryRef {
                    kind: RepositoryKind::Repository,
                    name: "repo".into(),
                    namespace: None,
                },
                identity: None,
                sources: vec![source],
                copy_method: Default::default(),
                volume_snapshot_class_name: None,
                group_by: None,
                retention: None,
                default_deletion_policy: None,
                compression: None,
                files: None,
                extra_args: vec![],
                error_handling: None,
                upload: None,
                verification: None,
                suspend: false,
                hooks: None,
                mover: None,
                credential_projection: None,
            },
        )
    }

    fn dummy_backup() -> Snapshot {
        Snapshot::new(
            "b1",
            kopiur_api::snapshot::SnapshotSpec {
                policy_ref: None,
                tags: None,
                failure_policy: None,
                deletion_policy: None,
                pin: false,
            },
        )
    }

    #[test]
    fn build_backup_run_maps_nfs_source_to_inline_nfs_mount() {
        use crate::jobs::MountSource;
        use kopiur_api::backend::NfsVolume;
        use kopiur_api::snapshot_policy::Source;
        let cfg = config_with_source(
            "media",
            Source {
                pvc: None,
                pvc_selector: None,
                nfs: Some(NfsVolume {
                    server: "expanse.internal".into(),
                    path: "/mnt/eros/Media".into(),
                }),
                source_path_override: None,
                source_path_strategy: None,
            },
        );
        let repo = resolved_s3_repo();
        let (ws, source_volume, repo_volume, _creds) =
            build_backup_run(&dummy_backup(), &cfg, &repo, "media-ns", "media").unwrap();

        // The NFS export becomes an inline-NFS source mount (read-only), mounted at
        // and snapshotted under the export path (no override → defaults to it).
        let src = source_volume.expect("an NFS source mount");
        assert_eq!(
            src.source,
            MountSource::Nfs {
                server: "expanse.internal".into(),
                path: "/mnt/eros/Media".into(),
            }
        );
        assert_eq!(src.mount_path, "/mnt/eros/Media");
        assert!(src.read_only, "a backup source is mounted read-only");
        // kopia records the export path as the snapshot source path.
        match ws.operation {
            Operation::Snapshot(op) => assert_eq!(op.source_path, "/mnt/eros/Media"),
            other => panic!("expected a Snapshot operation, got {other:?}"),
        }
        // Object-store repo → no repo volume to mount.
        assert!(repo_volume.is_none());
    }

    #[test]
    fn build_backup_run_honors_source_path_override_for_nfs() {
        use kopiur_api::backend::NfsVolume;
        use kopiur_api::snapshot_policy::Source;
        let cfg = config_with_source(
            "media",
            Source {
                pvc: None,
                pvc_selector: None,
                nfs: Some(NfsVolume {
                    server: "nas.lan".into(),
                    path: "/export/media".into(),
                }),
                source_path_override: Some("/data".into()),
                source_path_strategy: None,
            },
        );
        let repo = resolved_s3_repo();
        let (ws, source_volume, _repo, _creds) =
            build_backup_run(&dummy_backup(), &cfg, &repo, "ns", "media").unwrap();
        // The override drives both the mount path and the recorded source path.
        assert_eq!(source_volume.unwrap().mount_path, "/data");
        match ws.operation {
            Operation::Snapshot(op) => assert_eq!(op.source_path, "/data"),
            other => panic!("expected a Snapshot operation, got {other:?}"),
        }
    }

    #[test]
    fn build_backup_run_remaps_nfs_pseudo_root_source_off_container_rootfs() {
        // Regression: an NFSv4 pseudo-root export (`path: "/"`) was mounted at
        // the container "/" — the mover pod then failed to start with
        // `error mounting ... to rootfs at "/": mountpoint ... is on the top of
        // rootfs`. The server-side export path stays "/", but the in-container
        // mount path (and kopia source path) must be a safe non-root path.
        use crate::jobs::MountSource;
        use kopiur_api::backend::NfsVolume;
        use kopiur_api::snapshot_policy::Source;
        let cfg = config_with_source(
            "media",
            Source {
                pvc: None,
                pvc_selector: None,
                nfs: Some(NfsVolume {
                    server: "10.0.0.5".into(),
                    path: "/".into(),
                }),
                source_path_override: None,
                source_path_strategy: None,
            },
        );
        let repo = resolved_s3_repo();
        let (ws, source_volume, _repo, _creds) =
            build_backup_run(&dummy_backup(), &cfg, &repo, "ns", "media").unwrap();
        let src = source_volume.expect("an NFS source mount");
        // The NFS volume still exports the server-side pseudo-root.
        assert_eq!(
            src.source,
            MountSource::Nfs {
                server: "10.0.0.5".into(),
                path: "/".into(),
            }
        );
        // ...but it is NOT mounted at the container rootfs.
        assert_ne!(
            src.mount_path, "/",
            "must not mount over the container rootfs"
        );
        assert_eq!(src.mount_path, crate::consts::NFS_SOURCE_MOUNT_PATH);
        match ws.operation {
            Operation::Snapshot(op) => {
                assert_eq!(op.source_path, crate::consts::NFS_SOURCE_MOUNT_PATH)
            }
            other => panic!("expected a Snapshot operation, got {other:?}"),
        }
    }

    #[test]
    fn build_backup_run_maps_pvc_source_to_pvc_mount() {
        use crate::jobs::MountSource;
        use kopiur_api::snapshot_policy::{PvcSource, Source};
        let cfg = config_with_source(
            "pg",
            Source {
                pvc: Some(PvcSource {
                    name: "pg-data".into(),
                }),
                pvc_selector: None,
                nfs: None,
                source_path_override: None,
                source_path_strategy: None,
            },
        );
        let repo = resolved_s3_repo();
        let (_ws, source_volume, _repo, _creds) =
            build_backup_run(&dummy_backup(), &cfg, &repo, "ns", "pg").unwrap();
        let src = source_volume.expect("a PVC source mount");
        assert_eq!(
            src.source,
            MountSource::Pvc {
                claim_name: "pg-data".into()
            }
        );
        assert_eq!(src.mount_path, "/pvc/pg-data");
    }

    #[test]
    fn build_backup_run_rejects_a_source_with_neither_pvc_nor_nfs() {
        use kopiur_api::snapshot_policy::Source;
        // pvcSelector-only / empty single source: the single-source mover path
        // needs an explicit pvc or nfs (the webhook rejects this earlier; the
        // controller defends against it rather than building a bogus Job).
        let cfg = config_with_source(
            "x",
            Source {
                pvc: None,
                pvc_selector: None,
                nfs: None,
                source_path_override: None,
                source_path_strategy: None,
            },
        );
        let repo = resolved_s3_repo();
        assert!(build_backup_run(&dummy_backup(), &cfg, &repo, "ns", "x").is_err());
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
        let a = ann(&[("kopiur.home-operations.com/other", "x")]);
        assert_eq!(
            plan_deletion(DeletionPolicy::Delete, &a),
            DeletionPlan::DeleteSnapshot
        );
    }

    // --- namespace_delete_plan (ADR-0005 §5 data-loss prevention) -----------

    #[test]
    fn non_terminating_namespace_keeps_the_per_snapshot_plan() {
        // A lone `kubectl delete snapshot` (namespace healthy) honors the Snapshot's
        // own plan regardless of the repository's onNamespaceDelete policy.
        for policy in [NamespaceDeletePolicy::Orphan, NamespaceDeletePolicy::Delete] {
            for base in [
                DeletionPlan::DeleteSnapshot,
                DeletionPlan::RetainSnapshot,
                DeletionPlan::OrphanSnapshot,
            ] {
                assert_eq!(namespace_delete_plan(policy, false, base), base);
            }
        }
    }

    #[test]
    fn terminating_namespace_orphan_policy_forces_orphan() {
        // The fail-safe default: a deleted namespace must not run `kopia snapshot
        // delete`, even when the Snapshot's own plan was DeleteSnapshot.
        for base in [
            DeletionPlan::DeleteSnapshot,
            DeletionPlan::RetainSnapshot,
            DeletionPlan::OrphanSnapshot,
        ] {
            assert_eq!(
                namespace_delete_plan(NamespaceDeletePolicy::Orphan, true, base),
                DeletionPlan::OrphanSnapshot
            );
        }
    }

    #[test]
    fn terminating_namespace_delete_policy_cascades_to_base_plan() {
        // Opt-in cascade: with onNamespaceDelete=Delete, the per-Snapshot plan applies
        // (so a produced Snapshot still runs the snapshot delete).
        assert_eq!(
            namespace_delete_plan(
                NamespaceDeletePolicy::Delete,
                true,
                DeletionPlan::DeleteSnapshot
            ),
            DeletionPlan::DeleteSnapshot
        );
        // ...and a Retain/Orphan base is preserved unchanged.
        assert_eq!(
            namespace_delete_plan(
                NamespaceDeletePolicy::Delete,
                true,
                DeletionPlan::RetainSnapshot
            ),
            DeletionPlan::RetainSnapshot
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
