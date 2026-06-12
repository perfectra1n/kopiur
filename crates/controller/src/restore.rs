//! The `Restore` reconciler (ADR §4.6, §4.7).
//!
//! Resolves the source (`snapshotRef` / `fromPolicy` / `identity`), pins
//! `status.resolved`, creates a restore mover `Job`, and handles the passive
//! populator mode (a PVC's `spec.dataSourceRef` points at the `Restore`).
//!
//! The source-mode dispatch is an **exhaustive `match`** over the externally
//! tagged `RestoreSource` enum (no `_ =>`), and [`default_on_missing`] /
//! [`populator_state`] are pure decisions, all unit-tested. The pvc-prime
//! handshake IO is a documented minimal partial (see NOTE in the reconcile body).

use std::sync::Arc;

use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};

use kopiur_api::snapshot::Snapshot;
use kopiur_api::{
    OnMissingSnapshot, Restore, RestorePhase, RestoreSource, RestoreTarget, validate,
};
use kopiur_mover::workspec::{
    MoverOptions, MoverWorkSpec, Operation, RepositoryConnect, ResolvedIdentity as MoverIdentity,
    RestoreOp, TargetRef,
};

use crate::config;
use crate::consts::{
    ALLOW_PRIVILEGED_MOVER_ACTION, API_VERSION, CREDENTIALS_AVAILABLE_CONDITION,
    CREDENTIALS_PROJECTED_REASON, MISSING_CREDENTIALS_REASON, MOVER_PERMITTED_CONDITION,
    PRIVILEGED_MOVER_NOT_PERMITTED_REASON,
};
use crate::context::Context;
use crate::error::{Error, Result, error_policy_for};
use crate::io::{self, ResolvedRepository};
use crate::jobs::{self, JobLimits, MoverJobInputs, VolumeMountSpec};

/// Which source mode a restore uses, as a stable string (mirrors
/// `RestoreSource::kind_str`, re-derived through an exhaustive match so a new
/// variant must be handled here too).
pub fn source_mode(source: &RestoreSource) -> &'static str {
    match source {
        RestoreSource::SnapshotRef(_) => "SnapshotRef",
        RestoreSource::FromPolicy(_) => "FromPolicy",
        RestoreSource::Identity(_) => "Identity",
    }
}

/// The default `onMissingSnapshot` for a source mode when the spec doesn't set
/// it (ADR §4.6 / SKILL "Restores fail closed"): `fromPolicy` defaults to
/// `Continue` (deploy-or-restore), everything else fails closed (`Fail`).
pub fn default_on_missing(source: &RestoreSource) -> OnMissingSnapshot {
    match source {
        RestoreSource::FromPolicy(_) => OnMissingSnapshot::Continue,
        RestoreSource::SnapshotRef(_) | RestoreSource::Identity(_) => OnMissingSnapshot::Fail,
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
    /// `target: populator`: this `Restore` is a passive populator source, awaiting a
    /// PVC `dataSourceRef` to claim it (ADR-0005 §9).
    AwaitingClaim,
    /// An explicit `pvc`/`pvcRef` target: the operator drives the restore directly.
    DirectTarget,
}

/// Wall-clock duration (seconds) of a restore `Job` from its
/// `status.startTime`/`completionTime`. `None` if either is absent or the
/// interval is negative (clock skew). Pure. (`Time.0` is a jiff `Timestamp`.)
pub fn restore_job_duration_seconds(job: &k8s_openapi::api::batch::v1::Job) -> Option<i64> {
    let st = job.status.as_ref()?;
    let start = st.start_time.as_ref()?.0.as_second();
    let end = st.completion_time.as_ref()?.0.as_second();
    let secs = end - start;
    (secs >= 0).then_some(secs)
}

/// Decide the populator state from the restore `target` (ADR-0005 §9). Pure +
/// exhaustive over [`RestoreTarget`] (no `_ =>`), so a new target variant must be
/// considered here before it compiles: `populator` awaits a PVC `dataSourceRef`
/// claim; `pvc`/`pvcRef` is a direct, operator-driven restore.
pub fn populator_state(target: &RestoreTarget) -> PopulatorState {
    match target {
        RestoreTarget::Populator(_) => PopulatorState::AwaitingClaim,
        RestoreTarget::Pvc(_) | RestoreTarget::PvcRef(_) => PopulatorState::DirectTarget,
    }
}

/// Reconcile a `Restore`.
#[tracing::instrument(skip(restore, ctx), fields(kind = "Restore", namespace = %restore.namespace().unwrap_or_default(), name = %restore.name_any()))]
pub async fn reconcile(restore: Arc<Restore>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&restore, &ctx).await;
    ctx.metrics
        .record_reconcile("Restore", start.elapsed().as_secs_f64());
    record_restore_status_metrics(&restore, &ctx, result.is_ok()).await;
    result
}

/// Mirror a Restore's phase gauge. Zeroes it on deletion (so a Failed restore's
/// alert clears once the CR is gone) and re-reads the freshest status on success
/// — see the Snapshot equivalent for the rationale. (Restore *duration* is
/// recorded at the Job-completion site, not from status.)
async fn record_restore_status_metrics(restore: &Restore, ctx: &Context, ok: bool) {
    let (Some(ns), name) = (restore.namespace(), restore.name_any()) else {
        return;
    };
    if restore.metadata.deletion_timestamp.is_some() {
        ctx.metrics
            .clear_phase::<RestorePhase>("Restore", &ns, &name);
        return;
    }
    if !ok {
        return;
    }
    let api: Api<Restore> = Api::namespaced(ctx.client.clone(), &ns);
    if let Ok(Some(latest)) = api.get_opt(&name).await
        && let Some(phase) = latest.status.as_ref().and_then(|s| s.phase)
    {
        ctx.metrics.set_restore_phase(&ns, &name, phase);
    }
}

async fn reconcile_inner(restore: &Restore, ctx: &Context) -> Result<Action> {
    if let Err(e) = validate::validate_restore(&restore.spec) {
        return Err(Error::Validation(e.to_string()));
    }

    let namespace = restore
        .namespace()
        .ok_or_else(|| Error::Invariant("Restore has no namespace".into()))?;
    let name = restore.name_any();
    let api: Api<Restore> = Api::namespaced(ctx.client.clone(), &namespace);

    // Already terminal: a Restore is one-shot. Once Completed/Failed there is
    // nothing left to do until the spec changes, so don't re-resolve, re-pin a
    // fresh timestamp, or re-write the phase — each of which would churn status and
    // self-trigger another reconcile (the same hot-loop class as the repo bug).
    // Mirrors the Snapshot reconciler's terminal discipline.
    if matches!(
        restore.status.as_ref().and_then(|s| s.phase),
        Some(RestorePhase::Completed) | Some(RestorePhase::Failed)
    ) {
        return Ok(Action::requeue(std::time::Duration::from_secs(600)));
    }

    // §3: pin the resolved source kind to status so the SOURCE printer column shows
    // where the restore reads from. Deterministic (from the spec source variant), so
    // an unchanged value is a no-op patch.
    let source_kind = source_mode(&restore.spec.source);
    if restore
        .status
        .as_ref()
        .and_then(|s| s.source_kind.as_deref())
        != Some(source_kind)
    {
        io::patch_status(
            &api,
            &name,
            serde_json::json!({ "sourceKind": source_kind }),
        )
        .await?;
    }

    let state = populator_state(&restore.spec.target);
    let on_missing = effective_on_missing(
        restore
            .spec
            .policy
            .as_ref()
            .and_then(|p| p.on_missing_snapshot),
        &restore.spec.source,
    );

    // ADR §4.6: the resolution is pinned ONCE and never re-resolved — a restore
    // must not silently retarget when newer snapshots appear mid-flight. Reuse a
    // previously pinned id; resolve only while no pin exists yet.
    let pinned_id = restore
        .status
        .as_ref()
        .and_then(|s| s.resolved.as_ref())
        .and_then(|r| r.kopia_snapshot_id.clone());
    let snapshot_id = if let Some(id) = pinned_id {
        id
    } else {
        match resolve_snapshot(ctx, restore, &namespace).await? {
            Some(res) => {
                // Pin the FULL resolution (id + provenance + timestamp) exactly
                // once; the no-pin check above makes this a single write, so it
                // cannot churn status.
                let mut resolved = serde_json::json!({
                    "kopiaSnapshotID": res.kopia_snapshot_id,
                    "pinnedAt": chrono::Utc::now().to_rfc3339(),
                });
                if let Some(r) = &res.snapshot_ref {
                    resolved["snapshotRef"] = serde_json::to_value(r)?;
                }
                if let Some(i) = &res.identity {
                    resolved["identity"] = serde_json::to_value(i)?;
                }
                io::patch_status(
                    &api,
                    &name,
                    serde_json::json!({ "phase": "Resolving", "resolved": resolved }),
                )
                .await?;
                res.kopia_snapshot_id
            }
            None => {
                // No snapshot matched. While the `waitTimeout` window (anchored at
                // the Restore's creation) is open, keep waiting instead of giving
                // up — `onMissingSnapshot` applies only once the window closes
                // (ADR §4.6 G7).
                let now = chrono::Utc::now().timestamp();
                let created = restore
                    .metadata
                    .creation_timestamp
                    .as_ref()
                    .map(|t| t.0.as_second())
                    .unwrap_or(now);
                let wait_timeout = restore
                    .spec
                    .policy
                    .as_ref()
                    .and_then(|p| p.wait_timeout.as_deref());
                if let Some(remaining) = wait_remaining_secs(created, wait_timeout, now) {
                    let existing = restore
                        .status
                        .as_ref()
                        .map(|s| s.conditions.clone())
                        .unwrap_or_default();
                    // Static message (no countdown): an identical re-patch is a
                    // server-side no-op, so polling here cannot churn status.
                    let conditions = io::upsert_condition(
                        &existing,
                        "Resolved",
                        false,
                        "WaitingForSnapshot",
                        &format!(
                            "no snapshot matched the restore source yet; waiting up to \
                             waitTimeout ({}) from creation for it to appear before \
                             applying onMissingSnapshot",
                            wait_timeout.unwrap_or_default()
                        ),
                        restore.metadata.generation,
                    );
                    io::patch_status(
                        &api,
                        &name,
                        serde_json::json!({ "phase": "Pending", "conditions": conditions }),
                    )
                    .await?;
                    return Ok(Action::requeue(std::time::Duration::from_secs(
                        remaining.clamp(1, 15),
                    )));
                }
                // Window closed (or none configured): honor the closed enum exhaustively.
                return match on_missing {
                    OnMissingSnapshot::Fail => {
                        io::patch_status(
                            &api,
                            &name,
                            serde_json::json!({
                                "phase": "Failed",
                                "conditions": [condition(
                                    "Resolved", "False", "SnapshotNotFound",
                                    "no snapshot matched the restore source",
                                )],
                            }),
                        )
                        .await?;
                        Err(Error::MissingDependency(
                            "no snapshot matched restore source".into(),
                        ))
                    }
                    OnMissingSnapshot::Continue => {
                        // Deploy-or-restore: nothing to restore, complete cleanly.
                        io::patch_status(
                            &api,
                            &name,
                            serde_json::json!({
                                "phase": "Completed",
                                "conditions": [condition(
                                    "Resolved", "True", "NoSnapshotContinue",
                                    "no snapshot found; continuing (deploy-or-restore)",
                                )],
                            }),
                        )
                        .await?;
                        Ok(Action::requeue(std::time::Duration::from_secs(600)))
                    }
                };
            }
        }
    };

    match state {
        PopulatorState::DirectTarget => {
            drive_direct_restore(ctx, restore, &api, &namespace, &name, &snapshot_id).await
        }
        PopulatorState::AwaitingClaim => {
            // NOTE: passive populator mode. The full CSI populator handshake
            // (PVC dataSourceRef -> prime PVC -> bind) requires the
            // VolumePopulator lib-mover protocol. The minimal real implementation
            // here surfaces the awaiting-claim condition and pins the resolved
            // snapshot so a claim can proceed; wiring the prime-PVC dance is the
            // documented residual.
            io::patch_status(
                &api,
                &name,
                serde_json::json!({
                    "phase": "Pending",
                    "conditions": [condition(
                        "AwaitingClaim", "True", "AwaitingPvcDataSourceRef",
                        "passive populator: awaiting a PVC dataSourceRef to claim this Restore",
                    )],
                    "target": { "pvcPrime": "awaiting-claim" },
                }),
            )
            .await?;
            Ok(Action::requeue(std::time::Duration::from_secs(30)))
        }
    }
}

/// Drive a restore-with-explicit-target: create the restore mover Job (writing
/// into the target PVC), then track it to terminal.
async fn drive_direct_restore(
    ctx: &Context,
    restore: &Restore,
    api: &Api<Restore>,
    namespace: &str,
    name: &str,
    snapshot_id: &str,
) -> Result<Action> {
    use k8s_openapi::api::batch::v1::Job;
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);
    if let Some(job) = job_api.get_opt(name).await? {
        // Guard each phase write with a phase-equality check so a tracked Job that
        // sits terminal (or keeps running) doesn't re-patch an identical phase on
        // every requeue and self-trigger. Mirrors the Snapshot reconciler.
        let phase = restore.status.as_ref().and_then(|s| s.phase);
        return match crate::snapshot::job_terminal_state(&job) {
            Some(true) => {
                if let Some(secs) = restore_job_duration_seconds(&job) {
                    ctx.metrics.set_restore_duration(namespace, name, secs);
                }
                if phase != Some(RestorePhase::Completed) {
                    io::patch_status(api, name, serde_json::json!({ "phase": "Completed" }))
                        .await?;
                }
                Ok(Action::requeue(std::time::Duration::from_secs(600)))
            }
            Some(false) => {
                if phase != Some(RestorePhase::Failed) {
                    io::patch_status(api, name, serde_json::json!({ "phase": "Failed" })).await?;
                }
                Ok(Action::requeue(std::time::Duration::from_secs(120)))
            }
            None => {
                if phase != Some(RestorePhase::Restoring) {
                    io::patch_status(api, name, serde_json::json!({ "phase": "Restoring" }))
                        .await?;
                }
                Ok(Action::requeue(std::time::Duration::from_secs(30)))
            }
        };
    }

    // Resolve the repository + target PVC for the restore Job.
    let repo = resolve_restore_repository(ctx, restore, namespace).await?;
    // DirectTarget is only reached for an explicit PVC target (populator routes to
    // AwaitingClaim in the reconcile dispatch). Exhaustive over RestoreTarget so a new
    // variant must be considered here.
    let target_pvc = match &restore.spec.target {
        RestoreTarget::PvcRef(r) => r.name.clone(),
        // `target.pvc` means the operator CREATES the PVC (ADR §3.6) — without
        // this the mover Job references a claim nobody made and sits Pending
        // forever (FailedScheduling: persistentvolumeclaim not found).
        RestoreTarget::Pvc(t) => {
            ensure_restore_target_pvc(ctx, namespace, t).await?;
            t.name.clone()
        }
        RestoreTarget::Populator(_) => {
            return Err(Error::Invariant(
                "DirectTarget restore reached with a populator target (should route to \
                 AwaitingClaim)"
                    .into(),
            ));
        }
    };
    let target_path = "/restore".to_string();

    // The restore mover Job runs in this (workload) namespace: mint the mover SA +
    // RoleBinding here, then resolve the credential Secret(s) it loads via envFrom —
    // verifying the user-managed ones are present, or (with
    // `spec.credentialProjection`) projecting the repository's Secret(s) here owned
    // by this Restore. A problem surfaces as a clear condition + Event (ADR §4.12).
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

    // Resolve the restore mover's EFFECTIVE security context once (explicit, or
    // inherited from a workload pod via `inheritSecurityContextFrom`). Both the gate
    // and the Job use it, so an inherited root context is gated like an explicit one.
    // The effective container + pod security contexts — explicit, or both inherited
    // from a workload pod via `inheritSecurityContextFrom`.
    let (effective_sc, effective_pod_sc) =
        io::resolve_mover_security_contexts(&ctx.client, namespace, restore.spec.mover.as_ref())
            .await?;
    let privileged_mode = restore.spec.mover.as_ref().and_then(|m| m.privileged_mode);

    // Field-wise merge the repository's moverDefaults under the recipe's effective
    // contexts/resources/cache (`hardened ⊂ moverDefaults ⊂ recipe`, ADR-0004 §1/§2).
    // The gate and the Job both run on the MERGED result.
    let resolved_mover = kopiur_api::common::resolve_mover(
        repo.mover_defaults.as_ref(),
        effective_sc.as_ref(),
        effective_pod_sc.as_ref(),
        restore
            .spec
            .mover
            .as_ref()
            .and_then(|m| m.resources.as_ref()),
        restore.spec.mover.as_ref().and_then(|m| m.cache.as_ref()),
        restore
            .spec
            .mover
            .as_ref()
            .and_then(|m| m.ttl_seconds_after_finished),
    );

    // Privileged-mover gate (ADR §4.11/§G16, VolSync-parity): an elevated restore mover
    // (root/privileged/added caps/`privilegedMode`, container- OR pod-level) requires the
    // target namespace to opt in via the `kopiur.home-operations.com/privileged-movers`
    // annotation — a tenant there could otherwise reuse the minted mover SA at that
    // privilege. Refuse with a clear `MoverPermitted=False` condition + Event otherwise.
    // Mirrors the Snapshot gate.
    if kopiur_api::common::requires_privilege_resolved(
        Some(&resolved_mover.security_context),
        resolved_mover.pod_security_context.as_ref(),
        privileged_mode,
    ) && !io::namespace_allows_privileged_movers(&ctx.client, namespace).await?
    {
        let sa = ctx
            .mover_service_account
            .as_deref()
            .unwrap_or(config::DEFAULT_MOVER_NAME);
        let msg = io::privileged_mover_message("Restore", name, namespace, sa);
        let existing = restore
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
            restore.metadata.generation,
        );
        io::patch_status(
            api,
            name,
            serde_json::json!({ "phase": "Pending", "conditions": conditions }),
        )
        .await?;
        io::publish_warning_event(
            ctx,
            restore,
            PRIVILEGED_MOVER_NOT_PERMITTED_REASON,
            ALLOW_PRIVILEGED_MOVER_ACTION,
            &msg,
        )
        .await;
        // Like a missing creds Secret, the fix is an out-of-band namespace annotation —
        // Transient, so the short requeue cadence picks up the opt-in within ~30s.
        return Err(Error::MissingDependency(msg));
    }
    // Permitted: clear any stale `MoverPermitted=False` from a prior reconcile.
    if let Some(conds) = restore.status.as_ref().map(|s| s.conditions.as_slice())
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
            restore.metadata.generation,
        );
        io::patch_status(api, name, serde_json::json!({ "conditions": conditions })).await?;
    }

    let owner = io::owner_ref_for(restore, "Restore")?;
    let repo_ref = restore.spec.repository.as_ref();
    let creds = match io::resolve_mover_creds_for(
        &ctx.client,
        namespace,
        name,
        &owner,
        &repo,
        restore
            .spec
            .credential_projection
            .as_ref()
            .is_some_and(|p| p.enabled),
        repo_ref
            .map(|r| io::repo_kind_str(r.kind))
            .unwrap_or("Repository"),
        repo_ref
            .map(|r| r.name.as_str())
            .unwrap_or("(from source config)"),
    )
    .await
    {
        Ok(c) => c,
        Err(Error::MissingDependency(msg)) => {
            let existing = restore
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
                restore.metadata.generation,
            );
            io::patch_status(
                api,
                name,
                serde_json::json!({ "phase": "Pending", "conditions": conditions }),
            )
            .await?;
            io::publish_missing_creds_event(ctx, restore, &msg).await;
            return Err(Error::MissingDependency(msg));
        }
        Err(e) => return Err(e),
    };
    if creds.projected > 0 {
        ctx.metrics
            .inc_secrets_projected(namespace, creds.projected);
    }
    // Creds present (or projected): clear any stale `CredentialsAvailable=False`.
    if let Some(conds) = restore.status.as_ref().map(|s| s.conditions.as_slice())
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
            restore.metadata.generation,
        );
        io::patch_status(api, name, serde_json::json!({ "conditions": conditions })).await?;
    }
    let creds_secrets = creds.names;

    let identity = MoverIdentity {
        username: "restore".into(),
        hostname: namespace.to_string(),
        source_path: target_path.clone(),
    };
    // Carry the Restore CRD's options (ADR §4.6) through to the mover so kopia
    // honors them. `None` lets kopia use its defaults.
    let (ignore_permission_errors, write_files_atomically) = restore
        .spec
        .options
        .as_ref()
        .map(|o| (o.ignore_permission_errors, o.write_files_atomically))
        .unwrap_or((None, None));
    // Effective cache config (repository cacheDefaults overlaid by this restore's
    // mover.cache, ADR §3.1) drives both the connect budgets and the cache volume.
    let effective_cache = crate::cache::effective_cache(
        &repo,
        restore.spec.mover.as_ref().and_then(|m| m.cache.as_ref()),
    );
    let cache = crate::cache::cache_tuning(effective_cache.as_ref());
    let work_spec = MoverWorkSpec {
        version: 1,
        operation: Operation::Restore(RestoreOp {
            snapshot_id: snapshot_id.to_string(),
            target_path: target_path.clone(),
            ignore_permission_errors,
            write_files_atomically,
        }),
        identity,
        repository: restore_connect(&repo)?,
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "Restore".to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
        cache,
        // Repo throttle applies to restore too (§13(e)).
        throttle: io::throttle_spec(repo.mover_defaults.as_ref()),
    };
    let repo_volume =
        io::filesystem_repo_mount_source(&repo.backend).map(|source| VolumeMountSpec {
            source,
            mount_path: io::filesystem_repo_path(&repo.backend).unwrap_or_default(),
            read_only: true,
        });
    // Resolve the cache VOLUME; a persistent cache PVC is owned by this Restore.
    let cache_volume = crate::cache::resolve_cache_volume(
        &ctx.client,
        namespace,
        owner.clone(),
        &format!("kopiur-cache-{name}"),
        effective_cache.as_ref(),
    )
    .await?;
    // RWO Multi-Attach avoidance for the restore DESTINATION PVC: when restoring into
    // an existing ReadWriteOnce PVC held by a running app pod, pin the restore mover to
    // that node so the kubelet can attach the volume (a freshly-created `target.pvc`
    // has no holder → no pin). The resolved `sourceColocation` mode (default `Auto`)
    // decides. RWO multi-attach fix.
    let (mover_affinity, mover_tolerations) = {
        let decision = io::resolve_source_colocation(
            &ctx.client,
            namespace,
            &target_pvc,
            resolved_mover.source_colocation,
        )
        .await?;
        io::apply_colocation(
            decision,
            resolved_mover.affinity.clone(),
            resolved_mover.tolerations.clone(),
        )?
    };
    let inputs = MoverJobInputs {
        name,
        namespace,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: crate::snapshot::mover_pull_policy_pub(),
        limits: {
            let mut l = restore_job_limits(restore);
            if l.ttl_seconds_after_finished.is_none() {
                l.ttl_seconds_after_finished = resolved_mover.ttl_seconds_after_finished;
            }
            l
        },
        resources: resolved_mover.resources.clone(),
        // The fully-merged contexts (hardened ⊂ moverDefaults ⊂ recipe) — the same
        // values the privileged gate above ran on.
        security_context: resolved_mover.security_context.clone(),
        pod_security_context: resolved_mover.pod_security_context.clone(),
        node_selector: resolved_mover.node_selector.clone(),
        tolerations: mover_tolerations,
        affinity: mover_affinity,
        labels: io::child_labels(&[(crate::consts::OP_LABEL, crate::consts::OP_RESTORE)]),
        // Restore writes INTO the target PVC, mounted read-write at /restore.
        source_volume: Some(VolumeMountSpec::pvc(target_pvc, target_path, false)),
        repo_volume,
        creds_secrets,
        result_configmap: None,
        service_account: ctx.mover_service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
        annotations: Default::default(),
        cache_volume,
        readiness_exec: None,
    };
    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, namespace, name, &cm, &job).await?;
    io::patch_status(api, name, serde_json::json!({ "phase": "Restoring" })).await?;
    tracing::info!(restore = %name, %snapshot_id, "created restore Job");
    Ok(Action::requeue(std::time::Duration::from_secs(30)))
}

/// A fully-resolved restore source, ready to pin to `status.resolved` (ADR §4.6):
/// the exact kopia snapshot id plus its provenance (the `Snapshot` CR or the
/// kopia identity it was selected by).
#[derive(Debug, Clone)]
struct ResolvedSource {
    kopia_snapshot_id: String,
    snapshot_ref: Option<kopiur_api::common::ObjectRef>,
    identity: Option<kopiur_api::common::ResolvedIdentity>,
}

/// Create the `target.pvc` PVC if it doesn't exist (idempotent). Deliberately
/// NOT owner-referenced to the `Restore`: the restored data must survive
/// `kubectl delete restore` — GC'ing the target PVC with the CR would destroy
/// what the user just recovered. Missing `capacity` is rejected (webhook + here,
/// defensively): a silently-defaulted size could truncate the restored data.
async fn ensure_restore_target_pvc(
    ctx: &Context,
    namespace: &str,
    template: &kopiur_api::restore::PvcTemplate,
) -> Result<()> {
    use k8s_openapi::api::core::v1::PersistentVolumeClaim;
    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), namespace);
    if pvc_api.get_opt(&template.name).await?.is_some() {
        return Ok(());
    }
    let capacity = template.capacity.as_deref().ok_or_else(|| {
        Error::Validation(format!(
            "restore target.pvc {:?} has no capacity; set target.pvc.capacity (e.g. 10Gi, at \
             least the size of the data being restored) — the operator will not guess a size \
             for a PVC it creates",
            template.name
        ))
    })?;
    let access_modes = if template.access_modes.is_empty() {
        vec!["ReadWriteOnce".to_string()]
    } else {
        template.access_modes.clone()
    };
    let pvc: PersistentVolumeClaim = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {
            "name": template.name,
            "namespace": namespace,
            "labels": io::child_labels(&[(crate::consts::OP_LABEL, crate::consts::OP_RESTORE_TARGET)]),
        },
        "spec": {
            "accessModes": access_modes,
            "resources": { "requests": { "storage": capacity } },
            "storageClassName": template.storage_class_name,
        },
    }))?;
    match pvc_api
        .create(&kube::api::PostParams::default(), &pvc)
        .await
    {
        Ok(_) => {
            tracing::info!(pvc = %template.name, %namespace, "created restore target PVC");
            Ok(())
        }
        // Lost a create race with another reconcile — the PVC exists, which is
        // all this function guarantees.
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Resolve the restore's source to a concrete kopia snapshot. Returns `None`
/// when no snapshot matches (caller applies `waitTimeout` + `onMissingSnapshot`).
async fn resolve_snapshot(
    ctx: &Context,
    restore: &Restore,
    namespace: &str,
) -> Result<Option<ResolvedSource>> {
    use kopiur_api::common::{ObjectRef, ResolvedIdentity};
    match &restore.spec.source {
        RestoreSource::SnapshotRef(r) => {
            let ns = r.namespace.as_deref().unwrap_or(namespace);
            let api: Api<Snapshot> = Api::namespaced(ctx.client.clone(), ns);
            let backup = api.get_opt(&r.name).await?;
            Ok(backup
                .and_then(|b| b.status)
                .and_then(|s| s.snapshot)
                .map(|s| ResolvedSource {
                    kopia_snapshot_id: s.kopia_snapshot_id,
                    snapshot_ref: Some(ObjectRef {
                        name: r.name.clone(),
                        namespace: Some(ns.to_string()),
                    }),
                    identity: None,
                }))
        }
        RestoreSource::Identity(id) => {
            let identity = ResolvedIdentity {
                username: id.username.clone(),
                hostname: id.hostname.clone(),
                source_path: id.source_path.clone(),
            };
            // An explicit snapshot id wins; otherwise resolve via snapshot list.
            if let Some(sid) = &id.snapshot_id {
                return Ok(Some(ResolvedSource {
                    kopia_snapshot_id: sid.clone(),
                    snapshot_ref: None,
                    identity: Some(identity),
                }));
            }
            let repo = resolve_restore_repository(ctx, restore, namespace).await?;
            let snapshots = list_for_identity(
                ctx,
                &repo,
                namespace,
                &id.username,
                &id.hostname,
                id.source_path.as_deref(),
            )
            .await?;
            let snapshots = filter_as_of(snapshots, id.as_of.as_deref())?;
            Ok(
                pick_offset(snapshots, id.offset.unwrap_or(0)).map(|sid| ResolvedSource {
                    kopia_snapshot_id: sid,
                    snapshot_ref: None,
                    identity: Some(identity),
                }),
            )
        }
        RestoreSource::FromPolicy(c) => {
            // Resolve identity from the SnapshotPolicy, then list newest/offset.
            use kopiur_api::SnapshotPolicy;
            let cfg_ns = c.namespace.as_deref().unwrap_or(namespace);
            let cfg_api: Api<SnapshotPolicy> = Api::namespaced(ctx.client.clone(), cfg_ns);
            let config = cfg_api.get_opt(&c.name).await?.ok_or_else(|| {
                Error::MissingDependency(format!("SnapshotPolicy {cfg_ns}/{}", c.name))
            })?;
            let repo = resolve_restore_repository(ctx, restore, namespace).await?;
            let identity = crate::snapshot_policy::config_identity(
                &config,
                cfg_ns,
                repo.identity_defaults.as_ref(),
            )?;
            let snapshots = list_for_identity(
                ctx,
                &repo,
                namespace,
                &identity.username,
                &identity.hostname,
                identity.source_path.as_deref(),
            )
            .await?;
            let snapshots = filter_as_of(snapshots, c.as_of.as_deref())?;
            Ok(pick_offset(snapshots, c.offset).map(|sid| ResolvedSource {
                kopia_snapshot_id: sid,
                snapshot_ref: None,
                identity: Some(identity),
            }))
        }
    }
}

/// Keep only snapshots taken at or before `asOf` (point-in-time selection,
/// applied BEFORE `offset` so the two compose: "the previous one as of last
/// Tuesday"). `None` keeps the full list. The webhook validates the format at
/// admission; re-parsing here is defensive (one validator, two callers).
fn filter_as_of(
    mut snapshots: Vec<kopiur_kopia::SnapshotListEntry>,
    as_of: Option<&str>,
) -> Result<Vec<kopiur_kopia::SnapshotListEntry>> {
    let Some(s) = as_of else {
        return Ok(snapshots);
    };
    let cutoff = chrono::DateTime::parse_from_rfc3339(s)
        .map_err(|e| {
            Error::Validation(format!(
                "source asOf {s:?} is not an RFC3339 timestamp; use e.g. \
                 2026-05-01T00:00:00Z (the newest snapshot at or before this instant \
                 is restored): {e}"
            ))
        })?
        .with_timezone(&chrono::Utc);
    snapshots.retain(|e| e.end_time <= cutoff);
    Ok(snapshots)
}

/// Seconds left in the `waitTimeout` window that started at the Restore's
/// creation, or `None` when no (parseable) window is configured or it has
/// elapsed. Pure, clock-free — unit-tested without a cluster.
pub fn wait_remaining_secs(
    created_epoch: i64,
    wait_timeout: Option<&str>,
    now_epoch: i64,
) -> Option<u64> {
    let timeout = crate::snapshot_schedule::parse_go_duration(wait_timeout?)?;
    let deadline = created_epoch.saturating_add(timeout.as_secs().try_into().ok()?);
    (now_epoch < deadline).then(|| (deadline - now_epoch) as u64)
}

/// kopia snapshot list filtered to one identity (filesystem in-process path),
/// newest-first.
async fn list_for_identity(
    ctx: &Context,
    repo: &ResolvedRepository,
    namespace: &str,
    username: &str,
    hostname: &str,
    source_path: Option<&str>,
) -> Result<Vec<kopiur_kopia::SnapshotListEntry>> {
    use kopiur_api::backend::Backend;
    let creds = io::repo_credentials(&repo.encryption);
    match &repo.backend {
        Backend::Filesystem(fs) => {
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
            let filter = kopiur_kopia::SnapshotSource {
                host: hostname.to_string(),
                user_name: username.to_string(),
                path: source_path.unwrap_or("").to_string(),
            };
            let mut list = client.snapshot_list(Some(&filter)).await?;
            list.sort_by_key(|e| std::cmp::Reverse(e.end_time));
            Ok(list)
        }
        // In-process snapshot listing needs a locally mounted repo; object-store
        // backends cannot be listed here. Fail LOUDLY with the fix (snapshotRef or
        // a pinned snapshotID) instead of returning an empty list that would read
        // as "no snapshots" and silently Continue/Fail. Exhaustive so a new
        // backend must decide its resolution story before it compiles.
        b @ (Backend::S3(_)
        | Backend::Azure(_)
        | Backend::Gcs(_)
        | Backend::B2(_)
        | Backend::Sftp(_)
        | Backend::WebDav(_)
        | Backend::Rclone(_)) => Err(Error::UnsupportedSourceResolution {
            backend: b.kind_str(),
        }),
    }
}

/// Pick the snapshot at `offset` (0 = newest) from a newest-first list.
fn pick_offset(snapshots: Vec<kopiur_kopia::SnapshotListEntry>, offset: i64) -> Option<String> {
    let idx = offset.max(0) as usize;
    snapshots.into_iter().nth(idx).map(|e| e.id)
}

/// Derive the repository a `Snapshot` belongs to, for `Restore.spec.repository`
/// derivation (the CRD documents `repository` as derived-from-source for
/// `snapshotRef`). The pure rule lives in the api crate
/// ([`kopiur_api::snapshot::repository_ref_for`], with its tests) because the
/// `kubectl kopiur` browse data-plane shares it; re-exported here for
/// controller callers.
pub(crate) use kopiur_api::snapshot::repository_ref_for as repository_ref_from_snapshot;

/// Resolve the repository a restore targets: explicit `spec.repository`, or
/// derived from the source — the snapshotRef'd Snapshot's pinned/owning
/// repository, or the fromPolicy policy's repository. Only `source.identity`
/// has nothing to derive from and requires the explicit field.
async fn resolve_restore_repository(
    ctx: &Context,
    restore: &Restore,
    namespace: &str,
) -> Result<ResolvedRepository> {
    // Explicit `spec.repository` wins. Honors `kind` (namespaced vs.
    // ClusterRepository) via the shared resolver (ADR §5.5).
    if let Some(rref) = &restore.spec.repository {
        return io::resolve_repository_ref(&ctx.client, rref, namespace).await;
    }
    // SnapshotRef: derive from the referenced Snapshot (pinned resolved
    // repository for produced, owning repository for discovered).
    if let RestoreSource::SnapshotRef(sref) = &restore.spec.source {
        let snap_ns = sref.namespace.as_deref().unwrap_or(namespace);
        let snap_api: Api<Snapshot> = Api::namespaced(ctx.client.clone(), snap_ns);
        let snap = snap_api
            .get_opt(&sref.name)
            .await?
            .ok_or_else(|| Error::MissingDependency(format!("Snapshot {snap_ns}/{}", sref.name)))?;
        let rref = repository_ref_from_snapshot(&snap).ok_or_else(|| {
            Error::Validation(format!(
                "cannot derive the repository from Snapshot {snap_ns}/{}: it has neither a \
                 pinned status.resolved.repository nor a Repository/ClusterRepository owner; \
                 set restore.spec.repository explicitly",
                sref.name
            ))
        })?;
        // Resolved relative to the SNAPSHOT's namespace (an absent ref
        // namespace means "same as the snapshot", not "same as the restore").
        return io::resolve_repository_ref(&ctx.client, &rref, snap_ns).await;
    }
    // FromPolicy: resolve via the SnapshotPolicy's repository.
    if let RestoreSource::FromPolicy(c) = &restore.spec.source {
        use kopiur_api::SnapshotPolicy;
        let cfg_ns = c.namespace.as_deref().unwrap_or(namespace);
        let cfg_api: Api<SnapshotPolicy> = Api::namespaced(ctx.client.clone(), cfg_ns);
        let config = cfg_api.get_opt(&c.name).await?.ok_or_else(|| {
            Error::MissingDependency(format!("SnapshotPolicy {cfg_ns}/{}", c.name))
        })?;
        return io::resolve_repository_ref(&ctx.client, &config.spec.repository, cfg_ns).await;
    }
    Err(Error::Validation(
        "restore with source.identity requires spec.repository (snapshotRef and fromPolicy \
         sources derive it; a raw identity has nothing to derive from)"
            .into(),
    ))
}

/// Map a resolved repository backend to the mover connect spec for a restore.
fn restore_connect(repo: &ResolvedRepository) -> Result<RepositoryConnect> {
    crate::snapshot::repository_connect_pub(repo)
}

/// Mover `Job` limits from the restore's `failurePolicy`, falling back to ADR
/// defaults. Mirrors `snapshot::job_limits`; TTL stays unset so the one-Job-per-CR is
/// reaped by owner-reference GC when the `Restore` is deleted.
fn restore_job_limits(restore: &Restore) -> JobLimits {
    match &restore.spec.failure_policy {
        Some(fp) => JobLimits {
            backoff_limit: fp.backoff_limit.unwrap_or(2),
            active_deadline_seconds: fp.active_deadline_seconds,
            ..JobLimits::default()
        },
        None => JobLimits::default(),
    }
}

/// Build a Kubernetes condition object.
fn condition(type_: &str, status: &str, reason: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "type": type_,
        "status": status,
        "reason": reason,
        "message": message,
        "lastTransitionTime": chrono::Utc::now().to_rfc3339(),
        "observedGeneration": 0,
    })
}

/// `error_policy` for the `Restore` controller.
pub fn error_policy(obj: Arc<Restore>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("Restore", obj.as_ref(), err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::common::ObjectRef;
    use kopiur_api::restore::{FromPolicy, IdentitySource};

    // The repository-derivation tests moved to `kopiur_api::snapshot` with the
    // pure fn (`repository_ref_for`); the browse data-plane shares it.

    fn job_with_times(start: Option<&str>, end: Option<&str>) -> k8s_openapi::api::batch::v1::Job {
        use k8s_openapi::api::batch::v1::{Job, JobStatus};
        let parse = |s: &str| serde_json::from_value(serde_json::json!(s)).unwrap();
        Job {
            status: Some(JobStatus {
                start_time: start.map(parse),
                completion_time: end.map(parse),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn restore_duration_is_completion_minus_start() {
        let job = job_with_times(Some("2024-01-01T00:00:00Z"), Some("2024-01-01T00:01:30Z"));
        assert_eq!(restore_job_duration_seconds(&job), Some(90));
        // Missing completion → None (still running).
        assert_eq!(
            restore_job_duration_seconds(&job_with_times(Some("2024-01-01T00:00:00Z"), None)),
            None
        );
        // Negative interval (clock skew) → None.
        let skew = job_with_times(Some("2024-01-01T00:01:00Z"), Some("2024-01-01T00:00:00Z"));
        assert_eq!(restore_job_duration_seconds(&skew), None);
    }

    fn snapshot_ref() -> RestoreSource {
        RestoreSource::SnapshotRef(ObjectRef {
            name: "b".into(),
            namespace: None,
        })
    }
    fn from_config() -> RestoreSource {
        RestoreSource::FromPolicy(FromPolicy {
            name: "cfg".into(),
            namespace: None,
            as_of: None,
            offset: 0,
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
        assert_eq!(default_on_missing(&snapshot_ref()), OnMissingSnapshot::Fail);
        assert_eq!(default_on_missing(&identity()), OnMissingSnapshot::Fail);
    }

    #[test]
    fn explicit_on_missing_overrides_default() {
        // fromPolicy would default Continue, but an explicit Fail wins.
        assert_eq!(
            effective_on_missing(Some(OnMissingSnapshot::Fail), &from_config()),
            OnMissingSnapshot::Fail
        );
        // snapshotRef defaults Fail, explicit Continue wins.
        assert_eq!(
            effective_on_missing(Some(OnMissingSnapshot::Continue), &snapshot_ref()),
            OnMissingSnapshot::Continue
        );
    }

    #[test]
    fn source_mode_strings_match_each_variant() {
        assert_eq!(source_mode(&snapshot_ref()), "SnapshotRef");
        assert_eq!(source_mode(&from_config()), "FromPolicy");
        assert_eq!(source_mode(&identity()), "Identity");
    }

    fn list_entry(id: &str, end_time: &str) -> kopiur_kopia::SnapshotListEntry {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "source": { "host": "h", "userName": "u", "path": "/data" },
            "startTime": end_time,
            "endTime": end_time,
        }))
        .expect("valid SnapshotListEntry")
    }

    /// Three snapshots, newest-first (the order `list_for_identity` returns).
    fn three_snapshots() -> Vec<kopiur_kopia::SnapshotListEntry> {
        vec![
            list_entry("k3", "2026-06-03T00:00:00Z"),
            list_entry("k2", "2026-06-02T00:00:00Z"),
            list_entry("k1", "2026-06-01T00:00:00Z"),
        ]
    }

    #[test]
    fn filter_as_of_keeps_snapshots_at_or_before_the_instant() {
        // A cutoff between k2 and k3 drops k3 (newer than the instant); k2/k1 remain.
        let kept = filter_as_of(three_snapshots(), Some("2026-06-02T12:00:00Z")).unwrap();
        assert_eq!(
            kept.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
            ["k2", "k1"]
        );
        // Exactly AT a snapshot's endTime keeps it ("at or before").
        let kept = filter_as_of(three_snapshots(), Some("2026-06-02T00:00:00Z")).unwrap();
        assert_eq!(kept.first().map(|e| e.id.as_str()), Some("k2"));
        // Before everything → empty (caller applies onMissingSnapshot).
        let kept = filter_as_of(three_snapshots(), Some("2026-05-01T00:00:00Z")).unwrap();
        assert!(kept.is_empty());
        // No asOf → untouched.
        let kept = filter_as_of(three_snapshots(), None).unwrap();
        assert_eq!(kept.len(), 3);
    }

    #[test]
    fn filter_as_of_rejects_non_rfc3339_with_actionable_message() {
        let err = filter_as_of(three_snapshots(), Some("yesterday")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("yesterday"), "{msg}");
        assert!(msg.contains("RFC3339"), "{msg}");
        assert!(msg.contains("2026-05-01T00:00:00Z"), "{msg}");
    }

    #[test]
    fn as_of_composes_with_offset() {
        // "the previous one as of just after k2": asOf drops k3, offset 1 then
        // steps past k2 to k1.
        let kept = filter_as_of(three_snapshots(), Some("2026-06-02T12:00:00Z")).unwrap();
        assert_eq!(pick_offset(kept, 1), Some("k1".to_string()));
    }

    #[test]
    fn pick_offset_zero_is_newest_and_out_of_range_is_none() {
        assert_eq!(pick_offset(three_snapshots(), 0), Some("k3".to_string()));
        assert_eq!(pick_offset(three_snapshots(), 2), Some("k1".to_string()));
        assert_eq!(pick_offset(three_snapshots(), 3), None);
        // A negative offset clamps to newest rather than panicking.
        assert_eq!(pick_offset(three_snapshots(), -1), Some("k3".to_string()));
    }

    #[test]
    fn wait_remaining_counts_down_from_creation_and_closes() {
        // 5m window, 60s elapsed → 240s left.
        assert_eq!(wait_remaining_secs(1000, Some("5m"), 1060), Some(240));
        // Window exactly elapsed → closed (None), onMissingSnapshot applies.
        assert_eq!(wait_remaining_secs(1000, Some("5m"), 1300), None);
        assert_eq!(wait_remaining_secs(1000, Some("5m"), 1301), None);
        // No waitTimeout configured → no window at all.
        assert_eq!(wait_remaining_secs(1000, None, 1000), None);
        // Unparseable timeout → treated as no window (webhook rejects it at
        // admission; this is the defensive path).
        assert_eq!(wait_remaining_secs(1000, Some("bogus"), 1000), None);
    }

    #[test]
    fn populator_state_depends_on_target_variant() {
        use kopiur_api::PopulatorTarget;
        use kopiur_api::common::ObjectRef;
        use kopiur_api::restore::PvcTemplate;
        // populator target → passive AwaitingClaim.
        assert_eq!(
            populator_state(&RestoreTarget::Populator(PopulatorTarget {})),
            PopulatorState::AwaitingClaim
        );
        // explicit pvc/pvcRef → operator-driven DirectTarget.
        assert_eq!(
            populator_state(&RestoreTarget::PvcRef(ObjectRef {
                name: "data".into(),
                namespace: None,
            })),
            PopulatorState::DirectTarget
        );
        assert_eq!(
            populator_state(&RestoreTarget::Pvc(PvcTemplate {
                name: "created".into(),
                storage_class_name: None,
                capacity: None,
                access_modes: vec![],
            })),
            PopulatorState::DirectTarget
        );
    }
}
