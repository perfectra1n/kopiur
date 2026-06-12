//! The `Maintenance` reconciler (ADR §3.7, §4.5).
//!
//! Maintenance runs in a dedicated **mover pod** for every backend (filesystem
//! and object-store alike), consistent with backup/restore/bootstrap. The
//! controller is the *scheduler*: each reconcile it decides whether a quick or
//! full pass is due (croner + deterministic jitter via
//! [`crate::snapshot_schedule::next_fire`], full subsumes quick), then spawns at
//! most one per-slot mover Job and tracks it to terminal state. The lease
//! decision ([`kopiur_api::lease_action`]) lives in the mover, because reading
//! the current holder (`kopia maintenance info`) needs repo access the
//! controller does not have for object stores.
//!
//! Hardening (see the design doc): per-slot deterministic Job names for
//! idempotency (G1), `ttlSecondsAfterFinished` so finished Jobs self-reap (G2),
//! single-flight via a label selector (G3), a repository-readiness gate (G7),
//! a requeue cap so the lease/health is re-checked (G8), and transition-guarded
//! status writes so the reconcile does not hot-loop on its own status (G6).

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{DeleteParams, ListParams};
use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};

use kopiur_api::{Maintenance, validate};
use kopiur_kopia::MaintenanceMode;
use kopiur_mover::workspec::{
    MaintenanceOp, MoverOptions, MoverWorkSpec, Operation, ResolvedIdentity, TargetRef,
};

use crate::config;
use crate::consts::{
    API_VERSION, COMPONENT_LABEL, MAINTENANCE_COMPONENT, MAINTENANCE_INSTANCE_LABEL,
    MAINTENANCE_SLOT_ANNOTATION,
};
use crate::context::Context;
use crate::error::{Error, Result, error_policy_for};
use crate::io;
use crate::jobs::{self, JobLimits, MoverJobInputs, VolumeMountSpec};
use crate::snapshot::{backend_to_repository_connect, job_terminal_state, mover_pull_policy_pub};
use crate::snapshot_schedule::{next_fire, parse_go_duration};

/// How long a finished maintenance Job lingers before the TTL controller reaps
/// it (G2). Long enough that the controller reliably observes the terminal state
/// on its requeue cadence; short enough that per-slot Jobs do not pile up.
const MAINTENANCE_JOB_TTL_SECS: i64 = 3600;

/// Requeue while a maintenance Job is in flight (poll for terminal state).
const REQUEUE_RUNNING: Duration = Duration::from_secs(30);
/// Requeue while waiting for the repository to become `Ready` (G7).
const REQUEUE_NOT_READY: Duration = Duration::from_secs(60);
/// Requeue after a failed maintenance Job: re-check (and, once the failed Job is
/// TTL-reaped, re-spawn as a bounded retry).
const REQUEUE_FAILED: Duration = Duration::from_secs(300);
/// Upper bound on any requeue, so the lease/health/readiness is re-evaluated even
/// when the next slot is hours away (G8; aligned with the operator heartbeat).
const REQUEUE_CAP: Duration = Duration::from_secs(1800);

/// Reconcile a `Maintenance`.
#[tracing::instrument(skip(maint, ctx), fields(kind = "Maintenance", namespace = %maint.namespace().unwrap_or_default(), name = %maint.name_any()))]
pub async fn reconcile(
    maint: std::sync::Arc<Maintenance>,
    ctx: std::sync::Arc<Context>,
) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&maint, &ctx).await;
    ctx.metrics
        .record_reconcile("Maintenance", start.elapsed().as_secs_f64());
    record_maintenance_status_metrics(&maint, &ctx, result.is_ok()).await;
    result
}

/// Mirror the last full-maintenance reclaimed-bytes gauge from the freshest
/// status on success (Maintenance has no phase gauge to clear). See the Snapshot
/// equivalent for why the status is re-read rather than taken from the cache copy.
async fn record_maintenance_status_metrics(maint: &Maintenance, ctx: &Context, ok: bool) {
    let (Some(ns), name) = (maint.namespace(), maint.name_any()) else {
        return;
    };
    if !ok {
        return;
    }
    let api: Api<Maintenance> = Api::namespaced(ctx.client.clone(), &ns);
    if let Ok(Some(latest)) = api.get_opt(&name).await
        && let Some(bytes) = latest
            .status
            .as_ref()
            .and_then(|s| s.full.as_ref())
            .and_then(|f| f.last_content_reclaimed_bytes)
    {
        ctx.metrics
            .set_maintenance_reclaimed_bytes(&ns, &name, bytes);
    }
}

async fn reconcile_inner(maint: &Maintenance, ctx: &Context) -> Result<Action> {
    let errs = validate::validate_maintenance(&maint.spec);
    if let Some(first) = errs.into_iter().next() {
        return Err(Error::Validation(first.to_string()));
    }

    let namespace = maint
        .namespace()
        .ok_or_else(|| Error::Invariant("Maintenance has no namespace".into()))?;
    let name = maint.name_any();
    let api: Api<Maintenance> = Api::namespaced(ctx.client.clone(), &namespace);

    let repo_ref = &maint.spec.repository;
    let repo = io::resolve_repository_ref(&ctx.client, repo_ref, &namespace).await?;

    // G7: an object-store repository must be bootstrapped (connected/created)
    // before `kopia maintenance` can reach it. Spawning earlier just produces a
    // doomed pod, so wait for the repository to report `Ready`.
    if !io::repository_ready(&ctx.client, repo_ref, &namespace).await? {
        patch_condition_if_changed(
            &api,
            &name,
            maint,
            "False",
            "WaitingForRepository",
            "target repository is not Ready; deferring maintenance",
        )
        .await?;
        return Ok(Action::requeue(REQUEUE_NOT_READY));
    }

    let now = Utc::now();
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), &namespace);

    // The repository is Ready and we got this far: mark Maintenance Ready (ADR-0005
    // §2) so `kubectl wait --for=condition=Ready` works. Transition-guarded so it
    // does not hot-loop on its own status. The mover still owns the `LeaseOwned`
    // condition; `set_ready` upserts Ready/Reconciling/Stalled without clobbering it.
    set_ready_if_changed(&api, &name, maint).await?;

    // An annotation-requested manual run takes precedence over waiting for the
    // next cron slot — but flows through the SAME spawn path (mover, lease,
    // single-flight), so it can't bypass any guarantee the cron path has.
    // A MALFORMED annotation must not suspend scheduled maintenance: surface
    // it as a condition and fall through to the cron flow (degrade-not-crash).
    match manual_run_request(maint) {
        Ok(Some((requested, manual_mode))) => {
            if let Some(action) = handle_manual_run(
                ctx,
                &api,
                &job_api,
                &namespace,
                &name,
                maint,
                &repo,
                requested,
                manual_mode,
            )
            .await?
            {
                return Ok(action);
            }
        }
        Ok(None) => {}
        Err(Error::Validation(msg)) => {
            patch_condition_if_changed(&api, &name, maint, "False", "InvalidRunRequest", &msg)
                .await?;
        }
        Err(e) => return Err(e),
    }

    // Nothing due → sleep until the earliest next slot (capped).
    let Some((mode, slot)) = due_mode(maint, now) else {
        return Ok(Action::requeue(cap(next_wakeup(maint, now, None))));
    };

    let job_name = maintenance_job_name(&name, mode, slot);
    match job_api.get_opt(&job_name).await? {
        Some(job) => match job_terminal_state(&job) {
            // Succeeded: the slot was handled (the mover ran maintenance, or
            // yielded the lease and recorded a condition). Record that DURABLY in
            // `status.<mode>.lastHandledSlot` — the Job self-reaps via its TTL,
            // and a yield does not advance `lastRunAt`, so without the durable
            // marker the same slot re-fired after every TTL reap (a yield Job per
            // hour, forever). The work-spec ConfigMap is only needed while the
            // pod runs — drop it so per-slot ConfigMaps do not accumulate. Then
            // sleep until the next slot.
            Some(true) => {
                record_handled_slot(&api, &name, maint, mode, slot, now).await?;
                delete_work_spec_cm(ctx, &namespace, &job_name).await;
                Ok(Action::requeue(cap(next_wakeup(
                    maint,
                    now,
                    Some((mode, slot)),
                ))))
            }
            // Failed: surface the condition once (transition-guarded) and re-check.
            // The failed Job lingers until its TTL, then a fresh reconcile
            // re-spawns this slot as a bounded retry.
            Some(false) => {
                patch_condition_if_changed(
                    &api,
                    &name,
                    maint,
                    "False",
                    "MaintenanceFailed",
                    "maintenance Job failed; see the Job/pod logs",
                )
                .await?;
                Ok(Action::requeue(REQUEUE_FAILED))
            }
            // Still running: poll.
            None => Ok(Action::requeue(REQUEUE_RUNNING)),
        },
        None => {
            // G3: never run two maintenance Jobs for one repository at once.
            if has_active_maintenance_job(&job_api, &name).await? {
                return Ok(Action::requeue(REQUEUE_RUNNING));
            }
            spawn_maintenance_job(ctx, &namespace, &name, &job_name, maint, &repo, mode, slot)
                .await?;
            tracing::info!(maint = %name, ?mode, slot = %slot.to_rfc3339(), "spawned maintenance Job");
            Ok(Action::requeue(REQUEUE_RUNNING))
        }
    }
}

/// What the run-now annotations ask for, when there is an UNHANDLED request:
/// the pinned request timestamp + the run kind. Pure (ADR §5.2).
///
/// `None` when no annotation is present, or when `status.manualRun` already
/// answers this exact `requestedAt` (any phase — Running means a Job exists or
/// existed; the reconcile body resolves that separately). Unparseable values
/// are validation errors (the annotation is user input).
pub fn manual_run_request(
    maint: &Maintenance,
) -> Result<Option<(DateTime<Utc>, kopiur_api::ManualRunMode)>> {
    let annotations = maint.metadata.annotations.as_ref();
    let Some(raw) = annotations.and_then(|a| a.get(crate::consts::RUN_REQUESTED_ANNOTATION)) else {
        return Ok(None);
    };
    // Dedupe BEFORE parsing: a terminally-answered request stays answered even
    // if someone later scribbles garbage into the mode annotation.
    let answered = maint.status.as_ref().and_then(|st| st.manual_run.as_ref());
    if let Some(m) = answered
        && m.requested_at.as_deref() == Some(raw)
        && matches!(
            m.phase,
            Some(kopiur_api::ManualRunPhase::Succeeded | kopiur_api::ManualRunPhase::Failed)
        )
    {
        return Ok(None);
    }
    // Shared parse (also enforced at admission by the webhook) — one
    // validator, two callers.
    match kopiur_api::maintenance::parse_run_annotations(annotations) {
        Ok(request) => Ok(request),
        Err(msg) => Err(Error::Validation(msg)),
    }
}

/// Map the api-level manual mode onto the mover's maintenance mode. Exhaustive.
fn mover_mode(mode: kopiur_api::ManualRunMode) -> MaintenanceMode {
    match mode {
        kopiur_api::ManualRunMode::Quick => MaintenanceMode::Quick,
        kopiur_api::ManualRunMode::Full => MaintenanceMode::Full,
    }
}

/// Patch `status.manualRun` from the TYPED struct (never hand-written field
/// names — the structural schema silently prunes typos).
async fn patch_manual_run(
    api: &Api<Maintenance>,
    name: &str,
    manual: kopiur_api::ManualRunStatus,
) -> Result<()> {
    io::patch_status(
        api,
        name,
        serde_json::json!({ "manualRun": serde_json::to_value(&manual)? }),
    )
    .await
}

/// Drive an unhandled manual request: observe/spawn its Job, book-keep
/// `status.manualRun`. Returns `Some(action)` when the reconcile should stop
/// here (a manual Job is in flight), `None` to continue with the cron flow.
#[allow(clippy::too_many_arguments)]
async fn handle_manual_run(
    ctx: &Context,
    api: &Api<Maintenance>,
    job_api: &Api<Job>,
    namespace: &str,
    name: &str,
    maint: &Maintenance,
    repo: &io::ResolvedRepository,
    requested: DateTime<Utc>,
    mode: kopiur_api::ManualRunMode,
) -> Result<Option<Action>> {
    let requested_raw = requested.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    // The annotation value as the user wrote it (status pins it verbatim).
    let annotation_value = maint
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(crate::consts::RUN_REQUESTED_ANNOTATION))
        .cloned()
        .unwrap_or(requested_raw);
    let job_name = manual_job_name(name, mode, requested);
    let already_running = maint
        .status
        .as_ref()
        .and_then(|st| st.manual_run.as_ref())
        .is_some_and(|m| {
            m.requested_at.as_deref() == Some(annotation_value.as_str())
                && m.mode == Some(mode)
                && m.phase == Some(kopiur_api::ManualRunPhase::Running)
        });
    match job_api.get_opt(&job_name).await? {
        Some(job) => match job_terminal_state(&job) {
            Some(true) => {
                patch_manual_run(
                    api,
                    name,
                    kopiur_api::ManualRunStatus {
                        requested_at: Some(annotation_value),
                        mode: Some(mode),
                        phase: Some(kopiur_api::ManualRunPhase::Succeeded),
                        completed_at: Some(Utc::now().to_rfc3339()),
                    },
                )
                .await?;
                delete_work_spec_cm(ctx, namespace, &job_name).await;
                Ok(None)
            }
            Some(false) => {
                patch_manual_run(
                    api,
                    name,
                    kopiur_api::ManualRunStatus {
                        requested_at: Some(annotation_value),
                        mode: Some(mode),
                        phase: Some(kopiur_api::ManualRunPhase::Failed),
                        completed_at: Some(Utc::now().to_rfc3339()),
                    },
                )
                .await?;
                patch_condition_if_changed(
                    api,
                    name,
                    maint,
                    "False",
                    "MaintenanceFailed",
                    "manual maintenance Job failed; see the Job/pod logs",
                )
                .await?;
                Ok(None)
            }
            None => Ok(Some(Action::requeue(REQUEUE_RUNNING))),
        },
        None if already_running => {
            // The Job was TTL-reaped before its terminal state was observed:
            // be honest rather than silently re-running side-effectful work.
            patch_manual_run(
                api,
                name,
                kopiur_api::ManualRunStatus {
                    requested_at: Some(annotation_value),
                    mode: Some(mode),
                    phase: Some(kopiur_api::ManualRunPhase::Failed),
                    completed_at: Some(Utc::now().to_rfc3339()),
                },
            )
            .await?;
            patch_condition_if_changed(
                api,
                name,
                maint,
                "False",
                "ManualRunOutcomeLost",
                "the manual maintenance Job disappeared before its outcome was observed \
                 (TTL-reaped?); re-annotate to run again",
            )
            .await?;
            Ok(None)
        }
        None => {
            // G3 single-flight: never two maintenance Jobs for one repository.
            if has_active_maintenance_job(job_api, name).await? {
                return Ok(Some(Action::requeue(REQUEUE_RUNNING)));
            }
            spawn_maintenance_job(
                ctx,
                namespace,
                name,
                &job_name,
                maint,
                repo,
                mover_mode(mode),
                requested,
            )
            .await?;
            patch_manual_run(
                api,
                name,
                kopiur_api::ManualRunStatus {
                    requested_at: Some(annotation_value),
                    mode: Some(mode),
                    phase: Some(kopiur_api::ManualRunPhase::Running),
                    completed_at: None,
                },
            )
            .await?;
            tracing::info!(maint = %name, ?mode, requested = %requested.to_rfc3339(), "spawned MANUAL maintenance Job");
            Ok(Some(Action::requeue(REQUEUE_RUNNING)))
        }
    }
}

/// Deterministic name for a MANUAL run's Job: same shape as the cron slots but
/// with distinct `mq`/`mf` tokens, so a manual run at second X can never
/// collide with a cron slot at second X.
fn manual_job_name(cr: &str, mode: kopiur_api::ManualRunMode, requested: DateTime<Utc>) -> String {
    const MAX: usize = 52;
    let m = match mode {
        kopiur_api::ManualRunMode::Quick => "mq",
        kopiur_api::ManualRunMode::Full => "mf",
    };
    let suffix = format!("-{m}-{}", requested.timestamp());
    let budget = MAX.saturating_sub(suffix.len());
    if cr.len() <= budget {
        format!("{cr}{suffix}")
    } else {
        let hash = short_hash(cr); // 8 hex chars
        let keep = budget.saturating_sub(hash.len() + 1); // room for "-<hash>"
        let head: String = cr.chars().take(keep).collect();
        format!("{head}-{hash}{suffix}")
    }
}

/// Build + apply the per-slot work-spec ConfigMap and mover Job.
#[allow(clippy::too_many_arguments)]
async fn spawn_maintenance_job(
    ctx: &Context,
    namespace: &str,
    cr_name: &str,
    job_name: &str,
    maint: &Maintenance,
    repo: &io::ResolvedRepository,
    mode: MaintenanceMode,
    slot: DateTime<Utc>,
) -> Result<()> {
    // Effective cache config (repository cacheDefaults overlaid by this Maintenance's
    // mover.cache, ADR §3.1) drives both the connect budgets and the cache volume.
    let effective_cache = crate::cache::effective_cache(
        repo,
        maint.spec.mover.as_ref().and_then(|m| m.cache.as_ref()),
    );
    let cache = crate::cache::cache_tuning(effective_cache.as_ref());
    let work_spec = MoverWorkSpec {
        version: 1,
        operation: Operation::Maintenance(MaintenanceOp {
            mode,
            owner: maint.spec.ownership.owner.clone(),
            takeover_policy: maint.spec.ownership.takeover_policy,
        }),
        // Maintenance does not snapshot, so the identity is a stable sentinel
        // (like bootstrap's) — it is not a kopia snapshot source.
        identity: ResolvedIdentity {
            username: "kopiur-maintenance".to_string(),
            hostname: namespace.to_string(),
            source_path: String::new(),
        },
        repository: backend_to_repository_connect(&repo.backend),
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "Maintenance".to_string(),
            name: cr_name.to_string(),
            namespace: namespace.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
        cache,
        // Apply the repo throttle to the maintenance connection too (§13(e)).
        throttle: io::throttle_spec(repo.mover_defaults.as_ref()),
    };

    let mut labels = BTreeMap::new();
    labels.insert(
        COMPONENT_LABEL.to_string(),
        MAINTENANCE_COMPONENT.to_string(),
    );
    labels.insert(MAINTENANCE_INSTANCE_LABEL.to_string(), cr_name.to_string());

    let mut annotations = BTreeMap::new();
    annotations.insert(MAINTENANCE_SLOT_ANNOTATION.to_string(), slot.to_rfc3339());

    // Filesystem repos need the repo volume (PVC or inline NFS) mounted read-write;
    // object stores reach the backend over the network (creds via env), so none.
    let repo_volume =
        io::filesystem_repo_mount_source(&repo.backend).map(|source| VolumeMountSpec {
            source,
            mount_path: io::filesystem_repo_path(&repo.backend).unwrap_or_default(),
            read_only: false,
        });
    let owner = io::owner_ref_for(maint, "Maintenance")?;

    // The maintenance mover Job runs in this namespace as the dedicated mover SA
    // or the user's workload-identity SA (not the operator SA, which does not
    // exist here). Resolve the run identity FIRST — before resolving credentials,
    // which can fail (e.g. a ClusterRepository's Secret is absent here and
    // projection is off). The identity is a prerequisite independent of creds;
    // establishing it first means a missing-creds retry still leaves the RBAC in
    // place, and every other mover path (Snapshot/Restore/bootstrap) establishes
    // this too. Without it the Job FailedCreates with `serviceaccount ... not
    // found` and never schedules a pod (ADR §4.12).
    let mover_identity = io::ensure_mover_identity(
        &ctx.client,
        namespace,
        &[&repo.backend],
        ctx.mover_service_account.as_deref(),
        &ctx.mover_role_kind,
        &ctx.mover_clusterrole,
    )
    .await?;

    // Resolve the credential Secret(s) the mover loads via envFrom. A maintenance
    // Job for a ClusterRepository lands in a namespace that often lacks the source
    // Secret, so projection (`spec.credentialProjection`) is especially useful here:
    // the operator copies it in, owned by this Maintenance. Errors propagate as
    // MissingDependency (Transient) and the run requeues.
    let creds = io::resolve_mover_creds_for(
        &ctx.client,
        namespace,
        job_name,
        &owner,
        repo,
        maint
            .spec
            .credential_projection
            .as_ref()
            .is_some_and(|p| p.enabled),
        io::repo_kind_str(maint.spec.repository.kind),
        &maint.spec.repository.name,
    )
    .await?;
    if creds.projected > 0 {
        ctx.metrics
            .inc_secrets_projected(namespace, creds.projected);
    }
    let creds_secrets = creds.names;

    // Resolve the cache VOLUME; a persistent cache PVC is owned by this Maintenance.
    let cache_volume = crate::cache::resolve_cache_volume(
        &ctx.client,
        namespace,
        owner.clone(),
        &format!("kopiur-cache-{cr_name}"),
        effective_cache.as_ref(),
    )
    .await?;

    // Resolve the maintenance mover's effective security context (explicit or inherited)
    // and merge the repository's moverDefaults under it (`hardened ⊂ moverDefaults ⊂
    // recipe`, ADR-0004 §1/§2). Previously the maintenance mover passed its raw
    // securityContext and ran NO privileged gate — both fixed here so a maintenance
    // mover is hardened/gated exactly like backup/restore (and inherits moverDefaults,
    // closing the drift the ClusterRepository hardcoded-context bug caused).
    let (effective_sc, effective_pod_sc) =
        io::resolve_mover_security_contexts(&ctx.client, namespace, maint.spec.mover.as_ref())
            .await?;
    let resolved_mover = kopiur_api::common::resolve_mover(
        repo.mover_defaults.as_ref(),
        effective_sc.as_ref(),
        effective_pod_sc.as_ref(),
        maint.spec.mover.as_ref().and_then(|m| m.resources.as_ref()),
        maint.spec.mover.as_ref().and_then(|m| m.cache.as_ref()),
        maint
            .spec
            .mover
            .as_ref()
            .and_then(|m| m.ttl_seconds_after_finished),
    );
    let privileged_mode = maint.spec.mover.as_ref().and_then(|m| m.privileged_mode);
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
        let msg = io::privileged_mover_message("Maintenance", cr_name, namespace, sa);
        tracing::warn!(maintenance = %cr_name, namespace = %namespace, "{msg}; skipping maintenance run");
        return Ok(());
    }

    let mut limits = maintenance_job_limits(maint);
    if let Some(ttl) = resolved_mover.ttl_seconds_after_finished {
        limits.ttl_seconds_after_finished = Some(ttl);
    }
    mover_identity.decorate_labels(&mut labels);
    let inputs = MoverJobInputs {
        name: job_name,
        namespace,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: mover_pull_policy_pub(),
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
        service_account: mover_identity.service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
        annotations,
        cache_volume,
        readiness_exec: None,
    };

    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, namespace, job_name, &cm, &job).await?;
    Ok(())
}

/// Job limits for a maintenance run: backoff/deadline from `failurePolicy`
/// (falling back to defaults), plus a TTL so finished per-slot Jobs self-reap.
fn maintenance_job_limits(maint: &Maintenance) -> JobLimits {
    let base = JobLimits::default();
    match &maint.spec.failure_policy {
        Some(fp) => JobLimits {
            backoff_limit: fp.backoff_limit.unwrap_or(base.backoff_limit),
            active_deadline_seconds: fp.active_deadline_seconds,
            ttl_seconds_after_finished: Some(MAINTENANCE_JOB_TTL_SECS),
        },
        None => JobLimits {
            ttl_seconds_after_finished: Some(MAINTENANCE_JOB_TTL_SECS),
            ..base
        },
    }
}

/// Choose the maintenance pass due now, preferring full (it subsumes quick).
/// Returns the mode and its scheduled slot, or `None` if nothing is due.
fn due_mode(maint: &Maintenance, now: DateTime<Utc>) -> Option<(MaintenanceMode, DateTime<Utc>)> {
    for mode in [MaintenanceMode::Full, MaintenanceMode::Quick] {
        if let Ok(slot) = slot_for(maint, mode, mode_after(maint, mode))
            && now >= slot
        {
            return Some((mode, slot));
        }
    }
    None
}

/// The instant after which to search for `mode`'s next slot: the later of its
/// last *run* (mover actually ran maintenance) and its last *handled*
/// observation (terminal-success Job seen — covers yields, which deliberately
/// do not move `lastRunAt`). Falls back to a year ago so the first-ever
/// reconcile fires immediately.
fn mode_after(maint: &Maintenance, mode: MaintenanceMode) -> DateTime<Utc> {
    match (last_run_at(maint, mode), last_handled_at(maint, mode)) {
        (Some(run), Some(handled)) => run.max(handled),
        (Some(run), None) => run,
        (None, Some(handled)) => handled,
        (None, None) => Utc::now() - chrono::Duration::days(365),
    }
}

/// The next cron slot for `mode` strictly after `after` (croner + jitter, seeded
/// by the CR UID for a stable per-replica spread).
fn slot_for(
    maint: &Maintenance,
    mode: MaintenanceMode,
    after: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    let seed = maint.uid().unwrap_or_else(|| maint.name_any());
    let spec = match mode {
        MaintenanceMode::Quick => &maint.spec.schedule.quick,
        MaintenanceMode::Full => &maint.spec.schedule.full,
    };
    let jitter = spec.jitter.as_deref().and_then(parse_go_duration);
    next_fire(&spec.cron, jitter, &seed, after)
}

/// Parse `status.<mode>.lastRunAt` (RFC3339) into a `DateTime<Utc>`.
fn last_run_at(maint: &Maintenance, mode: MaintenanceMode) -> Option<DateTime<Utc>> {
    let status = maint.status.as_ref()?;
    let run = match mode {
        MaintenanceMode::Quick => status.quick.as_ref(),
        MaintenanceMode::Full => status.full.as_ref(),
    }?;
    run.last_run_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// Parse `status.<mode>.lastHandledAt` (RFC3339) into a `DateTime<Utc>`.
fn last_handled_at(maint: &Maintenance, mode: MaintenanceMode) -> Option<DateTime<Utc>> {
    let status = maint.status.as_ref()?;
    let run = match mode {
        MaintenanceMode::Quick => status.quick.as_ref(),
        MaintenanceMode::Full => status.full.as_ref(),
    }?;
    run.last_handled_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// Durably record that `mode`'s cron `slot` was handled to a terminal-success
/// Job (real run OR yield). Merge-patches only the per-mode `lastHandledAt`
/// (the mover owns `lastRunAt`/stats), guarded so repeat reconciles of the same
/// finished Job are status no-ops (G6) — the stamp only moves when a NEWER slot
/// than the recorded anchor was handled.
///
/// The recorded value is `now` (the observation instant), NOT the slot: a
/// first-ever slot sits ~a year in the past (the [`mode_after`] lookback), and
/// anchoring there would leave the next slot still in the past — a yield-only
/// Maintenance would march through the whole historic backlog one Job at a
/// time. Stamping `now` gives the same catch-up-once semantics as the mover's
/// `lastRunAt = now` on a real run.
async fn record_handled_slot(
    api: &Api<Maintenance>,
    name: &str,
    maint: &Maintenance,
    mode: MaintenanceMode,
    slot: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<()> {
    // Already anchored past this slot (this finished Job was seen before) —
    // a repeat observation must not churn status.
    if last_handled_at(maint, mode).is_some_and(|at| at >= slot) {
        return Ok(());
    }
    let mode_key = match mode {
        MaintenanceMode::Quick => "quick",
        MaintenanceMode::Full => "full",
    };
    io::patch_status(
        api,
        name,
        serde_json::json!({ mode_key: { "lastHandledAt": now.to_rfc3339() } }),
    )
    .await
}

/// How long until the controller should reconcile again. When `handled` is set,
/// that mode's clock is advanced past the just-handled slot (so a *yield*, which
/// does not move `lastRunAt`, still doesn't immediately re-fire the same slot);
/// the other mode is measured from its own `lastRunAt`. The result is floored at
/// the running cadence and capped by the caller.
fn next_wakeup(
    maint: &Maintenance,
    now: DateTime<Utc>,
    handled: Option<(MaintenanceMode, DateTime<Utc>)>,
) -> Duration {
    let mut earliest: Option<DateTime<Utc>> = None;
    for mode in [MaintenanceMode::Quick, MaintenanceMode::Full] {
        let after = match handled {
            Some((hm, hs)) if hm == mode => hs,
            _ => mode_after(maint, mode),
        };
        if let Ok(slot) = slot_for(maint, mode, after) {
            earliest = Some(earliest.map_or(slot, |e| e.min(slot)));
        }
    }
    match earliest {
        Some(slot) if slot > now => (slot - now)
            .to_std()
            .unwrap_or(REQUEUE_CAP)
            .max(REQUEUE_RUNNING),
        // A slot is already due (or schedules failed to parse): re-check soon.
        _ => REQUEUE_RUNNING,
    }
}

/// Cap a requeue so the lease/health/readiness is re-evaluated even when the next
/// slot is far out (G8).
fn cap(d: Duration) -> Duration {
    d.min(REQUEUE_CAP)
}

/// Deterministic, ≤52-char, DNS-1123-safe Job name for a maintenance slot (G1).
/// `<cr>-<q|f>-<unix_slot>`, truncating the CR component and appending a stable
/// hash when the full name would overflow (the Job name is copied into the
/// 63-char `job-name` label and the Job controller suffixes pod names).
fn maintenance_job_name(cr: &str, mode: MaintenanceMode, slot: DateTime<Utc>) -> String {
    const MAX: usize = 52;
    let m = match mode {
        MaintenanceMode::Quick => "q",
        MaintenanceMode::Full => "f",
    };
    let suffix = format!("-{m}-{}", slot.timestamp());
    let budget = MAX.saturating_sub(suffix.len());
    if cr.len() <= budget {
        format!("{cr}{suffix}")
    } else {
        let hash = short_hash(cr); // 8 hex chars
        let keep = budget.saturating_sub(hash.len() + 1); // room for "-<hash>"
        let trunc: String = cr.chars().take(keep).collect();
        format!("{trunc}-{hash}{suffix}")
    }
}

/// A short, stable (run-independent) 8-hex-char FNV-1a hash for name truncation.
fn short_hash(s: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{:08x}", (h & 0xffff_ffff))
}

/// Whether any non-terminal maintenance Job is owned by this `Maintenance` CR
/// (the single-flight gate, G3).
async fn has_active_maintenance_job(job_api: &Api<Job>, cr_name: &str) -> Result<bool> {
    let selector =
        format!("{COMPONENT_LABEL}={MAINTENANCE_COMPONENT},{MAINTENANCE_INSTANCE_LABEL}={cr_name}");
    let jobs = job_api
        .list(&ListParams::default().labels(&selector))
        .await?;
    Ok(jobs.items.iter().any(|j| job_terminal_state(j).is_none()))
}

/// Best-effort delete of a per-slot work-spec ConfigMap once its Job is done.
async fn delete_work_spec_cm(ctx: &Context, namespace: &str, name: &str) {
    let api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
    if let Err(e) = api.delete(name, &DeleteParams::default()).await {
        tracing::debug!(error = %e, configmap = %name, "work-spec ConfigMap cleanup failed (ignored)");
    }
}

/// Patch the single `LeaseOwned` condition only when its status/reason actually
/// changes, so the controller does not hot-loop on its own status writes (G6).
async fn patch_condition_if_changed(
    api: &Api<Maintenance>,
    name: &str,
    maint: &Maintenance,
    status: &str,
    reason: &str,
    message: &str,
) -> Result<()> {
    let unchanged = maint
        .status
        .as_ref()
        .map(|s| &s.conditions)
        .and_then(|cs| cs.iter().find(|c| c.type_ == "LeaseOwned"))
        .is_some_and(|c| c.status == status && c.reason == reason);
    if unchanged {
        return Ok(());
    }
    let observed_gen = maint.metadata.generation.unwrap_or(0);
    io::patch_status(
        api,
        name,
        serde_json::json!({
            "observedGeneration": observed_gen,
            "conditions": [{
                "type": "LeaseOwned",
                "status": status,
                "reason": reason,
                "message": message,
                "lastTransitionTime": Utc::now().to_rfc3339(),
                "observedGeneration": observed_gen,
            }],
        }),
    )
    .await?;
    Ok(())
}

/// Upsert the kstatus `Ready` conditions (ADR-0005 §2) for a `Maintenance` that has
/// reached a healthy reconciled state (its repository is Ready), but only when the
/// `Ready` condition actually changes — so the controller does not hot-loop on its
/// own status writes (G6). Preserves the mover-owned `LeaseOwned` condition via
/// [`io::set_ready`]'s upsert.
async fn set_ready_if_changed(
    api: &Api<Maintenance>,
    name: &str,
    maint: &Maintenance,
) -> Result<()> {
    let existing: Vec<_> = maint
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    // A mover-recorded lease block (`LeaseOwned=False` with a lease-holder
    // reason) means runs are *yielding*: kopia's recorded maintenance owner is a
    // foreign identity and the takeover policy forbids claiming it. Repository
    // GC/compaction is NOT happening and waiting cannot fix it, so surface
    // Stalled (kstatus, ADR-0005 §2) with the remediation instead of a
    // misleading Ready=True. Other `LeaseOwned=False` reasons (e.g.
    // WaitingForRepository, MaintenanceFailed) keep their own flows.
    let lease_blocked = existing
        .iter()
        .find(|c| {
            c.type_ == kopiur_api::maintenance::LEASE_OWNED_CONDITION
                && c.status == "False"
                && (c.reason == kopiur_api::maintenance::LEASE_HELD_BY_OTHER_REASON
                    || c.reason == kopiur_api::maintenance::LEASE_TAKEOVER_PROMPT_REASON)
        })
        .map(|c| c.message.clone());
    let (outcome, reason, message) = match &lease_blocked {
        Some(holder) => (
            io::ReadyOutcome::Stalled,
            "MaintenanceYielding",
            format!(
                "maintenance Jobs are yielding without running ({holder}); repository \
                 GC/compaction is not happening. Fix: set \
                 spec.ownership.takeoverPolicy=Force once so the operator claims kopia's \
                 maintenance ownership, then revert it"
            ),
        ),
        None => (
            io::ReadyOutcome::Ready,
            "Reconciled",
            "maintenance is reconciled; the repository is Ready".to_string(),
        ),
    };
    // Transition guard (G6): only write when Ready does not already reflect
    // this outcome + reason.
    let desired_ready = match outcome {
        io::ReadyOutcome::Ready => "True",
        io::ReadyOutcome::Reconciling | io::ReadyOutcome::Stalled => "False",
    };
    let unchanged = existing
        .iter()
        .find(|c| c.type_ == "Ready")
        .is_some_and(|c| c.status == desired_ready && c.reason == reason);
    if unchanged {
        return Ok(());
    }
    let observed_gen = maint.metadata.generation.unwrap_or(0);
    let conditions = io::set_ready(&existing, Some(observed_gen), outcome, reason, &message);
    io::patch_status(
        api,
        name,
        serde_json::json!({
            "observedGeneration": observed_gen,
            "conditions": conditions,
        }),
    )
    .await?;
    Ok(())
}

/// `error_policy` for the `Maintenance` controller.
pub fn error_policy(
    obj: std::sync::Arc<Maintenance>,
    err: &Error,
    ctx: std::sync::Arc<Context>,
) -> Action {
    error_policy_for("Maintenance", obj.as_ref(), err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::common::{CronSpec, RepositoryKind, RepositoryRef};
    use kopiur_api::maintenance::RunStatus;
    use kopiur_api::{MaintenanceSpec, MaintenanceStatus, Ownership, TakeoverPolicy};

    fn maint_with(
        quick_cron: &str,
        full_cron: &str,
        status: Option<MaintenanceStatus>,
    ) -> Maintenance {
        let mut m = Maintenance::new(
            "nas-primary",
            MaintenanceSpec {
                repository: RepositoryRef {
                    kind: RepositoryKind::Repository,
                    name: "nas-primary".into(),
                    namespace: None,
                },
                schedule: kopiur_api::MaintenanceSchedule {
                    quick: CronSpec {
                        cron: quick_cron.into(),
                        jitter: None,
                    },
                    full: CronSpec {
                        cron: full_cron.into(),
                        jitter: None,
                    },
                    timezone: None,
                },
                ownership: Ownership {
                    owner: "kopiur/prod/nas-primary".into(),
                    takeover_policy: TakeoverPolicy::Never,
                },
                mover: None,
                failure_policy: None,
                credential_projection: None,
            },
        );
        m.metadata.uid = Some("uid-maint-1".into());
        m.status = status;
        m
    }

    fn run_at(ts: &str) -> RunStatus {
        RunStatus {
            last_run_at: Some(ts.into()),
            ..Default::default()
        }
    }

    fn handled_at(ts: &str) -> RunStatus {
        RunStatus {
            last_handled_at: Some(ts.into()),
            ..Default::default()
        }
    }

    // Regression guard for the TTL-reap loop: a YIELDED slot advances
    // `lastHandledAt` but never `lastRunAt`. Once the slot's Job self-reaps
    // (ttlSecondsAfterFinished), the durable marker — not the Job's existence —
    // must keep the slot from re-firing, or a lease-blocked Maintenance spawns
    // a yield Job every TTL period forever.
    #[test]
    fn handled_slot_does_not_refire_after_its_job_is_ttl_reaped() {
        let now = Utc::now();
        let just = (now - chrono::Duration::seconds(1)).to_rfc3339();
        let status = MaintenanceStatus {
            quick: Some(handled_at(&just)),
            full: Some(handled_at(&just)),
            ..Default::default()
        };
        let m = maint_with("*/5 * * * *", "0 3 * * *", Some(status));
        assert!(
            due_mode(&m, now).is_none(),
            "a handled (yielded) slot must not re-fire after its Job is TTL-reaped"
        );
    }

    // The handled anchor must be the OBSERVATION instant, not the slot: a
    // first-ever slot sits ~a year back (the lookback fallback), and anchoring
    // there leaves the next slot still in the past — a yield-only Maintenance
    // would march through the whole historic backlog one Job at a time.
    #[test]
    fn handling_a_year_old_slot_does_not_start_a_backlog_march() {
        let now = Utc::now();
        // What record_handled_slot writes for the first-ever (year-old) slot:
        // the observation instant `now`, never the slot itself.
        let status = MaintenanceStatus {
            quick: Some(handled_at(&now.to_rfc3339())),
            full: Some(handled_at(&now.to_rfc3339())),
            ..Default::default()
        };
        let m = maint_with("0 3 * * *", "30 4 * * 0", Some(status));
        assert!(
            due_mode(&m, now).is_none(),
            "after handling the first-ever slot, the next due slot must be in \
             the FUTURE — not the next entry of a year-long backlog"
        );
    }

    #[test]
    fn mode_after_takes_the_later_of_run_and_handled() {
        let now = Utc::now();
        let old = (now - chrono::Duration::days(3)).to_rfc3339();
        let recent = (now - chrono::Duration::hours(1)).to_rfc3339();
        // Run long ago, handled recently (yield path) → handled wins.
        let status = MaintenanceStatus {
            full: Some(RunStatus {
                last_run_at: Some(old.clone()),
                last_handled_at: Some(recent.clone()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let m = maint_with("*/5 * * * *", "0 3 * * *", Some(status));
        let after = mode_after(&m, MaintenanceMode::Full);
        assert_eq!(after.to_rfc3339(), recent);
        // Handled long ago, run recently (real-run path) → run wins.
        let status = MaintenanceStatus {
            full: Some(RunStatus {
                last_run_at: Some(recent.clone()),
                last_handled_at: Some(old),
                ..Default::default()
            }),
            ..Default::default()
        };
        let m = maint_with("*/5 * * * *", "0 3 * * *", Some(status));
        assert_eq!(mode_after(&m, MaintenanceMode::Full).to_rfc3339(), recent);
        // Neither recorded → first-ever fires immediately (a slot exists in the
        // year-long lookback window).
        let m = maint_with("*/5 * * * *", "0 3 * * *", None);
        assert!(due_mode(&m, now).is_some());
    }

    #[test]
    fn first_ever_reconcile_is_due_and_prefers_full() {
        // No status → both due; full wins (it subsumes quick).
        let m = maint_with("*/5 * * * *", "0 3 * * *", None);
        let (mode, _slot) = due_mode(&m, Utc::now()).expect("first run is due");
        assert_eq!(mode, MaintenanceMode::Full);
    }

    #[test]
    fn not_due_right_after_a_run() {
        // Both ran one second ago → next slots are in the future → nothing due.
        let now = Utc::now();
        let just = (now - chrono::Duration::seconds(1)).to_rfc3339();
        let status = MaintenanceStatus {
            quick: Some(run_at(&just)),
            full: Some(run_at(&just)),
            ..Default::default()
        };
        let m = maint_with("*/5 * * * *", "0 3 * * *", Some(status));
        assert!(
            due_mode(&m, now).is_none(),
            "a mode that just ran must not be immediately due again"
        );
    }

    #[test]
    fn quick_due_when_full_recent() {
        // Full ran moments ago (not due), quick last ran long ago (due) → quick.
        let now = Utc::now();
        let status = MaintenanceStatus {
            quick: Some(run_at(&(now - chrono::Duration::days(2)).to_rfc3339())),
            full: Some(run_at(&(now - chrono::Duration::seconds(1)).to_rfc3339())),
            ..Default::default()
        };
        let m = maint_with("*/5 * * * *", "0 3 * * *", Some(status));
        let (mode, _) = due_mode(&m, now).expect("quick should be due");
        assert_eq!(mode, MaintenanceMode::Quick);
    }

    #[test]
    fn job_name_is_deterministic_and_within_limit() {
        let slot = DateTime::parse_from_rfc3339("2026-06-06T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let short = maintenance_job_name("nas-primary", MaintenanceMode::Full, slot);
        assert!(short.len() <= 52);
        assert!(short.starts_with("nas-primary-f-"));
        // Deterministic.
        assert_eq!(
            short,
            maintenance_job_name("nas-primary", MaintenanceMode::Full, slot)
        );
        // Quick vs full differ.
        assert_ne!(
            short,
            maintenance_job_name("nas-primary", MaintenanceMode::Quick, slot)
        );
    }

    #[test]
    fn job_name_truncates_and_hashes_long_cr_names() {
        let slot = DateTime::parse_from_rfc3339("2026-06-06T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let long = "a-very-long-repository-name-that-blows-the-dns-label-budget-easily";
        let n1 = maintenance_job_name(long, MaintenanceMode::Quick, slot);
        assert!(n1.len() <= 52, "got {} ({} chars)", n1, n1.len());
        // Stable across calls (hash is run-independent).
        assert_eq!(n1, maintenance_job_name(long, MaintenanceMode::Quick, slot));
        // A different long name produces a different truncated+hashed name.
        let other = "b-very-long-repository-name-that-blows-the-dns-label-budget-easily";
        assert_ne!(
            n1,
            maintenance_job_name(other, MaintenanceMode::Quick, slot)
        );
    }

    #[test]
    fn requeue_is_capped() {
        // Full daily, last ran moments ago → next full ~24h out, but the requeue
        // is capped so the controller still wakes within the heartbeat.
        let now = Utc::now();
        let status = MaintenanceStatus {
            quick: Some(run_at(&(now - chrono::Duration::seconds(1)).to_rfc3339())),
            full: Some(run_at(&(now - chrono::Duration::seconds(1)).to_rfc3339())),
            ..Default::default()
        };
        let m = maint_with("0 */6 * * *", "0 3 * * *", Some(status));
        assert!(cap(next_wakeup(&m, now, None)) <= REQUEUE_CAP);
    }

    // --- manual (annotation-requested) runs -----------------------------------

    fn maint_with_annotations(
        annotations: &[(&str, &str)],
        manual_status: Option<kopiur_api::ManualRunStatus>,
    ) -> Maintenance {
        let mut m: Maintenance = serde_json::from_value(serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Maintenance",
            "metadata": { "name": "maint", "namespace": "ns" },
            "spec": {
                "repository": { "kind": "Repository", "name": "repo" },
                "schedule": { "quick": { "cron": "0 */6 * * *" }, "full": { "cron": "0 3 * * *" } },
                "ownership": { "owner": "test" }
            }
        }))
        .expect("maintenance fixture");
        if !annotations.is_empty() {
            m.metadata.annotations = Some(
                annotations
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
        }
        if let Some(manual) = manual_status {
            m.status = Some(kopiur_api::MaintenanceStatus {
                manual_run: Some(manual),
                ..Default::default()
            });
        }
        m
    }

    #[test]
    fn manual_run_request_parses_annotations_and_defaults_to_quick() {
        use crate::consts::{RUN_MODE_ANNOTATION, RUN_REQUESTED_ANNOTATION};
        let m = maint_with_annotations(&[(RUN_REQUESTED_ANNOTATION, "2026-06-11T12:00:00Z")], None);
        let (at, mode) = manual_run_request(&m).expect("ok").expect("requested");
        assert_eq!(
            at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "2026-06-11T12:00:00Z"
        );
        assert_eq!(
            mode,
            kopiur_api::ManualRunMode::Quick,
            "mode defaults to quick"
        );

        let m = maint_with_annotations(
            &[
                (RUN_REQUESTED_ANNOTATION, "2026-06-11T12:00:00Z"),
                (RUN_MODE_ANNOTATION, "full"),
            ],
            None,
        );
        let (_, mode) = manual_run_request(&m).expect("ok").expect("requested");
        assert_eq!(mode, kopiur_api::ManualRunMode::Full);
    }

    #[test]
    fn manual_run_request_dedupes_an_answered_timestamp_but_not_a_new_one() {
        use crate::consts::RUN_REQUESTED_ANNOTATION;
        let answered = kopiur_api::ManualRunStatus {
            requested_at: Some("2026-06-11T12:00:00Z".into()),
            mode: Some(kopiur_api::ManualRunMode::Quick),
            phase: Some(kopiur_api::ManualRunPhase::Succeeded),
            completed_at: Some("2026-06-11T12:01:00Z".into()),
        };
        // Same timestamp, terminal phase: handled — a no-op.
        let m = maint_with_annotations(
            &[(RUN_REQUESTED_ANNOTATION, "2026-06-11T12:00:00Z")],
            Some(answered.clone()),
        );
        assert!(manual_run_request(&m).expect("ok").is_none());

        // A NEW timestamp re-arms the trigger.
        let m = maint_with_annotations(
            &[(RUN_REQUESTED_ANNOTATION, "2026-06-11T13:00:00Z")],
            Some(answered.clone()),
        );
        assert!(manual_run_request(&m).expect("ok").is_some());

        // A Running phase is NOT deduped here (the reconcile body resolves the
        // in-flight Job / lost-outcome cases).
        let running = kopiur_api::ManualRunStatus {
            phase: Some(kopiur_api::ManualRunPhase::Running),
            completed_at: None,
            ..answered
        };
        let m = maint_with_annotations(
            &[(RUN_REQUESTED_ANNOTATION, "2026-06-11T12:00:00Z")],
            Some(running),
        );
        assert!(manual_run_request(&m).expect("ok").is_some());
    }

    #[test]
    fn manual_run_request_rejects_garbage_with_a_fix() {
        use crate::consts::{RUN_MODE_ANNOTATION, RUN_REQUESTED_ANNOTATION};
        let m = maint_with_annotations(&[(RUN_REQUESTED_ANNOTATION, "yesterday")], None);
        let err = manual_run_request(&m).expect_err("bad timestamp");
        let msg = err.to_string();
        assert!(msg.contains("must be an RFC3339 timestamp"), "{msg}");
        assert!(msg.contains("kubectl kopiur maintenance run"), "{msg}");

        let m = maint_with_annotations(
            &[
                (RUN_REQUESTED_ANNOTATION, "2026-06-11T12:00:00Z"),
                (RUN_MODE_ANNOTATION, "FULL"),
            ],
            None,
        );
        let msg = manual_run_request(&m).expect_err("bad mode").to_string();
        assert!(msg.contains("must be `quick` or `full`"), "{msg}");
    }

    #[test]
    fn manual_job_names_never_collide_with_cron_slot_names() {
        let at = DateTime::parse_from_rfc3339("2026-06-11T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let manual_q = manual_job_name("maint", kopiur_api::ManualRunMode::Quick, at);
        let manual_f = manual_job_name("maint", kopiur_api::ManualRunMode::Full, at);
        let cron_q = maintenance_job_name("maint", MaintenanceMode::Quick, at);
        let cron_f = maintenance_job_name("maint", MaintenanceMode::Full, at);
        let names = [&manual_q, &manual_f, &cron_q, &cron_f];
        let unique: std::collections::BTreeSet<_> = names.iter().collect();
        assert_eq!(unique.len(), 4, "{names:?}");
        assert!(manual_q.contains("-mq-"), "{manual_q}");
        assert!(manual_f.contains("-mf-"), "{manual_f}");
        // Long CR names stay within the budget.
        let long = "m".repeat(80);
        assert!(manual_job_name(&long, kopiur_api::ManualRunMode::Full, at).len() <= 52);
    }
}
