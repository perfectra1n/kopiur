//! The `Repository` reconciler (ADR §3.1, §5.4).
//!
//! Responsibilities:
//! 1. Defensive re-validation (`api::validate`).
//! 2. Ensure the repo exists: connect, and create it if `create.enabled` — via a
//!    short-lived Job (ADR §5.4) so a controller restart never strands a kopia
//!    process. Set `status.phase`/`uniqueID`/`backend`/`storageStats`.
//! 3. Periodic catalog scan (`snapshot list`) materializing `origin: discovered`
//!    `Backup` CRs, bounded by `catalog.retain`, deduplicated by
//!    `(Repository.UID, kopiaSnapshotID)` (ADR §2.1).
//!
//! The catalog **dedup decision** is a pure function ([`catalog_dedup_key`] +
//! [`needs_materialization`]) and is unit-tested here; the kopia `snapshot list`
//! IO and `Backup` CR creation are the thin parts.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::ListParams;
use kube::runtime::controller::Action;
use kube::{Api, Resource, ResourceExt};

use kopiur_api::backend::Backend;
use kopiur_api::common::RepositoryKind;
use kopiur_api::{Backup, Repository, RepositoryPhase, validate};
use kopiur_kopia::{ConnectSpec, KopiaErrorClass, SnapshotListEntry};
use kopiur_mover::bootstrap::{BootstrapResult, RESULT_CONFIGMAP_KEY};
use kopiur_mover::workspec::{
    BootstrapRepositoryOp, MoverOptions, MoverWorkSpec, Operation, ResolvedIdentity, TargetRef,
};

use crate::backup::{backend_to_repository_connect, mover_pull_policy_pub};
use crate::consts::{
    API_VERSION, BOOTSTRAP_JOB_DEADLINE_SECS, ORIGIN_LABEL, REPOSITORY_BOOTSTRAPPED_CONDITION,
    REPOSITORY_UID_LABEL, SNAPSHOT_ID_LABEL,
};
use crate::context::Context;
use crate::error::{Error, Result, TERMINAL_HEARTBEAT, error_policy_for};
use crate::io;
use crate::jobs::{self, JobLimits, MoverJobInputs};

/// The dedup key for a discovered snapshot: `(Repository.UID, kopiaSnapshotID)`
/// (ADR §2.1). Two scans of the same repo never materialize the same snapshot
/// twice, and the same snapshot id under a *different* repository is distinct.
pub fn catalog_dedup_key(repo_uid: &str, snapshot_id: &str) -> (String, String) {
    (repo_uid.to_string(), snapshot_id.to_string())
}

/// Given the snapshot ids already materialized as `Backup` CRs (the existing
/// set, keyed by `(repo_uid, id)`) and a fresh `snapshot list`, return the
/// entries that still need a `Backup` CR created. Pure; the caller does the
/// `Backup` CR creation.
pub fn needs_materialization<'a>(
    repo_uid: &str,
    existing: &BTreeSet<(String, String)>,
    listing: &'a [SnapshotListEntry],
) -> Vec<&'a SnapshotListEntry> {
    listing
        .iter()
        .filter(|e| !existing.contains(&catalog_dedup_key(repo_uid, &e.id)))
        .collect()
}

/// Logical bytes under management: the sum, over each distinct snapshot source,
/// of the most-recent snapshot's logical `total_size`. Older snapshots of the
/// same source are not added (they would double-count unchanged data). Pure.
pub fn logical_bytes_under_management(listing: &[SnapshotListEntry]) -> i64 {
    use std::collections::HashMap;
    let mut newest: HashMap<&str, &SnapshotListEntry> = HashMap::new();
    for e in listing {
        let key = e.source.path.as_str();
        match newest.get(key) {
            Some(prev) if prev.end_time >= e.end_time => {}
            _ => {
                newest.insert(key, e);
            }
        }
    }
    newest
        .values()
        .map(|e| i64::try_from(e.stats.total_size).unwrap_or(i64::MAX))
        .sum()
}

/// Reconcile a `Repository`.
#[tracing::instrument(skip(repo, ctx), fields(kind = "Repository", namespace = %repo.namespace().unwrap_or_default(), name = %repo.name_any()))]
pub async fn reconcile(repo: Arc<Repository>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&repo, &ctx).await;
    ctx.metrics
        .record_reconcile("Repository", start.elapsed().as_secs_f64());
    record_repository_status_metrics(&repo, &ctx, result.is_ok()).await;
    result
}

/// Mirror a Repository's phase + catalog gauges. Zeroes the phase on deletion
/// (so Degraded/Failed alerts clear) and re-reads the freshest status on success
/// (the passed object is the pre-reconcile cache copy). See the Backup
/// equivalent for the rationale.
async fn record_repository_status_metrics(repo: &Repository, ctx: &Context, ok: bool) {
    let (Some(ns), name) = (repo.namespace(), repo.name_any()) else {
        return;
    };
    if repo.metadata.deletion_timestamp.is_some() {
        ctx.metrics
            .clear_phase::<RepositoryPhase>("Repository", &ns, &name);
        return;
    }
    if !ok {
        return;
    }
    let api: Api<Repository> = Api::namespaced(ctx.client.clone(), &ns);
    if let Ok(Some(latest)) = api.get_opt(&name).await
        && let Some(status) = latest.status.as_ref()
    {
        if let Some(phase) = status.phase {
            ctx.metrics
                .set_repository_phase("Repository", &ns, &name, phase);
        }
        let snapshots = status.storage_stats.as_ref().and_then(|s| s.snapshot_count);
        let discovered = status
            .catalog
            .as_ref()
            .and_then(|c| c.discovered_backup_count);
        if snapshots.is_some() || discovered.is_some() {
            ctx.metrics
                .set_repo_catalog(&ns, &name, snapshots, discovered);
        }
    }
}

async fn reconcile_inner(repo: &Repository, ctx: &Context) -> Result<Action> {
    if let Err(e) = validate::validate_repository_no_inline_retention(&repo.spec) {
        return Err(Error::Validation(e.to_string()));
    }

    let namespace = repo
        .namespace()
        .ok_or_else(|| Error::Invariant("Repository has no namespace".into()))?;
    let name = repo.name_any();
    let repo_uid = repo
        .uid()
        .ok_or_else(|| Error::Invariant("Repository has no uid".into()))?;
    let api: Api<Repository> = Api::namespaced(ctx.client.clone(), &namespace);

    // The controller may run kopia in-process for the FILESYSTEM backend only
    // (ADR §5.4 permits short idempotent ops; filesystem repos are reachable from
    // the controller's own filesystem when using a hostPath/shared mount, or in
    // the e2e harness). For object-store backends, connect-validate would run as
    // a short Job — documented as a follow-up below.
    match &repo.spec.backend {
        Backend::Filesystem(fs) => {
            // Hard-stop: if we already terminally failed to connect for THIS spec
            // generation, don't re-read the password Secret or re-hit the backend.
            // A non-retryable failure (e.g. PermissionDenied on the NFS export)
            // cannot succeed until the user edits the CR — which bumps
            // `metadata.generation` and reopens this gate. The 30 min heartbeat
            // keeps us resilient to a watch desync without spamming the backend or
            // the logs (this wake does no IO and logs nothing).
            if io::is_terminal_for_generation(
                repo.status.as_ref().and_then(|s| s.phase),
                repo.status.as_ref().and_then(|s| s.observed_generation),
                repo.metadata.generation,
            ) {
                return Ok(Action::requeue(TERMINAL_HEARTBEAT));
            }

            let creds = io::repo_credentials(&repo.spec.encryption);
            let password = io::read_repo_password(&ctx.client, &namespace, &creds).await?;
            let client = ctx.kopia.build([("KOPIA_PASSWORD".to_string(), password)]);
            let spec = ConnectSpec::Filesystem {
                path: fs.path.clone().into(),
            };

            // Idempotent connect; create on first use when enabled AND the
            // failure does not indicate an existing repo (auth/locked) — the same
            // safe gate the bootstrap mover applies (never recreate over data).
            if let Err(e) = client.repository_connect(&spec).await {
                let create_enabled = repo
                    .spec
                    .create
                    .as_ref()
                    .map(|c| c.enabled)
                    .unwrap_or(false);
                // Try create-then-connect when enabled and the failure isn't
                // "repo already there" (auth/locked); otherwise the connect error
                // is terminal. A terminal failure (connect OR a failed create)
                // surfaces Failed + an actionable Event (e.g. filesystem "Access
                // Denied") rather than an invisible reconcile error with no status.
                let outcome =
                    if kopiur_mover::bootstrap::should_attempt_create(create_enabled, e.class()) {
                        match client.repository_create(&spec).await {
                            Ok(_) => client.repository_connect(&spec).await,
                            Err(ce) => Err(ce),
                        }
                    } else {
                        Err(e)
                    };
                if let Err(e) = outcome {
                    let class = e.class();
                    let retryable = class.is_retryable();
                    // Reserve `Failed` (terminal, gated) for non-retryable classes;
                    // a retryable backend blip is `Degraded` and keeps retrying on
                    // the 30 s transient cadence.
                    let phase = if retryable { "Degraded" } else { "Failed" };
                    // Stable, volatile-free condition message — the full stderr (with
                    // its per-attempt temp filename) goes to the Event only, so the
                    // persisted status is byte-identical across repeated failures and
                    // the guarded write below becomes a true no-op.
                    let conditions =
                        bootstrap_condition(repo, false, class.as_str(), class.summary());
                    let current = serde_json::to_value(&repo.status).ok();
                    let wrote = io::patch_status_if_changed(
                        &api,
                        &name,
                        current.as_ref(),
                        serde_json::json!({
                            "phase": phase,
                            "backend": "Filesystem",
                            "observedGeneration": repo.metadata.generation,
                            "conditions": conditions,
                        }),
                    )
                    .await?;
                    // Fire the Warning Event only on a real transition (not on every
                    // requeue) — it carries the full stderr for `kubectl describe`.
                    if wrote {
                        io::publish_backend_failure(
                            ctx,
                            &repo.object_ref(&()),
                            &name,
                            class,
                            &e.to_string(),
                        )
                        .await;
                    }
                    return if retryable {
                        // Transient: surface as an Err so error_policy requeues at
                        // the 30 s cadence and we keep trying.
                        Err(Error::Kopia(e))
                    } else {
                        // Terminal: status is written; stop. The gate above makes
                        // subsequent wakes no-ops until the spec changes.
                        Ok(Action::requeue(TERMINAL_HEARTBEAT))
                    };
                }
            }

            // Status: phase/uniqueID/backend/storageStats.
            let status = client.repository_status().await?;
            io::patch_status(
                &api,
                &name,
                serde_json::json!({
                    "phase": "Ready",
                    "backend": "Filesystem",
                    "uniqueId": status.unique_id_hex,
                    "observedGeneration": repo.metadata.generation,
                }),
            )
            .await?;

            // Catalog scan: materialize discovered Backups for unseen snapshots,
            // bounded by catalog.retain.perIdentity. Filesystem lists in-process.
            let listing = client.snapshot_list(None).await?;
            let total = listing.len() as i64;
            scan_catalog(ctx, repo, &namespace, &name, &repo_uid, &listing, total).await?;

            // Now that the repo is Ready, ensure its managed Maintenance exists
            // (default-on) and surface the MaintenanceConfigured condition. A
            // namespaced Repository's Maintenance lives in the repo's namespace.
            // ADR §3.7.
            let conditions = repo
                .status
                .as_ref()
                .map(|s| s.conditions.clone())
                .unwrap_or_default();
            io::ensure_maintenance(
                ctx,
                &api,
                repo,
                &repo.object_ref(&()),
                RepositoryKind::Repository,
                "Repository",
                &namespace,
                Some(&namespace),
                Some(&namespace),
                &name,
                repo.spec.maintenance.as_ref(),
                &conditions,
                repo.metadata.generation,
            )
            .await;
        }
        other => {
            // Object-store backends run connect/create/status/catalog in a
            // short-lived mover Job (ADR §5.4): the controller cannot reach the
            // store in-process. The Job writes its result into the work-spec
            // ConfigMap; the controller (sole writer of the Repository status)
            // reads it back to set phase/uniqueId and materialize the catalog.
            return bootstrap_object_store(ctx, repo, &namespace, &name, &repo_uid, &api, other)
                .await;
        }
    }

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Drive the object-store bootstrap state machine: launch the mover Job, then on
/// each reconcile reflect its progress (`Initializing` → `Ready`/`Failed`),
/// reading the result the mover wrote into the work-spec ConfigMap.
#[allow(clippy::too_many_arguments)]
async fn bootstrap_object_store(
    ctx: &Context,
    repo: &Repository,
    namespace: &str,
    name: &str,
    repo_uid: &str,
    api: &Api<Repository>,
    backend: &Backend,
) -> Result<Action> {
    let job_name = format!("{name}-bootstrap");
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);

    if let Some(job) = job_api.get_opt(&job_name).await? {
        return match crate::backup::job_terminal_state(&job) {
            // Still running: surface Initializing and poll.
            None => {
                io::patch_status(
                    api,
                    name,
                    serde_json::json!({ "phase": "Initializing", "backend": backend.kind_str() }),
                )
                .await?;
                Ok(Action::requeue(Duration::from_secs(15)))
            }
            // Complete or backoff-exhausted: read the structured result.
            Some(success) => {
                finalize_bootstrap(
                    ctx, repo, namespace, name, repo_uid, api, backend, &job_name, success,
                )
                .await
            }
        };
    }

    // No Job yet: build + apply it (ConfigMap carries the work spec; the result
    // is written back into the same ConfigMap under `result.json`).
    let create_enabled = repo
        .spec
        .create
        .as_ref()
        .map(|c| c.enabled)
        .unwrap_or(false);
    let work_spec = bootstrap_work_spec(backend, name, namespace, create_enabled, true);
    let creds_secrets = io::mover_creds_secrets(backend, &repo.spec.encryption);
    // Mint the mover SA + RoleBinding in the Repository's namespace and confirm the
    // credential Secret is present before launching the bootstrap Job (ADR §4.12).
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
    io::ensure_creds_present(
        &ctx.client,
        namespace,
        &io::CredsContext {
            secret_names: &creds_secrets,
            repo_kind: "Repository",
            repo_name: name,
            repo_secret_namespace: repo
                .spec
                .encryption
                .password_secret_ref
                .namespace
                .as_deref(),
        },
    )
    .await?;
    let owner = io::owner_ref_for(repo, "Repository")?;
    let mut labels = BTreeMap::new();
    labels.insert(
        "kopiur.home-operations.com/repository".to_string(),
        name.to_string(),
    );
    let inputs = MoverJobInputs {
        name: &job_name,
        namespace,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: mover_pull_policy_pub(),
        // Bound the bootstrap Job: a pod that never schedules (missing mover SA,
        // image-pull failure) otherwise never reaches a `Failed` condition, so the
        // controller never finalizes and the repository hangs `Initializing` with
        // no Event. The deadline forces it terminal so `finalize_bootstrap` runs.
        limits: JobLimits {
            active_deadline_seconds: Some(BOOTSTRAP_JOB_DEADLINE_SECS),
            ..JobLimits::default()
        },
        resources: None,
        security_context: None,
        labels,
        source_pvc: None,
        repo_pvc: None,
        creds_secrets,
        result_configmap: Some(&job_name),
        service_account: ctx.mover_service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
        annotations: Default::default(),
    };
    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, namespace, &job_name, &cm, &job).await?;
    io::patch_status(
        api,
        name,
        serde_json::json!({ "phase": "Initializing", "backend": backend.kind_str() }),
    )
    .await?;
    tracing::info!(repo = %name, backend = backend.kind_str(), "launched repository bootstrap Job");
    Ok(Action::requeue(Duration::from_secs(15)))
}

/// Build the bootstrap work spec for an object-store backend. Identity is a
/// sentinel (bootstrap connects/creates the repo, it does not snapshot under any
/// identity). `scan_catalog` drives whether the mover returns the snapshot list
/// for discovered-Backup materialization.
fn bootstrap_work_spec(
    backend: &Backend,
    name: &str,
    namespace: &str,
    auto_create: bool,
    scan_catalog: bool,
) -> MoverWorkSpec {
    MoverWorkSpec {
        version: 1,
        operation: Operation::BootstrapRepository(BootstrapRepositoryOp {
            auto_create,
            scan_catalog,
        }),
        identity: ResolvedIdentity {
            username: "kopiur-bootstrap".to_string(),
            hostname: namespace.to_string(),
            source_path: String::new(),
        },
        repository: backend_to_repository_connect(backend),
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "Repository".to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
    }
}

/// Read the [`BootstrapResult`] the mover wrote into the work-spec ConfigMap.
/// `Ok(None)` if the ConfigMap or the result key is not (yet) present.
async fn read_bootstrap_result(
    ctx: &Context,
    namespace: &str,
    cm_name: &str,
) -> Result<Option<BootstrapResult>> {
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
    let Some(cm) = cm_api.get_opt(cm_name).await? else {
        return Ok(None);
    };
    let Some(raw) = cm.data.as_ref().and_then(|d| d.get(RESULT_CONFIGMAP_KEY)) else {
        return Ok(None);
    };
    let result: BootstrapResult = serde_json::from_str(raw)
        .map_err(|e| Error::Invariant(format!("parsing bootstrap result for {cm_name}: {e}")))?;
    Ok(Some(result))
}

/// Reflect a finished bootstrap Job into the Repository status. On success:
/// `Ready` + uniqueId, then materialize discovered Backups from the returned
/// snapshots. On failure: `Failed` + an actionable `Bootstrapped=False`
/// condition carrying the kopia error class/message.
#[allow(clippy::too_many_arguments)]
async fn finalize_bootstrap(
    ctx: &Context,
    repo: &Repository,
    namespace: &str,
    name: &str,
    repo_uid: &str,
    api: &Api<Repository>,
    backend: &Backend,
    job_name: &str,
    job_succeeded: bool,
) -> Result<Action> {
    let result = read_bootstrap_result(ctx, namespace, job_name).await?;

    // Classify any terminal failure as a typed value (ADR §5.5): a result-less
    // failed Job and a kopia-rejected connect are distinct, exhaustively-handled
    // modes — never a silent `Failed/Unknown` with no Event.
    let failure = match &result {
        // Result not visible yet (write/propagation race): requeue briefly rather
        // than guessing. A truly result-less Job stays terminal for the next pass.
        None if job_succeeded => {
            tracing::warn!(repo = %name, "bootstrap Job complete but result not readable yet; requeueing");
            return Ok(Action::requeue(Duration::from_secs(5)));
        }
        None => Some(io::BootstrapFailure::JobFailedWithoutResult {
            job_name: job_name.to_string(),
        }),
        Some(r) if !r.success => Some(io::BootstrapFailure::Backend {
            class: r
                .failure
                .as_ref()
                .map(|f| KopiaErrorClass::from_label(&f.kopia_error_class))
                .unwrap_or(KopiaErrorClass::Unknown),
            message: r
                .failure
                .as_ref()
                .map(|f| f.message.clone())
                .unwrap_or_else(|| "repository bootstrap failed".to_string()),
        }),
        Some(_) => None,
    };

    if let Some(failure) = failure {
        let reason = failure.reason();
        let conditions = bootstrap_condition(repo, false, reason, &failure.condition_message());
        // Guard the write so a re-confirmed failure fires the Event + warn log only
        // on the real transition, not on every 120 s re-read (the message is stable,
        // so this becomes a true no-op once written — no reconcile hot-loop).
        let current = serde_json::to_value(&repo.status).ok();
        let wrote = io::patch_status_if_changed(
            api,
            name,
            current.as_ref(),
            serde_json::json!({
                "phase": "Failed",
                "backend": backend.kind_str(),
                "observedGeneration": repo.metadata.generation,
                "conditions": conditions,
            }),
        )
        .await?;
        if wrote {
            failure.publish(ctx, &repo.object_ref(&()), name).await;
            tracing::warn!(repo = %name, reason, "repository bootstrap failed");
        }
        return Ok(Action::requeue(Duration::from_secs(120)));
    }

    // Success: the result is present and `success == true`.
    let result = result.expect("a non-failure bootstrap implies a readable, successful result");

    // Success: Ready + uniqueId + a Bootstrapped=True condition.
    let conditions = bootstrap_condition(
        repo,
        true,
        "Bootstrapped",
        if result.created {
            "created a new repository"
        } else {
            "connected to the existing repository"
        },
    );
    io::patch_status(
        api,
        name,
        serde_json::json!({
            "phase": "Ready",
            "backend": backend.kind_str(),
            "uniqueId": result.unique_id,
            "conditions": conditions,
        }),
    )
    .await?;
    if result.snapshots_truncated {
        tracing::warn!(
            repo = %name,
            snapshot_count = result.snapshot_count,
            "catalog larger than the materialization cap; not all snapshots were materialized"
        );
    }

    // Materialize discovered Backups from the snapshots the Job returned.
    scan_catalog(
        ctx,
        repo,
        namespace,
        name,
        repo_uid,
        &result.snapshots,
        result.snapshot_count,
    )
    .await?;

    // Ensure the managed Maintenance for this repo (ADR §3.7). Build on the
    // conditions we just patched (which include `Bootstrapped`), NOT the stale
    // cached object — otherwise this patch would drop the `Bootstrapped`
    // condition we set above (both writes replace the whole conditions array).
    io::ensure_maintenance(
        ctx,
        api,
        repo,
        &repo.object_ref(&()),
        RepositoryKind::Repository,
        "Repository",
        namespace,
        Some(namespace),
        Some(namespace),
        name,
        repo.spec.maintenance.as_ref(),
        &conditions,
        repo.metadata.generation,
    )
    .await;

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Upsert the `Bootstrapped` condition onto the repository's existing conditions.
fn bootstrap_condition(
    repo: &Repository,
    status: bool,
    reason: &str,
    message: &str,
) -> Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition> {
    let existing = repo
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    io::upsert_condition(
        &existing,
        REPOSITORY_BOOTSTRAPPED_CONDITION,
        status,
        reason,
        message,
        repo.metadata.generation,
    )
}

/// Compute which snapshots in `listing` still need a `Backup` CR, and create the
/// bounded `origin: discovered` set (forced `deletionPolicy: Retain`).
///
/// `listing` is the snapshot set to materialize from: produced in-process for the
/// filesystem backend, or carried back from the bootstrap Job for object stores.
/// `total_snapshot_count` is the authoritative repository-wide count for
/// `storageStats` (may exceed `listing.len()` if the Job capped the returned
/// entries — see `BootstrapResult::snapshots_truncated`).
#[allow(clippy::too_many_arguments)]
async fn scan_catalog(
    ctx: &Context,
    repo: &Repository,
    namespace: &str,
    repo_name: &str,
    repo_uid: &str,
    listing: &[SnapshotListEntry],
    total_snapshot_count: i64,
) -> Result<()> {
    // Existing discovered Backups keyed by (repo_uid, snapshot_id).
    let backup_api: Api<Backup> = Api::namespaced(ctx.client.clone(), namespace);
    let lp = ListParams::default().labels(&format!("{ORIGIN_LABEL}=discovered"));
    let existing_crs = backup_api.list(&lp).await?.items;
    let mut existing: BTreeSet<(String, String)> = BTreeSet::new();
    for b in &existing_crs {
        if let (Some(uid), Some(id)) = (
            b.labels().get(REPOSITORY_UID_LABEL),
            b.labels().get(SNAPSHOT_ID_LABEL),
        ) {
            existing.insert(catalog_dedup_key(uid, id));
        }
    }

    let mut need = needs_materialization(repo_uid, &existing, listing);

    // Bound by catalog.retain.perIdentity (most-recent N). We approximate the
    // global cap by sorting newest-first and truncating; per-identity refinement
    // is a documented follow-up.
    let retain = repo
        .spec
        .catalog
        .as_ref()
        .and_then(|c| c.retain.as_ref())
        .and_then(|r| r.per_identity)
        .map(|n| n.max(0) as usize);
    need.sort_by_key(|e| std::cmp::Reverse(e.end_time));
    if let Some(cap) = retain {
        need.truncate(cap);
    }

    let mut created = 0i64;
    for entry in need {
        create_discovered_backup(ctx, repo, namespace, repo_name, repo_uid, entry).await?;
        created += 1;
    }
    if created > 0 {
        tracing::info!(repo = %repo_name, created, "materialized discovered Backup CRs");
    }

    // Logical bytes under management is recorded directly from kopia's data
    // (the status field is a human string, so the gauge bypasses it).
    ctx.metrics.set_repo_size_bytes(
        namespace,
        repo_name,
        logical_bytes_under_management(listing),
    );

    // Report THIS repository's discovered count (the `existing` set is
    // namespace-wide for cross-repo dedup; the status count must be per-repo).
    let existing_this_repo = existing.iter().filter(|(uid, _)| uid == repo_uid).count() as i64;
    let api: Api<Repository> = Api::namespaced(ctx.client.clone(), namespace);
    io::patch_status(
        &api,
        repo_name,
        serde_json::json!({
            "catalog": {
                "discoveredBackupCount": existing_this_repo + created,
                "lastRefreshAt": chrono::Utc::now().to_rfc3339(),
            },
            "storageStats": { "snapshotCount": total_snapshot_count },
        }),
    )
    .await?;
    Ok(())
}

/// Create one `origin: discovered` Backup CR for a snapshot. `deletionPolicy` is
/// FORCED to `Retain` (the operator never deletes a discovered snapshot, §4.5).
async fn create_discovered_backup(
    ctx: &Context,
    repo: &Repository,
    namespace: &str,
    repo_name: &str,
    repo_uid: &str,
    entry: &SnapshotListEntry,
) -> Result<()> {
    use kopiur_api::backup::{BackupSpec, BackupStatus, SnapshotInfo};
    use kopiur_api::common::{DeletionPolicy, ResolvedIdentity};
    use kopiur_api::{BackupPhase, Origin};

    // CR name: stable from the (short) snapshot id, namespaced under repo.
    let short = entry.id.chars().take(16).collect::<String>();
    let cr_name = format!("{repo_name}-disc-{short}");

    let mut labels = std::collections::BTreeMap::new();
    labels.insert(ORIGIN_LABEL.to_string(), "discovered".to_string());
    labels.insert(REPOSITORY_UID_LABEL.to_string(), repo_uid.to_string());
    labels.insert(SNAPSHOT_ID_LABEL.to_string(), entry.id.clone());

    let owner = io::owner_ref_for(repo, "Repository")?;
    let mut backup = Backup::new(
        &cr_name,
        BackupSpec {
            config_ref: None,
            tags: None,
            failure_policy: None,
            // Forced Retain for discovered (webhook would reject otherwise).
            deletion_policy: Some(DeletionPolicy::Retain),
        },
    );
    backup.metadata = io::child_meta(&cr_name, namespace, labels, Some(owner));
    backup.status = Some(BackupStatus {
        phase: Some(BackupPhase::Discovered),
        origin: Some(Origin::Discovered),
        snapshot: Some(SnapshotInfo {
            kopia_snapshot_id: entry.id.clone(),
            identity: ResolvedIdentity {
                username: entry.source.user_name.clone(),
                hostname: entry.source.host.clone(),
                source_path: Some(entry.source.path.clone()),
            },
        }),
        ..Default::default()
    });

    let api: Api<Backup> = Api::namespaced(ctx.client.clone(), namespace);
    // Create the CR; the discovered status is then PATCHed onto the subresource.
    match io::apply(&api, &cr_name, &backup).await {
        Ok(_) => {}
        Err(Error::Kube(kube::Error::Api(ae))) if ae.code == 409 => return Ok(()),
        Err(e) => return Err(e),
    }
    io::patch_status(
        &api,
        &cr_name,
        serde_json::to_value(backup.status.unwrap_or_default())?,
    )
    .await?;
    let _ = repo; // repo retained for future per-identity bounding
    Ok(())
}

/// `error_policy` for the `Repository` controller.
pub fn error_policy(_obj: Arc<Repository>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("Repository", err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use kopiur_kopia::{SnapshotSource, SnapshotStats};

    fn entry(id: &str) -> SnapshotListEntry {
        SnapshotListEntry {
            id: id.into(),
            source: SnapshotSource {
                host: "h".into(),
                user_name: "u".into(),
                path: "/p".into(),
            },
            description: String::new(),
            start_time: Utc::now(),
            end_time: Utc::now(),
            stats: SnapshotStats::default(),
            root_entry: None,
            retention_reason: vec![],
        }
    }

    fn entry_sized(
        id: &str,
        path: &str,
        end: chrono::DateTime<Utc>,
        size: u64,
    ) -> SnapshotListEntry {
        let mut e = entry(id);
        e.source.path = path.into();
        e.end_time = end;
        e.stats.total_size = size;
        e
    }

    #[test]
    fn logical_bytes_sums_newest_snapshot_per_source() {
        let t0 = Utc::now();
        let t1 = t0 + chrono::Duration::seconds(10);
        let listing = vec![
            // Source /a: older 100, newer 150 → counts 150 (not 250).
            entry_sized("a-old", "/a", t0, 100),
            entry_sized("a-new", "/a", t1, 150),
            // Source /b: single snapshot 40.
            entry_sized("b", "/b", t0, 40),
        ];
        assert_eq!(logical_bytes_under_management(&listing), 190);
        assert_eq!(logical_bytes_under_management(&[]), 0);
    }

    #[test]
    fn dedup_key_combines_repo_uid_and_snapshot_id() {
        assert_eq!(
            catalog_dedup_key("repo-uid", "snap-1"),
            ("repo-uid".to_string(), "snap-1".to_string())
        );
        // Same snapshot id under a different repo is a distinct key.
        assert_ne!(
            catalog_dedup_key("repo-a", "snap-1"),
            catalog_dedup_key("repo-b", "snap-1")
        );
    }

    #[test]
    fn only_unseen_snapshots_need_materialization() {
        let listing = vec![entry("s1"), entry("s2"), entry("s3")];
        let mut existing = BTreeSet::new();
        existing.insert(catalog_dedup_key("repo-1", "s1"));
        existing.insert(catalog_dedup_key("repo-1", "s3"));
        let need = needs_materialization("repo-1", &existing, &listing);
        let ids: Vec<&str> = need.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, vec!["s2"], "only the unseen snapshot is materialized");
    }

    #[test]
    fn same_id_under_other_repo_is_not_deduped() {
        let listing = vec![entry("s1")];
        let mut existing = BTreeSet::new();
        // s1 already materialized, but under repo-OTHER.
        existing.insert(catalog_dedup_key("repo-OTHER", "s1"));
        let need = needs_materialization("repo-1", &existing, &listing);
        assert_eq!(need.len(), 1, "different repo UID → still needs its own CR");
    }

    #[test]
    fn nothing_to_do_when_all_present() {
        let listing = vec![entry("s1"), entry("s2")];
        let mut existing = BTreeSet::new();
        existing.insert(catalog_dedup_key("r", "s1"));
        existing.insert(catalog_dedup_key("r", "s2"));
        assert!(needs_materialization("r", &existing, &listing).is_empty());
    }
}
