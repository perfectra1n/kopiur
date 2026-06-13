//! The `Repository` reconciler (ADR §3.1, §5.4).
//!
//! Responsibilities:
//! 1. Defensive re-validation (`api::validate`).
//! 2. Ensure the repo exists: connect, and create it if `create.enabled` — via a
//!    short-lived Job (ADR §5.4) so a controller restart never strands a kopia
//!    process. Set `status.phase`/`uniqueID`/`backend`/`storageStats`.
//! 3. Periodic catalog scan (`snapshot list`) materializing/expiring
//!    `origin: discovered` `Snapshot` CRs, bounded by `catalog.retain`, on the
//!    `catalog.refreshInterval` cadence (ADR §2.1; rules + pure planner in
//!    [`crate::catalog`]). A bare-path filesystem repo re-lists in-process; a
//!    mover-bootstrapped repo (object store / volume-backed filesystem) re-lists
//!    by recycling its finished bootstrap Job for a fresh result.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::DeleteParams;
use kube::runtime::controller::Action;
use kube::{Api, Resource, ResourceExt};

use kopiur_api::backend::Backend;
use kopiur_api::common::{CatalogBounds, RepositoryKind};
use kopiur_api::{Repository, RepositoryPhase, validate};
use kopiur_kopia::{ConnectSpec, SnapshotListEntry};
use kopiur_mover::bootstrap::{BootstrapResult, RESULT_CONFIGMAP_KEY};
use kopiur_mover::workspec::{
    BootstrapRepositoryOp, MoverOptions, MoverWorkSpec, Operation, ResolvedIdentity, TargetRef,
};

use crate::catalog;
use crate::consts::{API_VERSION, BOOTSTRAP_JOB_DEADLINE_SECS, REPOSITORY_BOOTSTRAPPED_CONDITION};
use crate::context::Context;
use crate::error::{Error, Result, TERMINAL_HEARTBEAT, error_policy_for};
use crate::io;
use crate::jobs::{self, JobLimits, MoverJobInputs};
use crate::snapshot::{backend_to_repository_connect, mover_pull_policy_pub};

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
/// (the passed object is the pre-reconcile cache copy). See the Snapshot
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

    // §14(e): a suspended Repository skips connect/bootstrap AND maintenance
    // projection entirely — a declarative pause. Surface it via a condition and back
    // off long; nothing else runs.
    if repo.spec.suspend {
        let conds = repo
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();
        let conditions = io::set_ready(
            &conds,
            repo.meta().generation,
            io::ReadyOutcome::Reconciling,
            "Suspended",
            "Repository is suspended (spec.suspend); skipping connect and maintenance",
        );
        let current = serde_json::to_value(&repo.status).ok();
        io::patch_status_if_changed(
            &api,
            &name,
            current.as_ref(),
            serde_json::json!({ "observedGeneration": repo.meta().generation, "conditions": conditions }),
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // The controller may run kopia in-process for the FILESYSTEM backend only
    // (ADR §5.4 permits short idempotent ops; a *bare-path* filesystem repo is
    // reachable from the controller's own filesystem via a hostPath/shared mount,
    // or in the e2e harness). A filesystem repo backed by a PVC or an inline NFS
    // export is NOT reachable in-process, so — like object stores — it bootstraps
    // in a short mover Job that mounts the repo volume.
    match &repo.spec.backend {
        // Volume-backed filesystem repos (PVC / inline NFS) and every object store
        // bootstrap via the mover Job. `filesystem_repo_mount_source` returns the
        // volume to mount (None for object stores → no extra volume).
        Backend::Filesystem(fs) if fs.volume.is_some() => {
            return bootstrap_via_mover(
                ctx,
                repo,
                &namespace,
                &name,
                &repo_uid,
                &api,
                &repo.spec.backend,
            )
            .await;
        }
        Backend::Filesystem(fs) => {
            // Read the password Secret up front (one cheap GET). We need it to connect
            // anyway, and its `resourceVersion` drives the hard-stop below: a credential
            // fix after a terminal failure does NOT bump `metadata.generation`, so the
            // gate must also key on the Secret revision.
            let creds = io::repo_credentials(&repo.spec.encryption);
            let (password, cred_version) =
                io::read_repo_credential(&ctx.client, &namespace, &creds).await?;

            // Hard-stop: if we already terminally failed to connect for THIS spec
            // generation AND the password Secret is unchanged since, don't re-hit the
            // backend. A non-retryable failure (e.g. PermissionDenied on the NFS export,
            // or a wrong password) cannot succeed until an input changes — the CR spec
            // (bumps `generation`) or the password Secret (bumps its `resourceVersion`,
            // re-triggered by the Secret watch in `lib.rs`). The 30 min heartbeat keeps
            // us resilient to a watch desync without spamming the backend or the logs.
            if io::terminal_gate_holds(
                repo.status.as_ref().and_then(|s| s.phase),
                repo.status.as_ref().and_then(|s| s.observed_generation),
                repo.metadata.generation,
                repo.status
                    .as_ref()
                    .and_then(|s| s.resolved_credential_version.as_deref()),
                &cred_version,
            ) {
                return Ok(Action::requeue(TERMINAL_HEARTBEAT));
            }

            let client = ctx.kopia.build([("KOPIA_PASSWORD".to_string(), password)]);
            let spec = ConnectSpec::Filesystem {
                path: fs.path.clone().into(),
            };

            // Idempotent connect; create on first use when enabled AND the
            // failure does not indicate an existing repo (auth/locked) — the same
            // safe gate the bootstrap mover applies (never recreate over data).
            if let Err(e) = client
                .repository_connect(&spec, kopiur_kopia::CacheTuning::default())
                .await
            {
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
                // Create-time-fixed format knobs (encryption/splitter/hash/ECC),
                // resolved from spec.create and honored only on actual create
                // (ADR-0005 §13(a)). Immutable post-create (§7).
                let create_opts = kopiur_mover::workspec::CreateOptionsSpec::from_create(
                    repo.spec.create.as_ref(),
                )
                .to_kopia();
                let outcome =
                    if kopiur_mover::bootstrap::should_attempt_create(create_enabled, e.class()) {
                        match client
                            .repository_create(
                                &spec,
                                kopiur_kopia::CacheTuning::default(),
                                &create_opts,
                            )
                            .await
                        {
                            Ok(_) => {
                                client
                                    .repository_connect(&spec, kopiur_kopia::CacheTuning::default())
                                    .await
                            }
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
                            // Pin the Secret revision we just failed with, so a later
                            // content fix (same generation) reopens the hard-stop gate.
                            "resolvedCredentialVersion": cred_version,
                            "conditions": conditions,
                        }),
                    )
                    .await?;
                    // Fire the Warning Event only on a real transition (not on every
                    // requeue) — it carries the full stderr for `kubectl describe`.
                    if wrote {
                        io::publish_backend_failure(
                            ctx,
                            &io::event_ref(repo),
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

            // Status: phase/uniqueId/backend/resolvedCredentialVersion.
            let status = client.repository_status().await?;
            let current = serde_json::to_value(&repo.status).ok();
            io::patch_status_if_changed(
                &api,
                &name,
                current.as_ref(),
                serde_json::json!({
                    "phase": "Ready",
                    "backend": "Filesystem",
                    "uniqueId": status.unique_id_hex,
                    "observedGeneration": repo.metadata.generation,
                    "resolvedCredentialVersion": cred_version,
                }),
            )
            .await?;

            // Catalog scan on the `catalog.refreshInterval` cadence — or
            // immediately on a spec change (`scan_due`'s generation arm: a
            // `catalog.retain` edit must expire rows NOW, not at the next timed
            // refresh): a bare-path filesystem repo re-lists in-process (cheap),
            // materializing/expiring discovered Snapshots per `catalog.retain`.
            // Gating the scan also gates the `lastRefreshAt` write, so a Ready
            // repo's status is byte-stable between refreshes (no self-triggered
            // reconcile hot-loop).
            let interval = CatalogBounds::effective_refresh_interval(repo.spec.catalog.as_ref());
            if catalog::scan_due(
                repo.metadata.generation,
                repo.status.as_ref().and_then(|s| s.observed_generation),
                last_refresh_at(repo),
                interval,
                chrono::Utc::now(),
            ) {
                let listing = client.snapshot_list(None).await?;
                let total = listing.len() as i64;
                run_catalog_scan(
                    ctx, repo, &namespace, &name, &repo_uid, &listing, total, false,
                )
                .await?;
            }

            // Now that the repo is Ready, ensure its managed Maintenance exists
            // (default-on) and surface the MaintenanceConfigured condition. A
            // namespaced Repository's Maintenance lives in the repo's namespace.
            // ADR §3.7.
            let conditions = repo
                .status
                .as_ref()
                .map(|s| s.conditions.clone())
                .unwrap_or_default();
            // §11: a ReadOnly repository runs no maintenance (it serves restores
            // only). Skip the projection so no managed Maintenance is created.
            if repo.spec.mode.allows_writes() {
                io::ensure_maintenance(
                    ctx,
                    &api,
                    repo,
                    &io::event_ref(repo),
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
        }
        other => {
            // Object-store backends run connect/create/status/catalog in a
            // short-lived mover Job (ADR §5.4): the controller cannot reach the
            // store in-process. The Job writes its result into the work-spec
            // ConfigMap; the controller (sole writer of the Repository status)
            // reads it back to set phase/uniqueId and materialize the catalog.
            return bootstrap_via_mover(ctx, repo, &namespace, &name, &repo_uid, &api, other).await;
        }
    }

    Ok(Action::requeue(catalog::reconcile_interval(
        repo.spec.catalog.as_ref(),
    )))
}

/// `status.catalog.lastRefreshAt` from the cached object (the refresh-due gate).
fn last_refresh_at(repo: &Repository) -> Option<&str> {
    repo.status
        .as_ref()
        .and_then(|s| s.catalog.as_ref())
        .and_then(|c| c.last_refresh_at.as_deref())
}

/// Drive the mover-Job bootstrap state machine: launch the Job, then on each
/// reconcile reflect its progress (`Initializing` → `Ready`/`Failed`), reading the
/// result the mover wrote into the work-spec ConfigMap. Used for object stores AND
/// for volume-backed filesystem repos (PVC / inline NFS) — neither is reachable
/// from the controller in-process. A filesystem backend's repo volume is mounted
/// read-write so the mover can create/connect the repository.
#[allow(clippy::too_many_arguments)]
async fn bootstrap_via_mover(
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
        let already_ready =
            repo.status.as_ref().and_then(|s| s.phase) == Some(RepositoryPhase::Ready);
        return match crate::snapshot::job_terminal_state(&job) {
            // Still running: surface Initializing and poll. A catalog-refresh
            // re-run of an already-Ready repo keeps its phase — flapping
            // Ready→Initializing every refresh would be pure status churn.
            None => {
                if !already_ready {
                    io::patch_status(
                        api,
                        name,
                        serde_json::json!({ "phase": "Initializing", "backend": backend.kind_str() }),
                    )
                    .await?;
                }
                Ok(Action::requeue(Duration::from_secs(15)))
            }
            // Complete or backoff-exhausted: read the structured result — unless
            // the result is stale (catalog refresh due, or the spec changed since
            // it was taken): then recycle the Job so the next reconcile re-runs
            // the bootstrap for a fresh connect + `snapshot list`.
            Some(success) => {
                let interval =
                    CatalogBounds::effective_refresh_interval(repo.spec.catalog.as_ref());
                if catalog::bootstrap_recycle_due(
                    already_ready,
                    repo.metadata.generation,
                    repo.status.as_ref().and_then(|s| s.observed_generation),
                    last_refresh_at(repo),
                    interval,
                    chrono::Utc::now(),
                ) {
                    tracing::debug!(repo = %name, "recycling finished bootstrap Job for a catalog refresh");
                    job_api
                        .delete(&job_name, &DeleteParams::background())
                        .await?;
                    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
                    match cm_api.delete(&job_name, &DeleteParams::default()).await {
                        Ok(_) => {}
                        Err(kube::Error::Api(ae)) if ae.code == 404 => {}
                        Err(e) => return Err(Error::Kube(e)),
                    }
                    return Ok(Action::requeue(Duration::from_secs(5)));
                }
                finalize_bootstrap(
                    ctx, repo, namespace, name, repo_uid, api, backend, &job_name, success,
                )
                .await
            }
        };
    }

    // No Job: it either never ran, or the kube TTL controller reaped the finished
    // one (`ttlSecondsAfterFinished`). An already-Ready repo only re-creates it
    // when a re-run is actually warranted (`bootstrap_create_due`: catalog refresh
    // due, or spec changed) — re-creating unconditionally would pin the refresh
    // cadence to the Job TTL instead of `catalog.refreshInterval`.
    if !catalog::bootstrap_create_due(
        repo.status.as_ref().and_then(|s| s.phase) == Some(RepositoryPhase::Ready),
        repo.metadata.generation,
        repo.status.as_ref().and_then(|s| s.observed_generation),
        last_refresh_at(repo),
        CatalogBounds::effective_refresh_interval(repo.spec.catalog.as_ref()),
        chrono::Utc::now(),
    ) {
        return Ok(Action::requeue(catalog::reconcile_interval(
            repo.spec.catalog.as_ref(),
        )));
    }

    // Build + apply the Job (ConfigMap carries the work spec; the result is
    // written back into the same ConfigMap under `result.json`).
    let create_enabled = repo
        .spec
        .create
        .as_ref()
        .map(|c| c.enabled)
        .unwrap_or(false);
    let work_spec = bootstrap_work_spec(
        backend,
        name,
        namespace,
        create_enabled,
        true,
        repo.spec.create.as_ref(),
        repo.spec.mover_defaults.as_ref(),
    );
    // Resolve the bootstrap Job's run identity in the Repository's namespace:
    // the user's workload-identity SA (preflighted + bound to the mover role),
    // or the minted mover SA + RoleBinding (ADR §4.12).
    let mover_identity = io::ensure_mover_identity(
        &ctx.client,
        namespace,
        &[backend],
        ctx.mover_service_account.as_deref(),
        &ctx.mover_role_kind,
        &ctx.mover_clusterrole,
    )
    .await?;
    // Resolve the credential Secret(s) the bootstrap mover loads via envFrom:
    // verify the user-managed credential Secret is present. The bootstrap Job runs
    // in the Repository's own namespace, where its Secret already lives — so it
    // never needs projection (projection is a consumer-side opt-in on
    // SnapshotPolicy/Restore/Maintenance, not on the repository).
    let owner = io::owner_ref_for(repo, "Repository")?;
    let refs = io::mover_creds_secret_refs(backend, &repo.spec.encryption, Some(namespace));
    let creds_names: Vec<String> = refs.iter().map(|r| r.name.clone()).collect();
    let creds = io::resolve_mover_creds(
        &ctx.client,
        namespace,
        &job_name,
        &owner,
        &refs,
        false, // consumer opt-in: never project on the bootstrap path
        false, // owner allow: irrelevant on the same-namespace bootstrap path
        &io::CredsContext {
            secret_names: &creds_names,
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
    let creds_secrets = creds.names;
    let mut labels = BTreeMap::new();
    labels.insert(
        "kopiur.home-operations.com/repository".to_string(),
        name.to_string(),
    );
    mover_identity.decorate_labels(&mut labels);
    // A filesystem backend mounts its repo volume (PVC / inline NFS) read-write so
    // the mover can create/connect the repository; object stores mount nothing.
    let repo_volume =
        io::filesystem_repo_mount_source(backend).map(|source| jobs::VolumeMountSpec {
            source,
            mount_path: io::filesystem_repo_path(backend).unwrap_or_default(),
            read_only: false,
        });
    // The bootstrap (connect/create) Job has no recipe `mover`, but inherits the
    // repository's `moverDefaults` — the bootstrap-gap fix (ADR-0004 §1): a
    // filesystem/NFS repo on a non-65532-owned directory becomes bootstrappable by
    // setting `moverDefaults.podSecurityContext.fsGroup` / `securityContext.runAsUser`
    // once, with no special-case knob. Not subject to the privileged-mover namespace
    // gate: it runs in the repo's own namespace and is authored by the repo owner.
    let resolved_mover = kopiur_api::common::resolve_mover(
        repo.spec.mover_defaults.as_ref(),
        None,
        None,
        None,
        None,
        None,
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
            ttl_seconds_after_finished: resolved_mover.ttl_seconds_after_finished,
            ..JobLimits::default()
        },
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
        result_configmap: Some(&job_name),
        service_account: mover_identity.service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
        annotations: Default::default(),
        // Bootstrap is a short connect/create probe: an emptyDir cache suffices.
        cache_volume: Default::default(),
        readiness_exec: None,
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
/// for discovered-Snapshot materialization.
#[allow(clippy::too_many_arguments)]
fn bootstrap_work_spec(
    backend: &Backend,
    name: &str,
    namespace: &str,
    auto_create: bool,
    scan_catalog: bool,
    create: Option<&kopiur_api::common::CreateBehavior>,
    mover_defaults: Option<&kopiur_api::common::MoverDefaults>,
) -> MoverWorkSpec {
    MoverWorkSpec {
        version: 1,
        operation: Operation::BootstrapRepository(BootstrapRepositoryOp {
            auto_create,
            scan_catalog,
            // Create-time format knobs (encryption/splitter/hash/ECC) honored only
            // when the bootstrap creates the repo (ADR-0005 §13(a)).
            create_options: kopiur_mover::workspec::CreateOptionsSpec::from_create(create),
            // Stamped on CREATE only: the stable owner the managed Maintenance's
            // movers compare against and claim (never the creating pod's
            // ephemeral identity).
            maintenance_owner: Some(kopiur_api::maintenance::kopia_owner_for_lease(
                &kopiur_api::maintenance::managed_lease(
                    kopiur_api::common::RepositoryKind::Repository,
                    namespace,
                    name,
                ),
            )),
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
        // Bootstrap is a connect/create probe, not a data run: kopia defaults.
        cache: Default::default(),
        // Apply the repo throttle on the bootstrap connection too (§13(e)).
        throttle: io::throttle_spec(mover_defaults),
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
/// `Ready` + uniqueId, then materialize discovered Snapshots from the returned
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

    // Classify the (result, job state) pair as a typed outcome (ADR §5.5): a
    // result-less failed Job and a kopia-rejected connect are distinct,
    // exhaustively-handled modes — never a silent `Failed/Unknown` with no
    // Event — and the success arm *owns* the result, so there is no
    // `.expect()` invariant to get wrong.
    let result = match io::bootstrap_outcome(result, job_succeeded, job_name) {
        // Result not visible yet (write/propagation race): requeue briefly rather
        // than guessing. A truly result-less Job stays terminal for the next pass.
        io::BootstrapOutcome::ResultPending => {
            tracing::warn!(repo = %name, "bootstrap Job complete but result not readable yet; requeueing");
            return Ok(Action::requeue(Duration::from_secs(5)));
        }
        io::BootstrapOutcome::Failed(failure) => {
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
                failure.publish(ctx, &io::event_ref(repo), name).await;
                tracing::warn!(repo = %name, reason, "repository bootstrap failed");
            }
            return Ok(Action::requeue(Duration::from_secs(120)));
        }
        io::BootstrapOutcome::Succeeded(result) => result,
    };

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
    // Guarded write: this path re-runs on EVERY reconcile while the finished
    // bootstrap Job exists, so the steady-state pass must be a true no-op — a
    // re-write of identical status would bump `resourceVersion` and re-trigger
    // this reconciler through its own primary watch, in a tight loop.
    let current = serde_json::to_value(&repo.status).ok();
    io::patch_status_if_changed(
        api,
        name,
        current.as_ref(),
        serde_json::json!({
            "phase": "Ready",
            "backend": backend.kind_str(),
            "uniqueId": result.unique_id,
            // Track the generation this result was taken for — the recycle gate
            // compares it so a spec edit re-runs the bootstrap instead of
            // re-reporting the old repository's identity forever.
            "observedGeneration": repo.metadata.generation,
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

    // Materialize/expire discovered Snapshots from the snapshots the Job
    // returned — once per result: after the first scan stamps `lastRefreshAt`,
    // re-reads of the same finished Job skip the (stale) listing, and the next
    // due refresh recycles the Job for a fresh one. `scan_due`'s generation arm
    // covers the spec-change recycle: that fresh result must be scanned NOW
    // (e.g. a tightened `catalog.retain` expires rows), not at the next timed
    // refresh — `repo` is the pre-reconcile cache, so its observedGeneration is
    // still the old one exactly once per spec change.
    let interval = CatalogBounds::effective_refresh_interval(repo.spec.catalog.as_ref());
    if catalog::scan_due(
        repo.metadata.generation,
        repo.status.as_ref().and_then(|s| s.observed_generation),
        last_refresh_at(repo),
        interval,
        chrono::Utc::now(),
    ) {
        run_catalog_scan(
            ctx,
            repo,
            namespace,
            name,
            repo_uid,
            &result.snapshots,
            result.snapshot_count,
            result.snapshots_truncated,
        )
        .await?;
    }

    // Ensure the managed Maintenance for this repo (ADR §3.7). Build on the
    // conditions we just patched (which include `Bootstrapped`), NOT the stale
    // cached object — otherwise this patch would drop the `Bootstrapped`
    // condition we set above (both writes replace the whole conditions array).
    // §11: a ReadOnly repository runs no maintenance — skip the projection.
    if repo.spec.mode.allows_writes() {
        io::ensure_maintenance(
            ctx,
            api,
            repo,
            &io::event_ref(repo),
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
    }

    Ok(Action::requeue(catalog::reconcile_interval(
        repo.spec.catalog.as_ref(),
    )))
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

/// Run the shared catalog scan ([`crate::catalog::scan`]) for a namespaced
/// `Repository` and reflect the outcome: the discovered-row count +
/// `lastRefreshAt` stamp on `status.catalog`, the repository-wide snapshot count
/// on `storageStats`, and the size gauge.
///
/// `listing` is the snapshot set to reconcile against: produced in-process for
/// the bare-path filesystem backend, or carried back from the bootstrap Job for
/// everything else. `total_snapshot_count` is the authoritative repository-wide
/// count (may exceed `listing.len()` when the Job capped the returned entries —
/// `listing_truncated`, see `BootstrapResult::snapshots_truncated`).
#[allow(clippy::too_many_arguments)]
async fn run_catalog_scan(
    ctx: &Context,
    repo: &Repository,
    namespace: &str,
    repo_name: &str,
    repo_uid: &str,
    listing: &[SnapshotListEntry],
    total_snapshot_count: i64,
    listing_truncated: bool,
) -> Result<()> {
    let owner_ref = io::owner_ref_for(repo, "Repository")?;
    let outcome = catalog::scan(
        ctx,
        catalog::ScanOwner::Repository {
            name: repo_name,
            namespace,
        },
        owner_ref,
        repo_uid,
        catalog::Placement::Namespace(namespace),
        repo.spec.catalog.as_ref(),
        listing,
        listing_truncated,
    )
    .await?;

    // Logical bytes under management is recorded directly from kopia's data
    // (the status field is a human string, so the gauge bypasses it).
    ctx.metrics.set_repo_size_bytes(
        namespace,
        repo_name,
        logical_bytes_under_management(listing),
    );

    let api: Api<Repository> = Api::namespaced(ctx.client.clone(), namespace);
    io::patch_status(
        &api,
        repo_name,
        serde_json::json!({
            "catalog": {
                "discoveredBackupCount": outcome.discovered,
                "lastRefreshAt": chrono::Utc::now().to_rfc3339(),
            },
            "storageStats": { "snapshotCount": total_snapshot_count },
        }),
    )
    .await?;
    Ok(())
}

/// `error_policy` for the `Repository` controller.
pub fn error_policy(obj: Arc<Repository>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("Repository", obj.as_ref(), err, &ctx)
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
}
