//! The `ClusterRepository` reconciler (ADR §3.2, §2.3).
//!
//! Same storage lifecycle as [`crate::repository`] (connect/create, status,
//! catalog scan), plus the cluster-scoped placement rule for discovered
//! `Snapshot` CRs (ADR §2.3): a discovered snapshot is materialized in the
//! namespace named by its identity hostname **if** that namespace exists and is
//! in `allowedNamespaces`; otherwise it falls back to `catalog.fallbackNamespace`.
//!
//! [`placement_namespace`] encodes that rule purely and is unit-tested; the
//! existence check and `Snapshot` creation are the thin IO parts.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};

use kopiur_api::backend::Backend;
use kopiur_api::common::RepositoryKind;
use kopiur_api::{ClusterRepository, RepositoryPhase, validate};
use kopiur_kopia::ConnectSpec;
use kopiur_mover::bootstrap::{BootstrapResult, RESULT_CONFIGMAP_KEY};
use kopiur_mover::workspec::{
    BootstrapRepositoryOp, MoverOptions, MoverWorkSpec, Operation, ResolvedIdentity, TargetRef,
};

use crate::consts::{API_VERSION, BOOTSTRAP_JOB_DEADLINE_SECS, REPOSITORY_BOOTSTRAPPED_CONDITION};
use crate::context::Context;
use crate::error::{Error, Result, TERMINAL_HEARTBEAT, error_policy_for};
use crate::io;
use crate::jobs::{self, JobLimits, MoverJobInputs};
use crate::snapshot::{backend_to_repository_connect, mover_pull_policy_pub};

/// Where to materialize a discovered `Snapshot` under a `ClusterRepository`
/// (ADR §2.3). `identity_namespace` is the namespace named by the snapshot's
/// identity hostname; `namespace_allowed` is whether it exists and is in the
/// tenancy gate. Falls back to `fallback` when not allowed; returns `None` if
/// neither is available (caller skips materialization with a warning).
pub fn placement_namespace<'a>(
    identity_namespace: &'a str,
    namespace_allowed: bool,
    fallback: Option<&'a str>,
) -> Option<&'a str> {
    if namespace_allowed {
        Some(identity_namespace)
    } else {
        fallback
    }
}

/// Resolve where a `ClusterRepository`'s managed (namespaced) `Maintenance` CR
/// should live (ADR §3.7): `spec.maintenance.namespace` if set, else the
/// operator's own namespace (`KOPIUR_NAMESPACE`). `None` when neither is available —
/// `ensure_maintenance` then surfaces an actionable `MaintenanceNamespaceUnresolved`
/// condition rather than guessing.
fn cluster_maintenance_placement(ctx: &Context, repo: &ClusterRepository) -> Option<String> {
    repo.spec
        .maintenance
        .as_ref()
        .and_then(|m| m.namespace.clone())
        .or_else(|| ctx.operator_namespace.clone())
}

/// Reconcile a `ClusterRepository`.
#[tracing::instrument(skip(repo, ctx), fields(kind = "ClusterRepository", name = %repo.name_any()))]
pub async fn reconcile(repo: Arc<ClusterRepository>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&repo, &ctx).await;
    ctx.metrics
        .record_reconcile("ClusterRepository", start.elapsed().as_secs_f64());
    record_cluster_repository_status_metrics(&repo, &ctx, result.is_ok()).await;
    result
}

/// Mirror a ClusterRepository's phase + catalog gauges (cluster-scoped, so the
/// `namespace` label is empty). Zeroes the phase on deletion and re-reads the
/// freshest status on success — see the Snapshot equivalent for the rationale.
async fn record_cluster_repository_status_metrics(
    repo: &ClusterRepository,
    ctx: &Context,
    ok: bool,
) {
    let name = repo.name_any();
    if repo.metadata.deletion_timestamp.is_some() {
        ctx.metrics
            .clear_phase::<RepositoryPhase>("ClusterRepository", "", &name);
        return;
    }
    if !ok {
        return;
    }
    let api: Api<ClusterRepository> = Api::all(ctx.client.clone());
    if let Ok(Some(latest)) = api.get_opt(&name).await
        && let Some(status) = latest.status.as_ref()
    {
        if let Some(phase) = status.phase {
            ctx.metrics
                .set_repository_phase("ClusterRepository", "", &name, phase);
        }
        let snapshots = status.storage_stats.as_ref().and_then(|s| s.snapshot_count);
        let discovered = status
            .catalog
            .as_ref()
            .and_then(|c| c.discovered_backup_count);
        if snapshots.is_some() || discovered.is_some() {
            ctx.metrics
                .set_repo_catalog("", &name, snapshots, discovered);
        }
    }
}

async fn reconcile_inner(repo: &ClusterRepository, ctx: &Context) -> Result<Action> {
    let errs = validate::validate_cluster_repository(&repo.spec);
    if let Some(first) = errs.into_iter().next() {
        return Err(Error::Validation(first.to_string()));
    }

    let name = repo.name_any();
    let api: Api<ClusterRepository> = Api::all(ctx.client.clone());

    // §14(e): a suspended ClusterRepository skips connect/bootstrap and maintenance
    // projection — a declarative pause surfaced via a condition.
    if repo.spec.suspend {
        let conds = repo
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();
        let conditions = io::set_ready(
            &conds,
            repo.metadata.generation,
            io::ReadyOutcome::Reconciling,
            "Suspended",
            "ClusterRepository is suspended (spec.suspend); skipping connect and maintenance",
        );
        io::patch_status(
            &api,
            &name,
            serde_json::json!({ "observedGeneration": repo.metadata.generation, "conditions": conditions }),
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Same connect/create/status lifecycle as Repository. Cluster-scoped secret
    // refs MUST carry an explicit namespace (webhook-enforced).
    match &repo.spec.backend {
        // A PVC/NFS-backed filesystem repo is NOT reachable from the controller
        // in-process (the controller pod can't mount the repo volume), so — exactly
        // like a namespaced Repository and like object stores — it bootstraps in a
        // short mover Job that mounts the volume. WITHOUT this guard the in-process
        // connect/create below runs in the CONTROLLER pod where the volume isn't
        // mounted, "creates" the repo in the wrong place, and FALSELY reports `Ready`
        // while the real PVC stays empty — so every consumer then fails far away with
        // a cryptic `repository not initialized in the provided storage`. Routing
        // through the mover Job also makes the status honest: a failed bootstrap Job
        // surfaces `Failed`/`Degraded` + an actionable Event via
        // `finalize_cluster_bootstrap`, never a misleading `Ready`.
        Backend::Filesystem(fs) if fs.volume.is_some() => {
            return bootstrap_cluster_via_mover(ctx, repo, &name, &api, &repo.spec.backend).await;
        }
        Backend::Filesystem(fs) => {
            // Read the password Secret up front; its `resourceVersion` drives the
            // hard-stop below (see the namespaced Repository reconciler for the full
            // rationale: a credential fix does not bump `generation`, so the gate must
            // also key on the Secret revision).
            let creds = io::repo_credentials(&repo.spec.encryption);
            let secret_ns = creds.namespace.clone().ok_or_else(|| {
                Error::Validation(
                    "ClusterRepository encryption.passwordSecretRef.namespace is required".into(),
                )
            })?;
            let (password, cred_version) =
                io::read_repo_credential(&ctx.client, &secret_ns, &creds).await?;

            // Hard-stop: terminally Failed for this spec generation AND the password
            // Secret unchanged since → quiet heartbeat. Reopens on a spec change
            // (generation) or a Secret content edit (resourceVersion; re-triggered by
            // the Secret watch in `lib.rs`).
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
                    let phase = if retryable { "Degraded" } else { "Failed" };
                    // Stable, volatile-free condition message; full stderr → Event.
                    let conditions =
                        cluster_bootstrap_condition(repo, false, class.as_str(), class.summary());
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
                        Err(Error::Kopia(e))
                    } else {
                        Ok(Action::requeue(TERMINAL_HEARTBEAT))
                    };
                }
            }
            let status = client.repository_status().await?;
            let allowed_count = allowed_namespace_count(&repo.spec.allowed_namespaces);
            io::patch_status(
                &api,
                &name,
                serde_json::json!({
                    "phase": "Ready",
                    "backend": "Filesystem",
                    "uniqueId": status.unique_id_hex,
                    "allowedNamespaceCount": allowed_count,
                    "observedGeneration": repo.metadata.generation,
                    "resolvedCredentialVersion": cred_version,
                }),
            )
            .await?;

            // NOTE: catalog placement for a ClusterRepository materializes each
            // discovered Snapshot in the namespace named by the snapshot identity's
            // hostname when it's allowed (placement_namespace + the tested
            // validate_consumer_against_cluster_repo gate), else the catalog
            // fallbackNamespace. The placement DECISION is implemented and tested
            // (placement_namespace below); wiring the cross-namespace creation
            // loop is a focused follow-up that reuses the namespaced Repository
            // catalog scan with the placement function selecting the target ns.

            // Ensure the managed Maintenance for this ClusterRepository (ADR §3.7).
            // Cluster-scoped, so the metric namespace label is empty and
            // ref-matching ignores namespace. The (namespaced) Maintenance lands in
            // spec.maintenance.namespace, else the operator's own namespace.
            let conditions = repo
                .status
                .as_ref()
                .map(|s| s.conditions.clone())
                .unwrap_or_default();
            // §11: ReadOnly cluster repos run no maintenance.
            if repo.spec.mode.allows_writes() {
                let placement = cluster_maintenance_placement(ctx, repo);
                io::ensure_maintenance(
                    ctx,
                    &api,
                    repo,
                    &io::event_ref(repo),
                    RepositoryKind::ClusterRepository,
                    "ClusterRepository",
                    "",
                    None,
                    placement.as_deref(),
                    &name,
                    repo.spec.maintenance.as_ref(),
                    &conditions,
                    repo.metadata.generation,
                )
                .await;
            }
        }
        other => {
            // Object-store backends bootstrap via a short-lived mover Job (ADR
            // §5.4). The Job runs in the credentials Secret's namespace (so its
            // `envFrom` resolves) and is owned by this cluster-scoped CR (a
            // namespaced dependent may have a cluster-scoped owner; GC works).
            return bootstrap_cluster_via_mover(ctx, repo, &name, &api, other).await;
        }
    }

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Drive the mover-Job bootstrap for a `ClusterRepository` whose backend the
/// controller cannot reach in-process — object stores AND volume-backed (PVC / inline
/// NFS) filesystem repos (the Job mounts the repo volume). Mirrors the namespaced
/// [`crate::repository::reconcile`] path, minus discovered-Snapshot materialization:
/// cross-namespace catalog placement for a ClusterRepository is a separate concern
/// (see [`placement_namespace`]), so `scanCatalog` is off and the bootstrap reports
/// identity + snapshot count only.
async fn bootstrap_cluster_via_mover(
    ctx: &Context,
    repo: &ClusterRepository,
    name: &str,
    api: &Api<ClusterRepository>,
    backend: &Backend,
) -> Result<Action> {
    // The Job + its result ConfigMap live in the credentials Secret's namespace
    // (cluster-scoped refs must carry an explicit namespace — webhook-enforced).
    let creds = io::repo_credentials(&repo.spec.encryption);
    let job_ns = creds.namespace.clone().ok_or_else(|| {
        Error::Validation(
            "ClusterRepository encryption.passwordSecretRef.namespace is required".into(),
        )
    })?;
    let job_name = format!("{name}-bootstrap");
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), &job_ns);

    if let Some(job) = job_api.get_opt(&job_name).await? {
        return match crate::snapshot::job_terminal_state(&job) {
            None => {
                io::patch_status(
                    api,
                    name,
                    serde_json::json!({ "phase": "Initializing", "backend": backend.kind_str() }),
                )
                .await?;
                Ok(Action::requeue(Duration::from_secs(15)))
            }
            Some(success) => {
                finalize_cluster_bootstrap(
                    ctx, repo, name, &job_ns, &job_name, api, backend, success,
                )
                .await
            }
        };
    }

    let create_enabled = repo
        .spec
        .create
        .as_ref()
        .map(|c| c.enabled)
        .unwrap_or(false);
    // ClusterRepository: no discovered-Snapshot materialization (scanCatalog off).
    let work_spec = cluster_bootstrap_work_spec(
        backend,
        name,
        &job_ns,
        create_enabled,
        repo.spec.create.as_ref(),
        repo.spec.mover_defaults.as_ref(),
    );
    let creds_secrets = io::mover_creds_secrets(backend, &repo.spec.encryption);
    let owner = io::owner_ref_for(repo, "ClusterRepository")?;
    let mut labels = BTreeMap::new();
    labels.insert(
        "kopiur.home-operations.com/cluster-repository".to_string(),
        name.to_string(),
    );
    // The cluster-repo bootstrap Job inherits the repository's `moverDefaults`
    // (ADR-0004 §1) — same bootstrap-gap fix as the namespaced Repository. The Job
    // lands in `job_ns` (spec.maintenance.namespace or KOPIUR_NAMESPACE); not gated
    // (repo-owner-authored, operator namespace).
    let resolved_mover = kopiur_api::common::resolve_mover(
        repo.spec.mover_defaults.as_ref(),
        None,
        None,
        None,
        None,
        None,
    );
    // A volume-backed filesystem repo mounts its PVC / inline-NFS export read-write at
    // the backend path so the mover can connect/create the kopia repo there. Object
    // stores reach the backend over the network, so they mount nothing.
    let repo_volume =
        io::filesystem_repo_mount_source(backend).map(|source| jobs::VolumeMountSpec {
            source,
            mount_path: io::filesystem_repo_path(backend).unwrap_or_default(),
            read_only: false,
        });
    let inputs = MoverJobInputs {
        name: &job_name,
        namespace: &job_ns,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: mover_pull_policy_pub(),
        // Bound the bootstrap Job so a pod that never schedules (missing mover SA,
        // image-pull failure) becomes terminal-`Failed` instead of hanging — then
        // `finalize_cluster_bootstrap` runs and surfaces a Warning Event.
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
        service_account: ctx.mover_service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
        annotations: Default::default(),
        // Bootstrap is a short connect/create probe: an emptyDir cache suffices.
        cache_volume: Default::default(),
        readiness_exec: None,
    };
    // The bootstrap Job runs in the credentials Secret's namespace (`job_ns`), where
    // the Secret is present by construction — but the mover SA must still be minted
    // there (it is NOT the operator SA). Ensure the mover SA + RoleBinding exist
    // before launching (ADR §4.12).
    if let Some(sa) = ctx.mover_service_account.as_deref() {
        io::ensure_mover_rbac(
            &ctx.client,
            &job_ns,
            sa,
            &ctx.mover_role_kind,
            &ctx.mover_clusterrole,
        )
        .await?;
    }
    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, &job_ns, &job_name, &cm, &job).await?;
    io::patch_status(
        api,
        name,
        serde_json::json!({ "phase": "Initializing", "backend": backend.kind_str() }),
    )
    .await?;
    tracing::info!(repo = %name, backend = backend.kind_str(), namespace = %job_ns, "launched ClusterRepository bootstrap Job");
    Ok(Action::requeue(Duration::from_secs(15)))
}

/// Build the bootstrap work spec for a `ClusterRepository` object store.
fn cluster_bootstrap_work_spec(
    backend: &Backend,
    name: &str,
    job_ns: &str,
    auto_create: bool,
    create: Option<&kopiur_api::common::CreateBehavior>,
    mover_defaults: Option<&kopiur_api::common::MoverDefaults>,
) -> MoverWorkSpec {
    MoverWorkSpec {
        version: 1,
        operation: Operation::BootstrapRepository(BootstrapRepositoryOp {
            auto_create,
            scan_catalog: false,
            create_options: kopiur_mover::workspec::CreateOptionsSpec::from_create(create),
            // Stamped on CREATE only (see the Repository sibling).
            maintenance_owner: Some(kopiur_api::maintenance::kopia_owner_for_lease(
                &kopiur_api::maintenance::managed_lease(
                    kopiur_api::common::RepositoryKind::ClusterRepository,
                    job_ns,
                    name,
                ),
            )),
        }),
        identity: ResolvedIdentity {
            username: "kopiur-bootstrap".to_string(),
            hostname: name.to_string(),
            source_path: String::new(),
        },
        repository: backend_to_repository_connect(backend),
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "ClusterRepository".to_string(),
            name: name.to_string(),
            namespace: job_ns.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
        // Bootstrap is a connect/create probe, not a data run: kopia defaults.
        cache: Default::default(),
        throttle: io::throttle_spec(mover_defaults),
    }
}

/// Reflect a finished `ClusterRepository` bootstrap Job into its status.
#[allow(clippy::too_many_arguments)]
async fn finalize_cluster_bootstrap(
    ctx: &Context,
    repo: &ClusterRepository,
    name: &str,
    job_ns: &str,
    job_name: &str,
    api: &Api<ClusterRepository>,
    backend: &Backend,
    job_succeeded: bool,
) -> Result<Action> {
    let result = read_cluster_bootstrap_result(ctx, job_ns, job_name).await?;

    // Classify the (result, job state) pair as a typed outcome (ADR §5.5): a
    // result-less failed Job and a kopia-rejected connect are distinct,
    // exhaustively-handled modes — never a silent `Failed/Unknown` with no
    // Event — and the success arm *owns* the result, so there is no
    // `.expect()` invariant to get wrong.
    let result = match io::bootstrap_outcome(result, job_succeeded, job_name) {
        io::BootstrapOutcome::ResultPending => {
            tracing::warn!(repo = %name, "bootstrap Job complete but result not readable yet; requeueing");
            return Ok(Action::requeue(Duration::from_secs(5)));
        }
        io::BootstrapOutcome::Failed(failure) => {
            let reason = failure.reason();
            let conditions =
                cluster_bootstrap_condition(repo, false, reason, &failure.condition_message());
            // Guard the write so the Event + warn log fire only on the real transition,
            // not on every 120 s re-read (the message is stable → a true no-op once
            // written, so no reconcile hot-loop and no Event spam).
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
                tracing::warn!(repo = %name, reason, "ClusterRepository bootstrap failed");
            }
            return Ok(Action::requeue(Duration::from_secs(120)));
        }
        io::BootstrapOutcome::Succeeded(result) => result,
    };

    let allowed_count = allowed_namespace_count(&repo.spec.allowed_namespaces);
    let conditions = cluster_bootstrap_condition(
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
            // Report the generation we just bootstrapped, so `observedGeneration` tracks
            // `metadata.generation` after a successful first bootstrap (matching the
            // already-bootstrapped path and the namespaced Repository). Without it a
            // freshly-bootstrapped ClusterRepository shows no observedGeneration until the
            // next spec change. (Fix originally from community PR #82, ChosenQuill.)
            "observedGeneration": repo.metadata.generation,
            "backend": backend.kind_str(),
            "uniqueId": result.unique_id,
            "allowedNamespaceCount": allowed_count,
            "storageStats": { "snapshotCount": result.snapshot_count },
            "conditions": conditions,
        }),
    )
    .await?;

    // Ensure the managed Maintenance for this ClusterRepository (§3.7). Build on
    // the conditions we just patched (including `Bootstrapped`), not the stale
    // cached object, so this patch doesn't drop the `Bootstrapped` set above.
    // §11: ReadOnly cluster repos run no maintenance.
    if repo.spec.mode.allows_writes() {
        let placement = cluster_maintenance_placement(ctx, repo);
        io::ensure_maintenance(
            ctx,
            api,
            repo,
            &io::event_ref(repo),
            RepositoryKind::ClusterRepository,
            "ClusterRepository",
            "",
            None,
            placement.as_deref(),
            name,
            repo.spec.maintenance.as_ref(),
            &conditions,
            repo.metadata.generation,
        )
        .await;
    }

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Read the [`BootstrapResult`] the mover wrote into the work-spec ConfigMap (in
/// the credentials Secret's namespace).
async fn read_cluster_bootstrap_result(
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

/// Upsert the `Bootstrapped` condition onto the ClusterRepository's conditions.
fn cluster_bootstrap_condition(
    repo: &ClusterRepository,
    status: bool,
    reason: &str,
    message: &str,
) -> Vec<Condition> {
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

/// Count of namespaces a `List`/`All` gate resolves to (selector requires a live
/// namespace list and is reported as 0 here without that lookup).
fn allowed_namespace_count(allowed: &kopiur_api::AllowedNamespaces) -> i64 {
    use kopiur_api::AllowedNamespaces;
    match allowed {
        AllowedNamespaces::List(ns) => ns.len() as i64,
        AllowedNamespaces::All(true) => -1, // sentinel: all
        AllowedNamespaces::All(false) => 0,
        AllowedNamespaces::Selector(_) => 0,
    }
}

/// `error_policy` for the `ClusterRepository` controller.
pub fn error_policy(obj: Arc<ClusterRepository>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("ClusterRepository", obj.as_ref(), err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowed_namespace_is_used_directly() {
        assert_eq!(
            placement_namespace("billing", true, Some("kopia-system")),
            Some("billing")
        );
    }

    #[test]
    fn disallowed_namespace_falls_back() {
        assert_eq!(
            placement_namespace("evil", false, Some("kopia-system")),
            Some("kopia-system")
        );
    }

    #[test]
    fn disallowed_and_no_fallback_yields_none() {
        assert_eq!(placement_namespace("evil", false, None), None);
    }
}
