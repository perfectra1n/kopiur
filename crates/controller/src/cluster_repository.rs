//! The `ClusterRepository` reconciler (ADR §3.2, §2.3).
//!
//! Same storage lifecycle as [`crate::repository`] (connect/create, status,
//! catalog scan), plus the cluster-scoped placement rule for discovered
//! `Backup` CRs (ADR §2.3): a discovered snapshot is materialized in the
//! namespace named by its identity hostname **if** that namespace exists and is
//! in `allowedNamespaces`; otherwise it falls back to `catalog.fallbackNamespace`.
//!
//! [`placement_namespace`] encodes that rule purely and is unit-tested; the
//! existence check and `Backup` creation are the thin IO parts.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::runtime::controller::Action;
use kube::{Api, Resource, ResourceExt};

use kopiur_api::backend::Backend;
use kopiur_api::common::RepositoryKind;
use kopiur_api::{ClusterRepository, RepositoryPhase, validate};
use kopiur_kopia::{ConnectSpec, KopiaErrorClass};
use kopiur_mover::bootstrap::{BootstrapResult, RESULT_CONFIGMAP_KEY};
use kopiur_mover::workspec::{
    BootstrapRepositoryOp, MoverOptions, MoverWorkSpec, Operation, ResolvedIdentity, TargetRef,
};

use crate::backup::{backend_to_repository_connect, mover_pull_policy_pub};
use crate::consts::{API_VERSION, REPOSITORY_BOOTSTRAPPED_CONDITION};
use crate::context::Context;
use crate::error::{Error, Result, error_policy_for};
use crate::io;
use crate::jobs::{self, JobLimits, MoverJobInputs};

/// Where to materialize a discovered `Backup` under a `ClusterRepository`
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
/// freshest status on success — see the Backup equivalent for the rationale.
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

    // Same connect/create/status lifecycle as Repository. Cluster-scoped secret
    // refs MUST carry an explicit namespace (webhook-enforced).
    match &repo.spec.backend {
        Backend::Filesystem(fs) => {
            let creds = io::repo_credentials(&repo.spec.encryption);
            let secret_ns = creds.namespace.clone().ok_or_else(|| {
                Error::Validation(
                    "ClusterRepository encryption.passwordSecretRef.namespace is required".into(),
                )
            })?;
            let password = io::read_repo_password(&ctx.client, &secret_ns, &creds).await?;
            let client = ctx.kopia.build([("KOPIA_PASSWORD".to_string(), password)]);
            let spec = ConnectSpec::Filesystem {
                path: fs.path.clone().into(),
            };
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
                    let message = e.to_string();
                    let conditions =
                        cluster_bootstrap_condition(repo, false, class.as_str(), &message);
                    io::patch_status(
                        &api,
                        &name,
                        serde_json::json!({
                            "phase": "Failed",
                            "backend": "Filesystem",
                            "conditions": conditions,
                        }),
                    )
                    .await?;
                    io::publish_backend_failure(ctx, &repo.object_ref(&()), &name, class, &message)
                        .await;
                    return Err(Error::Kopia(e));
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
                }),
            )
            .await?;

            // NOTE: catalog placement for a ClusterRepository materializes each
            // discovered Backup in the namespace named by the snapshot identity's
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
            let placement = cluster_maintenance_placement(ctx, repo);
            io::ensure_maintenance(
                ctx,
                &api,
                repo,
                &repo.object_ref(&()),
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
        other => {
            // Object-store backends bootstrap via a short-lived mover Job (ADR
            // §5.4). The Job runs in the credentials Secret's namespace (so its
            // `envFrom` resolves) and is owned by this cluster-scoped CR (a
            // namespaced dependent may have a cluster-scoped owner; GC works).
            return bootstrap_cluster_object_store(ctx, repo, &name, &api, other).await;
        }
    }

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// Drive the object-store bootstrap for a `ClusterRepository`. Mirrors the
/// namespaced [`crate::repository::reconcile`] path, minus discovered-Backup
/// materialization: cross-namespace catalog placement for a ClusterRepository is
/// a separate concern (see [`placement_namespace`]), so `scanCatalog` is off and
/// the bootstrap reports identity + snapshot count only.
async fn bootstrap_cluster_object_store(
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
        return match crate::backup::job_terminal_state(&job) {
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
    // ClusterRepository: no discovered-Backup materialization (scanCatalog off).
    let work_spec = cluster_bootstrap_work_spec(backend, name, &job_ns, create_enabled);
    let creds_secrets = io::mover_creds_secrets(backend, &repo.spec.encryption);
    let owner = io::owner_ref_for(repo, "ClusterRepository")?;
    let mut labels = BTreeMap::new();
    labels.insert(
        "kopiur.home-operations.com/cluster-repository".to_string(),
        name.to_string(),
    );
    let inputs = MoverJobInputs {
        name: &job_name,
        namespace: &job_ns,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: mover_pull_policy_pub(),
        limits: JobLimits::default(),
        resources: None,
        security_context: None,
        labels,
        source_pvc: None,
        repo_pvc: None,
        creds_secrets,
        result_configmap: Some(&job_name),
        service_account: ctx.mover_service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
    };
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
) -> MoverWorkSpec {
    MoverWorkSpec {
        version: 1,
        operation: Operation::BootstrapRepository(BootstrapRepositoryOp {
            auto_create,
            scan_catalog: false,
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
    let Some(result) = result else {
        if job_succeeded {
            tracing::warn!(repo = %name, "bootstrap Job complete but result not readable yet; requeueing");
            return Ok(Action::requeue(Duration::from_secs(5)));
        }
        let conditions = cluster_bootstrap_condition(
            repo,
            false,
            "Unknown",
            "bootstrap Job failed without a result",
        );
        io::patch_status(
            api,
            name,
            serde_json::json!({
                "phase": "Failed",
                "backend": backend.kind_str(),
                "conditions": conditions,
            }),
        )
        .await?;
        return Ok(Action::requeue(Duration::from_secs(120)));
    };

    if !result.success {
        let (class, message) = result
            .failure
            .as_ref()
            .map(|f| (f.kopia_error_class.as_str(), f.message.as_str()))
            .unwrap_or(("Unknown", "repository bootstrap failed"));
        let conditions = cluster_bootstrap_condition(repo, false, class, message);
        io::patch_status(
            api,
            name,
            serde_json::json!({
                "phase": "Failed",
                "backend": backend.kind_str(),
                "conditions": conditions,
            }),
        )
        .await?;
        io::publish_backend_failure(
            ctx,
            &repo.object_ref(&()),
            name,
            KopiaErrorClass::from_label(class),
            message,
        )
        .await;
        tracing::warn!(repo = %name, class, "ClusterRepository bootstrap failed");
        return Ok(Action::requeue(Duration::from_secs(120)));
    }

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
    let placement = cluster_maintenance_placement(ctx, repo);
    io::ensure_maintenance(
        ctx,
        api,
        repo,
        &repo.object_ref(&()),
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
pub fn error_policy(_obj: Arc<ClusterRepository>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("ClusterRepository", err, &ctx)
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
