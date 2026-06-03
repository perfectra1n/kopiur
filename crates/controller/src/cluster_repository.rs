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

use std::sync::Arc;

use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};

use kopiur_api::backend::Backend;
use kopiur_api::{ClusterRepository, RepositoryPhase, validate};
use kopiur_kopia::ConnectSpec;

use crate::context::Context;
use crate::error::{Error, Result, error_policy_for};
use crate::io;

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
                if create_enabled {
                    client.repository_create(&spec).await?;
                    client.repository_connect(&spec).await?;
                } else {
                    io::patch_status(
                        &api,
                        &name,
                        serde_json::json!({ "phase": "Failed", "backend": "Filesystem" }),
                    )
                    .await?;
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
        }
        other => {
            io::patch_status(
                &api,
                &name,
                serde_json::json!({ "phase": "Pending", "backend": other.kind_str() }),
            )
            .await?;
            tracing::info!(
                repo = %name,
                backend = other.kind_str(),
                "object-store ClusterRepository: in-process validation not run (filesystem only)"
            );
        }
    }

    Ok(Action::requeue(std::time::Duration::from_secs(300)))
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
