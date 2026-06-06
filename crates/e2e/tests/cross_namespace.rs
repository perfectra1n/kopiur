//! End-to-end cross-namespace lifecycle: the full **bootstrap → repository →
//! Backup** path exercised in a workload namespace SEPARATE from the operator's.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`, skipping gracefully without a
//! cluster. Driven by `mise run //crates/e2e:test`, which stands up MinIO, the
//! `kopiur-xns-crepo` / `kopiur-xns-repo` buckets, and a workload namespace
//! (`kopiur-e2e-xns`) pre-seeded with a source PVC of known data and the S3
//! credentials Secret. Run:
//!
//! ```text
//! mise run //crates/e2e:test
//! ```
//!
//! These assert real operator output for the two cross-namespace shapes:
//!
//! 1. **ClusterRepository → Backup in another namespace.** A cluster-scoped
//!    `ClusterRepository` (its Secret pinned to the operator namespace) bootstraps
//!    against S3, then a `BackupConfig` + `Backup` in the WORKLOAD namespace drive
//!    a mover Job there. The controller must mint the least-privilege mover
//!    `ServiceAccount` + `RoleBinding` in the workload namespace and the Backup
//!    must reach `Succeeded` with a real kopia snapshot id.
//! 2. **Namespaced Repository → Backup, both in a non-operator namespace.** A
//!    `Repository` living entirely in the workload namespace bootstraps against S3
//!    (its bootstrap Job runs there as the minted mover SA), then a Backup in the
//!    same namespace reaches `Succeeded`.
//!
//! 3. **Maintenance mints mover RBAC.** Regression guard for the bug where the
//!    maintenance path was the one mover-Job path that did NOT mint the mover SA,
//!    so a maintenance Job in a fresh namespace `FailedCreate`d with
//!    `serviceaccount "kopiur-mover" not found`. A `Maintenance` in a brand-new
//!    namespace (where no Backup ran) must still get the mover SA minted there.

#![cfg(all(unix, feature = "e2e"))]

use kube::Api;
use kube::api::{DeleteParams, PostParams};
use serde::de::DeserializeOwned;

use k8s_openapi::api::core::v1::ServiceAccount;
use k8s_openapi::api::rbac::v1::RoleBinding;

use kopiur_api::{Backup, BackupConfig, ClusterRepository, Maintenance, Repository};
use kopiur_e2e::{
    E2E_NAMESPACE, Need, World, default_timeout, ensure_namespace, poll_interval, wait_until,
};

/// The workload namespace the harness pre-seeds (source PVC + S3 creds Secret),
/// separate from the operator namespace (`E2E_NAMESPACE`).
const XNS: &str = "kopiur-e2e-xns";
/// The chart-minted mover identity (release name `kopiur` → `kopiur-mover`).
const MOVER_NAME: &str = "kopiur-mover";
/// In-cluster MinIO endpoint (plain HTTP via `tls.disableTls`).
const S3_ENDPOINT: &str = "minio.kopiur-e2e.svc.cluster.local:9000";
/// The single Secret holding `KOPIA_PASSWORD` + the `AWS_*` keys (homelab layout).
const S3_CREDS: &str = "kopia-s3-creds";

/// Deserialize a CR from a JSON literal into its typed kube object.
fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// A cluster-scoped S3 `ClusterRepository`. Its Secret refs MUST carry an explicit
/// namespace (cluster-scoped resources can't infer one), pinned to the operator
/// namespace where the Secret lives. `allowedNamespaces.all` opens it to the
/// workload namespace.
fn s3_cluster_repository_json(name: &str, bucket: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "ClusterRepository",
        "metadata": { "name": name },
        "spec": {
            "backend": { "s3": {
                "bucket": bucket,
                "endpoint": S3_ENDPOINT,
                "region": "us-east-1",
                "tls": { "disableTls": true },
                "auth": { "secretRef": { "name": S3_CREDS, "namespace": E2E_NAMESPACE } }
            }},
            "encryption": {
                "passwordSecretRef": {
                    "name": S3_CREDS, "namespace": E2E_NAMESPACE, "key": "KOPIA_PASSWORD"
                }
            },
            "create": { "enabled": true },
            "allowedNamespaces": { "all": true }
        }
    })
}

/// A namespaced S3 `Repository` living entirely in `ns` (auth + password Secret
/// co-located there). Its bootstrap Job runs in `ns` as the minted mover SA.
fn s3_repository_json(ns: &str, name: &str, bucket: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "backend": { "s3": {
                "bucket": bucket,
                "endpoint": S3_ENDPOINT,
                "region": "us-east-1",
                "tls": { "disableTls": true },
                "auth": { "secretRef": { "name": S3_CREDS, "namespace": ns } }
            }},
            "encryption": {
                "passwordSecretRef": { "name": S3_CREDS, "key": "KOPIA_PASSWORD" }
            },
            "create": { "enabled": true }
        }
    })
}

/// A `BackupConfig` in `ns` referencing a repository by (`repo_kind`, `repo_name`).
/// A `ClusterRepository` ref carries no namespace (the whole point); a `Repository`
/// ref resolves in `ns`.
fn backup_config_json(ns: &str, name: &str, repo_kind: &str, repo_name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "BackupConfig",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "repository": { "kind": repo_kind, "name": repo_name },
            "sources": [ { "pvc": { "name": "e2e-src" } } ],
            "retention": { "keepLatest": 5 }
        }
    })
}

fn backup_json(ns: &str, name: &str, config: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Backup",
        "metadata": { "name": name, "namespace": ns },
        "spec": { "configRef": { "name": config }, "deletionPolicy": "Retain" }
    })
}

/// Poll a namespaced CR until `status.phase == want_phase`.
async fn wait_phase<K>(api: &Api<K>, name: &str, want_phase: &str) -> anyhow::Result<()>
where
    K: kube::Resource + Clone + DeserializeOwned + serde::Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    wait_until(
        &format!("{name} phase={want_phase}"),
        default_timeout(),
        poll_interval(),
        || async {
            match api.get_opt(name).await? {
                Some(obj) => {
                    let v = serde_json::to_value(&obj).unwrap_or_default();
                    let phase = v
                        .get("status")
                        .and_then(|s| s.get("phase"))
                        .and_then(|p| p.as_str())
                        .unwrap_or("");
                    Ok((phase == want_phase).then_some(()))
                }
                None => Ok(None),
            }
        },
    )
    .await
}

/// Read a CR's `status` as JSON (or `null` if absent).
async fn status_json<K>(api: &Api<K>, name: &str) -> serde_json::Value
where
    K: kube::Resource + Clone + DeserializeOwned + serde::Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    match api.get_opt(name).await.ok().flatten() {
        Some(obj) => serde_json::to_value(&obj)
            .ok()
            .and_then(|v| v.get("status").cloned())
            .unwrap_or(serde_json::Value::Null),
        None => serde_json::Value::Null,
    }
}

/// Assert the controller minted the least-privilege mover `ServiceAccount` +
/// `RoleBinding` in `ns` (the workload namespace where the mover Job runs). This is
/// the per-namespace RBAC every mover path must establish before launching a Job
/// (ADR §4.12) — without it the Job `FailedCreate`s with `serviceaccount ... not
/// found` and never schedules a pod.
async fn assert_mover_rbac_minted(client: &kube::Client, ns: &str) {
    let sas: Api<ServiceAccount> = Api::namespaced(client.clone(), ns);
    let rbs: Api<RoleBinding> = Api::namespaced(client.clone(), ns);
    wait_until(
        &format!("ServiceAccount {ns}/{MOVER_NAME} minted"),
        default_timeout(),
        poll_interval(),
        || async { sas.get_opt(MOVER_NAME).await.map(|o| o.map(|_| ())) },
    )
    .await
    .unwrap_or_else(|e| panic!("mover ServiceAccount must be minted in {ns}: {e}"));
    let rb = wait_until(
        &format!("RoleBinding {ns}/{MOVER_NAME} minted"),
        default_timeout(),
        poll_interval(),
        || async { rbs.get_opt(MOVER_NAME).await },
    )
    .await
    .unwrap_or_else(|e| panic!("mover RoleBinding must be minted in {ns}: {e}"));
    assert_eq!(
        rb.role_ref.name, MOVER_NAME,
        "the minted RoleBinding must target the mover role"
    );
}

/// Assert a Succeeded Backup carries a real (non-empty) kopia snapshot id.
fn assert_real_snapshot(status: &serde_json::Value, what: &str) {
    let snap_id = status
        .get("snapshot")
        .and_then(|s| s.get("kopiaSnapshotID"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !snap_id.is_empty(),
        "{what} must carry a real kopia snapshot id, got status: {status}"
    );
}

/// **ClusterRepository → Backup in a different namespace.** The cluster-scoped repo
/// bootstraps against S3 (its Secret in the operator namespace); a Backup in the
/// workload namespace then drives a mover Job there, the controller mints the mover
/// RBAC in that namespace, and the Backup reaches `Succeeded` with a real snapshot.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn clusterrepository_bootstrap_then_cross_namespace_backup_succeeds() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::WorkloadNs])
        .await
        .expect("provision MinIO + workload namespace");
    let client = world.client().clone();
    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), XNS);
    let backups: Api<Backup> = Api::namespaced(client.clone(), XNS);

    let crepo = "e2e-xns-full-crepo";
    let cfg = "e2e-xns-full-cfg";
    let backup = "e2e-xns-full-backup";

    // 1. ClusterRepository bootstraps against S3 → Ready.
    crepos
        .create(
            &PostParams::default(),
            &cr(s3_cluster_repository_json(crepo, "kopiur-xns-crepo")),
        )
        .await
        .expect("create S3 ClusterRepository");
    wait_phase(&crepos, crepo, "Ready")
        .await
        .expect("S3 ClusterRepository should bootstrap to Ready");

    // 2. BackupConfig + Backup in the WORKLOAD namespace.
    configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json(XNS, cfg, "ClusterRepository", crepo)),
        )
        .await
        .expect("create BackupConfig in workload namespace");
    backups
        .create(&PostParams::default(), &cr(backup_json(XNS, backup, cfg)))
        .await
        .expect("create Backup in workload namespace");

    // 3. The controller mints the mover SA + RoleBinding in the workload namespace.
    assert_mover_rbac_minted(&client, XNS).await;

    // 4. The Backup completes with a real snapshot id.
    wait_phase(&backups, backup, "Succeeded")
        .await
        .expect("cross-namespace ClusterRepository Backup should reach Succeeded");
    assert_real_snapshot(
        &status_json(&backups, backup).await,
        "cross-namespace ClusterRepository Backup",
    );

    // Cleanup: the cluster-scoped repo (the namespaced CRs persist with the shared
    // workload namespace, which the harness owns).
    let _ = crepos.delete(crepo, &DeleteParams::default()).await;
}

/// **Namespaced Repository → Backup, both in a non-operator namespace.** A
/// `Repository` living entirely in the workload namespace bootstraps against S3
/// (its bootstrap Job runs there as the minted mover SA), then a Backup in the same
/// namespace reaches `Succeeded` — proving the operator reconciles namespaced
/// Repositories in arbitrary namespaces and mints the mover RBAC there.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn repository_bootstrap_then_backup_in_workload_namespace_succeeds() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::WorkloadNs])
        .await
        .expect("provision MinIO + workload namespace");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), XNS);
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), XNS);
    let backups: Api<Backup> = Api::namespaced(client.clone(), XNS);

    let repo = "e2e-xns-repo";
    let cfg = "e2e-xns-repo-cfg";
    let backup = "e2e-xns-repo-backup";

    // 1. Namespaced Repository bootstraps against S3 in the workload namespace.
    repos
        .create(
            &PostParams::default(),
            &cr(s3_repository_json(XNS, repo, "kopiur-xns-repo")),
        )
        .await
        .expect("create S3 Repository in workload namespace");
    wait_phase(&repos, repo, "Ready")
        .await
        .expect("S3 Repository should bootstrap to Ready in the workload namespace");

    // 2. The bootstrap Job ran in the workload namespace, so the mover RBAC is there.
    assert_mover_rbac_minted(&client, XNS).await;

    // 3. BackupConfig + Backup in the same namespace → Succeeded with a real snapshot.
    configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json(XNS, cfg, "Repository", repo)),
        )
        .await
        .expect("create BackupConfig in workload namespace");
    backups
        .create(&PostParams::default(), &cr(backup_json(XNS, backup, cfg)))
        .await
        .expect("create Backup in workload namespace");
    wait_phase(&backups, backup, "Succeeded")
        .await
        .expect("workload-namespace Repository Backup should reach Succeeded");
    assert_real_snapshot(
        &status_json(&backups, backup).await,
        "workload-namespace Repository Backup",
    );

    // Cleanup the namespaced Repository (its Backup/Config GC with it / the ns).
    let _ = repos.delete(repo, &DeleteParams::default()).await;
}

/// **Maintenance mints mover RBAC (regression).** The maintenance path was the one
/// mover-Job path that did NOT mint the per-namespace mover SA, so a maintenance Job
/// in a namespace where no Backup had run `FailedCreate`d with `serviceaccount
/// "kopiur-mover" not found` and never scheduled a pod. A `Maintenance` created in a
/// brand-new namespace (no Backup there) must still get the mover SA minted there —
/// the controller mints it before launching the Job, and a first-ever reconcile is
/// due immediately, so the SA appears without waiting for a cron slot.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn maintenance_in_fresh_namespace_mints_mover_rbac() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio])
        .await
        .expect("provision MinIO + buckets");
    let client = world.client().clone();
    // A namespace dedicated to this scenario where NO Backup ever runs, so only the
    // maintenance path can mint the mover SA here.
    const MAINT_NS: &str = "kopiur-e2e-maint-xns";
    ensure_namespace(&client, MAINT_NS)
        .await
        .expect("create maintenance workload namespace");

    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    let maints: Api<Maintenance> = Api::namespaced(client.clone(), MAINT_NS);

    let crepo = "e2e-maint-xns-crepo";

    // A Ready ClusterRepository the Maintenance can resolve (so it launches a Job).
    crepos
        .create(
            &PostParams::default(),
            &cr(s3_cluster_repository_json(crepo, "kopiur-xns-crepo")),
        )
        .await
        .expect("create S3 ClusterRepository");
    wait_phase(&crepos, crepo, "Ready")
        .await
        .expect("S3 ClusterRepository should bootstrap to Ready");

    // A Maintenance in the fresh namespace, due immediately (first reconcile).
    let maint = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Maintenance",
        "metadata": { "name": "e2e-maint-xns", "namespace": MAINT_NS },
        "spec": {
            "repository": { "kind": "ClusterRepository", "name": crepo },
            "ownership": { "owner": "kopiur-e2e", "takeoverPolicy": "Force" },
            "schedule": {
                "quick": { "cron": "*/5 * * * *" },
                "full": { "cron": "0 3 * * 0" }
            }
        }
    });
    maints
        .create(&PostParams::default(), &cr::<Maintenance>(maint))
        .await
        .expect("create Maintenance in fresh namespace");

    // The maintenance path must mint the mover SA + RoleBinding in MAINT_NS before
    // launching its Job (the regression: it used to skip this and FailedCreate).
    assert_mover_rbac_minted(&client, MAINT_NS).await;

    // Cleanup: the cluster-scoped repo and the dedicated namespace.
    let _ = crepos.delete(crepo, &DeleteParams::default()).await;
    let nss: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(client.clone());
    let _ = nss.delete(MAINT_NS, &DeleteParams::default()).await;
}
