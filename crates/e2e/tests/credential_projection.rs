//! End-to-end credential projection: the opt-in `spec.credentialProjection` that
//! lets the operator copy a repository's credential Secret(s) into the namespace
//! where a mover Job runs, so users with many namespaces don't have to pre-create
//! the Secret everywhere.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`, skipping gracefully without a
//! cluster. Driven by `mise run //crates/e2e:test`, which stands up MinIO and the
//! `kopiur-proj-crepo` / `kopiur-proj-off` buckets. The decisive fixture is the
//! `kopiur-e2e-proj` namespace (`Need::ProjectionNs`): it has a source PVC but,
//! unlike the other workload namespaces, **no** credentials Secret — so a mover
//! there can only run if the operator projects the repository's Secret in.
//!
//! Two scenarios, asserting real operator output:
//!
//! 1. **Projection ON → Backup succeeds where no creds Secret exists.** A
//!    `ClusterRepository` with `credentialProjection.enabled: true` (its Secret
//!    pinned to the operator namespace) backs a Backup in the projection namespace.
//!    The operator projects a kopiur-managed `<backup>-creds-0` Secret there, owned
//!    by the Backup; the mover runs and the Backup reaches `Succeeded` with a real
//!    snapshot. Deleting the Backup garbage-collects the projected Secret (the
//!    ownerRef is valid because owner and copy share the namespace).
//!
//! 2. **Projection OFF → Backup blocks on missing creds (guards the default).** A
//!    `ClusterRepository` WITHOUT projection backs a Backup in the same namespace;
//!    the Secret is absent there, so the Backup stays `Pending` with
//!    `CredentialsAvailable=False` — the self-managed default is unchanged.

#![cfg(all(unix, feature = "e2e"))]

use kube::Api;
use kube::api::{DeleteParams, PostParams};
use serde::de::DeserializeOwned;

use k8s_openapi::api::core::v1::{Secret, ServiceAccount};
use k8s_openapi::api::rbac::v1::RoleBinding;

use kopiur_api::{Backup, BackupConfig, ClusterRepository};
use kopiur_e2e::consts::{PROJECTION_NS, SECRET_S3_CREDS};
use kopiur_e2e::{E2E_NAMESPACE, Need, World, default_timeout, poll_interval, wait_until};

/// The chart-minted mover identity (release name `kopiur` → `kopiur-mover`).
const MOVER_NAME: &str = "kopiur-mover";
/// In-cluster MinIO endpoint (plain HTTP via `tls.disableTls`).
const S3_ENDPOINT: &str = "minio.kopiur-e2e.svc.cluster.local:9000";

fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// A cluster-scoped S3 `ClusterRepository` whose creds live in the operator
/// namespace, opened to all namespaces. `project` toggles `credentialProjection`.
fn s3_cluster_repository_json(name: &str, bucket: &str, project: bool) -> serde_json::Value {
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
                "auth": { "secretRef": { "name": SECRET_S3_CREDS, "namespace": E2E_NAMESPACE } }
            }},
            "encryption": {
                "passwordSecretRef": {
                    "name": SECRET_S3_CREDS, "namespace": E2E_NAMESPACE, "key": "KOPIA_PASSWORD"
                }
            },
            "create": { "enabled": true },
            "allowedNamespaces": { "all": true },
            "credentialProjection": { "enabled": project }
        }
    })
}

fn backup_config_json(ns: &str, name: &str, repo_name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "BackupConfig",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "repository": { "kind": "ClusterRepository", "name": repo_name },
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
    wait_until(
        &format!("RoleBinding {ns}/{MOVER_NAME} minted"),
        default_timeout(),
        poll_interval(),
        || async { rbs.get_opt(MOVER_NAME).await.map(|o| o.map(|_| ())) },
    )
    .await
    .unwrap_or_else(|e| panic!("mover RoleBinding must be minted in {ns}: {e}"));
}

/// **Projection ON.** A `ClusterRepository` with `credentialProjection.enabled`
/// backs a Backup in a namespace that has NO creds Secret. The operator projects a
/// kopiur-managed copy there (owned by the Backup), the mover runs to `Succeeded`,
/// and deleting the Backup garbage-collects the projected Secret.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn projection_enables_backup_in_a_namespace_without_creds() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::ProjectionNs])
        .await
        .expect("provision MinIO + projection namespace (source PVC, no creds Secret)");
    let client = world.client().clone();
    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), PROJECTION_NS);
    let backups: Api<Backup> = Api::namespaced(client.clone(), PROJECTION_NS);
    let secrets: Api<Secret> = Api::namespaced(client.clone(), PROJECTION_NS);

    let crepo = "e2e-proj-crepo";
    let cfg = "e2e-proj-cfg";
    let backup = "e2e-proj-backup";
    let projected = format!("{backup}-creds-0");

    // Sanity: the projection namespace really has no source creds Secret.
    assert!(
        secrets.get_opt(SECRET_S3_CREDS).await.unwrap().is_none(),
        "projection namespace must start without the creds Secret"
    );

    // 1. ClusterRepository (projection ON) bootstraps against S3 → Ready.
    crepos
        .create(
            &PostParams::default(),
            &cr(s3_cluster_repository_json(crepo, "kopiur-proj-crepo", true)),
        )
        .await
        .expect("create S3 ClusterRepository with projection");
    wait_phase(&crepos, crepo, "Ready")
        .await
        .expect("ClusterRepository should bootstrap to Ready");

    // 2. BackupConfig + Backup in the creds-less projection namespace.
    configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json(PROJECTION_NS, cfg, crepo)),
        )
        .await
        .expect("create BackupConfig");
    backups
        .create(
            &PostParams::default(),
            &cr(backup_json(PROJECTION_NS, backup, cfg)),
        )
        .await
        .expect("create Backup");

    assert_mover_rbac_minted(&client, PROJECTION_NS).await;

    // 3. The operator projected the credential Secret into the namespace, owned by
    //    the Backup and labeled kopiur-managed, with the password key copied.
    let proj = wait_until(
        &format!("projected Secret {PROJECTION_NS}/{projected}"),
        default_timeout(),
        poll_interval(),
        || async { secrets.get_opt(&projected).await },
    )
    .await
    .expect("the operator must project the credential Secret into the mover namespace");
    let owners = proj.metadata.owner_references.unwrap_or_default();
    assert!(
        owners
            .iter()
            .any(|o| o.kind == "Backup" && o.name == backup),
        "projected Secret must be owned by its Backup (valid same-namespace ownerRef for GC)"
    );
    let labels = proj.metadata.labels.unwrap_or_default();
    assert_eq!(
        labels
            .get("app.kubernetes.io/managed-by")
            .map(String::as_str),
        Some("kopiur"),
        "projected Secret must be labeled kopiur-managed"
    );
    assert!(
        proj.data
            .as_ref()
            .is_some_and(|d| d.contains_key("KOPIA_PASSWORD")),
        "projected Secret must carry the repository password key"
    );

    // 4. The Backup completes — proving the projected creds actually worked.
    wait_phase(&backups, backup, "Succeeded")
        .await
        .expect("Backup using projected credentials should reach Succeeded");

    // 5. Deleting the Backup garbage-collects the projected Secret (ownerRef GC).
    backups
        .delete(backup, &DeleteParams::default())
        .await
        .expect("delete Backup");
    wait_until(
        &format!("projected Secret {PROJECTION_NS}/{projected} garbage-collected"),
        default_timeout(),
        poll_interval(),
        || async {
            Ok(match secrets.get_opt(&projected).await? {
                Some(_) => None,
                None => Some(()),
            })
        },
    )
    .await
    .expect("deleting the Backup must GC its projected credential Secret");

    let _ = configs.delete(cfg, &DeleteParams::default()).await;
    let _ = crepos.delete(crepo, &DeleteParams::default()).await;
}

/// **Projection OFF (default).** Without `credentialProjection`, a Backup in a
/// namespace lacking the creds Secret blocks on `CredentialsAvailable=False` and
/// never launches a mover — the self-managed default is unchanged.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn without_projection_a_backup_blocks_on_missing_credentials() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::ProjectionNs])
        .await
        .expect("provision MinIO + projection namespace");
    let client = world.client().clone();
    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), PROJECTION_NS);
    let backups: Api<Backup> = Api::namespaced(client.clone(), PROJECTION_NS);

    let crepo = "e2e-proj-off-crepo";
    let cfg = "e2e-proj-off-cfg";
    let backup = "e2e-proj-off-backup";

    // ClusterRepository WITHOUT projection → Ready (bootstrap runs in the operator
    // namespace where its Secret lives).
    crepos
        .create(
            &PostParams::default(),
            &cr(s3_cluster_repository_json(crepo, "kopiur-proj-off", false)),
        )
        .await
        .expect("create S3 ClusterRepository without projection");
    wait_phase(&crepos, crepo, "Ready")
        .await
        .expect("ClusterRepository should bootstrap to Ready");

    configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json(PROJECTION_NS, cfg, crepo)),
        )
        .await
        .expect("create BackupConfig");
    backups
        .create(
            &PostParams::default(),
            &cr(backup_json(PROJECTION_NS, backup, cfg)),
        )
        .await
        .expect("create Backup");

    // The Backup blocks Pending with an actionable CredentialsAvailable=False — it
    // must NOT progress to Running/Succeeded without the Secret.
    wait_until(
        &format!("{backup} reports CredentialsAvailable=False"),
        default_timeout(),
        poll_interval(),
        || async {
            let status = status_json(&backups, backup).await;
            let blocked = status
                .get("conditions")
                .and_then(|c| c.as_array())
                .map(|conds| {
                    conds.iter().any(|c| {
                        c.get("type").and_then(|t| t.as_str()) == Some("CredentialsAvailable")
                            && c.get("status").and_then(|s| s.as_str()) == Some("False")
                    })
                })
                .unwrap_or(false);
            Ok(blocked.then_some(()))
        },
    )
    .await
    .expect(
        "a non-projecting Backup must surface CredentialsAvailable=False when the Secret is absent",
    );
    let phase = status_json(&backups, backup)
        .await
        .get("phase")
        .and_then(|p| p.as_str())
        .unwrap_or_default()
        .to_string();
    assert_ne!(
        phase, "Succeeded",
        "the blocked Backup must not have succeeded"
    );
    assert_ne!(
        phase, "Running",
        "the blocked Backup must not have launched a mover"
    );

    let _ = backups.delete(backup, &DeleteParams::default()).await;
    let _ = configs.delete(cfg, &DeleteParams::default()).await;
    let _ = crepos.delete(crepo, &DeleteParams::default()).await;
}
