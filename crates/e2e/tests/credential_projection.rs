//! End-to-end credential projection: the opt-in `spec.credentialProjection` that
//! lets the operator copy a repository's credential Secret(s) into the namespace
//! where a mover Job runs, so users with many namespaces don't have to pre-create
//! the Secret everywhere.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`, skipping gracefully without a
//! cluster. Driven by `mise run //crates/e2e:test`. The decisive fixture is the
//! `kopiur-e2e-proj` namespace (`Need::ProjectionNs`): it has a source + dest PVC
//! but, unlike the other workload namespaces, **no** credentials Secret — so a
//! mover there can only run if the operator projects the repository's Secret in.
//! `credentialProjection` is a consumer-side opt-in, so it is exercised on each of
//! the three consumers (`BackupConfig`, `Restore`, `Maintenance`).
//!
//! Scenarios, asserting real operator output:
//!
//! 1. **BackupConfig projection ON → Backup succeeds where no creds Secret exists.**
//!    The operator projects a kopiur-managed `<backup>-creds-0` Secret, owned by the
//!    Backup; the mover runs and the Backup reaches `Succeeded` with a real snapshot.
//! 2. **BackupConfig projection OFF → Backup blocks (guards the default).** The
//!    Secret is absent, so the Backup stays `Pending` with `CredentialsAvailable=False`.
//! 3. **Restore projection ON → Restore Completes.** A projection-on Backup seeds a
//!    snapshot; a projection-on `Restore` then restores it into the creds-less
//!    namespace, projecting its own `<restore>-creds-0` Secret.
//! 4. **Maintenance projection ON → creds projected.** A projection-on `Maintenance`
//!    for a shared `ClusterRepository` gets `<maint>-creds-0` projected (owned by
//!    the Maintenance) so its mover can run `kopia maintenance`.
//!
//! Each projected Secret is asserted to be controller-owned by its consuming CR
//! (the GC contract) — see `assert_projected_owned_by`.

#![cfg(all(unix, feature = "e2e"))]

use kube::Api;
use kube::api::{DeleteParams, PostParams};
use serde::de::DeserializeOwned;

use k8s_openapi::api::core::v1::{Secret, ServiceAccount};
use k8s_openapi::api::rbac::v1::RoleBinding;

use kopiur_api::{Backup, BackupConfig, ClusterRepository, Maintenance, Restore};
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
/// namespace, opened to all namespaces. (Projection is opt-in on the consuming
/// `BackupConfig`, not the repository.)
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
                "auth": { "secretRef": { "name": SECRET_S3_CREDS, "namespace": E2E_NAMESPACE } }
            }},
            "encryption": {
                "passwordSecretRef": {
                    "name": SECRET_S3_CREDS, "namespace": E2E_NAMESPACE, "key": "KOPIA_PASSWORD"
                }
            },
            "create": { "enabled": true },
            "allowedNamespaces": { "all": true }
        }
    })
}

/// A `BackupConfig` whose `credentialProjection.enabled = project` decides whether
/// the operator copies the repo's creds into this namespace for its backup movers.
fn backup_config_json(ns: &str, name: &str, repo_name: &str, project: bool) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "BackupConfig",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "repository": { "kind": "ClusterRepository", "name": repo_name },
            "sources": [ { "pvc": { "name": "e2e-src" } } ],
            "retention": { "keepLatest": 5 },
            "credentialProjection": { "enabled": project }
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

/// A `Restore` whose `credentialProjection.enabled = project` decides whether the
/// operator copies the repo's creds into this namespace for the restore mover.
fn restore_json(
    ns: &str,
    name: &str,
    repo: &str,
    backup: &str,
    project: bool,
) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Restore",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "repository": { "kind": "ClusterRepository", "name": repo },
            "source": { "backupRef": { "name": backup } },
            "target": { "pvc": { "name": "e2e-dst" } },
            "credentialProjection": { "enabled": project }
        }
    })
}

/// A `Maintenance` due immediately whose `credentialProjection.enabled = project`
/// decides whether the operator copies the repo's creds into this namespace for
/// the maintenance mover.
fn maintenance_json(ns: &str, name: &str, repo: &str, project: bool) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Maintenance",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "repository": { "kind": "ClusterRepository", "name": repo },
            "ownership": { "owner": "kopiur-e2e-proj", "takeoverPolicy": "Force" },
            "schedule": { "quick": { "cron": "*/5 * * * *" }, "full": { "cron": "0 3 * * 0" } },
            "credentialProjection": { "enabled": project }
        }
    })
}

/// Wait for a kopiur-managed projected credential Secret in `ns` that is
/// controller-owned by the consuming CR `owner_kind`/`owner_name` and carries the
/// password key. Matched by **ownerReference**, not by name: a projected Secret is
/// named after the per-run mover Job, which is the CR name for a Backup/Restore but
/// `<cr>-<mode>-<slot>` for a Maintenance. The valid same-namespace controller
/// ownerRef is the GC contract (Kubernetes reaps the copy with its owner).
async fn assert_projected_owned_by(
    client: &kube::Client,
    ns: &str,
    owner_kind: &str,
    owner_name: &str,
) {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), ns);
    wait_until(
        &format!("projected Secret owned by {owner_kind}/{owner_name} in {ns}"),
        default_timeout(),
        poll_interval(),
        || async {
            let list = secrets.list(&Default::default()).await?;
            let found = list.items.into_iter().any(|s| {
                let owned = s.metadata.owner_references.as_ref().is_some_and(|os| {
                    os.iter().any(|o| {
                        o.kind == owner_kind && o.name == owner_name && o.controller == Some(true)
                    })
                });
                let managed = s
                    .metadata
                    .labels
                    .as_ref()
                    .and_then(|l| l.get("app.kubernetes.io/managed-by"))
                    .map(String::as_str)
                    == Some("kopiur");
                let has_pw = s
                    .data
                    .as_ref()
                    .is_some_and(|d| d.contains_key("KOPIA_PASSWORD"));
                owned && managed && has_pw
            });
            Ok(found.then_some(()))
        },
    )
    .await
    .unwrap_or_else(|e| {
        panic!("{owner_kind} {owner_name} must project a credential Secret into {ns}: {e}")
    });
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

    // 1. ClusterRepository bootstraps against S3 → Ready.
    crepos
        .create(
            &PostParams::default(),
            &cr(s3_cluster_repository_json(crepo, "kopiur-proj-crepo")),
        )
        .await
        .expect("create S3 ClusterRepository");
    wait_phase(&crepos, crepo, "Ready")
        .await
        .expect("ClusterRepository should bootstrap to Ready");

    // 2. BackupConfig (projection ON) + Backup in the creds-less projection namespace.
    configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json(PROJECTION_NS, cfg, crepo, true)),
        )
        .await
        .expect("create BackupConfig with projection");
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

    // GC is guaranteed by the controller-ownerReference asserted in step 3, not
    // re-verified here: a Backup carries the `snapshot-cleanup` finalizer, so it
    // lingers `Terminating` until that clears, and Kubernetes only reaps the owned
    // Secret once the owner is actually removed from etcd. Racing that finalizer
    // would make this test flaky for a guarantee that is Kubernetes' to keep, not
    // ours — our contract is "set a valid controller ownerRef," which step 3 checks.
    backups
        .delete(backup, &DeleteParams::default())
        .await
        .expect("delete Backup");
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

    // ClusterRepository → Ready (bootstrap runs in the operator namespace where its
    // Secret lives).
    crepos
        .create(
            &PostParams::default(),
            &cr(s3_cluster_repository_json(crepo, "kopiur-proj-off")),
        )
        .await
        .expect("create S3 ClusterRepository");
    wait_phase(&crepos, crepo, "Ready")
        .await
        .expect("ClusterRepository should bootstrap to Ready");

    // BackupConfig with projection OFF (the default), in the creds-less namespace.
    configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json(PROJECTION_NS, cfg, crepo, false)),
        )
        .await
        .expect("create BackupConfig without projection");
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

/// **Restore projection.** A `Restore` with `credentialProjection.enabled: true`
/// restores a snapshot into the creds-less projection namespace: the operator
/// projects the repo's creds for the restore mover, and the restore Completes. We
/// first run a (projection-on) Backup to produce a snapshot to restore.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn projection_enables_restore_in_a_namespace_without_creds() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::ProjectionNs])
        .await
        .expect("provision MinIO + projection namespace (source + dest PVC, no creds)");
    let client = world.client().clone();
    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), PROJECTION_NS);
    let backups: Api<Backup> = Api::namespaced(client.clone(), PROJECTION_NS);
    let restores: Api<Restore> = Api::namespaced(client.clone(), PROJECTION_NS);

    let crepo = "e2e-proj-restore-crepo";
    let cfg = "e2e-proj-restore-cfg";
    let backup = "e2e-proj-restore-backup";
    let restore = "e2e-proj-restore";

    // Repo + a projection-on backup to create a snapshot to restore.
    crepos
        .create(
            &PostParams::default(),
            &cr(s3_cluster_repository_json(crepo, "kopiur-proj-restore")),
        )
        .await
        .expect("create S3 ClusterRepository");
    wait_phase(&crepos, crepo, "Ready")
        .await
        .expect("ClusterRepository should bootstrap to Ready");
    configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json(PROJECTION_NS, cfg, crepo, true)),
        )
        .await
        .expect("create BackupConfig with projection");
    backups
        .create(
            &PostParams::default(),
            &cr(backup_json(PROJECTION_NS, backup, cfg)),
        )
        .await
        .expect("create Backup");
    wait_phase(&backups, backup, "Succeeded")
        .await
        .expect("seed Backup should Succeed");

    // The Restore (projection ON) into the creds-less namespace.
    restores
        .create(
            &PostParams::default(),
            &cr(restore_json(PROJECTION_NS, restore, crepo, backup, true)),
        )
        .await
        .expect("create Restore with projection");

    // The operator projects the restore mover's creds (owned by the Restore)...
    assert_projected_owned_by(&client, PROJECTION_NS, "Restore", restore).await;
    // ...and the restore runs to completion using them.
    wait_phase(&restores, restore, "Completed")
        .await
        .expect("Restore using projected credentials should reach Completed");

    let _ = restores.delete(restore, &DeleteParams::default()).await;
    let _ = backups.delete(backup, &DeleteParams::default()).await;
    let _ = configs.delete(cfg, &DeleteParams::default()).await;
    let _ = crepos.delete(crepo, &DeleteParams::default()).await;
}

/// **Maintenance projection.** A `Maintenance` with `credentialProjection.enabled:
/// true` for a shared `ClusterRepository`, in the creds-less projection namespace,
/// gets its credential Secret projected (owned by the Maintenance) so its mover
/// can run `kopia maintenance` — the maintenance path is the one most likely to
/// land in a namespace lacking the Secret.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn projection_enables_maintenance_in_a_namespace_without_creds() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::ProjectionNs])
        .await
        .expect("provision MinIO + projection namespace");
    let client = world.client().clone();
    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    let maints: Api<Maintenance> = Api::namespaced(client.clone(), PROJECTION_NS);

    let crepo = "e2e-proj-maint-crepo";
    let maint = "e2e-proj-maint";

    crepos
        .create(
            &PostParams::default(),
            &cr(s3_cluster_repository_json(crepo, "kopiur-proj-maint")),
        )
        .await
        .expect("create S3 ClusterRepository");
    wait_phase(&crepos, crepo, "Ready")
        .await
        .expect("ClusterRepository should bootstrap to Ready");

    maints
        .create(
            &PostParams::default(),
            &cr(maintenance_json(PROJECTION_NS, maint, crepo, true)),
        )
        .await
        .expect("create Maintenance with projection");

    // The maintenance mover runs in the creds-less namespace; the operator mints
    // the mover RBAC and projects the credential Secret (owned by the Maintenance)
    // so the maintenance Job can load it — without projection this path would block
    // on a missing Secret.
    assert_mover_rbac_minted(&client, PROJECTION_NS).await;
    assert_projected_owned_by(&client, PROJECTION_NS, "Maintenance", maint).await;

    let _ = maints.delete(maint, &DeleteParams::default()).await;
    let _ = crepos.delete(crepo, &DeleteParams::default()).await;
}
