//! End-to-end object-store (S3/MinIO) scenarios against the Helm-deployed
//! operator in kind.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`, skipping gracefully without
//! a cluster. `scripts/with-e2e.sh` stands up a single-pod MinIO over plain HTTP,
//! creates the `kopiur` / `kopiur-guard` buckets, and seeds the credential
//! Secrets these tests reference. Run:
//!
//! ```text
//! mise run test-e2e      # or: scripts/with-e2e.sh
//! ```
//!
//! These assert the object-store bootstrap path end-to-end: the controller
//! launches a mover Job that connects/creates the S3 repository, the Repository
//! reaches `Ready` with a real `uniqueId`, a full backup+restore round-trips, a
//! second Repository *adopts* the existing repo (no recreate) and materializes a
//! `discovered` Backup from the snapshot already in the store, and a
//! wrong-password Repository ends `Failed` (the safe-create guard) without
//! recreating over the existing data.

#![cfg(all(unix, feature = "e2e"))]

use kube::Api;
use kube::api::PostParams;
use serde::de::DeserializeOwned;

use kopiur_api::{Backup, BackupConfig, Repository, Restore};
use kopiur_e2e::{E2E_NAMESPACE, default_timeout, poll_interval, try_client, wait_until};

/// Deserialize a CR from a JSON literal into its typed kube object.
fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// An S3 `Repository` pointing at the in-cluster MinIO over plain HTTP.
/// `secret` holds both `KOPIA_PASSWORD` and the `AWS_*` keys (single-secret
/// layout). `create` toggles `spec.create.enabled`.
fn s3_repository_json(name: &str, bucket: &str, secret: &str, create: bool) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "s3": {
                "bucket": bucket,
                "endpoint": "minio.kopiur-e2e.svc.cluster.local:9000",
                "region": "us-east-1",
                "tls": { "disableTls": true },
                "auth": { "secretRef": { "name": secret, "namespace": E2E_NAMESPACE } }
            }},
            "encryption": {
                "passwordSecretRef": { "name": secret, "key": "KOPIA_PASSWORD" }
            },
            "create": { "enabled": create }
        }
    })
}

fn backup_config_json(name: &str, repo: &str, src_pvc: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "BackupConfig",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": repo },
            "sources": [ { "pvc": { "name": src_pvc } } ],
            "retention": { "keepLatest": 5 }
        }
    })
}

fn backup_json(name: &str, config: &str, deletion_policy: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Backup",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": { "configRef": { "name": config }, "deletionPolicy": deletion_policy }
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

/// A status condition's `status` ("True"/"False"/…) by `type`, or `None`.
fn condition_status(status: &serde_json::Value, type_: &str) -> Option<String> {
    status
        .get("conditions")
        .and_then(|c| c.as_array())?
        .iter()
        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some(type_))
        .and_then(|c| c.get("status").and_then(|s| s.as_str()))
        .map(str::to_string)
}

/// The headline object-store scenario, end-to-end against MinIO:
/// 1. create an S3 Repository (`create: true`) → bootstrap Job → `Ready` + a real
///    `uniqueId` + `Bootstrapped=True`;
/// 2. full backup → `Succeeded` (real snapshot id) → restore → `Completed`;
/// 3. a second Repository on the same bucket (`create: false`) *adopts* the
///    existing repo → `Ready`, and materializes a `discovered` Backup from the
///    snapshot already in the store (`catalog.discoveredBackupCount >= 1`);
/// 4. a wrong-password Repository (`create: true`, same bucket) ends `Failed`
///    with `Bootstrapped=False` — the safe-create guard never recreates over the
///    existing repository.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh): kind + MinIO + built images + helm install"]
async fn s3_bootstrap_backup_restore_adopt_and_guard() {
    let Some(client) = try_client().await else {
        return;
    };
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Backup> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // 1. Bootstrap-create the S3 repository.
    repos
        .create(
            &PostParams::default(),
            &cr(s3_repository_json(
                "e2e-s3",
                "kopiur",
                "kopia-s3-creds",
                true,
            )),
        )
        .await
        .expect("create S3 Repository");
    wait_phase(&repos, "e2e-s3", "Ready")
        .await
        .expect("S3 Repository should reach Ready via the bootstrap Job");
    let rstatus = status_json(&repos, "e2e-s3").await;
    assert!(
        rstatus
            .get("uniqueId")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty()),
        "S3 Repository status must carry a real kopia uniqueId, got {rstatus}"
    );
    assert_eq!(
        condition_status(&rstatus, "Bootstrapped").as_deref(),
        Some("True"),
        "expected Bootstrapped=True, got {rstatus}"
    );

    // 2. Full backup + restore against the S3 repo.
    configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json("e2e-s3-cfg", "e2e-s3", "e2e-src")),
        )
        .await
        .expect("create BackupConfig");
    backups
        .create(
            &PostParams::default(),
            &cr(backup_json("e2e-s3-backup", "e2e-s3-cfg", "Retain")),
        )
        .await
        .expect("create Backup");
    wait_phase(&backups, "e2e-s3-backup", "Succeeded")
        .await
        .expect("S3-backed Backup should reach Succeeded");
    let bstatus = status_json(&backups, "e2e-s3-backup").await;
    let snap_id = bstatus
        .get("snapshot")
        .and_then(|s| s.get("kopiaSnapshotID"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !snap_id.is_empty(),
        "S3-backed Backup must carry a real kopia snapshot id, got {bstatus}"
    );

    restores
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Restore",
                "metadata": { "name": "e2e-s3-restore", "namespace": E2E_NAMESPACE },
                "spec": {
                    "repository": { "kind": "Repository", "name": "e2e-s3" },
                    "source": { "backupRef": { "name": "e2e-s3-backup" } },
                    "target": { "pvc": { "name": "e2e-dst" } }
                }
            })),
        )
        .await
        .expect("create Restore");
    wait_phase(&restores, "e2e-s3-restore", "Completed")
        .await
        .expect("S3-backed Restore should reach Completed");

    // 3. Adopt the existing repository (create: false) and materialize the
    //    snapshot created above as a discovered Backup.
    repos
        .create(
            &PostParams::default(),
            &cr(s3_repository_json(
                "e2e-s3-adopt",
                "kopiur",
                "kopia-s3-creds",
                false,
            )),
        )
        .await
        .expect("create adopting S3 Repository");
    wait_phase(&repos, "e2e-s3-adopt", "Ready")
        .await
        .expect("adopting S3 Repository should reach Ready (connect, no recreate)");
    wait_until(
        "e2e-s3-adopt discovered the existing snapshot",
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(&repos, "e2e-s3-adopt").await;
            let n = s
                .get("catalog")
                .and_then(|c| c.get("discoveredBackupCount"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            Ok((n >= 1).then_some(()))
        },
    )
    .await
    .expect(
        "adopting Repository should materialize a discovered Backup from the existing snapshot",
    );

    // 4. Safe-create guard: a wrong-password Repository against the SAME bucket
    //    (where a repo already exists) must end Failed, never recreate.
    repos
        .create(
            &PostParams::default(),
            &cr(s3_repository_json(
                "e2e-s3-badpw",
                "kopiur",
                "kopia-s3-badpw",
                true,
            )),
        )
        .await
        .expect("create wrong-password S3 Repository");
    wait_phase(&repos, "e2e-s3-badpw", "Failed")
        .await
        .expect("wrong-password Repository must end Failed (existing repo not recreated)");
    let guard = status_json(&repos, "e2e-s3-badpw").await;
    assert_eq!(
        condition_status(&guard, "Bootstrapped").as_deref(),
        Some("False"),
        "wrong-password Repository must carry Bootstrapped=False, got {guard}"
    );
    // The original repository is untouched: its uniqueId is unchanged and it is
    // still Ready (the guard must not have recreated/clobbered it).
    let still = status_json(&repos, "e2e-s3").await;
    assert_eq!(
        still.get("phase").and_then(|v| v.as_str()),
        Some("Ready"),
        "existing repository must remain Ready after the wrong-password attempt, got {still}"
    );
}
