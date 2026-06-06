//! End-to-end lifecycle scenarios for the SFTP, WebDAV, and rclone backends
//! against the Helm-deployed operator in kind.
//!
//! Gated by `#[cfg(all(unix, feature = "e2e"))]` + `#[ignore]`, so the suite
//! compiles everywhere and is a no-op without a cluster. `mise run
//! //crates/e2e:test` stands up the in-cluster backend servers (atmoz/sftp,
//! bytemark/webdav) and seeds the credential Secrets these tests reference.
//!
//! Each test asserts the full bootstrap → backup → restore pipeline against a
//! real backend server, which only passes if the backend's credential plumbing
//! works end to end:
//!   * **SFTP** — the mover materializes the private key + `known_hosts` from the
//!     credentials Secret into files and passes `--keyfile`/`--known-hosts`
//!     (kopia's SFTP backend has no env-var credential form). A regression guard:
//!     before that plumbing, the controller passed `keyfile: None` and the
//!     bootstrap Job hung / failed host-key verification.
//!   * **WebDAV** — credentials flow as `KOPIA_WEBDAV_USERNAME`/`_PASSWORD` env.
//!   * **rclone** — the mover materializes `rclone.conf` and forwards it via
//!     `--rclone-args=--config=…`; needs the `rclone` binary in the mover image.

#![cfg(all(unix, feature = "e2e"))]

use kube::Api;
use kube::api::PostParams;
use serde::Serialize;
use serde::de::DeserializeOwned;

use kopiur_api::{Backup, BackupConfig, Repository, Restore};
use kopiur_e2e::{E2E_NAMESPACE, Need, World, consts, default_timeout, poll_interval, wait_until};

/// Deserialize a CR from a JSON literal into its typed kube object.
fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// Poll a namespaced CR until `status.phase == want_phase`.
async fn wait_phase<K>(api: &Api<K>, name: &str, want_phase: &str) -> anyhow::Result<()>
where
    K: kube::Resource + Clone + DeserializeOwned + Serialize + std::fmt::Debug,
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
    K: kube::Resource + Clone + DeserializeOwned + Serialize + std::fmt::Debug,
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

fn backup_json(name: &str, config: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Backup",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": { "configRef": { "name": config }, "deletionPolicy": "Retain" }
    })
}

fn restore_json(name: &str, repo: &str, backup: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Restore",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": repo },
            "source": { "backupRef": { "name": backup } },
            // Restore into the pre-provisioned destination PVC (from Need::Filesystem).
            "target": { "pvc": { "name": consts::PVC_DST } }
        }
    })
}

/// Drive the full pipeline for one backend: create the `Repository` (`create:
/// true`) → `Ready` with a real `uniqueId`; back up the source PVC → `Succeeded`
/// with a real kopia snapshot id; restore it → `Completed`. `prefix` namespaces
/// the CRs so the three backend tests can coexist on one reused cluster; the
/// restore reuses the shared `e2e-dst` PVC (tests run serially).
async fn run_backend_lifecycle(world: &World, prefix: &str, repository_json: serde_json::Value) {
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Backup> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = format!("{prefix}-repo");
    let cfg = format!("{prefix}-cfg");
    let backup = format!("{prefix}-backup");
    let restore = format!("{prefix}-restore");

    // 1. Bootstrap-create the repository against the real backend server.
    repos
        .create(&PostParams::default(), &cr(repository_json))
        .await
        .unwrap_or_else(|e| panic!("create {prefix} Repository: {e}"));
    wait_phase(&repos, &repo, "Ready")
        .await
        .unwrap_or_else(|e| panic!("{prefix} Repository should reach Ready: {e}"));
    let rstatus = status_json(&repos, &repo).await;
    assert!(
        rstatus
            .get("uniqueId")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty()),
        "{prefix} Repository must carry a real kopia uniqueId, got {rstatus}"
    );

    // 2. Back up the known source PVC → Succeeded with a real snapshot id.
    configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json(&cfg, &repo, consts::PVC_SRC)),
        )
        .await
        .unwrap_or_else(|e| panic!("create {prefix} BackupConfig: {e}"));
    backups
        .create(&PostParams::default(), &cr(backup_json(&backup, &cfg)))
        .await
        .unwrap_or_else(|e| panic!("create {prefix} Backup: {e}"));
    wait_phase(&backups, &backup, "Succeeded")
        .await
        .unwrap_or_else(|e| panic!("{prefix} Backup should reach Succeeded: {e}"));
    let bstatus = status_json(&backups, &backup).await;
    let snap_id = bstatus
        .get("snapshot")
        .and_then(|s| s.get("kopiaSnapshotID"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !snap_id.is_empty(),
        "{prefix} Backup must carry a real kopia snapshot id, got {bstatus}"
    );

    // 3. Restore it into the shared destination PVC → Completed.
    restores
        .create(
            &PostParams::default(),
            &cr(restore_json(&restore, &repo, &backup)),
        )
        .await
        .unwrap_or_else(|e| panic!("create {prefix} Restore: {e}"));
    wait_phase(&restores, &restore, "Completed")
        .await
        .unwrap_or_else(|e| panic!("{prefix} Restore should reach Completed: {e}"));
}

/// SFTP backend, end to end. The mover materializes the SSH private key +
/// known_hosts from the credentials Secret into files and passes
/// `--keyfile`/`--known-hosts` — without that plumbing this Repository never
/// reaches Ready (host-key verification / no key).
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + sftp server + built images + helm install"]
async fn sftp_bootstrap_backup_restore() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Sftp, Need::Filesystem])
        .await
        .expect("provision sftp server + source/dest PVCs");

    let repo_json = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": "e2e-sftp-repo", "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "sftp": {
                "host": consts::SFTP_HOST,
                "port": 22,
                "path": consts::SFTP_PATH,
                "username": consts::SFTP_USER,
                "auth": { "secretRef": { "name": consts::SECRET_SFTP_CREDS, "namespace": E2E_NAMESPACE } }
            }},
            "encryption": { "passwordSecretRef": { "name": consts::SECRET_SFTP_CREDS, "key": consts::KEY_KOPIA_PASSWORD } },
            "create": { "enabled": true }
        }
    });
    run_backend_lifecycle(&world, "e2e-sftp", repo_json).await;
}

/// WebDAV backend, end to end (basic auth via `KOPIA_WEBDAV_*` env).
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + webdav server + built images + helm install"]
async fn webdav_bootstrap_backup_restore() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::WebDav, Need::Filesystem])
        .await
        .expect("provision webdav server + source/dest PVCs");

    let repo_json = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": "e2e-webdav-repo", "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "webDav": {
                "url": consts::WEBDAV_URL,
                "auth": { "secretRef": { "name": consts::SECRET_WEBDAV_CREDS, "namespace": E2E_NAMESPACE } }
            }},
            "encryption": { "passwordSecretRef": { "name": consts::SECRET_WEBDAV_CREDS, "key": consts::KEY_KOPIA_PASSWORD } },
            "create": { "enabled": true }
        }
    });
    run_backend_lifecycle(&world, "e2e-webdav", repo_json).await;
}

/// rclone backend, end to end. kopia shells out to the `rclone` binary (shipped
/// in the mover image) using an `rclone.conf` the mover materializes from the
/// config Secret; the `miniors3` remote targets the in-cluster MinIO.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images (rclone) + helm install"]
async fn rclone_bootstrap_backup_restore() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Rclone, Need::Filesystem])
        .await
        .expect("provision rclone creds (MinIO) + source/dest PVCs");

    let repo_json = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": "e2e-rclone-repo", "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "rclone": {
                "remotePath": consts::RCLONE_REMOTE_PATH,
                "configSecretRef": { "name": consts::SECRET_RCLONE_CREDS, "namespace": E2E_NAMESPACE }
            }},
            "encryption": { "passwordSecretRef": { "name": consts::SECRET_RCLONE_CREDS, "key": consts::KEY_KOPIA_PASSWORD } },
            "create": { "enabled": true }
        }
    });
    run_backend_lifecycle(&world, "e2e-rclone", repo_json).await;
}
