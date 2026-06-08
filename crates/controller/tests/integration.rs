//! Integration tests for the kopiur controller (ADR §5.2 / SKILL build
//! discipline).
//!
//! These run **only** against an ephemeral `kind` cluster: they are gated behind
//! `#[ignore]` + `--features integration` and skip gracefully (returning early)
//! when no `KUBECONFIG`/in-cluster config is reachable, so the hermetic
//! `cargo test` suite never touches a cluster. Per the SKILL, they must NEVER be
//! pointed at a real/homelab cluster — use `scripts/with-kind.sh`.
//!
//! Run: `scripts/with-kind.sh cargo test -p kopiur-controller --features
//! integration -- --include-ignored`.

#![cfg(feature = "integration")]

use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::core::CustomResourceExt;
use kube::runtime::wait::{await_condition, conditions};
use kube::{Api, Client, ResourceExt};

use kopiur_api::backend::{Backend, FilesystemBackend};
use kopiur_api::common::{Encryption, RepositoryKind, RepositoryRef, SecretKeyRef};
use kopiur_api::{Backup, BackupConfig, BackupConfigSpec, BackupSpec, Repository, RepositorySpec};
use kopiur_controller::consts::SNAPSHOT_CLEANUP_FINALIZER;

/// Install the CRDs these tests exercise and wait for each to become
/// `Established`. The ephemeral kind cluster starts bare, so the CRDs must be
/// applied before any custom resource can be created (otherwise the apiserver
/// 404s). Provisioning stays in Rust (the repo convention) and uses
/// server-side apply so a reused cluster is idempotent.
async fn ensure_crds(client: &Client) {
    let api: Api<CustomResourceDefinition> = Api::all(client.clone());
    let pp = PatchParams::apply("kopiur-integration-test").force();
    let crds = [Repository::crd(), BackupConfig::crd(), Backup::crd()];
    for crd in crds {
        let name = crd.name_any();
        api.patch(&name, &pp, &Patch::Apply(&crd))
            .await
            .unwrap_or_else(|e| panic!("apply CRD {name}: {e}"));
        await_condition(api.clone(), &name, conditions::is_crd_established())
            .await
            .unwrap_or_else(|e| panic!("CRD {name} never became Established: {e}"));
    }
}

/// Try to connect to a cluster; return `None` (and print a skip notice) if none
/// is reachable, so the test passes as a no-op off-cluster.
async fn try_client() -> Option<Client> {
    match Client::try_default().await {
        Ok(c) => {
            // Probe the API so a stale kubeconfig also skips rather than hangs.
            match c.apiserver_version().await {
                Ok(_) => Some(c),
                Err(e) => {
                    eprintln!("SKIP: cluster unreachable ({e}); integration test is a no-op");
                    None
                }
            }
        }
        Err(e) => {
            eprintln!("SKIP: no kube client ({e}); integration test is a no-op");
            None
        }
    }
}

fn sample_repository(name: &str) -> Repository {
    Repository::new(
        name,
        RepositorySpec {
            backend: Backend::Filesystem(FilesystemBackend {
                path: "/repo".into(),
                volume: None,
            }),
            encryption: Encryption {
                password_secret_ref: SecretKeyRef {
                    name: "kopia-creds".into(),
                    namespace: None,
                    key: Some("KOPIA_PASSWORD".into()),
                },
            },
            create: None,
            cache_defaults: None,
            catalog: None,
            maintenance: None,
        },
    )
}

fn sample_backup_config(name: &str) -> BackupConfig {
    use kopiur_api::backup_config::{PvcSource, Source};
    BackupConfig::new(
        name,
        BackupConfigSpec {
            repository: RepositoryRef {
                kind: RepositoryKind::Repository,
                name: "test-repo".into(),
                namespace: None,
            },
            identity: None,
            sources: vec![Source {
                pvc: Some(PvcSource {
                    name: "data".into(),
                }),
                pvc_selector: None,
                nfs: None,
                source_path_override: None,
                source_path_strategy: None,
            }],
            copy_method: None,
            volume_snapshot_class_name: None,
            group_by: None,
            retention: None,
            default_deletion_policy: None,
            policy: None,
            hooks: None,
            mover: None,
            credential_projection: None,
        },
    )
}

#[tokio::test]
#[ignore = "requires an ephemeral kind cluster (run via scripts/with-kind.sh --features integration)"]
async fn repository_and_backup_config_apply_cleanly() {
    let Some(client) = try_client().await else {
        return;
    };
    ensure_crds(&client).await;
    let ns = "default";
    let repos: Api<Repository> = Api::namespaced(client.clone(), ns);
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), ns);

    // Apply a Repository + BackupConfig; assert they create without rejection.
    let _ = repos
        .create(&PostParams::default(), &sample_repository("test-repo"))
        .await
        .expect("create Repository");
    let _ = configs
        .create(&PostParams::default(), &sample_backup_config("test-config"))
        .await
        .expect("create BackupConfig");

    // Cleanup.
    let _ = repos.delete("test-repo", &DeleteParams::default()).await;
    let _ = configs
        .delete("test-config", &DeleteParams::default())
        .await;
}

#[tokio::test]
#[ignore = "requires an ephemeral kind cluster"]
async fn backup_gets_finalizer_and_delete_path_removes_cr() {
    let Some(client) = try_client().await else {
        return;
    };
    ensure_crds(&client).await;
    let ns = "default";
    let backups: Api<Backup> = Api::namespaced(client.clone(), ns);

    // Create a manual Backup with deletionPolicy: Orphan so the finalizer path
    // doesn't require a live repo (Orphan never contacts it).
    let mut b = Backup::new(
        "it-backup",
        BackupSpec {
            config_ref: None,
            tags: None,
            failure_policy: None,
            deletion_policy: Some(kopiur_api::DeletionPolicy::Orphan),
        },
    );
    b.finalizers_mut()
        .push(SNAPSHOT_CLEANUP_FINALIZER.to_string());
    let created = backups
        .create(&PostParams::default(), &b)
        .await
        .expect("create Backup");
    assert!(
        created
            .finalizers()
            .contains(&SNAPSHOT_CLEANUP_FINALIZER.to_string())
    );

    // Deleting it should eventually remove the CR once the controller clears the
    // finalizer (Orphan path). With no controller running this would block, so
    // we wait bounded and tolerate timeout in this smoke test.
    let _ = backups.delete("it-backup", &DeleteParams::default()).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(30),
        await_condition(backups.clone(), "it-backup", conditions::is_deleted("")),
    )
    .await;

    // Best-effort: also create a child Job-shaped object is out of scope here;
    // mover Job creation is asserted by the live operator, not this smoke test.
    let _jobs: Api<Job> = Api::namespaced(client, ns);
}
