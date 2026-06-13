//! End-to-end **workload identity** scenario: a Repository whose S3 backend
//! authenticates with `auth.workloadIdentity` — NO static keys anywhere — runs
//! the full bootstrap → backup → restore pipeline, with the mover Jobs running
//! as the user's ServiceAccount.
//!
//! In kind there is no cloud IAM, so the scenario rides the exact mechanism the
//! feature uses in a real cluster: the mover invokes kopia with explicitly-empty
//! `--access-key=` / `--secret-access-key=` flags, kopia's minio-go credential
//! chain finds no static/env/IAM credentials and resolves **anonymous**, and the
//! one e2e bucket with an anonymous read-write policy (`mc anonymous set
//! public`) accepts it. That proves the full wiring: the CRD field → webhook →
//! work-spec `ambientCredentials` → empty-flag kopia argv → a working
//! repository, plus the Job-shape contract (the user's `serviceAccountName`, no
//! backend-secret `envFrom`, the mover-role RoleBinding).
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`. Run:
//!
//! ```text
//! mise run //crates/e2e:test
//! ```

#![cfg(all(unix, feature = "e2e"))]

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{Secret, ServiceAccount};
use k8s_openapi::api::rbac::v1::RoleBinding;
use kube::Api;
use kube::api::{DeleteParams, PostParams};
use serde::de::DeserializeOwned;

use kopiur_api::{Repository, Restore, Snapshot, SnapshotPolicy};
use kopiur_e2e::consts::WI_BUCKET;
use kopiur_e2e::{E2E_NAMESPACE, Need, World, default_timeout, poll_interval, wait_until};

/// The user-supplied ServiceAccount the workload-identity movers run as. In a
/// real cluster it carries the cloud federation annotation; here it is bare —
/// the credential chain ends anonymous either way.
const WI_SA: &str = "wi-mover";
/// The password-only Secret (workload identity has no backend keys).
const WI_SECRET: &str = "kopia-wi-creds";

fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// Delete `name` (if present) and wait until it is fully gone (finalizers ran).
async fn delete_and_wait_gone<K>(api: &Api<K>, name: &str)
where
    K: kube::Resource + Clone + DeserializeOwned + serde::Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    let _ = api.delete(name, &DeleteParams::default()).await;
    wait_until(
        &format!("{name} deleted"),
        default_timeout(),
        poll_interval(),
        || async { Ok(api.get_opt(name).await?.is_none().then_some(())) },
    )
    .await
    .unwrap_or_else(|e| panic!("leftover {name} should delete: {e}"));
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

/// The headline workload-identity scenario:
/// 1. a bare ServiceAccount + a password-only Secret + a Repository whose S3
///    backend names `auth.workloadIdentity.serviceAccountName` (`create: true`)
///    → bootstrap Job → `Ready` — with zero static backend credentials;
/// 2. full backup → `Succeeded` → restore → `Completed`;
/// 3. the backup mover Job's shape proves the wiring: it runs as the USER'S
///    ServiceAccount, its `envFrom` carries only the password Secret, its
///    work-spec ConfigMap says `ambientCredentials`, and the controller bound
///    the mover role to the user's SA under the `kopiur-mover-wi-*` name;
/// 4. negative leg: with the SA deleted, a new Snapshot blocks on
///    `CredentialsAvailable=False` (reason `MissingServiceAccount`, message
///    naming the SA); re-creating the SA un-sticks it to `Succeeded`.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn workload_identity_s3_full_pipeline_without_static_keys() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem, Need::Minio])
        .await
        .expect("provision source PVCs + MinIO + buckets");
    let client = world.client().clone();
    let sas: Api<ServiceAccount> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let secrets: Api<Secret> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let bindings: Api<RoleBinding> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // Rerun hygiene: the scenario uses fixed names; a previous (killed/failed)
    // run on a reused cluster leaves them behind. Children before the
    // Repository so finalizers can run.
    delete_and_wait_gone(&restores, "e2e-wi-restore").await;
    delete_and_wait_gone(&backups, "e2e-wi-blocked").await;
    delete_and_wait_gone(&backups, "e2e-wi-backup").await;
    delete_and_wait_gone(&configs, "e2e-wi-cfg").await;
    delete_and_wait_gone(&repos, "e2e-wi").await;
    let _ = secrets.delete(WI_SECRET, &DeleteParams::default()).await;

    // 1. The user-side prerequisites: a bare SA and a password-only Secret.
    let _ = sas
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": { "name": WI_SA, "namespace": E2E_NAMESPACE },
            })),
        )
        .await;
    let _ = secrets
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": { "name": WI_SECRET, "namespace": E2E_NAMESPACE },
                "stringData": {
                    "KOPIA_PASSWORD": "wi-e2e-password",
                    // The IRSA-shaped env hints a real EKS identity webhook
                    // injects — pointing at a token that doesn't exist. This
                    // routes minio-go's credential chain down its REAL
                    // web-identity branch (which fails the file read instantly)
                    // and on to anonymous, instead of the IMDS fallback: in kind
                    // 169.254.169.254 black-holes, and the timeout-less dial
                    // would eat the whole bootstrap deadline. No AWS keys here —
                    // kopia still runs with empty --access-key= flags.
                    "AWS_WEB_IDENTITY_TOKEN_FILE": "/var/run/secrets/kopiur-e2e/no-such-token",
                    "AWS_ROLE_ARN": "arn:aws:iam::000000000000:role/kopiur-e2e-fast-fail",
                },
            })),
        )
        .await;

    repos
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Repository",
                "metadata": { "name": "e2e-wi", "namespace": E2E_NAMESPACE },
                "spec": {
                    "backend": { "s3": {
                        "bucket": WI_BUCKET,
                        "endpoint": "minio.kopiur-e2e.svc.cluster.local:9000",
                        "region": "us-east-1",
                        "tls": { "disableTls": true },
                        // The whole point: NO secretRef — the mover runs as the
                        // named SA and kopia authenticates via the ambient chain.
                        "auth": { "workloadIdentity": { "serviceAccountName": WI_SA } }
                    }},
                    "encryption": {
                        "passwordSecretRef": { "name": WI_SECRET, "key": "KOPIA_PASSWORD" }
                    },
                    "create": { "enabled": true }
                }
            })),
        )
        .await
        .expect("create workload-identity S3 Repository");
    wait_phase(&repos, "e2e-wi", "Ready")
        .await
        .expect("workload-identity Repository should bootstrap to Ready with no static keys");

    // 2. Full backup + restore.
    configs
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "SnapshotPolicy",
                "metadata": { "name": "e2e-wi-cfg", "namespace": E2E_NAMESPACE },
                "spec": {
                    "repository": { "kind": "Repository", "name": "e2e-wi" },
                    "sources": [ { "pvc": { "name": "e2e-src" } } ],
                    "retention": { "keepLatest": 5 }
                }
            })),
        )
        .await
        .expect("create SnapshotPolicy");
    backups
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Snapshot",
                "metadata": { "name": "e2e-wi-backup", "namespace": E2E_NAMESPACE },
                "spec": { "policyRef": { "name": "e2e-wi-cfg" }, "deletionPolicy": "Retain" }
            })),
        )
        .await
        .expect("create Snapshot");
    wait_phase(&backups, "e2e-wi-backup", "Succeeded")
        .await
        .expect("workload-identity Snapshot should reach Succeeded");

    // 3. Job-shape contract on the backup mover Job (named after the Snapshot).
    let mover_job = jobs
        .get("e2e-wi-backup")
        .await
        .expect("backup mover Job exists");
    let pod_spec = mover_job
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .expect("mover Job has a pod spec");
    assert_eq!(
        pod_spec.service_account_name.as_deref(),
        Some(WI_SA),
        "the mover must run as the USER'S workload-identity ServiceAccount"
    );
    let env_from_secrets: Vec<String> = pod_spec.containers[0]
        .env_from
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|e| e.secret_ref.as_ref().map(|s| s.name.clone()))
        .collect();
    assert_eq!(
        env_from_secrets,
        vec![WI_SECRET.to_string()],
        "envFrom must carry ONLY the password Secret — no backend keys"
    );
    let cms: Api<k8s_openapi::api::core::v1::ConfigMap> =
        Api::namespaced(client.clone(), E2E_NAMESPACE);
    let work_spec_cm = cms
        .get("e2e-wi-backup")
        .await
        .expect("work-spec ConfigMap exists");
    let work_spec = work_spec_cm
        .data
        .as_ref()
        .and_then(|d| d.values().next().cloned())
        .unwrap_or_default();
    assert!(
        work_spec.contains("ambientCredentials"),
        "the work spec must flag the ambient credential chain, got: {work_spec}"
    );
    bindings
        .get(&format!("kopiur-mover-wi-{WI_SA}"))
        .await
        .expect("the controller binds the mover role to the user's SA");

    restores
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Restore",
                "metadata": { "name": "e2e-wi-restore", "namespace": E2E_NAMESPACE },
                "spec": {
                    "repository": { "kind": "Repository", "name": "e2e-wi" },
                    "source": { "snapshotRef": { "name": "e2e-wi-backup" } },
                    "target": { "pvcRef": { "name": "e2e-dst" } }
                }
            })),
        )
        .await
        .expect("create Restore");
    wait_phase(&restores, "e2e-wi-restore", "Completed")
        .await
        .expect("workload-identity Restore should reach Completed");

    // 4. Negative leg (regression guard): delete the SA, create another
    //    Snapshot — it must surface `CredentialsAvailable=False` with the SA in
    //    the message, then recover once the SA is recreated.
    sas.delete(WI_SA, &DeleteParams::default())
        .await
        .expect("delete the workload-identity SA");
    backups
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Snapshot",
                "metadata": { "name": "e2e-wi-blocked", "namespace": E2E_NAMESPACE },
                "spec": { "policyRef": { "name": "e2e-wi-cfg" }, "deletionPolicy": "Retain" }
            })),
        )
        .await
        .expect("create blocked Snapshot");
    wait_until(
        "e2e-wi-blocked surfaces CredentialsAvailable=False naming the SA",
        default_timeout(),
        poll_interval(),
        || async {
            let Some(obj) = backups.get_opt("e2e-wi-blocked").await? else {
                return Ok(None);
            };
            let v = serde_json::to_value(&obj).unwrap_or_default();
            let cond = v
                .pointer("/status/conditions")
                .and_then(|c| c.as_array())
                .and_then(|conds| {
                    conds.iter().find(|c| {
                        c.get("type").and_then(|t| t.as_str()) == Some("CredentialsAvailable")
                    })
                })
                .cloned();
            let blocked = cond.as_ref().is_some_and(|c| {
                c.get("status").and_then(|s| s.as_str()) == Some("False")
                    && c.get("reason").and_then(|r| r.as_str()) == Some("MissingServiceAccount")
                    && c.get("message")
                        .and_then(|m| m.as_str())
                        .is_some_and(|m| m.contains(WI_SA))
            });
            Ok(blocked.then_some(()))
        },
    )
    .await
    .expect("a missing workload-identity SA must surface an actionable condition");

    // Recreate the SA: the blocked Snapshot must recover to Succeeded.
    sas.create(
        &PostParams::default(),
        &cr(serde_json::json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": { "name": WI_SA, "namespace": E2E_NAMESPACE },
        })),
    )
    .await
    .expect("recreate the workload-identity SA");
    wait_phase(&backups, "e2e-wi-blocked", "Succeeded")
        .await
        .expect("the blocked Snapshot should recover once the SA exists");
}
