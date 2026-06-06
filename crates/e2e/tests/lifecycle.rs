//! End-to-end lifecycle scenarios against a Helm-deployed operator in kind.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`, skipping gracefully without
//! a cluster. Driven by `scripts/with-e2e.sh`, which builds + loads the images,
//! installs the chart (webhook disabled — its admission logic is covered by the
//! unit/integration tiers), provisions a hostPath-backed repo PVC visible to both
//! the controller and the mover Jobs, and a source PVC pre-populated with known
//! data. Run:
//!
//! ```text
//! just test-e2e         # or: scripts/with-e2e.sh
//! ```
//!
//! These tests assert on real operator output: a Repository reaching Ready, a
//! Backup reaching Succeeded with a real kopia snapshot id, a Restore Completed,
//! schedule-driven Backup creation, finalizer-driven snapshot deletion, and a
//! Maintenance lease claim — across six of the seven CRDs.

#![cfg(all(unix, feature = "e2e"))]

use std::time::Duration;

use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::{Api, Client, ResourceExt};
use serde::de::DeserializeOwned;

use k8s_openapi::api::core::v1::ServiceAccount;
use k8s_openapi::api::rbac::v1::RoleBinding;

use kopiur_api::{
    Backup, BackupConfig, BackupSchedule, ClusterRepository, Maintenance, Repository, Restore,
};
use kopiur_e2e::{
    E2E_NAMESPACE, apply_secret, default_timeout, ensure_namespace, poll_interval, try_client,
    wait_until,
};

/// Deserialize a CR from a JSON literal into its typed kube object.
fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// The repository password Secret the chart-installed operator reads.
const CREDS_SECRET: &str = "kopia-creds";

fn repository_json(name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "filesystem": { "path": "/repo", "pvcName": "kopiur-e2e-repo" } },
            "encryption": {
                "passwordSecretRef": { "name": CREDS_SECRET, "key": "KOPIA_PASSWORD" }
            },
            "create": { "enabled": true }
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

/// A cluster-scoped `ClusterRepository` pointing at the same hostPath repo the
/// namespaced `Repository` uses. Secret refs MUST carry an explicit namespace
/// (cluster-scoped resources can't infer one), and `allowedNamespaces` opens it
/// to the e2e namespace.
fn cluster_repository_json(name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "ClusterRepository",
        "metadata": { "name": name },
        "spec": {
            "backend": { "filesystem": { "path": "/repo", "pvcName": "kopiur-e2e-repo" } },
            "encryption": {
                "passwordSecretRef": {
                    "name": CREDS_SECRET, "namespace": E2E_NAMESPACE, "key": "KOPIA_PASSWORD"
                }
            },
            "create": { "enabled": true },
            "allowedNamespaces": { "all": true }
        }
    })
}

/// A `BackupConfig` whose repository ref is a `ClusterRepository` (cluster-scoped:
/// note no `namespace` on the ref — that is the whole point of this scenario).
fn cluster_backup_config_json(name: &str, crepo: &str, src_pvc: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "BackupConfig",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "ClusterRepository", "name": crepo },
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
        "spec": {
            "configRef": { "name": config },
            "deletionPolicy": deletion_policy
        }
    })
}

/// A `Maintenance` (always namespaced) referencing a repository of the given
/// `repo_kind` (`Repository`/`ClusterRepository`) and `repo_name`. `Force`
/// takeover so it claims the lease without waiting on another holder.
fn maintenance_json(name: &str, repo_kind: &str, repo_name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Maintenance",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": repo_kind, "name": repo_name },
            "schedule": {
                "quick": { "cron": "*/5 * * * *" },
                "full": { "cron": "0 3 * * 0" }
            },
            "ownership": { "owner": "kopiur-e2e", "takeoverPolicy": "Force" }
        }
    })
}

/// Poll a namespaced CR until `pred(status_json)` is true, returning the object.
async fn wait_phase<K>(api: &Api<K>, name: &str, want_phase: &str) -> anyhow::Result<()>
where
    K: kube::Resource + Clone + DeserializeOwned + std::fmt::Debug,
    K: serde::Serialize,
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
                    if phase == want_phase {
                        Ok(Some(()))
                    } else {
                        Ok(None)
                    }
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

/// Extract a status condition's `status` ("True"/"False"/...) by `type`, or
/// `None` if the condition is absent.
fn condition_status(status: &serde_json::Value, type_: &str) -> Option<String> {
    status
        .get("conditions")
        .and_then(|c| c.as_array())?
        .iter()
        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some(type_))
        .and_then(|c| c.get("status").and_then(|s| s.as_str()))
        .map(str::to_string)
}

/// Poll a CR (namespaced or cluster-scoped via the passed `Api`) until its
/// `status.conditions[type=type_].status` equals `want`.
async fn wait_condition<K>(api: &Api<K>, name: &str, type_: &str, want: &str) -> anyhow::Result<()>
where
    K: kube::Resource + Clone + DeserializeOwned + serde::Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    wait_until(
        &format!("{name} {type_}={want}"),
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(api, name).await;
            Ok((condition_status(&s, type_).as_deref() == Some(want)).then_some(()))
        },
    )
    .await
}

/// The headline scenario: Repository → Backup (real kopia snapshot) → Restore →
/// finalizer-driven Delete. Proves the entire data path end-to-end.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh): kind + built images + helm install"]
async fn backup_restore_delete_lifecycle() {
    let Some(client) = try_client().await else {
        return;
    };
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Backup> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // 1. Repository becomes Ready (controller connects/creates the kopia repo).
    repos
        .create(&PostParams::default(), &cr(repository_json("e2e-repo")))
        .await
        .expect("create Repository");
    wait_phase(&repos, "e2e-repo", "Ready")
        .await
        .expect("Repository should reach Ready");

    // 2. BackupConfig + Backup → mover Job → Succeeded with a real snapshot id.
    configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json("e2e-cfg", "e2e-repo", "e2e-src")),
        )
        .await
        .expect("create BackupConfig");
    backups
        .create(
            &PostParams::default(),
            &cr(backup_json("e2e-backup", "e2e-cfg", "Retain")),
        )
        .await
        .expect("create Backup");
    wait_phase(&backups, "e2e-backup", "Succeeded")
        .await
        .expect("Backup should reach Succeeded");
    let bstatus = status_json(&backups, "e2e-backup").await;
    let snap_id = bstatus
        .get("snapshot")
        .and_then(|s| s.get("kopiaSnapshotID"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !snap_id.is_empty(),
        "Backup status must carry a real kopia snapshot id, got {bstatus}"
    );

    // 3. Restore that Backup into a target PVC → Completed.
    let restore = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Restore",
        "metadata": { "name": "e2e-restore", "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": "e2e-repo" },
            "source": { "backupRef": { "name": "e2e-backup" } },
            "target": { "pvc": { "name": "e2e-dst" } }
        }
    });
    restores
        .create(&PostParams::default(), &cr(restore))
        .await
        .expect("create Restore");
    wait_phase(&restores, "e2e-restore", "Completed")
        .await
        .expect("Restore should reach Completed");

    // 4. Delete a Backup whose deletionPolicy is Delete → finalizer runs a delete
    //    Job and the CR is removed. (Use a second Backup so step 2's Retain one
    //    survives for inspection.)
    backups
        .create(
            &PostParams::default(),
            &cr(backup_json("e2e-backup-del", "e2e-cfg", "Delete")),
        )
        .await
        .expect("create deletable Backup");
    wait_phase(&backups, "e2e-backup-del", "Succeeded")
        .await
        .expect("deletable Backup should reach Succeeded");
    backups
        .delete("e2e-backup-del", &DeleteParams::default())
        .await
        .expect("delete Backup");
    wait_until(
        "e2e-backup-del removed after finalizer",
        Duration::from_secs(120),
        poll_interval(),
        || async {
            match backups.get_opt("e2e-backup-del").await? {
                Some(_) => Ok(None),
                None => Ok(Some(())),
            }
        },
    )
    .await
    .expect("Backup CR should be removed once the snapshot-cleanup finalizer runs");
}

/// Regression guard (the "ClusterRepository references are ignored" bug): a
/// `BackupConfig` that references a cluster-scoped `ClusterRepository` must drive
/// a Backup all the way to `Succeeded`. Before the fix, the controller resolved
/// every repository ref as a namespaced `Repository` regardless of `kind`, so a
/// `kind: ClusterRepository` config failed with
/// `missing dependency: Repository <ns>/<name>` and never produced a snapshot —
/// this test would time out at `wait_phase(... "Succeeded")`.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh): kind + built images + helm install"]
async fn cluster_repository_backup_lifecycle() {
    let Some(client) = try_client().await else {
        return;
    };
    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Backup> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // 1. ClusterRepository becomes Ready (cluster-scoped; controller watches it
    //    cluster-wide under the e2e ClusterRole).
    crepos
        .create(
            &PostParams::default(),
            &cr(cluster_repository_json("e2e-crepo")),
        )
        .await
        .expect("create ClusterRepository");
    wait_phase(&crepos, "e2e-crepo", "Ready")
        .await
        .expect("ClusterRepository should reach Ready");

    // 2. A BackupConfig referencing it by `kind: ClusterRepository` (no namespace
    //    on the ref) + a Backup → mover Job → Succeeded with a real snapshot id.
    configs
        .create(
            &PostParams::default(),
            &cr(cluster_backup_config_json(
                "e2e-cfg-crepo",
                "e2e-crepo",
                "e2e-src",
            )),
        )
        .await
        .expect("create BackupConfig (ClusterRepository-backed)");
    backups
        .create(
            &PostParams::default(),
            &cr(backup_json("e2e-backup-crepo", "e2e-cfg-crepo", "Retain")),
        )
        .await
        .expect("create Backup");
    wait_phase(&backups, "e2e-backup-crepo", "Succeeded")
        .await
        .expect("ClusterRepository-backed Backup should reach Succeeded");
    let bstatus = status_json(&backups, "e2e-backup-crepo").await;
    let snap_id = bstatus
        .get("snapshot")
        .and_then(|s| s.get("kopiaSnapshotID"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        !snap_id.is_empty(),
        "ClusterRepository-backed Backup must carry a real kopia snapshot id, got {bstatus}"
    );

    // Cleanup the cluster-scoped resource (the namespaced ones GC with the ns).
    let _ = crepos.delete("e2e-crepo", &DeleteParams::default()).await;
}

/// Cross-namespace mover RBAC + credentials (ADR §4.12). A Backup in a workload
/// namespace SEPARATE from the operator's must (a) get a least-privilege
/// `kopiur-mover` ServiceAccount + RoleBinding MINTED in that namespace by the
/// controller, and (b) when its credentials Secret is absent there, surface a clear
/// `CredentialsAvailable=False` / `MissingCredentialsSecret` condition rather than
/// launching a Job that hangs. Adding the Secret clears the condition.
///
/// Before the fix the mover Job referenced an SA (and `envFrom` Secret) that only
/// existed in the operator namespace, so a cross-namespace Backup wedged in
/// `Running` with the Job stuck in `FailedCreate: serviceaccount ... not found` —
/// this test would time out waiting for the SA to appear.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh): kind + built images + helm install"]
async fn cross_namespace_backup_mints_mover_rbac_and_surfaces_missing_creds() {
    let Some(client) = try_client().await else {
        return;
    };
    // A workload namespace distinct from the operator's (E2E_NAMESPACE).
    const APP_NS: &str = "kopiur-e2e-app";
    // The chart-minted mover identity (release name `kopiur` → `kopiur-mover`).
    const MOVER_NAME: &str = "kopiur-mover";

    ensure_namespace(&client, APP_NS)
        .await
        .expect("create workload namespace");

    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), APP_NS);
    let backups: Api<Backup> = Api::namespaced(client.clone(), APP_NS);
    let sas: Api<ServiceAccount> = Api::namespaced(client.clone(), APP_NS);
    let rbs: Api<RoleBinding> = Api::namespaced(client.clone(), APP_NS);

    // 1. A ClusterRepository (its Secret lives in E2E_NAMESPACE) reaches Ready.
    crepos
        .create(
            &PostParams::default(),
            &cr(cluster_repository_json("e2e-xns-crepo")),
        )
        .await
        .expect("create ClusterRepository");
    wait_phase(&crepos, "e2e-xns-crepo", "Ready")
        .await
        .expect("ClusterRepository should reach Ready");

    // 2. BackupConfig + Backup in the WORKLOAD namespace. No creds Secret there yet.
    let cfg = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "BackupConfig",
        "metadata": { "name": "e2e-xns-cfg", "namespace": APP_NS },
        "spec": {
            "repository": { "kind": "ClusterRepository", "name": "e2e-xns-crepo" },
            "sources": [ { "pvc": { "name": "e2e-src" } } ],
            "retention": { "keepLatest": 5 }
        }
    });
    configs
        .create(&PostParams::default(), &cr(cfg))
        .await
        .expect("create BackupConfig in workload ns");
    backups
        .create(
            &PostParams::default(),
            &cr(backup_json_ns("e2e-xns-backup", "e2e-xns-cfg", APP_NS)),
        )
        .await
        .expect("create Backup in workload ns");

    // 3. The controller mints the mover SA + RoleBinding in the workload namespace
    //    (this is the core of the fix — it happens before the creds check).
    wait_until(
        &format!("ServiceAccount {APP_NS}/{MOVER_NAME} minted"),
        default_timeout(),
        poll_interval(),
        || async { sas.get_opt(MOVER_NAME).await.map(|o| o.map(|_| ())) },
    )
    .await
    .expect("mover ServiceAccount minted in workload namespace");
    let rb = wait_until(
        &format!("RoleBinding {APP_NS}/{MOVER_NAME} minted"),
        default_timeout(),
        poll_interval(),
        || async { rbs.get_opt(MOVER_NAME).await },
    )
    .await
    .expect("mover RoleBinding minted in workload namespace");
    assert_eq!(
        rb.role_ref.name, MOVER_NAME,
        "RoleBinding must target the mover role"
    );

    // 4. Missing creds → clear, actionable CredentialsAvailable=False condition.
    wait_condition(&backups, "e2e-xns-backup", "CredentialsAvailable", "False")
        .await
        .expect("Backup should report CredentialsAvailable=False when the Secret is absent");
    let s = status_json(&backups, "e2e-xns-backup").await;
    let cond = s
        .get("conditions")
        .and_then(|c| c.as_array())
        .and_then(|a| {
            a.iter()
                .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("CredentialsAvailable"))
        })
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    assert_eq!(
        cond.get("reason").and_then(|r| r.as_str()),
        Some("MissingCredentialsSecret")
    );
    let msg = cond.get("message").and_then(|m| m.as_str()).unwrap_or("");
    assert!(
        msg.contains(CREDS_SECRET) && msg.contains(APP_NS) && msg.contains("envFrom"),
        "condition message must name the Secret, the namespace, and explain envFrom; got: {msg}"
    );

    // 5. Place the Secret in the workload namespace → the condition clears to True.
    apply_secret(
        &client,
        APP_NS,
        CREDS_SECRET,
        &[("KOPIA_PASSWORD", "e2e-test-password-123")],
    )
    .await
    .expect("apply creds Secret into workload namespace");
    wait_condition(&backups, "e2e-xns-backup", "CredentialsAvailable", "True")
        .await
        .expect("Backup CredentialsAvailable should clear once the Secret is present");

    // Cleanup: the cluster-scoped repo + the workload namespace (GCs its CRs).
    let _ = crepos
        .delete("e2e-xns-crepo", &DeleteParams::default())
        .await;
    let nss: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(client.clone());
    let _ = nss.delete(APP_NS, &DeleteParams::default()).await;
}

/// A `Backup` JSON in an explicit namespace (cross-namespace scenarios).
fn backup_json_ns(name: &str, config: &str, ns: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Backup",
        "metadata": { "name": name, "namespace": ns },
        "spec": { "configRef": { "name": config }, "deletionPolicy": "Retain" }
    })
}

/// A BackupSchedule with an every-minute cron creates a scheduled Backup CR.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh)"]
async fn schedule_creates_backup() {
    let Some(client) = try_client().await else {
        return;
    };
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let schedules: Api<BackupSchedule> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Backup> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // Reuse / ensure a repo + config exist.
    let _ = repos
        .create(&PostParams::default(), &cr(repository_json("e2e-repo")))
        .await;
    let _ = configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json("e2e-cfg-sched", "e2e-repo", "e2e-src")),
        )
        .await;

    let sched = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "BackupSchedule",
        "metadata": { "name": "e2e-sched", "namespace": E2E_NAMESPACE },
        "spec": {
            "configRef": { "name": "e2e-cfg-sched" },
            "schedule": { "cron": "* * * * *", "runOnCreate": true }
        }
    });
    schedules
        .create(&PostParams::default(), &cr::<BackupSchedule>(sched))
        .await
        .expect("create BackupSchedule");

    // Within ~2 minutes a scheduled Backup (origin=scheduled) should appear.
    wait_until(
        "a scheduled Backup is created",
        Duration::from_secs(150),
        poll_interval(),
        || async {
            let list = backups.list(&Default::default()).await?;
            let found = list.items.iter().any(|b| {
                b.labels()
                    .get("kopiur.home-operations.com/origin")
                    .map(String::as_str)
                    == Some("scheduled")
            });
            Ok(found.then_some(()))
        },
    )
    .await
    .expect("schedule should create a Backup CR");
}

/// A Maintenance claims the repository lease.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh)"]
async fn maintenance_claims_lease() {
    let Some(client) = try_client().await else {
        return;
    };
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let maints: Api<Maintenance> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = repos
        .create(&PostParams::default(), &cr(repository_json("e2e-repo")))
        .await;

    let maint = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Maintenance",
        "metadata": { "name": "e2e-maint", "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": "e2e-repo" },
            "schedule": {
                "quick": { "cron": "*/5 * * * *" },
                "full": { "cron": "0 3 * * 0" }
            },
            "ownership": { "owner": "kopiur-e2e", "takeoverPolicy": "Force" }
        }
    });
    maints
        .create(&PostParams::default(), &cr::<Maintenance>(maint))
        .await
        .expect("create Maintenance");

    wait_until(
        "Maintenance claims the lease",
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(&maints, "e2e-maint").await;
            let claimed = s
                .get("ownership")
                .and_then(|o| o.get("owner"))
                .and_then(|v| v.as_str())
                .map(|o| !o.is_empty())
                .unwrap_or(false);
            Ok(claimed.then_some(()))
        },
    )
    .await
    .expect("Maintenance should claim the lease");
}

/// Scrape the controller's `/metrics` through the API server's Service-proxy
/// subresource (no port-forward / `ws` feature needed). The chart names the
/// controller metrics Service `kopiur-controller-metrics` on port 8080.
async fn scrape_controller_metrics(client: &Client) -> anyhow::Result<String> {
    let path = format!(
        "/api/v1/namespaces/{E2E_NAMESPACE}/services/kopiur-controller-metrics:8080/proxy/metrics"
    );
    let req = http::Request::get(path).body(Vec::new())?;
    Ok(client.request_text(req).await?)
}

/// Drive a Backup to Succeeded, then assert the controller exposes the expected
/// metric families with sane values — and that the exposition is valid
/// Prometheus text (a regression guard for the OTel→Prometheus name rewrite).
/// The webhook is disabled in the e2e harness, so webhook metrics are covered by
/// the unit tier, not here.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh): kind + built images + helm install"]
async fn metrics_reflect_backup_lifecycle() {
    let Some(client) = try_client().await else {
        return;
    };
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let configs: Api<BackupConfig> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Backup> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // A successful backup so phase/size/duration gauges have real values.
    let _ = repos
        .create(&PostParams::default(), &cr(repository_json("e2e-mx-repo")))
        .await;
    wait_phase(&repos, "e2e-mx-repo", "Ready")
        .await
        .expect("Repository should reach Ready");
    configs
        .create(
            &PostParams::default(),
            &cr(backup_config_json("e2e-mx-cfg", "e2e-mx-repo", "e2e-src")),
        )
        .await
        .expect("create BackupConfig");
    backups
        .create(
            &PostParams::default(),
            &cr(backup_json("e2e-mx-backup", "e2e-mx-cfg", "Retain")),
        )
        .await
        .expect("create Backup");
    wait_phase(&backups, "e2e-mx-backup", "Succeeded")
        .await
        .expect("Backup should reach Succeeded");

    // The Prometheus exporter publishes a family only after first observation and
    // the controller's own self-reconcile must record the Succeeded phase, so
    // poll until the key families are present.
    let text = wait_until(
        "controller /metrics exposes kopiur families",
        default_timeout(),
        poll_interval(),
        || {
            let client = client.clone();
            async move {
                match scrape_controller_metrics(&client).await {
                    Ok(t)
                        if t.contains("kopiur_controller_reconciliations_total")
                            && t.contains("kopiur_resource_phase")
                            && t.contains("kopiur_backup_size_bytes") =>
                    {
                        Ok(Some(t))
                    }
                    // Not ready yet, or the proxy isn't up — keep polling.
                    _ => Ok(None),
                }
            }
        },
    )
    .await
    .expect("controller should expose the kopiur metric families");

    // Reconcile loop metrics, per kind.
    assert!(
        text.contains("kopiur_controller_reconciliations_total{")
            && text.contains("kind=\"Backup\""),
        "missing per-kind reconciliations counter:\n{text}"
    );
    // Histogram buckets present (validates the OTel histogram → _bucket rewrite).
    assert!(
        text.contains("kopiur_controller_reconcile_duration_seconds_bucket"),
        "missing reconcile duration histogram buckets"
    );
    // Our backup's phase gauge: Succeeded == 1.
    let succeeded_series = text.lines().any(|l| {
        l.starts_with("kopiur_resource_phase{")
            && l.contains("kind=\"Backup\"")
            && l.contains("name=\"e2e-mx-backup\"")
            && l.contains("phase=\"Succeeded\"")
            && l.trim_end().ends_with(" 1")
    });
    assert!(
        succeeded_series,
        "expected kopiur_resource_phase ...Backup...Succeeded == 1:\n{text}"
    );
    // Backup stats gauges populated with a positive size.
    let positive_size = text.lines().any(|l| {
        l.starts_with("kopiur_backup_size_bytes{")
            && l.contains("name=\"e2e-mx-backup\"")
            && l.rsplit(' ')
                .next()
                .and_then(|v| v.parse::<f64>().ok())
                .is_some_and(|v| v > 0.0)
    });
    assert!(
        positive_size,
        "expected positive kopiur_backup_size_bytes:\n{text}"
    );
    // Valid Prometheus exposition: HELP/TYPE metadata present.
    assert!(
        text.contains("# TYPE kopiur_controller_reconciliations_total counter"),
        "exposition should carry # TYPE metadata"
    );
}

/// Default-managed maintenance (ADR §3.7): a `Repository` with no
/// `spec.maintenance` (default-on) gets an operator-*owned* `Maintenance` created
/// for it — same name as the repo, an `ownerReference` back to it, and the default
/// schedule — and the repo reports `MaintenanceConfigured=True`. Replaces the old
/// "warn when no Maintenance references the repo" behavior.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh)"]
async fn repository_default_creates_managed_maintenance() {
    let Some(client) = try_client().await else {
        return;
    };
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let maints: Api<Maintenance> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = "e2e-defmaint-repo";
    let _ = repos
        .create(&PostParams::default(), &cr(repository_json(repo)))
        .await;
    wait_phase(&repos, repo, "Ready")
        .await
        .expect("Repository should reach Ready");

    // The operator creates a Maintenance named after the repo, owned by it.
    let m = wait_managed_maintenance(&maints, repo, "Repository")
        .await
        .expect("operator should create an owned Maintenance for the repo");
    let mv = serde_json::to_value(&m).unwrap_or_default();
    assert_eq!(
        mv.pointer("/spec/schedule/quick/cron")
            .and_then(|v| v.as_str()),
        Some("0 */6 * * *"),
        "managed Maintenance should carry the default quick schedule: {mv}"
    );
    assert_eq!(
        mv.pointer("/spec/repository/name").and_then(|v| v.as_str()),
        Some(repo)
    );
    wait_condition(&repos, repo, "MaintenanceConfigured", "True")
        .await
        .expect("repo with default-managed maintenance should be MaintenanceConfigured=True");
}

/// Opting out: patching `spec.maintenance.enabled: false` removes the
/// operator-managed `Maintenance` and flips the condition to `False` (reason
/// `MaintenanceDisabled`, a deliberate opt-out — no Warning event).
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh)"]
async fn disabling_maintenance_removes_managed() {
    let Some(client) = try_client().await else {
        return;
    };
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let maints: Api<Maintenance> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = "e2e-optout-repo";
    let _ = repos
        .create(&PostParams::default(), &cr(repository_json(repo)))
        .await;
    wait_phase(&repos, repo, "Ready")
        .await
        .expect("Repository should reach Ready");
    wait_managed_maintenance(&maints, repo, "Repository")
        .await
        .expect("managed Maintenance should exist before opt-out");

    // Opt out.
    let patch = serde_json::json!({ "spec": { "maintenance": { "enabled": false } } });
    repos
        .patch(repo, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .expect("patch spec.maintenance.enabled=false");

    // The managed Maintenance is removed and the condition reports disabled.
    wait_until(
        "managed Maintenance removed after opt-out",
        default_timeout(),
        poll_interval(),
        || async {
            match maints.get_opt(repo).await? {
                Some(_) => Ok(None),
                None => Ok(Some(())),
            }
        },
    )
    .await
    .expect("operator should delete the managed Maintenance when disabled");
    wait_condition(&repos, repo, "MaintenanceConfigured", "False")
        .await
        .expect("disabled repo should report MaintenanceConfigured=False");
}

/// Foreign precedence: a user-authored `Maintenance` for a repository means the
/// operator must NOT create a duplicate managed one (named after the repo), and
/// the repo reports `MaintenanceConfigured=True`. Disabling `spec.maintenance`
/// must still leave the user's `Maintenance` untouched.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh)"]
async fn external_maintenance_is_not_duplicated() {
    let Some(client) = try_client().await else {
        return;
    };
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let maints: Api<Maintenance> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = "e2e-foreign-repo";
    let foreign = "e2e-foreign-maint";
    // Author the user's Maintenance first.
    maints
        .create(
            &PostParams::default(),
            &cr::<Maintenance>(maintenance_json(foreign, "Repository", repo)),
        )
        .await
        .expect("create user Maintenance");
    let _ = repos
        .create(&PostParams::default(), &cr(repository_json(repo)))
        .await;
    wait_phase(&repos, repo, "Ready")
        .await
        .expect("Repository should reach Ready");

    // Covered by the user's Maintenance.
    wait_condition(&repos, repo, "MaintenanceConfigured", "True")
        .await
        .expect("repo covered by an external Maintenance should be True");

    // The operator must NOT have created a managed Maintenance named after the repo.
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert!(
        maints.get_opt(repo).await.expect("get").is_none(),
        "operator must not duplicate a user-authored Maintenance"
    );

    // Disabling does not remove the user's Maintenance, nor change coverage.
    let patch = serde_json::json!({ "spec": { "maintenance": { "enabled": false } } });
    repos
        .patch(repo, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .expect("patch spec.maintenance.enabled=false");
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert!(
        maints.get_opt(foreign).await.expect("get").is_some(),
        "disabling spec.maintenance must never remove a user-authored Maintenance"
    );
    wait_condition(&repos, repo, "MaintenanceConfigured", "True")
        .await
        .expect("external-covered repo stays True even when spec.maintenance is disabled");
}

/// The cluster-scoped path: a `ClusterRepository` with no `spec.maintenance`
/// default-manages a `Maintenance` placed in the operator's own namespace
/// (`KOPIUR_NAMESPACE`, which the e2e harness installs as the e2e namespace),
/// owned by the (cluster-scoped) ClusterRepository.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh)"]
async fn cluster_repository_default_creates_managed_maintenance() {
    let Some(client) = try_client().await else {
        return;
    };
    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    let maints: Api<Maintenance> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let crepo = "e2e-defmaint-crepo";
    let _ = crepos
        .create(&PostParams::default(), &cr(cluster_repository_json(crepo)))
        .await;
    wait_phase(&crepos, crepo, "Ready")
        .await
        .expect("ClusterRepository should reach Ready");

    let m = wait_managed_maintenance(&maints, crepo, "ClusterRepository")
        .await
        .expect(
            "operator should create an owned Maintenance for the cluster repo in its namespace",
        );
    let mv = serde_json::to_value(&m).unwrap_or_default();
    assert_eq!(
        mv.pointer("/spec/repository/kind").and_then(|v| v.as_str()),
        Some("ClusterRepository")
    );
    wait_condition(&crepos, crepo, "MaintenanceConfigured", "True")
        .await
        .expect("cluster repo with default-managed maintenance should be True");

    let _ = crepos.delete(crepo, &DeleteParams::default()).await;
}

/// The `kopiur_repository_maintenance_configured` gauge reflects live state via
/// `/metrics`: `1` once the operator manages a `Maintenance` for the repo
/// (default-on), `0` after opting out. Proves the metric surface end-to-end.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh)"]
async fn maintenance_configured_reflected_in_metrics() {
    let Some(client) = try_client().await else {
        return;
    };
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = "e2e-mxmaint-repo";
    let _ = repos
        .create(&PostParams::default(), &cr(repository_json(repo)))
        .await;
    wait_phase(&repos, repo, "Ready")
        .await
        .expect("Repository should reach Ready");

    // Default-on: gauge reads 1 once the managed Maintenance exists.
    wait_until(
        "maintenance_configured gauge == 1",
        default_timeout(),
        poll_interval(),
        || {
            let client = client.clone();
            async move {
                Ok(scrape_controller_metrics(&client)
                    .await
                    .ok()
                    .filter(|t| maintenance_gauge_is(t, repo, "1"))
                    .map(|_| ()))
            }
        },
    )
    .await
    .expect("gauge should read 1 for a default-managed repo");

    // Opt out → gauge flips to 0.
    let patch = serde_json::json!({ "spec": { "maintenance": { "enabled": false } } });
    repos
        .patch(repo, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .expect("patch spec.maintenance.enabled=false");
    wait_until(
        "maintenance_configured gauge == 0",
        default_timeout(),
        poll_interval(),
        || {
            let client = client.clone();
            async move {
                Ok(scrape_controller_metrics(&client)
                    .await
                    .ok()
                    .filter(|t| maintenance_gauge_is(t, repo, "0"))
                    .map(|_| ()))
            }
        },
    )
    .await
    .expect("gauge should read 0 after opting out");
}

/// True if the exposition has a `kopiur_repository_maintenance_configured` series
/// for `name` whose value equals `want` ("0"/"1").
fn maintenance_gauge_is(text: &str, name: &str, want: &str) -> bool {
    text.lines().any(|l| {
        l.starts_with("kopiur_repository_maintenance_configured{")
            && l.contains(&format!("name=\"{name}\""))
            && l.trim_end().ends_with(&format!(" {want}"))
    })
}

/// Wait until a `Maintenance` named `name` exists and is owned (controller
/// `ownerReference`) by `owner_kind`/`name` — i.e. the operator-managed one.
async fn wait_managed_maintenance(
    maints: &Api<Maintenance>,
    name: &str,
    owner_kind: &str,
) -> anyhow::Result<Maintenance> {
    wait_until(
        &format!("managed Maintenance {name} owned by {owner_kind}"),
        default_timeout(),
        poll_interval(),
        || async {
            match maints.get_opt(name).await? {
                Some(m) => {
                    let owned = m.owner_references().iter().any(|o| {
                        o.kind == owner_kind && o.name == name && o.controller == Some(true)
                    });
                    Ok(owned.then_some(m))
                }
                None => Ok(None),
            }
        },
    )
    .await
}

/// Compile-time guard that `Client` is reachable from this crate even when the
/// `e2e` feature gates the bodies above — keeps the dependency graph honest.
#[allow(dead_code)]
fn _type_anchor(_c: Client) {}
