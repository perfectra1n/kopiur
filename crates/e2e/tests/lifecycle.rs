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

use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client, ResourceExt};
use serde::de::DeserializeOwned;

use kopiur_api::{
    Backup, BackupConfig, BackupSchedule, ClusterRepository, Maintenance, Repository, Restore,
};
use kopiur_e2e::{E2E_NAMESPACE, default_timeout, poll_interval, try_client, wait_until};

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

/// Compile-time guard that `Client` is reachable from this crate even when the
/// `e2e` feature gates the bodies above — keeps the dependency graph honest.
#[allow(dead_code)]
fn _type_anchor(_c: Client) {}
