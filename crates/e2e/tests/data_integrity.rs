//! e2e: data-integrity guarantees — `onNamespaceDelete` Orphan/Delete (ADR-0005 §5)
//! and `Snapshot.spec.pin` surviving GFS prune (ADR-0005 §13(c)).
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; driven by
//! `mise run //crates/e2e:test`. Skips gracefully without a cluster.

#![cfg(all(unix, feature = "e2e"))]

mod common;
use common::*;

use std::time::Duration;

use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};

use k8s_openapi::api::core::v1::Namespace;
use kopiur_api::{ClusterRepository, Repository, Snapshot, SnapshotPolicy};
use kopiur_e2e::{
    E2E_NAMESPACE, Need, World, apply_secret, ensure_namespace, poll_interval, wait_until,
};

// ---------------------------------------------------------------------------
// onNamespaceDelete Orphan (default) vs Delete — the data-loss-prevention fix
// ---------------------------------------------------------------------------

/// Drive a ClusterRepository (with the given `onNamespaceDelete` policy) + a
/// SnapshotPolicy + Snapshot in a fresh workload namespace to Succeeded, delete the
/// namespace, and return the snapshot count observed in the repo afterward.
async fn namespace_delete_scenario(
    client: &Client,
    label: &str,
    subpath: &str,
    on_namespace_delete: &str,
) -> i64 {
    let app_ns = format!("kopiur-e2e-nsdel-{label}");
    let crepo = format!("e2e-nsdel-{label}-crepo");

    ensure_repo(client, subpath).await;
    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    crepos
        .create(
            &PostParams::default(),
            &cr(cluster_repository_json(
                &crepo,
                subpath,
                serde_json::json!({ "onNamespaceDelete": on_namespace_delete }),
            )),
        )
        .await
        .expect("create ClusterRepository");
    wait_phase(&crepos, &crepo, "Ready")
        .await
        .expect("ClusterRepository should reach Ready");

    ensure_namespace(client, &app_ns)
        .await
        .expect("create workload namespace");
    // The workload namespace needs the repo password Secret (a mover loads it via
    // envFrom; a ClusterRepository's Secret lives in the operator namespace).
    apply_secret(
        client,
        &app_ns,
        CREDS_SECRET,
        &[("KOPIA_PASSWORD", "e2e-test-password-123")],
    )
    .await
    .expect("place creds Secret in workload namespace");
    // And its own source PVC over the shared source hostPath dir.
    ensure_workload_source(client, &app_ns, &format!("nsdel-{label}")).await;

    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), &app_ns);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), &app_ns);
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                &app_ns,
                "nsdel-policy",
                "ClusterRepository",
                &crepo,
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create SnapshotPolicy");
    // deletionPolicy: Delete so the per-Snapshot plan would delete — the namespace
    // cascade policy is what decides whether that plan actually runs.
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                &app_ns,
                "nsdel-backup",
                "nsdel-policy",
                serde_json::json!({ "deletionPolicy": "Delete" }),
            )),
        )
        .await
        .expect("create Snapshot");
    wait_phase(&backups, "nsdel-backup", "Succeeded")
        .await
        .expect("Snapshot should reach Succeeded");

    // Delete the consuming namespace and wait until it is fully gone (the Snapshot's
    // finalizer must run the namespace-delete cascade before the ns is reaped).
    let nss: Api<Namespace> = Api::all(client.clone());
    nss.delete(&app_ns, &DeleteParams::default())
        .await
        .expect("delete workload namespace");
    wait_until(
        &format!("namespace {app_ns} fully deleted"),
        Duration::from_secs(180),
        poll_interval(),
        || async {
            match nss.get_opt(&app_ns).await? {
                Some(_) => Ok(None),
                None => Ok(Some(())),
            }
        },
    )
    .await
    .expect("workload namespace should be fully deleted (finalizers cleared)");

    let count =
        observed_snapshot_count(client, &format!("e2e-nsdel-{label}-verify"), subpath).await;
    let _ = crepos.delete(&crepo, &DeleteParams::default()).await;
    count
}

/// DEFAULT (`Orphan`): deleting the consuming namespace must NOT destroy the kopia
/// snapshot — `kubectl delete ns` no longer loses off-site backup history (the
/// breaking default change, ADR-0005 §5). The snapshot survives in the repo.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn on_namespace_delete_orphan_keeps_snapshot() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    let count = namespace_delete_scenario(&client, "orphan", "nsdel-orphan", "Orphan").await;
    assert!(
        count >= 1,
        "with onNamespaceDelete: Orphan the kopia snapshot must survive a namespace delete, \
         but the repo reports {count} snapshots"
    );
}

/// Opt-in (`Delete`): the cascade honors each `Snapshot`'s own `deletionPolicy`, so a
/// `deletionPolicy: Delete` snapshot IS removed from the repo when the namespace is
/// deleted. Proves the opt-in path actually reclaims storage.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn on_namespace_delete_delete_cascades_snapshot() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    let count = namespace_delete_scenario(&client, "delete", "nsdel-delete", "Delete").await;
    assert_eq!(
        count, 0,
        "with onNamespaceDelete: Delete the snapshot's own deletionPolicy:Delete must cascade, \
         leaving the repo empty, but it reports {count} snapshots"
    );
}

// ---------------------------------------------------------------------------
// Snapshot pin survives GFS prune
// ---------------------------------------------------------------------------

/// A pinned `Snapshot` (`spec.pin: true`, ADR-0005 §13(c)) is EXEMPT from GFS
/// retention: with `keepLatest: 1`, a second (unpinned) snapshot would normally prune
/// the first — but a pinned first snapshot survives while an unpinned older snapshot
/// does not.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn pinned_snapshot_survives_gfs_prune() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_repo(&client, "pin").await;

    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = "e2e-pin-repo";
    let policy = "e2e-pin-policy";
    repos
        .create(
            &PostParams::default(),
            &cr(repository_json(repo, "pin", serde_json::json!({}))),
        )
        .await
        .expect("create Repository");
    wait_phase(&repos, repo, "Ready").await.expect("repo Ready");
    // keepLatest: 1 so any second snapshot prunes everything but the newest UNLESS pinned.
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                E2E_NAMESPACE,
                policy,
                "Repository",
                repo,
                serde_json::json!({ "retention": { "keepLatest": 1 } }),
            )),
        )
        .await
        .expect("create SnapshotPolicy keepLatest=1");

    // A PINNED first snapshot, and an UNPINNED second snapshot of the same policy.
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-pin-keep",
                policy,
                serde_json::json!({ "pin": true, "deletionPolicy": "Delete" }),
            )),
        )
        .await
        .expect("create pinned Snapshot");
    wait_phase(&backups, "e2e-pin-keep", "Succeeded")
        .await
        .expect("pinned Snapshot Succeeded");
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-pin-prune",
                policy,
                serde_json::json!({ "deletionPolicy": "Delete" }),
            )),
        )
        .await
        .expect("create unpinned Snapshot");
    wait_phase(&backups, "e2e-pin-prune", "Succeeded")
        .await
        .expect("unpinned Snapshot Succeeded");

    // The unpinned older snapshot must be pruned by retention (keepLatest=1)...
    wait_until(
        "unpinned older Snapshot pruned by GFS retention",
        Duration::from_secs(150),
        poll_interval(),
        || async {
            match backups.get_opt("e2e-pin-prune").await? {
                Some(_) => Ok(None),
                None => Ok(Some(())),
            }
        },
    )
    .await
    .expect("the unpinned Snapshot should be pruned by keepLatest=1 retention");

    // ...while the PINNED snapshot survives the same prune.
    let pinned = backups
        .get_opt("e2e-pin-keep")
        .await
        .expect("get pinned Snapshot");
    assert!(
        pinned.is_some(),
        "the pinned Snapshot must survive GFS retention that pruned the unpinned one"
    );

    // Cleanup.
    let _ = backups
        .delete("e2e-pin-keep", &DeleteParams::default())
        .await;
    let _ = policies.delete(policy, &DeleteParams::default()).await;
    let _ = repos.delete(repo, &DeleteParams::default()).await;
}
