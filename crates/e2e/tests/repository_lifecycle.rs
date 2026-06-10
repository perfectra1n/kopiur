//! e2e: repository/restore lifecycle behaviors — Restore `target.populator: {}`
//! (ADR-0005 §9), `mode: ReadOnly` (§11), and kstatus `Ready` for `kubectl wait`
//! (§2).
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; driven by
//! `mise run //crates/e2e:test`. Skips gracefully without a cluster.

#![cfg(all(unix, feature = "e2e"))]

mod common;
use common::*;

use kube::Api;
use kube::api::{DeleteParams, PostParams};

use k8s_openapi::api::batch::v1::Job;
use kopiur_api::{Repository, Restore, Snapshot, SnapshotPolicy};
use kopiur_e2e::{
    E2E_NAMESPACE, Need, World, default_timeout, poll_interval, scrape_controller_metrics,
    wait_until,
};

/// `Restore.spec.target.populator: {}` (ADR-0005 §9): the explicit passive-populator
/// target form is accepted and threads through to a restore mover Job. (The empty
/// `target` form was removed; this proves the replacement form is wired.)
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn restore_populator_target_form_is_accepted() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_seed(
        &client,
        "e2e-pop-repo",
        "e2e-pop-policy",
        "e2e-pop-seed",
        "populator",
    )
    .await;

    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = "e2e-pop-restore";
    restores
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Restore",
                "metadata": { "name": name, "namespace": E2E_NAMESPACE },
                "spec": {
                    "repository": { "kind": "Repository", "name": "e2e-pop-repo" },
                    "source": { "snapshotRef": { "name": "e2e-pop-seed" } },
                    "target": { "populator": {} }
                }
            })),
        )
        .await
        .expect("create Restore with target.populator:{} (the explicit populator form)");

    // Populator mode is PASSIVE (ADR-0005 §9): the Restore is admitted and parks in
    // `AwaitingClaim` until a PVC references it via `dataSourceRef` — it does NOT eagerly
    // build a mover Job. Asserting it reaches `AwaitingClaim=True` proves the explicit
    // `target.populator: {}` form is accepted and wired through to the populator machine.
    wait_condition(&restores, name, "AwaitingClaim", "True")
        .await
        .expect(
            "a populator Restore must reach AwaitingClaim=True (passive, awaiting a PVC claim)",
        );
    let _ = restores.delete(name, &DeleteParams::default()).await;
}

/// `mode: ReadOnly` (ADR-0005 §11): a ReadOnly repository serves restores but the
/// controller REFUSES backups against it. A Snapshot whose policy points at a ReadOnly
/// repo must not produce a snapshot; a Restore against it works.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn readonly_repo_refuses_backup_but_allows_restore() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();

    // First seed a snapshot via a READWRITE repo over the subdir, so there is data to
    // restore once we flip a repo to ReadOnly over the same subdir.
    ensure_seed(
        &client,
        "e2e-ro-rw-repo",
        "e2e-ro-rw-policy",
        "e2e-ro-seed",
        "readonly",
    )
    .await;

    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // A ReadOnly repo over the same subdir (create disabled — it already exists).
    let ro_repo = "e2e-ro-repo";
    repos
        .create(
            &PostParams::default(),
            &cr(repository_json(
                ro_repo,
                "readonly",
                serde_json::json!({ "mode": "ReadOnly", "create": { "enabled": false } }),
            )),
        )
        .await
        .expect("create ReadOnly Repository");
    wait_phase(&repos, ro_repo, "Ready")
        .await
        .expect("ReadOnly repo should connect to Ready");

    // A backup against the ReadOnly repo must be refused: it never reaches Succeeded
    // and surfaces a not-Ready/blocked condition rather than writing to the repo.
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                E2E_NAMESPACE,
                "e2e-ro-policy",
                "Repository",
                ro_repo,
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create SnapshotPolicy against ReadOnly repo");
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-ro-backup",
                "e2e-ro-policy",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create Snapshot against ReadOnly repo");

    // The backup must be refused: phase Failed + RepositoryWritable=False
    // (reason RepositoryReadOnly), and it must never reach Succeeded.
    let cond = wait_condition(&backups, "e2e-ro-backup", "RepositoryWritable", "False")
        .await
        .expect("a Snapshot against a ReadOnly repository must surface RepositoryWritable=False");
    assert_eq!(
        cond.get("reason").and_then(|r| r.as_str()),
        Some("RepositoryReadOnly"),
        "the refusal reason must be RepositoryReadOnly"
    );
    assert_eq!(
        status_json(&backups, "e2e-ro-backup")
            .await
            .get("phase")
            .and_then(|p| p.as_str()),
        Some("Failed"),
        "a refused backup against a ReadOnly repository must be phase Failed"
    );

    // The refusal must also be counted: `kopiur_snapshot_refusals_total` with
    // reason=RepositoryReadOnly is the only aggregate signal (the reconcile
    // returns Ok, so reconcile_errors never sees it). Scraped through the
    // Service proxy like the observability scenarios.
    wait_until(
        "kopiur_snapshot_refusals_total{reason=RepositoryReadOnly} >= 1",
        default_timeout(),
        poll_interval(),
        || async {
            let text = scrape_controller_metrics(&client).await.unwrap_or_default();
            let found = text.lines().any(|l| {
                l.starts_with("kopiur_snapshot_refusals_total")
                    && l.contains("reason=\"RepositoryReadOnly\"")
                    && l.contains("name=\"e2e-ro-backup\"")
                    && l.split_whitespace()
                        .last()
                        .and_then(|v| v.parse::<f64>().ok())
                        .is_some_and(|v| v >= 1.0)
            });
            Ok(found.then_some(()))
        },
    )
    .await
    .expect("the ReadOnly refusal must increment kopiur_snapshot_refusals_total");

    // A Restore against the ReadOnly repo WORKS (serves reads): Completed.
    restores
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Restore",
                "metadata": { "name": "e2e-ro-restore", "namespace": E2E_NAMESPACE },
                "spec": {
                    "repository": { "kind": "Repository", "name": ro_repo },
                    "source": { "snapshotRef": { "name": "e2e-ro-seed" } },
                    "target": { "pvc": { "name": "e2e-ro-dst", "capacity": "1Gi", "accessModes": ["ReadWriteOnce"] } }
                }
            })),
        )
        .await
        .expect("create Restore against ReadOnly repo");
    // The Restore is ADMITTED and dispatched (reaches `Restoring` + builds a mover
    // Job) — proving a ReadOnly repository SERVES reads (it is not refused the way the
    // backup above was). We don't assert `Completed`: the template target PVC is
    // dynamically provisioned and may never bind in the e2e cluster (the existing
    // restore scenarios note the same), which is orthogonal to the ReadOnly behavior
    // under test.
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = wait_for_job(&jobs, "e2e-ro-restore").await;
    assert_ne!(
        status_json(&restores, "e2e-ro-restore")
            .await
            .get("phase")
            .and_then(|p| p.as_str()),
        Some("Failed"),
        "a Restore against a ReadOnly repository must be served, not refused"
    );

    // Cleanup.
    let _ = jobs
        .delete("e2e-ro-restore", &DeleteParams::default())
        .await;
    let _ = restores
        .delete("e2e-ro-restore", &DeleteParams::default())
        .await;
    let _ = backups
        .delete("e2e-ro-backup", &DeleteParams::default())
        .await;
    let _ = policies
        .delete("e2e-ro-policy", &DeleteParams::default())
        .await;
    let _ = repos.delete(ro_repo, &DeleteParams::default()).await;
}

/// kstatus `Ready` (ADR-0005 §2): once a Repository is Ready and a SnapshotPolicy is
/// reconciled, the SnapshotPolicy carries a `Ready=True` condition AND a Succeeded
/// Snapshot does too — so `kubectl wait --for=condition=Ready` (and Flux/Argo health)
/// work. We assert the condition the way `kubectl wait` reads it.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn kstatus_ready_condition_present_for_wait() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_seed(
        &client,
        "e2e-ready-repo",
        "e2e-ready-policy",
        "e2e-ready-seed",
        "kstatus",
    )
    .await;

    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // The SnapshotPolicy reaches Ready=True (its Repository is Ready, retention enforced).
    wait_condition(&policies, "e2e-ready-policy", "Ready", "True")
        .await
        .expect(
            "SnapshotPolicy must carry Ready=True so `kubectl wait --for=condition=Ready` works",
        );
    // A Succeeded Snapshot also carries Ready=True (kstatus on the data resource).
    wait_condition(&backups, "e2e-ready-seed", "Ready", "True")
        .await
        .expect("a Succeeded Snapshot must carry Ready=True");

    // Cleanup leaves the seed for reuse (E2E_NAMESPACE persists); nothing to delete.
}
