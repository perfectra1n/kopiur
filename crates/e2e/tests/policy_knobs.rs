//! e2e: SnapshotPolicy "knob" surfaces that previously had no end-to-end guard —
//! `errorHandling.ignoreFileErrors` (against a REAL unreadable file) and the
//! `compression` / `upload` tuning (asserted at the controller→mover work-spec
//! contract, the seam where a regression would silently drop them while the
//! snapshot still succeeded).
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; skip gracefully off-cluster.

#![cfg(all(unix, feature = "e2e"))]

mod common;

use common::{
    cr, ensure_repo, observed_snapshot_count, repository_json, snapshot_json, snapshot_policy_json,
    wait_for_job, wait_phase,
};
use kube::Api;
use kube::api::PostParams;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;

use kopiur_api::{Repository, Snapshot, SnapshotPolicy};
use kopiur_e2e::{E2E_NAMESPACE, Need, World, consts};

const SUBPATH: &str = "errh";
const REPO: &str = "e2e-knobs-repo";

async fn ensure_knobs_repo(client: &kube::Client) {
    ensure_repo(client, SUBPATH).await;
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = repos
        .create(
            &PostParams::default(),
            &cr(repository_json(REPO, SUBPATH, serde_json::json!({}))),
        )
        .await;
    wait_phase(&repos, REPO, "Ready")
        .await
        .expect("knobs repository should reach Ready");
}

/// `errorHandling.ignoreFileErrors` against a REAL unreadable file (root-owned,
/// mode 0000, in the node-seeded `src-eh` dir). The negative control — the same
/// source WITHOUT the flag must FAIL — proves the fixture actually breaks a
/// default backup, so the flagged run passing means the flag reached kopia.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn ignore_file_errors_lets_snapshot_complete() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::ErrorSource])
        .await
        .expect("fixtures ready");
    let client = world.client().clone();
    ensure_knobs_repo(&client).await;

    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // Negative control: defaults must FAIL on the unreadable file.
    let strict = snapshot_policy_json(
        E2E_NAMESPACE,
        "e2e-eh-strict-cfg",
        "Repository",
        REPO,
        serde_json::json!({ "sources": [ { "pvc": { "name": consts::PVC_SRC_EH } } ] }),
    );
    let _ = configs.create(&PostParams::default(), &cr(strict)).await;
    let _ = backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-eh-strict",
                "e2e-eh-strict-cfg",
                // One attempt — the failure is deterministic.
                serde_json::json!({ "failurePolicy": { "backoffLimit": 0 } }),
            )),
        )
        .await;
    wait_phase(&backups, "e2e-eh-strict", "Failed")
        .await
        .expect(
            "a DEFAULT backup of a source with an unreadable file must fail — if this \
             succeeded, the fixture no longer breaks a default run and the positive case \
             below proves nothing",
        );

    // With the flag: the same source backs up cleanly.
    let lenient = snapshot_policy_json(
        E2E_NAMESPACE,
        "e2e-eh-lenient-cfg",
        "Repository",
        REPO,
        serde_json::json!({
            "sources": [ { "pvc": { "name": consts::PVC_SRC_EH } } ],
            "errorHandling": { "ignoreFileErrors": true }
        }),
    );
    let _ = configs.create(&PostParams::default(), &cr(lenient)).await;
    let _ = backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-eh-lenient",
                "e2e-eh-lenient-cfg",
                serde_json::json!({}),
            )),
        )
        .await;
    wait_phase(&backups, "e2e-eh-lenient", "Succeeded")
        .await
        .expect("ignoreFileErrors: true must let the backup complete past the unreadable file");

    // kopia-side proof that a real snapshot landed.
    let count = observed_snapshot_count(&client, "e2e-eh-verify", SUBPATH).await;
    assert!(
        count >= 1,
        "the lenient backup must have produced a kopia snapshot; verifier saw {count}"
    );
}

/// `compression` + `upload` knobs reach the controller→mover work-spec ConfigMap
/// (the contract the mover's `kopia policy set` consumes — its flag construction
/// is unit-tested in `crates/kopia`), and kopia accepts them (`Succeeded`: a bad
/// compressor would fail the mover's policy-set step).
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn compression_and_upload_knobs_reach_the_mover_contract() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("fixtures ready");
    let client = world.client().clone();
    ensure_knobs_repo(&client).await;

    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let cfg = snapshot_policy_json(
        E2E_NAMESPACE,
        "e2e-knobs-cfg",
        "Repository",
        REPO,
        serde_json::json!({
            "compression": { "compressor": "zstd", "neverCompress": ["*.zst"] },
            "upload": { "maxParallelSnapshots": 2, "maxParallelFileReads": 4 }
        }),
    );
    let _ = configs.create(&PostParams::default(), &cr(cfg)).await;
    let _ = backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-knobs",
                "e2e-knobs-cfg",
                serde_json::json!({}),
            )),
        )
        .await;

    // The work-spec ConfigMap (same name as the mover Job) carries the knobs.
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = wait_for_job(&jobs, "e2e-knobs").await;
    let cms: Api<ConfigMap> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let cm = cms.get("e2e-knobs").await.expect("work-spec ConfigMap");
    let spec_json = cm
        .data
        .as_ref()
        .and_then(|d| d.get("work-spec.json"))
        .expect("work-spec.json key");
    let spec: serde_json::Value =
        serde_json::from_str(spec_json).expect("work-spec parses as JSON");
    let policy = spec
        .pointer("/operation/snapshot/policy")
        .unwrap_or(&serde_json::Value::Null);
    assert_eq!(
        policy.get("compression").and_then(|v| v.as_str()),
        Some("zstd"),
        "compression must reach the mover contract; policy: {policy}"
    );
    assert_eq!(
        policy
            .get("neverCompress")
            .and_then(|v| v.as_array())
            .map(|a| a.len()),
        Some(1),
        "neverCompress must reach the mover contract; policy: {policy}"
    );
    assert_eq!(
        policy.get("maxParallelSnapshots").and_then(|v| v.as_i64()),
        Some(2),
        "upload.maxParallelSnapshots must reach the mover contract; policy: {policy}"
    );
    assert_eq!(
        policy.get("maxParallelFileReads").and_then(|v| v.as_i64()),
        Some(4),
        "upload.maxParallelFileReads must reach the mover contract; policy: {policy}"
    );

    // End-to-end close: kopia accepted the flags.
    wait_phase(&backups, "e2e-knobs", "Succeeded")
        .await
        .expect("the tuned backup should succeed (kopia accepted the policy flags)");
}
