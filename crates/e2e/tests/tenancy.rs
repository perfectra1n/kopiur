//! e2e: multi-tenancy authorization — the `credentialProjection.allowed` repository-
//! owner gate fails closed (ADR-0005 §8).
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; driven by
//! `mise run //crates/e2e:test`. Skips gracefully without a cluster.

#![cfg(all(unix, feature = "e2e"))]

mod common;
use common::*;

use kube::Api;
use kube::api::{DeleteParams, PostParams};

use k8s_openapi::api::core::v1::Namespace;
use kopiur_api::{ClusterRepository, Snapshot, SnapshotPolicy};
use kopiur_e2e::{Need, World, ensure_namespace};

/// `credentialProjection.allowed` fail-closed gate (ADR-0005 §8): a consumer that opts
/// in (`credentialProjection.enabled: true`) but whose `ClusterRepository` owner has
/// NOT set `credentialProjection.allowed: true` must be refused — the Snapshot blocks
/// on `CredentialsAvailable=False` naming the unmet owner gate, and never launches a
/// mover. (The projection-ON happy path is covered in `credential_projection.rs`,
/// where the owner allows it.)
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn credential_projection_fails_closed_when_owner_disallows() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();

    const APP_NS: &str = "kopiur-e2e-projgate";
    let crepo = "e2e-projgate-crepo";

    ensure_repo(&client, "projgate").await;
    // A ClusterRepository that does NOT allow projection (allowed defaults to false).
    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    crepos
        .create(
            &PostParams::default(),
            &cr(cluster_repository_json(
                crepo,
                "projgate",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create ClusterRepository (credentialProjection.allowed defaults false)");
    wait_phase(&crepos, crepo, "Ready")
        .await
        .expect("ClusterRepository Ready");

    // A workload namespace WITHOUT the creds Secret, and its own source PVC.
    ensure_namespace(&client, APP_NS)
        .await
        .expect("create workload namespace");
    ensure_workload_source(&client, APP_NS, "projgate").await;

    // A SnapshotPolicy that OPTS IN to projection (enabled: true) — but the owner
    // disallows it, so it must fail closed.
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), APP_NS);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), APP_NS);
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                APP_NS,
                "e2e-projgate-policy",
                "ClusterRepository",
                crepo,
                serde_json::json!({ "credentialProjection": { "enabled": true } }),
            )),
        )
        .await
        .expect("create SnapshotPolicy opting into projection");
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                APP_NS,
                "e2e-projgate-backup",
                "e2e-projgate-policy",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create Snapshot");

    // Fail closed: CredentialsAvailable=False, message names the owner-allow gate.
    let cond = wait_condition(
        &backups,
        "e2e-projgate-backup",
        "CredentialsAvailable",
        "False",
    )
    .await
    .expect("projection must fail closed when the ClusterRepository owner disallows it");
    let msg = cond.get("message").and_then(|m| m.as_str()).unwrap_or("");
    assert!(
        msg.contains("credentialProjection.allowed"),
        "the fail-closed message must point the user at the owner-allow gate; got: {msg}"
    );
    // It must NOT have launched a mover / Succeeded.
    let phase = status_json(&backups, "e2e-projgate-backup")
        .await
        .get("phase")
        .and_then(|p| p.as_str())
        .unwrap_or("")
        .to_string();
    assert_ne!(
        phase, "Succeeded",
        "a fail-closed Snapshot must not have succeeded"
    );

    // Cleanup.
    let _ = crepos.delete(crepo, &DeleteParams::default()).await;
    let nss: Api<Namespace> = Api::all(client.clone());
    let _ = nss.delete(APP_NS, &DeleteParams::default()).await;
}
