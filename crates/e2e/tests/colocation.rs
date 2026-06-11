//! e2e: RWO source-PVC node co-location (the Multi-Attach fix).
//!
//! A `ReadWriteOnce` PVC can only be *attached* to one node at a time. When an app
//! pod already holds an RWO PVC on node N and the backup mover lands elsewhere, the
//! mover pod is stuck `Multi-Attach error`. The controller resolves the node the PVC
//! is attached to (via the consuming pod) and pins the mover there with a required
//! `kubernetes.io/hostname` nodeAffinity, so it co-locates with the workload and the
//! kubelet can mount the volume.
//!
//! These scenarios reproduce the exact reported setup — an RWO PVC held by a running
//! pod — and assert the mover Job is tied to that pod's node (default `Auto` mode),
//! that the snapshot then succeeds, and that `Disabled` mode opts out of the pin.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; driven by
//! `mise run //crates/e2e:test`. Skips gracefully without a cluster.

#![cfg(all(unix, feature = "e2e"))]

mod common;
use common::*;

use kube::Api;
use kube::api::{DeleteParams, PostParams};

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use kopiur_api::{Repository, Snapshot, SnapshotPolicy};
use kopiur_e2e::{E2E_NAMESPACE, Need, World, default_timeout, poll_interval, wait_until};

/// The well-known node-hostname label the mover is pinned to.
const HOSTNAME_LABEL: &str = "kubernetes.io/hostname";

/// Extract the `kubernetes.io/hostname` `In` values from a mover Job's REQUIRED
/// nodeAffinity (`None` if the mover carries no such pin).
fn hostname_pin(job: &Job) -> Option<Vec<String>> {
    job.spec
        .as_ref()?
        .template
        .spec
        .as_ref()?
        .affinity
        .as_ref()?
        .node_affinity
        .as_ref()?
        .required_during_scheduling_ignored_during_execution
        .as_ref()?
        .node_selector_terms
        .iter()
        .flat_map(|t| t.match_expressions.iter().flatten())
        .find(|e| e.key == HOSTNAME_LABEL && e.operator == "In")
        .and_then(|e| e.values.clone())
}

/// Create an RWO PVC and a long-running consumer pod that mounts it, then wait until
/// the pod is `Running` on a node (which binds + attaches the RWO volume). Returns the
/// node the PVC is now attached to.
async fn rwo_pvc_held_on_a_node(client: &kube::Client, pvc: &str, consumer: &str) -> String {
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    pvcs.create(
        &PostParams::default(),
        &kopiur_e2e::builders::dynamic_pvc(E2E_NAMESPACE, pvc, "100Mi"),
    )
    .await
    .unwrap_or_else(|e| panic!("create RWO PVC {pvc}: {e}"));

    pods.create(
        &PostParams::default(),
        &kopiur_e2e::builders::sleeper_pod(
            E2E_NAMESPACE,
            consumer,
            &[("app", consumer)],
            pvc,
            "/data",
        ),
    )
    .await
    .unwrap_or_else(|e| panic!("create consumer pod {consumer}: {e}"));

    // The dynamic (WaitForFirstConsumer) PVC binds and attaches to whichever node the
    // consumer lands on; capture that node once the pod is Running.
    wait_until(
        &format!("consumer {consumer} Running on a node"),
        default_timeout(),
        poll_interval(),
        || async {
            let Some(p) = pods.get_opt(consumer).await? else {
                return Ok(None);
            };
            let running = p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running");
            let node = p.spec.as_ref().and_then(|s| s.node_name.clone());
            Ok(running.then_some(node).flatten())
        },
    )
    .await
    .unwrap_or_else(|_| panic!("consumer {consumer} should reach Running on a node"))
}

/// THE HEADLINE. With the default `Auto` mode, a backup whose source is an RWO PVC
/// held by a running pod gets its mover Job PINNED (required nodeAffinity on
/// `kubernetes.io/hostname`) to exactly that pod's node — so on a real multi-node
/// cluster it co-locates with the workload instead of failing `Multi-Attach error` —
/// and the snapshot then succeeds while the volume is concurrently mounted.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn auto_pins_rwo_source_mover_to_the_consumer_node() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_repo(&client, "colocation").await;

    let pvc = "e2e-colo-src";
    let consumer = "e2e-colo-consumer";
    let node = rwo_pvc_held_on_a_node(&client, pvc, consumer).await;

    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = "e2e-colo-repo";
    repos
        .create(
            &PostParams::default(),
            &cr(repository_json(repo, "colocation", serde_json::json!({}))),
        )
        .await
        .expect("create Repository");
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                E2E_NAMESPACE,
                "e2e-colo-policy",
                "Repository",
                repo,
                // Snapshot the held RWO PVC (overrides the shared `e2e-src` source).
                serde_json::json!({ "sources": [ { "pvc": { "name": pvc } } ] }),
            )),
        )
        .await
        .expect("create SnapshotPolicy over the RWO source");
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-colo-backup",
                "e2e-colo-policy",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create Snapshot");

    // The mover Job must be pinned to the node holding the RWO PVC.
    let bjob = wait_for_job(&jobs, "e2e-colo-backup").await;
    assert_eq!(
        hostname_pin(&bjob).as_deref(),
        Some([node.clone()].as_slice()),
        "Auto mode must pin the RWO-source mover to the consumer pod's node ({node})"
    );

    // And the snapshot succeeds while the consumer still holds the volume (proving the
    // co-mount on the same node works end-to-end).
    wait_phase(&backups, "e2e-colo-backup", "Succeeded")
        .await
        .expect("snapshot of the co-located RWO PVC should Succeed");

    // Cleanup.
    let _ = backups
        .delete("e2e-colo-backup", &DeleteParams::default())
        .await;
    let _ = policies
        .delete("e2e-colo-policy", &DeleteParams::default())
        .await;
    let _ = repos.delete(repo, &DeleteParams::default()).await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = pods.delete(consumer, &DeleteParams::default()).await;
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = pvcs.delete(pvc, &DeleteParams::default()).await;
}

/// The escape hatch: `moverDefaults.sourceColocation.mode: Disabled` must leave the
/// mover UNPINNED (no injected `kubernetes.io/hostname` nodeAffinity) even for an RWO
/// source held by a running pod — the pre-fix scheduling behavior, for topologies that
/// manage placement themselves. The snapshot still succeeds (single-node cluster).
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn disabled_mode_leaves_the_rwo_mover_unpinned() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_repo(&client, "colocation-off").await;

    let pvc = "e2e-colo-off-src";
    let consumer = "e2e-colo-off-consumer";
    let _node = rwo_pvc_held_on_a_node(&client, pvc, consumer).await;

    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = "e2e-colo-off-repo";
    repos
        .create(
            &PostParams::default(),
            &cr(repository_json(
                repo,
                "colocation-off",
                serde_json::json!({
                    "moverDefaults": { "sourceColocation": { "mode": "Disabled" } }
                }),
            )),
        )
        .await
        .expect("create Repository with sourceColocation.mode=Disabled");
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                E2E_NAMESPACE,
                "e2e-colo-off-policy",
                "Repository",
                repo,
                serde_json::json!({ "sources": [ { "pvc": { "name": pvc } } ] }),
            )),
        )
        .await
        .expect("create SnapshotPolicy");
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-colo-off-backup",
                "e2e-colo-off-policy",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create Snapshot");

    let bjob = wait_for_job(&jobs, "e2e-colo-off-backup").await;
    assert!(
        hostname_pin(&bjob).is_none(),
        "Disabled mode must not inject a hostname nodeAffinity pin, got {:?}",
        hostname_pin(&bjob)
    );
    wait_phase(&backups, "e2e-colo-off-backup", "Succeeded")
        .await
        .expect("snapshot should still Succeed with co-location disabled");

    // Cleanup.
    let _ = backups
        .delete("e2e-colo-off-backup", &DeleteParams::default())
        .await;
    let _ = policies
        .delete("e2e-colo-off-policy", &DeleteParams::default())
        .await;
    let _ = repos.delete(repo, &DeleteParams::default()).await;
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = pods.delete(consumer, &DeleteParams::default()).await;
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = pvcs.delete(pvc, &DeleteParams::default()).await;
}
