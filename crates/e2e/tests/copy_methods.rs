//! e2e: `copyMethod` — CSI snapshot/clone staging vs. direct (ADR §3.3).
//!
//! Two scenarios:
//!
//! 1. **Fail-loud** (always runs): `copyMethod: Snapshot` over a source PVC that is NOT
//!    CSI-provisioned (the static hostPath `e2e-src`) must FAIL with an actionable
//!    `SourceStaged=False` condition — never silently fall back to a live read.
//! 2. **Stage + cleanup** (runs only when the CSI snapshot stack is installed — see the
//!    `snapshot-stack` mise task; skips gracefully otherwise): `copyMethod: Snapshot`
//!    over a CSI-provisioned PVC creates a VolumeSnapshot + staged PVC, the mover reads
//!    the stage, the Snapshot Succeeds, and the staged objects are reaped.
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
use k8s_openapi::api::storage::v1::StorageClass;
use kopiur_api::{Repository, Snapshot, SnapshotPolicy};
use kopiur_e2e::{E2E_NAMESPACE, Need, World, default_timeout, poll_interval, wait_until};

/// The storage class + snapshot class the `snapshot-stack` mise task installs.
const CSI_STORAGE_CLASS: &str = "csi-hostpath-sc";

/// FAIL-LOUD: a `Snapshot`-mode backup of a non-CSI (static hostPath) source PVC must
/// fail with an actionable `SourceStaged=False` condition, not silently read the live
/// volume. Always runs (no CSI stack needed) — guards the headline reliability promise.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn snapshot_mode_on_non_csi_source_fails_with_actionable_condition() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_repo(&client, "copymethod").await;

    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    repos
        .create(
            &PostParams::default(),
            &cr(repository_json(
                "e2e-cm-repo",
                "copymethod",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create Repository");
    // copyMethod: Snapshot over the shared static hostPath `e2e-src` PVC (storageClassName
    // "" → no CSI provisioner → cannot be snapshotted).
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                E2E_NAMESPACE,
                "e2e-cm-policy",
                "Repository",
                "e2e-cm-repo",
                serde_json::json!({ "copyMethod": "Snapshot" }),
            )),
        )
        .await
        .expect("create SnapshotPolicy (copyMethod: Snapshot)");
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-cm-backup",
                "e2e-cm-policy",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create Snapshot");

    // The staging preflight fails with a clear, actionable reason, and the Snapshot is
    // Failed — it never created a mover Job reading the live volume.
    let cond = wait_condition(&backups, "e2e-cm-backup", "SourceStaged", "False")
        .await
        .expect("SourceStaged=False condition");
    let reason = cond
        .get("reason")
        .and_then(|r| r.as_str())
        .unwrap_or_default();
    assert_eq!(
        reason, "SourceNotCSIProvisioned",
        "expected an actionable non-CSI reason; condition was {cond:?}"
    );
    let msg = cond
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or_default();
    assert!(
        msg.contains("copyMethod: Direct"),
        "message should offer the Direct fallback: {msg}"
    );
    wait_phase(&backups, "e2e-cm-backup", "Failed")
        .await
        .expect("Snapshot reaches Failed");

    // No mover Job was created (staging failed before the Job).
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    assert!(
        jobs.get_opt("e2e-cm-backup")
            .await
            .expect("get job")
            .is_none(),
        "no mover Job should exist for a staging-failed backup"
    );

    let _ = backups
        .delete("e2e-cm-backup", &DeleteParams::default())
        .await;
    let _ = policies
        .delete("e2e-cm-policy", &DeleteParams::default())
        .await;
    let _ = repos.delete("e2e-cm-repo", &DeleteParams::default()).await;
}

/// STAGE + CLEANUP: `copyMethod: Snapshot` over a CSI-provisioned PVC creates a
/// VolumeSnapshot + staged PVC, the mover reads the stage, the Snapshot Succeeds, and
/// the staged objects are reaped. SKIPS when the CSI snapshot stack isn't installed.
#[tokio::test]
#[ignore = "requires the e2e harness + the CSI snapshot stack (mise run //crates/e2e:snapshot-stack)"]
async fn snapshot_mode_stages_a_csi_volumesnapshot_and_cleans_up() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();

    // Skip gracefully unless the CSI hostpath StorageClass is present.
    let scs: Api<StorageClass> = Api::all(client.clone());
    if scs
        .get_opt(CSI_STORAGE_CLASS)
        .await
        .expect("list storageclasses")
        .is_none()
    {
        eprintln!(
            "SKIP: storageclass {CSI_STORAGE_CLASS} not found — run `mise run //crates/e2e:snapshot-stack`"
        );
        return;
    }

    ensure_repo(&client, "copymethod-csi").await;
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // A CSI-provisioned source PVC, populated + bound by a one-shot pod.
    let src = "e2e-cm-csi-src";
    pvcs.create(
        &PostParams::default(),
        &cr(serde_json::json!({
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": { "name": src, "namespace": E2E_NAMESPACE },
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": CSI_STORAGE_CLASS,
                "resources": { "requests": { "storage": "64Mi" } },
            },
        })),
    )
    .await
    .expect("create CSI source PVC");
    pods.create(
        &PostParams::default(),
        &cr(serde_json::json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": { "name": "e2e-cm-csi-seed", "namespace": E2E_NAMESPACE },
            "spec": {
                "restartPolicy": "Never",
                "containers": [{
                    "name": "seed", "image": kopiur_e2e::consts::BUSYBOX_IMAGE,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["sh", "-c", "echo kopiur-csi > /data/marker.txt"],
                    "volumeMounts": [{ "name": "d", "mountPath": "/data" }],
                }],
                "volumes": [{ "name": "d", "persistentVolumeClaim": { "claimName": src } }],
            },
        })),
    )
    .await
    .expect("create seed pod");
    // Wait for the PVC to bind (the seed pod completing implies it bound).
    wait_until(
        "CSI source PVC Bound",
        default_timeout(),
        poll_interval(),
        || async {
            let bound = pvcs
                .get_opt(src)
                .await?
                .and_then(|p| p.status.and_then(|s| s.phase))
                .as_deref()
                == Some("Bound");
            Ok(bound.then_some(()))
        },
    )
    .await
    .expect("source PVC should bind");

    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    repos
        .create(
            &PostParams::default(),
            &cr(repository_json(
                "e2e-cm-csi-repo",
                "copymethod-csi",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create Repository");
    // copyMethod: Snapshot, volumeSnapshotClassName unset → auto-select the default
    // class for the hostpath driver (annotated default by the snapshot-stack task).
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                E2E_NAMESPACE,
                "e2e-cm-csi-policy",
                "Repository",
                "e2e-cm-csi-repo",
                serde_json::json!({
                    "copyMethod": "Snapshot",
                    "sources": [ { "pvc": { "name": src } } ],
                }),
            )),
        )
        .await
        .expect("create SnapshotPolicy");
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-cm-csi-backup",
                "e2e-cm-csi-policy",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create Snapshot");

    // The backup succeeds reading the staged PVC.
    wait_phase(&backups, "e2e-cm-csi-backup", "Succeeded")
        .await
        .expect("Snapshot of the CSI-staged source should Succeed");

    // status.staged recorded the VolumeSnapshot + staged PVC.
    let status = status_json(&backups, "e2e-cm-csi-backup").await;
    let staged = status.get("staged").cloned().unwrap_or_default();
    assert_eq!(
        staged.get("copyMethod").and_then(|v| v.as_str()),
        Some("Snapshot"),
        "status.staged should record copyMethod Snapshot: {status}"
    );
    assert!(
        staged
            .get("volumeSnapshotName")
            .and_then(|v| v.as_str())
            .is_some(),
        "status.staged should name the VolumeSnapshot: {status}"
    );

    // The staged PVC is reaped after success (no orphan). The controller names it
    // deterministically `<snapshot>-src`.
    let staged_pvc = "e2e-cm-csi-backup-src";
    wait_until(
        "staged PVC reaped",
        default_timeout(),
        poll_interval(),
        || async { Ok(pvcs.get_opt(staged_pvc).await?.is_none().then_some(())) },
    )
    .await
    .expect("staged PVC should be cleaned up after the backup succeeds");

    // Cleanup.
    let _ = backups
        .delete("e2e-cm-csi-backup", &DeleteParams::default())
        .await;
    let _ = policies
        .delete("e2e-cm-csi-policy", &DeleteParams::default())
        .await;
    let _ = repos
        .delete("e2e-cm-csi-repo", &DeleteParams::default())
        .await;
    let _ = pods
        .delete("e2e-cm-csi-seed", &DeleteParams::default())
        .await;
    let _ = pvcs.delete(src, &DeleteParams::default()).await;
}
