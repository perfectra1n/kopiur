//! e2e: `SnapshotPolicy.spec.hooks` (ADR §4.8) — its own shard (`bins: "hooks"`).
//!
//! Hooks were a fully INERT CRD surface before this milestone: the API,
//! webhook schema, and docs all existed, but the controller never read
//! `spec.hooks`. These scenarios prove each hook form against a live operator,
//! and prove ORDERING by data: a pre-hook writes a sentinel file into the
//! source PVC, and the backup must capture it — restore + read it back, so a
//! hook that ran late (or not at all) fails the test even if everything
//! "succeeded".
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; skip gracefully off-cluster.
//! Driven by `mise run //crates/e2e:test` (isolated `hooks` repo subpath; the
//! httpRequest scenario reuses the WebDAV fixture as its in-cluster receiver).

#![cfg(all(unix, feature = "e2e"))]

mod common;

use common::{
    cr, ensure_repo, repository_json, snapshot_json, snapshot_policy_json, status_json,
    wait_condition, wait_phase,
};
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;

use kopiur_api::{Repository, Restore, Snapshot, SnapshotPolicy};
use kopiur_e2e::{
    E2E_NAMESPACE, Need, World, builders, consts, default_timeout, poll_interval, wait, wait_until,
};

const SUBPATH: &str = "hooks";
const REPO: &str = "e2e-hooks-repo";

/// Repository + the test's own DYNAMIC source PVC (never the shared `e2e-src` —
/// hook sentinels written there would contaminate other shards' content
/// assertions), seeded with one base file so the snapshot is never empty.
async fn ensure_hooks_world(client: &Client, src_pvc: &str) {
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
        .expect("hooks Repository should reach Ready");

    use kopiur_e2e::apply::{Fixture, apply_all};
    let fixtures: Vec<Fixture> = vec![builders::dynamic_pvc(E2E_NAMESPACE, src_pvc, "1Gi").into()];
    apply_all(client, &fixtures).await.expect("source PVC");
    let seeder = format!("{src_pvc}-seed");
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    if pods.get_opt(&seeder).await.ok().flatten().is_none() {
        let pod = builders::one_shot_pod(
            E2E_NAMESPACE,
            &seeder,
            &["sh", "-c", "echo base > /data/base.txt"],
            &[(src_pvc, "/data")],
        );
        let _ = pods.create(&PostParams::default(), &pod).await;
    }
    wait::pod_succeeded(client, E2E_NAMESPACE, &seeder)
        .await
        .expect("source seeder pod should succeed");
}

/// Restore `snapshot` into a fresh operator-created PVC and assert (via a reader
/// pod) that `sentinel` exists in the restored data — ordering proved by data.
async fn assert_sentinel_in_snapshot(client: &Client, snapshot: &str, sentinel: &str) {
    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = format!("{snapshot}-verify");
    let dst = format!("{snapshot}-dst");
    let restore = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Restore",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": REPO },
            "source": { "snapshotRef": { "name": snapshot } },
            "target": { "pvc": { "name": dst, "capacity": "1Gi" } }
        }
    });
    let _ = restores.create(&PostParams::default(), &cr(restore)).await;
    wait_phase(&restores, &name, "Completed")
        .await
        .expect("verification restore should complete");

    let reader = format!("{snapshot}-reader");
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    if pods.get_opt(&reader).await.ok().flatten().is_none() {
        let script = format!("test -f /restore/{sentinel} && cat /restore/{sentinel}");
        let pod = builders::one_shot_pod(
            E2E_NAMESPACE,
            &reader,
            &["sh", "-c", &script],
            &[(dst.as_str(), "/restore")],
        );
        let _ = pods.create(&PostParams::default(), &pod).await;
    }
    wait::pod_succeeded(client, E2E_NAMESPACE, &reader)
        .await
        .unwrap_or_else(|e| {
            panic!("sentinel `{sentinel}` must be IN the snapshot (the hook ran before it): {e}")
        });
    let _ = restores.delete(&name, &DeleteParams::default()).await;
    let _ = pods.delete(&reader, &DeleteParams::default()).await;
}

/// `runJob` pre-hook: the hook Job's sentinel is captured by the snapshot, and
/// the hook Job finished before the mover Job started.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn run_job_pre_hook_sentinel_is_captured_by_snapshot() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("fixtures ready");
    let client = world.client().clone();
    let src = "e2e-hooks-src-runjob";
    ensure_hooks_world(&client, src).await;

    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let cfg = snapshot_policy_json(
        E2E_NAMESPACE,
        "e2e-hooks-runjob-cfg",
        "Repository",
        REPO,
        serde_json::json!({
            "sources": [ { "pvc": { "name": src } } ],
            "hooks": { "beforeSnapshot": [ { "runJob": {
                "timeout": "2m",
                "jobSpec": {
                    "backoffLimit": 0,
                    "template": { "spec": {
                        "restartPolicy": "Never",
                        "containers": [{
                            "name": "quiesce",
                            "image": consts::BUSYBOX_IMAGE,
                            "command": ["sh", "-c", "echo hooked > /data/sentinel-runjob.txt"],
                            "volumeMounts": [{ "name": "data", "mountPath": "/data" }]
                        }],
                        "volumes": [{ "name": "data", "persistentVolumeClaim": { "claimName": src } }]
                    } }
                }
            } } ] }
        }),
    );
    let _ = configs.create(&PostParams::default(), &cr(cfg)).await;

    let backup = "e2e-hooks-runjob";
    let _ = backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                backup,
                "e2e-hooks-runjob-cfg",
                serde_json::json!({}),
            )),
        )
        .await;
    wait_phase(&backups, backup, "Succeeded")
        .await
        .expect("Snapshot with a runJob pre-hook should succeed");

    // The hook Job exists, succeeded, and finished BEFORE the mover Job started.
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let hook_job = jobs
        .get(&format!("{backup}-prehook-0"))
        .await
        .expect("the runJob pre-hook Job must exist");
    let hook_done = hook_job
        .status
        .as_ref()
        .and_then(|s| s.completion_time.as_ref())
        .map(|t| t.0)
        .expect("hook Job must have completed");
    let mover_job = jobs.get(backup).await.expect("mover Job exists");
    let mover_started = mover_job
        .status
        .as_ref()
        .and_then(|s| s.start_time.as_ref())
        .map(|t| t.0)
        .expect("mover Job must have started");
    assert!(
        hook_done <= mover_started,
        "the pre-hook Job must complete before the mover Job starts \
         (hook done {hook_done}, mover started {mover_started})"
    );

    // The exactly-once stamp landed.
    let s = status_json(&backups, backup).await;
    assert!(
        s.pointer("/hooks/preCompletedAt")
            .and_then(|v| v.as_str())
            .is_some_and(|t| !t.is_empty()),
        "status.hooks.preCompletedAt must be stamped; got {s}"
    );

    // Ordering proved by DATA: the sentinel is in the snapshot.
    assert_sentinel_in_snapshot(&client, backup, "sentinel-runjob.txt").await;
}

/// `workloadExec` pre-hook: the exec'd command's sentinel (written inside the
/// workload pod) is captured by the snapshot.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn workload_exec_pre_hook_runs_in_workload_pod() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("fixtures ready");
    let client = world.client().clone();
    let src = "e2e-hooks-src-exec";
    ensure_hooks_world(&client, src).await;

    // The exec target: a long-running labeled workload pod mounting the source.
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let wl = "e2e-hook-workload";
    if pods.get_opt(wl).await.ok().flatten().is_none() {
        let pod = builders::sleeper_pod(E2E_NAMESPACE, wl, &[("app", "e2e-hook-wl")], src, "/data");
        let _ = pods.create(&PostParams::default(), &pod).await;
    }
    wait_until(
        "workload pod Running",
        default_timeout(),
        poll_interval(),
        || async {
            let p = pods.get_opt(wl).await?;
            Ok(p.and_then(|p| p.status)
                .and_then(|s| s.phase)
                .filter(|ph| ph == "Running"))
        },
    )
    .await
    .expect("workload pod should be Running");

    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let cfg = snapshot_policy_json(
        E2E_NAMESPACE,
        "e2e-hooks-exec-cfg",
        "Repository",
        REPO,
        serde_json::json!({
            "sources": [ { "pvc": { "name": src } } ],
            "hooks": { "beforeSnapshot": [ { "workloadExec": {
                "podSelector": { "matchLabels": { "app": "e2e-hook-wl" } },
                "command": ["sh", "-c", "echo execd > /data/sentinel-exec.txt"],
                "timeout": "1m"
            } } ] }
        }),
    );
    let _ = configs.create(&PostParams::default(), &cr(cfg)).await;

    let backup = "e2e-hooks-exec";
    let _ = backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                backup,
                "e2e-hooks-exec-cfg",
                serde_json::json!({}),
            )),
        )
        .await;
    wait_phase(&backups, backup, "Succeeded")
        .await
        .expect("Snapshot with a workloadExec pre-hook should succeed");

    assert_sentinel_in_snapshot(&client, backup, "sentinel-exec.txt").await;
    let _ = pods.delete(wl, &DeleteParams::default()).await;
}

/// A failing pre-hook ABORTS the backup (Failed + `HooksSucceeded=False
/// reason=PreHookFailed`, no mover Job) — unless the hook sets
/// `continueOnFailure: true`, in which case the backup proceeds.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn failing_pre_hook_fails_snapshot_unless_continue_on_failure() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("fixtures ready");
    let client = world.client().clone();
    let src = "e2e-hooks-src-fail";
    ensure_hooks_world(&client, src).await;

    // The exec target (shared by both policies below).
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let wl = "e2e-hook-fail-workload";
    if pods.get_opt(wl).await.ok().flatten().is_none() {
        let pod = builders::sleeper_pod(
            E2E_NAMESPACE,
            wl,
            &[("app", "e2e-hook-fail-wl")],
            src,
            "/data",
        );
        let _ = pods.create(&PostParams::default(), &pod).await;
    }
    wait_until(
        "workload pod Running",
        default_timeout(),
        poll_interval(),
        || async {
            let p = pods.get_opt(wl).await?;
            Ok(p.and_then(|p| p.status)
                .and_then(|s| s.phase)
                .filter(|ph| ph == "Running"))
        },
    )
    .await
    .expect("workload pod should be Running");

    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let hook = |continue_on_failure: bool| {
        serde_json::json!({
            "sources": [ { "pvc": { "name": src } } ],
            "hooks": { "beforeSnapshot": [ { "workloadExec": {
                "podSelector": { "matchLabels": { "app": "e2e-hook-fail-wl" } },
                "command": ["sh", "-c", "exit 1"],
                "timeout": "1m",
                "continueOnFailure": continue_on_failure
            } } ] }
        })
    };

    // (a) abort-by-default: Failed + condition naming the hook, and NO mover Job.
    let cfg = snapshot_policy_json(
        E2E_NAMESPACE,
        "e2e-hooks-abort-cfg",
        "Repository",
        REPO,
        hook(false),
    );
    let _ = configs.create(&PostParams::default(), &cr(cfg)).await;
    let aborted = "e2e-hooks-abort";
    let _ = backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                aborted,
                "e2e-hooks-abort-cfg",
                serde_json::json!({}),
            )),
        )
        .await;
    wait_phase(&backups, aborted, "Failed")
        .await
        .expect("a failing pre-hook must abort the backup");
    wait_condition(&backups, aborted, "HooksSucceeded", "False")
        .await
        .expect("HooksSucceeded=False must be surfaced");
    let s = status_json(&backups, aborted).await;
    let msg = s
        .get("conditions")
        .and_then(|c| c.as_array())
        .and_then(|a| {
            a.iter()
                .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("HooksSucceeded"))
        })
        .and_then(|c| c.get("message").and_then(|m| m.as_str()))
        .unwrap_or("");
    assert!(
        msg.contains("beforeSnapshot hook #0") && msg.contains("continueOnFailure"),
        "the condition message must name the hook and both fixes; got: {msg}"
    );
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    assert!(
        jobs.get_opt(aborted).await.expect("query jobs").is_none(),
        "an aborted backup must never create its mover Job"
    );

    // (b) continueOnFailure: the same failing hook, but the backup proceeds.
    let cfg = snapshot_policy_json(
        E2E_NAMESPACE,
        "e2e-hooks-cont-cfg",
        "Repository",
        REPO,
        hook(true),
    );
    let _ = configs.create(&PostParams::default(), &cr(cfg)).await;
    let proceeded = "e2e-hooks-cont";
    let _ = backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                proceeded,
                "e2e-hooks-cont-cfg",
                serde_json::json!({}),
            )),
        )
        .await;
    wait_phase(&backups, proceeded, "Succeeded")
        .await
        .expect("continueOnFailure must let the backup proceed past the failing hook");

    let _ = pods.delete(wl, &DeleteParams::default()).await;
}

/// `httpRequest` post-hook: a PUT (with URL userinfo → Basic auth) lands on an
/// in-cluster receiver (the WebDAV fixture), verified by reading the resource back.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn http_request_post_hook_hits_in_cluster_receiver() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem, Need::WebDav])
        .await
        .expect("fixtures ready");
    let client = world.client().clone();
    let src = "e2e-hooks-src-http";
    ensure_hooks_world(&client, src).await;

    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let mark_url = format!(
        "http://{}:{}@webdav.{}.svc.cluster.local/e2e-hook-mark.txt",
        consts::WEBDAV_USER,
        consts::WEBDAV_PASSWORD,
        E2E_NAMESPACE,
    );
    let cfg = snapshot_policy_json(
        E2E_NAMESPACE,
        "e2e-hooks-http-cfg",
        "Repository",
        REPO,
        serde_json::json!({
            "sources": [ { "pvc": { "name": src } } ],
            "hooks": { "afterSnapshot": [ { "httpRequest": {
                "url": mark_url,
                "method": "PUT",
                "body": "fired",
                "timeout": "30s"
            } } ] }
        }),
    );
    let _ = configs.create(&PostParams::default(), &cr(cfg)).await;

    let backup = "e2e-hooks-http";
    let _ = backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                backup,
                "e2e-hooks-http-cfg",
                serde_json::json!({}),
            )),
        )
        .await;
    wait_phase(&backups, backup, "Succeeded")
        .await
        .expect("Snapshot with an httpRequest post-hook should succeed");
    // `phase: Succeeded` is the mover's terminal stamp; the post-hook runs in the
    // controller's FOLLOW-UP reconcile and stamps `hooks.postCompletedAt` a beat
    // later (debounced). Poll for the stamp rather than reading status once right
    // after `wait_phase` — that races the post-hook reconcile and sees no `hooks`
    // block at all. Regression: the controller debounce widened the gap until the
    // race was lost every run.
    wait_until(
        "snapshot stamps hooks.postCompletedAt after the post-hook reconcile",
        default_timeout(),
        poll_interval(),
        || {
            let backups = backups.clone();
            async move {
                let s = status_json(&backups, backup).await;
                Ok(s.pointer("/hooks/postCompletedAt")
                    .and_then(|v| v.as_str())
                    .is_some_and(|t| !t.is_empty())
                    .then_some(s))
            }
        },
    )
    .await
    .expect("status.hooks.postCompletedAt must be stamped after a Succeeded post-hook snapshot");

    // The receiver got the PUT: read the resource back (busybox wget honors
    // URL userinfo for Basic auth) and check the body.
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let verifier = "e2e-hooks-http-verify";
    if pods.get_opt(verifier).await.ok().flatten().is_none() {
        let script = format!("wget -q -O - {mark_url} | grep -q fired");
        let pod = builders::one_shot_pod(E2E_NAMESPACE, verifier, &["sh", "-c", &script], &[]);
        let _ = pods.create(&PostParams::default(), &pod).await;
    }
    wait::pod_succeeded(&client, E2E_NAMESPACE, verifier)
        .await
        .expect("the httpRequest post-hook's PUT must have landed on the receiver");
    let _ = pods.delete(verifier, &DeleteParams::default()).await;
}
