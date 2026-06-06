//! Regression: a non-retryable filesystem failure must **hard-stop**, not spam.
//!
//! Reproduces the reported bug — a `Repository` whose filesystem path is not
//! writable by the operator's UID logged the same `repository connect` /
//! `PermissionDenied` error several times per second, forever, because each failed
//! reconcile re-wrote a `Failed` status whose condition message embedded kopia's
//! volatile temp filename (`.shards.tmp.<random>`), which re-triggered the
//! controller in a tight loop.
//!
//! This test points a `Repository` at `/ro-repo` — a node hostPath the harness
//! creates root-owned and `0555`, so the controller (uid 65532) gets EACCES when
//! it tries to create the kopia repo there. It asserts:
//!   1. the Repository settles in `phase: Failed` with `Bootstrapped=False`,
//!      `reason=PermissionDenied`;
//!   2. the condition **message is the stable, class-derived summary** (no
//!      `.shards`/`.tmp` volatile suffix) — the load-bearing fix;
//!   3. `status.observedGeneration == metadata.generation` (the terminal gate key);
//!   4. **no churn**: `metadata.resourceVersion` is stable over a window that the
//!      old hot-loop would have bumped dozens of times;
//!   5. editing the spec (bumping `generation`) **reopens the gate** — the
//!      controller re-attempts and advances `observedGeneration` to the new value.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; skips gracefully off-cluster.

#![cfg(all(unix, feature = "e2e"))]

use std::time::Duration;

use kube::api::{Patch, PatchParams, PostParams};
use kube::{Api, Resource};
use serde::de::DeserializeOwned;

use kopiur_api::Repository;
use kopiur_e2e::{E2E_NAMESPACE, Need, World, default_timeout, poll_interval, wait_until};

const CREDS_SECRET: &str = "kopia-creds";

fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// A Repository whose filesystem path is the harness's non-writable `/ro-repo`.
fn unwritable_repository_json(name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            // No pvcName: the controller reaches /ro-repo via its own mounted
            // hostPath (in-process filesystem ops). create.enabled so it attempts
            // to initialize — which is the write that fails with EACCES.
            "backend": { "filesystem": { "path": "/ro-repo" } },
            "encryption": {
                "passwordSecretRef": { "name": CREDS_SECRET, "key": "KOPIA_PASSWORD" }
            },
            "create": { "enabled": true }
        }
    })
}

/// Read a CR's `.status` as JSON (empty object when absent).
async fn status_json<K>(api: &Api<K>, name: &str) -> serde_json::Value
where
    K: kube::Resource + Clone + std::fmt::Debug + DeserializeOwned + serde::Serialize,
    K::DynamicType: Default,
{
    let obj = api.get(name).await.expect("get CR");
    serde_json::to_value(&obj)
        .ok()
        .and_then(|v| v.get("status").cloned())
        .unwrap_or_else(|| serde_json::json!({}))
}

/// A status condition field (`status`/`reason`/`message`) by condition `type`.
fn condition_field(status: &serde_json::Value, type_: &str, field: &str) -> Option<String> {
    status
        .get("conditions")
        .and_then(|c| c.as_array())?
        .iter()
        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some(type_))
        .and_then(|c| c.get(field).and_then(|s| s.as_str()))
        .map(str::to_string)
}

/// Poll until the Repository reports the wanted phase.
async fn wait_phase(api: &Api<Repository>, name: &str, want: &str) -> anyhow::Result<()> {
    wait_until(
        &format!("Repository {name} phase={want}"),
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(api, name).await;
            let got = s.get("phase").and_then(|p| p.as_str());
            Ok((got == Some(want)).then_some(()))
        },
    )
    .await
}

fn resource_version(repo: &Repository) -> String {
    repo.meta().resource_version.clone().unwrap_or_default()
}

#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn filesystem_permission_denied_hard_stops_without_spam() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("provision filesystem fixtures");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = "e2e-ro-repo";

    repos
        .create(
            &PostParams::default(),
            &cr(unwritable_repository_json(name)),
        )
        .await
        .expect("create unwritable Repository");

    // 1. Settles Failed (terminal, non-retryable PermissionDenied).
    wait_phase(&repos, name, "Failed")
        .await
        .expect("unwritable filesystem Repository must end Failed");

    let status = status_json(&repos, name).await;
    assert_eq!(
        condition_field(&status, "Bootstrapped", "status").as_deref(),
        Some("False"),
        "must carry Bootstrapped=False, got {status}"
    );
    assert_eq!(
        condition_field(&status, "Bootstrapped", "reason").as_deref(),
        Some("PermissionDenied"),
        "must classify as PermissionDenied, got {status}"
    );

    // 2. The condition message is the STABLE, class-derived summary — the
    //    volatile kopia temp filename (`.shards.tmp.<hex>`) must NOT leak into it
    //    (that volatility was the hot-loop's fuel; it now goes to the Event only).
    let msg = condition_field(&status, "Bootstrapped", "message").unwrap_or_default();
    assert!(
        msg.contains("not writable"),
        "message must be the actionable class summary, got {msg:?}"
    );
    assert!(
        !msg.contains(".shards") && !msg.contains(".tmp"),
        "message leaked a volatile temp path (the hot-loop bug): {msg:?}"
    );

    // 3. The terminal gate key: observedGeneration pinned to the current spec.
    let cur_gen = repos
        .get(name)
        .await
        .expect("get repo")
        .meta()
        .generation
        .expect("generation");
    assert_eq!(
        status.get("observedGeneration").and_then(|g| g.as_i64()),
        Some(cur_gen),
        "Failed status must pin observedGeneration to the current generation"
    );

    // 4. NO CHURN: resourceVersion must be stable. The old hot-loop re-wrote
    //    status several times per second; over a multi-second window the
    //    resourceVersion would jump many times. Assert it does not move.
    let rv_before = resource_version(&repos.get(name).await.expect("get repo"));
    tokio::time::sleep(Duration::from_secs(12)).await;
    let rv_after = resource_version(&repos.get(name).await.expect("get repo"));
    assert_eq!(
        rv_before, rv_after,
        "terminally-Failed Repository churned its status (hot-loop regression): \
         resourceVersion moved {rv_before} -> {rv_after}"
    );

    // 5. A spec edit bumps generation -> reopens the gate -> the controller
    //    re-attempts and advances observedGeneration to the new value (it will
    //    fail again, since /ro-repo is still read-only, but it MUST re-reconcile).
    let patch = serde_json::json!({ "spec": { "maintenance": { "enabled": false } } });
    repos
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .expect("patch spec to bump generation");
    let new_gen = repos
        .get(name)
        .await
        .expect("get repo")
        .meta()
        .generation
        .expect("generation");
    assert!(new_gen > cur_gen, "spec patch must bump generation");

    wait_until(
        "Repository re-reconciles after the spec change (gate reopened)",
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(&repos, name).await;
            let observed = s.get("observedGeneration").and_then(|g| g.as_i64());
            Ok((observed == Some(new_gen)).then_some(()))
        },
    )
    .await
    .expect("the gate must reopen on a spec change and re-pin observedGeneration");
}
