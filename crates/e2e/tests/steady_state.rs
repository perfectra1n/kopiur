//! e2e regression guard for the **status-churn reconcile hot-loop**: a Ready
//! repository's steady state must be a true no-op — zero status writes, zero
//! `resourceVersion` churn, near-zero reconciles.
//!
//! Found live: `upsert_condition` rebuilt the conditions array as
//! filter-out-then-append, moving the touched condition to the END on every
//! call. A Ready repository whose finished bootstrap Job still exists runs
//! `finalize_bootstrap` on every pass and writes status twice — upserting
//! `Bootstrapped`, then `MaintenanceConfigured` — so the array order flipped on
//! every reconcile. Each flip was a real write (`resourceVersion` bump → watch
//! event on the primary → immediate re-queue): the repository controllers
//! hot-looped at ~30 reconciles/s per object, fanning out through the
//! `repository_to_*` watch mappers (SnapshotPolicy rode along at ~230/s, each
//! pass LISTing Snapshots) — a pegged CPU core and a hammered apiserver, for as
//! long as the finished bootstrap Job existed.
//!
//! This guard catches the whole CLASS, not just the ordering bug: ANY
//! steady-state status write that differs between identical passes (volatile
//! timestamps, reordered arrays, alternating writers) fails it.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; driven by
//! `mise run //crates/e2e:test` (single-threaded: `--test-threads=1`, so the
//! quiet window is not raced by other scenarios).

#![cfg(all(unix, feature = "e2e"))]

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use kube::Api;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use serde::de::DeserializeOwned;

use common::{CREDS_SECRET, cr, wait_condition, wait_phase};
use kopiur_api::Repository;
use kopiur_e2e::{
    E2E_NAMESPACE, Need, World, default_timeout, poll_interval, scrape_controller_metrics,
    wait_until,
};

/// How long the repository must stay byte-stable. On the buggy code the
/// self-trigger loop churned `resourceVersion` ~30×/s, so even a short window
/// is unambiguous; 45s also comfortably exceeds the controller's debounce
/// window and any transient post-transition writes.
const QUIET_WINDOW: Duration = Duration::from_secs(45);

/// Upper bound on `kopiur_controller_reconciliations_total{kind="Repository"}`
/// growth across [`QUIET_WINDOW`]. Fixed code: the poked reconcile plus at most
/// a few requeue ticks from leftover scenario CRs (steady requeues are ≥300s).
/// Buggy code: ~30/s × 45s ≈ 1350 — three orders of magnitude over the bound.
const MAX_RECONCILES_IN_WINDOW: f64 = 60.0;

/// Read a CR's `metadata.resourceVersion`.
async fn resource_version<K>(api: &Api<K>, name: &str) -> String
where
    K: kube::Resource + Clone + DeserializeOwned + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    api.get(name)
        .await
        .expect("get CR for resourceVersion")
        .meta()
        .resource_version
        .clone()
        .expect("server objects always carry a resourceVersion")
}

/// Wait until the CR's `resourceVersion` holds still across two reads 5s apart,
/// returning the settled version. On the buggy code the RV never stops moving,
/// so this times out with a diagnosable message instead of flaking later.
async fn wait_rv_settled(api: &Api<Repository>, name: &str) -> String {
    wait_until(
        &format!("{name} resourceVersion settles (no status churn)"),
        default_timeout(),
        poll_interval(),
        || async {
            let before = resource_version(api, name).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
            let after = resource_version(api, name).await;
            Ok((before == after).then_some(after))
        },
    )
    .await
    .expect(
        "repository resourceVersion must settle once Ready — continuous churn means a \
         reconcile is re-writing status with content that differs every pass (the \
         status-churn hot-loop)",
    )
}

/// Parse `kopiur_controller_reconciliations_total` per `kind` out of the
/// Prometheus exposition text.
fn reconciliations_by_kind(metrics: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for line in metrics.lines() {
        let Some(rest) = line.strip_prefix("kopiur_controller_reconciliations_total{") else {
            continue;
        };
        let Some((labels, value)) = rest.rsplit_once("} ") else {
            continue;
        };
        let Some(kind) = labels.split(',').find_map(|l| {
            l.trim()
                .strip_prefix("kind=\"")
                .and_then(|v| v.strip_suffix('"'))
        }) else {
            continue;
        };
        let Ok(v) = value.trim().parse::<f64>() else {
            continue;
        };
        *out.entry(kind.to_string()).or_insert(0.0) += v;
    }
    out
}

/// A Ready repository (managed maintenance off, so the scenario is fully
/// self-contained — the `MaintenanceConfigured=False` condition is still
/// upserted every pass, which is all the loop needed) must hold byte-stable
/// status and a near-zero reconcile rate while its finished bootstrap Job
/// exists, including after a watch-event poke.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn ready_repository_steady_state_is_quiet() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("provision filesystem fixtures");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let name = "e2e-steady-state";
    let repo = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            // Volume-backed filesystem repo → bootstraps via a mover Job, and the
            // FINISHED Job persists (default TTL far exceeds the test), so every
            // reconcile takes the `finalize_bootstrap` path — the exact shape that
            // hot-looped on the buggy code.
            "backend": { "filesystem": { "path": "/repo", "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
            "encryption": { "passwordSecretRef": { "name": CREDS_SECRET, "key": "KOPIA_PASSWORD" } },
            "create": { "enabled": true },
            "maintenance": { "enabled": false }
        }
    });
    // Reused cluster: clear a leftover CR of the same name first.
    if repos.get_opt(name).await.expect("query leftover").is_some() {
        let _ = repos.delete(name, &DeleteParams::default()).await;
        wait_until(
            &format!("leftover {name} is gone"),
            default_timeout(),
            poll_interval(),
            || async { Ok(repos.get_opt(name).await?.is_none().then_some(())) },
        )
        .await
        .expect("leftover Repository should delete");
    }
    repos
        .create(&PostParams::default(), &cr(repo))
        .await
        .expect("create Repository");

    wait_phase(&repos, name, "Ready")
        .await
        .expect("repository should reach Ready");
    // Both conditions of the flip-flop pair must exist before the stability
    // checks — otherwise the first upsert of a missing condition is a
    // legitimate write inside the window.
    wait_condition(&repos, name, "Bootstrapped", "True")
        .await
        .expect("Bootstrapped=True");
    wait_condition(&repos, name, "MaintenanceConfigured", "False")
        .await
        .expect("MaintenanceConfigured=False (maintenance.enabled: false)");

    // 1. The RV must settle at all (the buggy loop never lets it).
    wait_rv_settled(&repos, name).await;

    // 2. A watch-event poke (annotation: no generation bump, like any Secret/
    //    ConfigMap/Job event mapping to this repo) must produce ONE reconcile
    //    whose status writes are no-ops — not restart the loop.
    let pre_poke = reconciliations_by_kind(
        &scrape_controller_metrics(&client)
            .await
            .expect("scrape /metrics"),
    );
    let poke = serde_json::json!({
        "metadata": { "annotations": { "kopiur-e2e/steady-state-poke": "1" } }
    });
    repos
        .patch(name, &PatchParams::default(), &Patch::Merge(&poke))
        .await
        .expect("poke Repository with an annotation");
    let settled_rv = wait_rv_settled(&repos, name).await;

    // 3. Quiet window: byte-stable RV and a bounded reconcile count. The
    //    counter baseline is taken AFTER the post-poke settle so the bounded
    //    window is exactly QUIET_WINDOW — `wait_rv_settled` has its own (long)
    //    timeout, and folding it into the measured window would let leftover
    //    scenario CRs' requeue ticks erode the bound's margin as the suite grows.
    let baseline = reconciliations_by_kind(
        &scrape_controller_metrics(&client)
            .await
            .expect("scrape /metrics"),
    );
    tokio::time::sleep(QUIET_WINDOW).await;

    let final_rv = resource_version(&repos, name).await;
    assert_eq!(
        final_rv, settled_rv,
        "steady-state hot-loop regression: the Ready repository's resourceVersion moved \
         during a quiet window with no spec change — a reconcile is writing status \
         content that differs between identical passes"
    );

    let after = reconciliations_by_kind(
        &scrape_controller_metrics(&client)
            .await
            .expect("scrape /metrics"),
    );
    let poke_delta = after.get("Repository").copied().unwrap_or(0.0)
        - pre_poke.get("Repository").copied().unwrap_or(0.0);
    assert!(
        poke_delta >= 1.0,
        "the poke must trigger at least one observable reconcile (got delta {poke_delta}); \
         if this fires, the metric name/labels changed and this guard went blind"
    );
    let delta = after.get("Repository").copied().unwrap_or(0.0)
        - baseline.get("Repository").copied().unwrap_or(0.0);
    assert!(
        delta < MAX_RECONCILES_IN_WINDOW,
        "steady-state hot-loop regression: {delta} Repository reconciles in \
         {QUIET_WINDOW:?} (expected at most a few requeue ticks, < \
         {MAX_RECONCILES_IN_WINDOW}) — the controller is re-triggering itself"
    );

    // Clean up: this guard is itself sensitive to leftover-CR background noise,
    // so don't become that noise for later scenarios.
    let _ = repos.delete(name, &DeleteParams::default()).await;
}
