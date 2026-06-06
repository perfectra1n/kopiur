//! End-to-end guard for operator observability against the Helm-deployed
//! operator in kind.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`, skipping gracefully without
//! a cluster (`mise run //crates/e2e:test`). Run with `mise run //crates/e2e:test`.
//!
//! The headline assertion is the regression guard for the silent-logs bug: the
//! controller and the mover Jobs used to emit **zero** bytes to stdout because
//! `init_tracing` attached an empty `Vec` of layers (which returns
//! `Interest::never()` and disables the whole subscriber) on the default no-OTLP
//! path. The unit test `kopiur_telemetry::tests::no_otlp_layer_stack_still_emits`
//! covers the layer assembly; this proves the *deployed, no-OTLP* operator
//! actually writes logs that `kubectl logs` can see — for both the long-running
//! controller and a short-lived mover Job.

#![cfg(all(unix, feature = "e2e"))]

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{ListParams, LogParams};

use kopiur_e2e::{E2E_NAMESPACE, World, default_timeout, poll_interval, wait_until};

/// Read the logs of the first non-terminating pod matching a label `selector`.
/// Returns `Ok(None)` when no such pod exists yet (so callers can poll). The
/// error type is `kube::Error` so this composes directly inside `wait_until`.
async fn pod_logs_for(
    client: &kube::Client,
    selector: &str,
) -> Result<Option<String>, kube::Error> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let list = pods.list(&ListParams::default().labels(selector)).await?;
    let Some(name) = list
        .items
        .into_iter()
        .filter(|p| p.metadata.deletion_timestamp.is_none())
        .find_map(|p| p.metadata.name)
    else {
        return Ok(None);
    };
    Ok(Some(pods.logs(&name, &LogParams::default()).await?))
}

/// Regression guard: the deployed operator on the **default no-OTLP path** must
/// produce stdout logs. Before the empty-layer-`Vec` fix this returned an empty
/// string for every operator binary.
///
/// The webhook is disabled in the harness, so we assert on the two binaries that
/// actually run: the long-running controller, and a short-lived **mover** Job
/// pod (a backup mover) — the mover/bootstrap silence was the worst case, since
/// its only other output was a result ConfigMap.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn operator_binaries_emit_logs() {
    let Some(world) = World::connect().await else {
        return;
    };
    let client = world.client().clone();

    // Controller: present from install, logs continuously.
    let controller = wait_until(
        "controller pod has logs",
        default_timeout(),
        poll_interval(),
        || async {
            Ok(
                pod_logs_for(&client, "app.kubernetes.io/component=controller")
                    .await?
                    .filter(|l| !l.trim().is_empty()),
            )
        },
    )
    .await
    .expect("controller should produce stdout logs (empty-layer-Vec regression)");
    assert!(
        !controller.trim().is_empty(),
        "controller produced ZERO stdout — the tracing subscriber is silent"
    );

    // Mover: a backup Job's pod carries the per-Backup mover label. Any mover pod
    // (from the lifecycle scenarios) carries the origin label, proving the mover
    // binary logs to stdout too. Best-effort: only assert when one exists, so
    // this test does not depend on run ordering or Job GC — but if a mover pod IS
    // present it MUST have logged.
    if let Some(mover) = pod_logs_for(&client, "kopiur.home-operations.com/origin")
        .await
        .ok()
        .flatten()
    {
        assert!(
            !mover.trim().is_empty(),
            "a mover Job pod produced ZERO stdout — the mover tracing subscriber is silent"
        );
    }
}
