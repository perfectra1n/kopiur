//! End-to-end guard for operator observability against the Helm-deployed
//! operator in kind.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`, skipping gracefully without
//! a cluster (`scripts/with-e2e.sh`). Run with `mise run test-e2e`.
//!
//! The headline assertion is the regression guard for the silent-logs bug: the
//! controller, webhook, and mover used to emit **zero** bytes to stdout because
//! `init_tracing` attached an empty `Vec` of layers (which returns
//! `Interest::never()` and disables the whole subscriber) on the default no-OTLP
//! path. The unit test `kopiur_telemetry::tests::no_otlp_layer_stack_still_emits`
//! covers the layer assembly; this proves the *deployed, no-OTLP* operator
//! actually writes logs that `kubectl logs` can see.

#![cfg(all(unix, feature = "e2e"))]

use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::api::{ListParams, LogParams};

use kopiur_e2e::{E2E_NAMESPACE, try_client};

/// Fetch the stdout/stderr of the first Ready pod matching `component`, via the
/// same path `kubectl logs` uses (the Pod log subresource).
async fn component_logs(client: &kube::Client, component: &str) -> anyhow::Result<String> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let selector = format!("app.kubernetes.io/component={component}");
    let list = pods.list(&ListParams::default().labels(&selector)).await?;
    let pod = list
        .items
        .into_iter()
        .find(|p| p.metadata.deletion_timestamp.is_none())
        .ok_or_else(|| anyhow::anyhow!("no {component} pod found in {E2E_NAMESPACE}"))?;
    let name = pod
        .metadata
        .name
        .ok_or_else(|| anyhow::anyhow!("{component} pod has no name"))?;
    Ok(pods.logs(&name, &LogParams::default()).await?)
}

/// Regression guard: the deployed operator on the **default no-OTLP path** must
/// produce stdout logs. Before the empty-layer-`Vec` fix this returned an empty
/// string for every operator binary.
#[tokio::test]
#[ignore = "requires the e2e harness (scripts/with-e2e.sh): kind + built images + helm install"]
async fn operator_binaries_emit_logs() {
    let Some(client) = try_client().await else {
        return;
    };

    for component in ["controller", "webhook"] {
        let logs = component_logs(&client, component)
            .await
            .unwrap_or_else(|e| panic!("reading {component} logs: {e}"));
        assert!(
            !logs.trim().is_empty(),
            "{component} produced ZERO stdout — the tracing subscriber is silent \
             (empty-layer-Vec regression). `kubectl logs` would show nothing."
        );
    }
}
