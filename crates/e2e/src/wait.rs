//! Condition-based waits over `kube::runtime::wait`, each bounded by
//! [`crate::default_timeout`]. These replace the shell `kubectl rollout status` /
//! `kubectl wait` the old harness used. The CR-phase polling the scenarios do is
//! still served by [`crate::wait_until`]/`wait_phase` (unchanged).

use anyhow::{Context, Result, anyhow};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Pod;
use kube::api::LogParams;
use kube::runtime::wait::{Condition, await_condition, conditions};
use kube::{Api, Client};

use crate::default_timeout;

/// Wait until a `Deployment` has completed its rollout (the `kubectl rollout
/// status` equivalent), failing with a tagged error on timeout.
pub async fn deployment_ready(client: &Client, ns: &str, name: &str) -> Result<()> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), ns);
    let cond = await_condition(api, name, conditions::is_deployment_completed());
    match tokio::time::timeout(default_timeout(), cond).await {
        Ok(res) => {
            res.with_context(|| format!("watching deployment {ns}/{name}"))?;
            Ok(())
        }
        Err(_) => Err(anyhow!(
            "deployment {ns}/{name} did not become ready within {:?}",
            default_timeout()
        )),
    }
}

/// True once a Pod reaches a terminal phase (`Succeeded` or `Failed`).
fn is_pod_terminal() -> impl Condition<Pod> {
    |obj: Option<&Pod>| {
        matches!(
            obj.and_then(|p| p.status.as_ref())
                .and_then(|s| s.phase.as_deref()),
            Some("Succeeded") | Some("Failed")
        )
    }
}

/// Wait for a one-shot Pod to finish and assert it `Succeeded`. On `Failed` (or
/// timeout) the error carries the Pod's logs so a CI failure is debuggable.
pub async fn pod_succeeded(client: &Client, ns: &str, name: &str) -> Result<()> {
    let api: Api<Pod> = Api::namespaced(client.clone(), ns);
    let cond = await_condition(api.clone(), name, is_pod_terminal());
    if tokio::time::timeout(default_timeout(), cond).await.is_err() {
        let logs = pod_logs(client, ns, name)
            .await
            .unwrap_or_else(|e| format!("<logs unavailable: {e}>"));
        return Err(anyhow!(
            "pod {ns}/{name} did not finish within {:?}; logs:\n{logs}",
            default_timeout()
        ));
    }
    let pod = api
        .get(name)
        .await
        .with_context(|| format!("get pod {ns}/{name}"))?;
    let phase = pod
        .status
        .and_then(|s| s.phase)
        .unwrap_or_else(|| "<unknown>".to_string());
    if phase == "Succeeded" {
        return Ok(());
    }
    let logs = pod_logs(client, ns, name)
        .await
        .unwrap_or_else(|e| format!("<logs unavailable: {e}>"));
    Err(anyhow!("pod {ns}/{name} ended {phase}; logs:\n{logs}"))
}

/// Fetch a Pod's logs (best-effort context for failure messages).
pub async fn pod_logs(client: &Client, ns: &str, name: &str) -> Result<String> {
    let api: Api<Pod> = Api::namespaced(client.clone(), ns);
    api.logs(name, &LogParams::default())
        .await
        .with_context(|| format!("fetch logs for pod {ns}/{name}"))
}
