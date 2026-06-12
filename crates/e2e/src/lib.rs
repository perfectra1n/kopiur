#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

use std::time::{Duration, Instant};

use kube::{Client, Error};

pub mod apply;
pub mod builders;
pub mod cli;
pub mod consts;
pub mod wait;
pub mod world;

pub use world::{Need, World};

/// The namespace the e2e harness installs the operator and runs scenarios in.
/// Back-compat alias for [`consts::OPERATOR_NS`].
pub const E2E_NAMESPACE: &str = consts::OPERATOR_NS;

/// Try to connect to a cluster, probing the API so a stale kubeconfig skips
/// rather than hangs. Returns `None` (printing a skip notice) when no cluster is
/// reachable, so an e2e test compiled with `--features e2e` still passes as a
/// no-op off-cluster.
pub async fn try_client() -> Option<Client> {
    match Client::try_default().await {
        Ok(c) => match c.apiserver_version().await {
            Ok(_) => Some(c),
            Err(e) => {
                eprintln!("SKIP: cluster unreachable ({e}); e2e test is a no-op");
                None
            }
        },
        Err(e) => {
            eprintln!("SKIP: no kube client ({e}); e2e test is a no-op");
            None
        }
    }
}

/// Poll `f` every `interval` until it returns `Ok(Some(value))`, giving up after
/// `timeout`. `f` returning `Ok(None)` means "not ready yet, keep waiting";
/// `Err` is a hard failure (e.g. the API server rejected the request). On
/// timeout returns an `anyhow` error tagged with `what` for a useful message.
pub async fn wait_until<T, F, Fut>(
    what: &str,
    timeout: Duration,
    interval: Duration,
    mut f: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>, Error>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        match f().await {
            Ok(Some(v)) => return Ok(v),
            Ok(None) => {}
            Err(e) => return Err(anyhow::anyhow!("{what}: API error while polling: {e}")),
        }
        if Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "{what}: condition not met within {:?}",
                timeout
            ));
        }
        tokio::time::sleep(interval).await;
    }
}

/// Sensible default poll budget for operator-driven state (Jobs scheduling,
/// images pulling, kopia running): generous enough for a cold kind node.
pub fn default_timeout() -> Duration {
    Duration::from_secs(180)
}

/// Default poll interval.
pub fn poll_interval() -> Duration {
    Duration::from_secs(3)
}

/// Scrape the controller's `/metrics` through the API server's Service-proxy
/// subresource (no port-forward / `ws` feature needed). The chart names the
/// controller metrics Service `kopiur-controller-metrics` on port 8080.
pub async fn scrape_controller_metrics(client: &Client) -> anyhow::Result<String> {
    let path = format!(
        "/api/v1/namespaces/{E2E_NAMESPACE}/services/kopiur-controller-metrics:8080/proxy/metrics"
    );
    let req = http::Request::get(path).body(Vec::new())?;
    Ok(client.request_text(req).await?)
}

/// Ensure a `Namespace` named `ns` exists (idempotent: a 409 Conflict is treated
/// as success). Used by the cross-namespace scenarios that run a workload + Snapshot
/// in a namespace separate from the operator's.
pub async fn ensure_namespace(client: &Client, ns: &str) -> anyhow::Result<()> {
    use k8s_openapi::api::core::v1::Namespace;
    use kube::api::PostParams;

    let api: kube::Api<Namespace> = kube::Api::all(client.clone());
    let obj: Namespace = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": ns },
    }))?;
    match api.create(&PostParams::default(), &obj).await {
        Ok(_) => Ok(()),
        // Already exists from a prior run on a reused cluster.
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Add (or update) a single annotation on a `Namespace` via a strategic-merge
/// patch. Used to flip the privileged-movers opt-in in the privileged-mover
/// scenario.
pub async fn annotate_namespace(
    client: &Client,
    ns: &str,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    use k8s_openapi::api::core::v1::Namespace;
    use kube::api::{Patch, PatchParams};

    let api: kube::Api<Namespace> = kube::Api::all(client.clone());
    let patch = serde_json::json!({ "metadata": { "annotations": { key: value } } });
    api.patch(ns, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    Ok(())
}

/// Server-side apply an Opaque `Secret` with the given string data into `ns`
/// (idempotent). Used to place a repository-credentials Secret into a workload
/// namespace so the mover Job's `envFrom` resolves there.
pub async fn apply_secret(
    client: &Client,
    ns: &str,
    name: &str,
    string_data: &[(&str, &str)],
) -> anyhow::Result<()> {
    use k8s_openapi::api::core::v1::Secret;
    use kube::api::{Patch, PatchParams};

    let data: serde_json::Map<String, serde_json::Value> = string_data
        .iter()
        .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
        .collect();
    let secret: Secret = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": ns },
        "type": "Opaque",
        "stringData": data,
    }))?;
    let api: kube::Api<Secret> = kube::Api::namespaced(client.clone(), ns);
    api.patch(
        name,
        &PatchParams::apply("kopiur-e2e").force(),
        &Patch::Apply(&secret),
    )
    .await?;
    Ok(())
}
