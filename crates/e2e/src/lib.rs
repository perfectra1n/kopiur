#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

use std::time::{Duration, Instant};

use kube::{Client, Error};

/// The namespace the e2e harness installs the operator and runs scenarios in.
/// Matches `scripts/with-e2e.sh`.
pub const E2E_NAMESPACE: &str = "kopiur-e2e";

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
