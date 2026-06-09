//! Best-effort integration test for the admission webhook (feature `integration`).
//!
//! This is `#[ignore]` and gated behind the `integration` feature, per the project's
//! test discipline (CLAUDE.md, SKILL "Build & verify discipline"). It must NEVER
//! target a real cluster — it only attempts to build a `kube::Client` from the
//! ambient `KUBECONFIG` and, if that succeeds, starts the webhook server on an
//! ephemeral port and POSTs a realistic `AdmissionReview` at it, asserting the
//! response shape. If no cluster/kubeconfig is reachable it skips gracefully.
//!
//! Run via the kind harness:
//! ```sh
//! scripts/with-kind.sh cargo test -p kopiur-webhook --features integration -- --include-ignored
//! ```

#![cfg(feature = "integration")]

use kopiur_webhook::app;
use serde_json::{Value, json};
use std::time::Duration;

#[tokio::test]
#[ignore = "requires a cluster / ephemeral kind; run with --features integration --include-ignored"]
async fn webhook_serves_admission_over_ephemeral_port() {
    // Best-effort client; skip if no cluster.
    let client = match kube::Client::try_default().await {
        Ok(c) => Some(c),
        Err(e) => {
            eprintln!("skipping integration test: no kube client ({e})");
            return;
        }
    };

    let router = app(client);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    // Give the server a moment to come up.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let body = json!({
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "request": {
            "uid": "integ-uid",
            "kind": { "group": "kopiur.home-operations.com", "version": "v1alpha1", "kind": "SnapshotSchedule" },
            "resource": { "group": "kopiur.home-operations.com", "version": "v1alpha1", "resource": "backupschedules" },
            "name": "nightly",
            "namespace": "default",
            "operation": "CREATE",
            "userInfo": { "username": "integ" },
            "object": {
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "SnapshotSchedule",
                "metadata": { "name": "nightly", "namespace": "default" },
                "spec": { "policyRef": { "name": "c" }, "schedule": { "cron": "0 2 * * *" } }
            }
        }
    });

    let url = format!("http://{addr}/admission");
    let resp = reqwest_post(&url, &body).await;

    assert_eq!(resp["response"]["uid"], "integ-uid");
    assert_eq!(resp["response"]["allowed"], true);

    server.abort();
}

/// Minimal POST helper using hyper directly to avoid adding a reqwest dependency.
async fn reqwest_post(url: &str, body: &Value) -> Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let url = url.strip_prefix("http://").unwrap();
    let (authority, path) = url.split_once('/').unwrap();
    let path = format!("/{path}");

    let mut stream = tokio::net::TcpStream::connect(authority).await.unwrap();
    let payload = serde_json::to_vec(body).unwrap();
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        payload.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.write_all(&payload).await.unwrap();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw);
    let body_start = text.find("\r\n\r\n").unwrap() + 4;
    serde_json::from_str(&text[body_start..]).unwrap()
}
