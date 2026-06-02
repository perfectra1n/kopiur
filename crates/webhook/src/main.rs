//! kopiur admission webhook server (ADR-0003 §5.3).
//!
//! Serves the admission router over HTTPS when TLS cert/key paths are provided via
//! env, otherwise over plain HTTP (for in-cluster use behind a service mesh, or for
//! local testing) with a loud warning. TLS is entirely runtime/env-gated, so the
//! binary builds and starts without any cert files present.
//!
//! Environment:
//! - `KOPIUR_WEBHOOK_ADDR`     bind address (default `0.0.0.0:8443`).
//! - `KOPIUR_WEBHOOK_TLS_CERT` PEM cert chain path (enables TLS when set with key).
//! - `KOPIUR_WEBHOOK_TLS_KEY`  PEM private key path.
//! - `RUST_LOG`                tracing filter (default `info`).

use kopiur_webhook::app;
use std::net::SocketAddr;
use tokio::signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    // Install the ring crypto provider for rustls (required before building any
    // rustls ServerConfig). Ignore the error if a provider is already installed.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let addr: SocketAddr = std::env::var("KOPIUR_WEBHOOK_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8443".to_string())
        .parse()?;

    // Best-effort kube client. If unavailable (no kubeconfig / not in-cluster), the
    // webhook still serves; ClusterRepository tenancy checks then fail closed.
    let client = match kube::Client::try_default().await {
        Ok(c) => {
            tracing::info!("connected kube client for ClusterRepository tenancy resolution");
            Some(c)
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "no Kubernetes client available; ClusterRepository tenancy checks will fail closed"
            );
            None
        }
    };

    let router = app(client);

    let cert = std::env::var("KOPIUR_WEBHOOK_TLS_CERT").ok();
    let key = std::env::var("KOPIUR_WEBHOOK_TLS_KEY").ok();

    match (cert, key) {
        (Some(cert), Some(key)) => {
            tracing::info!(%addr, cert, key, "serving admission webhook over HTTPS");
            serve_tls(addr, router, &cert, &key).await
        }
        _ => {
            tracing::warn!(
                %addr,
                "KOPIUR_WEBHOOK_TLS_CERT/KEY not set: serving admission webhook over PLAIN HTTP. \
                 This is only safe behind a TLS-terminating mesh or for local testing — the \
                 Kubernetes API server requires HTTPS for real webhook registration."
            );
            serve_http(addr, router).await
        }
    }
}

fn init_tracing() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .init();
}

async fn serve_http(addr: SocketAddr, router: axum::Router) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "listening (http)");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn serve_tls(
    addr: SocketAddr,
    router: axum::Router,
    cert_path: &str,
    key_path: &str,
) -> anyhow::Result<()> {
    use axum_server::tls_rustls::RustlsConfig;
    use axum_server::Handle;

    let config = RustlsConfig::from_pem_file(cert_path, key_path)
        .await
        .map_err(|e| anyhow::anyhow!("failed to load TLS cert/key: {e}"))?;

    let handle = Handle::new();
    // Trigger graceful shutdown of axum-server when a signal arrives.
    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown signal received; draining connections");
        shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
    });

    tracing::info!(%addr, "listening (https)");
    axum_server::bind_rustls(addr, config)
        .handle(handle)
        .serve(router.into_make_service())
        .await?;
    Ok(())
}

/// Resolve on SIGTERM (Kubernetes pod termination) or Ctrl-C.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.ok();
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
