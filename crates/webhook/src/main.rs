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
    // Tracing subscriber (fmt + OTLP traces/logs when configured). Held for the
    // process lifetime so buffered OTLP data flushes on shutdown.
    let _telemetry = kopiur_telemetry::init_tracing("kopiur-webhook")?;

    // Install the ring crypto provider for rustls (required before building any
    // rustls ServerConfig). Ignore the error if a provider is already installed.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let addr: SocketAddr = std::env::var(kopiur_webhook::config::WEBHOOK_ADDR_ENV)
        .unwrap_or_else(|_| kopiur_webhook::config::DEFAULT_ADDR.to_string())
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

    let cert = std::env::var(kopiur_webhook::config::WEBHOOK_TLS_CERT_ENV).ok();
    let key = std::env::var(kopiur_webhook::config::WEBHOOK_TLS_KEY_ENV).ok();

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
    use axum_server::Handle;
    use axum_server::tls_rustls::RustlsConfig;

    let config = RustlsConfig::from_pem_file(cert_path, key_path)
        .await
        .map_err(|e| anyhow::anyhow!("failed to load TLS cert/key: {e}"))?;

    // Hot-reload the serving cert so an operator-rotated leaf (the
    // `webhook.tls.mode: self` path: the controller rewrites the mounted Secret,
    // which the kubelet syncs into these files) is served without a pod restart.
    // A reload failure (e.g. mid-write files) keeps the current config and is
    // retried next tick — never fatal.
    spawn_cert_reload(config.clone(), cert_path.to_string(), key_path.to_string());

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

/// Periodically re-read the cert/key PEM files into the live [`RustlsConfig`],
/// so a rotated serving leaf is picked up with zero downtime. The first tick
/// fires immediately and is skipped (the config was just loaded).
fn spawn_cert_reload(
    config: axum_server::tls_rustls::RustlsConfig,
    cert_path: String,
    key_path: String,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(kopiur_webhook::config::TLS_RELOAD_INTERVAL);
        ticker.tick().await; // consume the immediate first tick
        loop {
            ticker.tick().await;
            match config.reload_from_pem_file(&cert_path, &key_path).await {
                Ok(()) => tracing::debug!("reloaded webhook TLS cert from disk"),
                Err(e) => {
                    tracing::warn!(error = %e, "failed to reload webhook TLS cert; keeping current")
                }
            }
        }
    });
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
