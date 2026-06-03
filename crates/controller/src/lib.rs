//! # kopiur-controller
//!
//! The kube-rs operator for Kopiur (ADR §5.2): one
//! [`kube::runtime::Controller`] per top-level CRD, each with its owned-resource
//! watches and a kind-aware `error_policy`. Long kopia operations run in mover
//! `Job`s; the controller only makes short, idempotent kopia calls (ADR §5.4).
//!
//! ## Module map
//! - [`context`] — shared [`context::Context`] (client, kopia factory, metrics,
//!   recorder).
//! - [`error`] — [`error::Error`] + the transient/structural `error_policy`.
//! - [`metrics`] — OTel instruments (Prometheus pull + optional OTLP push, via
//!   `kopiur-telemetry`) for the ADR §4.13 metrics.
//! - [`config`] — the controller's env var names + fixed config values.
//! - [`jobs`] — pure mover `Job`/`ConfigMap` builder (§4.10/§4.11).
//! - one module per reconciler: [`repository`], [`cluster_repository`],
//!   [`backup_config`], [`backup_schedule`], [`backup`], [`restore`],
//!   [`maintenance`].
//!
//! The reconcilers keep their **decision logic pure** (e.g.
//! [`backup::plan_deletion`], [`backup_schedule::next_fire`],
//! [`backup_config::backups_to_delete`]) so the type-safety thesis is unit-tested
//! without a cluster; the kube IO is thin and exercised by the feature-gated
//! integration tests.

pub mod backup;
pub mod backup_config;
pub mod backup_schedule;
pub mod cluster_repository;
pub mod config;
pub mod consts;
pub mod context;
pub mod error;
pub mod io;
pub mod jobs;
pub mod maintenance;
pub mod metrics;
pub mod repository;
pub mod restore;

use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::runtime::Controller;
use kube::runtime::events::{Recorder, Reporter};
use kube::runtime::watcher::Config as WatcherConfig;
use kube::{Api, Client};

use kopiur_api::{
    Backup, BackupConfig, BackupSchedule, ClusterRepository, Maintenance, Repository, Restore,
};

use crate::context::{Context, KopiaClientFactory};
use crate::metrics::Metrics;

/// Build the controller manager and run every controller concurrently, plus the
/// `/metrics` server, until shutdown.
///
/// Each `Controller` wires its owned-resource watches per ADR §5.2:
/// - `BackupSchedule` owns `Backup`.
/// - `BackupConfig` watches `Backup` (GFS retention).
/// - `Repository`/`ClusterRepository` watch discovered `Backup`.
/// - `Backup` owns `Job` + `ConfigMap` (mover run).
/// - `Restore` watches the target `PVC` (populator handshake).
pub async fn run() -> anyhow::Result<()> {
    // Install the tracing subscriber (fmt + OTLP traces/logs when configured).
    // Held for the process lifetime so buffered OTLP spans/logs flush on exit.
    // Errors only surface under KOPIUR_OTEL_STRICT; otherwise OTLP degrades to
    // fmt-only and the call succeeds.
    let _telemetry = kopiur_telemetry::init_tracing("kopiur-controller")?;

    // Install the process-level rustls CryptoProvider before the kube client
    // builds any TLS config; without this, kube's rustls-tls backend panics with
    // "no process-level CryptoProvider available". Idempotent: ignore the error
    // if a provider is already installed (e.g. the webhook installed it).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let client = Client::try_default().await?;
    let metrics = Metrics::new();
    let reporter = Reporter::from("kopiur-controller");
    let recorder = Recorder::new(client.clone(), reporter);
    // The mover image is configurable via KOPIUR_MOVER_IMAGE so a deployment (or
    // the e2e harness) can pin a locally-loaded image instead of the published
    // default (jobs::DEFAULT_MOVER_IMAGE).
    let mover_image = std::env::var(config::MOVER_IMAGE_ENV)
        .unwrap_or_else(|_| jobs::DEFAULT_MOVER_IMAGE.to_string());
    tracing::info!(mover_image = %mover_image, "mover image configured");
    // The mover PATCHes the owning CR's status, so its Job pods must run as an SA
    // bound to the operator's status-patch RBAC (not the namespace `default` SA).
    // The chart sets this to the operator ServiceAccount.
    let mover_service_account = std::env::var(config::MOVER_SERVICE_ACCOUNT_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    tracing::info!(mover_service_account = ?mover_service_account, "mover SA configured");
    // OTLP config the controller passes through to mover Jobs so their
    // traces/logs/metrics reach the same collector. Empty when OTLP is off.
    let mover_otlp_env = collect_mover_otlp_env();
    let ctx = Arc::new(Context::new(
        client.clone(),
        KopiaClientFactory::new(),
        metrics.clone(),
        recorder,
        mover_image,
        mover_service_account,
        mover_otlp_env,
    ));

    tracing::info!("starting kopiur controllers");

    let http_srv = tokio::spawn(serve_http(metrics.clone()));
    let controllers = spawn_all(client, ctx);

    tokio::select! {
        _ = controllers => tracing::warn!("all controllers exited"),
        r = http_srv => tracing::warn!(?r, "http server exited"),
    }
    Ok(())
}

/// Collect the standard `OTEL_EXPORTER_OTLP_*` env vars that are set in the
/// controller's environment so they can be stamped onto mover `Job`s. Returns
/// empty when OTLP is not configured (no endpoint), so movers stay fmt-only.
fn collect_mover_otlp_env() -> Vec<(String, String)> {
    if std::env::var(config::OTEL_EXPORTER_OTLP_ENDPOINT).is_err() {
        return Vec::new();
    }
    config::OTLP_PASSTHROUGH
        .iter()
        .filter_map(|k| std::env::var(k).ok().map(|v| (k.to_string(), v)))
        .collect()
}

/// Spawn all seven controllers and join them. Split out so it can be driven
/// independently of the metrics server.
async fn spawn_all(client: Client, ctx: Arc<Context>) {
    let cfg = WatcherConfig::default();

    macro_rules! controller {
        ($ty:ty, $module:ident) => {{
            let api: Api<$ty> = Api::all(client.clone());
            let ctx = ctx.clone();
            Controller::new(api, cfg.clone())
                .run($module::reconcile, $module::error_policy, ctx)
                .for_each(|res| async move {
                    if let Err(e) = res {
                        tracing::debug!(error = %e, "reconcile loop item error");
                    }
                })
        }};
    }

    // Backup owns its mover Job + ConfigMap (reaped via owner-ref GC, §4.10).
    let backup_api: Api<Backup> = Api::all(client.clone());
    let backup_ctx = ctx.clone();
    let backup_ctrl = Controller::new(backup_api, cfg.clone())
        .owns(Api::<Job>::all(client.clone()), cfg.clone())
        .owns(Api::<ConfigMap>::all(client.clone()), cfg.clone())
        .run(backup::reconcile, backup::error_policy, backup_ctx)
        .for_each(|res| async move {
            if let Err(e) = res {
                tracing::debug!(error = %e, "backup reconcile error");
            }
        });

    // BackupSchedule owns the Backup CRs it creates.
    let sched_api: Api<BackupSchedule> = Api::all(client.clone());
    let sched_ctx = ctx.clone();
    let sched_ctrl = Controller::new(sched_api, cfg.clone())
        .owns(Api::<Backup>::all(client.clone()), cfg.clone())
        .run(
            backup_schedule::reconcile,
            backup_schedule::error_policy,
            sched_ctx,
        )
        .for_each(|res| async move {
            if let Err(e) = res {
                tracing::debug!(error = %e, "schedule reconcile error");
            }
        });

    let repo_ctrl = controller!(Repository, repository);
    let crepo_ctrl = controller!(ClusterRepository, cluster_repository);
    let config_ctrl = controller!(BackupConfig, backup_config);
    let restore_ctrl = controller!(Restore, restore);
    let maint_ctrl = controller!(Maintenance, maintenance);

    tokio::join!(
        backup_ctrl,
        sched_ctrl,
        repo_ctrl,
        crepo_ctrl,
        config_ctrl,
        restore_ctrl,
        maint_ctrl,
    );
}

/// The controller's HTTP server: `/metrics` (Prometheus exposition) plus real
/// `/healthz` + `/readyz` endpoints matching the chart's liveness/readiness
/// probes (the previous raw listener returned the metrics body for any path).
async fn serve_http(metrics: Metrics) -> anyhow::Result<()> {
    use axum::extract::State;
    use axum::http::header::CONTENT_TYPE;
    use axum::response::IntoResponse;
    use axum::routing::get;

    async fn metrics_handler(State(metrics): State<Metrics>) -> impl IntoResponse {
        (
            [(CONTENT_TYPE, "text/plain; version=0.0.4")],
            metrics.gather(),
        )
    }
    async fn health() -> &'static str {
        "ok"
    }

    let app = axum::Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .with_state(metrics);

    let listener = tokio::net::TcpListener::bind(config::HTTP_ADDR).await?;
    tracing::info!(
        addr = config::HTTP_ADDR,
        "http server listening (/metrics, /healthz, /readyz)"
    );
    axum::serve(listener, app).await?;
    Ok(())
}
