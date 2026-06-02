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
//! - [`metrics`] — prometheus registry + the ADR §4.13 metrics.
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
pub mod consts;
pub mod context;
pub mod error;
pub mod jobs;
pub mod maintenance;
pub mod metrics;
pub mod repository;
pub mod restore;

use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::runtime::events::{Recorder, Reporter};
use kube::runtime::watcher::Config as WatcherConfig;
use kube::runtime::Controller;
use kube::{Api, Client};

use kopiur_api::{
    Backup, BackupConfig, BackupSchedule, ClusterRepository, Maintenance, Repository, Restore,
};

use crate::context::{Context, KopiaClientFactory};
use crate::metrics::Metrics;

/// The address the `/metrics` HTTP endpoint binds to.
pub const METRICS_ADDR: &str = "0.0.0.0:8080";

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
    init_tracing();

    let client = Client::try_default().await?;
    let metrics = Metrics::new();
    let reporter = Reporter::from("kopiur-controller");
    let recorder = Recorder::new(client.clone(), reporter);
    let ctx = Arc::new(Context::new(
        client.clone(),
        KopiaClientFactory::new(),
        metrics.clone(),
        recorder,
    ));

    tracing::info!("starting kopiur controllers");

    let metrics_srv = tokio::spawn(serve_metrics(metrics.clone()));
    let controllers = spawn_all(client, ctx);

    tokio::select! {
        _ = controllers => tracing::warn!("all controllers exited"),
        r = metrics_srv => tracing::warn!(?r, "metrics server exited"),
    }
    Ok(())
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

/// A tiny `/metrics` HTTP server using a raw `tokio` listener — no web framework
/// dependency. Responds to any GET with the Prometheus text exposition.
async fn serve_metrics(metrics: Metrics) -> anyhow::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(METRICS_ADDR).await?;
    tracing::info!(addr = METRICS_ADDR, "metrics server listening");
    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "metrics accept failed");
                continue;
            }
        };
        let body = metrics.gather();
        tokio::spawn(async move {
            // Drain the request line(s); we serve the same payload for any path.
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(header.as_bytes()).await;
            let _ = socket.write_all(&body).await;
            let _ = socket.shutdown().await;
        });
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}
