#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

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
use std::sync::atomic::{AtomicBool, Ordering};

use futures::StreamExt;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::runtime::events::{Recorder, Reporter};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher::Config as WatcherConfig;
use kube::runtime::{Controller, WatchStreamExt, reflector, watcher};
use kube::{Api, Client, ResourceExt};

use kopiur_api::common::RepositoryKind;
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
    // bound to the mover status-patch RBAC. This is a dedicated least-privilege SA
    // (not the operator SA): the controller mints it + a RoleBinding to the mover
    // role in each Job's (workload) namespace. The chart sets this name; `None`
    // (off-chart) keeps the legacy behaviour of the `default` SA with no minting.
    let mover_service_account = std::env::var(config::MOVER_SERVICE_ACCOUNT_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    tracing::info!(mover_service_account = ?mover_service_account, "mover SA configured");
    // Name of the mover ClusterRole/Role the minted RoleBinding references. Falls
    // back to the chart's default name when unset so minting still resolves.
    let mover_clusterrole = std::env::var(config::MOVER_CLUSTERROLE_ENV)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| config::DEFAULT_MOVER_NAME.to_string());
    // `roleRef.kind` for the minted mover RoleBinding (ClusterRole vs Role), set by
    // the chart from installScope.
    let mover_role_kind = std::env::var(config::MOVER_ROLE_KIND_ENV)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| config::DEFAULT_MOVER_ROLE_KIND.to_string());
    tracing::info!(mover_clusterrole = %mover_clusterrole, mover_role_kind = %mover_role_kind, "mover role configured");
    // The operator's own namespace (downward API: KOPIUR_NAMESPACE). Default
    // placement for a ClusterRepository's managed (namespaced) Maintenance CR.
    let operator_namespace = std::env::var(config::OPERATOR_NAMESPACE_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    tracing::info!(operator_namespace = ?operator_namespace, "operator namespace configured");
    // Telemetry + logging env the controller passes through to mover Jobs: OTLP
    // (when a collector is configured) plus RUST_LOG / KOPIUR_LOG_FORMAT so movers
    // inherit the controller's log level and format.
    let mover_env_passthrough = collect_mover_env_passthrough();

    // The writable base for the controller's in-process kopia cache/logs/config
    // (an emptyDir the chart mounts at the default). Overridable only if that
    // mount is relocated; without it kopia would try $HOME (/nonexistent) on the
    // read-only rootfs and fail to create its cache.
    let kopia_factory = match std::env::var(config::KOPIA_CACHE_DIR_ENV)
        .ok()
        .filter(|s| !s.is_empty())
    {
        Some(dir) => {
            tracing::info!(kopia_cache_dir = %dir, "kopia cache dir overridden");
            KopiaClientFactory::new().with_cache_dir(dir)
        }
        None => KopiaClientFactory::new(),
    };

    // Shared Maintenance informer: a single reflector-backed cache the
    // Repository/ClusterRepository reconcilers read to answer "is a Maintenance
    // configured for me?" without an `Api::list` per reconcile. We drive the
    // reflector stream ourselves in a spawned task (a standalone `Store`'s
    // `wait_until_ready()` does NOT drive the underlying watch — kube requires the
    // reflector stream to be polled separately), and flip `maintenance_synced`
    // once the initial list completes so a cold cache never yields a false
    // "not configured" warning on startup.
    let (maintenance_store, maintenance_writer) = reflector::store::<Maintenance>();
    let maintenance_synced = Arc::new(AtomicBool::new(false));
    {
        let reader = maintenance_store.clone();
        let synced = maintenance_synced.clone();
        let api: Api<Maintenance> = Api::all(client.clone());
        tokio::spawn(async move {
            // Flip the flag as soon as the reflector reports its first sync.
            let mark_ready = async move {
                if reader.wait_until_ready().await.is_ok() {
                    synced.store(true, Ordering::Relaxed);
                    tracing::info!("maintenance informer cache synced");
                } else {
                    tracing::warn!("maintenance informer writer dropped before sync");
                }
            };
            // Drive the watch → reflector store forever (with backoff on errors).
            let drive = async move {
                let stream = reflector(maintenance_writer, watcher(api, WatcherConfig::default()))
                    .default_backoff()
                    .touched_objects();
                futures::pin_mut!(stream);
                while let Some(ev) = stream.next().await {
                    if let Err(e) = ev {
                        tracing::debug!(error = %e, "maintenance informer watch error");
                    }
                }
            };
            tokio::join!(mark_ready, drive);
        });
    }

    let ctx = Arc::new(Context::new(
        client.clone(),
        kopia_factory,
        metrics.clone(),
        recorder,
        mover_image,
        mover_service_account,
        mover_clusterrole,
        mover_role_kind,
        mover_env_passthrough,
        maintenance_store,
        maintenance_synced,
        operator_namespace,
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

/// Collect the env vars the controller stamps onto every mover `Job` so a mover
/// inherits the controller's telemetry + logging configuration. Two groups:
///
/// - **OTLP** (`OTEL_EXPORTER_OTLP_*`): forwarded only when a collector endpoint
///   is set, so movers stay fully offline (fmt-only) otherwise.
/// - **Logging** (`RUST_LOG`, `KOPIUR_LOG_FORMAT`): forwarded whenever present,
///   regardless of OTLP, so `kubectl logs` on a mover Job honors the same level
///   and format the controller runs with.
///
/// `(name, value)` pairs, de-duplicated by name (the two groups don't overlap).
fn collect_mover_env_passthrough() -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = Vec::new();

    // OTLP only when a collector is configured.
    if std::env::var(config::OTEL_EXPORTER_OTLP_ENDPOINT).is_ok() {
        env.extend(
            config::OTLP_PASSTHROUGH
                .iter()
                .filter_map(|k| std::env::var(k).ok().map(|v| (k.to_string(), v))),
        );
    }

    // Logging always (when set in the controller's env).
    env.extend(
        config::LOG_PASSTHROUGH
            .iter()
            .filter_map(|k| std::env::var(k).ok().map(|v| (k.to_string(), v))),
    );

    env
}

/// Spawn all seven controllers and join them. Split out so it can be driven
/// independently of the metrics server. The shared Maintenance informer that the
/// repo reconcilers read is set up separately in [`run`].
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

    // Repository/ClusterRepository additionally watch Maintenance: a Maintenance
    // create/delete maps to the repo it references and triggers an immediate
    // re-reconcile, so the MaintenanceConfigured condition/warning clears within
    // seconds instead of waiting for the 300s requeue. The mappers are exhaustive
    // over RepositoryKind (a Repository ref never triggers a ClusterRepository
    // reconcile and vice versa).
    let repo_api: Api<Repository> = Api::all(client.clone());
    let repo_ctx = ctx.clone();
    let repo_ctrl = Controller::new(repo_api, cfg.clone())
        .watches(
            Api::<Maintenance>::all(client.clone()),
            cfg.clone(),
            |m: Maintenance| {
                let r = &m.spec.repository;
                match r.kind {
                    RepositoryKind::Repository => {
                        let ns = r.namespace.clone().or_else(|| m.namespace());
                        ns.map(|ns| ObjectRef::<Repository>::new(&r.name).within(&ns))
                    }
                    RepositoryKind::ClusterRepository => None,
                }
            },
        )
        .run(repository::reconcile, repository::error_policy, repo_ctx)
        .for_each(|res| async move {
            if let Err(e) = res {
                tracing::debug!(error = %e, "repository reconcile error");
            }
        });

    let crepo_api: Api<ClusterRepository> = Api::all(client.clone());
    let crepo_ctx = ctx.clone();
    let crepo_ctrl = Controller::new(crepo_api, cfg.clone())
        .watches(
            Api::<Maintenance>::all(client.clone()),
            cfg.clone(),
            |m: Maintenance| {
                let r = &m.spec.repository;
                match r.kind {
                    RepositoryKind::ClusterRepository => {
                        Some(ObjectRef::<ClusterRepository>::new(&r.name))
                    }
                    RepositoryKind::Repository => None,
                }
            },
        )
        .run(
            cluster_repository::reconcile,
            cluster_repository::error_policy,
            crepo_ctx,
        )
        .for_each(|res| async move {
            if let Err(e) = res {
                tracing::debug!(error = %e, "cluster_repository reconcile error");
            }
        });

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
