#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

pub mod cache;
pub mod catalog;
pub mod cluster_repository;
pub mod config;
pub mod consts;
pub mod context;
pub mod error;
pub mod hooks;
pub mod io;
pub mod jobs;
pub mod maintenance;
pub mod metrics;
pub mod repository;
pub mod repository_replication;
pub mod restore;
pub mod server;
pub mod snapshot;
pub mod snapshot_policy;
pub mod snapshot_schedule;
pub mod verification;
pub mod watch;
pub mod webhook_tls;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::{Stream, StreamExt};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Secret, Service, ServiceAccount};
use kube::core::PartialObjectMeta;
use kube::runtime::events::{Recorder, Reporter};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher::Config as WatcherConfig;
use kube::runtime::{Controller, WatchStreamExt, metadata_watcher, reflector, watcher};
use kube::{Api, Client, ResourceExt};

use kopiur_api::common::RepositoryKind;
use kopiur_api::{
    ClusterRepository, Maintenance, Repository, RepositoryReplication, Restore, Snapshot,
    SnapshotPolicy, SnapshotSchedule,
};

use crate::context::{Context, KopiaClientFactory};
use crate::metrics::Metrics;

/// Build the controller manager and run every controller concurrently, plus the
/// `/metrics` server, until shutdown.
///
/// Each `Controller` wires its owned-resource watches per ADR §5.2:
/// - `SnapshotSchedule` owns `Snapshot`.
/// - `SnapshotPolicy` watches `Snapshot` (GFS retention).
/// - `Repository`/`ClusterRepository` watch discovered `Snapshot`.
/// - `Snapshot` owns `Job` + `ConfigMap` (mover run).
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

    // Self-managed webhook TLS (`webhook.tls.mode: self`): mint the serving cert
    // and inject the caBundle so the API server trusts the webhook — no
    // cert-manager. Best-effort at boot (the webhook configs may not exist yet on
    // a first apply); a slow background task then handles drift + leaf rotation.
    // Absent the managed-mode env, this is a no-op (cert-manager / manual mode).
    if let Some(webhook_tls) = webhook_tls_config() {
        let ns = webhook_tls.namespace.clone();
        let boot_ok = match webhook_tls::ensure(&client, &webhook_tls).await {
            Ok(()) => {
                tracing::info!(namespace = %ns, "self-managed webhook TLS ready");
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "initial webhook TLS setup failed; retrying shortly");
                false
            }
        };
        spawn_webhook_tls_reconcile(client.clone(), webhook_tls, boot_ok);
    }

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

/// Assemble the [`webhook_tls::WebhookTlsConfig`] from env, or `None` when the
/// chart did not enable self-managed webhook TLS (cert-manager / manual mode, or
/// off-chart). Requires the managed gate plus a known operator namespace and both
/// webhook-configuration names; a partial config is treated as not-managed and
/// logged, rather than guessed.
fn webhook_tls_config() -> Option<webhook_tls::WebhookTlsConfig> {
    let managed = std::env::var(config::WEBHOOK_TLS_MANAGED_ENV)
        .map(|v| v == "true")
        .unwrap_or(false);
    if !managed {
        return None;
    }
    let env = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());

    let namespace = match env(config::OPERATOR_NAMESPACE_ENV) {
        Some(ns) => ns,
        None => {
            tracing::warn!(
                "{} is set but {} is unset; cannot place the webhook TLS Secret — skipping \
                 self-managed webhook TLS",
                config::WEBHOOK_TLS_MANAGED_ENV,
                config::OPERATOR_NAMESPACE_ENV
            );
            return None;
        }
    };
    let (Some(validating_config), Some(mutating_config)) = (
        env(config::WEBHOOK_VALIDATING_CONFIG_ENV),
        env(config::WEBHOOK_MUTATING_CONFIG_ENV),
    ) else {
        tracing::warn!(
            "self-managed webhook TLS requested but {}/{} are unset — skipping",
            config::WEBHOOK_VALIDATING_CONFIG_ENV,
            config::WEBHOOK_MUTATING_CONFIG_ENV
        );
        return None;
    };
    let secret_name = env(config::WEBHOOK_SECRET_NAME_ENV)
        .unwrap_or_else(|| config::DEFAULT_WEBHOOK_SECRET_NAME.to_string());
    let service_name = env(config::WEBHOOK_SERVICE_NAME_ENV).unwrap_or_else(|| secret_name.clone());

    Some(webhook_tls::WebhookTlsConfig {
        namespace,
        secret_name,
        service_name,
        validating_config,
        mutating_config,
    })
}

/// Drive [`webhook_tls::ensure`] in the background so the serving leaf rotates
/// before expiry and the `caBundle` self-heals if anything overwrites it.
///
/// The cadence is adaptive: after a success it waits the slow steady-state
/// interval ([`config::WEBHOOK_TLS_RECONCILE_INTERVAL`]); after a failure it
/// retries soon ([`config::WEBHOOK_TLS_RETRY_INTERVAL`]). This matters at boot —
/// if the webhook configurations aren't registered yet when the controller
/// starts, the first inject fails, and a fixed slow tick would leave admission
/// untrusted for hours. `boot_ok` seeds the first wait from the boot attempt's
/// result. Errors are logged, never fatal (degrade-not-crash).
fn spawn_webhook_tls_reconcile(client: Client, cfg: webhook_tls::WebhookTlsConfig, boot_ok: bool) {
    tokio::spawn(async move {
        let mut ok = boot_ok;
        loop {
            let delay = if ok {
                config::WEBHOOK_TLS_RECONCILE_INTERVAL
            } else {
                config::WEBHOOK_TLS_RETRY_INTERVAL
            };
            tokio::time::sleep(delay).await;
            ok = match webhook_tls::ensure(&client, &cfg).await {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!(error = %e, "webhook TLS reconcile failed; will retry soon");
                    false
                }
            };
        }
    });
}

/// A metadata-only trigger stream for a cluster-wide external referent `K`
/// (`Secret`/`ConfigMap`/`ServiceAccount`/`Namespace`). The referent mappers in
/// [`crate::watch`] need only the changed object's name/namespace — never its
/// `.data`/`.spec`/`.status` — so `metadata_watcher` keeps those payloads off the
/// wire entirely (no `Secret` plaintext ever reaches the controller, a memory AND
/// security win) and we additionally drop `managedFields` + `annotations` (the
/// largest remaining `ObjectMeta` bytes, unused by every mapper) before the events
/// fan in. Fed to [`Controller::watches_stream`] in place of a full-object
/// `.watches(..)`; `touched_objects` is the decode `watches_stream` expects.
fn referent_meta<K>(
    client: &Client,
    cfg: &WatcherConfig,
) -> impl Stream<Item = Result<PartialObjectMeta<K>, watcher::Error>> + Send + use<K>
where
    K: kube::Resource<DynamicType = ()>
        + Clone
        + serde::de::DeserializeOwned
        + std::fmt::Debug
        + Send
        + 'static,
{
    metadata_watcher(Api::<K>::all(client.clone()), cfg.clone())
        .modify(|m| {
            m.metadata.managed_fields = None;
            m.metadata.annotations = None;
        })
        .touched_objects()
}

/// Map a kopia web-UI server child (Deployment/Service) to its owning
/// `ClusterRepository` via the back-reference label. Cluster-scoped owners can't be
/// ownerReferences of a namespaced child, so `.owns()` won't fire — this label
/// mapping replaces it.
fn map_to_cluster_repository<K: kube::Resource>(obj: K) -> Option<ObjectRef<ClusterRepository>> {
    obj.labels()
        .get(consts::CLUSTER_REPOSITORY_LABEL)
        .map(|name| ObjectRef::new(name))
}

/// Spawn all eight controllers and join them. Split out so it can be driven
/// independently of the metrics server. The shared Maintenance informer that the
/// repo reconcilers read is set up separately in [`run`].
async fn spawn_all(client: Client, ctx: Arc<Context>) {
    let mut cfg = WatcherConfig::default();
    // Opt-in (off by default for older-apiserver safety): stream the initial list
    // via the WatchList API to cut peak memory on the cluster-wide resync.
    if crate::config::streaming_lists_enabled() {
        cfg = cfg.streaming_lists();
    }
    // Owned children (mover Jobs, work-spec ConfigMaps) ALWAYS carry the managed-by
    // label (io::finalizer::child_labels), so scope their watches server-side to
    // ours — the controller then lists/watches only kopiur's Jobs/ConfigMaps, not
    // every one in the cluster. Owner-ref mapping is unaffected by the label filter.
    let owned_cfg = cfg.clone().labels(&format!(
        "{}={}",
        crate::consts::MANAGED_BY_LABEL,
        crate::consts::MANAGED_BY_VALUE
    ));
    // Trailing-edge debounce on every controller: coalesce rapid re-triggers of
    // the same object (own status writes, owned-Job event bursts, referent
    // fan-out) into one reconcile. Belt-and-braces against write-triggered
    // SELF-loops — with order-stable conditions and guarded status writes the
    // steady state emits no events at all, but if a future write churns, the
    // event stream pauses while no reconcile runs, the quiet window always
    // arrives, and the loop is capped at ~1/window per object instead of
    // reconcile speed (~30/s, a pegged core).
    //
    // SIZING — this is a HEAL-LATENCY floor, not just a loop cap. Terminal state
    // is written in two passes: the mover stamps the terminal `phase`, then the
    // controller's follow-up reconcile heals the derived fields (kstatus
    // `Ready=True`, post-hook `hooks.postCompletedAt`, the `kopiur_resource_phase`
    // gauge). The debounce delays that second pass by its full duration, so a
    // consumer that gates on `phase` sees the derived fields lag by ~one window.
    // At 1s this was visible: e2e assertions reading conditions immediately after
    // `phase: Completed`/`Succeeded` raced the heal and read stale values
    // (restore_completed_reports_kstatus_ready, http_request_post_hook…,
    // metrics_reflect_backup_lifecycle). 250ms still coalesces owned-Job event
    // bursts and caps any residual self-loop at ~4/s (nowhere near a hot core),
    // while keeping the heal lag imperceptible to `kubectl wait --for=condition`
    // and Flux/Argo health gates.
    //
    // CAVEAT (trailing edge = deadline RESETS on every event): a sustained
    // EXTERNAL writer churning one object faster than the window would defer its
    // reconcile until the stream quiets, not rate-limit it — no such writer
    // exists today (the controller is the sole steady-state status writer; mover
    // stamps are one-shot), so if a reconciler ever looks starved, look for a new
    // sub-window event source on its primary.
    let ctrl_cfg = kube::runtime::controller::Config::default()
        .debounce(std::time::Duration::from_millis(250));

    // Snapshot owns its mover Job + ConfigMap (reaped via owner-ref GC, §4.10), and
    // watches its `SnapshotPolicy` recipe so a policy edit (or a policy whose
    // repository just became Ready) re-runs the snapshot promptly instead of waiting
    // out its requeue.
    let snapshot_api: Api<Snapshot> = Api::all(client.clone());
    let snapshot_ctx = ctx.clone();
    let snapshot_ctrl = Controller::new(snapshot_api, cfg.clone()).with_config(ctrl_cfg.clone());
    let snapshot_store = snapshot_ctrl.store();
    let snapshot_ctrl = snapshot_ctrl
        .owns_with(Api::<Job>::all(client.clone()), (), owned_cfg.clone())
        .owns_with(Api::<ConfigMap>::all(client.clone()), (), owned_cfg.clone())
        .watches(Api::<SnapshotPolicy>::all(client.clone()), cfg.clone(), {
            let store = snapshot_store.clone();
            move |p: SnapshotPolicy| watch::policy_to_snapshots(&store, &p)
        })
        // A Snapshot refused for an elevated mover waits on the namespace's
        // privileged-movers opt-in annotation — deliver that grant the moment it
        // lands instead of leaving the CR to its slow backstop requeue.
        .watches_stream(referent_meta::<Namespace>(&client, &cfg), {
            let store = snapshot_store.clone();
            move |n: PartialObjectMeta<Namespace>| watch::namespace_to_snapshots(&store, &n)
        })
        .run(snapshot::reconcile, snapshot::error_policy, snapshot_ctx)
        .for_each(|res| async move {
            if let Err(e) = res {
                tracing::debug!(error = %e, "snapshot reconcile error");
            }
        });

    // SnapshotSchedule owns the Snapshot CRs it creates, and watches SnapshotPolicy
    // (by `policyRef` or `policySelector`) so a new/relabeled/edited policy is picked
    // up promptly rather than only on the schedule's periodic re-list.
    let sched_api: Api<SnapshotSchedule> = Api::all(client.clone());
    let sched_ctx = ctx.clone();
    let sched_ctrl = Controller::new(sched_api, cfg.clone()).with_config(ctrl_cfg.clone());
    let sched_store = sched_ctrl.store();
    let sched_ctrl = sched_ctrl
        .owns(Api::<Snapshot>::all(client.clone()), cfg.clone())
        .watches(Api::<SnapshotPolicy>::all(client.clone()), cfg.clone(), {
            let store = sched_store.clone();
            move |p: SnapshotPolicy| watch::policy_to_schedules(&store, &p)
        })
        .run(
            snapshot_schedule::reconcile,
            snapshot_schedule::error_policy,
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
    // Repository/ClusterRepository additionally watch their credential `Secret`(s)
    // and TLS-CA `ConfigMap`: a content edit to a referenced Secret/ConfigMap does
    // NOT bump the repo's `generation`, so without these watches a fixed password
    // never re-triggers a connect (and the terminal-failure gate, keyed on the
    // Secret's `resourceVersion`, would only reopen on the 30-min heartbeat).
    let repo_api: Api<Repository> = Api::all(client.clone());
    let repo_ctx = ctx.clone();
    let repo_ctrl = Controller::new(repo_api, cfg.clone()).with_config(ctrl_cfg.clone());
    let repo_store = repo_ctrl.store();
    let repo_ctrl = repo_ctrl
        // Own the bootstrap Job (carries a controller ownerRef already): its
        // terminal state arrives as an EVENT instead of hoping a 15s poll lands
        // inside the Job's ttlSecondsAfterFinished window — a short TTL used to
        // reap the finished Job between polls, so the result was never read and
        // the repo churned Initializing + fresh bootstrap Jobs forever.
        .owns_with(Api::<Job>::all(client.clone()), (), owned_cfg.clone())
        // Own the optional kopia web-UI server children (`spec.server`): the
        // Repository owner-refs them in its own namespace, so an edit/delete of the
        // Deployment/Service/ConfigMap self-heals via owner-ref mapping. Scoped to
        // the managed-by label (the server builder stamps it) so we don't watch
        // every Deployment/Service in the cluster.
        .owns_with(
            Api::<Deployment>::all(client.clone()),
            (),
            owned_cfg.clone(),
        )
        .owns_with(Api::<Service>::all(client.clone()), (), owned_cfg.clone())
        .owns_with(Api::<ConfigMap>::all(client.clone()), (), owned_cfg.clone())
        .watches_stream(referent_meta::<Secret>(&client, &cfg), {
            let store = repo_store.clone();
            move |s: PartialObjectMeta<Secret>| watch::secret_to_repositories(&store, &s)
        })
        .watches_stream(referent_meta::<ConfigMap>(&client, &cfg), {
            let store = repo_store.clone();
            move |cm: PartialObjectMeta<ConfigMap>| watch::configmap_to_repositories(&store, &cm)
        })
        // Workload identity: creating the `auth.workloadIdentity` ServiceAccount
        // un-sticks a repository blocked on the SA preflight immediately.
        .watches_stream(referent_meta::<ServiceAccount>(&client, &cfg), {
            let store = repo_store.clone();
            move |sa: PartialObjectMeta<ServiceAccount>| {
                watch::serviceaccount_to_repositories(&store, &sa)
            }
        })
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
    let crepo_ctrl = Controller::new(crepo_api, cfg.clone()).with_config(ctrl_cfg.clone());
    let crepo_store = crepo_ctrl.store();
    let crepo_ctrl = crepo_ctrl
        // Own the bootstrap Job — same prompt-terminal-observation rationale as
        // the Repository controller above.
        .owns_with(Api::<Job>::all(client.clone()), (), owned_cfg.clone())
        // The kopia web-UI server children (`spec.server`) are namespaced and cannot
        // carry an ownerReference to a cluster-scoped owner, so `.owns()` would never
        // fire. Map child events back to the parent via the back-reference label
        // instead (scoped to the managed-by label, same as everything else).
        .watches(
            Api::<Deployment>::all(client.clone()),
            owned_cfg.clone(),
            map_to_cluster_repository,
        )
        .watches(
            Api::<Service>::all(client.clone()),
            owned_cfg.clone(),
            map_to_cluster_repository,
        )
        .watches_stream(referent_meta::<Secret>(&client, &cfg), {
            let store = crepo_store.clone();
            move |s: PartialObjectMeta<Secret>| watch::secret_to_cluster_repositories(&store, &s)
        })
        .watches_stream(referent_meta::<ConfigMap>(&client, &cfg), {
            let store = crepo_store.clone();
            move |cm: PartialObjectMeta<ConfigMap>| {
                watch::configmap_to_cluster_repositories(&store, &cm)
            }
        })
        // Workload identity: same SA-preflight un-stick as the Repository
        // controller (name-only match; movers run in many namespaces).
        .watches_stream(referent_meta::<ServiceAccount>(&client, &cfg), {
            let store = crepo_store.clone();
            move |sa: PartialObjectMeta<ServiceAccount>| {
                watch::serviceaccount_to_cluster_repositories(&store, &sa)
            }
        })
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

    // SnapshotPolicy owns the verification mover Jobs + ConfigMaps it spawns
    // (ADR-0005 §4), so they're GC'd with the policy and a Job event re-triggers the
    // reconcile (the verify scheduler).
    let config_api: Api<SnapshotPolicy> = Api::all(client.clone());
    let config_ctx = ctx.clone();
    let config_ctrl = Controller::new(config_api, cfg.clone()).with_config(ctrl_cfg.clone());
    let config_store = config_ctrl.store();
    let config_ctrl = config_ctrl
        .owns_with(Api::<Job>::all(client.clone()), (), owned_cfg.clone())
        .owns_with(Api::<ConfigMap>::all(client.clone()), (), owned_cfg.clone())
        // A produced `Snapshot` carries its owning policy in the config label. Watch
        // Snapshots so GFS retention (ADR §4.4) reconciles PROMPTLY when one is created
        // or deleted — without this the policy only re-runs on its periodic requeue, so
        // a new snapshot's prune (and a pinned snapshot's exemption) lags by minutes.
        .watches(
            Api::<Snapshot>::all(client.clone()),
            cfg.clone(),
            |s: Snapshot| match (s.labels().get(crate::consts::CONFIG_LABEL), s.namespace()) {
                (Some(policy), Some(ns)) => {
                    Some(ObjectRef::<SnapshotPolicy>::new(policy).within(&ns))
                }
                _ => None,
            },
        )
        // Watch the backing repository: when it becomes Ready (e.g. a credential was
        // fixed) the policy re-reconciles at once instead of waiting out its requeue.
        .watches(Api::<Repository>::all(client.clone()), cfg.clone(), {
            let store = config_store.clone();
            move |r: Repository| watch::repository_to_policies(&store, &r)
        })
        .watches(
            Api::<ClusterRepository>::all(client.clone()),
            cfg.clone(),
            {
                let store = config_store.clone();
                move |r: ClusterRepository| watch::cluster_repository_to_policies(&store, &r)
            },
        )
        .run(
            snapshot_policy::reconcile,
            snapshot_policy::error_policy,
            config_ctx,
        )
        .for_each(|res| async move {
            if let Err(e) = res {
                tracing::debug!(error = %e, "snapshot_policy reconcile error");
            }
        });

    // Restore watches its repository so a restore blocked on a not-yet-Ready repo
    // proceeds the moment the repo connects, rather than on the restore's requeue.
    let restore_api: Api<Restore> = Api::all(client.clone());
    let restore_ctx = ctx.clone();
    let restore_ctrl = Controller::new(restore_api, cfg.clone()).with_config(ctrl_cfg.clone());
    let restore_store = restore_ctrl.store();
    let restore_ctrl = restore_ctrl
        .watches(Api::<Repository>::all(client.clone()), cfg.clone(), {
            let store = restore_store.clone();
            move |r: Repository| watch::repository_to_restores(&store, &r)
        })
        .watches(
            Api::<ClusterRepository>::all(client.clone()),
            cfg.clone(),
            {
                let store = restore_store.clone();
                move |r: ClusterRepository| watch::cluster_repository_to_restores(&store, &r)
            },
        )
        // Same privileged-mover opt-in delivery as the Snapshot controller.
        .watches_stream(referent_meta::<Namespace>(&client, &cfg), {
            let store = restore_store.clone();
            move |n: PartialObjectMeta<Namespace>| watch::namespace_to_restores(&store, &n)
        })
        .run(restore::reconcile, restore::error_policy, restore_ctx)
        .for_each(|res| async move {
            if let Err(e) = res {
                tracing::debug!(error = %e, "restore reconcile error");
            }
        });

    // Maintenance watches its repository (same Ready-gate prompting as the others).
    let maint_api: Api<Maintenance> = Api::all(client.clone());
    let maint_ctx = ctx.clone();
    let maint_ctrl = Controller::new(maint_api, cfg.clone()).with_config(ctrl_cfg.clone());
    let maint_store = maint_ctrl.store();
    let maint_ctrl = maint_ctrl
        // Own the per-slot maintenance Jobs (controller ownerRef already set):
        // a yield/run's terminal state must be OBSERVED (it records
        // `lastHandledAt`) — with only the 30s poll, a short Job TTL could reap
        // the finished Job between polls and the slot would re-fire unrecorded.
        .owns_with(Api::<Job>::all(client.clone()), (), owned_cfg.clone())
        .watches(Api::<Repository>::all(client.clone()), cfg.clone(), {
            let store = maint_store.clone();
            move |r: Repository| watch::repository_to_maintenances(&store, &r)
        })
        .watches(
            Api::<ClusterRepository>::all(client.clone()),
            cfg.clone(),
            {
                let store = maint_store.clone();
                move |r: ClusterRepository| watch::cluster_repository_to_maintenances(&store, &r)
            },
        )
        .run(maintenance::reconcile, maintenance::error_policy, maint_ctx)
        .for_each(|res| async move {
            if let Err(e) = res {
                tracing::debug!(error = %e, "maintenance reconcile error");
            }
        });

    // RepositoryReplication owns its per-slot mover Jobs + ConfigMaps (ADR-0005 §13(d)),
    // watches its *source* repository (Ready-gate prompting), and its *destination*
    // credential Secret (a dest password/auth fix re-triggers the mirror promptly).
    let repl_api: Api<RepositoryReplication> = Api::all(client.clone());
    let repl_ctx = ctx.clone();
    let repl_ctrl = Controller::new(repl_api, cfg.clone()).with_config(ctrl_cfg.clone());
    let repl_store = repl_ctrl.store();
    let repl_ctrl = repl_ctrl
        .owns_with(Api::<Job>::all(client.clone()), (), owned_cfg.clone())
        .owns_with(Api::<ConfigMap>::all(client.clone()), (), owned_cfg.clone())
        .watches_stream(referent_meta::<Secret>(&client, &cfg), {
            let store = repl_store.clone();
            move |s: PartialObjectMeta<Secret>| watch::secret_to_replications(&store, &s)
        })
        .watches(Api::<Repository>::all(client.clone()), cfg.clone(), {
            let store = repl_store.clone();
            move |r: Repository| watch::repository_to_replications(&store, &r)
        })
        .watches(
            Api::<ClusterRepository>::all(client.clone()),
            cfg.clone(),
            {
                let store = repl_store.clone();
                move |r: ClusterRepository| watch::cluster_repository_to_replications(&store, &r)
            },
        )
        .run(
            repository_replication::reconcile,
            repository_replication::error_policy,
            repl_ctx,
        )
        .for_each(|res| async move {
            if let Err(e) = res {
                tracing::debug!(error = %e, "repository_replication reconcile error");
            }
        });

    tokio::join!(
        snapshot_ctrl,
        sched_ctrl,
        repo_ctrl,
        crepo_ctrl,
        config_ctrl,
        restore_ctrl,
        maint_ctrl,
        repl_ctrl,
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
