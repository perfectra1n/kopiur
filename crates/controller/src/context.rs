//! The shared reconcile [`Context`] handed to every controller.
//!
//! Long kopia operations run in mover `Job`s (the controller writes a
//! `ConfigMap` with a `MoverWorkSpec` and creates a `Job`). The controller only
//! ever spawns kopia directly for short, idempotent ops (repo connect-validate,
//! catalog `snapshot list`, finalizer `snapshot delete`) — and even those run
//! as short-lived Jobs per ADR §5.4. So the [`KopiaClientFactory`] here is a
//! thin builder used only where the design permits in-process invocation; the
//! decision logic is kept pure and unit-tested separately.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use kopiur_api::Maintenance;
use kopiur_kopia::{KopiaClient, KopiaClientBuilder, env as kopia_env};
use kube::Client;
use kube::runtime::events::Recorder;
use kube::runtime::reflector::Store;

use crate::metrics::Metrics;

/// Monotonic counter giving every built client a distinct kopia config file.
/// kopia persists the repository binding to `KOPIA_CONFIG_PATH`; concurrent
/// reconciles connecting to *different* repositories must not share one file or
/// they clobber each other's binding. Cache/log dirs are content-addressed and
/// stay shared.
static CONN_SEQ: AtomicU64 = AtomicU64::new(0);

/// Builds short-lived [`KopiaClient`]s for the controller's idempotent ops.
///
/// Holds the cross-cutting defaults: the binary path, the suppress-update env,
/// and the writable base directory kopia uses for its cache/logs/config (an
/// `emptyDir` the chart mounts at [`kopiur_kopia::env::DEFAULT_CACHE_DIR`]).
/// Without redirecting those off `$HOME` (`/nonexistent` on distroless +
/// read-only rootfs), every in-process kopia call fails to create its cache.
/// Per-repository credentials/env are layered on by the caller via [`build`].
///
/// [`build`]: KopiaClientFactory::build
#[derive(Clone, Debug)]
pub struct KopiaClientFactory {
    binary: Option<String>,
    /// Writable base for kopia cache (`<base>/cache`), logs (`<base>/logs`), and
    /// per-connection config files (`<base>/conn-<n>.config`).
    cache_dir: PathBuf,
}

impl Default for KopiaClientFactory {
    fn default() -> Self {
        KopiaClientFactory {
            binary: None,
            cache_dir: PathBuf::from(kopia_env::DEFAULT_CACHE_DIR),
        }
    }
}

impl KopiaClientFactory {
    /// Factory using the default `kopia` binary on `PATH` and the default cache
    /// base ([`kopiur_kopia::env::DEFAULT_CACHE_DIR`]).
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the kopia binary path (injectable for tests / custom images).
    pub fn with_binary(binary: impl Into<String>) -> Self {
        KopiaClientFactory {
            binary: Some(binary.into()),
            ..Self::default()
        }
    }

    /// Override the writable base directory kopia uses for cache/logs/config.
    /// Set from `KOPIUR_KOPIA_CACHE_DIR`; tests point it at a tempdir.
    pub fn with_cache_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = dir.into();
        self
    }

    /// Build a client carrying the given environment (e.g. `KOPIA_PASSWORD`,
    /// S3 credentials). The update check is suppressed globally, and kopia's
    /// cache/log/config dirs are pinned under the factory's writable base — the
    /// config path is unique per call so concurrent connects can't clobber it.
    pub fn build(&self, env: impl IntoIterator<Item = (String, String)>) -> KopiaClient {
        let cache = self.cache_dir.join("cache");
        let logs = self.cache_dir.join("logs");
        // Best-effort: kopia creates these itself, but pre-creating avoids edge
        // cases where it expects the parent to exist. Degrade, never crash — a
        // failure here surfaces as a clear kopia error if the dir is truly
        // unusable. (See [[actionable-error-messages]].)
        for dir in [&cache, &logs] {
            if let Err(e) = std::fs::create_dir_all(dir) {
                tracing::debug!(
                    dir = %dir.display(),
                    error = %e,
                    "could not pre-create kopia cache dir; letting kopia try"
                );
            }
        }
        let config = self.cache_dir.join(format!(
            "conn-{}.config",
            CONN_SEQ.fetch_add(1, Ordering::Relaxed)
        ));

        let mut b: KopiaClientBuilder = KopiaClient::builder()
            .env("KOPIA_CHECK_FOR_UPDATES", "false")
            .env(kopia_env::CACHE_DIRECTORY_ENV, cache.to_string_lossy())
            .env(kopia_env::LOG_DIR_ENV, logs.to_string_lossy())
            .env(kopia_env::CONFIG_PATH_ENV, config.to_string_lossy());
        if let Some(bin) = &self.binary {
            b = b.binary(bin.clone());
        }
        for (k, v) in env {
            b = b.env(k, v);
        }
        b.build()
    }
}

/// Shared state for all reconcilers. Cheap to `Arc`-wrap and clone: the kube
/// `Client` and the prometheus collectors are internally reference-counted.
#[derive(Clone)]
pub struct Context {
    /// The Kubernetes API client.
    pub client: Client,
    /// Factory for short-lived kopia clients (idempotent ops only).
    pub kopia: KopiaClientFactory,
    /// Controller + business metrics.
    pub metrics: Metrics,
    /// Event recorder for surfacing reconcile decisions on the objects.
    pub recorder: Recorder,
    /// Container image used for mover `Job`s (configurable per deployment via
    /// `KOPIUR_MOVER_IMAGE`; defaults to [`crate::jobs::DEFAULT_MOVER_IMAGE`]).
    pub mover_image: String,
    /// ServiceAccount the mover `Job` pods run as (configurable via
    /// `KOPIUR_MOVER_SERVICE_ACCOUNT`). The mover PATCHes the owning CR's
    /// `.status`, so this SA must be bound to the operator's status-patch rules.
    /// `None` falls back to the namespace `default` SA.
    pub mover_service_account: Option<String>,
    /// Env the controller passes through to every mover `Job`: OTLP
    /// (`OTEL_EXPORTER_OTLP_*`, only when a collector is configured) plus logging
    /// (`RUST_LOG`, `KOPIUR_LOG_FORMAT`, whenever set) so movers inherit the
    /// controller's telemetry export and log level/format. `(name, value)` pairs.
    pub mover_env_passthrough: Vec<(String, String)>,
    /// Shared informer cache of all `Maintenance` CRs, reused from the Maintenance
    /// controller's reflector (`Controller::store()`). The `Repository`/
    /// `ClusterRepository` reconcilers read it synchronously to answer "is a
    /// Maintenance configured for me?" without a per-reconcile `Api::list`.
    pub maintenance_store: Store<Maintenance>,
    /// `true` once [`maintenance_store`](Self::maintenance_store) has completed its
    /// initial list (the reflector synced). Until then the maintenance check is
    /// skipped so a cold cache never produces a false "not configured" warning.
    pub maintenance_synced: Arc<AtomicBool>,
    /// The operator's own namespace (`KOPIUR_NAMESPACE`), if known. Used as the
    /// default placement namespace for a `ClusterRepository`'s managed
    /// `Maintenance` CR when `spec.maintenance.namespace` is unset. `None` when
    /// unset (e.g. running out-of-cluster), in which case the cluster-repo
    /// placement is surfaced as unresolved rather than guessed.
    pub operator_namespace: Option<String>,
}

impl Context {
    /// Construct a context. The [`Recorder`] should be built from the same
    /// client with a `Reporter` naming this controller.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Client,
        kopia: KopiaClientFactory,
        metrics: Metrics,
        recorder: Recorder,
        mover_image: String,
        mover_service_account: Option<String>,
        mover_env_passthrough: Vec<(String, String)>,
        maintenance_store: Store<Maintenance>,
        maintenance_synced: Arc<AtomicBool>,
        operator_namespace: Option<String>,
    ) -> Self {
        Context {
            client,
            kopia,
            metrics,
            recorder,
            mover_image,
            mover_service_account,
            mover_env_passthrough,
            maintenance_store,
            maintenance_synced,
            operator_namespace,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- regression: the controller's in-process kopia inherited no writable
    // cache/log/config dir, so on a read-only rootfs with $HOME=/nonexistent
    // every `repository connect/create` failed with
    // `mkdir /nonexistent: read-only file system` and could never persist its
    // config. The factory must pin all three onto the writable base. ---

    fn base() -> PathBuf {
        std::env::temp_dir().join("kopiur-factory-test")
    }

    #[test]
    fn build_injects_kopia_cache_log_and_config_env() {
        let b = base();
        let client = KopiaClientFactory::new().with_cache_dir(&b).build([]);
        let env = client.common_env();

        assert_eq!(
            env.get(kopia_env::CACHE_DIRECTORY_ENV),
            Some(&b.join("cache").to_string_lossy().into_owned())
        );
        assert_eq!(
            env.get(kopia_env::LOG_DIR_ENV),
            Some(&b.join("logs").to_string_lossy().into_owned())
        );
        assert!(
            env.get(kopia_env::CONFIG_PATH_ENV)
                .expect("config path env set")
                .starts_with(&b.join("conn-").to_string_lossy().into_owned()),
            "config path should live under the cache base"
        );
        // The update-check suppression is still applied.
        assert_eq!(
            env.get("KOPIA_CHECK_FOR_UPDATES").map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn build_gives_each_client_a_distinct_config_path_but_shares_the_cache() {
        let factory = KopiaClientFactory::new().with_cache_dir(base());
        let a = factory.build([]);
        let b = factory.build([]);

        // Per-connection config files must differ so concurrent connects to
        // different repositories can't clobber each other's binding.
        assert_ne!(
            a.common_env().get(kopia_env::CONFIG_PATH_ENV),
            b.common_env().get(kopia_env::CONFIG_PATH_ENV)
        );
        // The content-addressed cache is safe to share.
        assert_eq!(
            a.common_env().get(kopia_env::CACHE_DIRECTORY_ENV),
            b.common_env().get(kopia_env::CACHE_DIRECTORY_ENV)
        );
    }

    #[test]
    fn build_passes_through_caller_env() {
        let client = KopiaClientFactory::new()
            .with_cache_dir(base())
            .build([("KOPIA_PASSWORD".to_string(), "s3cr3t".to_string())]);
        assert_eq!(
            client
                .common_env()
                .get("KOPIA_PASSWORD")
                .map(String::as_str),
            Some("s3cr3t")
        );
    }

    #[test]
    fn default_factory_uses_the_shared_cache_base() {
        let client = KopiaClientFactory::new().build([]);
        assert!(
            client
                .common_env()
                .get(kopia_env::CACHE_DIRECTORY_ENV)
                .expect("cache env set")
                .starts_with(kopia_env::DEFAULT_CACHE_DIR)
        );
    }
}
