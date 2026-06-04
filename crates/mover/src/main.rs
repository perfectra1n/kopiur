//! kopiur-mover: the per-`Backup`/`Restore` Job binary (ADR §4.10).
//!
//! Flow:
//! 1. Read the work-spec path from arg/env, parse [`MoverWorkSpec`].
//! 2. Build a [`KopiaClient`], connect to the repository.
//! 3. Run the operation (backup / restore / snapshot-delete), emitting periodic
//!    progress PATCHes (interval configurable via the work spec).
//! 4. PATCH a terminal success/failure status onto the CR `.status` subresource
//!    via `kube::Api::patch_status`. On failure, write a structured failure
//!    block and exit non-zero.
//!
//! The pure mapping layer (work spec parsing, KopiaError → FailureBlock,
//! SnapshotCreateResult → status) lives in [`workspec`] and [`status`] and is
//! fully unit-tested without a cluster. The kube interaction here is
//! intentionally thin and best-effort.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use kopiur_kopia::{KopiaClient, KopiaError};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use kopiur_mover::bootstrap::{
    BootstrapResult, MAX_RETURNED_SNAPSHOTS, RESULT_CONFIGMAP_KEY, should_attempt_create,
};
use kopiur_mover::env::{KOPIA_BINARY, RESULT_CONFIGMAP, WORK_SPEC_PATH};
use kopiur_mover::status::StatusUpdate;
use kopiur_mover::workspec::{self, BootstrapRepositoryOp, MoverWorkSpec, Operation};

fn main() -> std::process::ExitCode {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    match runtime.block_on(run()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "mover run failed");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    // Tracing subscriber (fmt + OTLP traces/logs when configured). The mover is a
    // short-lived Job, so OTLP push is the right model for its metrics — we flush
    // both before returning.
    let _telemetry = kopiur_telemetry::init_tracing("kopiur-mover")?;
    let metrics = MoverMetrics::new();

    // Install the process-level rustls CryptoProvider before building any kube
    // client (the rustls-tls backend panics without it). Idempotent.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let spec_path = work_spec_path().context("locating work spec")?;
    let spec = load_work_spec(&spec_path)
        .with_context(|| format!("loading work spec from {}", spec_path.display()))?;
    let operation = spec.operation.kind_str().to_string();
    info!(
        operation = %operation,
        target = %spec.target_ref.name,
        namespace = %spec.target_ref.namespace,
        "loaded work spec"
    );

    let client = build_client(&spec);

    let started = std::time::Instant::now();
    // Bootstrap owns its own connect/create lifecycle (and reports via a result
    // ConfigMap, not the CR status); every other operation connects first, then
    // runs with periodic progress PATCHes.
    let result = match &spec.operation {
        Operation::BootstrapRepository(op) => run_bootstrap_flow(&client, &spec, op).await,
        _ => {
            // A best-effort status reporter. If we cannot build a kube client
            // (e.g. running outside a cluster), we log instead of failing.
            let reporter = StatusReporter::try_new(&spec).await;
            match client
                .repository_connect(&spec.repository.to_connect_spec())
                .await
            {
                Err(e) => terminal_failure(&reporter, &e).await,
                Ok(()) => match execute(&client, &spec, &reporter).await {
                    Ok(update) => {
                        reporter.report(&update).await;
                        info!(phase = %update.phase, "operation succeeded");
                        Ok(())
                    }
                    Err(e) => terminal_failure(&reporter, &e).await,
                },
            }
        }
    };

    // Push the operation outcome metric, then flush OTLP before the Job exits.
    let outcome = if result.is_ok() {
        "succeeded"
    } else {
        "failed"
    };
    metrics.record(&operation, outcome, started.elapsed().as_secs_f64());
    metrics.shutdown();

    result
}

/// Execute the work-spec operation, emitting periodic "Running" updates while
/// kopia works. Returns the terminal success update or the kopia error.
async fn execute(
    client: &KopiaClient,
    spec: &MoverWorkSpec,
    reporter: &StatusReporter,
) -> Result<StatusUpdate, KopiaError> {
    let interval = Duration::from_secs(spec.options.progress_interval_secs.max(1));

    // Spawn the operation as a future and tick progress alongside it.
    let op = run_operation(client, spec);
    tokio::pin!(op);

    let mut ticker = tokio::time::interval(interval);
    // First tick fires immediately; skip reporting until the period elapses.
    ticker.tick().await;

    loop {
        tokio::select! {
            result = &mut op => return result,
            _ = ticker.tick() => {
                reporter.report(&StatusUpdate::running(chrono::Utc::now())).await;
            }
        }
    }
}

/// Dispatch on the operation kind. Exhaustive `match` — a new [`Operation`]
/// variant fails to compile until handled (the project's type-safety thesis).
async fn run_operation(
    client: &KopiaClient,
    spec: &MoverWorkSpec,
) -> Result<StatusUpdate, KopiaError> {
    match &spec.operation {
        Operation::Backup(op) => {
            // Record the snapshot under the operator-resolved identity
            // (`username@hostname:sourcePath`), not the mover pod's ambient
            // user/host — ADR §4.2. The catalog, retention, and restore paths
            // all key on this identity.
            let id = &spec.identity;
            let override_source = format!("{}@{}:{}", id.username, id.hostname, id.source_path);
            let result = client
                .snapshot_create(&op.source_path, &op.tags, Some(&override_source))
                .await?;
            Ok(StatusUpdate::succeeded_backup(&result, chrono::Utc::now()))
        }
        Operation::Restore(op) => {
            client
                .snapshot_restore_with(&op.snapshot_id, &op.target_path, &op.restore_options())
                .await?;
            Ok(StatusUpdate::succeeded(chrono::Utc::now()))
        }
        Operation::SnapshotDelete(op) => {
            // Just delete the snapshot. Space reclamation (maintenance) is a
            // separate concern owned by the Maintenance CRD, not the mover.
            client.snapshot_delete(&op.snapshot_id).await?;
            Ok(StatusUpdate::succeeded(chrono::Utc::now()))
        }
        // Bootstrap is dispatched in `run()` before the connect+execute path; it
        // owns its own connect/create lifecycle and never reaches here. Named
        // explicitly (not `_`) so a future Operation variant still fails to
        // compile until handled (ADR §5.5).
        Operation::BootstrapRepository(_) => {
            unreachable!("BootstrapRepository is handled by run_bootstrap_flow, not execute()")
        }
    }
}

/// Drive a `BootstrapRepository` run: connect/create, write the result to the
/// work-spec ConfigMap (so the controller can read it even on failure), and
/// translate success/failure into the process exit code.
async fn run_bootstrap_flow(
    client: &KopiaClient,
    spec: &MoverWorkSpec,
    op: &BootstrapRepositoryOp,
) -> Result<()> {
    let result = run_bootstrap(client, spec, op).await;
    // Persist BEFORE returning: a failed bootstrap still exits non-zero (so the
    // Job is marked Failed and backoff is bounded), but the controller must be
    // able to read the structured failure to set an actionable Repository status.
    write_bootstrap_result(spec, &result).await;
    if result.success {
        info!(
            created = result.created,
            unique_id = ?result.unique_id,
            snapshot_count = result.snapshot_count,
            "repository bootstrap succeeded"
        );
        Ok(())
    } else {
        let class = result
            .failure
            .as_ref()
            .map(|f| f.kopia_error_class.as_str())
            .unwrap_or("Unknown");
        error!(class, "repository bootstrap failed terminally");
        Err(anyhow::anyhow!(
            "repository bootstrap failed (class {class})"
        ))
    }
}

/// The bootstrap routine: connect-first (adopt an existing repo), create only
/// when gated by [`should_attempt_create`], then read identity + catalog.
async fn run_bootstrap(
    client: &KopiaClient,
    spec: &MoverWorkSpec,
    op: &BootstrapRepositoryOp,
) -> BootstrapResult {
    let connect_spec = spec.repository.to_connect_spec();

    let mut created = false;
    if let Err(e) = client.repository_connect(&connect_spec).await {
        if !should_attempt_create(op.auto_create, e.class()) {
            // Either auto_create is off, or the failure means a repo already
            // exists (auth/locked) — surface it, never recreate.
            return BootstrapResult::failed(&e);
        }
        info!(class = %e.class(), "connect failed; attempting repository create");
        if let Err(ce) = client.repository_create(&connect_spec).await {
            return BootstrapResult::failed(&ce);
        }
        if let Err(ce) = client.repository_connect(&connect_spec).await {
            return BootstrapResult::failed(&ce);
        }
        created = true;
    }

    let unique_id = match client.repository_status().await {
        Ok(s) => Some(s.unique_id_hex),
        Err(e) => return BootstrapResult::failed(&e),
    };

    // Always list to report an authoritative snapshot count; return the entries
    // for materialization only when scanning is requested.
    let listing = match client.snapshot_list(None).await {
        Ok(l) => l,
        Err(e) => return BootstrapResult::failed(&e),
    };
    let snapshot_count = listing.len() as i64;
    let (snapshots, truncated) = if op.scan_catalog {
        let truncated = listing.len() > MAX_RETURNED_SNAPSHOTS;
        let mut s = listing;
        if truncated {
            s.truncate(MAX_RETURNED_SNAPSHOTS);
        }
        (s, truncated)
    } else {
        (Vec::new(), false)
    };
    if truncated {
        warn!(
            snapshot_count,
            returned = MAX_RETURNED_SNAPSHOTS,
            "more snapshots than the materialization cap; only the newest were returned"
        );
    }

    BootstrapResult::ready(created, unique_id, snapshot_count, snapshots, truncated)
}

/// Persist a [`BootstrapResult`] into the work-spec ConfigMap (best-effort). The
/// controller reads it from key [`RESULT_CONFIGMAP_KEY`].
async fn write_bootstrap_result(spec: &MoverWorkSpec, result: &BootstrapResult) {
    let cm_name = match std::env::var(RESULT_CONFIGMAP) {
        Ok(n) if !n.is_empty() => n,
        _ => {
            warn!("{RESULT_CONFIGMAP} unset; bootstrap result not persisted");
            return;
        }
    };
    let ns = &spec.target_ref.namespace;
    match write_result_configmap(&cm_name, ns, result).await {
        Ok(()) => info!(configmap = %cm_name, "wrote bootstrap result"),
        Err(e) => warn!(error = %e, configmap = %cm_name, "failed to write bootstrap result"),
    }
}

/// Merge-patch the result JSON into the ConfigMap's `data` (adds
/// [`RESULT_CONFIGMAP_KEY`] without disturbing the work-spec key).
async fn write_result_configmap(
    cm_name: &str,
    namespace: &str,
    result: &BootstrapResult,
) -> Result<()> {
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube::api::{Patch, PatchParams};

    let client = kube::Client::try_default().await?;
    let api: kube::Api<ConfigMap> = kube::Api::namespaced(client, namespace);
    let body = serde_json::json!({
        "data": { RESULT_CONFIGMAP_KEY: serde_json::to_string(result)? }
    });
    api.patch(
        cm_name,
        &PatchParams::apply("kopiur.home-operations.com/mover"),
        &Patch::Merge(&body),
    )
    .await?;
    Ok(())
}

/// Report a terminal failure (PATCH the failure block) and return an error so
/// `main` exits non-zero.
async fn terminal_failure(reporter: &StatusReporter, err: &KopiaError) -> Result<()> {
    let update = StatusUpdate::failed(err, chrono::Utc::now());
    reporter.report(&update).await;
    error!(
        class = %err.class(),
        retry = err.class().is_retryable(),
        "kopia operation failed terminally"
    );
    Err(anyhow::Error::new(CloneableKopiaError(err.to_string())))
}

/// A lightweight error wrapper so we can return the failure through `anyhow`
/// without requiring `KopiaError: Clone`.
#[derive(Debug)]
struct CloneableKopiaError(String);

impl std::fmt::Display for CloneableKopiaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CloneableKopiaError {}

/// Mover metrics, pushed over OTLP (when configured) before the Job exits. The
/// Prometheus pull endpoint is irrelevant for a short-lived Job, so this only
/// adds value with `OTEL_EXPORTER_OTLP_ENDPOINT` set.
struct MoverMetrics {
    provider: kopiur_telemetry::MetricsProvider,
    operations: opentelemetry::metrics::Counter<u64>,
    duration: opentelemetry::metrics::Histogram<f64>,
}

impl MoverMetrics {
    fn new() -> Self {
        let provider = kopiur_telemetry::MetricsProvider::new("kopiur-mover");
        let m = provider.meter();
        let operations = m
            .u64_counter("kopiur_mover_operations")
            .with_description("Total mover operations by kind and result.")
            .build();
        let duration = m
            .f64_histogram("kopiur_mover_operation_duration_seconds")
            .with_description("Mover operation wall-clock duration in seconds.")
            .build();
        MoverMetrics {
            provider,
            operations,
            duration,
        }
    }

    fn record(&self, operation: &str, result: &str, seconds: f64) {
        use opentelemetry::KeyValue;
        let attrs = [
            KeyValue::new("operation", operation.to_string()),
            KeyValue::new("result", result.to_string()),
        ];
        self.operations.add(1, &attrs);
        self.duration.record(seconds, &attrs);
    }

    fn shutdown(&self) {
        self.provider.shutdown();
    }
}

fn work_spec_path() -> Result<PathBuf> {
    if let Some(arg) = std::env::args().nth(1) {
        return Ok(PathBuf::from(arg));
    }
    if let Ok(env) = std::env::var(WORK_SPEC_PATH) {
        return Ok(PathBuf::from(env));
    }
    anyhow::bail!("no work spec path: pass it as the first arg or set {WORK_SPEC_PATH}")
}

fn load_work_spec(path: &PathBuf) -> Result<MoverWorkSpec> {
    let raw = std::fs::read_to_string(path)?;
    let spec: MoverWorkSpec = serde_json::from_str(&raw)?;
    Ok(spec)
}

fn build_client(spec: &MoverWorkSpec) -> KopiaClient {
    let mut builder = KopiaClient::builder();
    if let Ok(bin) = std::env::var(KOPIA_BINARY) {
        builder = builder.binary(bin);
    }
    // Suppress the GitHub update check globally.
    builder = builder.env("KOPIA_CHECK_FOR_UPDATES", "false");
    if let Some(t) = spec.options.operation_timeout_secs {
        builder = builder.default_timeout(Duration::from_secs(t));
    }
    builder.build()
}

/// A thin, best-effort wrapper around the kube status PATCH. Kept separate from
/// the pure mapping so `main`'s correctness lives in the unit-tested layers.
/// When no cluster is reachable, status updates are logged instead.
struct StatusReporter {
    inner: Option<Arc<Mutex<KubeStatusReporter>>>,
    target: workspec::TargetRef,
}

impl StatusReporter {
    async fn try_new(spec: &MoverWorkSpec) -> Self {
        let target = spec.target_ref.clone();
        match KubeStatusReporter::try_new(&target).await {
            Ok(r) => StatusReporter {
                inner: Some(Arc::new(Mutex::new(r))),
                target,
            },
            Err(e) => {
                warn!(
                    error = %e,
                    "no kube client; status updates will be logged, not PATCHed"
                );
                StatusReporter {
                    inner: None,
                    target,
                }
            }
        }
    }

    async fn report(&self, update: &StatusUpdate) {
        match &self.inner {
            Some(r) => {
                let mut guard = r.lock().await;
                if let Err(e) = guard.patch(update).await {
                    warn!(error = %e, target = %self.target.name, "status PATCH failed");
                }
            }
            None => {
                info!(
                    target = %self.target.name,
                    phase = %update.phase,
                    "status update (no cluster): {}",
                    serde_json::to_string(update).unwrap_or_default()
                );
            }
        }
    }
}

/// The real kube PATCH path. Uses a dynamic API so the mover does not need to
/// depend on the typed CRD structs (it PATCHes a merge body under `.status`).
struct KubeStatusReporter {
    api: kube::Api<kube::api::DynamicObject>,
    name: String,
}

impl KubeStatusReporter {
    async fn try_new(target: &workspec::TargetRef) -> Result<Self> {
        use kube::core::{ApiResource, GroupVersionKind};

        let client = kube::Client::try_default().await?;
        let (group, version) = split_api_version(&target.api_version);
        let gvk = GroupVersionKind::gvk(&group, &version, &target.kind);
        let ar = ApiResource::from_gvk(&gvk);
        let api =
            kube::Api::<kube::api::DynamicObject>::namespaced_with(client, &target.namespace, &ar);
        Ok(KubeStatusReporter {
            api,
            name: target.name.clone(),
        })
    }

    async fn patch(&mut self, update: &StatusUpdate) -> Result<()> {
        use kube::api::{Patch, PatchParams};
        let body = update.as_patch_body();
        self.api
            .patch_status(&self.name, &PatchParams::default(), &Patch::Merge(&body))
            .await?;
        Ok(())
    }
}

/// Split `group/version` (or bare `version`) into `(group, version)`.
fn split_api_version(api_version: &str) -> (String, String) {
    match api_version.split_once('/') {
        Some((g, v)) => (g.to_string(), v.to_string()),
        None => (String::new(), api_version.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_api_version_grouped() {
        assert_eq!(
            split_api_version("kopiur.home-operations.com/v1alpha1"),
            (
                "kopiur.home-operations.com".to_string(),
                "v1alpha1".to_string()
            )
        );
    }

    #[test]
    fn split_api_version_core() {
        assert_eq!(split_api_version("v1"), (String::new(), "v1".to_string()));
    }
}
