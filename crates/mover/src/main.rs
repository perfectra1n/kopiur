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

use kopiur_mover::status::StatusUpdate;
use kopiur_mover::workspec::{self, MoverWorkSpec, Operation};

/// Env var naming the work-spec file path (downward-API/ConfigMap mount).
const WORK_SPEC_ENV: &str = "KOPIUR_WORK_SPEC_PATH";
/// Env var overriding the kopia binary path (defaults to `kopia` on PATH).
const KOPIA_BINARY_ENV: &str = "KOPIUR_KOPIA_BINARY";

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
    init_tracing();

    // Install the process-level rustls CryptoProvider before building any kube
    // client (the rustls-tls backend panics without it). Idempotent.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let spec_path = work_spec_path().context("locating work spec")?;
    let spec = load_work_spec(&spec_path)
        .with_context(|| format!("loading work spec from {}", spec_path.display()))?;
    info!(
        operation = spec.operation.kind_str(),
        target = %spec.target_ref.name,
        namespace = %spec.target_ref.namespace,
        "loaded work spec"
    );

    let client = build_client(&spec);

    // A best-effort status reporter. If we cannot build a kube client (e.g.
    // running outside a cluster), we log instead of failing the operation.
    let reporter = StatusReporter::try_new(&spec).await;

    // Connect to the repository first (short, idempotent).
    if let Err(e) = client
        .repository_connect(&spec.repository.to_connect_spec())
        .await
    {
        return terminal_failure(&reporter, &e).await;
    }

    // Run the operation with periodic progress reporting.
    let outcome = execute(&client, &spec, &reporter).await;

    match outcome {
        Ok(update) => {
            reporter.report(&update).await;
            info!(phase = %update.phase, "operation succeeded");
            Ok(())
        }
        Err(e) => terminal_failure(&reporter, &e).await,
    }
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
                .snapshot_restore(&op.snapshot_id, &op.target_path)
                .await?;
            Ok(StatusUpdate::succeeded(chrono::Utc::now()))
        }
        Operation::SnapshotDelete(op) => {
            // Just delete the snapshot. Space reclamation (maintenance) is a
            // separate concern owned by the Maintenance CRD, not the mover.
            client.snapshot_delete(&op.snapshot_id).await?;
            Ok(StatusUpdate::succeeded(chrono::Utc::now()))
        }
    }
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

fn init_tracing() {
    // Best-effort: ignore an error if a global subscriber is already set (e.g.
    // in tests).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

fn work_spec_path() -> Result<PathBuf> {
    if let Some(arg) = std::env::args().nth(1) {
        return Ok(PathBuf::from(arg));
    }
    if let Ok(env) = std::env::var(WORK_SPEC_ENV) {
        return Ok(PathBuf::from(env));
    }
    anyhow::bail!("no work spec path: pass it as the first arg or set {WORK_SPEC_ENV}")
}

fn load_work_spec(path: &PathBuf) -> Result<MoverWorkSpec> {
    let raw = std::fs::read_to_string(path)?;
    let spec: MoverWorkSpec = serde_json::from_str(&raw)?;
    Ok(spec)
}

fn build_client(spec: &MoverWorkSpec) -> KopiaClient {
    let mut builder = KopiaClient::builder();
    if let Ok(bin) = std::env::var(KOPIA_BINARY_ENV) {
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
            split_api_version("kopia.io/v1alpha1"),
            ("kopia.io".to_string(), "v1alpha1".to_string())
        );
    }

    #[test]
    fn split_api_version_core() {
        assert_eq!(split_api_version("v1"), (String::new(), "v1".to_string()));
    }
}
