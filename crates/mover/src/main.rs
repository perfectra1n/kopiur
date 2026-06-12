//! kopiur-mover: the per-`Snapshot`/`Restore` Job binary (ADR §4.10).
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

use kopiur_api::{LeaseAction, lease_action};
use kopiur_kopia::{ConnectSpec, KopiaClient, KopiaError, KopiaErrorClass, MaintenanceMode};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use kopiur_mover::bootstrap::{
    BootstrapResult, MAX_RETURNED_SNAPSHOTS, RESULT_CONFIGMAP_KEY, should_attempt_create,
};
use kopiur_mover::credentials;
use kopiur_mover::env::{KOPIA_BINARY, RESULT_CONFIGMAP, WORK_SPEC_PATH};
use kopiur_mover::error::{KopiaOp, MoverError, Result};
use kopiur_mover::status::StatusUpdate;
use kopiur_mover::workspec::{
    self, BootstrapRepositoryOp, BrowseSessionOp, KOPIUR_PIN_NAME, MaintenanceOp, MoverWorkSpec,
    Operation, ReplicateOp, VerifyOp, VerifyTier,
};

fn main() -> std::process::ExitCode {
    // Readiness-probe mode, BEFORE the work-spec loading path: a browse-session
    // pod's readinessProbe execs `kopiur-mover ready` (the distroless image has
    // no shell to `test -f` with), which must exit 0 iff the session marker
    // exists. Checked first so the probe never tries to parse "ready" as a
    // work-spec path; the decision itself is the pure `session_ready`.
    if std::env::args().nth(1).as_deref() == Some("ready") {
        return if session_ready(std::path::Path::new(kopiur_mover::env::READY_MARKER)) {
            std::process::ExitCode::SUCCESS
        } else {
            std::process::ExitCode::FAILURE
        };
    }

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    match runtime.block_on(run()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "mover run failed");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Whether the browse-session readiness marker exists — the entire decision the
/// `kopiur-mover ready` probe mode maps to an exit code. Pure over the path so
/// it is unit-testable.
fn session_ready(marker: &std::path::Path) -> bool {
    marker.exists()
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

    let spec_path = work_spec_path()?;
    let spec = load_work_spec(&spec_path)?;
    let operation = spec.operation.kind_str().to_string();
    info!(
        operation = %operation,
        target = %spec.target_ref.name,
        namespace = %spec.target_ref.namespace,
        "loaded work spec"
    );

    let client = build_client(&spec);

    let started = std::time::Instant::now();
    // Build the connect spec once, materializing any file-based backend
    // credentials (SFTP key/known_hosts, GCS service-account JSON, rclone.conf)
    // from the environment into files the kopia subprocess can read. Every flow
    // below connects with this prepared spec.
    let result = match prepare_connect_spec(&spec) {
        Err(e) => {
            error!(error = %e, "failed to materialize backend credentials for the mover");
            Err(e)
        }
        // Bootstrap owns its own connect/create lifecycle (and reports via a
        // result ConfigMap, not the CR status); every other operation connects
        // first, then runs with periodic progress PATCHes.
        Ok(connect) => match &spec.operation {
            Operation::BootstrapRepository(op) => {
                run_bootstrap_flow(&client, &spec, op, &connect).await
            }
            // Maintenance, like bootstrap, owns its own connect lifecycle: the
            // lease decision needs `kopia maintenance info`, which requires repo
            // access the controller does not have for object stores (ADR §3.7/§5.4).
            Operation::Maintenance(op) => run_maintenance_flow(&client, &spec, op, &connect).await,
            // Verify, like maintenance, owns its own connect lifecycle and PATCHes
            // the SnapshotPolicy `.status` directly (ADR-0005 §4).
            Operation::Verify(op) => run_verify_flow(&client, &spec, op, &connect).await,
            // Replicate connects to the source, then `repository sync-to` the
            // destination; PATCHes the RepositoryReplication `.status` (ADR-0005 §13(d)).
            Operation::Replicate(op) => run_replicate_flow(&client, &spec, op, &connect).await,
            // BrowseSession owns its own (read-only) connect lifecycle and has
            // no status to PATCH — its targetRef names nothing the controller
            // owns; the CLI surfaces failures from the pod logs.
            Operation::BrowseSession(op) => {
                run_browse_session_flow(&client, &spec, op, &connect).await
            }
            _ => {
                // A best-effort status reporter. If we cannot build a kube client
                // (e.g. running outside a cluster), we log instead of failing.
                let reporter = StatusReporter::try_new(&spec).await;
                match client.repository_connect(&connect, spec.cache).await {
                    Err(e) => {
                        terminal_failure(
                            &reporter,
                            MoverError::Kopia {
                                op: KopiaOp::RepositoryConnect,
                                source: e,
                            },
                        )
                        .await
                    }
                    Ok(()) => {
                        // Apply repository throttle (moverDefaults.throttle, ADR-0005
                        // §13(e)) after connecting, before the data op. A throttle
                        // failure is terminal: an un-throttled run could saturate the
                        // link the user explicitly capped.
                        if !spec.throttle.is_empty()
                            && let Err(e) = client
                                .repository_throttle_set(&spec.throttle.to_kopia())
                                .await
                        {
                            return terminal_failure(
                                &reporter,
                                MoverError::Kopia {
                                    op: KopiaOp::ThrottleSet,
                                    source: e,
                                },
                            )
                            .await;
                        }
                        match execute(&client, &spec, &reporter).await {
                            Ok(update) => {
                                reporter.report(&update).await;
                                info!(phase = %update.phase, "operation succeeded");
                                Ok(())
                            }
                            Err(e) => terminal_failure(&reporter, e).await,
                        }
                    }
                }
            }
        },
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

/// Directory under the writable kopia-cache `emptyDir` where the mover stages
/// file-based backend credentials (SFTP key/known_hosts, GCS JSON, rclone.conf).
/// Shares the cache mount so it is writable on the read-only-root mover pod.
fn credential_staging_dir() -> PathBuf {
    PathBuf::from(kopiur_kopia::env::DEFAULT_CACHE_DIR).join("creds")
}

/// Build the repository [`ConnectSpec`] for this run, first materializing any
/// file-based backend credentials (SFTP/GCS/rclone) from the environment into
/// files under [`credential_staging_dir`]. Env-only backends (S3/Azure/B2/WebDAV)
/// pass through unchanged.
fn prepare_connect_spec(spec: &MoverWorkSpec) -> Result<ConnectSpec> {
    let mut connect = spec.repository.to_connect_spec();
    credentials::materialize(&mut connect, &credential_staging_dir())?;
    Ok(connect)
}

/// Execute the work-spec operation, emitting periodic "Running" updates while
/// kopia works. Returns the terminal success update or the kopia error.
async fn execute(
    client: &KopiaClient,
    spec: &MoverWorkSpec,
    reporter: &StatusReporter,
) -> Result<StatusUpdate> {
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
async fn run_operation(client: &KopiaClient, spec: &MoverWorkSpec) -> Result<StatusUpdate> {
    // Each kopia call is wrapped with the `KopiaOp` naming it, so a failure's
    // message/log always says *which* invocation failed.
    let kopia = |op: KopiaOp| move |source: KopiaError| MoverError::Kopia { op, source };
    match &spec.operation {
        Operation::Snapshot(op) => {
            // Record the snapshot under the operator-resolved identity
            // (`username@hostname:sourcePath`), not the mover pod's ambient
            // user/host — ADR §4.2. The catalog, retention, and restore paths
            // all key on this identity.
            let id = &spec.identity;
            let override_source = format!("{}@{}:{}", id.username, id.hostname, id.source_path);
            // Apply the resolved kopia `policy set` knobs (compression / never-compress
            // / ignore rules / ignore-cache-dirs / backup-side error handling / upload
            // parallelism / extraArgs) against this snapshot's source identity BEFORE
            // creating the snapshot, so SnapshotPolicy.spec.{compression,files,
            // errorHandling,upload,extraArgs} actually reach kopia (ADR-0005 §13(b)/§13(f),
            // ADR-0004 §4b). Skipped when nothing is configured.
            if !op.policy.is_empty() {
                // kopia rejects `--max-parallel-snapshots` on a path-scoped
                // policy ("only global, username@hostname or @hostname"), so
                // that one knob is applied in a second `policy set` at the
                // identity scope (the policy_knobs e2e regression).
                let (path_policy, identity_policy) =
                    kopiur_kopia::split_policy_scopes(op.policy.to_kopia());
                client
                    .policy_set(&override_source, &path_policy)
                    .await
                    .map_err(kopia(KopiaOp::PolicySet))?;
                if let Some(p) = identity_policy {
                    let identity_scope = format!("{}@{}", id.username, id.hostname);
                    client
                        .policy_set(&identity_scope, &p)
                        .await
                        .map_err(kopia(KopiaOp::PolicySet))?;
                }
            }
            let result = client
                .snapshot_create(&op.source_path, &op.tags, Some(&override_source))
                .await
                .map_err(kopia(KopiaOp::SnapshotCreate))?;
            Ok(StatusUpdate::succeeded_backup(&result, chrono::Utc::now()))
        }
        Operation::Restore(op) => {
            client
                .snapshot_restore_with(&op.snapshot_id, &op.target_path, &op.restore_options())
                .await
                .map_err(kopia(KopiaOp::SnapshotRestore))?;
            // Restore's terminal success phase is `Completed`, not `Succeeded`
            // (the Snapshot phase) — the Restore CRD enum rejects `Succeeded`.
            Ok(StatusUpdate::completed(&op.snapshot_id, chrono::Utc::now()))
        }
        Operation::SnapshotDelete(op) => {
            // Just delete the snapshot. Space reclamation (maintenance) is a
            // separate concern owned by the Maintenance CRD, not the mover.
            client
                .snapshot_delete(&op.snapshot_id)
                .await
                .map_err(kopia(KopiaOp::SnapshotDelete))?;
            Ok(StatusUpdate::succeeded(chrono::Utc::now()))
        }
        Operation::SnapshotPin(op) => {
            // Reconcile kopia's pin state with Snapshot.spec.pin (ADR-0005 §13(c))
            // so kopia's own maintenance/expire honors the pin on object stores.
            // Idempotent: kopia treats a redundant add/remove as a no-op.
            if op.pin {
                client
                    .snapshot_pin(&op.snapshot_id, KOPIUR_PIN_NAME)
                    .await
                    .map_err(kopia(KopiaOp::SnapshotPin))?;
            } else {
                client
                    .snapshot_unpin(&op.snapshot_id, KOPIUR_PIN_NAME)
                    .await
                    .map_err(kopia(KopiaOp::SnapshotPin))?;
            }
            Ok(StatusUpdate::succeeded(chrono::Utc::now()))
        }
        // Bootstrap, Maintenance, and Verify are dispatched in `run()` before the
        // connect+execute path; they own their own connect lifecycle and never
        // reach here. Named explicitly (not `_`) so a future Operation variant
        // still fails to compile until handled (ADR §5.5).
        Operation::BootstrapRepository(_) => {
            unreachable!("BootstrapRepository is handled by run_bootstrap_flow, not execute()")
        }
        Operation::Maintenance(_) => {
            unreachable!("Maintenance is handled by run_maintenance_flow, not execute()")
        }
        Operation::Verify(_) => {
            unreachable!("Verify is handled by run_verify_flow, not execute()")
        }
        Operation::Replicate(_) => {
            unreachable!("Replicate is handled by run_replicate_flow, not execute()")
        }
        Operation::BrowseSession(_) => {
            unreachable!("BrowseSession is handled by run_browse_session_flow, not execute()")
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
    connect: &ConnectSpec,
) -> Result<()> {
    info!(
        backend = spec.repository.kind_str(),
        auto_create = op.auto_create,
        repository = %spec.target_ref.name,
        "bootstrapping repository"
    );
    let result = run_bootstrap(client, op, connect).await;
    // Persist BEFORE returning: a failed bootstrap still exits non-zero (so the
    // Job is marked Failed and backoff is bounded), but the controller must be
    // able to read the structured failure to set an actionable Repository status.
    write_bootstrap_result(spec, &result).await;
    if result.success {
        info!(
            backend = spec.repository.kind_str(),
            created = result.created,
            unique_id = ?result.unique_id,
            snapshot_count = result.snapshot_count,
            "repository bootstrap succeeded"
        );
        Ok(())
    } else {
        // Surface the full failure on stdout (class + human message + the kopia
        // stderr tail) so `kubectl logs` on the bootstrap Job tells the whole
        // story without needing the result ConfigMap.
        let (class, message, stderr_tail) = result
            .failure
            .as_ref()
            .map(|f| {
                (
                    f.kopia_error_class.as_str(),
                    f.message.as_str(),
                    f.stderr_tail.as_deref().unwrap_or(""),
                )
            })
            .unwrap_or(("Unknown", "repository bootstrap failed", ""));
        error!(
            backend = spec.repository.kind_str(),
            class, stderr_tail, "repository bootstrap failed terminally: {message}"
        );
        Err(MoverError::BootstrapFailed {
            class: KopiaErrorClass::from_label(class),
            message: message.to_string(),
        })
    }
}

/// The bootstrap routine: connect-first (adopt an existing repo), create only
/// when gated by [`should_attempt_create`], then read identity + catalog.
async fn run_bootstrap(
    client: &KopiaClient,
    op: &BootstrapRepositoryOp,
    connect: &ConnectSpec,
) -> BootstrapResult {
    let connect_spec = connect.clone();

    // Bootstrap (repo adopt/create) is a controller-driven probe, not a data run, so
    // it connects with kopia's default cache budgets.
    let cache = kopiur_kopia::CacheTuning::default();
    let mut created = false;
    if let Err(e) = client.repository_connect(&connect_spec, cache).await {
        if !should_attempt_create(op.auto_create, e.class()) {
            // We will NOT create. Two distinct decline reasons → two distinct,
            // accurate messages:
            //  - create opt-out (`auto_create` off) + a genuinely-absent repo
            //    (`NotFound`) ⇒ actionable "set spec.create.enabled: true": the repo
            //    just needs initializing. Scoped to `NotFound` only — an unreachable
            //    backend (`RepositoryUnavailable`) or a denied bucket (`AccessDenied`)
            //    is NOT "uninitialized", and telling the user to enable create there
            //    would be wrong advice.
            //  - everything else (a repo exists we can't open — auth/locked; an
            //    access/permission problem; or auto_create on but blocked) ⇒ surface
            //    the real kopia class; recreating would mask it or risk a 2nd repo.
            if !op.auto_create && e.class() == KopiaErrorClass::NotFound {
                return BootstrapResult::not_initialized();
            }
            return BootstrapResult::failed(&e);
        }
        info!(class = %e.class(), "connect failed; attempting repository create");
        if let Err(ce) = client
            .repository_create(&connect_spec, cache, &op.create_options())
            .await
        {
            return BootstrapResult::failed(&ce);
        }
        if let Err(ce) = client.repository_connect(&connect_spec, cache).await {
            return BootstrapResult::failed(&ce);
        }
        created = true;
        // Stamp the stable, lease-derived maintenance owner on a repo we just
        // CREATED (kopia auto-assigned this pod's ephemeral identity). Without
        // this, every maintenance mover sees a foreign owner and a
        // `takeoverPolicy: Never` yields forever. Best-effort: a failed stamp
        // is recoverable later via takeoverPolicy=Force (degrade-not-crash).
        if let Some(owner) = &op.maintenance_owner {
            match client.maintenance_set_owner(owner).await {
                Ok(()) => info!(%owner, "stamped maintenance owner on created repository"),
                Err(e) => warn!(
                    %owner,
                    class = %e.class(),
                    "could not stamp maintenance owner on created repository; \
                     maintenance will need takeoverPolicy=Force once"
                ),
            }
        }
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

    let client = kube::Client::try_default()
        .await
        .map_err(|source| MoverError::KubeClient {
            source: Box::new(source),
        })?;
    let api: kube::Api<ConfigMap> = kube::Api::namespaced(client, namespace);
    let body = serde_json::json!({
        "data": {
            RESULT_CONFIGMAP_KEY: serde_json::to_string(result)
                .map_err(|source| MoverError::ResultSerialize { source })?
        }
    });
    api.patch(
        cm_name,
        &PatchParams::apply("kopiur.home-operations.com/mover"),
        &Patch::Merge(&body),
    )
    .await
    .map_err(|source| MoverError::ResultConfigMapPatch {
        configmap: cm_name.to_string(),
        namespace: namespace.to_string(),
        source: Box::new(source),
    })?;
    Ok(())
}

/// Drive a `Maintenance` run: connect, read the ownership lease, apply the
/// takeover policy, run `kopia maintenance run` when we hold the lease, and PATCH
/// the `Maintenance` `.status` directly (ADR §3.7). Returns an error (non-zero
/// exit → Job `Failed`) only when a kopia call fails; a *yield* (lease held by
/// another owner under a non-`Force` policy) is a successful no-op run.
async fn run_maintenance_flow(
    client: &KopiaClient,
    spec: &MoverWorkSpec,
    op: &MaintenanceOp,
    connect: &ConnectSpec,
) -> Result<()> {
    info!(
        backend = spec.repository.kind_str(),
        mode = ?op.mode,
        maintenance = %spec.target_ref.name,
        "running maintenance"
    );
    // Connect first: for object stores this pod is the only place with repo
    // access, which is exactly why the lease decision is made here.
    if let Err(e) = client.repository_connect(connect, spec.cache).await {
        patch_maintenance_status(&spec.target_ref, &maintenance_failed_body(&e)).await;
        error!(class = %e.class(), "maintenance connect failed");
        return Err(MoverError::Kopia {
            op: KopiaOp::MaintenanceConnect,
            source: e,
        });
    }

    // Assume the STABLE lease-derived client identity before anything else:
    // this pod's own user@hostname is ephemeral (a fresh pod every run), so
    // kopia's recorded owner can only ever be compared against — and claimed
    // as — the stable identity. (kopia 0.23 has no identity override at
    // connect; `repository set-client` flips it post-connect.)
    let (lease_user, lease_host) = kopiur_api::maintenance::kopia_lease_identity(&op.owner);
    if let Err(e) = client
        .repository_set_client_identity(&lease_user, &lease_host)
        .await
    {
        patch_maintenance_status(&spec.target_ref, &maintenance_failed_body(&e)).await;
        error!(class = %e.class(), "maintenance set-client identity failed");
        return Err(MoverError::Kopia {
            op: KopiaOp::MaintenanceConnect,
            source: e,
        });
    }

    // Read the current lease holder and apply the takeover policy.
    let info = match client.maintenance_info().await {
        Ok(i) => i,
        Err(e) => {
            patch_maintenance_status(&spec.target_ref, &maintenance_failed_body(&e)).await;
            error!(class = %e.class(), "maintenance info failed");
            return Err(MoverError::Kopia {
                op: KopiaOp::MaintenanceInfo,
                source: e,
            });
        }
    };
    // Held by another when kopia's recorded owner is neither empty nor OUR
    // stable identity. Comparing against `op.owner` (the logical lease string,
    // never a kopia user@hostname) was the bug that made every run on a
    // mover-bootstrapped repo yield forever.
    let my_owner = kopiur_api::maintenance::kopia_owner_for_lease(&op.owner);
    let held_by_other = !info.owner.is_empty() && info.owner != my_owner;
    match lease_action(op.takeover_policy, held_by_other) {
        LeaseAction::Yield => {
            patch_maintenance_status(
                &spec.target_ref,
                &lease_blocked_body(
                    &info.owner,
                    "LeaseHeldByOther",
                    &format!(
                        "maintenance lease held by {}; takeoverPolicy=Never",
                        info.owner
                    ),
                ),
            )
            .await;
            info!(owner = %info.owner, "maintenance lease held by another owner; yielding");
            Ok(())
        }
        LeaseAction::Prompt => {
            patch_maintenance_status(
                &spec.target_ref,
                &lease_blocked_body(
                    &info.owner,
                    "LeaseTakeoverPrompt",
                    &format!(
                        "lease held by {}; set takeoverPolicy=Force to claim",
                        info.owner
                    ),
                ),
            )
            .await;
            info!(owner = %info.owner, "maintenance lease held; prompting for takeover");
            Ok(())
        }
        action @ (LeaseAction::Claim | LeaseAction::Takeover) => {
            // Claim kopia's maintenance ownership for THIS pod's identity first.
            // kopia rejects `maintenance run` from anyone but the designated owner,
            // and a repo the controller bootstrapped in-process is owned by the
            // controller's identity — so without this the run fails with
            // "maintenance must be run by designated user: …". The operator's own
            // lease (decided above via op.owner/takeover_policy) is the real
            // coordination; this just satisfies kopia's per-connection guard.
            if let Err(e) = client.maintenance_set_owner_me().await {
                patch_maintenance_status(&spec.target_ref, &maintenance_failed_body(&e)).await;
                error!(class = %e.class(), "maintenance ownership claim failed");
                return Err(MoverError::Kopia {
                    op: KopiaOp::MaintenanceSetOwner,
                    source: e,
                });
            }
            if let Err(e) = client.maintenance_run(op.mode).await {
                patch_maintenance_status(&spec.target_ref, &maintenance_failed_body(&e)).await;
                error!(class = %e.class(), "maintenance run failed");
                return Err(MoverError::Kopia {
                    op: KopiaOp::MaintenanceRun,
                    source: e,
                });
            }
            patch_maintenance_status(
                &spec.target_ref,
                &maintenance_ran_body(op, &chrono::Utc::now()),
            )
            .await;
            info!(?action, mode = ?op.mode, "maintenance run succeeded");
            Ok(())
        }
    }
}

/// Drive a `Verify` run (ADR-0005 §4): connect, run the quick (`kopia snapshot
/// verify`) or deep (scratch-restore) tier, evaluate the optional CEL `successExpr`
/// over the result, and PATCH the `SnapshotPolicy` `.status.lastVerified` on
/// success. Owns its own connect lifecycle like maintenance. Returns an error
/// (non-zero exit → Job `Failed`) when a kopia call fails or `successExpr` rejects.
async fn run_verify_flow(
    client: &KopiaClient,
    spec: &MoverWorkSpec,
    op: &VerifyOp,
    connect: &ConnectSpec,
) -> Result<()> {
    info!(
        backend = spec.repository.kind_str(),
        tier = op.tier.kind_str(),
        policy = %spec.target_ref.name,
        "running verification"
    );
    if let Err(e) = client.repository_connect(connect, spec.cache).await {
        patch_verify_status(&spec.target_ref, &verify_failed_body(&e.to_string())).await;
        error!(class = %e.class(), "verify connect failed");
        return Err(MoverError::Kopia {
            op: KopiaOp::VerifyConnect,
            source: e,
        });
    }

    // Run the tier and collect the result environment for successExpr. A kopia
    // failure is terminal; a clean run yields the stats the predicate inspects.
    let (stats, restored) = match &op.tier {
        VerifyTier::Quick(q) => {
            if let Err(e) = client.snapshot_verify(&q.to_kopia()).await {
                patch_verify_status(&spec.target_ref, &verify_failed_body(&e.to_string())).await;
                error!(class = %e.class(), "snapshot verify failed");
                return Err(MoverError::Kopia {
                    op: KopiaOp::SnapshotVerify,
                    source: e,
                });
            }
            // kopia `snapshot verify` reports no machine-readable file/byte counts on
            // stdout, so we conservatively report 0/0/0 for the predicate environment
            // and rely on the exit code for the integrity verdict. (A future kopia
            // JSON surface can populate real counts.)
            (kopiur_api::VerifyStats::default(), None)
        }
        VerifyTier::Deep(d) => {
            // Resolve the snapshot id to restore: the controller's choice, else the
            // newest snapshot for this identity.
            let id = match &d.snapshot_id {
                Some(id) => id.clone(),
                None => match resolve_latest_snapshot_id(client, spec).await {
                    Ok(Some(id)) => id,
                    Ok(None) => {
                        let err = MoverError::VerifyNoSnapshot {
                            source_path: spec.identity.source_path.clone(),
                        };
                        patch_verify_status(
                            &spec.target_ref,
                            &verify_failed_body(&err.to_string()),
                        )
                        .await;
                        return Err(err);
                    }
                    Err(e) => {
                        patch_verify_status(&spec.target_ref, &verify_failed_body(&e.to_string()))
                            .await;
                        return Err(MoverError::Kopia {
                            op: KopiaOp::DeepVerifySnapshotList,
                            source: e,
                        });
                    }
                },
            };
            if let Err(e) = client.snapshot_restore(&id, &d.scratch_path).await {
                patch_verify_status(&spec.target_ref, &verify_failed_body(&e.to_string())).await;
                error!(class = %e.class(), "deep verify scratch-restore failed");
                return Err(MoverError::Kopia {
                    op: KopiaOp::DeepVerifyRestore,
                    source: e,
                });
            }
            // Count what the scratch-restore produced so `restored.files`/`stats.files`
            // are meaningful to a successExpr. A read failure here is non-fatal: we
            // treat the restore exit code as authoritative and report 0.
            let files = count_files(&d.scratch_path).unwrap_or(0);
            (
                kopiur_api::VerifyStats {
                    files,
                    bytes: 0,
                    errors: 0,
                },
                Some(kopiur_api::RestoredStats {
                    files,
                    checksum_matches: true,
                }),
            )
        }
    };

    // Evaluate the optional CEL successExpr over the result — killing the silent
    // "0 files" success when the user opted in.
    if let Some(expr) = &op.success_expr {
        let snapshot = std::collections::BTreeMap::new();
        let inputs = kopiur_api::SuccessExprInputs {
            stats,
            snapshot,
            restored,
            _marker: std::marker::PhantomData,
        };
        match kopiur_api::eval_success_expr(expr, &inputs) {
            Ok(true) => {}
            Ok(false) => {
                let err = MoverError::SuccessExprFalse { expr: expr.clone() };
                let msg = err.to_string();
                patch_verify_status(&spec.target_ref, &verify_failed_body(&msg)).await;
                warn!("{msg}");
                return Err(err);
            }
            Err(e) => {
                let err = MoverError::SuccessExprEval { source: e };
                patch_verify_status(&spec.target_ref, &verify_failed_body(&err.to_string())).await;
                return Err(err);
            }
        }
    }

    patch_verify_status(
        &spec.target_ref,
        &verify_ok_body(op.tier.kind_str(), &chrono::Utc::now()),
    )
    .await;
    info!(tier = op.tier.kind_str(), "verification succeeded");
    Ok(())
}

/// The newest snapshot id for this run's identity, by source path (the kopia
/// catalog records the path authoritatively; the pod's user/host differ).
async fn resolve_latest_snapshot_id(
    client: &KopiaClient,
    spec: &MoverWorkSpec,
) -> Result<Option<String>, KopiaError> {
    let mut list = client.snapshot_list(None).await?;
    list.sort_by_key(|e| std::cmp::Reverse(e.end_time));
    let path = &spec.identity.source_path;
    Ok(list
        .into_iter()
        .find(|e| e.source.path == *path)
        .map(|e| e.id))
}

/// Best-effort recursive file count under `dir` for the deep-verify result
/// environment. Returns `None` on any IO error (the caller treats it as 0).
fn count_files(dir: &str) -> Option<i64> {
    fn walk(dir: &std::path::Path, count: &mut i64) -> std::io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let ft = entry.file_type()?;
            if ft.is_dir() {
                walk(&entry.path(), count)?;
            } else if ft.is_file() {
                *count += 1;
            }
        }
        Ok(())
    }
    let mut count = 0i64;
    walk(std::path::Path::new(dir), &mut count).ok()?;
    Some(count)
}

/// `{ "status": ... }` body for a successful verification: stamp `lastVerified`
/// and a `Verified=True` condition.
fn verify_ok_body(tier: &str, now: &chrono::DateTime<chrono::Utc>) -> serde_json::Value {
    let ts = now.to_rfc3339();
    serde_json::json!({
        "status": {
            "lastVerified": ts,
            "conditions": [{
                "type": "Verified",
                "status": "True",
                "reason": "VerificationSucceeded",
                "message": format!("{tier} verification succeeded"),
                "lastTransitionTime": ts,
                "observedGeneration": 0,
            }],
        }
    })
}

/// `{ "status": ... }` body for a failed verification: a `Verified=False` condition.
fn verify_failed_body(message: &str) -> serde_json::Value {
    serde_json::json!({
        "status": {
            "conditions": [{
                "type": "Verified",
                "status": "False",
                "reason": "VerificationFailed",
                "message": message,
                "lastTransitionTime": chrono::Utc::now().to_rfc3339(),
                "observedGeneration": 0,
            }],
        }
    })
}

/// PATCH a raw `{ "status": ... }` merge body onto the `SnapshotPolicy` `.status`
/// (best-effort; logged on failure). Reuses the same dynamic-API pattern as
/// [`patch_maintenance_status`].
async fn patch_verify_status(target: &workspec::TargetRef, body: &serde_json::Value) {
    patch_maintenance_status(target, body).await;
}

/// Drive a `Replicate` run (ADR-0005 §13(d)): connect to the *source* repository,
/// then `kopia repository sync-to <destination>` to mirror its blobs to the
/// destination backend. PATCHes the `RepositoryReplication` `.status`. Owns its own
/// connect lifecycle like maintenance. Returns an error (non-zero exit → Job
/// `Failed`) when a kopia call fails.
async fn run_replicate_flow(
    client: &KopiaClient,
    spec: &MoverWorkSpec,
    op: &ReplicateOp,
    connect: &ConnectSpec,
) -> Result<()> {
    info!(
        source_backend = spec.repository.kind_str(),
        destination_backend = op.destination.kind_str(),
        replication = %spec.target_ref.name,
        "replicating repository"
    );
    // Connect to the source first (this pod is the only place with repo access for
    // object stores — the same rationale as maintenance/verify).
    if let Err(e) = client.repository_connect(connect, spec.cache).await {
        patch_replicate_status(&spec.target_ref, &replicate_failed_body(&e.to_string())).await;
        error!(class = %e.class(), "replication source connect failed");
        return Err(MoverError::Kopia {
            op: KopiaOp::ReplicateConnect,
            source: e,
        });
    }

    // Materialize the DESTINATION's file-based credentials (SFTP key/GCS JSON/rclone)
    // into a separate staging dir so they don't collide with the source's, then run
    // sync-to. Env-only destinations (S3/Azure/B2/WebDAV/filesystem) pass through.
    let mut dest = op.destination.to_connect_spec();
    if let Err(e) = credentials::materialize(&mut dest, &credential_staging_dir().join("dest")) {
        // The CredentialWrite/CredentialStagingDir variants already name the env
        // key, path, and fix — propagate them untouched.
        patch_replicate_status(&spec.target_ref, &replicate_failed_body(&e.to_string())).await;
        return Err(e);
    }

    if let Err(e) = client.repository_sync_to(&dest, op.delete_extra).await {
        patch_replicate_status(&spec.target_ref, &replicate_failed_body(&e.to_string())).await;
        error!(class = %e.class(), "repository sync-to failed");
        return Err(MoverError::Kopia {
            op: KopiaOp::RepositorySyncTo,
            source: e,
        });
    }

    patch_replicate_status(
        &spec.target_ref,
        &replicate_ok_body(op.destination.kind_str(), &chrono::Utc::now()),
    )
    .await;
    info!(
        destination = op.destination.kind_str(),
        "replication succeeded"
    );
    Ok(())
}

/// `{ "status": ... }` body for a successful replication: stamp `lastReplicated`,
/// the destination backend, phase `Succeeded`, and a `Ready=True` condition.
fn replicate_ok_body(dest: &str, now: &chrono::DateTime<chrono::Utc>) -> serde_json::Value {
    let ts = now.to_rfc3339();
    serde_json::json!({
        "status": {
            "phase": "Succeeded",
            "destinationBackend": dest,
            "lastReplicated": ts,
            "conditions": [{
                "type": "Ready",
                "status": "True",
                "reason": "ReplicationSucceeded",
                "message": format!("replicated to {dest}"),
                "lastTransitionTime": ts,
                "observedGeneration": 0,
            }],
        }
    })
}

/// `{ "status": ... }` body for a failed replication: phase `Failed` + a
/// `Ready=False` condition.
fn replicate_failed_body(message: &str) -> serde_json::Value {
    serde_json::json!({
        "status": {
            "phase": "Failed",
            "conditions": [{
                "type": "Ready",
                "status": "False",
                "reason": "ReplicationFailed",
                "message": message,
                "lastTransitionTime": chrono::Utc::now().to_rfc3339(),
                "observedGeneration": 0,
            }],
        }
    })
}

/// PATCH a raw `{ "status": ... }` merge body onto the `RepositoryReplication`
/// `.status` (best-effort; logged on failure). Reuses the dynamic-API pattern.
async fn patch_replicate_status(target: &workspec::TargetRef, body: &serde_json::Value) {
    patch_maintenance_status(target, body).await;
}

/// Drive a `BrowseSession` run (M7a): connect to the repository **read-only**
/// (`repository connect --readonly` — the read-only bit persists in the client
/// config, so nothing this pod later execs can mutate the repo), write the
/// readiness marker so the pod's `kopiur-mover ready` probe starts passing,
/// then idle until the TTL elapses and exit cleanly. The CLI drives the actual
/// reads ([`kopiur_kopia::SessionCmd`]) via pod exec while the pod is Ready.
///
/// Unlike maintenance/verify/replicate there is NO status to PATCH: the
/// session's `targetRef` names nothing the controller owns. A failure here
/// logs the actionable error (class + message) and exits non-zero so the Job
/// goes `Failed` and the CLI surfaces the pod logs.
async fn run_browse_session_flow(
    client: &KopiaClient,
    spec: &MoverWorkSpec,
    op: &BrowseSessionOp,
    connect: &ConnectSpec,
) -> Result<()> {
    info!(
        backend = spec.repository.kind_str(),
        ttl_seconds = op.ttl_seconds,
        session = %spec.target_ref.name,
        "starting browse session (read-only connect)"
    );
    if let Err(e) = client
        .repository_connect_readonly(connect, spec.cache)
        .await
    {
        // Mirrors the maintenance connect-failure logging (class + message) so
        // `kubectl logs` on the session pod tells the whole story — minus the
        // status PATCH, which has no target for a browse session.
        error!(class = %e.class(), "browse session read-only connect failed");
        return Err(MoverError::Kopia {
            op: KopiaOp::BrowseConnect,
            source: e,
        });
    }

    // Signal readiness: the marker flips the pod Ready so the CLI knows the
    // session is exec-able. A marker that cannot be written would leave the
    // pod NotReady forever, so it is terminal (non-zero exit), not best-effort.
    let marker = std::path::Path::new(kopiur_mover::env::READY_MARKER);
    std::fs::write(marker, b"ready").map_err(|source| MoverError::ReadyMarkerWrite {
        path: marker.to_path_buf(),
        source,
    })?;
    info!(
        marker = %marker.display(),
        ttl_seconds = op.ttl_seconds,
        "browse session ready; holding the read-only connection until the TTL elapses"
    );

    tokio::time::sleep(Duration::from_secs(op.ttl_seconds)).await;
    info!(
        ttl_seconds = op.ttl_seconds,
        "browse session TTL elapsed; exiting"
    );
    Ok(())
}

/// `{ "status": ... }` body for a successful maintenance run. A full run also
/// advances the quick clock (full subsumes quick). `lastContentReclaimedBytes`
/// is `0`: `kopia maintenance run` emits no JSON, so the precise figure needs a
/// `maintenance info` delta (tracked separately; the field round-trips).
fn maintenance_ran_body(
    op: &MaintenanceOp,
    now: &chrono::DateTime<chrono::Utc>,
) -> serde_json::Value {
    let ts = now.to_rfc3339();
    let run = serde_json::json!({ "lastRunAt": ts, "lastContentReclaimedBytes": 0 });
    let mut status = serde_json::json!({
        "ownership": { "owner": op.owner, "claimedAt": ts },
        "conditions": [lease_condition_body("True", "LeaseClaimed", "maintenance lease claimed", now)],
    });
    match op.mode {
        MaintenanceMode::Quick => {
            status["quick"] = run;
        }
        MaintenanceMode::Full => {
            status["quick"] = run.clone();
            status["full"] = run;
        }
    }
    serde_json::json!({ "status": status })
}

/// `{ "status": ... }` body when the lease is held by another owner (yield /
/// prompt): record the observed holder and a `LeaseOwned=False` condition.
fn lease_blocked_body(owner: &str, reason: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "status": {
            "ownership": { "owner": owner },
            "conditions": [lease_condition_body("False", reason, message, &chrono::Utc::now())],
        }
    })
}

/// `{ "status": ... }` body for a failed kopia maintenance call.
fn maintenance_failed_body(e: &KopiaError) -> serde_json::Value {
    serde_json::json!({
        "status": {
            "conditions": [lease_condition_body(
                "False",
                "MaintenanceFailed",
                &format!("maintenance failed (class {}): {e}", e.class()),
                &chrono::Utc::now(),
            )],
        }
    })
}

/// A single `LeaseOwned` condition. The codebase uses a single-element
/// `conditions` array (last-writer-wins for the salient state) for `Maintenance`.
fn lease_condition_body(
    status: &str,
    reason: &str,
    message: &str,
    now: &chrono::DateTime<chrono::Utc>,
) -> serde_json::Value {
    serde_json::json!({
        "type": "LeaseOwned",
        "status": status,
        "reason": reason,
        "message": message,
        "lastTransitionTime": now.to_rfc3339(),
        "observedGeneration": 0,
    })
}

/// PATCH a raw `{ "status": ... }` merge body onto the `Maintenance` `.status`
/// (best-effort; logged on failure, like [`StatusReporter`]). Uses a dynamic API
/// so the mover need not depend on the typed CRD struct.
async fn patch_maintenance_status(target: &workspec::TargetRef, body: &serde_json::Value) {
    use kube::api::{Patch, PatchParams};
    use kube::core::{ApiResource, DynamicObject, GroupVersionKind};

    let attempt = async {
        let client =
            kube::Client::try_default()
                .await
                .map_err(|source| MoverError::KubeClient {
                    source: Box::new(source),
                })?;
        let (group, version) = split_api_version(&target.api_version);
        let gvk = GroupVersionKind::gvk(&group, &version, &target.kind);
        let ar = ApiResource::from_gvk(&gvk);
        let api = kube::Api::<DynamicObject>::namespaced_with(client, &target.namespace, &ar);
        api.patch_status(&target.name, &PatchParams::default(), &Patch::Merge(body))
            .await
            .map_err(|source| MoverError::StatusPatch {
                kind: target.kind.clone(),
                namespace: target.namespace.clone(),
                name: target.name.clone(),
                source: Box::new(source),
            })?;
        Ok::<(), MoverError>(())
    };
    if let Err(e) = attempt.await {
        warn!(error = %e, target = %target.name, "maintenance status PATCH failed");
    }
}

/// Report a terminal failure (PATCH the structured failure block) and return
/// the typed error so `main` exits non-zero. Takes ownership: the same
/// [`MoverError`] that built the `status.failure` block (class, stderr tail,
/// retry hint) is what the process exits with — no stringly re-wrap.
async fn terminal_failure(reporter: &StatusReporter, err: MoverError) -> Result<()> {
    let update = StatusUpdate::failed_mover(&err, chrono::Utc::now());
    reporter.report(&update).await;
    error!(
        class = %err.kopia_class(),
        retry = err.retry_recommended(),
        "kopia operation failed terminally"
    );
    Err(err)
}

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
    Err(MoverError::WorkSpecPathMissing)
}

fn load_work_spec(path: &PathBuf) -> Result<MoverWorkSpec> {
    let raw = std::fs::read_to_string(path).map_err(|source| MoverError::WorkSpecRead {
        path: path.clone(),
        source,
    })?;
    let spec: MoverWorkSpec =
        serde_json::from_str(&raw).map_err(|source| MoverError::WorkSpecParse {
            path: path.clone(),
            source,
        })?;
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
    kind: String,
    namespace: String,
    name: String,
}

impl KubeStatusReporter {
    async fn try_new(target: &workspec::TargetRef) -> Result<Self> {
        use kube::core::{ApiResource, GroupVersionKind};

        let client =
            kube::Client::try_default()
                .await
                .map_err(|source| MoverError::KubeClient {
                    source: Box::new(source),
                })?;
        let (group, version) = split_api_version(&target.api_version);
        let gvk = GroupVersionKind::gvk(&group, &version, &target.kind);
        let ar = ApiResource::from_gvk(&gvk);
        let api =
            kube::Api::<kube::api::DynamicObject>::namespaced_with(client, &target.namespace, &ar);
        Ok(KubeStatusReporter {
            api,
            kind: target.kind.clone(),
            namespace: target.namespace.clone(),
            name: target.name.clone(),
        })
    }

    async fn patch(&mut self, update: &StatusUpdate) -> Result<()> {
        use kube::api::{Patch, PatchParams};
        let body = update.as_patch_body();
        self.api
            .patch_status(&self.name, &PatchParams::default(), &Patch::Merge(&body))
            .await
            .map_err(|source| MoverError::StatusPatch {
                kind: self.kind.clone(),
                namespace: self.namespace.clone(),
                name: self.name.clone(),
                source: Box::new(source),
            })?;
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

    fn maint_op(mode: MaintenanceMode) -> MaintenanceOp {
        MaintenanceOp {
            mode,
            owner: "kopiur/prod/nas".into(),
            takeover_policy: kopiur_api::TakeoverPolicy::Never,
        }
    }

    #[test]
    fn quick_run_advances_only_quick_clock() {
        let now = chrono::Utc::now();
        let body = maintenance_ran_body(&maint_op(MaintenanceMode::Quick), &now);
        assert!(body["status"]["quick"]["lastRunAt"].is_string());
        assert!(
            body["status"]["full"].is_null(),
            "a quick run must not stamp the full clock"
        );
        assert_eq!(body["status"]["ownership"]["owner"], "kopiur/prod/nas");
    }

    #[test]
    fn full_run_subsumes_quick_clock() {
        let now = chrono::Utc::now();
        let body = maintenance_ran_body(&maint_op(MaintenanceMode::Full), &now);
        // Full subsumes quick: both clocks advance so quick isn't immediately due.
        assert!(body["status"]["full"]["lastRunAt"].is_string());
        assert!(body["status"]["quick"]["lastRunAt"].is_string());
        assert_eq!(
            body["status"]["full"]["lastRunAt"],
            body["status"]["quick"]["lastRunAt"]
        );
    }

    #[test]
    fn lease_blocked_records_observed_owner_and_false_condition() {
        let body = lease_blocked_body("other/owner", "LeaseHeldByOther", "held");
        assert_eq!(body["status"]["ownership"]["owner"], "other/owner");
        assert_eq!(body["status"]["conditions"][0]["status"], "False");
        assert_eq!(body["status"]["conditions"][0]["type"], "LeaseOwned");
    }

    // --- `kopiur-mover ready` probe mode: the pure marker decision ---

    #[test]
    fn session_ready_is_true_iff_the_marker_exists() {
        let dir = std::env::temp_dir().join(format!("kopiur-ready-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let marker = dir.join(".kopiur-session-ready");

        // No marker yet → the probe must fail (pod stays NotReady).
        assert!(!session_ready(&marker));

        // Marker written (what run_browse_session_flow does after a successful
        // read-only connect) → the probe passes.
        std::fs::write(&marker, b"ready").unwrap();
        assert!(session_ready(&marker));

        std::fs::remove_dir_all(&dir).ok();
    }
}
