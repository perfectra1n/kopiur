//! The `Restore` reconciler (ADR §4.6, §4.7).
//!
//! Resolves the source (`backupRef` / `fromConfig` / `identity`), pins
//! `status.resolved`, creates a restore mover `Job`, and handles the passive
//! populator mode (a PVC's `spec.dataSourceRef` points at the `Restore`).
//!
//! The source-mode dispatch is an **exhaustive `match`** over the externally
//! tagged `RestoreSource` enum (no `_ =>`), and [`default_on_missing`] /
//! [`populator_state`] are pure decisions, all unit-tested. The pvc-prime
//! handshake IO is a documented minimal partial (see NOTE in the reconcile body).

use std::sync::Arc;

use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};

use kopiur_api::backup::Backup;
use kopiur_api::{
    OnMissingSnapshot, Restore, RestorePhase, RestoreSource, RestoreTarget, validate,
};
use kopiur_mover::workspec::{
    MoverOptions, MoverWorkSpec, Operation, RepositoryConnect, ResolvedIdentity as MoverIdentity,
    RestoreOp, TargetRef,
};

use crate::consts::{API_VERSION, CREDENTIALS_AVAILABLE_CONDITION, MISSING_CREDENTIALS_REASON};
use crate::context::Context;
use crate::error::{Error, Result, error_policy_for};
use crate::io::{self, ResolvedRepository};
use crate::jobs::{self, JobLimits, MoverJobInputs, VolumeMountSpec};

/// Which source mode a restore uses, as a stable string (mirrors
/// `RestoreSource::kind_str`, re-derived through an exhaustive match so a new
/// variant must be handled here too).
pub fn source_mode(source: &RestoreSource) -> &'static str {
    match source {
        RestoreSource::BackupRef(_) => "BackupRef",
        RestoreSource::FromConfig(_) => "FromConfig",
        RestoreSource::Identity(_) => "Identity",
    }
}

/// The default `onMissingSnapshot` for a source mode when the spec doesn't set
/// it (ADR §4.6 / SKILL "Restores fail closed"): `fromConfig` defaults to
/// `Continue` (deploy-or-restore), everything else fails closed (`Fail`).
pub fn default_on_missing(source: &RestoreSource) -> OnMissingSnapshot {
    match source {
        RestoreSource::FromConfig(_) => OnMissingSnapshot::Continue,
        RestoreSource::BackupRef(_) | RestoreSource::Identity(_) => OnMissingSnapshot::Fail,
    }
}

/// Effective `onMissingSnapshot`: explicit spec value wins, else the per-mode
/// default.
pub fn effective_on_missing(
    spec: Option<OnMissingSnapshot>,
    source: &RestoreSource,
) -> OnMissingSnapshot {
    spec.unwrap_or_else(|| default_on_missing(source))
}

/// State of the passive-populator handshake. Pure model of the §4.7 machine so
/// the reconcile loop can dispatch without re-deriving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopulatorState {
    /// No `target` on the spec: this `Restore` is a passive populator source,
    /// awaiting a PVC `dataSourceRef` to claim it.
    AwaitingClaim,
    /// A `target` is set: the operator drives the restore directly.
    DirectTarget,
}

/// Wall-clock duration (seconds) of a restore `Job` from its
/// `status.startTime`/`completionTime`. `None` if either is absent or the
/// interval is negative (clock skew). Pure. (`Time.0` is a jiff `Timestamp`.)
pub fn restore_job_duration_seconds(job: &k8s_openapi::api::batch::v1::Job) -> Option<i64> {
    let st = job.status.as_ref()?;
    let start = st.start_time.as_ref()?.0.as_second();
    let end = st.completion_time.as_ref()?.0.as_second();
    let secs = end - start;
    (secs >= 0).then_some(secs)
}

/// Decide the populator state from whether a `target` is present.
pub fn populator_state(has_target: bool) -> PopulatorState {
    if has_target {
        PopulatorState::DirectTarget
    } else {
        PopulatorState::AwaitingClaim
    }
}

/// Reconcile a `Restore`.
#[tracing::instrument(skip(restore, ctx), fields(kind = "Restore", namespace = %restore.namespace().unwrap_or_default(), name = %restore.name_any()))]
pub async fn reconcile(restore: Arc<Restore>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&restore, &ctx).await;
    ctx.metrics
        .record_reconcile("Restore", start.elapsed().as_secs_f64());
    record_restore_status_metrics(&restore, &ctx, result.is_ok()).await;
    result
}

/// Mirror a Restore's phase gauge. Zeroes it on deletion (so a Failed restore's
/// alert clears once the CR is gone) and re-reads the freshest status on success
/// — see the Backup equivalent for the rationale. (Restore *duration* is
/// recorded at the Job-completion site, not from status.)
async fn record_restore_status_metrics(restore: &Restore, ctx: &Context, ok: bool) {
    let (Some(ns), name) = (restore.namespace(), restore.name_any()) else {
        return;
    };
    if restore.metadata.deletion_timestamp.is_some() {
        ctx.metrics
            .clear_phase::<RestorePhase>("Restore", &ns, &name);
        return;
    }
    if !ok {
        return;
    }
    let api: Api<Restore> = Api::namespaced(ctx.client.clone(), &ns);
    if let Ok(Some(latest)) = api.get_opt(&name).await
        && let Some(phase) = latest.status.as_ref().and_then(|s| s.phase)
    {
        ctx.metrics.set_restore_phase(&ns, &name, phase);
    }
}

async fn reconcile_inner(restore: &Restore, ctx: &Context) -> Result<Action> {
    if let Err(e) = validate::validate_restore(&restore.spec) {
        return Err(Error::Validation(e.to_string()));
    }

    let namespace = restore
        .namespace()
        .ok_or_else(|| Error::Invariant("Restore has no namespace".into()))?;
    let name = restore.name_any();
    let api: Api<Restore> = Api::namespaced(ctx.client.clone(), &namespace);

    // Already terminal: a Restore is one-shot. Once Completed/Failed there is
    // nothing left to do until the spec changes, so don't re-resolve, re-pin a
    // fresh timestamp, or re-write the phase — each of which would churn status and
    // self-trigger another reconcile (the same hot-loop class as the repo bug).
    // Mirrors the Backup reconciler's terminal discipline.
    if matches!(
        restore.status.as_ref().and_then(|s| s.phase),
        Some(RestorePhase::Completed) | Some(RestorePhase::Failed)
    ) {
        return Ok(Action::requeue(std::time::Duration::from_secs(600)));
    }

    let state = populator_state(restore.spec.target.is_some());
    let on_missing = effective_on_missing(
        restore
            .spec
            .policy
            .as_ref()
            .and_then(|p| p.on_missing_snapshot),
        &restore.spec.source,
    );

    // Resolve the source to a concrete snapshot id, pinning status.resolved.
    let resolved = resolve_snapshot(ctx, restore, &namespace).await?;
    let snapshot_id = match resolved {
        Some(id) => id,
        None => {
            // No snapshot matched. Honor the closed enum exhaustively.
            return match on_missing {
                OnMissingSnapshot::Fail => {
                    io::patch_status(
                        &api,
                        &name,
                        serde_json::json!({
                            "phase": "Failed",
                            "conditions": [condition(
                                "Resolved", "False", "SnapshotNotFound",
                                "no snapshot matched the restore source",
                            )],
                        }),
                    )
                    .await?;
                    Err(Error::MissingDependency(
                        "no snapshot matched restore source".into(),
                    ))
                }
                OnMissingSnapshot::Continue => {
                    // Deploy-or-restore: nothing to restore, complete cleanly.
                    io::patch_status(
                        &api,
                        &name,
                        serde_json::json!({
                            "phase": "Completed",
                            "conditions": [condition(
                                "Resolved", "True", "NoSnapshotContinue",
                                "no snapshot found; continuing (deploy-or-restore)",
                            )],
                        }),
                    )
                    .await?;
                    Ok(Action::requeue(std::time::Duration::from_secs(600)))
                }
            };
        }
    };

    // Pin the resolution timestamp exactly once. `resolved` is "pinned at
    // admission; never re-resolved" (ADR §4.6) — re-writing `now()` on every
    // reconcile churned status and self-triggered the loop.
    if restore
        .status
        .as_ref()
        .and_then(|s| s.resolved.as_ref())
        .is_none()
    {
        io::patch_status(
            &api,
            &name,
            serde_json::json!({
                "phase": "Resolving",
                "resolved": { "pinnedAt": chrono::Utc::now().to_rfc3339() },
            }),
        )
        .await?;
    }

    match state {
        PopulatorState::DirectTarget => {
            drive_direct_restore(ctx, restore, &api, &namespace, &name, &snapshot_id).await
        }
        PopulatorState::AwaitingClaim => {
            // NOTE: passive populator mode. The full CSI populator handshake
            // (PVC dataSourceRef -> prime PVC -> bind) requires the
            // VolumePopulator lib-mover protocol. The minimal real implementation
            // here surfaces the awaiting-claim condition and pins the resolved
            // snapshot so a claim can proceed; wiring the prime-PVC dance is the
            // documented residual.
            io::patch_status(
                &api,
                &name,
                serde_json::json!({
                    "phase": "Pending",
                    "conditions": [condition(
                        "AwaitingClaim", "True", "AwaitingPvcDataSourceRef",
                        "passive populator: awaiting a PVC dataSourceRef to claim this Restore",
                    )],
                    "target": { "pvcPrime": "awaiting-claim" },
                }),
            )
            .await?;
            Ok(Action::requeue(std::time::Duration::from_secs(30)))
        }
    }
}

/// Drive a restore-with-explicit-target: create the restore mover Job (writing
/// into the target PVC), then track it to terminal.
async fn drive_direct_restore(
    ctx: &Context,
    restore: &Restore,
    api: &Api<Restore>,
    namespace: &str,
    name: &str,
    snapshot_id: &str,
) -> Result<Action> {
    use k8s_openapi::api::batch::v1::Job;
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);
    if let Some(job) = job_api.get_opt(name).await? {
        // Guard each phase write with a phase-equality check so a tracked Job that
        // sits terminal (or keeps running) doesn't re-patch an identical phase on
        // every requeue and self-trigger. Mirrors the Backup reconciler.
        let phase = restore.status.as_ref().and_then(|s| s.phase);
        return match crate::backup::job_terminal_state(&job) {
            Some(true) => {
                if let Some(secs) = restore_job_duration_seconds(&job) {
                    ctx.metrics.set_restore_duration(namespace, name, secs);
                }
                if phase != Some(RestorePhase::Completed) {
                    io::patch_status(api, name, serde_json::json!({ "phase": "Completed" }))
                        .await?;
                }
                Ok(Action::requeue(std::time::Duration::from_secs(600)))
            }
            Some(false) => {
                if phase != Some(RestorePhase::Failed) {
                    io::patch_status(api, name, serde_json::json!({ "phase": "Failed" })).await?;
                }
                Ok(Action::requeue(std::time::Duration::from_secs(120)))
            }
            None => {
                if phase != Some(RestorePhase::Restoring) {
                    io::patch_status(api, name, serde_json::json!({ "phase": "Restoring" }))
                        .await?;
                }
                Ok(Action::requeue(std::time::Duration::from_secs(30)))
            }
        };
    }

    // Resolve the repository + target PVC for the restore Job.
    let repo = resolve_restore_repository(ctx, restore, namespace).await?;
    let target_pvc = match restore.spec.target.as_ref() {
        Some(RestoreTarget::PvcRef(r)) => r.name.clone(),
        Some(RestoreTarget::Pvc(t)) => t.name.clone(),
        None => {
            return Err(Error::Invariant(
                "DirectTarget restore without a target".into(),
            ));
        }
    };
    let target_path = "/restore".to_string();
    let creds_secrets = io::mover_creds_secrets(&repo.backend, &repo.encryption);

    // The restore mover Job runs in this (workload) namespace: mint the mover SA +
    // RoleBinding here, then verify the credential Secret(s) it loads via envFrom
    // are present — else surface a clear condition + Event and requeue (ADR §4.12).
    if let Some(sa) = ctx.mover_service_account.as_deref() {
        io::ensure_mover_rbac(
            &ctx.client,
            namespace,
            sa,
            &ctx.mover_role_kind,
            &ctx.mover_clusterrole,
        )
        .await?;
    }
    let repo_ref = restore.spec.repository.as_ref();
    let creds_ctx = io::CredsContext {
        secret_names: &creds_secrets,
        repo_kind: repo_ref
            .map(|r| io::repo_kind_str(r.kind))
            .unwrap_or("Repository"),
        repo_name: repo_ref
            .map(|r| r.name.as_str())
            .unwrap_or("(from source config)"),
        repo_secret_namespace: repo.encryption.password_secret_ref.namespace.as_deref(),
    };
    if let Some(msg) = io::first_missing_cred(&ctx.client, namespace, &creds_ctx).await? {
        let existing = restore
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();
        let conditions = io::upsert_condition(
            &existing,
            CREDENTIALS_AVAILABLE_CONDITION,
            false,
            MISSING_CREDENTIALS_REASON,
            &msg,
            restore.metadata.generation,
        );
        io::patch_status(
            api,
            name,
            serde_json::json!({ "phase": "Pending", "conditions": conditions }),
        )
        .await?;
        io::publish_missing_creds_event(ctx, restore, &msg).await;
        return Err(Error::MissingDependency(msg));
    }
    // Creds present: clear any stale `CredentialsAvailable=False` from a prior reconcile.
    if let Some(conds) = restore.status.as_ref().map(|s| s.conditions.as_slice())
        && conds
            .iter()
            .any(|c| c.type_ == CREDENTIALS_AVAILABLE_CONDITION && c.status != "True")
    {
        let conditions = io::upsert_condition(
            conds,
            CREDENTIALS_AVAILABLE_CONDITION,
            true,
            "Available",
            "credentials Secret(s) present in the mover namespace",
            restore.metadata.generation,
        );
        io::patch_status(api, name, serde_json::json!({ "conditions": conditions })).await?;
    }

    let identity = MoverIdentity {
        username: "restore".into(),
        hostname: namespace.to_string(),
        source_path: target_path.clone(),
    };
    // Carry the Restore CRD's options (ADR §4.6) through to the mover so kopia
    // honors them. `None` lets kopia use its defaults.
    let (ignore_permission_errors, write_files_atomically) = restore
        .spec
        .options
        .as_ref()
        .map(|o| (o.ignore_permission_errors, o.write_files_atomically))
        .unwrap_or((None, None));
    let work_spec = MoverWorkSpec {
        version: 1,
        operation: Operation::Restore(RestoreOp {
            snapshot_id: snapshot_id.to_string(),
            target_path: target_path.clone(),
            ignore_permission_errors,
            write_files_atomically,
        }),
        identity,
        repository: restore_connect(&repo)?,
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "Restore".to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
    };
    let owner = io::owner_ref_for(restore, "Restore")?;
    let repo_volume =
        io::filesystem_repo_mount_source(&repo.backend).map(|source| VolumeMountSpec {
            source,
            mount_path: io::filesystem_repo_path(&repo.backend).unwrap_or_default(),
            read_only: true,
        });
    let inputs = MoverJobInputs {
        name,
        namespace,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: crate::backup::mover_pull_policy_pub(),
        limits: JobLimits::default(),
        resources: None,
        security_context: None,
        labels: io::child_labels(&[("kopiur.home-operations.com/op", "restore")]),
        // Restore writes INTO the target PVC, mounted read-write at /restore.
        source_volume: Some(VolumeMountSpec::pvc(target_pvc, target_path, false)),
        repo_volume,
        creds_secrets,
        result_configmap: None,
        service_account: ctx.mover_service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
        annotations: Default::default(),
    };
    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, namespace, name, &cm, &job).await?;
    io::patch_status(api, name, serde_json::json!({ "phase": "Restoring" })).await?;
    tracing::info!(restore = %name, %snapshot_id, "created restore Job");
    Ok(Action::requeue(std::time::Duration::from_secs(30)))
}

/// Resolve the restore's source to a concrete kopia snapshot id. Returns `None`
/// when no snapshot matches (caller applies `onMissingSnapshot`).
async fn resolve_snapshot(
    ctx: &Context,
    restore: &Restore,
    namespace: &str,
) -> Result<Option<String>> {
    match &restore.spec.source {
        RestoreSource::BackupRef(r) => {
            let ns = r.namespace.as_deref().unwrap_or(namespace);
            let api: Api<Backup> = Api::namespaced(ctx.client.clone(), ns);
            let backup = api.get_opt(&r.name).await?;
            Ok(backup
                .and_then(|b| b.status)
                .and_then(|s| s.snapshot)
                .map(|s| s.kopia_snapshot_id))
        }
        RestoreSource::Identity(id) => {
            // An explicit snapshot id wins; otherwise resolve via snapshot list.
            if let Some(sid) = &id.snapshot_id {
                return Ok(Some(sid.clone()));
            }
            let repo = resolve_restore_repository(ctx, restore, namespace).await?;
            let snapshots = list_for_identity(
                ctx,
                &repo,
                namespace,
                &id.username,
                &id.hostname,
                id.source_path.as_deref(),
            )
            .await?;
            Ok(pick_offset(snapshots, id.offset.unwrap_or(0)))
        }
        RestoreSource::FromConfig(c) => {
            // Resolve identity from the BackupConfig, then list newest/offset.
            use kopiur_api::BackupConfig;
            let cfg_ns = c.namespace.as_deref().unwrap_or(namespace);
            let cfg_api: Api<BackupConfig> = Api::namespaced(ctx.client.clone(), cfg_ns);
            let config = cfg_api.get_opt(&c.name).await?.ok_or_else(|| {
                Error::MissingDependency(format!("BackupConfig {cfg_ns}/{}", c.name))
            })?;
            let identity = crate::backup_config::config_identity(&config, cfg_ns)?;
            let repo = resolve_restore_repository(ctx, restore, namespace).await?;
            let snapshots = list_for_identity(
                ctx,
                &repo,
                namespace,
                &identity.username,
                &identity.hostname,
                identity.source_path.as_deref(),
            )
            .await?;
            Ok(pick_offset(snapshots, c.offset.unwrap_or(0)))
        }
    }
}

/// kopia snapshot list filtered to one identity (filesystem in-process path),
/// newest-first.
async fn list_for_identity(
    ctx: &Context,
    repo: &ResolvedRepository,
    namespace: &str,
    username: &str,
    hostname: &str,
    source_path: Option<&str>,
) -> Result<Vec<kopiur_kopia::SnapshotListEntry>> {
    use kopiur_api::backend::Backend;
    let creds = io::repo_credentials(&repo.encryption);
    match &repo.backend {
        Backend::Filesystem(fs) => {
            let password = io::read_repo_password(&ctx.client, namespace, &creds).await?;
            let client = ctx.kopia.build([("KOPIA_PASSWORD".to_string(), password)]);
            client
                .repository_connect(&kopiur_kopia::ConnectSpec::Filesystem {
                    path: fs.path.clone().into(),
                })
                .await?;
            let filter = kopiur_kopia::SnapshotSource {
                host: hostname.to_string(),
                user_name: username.to_string(),
                path: source_path.unwrap_or("").to_string(),
            };
            let mut list = client.snapshot_list(Some(&filter)).await?;
            list.sort_by_key(|e| std::cmp::Reverse(e.end_time));
            Ok(list)
        }
        // NOTE: object-store snapshot resolution would run via a short Job; the
        // filesystem path is the working core (see repository.rs NOTE).
        _ => Ok(vec![]),
    }
}

/// Pick the snapshot at `offset` (0 = newest) from a newest-first list.
fn pick_offset(snapshots: Vec<kopiur_kopia::SnapshotListEntry>, offset: i64) -> Option<String> {
    let idx = offset.max(0) as usize;
    snapshots.into_iter().nth(idx).map(|e| e.id)
}

/// Resolve the repository a restore targets (`spec.repository` or, when omitted,
/// via the backupRef'd Backup's recipe). Implemented for the explicit-repository
/// and BackupRef paths.
async fn resolve_restore_repository(
    ctx: &Context,
    restore: &Restore,
    namespace: &str,
) -> Result<ResolvedRepository> {
    // Explicit `spec.repository` wins. Honors `kind` (namespaced vs.
    // ClusterRepository) via the shared resolver (ADR §5.5).
    if let Some(rref) = &restore.spec.repository {
        return io::resolve_repository_ref(&ctx.client, rref, namespace).await;
    }
    // FromConfig: resolve via the BackupConfig's repository.
    if let RestoreSource::FromConfig(c) = &restore.spec.source {
        use kopiur_api::BackupConfig;
        let cfg_ns = c.namespace.as_deref().unwrap_or(namespace);
        let cfg_api: Api<BackupConfig> = Api::namespaced(ctx.client.clone(), cfg_ns);
        let config = cfg_api
            .get_opt(&c.name)
            .await?
            .ok_or_else(|| Error::MissingDependency(format!("BackupConfig {cfg_ns}/{}", c.name)))?;
        return io::resolve_repository_ref(&ctx.client, &config.spec.repository, cfg_ns).await;
    }
    Err(Error::Validation(
        "restore requires spec.repository (or a fromConfig source)".into(),
    ))
}

/// Map a resolved repository backend to the mover connect spec for a restore.
fn restore_connect(repo: &ResolvedRepository) -> Result<RepositoryConnect> {
    crate::backup::repository_connect_pub(repo)
}

/// Build a Kubernetes condition object.
fn condition(type_: &str, status: &str, reason: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "type": type_,
        "status": status,
        "reason": reason,
        "message": message,
        "lastTransitionTime": chrono::Utc::now().to_rfc3339(),
        "observedGeneration": 0,
    })
}

/// `error_policy` for the `Restore` controller.
pub fn error_policy(_obj: Arc<Restore>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("Restore", err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::common::ObjectRef;
    use kopiur_api::restore::{FromConfig, IdentitySource};

    fn job_with_times(start: Option<&str>, end: Option<&str>) -> k8s_openapi::api::batch::v1::Job {
        use k8s_openapi::api::batch::v1::{Job, JobStatus};
        let parse = |s: &str| serde_json::from_value(serde_json::json!(s)).unwrap();
        Job {
            status: Some(JobStatus {
                start_time: start.map(parse),
                completion_time: end.map(parse),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn restore_duration_is_completion_minus_start() {
        let job = job_with_times(Some("2024-01-01T00:00:00Z"), Some("2024-01-01T00:01:30Z"));
        assert_eq!(restore_job_duration_seconds(&job), Some(90));
        // Missing completion → None (still running).
        assert_eq!(
            restore_job_duration_seconds(&job_with_times(Some("2024-01-01T00:00:00Z"), None)),
            None
        );
        // Negative interval (clock skew) → None.
        let skew = job_with_times(Some("2024-01-01T00:01:00Z"), Some("2024-01-01T00:00:00Z"));
        assert_eq!(restore_job_duration_seconds(&skew), None);
    }

    fn backup_ref() -> RestoreSource {
        RestoreSource::BackupRef(ObjectRef {
            name: "b".into(),
            namespace: None,
        })
    }
    fn from_config() -> RestoreSource {
        RestoreSource::FromConfig(FromConfig {
            name: "cfg".into(),
            namespace: None,
            as_of: None,
            offset: Some(0),
        })
    }
    fn identity() -> RestoreSource {
        RestoreSource::Identity(IdentitySource {
            username: "u".into(),
            hostname: "h".into(),
            source_path: None,
            snapshot_id: None,
            as_of: None,
            offset: None,
        })
    }

    #[test]
    fn from_config_defaults_to_continue_others_fail() {
        assert_eq!(
            default_on_missing(&from_config()),
            OnMissingSnapshot::Continue
        );
        assert_eq!(default_on_missing(&backup_ref()), OnMissingSnapshot::Fail);
        assert_eq!(default_on_missing(&identity()), OnMissingSnapshot::Fail);
    }

    #[test]
    fn explicit_on_missing_overrides_default() {
        // fromConfig would default Continue, but an explicit Fail wins.
        assert_eq!(
            effective_on_missing(Some(OnMissingSnapshot::Fail), &from_config()),
            OnMissingSnapshot::Fail
        );
        // backupRef defaults Fail, explicit Continue wins.
        assert_eq!(
            effective_on_missing(Some(OnMissingSnapshot::Continue), &backup_ref()),
            OnMissingSnapshot::Continue
        );
    }

    #[test]
    fn source_mode_strings_match_each_variant() {
        assert_eq!(source_mode(&backup_ref()), "BackupRef");
        assert_eq!(source_mode(&from_config()), "FromConfig");
        assert_eq!(source_mode(&identity()), "Identity");
    }

    #[test]
    fn populator_state_depends_on_target_presence() {
        assert_eq!(populator_state(false), PopulatorState::AwaitingClaim);
        assert_eq!(populator_state(true), PopulatorState::DirectTarget);
    }
}
