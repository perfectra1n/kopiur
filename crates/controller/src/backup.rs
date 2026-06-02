//! The `Backup` reconciler — the heart of the ADR §5.5 thesis.
//!
//! Two paths:
//! 1. **Normal reconcile** (produced backups): add the `kopiur.dev/snapshot-cleanup`
//!    finalizer, create a mover `Job` + `ConfigMap` (work spec), watch it to a
//!    terminal state, copy stats/phase into `status`, and reap (owner-ref GC).
//! 2. **Deletion** (finalizer present, `deletionTimestamp` set): run the
//!    EXHAUSTIVE [`plan_deletion`] decision, execute its IO, then remove the
//!    finalizer.
//!
//! [`plan_deletion`] is a pure function over `(DeletionPolicy, annotations)`
//! returning a [`DeletionPlan`]. It is the single most important thing to get
//! right and is exhaustively unit-tested — the `match` has **no** `_ =>` arm, so
//! a new `DeletionPolicy` variant cannot compile until handled (SKILL thesis).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use kube::runtime::controller::Action;
use kube::runtime::events::{Event, EventType};
use kube::{Api, Resource, ResourceExt};

use kopiur_api::backend::Backend;
use kopiur_api::backup::BackupPhase;
use kopiur_api::common::ResolvedIdentity as ApiResolvedIdentity;
use kopiur_api::{Backup, BackupConfig, DeletionPolicy, Origin, Repository};
use kopiur_mover::workspec::{
    BackupOp, MoverOptions, MoverWorkSpec, Operation, RepositoryConnect,
    ResolvedIdentity as MoverIdentity, SnapshotDeleteOp, TargetRef,
};

use crate::consts::{
    API_VERSION, CONFIG_LABEL, ORIGIN_LABEL, SKIP_SNAPSHOT_CLEANUP_ANNOTATION,
    SNAPSHOT_CLEANUP_FINALIZER,
};
use crate::context::Context;
use crate::error::{Error, Result, error_policy_for};
use crate::io;
use crate::jobs::{self, JobLimits, MoverJobInputs, PvcMount};

/// The decision the deletion handler must execute. Derived purely from the
/// effective `DeletionPolicy` and the object's annotations — no IO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionPlan {
    /// Run `kopia snapshot delete <id>` (via a short Job) then remove the
    /// finalizer. On failure, stay in `phase: Deleting` and back off — the CR
    /// is NOT dropped (ADR §4.5).
    DeleteSnapshot,
    /// Remove the finalizer without contacting the repository (snapshot stays).
    /// Used by `Retain`.
    RetainSnapshot,
    /// Remove the finalizer without contacting the repository, record the
    /// snapshot orphaned, emit `SnapshotOrphaned`, bump the orphan metric. Used
    /// by `Orphan` and by the `skip-snapshot-cleanup` annotation escape hatch.
    OrphanSnapshot,
}

/// Decide what to do on deletion. **Exhaustive** over [`DeletionPolicy`] with no
/// catch-all: a new variant fails to compile until handled here (ADR §5.5).
///
/// The `skip-snapshot-cleanup` annotation is the repo-offline escape hatch and
/// **overrides everything** — even `Delete` — because its entire purpose is "the
/// bucket is gone, just let me remove the CR" (ADR §4.5).
pub fn plan_deletion(
    policy: DeletionPolicy,
    annotations: &BTreeMap<String, String>,
) -> DeletionPlan {
    if annotations.contains_key(SKIP_SNAPSHOT_CLEANUP_ANNOTATION) {
        return DeletionPlan::OrphanSnapshot;
    }
    match policy {
        DeletionPolicy::Delete => DeletionPlan::DeleteSnapshot,
        DeletionPolicy::Retain => DeletionPlan::RetainSnapshot,
        DeletionPolicy::Orphan => DeletionPlan::OrphanSnapshot,
    }
}

/// Compute the effective `DeletionPolicy` for a `Backup`, honoring the
/// origin-aware default (ADR §4.5): discovered backups are forced to `Retain`,
/// produced backups default to `Delete` when unset.
pub fn effective_deletion_policy(
    spec_policy: Option<DeletionPolicy>,
    origin: Origin,
) -> DeletionPolicy {
    match origin {
        // Discovered snapshots are never ours to delete — forced Retain.
        Origin::Discovered => DeletionPolicy::Retain,
        Origin::Scheduled | Origin::Manual => spec_policy.unwrap_or(DeletionPolicy::Delete),
    }
}

/// Resolve a `Backup`'s origin from its status (canonical) or its
/// `kopiur.dev/origin` label, defaulting to `Manual` when neither is present
/// (a bare `kubectl create`).
pub fn resolve_origin(b: &Backup) -> Origin {
    if let Some(o) = b.status.as_ref().and_then(|s| s.origin) {
        return o;
    }
    match b
        .labels()
        .get(crate::consts::ORIGIN_LABEL)
        .map(String::as_str)
    {
        Some("scheduled") => Origin::Scheduled,
        Some("discovered") => Origin::Discovered,
        _ => Origin::Manual,
    }
}

/// Reconcile a `Backup`.
///
/// IO is intentionally thin here: the decision logic ([`plan_deletion`],
/// [`effective_deletion_policy`], the job builders in [`crate::jobs`]) is pure
/// and unit-tested; this function wires those decisions to the cluster.
#[tracing::instrument(skip(backup, ctx), fields(kind = "Backup", name = %backup.name_any()))]
pub async fn reconcile(backup: Arc<Backup>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&backup, &ctx).await;
    ctx.metrics
        .record_reconcile("Backup", start.elapsed().as_secs_f64());
    result
}

async fn reconcile_inner(backup: &Backup, ctx: &Context) -> Result<Action> {
    let origin = resolve_origin(backup);
    let policy = effective_deletion_policy(backup.spec.deletion_policy, origin);
    let namespace = backup
        .namespace()
        .ok_or_else(|| Error::Invariant("Backup has no namespace".into()))?;
    let name = backup.name_any();
    let api: Api<Backup> = Api::namespaced(ctx.client.clone(), &namespace);

    if backup.metadata.deletion_timestamp.is_some() {
        return handle_deletion(backup, ctx, &api, &namespace, &name, policy).await;
    }

    // Discovered backups are catalog rows, not runs: never spawn a Job. Pin the
    // Discovered phase if unset and stop.
    if origin == Origin::Discovered {
        if backup.status.as_ref().and_then(|s| s.phase) != Some(BackupPhase::Discovered) {
            io::patch_status(
                &api,
                &name,
                serde_json::json!({ "phase": "Discovered", "origin": "discovered" }),
            )
            .await?;
        }
        return Ok(Action::requeue(Duration::from_secs(600)));
    }

    // Ensure the snapshot-cleanup finalizer before doing any work that creates a
    // snapshot, so a delete during the run still triggers cleanup.
    if io::ensure_finalizer(&api, backup, SNAPSHOT_CLEANUP_FINALIZER).await? {
        // Requeue so the next pass sees the finalizer.
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // If the owned mover Job already reached a terminal state, copy phase/stats
    // into status (controller-as-source-of-truth for phase) and stop running.
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), &namespace);
    if let Some(job) = job_api.get_opt(&name).await? {
        match job_terminal_state(&job) {
            Some(true) => {
                if backup.status.as_ref().and_then(|s| s.phase) != Some(BackupPhase::Succeeded) {
                    finalize_succeeded(ctx, backup, &api, &name, &namespace).await?;
                }
                return Ok(Action::requeue(Duration::from_secs(600)));
            }
            Some(false) => {
                if backup.status.as_ref().and_then(|s| s.phase) != Some(BackupPhase::Failed) {
                    io::patch_status(&api, &name, serde_json::json!({ "phase": "Failed" })).await?;
                }
                return Ok(Action::requeue(Duration::from_secs(120)));
            }
            None => {
                // Job exists but is still running; mark Running and wait.
                if backup.status.as_ref().and_then(|s| s.phase) != Some(BackupPhase::Running) {
                    io::patch_status(&api, &name, serde_json::json!({ "phase": "Running" }))
                        .await?;
                }
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
        }
    }

    // No Job yet: resolve the recipe and create the mover Job + ConfigMap.
    let (config, repo) = resolve_recipe(ctx, backup, &namespace).await?;
    let (work_spec, source_pvc, repo_pvc, creds_secret) =
        build_backup_run(backup, &config, &repo, &namespace, &name)?;

    let owner = io::owner_ref_for(backup, "Backup")?;
    let labels = run_labels(&config, origin);
    let limits = job_limits(backup);
    let inputs = MoverJobInputs {
        name: &name,
        namespace: &namespace,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: mover_pull_policy(),
        limits,
        resources: config.spec.mover.as_ref().and_then(|m| m.resources.clone()),
        security_context: config
            .spec
            .mover
            .as_ref()
            .and_then(|m| m.security_context.clone()),
        labels,
        source_pvc,
        repo_pvc,
        creds_secret: Some(&creds_secret),
        service_account: ctx.mover_service_account.as_deref(),
    };
    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, &namespace, &name, &cm, &job).await?;

    io::patch_status(
        &api,
        &name,
        serde_json::json!({ "phase": "Running", "origin": origin_str(origin) }),
    )
    .await?;
    tracing::info!(backup = %name, "created mover Job for backup");

    Ok(Action::requeue(Duration::from_secs(30)))
}

/// Execute the deletion plan (the tested [`plan_deletion`] decision) against the
/// cluster, then remove the finalizer when cleanup completes.
async fn handle_deletion(
    backup: &Backup,
    ctx: &Context,
    api: &Api<Backup>,
    namespace: &str,
    name: &str,
    policy: DeletionPolicy,
) -> Result<Action> {
    // Nothing to clean up if our finalizer isn't present.
    if !backup
        .finalizers()
        .iter()
        .any(|f| f == SNAPSHOT_CLEANUP_FINALIZER)
    {
        return Ok(Action::await_change());
    }

    let plan = plan_deletion(policy, backup.annotations());
    tracing::info!(?plan, backup = %name, "executing backup deletion plan");

    match plan {
        DeletionPlan::DeleteSnapshot => {
            let snapshot_id = backup
                .status
                .as_ref()
                .and_then(|s| s.snapshot.as_ref())
                .map(|s| s.kopia_snapshot_id.clone());
            match snapshot_id {
                // No snapshot was ever recorded: nothing to delete in the repo.
                None => {
                    io::remove_finalizer(api, backup, SNAPSHOT_CLEANUP_FINALIZER).await?;
                    Ok(Action::await_change())
                }
                Some(id) => delete_snapshot_via_job(backup, ctx, api, namespace, name, &id).await,
            }
        }
        DeletionPlan::RetainSnapshot => {
            io::remove_finalizer(api, backup, SNAPSHOT_CLEANUP_FINALIZER).await?;
            Ok(Action::await_change())
        }
        DeletionPlan::OrphanSnapshot => {
            ctx.metrics
                .orphaned_snapshots
                .with_label_values(&[namespace])
                .inc();
            let _ = ctx
                .recorder
                .publish(
                    &Event {
                        type_: EventType::Normal,
                        reason: "SnapshotOrphaned".into(),
                        note: Some(format!(
                            "snapshot for backup {name} orphaned (policy/escape-hatch); finalizer removed without contacting the repository"
                        )),
                        action: "Orphan".into(),
                        secondary: None,
                    },
                    &backup.object_ref(&()),
                )
                .await;
            io::remove_finalizer(api, backup, SNAPSHOT_CLEANUP_FINALIZER).await?;
            Ok(Action::await_change())
        }
    }
}

/// Drive a SnapshotDelete mover Job for the deletion path. Creates the Job if
/// absent; on terminal success removes the finalizer; on failure records a
/// Deleting phase, bumps the failure metric, and requeues.
async fn delete_snapshot_via_job(
    backup: &Backup,
    ctx: &Context,
    api: &Api<Backup>,
    namespace: &str,
    name: &str,
    snapshot_id: &str,
) -> Result<Action> {
    let job_name = format!("{name}-delete");
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), namespace);

    if let Some(job) = job_api.get_opt(&job_name).await? {
        match job_terminal_state(&job) {
            Some(true) => {
                io::remove_finalizer(api, backup, SNAPSHOT_CLEANUP_FINALIZER).await?;
                tracing::info!(backup = %name, %snapshot_id, "snapshot deleted; finalizer removed");
                return Ok(Action::await_change());
            }
            Some(false) => {
                ctx.metrics
                    .snapshot_deletion_failures
                    .with_label_values(&[namespace])
                    .inc();
                io::patch_status(api, name, serde_json::json!({ "phase": "Deleting" })).await?;
                tracing::warn!(backup = %name, "snapshot delete Job failed; backing off");
                return Ok(Action::requeue(Duration::from_secs(60)));
            }
            None => return Ok(Action::requeue(Duration::from_secs(15))),
        }
    }

    // Create the SnapshotDelete Job. We need the recipe to know how to connect
    // and authenticate to the repository.
    let (config, repo) = resolve_recipe(ctx, backup, namespace).await?;
    let identity = resolve_identity_for(&config, namespace)?;
    let creds = io::repo_credentials(&repo.spec.encryption);
    let work_spec = MoverWorkSpec {
        version: 1,
        operation: Operation::SnapshotDelete(SnapshotDeleteOp {
            snapshot_id: snapshot_id.to_string(),
        }),
        identity,
        repository: repository_connect(&repo)?,
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "Backup".to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
    };

    let owner = io::owner_ref_for(backup, "Backup")?;
    let mut labels = run_labels(&config, resolve_origin(backup));
    labels.insert("kopiur.dev/op".to_string(), "snapshot-delete".to_string());
    let repo_pvc = io::filesystem_repo_pvc(&repo.spec.backend).map(|claim_name| PvcMount {
        claim_name,
        mount_path: io::filesystem_repo_path(&repo.spec.backend).unwrap_or_default(),
        read_only: false,
    });
    let inputs = MoverJobInputs {
        name: &job_name,
        namespace,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: mover_pull_policy(),
        limits: JobLimits::default(),
        resources: None,
        security_context: None,
        labels,
        source_pvc: None,
        repo_pvc,
        creds_secret: Some(&creds.secret_name),
        service_account: ctx.mover_service_account.as_deref(),
    };
    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, namespace, &job_name, &cm, &job).await?;
    io::patch_status(api, name, serde_json::json!({ "phase": "Deleting" })).await?;
    tracing::info!(backup = %name, %snapshot_id, "created SnapshotDelete Job");
    Ok(Action::requeue(Duration::from_secs(15)))
}

/// On a Job's terminal success, pin phase=Succeeded and the resulting kopia
/// snapshot id/identity into status. The controller is the authoritative source
/// of the terminal phase AND (for the filesystem backend) of the snapshot id: it
/// resolves the newest snapshot for the run's identity in-process, so status is
/// complete even when the in-cluster mover cannot PATCH back (best-effort path).
/// The mover still PATCHes stats when it can reach the API server.
async fn finalize_succeeded(
    ctx: &Context,
    backup: &Backup,
    api: &Api<Backup>,
    name: &str,
    namespace: &str,
) -> Result<()> {
    // Try to resolve the snapshot id authoritatively for the filesystem backend.
    let snapshot = resolve_succeeded_snapshot(ctx, backup, namespace).await;
    let status = match snapshot {
        Ok(Some((id, identity))) => serde_json::json!({
            "phase": "Succeeded",
            "snapshot": {
                "kopiaSnapshotID": id,
                "identity": identity,
            },
        }),
        // Either object-store backend (mover PATCHes id) or no match yet.
        _ => serde_json::json!({ "phase": "Succeeded" }),
    };
    io::patch_status(api, name, status).await?;
    ctx.metrics
        .backup_last_success_timestamp
        .with_label_values(&[namespace, name])
        .set(chrono::Utc::now().timestamp());
    tracing::info!(backup = %name, "backup Job succeeded; phase=Succeeded");
    Ok(())
}

/// Resolve the newest snapshot matching this backup's identity for the
/// filesystem backend (in-process, ADR §5.4). Returns the snapshot id and a
/// status `identity` JSON body, or `None` when not resolvable in-process.
async fn resolve_succeeded_snapshot(
    ctx: &Context,
    backup: &Backup,
    namespace: &str,
) -> Result<Option<(String, serde_json::Value)>> {
    let (config, repo) = resolve_recipe(ctx, backup, namespace).await?;
    let identity = resolve_identity_for(&config, namespace)?;
    match &repo.spec.backend {
        Backend::Filesystem(fs) => {
            let creds = io::repo_credentials(&repo.spec.encryption);
            let password = io::read_repo_password(&ctx.client, namespace, &creds).await?;
            let client = ctx.kopia.build([("KOPIA_PASSWORD".to_string(), password)]);
            client
                .repository_connect(&kopiur_kopia::ConnectSpec::Filesystem {
                    path: fs.path.clone().into(),
                })
                .await?;
            // Match the snapshot by its source path (the path we snapshotted),
            // newest first. The pod's recorded user/host differ from our
            // resolved identity (a documented mover-identity follow-up), so we
            // key on the source path which IS authoritative.
            let mut list = client.snapshot_list(None).await?;
            list.sort_by_key(|e| std::cmp::Reverse(e.end_time));
            let matched = list
                .into_iter()
                .find(|e| e.source.path == identity.source_path);
            Ok(matched.map(|e| {
                let id = e.id.clone();
                let body = serde_json::json!({
                    "username": e.source.user_name,
                    "hostname": e.source.host,
                    "sourcePath": e.source.path,
                });
                (id, body)
            }))
        }
        _ => Ok(None),
    }
}

/// Resolve a `Backup`'s referenced `BackupConfig` and that config's
/// `Repository`. Cluster references and non-filesystem backends still resolve
/// here; backend-specific behavior is decided downstream.
async fn resolve_recipe(
    ctx: &Context,
    backup: &Backup,
    namespace: &str,
) -> Result<(BackupConfig, Repository)> {
    let config_ref = backup
        .spec
        .config_ref
        .as_ref()
        .ok_or_else(|| Error::Invariant("produced Backup has no configRef".into()))?;
    let cfg_ns = config_ref.namespace.as_deref().unwrap_or(namespace);
    let cfg_api: Api<BackupConfig> = Api::namespaced(ctx.client.clone(), cfg_ns);
    let config = cfg_api.get_opt(&config_ref.name).await?.ok_or_else(|| {
        Error::MissingDependency(format!("BackupConfig {cfg_ns}/{}", config_ref.name))
    })?;

    let repo_ref = &config.spec.repository;
    // NOTE: ClusterRepository-backed configs resolve their repo cluster-scoped;
    // this e2e/core path implements the namespaced Repository case fully. A
    // ClusterRepository lookup would use `Api::all` — left as a focused
    // follow-up since the namespaced path exercises the full backup pipeline.
    let repo_ns = repo_ref.namespace.as_deref().unwrap_or(cfg_ns);
    let repo_api: Api<Repository> = Api::namespaced(ctx.client.clone(), repo_ns);
    let repo = repo_api.get_opt(&repo_ref.name).await?.ok_or_else(|| {
        Error::MissingDependency(format!("Repository {repo_ns}/{}", repo_ref.name))
    })?;
    Ok((config, repo))
}

/// Build everything a backup run needs: the work spec, the source PVC mount, the
/// repo PVC mount (filesystem only), and the credentials Secret name.
type BackupRun<'a> = (MoverWorkSpec, Option<PvcMount>, Option<PvcMount>, String);
fn build_backup_run(
    _backup: &Backup,
    config: &BackupConfig,
    repo: &Repository,
    namespace: &str,
    _name: &str,
) -> Result<BackupRun<'static>> {
    let identity = resolve_identity_for(config, namespace)?;

    // First source's PVC + path drive the mount and the snapshot source path.
    let source = config
        .spec
        .sources
        .first()
        .ok_or_else(|| Error::Invariant("BackupConfig has no sources".into()))?;
    let pvc_name = source.pvc.as_ref().map(|p| p.name.clone()).ok_or_else(|| {
        Error::Invariant("e2e backup path requires an explicit source.pvc".into())
    })?;
    let source_path = source
        .source_path_override
        .clone()
        .unwrap_or_else(|| format!("/pvc/{pvc_name}"));

    let creds = io::repo_credentials(&repo.spec.encryption);

    let work_spec = MoverWorkSpec {
        version: 1,
        operation: Operation::Backup(BackupOp {
            source_path: source_path.clone(),
            tags: tags_for(config),
        }),
        identity,
        repository: repository_connect(repo)?,
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "Backup".to_string(),
            name: _name.to_string(),
            namespace: namespace.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
    };

    let source_pvc = Some(PvcMount {
        claim_name: pvc_name,
        mount_path: source_path,
        read_only: true,
    });
    let repo_pvc = io::filesystem_repo_pvc(&repo.spec.backend).map(|claim_name| PvcMount {
        claim_name,
        mount_path: io::filesystem_repo_path(&repo.spec.backend).unwrap_or_default(),
        read_only: false,
    });

    Ok((work_spec, source_pvc, repo_pvc, creds.secret_name))
}

/// Resolve identity from a `BackupConfig` (overrides + defaults) into the mover
/// wire identity. Reuses `api::identity::resolve_identity` (the tested kernel).
fn resolve_identity_for(config: &BackupConfig, namespace: &str) -> Result<MoverIdentity> {
    let pvc_name = config
        .spec
        .sources
        .first()
        .and_then(|s| s.pvc.as_ref().map(|p| p.name.clone()));
    let source_path_override = config
        .spec
        .sources
        .first()
        .and_then(|s| s.source_path_override.clone());
    let inputs = kopiur_api::IdentityInputs {
        object_name: &config.name_any(),
        namespace,
        overrides: config.spec.identity.as_ref(),
        template: None,
        pvc_name: pvc_name.as_deref(),
        source_path_override: source_path_override.as_deref(),
    };
    let resolved: ApiResolvedIdentity =
        kopiur_api::resolve_identity(&inputs).map_err(|e| Error::Validation(e.to_string()))?;
    Ok(MoverIdentity {
        username: resolved.username,
        hostname: resolved.hostname,
        source_path: resolved.source_path.unwrap_or_else(|| "/data".to_string()),
    })
}

/// Public wrapper so the restore reconciler can reuse the backend mapping.
pub(crate) fn repository_connect_pub(repo: &Repository) -> Result<RepositoryConnect> {
    repository_connect(repo)
}

/// Public wrapper for the mover image pull policy (reused by restore).
pub(crate) fn mover_pull_policy_pub() -> Option<&'static str> {
    mover_pull_policy()
}

/// Map a `Repository`'s backend to the mover's `RepositoryConnect`.
///
/// Exhaustive over every CRD `Backend` variant — a new backend cannot compile
/// until it is wired through to the mover. Credentials never appear here; they
/// flow to the mover Job as env vars from the referenced Secret (ADR §4.10).
fn repository_connect(repo: &Repository) -> Result<RepositoryConnect> {
    Ok(backend_to_repository_connect(&repo.spec.backend))
}

/// Pure `Backend -> RepositoryConnect` translation (no kube types), so it is
/// unit-testable and shared by the backup and restore reconcilers.
pub(crate) fn backend_to_repository_connect(backend: &Backend) -> RepositoryConnect {
    match backend {
        Backend::Filesystem(f) => RepositoryConnect::Filesystem {
            path: f.path.clone(),
        },
        Backend::S3(s) => RepositoryConnect::S3 {
            bucket: s.bucket.clone(),
            endpoint: s.endpoint.clone(),
            prefix: s.prefix.clone(),
            region: s.region.clone(),
        },
        Backend::Azure(a) => RepositoryConnect::Azure {
            container: a.container.clone(),
            storage_account: a.storage_account.clone(),
            prefix: a.prefix.clone(),
        },
        Backend::Gcs(g) => RepositoryConnect::Gcs {
            bucket: g.bucket.clone(),
            prefix: g.prefix.clone(),
        },
        Backend::B2(b) => RepositoryConnect::B2 {
            bucket: b.bucket.clone(),
            prefix: b.prefix.clone(),
        },
        Backend::Sftp(s) => RepositoryConnect::Sftp {
            host: s.host.clone(),
            path: s.path.clone(),
            port: s.port,
            username: s.username.clone(),
            keyfile: None,
        },
        Backend::WebDav(w) => RepositoryConnect::WebDav { url: w.url.clone() },
        Backend::Rclone(r) => RepositoryConnect::Rclone {
            remote_path: r.remote_path.clone(),
        },
    }
}

/// Snapshot tags from the config + run metadata.
fn tags_for(config: &BackupConfig) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    tags.insert("kopiur:config".to_string(), config.name_any());
    tags
}

/// Labels applied to the mover Job/ConfigMap and any child objects.
fn run_labels(config: &BackupConfig, origin: Origin) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(ORIGIN_LABEL.to_string(), origin_str(origin).to_string());
    labels.insert(CONFIG_LABEL.to_string(), config.name_any());
    labels
}

fn origin_str(origin: Origin) -> &'static str {
    match origin {
        Origin::Scheduled => "scheduled",
        Origin::Manual => "manual",
        Origin::Discovered => "discovered",
    }
}

/// Job limits from the backup's `failurePolicy`, falling back to ADR defaults.
fn job_limits(backup: &Backup) -> JobLimits {
    match &backup.spec.failure_policy {
        Some(fp) => JobLimits {
            backoff_limit: fp.backoff_limit.unwrap_or(2),
            active_deadline_seconds: fp.active_deadline_seconds,
        },
        None => JobLimits::default(),
    }
}

/// `IfNotPresent` when running against a locally-loaded mover image (kind e2e),
/// else `None` (cluster default). Controlled by the same env that picks the
/// image so the two stay consistent.
fn mover_pull_policy() -> Option<&'static str> {
    if std::env::var("KOPIUR_MOVER_IMAGE").is_ok() {
        Some("IfNotPresent")
    } else {
        None
    }
}

/// Whether a Job reached a terminal state: `Some(true)` complete, `Some(false)`
/// failed, `None` still running.
pub(crate) fn job_terminal_state(job: &Job) -> Option<bool> {
    let status = job.status.as_ref()?;
    let conditions = status.conditions.as_ref();
    if let Some(conds) = conditions {
        for c in conds {
            if c.status == "True" {
                match c.type_.as_str() {
                    "Complete" => return Some(true),
                    "Failed" => return Some(false),
                    _ => {}
                }
            }
        }
    }
    // Fall back to counts when conditions aren't populated yet.
    if status.succeeded.unwrap_or(0) >= 1 {
        return Some(true);
    }
    None
}

/// `error_policy` for the `Backup` controller.
pub fn error_policy(_backup: Arc<Backup>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("Backup", err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ann(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // --- backend_to_repository_connect: every CRD Backend variant must map to a
    // mover RepositoryConnect (no silent reject). A new Backend variant fails to
    // compile in the mapping until handled. ---

    #[test]
    fn every_backend_maps_to_a_repository_connect() {
        use kopiur_api::backend::{
            AzureBackend, B2Backend, FilesystemBackend, GcsBackend, RcloneBackend, S3Backend,
            SftpBackend, WebDavBackend,
        };
        let cases = vec![
            Backend::Filesystem(FilesystemBackend {
                path: "/repo".into(),
                pvc_name: None,
            }),
            Backend::S3(S3Backend {
                bucket: "b".into(),
                prefix: None,
                endpoint: None,
                region: None,
                auth: None,
                tls: None,
            }),
            Backend::Azure(AzureBackend {
                container: "c".into(),
                prefix: None,
                storage_account: Some("acct".into()),
                auth: None,
            }),
            Backend::Gcs(GcsBackend {
                bucket: "b".into(),
                prefix: None,
                auth: None,
            }),
            Backend::B2(B2Backend {
                bucket: "b".into(),
                prefix: None,
                auth: None,
            }),
            Backend::Sftp(SftpBackend {
                host: "h".into(),
                path: "/r".into(),
                port: Some(22),
                username: Some("u".into()),
                auth: None,
            }),
            Backend::WebDav(WebDavBackend {
                url: "https://dav".into(),
                auth: None,
            }),
            Backend::Rclone(RcloneBackend {
                remote_path: "r:bucket".into(),
                config_secret_ref: None,
            }),
        ];
        // Each maps without panicking and converts cleanly to a kopia ConnectSpec
        // whose discriminant matches the backend kind.
        for backend in cases {
            let rc = backend_to_repository_connect(&backend);
            let spec = rc.to_connect_spec();
            let want = match backend.kind_str() {
                "WebDav" => "webdav",
                other => &other.to_ascii_lowercase(),
            };
            assert_eq!(spec.kind_str(), want, "backend {}", backend.kind_str());
        }
    }

    // --- plan_deletion: exhaustive over every DeletionPolicy ----------------

    #[test]
    fn delete_policy_plans_snapshot_delete() {
        assert_eq!(
            plan_deletion(DeletionPolicy::Delete, &BTreeMap::new()),
            DeletionPlan::DeleteSnapshot
        );
    }

    #[test]
    fn retain_policy_plans_retain() {
        assert_eq!(
            plan_deletion(DeletionPolicy::Retain, &BTreeMap::new()),
            DeletionPlan::RetainSnapshot
        );
    }

    #[test]
    fn orphan_policy_plans_orphan() {
        assert_eq!(
            plan_deletion(DeletionPolicy::Orphan, &BTreeMap::new()),
            DeletionPlan::OrphanSnapshot
        );
    }

    #[test]
    fn skip_annotation_overrides_delete_to_orphan() {
        // The repo-offline escape hatch: even Delete becomes Orphan so we never
        // contact a dead repository.
        let a = ann(&[(SKIP_SNAPSHOT_CLEANUP_ANNOTATION, "true")]);
        assert_eq!(
            plan_deletion(DeletionPolicy::Delete, &a),
            DeletionPlan::OrphanSnapshot
        );
    }

    #[test]
    fn skip_annotation_overrides_every_policy() {
        let a = ann(&[(SKIP_SNAPSHOT_CLEANUP_ANNOTATION, "")]);
        for p in [
            DeletionPolicy::Delete,
            DeletionPolicy::Retain,
            DeletionPolicy::Orphan,
        ] {
            assert_eq!(plan_deletion(p, &a), DeletionPlan::OrphanSnapshot);
        }
    }

    #[test]
    fn unrelated_annotations_do_not_trigger_skip() {
        let a = ann(&[("kopiur.dev/other", "x")]);
        assert_eq!(
            plan_deletion(DeletionPolicy::Delete, &a),
            DeletionPlan::DeleteSnapshot
        );
    }

    // --- effective_deletion_policy ------------------------------------------

    #[test]
    fn discovered_is_forced_to_retain_regardless_of_spec() {
        for p in [
            None,
            Some(DeletionPolicy::Delete),
            Some(DeletionPolicy::Orphan),
            Some(DeletionPolicy::Retain),
        ] {
            assert_eq!(
                effective_deletion_policy(p, Origin::Discovered),
                DeletionPolicy::Retain
            );
        }
    }

    #[test]
    fn produced_defaults_to_delete_when_unset() {
        assert_eq!(
            effective_deletion_policy(None, Origin::Scheduled),
            DeletionPolicy::Delete
        );
        assert_eq!(
            effective_deletion_policy(None, Origin::Manual),
            DeletionPolicy::Delete
        );
    }

    #[test]
    fn produced_honors_explicit_spec_policy() {
        assert_eq!(
            effective_deletion_policy(Some(DeletionPolicy::Orphan), Origin::Manual),
            DeletionPolicy::Orphan
        );
        assert_eq!(
            effective_deletion_policy(Some(DeletionPolicy::Retain), Origin::Scheduled),
            DeletionPolicy::Retain
        );
    }
}
