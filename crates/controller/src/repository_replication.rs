//! The `RepositoryReplication` reconciler (ADR-0005 §13(d)).
//!
//! Mirrors the `Maintenance` scheduler (`crate::maintenance`): the controller is the
//! *scheduler*. Each reconcile it decides whether a replication is due (croner +
//! deterministic jitter via [`crate::snapshot_schedule::next_fire`], seeded by the
//! CR UID), gates on the source repository being Ready, then spawns at most one
//! per-slot owned mover Job (`kopia repository sync-to`) and tracks it to terminal.
//! The mover PATCHes `.status` (phase, `lastReplicated`).
//!
//! Hardening matches maintenance: per-slot deterministic Job names, single-flight via
//! a label selector, a repo-ready gate, a requeue cap, and transition-guarded status.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::Job;
use kube::api::ListParams;
use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};

use kopiur_api::{RepositoryReplication, validate};
use kopiur_mover::workspec::{
    MoverOptions, MoverWorkSpec, Operation, ReplicateOp, ResolvedIdentity, TargetRef,
};

use crate::consts::{
    API_VERSION, COMPONENT_LABEL, REPLICATION_COMPONENT, REPLICATION_INSTANCE_LABEL,
    REPLICATION_SLOT_ANNOTATION,
};
use crate::context::Context;
use crate::error::{Error, Result, error_policy_for};
use crate::io::{self, ResolvedRepository};
use crate::jobs::{self, JobLimits, MoverJobInputs, VolumeMountSpec};
use crate::snapshot::{backend_to_repository_connect, job_terminal_state, mover_pull_policy_pub};
use crate::snapshot_schedule::{next_fire, parse_go_duration};

/// How long a finished replication Job lingers before TTL-reaping.
const REPLICATION_JOB_TTL_SECS: i64 = 3600;
/// Requeue while a replication Job is in flight.
const REQUEUE_RUNNING: Duration = Duration::from_secs(30);
/// Requeue while waiting for the source repository to become Ready.
const REQUEUE_NOT_READY: Duration = Duration::from_secs(60);
/// Requeue after a failed replication Job (re-check / bounded retry once TTL-reaped).
const REQUEUE_FAILED: Duration = Duration::from_secs(300);
/// Upper bound on any requeue so the schedule/readiness is re-evaluated.
const REQUEUE_CAP: Duration = Duration::from_secs(1800);

/// Reconcile a `RepositoryReplication`.
#[tracing::instrument(skip(repl, ctx), fields(kind = "RepositoryReplication", namespace = %repl.namespace().unwrap_or_default(), name = %repl.name_any()))]
pub async fn reconcile(
    repl: std::sync::Arc<RepositoryReplication>,
    ctx: std::sync::Arc<Context>,
) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&repl, &ctx).await;
    ctx.metrics
        .record_reconcile("RepositoryReplication", start.elapsed().as_secs_f64());
    result
}

async fn reconcile_inner(repl: &RepositoryReplication, ctx: &Context) -> Result<Action> {
    // Defensive re-validation (one validator, two callers).
    let errs = validate::validate_repository_replication(&repl.spec);
    if let Some(first) = errs.into_iter().next() {
        return Err(Error::Validation(first.to_string()));
    }

    let namespace = repl
        .namespace()
        .ok_or_else(|| Error::Invariant("RepositoryReplication has no namespace".into()))?;
    let name = repl.name_any();
    let api: Api<RepositoryReplication> = Api::namespaced(ctx.client.clone(), &namespace);

    // §14(e): a suspended replication is skipped (surface phase + Ready=Reconciling).
    if repl.spec.suspend {
        patch_ready_if_changed(
            &api,
            &name,
            repl,
            io::ReadyOutcome::Reconciling,
            "Suspended",
            "replication is suspended (spec.suspend)",
            Some("Suspended"),
        )
        .await?;
        return Ok(Action::requeue(REQUEUE_CAP));
    }

    let source_ref = &repl.spec.source_ref;
    let repo = io::resolve_repository_ref(&ctx.client, source_ref, &namespace).await?;

    // Gate on the source repository being Ready (an object-store repo must be
    // bootstrapped before `sync-to` can reach it) — mirrors maintenance's G7.
    if !io::repository_ready(&ctx.client, source_ref, &namespace).await? {
        patch_ready_if_changed(
            &api,
            &name,
            repl,
            io::ReadyOutcome::Reconciling,
            "WaitingForRepository",
            "source repository is not Ready; deferring replication",
            None,
        )
        .await?;
        return Ok(Action::requeue(REQUEUE_NOT_READY));
    }

    let now = Utc::now();
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), &namespace);

    let Some(slot) = due_slot(repl, now) else {
        // Mark Ready (idle, waiting for the next slot) and sleep.
        patch_ready_if_changed(
            &api,
            &name,
            repl,
            io::ReadyOutcome::Ready,
            "Idle",
            "replication is reconciled; waiting for the next scheduled slot",
            None,
        )
        .await?;
        return Ok(Action::requeue(cap(next_wakeup(repl, now, None))));
    };

    let job_name = replication_job_name(&name, slot);
    match job_api.get_opt(&job_name).await? {
        Some(job) => match job_terminal_state(&job) {
            // Success: the mover stamped status; sleep until the next slot.
            Some(true) => Ok(Action::requeue(cap(next_wakeup(repl, now, Some(slot))))),
            Some(false) => {
                patch_ready_if_changed(
                    &api,
                    &name,
                    repl,
                    io::ReadyOutcome::Stalled,
                    "ReplicationFailed",
                    "replication Job failed; see the Job/pod logs",
                    Some("Failed"),
                )
                .await?;
                Ok(Action::requeue(REQUEUE_FAILED))
            }
            None => Ok(Action::requeue(REQUEUE_RUNNING)),
        },
        None => {
            if has_active_replication_job(&job_api, &name).await? {
                return Ok(Action::requeue(REQUEUE_RUNNING));
            }
            spawn_replication_job(ctx, &namespace, &name, &job_name, repl, &repo, slot).await?;
            tracing::info!(replication = %name, slot = %slot.to_rfc3339(), "spawned replication Job");
            Ok(Action::requeue(REQUEUE_RUNNING))
        }
    }
}

/// Build + apply the per-slot replication mover Job.
#[allow(clippy::too_many_arguments)]
async fn spawn_replication_job(
    ctx: &Context,
    namespace: &str,
    cr_name: &str,
    job_name: &str,
    repl: &RepositoryReplication,
    repo: &ResolvedRepository,
    slot: DateTime<Utc>,
) -> Result<()> {
    let work_spec = build_replication_work_spec(repl, repo, namespace, cr_name);

    let mut labels = BTreeMap::new();
    labels.insert(
        COMPONENT_LABEL.to_string(),
        REPLICATION_COMPONENT.to_string(),
    );
    labels.insert(REPLICATION_INSTANCE_LABEL.to_string(), cr_name.to_string());
    let mut annotations = BTreeMap::new();
    annotations.insert(REPLICATION_SLOT_ANNOTATION.to_string(), slot.to_rfc3339());

    // Source filesystem repos need the repo volume mounted; object stores reach the
    // backend over the network.
    let repo_volume =
        io::filesystem_repo_mount_source(&repo.backend).map(|source| VolumeMountSpec {
            source,
            mount_path: io::filesystem_repo_path(&repo.backend).unwrap_or_default(),
            read_only: false,
        });
    // A filesystem DESTINATION needs its volume mounted too — `kopia repository
    // sync-to` writes the mirror into it. Carried in the `source_volume` slot (the Job
    // builder just turns it into a pod volume/mount at the destination's path, which
    // the webhook guarantees differs from the source repo's path, so the two mounts
    // never collide). Object-store destinations reach the backend over the network.
    let dest_volume =
        io::filesystem_repo_mount_source(&repl.spec.destination).map(|source| VolumeMountSpec {
            source,
            mount_path: io::filesystem_repo_path(&repl.spec.destination).unwrap_or_default(),
            read_only: false,
        });
    let owner = io::owner_ref_for(repl, "RepositoryReplication")?;

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

    let creds = io::resolve_mover_creds_for(
        &ctx.client,
        namespace,
        job_name,
        &owner,
        repo,
        // RepositoryReplication has no credentialProjection of its own; the source
        // repo's Secret must co-reside (the CR lives in the source's namespace).
        false,
        io::repo_kind_str(repl.spec.source_ref.kind),
        &repl.spec.source_ref.name,
    )
    .await?;
    if creds.projected > 0 {
        ctx.metrics
            .inc_secrets_projected(namespace, creds.projected);
    }
    let creds_secrets = creds.names;

    let resolved_mover = kopiur_api::common::resolve_mover(
        repo.mover_defaults.as_ref(),
        repl.spec
            .mover
            .as_ref()
            .and_then(|m| m.security_context.as_ref()),
        repl.spec
            .mover
            .as_ref()
            .and_then(|m| m.pod_security_context.as_ref()),
        repl.spec.mover.as_ref().and_then(|m| m.resources.as_ref()),
        repl.spec.mover.as_ref().and_then(|m| m.cache.as_ref()),
        repl.spec
            .mover
            .as_ref()
            .and_then(|m| m.ttl_seconds_after_finished),
    );
    let limits = JobLimits {
        ttl_seconds_after_finished: resolved_mover
            .ttl_seconds_after_finished
            .or(Some(REPLICATION_JOB_TTL_SECS)),
        ..JobLimits::default()
    };

    let inputs = MoverJobInputs {
        name: job_name,
        namespace,
        owner,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: mover_pull_policy_pub(),
        limits,
        resources: resolved_mover.resources.clone(),
        security_context: resolved_mover.security_context.clone(),
        pod_security_context: resolved_mover.pod_security_context.clone(),
        node_selector: resolved_mover.node_selector.clone(),
        tolerations: resolved_mover.tolerations.clone(),
        affinity: resolved_mover.affinity.clone(),
        labels,
        source_volume: dest_volume,
        repo_volume,
        creds_secrets,
        result_configmap: None,
        service_account: ctx.mover_service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
        annotations,
        cache_volume: Default::default(),
    };
    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, namespace, job_name, &cm, &job).await?;
    Ok(())
}

/// Build the replication mover work spec. Pure (no IO) so the source→destination
/// mapping is unit-testable: connect to the source repository, sync-to the
/// destination backend.
pub fn build_replication_work_spec(
    repl: &RepositoryReplication,
    repo: &ResolvedRepository,
    namespace: &str,
    cr_name: &str,
) -> MoverWorkSpec {
    MoverWorkSpec {
        version: 1,
        operation: Operation::Replicate(ReplicateOp {
            destination: backend_to_repository_connect(&repl.spec.destination),
            // Additive sync by default (never prune the destination automatically).
            delete_extra: false,
        }),
        // Replication does not snapshot; a stable sentinel identity (like maintenance).
        identity: ResolvedIdentity {
            username: "kopiur-replication".to_string(),
            hostname: namespace.to_string(),
            source_path: String::new(),
        },
        repository: backend_to_repository_connect(&repo.backend),
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "RepositoryReplication".to_string(),
            name: cr_name.to_string(),
            namespace: namespace.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
        cache: Default::default(),
        throttle: io::throttle_spec(repo.mover_defaults.as_ref()),
    }
}

/// The replication slot due now (cron + jitter strictly after the last run), or
/// `None` if not yet due. Pure given the CR and `now`.
pub fn due_slot(repl: &RepositoryReplication, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let after = last_run_at(repl).unwrap_or_else(|| now - chrono::Duration::days(365));
    match slot_for(repl, after) {
        Ok(slot) if now >= slot => Some(slot),
        _ => None,
    }
}

/// The next cron slot for this replication strictly after `after` (croner + jitter,
/// seeded by the CR UID).
fn slot_for(repl: &RepositoryReplication, after: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let seed = repl.uid().unwrap_or_else(|| repl.name_any());
    let jitter = repl
        .spec
        .schedule
        .jitter
        .as_deref()
        .and_then(parse_go_duration);
    next_fire(&repl.spec.schedule.cron, jitter, &seed, after)
}

/// Parse `status.lastReplicated` (RFC3339) into a `DateTime<Utc>`.
fn last_run_at(repl: &RepositoryReplication) -> Option<DateTime<Utc>> {
    repl.status
        .as_ref()
        .and_then(|s| s.last_replicated.as_deref())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// How long until the next replication slot. When `handled` is set, that slot is the
/// search anchor (so a just-handled slot doesn't immediately re-fire). Floored at the
/// running cadence, capped by the caller.
fn next_wakeup(
    repl: &RepositoryReplication,
    now: DateTime<Utc>,
    handled: Option<DateTime<Utc>>,
) -> Duration {
    let after = handled.unwrap_or_else(|| last_run_at(repl).unwrap_or(now));
    match slot_for(repl, after) {
        Ok(slot) if slot > now => (slot - now)
            .to_std()
            .unwrap_or(REQUEUE_CAP)
            .max(REQUEUE_RUNNING),
        _ => REQUEUE_RUNNING,
    }
}

/// Cap a requeue so the schedule/readiness is re-evaluated within the heartbeat.
fn cap(d: Duration) -> Duration {
    d.min(REQUEUE_CAP)
}

/// Deterministic, ≤52-char, DNS-1123-safe per-slot replication Job name:
/// `<cr>-repl-<unix_slot>` (truncate+hash long names, like maintenance).
fn replication_job_name(cr: &str, slot: DateTime<Utc>) -> String {
    const MAX: usize = 52;
    let suffix = format!("-repl-{}", slot.timestamp());
    let budget = MAX.saturating_sub(suffix.len());
    if cr.len() <= budget {
        format!("{cr}{suffix}")
    } else {
        let hash = short_hash(cr);
        let keep = budget.saturating_sub(hash.len() + 1);
        let trunc: String = cr.chars().take(keep).collect();
        format!("{trunc}-{hash}{suffix}")
    }
}

/// A short, stable 8-hex-char FNV-1a hash for name truncation (matches maintenance).
fn short_hash(s: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{:08x}", (h & 0xffff_ffff))
}

/// Whether any non-terminal replication Job is owned by this CR (single-flight gate).
async fn has_active_replication_job(job_api: &Api<Job>, cr_name: &str) -> Result<bool> {
    let selector =
        format!("{COMPONENT_LABEL}={REPLICATION_COMPONENT},{REPLICATION_INSTANCE_LABEL}={cr_name}");
    let jobs = job_api
        .list(&ListParams::default().labels(&selector))
        .await?;
    Ok(jobs.items.iter().any(|j| job_terminal_state(j).is_none()))
}

/// Patch the kstatus Ready conditions (+ optional phase + destinationBackend) only
/// when the `Ready` condition changes, so the reconcile does not hot-loop on its own
/// status writes (transition-guarded). `phase` is the optional phase string to set.
async fn patch_ready_if_changed(
    api: &Api<RepositoryReplication>,
    name: &str,
    repl: &RepositoryReplication,
    outcome: io::ReadyOutcome,
    reason: &str,
    message: &str,
    phase: Option<&str>,
) -> Result<()> {
    let existing: Vec<_> = repl
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();
    let current = existing
        .iter()
        .find(|c| c.type_ == "Ready")
        .map(|c| (c.status.clone(), c.reason.clone()));
    let target_status = match outcome {
        io::ReadyOutcome::Ready => "True",
        _ => "False",
    };
    if current.as_ref() == Some(&(target_status.to_string(), reason.to_string())) {
        return Ok(());
    }
    let observed_gen = repl.metadata.generation.unwrap_or(0);
    let conditions = io::set_ready(&existing, Some(observed_gen), outcome, reason, message);
    let mut status = serde_json::json!({
        "observedGeneration": observed_gen,
        "conditions": conditions,
        // Mirror the destination backend kind for the print column (deterministic).
        "destinationBackend": repl.spec.destination.kind_str(),
    });
    if let Some(p) = phase {
        status["phase"] = serde_json::json!(p);
    }
    io::patch_status(api, name, status).await?;
    Ok(())
}

/// `error_policy` for the `RepositoryReplication` controller.
pub fn error_policy(
    _obj: std::sync::Arc<RepositoryReplication>,
    err: &Error,
    ctx: std::sync::Arc<Context>,
) -> Action {
    error_policy_for("RepositoryReplication", err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::backend::{Backend, FilesystemBackend, S3Backend};
    use kopiur_api::common::{
        CronSpec, Encryption, RepositoryKind, RepositoryMode, RepositoryRef, SecretKeyRef,
    };
    use kopiur_api::{RepositoryReplicationSpec, RepositoryReplicationStatus};

    fn repl_with(cron: &str, status: Option<RepositoryReplicationStatus>) -> RepositoryReplication {
        let mut r = RepositoryReplication::new(
            "offsite",
            RepositoryReplicationSpec {
                source_ref: RepositoryRef {
                    kind: RepositoryKind::Repository,
                    name: "nas-primary".into(),
                    namespace: None,
                },
                destination: Backend::S3(S3Backend {
                    bucket: "mirror".into(),
                    prefix: None,
                    endpoint: None,
                    region: None,
                    auth: None,
                    tls: None,
                }),
                destination_encryption: None,
                schedule: CronSpec {
                    cron: cron.into(),
                    jitter: None,
                },
                mover: None,
                suspend: false,
            },
        );
        r.metadata.uid = Some("uid-repl-1".into());
        r.status = status;
        r
    }

    fn sample_repo() -> ResolvedRepository {
        ResolvedRepository {
            backend: Backend::Filesystem(FilesystemBackend {
                path: "/repo".into(),
                volume: None,
            }),
            encryption: Encryption {
                password_secret_ref: SecretKeyRef {
                    name: "s".into(),
                    namespace: None,
                    key: None,
                },
            },
            repo_namespace: Some("ns".into()),
            mover_defaults: None,
            identity_defaults: None,
            on_namespace_delete: Default::default(),
            credential_projection_allowed: false,
            mode: RepositoryMode::ReadWrite,
        }
    }

    #[test]
    fn first_ever_reconcile_is_due() {
        let r = repl_with("0 5 * * *", None);
        assert!(due_slot(&r, Utc::now()).is_some());
    }

    #[test]
    fn not_due_right_after_a_run() {
        let now = Utc::now();
        let just = (now - chrono::Duration::seconds(1)).to_rfc3339();
        let status = RepositoryReplicationStatus {
            last_replicated: Some(just),
            ..Default::default()
        };
        let r = repl_with("0 5 * * *", Some(status));
        assert!(
            due_slot(&r, now).is_none(),
            "a replication that just ran must not be immediately due again"
        );
    }

    #[test]
    fn requeue_is_capped() {
        let now = Utc::now();
        let just = (now - chrono::Duration::seconds(1)).to_rfc3339();
        let status = RepositoryReplicationStatus {
            last_replicated: Some(just),
            ..Default::default()
        };
        let r = repl_with("0 5 * * *", Some(status));
        assert!(cap(next_wakeup(&r, now, None)) <= REQUEUE_CAP);
    }

    #[test]
    fn job_name_deterministic_and_bounded() {
        let slot = DateTime::parse_from_rfc3339("2026-06-09T05:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let n = replication_job_name("offsite", slot);
        assert!(n.len() <= 52);
        assert!(n.starts_with("offsite-repl-"));
        assert_eq!(n, replication_job_name("offsite", slot));
        let long = "a-very-long-repository-replication-name-blowing-the-dns-budget";
        assert!(replication_job_name(long, slot).len() <= 52);
    }

    #[test]
    fn work_spec_maps_source_and_destination() {
        let r = repl_with("0 5 * * *", None);
        let repo = sample_repo();
        let ws = build_replication_work_spec(&r, &repo, "ns", "offsite");
        // Operation is Replicate with the S3 destination; source is the filesystem repo.
        match &ws.operation {
            Operation::Replicate(op) => {
                assert_eq!(op.destination.kind_str(), "S3");
                assert!(!op.delete_extra);
            }
            other => panic!("expected replicate op, got {}", other.kind_str()),
        }
        assert_eq!(ws.repository.kind_str(), "Filesystem");
        assert_eq!(ws.target_ref.kind, "RepositoryReplication");
    }
}
