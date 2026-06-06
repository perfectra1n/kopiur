//! The `Maintenance` reconciler (ADR §3.7, §4.5).
//!
//! Maintenance runs in a dedicated **mover pod** for every backend (filesystem
//! and object-store alike), consistent with backup/restore/bootstrap. The
//! controller is the *scheduler*: each reconcile it decides whether a quick or
//! full pass is due (croner + deterministic jitter via
//! [`crate::backup_schedule::next_fire`], full subsumes quick), then spawns at
//! most one per-slot mover Job and tracks it to terminal state. The lease
//! decision ([`kopiur_api::lease_action`]) lives in the mover, because reading
//! the current holder (`kopia maintenance info`) needs repo access the
//! controller does not have for object stores.
//!
//! Hardening (see the design doc): per-slot deterministic Job names for
//! idempotency (G1), `ttlSecondsAfterFinished` so finished Jobs self-reap (G2),
//! single-flight via a label selector (G3), a repository-readiness gate (G7),
//! a requeue cap so the lease/health is re-checked (G8), and transition-guarded
//! status writes so the reconcile does not hot-loop on its own status (G6).

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{DeleteParams, ListParams};
use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};

use kopiur_api::{Maintenance, validate};
use kopiur_kopia::MaintenanceMode;
use kopiur_mover::workspec::{
    MaintenanceOp, MoverOptions, MoverWorkSpec, Operation, ResolvedIdentity, TargetRef,
};

use crate::backup::{backend_to_repository_connect, job_terminal_state, mover_pull_policy_pub};
use crate::backup_schedule::{next_fire, parse_go_duration};
use crate::consts::{
    API_VERSION, COMPONENT_LABEL, MAINTENANCE_COMPONENT, MAINTENANCE_INSTANCE_LABEL,
    MAINTENANCE_SLOT_ANNOTATION,
};
use crate::context::Context;
use crate::error::{Error, Result, error_policy_for};
use crate::io;
use crate::jobs::{self, JobLimits, MoverJobInputs, PvcMount};

/// How long a finished maintenance Job lingers before the TTL controller reaps
/// it (G2). Long enough that the controller reliably observes the terminal state
/// on its requeue cadence; short enough that per-slot Jobs do not pile up.
const MAINTENANCE_JOB_TTL_SECS: i64 = 3600;

/// Requeue while a maintenance Job is in flight (poll for terminal state).
const REQUEUE_RUNNING: Duration = Duration::from_secs(30);
/// Requeue while waiting for the repository to become `Ready` (G7).
const REQUEUE_NOT_READY: Duration = Duration::from_secs(60);
/// Requeue after a failed maintenance Job: re-check (and, once the failed Job is
/// TTL-reaped, re-spawn as a bounded retry).
const REQUEUE_FAILED: Duration = Duration::from_secs(300);
/// Upper bound on any requeue, so the lease/health/readiness is re-evaluated even
/// when the next slot is hours away (G8; aligned with the operator heartbeat).
const REQUEUE_CAP: Duration = Duration::from_secs(1800);

/// Reconcile a `Maintenance`.
#[tracing::instrument(skip(maint, ctx), fields(kind = "Maintenance", namespace = %maint.namespace().unwrap_or_default(), name = %maint.name_any()))]
pub async fn reconcile(
    maint: std::sync::Arc<Maintenance>,
    ctx: std::sync::Arc<Context>,
) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&maint, &ctx).await;
    ctx.metrics
        .record_reconcile("Maintenance", start.elapsed().as_secs_f64());
    record_maintenance_status_metrics(&maint, &ctx, result.is_ok()).await;
    result
}

/// Mirror the last full-maintenance reclaimed-bytes gauge from the freshest
/// status on success (Maintenance has no phase gauge to clear). See the Backup
/// equivalent for why the status is re-read rather than taken from the cache copy.
async fn record_maintenance_status_metrics(maint: &Maintenance, ctx: &Context, ok: bool) {
    let (Some(ns), name) = (maint.namespace(), maint.name_any()) else {
        return;
    };
    if !ok {
        return;
    }
    let api: Api<Maintenance> = Api::namespaced(ctx.client.clone(), &ns);
    if let Ok(Some(latest)) = api.get_opt(&name).await
        && let Some(bytes) = latest
            .status
            .as_ref()
            .and_then(|s| s.full.as_ref())
            .and_then(|f| f.last_content_reclaimed_bytes)
    {
        ctx.metrics
            .set_maintenance_reclaimed_bytes(&ns, &name, bytes);
    }
}

async fn reconcile_inner(maint: &Maintenance, ctx: &Context) -> Result<Action> {
    let errs = validate::validate_maintenance(&maint.spec);
    if let Some(first) = errs.into_iter().next() {
        return Err(Error::Validation(first.to_string()));
    }

    let namespace = maint
        .namespace()
        .ok_or_else(|| Error::Invariant("Maintenance has no namespace".into()))?;
    let name = maint.name_any();
    let api: Api<Maintenance> = Api::namespaced(ctx.client.clone(), &namespace);

    let repo_ref = &maint.spec.repository;
    let repo = io::resolve_repository_ref(&ctx.client, repo_ref, &namespace).await?;

    // G7: an object-store repository must be bootstrapped (connected/created)
    // before `kopia maintenance` can reach it. Spawning earlier just produces a
    // doomed pod, so wait for the repository to report `Ready`.
    if !io::repository_ready(&ctx.client, repo_ref, &namespace).await? {
        patch_condition_if_changed(
            &api,
            &name,
            maint,
            "False",
            "WaitingForRepository",
            "target repository is not Ready; deferring maintenance",
        )
        .await?;
        return Ok(Action::requeue(REQUEUE_NOT_READY));
    }

    let now = Utc::now();
    let job_api: Api<Job> = Api::namespaced(ctx.client.clone(), &namespace);

    // Nothing due → sleep until the earliest next slot (capped).
    let Some((mode, slot)) = due_mode(maint, now) else {
        return Ok(Action::requeue(cap(next_wakeup(maint, now, None))));
    };

    let job_name = maintenance_job_name(&name, mode, slot);
    match job_api.get_opt(&job_name).await? {
        Some(job) => match job_terminal_state(&job) {
            // Succeeded: the slot was handled (the mover ran maintenance, or
            // yielded the lease and recorded a condition). The work-spec
            // ConfigMap is only needed while the pod runs — drop it so per-slot
            // ConfigMaps do not accumulate (the Job self-reaps via TTL). Then
            // sleep until the next slot.
            Some(true) => {
                delete_work_spec_cm(ctx, &namespace, &job_name).await;
                Ok(Action::requeue(cap(next_wakeup(
                    maint,
                    now,
                    Some((mode, slot)),
                ))))
            }
            // Failed: surface the condition once (transition-guarded) and re-check.
            // The failed Job lingers until its TTL, then a fresh reconcile
            // re-spawns this slot as a bounded retry.
            Some(false) => {
                patch_condition_if_changed(
                    &api,
                    &name,
                    maint,
                    "False",
                    "MaintenanceFailed",
                    "maintenance Job failed; see the Job/pod logs",
                )
                .await?;
                Ok(Action::requeue(REQUEUE_FAILED))
            }
            // Still running: poll.
            None => Ok(Action::requeue(REQUEUE_RUNNING)),
        },
        None => {
            // G3: never run two maintenance Jobs for one repository at once.
            if has_active_maintenance_job(&job_api, &name).await? {
                return Ok(Action::requeue(REQUEUE_RUNNING));
            }
            spawn_maintenance_job(ctx, &namespace, &name, &job_name, maint, &repo, mode, slot)
                .await?;
            tracing::info!(maint = %name, ?mode, slot = %slot.to_rfc3339(), "spawned maintenance Job");
            Ok(Action::requeue(REQUEUE_RUNNING))
        }
    }
}

/// Build + apply the per-slot work-spec ConfigMap and mover Job.
#[allow(clippy::too_many_arguments)]
async fn spawn_maintenance_job(
    ctx: &Context,
    namespace: &str,
    cr_name: &str,
    job_name: &str,
    maint: &Maintenance,
    repo: &io::ResolvedRepository,
    mode: MaintenanceMode,
    slot: DateTime<Utc>,
) -> Result<()> {
    let work_spec = MoverWorkSpec {
        version: 1,
        operation: Operation::Maintenance(MaintenanceOp {
            mode,
            owner: maint.spec.ownership.owner.clone(),
            takeover_policy: maint.spec.ownership.takeover_policy,
        }),
        // Maintenance does not snapshot, so the identity is a stable sentinel
        // (like bootstrap's) — it is not a kopia snapshot source.
        identity: ResolvedIdentity {
            username: "kopiur-maintenance".to_string(),
            hostname: namespace.to_string(),
            source_path: String::new(),
        },
        repository: backend_to_repository_connect(&repo.backend),
        target_ref: TargetRef {
            api_version: API_VERSION.to_string(),
            kind: "Maintenance".to_string(),
            name: cr_name.to_string(),
            namespace: namespace.to_string(),
        },
        hook_plan: Default::default(),
        options: MoverOptions::default(),
    };

    let mut labels = BTreeMap::new();
    labels.insert(
        COMPONENT_LABEL.to_string(),
        MAINTENANCE_COMPONENT.to_string(),
    );
    labels.insert(MAINTENANCE_INSTANCE_LABEL.to_string(), cr_name.to_string());

    let mut annotations = BTreeMap::new();
    annotations.insert(MAINTENANCE_SLOT_ANNOTATION.to_string(), slot.to_rfc3339());

    // Filesystem repos need the repo PVC mounted read-write; object stores reach
    // the backend over the network (creds via env), so no PVC.
    let repo_pvc = io::filesystem_repo_pvc(&repo.backend).map(|claim_name| PvcMount {
        claim_name,
        mount_path: io::filesystem_repo_path(&repo.backend).unwrap_or_default(),
        read_only: false,
    });
    let creds_secrets = io::mover_creds_secrets(&repo.backend, &repo.encryption);

    let inputs = MoverJobInputs {
        name: job_name,
        namespace,
        owner: io::owner_ref_for(maint, "Maintenance")?,
        work_spec: &work_spec,
        image: &ctx.mover_image,
        image_pull_policy: mover_pull_policy_pub(),
        limits: maintenance_job_limits(maint),
        resources: maint.spec.mover.as_ref().and_then(|m| m.resources.clone()),
        security_context: maint
            .spec
            .mover
            .as_ref()
            .and_then(|m| m.security_context.clone()),
        labels,
        source_pvc: None,
        repo_pvc,
        creds_secrets,
        result_configmap: None,
        service_account: ctx.mover_service_account.as_deref(),
        passthrough_env: ctx.mover_env_passthrough.clone(),
        annotations,
    };
    let cm = jobs::build_config_map(&inputs)?;
    let job = jobs::build_job(&inputs);
    io::apply_mover_objects(&ctx.client, namespace, job_name, &cm, &job).await?;
    Ok(())
}

/// Job limits for a maintenance run: backoff/deadline from `failurePolicy`
/// (falling back to defaults), plus a TTL so finished per-slot Jobs self-reap.
fn maintenance_job_limits(maint: &Maintenance) -> JobLimits {
    let base = JobLimits::default();
    match &maint.spec.failure_policy {
        Some(fp) => JobLimits {
            backoff_limit: fp.backoff_limit.unwrap_or(base.backoff_limit),
            active_deadline_seconds: fp.active_deadline_seconds,
            ttl_seconds_after_finished: Some(MAINTENANCE_JOB_TTL_SECS),
        },
        None => JobLimits {
            ttl_seconds_after_finished: Some(MAINTENANCE_JOB_TTL_SECS),
            ..base
        },
    }
}

/// Choose the maintenance pass due now, preferring full (it subsumes quick).
/// Returns the mode and its scheduled slot, or `None` if nothing is due.
fn due_mode(maint: &Maintenance, now: DateTime<Utc>) -> Option<(MaintenanceMode, DateTime<Utc>)> {
    for mode in [MaintenanceMode::Full, MaintenanceMode::Quick] {
        if let Ok(slot) = slot_for(maint, mode, mode_after(maint, mode))
            && now >= slot
        {
            return Some((mode, slot));
        }
    }
    None
}

/// The instant after which to search for `mode`'s next slot: its last run, or a
/// year ago so the first-ever reconcile fires immediately.
fn mode_after(maint: &Maintenance, mode: MaintenanceMode) -> DateTime<Utc> {
    last_run_at(maint, mode).unwrap_or_else(|| Utc::now() - chrono::Duration::days(365))
}

/// The next cron slot for `mode` strictly after `after` (croner + jitter, seeded
/// by the CR UID for a stable per-replica spread).
fn slot_for(
    maint: &Maintenance,
    mode: MaintenanceMode,
    after: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    let seed = maint.uid().unwrap_or_else(|| maint.name_any());
    let spec = match mode {
        MaintenanceMode::Quick => &maint.spec.schedule.quick,
        MaintenanceMode::Full => &maint.spec.schedule.full,
    };
    let jitter = spec.jitter.as_deref().and_then(parse_go_duration);
    next_fire(&spec.cron, jitter, &seed, after)
}

/// Parse `status.<mode>.lastRunAt` (RFC3339) into a `DateTime<Utc>`.
fn last_run_at(maint: &Maintenance, mode: MaintenanceMode) -> Option<DateTime<Utc>> {
    let status = maint.status.as_ref()?;
    let run = match mode {
        MaintenanceMode::Quick => status.quick.as_ref(),
        MaintenanceMode::Full => status.full.as_ref(),
    }?;
    run.last_run_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// How long until the controller should reconcile again. When `handled` is set,
/// that mode's clock is advanced past the just-handled slot (so a *yield*, which
/// does not move `lastRunAt`, still doesn't immediately re-fire the same slot);
/// the other mode is measured from its own `lastRunAt`. The result is floored at
/// the running cadence and capped by the caller.
fn next_wakeup(
    maint: &Maintenance,
    now: DateTime<Utc>,
    handled: Option<(MaintenanceMode, DateTime<Utc>)>,
) -> Duration {
    let mut earliest: Option<DateTime<Utc>> = None;
    for mode in [MaintenanceMode::Quick, MaintenanceMode::Full] {
        let after = match handled {
            Some((hm, hs)) if hm == mode => hs,
            _ => mode_after(maint, mode),
        };
        if let Ok(slot) = slot_for(maint, mode, after) {
            earliest = Some(earliest.map_or(slot, |e| e.min(slot)));
        }
    }
    match earliest {
        Some(slot) if slot > now => (slot - now)
            .to_std()
            .unwrap_or(REQUEUE_CAP)
            .max(REQUEUE_RUNNING),
        // A slot is already due (or schedules failed to parse): re-check soon.
        _ => REQUEUE_RUNNING,
    }
}

/// Cap a requeue so the lease/health/readiness is re-evaluated even when the next
/// slot is far out (G8).
fn cap(d: Duration) -> Duration {
    d.min(REQUEUE_CAP)
}

/// Deterministic, ≤52-char, DNS-1123-safe Job name for a maintenance slot (G1).
/// `<cr>-<q|f>-<unix_slot>`, truncating the CR component and appending a stable
/// hash when the full name would overflow (the Job name is copied into the
/// 63-char `job-name` label and the Job controller suffixes pod names).
fn maintenance_job_name(cr: &str, mode: MaintenanceMode, slot: DateTime<Utc>) -> String {
    const MAX: usize = 52;
    let m = match mode {
        MaintenanceMode::Quick => "q",
        MaintenanceMode::Full => "f",
    };
    let suffix = format!("-{m}-{}", slot.timestamp());
    let budget = MAX.saturating_sub(suffix.len());
    if cr.len() <= budget {
        format!("{cr}{suffix}")
    } else {
        let hash = short_hash(cr); // 8 hex chars
        let keep = budget.saturating_sub(hash.len() + 1); // room for "-<hash>"
        let trunc: String = cr.chars().take(keep).collect();
        format!("{trunc}-{hash}{suffix}")
    }
}

/// A short, stable (run-independent) 8-hex-char FNV-1a hash for name truncation.
fn short_hash(s: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{:08x}", (h & 0xffff_ffff))
}

/// Whether any non-terminal maintenance Job is owned by this `Maintenance` CR
/// (the single-flight gate, G3).
async fn has_active_maintenance_job(job_api: &Api<Job>, cr_name: &str) -> Result<bool> {
    let selector =
        format!("{COMPONENT_LABEL}={MAINTENANCE_COMPONENT},{MAINTENANCE_INSTANCE_LABEL}={cr_name}");
    let jobs = job_api
        .list(&ListParams::default().labels(&selector))
        .await?;
    Ok(jobs.items.iter().any(|j| job_terminal_state(j).is_none()))
}

/// Best-effort delete of a per-slot work-spec ConfigMap once its Job is done.
async fn delete_work_spec_cm(ctx: &Context, namespace: &str, name: &str) {
    let api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
    if let Err(e) = api.delete(name, &DeleteParams::default()).await {
        tracing::debug!(error = %e, configmap = %name, "work-spec ConfigMap cleanup failed (ignored)");
    }
}

/// Patch the single `LeaseOwned` condition only when its status/reason actually
/// changes, so the controller does not hot-loop on its own status writes (G6).
async fn patch_condition_if_changed(
    api: &Api<Maintenance>,
    name: &str,
    maint: &Maintenance,
    status: &str,
    reason: &str,
    message: &str,
) -> Result<()> {
    let unchanged = maint
        .status
        .as_ref()
        .map(|s| &s.conditions)
        .and_then(|cs| cs.iter().find(|c| c.type_ == "LeaseOwned"))
        .is_some_and(|c| c.status == status && c.reason == reason);
    if unchanged {
        return Ok(());
    }
    let observed_gen = maint.metadata.generation.unwrap_or(0);
    io::patch_status(
        api,
        name,
        serde_json::json!({
            "observedGeneration": observed_gen,
            "conditions": [{
                "type": "LeaseOwned",
                "status": status,
                "reason": reason,
                "message": message,
                "lastTransitionTime": Utc::now().to_rfc3339(),
                "observedGeneration": observed_gen,
            }],
        }),
    )
    .await?;
    Ok(())
}

/// `error_policy` for the `Maintenance` controller.
pub fn error_policy(
    _obj: std::sync::Arc<Maintenance>,
    err: &Error,
    ctx: std::sync::Arc<Context>,
) -> Action {
    error_policy_for("Maintenance", err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::common::{CronSpec, RepositoryKind, RepositoryRef};
    use kopiur_api::maintenance::RunStatus;
    use kopiur_api::{MaintenanceSpec, MaintenanceStatus, Ownership, TakeoverPolicy};

    fn maint_with(
        quick_cron: &str,
        full_cron: &str,
        status: Option<MaintenanceStatus>,
    ) -> Maintenance {
        let mut m = Maintenance::new(
            "nas-primary",
            MaintenanceSpec {
                repository: RepositoryRef {
                    kind: RepositoryKind::Repository,
                    name: "nas-primary".into(),
                    namespace: None,
                },
                schedule: kopiur_api::MaintenanceSchedule {
                    quick: CronSpec {
                        cron: quick_cron.into(),
                        jitter: None,
                    },
                    full: CronSpec {
                        cron: full_cron.into(),
                        jitter: None,
                    },
                    timezone: None,
                },
                ownership: Ownership {
                    owner: "kopiur/prod/nas-primary".into(),
                    takeover_policy: TakeoverPolicy::Never,
                },
                mover: None,
                failure_policy: None,
            },
        );
        m.metadata.uid = Some("uid-maint-1".into());
        m.status = status;
        m
    }

    fn run_at(ts: &str) -> RunStatus {
        RunStatus {
            last_run_at: Some(ts.into()),
            ..Default::default()
        }
    }

    #[test]
    fn first_ever_reconcile_is_due_and_prefers_full() {
        // No status → both due; full wins (it subsumes quick).
        let m = maint_with("*/5 * * * *", "0 3 * * *", None);
        let (mode, _slot) = due_mode(&m, Utc::now()).expect("first run is due");
        assert_eq!(mode, MaintenanceMode::Full);
    }

    #[test]
    fn not_due_right_after_a_run() {
        // Both ran one second ago → next slots are in the future → nothing due.
        let now = Utc::now();
        let just = (now - chrono::Duration::seconds(1)).to_rfc3339();
        let status = MaintenanceStatus {
            quick: Some(run_at(&just)),
            full: Some(run_at(&just)),
            ..Default::default()
        };
        let m = maint_with("*/5 * * * *", "0 3 * * *", Some(status));
        assert!(
            due_mode(&m, now).is_none(),
            "a mode that just ran must not be immediately due again"
        );
    }

    #[test]
    fn quick_due_when_full_recent() {
        // Full ran moments ago (not due), quick last ran long ago (due) → quick.
        let now = Utc::now();
        let status = MaintenanceStatus {
            quick: Some(run_at(&(now - chrono::Duration::days(2)).to_rfc3339())),
            full: Some(run_at(&(now - chrono::Duration::seconds(1)).to_rfc3339())),
            ..Default::default()
        };
        let m = maint_with("*/5 * * * *", "0 3 * * *", Some(status));
        let (mode, _) = due_mode(&m, now).expect("quick should be due");
        assert_eq!(mode, MaintenanceMode::Quick);
    }

    #[test]
    fn job_name_is_deterministic_and_within_limit() {
        let slot = DateTime::parse_from_rfc3339("2026-06-06T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let short = maintenance_job_name("nas-primary", MaintenanceMode::Full, slot);
        assert!(short.len() <= 52);
        assert!(short.starts_with("nas-primary-f-"));
        // Deterministic.
        assert_eq!(
            short,
            maintenance_job_name("nas-primary", MaintenanceMode::Full, slot)
        );
        // Quick vs full differ.
        assert_ne!(
            short,
            maintenance_job_name("nas-primary", MaintenanceMode::Quick, slot)
        );
    }

    #[test]
    fn job_name_truncates_and_hashes_long_cr_names() {
        let slot = DateTime::parse_from_rfc3339("2026-06-06T03:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let long = "a-very-long-repository-name-that-blows-the-dns-label-budget-easily";
        let n1 = maintenance_job_name(long, MaintenanceMode::Quick, slot);
        assert!(n1.len() <= 52, "got {} ({} chars)", n1, n1.len());
        // Stable across calls (hash is run-independent).
        assert_eq!(n1, maintenance_job_name(long, MaintenanceMode::Quick, slot));
        // A different long name produces a different truncated+hashed name.
        let other = "b-very-long-repository-name-that-blows-the-dns-label-budget-easily";
        assert_ne!(
            n1,
            maintenance_job_name(other, MaintenanceMode::Quick, slot)
        );
    }

    #[test]
    fn requeue_is_capped() {
        // Full daily, last ran moments ago → next full ~24h out, but the requeue
        // is capped so the controller still wakes within the heartbeat.
        let now = Utc::now();
        let status = MaintenanceStatus {
            quick: Some(run_at(&(now - chrono::Duration::seconds(1)).to_rfc3339())),
            full: Some(run_at(&(now - chrono::Duration::seconds(1)).to_rfc3339())),
            ..Default::default()
        };
        let m = maint_with("0 */6 * * *", "0 3 * * *", Some(status));
        assert!(cap(next_wakeup(&m, now, None)) <= REQUEUE_CAP);
    }
}
