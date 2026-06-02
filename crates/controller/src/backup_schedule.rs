//! The `BackupSchedule` reconciler — *when* a backup runs (ADR §4.1).
//!
//! ## Timing: requeue-based, not a `tokio::interval` task (decision)
//!
//! We compute the next wall-clock slot during each reconcile and return
//! `Action::requeue(time_until_slot)`. When that requeue fires, we check whether
//! the slot is due and, if so, create a `Backup` CR; then we recompute and
//! requeue again. This is **HA-safe and restart-safe**: there is no per-schedule
//! background task to leak, leader-election is handled by the controller runtime
//! (only the active replica reconciles), and a restart simply recomputes the
//! same wall-clock slot. A `tokio::interval` task per schedule would duplicate
//! across replicas and strand on restart. (ADR §4.1 anchors on `cron(now)`.)
//!
//! The scheduling kernel here is **pure**: [`next_fire`] computes the jittered
//! next slot deterministically (reusing `api::jitter`), and [`should_fire_now`]
//! / [`concurrency_allows`] are clock-free decisions, so they are unit-tested
//! without a cluster.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use kube::runtime::controller::Action;
use kube::ResourceExt;

use kopiur_api::{jitter, validate, BackupSchedule, ConcurrencyPolicy, ScheduleSpec};

use crate::context::Context;
use crate::error::{error_policy_for, Error, Result};

/// Parse Go-style duration strings used in the CRD (`30m`, `1h`, `90s`). Returns
/// `None` for unparseable input (caller treats as "no jitter window").
pub fn parse_go_duration(s: &str) -> Option<StdDuration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Support a single unit suffix (s/m/h) or a bare number of seconds.
    let (num, mult) = if let Some(stripped) = s.strip_suffix('h') {
        (stripped, 3600u64)
    } else if let Some(stripped) = s.strip_suffix('m') {
        (stripped, 60)
    } else if let Some(stripped) = s.strip_suffix('s') {
        (stripped, 1)
    } else {
        (s, 1)
    };
    num.trim()
        .parse::<u64>()
        .ok()
        .map(|n| StdDuration::from_secs(n * mult))
}

/// Compute the next fire time at or after `after`, applying deterministic
/// jitter (ADR §4.1). `H` tokens are resolved first via `jitter::substitute_h`,
/// then the cron's next slot is found, then a per-`(seed, slot)` offset within
/// the `jitter` window is added.
///
/// `seed` should be the schedule's UID (stable across replicas/restarts).
/// Returns an [`Error::InvalidSchedule`] if the (post-substitution) cron fails
/// to parse — defensive, since the webhook validates shape at admission.
pub fn next_fire(
    cron_expr: &str,
    jitter_window: Option<StdDuration>,
    seed: &str,
    after: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    let resolved = jitter::substitute_h(cron_expr, seed);
    let cron = croner::Cron::new(&resolved)
        .parse()
        .map_err(|e| Error::InvalidSchedule(format!("{resolved}: {e}")))?;
    let slot = cron
        .find_next_occurrence(&after, false)
        .map_err(|e| Error::InvalidSchedule(format!("no next occurrence for {resolved}: {e}")))?;
    let offset = match jitter_window {
        Some(w) => jitter::offset(seed, slot.timestamp(), w),
        None => StdDuration::ZERO,
    };
    Ok(slot + chrono::Duration::from_std(offset).unwrap_or_else(|_| chrono::Duration::zero()))
}

/// Whether a slot is due to fire at `now` (i.e. the scheduled time has arrived).
pub fn should_fire_now(slot: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now >= slot
}

/// Whether the `starting_deadline_seconds` has been missed for a slot (the slot
/// is too old to still run). `None` deadline means "never expires."
pub fn missed_deadline(
    slot: DateTime<Utc>,
    now: DateTime<Utc>,
    starting_deadline_seconds: Option<i64>,
) -> bool {
    match starting_deadline_seconds {
        Some(d) => (now - slot).num_seconds() > d,
        None => false,
    }
}

/// Whether a new run may start given the concurrency policy and whether a run
/// is currently active. `Forbid` skips when active; `Allow`/`Replace` proceed
/// (`Replace`'s cancel-the-old behavior is the caller's IO responsibility).
pub fn concurrency_allows(policy: ConcurrencyPolicy, run_active: bool) -> bool {
    match policy {
        ConcurrencyPolicy::Forbid => !run_active,
        ConcurrencyPolicy::Allow | ConcurrencyPolicy::Replace => true,
    }
}

/// Whether the schedule should produce any `Backup` at all right now, combining
/// `suspend`, the slot being due, the deadline, and concurrency. Pure decision.
pub fn should_create_backup(
    schedule: &ScheduleSpec,
    slot: DateTime<Utc>,
    now: DateTime<Utc>,
    run_active: bool,
) -> bool {
    if schedule.suspend {
        return false;
    }
    if !should_fire_now(slot, now) {
        return false;
    }
    if missed_deadline(slot, now, schedule.starting_deadline_seconds) {
        return false;
    }
    concurrency_allows(schedule.concurrency_policy, run_active)
}

/// Reconcile a `BackupSchedule`.
#[tracing::instrument(skip(schedule, ctx), fields(kind = "BackupSchedule", name = %schedule.name_any()))]
pub async fn reconcile(schedule: Arc<BackupSchedule>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&schedule, &ctx).await;
    ctx.metrics
        .record_reconcile("BackupSchedule", start.elapsed().as_secs_f64());
    result
}

async fn reconcile_inner(schedule: &BackupSchedule, _ctx: &Context) -> Result<Action> {
    // Defensive re-validation (one validator, two callers — SKILL hard-rule 4).
    let errs = validate::validate_backup_schedule(&schedule.spec);
    if let Some(first) = errs.into_iter().next() {
        return Err(Error::Validation(first.to_string()));
    }

    let seed = schedule.uid().unwrap_or_else(|| schedule.name_any());
    let now = Utc::now();
    let jitter_window = schedule
        .spec
        .schedule
        .jitter
        .as_deref()
        .and_then(parse_go_duration);
    let slot = next_fire(&schedule.spec.schedule.cron, jitter_window, &seed, now)?;

    // TODO(M6): if a *previous* slot is due (status.nextSchedule <= now) and
    // should_create_backup() is true, create a Backup CR (owner-ref to the
    // schedule, kopia.io/origin=scheduled) and update status.lastSchedule /
    // lastSuccessfulSchedule / consecutiveFailures. The Backup-owned watch then
    // recomputes. The pure decisions above (should_create_backup) are tested.

    let until = (slot - now).to_std().unwrap_or(StdDuration::from_secs(60));
    Ok(Action::requeue(until.max(StdDuration::from_secs(1))))
}

/// `error_policy` for the `BackupSchedule` controller.
pub fn error_policy(_obj: Arc<BackupSchedule>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("BackupSchedule", err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).single().unwrap()
    }

    fn schedule_spec(
        cron: &str,
        suspend: bool,
        policy: ConcurrencyPolicy,
        deadline: Option<i64>,
    ) -> ScheduleSpec {
        ScheduleSpec {
            cron: cron.into(),
            jitter: None,
            timezone: None,
            run_on_create: false,
            suspend,
            concurrency_policy: policy,
            starting_deadline_seconds: deadline,
        }
    }

    #[test]
    fn parse_go_duration_handles_units() {
        assert_eq!(parse_go_duration("30m"), Some(StdDuration::from_secs(1800)));
        assert_eq!(parse_go_duration("1h"), Some(StdDuration::from_secs(3600)));
        assert_eq!(parse_go_duration("45s"), Some(StdDuration::from_secs(45)));
        assert_eq!(parse_go_duration("120"), Some(StdDuration::from_secs(120)));
        assert_eq!(parse_go_duration(""), None);
        assert_eq!(parse_go_duration("bogus"), None);
    }

    #[test]
    fn next_fire_is_deterministic_for_same_seed_and_after() {
        // 02:00 daily, no jitter. From 2026-05-24T03:00 the next slot is the
        // following day's 02:00.
        let after = at(2026, 5, 24, 3, 0);
        let a = next_fire("0 2 * * *", None, "uid-1", after).unwrap();
        let b = next_fire("0 2 * * *", None, "uid-1", after).unwrap();
        assert_eq!(a, b);
        assert_eq!(a, at(2026, 5, 25, 2, 0));
    }

    #[test]
    fn next_fire_applies_deterministic_jitter_within_window() {
        let after = at(2026, 5, 24, 3, 0);
        let window = StdDuration::from_secs(1800); // 30m
        let fired = next_fire("0 2 * * *", Some(window), "uid-1", after).unwrap();
        let base = at(2026, 5, 25, 2, 0);
        let delta = (fired - base).num_seconds();
        assert!(
            (0..1800).contains(&delta),
            "jittered fire {fired} must be within [base, base+30m); delta={delta}"
        );
        // Deterministic: same inputs reproduce the exact same fire time.
        let again = next_fire("0 2 * * *", Some(window), "uid-1", after).unwrap();
        assert_eq!(fired, again);
    }

    #[test]
    fn next_fire_resolves_jenkins_h() {
        // `H 2 * * *` must parse (H resolved deterministically) and land at
        // some minute past 02:00.
        let after = at(2026, 5, 24, 3, 0);
        let fired = next_fire("H 2 * * *", None, "uid-x", after).unwrap();
        assert_eq!(fired.format("%H").to_string(), "02");
    }

    #[test]
    fn next_fire_rejects_bad_cron() {
        let after = at(2026, 5, 24, 3, 0);
        let err = next_fire("totally bad", None, "uid", after).unwrap_err();
        assert!(matches!(err, Error::InvalidSchedule(_)));
    }

    #[test]
    fn suspend_blocks_creation() {
        let spec = schedule_spec("0 2 * * *", true, ConcurrencyPolicy::Allow, None);
        let slot = at(2026, 5, 24, 2, 0);
        let now = at(2026, 5, 24, 2, 1);
        assert!(!should_create_backup(&spec, slot, now, false));
    }

    #[test]
    fn forbid_skips_when_a_run_is_active() {
        let spec = schedule_spec("0 2 * * *", false, ConcurrencyPolicy::Forbid, None);
        let slot = at(2026, 5, 24, 2, 0);
        let now = at(2026, 5, 24, 2, 1);
        // Active run + Forbid → skip.
        assert!(!should_create_backup(&spec, slot, now, true));
        // No active run → proceed.
        assert!(should_create_backup(&spec, slot, now, false));
    }

    #[test]
    fn allow_and_replace_proceed_even_when_active() {
        for p in [ConcurrencyPolicy::Allow, ConcurrencyPolicy::Replace] {
            assert!(concurrency_allows(p, true));
        }
        assert!(!concurrency_allows(ConcurrencyPolicy::Forbid, true));
    }

    #[test]
    fn slot_not_due_yet_does_not_fire() {
        let spec = schedule_spec("0 2 * * *", false, ConcurrencyPolicy::Allow, None);
        let slot = at(2026, 5, 24, 2, 0);
        let now = at(2026, 5, 24, 1, 30); // before the slot
        assert!(!should_create_backup(&spec, slot, now, false));
    }

    #[test]
    fn missed_starting_deadline_skips() {
        let spec = schedule_spec("0 2 * * *", false, ConcurrencyPolicy::Allow, Some(600));
        let slot = at(2026, 5, 24, 2, 0);
        // 20 minutes late, deadline is 10 minutes → missed.
        let now = at(2026, 5, 24, 2, 20);
        assert!(missed_deadline(slot, now, Some(600)));
        assert!(!should_create_backup(&spec, slot, now, false));
        // Within deadline → fires.
        let now_ok = at(2026, 5, 24, 2, 5);
        assert!(should_create_backup(&spec, slot, now_ok, false));
    }
}
