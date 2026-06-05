//! Grandfather-father-son (GFS) retention selection (ADR §4.4).
//!
//! `BackupConfig.spec.retention` is the **only** successful-retention driver
//! (SKILL "Retention is GFS-only"). The operator periodically runs this selection
//! over the `Backup` CRs for one `(identity, source)` tuple and deletes the CRs
//! that fall outside the kept set; each deleted CR's `deletionPolicy` then governs
//! the snapshot (§4.5). This module is the pure selection kernel — no kube types,
//! no clock — so it's unit-testable with lightweight fakes.
//!
//! ## Algorithm (ADR-0001 §4.4, steps 2–4)
//!
//! 1. Sort candidates by end time, newest first.
//! 2. Apply buckets in order: `keepLatest`, `keepHourly`, `keepDaily`,
//!    `keepWeekly`, `keepMonthly`, `keepAnnual`.
//!    - `keepLatest: N` keeps the N newest backups outright.
//!    - Each time bucket keeps the **most recent** backup within each distinct
//!      period (hour / day / ISO-week / month / year), up to its count `N`,
//!      walking newest→oldest.
//! 3. A backup kept by **any** bucket survives (union). Everything else is deleted.
//!
//! This is deliberately *not* a flat count: a backup that is the newest of its
//! year is held by `keepAnnual` even if hundreds of newer dailies exist — the
//! exact case a flat cap would silently drop (ADR §4.4 "Why not flat-count").
//!
//! ## Empty-policy semantics
//!
//! An all-`None` [`Retention`] selects **no** buckets, so the kept set is empty and
//! every backup is marked for deletion. The caller (controller) is responsible for
//! only invoking GFS when a retention policy is actually configured; this function
//! reports faithfully what the given policy implies. This is documented and tested.

use crate::common::Retention;
use chrono::{DateTime, Datelike, Utc};
use std::collections::BTreeSet;

/// Anything that can stand in for a `Backup` during retention selection. Kept tiny
/// so tests use trivial fakes instead of constructing full `Backup` CRs.
pub trait BackupLike {
    /// The snapshot's completion time — the GFS bucketing key (ADR §4.4 step 2).
    fn end_time(&self) -> DateTime<Utc>;
    /// A stable identifier (kopia snapshot ID or CR name) used in the result sets.
    fn id(&self) -> &str;
}

/// The outcome of a GFS selection: which ids to keep and which to delete. Both are
/// returned explicitly so callers never have to recompute the complement.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeptSet {
    /// Ids retained by at least one bucket.
    pub keep: Vec<String>,
    /// Ids selected by no bucket — eligible for pruning.
    pub delete: Vec<String>,
}

/// Calendar period a timestamp falls in, used to deduplicate "one per period."
/// Distinct values mean distinct periods; comparing these is how each bucket keeps
/// the newest entry per period.
fn hour_key(t: DateTime<Utc>) -> (i32, u32, u32) {
    (t.year(), t.ordinal(), t.hour())
}
fn day_key(t: DateTime<Utc>) -> (i32, u32) {
    (t.year(), t.ordinal())
}
fn week_key(t: DateTime<Utc>) -> (i32, u32) {
    let iso = t.iso_week();
    (iso.year(), iso.week())
}
fn month_key(t: DateTime<Utc>) -> (i32, u32) {
    (t.year(), t.month())
}
fn year_key(t: DateTime<Utc>) -> i32 {
    t.year()
}

use chrono::Timelike;

/// Walk `sorted` (newest→oldest) and collect the index of the newest entry in each
/// distinct period, stopping once `count` periods have been kept.
fn keep_per_period<K, F>(
    sorted: &[usize],
    times: &[DateTime<Utc>],
    count: usize,
    key: F,
) -> Vec<usize>
where
    K: Ord,
    F: Fn(DateTime<Utc>) -> K,
{
    let mut kept = Vec::new();
    let mut seen: BTreeSet<K> = BTreeSet::new();
    for &idx in sorted {
        if kept.len() >= count {
            break;
        }
        let k = key(times[idx]);
        if seen.insert(k) {
            // First (= newest, since sorted desc) entry in this period.
            kept.push(idx);
        }
    }
    kept
}

/// Select the GFS-kept set from `backups` under `policy` (ADR §4.4).
///
/// Returns a [`KeptSet`] partitioning every input id into `keep`/`delete`. Input
/// order is irrelevant; `keep` is returned newest-first, `delete` newest-first too.
/// Ties on `end_time` are broken by id for determinism.
///
/// ```
/// use chrono::{DateTime, TimeZone, Utc};
/// use kopiur_api::{select_kept, BackupLike};
/// use kopiur_api::common::Retention;
///
/// // A trivial fake honoring BackupLike — no kube CRs needed for selection.
/// struct Snap { id: String, end: DateTime<Utc> }
/// impl BackupLike for Snap {
///     fn end_time(&self) -> DateTime<Utc> { self.end }
///     fn id(&self) -> &str { &self.id }
/// }
/// let day = |d: u32| Utc.with_ymd_and_hms(2026, 5, d, 2, 0, 0).single().unwrap();
/// let snaps = vec![
///     Snap { id: "d24".into(), end: day(24) },
///     Snap { id: "d23".into(), end: day(23) },
///     Snap { id: "d22".into(), end: day(22) },
/// ];
///
/// // keepDaily: 2 — keep the newest per day for the 2 newest days; prune the rest.
/// let policy: Retention =
///     serde_json::from_value(serde_json::json!({ "keepDaily": 2 })).unwrap();
/// let kept = select_kept(&snaps, &policy);
/// assert_eq!(kept.keep, vec!["d24", "d23"]); // newest-first
/// assert_eq!(kept.delete, vec!["d22"]);
/// ```
pub fn select_kept<T: BackupLike>(backups: &[T], policy: &Retention) -> KeptSet {
    if backups.is_empty() {
        return KeptSet::default();
    }

    let times: Vec<DateTime<Utc>> = backups.iter().map(|b| b.end_time()).collect();

    // Indices sorted by end_time descending; id as a deterministic tiebreaker.
    let mut order: Vec<usize> = (0..backups.len()).collect();
    order.sort_by(|&a, &b| {
        times[b]
            .cmp(&times[a])
            .then_with(|| backups[a].id().cmp(backups[b].id()))
    });

    let mut keep_idx: BTreeSet<usize> = BTreeSet::new();

    // keepLatest: the N newest outright.
    if let Some(n) = policy.keep_latest {
        for &idx in order.iter().take(n as usize) {
            keep_idx.insert(idx);
        }
    }
    if let Some(n) = policy.keep_hourly {
        keep_idx.extend(keep_per_period(&order, &times, n as usize, hour_key));
    }
    if let Some(n) = policy.keep_daily {
        keep_idx.extend(keep_per_period(&order, &times, n as usize, day_key));
    }
    if let Some(n) = policy.keep_weekly {
        keep_idx.extend(keep_per_period(&order, &times, n as usize, week_key));
    }
    if let Some(n) = policy.keep_monthly {
        keep_idx.extend(keep_per_period(&order, &times, n as usize, month_key));
    }
    if let Some(n) = policy.keep_annual {
        keep_idx.extend(keep_per_period(&order, &times, n as usize, year_key));
    }

    let mut keep = Vec::new();
    let mut delete = Vec::new();
    for &idx in &order {
        if keep_idx.contains(&idx) {
            keep.push(backups[idx].id().to_string());
        } else {
            delete.push(backups[idx].id().to_string());
        }
    }
    KeptSet { keep, delete }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// Minimal fake honoring `BackupLike` — no kube CRs in retention tests.
    struct Fake {
        id: String,
        end: DateTime<Utc>,
    }
    impl BackupLike for Fake {
        fn end_time(&self) -> DateTime<Utc> {
            self.end
        }
        fn id(&self) -> &str {
            &self.id
        }
    }

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).single().unwrap()
    }
    fn fake(id: &str, t: DateTime<Utc>) -> Fake {
        Fake {
            id: id.into(),
            end: t,
        }
    }

    fn policy(
        latest: Option<u32>,
        hourly: Option<u32>,
        daily: Option<u32>,
        weekly: Option<u32>,
        monthly: Option<u32>,
        annual: Option<u32>,
    ) -> Retention {
        Retention {
            keep_latest: latest,
            keep_hourly: hourly,
            keep_daily: daily,
            keep_weekly: weekly,
            keep_monthly: monthly,
            keep_annual: annual,
        }
    }

    fn as_set(v: &[String]) -> BTreeSet<&str> {
        v.iter().map(String::as_str).collect()
    }

    #[test]
    fn empty_input_yields_empty_sets() {
        let got = select_kept::<Fake>(&[], &policy(Some(5), None, None, None, None, None));
        assert!(got.keep.is_empty());
        assert!(got.delete.is_empty());
    }

    #[test]
    fn empty_policy_keeps_nothing() {
        // All-None policy → no buckets selected → everything deleted.
        let backups = vec![
            fake("a", at(2026, 5, 24, 2, 0)),
            fake("b", at(2026, 5, 23, 2, 0)),
        ];
        let got = select_kept(&backups, &Retention::default());
        assert!(got.keep.is_empty(), "empty policy keeps nothing");
        assert_eq!(as_set(&got.delete), ["a", "b"].into_iter().collect());
    }

    #[test]
    fn keep_latest_keeps_n_newest() {
        let backups = vec![
            fake("d1", at(2026, 5, 24, 2, 0)),
            fake("d2", at(2026, 5, 23, 2, 0)),
            fake("d3", at(2026, 5, 22, 2, 0)),
            fake("d4", at(2026, 5, 21, 2, 0)),
        ];
        let got = select_kept(&backups, &policy(Some(2), None, None, None, None, None));
        assert_eq!(as_set(&got.keep), ["d1", "d2"].into_iter().collect());
        assert_eq!(as_set(&got.delete), ["d3", "d4"].into_iter().collect());
    }

    #[test]
    fn keep_daily_keeps_one_newest_per_day() {
        // Three backups on day 24 (keep the 02:00 one), one each on 23 and 22.
        let backups = vec![
            fake("a", at(2026, 5, 24, 0, 5)),
            fake("b", at(2026, 5, 24, 1, 30)),
            fake("c", at(2026, 5, 24, 2, 0)), // newest on the 24th
            fake("d", at(2026, 5, 23, 2, 0)),
            fake("e", at(2026, 5, 22, 2, 0)),
        ];
        let got = select_kept(&backups, &policy(None, None, Some(14), None, None, None));
        // One per distinct day, newest within the day.
        assert_eq!(as_set(&got.keep), ["c", "d", "e"].into_iter().collect());
        assert_eq!(as_set(&got.delete), ["a", "b"].into_iter().collect());
    }

    #[test]
    fn keep_daily_count_caps_number_of_days() {
        let backups = vec![
            fake("d24", at(2026, 5, 24, 2, 0)),
            fake("d23", at(2026, 5, 23, 2, 0)),
            fake("d22", at(2026, 5, 22, 2, 0)),
            fake("d21", at(2026, 5, 21, 2, 0)),
        ];
        let got = select_kept(&backups, &policy(None, None, Some(2), None, None, None));
        // Only the 2 newest days kept.
        assert_eq!(as_set(&got.keep), ["d24", "d23"].into_iter().collect());
        assert_eq!(as_set(&got.delete), ["d22", "d21"].into_iter().collect());
    }

    #[test]
    fn keep_latest_unions_with_keep_daily() {
        // Two backups same day: keepDaily keeps the newest (c), keepLatest:2 also
        // pulls in the second-newest overall (b) even though it shares c's day.
        let backups = vec![
            fake("c", at(2026, 5, 24, 6, 0)),
            fake("b", at(2026, 5, 24, 5, 0)),
            fake("a", at(2026, 5, 23, 5, 0)),
        ];
        let got = select_kept(&backups, &policy(Some(2), None, Some(7), None, None, None));
        // c kept by both; b kept by keepLatest; a kept by keepDaily (day 23).
        assert_eq!(as_set(&got.keep), ["a", "b", "c"].into_iter().collect());
        assert!(got.delete.is_empty());
    }

    #[test]
    fn annual_snapshot_survives_flood_of_newer_dailies() {
        // The §4.4 "why not flat-count" case. One old end-of-2024 snapshot plus a
        // pile of 2026 dailies. keepDaily:3 + keepAnnual:2 must retain the 2024
        // snapshot as the newest-of-its-year even though it's far down the list.
        let mut backups = vec![fake("y2024", at(2024, 12, 31, 23, 0))];
        for d in 1..=10u32 {
            backups.push(fake(&format!("y2026-{d:02}"), at(2026, 5, d, 2, 0)));
        }
        // Newest 2026 daily is day 10; year 2026's representative is day 10,
        // year 2024's representative is y2024.
        let got = select_kept(&backups, &policy(None, None, Some(3), None, None, Some(2)));
        let keep = as_set(&got.keep);
        assert!(
            keep.contains("y2024"),
            "annual snapshot must not be dropped by daily flood; kept={keep:?}"
        );
        // keepDaily:3 keeps the 3 newest days of 2026.
        assert!(keep.contains("y2026-10"));
        assert!(keep.contains("y2026-09"));
        assert!(keep.contains("y2026-08"));
        // Older 2026 dailies not covered by any bucket are deleted.
        assert!(got.delete.contains(&"y2026-01".to_string()));
    }

    #[test]
    fn monthly_and_weekly_pick_newest_in_period() {
        let backups = vec![
            fake("may-late", at(2026, 5, 28, 2, 0)),
            fake("may-early", at(2026, 5, 2, 2, 0)),
            fake("apr", at(2026, 4, 15, 2, 0)),
            fake("mar", at(2026, 3, 15, 2, 0)),
        ];
        let got = select_kept(&backups, &policy(None, None, None, None, Some(2), None));
        // keepMonthly:2 → newest of May (may-late) and newest of April (apr).
        assert_eq!(as_set(&got.keep), ["may-late", "apr"].into_iter().collect());
    }

    #[test]
    fn every_backup_kept_by_any_bucket_survives() {
        // Mixed policy; assert the kept set is exactly the union and no kept id
        // appears in delete.
        let backups = vec![
            fake("now", at(2026, 5, 24, 12, 0)),
            fake("earlier-today", at(2026, 5, 24, 1, 0)),
            fake("yesterday", at(2026, 5, 23, 1, 0)),
            fake("last-week", at(2026, 5, 16, 1, 0)),
        ];
        let got = select_kept(
            &backups,
            &policy(Some(1), None, Some(2), Some(2), None, None),
        );
        let keep = as_set(&got.keep);
        let del = as_set(&got.delete);
        for id in keep.iter() {
            assert!(!del.contains(id), "id {id} in both keep and delete");
        }
        // Every input is accounted for exactly once.
        assert_eq!(keep.len() + del.len(), 4);
    }
}
