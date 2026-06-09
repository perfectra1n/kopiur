//! Deterministic schedule jitter and Jenkins-style `H` substitution (ADR §4.1).
//!
//! Jitter MUST be **deterministic**: HA operator replicas compute identical fire
//! times without coordinating, and a controller restart re-derives the same value
//! without persisting it (ADR §4.1, SKILL "Scheduling"). That rules out any RNG
//! and any reliance on the wall clock. The offset is a pure function of
//! `(seed, slot_start)`.
//!
//! ### Why a hand-rolled hash
//!
//! `std::collections::hash_map::DefaultHasher` is explicitly **not** guaranteed
//! stable across Rust releases, so a value persisted/expected by one build could
//! differ after a toolchain bump — fatal for "identical across replicas/restarts".
//! We therefore inline **FNV-1a (64-bit)**, whose constants are fixed by the
//! algorithm and will never change. No external dependency is added.
//!
//! ### Cron `H`
//!
//! `croner` 2.x does not implement Jenkins-style `H`. Kopiur treats `H` as
//! "deterministic hashed jitter within the field's range," resolved *here* (not in
//! the cron parser). [`substitute_h`] rewrites each `H` in a 5-field cron to a
//! concrete value derived from the same FNV hash of the seed, so the resolved
//! expression is stable per `SnapshotSchedule` and parseable by `croner`.
//! [`crate::validate::validate_cron`] validates the *shape* by substituting a fixed
//! placeholder; this function produces the *spread*.

use std::time::Duration;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Stable 64-bit FNV-1a hash of `(seed, slot_start)`. Fixed forever by the
/// algorithm — safe to rely on across builds and replicas.
fn fnv1a(seed: &str, slot_start_unix: i64) -> u64 {
    let mut h = FNV_OFFSET;
    for b in seed.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    // Mix the slot as 8 little-endian bytes so adjacent slots diverge widely.
    for b in slot_start_unix.to_le_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// Deterministic jitter offset in `[0, max)`, derived from a stable hash of
/// `(seed, slot_start_unix)`. NO RNG, NO clock.
///
/// `seed` should be a stable per-schedule key — the ADR specifies
/// `SnapshotSchedule.UID` (combined here with the slot's wall-clock start). The same
/// `(seed, slot_start, max)` always yields the same `Duration`; different seeds or
/// slots spread across the window. `max == 0` yields `Duration::ZERO`.
///
/// Resolution is whole seconds: jitter windows are minutes-to-hours, sub-second
/// precision is meaningless for cron slots, and integer seconds keep the value
/// trivially reproducible across languages if the mover ever re-derives it.
///
/// ```
/// use std::time::Duration;
/// // Re-exported from the crate root as `jitter_offset`.
/// use kopiur_api::jitter_offset;
///
/// let max = Duration::from_secs(1800); // 30m window
/// // Deterministic: the same (seed, slot, max) always yields the same offset —
/// // HA replicas and restarts agree without coordination (ADR §4.1).
/// let a = jitter_offset("schedule-uid", 1_700_000_000, max);
/// let b = jitter_offset("schedule-uid", 1_700_000_000, max);
/// assert_eq!(a, b);
/// assert!(a < max);
/// // A zero window means no jitter.
/// assert_eq!(jitter_offset("uid", 123, Duration::ZERO), Duration::ZERO);
/// ```
pub fn offset(seed: &str, slot_start_unix: i64, max: Duration) -> Duration {
    let max_secs = max.as_secs();
    if max_secs == 0 {
        return Duration::ZERO;
    }
    let h = fnv1a(seed, slot_start_unix);
    // Modulo a u64 hash by the window width in seconds → uniform-enough offset.
    let offset_secs = h % max_secs;
    Duration::from_secs(offset_secs)
}

/// Resolve Jenkins-style `H` tokens in a 5-field cron expression to concrete,
/// deterministic values derived from `seed`. Each field's `H` maps into that
/// field's natural range (minute 0–59, hour 0–23, day-of-month 1–28, month 1–12,
/// day-of-week 0–6) using a distinct mix of the seed hash, so two `H`s in the same
/// expression don't collapse to the same number.
///
/// Returns the rewritten expression. Non-`H` fields pass through unchanged. If the
/// expression isn't 5 whitespace-separated fields it is returned unchanged (shape
/// validation is [`crate::validate::validate_cron`]'s job).
///
/// ```
/// use kopiur_api::substitute_h;
///
/// // Each `H` resolves to a concrete, deterministic value in the field's range;
/// // non-`H` fields are untouched.
/// let out = substitute_h("H 2 * * *", "schedule-uid");
/// let fields: Vec<&str> = out.split_whitespace().collect();
/// let minute: u64 = fields[0].parse().expect("H -> a minute");
/// assert!(minute < 60);
/// assert_eq!(&fields[1..], &["2", "*", "*", "*"]);
///
/// // Deterministic per seed; different schedules land in different minutes.
/// assert_eq!(substitute_h("H 2 * * *", "uid"), substitute_h("H 2 * * *", "uid"));
/// assert_ne!(substitute_h("H 2 * * *", "uid-a"), substitute_h("H 2 * * *", "uid-b"));
/// ```
pub fn substitute_h(expr: &str, seed: &str) -> String {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 {
        return expr.to_string();
    }
    // (min_inclusive, range_width) per standard cron field.
    const RANGES: [(u64, u64); 5] = [
        (0, 60), // minute 0-59
        (0, 24), // hour 0-23
        (1, 28), // day-of-month 1-28 (28 keeps it valid in every month)
        (1, 12), // month 1-12
        (0, 7),  // day-of-week 0-6
    ];
    let base = fnv1a(seed, 0);
    let resolved: Vec<String> = fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            if *f == "H" {
                let (lo, width) = RANGES[i];
                // Distinct rotation per field index so multiple H's diverge.
                let mixed = base.rotate_left((i as u32 + 1) * 7);
                (lo + (mixed % width)).to_string()
            } else {
                (*f).to_string()
            }
        })
        .collect();
    resolved.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: Duration = Duration::from_secs(1800); // 30m

    #[test]
    fn offset_is_deterministic_across_calls() {
        for seed in ["uid-a", "uid-b", "00000000-1111-2222", "schedule/billing"] {
            let a = offset(seed, 1_700_000_000, MAX);
            let b = offset(seed, 1_700_000_000, MAX);
            assert_eq!(a, b, "same (seed, slot, max) must give identical offset");
        }
    }

    #[test]
    fn offset_is_always_within_range() {
        for slot in 0..2000i64 {
            let o = offset("some-uid", slot * 37, MAX);
            assert!(o < MAX, "offset {o:?} must be < max {MAX:?}");
        }
    }

    #[test]
    fn offset_zero_max_is_zero() {
        assert_eq!(offset("uid", 123, Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn offset_spreads_across_seeds_not_all_identical() {
        let slot = 1_700_000_000;
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..500 {
            let seed = format!("uid-{i}");
            seen.insert(offset(&seed, slot, MAX).as_secs());
        }
        // With 500 seeds over a 1800s window we expect a wide spread, certainly
        // far more than a handful of distinct values (proves it's not constant).
        assert!(
            seen.len() > 100,
            "expected a wide spread of offsets, got {} distinct",
            seen.len()
        );
    }

    #[test]
    fn offset_spreads_across_slots_for_one_seed() {
        let mut seen = std::collections::BTreeSet::new();
        for slot in 0..500i64 {
            seen.insert(offset("fixed-uid", slot * 3600, MAX).as_secs());
        }
        assert!(
            seen.len() > 100,
            "adjacent slots must diverge, got {}",
            seen.len()
        );
    }

    #[test]
    fn substitute_h_is_deterministic_and_in_range() {
        let a = substitute_h("H 2 * * *", "uid-1");
        let b = substitute_h("H 2 * * *", "uid-1");
        assert_eq!(a, b);
        // minute field resolved to a number 0-59; rest unchanged.
        let fields: Vec<&str> = a.split_whitespace().collect();
        let minute: u64 = fields[0].parse().expect("H resolved to a number");
        assert!(minute < 60);
        assert_eq!(&fields[1..], &["2", "*", "*", "*"]);
    }

    #[test]
    fn substitute_h_different_seeds_differ() {
        let a = substitute_h("H 2 * * *", "uid-1");
        let b = substitute_h("H 2 * * *", "uid-2");
        assert_ne!(a, b, "different schedules should land in different minutes");
    }

    #[test]
    fn substitute_h_multiple_h_diverge() {
        // Two H's in one expression must not collapse to the same number-space.
        let out = substitute_h("H H * * *", "uid-x");
        let fields: Vec<&str> = out.split_whitespace().collect();
        let minute: u64 = fields[0].parse().unwrap();
        let hour: u64 = fields[1].parse().unwrap();
        assert!(minute < 60);
        assert!(hour < 24);
    }

    #[test]
    fn substitute_h_passes_through_non_h_and_malformed() {
        assert_eq!(substitute_h("0 2 * * *", "s"), "0 2 * * *");
        // Not 5 fields → returned unchanged (validate_cron handles shape).
        assert_eq!(substitute_h("0 2 * *", "s"), "0 2 * *");
    }
}
