//! e2e: GFS **time-bucketed** retention (ADR §4.4) — daily/weekly/monthly/annual
//! pruning against a live operator. Every other e2e uses `keepLatest` only, so a
//! regression in the calendar bucketing (day/ISO-week/month/year keys, the
//! newest-per-bucket pick, or the union semantics) — a silent storage leak or,
//! worse, over-deletion — had no end-to-end guard.
//!
//! Approach: seed REAL Succeeded snapshots, then **backdate**
//! `status.timing.endTime` via the status subresource — the exact field the
//! operator's `retention_view` reads — to span hours/days/weeks/months/years.
//! Expectations are computed in-test with the SAME pure kernel the operator
//! uses (`kopiur_api::select_kept`) over the same `(name, endTime)` views, plus
//! hardcoded sanity asserts so the test cannot pass degenerately. The prune
//! pipeline is fully real: the policy reconciler deletes the pruned `Snapshot`
//! CRs, each finalizer runs a kopia snapshot-delete mover Job, and the
//! kopia-side count is proven via a ReadOnly verifier repository.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; skip gracefully off-cluster.

#![cfg(all(unix, feature = "e2e"))]

mod common;

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use common::{
    cr, ensure_repo, observed_snapshot_count, repository_json, snapshot_json, snapshot_policy_json,
    status_json, wait_phase,
};
use kube::api::{Patch, PatchParams, PostParams};
use kube::{Api, Client};

use kopiur_api::common::Retention;
use kopiur_api::{Repository, Snapshot, SnapshotLike, SnapshotPolicy, select_kept};
use kopiur_e2e::{E2E_NAMESPACE, Need, World, default_timeout, poll_interval, wait_until};

const SUBPATH: &str = "gfs";
const REPO: &str = "e2e-gfs-repo";
const POLICY: &str = "e2e-gfs-policy";

/// The backdated history: name → fixed absolute `endTime`. Fixed PAST instants
/// (not now()-relative) keep the calendar bucketing fully deterministic —
/// `select_kept` buckets relative to the snapshots themselves, not the wall clock.
const HISTORY: &[(&str, &str)] = &[
    ("e2e-gfs-1", "2025-04-10T10:00:00Z"), // last year → held by keepAnnual: 2 (bucket #2)
    ("e2e-gfs-2", "2026-03-02T10:00:00Z"), // ~3 months back → monthly
    ("e2e-gfs-3", "2026-04-06T10:00:00Z"), // ~2 months back → monthly
    ("e2e-gfs-4", "2026-05-25T10:00:00Z"), // ~2 weeks back → weekly
    ("e2e-gfs-5", "2026-06-01T10:00:00Z"), // ~1 week back → weekly/daily
    ("e2e-gfs-6", "2026-06-08T10:00:00Z"), // same-day OLDER duplicate → must prune
    ("e2e-gfs-7", "2026-06-08T11:00:00Z"), // newest → keepLatest
];

/// The view `select_kept` consumes — mirrors the operator's `retention_view`
/// (name + endTime; none of these are pinned).
struct View {
    name: String,
    end: DateTime<Utc>,
}
impl SnapshotLike for View {
    fn end_time(&self) -> DateTime<Utc> {
        self.end
    }
    fn id(&self) -> &str {
        &self.name
    }
    fn pinned(&self) -> bool {
        false
    }
}

/// Backdate a Snapshot's `status.timing.endTime` via the status subresource.
async fn backdate(api: &Api<Snapshot>, name: &str, end_time: &str) {
    let patch = serde_json::json!({ "status": { "timing": { "endTime": end_time } } });
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .unwrap_or_else(|e| panic!("backdate {name} endTime: {e}"));
}

#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn gfs_time_buckets_prune_backdated_history() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("fixtures ready");
    let client: Client = world.client().clone();
    ensure_repo(&client, SUBPATH).await;

    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let _ = repos
        .create(
            &PostParams::default(),
            &cr(repository_json(REPO, SUBPATH, serde_json::json!({}))),
        )
        .await;
    wait_phase(&repos, REPO, "Ready")
        .await
        .expect("GFS repository should reach Ready");

    // The policy starts with NO retention (the helper's default keepLatest: 5
    // would prune during seeding), over the shared e2e-src source.
    let cfg = snapshot_policy_json(
        E2E_NAMESPACE,
        POLICY,
        "Repository",
        REPO,
        serde_json::json!({ "retention": null }),
    );
    let _ = configs.create(&PostParams::default(), &cr(cfg)).await;

    // Seed 7 REAL snapshots sequentially (each must Succeed: only
    // terminal-successful snapshots participate in GFS), then backdate each.
    for (name, end_time) in HISTORY {
        if backups.get_opt(name).await.ok().flatten().is_none() {
            let b = snapshot_json(
                E2E_NAMESPACE,
                name,
                POLICY,
                serde_json::json!({ "deletionPolicy": "Delete" }),
            );
            let _ = backups.create(&PostParams::default(), &cr(b)).await;
        }
        wait_phase(&backups, name, "Succeeded")
            .await
            .unwrap_or_else(|e| panic!("seed snapshot {name} should succeed: {e}"));
        backdate(&backups, name, end_time).await;
    }
    // The backdated endTime must STICK (a reconciler that rewrites terminal
    // timing would be a status-churn bug — and would invalidate retention).
    for (name, end_time) in HISTORY {
        let s = status_json(&backups, name).await;
        assert_eq!(
            s.pointer("/timing/endTime").and_then(|v| v.as_str()),
            Some(*end_time),
            "backdated endTime must persist on {name}; got {s}"
        );
    }

    // Expected partition, computed with the operator's OWN pure kernel over the
    // same views — plus hardcoded sanity asserts so a degenerate kernel (keep
    // everything / delete everything) cannot pass.
    let retention: Retention = serde_json::from_value(serde_json::json!({
        "keepLatest": 1, "keepDaily": 2, "keepWeekly": 2, "keepMonthly": 2, "keepAnnual": 2
    }))
    .unwrap();
    let views: Vec<View> = HISTORY
        .iter()
        .map(|(name, end)| View {
            name: name.to_string(),
            end: end.parse().unwrap(),
        })
        .collect();
    let kept = select_kept(&views, &retention);
    let keep: BTreeSet<&str> = kept.keep.iter().map(String::as_str).collect();
    let delete: BTreeSet<&str> = kept.delete.iter().map(String::as_str).collect();
    assert!(!delete.is_empty(), "the spec must prune something");
    assert!(
        keep.contains("e2e-gfs-1"),
        "keepAnnual must hold the ~400-day-old snapshot against newer dailies; keep: {keep:?}"
    );
    assert!(
        delete.contains("e2e-gfs-6"),
        "the same-day OLDER duplicate must be pruned (daily keeps the newest per day); \
         delete: {delete:?}"
    );
    assert!(
        keep.contains("e2e-gfs-7"),
        "keepLatest must hold the newest snapshot; keep: {keep:?}"
    );

    // Flip retention ON (generation bump → the policy reconciler prunes).
    let patch = serde_json::json!({ "spec": { "retention": {
        "keepLatest": 1, "keepDaily": 2, "keepWeekly": 2, "keepMonthly": 2, "keepAnnual": 2
    } } });
    configs
        .patch(POLICY, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .expect("set retention on the policy");

    // Every expected-deleted CR disappears (CR deletion completes only after the
    // finalizer's kopia snapshot-delete Job — so this covers the kopia side too)…
    for name in &delete {
        wait_until(
            &format!("{name} pruned by GFS retention"),
            default_timeout(),
            poll_interval(),
            || async { Ok(backups.get_opt(name).await?.is_none().then_some(())) },
        )
        .await
        .unwrap_or_else(|e| panic!("{name} should be pruned by GFS retention: {e}"));
    }
    // …and every expected-kept CR survives the prune wave.
    for name in &keep {
        assert!(
            backups
                .get_opt(name)
                .await
                .expect("query kept snapshot")
                .is_some(),
            "{name} must SURVIVE the prune (over-deletion would be data loss)"
        );
    }

    // The policy's bookkeeping converges: once the delete cascade settles, a
    // final reconcile stamps `activeSnapshotCount == keep.len()`. (This field
    // used to be written under the pre-rename `activeBackupCount` name and was
    // SILENTLY PRUNED by the apiserver — this wait is the regression guard.)
    // Pruning can happen across several watch-triggered waves, so
    // `lastPruneDeleted` is per-wave — assert it (and the timestamp) landed,
    // not its exact value.
    wait_until(
        "policy stamps activeSnapshotCount for the kept set",
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(&configs, POLICY).await;
            Ok((s
                .pointer("/retention/activeSnapshotCount")
                .and_then(|v| v.as_i64())
                == Some(keep.len() as i64))
            .then_some(()))
        },
    )
    .await
    .expect("status.retention.activeSnapshotCount must converge to the kept count");
    let s = status_json(&configs, POLICY).await;
    assert!(
        s.pointer("/retention/lastPruneDeleted")
            .and_then(|v| v.as_i64())
            .is_some_and(|n| n >= 1),
        "lastPruneDeleted must record the (last) prune wave; got {s}"
    );
    assert!(
        s.pointer("/retention/lastPruneAt")
            .and_then(|v| v.as_str())
            .is_some_and(|t| !t.is_empty()),
        "lastPruneAt must be stamped; got {s}"
    );

    // kopia-side truth: a ReadOnly verifier repo over the SAME dir sees exactly
    // the kept snapshots.
    let count = observed_snapshot_count(&client, "e2e-gfs-verify", SUBPATH).await;
    assert_eq!(
        count,
        keep.len() as i64,
        "kopia must hold exactly the kept snapshots after the prune"
    );
}
