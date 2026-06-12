//! Discovered-snapshot catalog: materialize (and expire) `origin: discovered`
//! `Snapshot` CRs from a repository's kopia snapshot listing (ADR §2.1/§2.3,
//! §3.1 `catalog`).
//!
//! Shared by the `Repository` and `ClusterRepository` reconcilers so the rules
//! cannot fork between the two kinds. The catalog **decision** is a pure
//! function ([`plan_catalog`]) and is unit-tested exhaustively here; the kube
//! LIST/create/delete and the cross-namespace placement lookups are the thin IO
//! parts ([`scan`]).
//!
//! ## The rules (all enforced by [`plan_catalog`])
//!
//! - **Dedup** by `(repository CR UID, kopiaSnapshotID)`: two scans never
//!   materialize the same snapshot twice, and the same kopia snapshot under a
//!   *different* repository CR is a distinct row (that's how adopting a
//!   repository materializes snapshots another `Repository` produced).
//! - **Produced snapshots are not "discovered".** A snapshot whose id is already
//!   carried by a scheduled/manual `Snapshot` CR resolving to *this* repository
//!   never gets a discovered row — `discovered` means "found in the repository,
//!   not produced through this CR" (a rescan must not duplicate this cluster's
//!   own backups).
//! - **Bounds** (`spec.catalog.retain`): the most-recent `perIdentity` rows per
//!   `username@hostname:path`, nothing older than `maxAgeDays`. Rows beyond the
//!   bounds are **expired — the CR is deleted, the kopia snapshot is untouched**
//!   (discovered rows are forced `deletionPolicy: Retain`, §4.5).
//! - **Absence expiry**: a row whose snapshot no longer appears in a *complete*
//!   listing was deleted repository-side (an external writer pruned it) — the
//!   stale row is expired. Skipped when the listing was truncated (absence is
//!   unknowable from a partial list).
//!
//! ## Refresh cadence
//!
//! Scans run when `status.catalog.lastRefreshAt` is older than the effective
//! `spec.catalog.refreshInterval` (default
//! [`kopiur_api::consts::DEFAULT_CATALOG_REFRESH_INTERVAL`]) — see
//! [`refresh_due`]. Gating the scan also gates the `lastRefreshAt` status write,
//! so a Ready repository's status is byte-stable between refreshes (the
//! status-churn rule). Object-store repositories re-list by recycling their
//! finished bootstrap Job ([`bootstrap_recycle_due`]); bare-path filesystem
//! repositories re-list in-process.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use kube::api::{Api, DeleteParams, ListParams};

use kopiur_api::cluster_repository::AllowedNamespaces;
use kopiur_api::common::{CatalogBounds, CatalogRetain, RepositoryKind};
use kopiur_api::snapshot::repository_ref_for;
use kopiur_api::{Snapshot, validate};
use kopiur_kopia::{SnapshotListEntry, SnapshotSource};

use crate::consts::{ORIGIN_LABEL, REPOSITORY_UID_LABEL, SNAPSHOT_ID_LABEL};
use crate::context::Context;
use crate::error::{Error, Result};
use crate::io;

/// The dedup key for a discovered snapshot: `(Repository CR UID, kopiaSnapshotID)`
/// (ADR §2.1).
pub fn catalog_dedup_key(repo_uid: &str, snapshot_id: &str) -> (String, String) {
    (repo_uid.to_string(), snapshot_id.to_string())
}

/// The kopia identity a snapshot was taken under, as the canonical
/// `username@hostname:path` string `catalog.retain.perIdentity` groups by.
pub fn identity_key(source: &SnapshotSource) -> String {
    format!("{}@{}:{}", source.user_name, source.host, source.path)
}

/// `true` when a catalog scan is due: never scanned, an unparseable stamp
/// (defensive — we wrote it), or `last_refresh_at + interval <= now`.
pub fn refresh_due(
    last_refresh_at: Option<&str>,
    interval: std::time::Duration,
    now: DateTime<Utc>,
) -> bool {
    let Some(raw) = last_refresh_at else {
        return true;
    };
    let Ok(last) = DateTime::parse_from_rfc3339(raw) else {
        return true;
    };
    let Ok(interval) = chrono::Duration::from_std(interval) else {
        return true;
    };
    last.with_timezone(&Utc) + interval <= now
}

/// The steady-state requeue for a Ready repository: the usual 5 minutes, or the
/// catalog refresh interval when the user asked for a faster re-scan cadence
/// (otherwise a sub-5m `refreshInterval` would silently never fire on time).
pub fn reconcile_interval(catalog: Option<&CatalogBounds>) -> std::time::Duration {
    std::time::Duration::from_secs(300).min(CatalogBounds::effective_refresh_interval(catalog))
}

/// `true` when a *finished* bootstrap Job should be deleted so the next
/// reconcile re-runs it with a fresh `snapshot list`: the repository is `Ready`
/// and either the catalog refresh is due or the spec changed since the result
/// was taken (`generation != observedGeneration` — a re-pointed backend must
/// re-bootstrap, not keep reporting the old repository's identity).
pub fn bootstrap_recycle_due(
    phase_is_ready: bool,
    generation: Option<i64>,
    observed_generation: Option<i64>,
    last_refresh_at: Option<&str>,
    interval: std::time::Duration,
    now: DateTime<Utc>,
) -> bool {
    if !phase_is_ready {
        return false;
    }
    if generation != observed_generation {
        return true;
    }
    refresh_due(last_refresh_at, interval, now)
}

/// `true` when a fresh repository listing should actually be SCANNED into the
/// catalog (materialize/expire discovered rows): the timed refresh is due, OR
/// the spec changed since the last reconciled generation. The generation arm is
/// load-bearing for `catalog.retain` edits: a tightened `perIdentity` recycles
/// the bootstrap Job for a fresh listing ([`bootstrap_recycle_due`]'s own
/// generation arm), but gating the scan on `refresh_due` alone then threw that
/// fresh result away — the over-cap rows only expired at the NEXT timed
/// refresh (up to `refreshInterval` later), not on the spec change that asked
/// for it. The caller passes the PRE-reconcile `status.observedGeneration`
/// (the cached object), so the scan runs exactly once per spec change.
pub fn scan_due(
    generation: Option<i64>,
    observed_generation: Option<i64>,
    last_refresh_at: Option<&str>,
    interval: std::time::Duration,
    now: DateTime<Utc>,
) -> bool {
    if generation != observed_generation {
        return true;
    }
    refresh_due(last_refresh_at, interval, now)
}

/// `true` when the *no-Job* path may (re-)create the bootstrap Job. The finished
/// Job normally lingers until [`bootstrap_recycle_due`] recycles it — but the
/// kube TTL controller can reap it first (`ttlSecondsAfterFinished`), and an
/// unconditional re-create on that wake would pin the catalog refresh cadence to
/// the Job TTL instead of `catalog.refreshInterval`. So: a repo that is not yet
/// `Ready` always proceeds (first bootstrap / failure retry), and a `Ready` repo
/// proceeds only when the same recycle predicate says a re-run is warranted
/// (refresh due, or the spec changed since the last result was taken).
///
/// Deliberate: a `Failed`/`Degraded` mover-bootstrapped repo keeps re-trying on
/// the Job-TTL cadence (default 1h) — a bounded, infrequent retry against a
/// backend that may have been fixed out-of-band (creds repaired, bucket
/// created). Unlike re-running *succeeded* work this converges, and the
/// in-process filesystem path's stricter `terminal_gate_holds` hard-stop keys
/// on a credential `resourceVersion` the mover path does not pin (yet).
pub fn bootstrap_create_due(
    phase_is_ready: bool,
    generation: Option<i64>,
    observed_generation: Option<i64>,
    last_refresh_at: Option<&str>,
    interval: std::time::Duration,
    now: DateTime<Utc>,
) -> bool {
    if !phase_is_ready {
        return true;
    }
    bootstrap_recycle_due(
        true,
        generation,
        observed_generation,
        last_refresh_at,
        interval,
        now,
    )
}

/// A materialized discovered row (one `origin: discovered` `Snapshot` CR of this
/// repository), as the planner sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRow {
    /// Namespace the CR lives in.
    pub namespace: String,
    /// CR name.
    pub name: String,
    /// The kopia snapshot id (from the `snapshot-id` label).
    pub snapshot_id: String,
    /// The snapshot's end time (from `status.timing.endTime`), used for
    /// per-identity ordering. Rows written before timing was recorded sort oldest.
    pub end_time: Option<DateTime<Utc>>,
}

/// What a scan decided: entries to materialize and rows to expire.
#[derive(Debug, Default)]
pub struct CatalogPlan<'a> {
    /// Listing entries that need a discovered `Snapshot` CR created.
    pub create: Vec<&'a SnapshotListEntry>,
    /// Existing rows to delete (namespace, name). Deleting a row never touches
    /// the kopia snapshot (discovered rows are forced `Retain`).
    pub expire: Vec<(String, String)>,
}

/// Decide creations and expiries. Pure — see the module docs for the rules.
pub fn plan_catalog<'a>(
    rows: &[CatalogRow],
    produced_ids: &BTreeSet<String>,
    listing: &'a [SnapshotListEntry],
    listing_truncated: bool,
    retain: Option<&CatalogRetain>,
    now: DateTime<Utc>,
) -> CatalogPlan<'a> {
    // Eligible = in the listing, not produced by this repository CR, within the
    // age bound.
    let max_age = retain
        .and_then(|r| r.max_age_days)
        .filter(|d| *d >= 1)
        .map(|d| chrono::Duration::days(d));
    let eligible = listing
        .iter()
        .filter(|e| !produced_ids.contains(&e.id))
        .filter(|e| max_age.is_none_or(|a| e.end_time + a > now));

    // Keep-set: the most-recent `perIdentity` eligible entries per identity.
    let per_identity = retain
        .and_then(|r| r.per_identity)
        .filter(|n| *n >= 0)
        .map(|n| n as usize);
    let mut by_identity: BTreeMap<String, Vec<&SnapshotListEntry>> = BTreeMap::new();
    for e in eligible {
        by_identity
            .entry(identity_key(&e.source))
            .or_default()
            .push(e);
    }
    let mut keep: BTreeMap<&str, &SnapshotListEntry> = BTreeMap::new();
    for entries in by_identity.values_mut() {
        entries.sort_by_key(|e| std::cmp::Reverse(e.end_time));
        let cap = per_identity.unwrap_or(entries.len());
        for e in entries.iter().take(cap) {
            keep.insert(e.id.as_str(), e);
        }
    }

    let have: BTreeSet<&str> = rows.iter().map(|r| r.snapshot_id.as_str()).collect();
    let listed: BTreeSet<&str> = listing.iter().map(|e| e.id.as_str()).collect();

    let mut create: Vec<&SnapshotListEntry> = keep
        .values()
        .filter(|e| !have.contains(e.id.as_str()))
        .copied()
        .collect();
    // Newest-first creation order so a creation interrupted mid-batch has
    // materialized the most useful rows first.
    create.sort_by_key(|e| std::cmp::Reverse(e.end_time));

    let expire = rows
        .iter()
        .filter(|r| {
            if keep.contains_key(r.snapshot_id.as_str()) {
                return false;
            }
            // In the listing but outside the keep-set: aged out, over the
            // per-identity cap, or shadowing a produced snapshot — expire (safe
            // even under truncation; we saw the entry). Absent from the listing:
            // deleted repository-side — expire only when the listing is complete.
            listed.contains(r.snapshot_id.as_str()) || !listing_truncated
        })
        .map(|r| (r.namespace.clone(), r.name.clone()))
        .collect();

    CatalogPlan { create, expire }
}

/// Extract this repository's discovered rows from a `Snapshot` LIST (rows carry
/// the `(repository-uid, snapshot-id)` labels). Pure.
pub fn rows_for(repo_uid: &str, snapshots: &[Snapshot]) -> Vec<CatalogRow> {
    snapshots
        .iter()
        .filter_map(|s| {
            let labels = s.labels();
            if labels.get(ORIGIN_LABEL).map(String::as_str) != Some("discovered") {
                return None;
            }
            if labels.get(REPOSITORY_UID_LABEL).map(String::as_str) != Some(repo_uid) {
                return None;
            }
            let id = labels.get(SNAPSHOT_ID_LABEL)?.clone();
            let end_time = s
                .status
                .as_ref()
                .and_then(|st| st.timing.as_ref())
                .and_then(|t| t.end_time.as_deref())
                .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
                .map(|t| t.with_timezone(&Utc));
            Some(CatalogRow {
                namespace: s.namespace().unwrap_or_default(),
                name: s.name_any(),
                snapshot_id: id,
                end_time,
            })
        })
        .collect()
}

/// The repository CR a scan runs for, for matching produced `Snapshot` CRs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanOwner<'a> {
    /// A namespaced `Repository`.
    Repository {
        /// CR name.
        name: &'a str,
        /// CR namespace.
        namespace: &'a str,
    },
    /// A cluster-scoped `ClusterRepository`.
    ClusterRepository {
        /// CR name.
        name: &'a str,
    },
}

impl ScanOwner<'_> {
    fn kind(&self) -> RepositoryKind {
        match self {
            ScanOwner::Repository { .. } => RepositoryKind::Repository,
            ScanOwner::ClusterRepository { .. } => RepositoryKind::ClusterRepository,
        }
    }
}

/// The kopia snapshot ids of scheduled/manual `Snapshot` CRs that resolve to
/// this repository CR (via `status.resolved.repository` or the owner reference —
/// the same derivation `Restore` and the kubectl plugin use). These are this
/// cluster's *produced* snapshots: a rescan must never duplicate them as
/// discovered rows. Origin uses [`crate::snapshot::resolve_origin`]'s precedence
/// (status, then label, default manual) — NOT the label alone, because a bare
/// `kubectl create` manual Snapshot may never carry the origin label. Pure.
pub fn produced_ids_for(owner: ScanOwner<'_>, snapshots: &[Snapshot]) -> BTreeSet<String> {
    use kopiur_api::Origin;
    snapshots
        .iter()
        .filter(|s| match crate::snapshot::resolve_origin(s) {
            Origin::Scheduled | Origin::Manual => true,
            Origin::Discovered => false,
        })
        .filter(|s| {
            let Some(rref) = repository_ref_for(s) else {
                return false;
            };
            if rref.kind != owner.kind() {
                return false;
            }
            match owner {
                ScanOwner::Repository { name, namespace } => {
                    let ref_ns = rref
                        .namespace
                        .clone()
                        .or_else(|| s.namespace())
                        .unwrap_or_default();
                    rref.name == name && ref_ns == namespace
                }
                ScanOwner::ClusterRepository { name } => rref.name == name,
            }
        })
        .filter_map(|s| {
            s.status
                .as_ref()
                .and_then(|st| st.snapshot.as_ref())
                .map(|i| i.kopia_snapshot_id.clone())
        })
        .collect()
}

/// Where discovered rows are created.
pub enum Placement<'a> {
    /// A namespaced `Repository`: always its own namespace.
    Namespace(&'a str),
    /// A `ClusterRepository`: the namespace named by the snapshot identity's
    /// hostname when it exists and passes the tenancy gate, else
    /// `catalog.fallbackNamespace`, else the entry is skipped (ADR §2.3).
    Cluster {
        /// The tenancy gate an identity-hostname namespace must pass.
        allowed: &'a AllowedNamespaces,
        /// `catalog.fallbackNamespace` for identities that don't.
        fallback: Option<&'a str>,
    },
}

/// What a [`scan`] did, for the caller's status patch / metrics / events.
#[derive(Debug, Default)]
pub struct ScanOutcome {
    /// Discovered rows created this scan.
    pub created: i64,
    /// Discovered rows expired (CR deleted; kopia snapshot untouched).
    pub expired: i64,
    /// Discovered rows of this repository after the scan (the
    /// `status.catalog.discoveredBackupCount` value).
    pub discovered: i64,
    /// `ClusterRepository` only: identity hostnames that mapped to no allowed
    /// namespace and had no `fallbackNamespace` — their snapshots got no row.
    /// The caller surfaces these (Event + log) with the fix.
    pub unplaced_hosts: BTreeSet<String>,
}

/// Run a catalog scan: LIST the relevant `Snapshot` CRs, [`plan_catalog`], then
/// create/expire rows. The caller supplies the kopia listing (in-process for
/// bare-path filesystem, from the bootstrap Job's result for everything else)
/// and patches its own status from the returned outcome.
#[allow(clippy::too_many_arguments)]
pub async fn scan(
    ctx: &Context,
    owner: ScanOwner<'_>,
    owner_ref: OwnerReference,
    repo_uid: &str,
    placement: Placement<'_>,
    catalog: Option<&CatalogBounds>,
    listing: &[SnapshotListEntry],
    listing_truncated: bool,
) -> Result<ScanOutcome> {
    let repo_name = match owner {
        ScanOwner::Repository { name, .. } | ScanOwner::ClusterRepository { name } => name,
    };

    // One cluster-wide LIST serves both sides of the plan: this repository's
    // discovered rows (by the dedup labels — always controller-stamped) AND the
    // produced snapshots whose ids must never be re-discovered. Cluster-wide on
    // purpose: a ClusterRepository's rows live in many namespaces, and a
    // SnapshotPolicy may reference a Repository across namespaces. No label
    // selector: a bare `kubectl create` manual Snapshot may carry no origin
    // label, and missing it here would duplicate it as a discovered row. The
    // LIST is refresh-gated (default 1h), not per-reconcile.
    let all_api: Api<Snapshot> = Api::all(ctx.client.clone());
    let all_snapshots = all_api.list(&ListParams::default()).await?.items;
    let rows = rows_for(repo_uid, &all_snapshots);
    let produced_ids = produced_ids_for(owner, &all_snapshots);

    let retain = catalog.and_then(|c| c.retain.as_ref());
    let plan = plan_catalog(
        &rows,
        &produced_ids,
        listing,
        listing_truncated,
        retain,
        Utc::now(),
    );

    let mut outcome = ScanOutcome::default();

    // Resolve placement per entry (cached Namespace lookups for the cluster case),
    // then materialize.
    let mut ns_cache: BTreeMap<String, Option<BTreeMap<String, String>>> = BTreeMap::new();
    for entry in &plan.create {
        let target_ns: String = match &placement {
            Placement::Namespace(ns) => (*ns).to_string(),
            Placement::Cluster { allowed, fallback } => {
                let host = entry.source.host.as_str();
                let labels = match ns_cache.get(host) {
                    Some(cached) => cached.clone(),
                    None => {
                        let ns_api: Api<Namespace> = Api::all(ctx.client.clone());
                        let looked_up = ns_api
                            .get_opt(host)
                            .await?
                            .map(|n| n.metadata.labels.unwrap_or_default());
                        ns_cache.insert(host.to_string(), looked_up.clone());
                        looked_up
                    }
                };
                let allowed_here = labels.as_ref().is_some_and(|l| {
                    validate::validate_consumer_against_cluster_repo(
                        host,
                        repo_name,
                        allowed,
                        Some(l),
                    )
                    .is_ok()
                });
                match crate::cluster_repository::placement_namespace(host, allowed_here, *fallback)
                {
                    Some(ns) => ns.to_string(),
                    None => {
                        outcome.unplaced_hosts.insert(host.to_string());
                        continue;
                    }
                }
            }
        };
        materialize_discovered(ctx, &owner_ref, &target_ns, repo_name, repo_uid, entry).await?;
        outcome.created += 1;
    }

    for (ns, name) in &plan.expire {
        let api: Api<Snapshot> = Api::namespaced(ctx.client.clone(), ns);
        match api.delete(name, &DeleteParams::default()).await {
            Ok(_) => outcome.expired += 1,
            Err(kube::Error::Api(ae)) if ae.code == 404 => {}
            Err(e) => return Err(Error::Kube(e)),
        }
    }

    outcome.discovered = (rows.len() as i64 - outcome.expired).max(0) + outcome.created;
    if outcome.created > 0 || outcome.expired > 0 {
        tracing::info!(
            repo = repo_name,
            created = outcome.created,
            expired = outcome.expired,
            discovered = outcome.discovered,
            "catalog scan reconciled discovered Snapshot CRs"
        );
    }
    Ok(outcome)
}

/// Create one `origin: discovered` `Snapshot` CR for a listing entry.
/// `deletionPolicy` is FORCED to `Retain` (the operator never deletes a
/// discovered snapshot, §4.5); identity, timing, and size come from the kopia
/// listing so `kubectl kopiur snapshots list` shows real data for foreign rows.
async fn materialize_discovered(
    ctx: &Context,
    owner: &OwnerReference,
    namespace: &str,
    repo_name: &str,
    repo_uid: &str,
    entry: &SnapshotListEntry,
) -> Result<()> {
    use kopiur_api::common::{DeletionPolicy, ResolvedIdentity};
    use kopiur_api::snapshot::{
        SnapshotInfo, SnapshotSpec, SnapshotStats, SnapshotStatus, SnapshotTiming,
    };
    use kopiur_api::{Origin, SnapshotPhase};

    // CR name: stable from the (short) snapshot id, namespaced under the repo.
    let short = entry.id.chars().take(16).collect::<String>();
    let cr_name = format!("{repo_name}-disc-{short}");

    let mut labels = BTreeMap::new();
    labels.insert(ORIGIN_LABEL.to_string(), "discovered".to_string());
    labels.insert(REPOSITORY_UID_LABEL.to_string(), repo_uid.to_string());
    labels.insert(SNAPSHOT_ID_LABEL.to_string(), entry.id.clone());

    let mut backup = Snapshot::new(
        &cr_name,
        SnapshotSpec {
            policy_ref: None,
            tags: None,
            failure_policy: None,
            // Forced Retain for discovered (webhook would reject otherwise).
            deletion_policy: Some(DeletionPolicy::Retain),
            // Discovered snapshots are not pinned by the operator.
            pin: false,
        },
    );
    backup.metadata = io::child_meta(&cr_name, namespace, labels, Some(owner.clone()));
    backup.status = Some(SnapshotStatus {
        phase: Some(SnapshotPhase::Discovered),
        origin: Some(Origin::Discovered),
        snapshot: Some(SnapshotInfo {
            kopia_snapshot_id: entry.id.clone(),
            identity: ResolvedIdentity {
                username: entry.source.user_name.clone(),
                hostname: entry.source.host.clone(),
                source_path: Some(entry.source.path.clone()),
            },
        }),
        timing: Some(SnapshotTiming {
            start_time: Some(entry.start_time.to_rfc3339()),
            end_time: Some(entry.end_time.to_rfc3339()),
            duration_seconds: Some((entry.end_time - entry.start_time).num_seconds()),
        }),
        stats: Some(SnapshotStats {
            size_bytes: i64::try_from(entry.stats.total_size).ok(),
            ..Default::default()
        }),
        ..Default::default()
    });

    let api: Api<Snapshot> = Api::namespaced(ctx.client.clone(), namespace);
    // Create the CR; the discovered status is then PATCHed onto the subresource.
    match io::apply(&api, &cr_name, &backup).await {
        Ok(_) => {}
        Err(Error::Kube(kube::Error::Api(ae))) if ae.code == 409 => return Ok(()),
        Err(e) => return Err(e),
    }
    io::patch_status(
        &api,
        &cr_name,
        serde_json::to_value(backup.status.unwrap_or_default())?,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_kopia::SnapshotStats;

    fn entry(id: &str, identity: (&str, &str, &str), end: DateTime<Utc>) -> SnapshotListEntry {
        SnapshotListEntry {
            id: id.into(),
            source: SnapshotSource {
                user_name: identity.0.into(),
                host: identity.1.into(),
                path: identity.2.into(),
            },
            description: String::new(),
            start_time: end - chrono::Duration::seconds(60),
            end_time: end,
            stats: SnapshotStats::default(),
            root_entry: None,
            retention_reason: vec![],
        }
    }

    fn row(name: &str, id: &str, end: Option<DateTime<Utc>>) -> CatalogRow {
        CatalogRow {
            namespace: "ns".into(),
            name: name.into(),
            snapshot_id: id.into(),
            end_time: end,
        }
    }

    fn t(mins_ago: i64) -> DateTime<Utc> {
        Utc::now() - chrono::Duration::minutes(mins_ago)
    }

    fn ids<'a>(plan: &'a CatalogPlan<'_>) -> Vec<&'a str> {
        plan.create.iter().map(|e| e.id.as_str()).collect()
    }

    fn expired<'a>(plan: &'a CatalogPlan<'_>) -> Vec<&'a str> {
        plan.expire.iter().map(|(_, n)| n.as_str()).collect()
    }

    #[test]
    fn materializes_unseen_entries_and_dedups_existing() {
        let listing = vec![
            entry("aaa", ("u", "h", "/p"), t(10)),
            entry("bbb", ("u", "h", "/p"), t(5)),
        ];
        let rows = vec![row("r-aaa", "aaa", Some(t(10)))];
        let plan = plan_catalog(&rows, &BTreeSet::new(), &listing, false, None, Utc::now());
        assert_eq!(ids(&plan), vec!["bbb"]);
        assert!(plan.expire.is_empty());
    }

    #[test]
    fn produced_snapshots_never_become_discovered_rows() {
        // A rescan of a repository this cluster writes to must not duplicate its
        // own scheduled/manual backups as discovered rows.
        let listing = vec![
            entry("ours", ("app", "ns", "/data"), t(5)),
            entry("foreign", ("legacy", "old-host", "/data"), t(7)),
        ];
        let produced: BTreeSet<String> = ["ours".to_string()].into();
        let plan = plan_catalog(&[], &produced, &listing, false, None, Utc::now());
        assert_eq!(ids(&plan), vec!["foreign"]);
    }

    #[test]
    fn a_stale_discovered_row_shadowing_a_produced_snapshot_is_expired() {
        // Cleanup path for rows created by the old (pre-dedup) scan.
        let listing = vec![entry("ours", ("app", "ns", "/data"), t(5))];
        let produced: BTreeSet<String> = ["ours".to_string()].into();
        let rows = vec![row("r-ours", "ours", Some(t(5)))];
        let plan = plan_catalog(&rows, &produced, &listing, false, None, Utc::now());
        assert!(plan.create.is_empty());
        assert_eq!(expired(&plan), vec!["r-ours"]);
    }

    #[test]
    fn per_identity_cap_is_per_identity_not_global() {
        // Identity A has 3 snapshots, identity B has 1; perIdentity=2 must keep
        // the 2 newest of A AND B's single one (a global cap would starve B).
        let listing = vec![
            entry("a1", ("u", "a", "/p"), t(30)),
            entry("a2", ("u", "a", "/p"), t(20)),
            entry("a3", ("u", "a", "/p"), t(10)),
            entry("b1", ("u", "b", "/p"), t(40)),
        ];
        let retain = CatalogRetain {
            per_identity: Some(2),
            max_age_days: None,
        };
        let plan = plan_catalog(
            &[],
            &BTreeSet::new(),
            &listing,
            false,
            Some(&retain),
            Utc::now(),
        );
        let mut got = ids(&plan);
        got.sort();
        assert_eq!(got, vec!["a2", "a3", "b1"]);
    }

    #[test]
    fn per_identity_zero_disables_materialization() {
        let listing = vec![entry("aaa", ("u", "h", "/p"), t(5))];
        let retain = CatalogRetain {
            per_identity: Some(0),
            max_age_days: None,
        };
        let plan = plan_catalog(
            &[],
            &BTreeSet::new(),
            &listing,
            false,
            Some(&retain),
            Utc::now(),
        );
        assert!(plan.create.is_empty());
    }

    #[test]
    fn over_cap_rows_are_expired_oldest_first_semantics() {
        // 3 rows exist for one identity; perIdentity=1 keeps only the newest and
        // expires the other two CRs (never the kopia snapshots).
        let listing = vec![
            entry("a1", ("u", "a", "/p"), t(30)),
            entry("a2", ("u", "a", "/p"), t(20)),
            entry("a3", ("u", "a", "/p"), t(10)),
        ];
        let rows = vec![
            row("r-a1", "a1", Some(t(30))),
            row("r-a2", "a2", Some(t(20))),
            row("r-a3", "a3", Some(t(10))),
        ];
        let retain = CatalogRetain {
            per_identity: Some(1),
            max_age_days: None,
        };
        let plan = plan_catalog(
            &rows,
            &BTreeSet::new(),
            &listing,
            false,
            Some(&retain),
            Utc::now(),
        );
        assert!(plan.create.is_empty());
        let mut gone = expired(&plan);
        gone.sort();
        assert_eq!(gone, vec!["r-a1", "r-a2"]);
    }

    #[test]
    fn max_age_days_excludes_old_snapshots_and_expires_their_rows() {
        let old = Utc::now() - chrono::Duration::days(120);
        let listing = vec![
            entry("old", ("u", "h", "/p"), old),
            entry("new", ("u", "h", "/p"), t(5)),
        ];
        let rows = vec![row("r-old", "old", Some(old))];
        let retain = CatalogRetain {
            per_identity: None,
            max_age_days: Some(90),
        };
        let plan = plan_catalog(
            &rows,
            &BTreeSet::new(),
            &listing,
            false,
            Some(&retain),
            Utc::now(),
        );
        assert_eq!(ids(&plan), vec!["new"]);
        assert_eq!(expired(&plan), vec!["r-old"]);
    }

    #[test]
    fn absent_rows_expire_only_when_the_listing_is_complete() {
        // The row's snapshot was deleted repository-side.
        let rows = vec![row("r-gone", "gone", Some(t(10)))];
        let listing = vec![entry("still", ("u", "h", "/p"), t(5))];
        // Complete listing → stale row expires.
        let plan = plan_catalog(&rows, &BTreeSet::new(), &listing, false, None, Utc::now());
        assert_eq!(expired(&plan), vec!["r-gone"]);
        // Truncated listing → absence is unknowable; the row survives.
        let plan = plan_catalog(&rows, &BTreeSet::new(), &listing, true, None, Utc::now());
        assert!(plan.expire.is_empty());
    }

    #[test]
    fn truncated_listing_still_expires_rows_it_can_see_are_over_cap() {
        let listing = vec![
            entry("a1", ("u", "a", "/p"), t(30)),
            entry("a2", ("u", "a", "/p"), t(10)),
        ];
        let rows = vec![
            row("r-a1", "a1", Some(t(30))),
            row("r-a2", "a2", Some(t(10))),
        ];
        let retain = CatalogRetain {
            per_identity: Some(1),
            max_age_days: None,
        };
        let plan = plan_catalog(
            &rows,
            &BTreeSet::new(),
            &listing,
            true,
            Some(&retain),
            Utc::now(),
        );
        assert_eq!(expired(&plan), vec!["r-a1"]);
    }

    #[test]
    fn refresh_due_gates_on_the_stamp() {
        let now = Utc::now();
        let interval = std::time::Duration::from_secs(3600);
        // Never scanned → due.
        assert!(refresh_due(None, interval, now));
        // Unparseable stamp → due (defensive).
        assert!(refresh_due(Some("not-a-time"), interval, now));
        // Fresh → not due.
        let fresh = (now - chrono::Duration::minutes(5)).to_rfc3339();
        assert!(!refresh_due(Some(&fresh), interval, now));
        // Stale → due.
        let stale = (now - chrono::Duration::minutes(61)).to_rfc3339();
        assert!(refresh_due(Some(&stale), interval, now));
    }

    #[test]
    fn bootstrap_recycle_requires_ready_and_fires_on_due_or_spec_change() {
        let now = Utc::now();
        let interval = std::time::Duration::from_secs(3600);
        let fresh = (now - chrono::Duration::minutes(5)).to_rfc3339();
        let stale = (now - chrono::Duration::minutes(61)).to_rfc3339();
        // Not Ready: never recycle (a Failed bootstrap is gated elsewhere).
        assert!(!bootstrap_recycle_due(
            false,
            Some(2),
            Some(1),
            None,
            interval,
            now
        ));
        // Ready + spec changed → recycle even when fresh.
        assert!(bootstrap_recycle_due(
            true,
            Some(2),
            Some(1),
            Some(&fresh),
            interval,
            now
        ));
        // Ready + same generation + fresh → keep the finished Job.
        assert!(!bootstrap_recycle_due(
            true,
            Some(2),
            Some(2),
            Some(&fresh),
            interval,
            now
        ));
        // Ready + same generation + stale → recycle for a fresh listing.
        assert!(bootstrap_recycle_due(
            true,
            Some(2),
            Some(2),
            Some(&stale),
            interval,
            now
        ));
    }

    // Regression guard (caught by the catalog_retain e2e): a spec change
    // recycles the bootstrap Job for a fresh listing, but the SCAN of that
    // result was gated on the timed refresh alone — a tightened
    // `catalog.retain` only expired rows at the next refreshInterval, not on
    // the edit that asked for it.
    #[test]
    fn scan_due_fires_on_spec_change_even_when_the_timed_refresh_is_not() {
        let now = Utc::now();
        let interval = std::time::Duration::from_secs(3600);
        let fresh = (now - chrono::Duration::minutes(5)).to_rfc3339();
        // Spec changed (gen != observed) + fresh stamp → scan NOW.
        assert!(scan_due(Some(3), Some(2), Some(&fresh), interval, now));
        // Settled generation + fresh stamp → byte-stable, no scan (the
        // status-churn rule).
        assert!(!scan_due(Some(3), Some(3), Some(&fresh), interval, now));
        // Settled generation + stale stamp → the timed refresh still fires.
        let stale = (now - chrono::Duration::minutes(61)).to_rfc3339();
        assert!(scan_due(Some(3), Some(3), Some(&stale), interval, now));
        // Never scanned → due regardless.
        assert!(scan_due(Some(1), Some(1), None, interval, now));
    }

    // Regression guard for the TTL-reap loop: when the kube TTL controller
    // deletes the finished bootstrap Job before the refresh interval elapses,
    // the no-Job path must NOT re-create it — otherwise the Job TTL (default
    // 1h) silently overrides `catalog.refreshInterval`.
    #[test]
    fn bootstrap_create_after_ttl_reap_waits_for_the_refresh_interval() {
        let now = Utc::now();
        let interval = std::time::Duration::from_secs(3600);
        let fresh = (now - chrono::Duration::minutes(5)).to_rfc3339();
        let stale = (now - chrono::Duration::minutes(61)).to_rfc3339();
        // Not Ready → always proceed (first bootstrap / failure retry).
        assert!(bootstrap_create_due(
            false,
            Some(1),
            None,
            None,
            interval,
            now
        ));
        // Ready + same generation + fresh scan → HOLD: the reaped Job must not
        // come back until the refresh is due.
        assert!(!bootstrap_create_due(
            true,
            Some(2),
            Some(2),
            Some(&fresh),
            interval,
            now
        ));
        // Ready + refresh due → re-create for a fresh listing.
        assert!(bootstrap_create_due(
            true,
            Some(2),
            Some(2),
            Some(&stale),
            interval,
            now
        ));
        // Ready + spec changed → re-create even when fresh.
        assert!(bootstrap_create_due(
            true,
            Some(3),
            Some(2),
            Some(&fresh),
            interval,
            now
        ));
        // Ready but never stamped (e.g. pre-catalog status) → defensive re-run.
        assert!(bootstrap_create_due(
            true,
            Some(2),
            Some(2),
            None,
            interval,
            now
        ));
    }

    #[test]
    fn rows_and_produced_extraction_respect_labels_and_refs() {
        use kopiur_api::common::{RepositoryKind, RepositoryRef};
        use kopiur_api::snapshot::{ResolvedSnapshot, SnapshotInfo, SnapshotStatus};

        fn snap(
            name: &str,
            ns: &str,
            origin: &str,
            extra_labels: &[(&str, &str)],
            status: Option<SnapshotStatus>,
        ) -> Snapshot {
            let mut s = Snapshot::new(
                name,
                kopiur_api::snapshot::SnapshotSpec {
                    policy_ref: None,
                    tags: None,
                    failure_policy: None,
                    deletion_policy: None,
                    pin: false,
                },
            );
            let mut labels = BTreeMap::new();
            labels.insert(ORIGIN_LABEL.to_string(), origin.to_string());
            for (k, v) in extra_labels {
                labels.insert((*k).to_string(), (*v).to_string());
            }
            s.metadata.namespace = Some(ns.to_string());
            s.metadata.labels = Some(labels);
            s.status = status;
            s
        }

        let discovered = snap(
            "repo-disc-aaa",
            "ns1",
            "discovered",
            &[(REPOSITORY_UID_LABEL, "uid-1"), (SNAPSHOT_ID_LABEL, "aaa")],
            None,
        );
        let other_uid = snap(
            "other-disc-bbb",
            "ns1",
            "discovered",
            &[(REPOSITORY_UID_LABEL, "uid-2"), (SNAPSHOT_ID_LABEL, "bbb")],
            None,
        );
        let produced = snap(
            "nightly-1",
            "ns1",
            "scheduled",
            &[],
            Some(SnapshotStatus {
                snapshot: Some(SnapshotInfo {
                    kopia_snapshot_id: "ccc".into(),
                    identity: kopiur_api::common::ResolvedIdentity {
                        username: "u".into(),
                        hostname: "h".into(),
                        source_path: Some("/p".into()),
                    },
                }),
                resolved: Some(ResolvedSnapshot {
                    repository: Some(RepositoryRef {
                        kind: RepositoryKind::Repository,
                        name: "repo".into(),
                        namespace: None,
                    }),
                    sources: vec![],
                }),
                ..Default::default()
            }),
        );

        let all = vec![discovered, other_uid, produced];
        let rows = rows_for("uid-1", &all);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].snapshot_id, "aaa");

        // A bare `kubectl create` manual Snapshot: NO origin label at all, no
        // status.origin — resolve_origin defaults it to Manual, so its id must
        // still be excluded from discovery (label-selector matching would miss it).
        let mut bare = snap(
            "manual-1",
            "ns1",
            "ignored",
            &[],
            Some(SnapshotStatus {
                snapshot: Some(SnapshotInfo {
                    kopia_snapshot_id: "ddd".into(),
                    identity: kopiur_api::common::ResolvedIdentity {
                        username: "u".into(),
                        hostname: "h".into(),
                        source_path: Some("/p".into()),
                    },
                }),
                resolved: Some(ResolvedSnapshot {
                    repository: Some(RepositoryRef {
                        kind: RepositoryKind::Repository,
                        name: "repo".into(),
                        namespace: None,
                    }),
                    sources: vec![],
                }),
                ..Default::default()
            }),
        );
        bare.metadata.labels = None;

        let all = {
            let mut v = all;
            v.push(bare);
            v
        };
        // The produced snapshots resolve to Repository/repo in their own namespace.
        let ids = produced_ids_for(
            ScanOwner::Repository {
                name: "repo",
                namespace: "ns1",
            },
            &all,
        );
        assert_eq!(ids, ["ccc".to_string(), "ddd".to_string()].into());
        // …but not to a different Repository, nor to a ClusterRepository.
        assert!(
            produced_ids_for(
                ScanOwner::Repository {
                    name: "repo",
                    namespace: "other-ns",
                },
                &all,
            )
            .is_empty()
        );
        assert!(produced_ids_for(ScanOwner::ClusterRepository { name: "repo" }, &all).is_empty());
    }

    #[test]
    fn reconcile_interval_honors_fast_refresh_but_caps_at_five_minutes() {
        // No catalog / slow refresh → the usual 5 minutes.
        assert_eq!(
            reconcile_interval(None),
            std::time::Duration::from_secs(300)
        );
        let slow: CatalogBounds =
            serde_json::from_value(serde_json::json!({ "refreshInterval": "2h" })).unwrap();
        assert_eq!(
            reconcile_interval(Some(&slow)),
            std::time::Duration::from_secs(300)
        );
        // A faster refresh shortens the requeue so the cadence actually fires.
        let fast: CatalogBounds =
            serde_json::from_value(serde_json::json!({ "refreshInterval": "30s" })).unwrap();
        assert_eq!(
            reconcile_interval(Some(&fast)),
            std::time::Duration::from_secs(30)
        );
    }

    #[test]
    fn identity_key_is_user_at_host_colon_path() {
        let src = SnapshotSource {
            user_name: "legacy".into(),
            host: "media".into(),
            path: "/data".into(),
        };
        assert_eq!(identity_key(&src), "legacy@media:/data");
    }
}
