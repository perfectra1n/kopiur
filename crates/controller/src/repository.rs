//! The `Repository` reconciler (ADR §3.1, §5.4).
//!
//! Responsibilities:
//! 1. Defensive re-validation (`api::validate`).
//! 2. Ensure the repo exists: connect, and create it if `create.enabled` — via a
//!    short-lived Job (ADR §5.4) so a controller restart never strands a kopia
//!    process. Set `status.phase`/`uniqueID`/`backend`/`storageStats`.
//! 3. Periodic catalog scan (`snapshot list`) materializing `origin: discovered`
//!    `Backup` CRs, bounded by `catalog.retain`, deduplicated by
//!    `(Repository.UID, kopiaSnapshotID)` (ADR §2.1).
//!
//! The catalog **dedup decision** is a pure function ([`catalog_dedup_key`] +
//! [`needs_materialization`]) and is unit-tested here; the kopia `snapshot list`
//! IO and `Backup` CR creation are the thin parts.

use std::collections::BTreeSet;
use std::sync::Arc;

use kube::runtime::controller::Action;
use kube::ResourceExt;

use kopiur_api::{validate, Repository};
use kopiur_kopia::SnapshotListEntry;

use crate::context::Context;
use crate::error::{error_policy_for, Error, Result};

/// The dedup key for a discovered snapshot: `(Repository.UID, kopiaSnapshotID)`
/// (ADR §2.1). Two scans of the same repo never materialize the same snapshot
/// twice, and the same snapshot id under a *different* repository is distinct.
pub fn catalog_dedup_key(repo_uid: &str, snapshot_id: &str) -> (String, String) {
    (repo_uid.to_string(), snapshot_id.to_string())
}

/// Given the snapshot ids already materialized as `Backup` CRs (the existing
/// set, keyed by `(repo_uid, id)`) and a fresh `snapshot list`, return the
/// entries that still need a `Backup` CR created. Pure; the caller does the
/// `Backup` CR creation.
pub fn needs_materialization<'a>(
    repo_uid: &str,
    existing: &BTreeSet<(String, String)>,
    listing: &'a [SnapshotListEntry],
) -> Vec<&'a SnapshotListEntry> {
    listing
        .iter()
        .filter(|e| !existing.contains(&catalog_dedup_key(repo_uid, &e.id)))
        .collect()
}

/// Reconcile a `Repository`.
#[tracing::instrument(skip(repo, ctx), fields(kind = "Repository", name = %repo.name_any()))]
pub async fn reconcile(repo: Arc<Repository>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&repo, &ctx).await;
    ctx.metrics
        .record_reconcile("Repository", start.elapsed().as_secs_f64());
    result
}

async fn reconcile_inner(repo: &Repository, _ctx: &Context) -> Result<Action> {
    if let Err(e) = validate::validate_repository_no_inline_retention(&repo.spec) {
        return Err(Error::Validation(e.to_string()));
    }

    // TODO(M6): connect-validate (short Job); if create.enabled and connect
    // fails with "not found", create the repo; set status.phase/uniqueID/
    // backend discriminant/storageStats from repository_status(); on the
    // catalog refresh interval, run snapshot_list, compute needs_materialization
    // against existing discovered Backups (keyed by (UID, id)), and create the
    // bounded (catalog.retain) set of origin: discovered Backup CRs. The dedup
    // decision (needs_materialization) is tested below.

    Ok(Action::requeue(std::time::Duration::from_secs(300)))
}

/// `error_policy` for the `Repository` controller.
pub fn error_policy(_obj: Arc<Repository>, err: &Error, ctx: Arc<Context>) -> Action {
    error_policy_for("Repository", err, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use kopiur_kopia::{SnapshotSource, SnapshotStats};

    fn entry(id: &str) -> SnapshotListEntry {
        SnapshotListEntry {
            id: id.into(),
            source: SnapshotSource {
                host: "h".into(),
                user_name: "u".into(),
                path: "/p".into(),
            },
            description: String::new(),
            start_time: Utc::now(),
            end_time: Utc::now(),
            stats: SnapshotStats::default(),
            root_entry: None,
            retention_reason: vec![],
        }
    }

    #[test]
    fn dedup_key_combines_repo_uid_and_snapshot_id() {
        assert_eq!(
            catalog_dedup_key("repo-uid", "snap-1"),
            ("repo-uid".to_string(), "snap-1".to_string())
        );
        // Same snapshot id under a different repo is a distinct key.
        assert_ne!(
            catalog_dedup_key("repo-a", "snap-1"),
            catalog_dedup_key("repo-b", "snap-1")
        );
    }

    #[test]
    fn only_unseen_snapshots_need_materialization() {
        let listing = vec![entry("s1"), entry("s2"), entry("s3")];
        let mut existing = BTreeSet::new();
        existing.insert(catalog_dedup_key("repo-1", "s1"));
        existing.insert(catalog_dedup_key("repo-1", "s3"));
        let need = needs_materialization("repo-1", &existing, &listing);
        let ids: Vec<&str> = need.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, vec!["s2"], "only the unseen snapshot is materialized");
    }

    #[test]
    fn same_id_under_other_repo_is_not_deduped() {
        let listing = vec![entry("s1")];
        let mut existing = BTreeSet::new();
        // s1 already materialized, but under repo-OTHER.
        existing.insert(catalog_dedup_key("repo-OTHER", "s1"));
        let need = needs_materialization("repo-1", &existing, &listing);
        assert_eq!(need.len(), 1, "different repo UID → still needs its own CR");
    }

    #[test]
    fn nothing_to_do_when_all_present() {
        let listing = vec![entry("s1"), entry("s2")];
        let mut existing = BTreeSet::new();
        existing.insert(catalog_dedup_key("r", "s1"));
        existing.insert(catalog_dedup_key("r", "s2"));
        assert!(needs_materialization("r", &existing, &listing).is_empty());
    }
}
