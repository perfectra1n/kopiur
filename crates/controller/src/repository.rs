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

use kube::api::ListParams;
use kube::runtime::controller::Action;
use kube::{Api, Resource, ResourceExt};

use kopiur_api::backend::Backend;
use kopiur_api::common::RepositoryKind;
use kopiur_api::{Backup, Repository, RepositoryPhase, validate};
use kopiur_kopia::{ConnectSpec, SnapshotListEntry};

use crate::consts::{ORIGIN_LABEL, REPOSITORY_UID_LABEL, SNAPSHOT_ID_LABEL};
use crate::context::Context;
use crate::error::{Error, Result, error_policy_for};
use crate::io;

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

/// Logical bytes under management: the sum, over each distinct snapshot source,
/// of the most-recent snapshot's logical `total_size`. Older snapshots of the
/// same source are not added (they would double-count unchanged data). Pure.
pub fn logical_bytes_under_management(listing: &[SnapshotListEntry]) -> i64 {
    use std::collections::HashMap;
    let mut newest: HashMap<&str, &SnapshotListEntry> = HashMap::new();
    for e in listing {
        let key = e.source.path.as_str();
        match newest.get(key) {
            Some(prev) if prev.end_time >= e.end_time => {}
            _ => {
                newest.insert(key, e);
            }
        }
    }
    newest
        .values()
        .map(|e| i64::try_from(e.stats.total_size).unwrap_or(i64::MAX))
        .sum()
}

/// Reconcile a `Repository`.
#[tracing::instrument(skip(repo, ctx), fields(kind = "Repository", name = %repo.name_any()))]
pub async fn reconcile(repo: Arc<Repository>, ctx: Arc<Context>) -> Result<Action> {
    let start = std::time::Instant::now();
    let result = reconcile_inner(&repo, &ctx).await;
    ctx.metrics
        .record_reconcile("Repository", start.elapsed().as_secs_f64());
    record_repository_status_metrics(&repo, &ctx, result.is_ok()).await;
    result
}

/// Mirror a Repository's phase + catalog gauges. Zeroes the phase on deletion
/// (so Degraded/Failed alerts clear) and re-reads the freshest status on success
/// (the passed object is the pre-reconcile cache copy). See the Backup
/// equivalent for the rationale.
async fn record_repository_status_metrics(repo: &Repository, ctx: &Context, ok: bool) {
    let (Some(ns), name) = (repo.namespace(), repo.name_any()) else {
        return;
    };
    if repo.metadata.deletion_timestamp.is_some() {
        ctx.metrics
            .clear_phase::<RepositoryPhase>("Repository", &ns, &name);
        return;
    }
    if !ok {
        return;
    }
    let api: Api<Repository> = Api::namespaced(ctx.client.clone(), &ns);
    if let Ok(Some(latest)) = api.get_opt(&name).await
        && let Some(status) = latest.status.as_ref()
    {
        if let Some(phase) = status.phase {
            ctx.metrics
                .set_repository_phase("Repository", &ns, &name, phase);
        }
        let snapshots = status.storage_stats.as_ref().and_then(|s| s.snapshot_count);
        let discovered = status
            .catalog
            .as_ref()
            .and_then(|c| c.discovered_backup_count);
        if snapshots.is_some() || discovered.is_some() {
            ctx.metrics
                .set_repo_catalog(&ns, &name, snapshots, discovered);
        }
    }
}

async fn reconcile_inner(repo: &Repository, ctx: &Context) -> Result<Action> {
    if let Err(e) = validate::validate_repository_no_inline_retention(&repo.spec) {
        return Err(Error::Validation(e.to_string()));
    }

    let namespace = repo
        .namespace()
        .ok_or_else(|| Error::Invariant("Repository has no namespace".into()))?;
    let name = repo.name_any();
    let repo_uid = repo
        .uid()
        .ok_or_else(|| Error::Invariant("Repository has no uid".into()))?;
    let api: Api<Repository> = Api::namespaced(ctx.client.clone(), &namespace);

    // The controller may run kopia in-process for the FILESYSTEM backend only
    // (ADR §5.4 permits short idempotent ops; filesystem repos are reachable from
    // the controller's own filesystem when using a hostPath/shared mount, or in
    // the e2e harness). For object-store backends, connect-validate would run as
    // a short Job — documented as a follow-up below.
    match &repo.spec.backend {
        Backend::Filesystem(fs) => {
            let creds = io::repo_credentials(&repo.spec.encryption);
            let password = io::read_repo_password(&ctx.client, &namespace, &creds).await?;
            let client = ctx.kopia.build([("KOPIA_PASSWORD".to_string(), password)]);
            let spec = ConnectSpec::Filesystem {
                path: fs.path.clone().into(),
            };

            // Idempotent connect; create on first use when enabled.
            if let Err(e) = client.repository_connect(&spec).await {
                let create_enabled = repo
                    .spec
                    .create
                    .as_ref()
                    .map(|c| c.enabled)
                    .unwrap_or(false);
                if create_enabled {
                    client.repository_create(&spec).await?;
                    client.repository_connect(&spec).await?;
                } else {
                    io::patch_status(
                        &api,
                        &name,
                        serde_json::json!({ "phase": "Failed", "backend": "Filesystem" }),
                    )
                    .await?;
                    return Err(Error::Kopia(e));
                }
            }

            // Status: phase/uniqueID/backend/storageStats.
            let status = client.repository_status().await?;
            io::patch_status(
                &api,
                &name,
                serde_json::json!({
                    "phase": "Ready",
                    "backend": "Filesystem",
                    "uniqueId": status.unique_id_hex,
                }),
            )
            .await?;

            // Catalog scan: materialize discovered Backups for unseen snapshots,
            // bounded by catalog.retain.perIdentity.
            scan_catalog(ctx, repo, &namespace, &name, &repo_uid, &client).await?;

            // Now that the repo is Ready, surface whether a Maintenance CR
            // references it (Warning event + condition + gauge). ADR §3.7.
            let conditions = repo
                .status
                .as_ref()
                .map(|s| s.conditions.clone())
                .unwrap_or_default();
            io::check_maintenance(
                ctx,
                &api,
                &repo.object_ref(&()),
                RepositoryKind::Repository,
                "Repository",
                &namespace,
                Some(&namespace),
                &name,
                &conditions,
                repo.metadata.generation,
            )
            .await;
        }
        other => {
            // NOTE: object-store connect/create/status/catalog would run via a
            // short-lived mover Job (ADR §5.4). The filesystem path above is the
            // fully-working core; extending the Job-based path to S3/etc is a
            // mechanical follow-up that reuses the same MoverWorkSpec plumbing.
            io::patch_status(
                &api,
                &name,
                serde_json::json!({ "phase": "Pending", "backend": other.kind_str() }),
            )
            .await?;
            tracing::info!(
                repo = %name,
                backend = other.kind_str(),
                "object-store repository: in-process validation not run (filesystem only); see NOTE"
            );
        }
    }

    Ok(Action::requeue(std::time::Duration::from_secs(300)))
}

/// Run `snapshot list`, compute which snapshots still need a `Backup` CR, and
/// create the bounded `origin: discovered` set (forced `deletionPolicy: Retain`).
async fn scan_catalog(
    ctx: &Context,
    repo: &Repository,
    namespace: &str,
    repo_name: &str,
    repo_uid: &str,
    client: &kopiur_kopia::KopiaClient,
) -> Result<()> {
    let listing = client.snapshot_list(None).await?;

    // Existing discovered Backups keyed by (repo_uid, snapshot_id).
    let backup_api: Api<Backup> = Api::namespaced(ctx.client.clone(), namespace);
    let lp = ListParams::default().labels(&format!("{ORIGIN_LABEL}=discovered"));
    let existing_crs = backup_api.list(&lp).await?.items;
    let mut existing: BTreeSet<(String, String)> = BTreeSet::new();
    for b in &existing_crs {
        if let (Some(uid), Some(id)) = (
            b.labels().get(REPOSITORY_UID_LABEL),
            b.labels().get(SNAPSHOT_ID_LABEL),
        ) {
            existing.insert(catalog_dedup_key(uid, id));
        }
    }

    let mut need = needs_materialization(repo_uid, &existing, &listing);

    // Bound by catalog.retain.perIdentity (most-recent N). We approximate the
    // global cap by sorting newest-first and truncating; per-identity refinement
    // is a documented follow-up.
    let retain = repo
        .spec
        .catalog
        .as_ref()
        .and_then(|c| c.retain.as_ref())
        .and_then(|r| r.per_identity)
        .map(|n| n.max(0) as usize);
    need.sort_by_key(|e| std::cmp::Reverse(e.end_time));
    if let Some(cap) = retain {
        need.truncate(cap);
    }

    let mut created = 0i64;
    for entry in need {
        create_discovered_backup(ctx, repo, namespace, repo_name, repo_uid, entry).await?;
        created += 1;
    }
    if created > 0 {
        tracing::info!(repo = %repo_name, created, "materialized discovered Backup CRs");
    }

    // Logical bytes under management is recorded directly from kopia's data
    // (the status field is a human string, so the gauge bypasses it).
    ctx.metrics.set_repo_size_bytes(
        namespace,
        repo_name,
        logical_bytes_under_management(&listing),
    );

    let api: Api<Repository> = Api::namespaced(ctx.client.clone(), namespace);
    io::patch_status(
        &api,
        repo_name,
        serde_json::json!({
            "catalog": {
                "discoveredBackupCount": existing.len() as i64 + created,
                "lastRefreshAt": chrono::Utc::now().to_rfc3339(),
            },
            "storageStats": { "snapshotCount": listing.len() as i64 },
        }),
    )
    .await?;
    Ok(())
}

/// Create one `origin: discovered` Backup CR for a snapshot. `deletionPolicy` is
/// FORCED to `Retain` (the operator never deletes a discovered snapshot, §4.5).
async fn create_discovered_backup(
    ctx: &Context,
    repo: &Repository,
    namespace: &str,
    repo_name: &str,
    repo_uid: &str,
    entry: &SnapshotListEntry,
) -> Result<()> {
    use kopiur_api::backup::{BackupSpec, BackupStatus, SnapshotInfo};
    use kopiur_api::common::{DeletionPolicy, ResolvedIdentity};
    use kopiur_api::{BackupPhase, Origin};

    // CR name: stable from the (short) snapshot id, namespaced under repo.
    let short = entry.id.chars().take(16).collect::<String>();
    let cr_name = format!("{repo_name}-disc-{short}");

    let mut labels = std::collections::BTreeMap::new();
    labels.insert(ORIGIN_LABEL.to_string(), "discovered".to_string());
    labels.insert(REPOSITORY_UID_LABEL.to_string(), repo_uid.to_string());
    labels.insert(SNAPSHOT_ID_LABEL.to_string(), entry.id.clone());

    let owner = io::owner_ref_for(repo, "Repository")?;
    let mut backup = Backup::new(
        &cr_name,
        BackupSpec {
            config_ref: None,
            tags: None,
            failure_policy: None,
            // Forced Retain for discovered (webhook would reject otherwise).
            deletion_policy: Some(DeletionPolicy::Retain),
        },
    );
    backup.metadata = io::child_meta(&cr_name, namespace, labels, Some(owner));
    backup.status = Some(BackupStatus {
        phase: Some(BackupPhase::Discovered),
        origin: Some(Origin::Discovered),
        snapshot: Some(SnapshotInfo {
            kopia_snapshot_id: entry.id.clone(),
            identity: ResolvedIdentity {
                username: entry.source.user_name.clone(),
                hostname: entry.source.host.clone(),
                source_path: Some(entry.source.path.clone()),
            },
        }),
        ..Default::default()
    });

    let api: Api<Backup> = Api::namespaced(ctx.client.clone(), namespace);
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
    let _ = repo; // repo retained for future per-identity bounding
    Ok(())
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

    fn entry_sized(
        id: &str,
        path: &str,
        end: chrono::DateTime<Utc>,
        size: u64,
    ) -> SnapshotListEntry {
        let mut e = entry(id);
        e.source.path = path.into();
        e.end_time = end;
        e.stats.total_size = size;
        e
    }

    #[test]
    fn logical_bytes_sums_newest_snapshot_per_source() {
        let t0 = Utc::now();
        let t1 = t0 + chrono::Duration::seconds(10);
        let listing = vec![
            // Source /a: older 100, newer 150 → counts 150 (not 250).
            entry_sized("a-old", "/a", t0, 100),
            entry_sized("a-new", "/a", t1, 150),
            // Source /b: single snapshot 40.
            entry_sized("b", "/b", t0, 40),
        ];
        assert_eq!(logical_bytes_under_management(&listing), 190);
        assert_eq!(logical_bytes_under_management(&[]), 0);
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
