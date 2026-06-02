//! Unit tests parsing the captured kopia 0.23 fixture JSON into the typed
//! models. These assert the key fields the operator depends on, and prove the
//! `camelCase` / explicit-rename mapping matches kopia's actual keys.

use kopiur_kopia::{MaintenanceInfo, RepositoryStatus, SnapshotCreateResult, SnapshotListEntry};

const SNAPSHOT_CREATE: &str = include_str!("fixtures/snapshot_create.json");
const SNAPSHOT_LIST: &str = include_str!("fixtures/snapshot_list.json");
const REPOSITORY_STATUS: &str = include_str!("fixtures/repository_status.json");
const MAINTENANCE_INFO: &str = include_str!("fixtures/maintenance_info.json");

#[test]
fn parse_snapshot_create() {
    let r: SnapshotCreateResult = serde_json::from_str(SNAPSHOT_CREATE).unwrap();
    assert_eq!(r.id, "edf6ef74ec18dffc79e26907e0c3c7fc");
    assert_eq!(r.source.user_name, "root");
    assert_eq!(r.source.host, "desktop-8emkv7q");
    assert_eq!(r.source.path, "/tmp/claude-0/tmp.Zz62PJwR0o");
    assert_eq!(
        r.source.identity(),
        "root@desktop-8emkv7q:/tmp/claude-0/tmp.Zz62PJwR0o"
    );
    // Stats come from rootEntry.summ on the create result.
    assert_eq!(r.total_bytes(), 12);
    assert_eq!(r.file_count(), 2);
    assert_eq!(r.error_count(), 0);
    assert!(r.end_time >= r.start_time);
}

#[test]
fn parse_snapshot_list() {
    let entries: Vec<SnapshotListEntry> = serde_json::from_str(SNAPSHOT_LIST).unwrap();
    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert_eq!(e.id, "edf6ef74ec18dffc79e26907e0c3c7fc");
    assert_eq!(e.source.path, "/tmp/claude-0/tmp.Zz62PJwR0o");
    // The `stats` block on list entries.
    assert_eq!(e.stats.total_size, 12);
    assert_eq!(e.stats.file_count, 2);
    assert_eq!(e.stats.non_cached_files, 2);
    assert_eq!(e.stats.cached_files, 0);
    assert_eq!(e.stats.dir_count, 2);
    assert_eq!(e.stats.error_count, 0);
    // GFS retention reasons.
    assert!(e.retention_reason.contains(&"latest-1".to_string()));
    assert!(e.retention_reason.contains(&"daily-1".to_string()));
}

#[test]
fn parse_repository_status() {
    let s: RepositoryStatus = serde_json::from_str(REPOSITORY_STATUS).unwrap();
    // uniqueIDHex is the stable repo identity (explicit rename).
    assert_eq!(
        s.unique_id_hex,
        "e9f0365501231565390cc1327cb9665c80dd54ff41ce0419b0ccaef69a24f021"
    );
    assert_eq!(s.client_options.hostname, "desktop-8emkv7q");
    assert_eq!(s.client_options.username, "root");
    assert_eq!(s.storage.storage_type, "filesystem");
    assert_eq!(s.content_format.hash, "BLAKE2B-256-128");
    assert_eq!(s.content_format.encryption, "AES256-GCM-HMAC-SHA256");
    assert_eq!(s.content_format.version, 3);
    // Backend config stays opaque but is preserved.
    assert_eq!(
        s.storage.config.get("path").and_then(|v| v.as_str()),
        Some("/tmp/claude-0/tmp.xo1UiqvExG")
    );
}

#[test]
fn parse_maintenance_info() {
    let m: MaintenanceInfo = serde_json::from_str(MAINTENANCE_INFO).unwrap();
    assert_eq!(m.owner, "root@desktop-8emkv7q");
    assert!(m.quick.enabled);
    assert!(m.full.enabled);
    assert_eq!(m.quick.interval, 3_600_000_000_000);
    assert_eq!(m.full.interval, 86_400_000_000_000);
    let sched = m.schedule.expect("schedule present");
    assert!(sched.next_full_maintenance.is_some());
    assert!(sched.next_quick_maintenance.is_some());
}

#[test]
fn tolerates_unknown_fields() {
    // Kopia adds fields across releases; we must not reject them.
    let json = r#"{
        "id":"x","source":{"host":"h","userName":"u","path":"/p","futureField":1},
        "startTime":"2026-06-02T03:13:59Z","endTime":"2026-06-02T03:14:00Z",
        "brandNewTopLevelField":true
    }"#;
    let r: SnapshotCreateResult = serde_json::from_str(json).unwrap();
    assert_eq!(r.id, "x");
    assert_eq!(r.total_bytes(), 0); // no rootEntry → defaults
}
