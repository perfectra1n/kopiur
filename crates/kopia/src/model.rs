//! Typed models for kopia `--json` output (kopia 0.23).
//!
//! These structs are modeled against the *actual* JSON kopia emits, captured by
//! round-tripping a filesystem repository. Field names match kopia's keys
//! exactly via `#[serde(rename_all = "camelCase")]` (plus explicit `rename`s
//! where kopia diverges, e.g. `uniqueIDHex`). None of these use
//! `deny_unknown_fields`: kopia adds fields across releases and we must tolerate
//! them. Times are `chrono::DateTime<Utc>`.
//!
//! Note on stdout vs stderr: kopia prints its progress (`Snapshotting ...`,
//! `Restored N files`) to **stderr** and the machine-readable `--json` result
//! to **stdout**. The client parses stdout only.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Kopia's snapshot identity triple: `userName@host:path`. Present on both
/// snapshot-create results and snapshot-list entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotSource {
    /// The kopia "host" component of identity.
    pub host: String,
    /// The kopia "user" component of identity. kopia's JSON key is `userName`.
    pub user_name: String,
    /// The absolute source path that was snapshotted.
    pub path: String,
}

impl SnapshotSource {
    /// Render kopia's canonical `user@host:path` identity string.
    pub fn identity(&self) -> String {
        format!("{}@{}:{}", self.user_name, self.host, self.path)
    }
}

/// Directory summary embedded under a root entry (`summ`). Carries the
/// aggregate counts kopia computed while walking the tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DirSummary {
    /// Total logical size in bytes.
    #[serde(default)]
    pub size: u64,
    /// Number of files.
    #[serde(default)]
    pub files: u64,
    /// Number of symlinks.
    #[serde(default)]
    pub symlinks: u64,
    /// Number of directories.
    #[serde(default)]
    pub dirs: u64,
    /// Newest mtime found in the tree.
    #[serde(default, rename = "maxTime")]
    pub max_time: Option<DateTime<Utc>>,
    /// Count of entries that failed during the walk.
    #[serde(default, rename = "numFailed")]
    pub num_failed: u64,
}

/// The `rootEntry` of a snapshot — the top directory object plus its summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootEntry {
    /// Entry name (basename of the snapshotted path).
    #[serde(default)]
    pub name: String,
    /// Entry type, e.g. "d" for directory.
    #[serde(default, rename = "type")]
    pub entry_type: String,
    /// The kopia object id of the root (the `k...` handle).
    #[serde(default)]
    pub obj: String,
    /// Aggregate directory summary. Optional because non-directory roots omit
    /// it.
    #[serde(default, rename = "summ")]
    pub summary: Option<DirSummary>,
}

/// Result of `kopia snapshot create <path> --json`.
///
/// kopia emits a single JSON object on stdout. The aggregate counts live under
/// `rootEntry.summ`; the create result itself does not carry a top-level
/// `stats` block (that appears on snapshot-list entries). We surface
/// convenience accessors for the common stats the mover reports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotCreateResult {
    /// The new snapshot's manifest id.
    pub id: String,
    /// Identity of the snapshot.
    pub source: SnapshotSource,
    /// Free-form description (usually empty).
    #[serde(default)]
    pub description: String,
    /// When the snapshot started.
    pub start_time: DateTime<Utc>,
    /// When the snapshot finished.
    pub end_time: DateTime<Utc>,
    /// Root directory entry with its summary.
    #[serde(default)]
    pub root_entry: Option<RootEntry>,
}

impl SnapshotCreateResult {
    /// Total logical bytes in the snapshot, from the root summary (0 if absent).
    pub fn total_bytes(&self) -> u64 {
        self.root_entry
            .as_ref()
            .and_then(|r| r.summary.as_ref())
            .map(|s| s.size)
            .unwrap_or(0)
    }

    /// Total file count in the snapshot, from the root summary (0 if absent).
    pub fn file_count(&self) -> u64 {
        self.root_entry
            .as_ref()
            .and_then(|r| r.summary.as_ref())
            .map(|s| s.files)
            .unwrap_or(0)
    }

    /// Number of entries that failed during the walk (0 if absent).
    pub fn error_count(&self) -> u64 {
        self.root_entry
            .as_ref()
            .and_then(|r| r.summary.as_ref())
            .map(|s| s.num_failed)
            .unwrap_or(0)
    }
}

/// The `stats` block present on each `kopia snapshot list --json` entry. These
/// are the new/modified/unchanged-style counters.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotStats {
    /// Total logical size of all included files.
    #[serde(default)]
    pub total_size: u64,
    /// Size excluded by policy.
    #[serde(default)]
    pub excluded_total_size: u64,
    /// Number of files included.
    #[serde(default)]
    pub file_count: u64,
    /// Files served from cache (unchanged since the prior snapshot).
    #[serde(default)]
    pub cached_files: u64,
    /// Files re-read because they were new or modified.
    #[serde(default)]
    pub non_cached_files: u64,
    /// Number of directories.
    #[serde(default)]
    pub dir_count: u64,
    /// Files excluded by policy.
    #[serde(default)]
    pub excluded_file_count: u64,
    /// Directories excluded by policy.
    #[serde(default)]
    pub excluded_dir_count: u64,
    /// Errors that were ignored (per ignore-error policy).
    #[serde(default)]
    pub ignored_error_count: u64,
    /// Hard errors encountered.
    #[serde(default)]
    pub error_count: u64,
}

/// One entry from `kopia snapshot list --json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotListEntry {
    /// The snapshot manifest id.
    pub id: String,
    /// Identity of the snapshot.
    pub source: SnapshotSource,
    /// Free-form description.
    #[serde(default)]
    pub description: String,
    /// When the snapshot started.
    pub start_time: DateTime<Utc>,
    /// When the snapshot finished.
    pub end_time: DateTime<Utc>,
    /// Per-snapshot statistics.
    #[serde(default)]
    pub stats: SnapshotStats,
    /// Root directory entry.
    #[serde(default)]
    pub root_entry: Option<RootEntry>,
    /// Why this snapshot is being retained (kopia GFS reasons such as
    /// `latest-1`, `daily-1`). Empty for snapshots outside any retention class.
    #[serde(default)]
    pub retention_reason: Vec<String>,
}

/// Client identity options reported by `kopia repository status`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientOptions {
    /// The configured hostname for this client.
    #[serde(default)]
    pub hostname: String,
    /// The configured username for this client.
    #[serde(default)]
    pub username: String,
    /// Human-readable repository description.
    #[serde(default)]
    pub description: String,
    /// Whether snapshot actions are enabled.
    #[serde(default)]
    pub enable_actions: bool,
}

/// Storage backend block from `kopia repository status`. `config` is left as a
/// raw JSON value because its shape is backend-specific (filesystem path vs S3
/// bucket/endpoint vs ...).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageInfo {
    /// Backend type, e.g. "filesystem", "s3", "gcs".
    #[serde(default, rename = "type")]
    pub storage_type: String,
    /// Backend-specific configuration, opaque here.
    #[serde(default)]
    pub config: serde_json::Value,
}

/// Content format block from `kopia repository status`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContentFormat {
    /// Hash algorithm, e.g. "BLAKE2B-256-128".
    #[serde(default)]
    pub hash: String,
    /// Encryption algorithm, e.g. "AES256-GCM-HMAC-SHA256".
    #[serde(default)]
    pub encryption: String,
    /// Repository format version.
    #[serde(default)]
    pub version: u32,
}

/// Result of `kopia repository status --json`.
///
/// The repository's stable identity is `uniqueIDHex` (kopia's JSON key, hence
/// the explicit rename). We keep the high-value fields typed and leave the rest
/// (volume capacity, object format, epoch params) for future expansion without
/// breaking on unknown fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryStatus {
    /// Path to the local repository config file.
    #[serde(default)]
    pub config_file: String,
    /// The repository's stable unique id. kopia's key is `uniqueIDHex`.
    #[serde(default, rename = "uniqueIDHex")]
    pub unique_id_hex: String,
    /// Client identity options.
    pub client_options: ClientOptions,
    /// Storage backend info.
    pub storage: StorageInfo,
    /// Content format (hash/encryption/version).
    pub content_format: ContentFormat,
}

/// A maintenance cadence block (`quick` / `full`) from `maintenance info`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceCadence {
    /// Whether this maintenance class is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Interval between runs, in nanoseconds (kopia's Go `time.Duration`).
    #[serde(default)]
    pub interval: i64,
}

/// The `schedule` block: when maintenance next runs. The detailed per-task
/// `runs` history is left as a raw value (its shape is large and unstable).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceSchedule {
    /// Next scheduled full maintenance, if known.
    #[serde(default)]
    pub next_full_maintenance: Option<DateTime<Utc>>,
    /// Next scheduled quick maintenance, if known.
    #[serde(default)]
    pub next_quick_maintenance: Option<DateTime<Utc>>,
}

/// Result of `kopia maintenance info --json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceInfo {
    /// The `user@host` that owns the maintenance lease.
    #[serde(default)]
    pub owner: String,
    /// Quick maintenance cadence.
    pub quick: MaintenanceCadence,
    /// Full maintenance cadence.
    pub full: MaintenanceCadence,
    /// Schedule with next-run timestamps.
    #[serde(default)]
    pub schedule: Option<MaintenanceSchedule>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_source_identity() {
        let s = SnapshotSource {
            host: "h".into(),
            user_name: "u".into(),
            path: "/p".into(),
        };
        assert_eq!(s.identity(), "u@h:/p");
    }
}
