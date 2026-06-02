//! Well-known string constants: finalizers, annotations, labels (ADR §4.5).

/// The finalizer every `Backup` carries so the operator can run snapshot
/// cleanup before the CR is removed (ADR §4.5 / SKILL "Snapshot lifecycle =
/// CR lifecycle").
pub const SNAPSHOT_CLEANUP_FINALIZER: &str = "kopiur.dev/snapshot-cleanup";

/// Repo-offline escape hatch: when present, the finalizer is removed *without*
/// contacting the repository, the snapshot is recorded orphaned, and a
/// `SnapshotOrphaned` event is emitted (ADR §4.5).
pub const SKIP_SNAPSHOT_CLEANUP_ANNOTATION: &str = "kopiur.dev/skip-snapshot-cleanup";

/// Label mirroring a `Backup`'s origin (`scheduled`/`manual`/`discovered`).
pub const ORIGIN_LABEL: &str = "kopiur.dev/origin";
/// Label keying a discovered `Backup` to its kopia snapshot id (dedup, §2.1).
pub const SNAPSHOT_ID_LABEL: &str = "kopiur.dev/snapshot-id";
/// Label keying a discovered `Backup` to the owning Repository UID (dedup).
pub const REPOSITORY_UID_LABEL: &str = "kopiur.dev/repository-uid";
/// Label naming the `BackupConfig` a `Backup` was produced from.
pub const CONFIG_LABEL: &str = "kopiur.dev/config";

/// The API version string for kopiur CRDs (used in mover `TargetRef`s).
pub const API_VERSION: &str = "kopiur.dev/v1alpha1";
