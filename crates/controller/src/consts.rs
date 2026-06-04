//! Well-known string constants: finalizers, annotations, labels (ADR §4.5).

/// The finalizer every `Backup` carries so the operator can run snapshot
/// cleanup before the CR is removed (ADR §4.5 / SKILL "Snapshot lifecycle =
/// CR lifecycle").
pub const SNAPSHOT_CLEANUP_FINALIZER: &str = "kopiur.home-operations.com/snapshot-cleanup";

/// Repo-offline escape hatch: when present, the finalizer is removed *without*
/// contacting the repository, the snapshot is recorded orphaned, and a
/// `SnapshotOrphaned` event is emitted (ADR §4.5).
pub const SKIP_SNAPSHOT_CLEANUP_ANNOTATION: &str =
    "kopiur.home-operations.com/skip-snapshot-cleanup";

/// Label mirroring a `Backup`'s origin (`scheduled`/`manual`/`discovered`).
pub const ORIGIN_LABEL: &str = "kopiur.home-operations.com/origin";
/// Label keying a discovered `Backup` to its kopia snapshot id (dedup, §2.1).
pub const SNAPSHOT_ID_LABEL: &str = "kopiur.home-operations.com/snapshot-id";
/// Label keying a discovered `Backup` to the owning Repository UID (dedup).
pub const REPOSITORY_UID_LABEL: &str = "kopiur.home-operations.com/repository-uid";
/// Label naming the `BackupConfig` a `Backup` was produced from.
pub const CONFIG_LABEL: &str = "kopiur.home-operations.com/config";

/// The API version string for kopiur CRDs (used in mover `TargetRef`s).
pub const API_VERSION: &str = "kopiur.home-operations.com/v1alpha1";

/// Status condition `type` set on a `Repository`/`ClusterRepository` recording
/// whether a `Maintenance` CR references it (ADR §3.7: maintenance is opt-in and
/// an unmaintained repository never reclaims storage).
pub const MAINTENANCE_CONFIGURED_CONDITION: &str = "MaintenanceConfigured";
/// Event reason + condition reason when no `Maintenance` references the repo.
pub const MAINTENANCE_NOT_CONFIGURED_REASON: &str = "MaintenanceNotConfigured";
/// Condition reason when a `Maintenance` does reference the repo.
pub const MAINTENANCE_CONFIGURED_REASON: &str = "MaintenanceConfigured";
/// `action` for the maintenance-configuration check Event.
pub const CHECK_MAINTENANCE_ACTION: &str = "CheckMaintenance";

/// Status condition `type` recording the outcome of an object-store repository
/// bootstrap Job (connect/create). `True` once the repository is reachable;
/// `False` carries the kopia error class + message so a failure is actionable.
pub const REPOSITORY_BOOTSTRAPPED_CONDITION: &str = "Bootstrapped";
