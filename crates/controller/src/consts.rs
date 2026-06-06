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

/// Standard component label. `maintenance` marks the mover Jobs the `Maintenance`
/// reconciler spawns, so it can enforce single-flight (at most one maintenance
/// Job per repository at a time, G3) via a label selector.
pub const COMPONENT_LABEL: &str = "app.kubernetes.io/component";
/// `COMPONENT_LABEL` value for maintenance mover Jobs.
pub const MAINTENANCE_COMPONENT: &str = "maintenance";
/// Label tying a maintenance Job back to its owning `Maintenance` CR (the
/// single-flight selector is `COMPONENT_LABEL`=maintenance + this = CR name).
pub const MAINTENANCE_INSTANCE_LABEL: &str = "kopiur.home-operations.com/maintenance";
/// Annotation on a maintenance Job recording the scheduled slot it runs (RFC3339;
/// not a valid *label* value because of the colons). Mirrors the upstream
/// `batch.kubernetes.io/cronjob-scheduled-timestamp` (G9).
pub const MAINTENANCE_SLOT_ANNOTATION: &str = "kopiur.home-operations.com/maintenance-slot";

/// Status condition `type` set on a `Repository`/`ClusterRepository` recording
/// whether a `Maintenance` covers it (ADR §3.7). Maintenance is default-managed:
/// `True` once the operator manages one (or an external one exists); `False` only
/// when explicitly disabled or a `ClusterRepository`'s placement is unresolved.
pub const MAINTENANCE_CONFIGURED_CONDITION: &str = "MaintenanceConfigured";
/// Condition reason when a `Maintenance` (managed or external) covers the repo.
pub const MAINTENANCE_CONFIGURED_REASON: &str = "MaintenanceConfigured";
/// `action` for the maintenance-configuration check Event.
pub const CHECK_MAINTENANCE_ACTION: &str = "CheckMaintenance";
/// Condition reason when `spec.maintenance.enabled: false` and no external
/// `Maintenance` covers the repo — a deliberate opt-out, surfaced informationally
/// (no Warning event).
pub const MAINTENANCE_DISABLED_REASON: &str = "MaintenanceDisabled";
/// Event + condition reason when a `ClusterRepository`'s managed `Maintenance`
/// cannot be placed: neither `spec.maintenance.namespace` nor the operator
/// namespace (`KOPIUR_NAMESPACE`) is set. A real misconfiguration, so it warns.
pub const MAINTENANCE_NAMESPACE_UNRESOLVED_REASON: &str = "MaintenanceNamespaceUnresolved";

/// Status condition `type` recording the outcome of an object-store repository
/// bootstrap Job (connect/create). `True` once the repository is reachable;
/// `False` carries the kopia error class + message so a failure is actionable.
pub const REPOSITORY_BOOTSTRAPPED_CONDITION: &str = "Bootstrapped";

/// `activeDeadlineSeconds` on the object-store bootstrap Job. A bootstrap whose
/// pods never schedule (e.g. a missing mover ServiceAccount, an image-pull
/// failure) otherwise never gets a `Failed` condition, so the controller never
/// finalizes and the repository hangs `Initializing` with no Event. The deadline
/// forces the Job terminal-`Failed` so `finalize_*` runs and surfaces a Warning.
/// Sized comfortably under the e2e Event-publish budget (180s).
pub const BOOTSTRAP_JOB_DEADLINE_SECS: i64 = 120;

// A repository connect/create (bootstrap) failure is surfaced as a Warning Event
// whose `reason` is the kopia error class itself (`KopiaErrorClass::as_str`, e.g.
// `AccessDenied`/`PermissionDenied`) so it matches the `Bootstrapped=False`
// condition reason and is machine-readable. Only the Event `action` (the
// remediation hint) is a controller-side constant:

/// `action` for credential-class failures (`AccessDenied`/`AuthFailure`): check
/// the repository credentials Secret and bucket/path grants.
pub const CHECK_CREDENTIALS_ACTION: &str = "CheckCredentials";

/// Machine-readable `reason` (condition + Warning Event) when a bootstrap Job
/// reaches a terminal/failed state but wrote **no** structured result — the mover
/// pod crashed, was evicted, hit its [`BOOTSTRAP_JOB_DEADLINE_SECS`] deadline, or
/// never scheduled (e.g. a missing mover ServiceAccount). Distinct from a kopia
/// error class so the failure mode is not silently conflated with a backend
/// rejection ([`crate::io::BootstrapFailure`]).
pub const BOOTSTRAP_JOB_FAILED_REASON: &str = "BootstrapJobFailed";

/// `Backup`/`Restore` condition surfaced when the mover Job's credential Secret is
/// absent from the workload namespace — `False` carries the actionable message
/// (which Secret, which namespace, why, and how to fix). ADR §4.12.
pub const CREDENTIALS_AVAILABLE_CONDITION: &str = "CredentialsAvailable";
/// `reason`/Event reason for [`CREDENTIALS_AVAILABLE_CONDITION`] = `False`.
pub const MISSING_CREDENTIALS_REASON: &str = "MissingCredentialsSecret";

/// Namespace annotation a cluster admin sets to allow elevated (root/privileged)
/// movers in that namespace (ADR §4.11/§G16). Without it, a `BackupConfig` whose
/// `spec.mover` requests privilege is refused — a tenant could otherwise reuse the
/// minted mover ServiceAccount at that privilege. Mirrors VolSync's
/// `volsync.backube/privileged-movers`.
pub const PRIVILEGED_MOVERS_ANNOTATION: &str = "kopiur.home-operations.com/privileged-movers";
/// `Backup` condition surfaced when a privileged mover is requested in a namespace
/// that has not opted in — `False` carries the actionable message.
pub const MOVER_PERMITTED_CONDITION: &str = "MoverPermitted";
/// `reason`/Event reason for [`MOVER_PERMITTED_CONDITION`] = `False`.
pub const PRIVILEGED_MOVER_NOT_PERMITTED_REASON: &str = "PrivilegedMoverNotPermitted";
/// Event `action` (remediation hint) for a refused privileged mover.
pub const ALLOW_PRIVILEGED_MOVER_ACTION: &str = "AnnotateNamespaceForPrivilegedMovers";
/// `action` for a `PermissionDenied` failure: make the repository path/PVC
/// writable by the operator's UID.
pub const CHECK_PERMISSIONS_ACTION: &str = "CheckPermissions";
/// `action` for any other backend failure: check the backend configuration.
pub const CHECK_BACKEND_ACTION: &str = "CheckBackend";
