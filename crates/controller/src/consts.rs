//! Controller-internal string constants: event reasons/actions, single-flight
//! labels, deadlines (ADR §4.5).
//!
//! The *wire-contract* strings (finalizer, origin/config/dedup labels,
//! `managed-by`, kstatus condition types, API version) live in
//! [`kopiur_api::consts`] so external tooling shares one definition; they are
//! re-exported here so controller call sites keep their existing import paths.

pub use kopiur_api::consts::{
    API_VERSION, CONFIG_LABEL, MAINTENANCE_CONFIGURED_CONDITION, MANAGED_BY_LABEL,
    MANAGED_BY_VALUE, OP_LABEL, OP_RESTORE, OP_RESTORE_TARGET, ORIGIN_LABEL, READY_CONDITION,
    RECONCILING_CONDITION, REPOSITORY_UID_LABEL, RUN_MODE_ANNOTATION, RUN_REQUESTED_ANNOTATION,
    SCHEDULE_LABEL, SKIP_SNAPSHOT_CLEANUP_ANNOTATION, SNAPSHOT_CLEANUP_FINALIZER,
    SNAPSHOT_ID_LABEL, STALLED_CONDITION,
};

/// `Snapshot` condition recording whether its repository accepts writes (§11). Set
/// `False` (with [`REPOSITORY_READ_ONLY_REASON`]) when a backup is refused because
/// the repository is `mode: ReadOnly`.
pub const REPOSITORY_WRITABLE_CONDITION: &str = "RepositoryWritable";
/// `reason`/Event reason when a backup or maintenance is refused on a `ReadOnly`
/// repository (ADR-0005 §11).
pub const REPOSITORY_READ_ONLY_REASON: &str = "RepositoryReadOnly";

/// In-container mount path for an inline-NFS backup *source* whose server-side
/// export is the NFSv4 pseudo-root (`/`). The export's server path and the
/// container mount path are independent; reusing `/` as the mount path would
/// mount the volume over the container rootfs and the pod fails to start
/// (`error mounting ... to rootfs at "/": mountpoint ... is on the top of
/// rootfs`). kopia snapshots whatever is mounted here.
pub const NFS_SOURCE_MOUNT_PATH: &str = "/nfs";

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

/// `COMPONENT_LABEL` value for verification mover Jobs (ADR-0005 §4).
pub const VERIFY_COMPONENT: &str = "verify";
/// Label tying a verification Job back to its owning `SnapshotPolicy` (single-flight
/// selector: `COMPONENT_LABEL`=verify + this = policy name).
pub const VERIFY_INSTANCE_LABEL: &str = "kopiur.home-operations.com/verify";
/// Annotation on a verification Job recording the scheduled slot it runs (RFC3339).
pub const VERIFY_SLOT_ANNOTATION: &str = "kopiur.home-operations.com/verify-slot";

/// `COMPONENT_LABEL` value for replication mover Jobs (ADR-0005 §13(d)).
pub const REPLICATION_COMPONENT: &str = "replication";
/// Label tying a replication Job back to its owning `RepositoryReplication`.
pub const REPLICATION_INSTANCE_LABEL: &str = "kopiur.home-operations.com/replication";
/// Annotation on a replication Job recording the scheduled slot it runs (RFC3339).
pub const REPLICATION_SLOT_ANNOTATION: &str = "kopiur.home-operations.com/replication-slot";

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

/// `Snapshot`/`Restore` condition surfaced when the mover Job's credential Secret is
/// absent from the workload namespace — `False` carries the actionable message
/// (which Secret, which namespace, why, and how to fix). ADR §4.12.
pub const CREDENTIALS_AVAILABLE_CONDITION: &str = "CredentialsAvailable";
/// `reason`/Event reason for [`CREDENTIALS_AVAILABLE_CONDITION`] = `False`.
pub const MISSING_CREDENTIALS_REASON: &str = "MissingCredentialsSecret";
/// `reason` for [`CREDENTIALS_AVAILABLE_CONDITION`] = `True` when the operator
/// supplied the credential Secret(s) itself via projection (opt-in
/// `spec.credentialProjection`), rather than the user pre-creating them.
pub const CREDENTIALS_PROJECTED_REASON: &str = "Projected";
/// Annotation stamped on a projected credential Secret recording its source
/// (`<namespace>/<name>`), so an operator can see a copy is kopiur-managed and
/// where it came from. Paired with the `app.kubernetes.io/managed-by=kopiur` +
/// `app.kubernetes.io/component=credentials` labels.
pub const PROJECTED_FROM_ANNOTATION: &str = "kopiur.home-operations.com/projected-from";

/// Namespace annotation a cluster admin sets to allow elevated (root/privileged)
/// movers in that namespace (ADR §4.11/§G16). Without it, a `SnapshotPolicy` whose
/// `spec.mover` requests privilege is refused — a tenant could otherwise reuse the
/// minted mover ServiceAccount at that privilege. Mirrors VolSync's
/// `volsync.backube/privileged-movers`.
pub const PRIVILEGED_MOVERS_ANNOTATION: &str = "kopiur.home-operations.com/privileged-movers";
/// `Snapshot` condition surfaced when a privileged mover is requested in a namespace
/// that has not opted in — `False` carries the actionable message.
pub const MOVER_PERMITTED_CONDITION: &str = "MoverPermitted";
/// `reason`/Event reason for [`MOVER_PERMITTED_CONDITION`] = `False`.
pub const PRIVILEGED_MOVER_NOT_PERMITTED_REASON: &str = "PrivilegedMoverNotPermitted";
/// Event `action` (remediation hint) for a refused privileged mover.
pub const ALLOW_PRIVILEGED_MOVER_ACTION: &str = "AnnotateNamespaceForPrivilegedMovers";
/// `Snapshot` condition for CSI source staging (`copyMethod: Snapshot`/`Clone`,
/// ADR §3.3): `True` once the staged VolumeSnapshot/PVC is ready for the mover;
/// `False` while waiting (reason [`STAGING_WAITING_REASON`]) or on a preflight
/// failure (the `io::staging::REASON_*` tokens: stack/class missing, snapshot error).
pub const SOURCE_STAGED_CONDITION: &str = "SourceStaged";
/// `reason` for [`SOURCE_STAGED_CONDITION`] = `True`.
pub const SOURCE_STAGED_REASON: &str = "SourceStaged";
/// `reason` for [`SOURCE_STAGED_CONDITION`] = `False` while the VolumeSnapshot is
/// still becoming `readyToUse` (a transient, requeued wait — not a failure).
pub const STAGING_WAITING_REASON: &str = "WaitingForVolumeSnapshot";
/// Event `action` (remediation hint) for a staging preflight failure: install the
/// CSI snapshot stack / VolumeSnapshotClass, or set `copyMethod: Direct`.
pub const FIX_SNAPSHOT_STACK_ACTION: &str = "InstallSnapshotStackOrUseDirect";
/// `Snapshot` condition for `spec.hooks` execution (ADR §4.8) — `False` carries
/// the failing hook's index, form, and actionable cause.
pub const HOOKS_SUCCEEDED_CONDITION: &str = "HooksSucceeded";
/// Event `action` (remediation hint) for an aborting hook failure.
pub const FIX_HOOK_ACTION: &str = "FixHookOrSetContinueOnFailure";
/// `action` for a `PermissionDenied` failure: make the repository path/PVC
/// writable by the operator's UID.
pub const CHECK_PERMISSIONS_ACTION: &str = "CheckPermissions";
/// `action` for any other backend failure: check the backend configuration.
pub const CHECK_BACKEND_ACTION: &str = "CheckBackend";

/// Machine-readable `reason` (condition + Warning Event) when a bootstrap connect
/// found **no** repository at the backend and `spec.create.enabled` is `false`, so
/// kopiur declined to initialize one. Distinct from a kopia error class so the
/// "just needs `create.enabled: true`" case is never conflated with a real backend
/// `NotFound` ([`crate::io::BootstrapFailure`]).
pub const REPOSITORY_NOT_INITIALIZED_REASON: &str = "RepositoryNotInitialized";
/// `action` (remediation hint) for [`REPOSITORY_NOT_INITIALIZED_REASON`]: enable
/// repository creation (or point at an existing repository).
pub const ENABLE_CREATE_ACTION: &str = "EnableRepositoryCreate";

// Every reconcile error is surfaced as a Warning Event on the failing object
// (via `error_policy_for` → `io::reconcile_failure_event`), so a failure is
// visible in `kubectl get events`/`describe` for **every** CRD kind, not only
// the ones with bespoke in-reconcile publishes. A kopia failure reuses the
// kopia class as its `reason` (see `backend_failure_event`); the non-kopia
// `Error` variants get the reasons/actions below:

/// Event `reason` when a reconcile failed on a Kubernetes API call.
pub const KUBE_API_ERROR_REASON: &str = "KubeApiError";
/// `action` for a failed Kubernetes API call: check API-server health and the
/// controller's RBAC.
pub const CHECK_API_SERVER_ACTION: &str = "CheckApiServer";
/// Event `reason` when defensive re-validation rejected the object's spec.
pub const INVALID_SPEC_REASON: &str = "InvalidSpec";
/// `action` for a spec that failed validation: the user must fix the spec.
pub const FIX_SPEC_ACTION: &str = "FixSpec";
/// Event `reason` when a referenced object (Repository, SnapshotPolicy, …) was
/// not found.
pub const MISSING_DEPENDENCY_REASON: &str = "MissingDependency";
/// `action` for a missing dependency: create it or fix the reference.
pub const CHECK_REFERENCES_ACTION: &str = "CheckReferences";
/// Event `reason` when JSON (de)serialization of a spec/status failed.
pub const SERIALIZATION_FAILED_REASON: &str = "SerializationFailed";
/// `action` for failures that indicate a kopiur bug (serialization, violated
/// invariants): report the issue.
pub const REPORT_ISSUE_ACTION: &str = "ReportIssue";
/// Event `reason` when a cron expression failed to parse at scheduling time.
pub const INVALID_SCHEDULE_REASON: &str = "InvalidSchedule";
/// `action` for an unparseable cron expression: fix the schedule in the spec.
pub const FIX_SCHEDULE_ACTION: &str = "FixSchedule";
/// Event `reason` when an object lacked a field the reconciler requires.
pub const INVARIANT_VIOLATED_REASON: &str = "InvariantViolated";
/// Event `reason` when a reconcile is blocked on an out-of-band grant an admin
/// applies on ANOTHER object (e.g. the `privileged-movers` namespace annotation).
pub const BLOCKED_ON_GRANT_REASON: &str = "BlockedOnGrant";
/// `action` for a blocked grant: apply the named grant on the named object —
/// the granting object is watched, so the blocked CR re-reconciles the moment
/// the grant lands.
pub const APPLY_GRANT_ACTION: &str = "ApplyGrant";
/// Event `reason` when self-managed webhook TLS setup failed.
pub const WEBHOOK_SETUP_FAILED_REASON: &str = "WebhookSetupFailed";
/// `action` for a webhook TLS setup failure: check the webhook configuration.
pub const CHECK_WEBHOOK_CONFIGURATION_ACTION: &str = "CheckWebhookConfiguration";

/// Annotation the controller stamps on the self-managed webhook TLS Secret
/// recording the serving leaf's `notAfter` as a Unix timestamp (seconds). Read
/// back to decide leaf rotation without parsing the certificate
/// ([`crate::webhook_tls`]).
pub const WEBHOOK_CERT_NOT_AFTER_ANNOTATION: &str =
    "kopiur.home-operations.com/webhook-cert-not-after";
