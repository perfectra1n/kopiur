//! The `SnapshotPolicy` CRD — the *recipe*. Idempotent; runs nothing on its own.
//! ADR-0001 §3.3, ADR-0003 §4.8.

use crate::backend::NfsVolume;
use crate::common::{
    CredentialProjection, CronSpec, DeletionPolicy, Identity, MoverSpec, PodSelector,
    RepositoryRef, ResolvedIdentity, Retention,
};
use k8s_openapi::api::batch::v1::JobSpec;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, LabelSelector};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// *What* to back up: sources, identity, retention, policy, hooks. ADR §3.3.
///
/// Not `Eq`: transitively embeds k8s-openapi types via `mover` and `hooks` (`JobSpec`).
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "kopiur.home-operations.com",
    version = "v1alpha1",
    kind = "SnapshotPolicy",
    plural = "snapshotpolicies",
    namespaced,
    status = "SnapshotPolicyStatus",
    shortname = "kopiasp",
    category = "kopiur",
    printcolumn = r#"{"name":"Repository","type":"string","jsonPath":".spec.repository.name"}"#,
    printcolumn = r#"{"name":"Last-Snapshot","type":"date","jsonPath":".status.lastSuccessfulSnapshot"}"#,
    printcolumn = r#"{"name":"Last-Verified","type":"date","jsonPath":".status.lastVerified"}"#,
    printcolumn = r#"{"name":"Suspended","type":"boolean","jsonPath":".spec.suspend"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotPolicySpec {
    /// Discriminated reference to a `Repository` or `ClusterRepository`. ADR §3.2.
    pub repository: RepositoryRef,
    /// Identity overrides — what kopia records as `username@hostname:path`. ADR §3.3/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<Identity>,
    /// What to back up. At least one source; webhook-enforced. ADR §3.3.
    ///
    /// `maxItems` is set so the apiserver can bound the cost of the per-item
    /// `x-kubernetes-validations` exactly-one-of rule on [`Source`] (CEL rule cost is
    /// `rule_cost × maxItems`; without a bound the apiserver assumes a huge array and
    /// rejects the CRD as over budget). 100 sources per recipe is far past any real
    /// use.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(length(max = 100))]
    pub sources: Vec<Source>,
    /// How the source volume is captured before kopia reads it: `Direct` (read the live
    /// PVC, the **default** — works on any storage), `Snapshot` (point-in-time CSI
    /// snapshot, opt-in), or `Clone` (CSI clone, opt-in). ADR §3.3 / [Copy methods].
    ///
    /// Carries a real OpenAPI `default: Direct` so it materializes into the stored object
    /// and `kubectl explain`, and GitOps engines stop diff-thrashing on a controller-set
    /// value. A bare value (not `Option`) so the default is never dropped by
    /// `skip_serializing_if`.
    ///
    /// [Copy methods]: https://github.com/home-operations/kopiur/blob/main/docs/copy-methods.md
    #[serde(default = "default_copy_method")]
    #[schemars(default = "default_copy_method")]
    pub copy_method: CopyMethod,
    /// `VolumeSnapshotClass` used when `copyMethod` snapshots/clones the source.
    /// ADR §3.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_snapshot_class_name: Option<String>,
    /// Default `VolumeGroupSnapshot` for multi-PVC sources; `None` opts into per-PVC. ADR §4.9.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_by: Option<GroupBy>,
    /// GFS retention — enforced by the operator pruning `Snapshot` CRs. ADR §4.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<Retention>,
    /// Default `deletionPolicy` for `Snapshot` CRs created against this config. ADR §3.3/§4.5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_deletion_policy: Option<DeletionPolicy>,
    /// Compression algorithm + per-extension opt-outs. ADR §3.3 (G12).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    /// Paths/patterns kopia should skip while snapshotting. ADR §3.3 (G12).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Files>,
    /// Escape hatch for kopia flags not yet modeled. ADR §3.3 (G12).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,
    /// Backup-side error handling — the missing analog of restore's
    /// `ignorePermissionErrors`. Lets a snapshot complete-with-errors instead of
    /// failing outright (`--ignore-file-errors` / `--ignore-dir-errors` /
    /// `--ignore-unknown-types`). ADR-0005 §13(b).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_handling: Option<ErrorHandling>,
    /// Upload parallelism (kopia's upload policy: `--max-parallel-snapshots`,
    /// `--max-parallel-file-reads`). ADR-0005 §13(f).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload: Option<Upload>,
    /// First-class backup verification (ADR-0005 §4). Opt-in: when absent, no
    /// verification runs (no behavior change). When set, the controller schedules
    /// periodic `kopia snapshot verify` (quick) and/or a scratch-restore test (deep),
    /// gated by an optional CEL `successExpr` predicate over the result, and surfaces
    /// `status.lastVerified`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<Verification>,
    /// Pause this recipe declaratively: a suspended `SnapshotPolicy` is skipped by
    /// schedules and by its own reconcile (no retention prune, no backup creation),
    /// surfaced via a condition/column. ADR-0005 §14(e).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspend: bool,
    /// Pre/post snapshot hooks that run in the workload, not the mover. ADR §4.8 (G13).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hooks: Option<Hooks>,
    /// Per-recipe mover overrides (resources, cache, security context). ADR §3.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mover: Option<MoverSpec>,
    /// Opt-in credential-Secret projection for this recipe's backup movers
    /// (default off). When `enabled: true`, the operator copies the referenced
    /// repository's credential Secret(s) into the namespace where each backup
    /// mover runs (a no-op when they already live there) — so a workload backing
    /// up to a shared `ClusterRepository` need not pre-create the Secret in its
    /// own namespace. Inherited by `Snapshot`s produced from this config. ADR §4.11.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_projection: Option<CredentialProjection>,
}

/// A single backup source. `pvc`, `pvcSelector`, and `nfs` are mutually exclusive
/// (webhook-enforced — NOT an enum, because the forms share the sibling
/// `sourcePath*` keys and YAML lists them as optional siblings). ADR §3.3.
///
/// §15: an operator-authored `x-kubernetes-validations` rule enforces exactly-one-of
/// in the apiserver + CI, complementing `validate_source`.
// The exactly-one-of rule is written as an integer sum of `has()` ternaries rather
// than `[...].filter(x,x).size()==1`: the apiserver estimates per-item CEL cost ×
// `maxItems`, and a list-construction + lambda `filter` blows the budget on the
// repeating `sources` list. The sum form is a cheap constant per item.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[schemars(extend("x-kubernetes-validations" = [{
    "rule": "(has(self.pvc) ? 1 : 0) + (has(self.pvcSelector) ? 1 : 0) + (has(self.nfs) ? 1 : 0) == 1",
    "message": "exactly one of pvc, pvcSelector, nfs"
}]))]
#[serde(rename_all = "camelCase")]
pub struct Source {
    /// Single PVC by name. Mutually exclusive with `pvcSelector`/`nfs`. ADR §3.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc: Option<PvcSource>,
    /// Label/namespace selector matching many PVCs (multi-PVC sources).
    /// Mutually exclusive with `pvc`/`nfs`. ADR §3.3/§5.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc_selector: Option<PvcSelector>,
    /// An inline NFS export to back up directly — no PVC. Mounted read-only at the
    /// source path (default: the export's `path`). Mutually exclusive with
    /// `pvc`/`pvcSelector`. ADR §3.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nfs: Option<NfsVolume>,
    /// What kopia records as the source path (default `/pvc/<name>` for a PVC, or
    /// the NFS export `path` for an NFS source). ADR §3.3/§4.2.
    ///
    /// `maxLength` bounds the per-item cost of the exactly-one-of CEL rule on this
    /// struct (an unbounded string makes the apiserver assume a huge value and blow
    /// the rule's cost budget). A filesystem path is far shorter than 4096.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 4096))]
    pub source_path_override: Option<String>,
    /// How a selector-matched PVC's source path is derived (`pvcName` vs
    /// `pvcNamespacedName`). Applies to `pvcSelector` sources. ADR §3.3/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path_strategy: Option<SourcePathStrategy>,
}

/// A single backup source addressed by PVC name. ADR §3.3.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PvcSource {
    /// Name of the `PersistentVolumeClaim` to back up (in the `SnapshotPolicy`'s
    /// namespace). ADR §3.3.
    pub name: String,
}

/// Selects PVCs across namespaces by label. ADR §3.3/§5.4.
///
/// Not `Eq`: embeds `LabelSelector` (k8s-openapi, `PartialEq` only).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PvcSelector {
    /// Restricts the search to specific namespaces; absent means the
    /// `SnapshotPolicy`'s own namespace. ADR §3.3/§5.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<NamespaceSelector>,
    /// Standard Kubernetes label selector matching the PVCs to include. ADR §3.3/§5.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label_selector: Option<LabelSelector>,
}

/// Restricts a [`PvcSelector`] to an explicit set of namespaces. ADR §3.3/§5.4.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceSelector {
    /// Exact namespace names to search; empty means the `SnapshotPolicy`'s own
    /// namespace. ADR §3.3/§5.4.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub match_names: Vec<String>,
}

/// serde/schemars `default` for [`SnapshotPolicySpec::copy_method`] — **`Direct`**.
///
/// `Direct` (read the live PVC) is the default for **backward compatibility and
/// portability**: it is the behavior that was actually in effect before `copyMethod`
/// was wired (the field was inert), and it works on **any** storage — no CSI snapshot
/// stack required. `Snapshot`/`Clone` (point-in-time CSI capture) are an explicit
/// opt-in for users who have the snapshot stack and want app-decoupled, point-in-time
/// backups. (Originally ADR-0005 §1 proposed `Snapshot` as the default; defaulting to it
/// would silently break every existing policy / non-CSI source on upgrade, so the
/// implemented default is `Direct`.)
///
/// A named fn so it backs BOTH `#[serde(default = ...)]` and `#[schemars(default = ...)]`,
/// which is what makes schemars 1 emit a real OpenAPI `default:` in the generated CRD.
fn default_copy_method() -> CopyMethod {
    CopyMethod::Direct
}

/// Volume snapshot copy method. Closed enum. ADR §3.3.
///
/// ```
/// use kopiur_api::CopyMethod;
///
/// // Defaults to a live read (Direct) — works on any storage, no CSI snapshot stack.
/// assert_eq!(CopyMethod::default(), CopyMethod::Direct);
/// // Serializes as a bare PascalCase string (no external tagging — it has no payload).
/// assert_eq!(serde_json::to_value(CopyMethod::Snapshot).unwrap(), "Snapshot");
/// assert_eq!(serde_json::to_value(CopyMethod::Direct).unwrap(), "Direct");
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum CopyMethod {
    /// Point-in-time CSI volume snapshot. Opt-in: requires the CSI snapshot stack + a
    /// `VolumeSnapshotClass` for the source's driver.
    Snapshot,
    /// CSI volume clone of the source, mounted read-only for the snapshot. Opt-in:
    /// requires a CSI driver that supports cloning.
    Clone,
    /// Read the live PVC directly with no intermediate snapshot/clone (no point-in-time
    /// guarantee; the mover co-locates on the volume's node for RWO). The **default** —
    /// works on any storage. ADR §3.3.
    #[default]
    Direct,
}

/// Multi-PVC grouping strategy. Closed enum. ADR §4.9.
///
/// Defaults to a consistent group snapshot; `None` must be set *explicitly* to
/// accept independent per-PVC snapshots, because a silent per-PVC fallback would
/// produce inconsistent backups (the data-integrity hazard ADR §4.9 guards against).
///
/// ```
/// use kopiur_api::GroupBy;
///
/// assert_eq!(GroupBy::default(), GroupBy::VolumeGroupSnapshot);
/// assert_eq!(serde_json::to_value(GroupBy::None).unwrap(), "None");
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum GroupBy {
    /// Consistent group snapshot across all PVCs (default for multi-PVC).
    #[default]
    VolumeGroupSnapshot,
    /// Opt into independent per-PVC snapshots. ADR §4.9.
    None,
}

/// How a selector-matched PVC's source path is derived. Closed enum. ADR §3.3/§4.2.
///
/// Only relevant for `pvcSelector` sources, where one recipe expands to many PVCs
/// and each needs a distinct kopia source path.
///
/// ```
/// use kopiur_api::SourcePathStrategy;
///
/// assert_eq!(SourcePathStrategy::default(), SourcePathStrategy::PvcName);
/// assert_eq!(
///     serde_json::to_value(SourcePathStrategy::PvcNamespacedName).unwrap(),
///     "PvcNamespacedName"
/// );
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum SourcePathStrategy {
    /// Path derived from the PVC name alone (default). ADR §3.3.
    #[default]
    PvcName,
    /// Path derived from `<namespace>/<name>` to disambiguate same-named PVCs
    /// across namespaces. ADR §3.3.
    PvcNamespacedName,
}

/// Compression policy (flattened onto [`SnapshotPolicySpec`], ADR-0004 §4b). ADR §3.3.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Compression {
    /// kopia compressor name (e.g. `zstd`); absent leaves kopia's default. ADR §3.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compressor: Option<String>,
    /// Filename globs to leave uncompressed (e.g. already-compressed media). ADR §3.3.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub never_compress: Vec<String>,
}

/// File-ignore policy (flattened onto [`SnapshotPolicySpec`], ADR-0004 §4b). ADR §3.3.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Files {
    /// Filename/path globs to exclude from the snapshot (e.g. `*.tmp`,
    /// `*/cache/*`, `lost+found`). ADR §3.3.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ignore_rules: Vec<String>,
    /// Honor `CACHEDIR.TAG`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ignore_cache_dirs: bool,
    /// fork issue #13.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ignore_identical_snapshots: bool,
}

/// Backup-side error-handling policy (ADR-0005 §13(b)). The missing backup-side
/// analog of restore's `ignorePermissionErrors`: lets kopia complete a snapshot
/// with errors rather than aborting. Each bool defaults `false` (kopia's
/// fail-on-error default). Maps to `--ignore-file-errors` / `--ignore-dir-errors`
/// / `--ignore-unknown-types`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ErrorHandling {
    /// Continue the snapshot when a file cannot be read (`--ignore-file-errors`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ignore_file_errors: bool,
    /// Continue the snapshot when a directory cannot be read (`--ignore-dir-errors`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ignore_dir_errors: bool,
    /// Continue past entries of unknown type (`--ignore-unknown-types`).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ignore_unknown_types: bool,
}

/// Upload parallelism (kopia's upload policy, ADR-0005 §13(f)). Both knobs are
/// optional; absent leaves kopia's default. Maps to `--max-parallel-snapshots` /
/// `--max-parallel-file-reads`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Upload {
    /// `--max-parallel-snapshots`: how many sources snapshot concurrently.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallel_snapshots: Option<i64>,
    /// `--max-parallel-file-reads`: file-read concurrency within a snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallel_file_reads: Option<i64>,
}

/// First-class backup verification (ADR-0005 §4). Opt-in capability that proves a
/// repository's snapshots are *restorable*, not just that maintenance ran. Two
/// tiers (mirroring `Maintenance` quick/full): a frequent blob-level
/// `kopia snapshot verify` (`quick`) and an optional, rarer scratch-restore test
/// (`deep`). Each tier is scheduled with cron + deterministic jitter just like
/// maintenance, and an optional `successExpr` (CEL) asserts the result is good —
/// killing the silent "0 files" success.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Verification {
    /// Schedule for the frequent blob-level `kopia snapshot verify`. Absent ⇒ no
    /// quick verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quick: Option<CronSpec>,
    /// Schedule + knobs for the rarer scratch-restore restorability test. Absent ⇒
    /// no deep verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deep: Option<DeepVerification>,
    /// A CEL pass/fail predicate over the verify result (ADR-0005 §15). Environment:
    /// `stats{files,bytes,errors}`, `snapshot`, and (deep only) `restored{files,
    /// checksumMatches}`. Returns a bool; when it evaluates `false` the verification
    /// run fails. Validated at admission (`validate_success_expr`). Applies to both
    /// tiers. Example: `"stats.files > 0 && stats.errors == 0"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success_expr: Option<String>,
    /// How many files `quick` verifies fully (`--verify-files-percent`); absent
    /// leaves kopia's default. Tuning knob for the quick tier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_files_percent: Option<u8>,
}

/// Deep (scratch-restore) verification (ADR-0005 §4): restore the latest snapshot
/// into an ephemeral volume, sanity-check it, then discard. The most thorough
/// restorability proof; scheduled rarely.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeepVerification {
    /// Cron + jitter for the deep restore-test (e.g. weekly).
    pub schedule: CronSpec,
    /// StorageClass for the ephemeral scratch PVC; absent uses the cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
    /// Size of the ephemeral scratch PVC (e.g. `10Gi`); absent uses a built-in
    /// default. Should comfortably hold the restored snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<String>,
}

/// Pre/post snapshot hook lists. ADR §3.3/§4.8.
///
/// Not `Eq`: `Hook::RunJob` embeds `JobSpec`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Hooks {
    /// Hooks run (in order) before the snapshot is taken — e.g. quiescing a
    /// database. ADR §4.8.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub before_snapshot: Vec<Hook>,
    /// Hooks run (in order) after the snapshot completes — e.g. resuming the
    /// workload. ADR §4.8.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after_snapshot: Vec<Hook>,
}

/// One of three hook forms. ADR §4.8.
///
/// Externally-tagged: wire shape is `{ workloadExec: {...} }`, `{ runJob: {...} }`,
/// or `{ httpRequest: {...} }`. Exactly one variant by construction.
///
/// Not `Eq`: `RunJob` embeds `JobSpec` (k8s-openapi, `PartialEq` only).
///
/// ```
/// use kopiur_api::snapshot_policy::{Hook, HttpRequestHook};
///
/// // Construct directly — the type system guarantees exactly one variant.
/// let hook = Hook::HttpRequest(HttpRequestHook {
///     url: "https://example/notify".into(),
///     method: Some("POST".into()),
///     body: None,
///     timeout: None,
///     continue_on_failure: false,
/// });
/// assert_eq!(hook.kind_str(), "HttpRequest");
///
/// // Externally tagged on the wire: `{ httpRequest: { url: ... } }`.
/// let json = serde_json::to_value(&hook).unwrap();
/// assert_eq!(json["httpRequest"]["url"], "https://example/notify");
/// ```
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Hook {
    /// `kubectl exec`-style into a matched workload pod/container (the default form,
    /// fork #22).
    WorkloadExec(WorkloadExecHook),
    /// Full `JobSpec` run as a one-shot Job (k8up `PreBackupPod` analog).
    ///
    /// Boxed: `RunJobHook` embeds a `JobSpec` (~2 KB), which would otherwise bloat
    /// every `Hook` (incl. the common `WorkloadExec`). `Box<T>` is transparent to
    /// serde, so the externally-tagged `{ runJob: {...} }` wire shape is unchanged.
    RunJob(Box<RunJobHook>),
    /// Typed POST to a URL for cross-system orchestration.
    HttpRequest(HttpRequestHook),
}

impl Hook {
    /// Stable discriminant string for status/metrics — one of `"WorkloadExec"`,
    /// `"RunJob"`, or `"HttpRequest"`.
    ///
    /// ```
    /// use kopiur_api::snapshot_policy::{Hook, HttpRequestHook};
    ///
    /// let hook = Hook::HttpRequest(HttpRequestHook {
    ///     url: "https://example/notify".into(),
    ///     method: None,
    ///     body: None,
    ///     timeout: None,
    ///     continue_on_failure: false,
    /// });
    /// assert_eq!(hook.kind_str(), "HttpRequest");
    /// ```
    pub fn kind_str(&self) -> &'static str {
        match self {
            Hook::WorkloadExec(_) => "WorkloadExec",
            Hook::RunJob(_) => "RunJob",
            Hook::HttpRequest(_) => "HttpRequest",
        }
    }
}

/// Hook failures abort the backup by default; `continueOnFailure: true` is opt-in. ADR §4.8.
///
/// Not `Eq`: embeds `LabelSelector` via `PodSelector`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadExecHook {
    /// Selects the workload pod/container to exec into (flattened onto the hook).
    /// ADR §4.8.
    #[serde(flatten)]
    pub selector: PodSelector,
    /// Command + args to run inside the selected container. ADR §4.8.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    /// Max time to wait for the command (Go duration string, e.g. `2m`). ADR §4.8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    /// If `true`, a failed hook does not abort the backup (default: abort). ADR §4.8.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub continue_on_failure: bool,
}

/// A hook that materializes a full one-shot Job (k8up `PreBackupPod` analog). ADR §4.8.
///
/// Not `Eq`: embeds `JobSpec`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunJobHook {
    /// The full Kubernetes `JobSpec` to run. ADR §4.8.
    pub job_spec: JobSpec,
    /// Max time to wait for the Job to complete (Go duration string). ADR §4.8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    /// If `true`, a failed Job does not abort the backup (default: abort). ADR §4.8.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub continue_on_failure: bool,
}

/// A hook that issues an HTTP request for cross-system orchestration. ADR §4.8.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HttpRequestHook {
    /// Target URL to call. ADR §4.8.
    pub url: String,
    /// HTTP method (default `POST`). ADR §4.8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Optional request body. ADR §4.8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// Max time to wait for the response (Go duration string). ADR §4.8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    /// If `true`, a failed request does not abort the backup (default: abort). ADR §4.8.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub continue_on_failure: bool,
}

/// Observed state of a [`SnapshotPolicy`]. ADR §3.3 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotPolicyStatus {
    /// `metadata.generation` last reconciled, for staleness detection. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// What would be passed to kopia — pinned at admission. ADR §3.3/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedPolicy>,
    /// Summary of GFS retention pruning against this config's `Snapshot` CRs.
    /// ADR §3.3 status/§4.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionSummary>,
    /// RFC3339 timestamp of the most recent successful child `Snapshot` produced
    /// from this recipe. Backs the `LAST-SNAPSHOT` printer column and the
    /// `prometheusRule` staleness alert (ADR-0005 §3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_successful_snapshot: Option<String>,
    /// RFC3339 timestamp of the most recent successful verification (any tier),
    /// ADR-0005 §4. Backs the `kopiur_snapshot_verified_timestamp` gauge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_verified: Option<String>,
    /// Standard Kubernetes conditions (e.g. `RepositoryReachable`,
    /// `GroupSnapshotSupported`). ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// The recipe as kopia would see it, pinned at admission and never re-rendered
/// (ADR §4.2). ADR §3.3 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedPolicy {
    /// The resolved `username@hostname` identity. ADR §3.3/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<ResolvedIdentity>,
    /// The concrete PVCs + source paths after selector expansion. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<ResolvedPolicySource>,
}

/// One resolved source — a concrete PVC and the path kopia records for it. ADR §3.3 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedPolicySource {
    /// `namespace/name` of the PVC, as kopia sees it. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc: Option<String>,
    /// The source path kopia records for this PVC. ADR §3.3/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
}

/// Summary of the most recent GFS retention prune for a [`SnapshotPolicy`]. ADR §3.3 status/§4.4.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionSummary {
    /// CRs currently inside the GFS window. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_snapshot_count: Option<i64>,
    /// RFC3339 timestamp of the last prune pass. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_prune_at: Option<String>,
    /// Number of `Snapshot` CRs deleted by the last prune pass. ADR §3.3 status/§4.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_prune_deleted: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::RepositoryKind;
    use crate::testutil::from_yaml;
    use kube::core::CustomResourceExt;

    #[test]
    fn snapshot_policy_crd_metadata_is_correct() {
        let crd = SnapshotPolicy::crd();
        assert_eq!(crd.spec.group, "kopiur.home-operations.com");
        assert_eq!(crd.spec.names.kind, "SnapshotPolicy");
        assert_eq!(crd.spec.names.plural, "snapshotpolicies");
        assert_eq!(
            crd.spec.names.short_names.as_deref(),
            Some(&["kopiasp".to_string()][..])
        );
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn copy_method_carries_static_openapi_default_in_crd() {
        // copyMethod must carry a real schema `default: Direct` so it appears in
        // `kubectl explain` / the stored object and GitOps stops thrashing. `Direct` (not
        // the ADR-0005 §1 `Snapshot`) so wiring the field doesn't silently break every
        // existing policy / non-CSI source on upgrade — Snapshot/Clone are opt-in.
        let crd = SnapshotPolicy::crd();
        let json = serde_json::to_value(&crd).expect("serialize CRD");
        let default = &json["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["copyMethod"]["default"];
        assert_eq!(
            default, "Direct",
            "copyMethod must emit `default: Direct` in the CRD schema; got {default:?}"
        );
    }

    #[test]
    fn copy_method_defaults_to_direct_when_absent() {
        // A bare value with a serde default: an omitted copyMethod parses to Direct (the
        // portable, backward-compatible live-mount behavior).
        let spec: SnapshotPolicySpec = from_yaml(
            "repository: { kind: Repository, name: r }\nsources: [ { pvc: { name: d } } ]\n",
        );
        assert_eq!(spec.copy_method, CopyMethod::Direct);
        // And it serializes (not skip-elided), so the materialized value round-trips.
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["copyMethod"], "Direct");
    }

    #[test]
    fn backup_config_roundtrip_matches_adr_shape() {
        // Mirrors ADR-0001 §3.3.
        let yaml = r#"
repository:
  kind: Repository
  name: nas-primary
  namespace: backups
identity:
  username: "postgres-data"
  hostname: "billing"
sources:
  - pvc: { name: postgres-data }
    sourcePathOverride: /data
copyMethod: Snapshot
volumeSnapshotClassName: csi-snap-class
groupBy: VolumeGroupSnapshot
retention:
  keepLatest: 10
  keepDaily: 14
defaultDeletionPolicy: Delete
compression:
  compressor: zstd
  neverCompress: ["*.zip", "*.gz", "*.mp4"]
files:
  ignoreRules: ["*.tmp", "*/cache/*", "lost+found"]
  ignoreCacheDirs: true
  ignoreIdenticalSnapshots: true
extraArgs: []
hooks:
  beforeSnapshot:
    - workloadExec:
        podSelector: { matchLabels: { app: postgres } }
        container: postgres
        command: ["pg_start_backup", "snap"]
        timeout: 2m
  afterSnapshot:
    - workloadExec:
        podSelector: { matchLabels: { app: postgres } }
        container: postgres
        command: ["pg_stop_backup"]
        timeout: 2m
mover:
  resources:
    requests: { cpu: 250m, memory: 512Mi }
    limits: { cpu: "2", memory: 4Gi }
  cache:
    capacity: 16Gi
    storageClassName: fast-ssd
  securityContext:
    runAsUser: 1000
    runAsGroup: 1000
    runAsNonRoot: true
    allowPrivilegeEscalation: false
    capabilities: { drop: ["ALL"] }
    seccompProfile: { type: RuntimeDefault }
  podSecurityContext:
    fsGroup: 1000
    fsGroupChangePolicy: OnRootMismatch
"#;
        let spec: SnapshotPolicySpec = from_yaml(yaml);
        assert_eq!(spec.repository.kind, RepositoryKind::Repository);
        assert_eq!(spec.repository.name, "nas-primary");
        assert_eq!(spec.sources.len(), 1);
        assert_eq!(spec.sources[0].pvc.as_ref().unwrap().name, "postgres-data");
        assert_eq!(
            spec.sources[0].source_path_override.as_deref(),
            Some("/data")
        );
        assert_eq!(spec.copy_method, CopyMethod::Snapshot);
        assert_eq!(spec.group_by, Some(GroupBy::VolumeGroupSnapshot));
        assert_eq!(spec.default_deletion_policy, Some(DeletionPolicy::Delete));
        let comp = spec.compression.as_ref().unwrap();
        assert_eq!(comp.compressor.as_deref(), Some("zstd"));
        let files = spec.files.as_ref().unwrap();
        assert_eq!(files.ignore_rules.len(), 3);
        assert!(files.ignore_cache_dirs);
        assert!(spec.extra_args.is_empty());
        let hooks = spec.hooks.as_ref().unwrap();
        assert_eq!(hooks.before_snapshot.len(), 1);
        assert_eq!(hooks.before_snapshot[0].kind_str(), "WorkloadExec");
        // Both the container- and pod-level security contexts round-trip on the mover.
        let mover = spec.mover.as_ref().expect("mover");
        assert_eq!(
            mover.security_context.as_ref().and_then(|s| s.run_as_user),
            Some(1000)
        );
        assert_eq!(
            mover.pod_security_context.as_ref().and_then(|p| p.fs_group),
            Some(1000)
        );
        // Container UID/GID match + fsGroup is unprivileged (no namespace opt-in).
        assert!(!mover.requires_privilege());

        let json = serde_json::to_value(&spec).expect("serialize");
        let reparsed: SnapshotPolicySpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn credential_projection_roundtrip() {
        // Opt-in projection now lives on the recipe (SnapshotPolicy), parses the
        // cluster's way, and round-trips.
        let yaml = r#"
repository: { kind: ClusterRepository, name: shared }
sources:
  - pvc: { name: data }
retention: { keepLatest: 5 }
credentialProjection:
  enabled: true
"#;
        let spec: SnapshotPolicySpec = from_yaml(yaml);
        assert_eq!(
            spec.credential_projection.as_ref().map(|p| p.enabled),
            Some(true)
        );
        let json = serde_json::to_value(&spec).expect("serialize");
        assert_eq!(json["credentialProjection"]["enabled"], true);
        let reparsed: SnapshotPolicySpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);

        // Absent ⇒ None (self-managed default); not serialized.
        let bare: SnapshotPolicySpec = from_yaml(
            "repository: { kind: Repository, name: r }\nsources: [ { pvc: { name: d } } ]\n",
        );
        assert!(bare.credential_projection.is_none());
        assert!(
            serde_json::to_value(&bare)
                .unwrap()
                .get("credentialProjection")
                .is_none()
        );
        // Empty `{}` defaults enabled=false (opt-in).
        let empty: SnapshotPolicySpec = from_yaml(
            "repository: { kind: Repository, name: r }\nsources: [ { pvc: { name: d } } ]\ncredentialProjection: {}\n",
        );
        assert_eq!(empty.credential_projection.map(|p| p.enabled), Some(false));
    }

    #[test]
    fn backup_config_minimal_selector_source() {
        // Mirrors ADR-0001 §5.4 (multi-PVC selector).
        let yaml = r#"
repository: { kind: Repository, name: nas-primary, namespace: backups }
identity: { username: app-bundle, hostname: billing }
sources:
  - pvcSelector:
      labelSelector: { matchLabels: { backup: include } }
    sourcePathStrategy: PvcName
groupBy: VolumeGroupSnapshot
retention: { keepDaily: 14 }
"#;
        let spec: SnapshotPolicySpec = from_yaml(yaml);
        let src = &spec.sources[0];
        assert!(src.pvc.is_none());
        assert!(src.pvc_selector.is_some());
        assert_eq!(src.source_path_strategy, Some(SourcePathStrategy::PvcName));

        let json = serde_json::to_value(&spec).unwrap();
        let reparsed: SnapshotPolicySpec = serde_json::from_value(json).unwrap();
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn hook_run_job_variant_with_job_spec() {
        // RunJob embeds a full k8s-openapi JobSpec (so the struct is not Eq).
        let yaml = r#"
runJob:
  jobSpec:
    template:
      spec:
        restartPolicy: Never
        containers:
          - name: pre
            image: busybox
            command: ["sh", "-c", "echo hi"]
  timeout: 5m
  continueOnFailure: true
"#;
        let hook: Hook = from_yaml(yaml);
        assert_eq!(hook.kind_str(), "RunJob");
        match &hook {
            Hook::RunJob(j) => {
                assert!(j.continue_on_failure);
                assert_eq!(j.timeout.as_deref(), Some("5m"));
                assert_eq!(
                    j.job_spec
                        .template
                        .spec
                        .as_ref()
                        .unwrap()
                        .restart_policy
                        .as_deref(),
                    Some("Never")
                );
            }
            other => panic!("expected RunJob, got {}", other.kind_str()),
        }
        let json = serde_json::to_value(&hook).unwrap();
        assert!(json.get("runJob").is_some());
    }

    #[test]
    fn hook_http_request_variant() {
        let hook: Hook = from_yaml("httpRequest:\n  url: https://example/notify\n  method: POST\n");
        assert_eq!(hook.kind_str(), "HttpRequest");
        let json = serde_json::to_value(&hook).unwrap();
        assert_eq!(json["httpRequest"]["url"], "https://example/notify");
    }

    #[test]
    fn hook_unknown_variant_is_rejected() {
        let value: serde_json::Value = serde_yaml::from_str("teleport:\n  url: x\n").unwrap();
        assert!(serde_json::from_value::<Hook>(value).is_err());
    }

    #[test]
    fn error_handling_upload_and_suspend_roundtrip() {
        // ADR-0005 §13(b)/§13(f)/§14(e): the new policy knobs parse the cluster's
        // way, default sanely when absent, and round-trip.
        let yaml = r#"
repository: { kind: Repository, name: r }
sources: [ { pvc: { name: d } } ]
errorHandling:
  ignoreFileErrors: true
  ignoreDirErrors: false
  ignoreUnknownTypes: true
upload:
  maxParallelSnapshots: 4
  maxParallelFileReads: 8
suspend: true
"#;
        let spec: SnapshotPolicySpec = from_yaml(yaml);
        let eh = spec.error_handling.as_ref().expect("errorHandling");
        assert!(eh.ignore_file_errors);
        assert!(!eh.ignore_dir_errors);
        assert!(eh.ignore_unknown_types);
        let up = spec.upload.as_ref().expect("upload");
        assert_eq!(up.max_parallel_snapshots, Some(4));
        assert_eq!(up.max_parallel_file_reads, Some(8));
        assert!(spec.suspend);

        let json = serde_json::to_value(&spec).expect("serialize");
        assert_eq!(json["suspend"], true);
        assert_eq!(json["errorHandling"]["ignoreFileErrors"], true);
        assert_eq!(json["upload"]["maxParallelSnapshots"], 4);
        let reparsed: SnapshotPolicySpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);

        // Absent ⇒ None / false (not serialized).
        let bare: SnapshotPolicySpec = from_yaml(
            "repository: { kind: Repository, name: r }\nsources: [ { pvc: { name: d } } ]\n",
        );
        assert!(bare.error_handling.is_none());
        assert!(bare.upload.is_none());
        assert!(!bare.suspend);
        let bare_json = serde_json::to_value(&bare).unwrap();
        assert!(bare_json.get("suspend").is_none());
        assert!(bare_json.get("errorHandling").is_none());
    }

    #[test]
    fn source_schema_carries_exactly_one_of_validation() {
        // §15: the Source sub-object schema carries the exactly-one-of(pvc/
        // pvcSelector/nfs) rule, surviving kube's structural-schema rewriter even as a
        // list-item sub-object.
        let crd = SnapshotPolicy::crd();
        let json = serde_json::to_value(&crd).expect("serialize CRD");
        let source = &json["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["sources"]["items"];
        let rules = source["x-kubernetes-validations"]
            .as_array()
            .expect("sources.items.x-kubernetes-validations present");
        assert!(rules.iter().any(|r| {
            r["rule"]
                .as_str()
                .is_some_and(|s| s.contains("pvcSelector") && s.contains("nfs"))
        }));
    }

    #[test]
    fn snapshot_policy_has_last_snapshot_and_suspended_columns() {
        // ADR-0005 §3: the LAST-SNAPSHOT (status.lastSuccessfulSnapshot) and
        // §14(e) SUSPENDED columns are present in the CRD with the right jsonPaths.
        let crd = SnapshotPolicy::crd();
        let json = serde_json::to_value(&crd).expect("serialize CRD");
        let cols = json["spec"]["versions"][0]["additionalPrinterColumns"]
            .as_array()
            .expect("printer columns");
        let by_name = |name: &str| {
            cols.iter()
                .find(|c| c["name"] == name)
                .unwrap_or_else(|| panic!("missing column {name}"))
        };
        assert_eq!(
            by_name("Last-Snapshot")["jsonPath"],
            ".status.lastSuccessfulSnapshot"
        );
        assert_eq!(by_name("Suspended")["jsonPath"], ".spec.suspend");
    }

    #[test]
    fn verification_roundtrip_and_opt_in() {
        // ADR-0005 §4: verification parses the cluster's way, round-trips, and is
        // opt-in (absent ⇒ None, no behavior change).
        let yaml = r#"
repository: { kind: Repository, name: r }
sources: [ { pvc: { name: d } } ]
verification:
  quick: { cron: "0 4 * * *", jitter: 30m }
  deep:
    schedule: { cron: "0 5 * * 0", jitter: 1h }
    capacity: 10Gi
    storageClassName: fast-ssd
  successExpr: "stats.files > 0 && stats.errors == 0"
  verifyFilesPercent: 10
"#;
        let spec: SnapshotPolicySpec = from_yaml(yaml);
        let v = spec.verification.as_ref().expect("verification");
        assert_eq!(v.quick.as_ref().unwrap().cron, "0 4 * * *");
        let deep = v.deep.as_ref().expect("deep");
        assert_eq!(deep.schedule.cron, "0 5 * * 0");
        assert_eq!(deep.capacity.as_deref(), Some("10Gi"));
        assert_eq!(
            v.success_expr.as_deref(),
            Some("stats.files > 0 && stats.errors == 0")
        );
        assert_eq!(v.verify_files_percent, Some(10));

        let json = serde_json::to_value(&spec).expect("serialize");
        assert_eq!(json["verification"]["quick"]["cron"], "0 4 * * *");
        let reparsed: SnapshotPolicySpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);

        // Absent ⇒ None (no behavior change).
        let bare: SnapshotPolicySpec = from_yaml(
            "repository: { kind: Repository, name: r }\nsources: [ { pvc: { name: d } } ]\n",
        );
        assert!(bare.verification.is_none());
        assert!(
            serde_json::to_value(&bare)
                .unwrap()
                .get("verification")
                .is_none()
        );
    }

    #[test]
    fn snapshot_policy_has_last_verified_column() {
        // ADR-0005 §4: the LAST-VERIFIED (status.lastVerified) column is present.
        let crd = SnapshotPolicy::crd();
        let json = serde_json::to_value(&crd).expect("serialize CRD");
        let cols = json["spec"]["versions"][0]["additionalPrinterColumns"]
            .as_array()
            .expect("printer columns");
        let col = cols
            .iter()
            .find(|c| c["name"] == "Last-Verified")
            .expect("Last-Verified column");
        assert_eq!(col["jsonPath"], ".status.lastVerified");
    }

    #[test]
    fn status_last_successful_snapshot_roundtrips() {
        let status: SnapshotPolicyStatus =
            from_yaml("lastSuccessfulSnapshot: 2026-06-09T02:00:00Z\n");
        assert_eq!(
            status.last_successful_snapshot.as_deref(),
            Some("2026-06-09T02:00:00Z")
        );
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["lastSuccessfulSnapshot"], "2026-06-09T02:00:00Z");
    }
}
