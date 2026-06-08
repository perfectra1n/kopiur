//! The `BackupConfig` CRD — the *recipe*. Idempotent; runs nothing on its own.
//! ADR-0001 §3.3, ADR-0003 §4.8.

use crate::backend::NfsVolume;
use crate::common::{
    CredentialProjection, DeletionPolicy, Identity, MoverSpec, PodSelector, RepositoryRef,
    ResolvedIdentity, Retention,
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
    kind = "BackupConfig",
    namespaced,
    status = "BackupConfigStatus",
    shortname = "kopiabc",
    category = "kopiur",
    printcolumn = r#"{"name":"Repository","type":"string","jsonPath":".spec.repository.name"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BackupConfigSpec {
    /// Discriminated reference to a `Repository` or `ClusterRepository`. ADR §3.2.
    pub repository: RepositoryRef,
    /// Identity overrides — what kopia records as `username@hostname:path`. ADR §3.3/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<Identity>,
    /// What to back up. At least one source; webhook-enforced. ADR §3.3.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<Source>,
    /// How the source volume is captured before kopia reads it: `Snapshot`
    /// (point-in-time CSI snapshot, default), `Clone`, or `Direct`. ADR §3.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy_method: Option<CopyMethod>,
    /// `VolumeSnapshotClass` used when `copyMethod` snapshots/clones the source.
    /// ADR §3.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_snapshot_class_name: Option<String>,
    /// Default `VolumeGroupSnapshot` for multi-PVC sources; `None` opts into per-PVC. ADR §4.9.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_by: Option<GroupBy>,
    /// GFS retention — enforced by the operator pruning `Backup` CRs. ADR §4.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<Retention>,
    /// Default `deletionPolicy` for `Backup` CRs created against this config. ADR §3.3/§4.5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_deletion_policy: Option<DeletionPolicy>,
    /// Typed kopia policy + `extraArgs` escape hatch. ADR §3.3 (G12).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<Policy>,
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
    /// own namespace. Inherited by `Backup`s produced from this config. ADR §4.11.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_projection: Option<CredentialProjection>,
}

/// A single backup source. `pvc`, `pvcSelector`, and `nfs` are mutually exclusive
/// (webhook-enforced — NOT an enum, because the forms share the sibling
/// `sourcePath*` keys and YAML lists them as optional siblings). ADR §3.3.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    /// Name of the `PersistentVolumeClaim` to back up (in the `BackupConfig`'s
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
    /// `BackupConfig`'s own namespace. ADR §3.3/§5.4.
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
    /// Exact namespace names to search; empty means the `BackupConfig`'s own
    /// namespace. ADR §3.3/§5.4.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub match_names: Vec<String>,
}

/// Volume snapshot copy method. Closed enum. ADR §3.3.
///
/// ```
/// use kopiur_api::CopyMethod;
///
/// // Defaults to a point-in-time CSI snapshot.
/// assert_eq!(CopyMethod::default(), CopyMethod::Snapshot);
/// // Serializes as a bare PascalCase string (no external tagging — it has no payload).
/// assert_eq!(serde_json::to_value(CopyMethod::Snapshot).unwrap(), "Snapshot");
/// assert_eq!(serde_json::to_value(CopyMethod::Direct).unwrap(), "Direct");
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum CopyMethod {
    /// Point-in-time CSI volume snapshot (default).
    #[default]
    Snapshot,
    /// CSI volume clone of the source, mounted read-only for the snapshot.
    Clone,
    /// Read the live PVC directly with no intermediate snapshot/clone (no
    /// point-in-time guarantee). ADR §3.3.
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

/// Typed kopia policy fields plus an `extraArgs` escape hatch. ADR §3.3 (G12).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    /// Compression algorithm + per-extension opt-outs. ADR §3.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    /// kopia object splitter (e.g. `DYNAMIC-4M-BUZHASH`). ADR §3.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub splitter: Option<String>,
    /// Paths/patterns kopia should skip while snapshotting. ADR §3.3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore: Option<IgnorePolicy>,
    /// Escape hatch for kopia flags not yet modeled. ADR §3.3.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,
}

/// Compression policy for a [`Policy`]. ADR §3.3.
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

/// Path-ignore policy for a [`Policy`]. ADR §3.3.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IgnorePolicy {
    /// Filename/path globs to exclude from the snapshot (e.g. `*.tmp`,
    /// `*/cache/*`, `lost+found`). ADR §3.3.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<String>,
    /// Honor `CACHEDIR.TAG`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cache_dirs: bool,
    /// fork issue #13.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ignore_identical_snapshots: bool,
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
/// use kopiur_api::backup_config::{Hook, HttpRequestHook};
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
    /// use kopiur_api::backup_config::{Hook, HttpRequestHook};
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

/// Observed state of a [`BackupConfig`]. ADR §3.3 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupConfigStatus {
    /// `metadata.generation` last reconciled, for staleness detection. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// What would be passed to kopia — pinned at admission. ADR §3.3/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedConfig>,
    /// Summary of GFS retention pruning against this config's `Backup` CRs.
    /// ADR §3.3 status/§4.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionSummary>,
    /// Standard Kubernetes conditions (e.g. `RepositoryReachable`,
    /// `GroupSnapshotSupported`). ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// The recipe as kopia would see it, pinned at admission and never re-rendered
/// (ADR §4.2). ADR §3.3 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedConfig {
    /// The resolved `username@hostname` identity. ADR §3.3/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<ResolvedIdentity>,
    /// The concrete PVCs + source paths after selector expansion. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<ResolvedConfigSource>,
}

/// One resolved source — a concrete PVC and the path kopia records for it. ADR §3.3 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedConfigSource {
    /// `namespace/name` of the PVC, as kopia sees it. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc: Option<String>,
    /// The source path kopia records for this PVC. ADR §3.3/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
}

/// Summary of the most recent GFS retention prune for a [`BackupConfig`]. ADR §3.3 status/§4.4.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionSummary {
    /// CRs currently inside the GFS window. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_backup_count: Option<i64>,
    /// RFC3339 timestamp of the last prune pass. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_prune_at: Option<String>,
    /// Number of `Backup` CRs deleted by the last prune pass. ADR §3.3 status/§4.4.
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
    fn backup_config_crd_metadata_is_correct() {
        let crd = BackupConfig::crd();
        assert_eq!(crd.spec.group, "kopiur.home-operations.com");
        assert_eq!(crd.spec.names.kind, "BackupConfig");
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
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
policy:
  compression:
    compressor: zstd
    neverCompress: ["*.zip", "*.gz", "*.mp4"]
  splitter: DYNAMIC-4M-BUZHASH
  ignore:
    paths: ["*.tmp", "*/cache/*", "lost+found"]
    cacheDirs: true
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
"#;
        let spec: BackupConfigSpec = from_yaml(yaml);
        assert_eq!(spec.repository.kind, RepositoryKind::Repository);
        assert_eq!(spec.repository.name, "nas-primary");
        assert_eq!(spec.sources.len(), 1);
        assert_eq!(spec.sources[0].pvc.as_ref().unwrap().name, "postgres-data");
        assert_eq!(
            spec.sources[0].source_path_override.as_deref(),
            Some("/data")
        );
        assert_eq!(spec.copy_method, Some(CopyMethod::Snapshot));
        assert_eq!(spec.group_by, Some(GroupBy::VolumeGroupSnapshot));
        assert_eq!(spec.default_deletion_policy, Some(DeletionPolicy::Delete));
        let comp = spec.policy.as_ref().unwrap().compression.as_ref().unwrap();
        assert_eq!(comp.compressor.as_deref(), Some("zstd"));
        let hooks = spec.hooks.as_ref().unwrap();
        assert_eq!(hooks.before_snapshot.len(), 1);
        assert_eq!(hooks.before_snapshot[0].kind_str(), "WorkloadExec");

        let json = serde_json::to_value(&spec).expect("serialize");
        let reparsed: BackupConfigSpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn credential_projection_roundtrip() {
        // Opt-in projection now lives on the recipe (BackupConfig), parses the
        // cluster's way, and round-trips.
        let yaml = r#"
repository: { kind: ClusterRepository, name: shared }
sources:
  - pvc: { name: data }
retention: { keepLatest: 5 }
credentialProjection:
  enabled: true
"#;
        let spec: BackupConfigSpec = from_yaml(yaml);
        assert_eq!(
            spec.credential_projection.as_ref().map(|p| p.enabled),
            Some(true)
        );
        let json = serde_json::to_value(&spec).expect("serialize");
        assert_eq!(json["credentialProjection"]["enabled"], true);
        let reparsed: BackupConfigSpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);

        // Absent ⇒ None (self-managed default); not serialized.
        let bare: BackupConfigSpec = from_yaml(
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
        let empty: BackupConfigSpec = from_yaml(
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
        let spec: BackupConfigSpec = from_yaml(yaml);
        let src = &spec.sources[0];
        assert!(src.pvc.is_none());
        assert!(src.pvc_selector.is_some());
        assert_eq!(src.source_path_strategy, Some(SourcePathStrategy::PvcName));

        let json = serde_json::to_value(&spec).unwrap();
        let reparsed: BackupConfigSpec = serde_json::from_value(json).unwrap();
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
}
