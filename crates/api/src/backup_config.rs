//! The `BackupConfig` CRD — the *recipe*. Idempotent; runs nothing on its own.
//! ADR-0001 §3.3, ADR-0003 §4.8.

use crate::common::{
    DeletionPolicy, Identity, MoverSpec, PodSelector, RepositoryRef, ResolvedIdentity, Retention,
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
    group = "kopia.io",
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy_method: Option<CopyMethod>,
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
}

/// A single backup source. `pvc` and `pvcSelector` are mutually exclusive
/// (webhook-enforced — NOT an enum, because both forms share the sibling
/// `sourcePath*` keys and YAML lists them as optional siblings). ADR §3.3.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc: Option<PvcSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc_selector: Option<PvcSelector>,
    /// What kopia records as the source path (default `/pvc/<name>`). ADR §3.3/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path_override: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path_strategy: Option<SourcePathStrategy>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PvcSource {
    pub name: String,
}

/// Selects PVCs across namespaces by label. ADR §3.3/§5.4.
///
/// Not `Eq`: embeds `LabelSelector` (k8s-openapi, `PartialEq` only).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PvcSelector {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace_selector: Option<NamespaceSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label_selector: Option<LabelSelector>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceSelector {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub match_names: Vec<String>,
}

/// Volume snapshot copy method. Closed enum. ADR §3.3.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum CopyMethod {
    /// Point-in-time CSI volume snapshot (default).
    #[default]
    Snapshot,
    Clone,
    Direct,
}

/// Multi-PVC grouping strategy. Closed enum. ADR §4.9.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum GroupBy {
    /// Consistent group snapshot across all PVCs (default for multi-PVC).
    #[default]
    VolumeGroupSnapshot,
    /// Opt into independent per-PVC snapshots. ADR §4.9.
    None,
}

/// How a selector-matched PVC's source path is derived. Closed enum. ADR §3.3/§4.2.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum SourcePathStrategy {
    #[default]
    PvcName,
    PvcNamespacedName,
}

/// Typed kopia policy fields plus an `extraArgs` escape hatch. ADR §3.3 (G12).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub splitter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore: Option<IgnorePolicy>,
    /// Escape hatch for kopia flags not yet modeled. ADR §3.3.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_args: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Compression {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compressor: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub never_compress: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IgnorePolicy {
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub before_snapshot: Vec<Hook>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after_snapshot: Vec<Hook>,
}

/// One of three hook forms. ADR §4.8.
///
/// Externally-tagged: wire shape is `{ workloadExec: {...} }`, `{ runJob: {...} }`,
/// or `{ httpRequest: {...} }`. Exactly one variant by construction.
///
/// Not `Eq`: `RunJob` embeds `JobSpec` (k8s-openapi, `PartialEq` only).
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
    /// Stable discriminant string for status/metrics.
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
    #[serde(flatten)]
    pub selector: PodSelector,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub continue_on_failure: bool,
}

/// Not `Eq`: embeds `JobSpec`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunJobHook {
    pub job_spec: JobSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub continue_on_failure: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HttpRequestHook {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub continue_on_failure: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupConfigStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// What would be passed to kopia — pinned at admission. ADR §3.3/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<ResolvedIdentity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<ResolvedConfigSource>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedConfigSource {
    /// `namespace/name` of the PVC, as kopia sees it. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionSummary {
    /// CRs currently inside the GFS window. ADR §3.3 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_backup_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_prune_at: Option<String>,
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
        assert_eq!(crd.spec.group, "kopia.io");
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
