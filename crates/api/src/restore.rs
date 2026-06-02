//! The `Restore` CRD — a restore from a snapshot/identity to a PVC, or a passive
//! populator source. ADR-0001 §3.6, ADR-0003 §4.6.

use crate::common::{ObjectRef, RepositoryRef, ResolvedIdentity};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A restore operation. `target` is optional: absence = passive populator mode,
/// consumed by a PVC's `spec.dataSourceRef`. ADR §3.6/§4.7.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "kopiur.dev",
    version = "v1alpha1",
    kind = "Restore",
    namespaced,
    status = "RestoreStatus",
    shortname = "kopiarestore",
    category = "kopiur",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RestoreSpec {
    /// Derived from `source` when omitted; REQUIRED only with `source.identity`. ADR §3.6/§4.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<RepositoryRef>,
    /// Exactly one source mode; webhook-enforced. ADR §3.6.
    pub source: RestoreSource,
    /// Absence = passive populator mode. ADR §3.6/§4.7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<RestoreTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<RestoreOptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<RestorePolicy>,
}

/// Where to restore from. Externally-tagged; exactly one variant. ADR §3.6/§4.6.
///
/// Wire shape: `source: { backupRef: {...} }`, `{ fromConfig: {...} }`, or
/// `{ identity: {...} }`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RestoreSource {
    /// A `Backup` CR (scheduled, manual, or discovered — all the same kind). Default mode.
    BackupRef(ObjectRef),
    /// A `BackupConfig` CR — resolves via identity even with no `Backup` CR present
    /// (deploy-or-restore). Default `onMissingSnapshot: Continue`. ADR §4.6.
    FromConfig(FromConfig),
    /// A raw kopia identity (foreign writers / aged-out catalog). Requires `spec.repository`.
    Identity(IdentitySource),
}

impl RestoreSource {
    /// Stable discriminant string for status/metrics.
    pub fn kind_str(&self) -> &'static str {
        match self {
            RestoreSource::BackupRef(_) => "BackupRef",
            RestoreSource::FromConfig(_) => "FromConfig",
            RestoreSource::Identity(_) => "Identity",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FromConfig {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
    /// 0 = latest, 1 = previous, ... ADR §3.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IdentitySource {
    pub username: String,
    pub hostname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    /// Renamed to match the ADR wire shape exactly (`snapshotID`, capital `ID`).
    #[serde(
        default,
        rename = "snapshotID",
        skip_serializing_if = "Option::is_none"
    )]
    pub snapshot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
}

/// Where to restore to. Externally-tagged; exactly one variant when present. ADR §3.6.
///
/// Wire shape: `target: { pvc: {...} }` or `{ pvcRef: {...} }`. Omitting `target`
/// entirely (modeled as `Option<RestoreTarget>` on the spec) is passive mode.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RestoreTarget {
    /// Operator creates the PVC.
    Pvc(PvcTemplate),
    /// Write into an existing PVC.
    PvcRef(ObjectRef),
}

impl RestoreTarget {
    /// Stable discriminant string for status/metrics.
    pub fn kind_str(&self) -> &'static str {
        match self {
            RestoreTarget::Pvc(_) => "Pvc",
            RestoreTarget::PvcRef(_) => "PvcRef",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PvcTemplate {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub access_modes: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreOptions {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub enable_file_deletion: bool,
    /// Default true; surfaces a condition if any errors occurred. ADR §4.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_permission_errors: Option<bool>,
    /// Default true. ADR §4.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_files_atomically: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestorePolicy {
    /// Default `Fail` for explicit sources; `Continue` for `fromConfig`. ADR §4.6 (G7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_missing_snapshot: Option<OnMissingSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_timeout: Option<String>,
}

/// What to do when the resolved source matches no snapshot. Closed enum. ADR §4.6 (G7).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum OnMissingSnapshot {
    /// Fail-closed; the default for explicit `backupRef`/`identity` sources.
    #[default]
    Fail,
    /// Proceed (deploy-or-restore); the default for `fromConfig`.
    Continue,
}

/// Lifecycle phase of a restore. Closed enum. ADR §3.6 status.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum RestorePhase {
    #[default]
    Pending,
    Resolving,
    Restoring,
    Completed,
    Failed,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<RestorePhase>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Pinned at admission; never re-resolved. ADR §3.6/§4.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedRestore>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<RestoreTargetStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing: Option<RestoreTiming>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<RestoreProgress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedRestore {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_ref: Option<ObjectRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<RepositoryRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<ResolvedIdentity>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreTargetStatus {
    /// Populator handshake (passive / pvc-create modes). ADR §3.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc_prime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc_ref: Option<ObjectRef>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreTiming {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreProgress {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_restored: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_restored: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::from_yaml;
    use kube::core::CustomResourceExt;

    #[test]
    fn restore_crd_metadata_is_correct() {
        let crd = Restore::crd();
        assert_eq!(crd.spec.group, "kopiur.dev");
        assert_eq!(crd.spec.names.kind, "Restore");
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn restore_backup_ref_roundtrip_matches_adr_shape() {
        // Mirrors ADR-0001 §3.6 / §5.3.
        let yaml = r#"
source:
  backupRef: { name: postgres-data-20260524-021300, namespace: billing }
target:
  pvc:
    name: postgres-data-restored
    storageClassName: fast-ssd
    capacity: 100Gi
    accessModes: [ReadWriteOnce]
options:
  enableFileDeletion: false
  ignorePermissionErrors: true
  writeFilesAtomically: true
policy:
  onMissingSnapshot: Fail
  waitTimeout: 5m
"#;
        let spec: RestoreSpec = from_yaml(yaml);
        assert_eq!(spec.source.kind_str(), "BackupRef");
        match &spec.source {
            RestoreSource::BackupRef(r) => {
                assert_eq!(r.name, "postgres-data-20260524-021300");
                assert_eq!(r.namespace.as_deref(), Some("billing"));
            }
            other => panic!("expected BackupRef, got {}", other.kind_str()),
        }
        let target = spec.target.as_ref().expect("target");
        assert_eq!(target.kind_str(), "Pvc");
        match target {
            RestoreTarget::Pvc(t) => {
                assert_eq!(t.name, "postgres-data-restored");
                assert_eq!(t.access_modes, vec!["ReadWriteOnce".to_string()]);
            }
            other => panic!("expected Pvc, got {}", other.kind_str()),
        }
        assert_eq!(
            spec.policy.as_ref().unwrap().on_missing_snapshot,
            Some(OnMissingSnapshot::Fail)
        );

        let json = serde_json::to_value(&spec).expect("serialize");
        let reparsed: RestoreSpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn restore_passive_populator_mode_has_no_target() {
        // Mirrors ADR-0001 §5.5 deploy-or-restore: fromConfig + Continue, no target.
        let yaml = r#"
source: { fromConfig: { name: postgres-data, offset: 0 } }
policy: { onMissingSnapshot: Continue }
"#;
        let spec: RestoreSpec = from_yaml(yaml);
        assert_eq!(spec.source.kind_str(), "FromConfig");
        assert!(spec.target.is_none(), "passive mode omits target");
        match &spec.source {
            RestoreSource::FromConfig(c) => {
                assert_eq!(c.name, "postgres-data");
                assert_eq!(c.offset, Some(0));
            }
            other => panic!("expected FromConfig, got {}", other.kind_str()),
        }

        let json = serde_json::to_value(&spec).unwrap();
        let reparsed: RestoreSpec = serde_json::from_value(json).unwrap();
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn restore_identity_source_requires_repository_in_practice() {
        // The `identity` source variant; spec.repository is webhook-required (not type-required).
        let yaml = r#"
repository: { kind: Repository, name: nas-primary, namespace: backups }
source:
  identity:
    username: postgres-data
    hostname: billing
    sourcePath: /data
    snapshotID: k1f1ec0a8
target:
  pvcRef: { name: postgres-data-restored }
"#;
        let spec: RestoreSpec = from_yaml(yaml);
        assert_eq!(spec.source.kind_str(), "Identity");
        assert!(spec.repository.is_some());
        match &spec.source {
            RestoreSource::Identity(i) => {
                assert_eq!(i.username, "postgres-data");
                assert_eq!(i.snapshot_id.as_deref(), Some("k1f1ec0a8"));
            }
            other => panic!("expected Identity, got {}", other.kind_str()),
        }
        assert_eq!(spec.target.as_ref().unwrap().kind_str(), "PvcRef");

        let json = serde_json::to_value(&spec).unwrap();
        let reparsed: RestoreSpec = serde_json::from_value(json).unwrap();
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn restore_source_unknown_variant_is_rejected() {
        let value: serde_json::Value = serde_yaml::from_str("snapshotUrl:\n  url: x\n").unwrap();
        assert!(serde_json::from_value::<RestoreSource>(value).is_err());
    }

    #[test]
    fn on_missing_snapshot_and_phase_serialize_to_expected_strings() {
        assert_eq!(
            serde_json::to_value(OnMissingSnapshot::Fail).unwrap(),
            "Fail"
        );
        assert_eq!(
            serde_json::to_value(OnMissingSnapshot::Continue).unwrap(),
            "Continue"
        );
        assert_eq!(
            serde_json::to_value(RestorePhase::Restoring).unwrap(),
            "Restoring"
        );
        assert_eq!(
            serde_json::to_value(RestorePhase::Completed).unwrap(),
            "Completed"
        );
    }
}
