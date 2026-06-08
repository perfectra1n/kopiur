//! The `Restore` CRD — a restore from a snapshot/identity to a PVC, or a passive
//! populator source. ADR-0001 §3.6, ADR-0003 §4.6.

use crate::common::{CredentialProjection, ObjectRef, RepositoryRef, ResolvedIdentity};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A restore operation. `target` is optional: absence = passive populator mode,
/// consumed by a PVC's `spec.dataSourceRef`. ADR §3.6/§4.7.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "kopiur.home-operations.com",
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
/// Desired state of a [`Restore`]: where to read from, where to write to, and how
/// to behave when the snapshot is missing. ADR §3.6/§4.6.
pub struct RestoreSpec {
    /// Derived from `source` when omitted; REQUIRED only with `source.identity`. ADR §3.6/§4.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<RepositoryRef>,
    /// Exactly one source mode; webhook-enforced. ADR §3.6.
    pub source: RestoreSource,
    /// Absence = passive populator mode. ADR §3.6/§4.7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<RestoreTarget>,
    /// kopia restore behavior (file deletion, permission/atomicity handling). ADR §4.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<RestoreOptions>,
    /// Missing-snapshot handling and wait timeout. ADR §4.6 (G7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<RestorePolicy>,
    /// Opt-in credential-Secret projection for this restore's mover (default off).
    /// When `enabled: true`, the operator copies the referenced repository's
    /// credential Secret(s) into the restore mover's namespace (a no-op when they
    /// already live there) — so restoring from a shared `ClusterRepository` into a
    /// fresh namespace need not pre-create the Secret there. ADR §4.11.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_projection: Option<CredentialProjection>,
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
    ///
    /// ```
    /// use kopiur_api::common::ObjectRef;
    /// use kopiur_api::restore::RestoreSource;
    ///
    /// let src = RestoreSource::BackupRef(ObjectRef { name: "pg-20260524".into(), namespace: None });
    /// assert_eq!(src.kind_str(), "BackupRef");
    ///
    /// // Externally tagged: each variant deserializes under its own camelCase key.
    /// let from_cfg: RestoreSource =
    ///     serde_json::from_value(serde_json::json!({ "fromConfig": { "name": "pg" } })).unwrap();
    /// assert_eq!(from_cfg.kind_str(), "FromConfig");
    /// ```
    pub fn kind_str(&self) -> &'static str {
        match self {
            RestoreSource::BackupRef(_) => "BackupRef",
            RestoreSource::FromConfig(_) => "FromConfig",
            RestoreSource::Identity(_) => "Identity",
        }
    }
}

/// The `fromConfig` source: resolve a snapshot via a `BackupConfig`'s identity,
/// even when no `Backup` CR exists yet (deploy-or-restore). ADR §4.6.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FromConfig {
    /// Name of the `BackupConfig` whose identity selects the snapshot.
    pub name: String,
    /// Namespace of the `BackupConfig`; absent = the `Restore`'s own namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Restore the newest snapshot at or before this RFC3339 timestamp (point-in-time).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
    /// 0 = latest, 1 = previous, ... ADR §3.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
}

/// The `identity` source: a raw kopia `username@hostname:path` identity for
/// foreign writers or snapshots aged out of the catalog. Requires `spec.repository`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IdentitySource {
    /// The kopia `username` to match.
    pub username: String,
    /// The kopia `hostname` to match.
    pub hostname: String,
    /// The kopia source path to match; absent matches any path for the identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    /// Pin an exact kopia snapshot by ID. Renamed to match the ADR wire shape
    /// exactly (`snapshotID`, capital `ID`).
    #[serde(
        default,
        rename = "snapshotID",
        skip_serializing_if = "Option::is_none"
    )]
    pub snapshot_id: Option<String>,
    /// Restore the newest snapshot at or before this RFC3339 timestamp (point-in-time).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
    /// 0 = latest, 1 = previous, ... mutually exclusive with `snapshotID`/`asOf` in practice.
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
    ///
    /// ```
    /// use kopiur_api::common::ObjectRef;
    /// use kopiur_api::restore::RestoreTarget;
    ///
    /// let into_existing = RestoreTarget::PvcRef(ObjectRef { name: "data".into(), namespace: None });
    /// assert_eq!(into_existing.kind_str(), "PvcRef");
    ///
    /// // Externally tagged: `{ pvc: {...} }` selects the create-PVC variant.
    /// let created: RestoreTarget =
    ///     serde_json::from_value(serde_json::json!({ "pvc": { "name": "restored" } })).unwrap();
    /// assert_eq!(created.kind_str(), "Pvc");
    /// ```
    pub fn kind_str(&self) -> &'static str {
        match self {
            RestoreTarget::Pvc(_) => "Pvc",
            RestoreTarget::PvcRef(_) => "PvcRef",
        }
    }
}

/// Template for a PVC the operator creates as the restore target. ADR §3.6.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PvcTemplate {
    /// Name of the PVC to create.
    pub name: String,
    /// StorageClass for the new PVC; absent uses the cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
    /// Requested size of the new PVC (e.g. `100Gi`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<String>,
    /// Access modes for the new PVC (e.g. `["ReadWriteOnce"]`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub access_modes: Vec<String>,
}

/// kopia restore behavior knobs. ADR §4.6.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreOptions {
    /// Delete files in the target that are not present in the snapshot (make the
    /// target an exact mirror). Off by default — additive restore is the safe default.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub enable_file_deletion: bool,
    /// Default true; surfaces a condition if any errors occurred. ADR §4.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_permission_errors: Option<bool>,
    /// Default true. ADR §4.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_files_atomically: Option<bool>,
}

/// How the restore reacts to a missing snapshot and how long it waits. ADR §4.6 (G7).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestorePolicy {
    /// Default `Fail` for explicit sources; `Continue` for `fromConfig`. ADR §4.6 (G7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_missing_snapshot: Option<OnMissingSnapshot>,
    /// How long to wait for the source snapshot to appear before giving up
    /// (Go-style duration string, e.g. `5m`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_timeout: Option<String>,
}

/// What to do when the resolved source matches no snapshot. Closed enum. ADR §4.6 (G7).
///
/// ```
/// use kopiur_api::restore::OnMissingSnapshot;
///
/// // Fail-closed is the default so an explicit restore can't silently no-op.
/// assert_eq!(OnMissingSnapshot::default(), OnMissingSnapshot::Fail);
/// assert_eq!(serde_json::to_value(OnMissingSnapshot::Continue).unwrap(), "Continue");
/// ```
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
    /// Admitted but not yet acted on; the default initial phase.
    #[default]
    Pending,
    /// Resolving the source to a concrete snapshot and pinning it to status.
    Resolving,
    /// The mover `Job` is actively writing data into the target.
    Restoring,
    /// The restore finished successfully.
    Completed,
    /// The restore terminally failed; see `conditions` for the reason.
    Failed,
}

impl crate::common::PhaseLabel for RestorePhase {
    const ALL: &'static [Self] = &[
        Self::Pending,
        Self::Resolving,
        Self::Restoring,
        Self::Completed,
        Self::Failed,
    ];
    fn label(&self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Resolving => "Resolving",
            Self::Restoring => "Restoring",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
        }
    }
}

/// Observed state of a [`Restore`], written by the controller/mover. ADR §3.6 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreStatus {
    /// Current lifecycle phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<RestorePhase>,
    /// `metadata.generation` last reconciled, so stale status is detectable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Pinned at admission; never re-resolved. ADR §3.6/§4.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedRestore>,
    /// Resolved target details (the PVC written to / populator handshake).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<RestoreTargetStatus>,
    /// Start/end timestamps for the restore run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing: Option<RestoreTiming>,
    /// Bytes/files restored so far, patched periodically by the mover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<RestoreProgress>,
    /// Standard Kubernetes conditions carrying the human-readable status/reason.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// The source resolved and pinned at admission, so a restore never silently retargets. ADR §4.6.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedRestore {
    /// The concrete `Backup` CR the source resolved to, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_ref: Option<ObjectRef>,
    /// The repository the snapshot lives in, resolved from the source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<RepositoryRef>,
    /// Timestamp at which the source was pinned (RFC3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned_at: Option<String>,
    /// The resolved kopia identity (`username@hostname:path`) of the snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<ResolvedIdentity>,
}

/// Resolved restore target details written to status. ADR §3.6.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreTargetStatus {
    /// Populator handshake (passive / pvc-create modes). ADR §3.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc_prime: Option<String>,
    /// The PVC actually written to (created or pre-existing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc_ref: Option<ObjectRef>,
}

/// Start/end timestamps of a restore run. ADR §3.6 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreTiming {
    /// When the mover began restoring (RFC3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    /// When the restore reached a terminal phase (RFC3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
}

/// Live progress counters patched by the mover during a restore. ADR §3.6 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreProgress {
    /// Total bytes restored so far.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_restored: Option<i64>,
    /// Total files restored so far.
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
        assert_eq!(crd.spec.group, "kopiur.home-operations.com");
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
