//! The `Backup` CRD — a single kopia snapshot as a Kubernetes object.
//! ADR-0001 §3.4, ADR-0003 §4.5.
//!
//! Three origins (canonical value lives in `status.origin`):
//! - `scheduled` — created by a `BackupSchedule`; spec carries `configRef`.
//! - `manual`    — created by `kubectl create` / external automation; spec carries `configRef`.
//! - `discovered`— materialized by the catalog scan; spec is empty/absent.

use crate::common::{ConfigRef, DeletionPolicy, FailurePolicy, RepositoryRef, ResolvedIdentity};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A single kopia snapshot represented as a Kubernetes object. ADR §3.4.
///
/// For `scheduled`/`manual` backups the spec carries `configRef` (+ optional
/// overrides). For `discovered` backups the spec is empty — every field is optional.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "kopiur.home-operations.com",
    version = "v1alpha1",
    kind = "Backup",
    namespaced,
    status = "BackupStatus",
    shortname = "kopiabak",
    category = "kopiur",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Origin","type":"string","jsonPath":".status.origin"}"#,
    printcolumn = r#"{"name":"Snapshot","type":"string","jsonPath":".status.snapshot.kopiaSnapshotID"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BackupSpec {
    /// The recipe to run. Absent for `discovered` backups. ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_ref: Option<ConfigRef>,
    /// Arbitrary kopia snapshot tags (e.g. `reason: scheduled-nightly`). ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<BTreeMap<String, String>>,
    /// Per-run failure controls passed to the mover `Job`. ADR §3.4 (G6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_policy: Option<FailurePolicy>,
    /// Lifecycle of the underlying snapshot when this CR is deleted. Origin-aware
    /// default (§4.5): `Delete` for scheduled/manual, forced `Retain` for discovered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletion_policy: Option<DeletionPolicy>,
}

/// How a `Backup` came to exist. Canonical value mirrored from the `kopiur.home-operations.com/origin`
/// label. Closed enum. ADR §3.4.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Origin {
    #[default]
    Scheduled,
    Manual,
    Discovered,
}

/// Lifecycle phase of a `Backup`. Closed enum. ADR §3.4 status.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum BackupPhase {
    #[default]
    Pending,
    Running,
    Succeeded,
    Failed,
    Deleting,
    Discovered,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<BackupPhase>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The kopia artifact this CR represents. ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing: Option<BackupTiming>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<BackupStats>,
    /// Present for scheduled/manual; absent for discovered. ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<JobStatus>,
    /// Frozen recipe values at run time (scheduled/manual). ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedBackup>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    /// Capped at ~4KB; full logs live in the Job pod. ADR §3.4/§4.10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_tail: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotInfo {
    /// Renamed to match the ADR wire shape exactly (`kopiaSnapshotID`, capital `ID`);
    /// serde's `camelCase` would otherwise produce `kopiaSnapshotId`.
    #[serde(rename = "kopiaSnapshotID")]
    pub kopia_snapshot_id: String,
    pub identity: ResolvedIdentity,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupTiming {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<i64>,
}

/// Stats populated from kopia's JSON output. ADR §3.4.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupStats {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_new: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_new: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_modified: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_unchanged: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempts: Option<i32>,
}

/// Frozen recipe values pinned at run time. ADR §3.4.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedBackup {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<RepositoryRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<ResolvedSource>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedSource {
    /// `namespace/name` of the PVC, as kopia sees it. ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::from_yaml;
    use kube::core::CustomResourceExt;

    #[test]
    fn backup_crd_metadata_is_correct() {
        let crd = Backup::crd();
        assert_eq!(crd.spec.group, "kopiur.home-operations.com");
        assert_eq!(crd.spec.names.kind, "Backup");
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn backup_manual_roundtrip_matches_adr_shape() {
        // Mirrors ADR-0001 §3.4 spec block + §5.6.
        let yaml = r#"
configRef: { name: postgres-data }
tags:
  reason: "scheduled-nightly"
failurePolicy:
  backoffLimit: 2
  activeDeadlineSeconds: 7200
deletionPolicy: Delete
"#;
        let spec: BackupSpec = from_yaml(yaml);
        assert_eq!(spec.config_ref.as_ref().unwrap().name, "postgres-data");
        assert_eq!(spec.tags.as_ref().unwrap()["reason"], "scheduled-nightly");
        assert_eq!(spec.failure_policy.as_ref().unwrap().backoff_limit, Some(2));
        assert_eq!(spec.deletion_policy, Some(DeletionPolicy::Delete));

        let json = serde_json::to_value(&spec).expect("serialize");
        let reparsed: BackupSpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn backup_discovered_spec_is_empty() {
        // Discovered backups carry no spec fields.
        let spec: BackupSpec = from_yaml("{}\n");
        assert!(spec.config_ref.is_none());
        assert!(spec.deletion_policy.is_none());
        // Empty spec serializes to an empty object (all fields skip).
        assert_eq!(serde_json::to_value(&spec).unwrap(), serde_json::json!({}));
    }

    #[test]
    fn deletion_policy_serializes_to_expected_strings() {
        assert_eq!(
            serde_json::to_value(DeletionPolicy::Delete).unwrap(),
            "Delete"
        );
        assert_eq!(
            serde_json::to_value(DeletionPolicy::Retain).unwrap(),
            "Retain"
        );
        assert_eq!(
            serde_json::to_value(DeletionPolicy::Orphan).unwrap(),
            "Orphan"
        );
        // DeletionPolicy is Copy (ADR-0003 §4.5).
        let p = DeletionPolicy::Retain;
        let _copy = p;
        assert_eq!(p, DeletionPolicy::Retain);
    }

    #[test]
    fn origin_and_phase_serialize_to_expected_strings() {
        assert_eq!(
            serde_json::to_value(Origin::Scheduled).unwrap(),
            "scheduled"
        );
        assert_eq!(serde_json::to_value(Origin::Manual).unwrap(), "manual");
        assert_eq!(
            serde_json::to_value(Origin::Discovered).unwrap(),
            "discovered"
        );
        assert_eq!(
            serde_json::to_value(BackupPhase::Succeeded).unwrap(),
            "Succeeded"
        );
        assert_eq!(
            serde_json::to_value(BackupPhase::Deleting).unwrap(),
            "Deleting"
        );
    }

    #[test]
    fn backup_status_roundtrips() {
        // Mirrors ADR-0001 §3.4 status block.
        let yaml = r#"
phase: Succeeded
origin: scheduled
snapshot:
  kopiaSnapshotID: k1f1ec0a8
  identity:
    username: postgres-data
    hostname: billing
    sourcePath: /data
timing:
  startTime: 2026-05-24T02:13:00Z
  endTime: 2026-05-24T02:18:42Z
  durationSeconds: 342
stats:
  sizeBytes: 4321098765
  bytesNew: 12345678
  filesNew: 1233
resolved:
  repository: { kind: Repository, name: nas-primary, namespace: backups }
  sources:
    - pvc: billing/postgres-data
      sourcePath: /data
logTail: "Snapshot created: k1f1ec0a8"
"#;
        let status: BackupStatus = from_yaml(yaml);
        assert_eq!(status.phase, Some(BackupPhase::Succeeded));
        assert_eq!(status.origin, Some(Origin::Scheduled));
        assert_eq!(
            status.snapshot.as_ref().unwrap().kopia_snapshot_id,
            "k1f1ec0a8"
        );
        assert_eq!(status.stats.as_ref().unwrap().size_bytes, Some(4321098765));

        let json = serde_json::to_value(&status).unwrap();
        let reparsed: BackupStatus = serde_json::from_value(json).unwrap();
        assert_eq!(status, reparsed);
    }
}
