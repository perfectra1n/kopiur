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
///
/// Origin drives the deletion-policy default (ADR §4.5): `discovered` backups are
/// forced to `Retain` because the operator did not create those snapshots.
///
/// ```
/// use kopiur_api::Origin;
///
/// assert_eq!(Origin::default(), Origin::Scheduled);
/// // Serializes camelCase, matching the `origin` label/status value.
/// assert_eq!(serde_json::to_value(Origin::Discovered).unwrap(), "discovered");
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Origin {
    /// Created by a `BackupSchedule`; spec carries `configRef`. ADR §3.4.
    #[default]
    Scheduled,
    /// Created by `kubectl create` / external automation; spec carries `configRef`. ADR §3.4.
    Manual,
    /// Materialized by the catalog scan for a snapshot kopiur didn't produce;
    /// spec is empty and `deletionPolicy` is forced to `Retain`. ADR §3.4/§4.5.
    Discovered,
}

/// Lifecycle phase of a `Backup`. Closed enum. ADR §3.4 status.
///
/// ```
/// use kopiur_api::{BackupPhase, PhaseLabel};
///
/// assert_eq!(BackupPhase::default(), BackupPhase::Pending);
/// // `PhaseLabel::label` gives the stable string used in status/metrics.
/// assert_eq!(BackupPhase::Succeeded.label(), "Succeeded");
/// // Every variant is enumerated for metric reset.
/// assert_eq!(BackupPhase::ALL.len(), 6);
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum BackupPhase {
    /// Admitted, not yet started (also the default). ADR §3.4 status.
    #[default]
    Pending,
    /// Mover Job is in flight. ADR §3.4 status.
    Running,
    /// Snapshot created successfully. ADR §3.4 status.
    Succeeded,
    /// Mover Job exhausted its retries. ADR §3.4 status.
    Failed,
    /// CR is being deleted; finalizer is reclaiming the snapshot. ADR §3.4 status/§4.5.
    Deleting,
    /// Catalog-materialized backup kopiur didn't produce. ADR §3.4 status.
    Discovered,
}

impl crate::common::PhaseLabel for BackupPhase {
    const ALL: &'static [Self] = &[
        Self::Pending,
        Self::Running,
        Self::Succeeded,
        Self::Failed,
        Self::Deleting,
        Self::Discovered,
    ];
    fn label(&self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Running => "Running",
            Self::Succeeded => "Succeeded",
            Self::Failed => "Failed",
            Self::Deleting => "Deleting",
            Self::Discovered => "Discovered",
        }
    }
}

/// Observed state of a [`Backup`]. ADR §3.4 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupStatus {
    /// Current lifecycle phase. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<BackupPhase>,
    /// Canonical origin (also mirrored to the `origin` label). ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
    /// `metadata.generation` last reconciled, for staleness detection. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The kopia artifact this CR represents. ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotInfo>,
    /// Start/end/duration of the snapshot run. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing: Option<BackupTiming>,
    /// Byte/file counts parsed from kopia's JSON output. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<BackupStats>,
    /// Present for scheduled/manual; absent for discovered. ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<JobStatus>,
    /// Frozen recipe values at run time (scheduled/manual). ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedBackup>,
    /// Standard Kubernetes conditions (e.g. `SourcesQuiesced`, `SnapshotCreated`).
    /// ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    /// Capped at ~4KB; full logs live in the Job pod. ADR §3.4/§4.10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_tail: Option<String>,
}

/// Identifies the kopia snapshot a [`Backup`] CR owns. ADR §3.4.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotInfo {
    /// kopia's snapshot ID — the handle the finalizer uses to delete content.
    ///
    /// Renamed to match the ADR wire shape exactly (`kopiaSnapshotID`, capital `ID`);
    /// serde's `camelCase` would otherwise produce `kopiaSnapshotId`.
    #[serde(rename = "kopiaSnapshotID")]
    pub kopia_snapshot_id: String,
    /// The `username@hostname:path` identity recorded for this snapshot. ADR §3.4/§4.2.
    pub identity: ResolvedIdentity,
}

/// Timing of a snapshot run. ADR §3.4 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupTiming {
    /// RFC3339 start time of the run. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    /// RFC3339 end time of the run. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    /// Wall-clock duration in seconds. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<i64>,
}

/// Stats populated from kopia's JSON output. ADR §3.4.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupStats {
    /// Total logical size of the snapshot in bytes. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<i64>,
    /// Bytes newly uploaded this run (after dedup/compression). ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_new: Option<i64>,
    /// Count of files new since the previous snapshot. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_new: Option<i64>,
    /// Count of files changed since the previous snapshot. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_modified: Option<i64>,
    /// Count of files unchanged since the previous snapshot. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_unchanged: Option<i64>,
}

/// The mover Job backing a scheduled/manual `Backup`; absent for discovered. ADR §3.4 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
    /// Name of the mover `Job`. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Number of attempts so far (bounded by `failurePolicy.backoffLimit`). ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempts: Option<i32>,
}

/// Frozen recipe values pinned at run time. ADR §3.4.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedBackup {
    /// The repository this run targeted, frozen at run time. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<RepositoryRef>,
    /// The concrete PVCs + source paths backed up this run. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<ResolvedSource>,
}

/// One resolved source backed up by a run — a concrete PVC and its kopia path. ADR §3.4 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedSource {
    /// `namespace/name` of the PVC, as kopia sees it. ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc: Option<String>,
    /// The source path kopia recorded for this PVC. ADR §3.4/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::PhaseLabel;
    use crate::testutil::from_yaml;
    use kube::core::CustomResourceExt;

    #[test]
    fn backup_phase_all_covers_every_variant_uniquely() {
        // Guards the enumerate-and-reset contract: every variant is in ALL with
        // a unique, non-empty label. A new variant added without updating ALL
        // makes this fail (and `label`'s exhaustive match won't compile at all).
        let labels: Vec<&str> = BackupPhase::ALL.iter().map(|p| p.label()).collect();
        assert_eq!(BackupPhase::ALL.len(), 6);
        assert!(labels.iter().all(|l| !l.is_empty()));
        let mut sorted = labels.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), labels.len(), "phase labels must be unique");
        // Default is reachable through ALL.
        assert!(BackupPhase::ALL.contains(&BackupPhase::default()));
    }

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
