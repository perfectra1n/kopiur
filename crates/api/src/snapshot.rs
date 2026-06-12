//! The `Snapshot` CRD — a single kopia snapshot as a Kubernetes object.
//! ADR-0001 §3.4, ADR-0003 §4.5.
//!
//! Three origins (canonical value lives in `status.origin`):
//! - `scheduled` — created by a `SnapshotSchedule`; spec carries `policyRef`.
//! - `manual`    — created by `kubectl create` / external automation; spec carries `policyRef`.
//! - `discovered`— materialized by the catalog scan; spec is empty/absent.

use crate::common::{DeletionPolicy, FailurePolicy, PolicyRef, RepositoryRef, ResolvedIdentity};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A single kopia snapshot represented as a Kubernetes object. ADR §3.4.
///
/// For `scheduled`/`manual` backups the spec carries `policyRef` (+ optional
/// overrides). For `discovered` backups the spec is empty — every field is optional.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "kopiur.home-operations.com",
    version = "v1alpha1",
    kind = "Snapshot",
    namespaced,
    status = "SnapshotStatus",
    shortname = "kopiasnap",
    category = "kopiur",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Origin","type":"string","jsonPath":".status.origin"}"#,
    printcolumn = r#"{"name":"Snapshot","type":"string","jsonPath":".status.snapshot.kopiaSnapshotID"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotSpec {
    /// The recipe to run. Absent for `discovered` backups. ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_ref: Option<PolicyRef>,
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
    /// Pin this snapshot to exempt it from GFS retention (ADR-0005 §13(c)). When
    /// `true` the reconciler applies a kopia snapshot pin and the GFS pruner never
    /// selects it for deletion — for pre-migration / compliance holds. Clearing it
    /// removes the pin. Default `false`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub pin: bool,
}

/// How a `Snapshot` came to exist. Canonical value mirrored from the `kopiur.home-operations.com/origin`
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
    /// Created by a `SnapshotSchedule`; spec carries `policyRef`. ADR §3.4.
    #[default]
    Scheduled,
    /// Created by `kubectl create` / external automation; spec carries `policyRef`. ADR §3.4.
    Manual,
    /// Materialized by the catalog scan for a snapshot kopiur didn't produce;
    /// spec is empty and `deletionPolicy` is forced to `Retain`. ADR §3.4/§4.5.
    Discovered,
}

/// Lifecycle phase of a `Snapshot`. Closed enum. ADR §3.4 status.
///
/// ```
/// use kopiur_api::{SnapshotPhase, PhaseLabel};
///
/// assert_eq!(SnapshotPhase::default(), SnapshotPhase::Pending);
/// // `PhaseLabel::label` gives the stable string used in status/metrics.
/// assert_eq!(SnapshotPhase::Succeeded.label(), "Succeeded");
/// // Every variant is enumerated for metric reset.
/// assert_eq!(SnapshotPhase::ALL.len(), 6);
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum SnapshotPhase {
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

impl Origin {
    /// The stable wire/label value (the serde camelCase encoding), for the
    /// `kopiur.home-operations.com/origin` label and `status.origin` — single
    /// definition so producers (controller, kubectl plugin) cannot drift.
    pub fn label_value(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Manual => "manual",
            Self::Discovered => "discovered",
        }
    }
}

impl crate::common::PhaseLabel for SnapshotPhase {
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

/// Observed state of a [`Snapshot`]. ADR §3.4 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotStatus {
    /// Current lifecycle phase. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<SnapshotPhase>,
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
    pub timing: Option<SnapshotTiming>,
    /// Byte/file counts parsed from kopia's JSON output. ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<SnapshotStats>,
    /// Present for scheduled/manual; absent for discovered. ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<JobStatus>,
    /// Frozen recipe values at run time (scheduled/manual). ADR §3.4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedSnapshot>,
    /// Standard Kubernetes conditions (e.g. `SourcesQuiesced`, `SnapshotCreated`).
    /// ADR §3.4 status.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    /// The last lines of the run's output, written by the mover at the terminal
    /// transition (success: the `Snapshot created: <id>` line; failure: the
    /// actionable error + kopia stderr tail). Capped at
    /// [`crate::common::MAX_LOG_TAIL_BYTES`]; full logs live in the Job pod.
    /// ADR §3.4/§4.10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_tail: Option<String>,
    /// Structured terminal-failure detail (kopia error class, stderr tail, retry
    /// hint), written by the mover before it exits non-zero. ADR §4.10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<crate::common::FailureBlock>,
    /// The observed kopia-side pin state (ADR-0005 §13(c)): `Some(true)` once the
    /// operator has applied the pin, `Some(false)` once it has removed it, `None`
    /// before any pin reconcile. The reconciler compares `spec.pin` against this to
    /// decide whether to issue a `kopia snapshot pin`/`unpin`, so a redundant op is
    /// never spawned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pinned: Option<bool>,
    /// Hook-execution bookkeeping (ADR §4.8): completion timestamps the
    /// reconciler stamps so each hook list runs exactly once per Snapshot across
    /// requeues and controller restarts (hooks have side effects — quiesce,
    /// resume — that must not repeat).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hooks: Option<HookExecutionStatus>,
    /// The CSI staging objects the run created for `copyMethod: Snapshot`/`Clone`
    /// (ADR §3.3). Pinned once when the stage is provisioned so the reconciler can
    /// (a) reuse the same VolumeSnapshot/PVC across mover-Job retries idempotently
    /// and (b) reap them on the terminal transition. Absent for `Direct` (and NFS),
    /// which mount the live source with no staging.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged: Option<StagedSources>,
}

/// The CSI staging objects a backup created so kopia reads a point-in-time copy
/// instead of the live source PVC (`copyMethod: Snapshot`/`Clone`, ADR §3.3).
///
/// Recorded once the stage is provisioned (stable values — never re-stamped, per the
/// status-churn rule) so the controller reaps exactly these objects on completion and
/// never double-creates them across requeues / controller restarts.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StagedSources {
    /// The resolved capture method (`Snapshot` or `Clone`) that produced this stage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy_method: Option<String>,
    /// Name of the `VolumeSnapshot` created from the source PVC (`copyMethod: Snapshot`
    /// only; absent for `Clone`, which stages directly from the source PVC).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_snapshot_name: Option<String>,
    /// Name of the staged `PersistentVolumeClaim` the mover mounts in place of the
    /// live source PVC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc_name: Option<String>,
    /// `true` once the stage is ready for the mover (VolumeSnapshot `readyToUse` and
    /// the staged PVC applied). Before that the reconcile is still provisioning it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready: Option<bool>,
}

/// When each hook list completed (ADR §4.8). Written once per list, at the
/// transition — never re-stamped — per the status-churn rule.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HookExecutionStatus {
    /// When the `beforeSnapshot` list completed (RFC3339); absent until it has.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_completed_at: Option<String>,
    /// When the `afterSnapshot` list completed (RFC3339); absent until it has.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub post_completed_at: Option<String>,
}

/// Identifies the kopia snapshot a [`Snapshot`] CR owns. ADR §3.4.
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
pub struct SnapshotTiming {
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
pub struct SnapshotStats {
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

/// The mover Job backing a scheduled/manual `Snapshot`; absent for discovered. ADR §3.4 status.
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
pub struct ResolvedSnapshot {
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

/// Derive the repository a `Snapshot` belongs to: a *produced* snapshot pins it
/// in `status.resolved.repository`; a *discovered* snapshot carries its
/// `Repository`/`ClusterRepository` as the controller `ownerReference` (it has
/// no `resolved` block). Pure. Shared by the `Restore` reconciler
/// (`spec.repository` derivation for `snapshotRef`) and the `kubectl kopiur`
/// browse data-plane, so the derivation rule cannot fork.
pub fn repository_ref_for(snap: &Snapshot) -> Option<RepositoryRef> {
    use crate::common::RepositoryKind;
    if let Some(rref) = snap
        .status
        .as_ref()
        .and_then(|s| s.resolved.as_ref())
        .and_then(|r| r.repository.clone())
    {
        return Some(rref);
    }
    let owners = snap
        .metadata
        .owner_references
        .as_deref()
        .unwrap_or_default();
    owners.iter().find_map(|o| {
        if o.api_version != crate::consts::API_VERSION {
            return None;
        }
        let kind = match o.kind.as_str() {
            "Repository" => RepositoryKind::Repository,
            "ClusterRepository" => RepositoryKind::ClusterRepository,
            _ => return None,
        };
        Some(RepositoryRef {
            kind,
            name: o.name.clone(),
            // Absent = resolved relative to the Snapshot's own namespace.
            namespace: None,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::PhaseLabel;
    use crate::testutil::from_yaml;
    use kube::core::CustomResourceExt;

    #[test]
    fn origin_label_value_matches_the_serde_encoding() {
        for origin in [Origin::Scheduled, Origin::Manual, Origin::Discovered] {
            assert_eq!(
                serde_json::to_value(origin).unwrap(),
                origin.label_value(),
                "{origin:?}"
            );
        }
    }

    #[test]
    fn backup_phase_all_covers_every_variant_uniquely() {
        // Guards the enumerate-and-reset contract: every variant is in ALL with
        // a unique, non-empty label. A new variant added without updating ALL
        // makes this fail (and `label`'s exhaustive match won't compile at all).
        let labels: Vec<&str> = SnapshotPhase::ALL.iter().map(|p| p.label()).collect();
        assert_eq!(SnapshotPhase::ALL.len(), 6);
        assert!(labels.iter().all(|l| !l.is_empty()));
        let mut sorted = labels.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), labels.len(), "phase labels must be unique");
        // Default is reachable through ALL.
        assert!(SnapshotPhase::ALL.contains(&SnapshotPhase::default()));
    }

    /// Regression for the inert "derived from source" contract (found by the
    /// kubectl-plugin e2e): a snapshotRef Restore with no spec.repository was
    /// refused with "restore requires spec.repository" even though the CRD
    /// documents derivation. The pure derivation must cover both snapshot
    /// origins. (Moved here from the controller when the browse data-plane
    /// started sharing it.)
    mod repository_derivation {
        use super::super::repository_ref_for;
        use crate::Snapshot;
        use crate::common::RepositoryKind;

        fn snap(v: serde_json::Value) -> Snapshot {
            serde_json::from_value(v).expect("snapshot fixture")
        }

        #[test]
        fn produced_snapshot_uses_the_pinned_resolved_repository() {
            let s = snap(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Snapshot",
                "metadata": { "name": "s", "namespace": "media" },
                "spec": { "policyRef": { "name": "pol" } },
                "status": { "resolved": { "repository": { "kind": "ClusterRepository", "name": "nas" } } }
            }));
            let rref = repository_ref_for(&s).expect("derived");
            assert_eq!(rref.kind, RepositoryKind::ClusterRepository);
            assert_eq!(rref.name, "nas");
        }

        #[test]
        fn discovered_snapshot_uses_the_owning_repository() {
            for (kind_str, kind) in [
                ("Repository", RepositoryKind::Repository),
                ("ClusterRepository", RepositoryKind::ClusterRepository),
            ] {
                let s = snap(serde_json::json!({
                    "apiVersion": "kopiur.home-operations.com/v1alpha1",
                    "kind": "Snapshot",
                    "metadata": {
                        "name": "repo-disc-abc", "namespace": "media",
                        "ownerReferences": [{
                            "apiVersion": "kopiur.home-operations.com/v1alpha1",
                            "kind": kind_str, "name": "nas", "uid": "u1", "controller": true
                        }]
                    },
                    "spec": {},
                    "status": { "phase": "Discovered", "origin": "discovered" }
                }));
                let rref = repository_ref_for(&s).expect(kind_str);
                assert_eq!(rref.kind, kind, "{kind_str}");
                assert_eq!(rref.name, "nas");
                assert_eq!(rref.namespace, None, "resolved relative to the snapshot ns");
            }
        }

        #[test]
        fn foreign_owners_and_bare_snapshots_derive_nothing() {
            // A non-kopiur owner (e.g. a Job) must not be mistaken for a repository.
            let s = snap(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Snapshot",
                "metadata": {
                    "name": "s", "namespace": "media",
                    "ownerReferences": [{
                        "apiVersion": "batch/v1", "kind": "Job", "name": "j", "uid": "u2"
                    }]
                },
                "spec": {}
            }));
            assert!(repository_ref_for(&s).is_none());
        }
    }

    #[test]
    fn backup_crd_metadata_is_correct() {
        let crd = Snapshot::crd();
        assert_eq!(crd.spec.group, "kopiur.home-operations.com");
        assert_eq!(crd.spec.names.kind, "Snapshot");
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn backup_manual_roundtrip_matches_adr_shape() {
        // Mirrors ADR-0001 §3.4 spec block + §5.6.
        let yaml = r#"
policyRef: { name: postgres-data }
tags:
  reason: "scheduled-nightly"
failurePolicy:
  backoffLimit: 2
  activeDeadlineSeconds: 7200
deletionPolicy: Delete
"#;
        let spec: SnapshotSpec = from_yaml(yaml);
        assert_eq!(spec.policy_ref.as_ref().unwrap().name, "postgres-data");
        assert_eq!(spec.tags.as_ref().unwrap()["reason"], "scheduled-nightly");
        assert_eq!(spec.failure_policy.as_ref().unwrap().backoff_limit, Some(2));
        assert_eq!(spec.deletion_policy, Some(DeletionPolicy::Delete));

        let json = serde_json::to_value(&spec).expect("serialize");
        let reparsed: SnapshotSpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn backup_discovered_spec_is_empty() {
        // Discovered backups carry no spec fields.
        let spec: SnapshotSpec = from_yaml("{}\n");
        assert!(spec.policy_ref.is_none());
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
            serde_json::to_value(SnapshotPhase::Succeeded).unwrap(),
            "Succeeded"
        );
        assert_eq!(
            serde_json::to_value(SnapshotPhase::Deleting).unwrap(),
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
        let status: SnapshotStatus = from_yaml(yaml);
        assert_eq!(status.phase, Some(SnapshotPhase::Succeeded));
        assert_eq!(status.origin, Some(Origin::Scheduled));
        assert_eq!(
            status.snapshot.as_ref().unwrap().kopia_snapshot_id,
            "k1f1ec0a8"
        );
        assert_eq!(status.stats.as_ref().unwrap().size_bytes, Some(4321098765));

        let json = serde_json::to_value(&status).unwrap();
        let reparsed: SnapshotStatus = serde_json::from_value(json).unwrap();
        assert_eq!(status, reparsed);
    }
}
