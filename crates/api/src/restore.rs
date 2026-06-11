//! The `Restore` CRD — a restore from a snapshot/identity to a PVC, or a passive
//! populator source. ADR-0001 §3.6, ADR-0003 §4.6.

use crate::common::{
    CredentialProjection, FailurePolicy, MoverSpec, ObjectRef, RepositoryRef, ResolvedIdentity,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A restore operation. `target` is **required** (ADR-0005 §9): explicit
/// `pvc`/`pvcRef`, or `populator: {}` for passive populator mode (consumed by a
/// PVC's `spec.dataSourceRef`). The empty-`target` form is removed. ADR §3.6/§4.7.
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
    printcolumn = r#"{"name":"Source","type":"string","jsonPath":".status.sourceKind"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
// §15: operator-authored CEL in the CRD schema — exactly one of
// target.pvc/target.pvcRef/target.populator. Validates in the apiserver + CI
// (`kubeconform`), complementing the webhook. `target` is required so `has(self.target)`
// is always true; the rule counts the present sub-keys.
#[schemars(extend("x-kubernetes-validations" = [{
    "rule": "[has(self.target.pvc), has(self.target.pvcRef), has(self.target.populator)].filter(x, x).size() == 1",
    "message": "exactly one of target.pvc, target.pvcRef, target.populator"
}]))]
#[serde(rename_all = "camelCase")]
/// Desired state of a [`Restore`]: where to read from, where to write to, and how
/// to behave when the snapshot is missing. ADR §3.6/§4.6.
pub struct RestoreSpec {
    /// Derived from `source` when omitted; REQUIRED only with `source.identity`. ADR §3.6/§4.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<RepositoryRef>,
    /// Exactly one source mode; webhook-enforced. ADR §3.6.
    pub source: RestoreSource,
    /// Where to restore to — **required** (ADR-0005 §9): `pvc` (operator creates),
    /// `pvcRef` (existing PVC), or `populator: {}` (passive populator mode, claimed
    /// via a PVC's `spec.dataSourceRef`). The former empty-`target` form is removed;
    /// a `Restore` with no `target` is now invalid (deserialization fails).
    pub target: RestoreTarget,
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
    /// Per-run mover overrides for this restore's Job — resource requests/limits,
    /// kopia cache sizing, container `securityContext` (UID/GID match), `privilegedMode`,
    /// and `inheritSecurityContextFrom`. The same surface `SnapshotPolicy.spec.mover`
    /// gives a backup. An elevated context is namespace-gated exactly like a backup's
    /// (ADR §4.11/§G16); `securityContext` and `inheritSecurityContextFrom` are
    /// mutually exclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mover: Option<MoverSpec>,
    /// Mover `Job` retry/deadline limits (`backoffLimit`, `activeDeadlineSeconds`),
    /// mirroring `Snapshot.spec.failurePolicy`. Absent uses the ADR defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_policy: Option<FailurePolicy>,
}

/// Where to restore from. Externally-tagged; exactly one variant. ADR §3.6/§4.6.
///
/// Wire shape: `source: { snapshotRef: {...} }`, `{ fromPolicy: {...} }`, or
/// `{ identity: {...} }`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RestoreSource {
    /// A `Snapshot` CR (scheduled, manual, or discovered — all the same kind). Default mode.
    SnapshotRef(ObjectRef),
    /// A `SnapshotPolicy` CR — resolves via identity even with no `Snapshot` CR present
    /// (deploy-or-restore). Default `onMissingSnapshot: Continue`. ADR §4.6.
    FromPolicy(FromPolicy),
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
    /// let src = RestoreSource::SnapshotRef(ObjectRef { name: "pg-20260524".into(), namespace: None });
    /// assert_eq!(src.kind_str(), "SnapshotRef");
    ///
    /// // Externally tagged: each variant deserializes under its own camelCase key.
    /// let from_cfg: RestoreSource =
    ///     serde_json::from_value(serde_json::json!({ "fromPolicy": { "name": "pg" } })).unwrap();
    /// assert_eq!(from_cfg.kind_str(), "FromPolicy");
    /// ```
    pub fn kind_str(&self) -> &'static str {
        match self {
            RestoreSource::SnapshotRef(_) => "SnapshotRef",
            RestoreSource::FromPolicy(_) => "FromPolicy",
            RestoreSource::Identity(_) => "Identity",
        }
    }
}

/// The `fromPolicy` source: resolve a snapshot via a `SnapshotPolicy`'s identity,
/// even when no `Snapshot` CR exists yet (deploy-or-restore). ADR §4.6.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FromPolicy {
    /// Name of the `SnapshotPolicy` whose identity selects the snapshot.
    pub name: String,
    /// Namespace of the `SnapshotPolicy`; absent = the `Restore`'s own namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Restore the newest snapshot at or before this RFC3339 timestamp (point-in-time).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
    /// 0 = latest, 1 = previous, ... ADR §3.6.
    ///
    /// Carries a real OpenAPI `default: 0` (ADR-0005 §1): unconditional, so it
    /// materializes into the stored object / `kubectl explain` and GitOps stops
    /// diff-thrashing. A bare `i64` (not `Option`) so the default is never dropped.
    #[serde(default = "default_offset")]
    #[schemars(default = "default_offset")]
    pub offset: i64,
}

/// serde/schemars `default` for [`FromPolicy::offset`] — `0`, the latest snapshot
/// (ADR-0005 §1). A named fn so it backs BOTH `#[serde(default = ...)]` and
/// `#[schemars(default = ...)]`, which is what makes schemars 1 emit the OpenAPI
/// `default:` in the generated CRD schema.
fn default_offset() -> i64 {
    0
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

/// Where to restore to. Externally-tagged; exactly one variant. ADR §3.6 / ADR-0005 §9.
///
/// Wire shape: `target: { pvc: {...} }`, `{ pvcRef: {...} }`, or `{ populator: {} }`.
/// The `populator` variant is **explicit** passive-populator mode (claimed via a
/// PVC's `spec.dataSourceRef`); the former empty-`target` form is removed (ADR-0005 §9).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RestoreTarget {
    /// Operator creates the PVC.
    Pvc(PvcTemplate),
    /// Write into an existing PVC.
    PvcRef(ObjectRef),
    /// Passive populator mode: no workload target at provision time — the restore is
    /// claimed by a PVC's `spec.dataSourceRef` (ADR-0005 §9). An (empty) sub-object so
    /// future populator knobs slot in without API breakage.
    Populator(PopulatorTarget),
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
            RestoreTarget::Populator(_) => "Populator",
        }
    }
}

/// Passive-populator target marker (ADR-0005 §9). An empty sub-object — its presence
/// selects populator mode; fields slot in later without an API break. Wire: `{}`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PopulatorTarget {}

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
    /// Default `Fail` for explicit sources; `Continue` for `fromPolicy`. ADR §4.6 (G7).
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
    /// Fail-closed; the default for explicit `snapshotRef`/`identity` sources.
    #[default]
    Fail,
    /// Proceed (deploy-or-restore); the default for `fromPolicy`.
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
    /// The pinned source kind (`RestoreSource::kind_str` — `SnapshotRef`/`FromPolicy`/
    /// `Identity`), set by the reconciler. Backs the `SOURCE` printer column so
    /// `kubectl get restores` shows where a restore reads from. ADR-0005 §3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_kind: Option<String>,
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
    /// The last lines of the run's output, written by the mover at the terminal
    /// transition (success: the `Restore completed: snapshot <id>` line; failure:
    /// the actionable error + kopia stderr tail). Capped at
    /// [`crate::common::MAX_LOG_TAIL_BYTES`]; full logs live in the Job pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_tail: Option<String>,
    /// Structured terminal-failure detail (kopia error class, stderr tail, retry
    /// hint), written by the mover before it exits non-zero. ADR §4.10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<crate::common::FailureBlock>,
}

/// The source resolved and pinned at admission, so a restore never silently retargets. ADR §4.6.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedRestore {
    /// The exact kopia snapshot manifest id the source resolved to. Pinned once;
    /// subsequent reconciles restore THIS id even if newer snapshots appear, so a
    /// restore never silently retargets (ADR §4.6). Matches
    /// `Snapshot.status.snapshot.kopiaSnapshotID`.
    #[serde(
        default,
        rename = "kopiaSnapshotID",
        skip_serializing_if = "Option::is_none"
    )]
    pub kopia_snapshot_id: Option<String>,
    /// The concrete `Snapshot` CR the source resolved to, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_ref: Option<ObjectRef>,
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
    fn restore_crd_carries_target_xor_x_kubernetes_validation() {
        // §15: the generated CRD spec schema must carry the operator-authored
        // x-kubernetes-validations rule (exactly-one-of target.*) at the spec level,
        // surviving kube's structural-schema rewriter.
        let crd = Restore::crd();
        let json = serde_json::to_value(&crd).expect("serialize CRD");
        let spec_schema =
            &json["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
        let rules = spec_schema["x-kubernetes-validations"]
            .as_array()
            .expect("spec.x-kubernetes-validations present");
        assert!(
            rules.iter().any(|r| r["rule"]
                .as_str()
                .is_some_and(|s| s.contains("target.populator"))),
            "expected the target XOR rule; got {rules:?}"
        );
    }

    #[test]
    fn from_policy_offset_carries_static_openapi_default_in_crd() {
        // ADR-0005 §1: source.fromPolicy.offset must carry a real schema `default: 0`.
        let crd = Restore::crd();
        let json = serde_json::to_value(&crd).expect("serialize CRD");
        let default = &json["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["source"]["properties"]["fromPolicy"]["properties"]["offset"]["default"];
        assert_eq!(
            default, 0,
            "fromPolicy.offset must emit `default: 0` in the CRD schema; got {default:?}"
        );
    }

    #[test]
    fn from_policy_offset_defaults_to_zero_when_absent() {
        let spec: RestoreSpec =
            from_yaml("source: { fromPolicy: { name: pg } }\ntarget: { populator: {} }\n");
        match &spec.source {
            RestoreSource::FromPolicy(c) => assert_eq!(c.offset, 0),
            other => panic!("expected FromPolicy, got {}", other.kind_str()),
        }
    }

    #[test]
    fn restore_backup_ref_roundtrip_matches_adr_shape() {
        // Mirrors ADR-0001 §3.6 / §5.3.
        let yaml = r#"
source:
  snapshotRef: { name: postgres-data-20260524-021300, namespace: billing }
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
        assert_eq!(spec.source.kind_str(), "SnapshotRef");
        match &spec.source {
            RestoreSource::SnapshotRef(r) => {
                assert_eq!(r.name, "postgres-data-20260524-021300");
                assert_eq!(r.namespace.as_deref(), Some("billing"));
            }
            other => panic!("expected SnapshotRef, got {}", other.kind_str()),
        }
        let target = &spec.target;
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
    fn restore_passive_populator_mode_uses_explicit_populator_target() {
        // ADR-0005 §9: passive populator mode is now an EXPLICIT `target.populator: {}`
        // (the empty-`target` form is removed). Mirrors ADR-0001 §5.5
        // deploy-or-restore: fromPolicy + Continue + populator.
        let yaml = r#"
source: { fromPolicy: { name: postgres-data, offset: 0 } }
target: { populator: {} }
policy: { onMissingSnapshot: Continue }
"#;
        let spec: RestoreSpec = from_yaml(yaml);
        assert_eq!(spec.source.kind_str(), "FromPolicy");
        assert_eq!(spec.target.kind_str(), "Populator");
        assert!(matches!(spec.target, RestoreTarget::Populator(_)));
        match &spec.source {
            RestoreSource::FromPolicy(c) => {
                assert_eq!(c.name, "postgres-data");
                assert_eq!(c.offset, 0);
            }
            other => panic!("expected FromPolicy, got {}", other.kind_str()),
        }

        // Externally tagged: `{ populator: {} }`.
        let json = serde_json::to_value(&spec).unwrap();
        assert!(json["target"]["populator"].is_object());
        let reparsed: RestoreSpec = serde_json::from_value(json).unwrap();
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn restore_without_target_fails_to_deserialize() {
        // ADR-0005 §9 breaking change: a Restore with no `target` is invalid.
        let value: serde_json::Value =
            serde_yaml::from_str("source: { snapshotRef: { name: b } }\n").unwrap();
        assert!(
            serde_json::from_value::<RestoreSpec>(value).is_err(),
            "an absent target must be rejected (no empty-target form, ADR-0005 §9)"
        );
    }

    #[test]
    fn restore_populator_rejects_inherit_security_context() {
        // ADR-0005 §9: inheritSecurityContextFrom is meaningless with a populator
        // target (no workload pod exists at provision time) — the validator rejects it.
        use crate::common::{MoverSpec, PodSelector};
        use crate::validate::validate_restore;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
        let spec = RestoreSpec {
            repository: None,
            source: RestoreSource::FromPolicy(FromPolicy {
                name: "pg".into(),
                namespace: None,
                as_of: None,
                offset: 0,
            }),
            target: RestoreTarget::Populator(PopulatorTarget {}),
            options: None,
            policy: None,
            credential_projection: None,
            mover: Some(MoverSpec {
                inherit_security_context_from: Some(PodSelector {
                    pod_selector: LabelSelector::default(),
                    container: None,
                }),
                ..Default::default()
            }),
            failure_policy: None,
        };
        assert!(matches!(
            validate_restore(&spec),
            Err(crate::error::ValidationError::InvalidFieldValue { .. })
        ));

        // The same populator target WITHOUT inherit is fine.
        let ok = RestoreSpec {
            mover: None,
            ..spec
        };
        assert!(validate_restore(&ok).is_ok());
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
        assert_eq!(spec.target.kind_str(), "PvcRef");

        let json = serde_json::to_value(&spec).unwrap();
        let reparsed: RestoreSpec = serde_json::from_value(json).unwrap();
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn restore_mover_and_failure_policy_roundtrip() {
        // Restore carries the same mover surface a backup gets (resources +
        // securityContext for UID/GID match + cache) plus a failurePolicy.
        let yaml = r#"
source: { snapshotRef: { name: app-data-backup } }
target: { pvcRef: { name: app-data-restored } }
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
failurePolicy:
  backoffLimit: 4
  activeDeadlineSeconds: 3600
"#;
        let spec: RestoreSpec = from_yaml(yaml);
        let mover = spec.mover.as_ref().expect("mover");
        assert!(mover.resources.is_some());
        assert_eq!(
            mover.cache.as_ref().and_then(|c| c.capacity.as_deref()),
            Some("16Gi")
        );
        assert_eq!(
            mover.security_context.as_ref().and_then(|s| s.run_as_user),
            Some(1000)
        );
        // fsGroup is carried on the pod-level securityContext (makes a fresh restore
        // volume group-writable for an unprivileged mover).
        assert_eq!(
            mover.pod_security_context.as_ref().and_then(|p| p.fs_group),
            Some(1000)
        );
        // A hardened non-root container + fsGroup is NOT privileged: the gate lets it run.
        assert!(!mover.requires_privilege());
        let fp = spec.failure_policy.as_ref().expect("failurePolicy");
        assert_eq!(fp.backoff_limit, Some(4));
        assert_eq!(fp.active_deadline_seconds, Some(3600));

        let json = serde_json::to_value(&spec).expect("serialize");
        let reparsed: RestoreSpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn restore_mover_root_context_is_privileged() {
        // `runAsUser: 0` on a restore mover trips the same privileged-mover gate as a
        // backup — the controller refuses it unless the namespace opts in.
        let yaml = r#"
source: { snapshotRef: { name: app-data-backup } }
target: { pvcRef: { name: app-data-restored } }
mover:
  securityContext:
    runAsUser: 0
    runAsNonRoot: false
"#;
        let spec: RestoreSpec = from_yaml(yaml);
        assert!(spec.mover.as_ref().unwrap().requires_privilege());
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
