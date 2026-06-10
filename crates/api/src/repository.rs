//! The `Repository` CRD — a namespaced kopia repository. ADR-0003 §3.1.

use crate::backend::Backend;
use crate::common::{
    CatalogBounds, CreateBehavior, Encryption, MoverDefaults, NamespaceDeletePolicy,
    RepositoryMode, default_namespace_delete_policy, default_repository_mode,
};
use crate::maintenance::RepositoryMaintenanceSpec;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A kopia repository owned by one namespace: credentials, backend, encryption,
/// and optional catalog-materialization bounds. Many `SnapshotPolicy`s / `Restore`s
/// reference one. ADR §3.1.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "kopiur.home-operations.com",
    version = "v1alpha1",
    kind = "Repository",
    namespaced,
    status = "RepositoryStatus",
    shortname = "kopiarepo",
    category = "kopiur",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Backend","type":"string","jsonPath":".status.backend"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
// §7/§15: create-time-immutability transition rules in the CRD schema (apiserver +
// CI), complementing the webhook checks. The `create.*` rules only bite when `create`
// is present on both sides. `encryption` (the password Secret reference) is deliberately
// NOT locked: kopia fixes only the resolved password value in the repo format, never the
// Secret name/key, and the reference is not a reliable proxy (a rename with identical
// content must not be rejected — that broke GitOps). See `validate::diff_immutable_repo_fields`.
// Each leaf is `has()`-guarded: CEL field access on an absent optional key raises a
// "no such key" error (which fails the WHOLE rule → 422 on *every* update, blocking
// the controller's finalizer/status writes), so we compare presence first and only
// dereference when set — the common `create: {enabled: true}` case (no splitter/
// hash/encryption/ecc) must reconcile, not wedge. Mirrors the webhook's None-vs-Some
// semantics in `validate::diff_immutable_repo_fields`.
#[schemars(extend("x-kubernetes-validations" = [
    {"rule": "!has(self.create) || !has(oldSelf.create) || (has(self.create.splitter) == has(oldSelf.create.splitter) && (!has(self.create.splitter) || self.create.splitter == oldSelf.create.splitter))", "message": "create.splitter is immutable after creation"},
    {"rule": "!has(self.create) || !has(oldSelf.create) || (has(self.create.hash) == has(oldSelf.create.hash) && (!has(self.create.hash) || self.create.hash == oldSelf.create.hash))", "message": "create.hash is immutable after creation"},
    {"rule": "!has(self.create) || !has(oldSelf.create) || (has(self.create.encryption) == has(oldSelf.create.encryption) && (!has(self.create.encryption) || self.create.encryption == oldSelf.create.encryption))", "message": "create.encryption is immutable after creation"},
    {"rule": "!has(self.create) || !has(oldSelf.create) || (has(self.create.ecc) == has(oldSelf.create.ecc) && (!has(self.create.ecc) || self.create.ecc == oldSelf.create.ecc))", "message": "create.ecc is immutable after creation"}
]))]
#[serde(rename_all = "camelCase")]
pub struct RepositorySpec {
    /// Exactly one backend, enforced at the type level by the `Backend` enum. ADR §3.1.
    pub backend: Backend,
    /// Repository password, always a Secret reference. A sub-object so future
    /// rotation fields slot in without API breakage. ADR §3.1/§4.11.
    pub encryption: Encryption,
    /// What to do when the repository does not yet exist. Absent/disabled means
    /// it must already exist; enabled means the operator creates it with the
    /// given encryption/splitter/hash algorithms. ADR §3.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<CreateBehavior>,
    /// Mover defaults (security context, pod security context, resources, cache,
    /// nodeSelector/tolerations/affinity, Job TTL) inherited by **every** mover this
    /// repository spawns — bootstrap, backup, restore, maintenance — overridable
    /// per-recipe via `mover` and merged field-wise (ADR-0004 §1/§2). Absorbs the
    /// former `cacheDefaults` (now `moverDefaults.cache`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mover_defaults: Option<MoverDefaults>,
    /// Bounds materialization of `origin: discovered` `Snapshot` CRs from the kopia
    /// catalog, keeping etcd footprint sane for large repositories. ADR §3.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogBounds>,
    /// Maintenance control. Default-managed: when absent or `enabled: true`, the
    /// reconciler creates and owns a `Maintenance` CR for this repository in this
    /// namespace. ADR §3.1/§3.7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance: Option<RepositoryMaintenanceSpec>,
    /// What happens to this repository's snapshots when a consuming **namespace** is
    /// deleted: `Orphan` (default — keep history, release ownership) or `Delete`
    /// (cascade per-`Snapshot` `deletionPolicy`). BREAKING default change in
    /// ADR-0005 §5 — `kubectl delete ns` no longer destroys snapshots by default.
    /// Carries a real OpenAPI `default: Orphan`.
    #[serde(default = "default_namespace_delete_policy")]
    #[schemars(default = "default_namespace_delete_policy")]
    pub on_namespace_delete: NamespaceDeletePolicy,
    /// Access mode (ADR-0005 §11): `ReadWrite` (default) or `ReadOnly`. A `ReadOnly`
    /// repository serves restores only — the reconciler refuses backup Jobs and skips
    /// maintenance projection — for decommissioning/migration without write risk.
    /// Carries a real OpenAPI `default: ReadWrite`.
    #[serde(default = "default_repository_mode")]
    #[schemars(default = "default_repository_mode")]
    pub mode: RepositoryMode,
    /// Pause this repository declaratively (ADR-0005 §14(e)): a suspended repository
    /// skips connect/bootstrap and maintenance projection. Surfaced via a condition.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspend: bool,
}

/// Lifecycle phase of a repository. ADR §3.1 status.
///
/// A freshly admitted CR starts in the `#[default]` [`RepositoryPhase::Pending`]:
///
/// ```
/// use kopiur_api::repository::RepositoryPhase;
///
/// assert_eq!(RepositoryPhase::default(), RepositoryPhase::Pending);
/// // Serializes as a bare string (closed unit enum).
/// assert_eq!(
///     serde_json::to_value(RepositoryPhase::Ready).unwrap(),
///     serde_json::json!("Ready")
/// );
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum RepositoryPhase {
    /// Accepted by the API server but not yet reconciled.
    #[default]
    Pending,
    /// Connecting to (or creating) the kopia repository.
    Initializing,
    /// Connected and healthy.
    Ready,
    /// Reachable, but a sub-operation (e.g. maintenance) is failing; see conditions.
    Degraded,
    /// Connect/create failed; see conditions for the actionable reason.
    Failed,
}

impl crate::common::PhaseLabel for RepositoryPhase {
    const ALL: &'static [Self] = &[
        Self::Pending,
        Self::Initializing,
        Self::Ready,
        Self::Degraded,
        Self::Failed,
    ];
    fn label(&self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Initializing => "Initializing",
            Self::Ready => "Ready",
            Self::Degraded => "Degraded",
            Self::Failed => "Failed",
        }
    }
}

/// Observed state of a [`Repository`]. Carries resolved values pinned by the
/// reconciler. ADR §3.1 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryStatus {
    /// Current lifecycle phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<RepositoryPhase>,
    /// `metadata.generation` of the `spec` last reconciled; drives staleness detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// `resourceVersion` of the password Secret observed at the last connect attempt.
    /// The terminal-failure hard-stop reopens when this changes, so editing the
    /// Secret's *content* (which does NOT bump `metadata.generation`) re-triggers a
    /// connect instead of parking the repository as `Failed` forever.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_credential_version: Option<String>,
    /// Kopia repository unique ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<String>,
    /// Mirror of `spec.backend` discriminant for the print column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
    /// Repository size and snapshot counts from the last catalog scan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_stats: Option<StorageStats>,
    /// Catalog-materialization status (how many discovered `Snapshot`s, last refresh).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogStatus>,
    /// Standard Kubernetes conditions (e.g. `Connected`, `MaintenanceOwned`). ADR §3.1.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Aggregate repository storage figures from the last catalog scan. ADR §3.1 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageStats {
    /// Total snapshots present in the repository (across all identities).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_count: Option<i64>,
    /// Human-readable total on-disk size (e.g. `412Gi`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_size: Option<String>,
    /// RFC 3339 timestamp these stats were last observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_observed_at: Option<String>,
}

/// Status of catalog materialization for `origin: discovered` `Snapshot` CRs. ADR §3.1 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogStatus {
    /// How many `Snapshot` CRs were materialized from the catalog scan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovered_backup_count: Option<i64>,
    /// RFC 3339 timestamp of the last catalog refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_refresh_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::RepositoryMode;
    use crate::testutil::from_yaml;
    use kube::core::CustomResourceExt;

    #[test]
    fn mode_suspend_and_ecc_roundtrip() {
        // ADR-0005 §11/§14(e)/§13(a): mode, suspend, and create.ecc parse the
        // cluster's way and round-trip.
        let yaml = r#"
backend: { filesystem: { path: /repo } }
encryption: { passwordSecretRef: { name: s } }
create:
  enabled: true
  encryption: AES256-GCM-HMAC-SHA256
  ecc:
    algorithm: REED-SOLOMON-CRC32
    overheadPercent: 2
mode: ReadOnly
suspend: true
"#;
        let spec: RepositorySpec = from_yaml(yaml);
        assert_eq!(spec.mode, RepositoryMode::ReadOnly);
        assert!(!spec.mode.allows_writes());
        assert!(spec.suspend);
        let ecc = spec.create.as_ref().unwrap().ecc.as_ref().expect("ecc");
        assert_eq!(ecc.algorithm.as_deref(), Some("REED-SOLOMON-CRC32"));
        assert_eq!(ecc.overhead_percent, Some(2));

        let json = serde_json::to_value(&spec).expect("serialize");
        assert_eq!(json["mode"], "ReadOnly");
        assert_eq!(json["suspend"], true);
        let reparsed: RepositorySpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn repository_crd_carries_immutability_transition_rules() {
        // §7/§15: the spec schema carries the create.{splitter,hash,encryption,ecc}
        // immutability transition rules — but NOT an `encryption` (password Secret ref)
        // rule: the reference is mutable (kopia fixes only the resolved value, so a
        // rename with identical content must pass).
        let crd = Repository::crd();
        let json = serde_json::to_value(&crd).unwrap();
        let rules = json["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["x-kubernetes-validations"]
            .as_array()
            .expect("spec.x-kubernetes-validations present");
        let has = |needle: &str| {
            rules
                .iter()
                .any(|r| r["rule"].as_str().is_some_and(|s| s.contains(needle)))
        };
        assert!(
            !has("self.encryption == oldSelf.encryption"),
            "the password Secret ref must NOT be locked (a rename must be allowed)"
        );
        assert!(has("self.create.splitter == oldSelf.create.splitter"));
        assert!(has("self.create.hash == oldSelf.create.hash"));
        assert!(has("self.create.ecc == oldSelf.create.ecc"));
    }

    #[test]
    fn create_immutability_rules_guard_each_optional_leaf_with_has() {
        // Regression (e2e): a `create.*` immutability rule that dereferences the leaf
        // without a `has()` guard (`self.create.splitter == oldSelf.create.splitter`)
        // raises a CEL "no such key" error whenever `create` is present but the
        // optional leaf is absent — the common `create: {enabled: true}` case. That
        // error fails the WHOLE rule → the apiserver 422s *every* update, so the
        // controller can never add its finalizer or write status and the Repository
        // wedges below Ready. Each `create.*` leaf must therefore be `has()`-guarded.
        let crd = Repository::crd();
        let json = serde_json::to_value(&crd).unwrap();
        let rules = json["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["x-kubernetes-validations"]
            .as_array()
            .expect("spec.x-kubernetes-validations present");
        for leaf in ["splitter", "hash", "encryption", "ecc"] {
            let rule = rules
                .iter()
                .find_map(|r| {
                    let s = r["rule"].as_str()?;
                    s.contains(&format!("self.create.{leaf} == oldSelf.create.{leaf}"))
                        .then_some(s)
                })
                .unwrap_or_else(|| panic!("missing create.{leaf} immutability rule"));
            assert!(
                rule.contains(&format!("has(self.create.{leaf})"))
                    && rule.contains(&format!("has(oldSelf.create.{leaf})")),
                "create.{leaf} immutability rule must `has()`-guard the leaf on BOTH sides \
                 (else `create: {{enabled: true}}` 422s every update); got: {rule}"
            );
        }
    }

    #[test]
    fn mode_defaults_to_readwrite_and_emits_openapi_default() {
        // Absent ⇒ ReadWrite (parses) and the schema carries `default: ReadWrite`.
        let spec: RepositorySpec = from_yaml(
            "backend: { filesystem: { path: /repo } }\nencryption: { passwordSecretRef: { name: s } }\n",
        );
        assert_eq!(spec.mode, RepositoryMode::ReadWrite);
        assert!(!spec.suspend);
        // Materialized (not skip-elided), so it round-trips into the stored object.
        assert_eq!(serde_json::to_value(&spec).unwrap()["mode"], "ReadWrite");

        let crd = Repository::crd();
        let json = serde_json::to_value(&crd).unwrap();
        let default = &json["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["mode"]["default"];
        assert_eq!(default, "ReadWrite");
    }
}
