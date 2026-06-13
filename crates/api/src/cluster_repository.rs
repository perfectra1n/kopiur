//! The `ClusterRepository` CRD — a cluster-scoped, shared kopia repository
//! operated by a platform team. ADR-0001 §3.2, ADR-0003 §3.2.
//!
//! Same spec surface as `Repository` (backend/encryption/create/moverDefaults/
//! catalog), plus a tenancy gate (`allowedNamespaces`) and per-namespace identity
//! expressions (`identityDefaults`).

use crate::backend::Backend;
use crate::common::{
    CatalogBounds, CreateBehavior, Encryption, MoverDefaults, NamespaceDeletePolicy,
    RepositoryMode, default_namespace_delete_policy, default_repository_mode,
};
use crate::maintenance::RepositoryMaintenanceSpec;
use crate::repository::{CatalogStatus, RepositoryPhase, StorageStats};
use crate::server::{ClusterServerSpec, ServerStatus};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, LabelSelector};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A shared kopia repository referenceable from allow-listed namespaces. ADR §3.2.
///
/// Cluster-scoped: note the absence of `namespaced` in `#[kube(...)]`. Secret/config
/// references in `backend`/`encryption` therefore MUST carry an explicit `namespace`
/// (webhook-enforced — the type system cannot express that requirement here).
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "kopiur.home-operations.com",
    version = "v1alpha1",
    kind = "ClusterRepository",
    status = "ClusterRepositoryStatus",
    shortname = "kopiacrepo",
    category = "kopiur",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Backend","type":"string","jsonPath":".status.backend"}"#,
    printcolumn = r#"{"name":"Namespaces","type":"integer","jsonPath":".status.allowedNamespaceCount"}"#,
    printcolumn = r#"{"name":"Server","type":"string","jsonPath":".status.server.endpoint"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
// §7/§15: create-time-immutability transition rules (apiserver + CI), same set as
// the namespaced Repository — and like it, `encryption` (the password Secret reference)
// is deliberately NOT locked (kopia fixes only the resolved value; a rename with identical
// content must pass). Each `create.*` leaf is `has()`-guarded: CEL field access on an
// absent optional key raises a "no such key" error that fails the whole rule (→ 422 on
// *every* update, wedging the controller's finalizer/status writes), so we compare
// presence first and only dereference when set — see the namespaced `Repository` for the
// full rationale.
#[schemars(extend("x-kubernetes-validations" = [
    {"rule": "!has(self.create) || !has(oldSelf.create) || (has(self.create.splitter) == has(oldSelf.create.splitter) && (!has(self.create.splitter) || self.create.splitter == oldSelf.create.splitter))", "message": "create.splitter is immutable after creation"},
    {"rule": "!has(self.create) || !has(oldSelf.create) || (has(self.create.hash) == has(oldSelf.create.hash) && (!has(self.create.hash) || self.create.hash == oldSelf.create.hash))", "message": "create.hash is immutable after creation"},
    {"rule": "!has(self.create) || !has(oldSelf.create) || (has(self.create.encryption) == has(oldSelf.create.encryption) && (!has(self.create.encryption) || self.create.encryption == oldSelf.create.encryption))", "message": "create.encryption is immutable after creation"},
    {"rule": "!has(self.create) || !has(oldSelf.create) || (has(self.create.ecc) == has(oldSelf.create.ecc) && (!has(self.create.ecc) || self.create.ecc == oldSelf.create.ecc))", "message": "create.ecc is immutable after creation"}
]))]
#[serde(rename_all = "camelCase")]
pub struct ClusterRepositorySpec {
    /// Exactly one backend, enforced at the type level by the `Backend` enum. ADR §3.1.
    pub backend: Backend,
    /// Repository password, always a Secret reference. As this CR is cluster-scoped,
    /// the ref MUST carry an explicit `namespace` (webhook-enforced). ADR §3.1/§3.2.
    pub encryption: Encryption,
    /// What to do when the repository does not yet exist. Same semantics as
    /// `Repository.spec.create`. ADR §3.1/§3.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<CreateBehavior>,
    /// Mover defaults inherited by **every** mover this repository spawns — bootstrap,
    /// consumer backup/restore, and maintenance — overridable per-recipe and merged
    /// field-wise (ADR-0004 §1/§2). Absorbs the former `cacheDefaults`
    /// (now `moverDefaults.cache`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mover_defaults: Option<MoverDefaults>,
    /// Bounds materialization of `origin: discovered` `Snapshot` CRs from the kopia
    /// catalog. For a shared repo this also picks where to land discovered snapshots
    /// via `catalog.fallbackNamespace`. ADR §3.1/§3.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogBounds>,
    /// Tenancy gate — webhook-enforced on every consumer CR. ADR §3.2.
    pub allowed_namespaces: AllowedNamespaces,
    /// Identity defaults (CEL `*Expr`) applied when consumers don't override.
    /// ADR §3.2 / ADR-0004 §5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_defaults: Option<IdentityDefaults>,
    /// Optional kopia web-UI server. Cluster-scoped, so the target `namespace` is
    /// required (see [`ClusterServerSpec`]). Presence means enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<ClusterServerSpec>,
    /// Maintenance control. Default-managed: when absent or `enabled: true`, the
    /// reconciler creates and owns a `Maintenance` CR for this cluster repository.
    /// As `Maintenance` is namespaced, `maintenance.namespace` selects where it
    /// lands (defaulting to the operator's namespace). ADR §3.2/§3.7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance: Option<RepositoryMaintenanceSpec>,
    /// What happens to this repository's snapshots when a consuming **namespace** is
    /// deleted: `Orphan` (default — keep history) or `Delete` (cascade). BREAKING
    /// default change in ADR-0005 §5. Carries a real OpenAPI `default: Orphan`.
    #[serde(default = "default_namespace_delete_policy")]
    #[schemars(default = "default_namespace_delete_policy")]
    pub on_namespace_delete: NamespaceDeletePolicy,
    /// Repository-owner gate for credential-Secret projection into a foreign consumer
    /// namespace. **Default off** (`allowed: false`): a consumer's
    /// `credentialProjection.enabled` is necessary but not sufficient — the
    /// `ClusterRepository` owner must also allow it. BREAKING (ADR-0005 §8). A
    /// namespaced `Repository` has no such gate (projection there is a same-namespace
    /// no-op).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_projection: Option<ClusterRepoCredentialProjection>,
    /// Access mode (ADR-0005 §11): `ReadWrite` (default) or `ReadOnly`. A `ReadOnly`
    /// cluster repository serves restores only — backups and maintenance are refused.
    /// Carries a real OpenAPI `default: ReadWrite`.
    #[serde(default = "default_repository_mode")]
    #[schemars(default = "default_repository_mode")]
    pub mode: RepositoryMode,
    /// Pause this cluster repository declaratively (ADR-0005 §14(e)): skips
    /// connect/bootstrap and maintenance projection. Surfaced via a condition.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspend: bool,
}

/// The repository-OWNER side of credential projection on a `ClusterRepository`
/// (ADR-0005 §8) — distinct from the consumer `credentialProjection.enabled` on a
/// `SnapshotPolicy`/`Restore`. A sub-object (not a bare `bool`) so future knobs
/// (allow-listed consumer namespaces, key remapping) slot in without API breakage.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRepoCredentialProjection {
    /// When `true`, the repository owner permits projecting this repository's
    /// credential Secret(s) into a foreign consumer namespace (still requires the
    /// consumer opt-in AND operator RBAC — fail-closed). Off by default.
    #[serde(default)]
    pub allowed: bool,
}

/// The set of namespaces permitted to reference this `ClusterRepository`. ADR §3.2.
///
/// Externally-tagged: wire shape is `allowedNamespaces: { list: [...] }`,
/// `{ selector: {...} }`, or `{ all: true }`. Exactly one variant by construction.
///
/// Not `Eq`: the `Selector` variant embeds `LabelSelector` (k8s-openapi, `PartialEq` only).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AllowedNamespaces {
    /// Explicit namespace names.
    List(Vec<String>),
    /// Match namespaces by label.
    Selector(LabelSelector),
    /// Allow all namespaces (must be `true`; `false` is meaningless and rejected by webhook).
    All(bool),
}

impl AllowedNamespaces {
    /// Stable discriminant string for status/metrics.
    ///
    /// ```
    /// use kopiur_api::cluster_repository::AllowedNamespaces;
    ///
    /// let ns = AllowedNamespaces::List(vec!["production".into(), "staging".into()]);
    /// assert_eq!(ns.kind_str(), "List");
    /// assert_eq!(AllowedNamespaces::All(true).kind_str(), "All");
    /// ```
    pub fn kind_str(&self) -> &'static str {
        match self {
            AllowedNamespaces::List(_) => "List",
            AllowedNamespaces::Selector(_) => "Selector",
            AllowedNamespaces::All(_) => "All",
        }
    }
}

/// CEL expressions evaluated at admission to derive consumer identity when a
/// `SnapshotPolicy` doesn't override (ADR-0004 §5). Each `*Expr` returns a string
/// and is evaluated against the environment `namespace`, `policyName`, `labels`,
/// `annotations` (the consuming `SnapshotPolicy`'s metadata). Sandboxed, no I/O;
/// validated at admission so a typo/out-of-scope variable is rejected on apply.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IdentityDefaults {
    /// CEL expression for the kopia identity *hostname* (e.g. `"namespace"`).
    /// Returns a string. ADR-0004 §5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname_expr: Option<String>,
    /// CEL expression for the kopia identity *username*
    /// (e.g. `"namespace + '-' + policyName"`). Returns a string. ADR-0004 §5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username_expr: Option<String>,
}

/// Mirrors `RepositoryStatus` (ADR §3.1) plus `allowedNamespaceCount`. ADR §3.2.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRepositoryStatus {
    /// Current lifecycle phase (shared with `Repository`).
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
    /// Number of namespaces currently resolved by `spec.allowedNamespaces`. ADR §3.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_namespace_count: Option<i64>,
    /// Repository size and snapshot counts from the last catalog scan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_stats: Option<StorageStats>,
    /// Catalog-materialization status (discovered-backup count, last refresh).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogStatus>,
    /// Resolved kopia server endpoint/auth, pinned by the reconciler.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<ServerStatus>,
    /// Standard Kubernetes conditions (e.g. `Connected`, `MaintenanceOwned`). ADR §3.2.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::from_yaml;
    use kube::core::CustomResourceExt;

    #[test]
    fn cluster_repository_crd_metadata_is_correct() {
        // `crd()` exercises schema generation; mis-encoded enums panic here.
        let crd = ClusterRepository::crd();
        assert_eq!(crd.spec.group, "kopiur.home-operations.com");
        assert_eq!(crd.spec.names.kind, "ClusterRepository");
        // Cluster-scoped: this is the load-bearing assertion vs. namespaced CRDs.
        assert_eq!(crd.spec.scope, "Cluster");
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn cluster_repository_roundtrip_matches_adr_shape() {
        // Mirrors ADR-0001 §3.2 / §5.2.
        let yaml = r#"
backend:
  s3:
    bucket: org-kopia-repo
    prefix: ""
    endpoint: s3.us-east-1.amazonaws.com
    region: us-east-1
    auth:
      secretRef:
        name: kopia-platform-creds
        namespace: kopia-system
encryption:
  passwordSecretRef:
    name: kopia-platform-creds
    namespace: kopia-system
    key: KOPIA_PASSWORD
create:
  enabled: true
  encryption: AES256-GCM-HMAC-SHA256
allowedNamespaces:
  list: [production, staging, billing]
identityDefaults:
  hostnameExpr: "namespace"
  usernameExpr: "namespace + '-' + policyName"
catalog:
  retain:
    perIdentity: 50
    maxAgeDays: 60
  refreshInterval: 5m
  fallbackNamespace: kopia-system
"#;
        let spec: ClusterRepositorySpec = from_yaml(yaml);
        match &spec.backend {
            Backend::S3(s3) => assert_eq!(s3.bucket, "org-kopia-repo"),
            other => panic!("expected S3 backend, got {}", other.kind_str()),
        }
        match &spec.allowed_namespaces {
            AllowedNamespaces::List(ns) => {
                assert_eq!(ns, &["production", "staging", "billing"]);
            }
            other => panic!("expected List, got {}", other.kind_str()),
        }
        let id = spec.identity_defaults.as_ref().expect("identityDefaults");
        assert_eq!(id.hostname_expr.as_deref(), Some("namespace"));
        assert_eq!(
            id.username_expr.as_deref(),
            Some("namespace + '-' + policyName")
        );
        assert_eq!(
            spec.catalog.as_ref().unwrap().fallback_namespace.as_deref(),
            Some("kopia-system")
        );

        let json = serde_json::to_value(&spec).expect("serialize");
        let reparsed: ClusterRepositorySpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn allowed_namespaces_selector_variant() {
        let v: AllowedNamespaces = from_yaml(
            "selector:\n  matchLabels: { kopiur.home-operations.com/tier: enterprise }\n",
        );
        assert_eq!(v.kind_str(), "Selector");
        let json = serde_json::to_value(&v).unwrap();
        assert_eq!(
            json["selector"]["matchLabels"]["kopiur.home-operations.com/tier"],
            "enterprise"
        );
    }

    #[test]
    fn allowed_namespaces_all_variant() {
        let v: AllowedNamespaces = from_yaml("all: true\n");
        assert_eq!(v.kind_str(), "All");
        assert_eq!(serde_json::to_value(&v).unwrap()["all"], true);
    }

    #[test]
    fn allowed_namespaces_unknown_variant_is_rejected() {
        let value: serde_json::Value = serde_yaml::from_str("everyone: true\n").unwrap();
        assert!(serde_json::from_value::<AllowedNamespaces>(value).is_err());
    }
}
