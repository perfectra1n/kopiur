//! The `ClusterRepository` CRD — a cluster-scoped, shared kopia repository
//! operated by a platform team. ADR-0001 §3.2, ADR-0003 §3.2.
//!
//! Same spec surface as `Repository` (backend/encryption/create/cacheDefaults/
//! catalog), plus a tenancy gate (`allowedNamespaces`) and per-namespace identity
//! templating (`identityDefaults`).

use crate::backend::Backend;
use crate::common::{CacheDefaults, CatalogBounds, CreateBehavior, Encryption};
use crate::repository::{CatalogStatus, RepositoryPhase, StorageStats};
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
    group = "kopia.io",
    version = "v1alpha1",
    kind = "ClusterRepository",
    status = "ClusterRepositoryStatus",
    shortname = "kopiacrepo",
    category = "kopiur",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Backend","type":"string","jsonPath":".status.backend"}"#,
    printcolumn = r#"{"name":"Namespaces","type":"integer","jsonPath":".status.allowedNamespaceCount"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRepositorySpec {
    /// Exactly one backend, enforced at the type level by the `Backend` enum. ADR §3.1.
    pub backend: Backend,
    pub encryption: Encryption,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<CreateBehavior>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_defaults: Option<CacheDefaults>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogBounds>,
    /// Tenancy gate — webhook-enforced on every consumer CR. ADR §3.2.
    pub allowed_namespaces: AllowedNamespaces,
    /// Identity defaults applied when consumers don't override. ADR §3.2/§4.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_defaults: Option<IdentityTemplate>,
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
    pub fn kind_str(&self) -> &'static str {
        match self {
            AllowedNamespaces::List(_) => "List",
            AllowedNamespaces::Selector(_) => "Selector",
            AllowedNamespaces::All(_) => "All",
        }
    }
}

/// Templates rendered (Jinja2-compatible) at admission to derive consumer identity
/// when a `BackupConfig` doesn't override. ADR §3.2/§4.2.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IdentityTemplate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname_template: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username_template: Option<String>,
}

/// Mirrors `RepositoryStatus` (ADR §3.1) plus `allowedNamespaceCount`. ADR §3.2.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRepositoryStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<RepositoryPhase>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Kopia repository unique ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<String>,
    /// Mirror of `spec.backend` discriminant for the print column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
    /// Number of namespaces currently resolved by `spec.allowedNamespaces`. ADR §3.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_namespace_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_stats: Option<StorageStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogStatus>,
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
        assert_eq!(crd.spec.group, "kopia.io");
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
  hostnameTemplate: "{{ .Namespace }}"
  usernameTemplate: "{{ .Namespace }}-{{ .ConfigName }}"
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
        assert_eq!(id.hostname_template.as_deref(), Some("{{ .Namespace }}"));
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
        let v: AllowedNamespaces =
            from_yaml("selector:\n  matchLabels: { kopia.io/tier: enterprise }\n");
        assert_eq!(v.kind_str(), "Selector");
        let json = serde_json::to_value(&v).unwrap();
        assert_eq!(
            json["selector"]["matchLabels"]["kopia.io/tier"],
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
