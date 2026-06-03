//! The `Repository` CRD — a namespaced kopia repository. ADR-0003 §3.1.

use crate::backend::Backend;
use crate::common::{CacheDefaults, CatalogBounds, CreateBehavior, Encryption};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A kopia repository owned by one namespace: credentials, backend, encryption,
/// and optional catalog-materialization bounds. Many `BackupConfig`s / `Restore`s
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
#[serde(rename_all = "camelCase")]
pub struct RepositorySpec {
    /// Exactly one backend, enforced at the type level by the `Backend` enum. ADR §3.1.
    pub backend: Backend,
    pub encryption: Encryption,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<CreateBehavior>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_defaults: Option<CacheDefaults>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogBounds>,
}

/// Lifecycle phase of a repository. ADR §3.1 status.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum RepositoryPhase {
    #[default]
    Pending,
    Initializing,
    Ready,
    Degraded,
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryStatus {
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_stats: Option<StorageStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageStats {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_observed_at: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovered_backup_count: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_refresh_at: Option<String>,
}
