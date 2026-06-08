//! The `Repository` CRD — a namespaced kopia repository. ADR-0003 §3.1.

use crate::backend::Backend;
use crate::common::{CacheDefaults, CatalogBounds, CreateBehavior, Encryption};
use crate::maintenance::RepositoryMaintenanceSpec;
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
    /// Repository password, always a Secret reference. A sub-object so future
    /// rotation fields slot in without API breakage. ADR §3.1/§4.11.
    pub encryption: Encryption,
    /// What to do when the repository does not yet exist. Absent/disabled means
    /// it must already exist; enabled means the operator creates it with the
    /// given encryption/splitter/hash algorithms. ADR §3.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<CreateBehavior>,
    /// Cache sizing inherited by `Backup`/`Restore` movers unless overridden. ADR §3.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_defaults: Option<CacheDefaults>,
    /// Bounds materialization of `origin: discovered` `Backup` CRs from the kopia
    /// catalog, keeping etcd footprint sane for large repositories. ADR §3.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogBounds>,
    /// Maintenance control. Default-managed: when absent or `enabled: true`, the
    /// reconciler creates and owns a `Maintenance` CR for this repository in this
    /// namespace. ADR §3.1/§3.7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance: Option<RepositoryMaintenanceSpec>,
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
    /// Kopia repository unique ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<String>,
    /// Mirror of `spec.backend` discriminant for the print column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
    /// Repository size and snapshot counts from the last catalog scan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_stats: Option<StorageStats>,
    /// Catalog-materialization status (how many discovered `Backup`s, last refresh).
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

/// Status of catalog materialization for `origin: discovered` `Backup` CRs. ADR §3.1 status.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogStatus {
    /// How many `Backup` CRs were materialized from the catalog scan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovered_backup_count: Option<i64>,
    /// RFC 3339 timestamp of the last catalog refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_refresh_at: Option<String>,
}
