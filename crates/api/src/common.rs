//! Shared sub-objects reused across multiple CRDs.
//!
//! Per ADR-0003 §2.2 (principle 10) and §4.11, every credential, policy, and
//! identity surface is modeled as a sub-object so future fields slot in without
//! API breakage. Leaf Kubernetes types (`LabelSelector`, `ResourceRequirements`,
//! `PodSecurityContext`) are reused from `k8s-openapi` rather than re-invented.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A lifecycle-phase enum that can be rendered as a metric label.
///
/// The single source of truth for a CRD's phase labels: [`PhaseLabel::ALL`]
/// enumerates every variant and [`PhaseLabel::label`] is an exhaustive match.
/// The controller's `kopiur_resource_phase` gauge uses these to set the active
/// phase to 1 and the rest to 0 (and to clear all on deletion), so both the
/// label string and the reset set come from the enum itself rather than a
/// stringly-typed table that can silently drift (ADR §5.5 type-safety thesis).
pub trait PhaseLabel: Copy + PartialEq + 'static {
    /// Every variant, in declaration order.
    const ALL: &'static [Self];
    /// The stable metric label string for this variant (exhaustive `match`).
    fn label(&self) -> &'static str;
}

/// Reference to a key within a `Secret` in the same namespace as the referrer,
/// unless `namespace` is given (required for cluster-scoped CRs — ADR §3.2).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Defaults are documented per-field on the consuming struct.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// Reference to an entire `Secret` (the operator reads well-known keys from it,
/// e.g. `AWS_ACCESS_KEY_ID`). See ADR §3.1 backend `auth.secretRef`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretRef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Reference to a key within a `ConfigMap` (e.g. a CA bundle). ADR §3.1 `tls.caBundleRef`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapKeyRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_map_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// TLS settings for object-store backends. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_bundle_ref: Option<ConfigMapKeyRef>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub insecure_skip_verify: bool,
}

/// Which kind of repository a consumer CR references. ADR §3.2/§3.3.
///
/// This is a closed enum: a consumer's `repository.kind` is always exactly one
/// of these two values, so reconcilers `match` it exhaustively.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum RepositoryKind {
    #[default]
    Repository,
    ClusterRepository,
}

/// Discriminated reference from a consumer CR (`BackupConfig`, `Backup`,
/// `Restore`, `Maintenance`) to a `Repository` or `ClusterRepository`. ADR §3.2.
///
/// When `kind == ClusterRepository`, `namespace` MUST be absent — enforced by the
/// admission webhook (`api::validate`), since the type system cannot express
/// "this field is forbidden only for one variant of a sibling field".
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryRef {
    #[serde(default)]
    pub kind: RepositoryKind,
    pub name: String,
    /// Cross-namespace `Repository` reference; ignored/forbidden for `ClusterRepository`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Repository encryption settings. A sub-object so future rotation fields
/// (`rotation`, `previousPasswords`) slot in without breakage (ADR §4.11).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Encryption {
    /// Always a Secret ref; never inline. ADR §3.1.
    pub password_secret_ref: SecretKeyRef,
}

/// Behavior when the repository does not yet exist. ADR §3.1 `create`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateBehavior {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub splitter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

/// Cache defaults inherited by `Backup`/`Restore` movers unless overridden. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CacheDefaults {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_cache_size_mb: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_cache_size_mb: Option<i64>,
}

/// Bounds on materialization of `origin: discovered` `Backup` CRs. ADR §3.1 `catalog`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogBounds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain: Option<CatalogRetain>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_interval: Option<String>,
    /// Where to materialize discovered `Backup`s whose identity hostname does not
    /// map to an allowed namespace (ClusterRepository only). ADR §3.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_namespace: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogRetain {
    /// Most-recent N per `username@hostname:path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_identity: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_days: Option<i64>,
}

/// GFS retention policy. The single successful-retention driver (ADR §4.4).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Retention {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_latest: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_hourly: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_daily: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_weekly: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_monthly: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_annual: Option<u32>,
}

/// Identity overrides — what kopia records as `username@hostname:path`. ADR §3.3/§4.2.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
}

/// Fully-resolved identity pinned into status; never re-rendered after admission. ADR §4.2.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedIdentity {
    pub username: String,
    pub hostname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
}

/// Per-run failure controls passed through to the mover `Job`. ADR §3.4/§4.10 (G6).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FailurePolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_limit: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_deadline_seconds: Option<i64>,
}

/// Per-recipe mover overrides (resources, cache, security context). ADR §3.3.
///
/// Not `Eq`: embeds `k8s-openapi` types (`ResourceRequirements`, `SecurityContext`)
/// which only implement `PartialEq`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MoverSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<k8s_openapi::api::core::v1::ResourceRequirements>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheDefaults>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_context: Option<k8s_openapi::api::core::v1::SecurityContext>,
    /// Opt-in, namespace-gated; preserves UID/GID on restore. ADR §4.11/§G16.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privileged_mode: Option<bool>,
    /// Opt-in: copy security context from a live workload pod. ADR §4.11.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherit_security_context_from: Option<PodSelector>,
}

/// Selects workload pods by label. Reuses k8s-openapi `LabelSelector`. ADR §3.3 hooks.
///
/// Not `Eq`: `LabelSelector` only implements `PartialEq`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PodSelector {
    pub pod_selector: LabelSelector,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
}

/// Reference to a `BackupConfig` CR (used by `Backup.spec.configRef` and
/// `BackupSchedule.spec.configRef`). May cross namespaces, subject to RBAC. ADR §3.4/§3.5.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigRef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Generic name/namespace reference to another namespaced object — e.g. a `Backup`
/// CR (`Restore.spec.source.backupRef`) or a PVC (`Restore.spec.target.pvcRef`). ADR §3.6.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObjectRef {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Lifecycle of the underlying kopia snapshot when its `Backup` CR is deleted.
/// Shared by `BackupConfig.spec.defaultDeletionPolicy` and `Backup.spec.deletionPolicy`.
/// ADR-0003 §4.5 / ADR-0001 §4.5.
///
/// The reconciler distinguishes the three cases with an exhaustive `match` — Rust
/// enforces that any new variant added later must be handled in every match site,
/// preventing the class of bug where a new policy slips into production without a
/// corresponding reconcile branch.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum DeletionPolicy {
    /// Default for `origin: scheduled`/`manual`. Finalizer runs
    /// `kopia snapshot delete <id>` then removes the finalizer.
    #[default]
    Delete,
    /// Default for `origin: discovered`. CR is removed; snapshot stays.
    /// Forced via webhook for discovered backups; cannot be overridden.
    Retain,
    /// CR is removed without contacting the repository at all (escape hatch
    /// for "the bucket is gone, just let me delete the CR"). Status records
    /// `orphaned: true` for the snapshot ID before removal.
    Orphan,
}

/// A single cron entry with optional deterministic jitter. Shared by `Maintenance`'s
/// quick/full schedules. ADR §3.7. `jitter` is a Go-style duration string (e.g. `30m`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CronSpec {
    pub cron: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter: Option<String>,
}

impl RepositoryRef {
    /// True if this reference points at the given repository.
    ///
    /// `owner_namespace` is the namespace of the resource that holds the ref
    /// (e.g. the `Maintenance` CR's own namespace), used to resolve a namespaced
    /// `Repository` reference that omits `namespace`. The match is exhaustive over
    /// [`RepositoryKind`] (ADR §5.5):
    ///
    /// - [`RepositoryKind::Repository`]: kind+name must match AND the effective
    ///   namespace (`self.namespace` or `owner_namespace`) must equal
    ///   `target_namespace`.
    /// - [`RepositoryKind::ClusterRepository`]: kind+name must match; namespace is
    ///   ignored on both sides (cluster-scoped).
    ///
    /// `target_namespace` is `None` for a `ClusterRepository` target.
    pub fn resolves_to(
        &self,
        owner_namespace: &str,
        target_kind: RepositoryKind,
        target_name: &str,
        target_namespace: Option<&str>,
    ) -> bool {
        if self.kind != target_kind || self.name != target_name {
            return false;
        }
        match self.kind {
            RepositoryKind::Repository => {
                Some(self.namespace.as_deref().unwrap_or(owner_namespace)) == target_namespace
            }
            RepositoryKind::ClusterRepository => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ref_of(kind: RepositoryKind, name: &str, namespace: Option<&str>) -> RepositoryRef {
        RepositoryRef {
            kind,
            name: name.into(),
            namespace: namespace.map(str::to_string),
        }
    }

    #[test]
    fn resolves_to_same_namespace_when_ref_omits_it() {
        // A Maintenance in `apps` referencing `{ kind: Repository, name: nas }`
        // (no namespace) points at Repository apps/nas.
        let r = ref_of(RepositoryKind::Repository, "nas", None);
        assert!(r.resolves_to("apps", RepositoryKind::Repository, "nas", Some("apps")));
        assert!(!r.resolves_to("apps", RepositoryKind::Repository, "nas", Some("other")));
    }

    #[test]
    fn resolves_to_honors_explicit_cross_namespace_ref() {
        let r = ref_of(RepositoryKind::Repository, "nas", Some("backups"));
        // Owner namespace is irrelevant once the ref pins one.
        assert!(r.resolves_to("apps", RepositoryKind::Repository, "nas", Some("backups")));
        assert!(!r.resolves_to("apps", RepositoryKind::Repository, "nas", Some("apps")));
    }

    #[test]
    fn resolves_to_name_mismatch_is_false() {
        let r = ref_of(RepositoryKind::Repository, "nas", None);
        assert!(!r.resolves_to("apps", RepositoryKind::Repository, "other", Some("apps")));
    }

    #[test]
    fn resolves_to_kind_mismatch_is_false_even_with_same_name() {
        // A `Repository` ref must never satisfy a `ClusterRepository` target and
        // vice versa, even when the names collide.
        let r = ref_of(RepositoryKind::Repository, "shared", None);
        assert!(!r.resolves_to("apps", RepositoryKind::ClusterRepository, "shared", None));

        let cr = ref_of(RepositoryKind::ClusterRepository, "shared", None);
        assert!(!cr.resolves_to("apps", RepositoryKind::Repository, "shared", Some("apps")));
    }

    #[test]
    fn resolves_to_cluster_repository_ignores_namespace() {
        let cr = ref_of(RepositoryKind::ClusterRepository, "hetzner", None);
        assert!(cr.resolves_to("apps", RepositoryKind::ClusterRepository, "hetzner", None));
        // Even a stray namespace on the ref (webhook normally forbids it) still
        // resolves cluster-scoped.
        let stray = ref_of(RepositoryKind::ClusterRepository, "hetzner", Some("oops"));
        assert!(stray.resolves_to("apps", RepositoryKind::ClusterRepository, "hetzner", None));
    }
}
