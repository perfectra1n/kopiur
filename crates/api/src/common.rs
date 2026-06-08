//! Shared sub-objects reused across multiple CRDs.
//!
//! Per ADR-0003 §2.2 (principle 10) and §4.11, every credential, policy, and
//! identity surface is modeled as a sub-object so future fields slot in without
//! API breakage. Leaf Kubernetes types (`LabelSelector`, `ResourceRequirements`,
//! `PodSecurityContext`) are reused from `k8s-openapi` rather than re-invented.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// serde `default` for a `bool` field whose absent value is `true`. Used by
/// "enabled by default, opt out explicitly" surfaces (e.g.
/// `RepositoryMaintenanceSpec.enabled`). `bool::default()` is `false`, so a
/// default-true field cannot lean on `#[serde(default)]` alone.
pub(crate) fn default_true() -> bool {
    true
}

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
    /// Name of the `Secret`.
    pub name: String,
    /// Namespace of the `Secret`. Absent = same namespace as the referrer;
    /// required for cluster-scoped CRs which have no own namespace (ADR §3.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Which key inside the `Secret` to read. Defaults are documented per-field on
    /// the consuming struct.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// Reference to an entire `Secret` (the operator reads well-known keys from it,
/// e.g. `AWS_ACCESS_KEY_ID`). See ADR §3.1 backend `auth.secretRef`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretRef {
    /// Name of the `Secret`.
    pub name: String,
    /// Namespace of the `Secret`. Absent = same namespace as the referrer;
    /// required for cluster-scoped CRs (ADR §3.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Reference to a key within a `ConfigMap` (e.g. a CA bundle). ADR §3.1 `tls.caBundleRef`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapKeyRef {
    /// Name of the `ConfigMap` holding the value (e.g. a CA bundle).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_map_name: Option<String>,
    /// Which key inside the `ConfigMap` to read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// TLS settings for object-store backends. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    /// CA bundle (PEM) used to verify the endpoint's certificate, sourced from a
    /// `ConfigMap`. Preferred over `insecureSkipVerify` for self-signed endpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_bundle_ref: Option<ConfigMapKeyRef>,
    /// Skip TLS certificate verification (still uses TLS). Maps to kopia's
    /// `--disable-tls-verification`. For self-signed endpoints; prefer
    /// `caBundleRef` in production.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub insecure_skip_verify: bool,
    /// Disable TLS entirely and talk plain HTTP. Maps to kopia's `--disable-tls`.
    /// Needed for HTTP-only endpoints (e.g. an in-cluster MinIO/RustFS service);
    /// kopia's S3 path otherwise assumes HTTPS. Off by default.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub disable_tls: bool,
}

/// Which kind of repository a consumer CR references. ADR §3.2/§3.3.
///
/// This is a closed enum: a consumer's `repository.kind` is always exactly one
/// of these two values, so reconcilers `match` it exhaustively.
///
/// ```
/// use kopiur_api::common::RepositoryKind;
///
/// // Defaults to the namespaced `Repository`, so a same-namespace ref needs no `kind`.
/// assert_eq!(RepositoryKind::default(), RepositoryKind::Repository);
/// // Serializes to the bare CRD kind name (no payload — a plain string).
/// assert_eq!(
///     serde_json::to_value(RepositoryKind::ClusterRepository).unwrap(),
///     "ClusterRepository"
/// );
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum RepositoryKind {
    /// The namespaced `Repository` CRD; the default when `kind` is omitted.
    #[default]
    Repository,
    /// The cluster-scoped `ClusterRepository` CRD; namespace is meaningless for it.
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
    /// Which repository CRD this points at; defaults to [`RepositoryKind::Repository`].
    #[serde(default)]
    pub kind: RepositoryKind,
    /// Name of the referenced `Repository`/`ClusterRepository`.
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

/// Opt-in projection of a repository's credential `Secret`(s) into the namespace
/// where each mover Job runs. **Default off.** ADR §3.1/§4.11.
///
/// Kopiur's baseline contract — like VolSync and K8up — is that the credential
/// Secret already exists in the namespace where a mover runs (it loads creds via
/// namespace-local `envFrom`). For a shared `ClusterRepository` whose Secret is
/// pinned to one namespace, that means placing a copy in each consuming namespace.
/// When `enabled`, the operator does that for you: before each run it reads the
/// source Secret(s) and writes a kopiur-managed copy into the Job's namespace,
/// owned by the consuming CR (garbage-collected with it) and refreshed from source
/// every run. (Cross-namespace secret distribution is opt-in across the ecosystem;
/// keeping it off by default preserves the namespace-as-trust-boundary posture.)
///
/// Even when enabled, projection is a no-op where the source Secret already lives
/// in the Job's namespace (the common namespaced-`Repository` layout): there is
/// nothing to copy, so the operator just verifies it is present. It only actually
/// copies for the cross-namespace case (a shared `ClusterRepository`).
///
/// A sub-object (not a bare `bool`) so future knobs (key remapping, a copy-name
/// template, immutability) slot in without API breakage (ADR §4.11).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CredentialProjection {
    /// When true, the operator copies the repository's credential Secret(s) into
    /// the namespace of each mover Job that uses this repository. Off by default.
    #[serde(default)]
    pub enabled: bool,
}

/// Behavior when the repository does not yet exist. ADR §3.1 `create`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateBehavior {
    /// Create the repository if it does not exist yet. Off by default, so a typo'd
    /// backend can't silently spin up a brand-new empty repository.
    #[serde(default)]
    pub enabled: bool,
    /// kopia encryption algorithm for a freshly-created repository (e.g.
    /// `AES256-GCM-HMAC-SHA256`); only consulted at creation time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<String>,
    /// kopia object splitter for a freshly-created repository; creation-time only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub splitter: Option<String>,
    /// kopia content hash algorithm for a freshly-created repository; creation-time only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

/// How a mover's kopia cache volume is provisioned. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum CacheVolumeMode {
    /// Cache lives only for the run: an inline generic ephemeral volume (when
    /// `capacity` is set) or an `emptyDir`, provisioned and garbage-collected with
    /// the mover `Job`. Fresh each run. The default.
    #[default]
    Ephemeral,
    /// Cache persists across runs in a controller-owned PVC (a warm kopia cache).
    /// `ReadWriteOnce`, so it assumes non-overlapping runs for a given owner.
    Persistent,
}

/// Cache defaults inherited by `Backup`/`Restore` movers unless overridden. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CacheDefaults {
    /// Size of the PVC backing the mover's kopia cache (e.g. `10Gi`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<String>,
    /// StorageClass for the cache PVC; absent uses the cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
    /// kopia metadata cache budget in MiB (`--metadata-cache-size-mb`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_cache_size_mb: Option<i64>,
    /// kopia content cache budget in MiB (`--content-cache-size-mb`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_cache_size_mb: Option<i64>,
    /// How the cache volume is provisioned (`Ephemeral` default, or `Persistent`
    /// for a warm cache reused across runs). ADR §3.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<CacheVolumeMode>,
}

impl CacheDefaults {
    /// Overlay `over` onto `base` field-by-field — a value set in `over` wins,
    /// otherwise `base`'s is kept. Resolves a mover's effective cache config from the
    /// repository's `cacheDefaults` (base) and the run's `mover.cache` (override).
    /// Returns `None` only when both are absent.
    pub fn merge(
        base: Option<&CacheDefaults>,
        over: Option<&CacheDefaults>,
    ) -> Option<CacheDefaults> {
        match (base, over) {
            (None, None) => None,
            (Some(b), None) => Some(b.clone()),
            (None, Some(o)) => Some(o.clone()),
            (Some(b), Some(o)) => Some(CacheDefaults {
                capacity: o.capacity.clone().or_else(|| b.capacity.clone()),
                storage_class_name: o
                    .storage_class_name
                    .clone()
                    .or_else(|| b.storage_class_name.clone()),
                metadata_cache_size_mb: o.metadata_cache_size_mb.or(b.metadata_cache_size_mb),
                content_cache_size_mb: o.content_cache_size_mb.or(b.content_cache_size_mb),
                mode: o.mode.or(b.mode),
            }),
        }
    }

    /// The provisioning mode, defaulting to `Ephemeral` when unset.
    pub fn effective_mode(&self) -> CacheVolumeMode {
        self.mode.unwrap_or_default()
    }
}

/// Bounds on materialization of `origin: discovered` `Backup` CRs. ADR §3.1 `catalog`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogBounds {
    /// How many discovered `Backup` CRs to keep materialized; bounds etcd footprint
    /// for large repositories. Never deletes real snapshots (discovered backups are
    /// always `deletionPolicy: Retain`). ADR §3.1/§4.5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain: Option<CatalogRetain>,
    /// How often to re-scan the repository for new snapshots to materialize
    /// (Go-style duration, e.g. `1h`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_interval: Option<String>,
    /// Where to materialize discovered `Backup`s whose identity hostname does not
    /// map to an allowed namespace (ClusterRepository only). ADR §3.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_namespace: Option<String>,
}

/// Bounds on the *number* of discovered `Backup` CRs kept materialized. ADR §3.1 `catalog.retain`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogRetain {
    /// Most-recent N per `username@hostname:path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_identity: Option<i64>,
    /// Drop materialized discovered `Backup`s for snapshots older than this many days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_days: Option<i64>,
}

/// GFS retention policy. The single successful-retention driver (ADR §4.4).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Retention {
    /// Keep the N most-recent snapshots regardless of age.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_latest: Option<u32>,
    /// Keep one snapshot per hour for the most-recent N hours.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_hourly: Option<u32>,
    /// Keep one snapshot per day for the most-recent N days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_daily: Option<u32>,
    /// Keep one snapshot per week for the most-recent N weeks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_weekly: Option<u32>,
    /// Keep one snapshot per month for the most-recent N months.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_monthly: Option<u32>,
    /// Keep one snapshot per year for the most-recent N years.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_annual: Option<u32>,
}

/// Identity overrides — what kopia records as `username@hostname:path`. ADR §3.3/§4.2.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    /// Override the `username` portion of `username@hostname:path`; absent uses the
    /// resolved default. Templated with `tera` and pinned at admission (ADR §4.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Override the `hostname` portion of `username@hostname:path`; absent uses the
    /// resolved default. Templated with `tera` and pinned at admission (ADR §4.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
}

/// Fully-resolved identity pinned into status; never re-rendered after admission. ADR §4.2.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedIdentity {
    /// The final `username` kopia records, fixed at admission.
    pub username: String,
    /// The final `hostname` kopia records, fixed at admission.
    pub hostname: String,
    /// The resolved snapshot source path, when applicable (`username@hostname:path`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
}

/// Per-run failure controls passed through to the mover `Job`. ADR §3.4/§4.10 (G6).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FailurePolicy {
    /// Passed through to the mover `Job.spec.backoffLimit` — how many times a failed
    /// run is retried before the Job is marked failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_limit: Option<i32>,
    /// Passed through to the mover `Job.spec.activeDeadlineSeconds` — wall-clock cap
    /// after which a still-running run is killed.
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
    /// Resource requests/limits for the mover container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<k8s_openapi::api::core::v1::ResourceRequirements>,
    /// Override the repository's [`CacheDefaults`] for this recipe's movers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheDefaults>,
    /// Security context applied to the mover **container** (`runAsUser`/`runAsGroup`,
    /// capabilities, seccomp, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_context: Option<k8s_openapi::api::core::v1::SecurityContext>,
    /// Security context applied to the mover **pod** — notably `fsGroup`, which makes
    /// a freshly-provisioned volume group-writable so an unprivileged
    /// (`runAsUser != 0`) mover can populate it on **restore** without root. Distinct
    /// from the container-level [`MoverSpec::security_context`]; a pod-level
    /// `runAsUser: 0` / `runAsNonRoot: false` here is still gated as privileged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_security_context: Option<k8s_openapi::api::core::v1::PodSecurityContext>,
    /// Opt-in, namespace-gated; preserves UID/GID on restore. ADR §4.11/§G16.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privileged_mode: Option<bool>,
    /// Opt-in: copy security context from a live workload pod. ADR §4.11.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherit_security_context_from: Option<PodSelector>,
}

impl MoverSpec {
    /// Whether this mover requests **elevated privileges** that the workload
    /// namespace must explicitly opt into (ADR §4.11/§G16). True when
    /// `privilegedMode` is set, or the `securityContext` runs as root / privileged
    /// / with escalation / with added Linux capabilities.
    ///
    /// The rationale is the same as VolSync's `privileged-movers` model: the
    /// controller mints a mover `ServiceAccount` in the workload namespace, and a
    /// tenant with access there could reuse it to run pods at the mover's privilege.
    /// Granting an elevated mover is therefore a per-namespace admin decision, gated
    /// by a namespace annotation rather than allowed implicitly. Pure + exhaustive
    /// so the definition of "privileged" lives in one tested place.
    pub fn requires_privilege(&self) -> bool {
        requires_privilege_resolved(
            self.security_context.as_ref(),
            self.pod_security_context.as_ref(),
            self.privileged_mode,
        )
    }
}

/// Whether a mover with the given **effective** container security context (the
/// explicit `securityContext`, or the one resolved from `inheritSecurityContextFrom`),
/// **pod** security context, and `privilegedMode` is privileged. The controller
/// resolves an inherited context to a concrete `SecurityContext` and gates on *that* —
/// so an inherited root context is caught exactly like an explicit one — and inspects
/// the pod-level context too so a pod-level `runAsUser: 0` can't slip past. Pure +
/// exhaustive: the single definition of "privileged" for both the spec-only
/// ([`MoverSpec::requires_privilege`]) and the resolved paths.
pub fn requires_privilege_resolved(
    security_context: Option<&k8s_openapi::api::core::v1::SecurityContext>,
    pod_security_context: Option<&k8s_openapi::api::core::v1::PodSecurityContext>,
    privileged_mode: Option<bool>,
) -> bool {
    privileged_mode == Some(true)
        || security_context.is_some_and(security_context_is_elevated)
        || pod_security_context.is_some_and(pod_security_context_is_elevated)
}

/// Whether a container `SecurityContext` requests privileges beyond a normal
/// unprivileged user (root UID, `privileged`, escalation, added capabilities, or an
/// explicit `runAsNonRoot: false`). Pure helper for [`MoverSpec::requires_privilege`].
pub fn security_context_is_elevated(sc: &k8s_openapi::api::core::v1::SecurityContext) -> bool {
    sc.privileged == Some(true)
        || sc.run_as_user == Some(0)
        || sc.run_as_non_root == Some(false)
        || sc.allow_privilege_escalation == Some(true)
        || sc
            .capabilities
            .as_ref()
            .and_then(|c| c.add.as_ref())
            .is_some_and(|add| !add.is_empty())
}

/// Whether a **pod** `PodSecurityContext` requests root. Pod-level only carries a
/// subset of the container knobs — `runAsUser` / `runAsNonRoot` are the ones that can
/// make the mover root (capabilities/privileged are container-only). `fsGroup` and
/// friends are NOT elevation. Pure helper for [`requires_privilege_resolved`].
pub fn pod_security_context_is_elevated(
    psc: &k8s_openapi::api::core::v1::PodSecurityContext,
) -> bool {
    psc.run_as_user == Some(0) || psc.run_as_non_root == Some(false)
}

/// Selects workload pods by label. Reuses k8s-openapi `LabelSelector`. ADR §3.3 hooks.
///
/// Not `Eq`: `LabelSelector` only implements `PartialEq`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PodSelector {
    /// Label selector matching the workload pod(s) to read context/hooks from.
    pub pod_selector: LabelSelector,
    /// Which container within the matched pod; absent uses the first/only container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
}

/// Reference to a `BackupConfig` CR (used by `Backup.spec.configRef` and
/// `BackupSchedule.spec.configRef`). May cross namespaces, subject to RBAC. ADR §3.4/§3.5.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigRef {
    /// Name of the referenced `BackupConfig`.
    pub name: String,
    /// Namespace of the `BackupConfig`; absent = same namespace as the referrer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Generic name/namespace reference to another namespaced object — e.g. a `Backup`
/// CR (`Restore.spec.source.backupRef`) or a PVC (`Restore.spec.target.pvcRef`). ADR §3.6.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObjectRef {
    /// Name of the referenced object.
    pub name: String,
    /// Namespace of the referenced object; absent = same namespace as the referrer.
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
///
/// ```
/// use kopiur_api::common::DeletionPolicy;
///
/// // Produced backups default to deleting the snapshot with the CR.
/// assert_eq!(DeletionPolicy::default(), DeletionPolicy::Delete);
/// // Variants serialize to their bare PascalCase names (plain string enum).
/// assert_eq!(serde_json::to_value(DeletionPolicy::Retain).unwrap(), "Retain");
/// assert_eq!(serde_json::to_value(DeletionPolicy::Orphan).unwrap(), "Orphan");
/// ```
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
    /// The cron expression, parsed by `croner`. May contain an `H` placeholder for
    /// deterministic per-schedule jitter (ADR §3.7).
    pub cron: String,
    /// Optional deterministic jitter window as a Go-style duration string (e.g.
    /// `30m`), derived from `(scheduleUID, slot)` so it is stable across restarts.
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
    ///
    /// ```
    /// use kopiur_api::common::{RepositoryKind, RepositoryRef};
    ///
    /// // A namespaced ref that omits `namespace` resolves against the owner's namespace.
    /// let r = RepositoryRef { kind: RepositoryKind::Repository, name: "nas".into(), namespace: None };
    /// assert!(r.resolves_to("apps", RepositoryKind::Repository, "nas", Some("apps")));
    /// assert!(!r.resolves_to("apps", RepositoryKind::Repository, "nas", Some("other")));
    ///
    /// // A cluster-scoped target ignores namespace entirely.
    /// let cr = RepositoryRef {
    ///     kind: RepositoryKind::ClusterRepository,
    ///     name: "hetzner".into(),
    ///     namespace: None,
    /// };
    /// assert!(cr.resolves_to("apps", RepositoryKind::ClusterRepository, "hetzner", None));
    /// // Kind must match even when names collide.
    /// assert!(!r.resolves_to("apps", RepositoryKind::ClusterRepository, "nas", None));
    /// ```
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

    // --- cache-defaults merge (repository cacheDefaults ← mover.cache) ---

    #[test]
    fn cache_defaults_merge_overlays_field_by_field() {
        // Neither side → nothing to apply.
        assert_eq!(CacheDefaults::merge(None, None), None);

        let repo = CacheDefaults {
            capacity: Some("8Gi".into()),
            storage_class_name: Some("standard".into()),
            metadata_cache_size_mb: Some(1024),
            content_cache_size_mb: Some(4096),
            mode: Some(CacheVolumeMode::Ephemeral),
        };
        // Only base → base verbatim.
        assert_eq!(CacheDefaults::merge(Some(&repo), None), Some(repo.clone()));

        // Override wins per-field; unset override fields fall back to base.
        let mover = CacheDefaults {
            capacity: Some("32Gi".into()),
            storage_class_name: None,
            metadata_cache_size_mb: None,
            content_cache_size_mb: Some(16384),
            mode: Some(CacheVolumeMode::Persistent),
        };
        let merged = CacheDefaults::merge(Some(&repo), Some(&mover)).unwrap();
        assert_eq!(merged.capacity.as_deref(), Some("32Gi")); // override
        assert_eq!(merged.storage_class_name.as_deref(), Some("standard")); // base
        assert_eq!(merged.metadata_cache_size_mb, Some(1024)); // base
        assert_eq!(merged.content_cache_size_mb, Some(16384)); // override
        assert_eq!(merged.mode, Some(CacheVolumeMode::Persistent)); // override
        assert_eq!(merged.effective_mode(), CacheVolumeMode::Persistent);

        // Unset mode defaults to Ephemeral.
        assert_eq!(
            CacheDefaults::default().effective_mode(),
            CacheVolumeMode::Ephemeral
        );
    }

    // --- privileged-mover detection (ADR §4.11/§G16, namespace-gated). ---

    use k8s_openapi::api::core::v1::{Capabilities, SecurityContext};

    fn mover_with(sc: Option<SecurityContext>, privileged_mode: Option<bool>) -> MoverSpec {
        MoverSpec {
            security_context: sc,
            privileged_mode,
            ..Default::default()
        }
    }

    #[test]
    fn default_mover_is_unprivileged() {
        assert!(!MoverSpec::default().requires_privilege());
        // A benign securityContext (non-root, no escalation) is not privileged.
        let benign = SecurityContext {
            run_as_user: Some(1000),
            run_as_non_root: Some(true),
            allow_privilege_escalation: Some(false),
            ..Default::default()
        };
        assert!(!mover_with(Some(benign), None).requires_privilege());
    }

    #[test]
    fn run_as_root_requires_privilege() {
        // The trilium-rain case: mover.securityContext.runAsUser: 0.
        let root = SecurityContext {
            run_as_user: Some(0),
            ..Default::default()
        };
        assert!(mover_with(Some(root), None).requires_privilege());
    }

    #[test]
    fn privileged_flag_and_escalation_and_caps_and_nonroot_false_all_count() {
        let priv_ctx = SecurityContext {
            privileged: Some(true),
            ..Default::default()
        };
        assert!(mover_with(Some(priv_ctx), None).requires_privilege());

        let escalate = SecurityContext {
            allow_privilege_escalation: Some(true),
            ..Default::default()
        };
        assert!(mover_with(Some(escalate), None).requires_privilege());

        let caps = SecurityContext {
            capabilities: Some(Capabilities {
                add: Some(vec!["SYS_ADMIN".into()]),
                drop: None,
            }),
            ..Default::default()
        };
        assert!(mover_with(Some(caps), None).requires_privilege());

        let nonroot_false = SecurityContext {
            run_as_non_root: Some(false),
            ..Default::default()
        };
        assert!(mover_with(Some(nonroot_false), None).requires_privilege());
    }

    #[test]
    fn privileged_mode_flag_alone_requires_privilege() {
        assert!(mover_with(None, Some(true)).requires_privilege());
        assert!(!mover_with(None, Some(false)).requires_privilege());
    }

    #[test]
    fn empty_added_capabilities_is_not_privileged() {
        let caps = SecurityContext {
            capabilities: Some(Capabilities {
                add: Some(vec![]),
                drop: Some(vec!["ALL".into()]),
            }),
            ..Default::default()
        };
        assert!(!mover_with(Some(caps), None).requires_privilege());
    }

    #[test]
    fn pod_level_fsgroup_is_not_privileged_but_pod_level_root_is() {
        use k8s_openapi::api::core::v1::PodSecurityContext;
        // fsGroup (the headline use) is NOT elevation — an unprivileged mover with
        // fsGroup must run without a namespace opt-in.
        let fsgroup = MoverSpec {
            pod_security_context: Some(PodSecurityContext {
                fs_group: Some(1000),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(!fsgroup.requires_privilege());

        // ...but a pod-level runAsUser: 0 / runAsNonRoot: false IS gated, so it can't
        // slip past the container-only check.
        let pod_root = MoverSpec {
            pod_security_context: Some(PodSecurityContext {
                run_as_user: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(pod_root.requires_privilege());
        let pod_nonroot_false = MoverSpec {
            pod_security_context: Some(PodSecurityContext {
                run_as_non_root: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(pod_nonroot_false.requires_privilege());
    }
}
