//! Shared sub-objects reused across multiple CRDs.
//!
//! Per ADR-0003 §2.2 (principle 10) and §4.11, every credential, policy, and
//! identity surface is modeled as a sub-object so future fields slot in without
//! API breakage. Leaf Kubernetes types (`LabelSelector`, `ResourceRequirements`,
//! `PodSecurityContext`) are reused from `k8s-openapi` rather than re-invented.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    Affinity, Capabilities, PodSecurityContext, ResourceRequirements, SeccompProfile,
    SecurityContext, Toleration,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
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

/// Discriminated reference from a consumer CR (`SnapshotPolicy`, `Snapshot`,
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
    /// Reed-Solomon ECC parity guarding repo blobs against backend bit-rot
    /// (`kopia repository create --ecc=... --ecc-overhead-percent=...`). Creation-time
    /// only and immutable post-create (ADR-0005 §13(a), gated by §7). ADR-0005 §13(a).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ecc: Option<Ecc>,
}

/// Reed-Solomon error-correcting-code parity for a freshly-created repository
/// (ADR-0005 §13(a)). Both fields creation-time-fixed; immutable post-create (§7).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Ecc {
    /// ECC algorithm, e.g. `REED-SOLOMON-CRC32` (`--ecc`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// Parity overhead as a percentage (`--ecc-overhead-percent`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overhead_percent: Option<i64>,
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

/// Cache defaults inherited by `Snapshot`/`Restore` movers unless overridden. ADR §3.1.
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

/// Bounds on materialization of `origin: discovered` `Snapshot` CRs. ADR §3.1 `catalog`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogBounds {
    /// How many discovered `Snapshot` CRs to keep materialized; bounds etcd footprint
    /// for large repositories. Expiring a CR row never deletes the kopia snapshot
    /// behind it (discovered snapshots are always `deletionPolicy: Retain`).
    /// ADR §3.1/§4.5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain: Option<CatalogRetain>,
    /// How often to re-scan the repository for snapshots to materialize as (or
    /// expire from) `origin: discovered` `Snapshot` CRs. Go-style duration
    /// (`30s`, `5m`, `1h`); minimum `30s` (webhook-enforced), default `1h`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_interval: Option<String>,
    /// Where to materialize discovered `Snapshot`s whose identity hostname does not
    /// map to an allowed namespace (ClusterRepository only; rejected on a namespaced
    /// `Repository`, which always materializes into its own namespace). ADR §3.2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_namespace: Option<String>,
}

impl CatalogBounds {
    /// The effective catalog re-scan cadence: `refreshInterval` when set and
    /// parseable, else [`crate::consts::DEFAULT_CATALOG_REFRESH_INTERVAL`].
    /// (The webhook rejects an unparseable value, so the fallback only covers
    /// objects admitted before the validator existed.)
    pub fn effective_refresh_interval(catalog: Option<&Self>) -> std::time::Duration {
        catalog
            .and_then(|c| c.refresh_interval.as_deref())
            .and_then(crate::duration::parse_go_duration)
            .unwrap_or(crate::consts::DEFAULT_CATALOG_REFRESH_INTERVAL)
    }
}

/// Bounds on the *number* of discovered `Snapshot` CRs kept materialized. ADR §3.1 `catalog.retain`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogRetain {
    /// Keep the most-recent N discovered `Snapshot` CRs per `username@hostname:path`
    /// identity (snapshots this cluster produced don't count against N). `0` disables
    /// discovered-Snapshot materialization entirely; negative values are rejected by
    /// the webhook. Rows beyond N are expired (the CR is deleted; the kopia snapshot
    /// is untouched and stays restorable via `Restore.source.identity`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_identity: Option<i64>,
    /// Don't materialize (and expire) discovered `Snapshot` CRs for snapshots whose
    /// end time is older than this many days. Minimum 1 (webhook-enforced). The
    /// kopia snapshots themselves are untouched.
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
    /// resolved default (the repository's `identityDefaults` CEL expression, or the
    /// object name). Used verbatim and pinned at admission (ADR §4.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Override the `hostname` portion of `username@hostname:path`; absent uses the
    /// resolved default (the repository's `identityDefaults` CEL expression, or the
    /// namespace). Used verbatim and pinned at admission (ADR §4.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
}

/// Byte cap for `status.logTail` (and the stderr tail inside
/// [`FailureBlock`]): the mover truncates to the LAST `MAX_LOG_TAIL_BYTES`
/// bytes before patching status, so a noisy kopia run can't bloat etcd. Full
/// logs live in the mover Job's pod. ADR §3.4/§4.10.
pub const MAX_LOG_TAIL_BYTES: usize = 4096;

/// A structured terminal-failure block written by the mover to `status.failure`
/// (ADR §4.10): the kopia error class, a human-readable message, the last
/// stderr lines, and a retry recommendation. Defined in `kopiur-api` (not the
/// mover) so the field names cannot drift from the CRD structural schema — a
/// mismatched name is silently pruned by the API server.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FailureBlock {
    /// kopia error class (e.g. `RepositoryUnavailable`, `AuthFailure`).
    pub kopia_error_class: String,
    /// A short human-readable message: what failed, why, and how to fix it.
    pub message: String,
    /// The last lines of kopia's stderr, if any were captured (bounded by
    /// [`MAX_LOG_TAIL_BYTES`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_tail: Option<String>,
    /// The process exit code, if one was reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Whether retrying the same operation unchanged could succeed.
    pub retry_recommended: bool,
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
/// These overlay the repository's [`MoverDefaults`] **field-wise** (recipe wins, the
/// repo default fills, the hardened base underneath) via [`resolve_mover`] — they are
/// merged, never replace-the-whole-context (ADR-0004 §2). A partial `securityContext`
/// here can therefore only *tighten*; it never drops the hardened `drop:[ALL]`/seccomp.
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
    /// capabilities, seccomp, …). Merged field-wise over `moverDefaults.securityContext`
    /// and the hardened base (ADR-0004 §2) — set only the fields you want to change.
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
    /// Per-recipe override of `moverDefaults.ttlSecondsAfterFinished` — the
    /// `Job.spec.ttlSecondsAfterFinished` for this recipe's mover Jobs so finished
    /// backup/restore Jobs self-GC. Recipe wins over the repo default; when neither
    /// is set a built-in default applies ([`DEFAULT_JOB_TTL_SECONDS`]). ADR-0005 §12.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds_after_finished: Option<i64>,
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

/// The restricted-PSA-compatible **hardened** container security context (§4.11/G16):
/// non-root, no privilege escalation, drop ALL caps, seccomp `RuntimeDefault`.
///
/// This is the LOWEST merge layer (ADR-0004 §2): `repo.moverDefaults.securityContext`
/// then the recipe's `mover.securityContext` overlay it **field-wise**, so a partial
/// override can only *tighten* — it never drops `capabilities.drop:[ALL]` /
/// `seccompProfile`. Lives in `api` (not the controller) so the webhook and controller
/// share one definition and both resolve the effective mover context identically.
pub fn hardened_security_context() -> SecurityContext {
    SecurityContext {
        run_as_non_root: Some(true),
        allow_privilege_escalation: Some(false),
        read_only_root_filesystem: Some(false),
        capabilities: Some(Capabilities {
            drop: Some(vec!["ALL".to_string()]),
            add: None,
        }),
        seccomp_profile: Some(SeccompProfile {
            type_: "RuntimeDefault".to_string(),
            localhost_profile: None,
        }),
        ..Default::default()
    }
}

/// The nonroot UID/GID baked into the mover image (`docker/Dockerfile.mover`:
/// `USER 65532:65532`, distroless `nonroot`). The hardened **pod** context defaults
/// `fsGroup` to this so the kubelet group-owns every mounted volume to the gid the
/// mover actually runs as — most importantly the operator-managed kopia cache, which
/// is otherwise created `root:root` on PVC-backed storage and unwritable by the
/// unprivileged mover. Centralized here (the single source of the hardened defaults)
/// so the value can never drift from the image.
pub const MOVER_NONROOT_ID: i64 = 65532;

/// The restricted-PSA-compatible **hardened pod** security context — the pod-level
/// peer of [`hardened_security_context`]. Defaults `fsGroup` to [`MOVER_NONROOT_ID`]
/// so every mover pod's volumes (notably the cache) are writable by the unprivileged
/// mover; `fsGroupChangePolicy: OnRootMismatch` skips the recursive chown when the
/// volume root already matches, so it does not needlessly rewrite ownership on every
/// run.
///
/// Same merge story as the container context (ADR-0004 §2): this is the LOWEST layer,
/// overlaid field-wise by `repo.moverDefaults.podSecurityContext` then the recipe's
/// `mover.podSecurityContext`, so any of `fsGroup`/`runAsUser`/… can be overridden
/// (e.g. a restore that must own files as the app's UID) while unset fields keep the
/// hardened default. Lives in `api` so the webhook and controller resolve it identically.
pub fn hardened_pod_security_context() -> PodSecurityContext {
    PodSecurityContext {
        fs_group: Some(MOVER_NONROOT_ID),
        fs_group_change_policy: Some("OnRootMismatch".to_string()),
        ..Default::default()
    }
}

/// Deep-merge two [`Capabilities`]: each of `add`/`drop` is taken from `over` when set,
/// else from `base`. So an `over` that sets only `add` keeps `base.drop` — an add-only
/// override never silently drops the hardened `drop:[ALL]` (the bug ADR-0004 §2 cites).
pub fn merge_capabilities(base: &Capabilities, over: &Capabilities) -> Capabilities {
    Capabilities {
        add: over.add.clone().or_else(|| base.add.clone()),
        drop: over.drop.clone().or_else(|| base.drop.clone()),
    }
}

/// Field-wise overlay of container [`SecurityContext`] `over` onto `base`: each `Some`
/// field in `over` wins, unset fields inherit `base`; `capabilities` deep-merge via
/// [`merge_capabilities`] (ADR-0004 §2).
///
/// The struct literal is **exhaustive** (no `..base` tail) on purpose: when the pinned
/// k8s-openapi `SecurityContext` gains a field, this stops compiling until the new field
/// is considered — the same discipline as the exhaustive-`match` enum thesis (§5.5).
pub fn merge_security_context(base: &SecurityContext, over: &SecurityContext) -> SecurityContext {
    SecurityContext {
        allow_privilege_escalation: over
            .allow_privilege_escalation
            .or(base.allow_privilege_escalation),
        app_armor_profile: over
            .app_armor_profile
            .clone()
            .or_else(|| base.app_armor_profile.clone()),
        capabilities: match (base.capabilities.as_ref(), over.capabilities.as_ref()) {
            (Some(b), Some(o)) => Some(merge_capabilities(b, o)),
            (b, o) => o.cloned().or_else(|| b.cloned()),
        },
        privileged: over.privileged.or(base.privileged),
        proc_mount: over.proc_mount.clone().or_else(|| base.proc_mount.clone()),
        read_only_root_filesystem: over
            .read_only_root_filesystem
            .or(base.read_only_root_filesystem),
        run_as_group: over.run_as_group.or(base.run_as_group),
        run_as_non_root: over.run_as_non_root.or(base.run_as_non_root),
        run_as_user: over.run_as_user.or(base.run_as_user),
        se_linux_options: over
            .se_linux_options
            .clone()
            .or_else(|| base.se_linux_options.clone()),
        seccomp_profile: over
            .seccomp_profile
            .clone()
            .or_else(|| base.seccomp_profile.clone()),
        windows_options: over
            .windows_options
            .clone()
            .or_else(|| base.windows_options.clone()),
    }
}

/// Field-wise overlay of pod [`PodSecurityContext`] `over` onto `base`. Exhaustive
/// literal for the same reason as [`merge_security_context`].
pub fn merge_pod_security_context(
    base: &PodSecurityContext,
    over: &PodSecurityContext,
) -> PodSecurityContext {
    PodSecurityContext {
        app_armor_profile: over
            .app_armor_profile
            .clone()
            .or_else(|| base.app_armor_profile.clone()),
        fs_group: over.fs_group.or(base.fs_group),
        fs_group_change_policy: over
            .fs_group_change_policy
            .clone()
            .or_else(|| base.fs_group_change_policy.clone()),
        run_as_group: over.run_as_group.or(base.run_as_group),
        run_as_non_root: over.run_as_non_root.or(base.run_as_non_root),
        run_as_user: over.run_as_user.or(base.run_as_user),
        se_linux_change_policy: over
            .se_linux_change_policy
            .clone()
            .or_else(|| base.se_linux_change_policy.clone()),
        se_linux_options: over
            .se_linux_options
            .clone()
            .or_else(|| base.se_linux_options.clone()),
        seccomp_profile: over
            .seccomp_profile
            .clone()
            .or_else(|| base.seccomp_profile.clone()),
        supplemental_groups: over
            .supplemental_groups
            .clone()
            .or_else(|| base.supplemental_groups.clone()),
        supplemental_groups_policy: over
            .supplemental_groups_policy
            .clone()
            .or_else(|| base.supplemental_groups_policy.clone()),
        sysctls: over.sysctls.clone().or_else(|| base.sysctls.clone()),
        windows_options: over
            .windows_options
            .clone()
            .or_else(|| base.windows_options.clone()),
    }
}

/// Per-key merge of two `limits`/`requests` quantity maps: `over` keys win, `base` keys
/// fill. Returns `None` only when both are absent.
fn merge_quantity_map(
    base: Option<&BTreeMap<String, Quantity>>,
    over: Option<&BTreeMap<String, Quantity>>,
) -> Option<BTreeMap<String, Quantity>> {
    match (base, over) {
        (None, None) => None,
        (Some(b), None) => Some(b.clone()),
        (None, Some(o)) => Some(o.clone()),
        (Some(b), Some(o)) => {
            let mut merged = b.clone();
            for (k, v) in o {
                merged.insert(k.clone(), v.clone());
            }
            Some(merged)
        }
    }
}

/// Field-wise overlay of [`ResourceRequirements`]: `limits`/`requests` merge per-key
/// (via `merge_quantity_map`); `claims` is taken from `over` when set, else `base`.
pub fn merge_resources(
    base: &ResourceRequirements,
    over: &ResourceRequirements,
) -> ResourceRequirements {
    ResourceRequirements {
        claims: over.claims.clone().or_else(|| base.claims.clone()),
        limits: merge_quantity_map(base.limits.as_ref(), over.limits.as_ref()),
        requests: merge_quantity_map(base.requests.as_ref(), over.requests.as_ref()),
    }
}

/// `Option`-aware [`merge_security_context`] (handles the four `None`/`Some` cases).
pub fn merge_security_context_opt(
    base: Option<&SecurityContext>,
    over: Option<&SecurityContext>,
) -> Option<SecurityContext> {
    match (base, over) {
        (None, None) => None,
        (Some(b), None) => Some(b.clone()),
        (None, Some(o)) => Some(o.clone()),
        (Some(b), Some(o)) => Some(merge_security_context(b, o)),
    }
}

/// `Option`-aware [`merge_pod_security_context`].
pub fn merge_pod_security_context_opt(
    base: Option<&PodSecurityContext>,
    over: Option<&PodSecurityContext>,
) -> Option<PodSecurityContext> {
    match (base, over) {
        (None, None) => None,
        (Some(b), None) => Some(b.clone()),
        (None, Some(o)) => Some(o.clone()),
        (Some(b), Some(o)) => Some(merge_pod_security_context(b, o)),
    }
}

/// `Option`-aware [`merge_resources`].
pub fn merge_resources_opt(
    base: Option<&ResourceRequirements>,
    over: Option<&ResourceRequirements>,
) -> Option<ResourceRequirements> {
    match (base, over) {
        (None, None) => None,
        (Some(b), None) => Some(b.clone()),
        (None, Some(o)) => Some(o.clone()),
        (Some(b), Some(o)) => Some(merge_resources(b, o)),
    }
}

/// How the mover pod co-locates with the node a `ReadWriteOnce` source/destination
/// PVC is attached to, to avoid a Kubernetes **Multi-Attach error**.
///
/// A `ReadWriteOnce` (RWO) PVC can only be attached to one node at a time, but it
/// *can* be mounted by multiple pods **on that same node**. When an app pod already
/// holds an RWO PVC on node A and the mover lands on node B, the kubelet on B cannot
/// attach the volume and the mover pod is stuck `Multi-Attach error`. The controller
/// resolves the node the PVC is attached to (consuming pod → PV `nodeAffinity` →
/// `VolumeAttachment`) and pins the mover there so it co-locates with the workload.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum SourceColocationMode {
    /// Pin an RWO PVC's mover to the node the PVC is attached to **when** that node
    /// is discoverable; otherwise schedule freely (nothing holds the volume, so the
    /// mover can attach it anywhere). `ReadWriteMany`/`ReadOnlyMany` are never pinned.
    /// A `ReadWriteOncePod` PVC that is already held by a live pod fails with guidance
    /// (a second pod cannot mount it even on the same node). The default — fixes the
    /// Multi-Attach error with no configuration.
    #[default]
    Auto,
    /// Like `Auto`, but if an RWO PVC's node cannot be determined, **fail** the run
    /// with an actionable error instead of scheduling freely. Use when an RWO source
    /// must never be backed up from the wrong node.
    Required,
    /// Never compute a node pin; the mover uses only the explicit
    /// `nodeSelector`/`affinity`/`tolerations`. The pre-fix behavior — an escape hatch
    /// for topologies that manage placement themselves.
    Disabled,
}

/// Controls mover/source-PVC node co-location (RWO Multi-Attach avoidance). A
/// sub-object (not a bare enum) so future knobs — e.g. a custom hostname label key
/// for non-standard topologies — slot in without an API break (ADR §4.11).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SourceColocation {
    /// The co-location strategy. Defaults to [`SourceColocationMode::Auto`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<SourceColocationMode>,
}

/// Repository-wide mover defaults inherited by **every** mover the repository spawns —
/// bootstrap, backup, restore, maintenance — overridable per-recipe via `mover`
/// (ADR-0004 §1). Replaces the former `cacheDefaults`: the cache lives at
/// [`MoverDefaults::cache`] now.
///
/// `securityContext`/`podSecurityContext`/`resources`/`cache` resolve by **field-wise
/// merge** (`hardened ⊂ moverDefaults ⊂ recipe`, ADR-0004 §2) via [`resolve_mover`];
/// they are never replaced wholesale, so a repo-wide default composes with a partial
/// per-recipe override. This is the single place a repository defines mover
/// identity/hardening/resources/cache — closing the drift between maintenance and
/// backup/restore movers and the bootstrap-mover gap (a filesystem/NFS repo on a
/// non-`65532`-owned directory becomes bootstrappable with no special-case knob).
///
/// Not `Eq`: embeds `k8s-openapi` types (`SecurityContext`, `PodSecurityContext`,
/// `ResourceRequirements`, `Toleration`, `Affinity`) which are `PartialEq` only.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MoverDefaults {
    /// Container security-context base for every mover, merged *under* the recipe's
    /// `mover.securityContext` and *over* the hardened default ([`hardened_security_context`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_context: Option<SecurityContext>,
    /// Pod security-context base (notably `fsGroup`) for every mover, merged under the
    /// recipe's `mover.podSecurityContext`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_security_context: Option<PodSecurityContext>,
    /// Resource requests/limits base for the mover container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
    /// kopia cache defaults (the former repository `cacheDefaults`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheDefaults>,
    /// Pod `nodeSelector` for every mover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<BTreeMap<String, String>>,
    /// Pod tolerations for every mover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerations: Option<Vec<Toleration>>,
    /// Pod affinity for every mover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<Affinity>,
    /// How a mover co-locates with the node its RWO source/destination PVC is
    /// attached to, to avoid a Multi-Attach error. Defaults to
    /// [`SourceColocationMode::Auto`] when unset. ADR §3.7 / RWO multi-attach fix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_colocation: Option<SourceColocation>,
    /// `Job.spec.ttlSecondsAfterFinished` for every mover Job, so finished
    /// backup/restore/maintenance Jobs self-GC (ADR-0005 §12). A recipe's
    /// `mover` can override it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds_after_finished: Option<i64>,
    /// Repository throttle limits (`kopia repository throttle set`) applied by every
    /// mover after it connects, so a run doesn't saturate the link / hammer the
    /// object store. ADR-0005 §13(e).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub throttle: Option<Throttle>,
}

/// Built-in default `Job.spec.ttlSecondsAfterFinished` (1h) applied to a mover Job
/// when neither `moverDefaults.ttlSecondsAfterFinished` nor the recipe's
/// `mover.ttlSecondsAfterFinished` sets one, so finished backup/restore Jobs and
/// their pods self-GC instead of lingering (ADR-0005 §12).
pub const DEFAULT_JOB_TTL_SECONDS: i64 = 3600;

/// Repository-wide throttling for a mover's kopia connection (ADR-0005 §13(e)).
/// Each `None` leaves kopia's current limit. Maps to `kopia repository throttle set`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Throttle {
    /// Cap upload throughput in bytes/sec (`--upload-bytes-per-second`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_bytes_per_second: Option<i64>,
    /// Cap download throughput in bytes/sec (`--download-bytes-per-second`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download_bytes_per_second: Option<i64>,
    /// Cap read/list ops/sec (`--read-requests-per-second`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_ops_per_second: Option<i64>,
    /// Cap write ops/sec (`--write-requests-per-second`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_ops_per_second: Option<i64>,
}

/// The fully-resolved mover configuration for a single run, after the 3-layer
/// field-wise merge `hardened ⊂ repo.moverDefaults ⊂ recipe.mover` (ADR-0004 §1/§2).
/// `security_context` is ALWAYS present (the hardened base guarantees it); the rest are
/// `Some` only when some layer set them. The privileged-mover gate (§4.11/§G16) runs on
/// `security_context`/`pod_security_context` *here* — the merged result — not on the raw
/// recipe, so an elevation introduced by `moverDefaults` is still gated.
pub struct ResolvedMover {
    /// Merged container security context — always present (hardened base).
    pub security_context: SecurityContext,
    /// Merged pod security context, if any layer set one.
    pub pod_security_context: Option<PodSecurityContext>,
    /// Merged resource requirements, if any layer set them.
    pub resources: Option<ResourceRequirements>,
    /// Merged cache config, if any layer set it.
    pub cache: Option<CacheDefaults>,
    /// Pod node selector from `moverDefaults` (no per-recipe override surface today).
    pub node_selector: Option<BTreeMap<String, String>>,
    /// Pod tolerations from `moverDefaults`.
    pub tolerations: Option<Vec<Toleration>>,
    /// Pod affinity from `moverDefaults`.
    pub affinity: Option<Affinity>,
    /// Resolved RWO source/destination co-location mode (`moverDefaults.sourceColocation.mode`),
    /// defaulting to [`SourceColocationMode::Auto`]. Always `Some` so the reconciler
    /// has a concrete strategy. RWO multi-attach fix.
    pub source_colocation: SourceColocationMode,
    /// Resolved Job TTL (recipe `mover.ttlSecondsAfterFinished` wins over
    /// `moverDefaults.ttlSecondsAfterFinished`, falling back to
    /// [`DEFAULT_JOB_TTL_SECONDS`]). Always `Some` so finished Jobs self-GC. §12.
    pub ttl_seconds_after_finished: Option<i64>,
    /// Resolved repository throttle (`moverDefaults.throttle`), if any. §13(e).
    pub throttle: Option<Throttle>,
}

/// Resolve the effective mover configuration via the 3-layer field-wise merge
/// `hardened ⊂ moverDefaults ⊂ recipe` (ADR-0004 §1/§2).
///
/// - `defaults`: the repository's `moverDefaults` (None when the repo sets none).
/// - `recipe_sc`/`recipe_psc`: the recipe's **effective** container/pod context — the
///   explicit `mover.securityContext`/`podSecurityContext`, OR the context the controller
///   resolved from `inheritSecurityContextFrom`. Inheritance is mutually exclusive with
///   explicit (webhook-enforced), so at most one is `Some`. Inherited context enters here
///   as the *recipe layer*, NOT a whole-chain replacement — so the hardened base +
///   `moverDefaults` still supply `drop:[ALL]`/seccomp and an inherited partial context
///   can only tighten.
/// - `recipe_resources`/`recipe_cache`: from `mover.resources` / `mover.cache`.
///
/// `node_selector`/`tolerations`/`affinity`/`ttl` flow from `moverDefaults` (no per-recipe
/// surface for the first three today; TTL is overridable by the caller post-resolve).
pub fn resolve_mover(
    defaults: Option<&MoverDefaults>,
    recipe_sc: Option<&SecurityContext>,
    recipe_psc: Option<&PodSecurityContext>,
    recipe_resources: Option<&ResourceRequirements>,
    recipe_cache: Option<&CacheDefaults>,
    recipe_ttl_seconds_after_finished: Option<i64>,
) -> ResolvedMover {
    let hardened = hardened_security_context();
    // hardened ⊂ moverDefaults.securityContext
    let sc_base = match defaults.and_then(|d| d.security_context.as_ref()) {
        Some(d_sc) => merge_security_context(&hardened, d_sc),
        None => hardened,
    };
    // (hardened ⊂ moverDefaults) ⊂ recipe.securityContext
    let security_context = match recipe_sc {
        Some(r) => merge_security_context(&sc_base, r),
        None => sc_base,
    };
    // Pod context resolves identically to the container one (ADR-0004 §2): a hardened
    // base (notably the `fsGroup` that makes the cache writable) overlaid by
    // moverDefaults then the recipe. Always `Some` so every mover pod — bootstrap,
    // backup, restore, maintenance, verification, replication — carries the same
    // hardened fsGroup unless explicitly overridden.
    let hardened_psc = hardened_pod_security_context();
    // hardened ⊂ moverDefaults.podSecurityContext
    let psc_base = match defaults.and_then(|d| d.pod_security_context.as_ref()) {
        Some(d_psc) => merge_pod_security_context(&hardened_psc, d_psc),
        None => hardened_psc,
    };
    // (hardened ⊂ moverDefaults) ⊂ recipe.podSecurityContext
    let pod_security_context = Some(match recipe_psc {
        Some(r) => merge_pod_security_context(&psc_base, r),
        None => psc_base,
    });
    ResolvedMover {
        security_context,
        pod_security_context,
        resources: merge_resources_opt(
            defaults.and_then(|d| d.resources.as_ref()),
            recipe_resources,
        ),
        cache: CacheDefaults::merge(defaults.and_then(|d| d.cache.as_ref()), recipe_cache),
        node_selector: defaults.and_then(|d| d.node_selector.clone()),
        tolerations: defaults.and_then(|d| d.tolerations.clone()),
        affinity: defaults.and_then(|d| d.affinity.clone()),
        // `moverDefaults.sourceColocation.mode`, defaulting to `Auto` so RWO movers
        // co-locate with their source PVC's node out of the box (RWO multi-attach fix).
        source_colocation: defaults
            .and_then(|d| d.source_colocation.as_ref())
            .and_then(|c| c.mode)
            .unwrap_or_default(),
        // Recipe TTL wins over the repo default; a built-in default applies when
        // neither sets one so every finished Job self-GCs (ADR-0005 §12).
        ttl_seconds_after_finished: Some(
            recipe_ttl_seconds_after_finished
                .or_else(|| defaults.and_then(|d| d.ttl_seconds_after_finished))
                .unwrap_or(DEFAULT_JOB_TTL_SECONDS),
        ),
        throttle: defaults.and_then(|d| d.throttle.clone()),
    }
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

/// Reference to a `SnapshotPolicy` CR (used by `Snapshot.spec.policyRef` and
/// `SnapshotSchedule.spec.policyRef`). May cross namespaces, subject to RBAC. ADR §3.4/§3.5.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PolicyRef {
    /// Name of the referenced `SnapshotPolicy`.
    pub name: String,
    /// Namespace of the `SnapshotPolicy`; absent = same namespace as the referrer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Generic name/namespace reference to another namespaced object — e.g. a `Snapshot`
/// CR (`Restore.spec.source.snapshotRef`) or a PVC (`Restore.spec.target.pvcRef`). ADR §3.6.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObjectRef {
    /// Name of the referenced object.
    pub name: String,
    /// Namespace of the referenced object; absent = same namespace as the referrer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Lifecycle of the underlying kopia snapshot when its `Snapshot` CR is deleted.
/// Shared by `SnapshotPolicy.spec.defaultDeletionPolicy` and `Snapshot.spec.deletionPolicy`.
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
    /// Forced via webhook for discovered snapshots; cannot be overridden.
    Retain,
    /// CR is removed without contacting the repository at all (escape hatch
    /// for "the bucket is gone, just let me delete the CR"). Status records
    /// `orphaned: true` for the snapshot ID before removal.
    Orphan,
}

/// What happens to a repository's snapshots when a consuming **namespace** is
/// deleted. Closed enum, default `Orphan` (fail-safe). ADR-0005 §5.
///
/// A `kubectl delete ns` must not silently destroy off-site backup history (and
/// must not hang the namespace teardown on N `kopia snapshot delete` calls). So the
/// repository owner opts *in* to cascade-delete; the default releases ownership
/// (removes the finalizer) without touching the snapshots. This is distinct from a
/// single `kubectl delete snapshot`, which still honors that `Snapshot`'s own
/// `deletionPolicy`.
///
/// ```
/// use kopiur_api::common::NamespaceDeletePolicy;
///
/// // Fail-safe: a deleted namespace orphans (keeps) snapshots by default.
/// assert_eq!(NamespaceDeletePolicy::default(), NamespaceDeletePolicy::Orphan);
/// // Bare PascalCase strings (plain unit enum).
/// assert_eq!(serde_json::to_value(NamespaceDeletePolicy::Delete).unwrap(), "Delete");
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum NamespaceDeletePolicy {
    /// Release ownership (remove the `Snapshot` finalizers) without deleting the
    /// underlying kopia snapshots. The fail-safe default — `kubectl delete ns` keeps
    /// history.
    #[default]
    Orphan,
    /// Cascade: when a namespace is deleted, the per-`Snapshot` `deletionPolicy`
    /// applies (so produced snapshots are `kopia snapshot delete`d). Opt-in only.
    Delete,
}

/// Repository access mode (ADR-0005 §11). A `ReadOnly` repository serves restores
/// only — no backups, no maintenance — for decommissioning a backend or migrating
/// between repositories without risking writes. Maps to kopia's read-only
/// connection. Closed enum, default `ReadWrite`.
///
/// ```
/// use kopiur_api::common::RepositoryMode;
///
/// assert_eq!(RepositoryMode::default(), RepositoryMode::ReadWrite);
/// assert_eq!(serde_json::to_value(RepositoryMode::ReadOnly).unwrap(), "ReadOnly");
/// // ReadOnly forbids writes (backups + maintenance); restores are allowed.
/// assert!(!RepositoryMode::ReadOnly.allows_writes());
/// assert!(RepositoryMode::ReadWrite.allows_writes());
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum RepositoryMode {
    /// Normal read-write repository (default): backups, restores, maintenance.
    #[default]
    ReadWrite,
    /// Read-only: restores only. Backup Jobs and maintenance are refused. §11.
    ReadOnly,
}

impl RepositoryMode {
    /// Whether this mode permits write operations (backup Jobs + maintenance).
    /// Pure + exhaustive so the single definition lives in one tested place.
    pub fn allows_writes(&self) -> bool {
        match self {
            RepositoryMode::ReadWrite => true,
            RepositoryMode::ReadOnly => false,
        }
    }
}

/// serde/schemars `default` for the repository `mode` field — `ReadWrite`
/// (ADR-0005 §11). Named fn so it backs BOTH serde + schemars defaults.
pub(crate) fn default_repository_mode() -> RepositoryMode {
    RepositoryMode::ReadWrite
}

/// serde/schemars `default` for the repository `on_namespace_delete` field —
/// `Orphan` (ADR-0005 §5). A named fn so it backs BOTH `#[serde(default = ...)]`
/// and `#[schemars(default = ...)]`, emitting a real OpenAPI `default:`.
pub(crate) fn default_namespace_delete_policy() -> NamespaceDeletePolicy {
    NamespaceDeletePolicy::Orphan
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

    #[test]
    fn requires_privilege_resolved_covers_the_gate_inputs() {
        use k8s_openapi::api::core::v1::PodSecurityContext;
        let root = SecurityContext {
            run_as_user: Some(0),
            ..Default::default()
        };
        let benign = SecurityContext {
            run_as_user: Some(1000),
            run_as_non_root: Some(true),
            ..Default::default()
        };
        let fsgroup = PodSecurityContext {
            fs_group: Some(1000),
            ..Default::default()
        };
        let pod_root = PodSecurityContext {
            run_as_user: Some(0),
            ..Default::default()
        };

        // Nothing set → not privileged.
        assert!(!requires_privilege_resolved(None, None, None));
        // Benign container + fsGroup pod context → still not privileged.
        assert!(!requires_privilege_resolved(
            Some(&benign),
            Some(&fsgroup),
            None
        ));
        // An (e.g. inherited) root CONTAINER context → privileged.
        assert!(requires_privilege_resolved(Some(&root), None, None));
        // A root POD context with a benign container → privileged (can't slip past).
        assert!(requires_privilege_resolved(
            Some(&benign),
            Some(&pod_root),
            None
        ));
        // privilegedMode alone → privileged.
        assert!(requires_privilege_resolved(None, None, Some(true)));
        // The pure helpers agree.
        assert!(security_context_is_elevated(&root));
        assert!(!security_context_is_elevated(&benign));
        assert!(pod_security_context_is_elevated(&pod_root));
        assert!(!pod_security_context_is_elevated(&fsgroup));
    }

    // --- moverDefaults field-wise merge (ADR-0004 §1/§2) ---

    #[test]
    fn resolve_mover_with_no_layers_is_the_hardened_default() {
        let m = resolve_mover(None, None, None, None, None, None);
        let sc = m.security_context;
        assert_eq!(sc.run_as_non_root, Some(true));
        assert_eq!(sc.allow_privilege_escalation, Some(false));
        assert_eq!(sc.capabilities.unwrap().drop.unwrap(), vec!["ALL"]);
        assert_eq!(sc.seccomp_profile.unwrap().type_, "RuntimeDefault");
        // The pod context is now hardened too (not None): fsGroup matches the mover
        // image's nonroot gid so the cache is writable on PVC-backed storage, with
        // OnRootMismatch so an already-correct volume isn't re-chowned every run.
        let psc = m
            .pod_security_context
            .expect("hardened pod context is always present");
        assert_eq!(psc.fs_group, Some(MOVER_NONROOT_ID));
        assert_eq!(
            psc.fs_group_change_policy.as_deref(),
            Some("OnRootMismatch")
        );
        assert!(m.resources.is_none());
        assert!(m.cache.is_none());
    }

    #[test]
    fn recipe_or_defaults_can_override_the_hardened_fsgroup() {
        // The hardened fsGroup is a floor, not a ceiling: a moverDefaults fsGroup wins
        // over it, and a recipe fsGroup wins over moverDefaults — while unset pod
        // fields (here fsGroupChangePolicy) keep the hardened default. This is what
        // lets a restore own files as the app's UID/GID.
        let defaults = MoverDefaults {
            pod_security_context: Some(PodSecurityContext {
                fs_group: Some(1000),
                ..Default::default()
            }),
            ..Default::default()
        };
        let recipe_psc = PodSecurityContext {
            fs_group: Some(3000),
            run_as_user: Some(3000),
            ..Default::default()
        };

        // moverDefaults overrides the hardened fsGroup; change policy still inherited.
        let only_defaults = resolve_mover(Some(&defaults), None, None, None, None, None);
        let psc = only_defaults.pod_security_context.unwrap();
        assert_eq!(psc.fs_group, Some(1000));
        assert_eq!(
            psc.fs_group_change_policy.as_deref(),
            Some("OnRootMismatch")
        );

        // recipe wins over moverDefaults, which wins over hardened.
        let m = resolve_mover(Some(&defaults), None, Some(&recipe_psc), None, None, None);
        let psc = m.pod_security_context.unwrap();
        assert_eq!(psc.fs_group, Some(3000), "recipe fsGroup must win");
        assert_eq!(psc.run_as_user, Some(3000));
        assert_eq!(
            psc.fs_group_change_policy.as_deref(),
            Some("OnRootMismatch")
        );
    }

    #[test]
    fn recipe_partial_override_only_tightens_keeping_hardening() {
        // The de-hardening bug ADR-0004 §2 cites: a recipe that sets only runAsUser
        // must NOT wipe the hardened drop:[ALL]/seccomp/escalation defaults.
        let recipe = SecurityContext {
            run_as_user: Some(1000),
            ..Default::default()
        };
        let m = resolve_mover(None, Some(&recipe), None, None, None, None);
        let sc = m.security_context;
        assert_eq!(sc.run_as_user, Some(1000)); // recipe wins
        assert_eq!(sc.run_as_non_root, Some(true)); // hardened preserved
        assert_eq!(sc.allow_privilege_escalation, Some(false)); // hardened preserved
        assert_eq!(sc.capabilities.unwrap().drop.unwrap(), vec!["ALL"]); // never lost
        assert_eq!(sc.seccomp_profile.unwrap().type_, "RuntimeDefault");
    }

    #[test]
    fn three_layer_precedence_hardened_then_defaults_then_recipe() {
        let defaults = MoverDefaults {
            security_context: Some(SecurityContext {
                run_as_group: Some(568),
                run_as_user: Some(568),
                ..Default::default()
            }),
            ..Default::default()
        };
        let recipe = SecurityContext {
            run_as_user: Some(1000), // recipe overrides the moverDefaults runAsUser
            ..Default::default()
        };
        let m = resolve_mover(Some(&defaults), Some(&recipe), None, None, None, None);
        let sc = m.security_context;
        assert_eq!(sc.run_as_user, Some(1000)); // recipe wins over defaults
        assert_eq!(sc.run_as_group, Some(568)); // from moverDefaults
        assert_eq!(sc.run_as_non_root, Some(true)); // from hardened base
        assert_eq!(sc.capabilities.unwrap().drop.unwrap(), vec!["ALL"]);
    }

    #[test]
    fn add_only_capabilities_override_keeps_hardened_drop_all() {
        // Deep-merge: a recipe adding NET_BIND_SERVICE (with no `drop`) must keep the
        // hardened drop:[ALL] (the precise bug ADR-0004 §2 calls out).
        let recipe = SecurityContext {
            capabilities: Some(Capabilities {
                add: Some(vec!["NET_BIND_SERVICE".into()]),
                drop: None,
            }),
            ..Default::default()
        };
        let m = resolve_mover(None, Some(&recipe), None, None, None, None);
        let caps = m.security_context.capabilities.unwrap();
        assert_eq!(caps.add.unwrap(), vec!["NET_BIND_SERVICE"]);
        assert_eq!(caps.drop.unwrap(), vec!["ALL"]); // hardened drop survives
    }

    #[test]
    fn pod_security_context_merges_fsgroup_from_defaults_with_recipe() {
        let defaults = MoverDefaults {
            pod_security_context: Some(PodSecurityContext {
                fs_group: Some(568),
                fs_group_change_policy: Some("OnRootMismatch".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let recipe_psc = PodSecurityContext {
            run_as_user: Some(1000),
            ..Default::default()
        };
        let m = resolve_mover(Some(&defaults), None, Some(&recipe_psc), None, None, None);
        let psc = m.pod_security_context.unwrap();
        assert_eq!(psc.fs_group, Some(568)); // from defaults
        assert_eq!(
            psc.fs_group_change_policy.as_deref(),
            Some("OnRootMismatch")
        );
        assert_eq!(psc.run_as_user, Some(1000)); // from recipe
    }

    #[test]
    fn resources_merge_per_key_with_recipe_winning() {
        use std::collections::BTreeMap;
        let defaults = MoverDefaults {
            resources: Some(ResourceRequirements {
                requests: Some(BTreeMap::from([
                    ("cpu".to_string(), Quantity("100m".into())),
                    ("memory".to_string(), Quantity("128Mi".into())),
                ])),
                ..Default::default()
            }),
            ..Default::default()
        };
        let recipe_res = ResourceRequirements {
            requests: Some(BTreeMap::from([(
                "cpu".to_string(),
                Quantity("500m".into()),
            )])),
            ..Default::default()
        };
        let m = resolve_mover(Some(&defaults), None, None, Some(&recipe_res), None, None);
        let req = m.resources.unwrap().requests.unwrap();
        assert_eq!(req["cpu"].0, "500m"); // recipe wins
        assert_eq!(req["memory"].0, "128Mi"); // defaults fills
    }

    #[test]
    fn privileged_gate_fires_on_merged_root_from_defaults_but_not_benign() {
        // moverDefaults setting runAsUser:0 produces a privileged merged context even
        // with no recipe override — the gate must see the merged result.
        let root_defaults = MoverDefaults {
            security_context: Some(SecurityContext {
                run_as_user: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };
        let m = resolve_mover(Some(&root_defaults), None, None, None, None, None);
        assert!(requires_privilege_resolved(
            Some(&m.security_context),
            m.pod_security_context.as_ref(),
            None
        ));

        // A benign merge (hardened base only) must NOT trip the gate.
        let benign = resolve_mover(None, None, None, None, None, None);
        assert!(!requires_privilege_resolved(
            Some(&benign.security_context),
            benign.pod_security_context.as_ref(),
            None
        ));
    }

    #[test]
    fn mover_defaults_flows_cache_node_selector_and_ttl() {
        let defaults = MoverDefaults {
            cache: Some(CacheDefaults {
                capacity: Some("10Gi".into()),
                ..Default::default()
            }),
            node_selector: Some(std::collections::BTreeMap::from([(
                "disktype".to_string(),
                "ssd".to_string(),
            )])),
            ttl_seconds_after_finished: Some(3600),
            ..Default::default()
        };
        let m = resolve_mover(Some(&defaults), None, None, None, None, None);
        assert_eq!(m.cache.unwrap().capacity.as_deref(), Some("10Gi"));
        assert_eq!(m.node_selector.unwrap()["disktype"], "ssd");
        assert_eq!(m.ttl_seconds_after_finished, Some(3600));
    }

    // --- RWO multi-attach: sourceColocation flows from moverDefaults, defaults to Auto ---

    #[test]
    fn source_colocation_defaults_to_auto_when_unset() {
        // No moverDefaults at all → Auto (the bug-fixing default).
        let none = resolve_mover(None, None, None, None, None, None);
        assert_eq!(none.source_colocation, SourceColocationMode::Auto);
        // moverDefaults present but sourceColocation unset → still Auto.
        let defaults = MoverDefaults {
            node_selector: Some(std::collections::BTreeMap::from([(
                "disktype".to_string(),
                "ssd".to_string(),
            )])),
            ..Default::default()
        };
        let m = resolve_mover(Some(&defaults), None, None, None, None, None);
        assert_eq!(m.source_colocation, SourceColocationMode::Auto);
    }

    #[test]
    fn source_colocation_mode_flows_from_defaults() {
        let defaults = MoverDefaults {
            source_colocation: Some(SourceColocation {
                mode: Some(SourceColocationMode::Disabled),
            }),
            ..Default::default()
        };
        let m = resolve_mover(Some(&defaults), None, None, None, None, None);
        assert_eq!(m.source_colocation, SourceColocationMode::Disabled);
    }

    #[test]
    fn source_colocation_parses_the_cluster_way() {
        // YAML → serde_json::Value → typed (the cluster's path), never serde_yaml direct.
        let defaults: MoverDefaults = crate::testutil::from_yaml(
            r#"
            sourceColocation:
              mode: Required
            "#,
        );
        assert_eq!(
            defaults.source_colocation,
            Some(SourceColocation {
                mode: Some(SourceColocationMode::Required),
            })
        );
        // An empty sub-object resolves to Auto (mode unset).
        let bare: MoverDefaults = crate::testutil::from_yaml(
            r#"
            sourceColocation: {}
            "#,
        );
        assert_eq!(
            resolve_mover(Some(&bare), None, None, None, None, None).source_colocation,
            SourceColocationMode::Auto,
        );
    }

    // --- §12 mover Job TTL precedence (recipe over default over built-in) ---

    #[test]
    fn ttl_precedence_recipe_over_default_over_builtin() {
        // Built-in default when neither sets one (so finished Jobs always self-GC).
        let none = resolve_mover(None, None, None, None, None, None);
        assert_eq!(
            none.ttl_seconds_after_finished,
            Some(DEFAULT_JOB_TTL_SECONDS)
        );

        // moverDefaults sets it → used when the recipe doesn't override.
        let defaults = MoverDefaults {
            ttl_seconds_after_finished: Some(7200),
            ..Default::default()
        };
        let from_default = resolve_mover(Some(&defaults), None, None, None, None, None);
        assert_eq!(from_default.ttl_seconds_after_finished, Some(7200));

        // Recipe override wins over the repo default.
        let from_recipe = resolve_mover(Some(&defaults), None, None, None, None, Some(900));
        assert_eq!(from_recipe.ttl_seconds_after_finished, Some(900));

        // Recipe override alone (no repo default) also wins over the built-in.
        let recipe_only = resolve_mover(None, None, None, None, None, Some(120));
        assert_eq!(recipe_only.ttl_seconds_after_finished, Some(120));
    }

    // --- §13(e) throttle flows from moverDefaults into ResolvedMover ---

    #[test]
    fn resolve_mover_carries_throttle_from_defaults() {
        let defaults = MoverDefaults {
            throttle: Some(Throttle {
                upload_bytes_per_second: Some(10_000_000),
                download_bytes_per_second: None,
                read_ops_per_second: Some(50),
                write_ops_per_second: None,
            }),
            ..Default::default()
        };
        let m = resolve_mover(Some(&defaults), None, None, None, None, None);
        let t = m.throttle.expect("throttle");
        assert_eq!(t.upload_bytes_per_second, Some(10_000_000));
        assert_eq!(t.read_ops_per_second, Some(50));
        // Absent on a repo with no throttle.
        assert!(
            resolve_mover(None, None, None, None, None, None)
                .throttle
                .is_none()
        );
    }
}
