//! The single place for the controller's runtime configuration: the names of
//! every environment variable it reads, plus fixed config values (bind
//! addresses). Domain string constants (labels/finalizers/annotations) live in
//! [`crate::consts`]; OTLP env var names are owned by [`kopiur_telemetry::env`]
//! and re-exported here so callers have one import.

/// Container image the controller stamps into every mover `Job`. Overrides
/// [`crate::jobs::DEFAULT_MOVER_IMAGE`] when set.
pub const MOVER_IMAGE_ENV: &str = "KOPIUR_MOVER_IMAGE";

/// ServiceAccount the mover `Job` pods run as. A dedicated least-privilege SA
/// (NOT the operator SA): the controller mints it — plus a `RoleBinding` to the
/// mover role named by [`MOVER_CLUSTERROLE_ENV`] — in each mover Job's namespace,
/// because a mover Job runs in the workload namespace where the operator SA does
/// not exist (ADR §4.12).
pub const MOVER_SERVICE_ACCOUNT_ENV: &str = "KOPIUR_MOVER_SERVICE_ACCOUNT";

/// Name of the mover `ClusterRole` (cluster install) / `Role` (namespaced install)
/// shipped by the chart, that the controller-minted per-namespace mover
/// `RoleBinding` references. Defaults to [`DEFAULT_MOVER_NAME`].
pub const MOVER_CLUSTERROLE_ENV: &str = "KOPIUR_MOVER_CLUSTERROLE";

/// `roleRef.kind` for the minted mover `RoleBinding`: `ClusterRole` for a
/// cluster-scoped install (one shared mover ClusterRole, bound per namespace) or
/// `Role` for a namespaced install (a mover Role in the operator's namespace). The
/// chart sets this from `installScope`; defaults to [`DEFAULT_MOVER_ROLE_KIND`].
pub const MOVER_ROLE_KIND_ENV: &str = "KOPIUR_MOVER_ROLE_KIND";

/// Fallback name for the mover ServiceAccount and mover Role/ClusterRole when the
/// respective env var is unset (off-chart / test runs). Matches the chart's
/// `kopiur.moverName` helper for the default release name.
pub const DEFAULT_MOVER_NAME: &str = "kopiur-mover";

/// Default `roleRef.kind` for the mover `RoleBinding` (cluster-scoped install).
pub const DEFAULT_MOVER_ROLE_KIND: &str = "ClusterRole";

/// The operator's own namespace, injected by the chart via the downward API
/// (`fieldRef: metadata.namespace`). Used as the default placement namespace for
/// a `ClusterRepository`'s managed (namespaced) `Maintenance` CR when
/// `spec.maintenance.namespace` is unset. Absent → that placement is unresolved
/// and surfaced as an actionable condition rather than guessed.
pub const OPERATOR_NAMESPACE_ENV: &str = "KOPIUR_NAMESPACE";

/// Override for the writable base directory the controller's in-process kopia
/// uses for its cache/logs/config. Defaults to
/// [`kopiur_kopia::env::DEFAULT_CACHE_DIR`] (`/var/cache/kopia`), where the chart
/// mounts an `emptyDir`; set this only when relocating that mount.
pub const KOPIA_CACHE_DIR_ENV: &str = "KOPIUR_KOPIA_CACHE_DIR";

/// Address the controller's HTTP server (`/metrics`, `/healthz`, `/readyz`)
/// binds to. Matches the chart's `controller.probePort` (8080).
pub const HTTP_ADDR: &str = "0.0.0.0:8080";

/// The OTLP + logging env vars the controller passes through to mover `Job`s,
/// owned by the telemetry crate so the name lists have a single definition.
/// OTLP vars are only forwarded when a collector endpoint is set; the logging
/// vars (`RUST_LOG`, `KOPIUR_LOG_FORMAT`) are forwarded whenever present so a
/// mover inherits the controller's log level and format regardless of OTLP.
pub use kopiur_telemetry::env::{LOG_PASSTHROUGH, OTEL_EXPORTER_OTLP_ENDPOINT, OTLP_PASSTHROUGH};
