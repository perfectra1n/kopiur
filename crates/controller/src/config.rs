//! The single place for the controller's runtime configuration: the names of
//! every environment variable it reads, plus fixed config values (bind
//! addresses). Domain string constants (labels/finalizers/annotations) live in
//! [`crate::consts`]; OTLP env var names are owned by [`kopiur_telemetry::env`]
//! and re-exported here so callers have one import.

/// Container image the controller stamps into every mover `Job`. Overrides
/// [`crate::jobs::DEFAULT_MOVER_IMAGE`] when set.
pub const MOVER_IMAGE_ENV: &str = "KOPIUR_MOVER_IMAGE";

/// ServiceAccount the mover `Job` pods run as. Must be bound to the operator's
/// status-patch RBAC (the mover PATCHes the owning CR `.status`).
pub const MOVER_SERVICE_ACCOUNT_ENV: &str = "KOPIUR_MOVER_SERVICE_ACCOUNT";

/// Address the controller's HTTP server (`/metrics`, `/healthz`, `/readyz`)
/// binds to. Matches the chart's `controller.probePort` (8080).
pub const HTTP_ADDR: &str = "0.0.0.0:8080";

/// The OTLP env vars the controller passes through to mover `Job`s, owned by
/// the telemetry crate so the name list has a single definition.
pub use kopiur_telemetry::env::{OTEL_EXPORTER_OTLP_ENDPOINT, OTLP_PASSTHROUGH};
