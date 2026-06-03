//! The single place that names every environment variable this crate reads.
//!
//! Other crates that need to *pass these through* to child processes (e.g. the
//! controller stamping OTLP config onto mover `Job`s) reference [`OTLP_PASSTHROUGH`]
//! rather than hard-coding the names again.

/// Collector endpoint. When unset, OTLP export is disabled entirely.
pub const OTEL_EXPORTER_OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
/// OTLP transport. Only `grpc` is compiled in; other values are rejected.
pub const OTEL_EXPORTER_OTLP_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";
/// Extra OTLP headers (honored by `opentelemetry-otlp` directly).
pub const OTEL_EXPORTER_OTLP_HEADERS: &str = "OTEL_EXPORTER_OTLP_HEADERS";
/// `true`/`1` makes telemetry misconfiguration fail-fast instead of degrading.
pub const KOPIUR_OTEL_STRICT: &str = "KOPIUR_OTEL_STRICT";

/// The OTLP env vars a parent process should pass through to children so their
/// telemetry reaches the same collector. Ordered with the endpoint first.
pub const OTLP_PASSTHROUGH: &[&str] = &[
    OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_EXPORTER_OTLP_HEADERS,
];
