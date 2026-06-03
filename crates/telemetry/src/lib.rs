//! # kopiur-telemetry
//!
//! Shared observability for the controller, webhook, and mover. One idea:
//! **instrument once against the OpenTelemetry metrics API, fan out to two
//! readers** —
//!
//! 1. an [`opentelemetry-prometheus`] exporter that populates a
//!    [`prometheus::Registry`] for the always-on `/metrics` pull endpoint
//!    (so the existing `ServiceMonitor` keeps working, no topology change), and
//! 2. an OTLP [`PeriodicReader`] that *pushes* the same measurements to a
//!    collector — added only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
//!
//! Traces (the controller's existing `#[instrument]` spans) and logs (bridged
//! from `tracing` events) export over OTLP via the maintained crates, also
//! env-gated. With no OTLP endpoint configured the behavior is identical to the
//! previous fmt-only setup, so `cargo test` stays hermetic and offline.
//!
//! OTLP setup is **non-critical**: a misconfiguration is logged with an
//! actionable [`TelemetryError`] and the operator degrades to fmt-logging +
//! the Prometheus pull, unless `KOPIUR_OTEL_STRICT=true`.

pub mod env;
mod error;

pub use error::{Signal, TelemetryError};

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use prometheus::{Encoder, Registry, TextEncoder};

/// Resolved OTLP configuration. Absent when `OTEL_EXPORTER_OTLP_ENDPOINT` is
/// unset — the operator then runs fully offline (fmt logs + Prometheus pull).
#[derive(Debug, Clone)]
pub struct OtlpConfig {
    /// The collector endpoint (validated as a URL).
    pub endpoint: String,
}

impl OtlpConfig {
    /// Read + validate OTLP config from the standard `OTEL_*` env.
    ///
    /// - `Ok(None)` — `OTEL_EXPORTER_OTLP_ENDPOINT` unset: OTLP disabled.
    /// - `Ok(Some)` — endpoint present and valid.
    /// - `Err` — endpoint set but malformed, or an unsupported protocol
    ///   requested. Callers degrade (log + continue) unless strict.
    pub fn from_env() -> Result<Option<Self>, TelemetryError> {
        let endpoint = match std::env::var(env::OTEL_EXPORTER_OTLP_ENDPOINT) {
            Ok(v) if !v.trim().is_empty() => v,
            _ => return Ok(None),
        };

        // Only gRPC is compiled in; reject other protocols with a clear fix.
        if let Ok(proto) = std::env::var(env::OTEL_EXPORTER_OTLP_PROTOCOL) {
            let proto = proto.trim();
            if !proto.is_empty() && !proto.eq_ignore_ascii_case("grpc") {
                return Err(TelemetryError::UnsupportedProtocol {
                    value: proto.to_string(),
                });
            }
        }

        url::Url::parse(&endpoint).map_err(|source| TelemetryError::InvalidOtlpEndpoint {
            value: endpoint.clone(),
            source,
        })?;

        Ok(Some(OtlpConfig { endpoint }))
    }

    /// Resolve OTLP config, honoring strict mode. On a non-strict config error
    /// this logs the actionable message and returns `Ok(None)` (degrade);
    /// strict mode propagates the error so the binary can fail-fast.
    fn resolve() -> Result<Option<Self>, TelemetryError> {
        match Self::from_env() {
            Ok(opt) => Ok(opt),
            Err(e) if TelemetryError::strict_mode() => Err(e),
            Err(e) => {
                tracing::error!(error = %e, "OTLP disabled (misconfiguration); continuing with local telemetry");
                Ok(None)
            }
        }
    }
}

fn resource(service_name: &str) -> Resource {
    Resource::builder()
        .with_service_name(service_name.to_string())
        .build()
}

/// Owns the metrics meter provider and the Prometheus registry it feeds.
///
/// Each binary (and each test) builds its own — there is no global meter
/// provider, so parallel tests stay isolated. Instruments are created from
/// [`MetricsProvider::meter`]; the `/metrics` endpoint serves
/// [`MetricsProvider::gather`].
pub struct MetricsProvider {
    provider: SdkMeterProvider,
    registry: Registry,
    meter: opentelemetry::metrics::Meter,
}

impl MetricsProvider {
    /// Build a meter provider with the Prometheus exporter (always) plus an OTLP
    /// `PeriodicReader` when an endpoint is configured. `service_name` labels the
    /// OTLP resource.
    ///
    /// **Infallible** — metrics are a non-critical subsystem, so a build failure
    /// degrades to an empty no-op provider (logged) rather than crashing the
    /// process. Strict-mode OTLP misconfiguration is caught earlier by
    /// [`init_tracing`], which fails fast before this is reached.
    pub fn new(service_name: &str) -> Self {
        Self::try_new(service_name).unwrap_or_else(|e| {
            tracing::error!(error = %e, "metrics provider degraded to no-op; /metrics will be empty until fixed");
            Self::degraded(service_name)
        })
    }

    fn try_new(service_name: &str) -> Result<Self, TelemetryError> {
        Self::build(service_name, OtlpConfig::resolve()?)
    }

    /// An empty provider (no readers) used when the real build fails, so callers
    /// keep a valid `MetricsProvider` whose `gather()` is empty.
    fn degraded(service_name: &str) -> Self {
        let registry = Registry::new();
        let provider = SdkMeterProvider::builder()
            .with_resource(resource(service_name))
            .build();
        let meter = provider.meter("kopiur");
        MetricsProvider {
            provider,
            registry,
            meter,
        }
    }

    fn build(service_name: &str, otlp: Option<OtlpConfig>) -> Result<Self, TelemetryError> {
        let registry = Registry::new();
        let prom_exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()
            .map_err(|e| TelemetryError::PrometheusRegister {
                source: Box::new(e),
            })?;

        let mut builder = SdkMeterProvider::builder()
            .with_reader(prom_exporter)
            .with_resource(resource(service_name));

        if let Some(cfg) = otlp.as_ref() {
            match build_metric_exporter(cfg) {
                Ok(exporter) => {
                    let reader = PeriodicReader::builder(exporter).build();
                    builder = builder.with_reader(reader);
                }
                Err(e) if TelemetryError::strict_mode() => return Err(e),
                Err(e) => {
                    tracing::error!(error = %e, "OTLP metrics export disabled; Prometheus pull still active")
                }
            }
        }

        let provider = builder.build();
        let meter = provider.meter("kopiur");
        Ok(MetricsProvider {
            provider,
            registry,
            meter,
        })
    }

    /// The meter for creating instruments (counters/histograms/gauges).
    pub fn meter(&self) -> &opentelemetry::metrics::Meter {
        &self.meter
    }

    /// Render the Prometheus text exposition for `/metrics`.
    pub fn gather(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        // Encoding only fails on a broken writer; a Vec never errors.
        let _ = encoder.encode(&families, &mut buf);
        buf
    }

    /// Flush + shut down the meter provider. Call before a short-lived process
    /// (the mover) exits so the final OTLP push lands.
    pub fn shutdown(&self) {
        let _ = self.provider.shutdown();
    }
}

fn build_metric_exporter(
    cfg: &OtlpConfig,
) -> Result<opentelemetry_otlp::MetricExporter, TelemetryError> {
    opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(cfg.endpoint.clone())
        .build()
        .map_err(|e| TelemetryError::ExporterBuild {
            signal: Signal::Metrics,
            source: Box::new(e),
        })
}

/// Holds the OTLP trace/log providers for the process lifetime; flushes them on
/// drop so buffered spans/logs are exported before exit.
#[must_use = "drop the guard at process end so traces/logs flush"]
pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    logger_provider: Option<SdkLoggerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(tp) = self.tracer_provider.take() {
            let _ = tp.shutdown();
        }
        if let Some(lp) = self.logger_provider.take() {
            let _ = lp.shutdown();
        }
    }
}

/// Install the tracing subscriber for `service_name`: an fmt layer (preserving
/// the previous console behavior) plus OTLP trace + log layers when an endpoint
/// is configured. Returns a guard that flushes the OTLP providers on drop.
///
/// Idempotent-ish: uses `try_init`, so a second call (e.g. controller + webhook
/// in one test binary) is a no-op for the subscriber.
pub fn init_tracing(service_name: &str) -> Result<TelemetryGuard, TelemetryError> {
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{EnvFilter, fmt};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Degrade messages are DEFERRED until after the subscriber is installed:
    // logging them here (before `try_init`) would drop them, leaving an operator
    // with no hint that OTLP was silently disabled. Strict mode still fails fast
    // (returns Err before any global subscriber is installed).
    let mut deferred: Vec<String> = Vec::new();

    let otlp = match OtlpConfig::from_env() {
        Ok(o) => o,
        Err(e) if TelemetryError::strict_mode() => return Err(e),
        Err(e) => {
            deferred.push(format!(
                "OTLP disabled (misconfiguration); continuing with local telemetry: {e}"
            ));
            None
        }
    };

    let mut tracer_provider = None;
    let mut logger_provider = None;
    let mut otel_layers: Vec<Box<dyn tracing_subscriber::Layer<_> + Send + Sync>> = Vec::new();

    if let Some(cfg) = otlp.as_ref() {
        match build_trace_provider(service_name, cfg) {
            Ok(tp) => {
                let tracer = tp.tracer("kopiur");
                otel_layers.push(tracing_opentelemetry::layer().with_tracer(tracer).boxed());
                tracer_provider = Some(tp);
            }
            Err(e) if TelemetryError::strict_mode() => return Err(e),
            Err(e) => deferred.push(format!("OTLP trace export disabled: {e}")),
        }
        match build_log_provider(service_name, cfg) {
            Ok(lp) => {
                otel_layers.push(
                    opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&lp)
                        .boxed(),
                );
                logger_provider = Some(lp);
            }
            Err(e) if TelemetryError::strict_mode() => return Err(e),
            Err(e) => deferred.push(format!("OTLP log export disabled: {e}")),
        }
    }

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .with(otel_layers)
        .try_init();

    // Subscriber is now installed — surface any deferred degrade messages.
    for msg in deferred {
        tracing::error!("{msg}");
    }

    Ok(TelemetryGuard {
        tracer_provider,
        logger_provider,
    })
}

fn build_trace_provider(
    service_name: &str,
    cfg: &OtlpConfig,
) -> Result<SdkTracerProvider, TelemetryError> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(cfg.endpoint.clone())
        .build()
        .map_err(|e| TelemetryError::ExporterBuild {
            signal: Signal::Traces,
            source: Box::new(e),
        })?;
    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource(service_name))
        .build())
}

fn build_log_provider(
    service_name: &str,
    cfg: &OtlpConfig,
) -> Result<SdkLoggerProvider, TelemetryError> {
    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(cfg.endpoint.clone())
        .build()
        .map_err(|e| TelemetryError::ExporterBuild {
            signal: Signal::Logs,
            source: Box::new(e),
        })?;
    Ok(SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource(service_name))
        .build())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_provider_builds_offline_and_gathers() {
        // No OTLP endpoint: prometheus-only path, fully offline.
        let mp = MetricsProvider::build("kopiur-test", None).expect("build offline");
        let counter = mp.meter().u64_counter("kopiur_test_total").build();
        counter.add(1, &[opentelemetry::KeyValue::new("kind", "Backup")]);
        let text = String::from_utf8(mp.gather()).expect("utf8 exposition");
        assert!(
            text.contains("kopiur_test_total"),
            "exposition missing counter: {text}"
        );
    }

    #[test]
    fn otlp_disabled_when_endpoint_unset() {
        // Snapshot + clear the env so the test is deterministic regardless of
        // the runner's environment.
        let prev = std::env::var(env::OTEL_EXPORTER_OTLP_ENDPOINT).ok();
        unsafe { std::env::remove_var(env::OTEL_EXPORTER_OTLP_ENDPOINT) };
        assert!(OtlpConfig::from_env().unwrap().is_none());
        if let Some(v) = prev {
            unsafe { std::env::set_var(env::OTEL_EXPORTER_OTLP_ENDPOINT, v) };
        }
    }
}
