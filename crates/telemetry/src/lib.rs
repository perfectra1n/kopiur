#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

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

/// Console log format for the fmt layer. Selected by `KOPIUR_LOG_FORMAT`.
///
/// `Text` is the default and preserves the human-readable console output;
/// `Json` emits one structured JSON object per event for log aggregators
/// (Loki/ELK/Datadog). An unrecognized value degrades to `Text` rather than
/// failing a backup operator's startup (see `LogFormat::from_env`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable console output (the default).
    Text,
    /// One structured JSON object per event, for log aggregators (Loki/ELK/Datadog).
    Json,
}

impl LogFormat {
    /// Resolve the format from `KOPIUR_LOG_FORMAT`. Unset/empty → `Text`. An
    /// unknown value → `Text` plus `Err(unknown_value)` so the caller can defer
    /// a degrade warning until after the subscriber is installed.
    fn from_env() -> (Self, Option<String>) {
        match std::env::var(env::KOPIUR_LOG_FORMAT) {
            Ok(v) => Self::parse(&v),
            Err(_) => (Self::Text, None),
        }
    }

    /// Parse a single value. Returns the resolved format and, when the input was
    /// non-empty but unrecognized, the offending string for a degrade message.
    fn parse(value: &str) -> (Self, Option<String>) {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "text" | "plain" | "fmt" => (Self::Text, None),
            "json" => (Self::Json, None),
            _ => (Self::Text, Some(value.trim().to_string())),
        }
    }
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

/// A type-erased `tracing` layer over the root [`tracing_subscriber::Registry`].
/// We collect every active layer (the always-on fmt layer first, then any OTLP
/// layers) into a single `Vec` of these and attach it in one `.with(..)` call.
type BoxedLayer = Box<dyn tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync>;

/// Build the ordered list of subscriber layers: the fmt (console) layer **first**
/// so the `Vec` is *never empty*, then any caller-supplied OTLP layers.
///
/// The "fmt first / never empty" invariant is load-bearing, not cosmetic: an
/// **empty** `Vec<L>` is a valid `Layer` whose `register_callsite` returns
/// `Interest::never()`, which `Layered::pick_interest` short-circuits to disable
/// **every** event for the whole subscriber — silently, while `try_init()` still
/// returns `Ok`. That is the exact bug this function exists to make impossible
/// (regressions are caught by `no_otlp_layer_stack_still_emits`). Keeping fmt at
/// index 0 guarantees ≥1 element on every path, including the common no-OTLP one.
fn build_layers<W>(
    log_format: LogFormat,
    make_writer: W,
    ansi: bool,
    mut otel_layers: Vec<BoxedLayer>,
) -> Vec<BoxedLayer>
where
    W: for<'a> tracing_subscriber::fmt::MakeWriter<'a> + Send + Sync + 'static,
{
    use tracing_subscriber::Layer as _;
    let fmt_layer: BoxedLayer = match log_format {
        LogFormat::Text => tracing_subscriber::fmt::layer()
            .with_ansi(ansi)
            .with_writer(make_writer)
            .boxed(),
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(false)
            .with_writer(make_writer)
            .boxed(),
    };
    let mut layers = Vec::with_capacity(1 + otel_layers.len());
    layers.push(fmt_layer);
    layers.append(&mut otel_layers);
    layers
}

/// Install the tracing subscriber for `service_name`: an fmt layer (preserving
/// the previous console behavior) plus OTLP trace + log layers when an endpoint
/// is configured. Returns a guard that flushes the OTLP providers on drop.
///
/// Installs the global subscriber exactly once per process (guarded by an
/// `AtomicBool`), so a second call (e.g. controller + webhook in one test
/// binary) is a no-op. A genuine **first**-install failure is fatal
/// ([`TelemetryError::SubscriberInit`]) — a backup operator must not run blind.
pub fn init_tracing(service_name: &str) -> Result<TelemetryGuard, TelemetryError> {
    use std::io::IsTerminal;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::prelude::*;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Degrade messages are DEFERRED until after the subscriber is installed:
    // logging them here (before `try_init`) would drop them, leaving an operator
    // with no hint that OTLP was silently disabled. Strict mode still fails fast
    // (returns Err before any global subscriber is installed).
    let mut deferred: Vec<String> = Vec::new();

    // Console format: text (default) or json. An unrecognized value degrades to
    // text rather than failing startup — the message is deferred like the rest.
    let (log_format, bad_format) = LogFormat::from_env();
    if let Some(v) = bad_format {
        deferred.push(format!(
            "{}={v:?} not recognized (use `text` or `json`); defaulting to text",
            env::KOPIUR_LOG_FORMAT
        ));
    }
    // Suppress ANSI escapes when stdout is not a TTY (i.e. inside a container),
    // so `kubectl logs` shows clean text instead of color control codes.
    let ansi = std::io::stdout().is_terminal();

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
    let mut otel_layers: Vec<BoxedLayer> = Vec::new();

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

    // Build the layer list with the fmt layer ALWAYS first (never an empty Vec —
    // see `build_layers`). Production logs to stdout.
    let layers = build_layers(log_format, std::io::stdout, ansi, otel_layers);

    // Install the global subscriber exactly once per process. The `AtomicBool`
    // swap means a second `init_tracing` (controller + webhook in one test
    // binary) is a clean no-op instead of a spurious `SetGlobalDefaultError`.
    if INITIALIZED.swap(true, std::sync::atomic::Ordering::SeqCst) {
        return Ok(TelemetryGuard {
            tracer_provider,
            logger_provider,
        });
    }

    if let Err(e) = tracing_subscriber::registry()
        .with(layers)
        .with(filter)
        .try_init()
    {
        // No subscriber is installed, so `tracing::*` would go nowhere — write
        // straight to stderr and fail fast: a backup operator must not run blind.
        eprintln!(
            "kopiur-telemetry: FATAL: could not install the tracing subscriber for \
             {service_name}: {e}"
        );
        return Err(TelemetryError::SubscriberInit {
            source: Box::new(e),
        });
    }

    // Subscriber is now installed — surface any deferred degrade messages.
    for msg in deferred {
        tracing::error!("{msg}");
    }

    Ok(TelemetryGuard {
        tracer_provider,
        logger_provider,
    })
}

/// Set the first time [`init_tracing`] attempts to install the global subscriber,
/// so subsequent calls in the same process are no-ops (test binaries init twice).
static INITIALIZED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

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
    fn log_format_parses_text_json_and_degrades_unknown() {
        // Default / explicit text variants → Text, no degrade message.
        for v in ["", "text", "TEXT", " plain ", "fmt"] {
            assert_eq!(LogFormat::parse(v), (LogFormat::Text, None), "input {v:?}");
        }
        // json (case-insensitive) → Json, no degrade message.
        assert_eq!(LogFormat::parse("json"), (LogFormat::Json, None));
        assert_eq!(LogFormat::parse("JSON"), (LogFormat::Json, None));
        // Unknown non-empty value → Text, carries the offending string so the
        // caller can warn.
        let (fmt, bad) = LogFormat::parse("yaml");
        assert_eq!(fmt, LogFormat::Text);
        assert_eq!(bad.as_deref(), Some("yaml"));
    }

    #[test]
    fn log_format_from_env_defaults_to_text_when_unset() {
        let prev = std::env::var(env::KOPIUR_LOG_FORMAT).ok();
        unsafe { std::env::remove_var(env::KOPIUR_LOG_FORMAT) };
        assert_eq!(LogFormat::from_env(), (LogFormat::Text, None));
        if let Some(v) = prev {
            unsafe { std::env::set_var(env::KOPIUR_LOG_FORMAT, v) };
        }
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

    /// A `MakeWriter` that captures everything written into a shared buffer, so a
    /// test can assert what the fmt layer actually emitted.
    #[derive(Clone)]
    struct CaptureWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Regression guard for the silent-logs bug: the no-OTLP layer stack (an
    /// fmt layer + ZERO OTLP layers) MUST still emit. Before the fix, the OTLP
    /// layers were attached as a separate **empty** `Vec`, whose `register_callsite`
    /// returns `Interest::never()` and disabled every event for the whole
    /// subscriber (while `try_init()` happily returned `Ok`). This exercises the
    /// exact production assembly via `build_layers` against a scoped dispatcher,
    /// so it fails if anyone reintroduces an empty outer layer.
    #[test]
    fn no_otlp_layer_stack_still_emits() {
        use tracing_subscriber::prelude::*;

        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let writer = CaptureWriter(buf.clone());

        // Same call as production's no-OTLP path: fmt first, no OTLP layers.
        let layers = build_layers(LogFormat::Text, writer, false, Vec::new());
        let subscriber = tracing_subscriber::registry()
            .with(layers)
            .with(tracing_subscriber::EnvFilter::new("info"));

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("PROBE-LINE-9f3a");
        });

        let out = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            out.contains("PROBE-LINE-9f3a"),
            "no-OTLP subscriber emitted nothing (empty-outer-layer regression); captured: {out:?}"
        );
    }
}
