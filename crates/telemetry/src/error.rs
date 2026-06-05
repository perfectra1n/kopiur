//! Strongly-typed, actionable telemetry errors.
//!
//! Every variant states **what** failed, **why**, and the concrete **fix** — the
//! type-safety thesis extended to operator UX (see the `error-handling-and-e2e`
//! skill). OTLP setup is non-critical: callers log these and degrade to
//! fmt-logging + the always-on Prometheus pull rather than crash a backup
//! operator, unless `KOPIUR_OTEL_STRICT=true` opts into fail-fast.

/// The signal an OTLP exporter carries, used to attribute build failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    /// Distributed traces (reconcile spans).
    Traces,
    /// Structured logs bridged from `tracing` events.
    Logs,
    /// Metrics pushed via the OTLP periodic reader.
    Metrics,
}

impl Signal {
    /// Human label used in messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Signal::Traces => "traces",
            Signal::Logs => "logs",
            Signal::Metrics => "metrics",
        }
    }
}

impl std::fmt::Display for Signal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// All ways telemetry setup can fail. Non-fatal by policy (callers degrade),
/// except under `KOPIUR_OTEL_STRICT=true`.
#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` is set but not a usable URL.
    #[error(
        "OTEL_EXPORTER_OTLP_ENDPOINT='{value}' is not a valid URL ({source}). \
         Use scheme+host+port, e.g. http://otel-collector:4317 for OTLP/gRPC; \
         unset it to disable OTLP export."
    )]
    InvalidOtlpEndpoint {
        /// The offending value.
        value: String,
        /// The underlying parse error.
        #[source]
        source: url::ParseError,
    },

    /// `OTEL_EXPORTER_OTLP_PROTOCOL` requests a transport this build doesn't ship.
    #[error(
        "OTEL_EXPORTER_OTLP_PROTOCOL='{value}' is not supported by this build. \
         Only 'grpc' is compiled in; set OTEL_EXPORTER_OTLP_PROTOCOL=grpc (or unset it) \
         and point OTEL_EXPORTER_OTLP_ENDPOINT at the collector's gRPC port (4317)."
    )]
    UnsupportedProtocol {
        /// The requested protocol.
        value: String,
    },

    /// An OTLP exporter for `signal` failed to build (bad config / runtime).
    #[error(
        "failed to build the OTLP {signal} exporter: {source}. \
         Verify OTEL_EXPORTER_OTLP_ENDPOINT points at a reachable collector \
         (e.g. http://otel-collector:4317) and that a Tokio runtime is active; \
         unset OTEL_EXPORTER_OTLP_ENDPOINT to disable OTLP."
    )]
    ExporterBuild {
        /// Which signal's exporter failed.
        signal: Signal,
        /// The underlying exporter build error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The Prometheus exporter could not be registered into the registry.
    #[error(
        "failed to register the Prometheus exporter: {source}. \
         This is an internal error (duplicate metric registration); please file a bug."
    )]
    PrometheusRegister {
        /// The underlying registration error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The global tracing subscriber could not be installed. **Fatal** — the
    /// process would run with no logs at all, which for a backup operator is
    /// operationally blind, so [`crate::init_tracing`] returns this and the
    /// caller exits. In practice this only happens if a subscriber was already
    /// installed in the same process at startup (a bug worth filing).
    #[error(
        "failed to install the global tracing subscriber: {source}. \
         The process would run with no logs; refusing to continue. This usually means a \
         tracing subscriber was already installed in this process — please file a bug."
    )]
    SubscriberInit {
        /// The underlying `try_init` error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl TelemetryError {
    /// True when `KOPIUR_OTEL_STRICT=true` — the operator should fail-fast on
    /// telemetry misconfiguration instead of degrading.
    pub fn strict_mode() -> bool {
        std::env::var(crate::env::KOPIUR_OTEL_STRICT)
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_endpoint_message_is_actionable() {
        let err = TelemetryError::InvalidOtlpEndpoint {
            value: "://nope".into(),
            source: url::Url::parse("://nope").unwrap_err(),
        };
        let msg = err.to_string();
        assert!(msg.contains("OTEL_EXPORTER_OTLP_ENDPOINT"));
        assert!(msg.contains("4317"), "should suggest the gRPC port: {msg}");
        assert!(
            msg.contains("unset"),
            "should offer the disable path: {msg}"
        );
    }

    #[test]
    fn unsupported_protocol_names_the_fix() {
        let err = TelemetryError::UnsupportedProtocol {
            value: "http/protobuf".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("grpc"), "should point at grpc: {msg}");
        assert!(msg.contains("4317"));
    }

    #[test]
    fn exporter_build_names_the_signal() {
        let err = TelemetryError::ExporterBuild {
            signal: Signal::Traces,
            source: "boom".into(),
        };
        assert!(err.to_string().contains("traces"));
    }
}
