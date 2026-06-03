//! Webhook admission metrics.
//!
//! A small meter provider (Prometheus pull + optional OTLP push, via
//! [`kopiur_telemetry::MetricsProvider`]) exposing one counter:
//! `kopiur_webhook_admission_total{kind,decision}`. Served at `/metrics` from
//! the same axum app as the admission endpoint.

use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;

use kopiur_telemetry::MetricsProvider;

/// Admission metrics for the webhook.
pub struct WebhookMetrics {
    provider: MetricsProvider,
    admission: Counter<u64>,
}

impl Default for WebhookMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl WebhookMetrics {
    /// Build the meter provider and the admission counter.
    pub fn new() -> Self {
        let provider = MetricsProvider::new("kopiur-webhook");
        let admission = provider
            .meter()
            .u64_counter("kopiur_webhook_admission")
            .with_description("Total admission reviews by CRD kind and decision.")
            .build();
        WebhookMetrics {
            provider,
            admission,
        }
    }

    /// Record one admission review's outcome.
    pub fn record(&self, kind: &str, allowed: bool) {
        self.admission.add(
            1,
            &[
                KeyValue::new("kind", kind.to_string()),
                KeyValue::new("decision", if allowed { "allowed" } else { "denied" }),
            ],
        );
    }

    /// Render the Prometheus text exposition for `/metrics`.
    pub fn gather(&self) -> Vec<u8> {
        self.provider.gather()
    }
}
