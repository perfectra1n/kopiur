//! Prometheus metrics for the controller (ADR §4.13 / §4.10).
//!
//! A single [`Metrics`] struct owns the registry and every collector. It is
//! cloned into the shared [`crate::context::Context`] (collectors are `Arc`
//! internally, so clones share state). The standard kube-rs reconcile metrics
//! plus the per-CRD business metrics named in the ADR live here.
//!
//! The `/metrics` text exposition is rendered by [`Metrics::gather`]; the tiny
//! HTTP server that serves it lives in `lib.rs` (no extra web-framework
//! dependency — a raw `tokio` listener).

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
};

/// All controller metrics, sharing one registry.
#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    /// Total reconciliations, labeled by `kind`.
    pub reconciliations: IntCounterVec,
    /// Total reconcile errors, labeled by `kind` and error `class`
    /// (`transient`/`structural`).
    pub reconcile_errors: IntCounterVec,
    /// Reconcile duration seconds, labeled by `kind`.
    pub reconcile_duration: HistogramVec,
    /// Unix timestamp of the last successful backup, labeled by
    /// `namespace`/`name` of the BackupConfig.
    pub backup_last_success_timestamp: IntGaugeVec,
    /// Consecutive backup failures, labeled by `namespace`/`name`.
    pub backup_consecutive_failures: IntGaugeVec,
    /// Total snapshot-deletion failures (finalizer path), labeled by
    /// `namespace`.
    pub snapshot_deletion_failures: IntCounterVec,
    /// Total snapshots orphaned via the skip annotation / Orphan policy.
    pub orphaned_snapshots: IntCounterVec,
    /// Repository size in bytes, labeled by `namespace`/`name`.
    pub repo_size_bytes: IntGaugeVec,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Build the registry and register every collector. Panics only on a
    /// programmer error (duplicate metric name), which is caught by the unit
    /// test below.
    pub fn new() -> Self {
        let registry = Registry::new();

        let reconciliations = IntCounterVec::new(
            Opts::new(
                "controller_reconciliations_total",
                "Total reconciliations per CRD kind.",
            ),
            &["kind"],
        )
        .expect("valid metric");
        let reconcile_errors = IntCounterVec::new(
            Opts::new(
                "controller_reconcile_errors_total",
                "Total reconcile errors per CRD kind and error class.",
            ),
            &["kind", "class"],
        )
        .expect("valid metric");
        let reconcile_duration = HistogramVec::new(
            HistogramOpts::new(
                "controller_reconcile_duration_seconds",
                "Reconcile duration in seconds per CRD kind.",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["kind"],
        )
        .expect("valid metric");
        let backup_last_success_timestamp = IntGaugeVec::new(
            Opts::new(
                "kopia_backup_last_success_timestamp_seconds",
                "Unix timestamp of the most recent successful backup.",
            ),
            &["namespace", "name"],
        )
        .expect("valid metric");
        let backup_consecutive_failures = IntGaugeVec::new(
            Opts::new(
                "kopia_backup_consecutive_failures",
                "Number of consecutive backup failures.",
            ),
            &["namespace", "name"],
        )
        .expect("valid metric");
        let snapshot_deletion_failures = IntCounterVec::new(
            Opts::new(
                "kopia_snapshot_deletion_failures_total",
                "Total kopia snapshot-deletion failures during finalizer handling.",
            ),
            &["namespace"],
        )
        .expect("valid metric");
        let orphaned_snapshots = IntCounterVec::new(
            Opts::new(
                "kopia_orphaned_snapshots_total",
                "Total snapshots orphaned (Orphan policy or skip-snapshot-cleanup annotation).",
            ),
            &["namespace"],
        )
        .expect("valid metric");
        let repo_size_bytes = IntGaugeVec::new(
            Opts::new("kopia_repo_size_bytes", "Repository size in bytes."),
            &["namespace", "name"],
        )
        .expect("valid metric");

        for c in [
            Box::new(reconciliations.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(reconcile_errors.clone()),
            Box::new(backup_last_success_timestamp.clone()),
            Box::new(backup_consecutive_failures.clone()),
            Box::new(snapshot_deletion_failures.clone()),
            Box::new(orphaned_snapshots.clone()),
            Box::new(repo_size_bytes.clone()),
        ] {
            registry.register(c).expect("register metric");
        }
        registry
            .register(Box::new(reconcile_duration.clone()))
            .expect("register histogram");

        Metrics {
            registry,
            reconciliations,
            reconcile_errors,
            reconcile_duration,
            backup_last_success_timestamp,
            backup_consecutive_failures,
            snapshot_deletion_failures,
            orphaned_snapshots,
            repo_size_bytes,
        }
    }

    /// Record a completed reconcile of `kind` lasting `seconds`.
    pub fn record_reconcile(&self, kind: &str, seconds: f64) {
        self.reconciliations.with_label_values(&[kind]).inc();
        self.reconcile_duration
            .with_label_values(&[kind])
            .observe(seconds);
    }

    /// Record a reconcile error of `kind` with the given error `class`.
    pub fn record_error(&self, kind: &str, class: &str) {
        self.reconcile_errors
            .with_label_values(&[kind, class])
            .inc();
    }

    /// Render the Prometheus text exposition for the `/metrics` endpoint.
    pub fn gather(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        // Encoding only fails on a broken writer; a Vec never errors.
        let _ = encoder.encode(&families, &mut buf);
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_register_without_panicking() {
        let m = Metrics::new();
        m.record_reconcile("Backup", 0.1);
        m.record_error("Backup", "transient");
        m.orphaned_snapshots.with_label_values(&["ns"]).inc();
        let text = String::from_utf8(m.gather()).unwrap();
        assert!(text.contains("controller_reconciliations_total"));
        assert!(text.contains("kopia_orphaned_snapshots_total"));
    }
}
