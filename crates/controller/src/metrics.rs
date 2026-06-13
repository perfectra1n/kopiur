//! Controller metrics (ADR §4.13 / §4.10).
//!
//! Instrumented **once** against the OpenTelemetry metrics API and fanned out to
//! two readers by [`kopiur_telemetry::MetricsProvider`]: an always-on Prometheus
//! exporter (the `/metrics` pull endpoint + `ServiceMonitor`) and — when
//! `OTEL_EXPORTER_OTLP_ENDPOINT` is set — an OTLP push reader. Recording a value
//! updates both; there is no double instrumentation.
//!
//! Every metric is under the `kopiur_` namespace. The Prometheus exporter
//! applies the usual OTel→Prometheus conventions, so a `u64_counter` named
//! `kopiur_controller_reconciliations` is exported as
//! `kopiur_controller_reconciliations_total`. The `/metrics` text is rendered by
//! [`Metrics::gather`]; the HTTP server lives in `lib.rs`.
//!
//! [`Metrics`] is cloned into the shared [`crate::context::Context`]; the
//! OpenTelemetry instruments and the provider are internally reference-counted,
//! so clones share state.

use std::sync::Arc;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram};

use kopiur_api::{PhaseLabel, RepositoryPhase, RestorePhase, SnapshotPhase};
use kopiur_telemetry::MetricsProvider;

/// Resident set size (RSS) of the current process in bytes, read from Linux
/// `/proc/self/statm` (field 2 is the resident page count). Returns `None` off
/// Linux or on any read/parse failure, so the gauge is simply absent rather than
/// fabricated — telemetry is non-critical.
fn resident_memory_bytes() -> Option<i64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: i64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    // SAFETY: `sysconf` is a pure lookup with no preconditions or side effects.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page_size > 0).then(|| resident_pages.saturating_mul(page_size))
}

/// All controller metrics, sharing one meter provider + Prometheus registry.
#[derive(Clone)]
pub struct Metrics {
    provider: Arc<MetricsProvider>,

    // Reconcile loop (kube-rs standard).
    reconciliations: Counter<u64>,
    reconcile_errors: Counter<u64>,
    reconcile_duration: Histogram<f64>,

    // Per-resource lifecycle phase: value 1 for the active phase, 0 for the
    // others (enumerate-and-reset), labeled by kind/namespace/name/phase.
    resource_phase: Gauge<i64>,

    // Snapshot business metrics.
    backup_last_success_timestamp: Gauge<i64>,
    backup_consecutive_failures: Gauge<i64>,
    backup_size_bytes: Gauge<i64>,
    backup_files: Gauge<i64>,
    backup_duration_seconds: Gauge<i64>,
    snapshot_deletion_failures: Counter<u64>,
    orphaned_snapshots: Counter<u64>,
    schedule_backups_created: Counter<u64>,
    secrets_projected: Counter<u64>,
    backups_refused: Counter<u64>,

    // Repository business metrics.
    repo_size_bytes: Gauge<i64>,
    repo_snapshot_count: Gauge<i64>,
    repo_discovered_backups: Gauge<i64>,
    repo_maintenance_configured: Gauge<i64>,

    // Restore + maintenance.
    restore_duration_seconds: Gauge<i64>,
    maintenance_reclaimed_bytes: Gauge<i64>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Build the meter provider (Prometheus + optional OTLP) and every
    /// instrument. Infallible: a provider build failure degrades to an empty
    /// `/metrics` rather than crashing the controller (telemetry is non-critical).
    pub fn new() -> Self {
        let provider = MetricsProvider::new("kopiur-controller");
        let m = provider.meter();

        let reconciliations = m
            .u64_counter("kopiur_controller_reconciliations")
            .with_description("Total reconciliations per CRD kind.")
            .build();
        let reconcile_errors = m
            .u64_counter("kopiur_controller_reconcile_errors")
            .with_description("Total reconcile errors per CRD kind and error class.")
            .build();
        let reconcile_duration = m
            .f64_histogram("kopiur_controller_reconcile_duration_seconds")
            .with_description("Reconcile duration in seconds per CRD kind.")
            .with_boundaries(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
            .build();

        let resource_phase = m
            .i64_gauge("kopiur_resource_phase")
            .with_description("1 for a resource's active lifecycle phase, 0 otherwise.")
            .build();

        // Process RSS, sampled at scrape time. The returned handle is a phantom
        // marker — the callback is retained by the meter provider — so it is built
        // and discarded. Surfaces the controller's footprint on `/metrics` and guards
        // the memory-reduction work (mimalloc, worker-thread cap, scoped/metadata
        // watches) against regressions. Bytes; absent off Linux.
        // Bytes are in the name (matching kopiur_snapshot_size_bytes etc.) with no
        // `with_unit` — the Prometheus exporter appends a unit suffix, which would
        // otherwise produce a doubled `..._bytes_bytes`.
        let _ = m
            .i64_observable_gauge("kopiur_process_resident_memory_bytes")
            .with_description("Resident set size (RSS) of the controller process, in bytes.")
            .with_callback(|observer| {
                if let Some(rss) = resident_memory_bytes() {
                    observer.observe(rss, &[]);
                }
            })
            .build();

        let backup_last_success_timestamp = m
            .i64_gauge("kopiur_snapshot_last_success_timestamp_seconds")
            .with_description("Unix timestamp of the most recent successful backup.")
            .build();
        let backup_consecutive_failures = m
            .i64_gauge("kopiur_snapshot_consecutive_failures")
            .with_description("Number of consecutive backup failures.")
            .build();
        let backup_size_bytes = m
            .i64_gauge("kopiur_snapshot_size_bytes")
            .with_description("Logical size in bytes of the last successful backup.")
            .build();
        let backup_files = m
            .i64_gauge("kopiur_snapshot_files")
            .with_description("File count of the last successful backup.")
            .build();
        let backup_duration_seconds = m
            .i64_gauge("kopiur_snapshot_duration_seconds")
            .with_description("Duration in seconds of the last successful backup.")
            .build();
        let snapshot_deletion_failures = m
            .u64_counter("kopiur_snapshot_deletion_failures")
            .with_description("Total kopia snapshot-deletion failures during finalizer handling.")
            .build();
        let orphaned_snapshots = m
            .u64_counter("kopiur_orphaned_snapshots")
            .with_description(
                "Total snapshots orphaned (Orphan policy or skip-snapshot-cleanup annotation).",
            )
            .build();
        let schedule_backups_created = m
            .u64_counter("kopiur_schedule_snapshots_created")
            .with_description("Total Snapshot CRs created by a SnapshotSchedule.")
            .build();
        let secrets_projected = m
            .u64_counter("kopiur_secrets_projected")
            .with_description(
                "Total credential Secrets projected into a mover Job's namespace \
                 (opt-in spec.credentialProjection).",
            )
            .build();
        let backups_refused = m
            .u64_counter("kopiur_snapshot_refusals")
            .with_description(
                "Total backups refused by policy (e.g. a ReadOnly repository, a privileged \
                 mover without the namespace opt-in), labeled by reason. Refusals are \
                 deliberate decisions, not reconcile errors, so they are not in \
                 kopiur_controller_reconcile_errors.",
            )
            .build();

        let repo_size_bytes = m
            .i64_gauge("kopiur_repo_size_bytes")
            .with_description(
                "Logical bytes under management (sum of the latest snapshot per source).",
            )
            .build();
        let repo_snapshot_count = m
            .i64_gauge("kopiur_repo_snapshot_count")
            .with_description("Number of snapshots in the repository.")
            .build();
        let repo_discovered_backups = m
            .i64_gauge("kopiur_repo_discovered_snapshots")
            .with_description("Number of backups discovered in the repository catalog.")
            .build();
        let repo_maintenance_configured = m
            .i64_gauge("kopiur_repository_maintenance_configured")
            .with_description(
                "1 if a Maintenance CR references the repository, 0 otherwise (unmaintained \
                 repositories never reclaim storage).",
            )
            .build();

        let restore_duration_seconds = m
            .i64_gauge("kopiur_restore_duration_seconds")
            .with_description("Wall-clock duration in seconds of the last restore Job.")
            .build();
        let maintenance_reclaimed_bytes = m
            .i64_gauge("kopiur_maintenance_last_reclaimed_bytes")
            .with_description("Bytes reclaimed by the last full maintenance run.")
            .build();

        Metrics {
            provider: Arc::new(provider),
            reconciliations,
            reconcile_errors,
            reconcile_duration,
            resource_phase,
            backup_last_success_timestamp,
            backup_consecutive_failures,
            backup_size_bytes,
            backup_files,
            backup_duration_seconds,
            snapshot_deletion_failures,
            orphaned_snapshots,
            schedule_backups_created,
            secrets_projected,
            backups_refused,
            repo_size_bytes,
            repo_snapshot_count,
            repo_discovered_backups,
            repo_maintenance_configured,
            restore_duration_seconds,
            maintenance_reclaimed_bytes,
        }
    }

    // ---- reconcile loop ----------------------------------------------------

    /// Record a completed reconcile of `kind` lasting `seconds`.
    pub fn record_reconcile(&self, kind: &str, seconds: f64) {
        let attrs = [KeyValue::new("kind", kind.to_string())];
        self.reconciliations.add(1, &attrs);
        self.reconcile_duration.record(seconds, &attrs);
    }

    /// Record a reconcile error of `kind` with the given error `class`.
    pub fn record_error(&self, kind: &str, class: &str) {
        self.reconcile_errors.add(
            1,
            &[
                KeyValue::new("kind", kind.to_string()),
                KeyValue::new("class", class.to_string()),
            ],
        );
    }

    // ---- per-resource phase (enumerate-and-reset) --------------------------

    /// Record `kind`'s phase gauge for `active`: 1 for the active variant, 0 for
    /// every other. The variant set + labels come from the [`PhaseLabel`] enum
    /// itself (single source of truth — a new variant can't be silently missed).
    fn write_phase<P: PhaseLabel>(&self, kind: &str, ns: &str, name: &str, active: Option<P>) {
        for p in P::ALL {
            let v = if Some(*p) == active { 1 } else { 0 };
            self.resource_phase.record(
                v,
                &[
                    KeyValue::new("kind", kind.to_string()),
                    KeyValue::new("namespace", ns.to_string()),
                    KeyValue::new("name", name.to_string()),
                    KeyValue::new("phase", p.label()),
                ],
            );
        }
    }

    /// Clear `kind`'s phase gauge for a resource (all variants → 0). Call this on
    /// deletion so `kopiur_resource_phase{...} == 1` alerts stop firing for a CR
    /// that no longer exists (OTel sync gauges can't drop a series; zeroing it is
    /// the available remedy).
    pub fn clear_phase<P: PhaseLabel>(&self, kind: &str, ns: &str, name: &str) {
        self.write_phase::<P>(kind, ns, name, None);
    }

    /// Record a `Repository`/`ClusterRepository` phase gauge.
    pub fn set_repository_phase(&self, kind: &str, ns: &str, name: &str, phase: RepositoryPhase) {
        self.write_phase(kind, ns, name, Some(phase));
    }

    /// Record a `Snapshot` phase gauge.
    pub fn set_backup_phase(&self, ns: &str, name: &str, phase: SnapshotPhase) {
        self.write_phase("Snapshot", ns, name, Some(phase));
    }

    /// Record a `Restore` phase gauge.
    pub fn set_restore_phase(&self, ns: &str, name: &str, phase: RestorePhase) {
        self.write_phase("Restore", ns, name, Some(phase));
    }

    // ---- backup business metrics -------------------------------------------

    /// Stamp the Unix timestamp of a successful backup.
    pub fn set_backup_last_success(&self, ns: &str, name: &str, ts: i64) {
        self.backup_last_success_timestamp
            .record(ts, &ns_name(ns, name));
    }

    /// Set the consecutive-failure count for a SnapshotPolicy.
    pub fn set_backup_consecutive_failures(&self, ns: &str, name: &str, n: i64) {
        self.backup_consecutive_failures
            .record(n, &ns_name(ns, name));
    }

    /// Set the last successful backup's size/files/duration gauges.
    pub fn set_backup_stats(
        &self,
        ns: &str,
        name: &str,
        size_bytes: Option<i64>,
        files: Option<i64>,
        duration_seconds: Option<i64>,
    ) {
        let labels = ns_name(ns, name);
        if let Some(v) = size_bytes {
            self.backup_size_bytes.record(v, &labels);
        }
        if let Some(v) = files {
            self.backup_files.record(v, &labels);
        }
        if let Some(v) = duration_seconds {
            self.backup_duration_seconds.record(v, &labels);
        }
    }

    /// Count `n` credential Secrets projected into mover namespace `ns` (opt-in
    /// `spec.credentialProjection`).
    pub fn inc_secrets_projected(&self, ns: &str, n: u64) {
        self.secrets_projected
            .add(n, &[KeyValue::new("namespace", ns.to_string())]);
    }

    /// Count a snapshot-deletion (finalizer) failure in `namespace`.
    pub fn inc_snapshot_deletion_failure(&self, ns: &str) {
        self.snapshot_deletion_failures
            .add(1, &[KeyValue::new("namespace", ns.to_string())]);
    }

    /// Count a snapshot orphaned (Orphan policy / escape hatch) in `namespace`.
    pub fn inc_orphaned_snapshot(&self, ns: &str) {
        self.orphaned_snapshots
            .add(1, &[KeyValue::new("namespace", ns.to_string())]);
    }

    /// Count a Snapshot CR created by a SnapshotSchedule.
    pub fn inc_schedule_backup_created(&self, ns: &str, name: &str) {
        self.schedule_backups_created.add(1, &ns_name(ns, name));
    }

    /// Count a backup refused by policy. `reason` is the same machine-readable
    /// label as the Event/condition reason (e.g. `RepositoryReadOnly`,
    /// `PrivilegedMoverNotPermitted`) so dashboards and `kubectl get events`
    /// agree on the cause.
    pub fn inc_backup_refused(&self, ns: &str, name: &str, reason: &'static str) {
        self.backups_refused.add(
            1,
            &[
                KeyValue::new("namespace", ns.to_string()),
                KeyValue::new("name", name.to_string()),
                KeyValue::new("reason", reason),
            ],
        );
    }

    // ---- repository / restore / maintenance --------------------------------

    /// Set the repository size gauge.
    pub fn set_repo_size_bytes(&self, ns: &str, name: &str, bytes: i64) {
        self.repo_size_bytes.record(bytes, &ns_name(ns, name));
    }

    /// Set the repository snapshot-count and discovered-backup gauges.
    pub fn set_repo_catalog(
        &self,
        ns: &str,
        name: &str,
        snapshot_count: Option<i64>,
        discovered: Option<i64>,
    ) {
        let labels = ns_name(ns, name);
        if let Some(v) = snapshot_count {
            self.repo_snapshot_count.record(v, &labels);
        }
        if let Some(v) = discovered {
            self.repo_discovered_backups.record(v, &labels);
        }
    }

    /// Set the maintenance-configured gauge for a repository: 1 if a `Maintenance`
    /// CR references it, 0 otherwise. `kind` is `Repository`/`ClusterRepository`;
    /// `ns` is empty for a cluster-scoped `ClusterRepository`.
    pub fn set_repository_maintenance_configured(
        &self,
        kind: &str,
        ns: &str,
        name: &str,
        configured: bool,
    ) {
        self.repo_maintenance_configured.record(
            configured as i64,
            &[
                KeyValue::new("kind", kind.to_string()),
                KeyValue::new("namespace", ns.to_string()),
                KeyValue::new("name", name.to_string()),
            ],
        );
    }

    /// Set the last restore's duration gauge.
    pub fn set_restore_duration(&self, ns: &str, name: &str, seconds: i64) {
        self.restore_duration_seconds
            .record(seconds, &ns_name(ns, name));
    }

    /// Set the last full-maintenance reclaimed-bytes gauge.
    pub fn set_maintenance_reclaimed_bytes(&self, ns: &str, name: &str, bytes: i64) {
        self.maintenance_reclaimed_bytes
            .record(bytes, &ns_name(ns, name));
    }

    // ---- exposition --------------------------------------------------------

    /// Render the Prometheus text exposition for the `/metrics` endpoint.
    pub fn gather(&self) -> Vec<u8> {
        self.provider.gather()
    }
}

fn ns_name(ns: &str, name: &str) -> [KeyValue; 2] {
    [
        KeyValue::new("namespace", ns.to_string()),
        KeyValue::new("name", name.to_string()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_register_and_export_under_kopiur_namespace() {
        let m = Metrics::new();
        m.record_reconcile("Snapshot", 0.1);
        m.record_error("Snapshot", "transient");
        m.inc_orphaned_snapshot("ns");
        m.set_backup_phase("ns", "db", SnapshotPhase::Succeeded);
        m.set_backup_stats("ns", "db", Some(1234), Some(10), Some(5));
        m.set_repository_maintenance_configured("Repository", "ns", "nas", false);
        let text = String::from_utf8(m.gather()).unwrap();
        // The Prometheus exporter appends `_total` to counters.
        assert!(
            text.contains("kopiur_controller_reconciliations_total"),
            "{text}"
        );
        assert!(text.contains("kopiur_orphaned_snapshots_total"), "{text}");
        assert!(text.contains("kopiur_resource_phase"), "{text}");
        assert!(text.contains("kopiur_snapshot_size_bytes"), "{text}");
        assert!(
            text.contains("kopiur_repository_maintenance_configured"),
            "{text}"
        );
    }

    #[test]
    fn backup_refusals_export_with_the_reason_label() {
        // The refusal counter is the dashboard-visible side of a policy
        // refusal (read-only repo / ungated privileged mover) — the reconcile
        // itself returns Ok, so kopiur_controller_reconcile_errors_total never
        // sees it and this counter is the only aggregate signal.
        let m = Metrics::new();
        m.inc_backup_refused("apps", "db-daily", "RepositoryReadOnly");
        let text = String::from_utf8(m.gather()).unwrap();
        assert!(text.contains("kopiur_snapshot_refusals_total"), "{text}");
        assert!(text.contains("reason=\"RepositoryReadOnly\""), "{text}");
        assert!(text.contains("name=\"db-daily\""), "{text}");
    }

    #[test]
    fn clear_phase_zeros_all_variants() {
        let m = Metrics::new();
        m.set_backup_phase("ns", "db", SnapshotPhase::Failed);
        m.clear_phase::<SnapshotPhase>("Snapshot", "ns", "db");
        let text = String::from_utf8(m.gather()).unwrap();
        // After clearing, no Snapshot phase series for db is 1.
        for line in text.lines() {
            if line.starts_with("kopiur_resource_phase{")
                && line.contains("name=\"db\"")
                && line.contains("kind=\"Snapshot\"")
            {
                assert!(line.trim_end().ends_with(" 0"), "phase not cleared: {line}");
            }
        }
    }
}
