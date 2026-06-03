# Observability

Kopiur exposes Prometheus metrics, and can additionally export OpenTelemetry
(OTLP) traces, logs, and metrics. The implementation lives in the
**`kopiur-telemetry`** crate, shared by the controller, webhook, and mover.

## The one idea: instrument once, two readers

Metrics are instrumented **once** against the OpenTelemetry metrics API. A single
`SdkMeterProvider` fans out to two readers:

1. an **`opentelemetry-prometheus` exporter** that populates a `prometheus::Registry`
   behind the always-on `/metrics` pull endpoint (so a `ServiceMonitor` scrapes the
   pods directly — no collector required), and
2. an **OTLP `PeriodicReader`** that *pushes* the same measurements to a collector
   — added only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

Recording a value updates both; there is no double instrumentation. Traces (the
controller's `#[instrument]` reconcile spans) and logs (bridged from `tracing`
events) export over OTLP via `tracing-opentelemetry` and
`opentelemetry-appender-tracing`.

**OTLP is env-gated and off by default.** With no endpoint configured the
behavior is identical to fmt-only logging + the Prometheus pull, so the hermetic
test suite stays offline. A misconfiguration is logged with an actionable error
and **degrades** to fmt-logging + the Prometheus pull rather than crashing a
backup operator — unless `KOPIUR_OTEL_STRICT=true`, which makes it fail fast.

## HTTP endpoints

| Component | Endpoint | Notes |
|---|---|---|
| Controller | `GET /metrics`, `/healthz`, `/readyz` on `:8080` (axum) | probes hit the real health routes |
| Webhook | `GET /metrics` on its TLS port (8443) | plus `/healthz`, `/readyz` |
| Mover | none (short-lived Job) | OTLP **push** only; flushed before exit |

## Metrics

All metrics are under the `kopiur_` namespace. The Prometheus exporter applies
the OTel→Prometheus conventions, so a counter instrument named
`kopiur_x` is exported as `kopiur_x_total`.

| Metric | Type | Labels | Source |
|---|---|---|---|
| `kopiur_controller_reconciliations_total` | counter | `kind` | every reconcile |
| `kopiur_controller_reconcile_errors_total` | counter | `kind`, `class` (`transient`/`structural`) | `error_policy` |
| `kopiur_controller_reconcile_duration_seconds` | histogram | `kind` | every reconcile |
| `kopiur_resource_phase` | gauge (0/1) | `kind`, `namespace`, `name`, `phase` | CR status; 1 = active phase, 0 = others; **zeroed on deletion** |
| `kopiur_backup_last_success_timestamp_seconds` | gauge | `namespace`, `name` | Backup → Succeeded |
| `kopiur_backup_consecutive_failures` | gauge | `namespace`, `name` | BackupConfig reconcile (trailing Failed before the latest Succeeded) |
| `kopiur_backup_size_bytes` | gauge | `namespace`, `name` | Backup `status.stats.sizeBytes` |
| `kopiur_backup_files` | gauge | `namespace`, `name` | Backup file counts (absent when unknown) |
| `kopiur_backup_duration_seconds` | gauge | `namespace`, `name` | Backup `status.timing.durationSeconds` |
| `kopiur_orphaned_snapshots_total` | counter | `namespace` | Orphan policy / skip-cleanup escape hatch |
| `kopiur_snapshot_deletion_failures_total` | counter | `namespace` | finalizer snapshot-delete failures |
| `kopiur_schedule_backups_created_total` | counter | `namespace`, `name` | BackupSchedule fires |
| `kopiur_repo_size_bytes` | gauge | `namespace`, `name` | logical bytes under management (newest snapshot per source) |
| `kopiur_repo_snapshot_count` | gauge | `namespace`, `name` | repository catalog scan |
| `kopiur_repo_discovered_backups` | gauge | `namespace`, `name` | repository catalog scan |
| `kopiur_restore_duration_seconds` | gauge | `namespace`, `name` | restore Job completion − start |
| `kopiur_maintenance_last_reclaimed_bytes` | gauge | `namespace`, `name` | full maintenance run |
| `kopiur_webhook_admission_total` | counter | `kind`, `decision` (`allowed`/`denied`) | admission webhook |
| `kopiur_mover_operations_total` | counter | `operation`, `result` | mover Job (OTLP push) |
| `kopiur_mover_operation_duration_seconds` | histogram | `operation`, `result` | mover Job (OTLP push) |

Notes:
- `kopiur_resource_phase` is **zeroed when a CR is deleted** so `… == 1` alerts
  clear before the object is garbage-collected (OTel sync gauges can't drop a
  series; zeroing is the available remedy). Series for long-deleted resources
  persist at `0`.
- Per-resource gauges are re-read from the freshest status on each successful
  reconcile, so they don't lag a cycle behind a phase transition.

## Enabling everything (Helm)

```bash
helm upgrade --install kopiur deploy/helm/kopiur -n kopiur-system \
  --set metrics.serviceMonitor.enabled=true \
  --set metrics.prometheusRule.enabled=true \
  --set grafanaDashboard.enabled=true \
  --set webhook.serviceMonitor.enabled=true \
  --set observability.otlp.enabled=true \
  --set observability.otlp.endpoint=http://otel-collector.observability.svc:4317
```

A ready-to-use values overlay is at
[`deploy/observability-values.yaml`](../../deploy/observability-values.yaml):

```bash
helm upgrade --install kopiur deploy/helm/kopiur -n kopiur-system \
  -f deploy/observability-values.yaml
```

Keys (see `deploy/helm/kopiur/values.yaml` for the full set):

| Key | Default | Effect |
|---|---|---|
| `metrics.serviceMonitor.enabled` | `false` | scrape the controller `/metrics` |
| `metrics.prometheusRule.enabled` | `false` | install the kopiur alert rules |
| `grafanaDashboard.enabled` | `false` | ship the dashboard as a sidecar ConfigMap |
| `webhook.serviceMonitor.enabled` | `false` | scrape the webhook `/metrics` (HTTPS) |
| `observability.otlp.enabled` | `false` | export OTLP from all components |
| `observability.otlp.endpoint` | `…:4317` | collector gRPC endpoint (required when enabled) |
| `observability.otlp.protocol` | `grpc` | only gRPC is compiled in |
| `observability.otlp.headers` | `""` | e.g. `authorization=Bearer …` |
| `observability.otlp.strict` | `false` | fail-fast on telemetry misconfig |

When OTLP is enabled the controller passes the same `OTEL_EXPORTER_OTLP_*` env to
every mover `Job` it creates, so mover traces/logs/metrics reach the same collector.

## Environment variables

The env var **names** are centralized in `crates/telemetry/src/env.rs`
(`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL`,
`OTEL_EXPORTER_OTLP_HEADERS`, `KOPIUR_OTEL_STRICT`); the Helm `observability.otlp`
block sets them. Only gRPC is compiled in — point the endpoint at the collector's
gRPC port (4317). Setting `OTEL_EXPORTER_OTLP_PROTOCOL` to anything other than
`grpc` is rejected with an actionable error.

## Dashboard

`deploy/dashboards/kopiur.json` is the source of truth (import it into Grafana
directly). The chart copy under `deploy/helm/kopiur/files/dashboards/kopiur.json`
is **generated** from it by `cargo xtask gen-all` and guarded by
`cargo xtask gen-all --check`, so the two can never drift. Edit the source, then
regenerate.

## Grafana via the OTLP path

If you run OTLP-only and don't scrape the pods, point Prometheus at the
collector instead. A minimal OpenTelemetry Collector that ingests OTLP and
re-exposes a Prometheus scrape target:

```yaml
# otel-collector config (configmap data)
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
exporters:
  prometheus:
    endpoint: 0.0.0.0:8889       # scrape this with Prometheus
  # debug:                        # uncomment to see traces/logs in the collector log
service:
  pipelines:
    metrics: { receivers: [otlp], exporters: [prometheus] }
    traces:  { receivers: [otlp], exporters: [debug] }
    logs:    { receivers: [otlp], exporters: [debug] }
```

For most users the direct-scrape `ServiceMonitor` path is simpler; OTLP is for
shops that already run a collector and want traces + logs alongside metrics.
```
