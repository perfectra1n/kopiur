# kopiur-telemetry

Shared observability for Kopiur — instrument once, fan out to a Prometheus pull endpoint and an optional OTLP push.

## Role in the workspace

`kopiur-telemetry` is the single observability surface imported by the
**controller**, **webhook**, and **mover**. Its one load-bearing idea:
**instrument once against the OpenTelemetry metrics API, then fan out to two
readers** —

1. an [`opentelemetry-prometheus`] exporter that populates a
   [`prometheus::Registry`] behind the always-on `/metrics` pull endpoint (so a
   `ServiceMonitor` scrapes the pods directly — no collector required, no
   topology change), and
2. an OTLP [`PeriodicReader`] that _pushes_ the same measurements to a collector
   — added **only** when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

Recording a value updates both; there is no double instrumentation. Traces (the
controller's `#[instrument]` reconcile spans) and logs (bridged from `tracing`
events) export over OTLP via the maintained `tracing-opentelemetry` and
`opentelemetry-appender-tracing` crates, also env-gated.

**OTLP is env-gated and off by default.** With no endpoint configured the
behavior is identical to the previous fmt-only setup, so `cargo test` stays
hermetic and offline. OTLP setup is treated as **non-critical**: a
misconfiguration is logged with an actionable [`TelemetryError`] and the operator
**degrades** to fmt-logging + the Prometheus pull — unless
`KOPIUR_OTEL_STRICT=true`, which makes it fail fast (a backup operator must not
run blind).

## Key types

| Item                            | Role                                                                                                                                                                                              |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`init_tracing`]                | Installs the global `tracing` subscriber once per process (fmt layer always first, then OTLP trace/log layers when configured). Returns a [`TelemetryGuard`] that flushes OTLP providers on drop. |
| [`MetricsProvider`]             | Owns the meter provider + Prometheus registry. `meter()` creates instruments; `gather()` renders the `/metrics` text exposition; `shutdown()` flushes before a short-lived mover exits.           |
| [`OtlpConfig`]                  | Resolved (validated) OTLP endpoint, or `None` when OTLP is disabled.                                                                                                                              |
| [`LogFormat`]                   | Console fmt format selected by `KOPIUR_LOG_FORMAT`: `Text` (default) or `Json`.                                                                                                                   |
| [`TelemetryError`] / [`Signal`] | Actionable, classified telemetry errors and the signal (metrics/traces/logs) they apply to.                                                                                                       |

## Environment variables

The names are centralized in the [`env`](mod@crate::env) module — read sites use the constants,
never string literals.

| Constant                             | Variable                      | Effect                                                               |
| ------------------------------------ | ----------------------------- | -------------------------------------------------------------------- |
| [`env::OTEL_EXPORTER_OTLP_ENDPOINT`] | `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector gRPC endpoint; unset disables OTLP entirely.               |
| [`env::OTEL_EXPORTER_OTLP_PROTOCOL`] | `OTEL_EXPORTER_OTLP_PROTOCOL` | Only `grpc` is compiled in; other values are rejected.               |
| [`env::OTEL_EXPORTER_OTLP_HEADERS`]  | `OTEL_EXPORTER_OTLP_HEADERS`  | Extra OTLP headers (e.g. `authorization=Bearer …`).                  |
| [`env::KOPIUR_OTEL_STRICT`]          | `KOPIUR_OTEL_STRICT`          | `true`/`1` makes telemetry misconfig fail-fast instead of degrading. |
| [`env::RUST_LOG`]                    | `RUST_LOG`                    | Standard `tracing` filter (default `info`; e.g. `info,kopia=debug`). |
| [`env::KOPIUR_LOG_FORMAT`]           | `KOPIUR_LOG_FORMAT`           | `text` (default) or `json`; unknown values degrade to `text`.        |

[`env::OTLP_PASSTHROUGH`] and [`env::LOG_PASSTHROUGH`] list the vars the
controller forwards onto mover `Job`s (OTLP only when a collector is configured;
logging whenever set).

## Example

The OTLP passthrough list is a pure, importable constant — the endpoint is always
forwarded first and the protocol var is always included:

```rust
use kopiur_telemetry::env;

// The controller forwards these onto every mover Job; endpoint first.
assert_eq!(env::OTLP_PASSTHROUGH[0], env::OTEL_EXPORTER_OTLP_ENDPOINT);
assert!(env::OTLP_PASSTHROUGH.contains(&env::OTEL_EXPORTER_OTLP_PROTOCOL));

// Logging vars are forwarded even with no collector configured.
assert!(env::LOG_PASSTHROUGH.contains(&env::RUST_LOG));
assert!(env::LOG_PASSTHROUGH.contains(&env::KOPIUR_LOG_FORMAT));
```

Wiring it into a binary (needs a running process, so `no_run`):

```rust,no_run
use kopiur_telemetry::{init_tracing, MetricsProvider};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
// Install the subscriber once; keep the guard alive for the process lifetime.
let _guard = init_tracing("kopiur-controller")?;

// Build the meter provider + Prometheus registry; create instruments from it.
let metrics = MetricsProvider::new("kopiur-controller");
let reconciles = metrics.meter().u64_counter("kopiur_controller_reconciliations_total").build();
reconciles.add(1, &[opentelemetry::KeyValue::new("kind", "Snapshot")]);

// Serve `metrics.gather()` from `/metrics`; call `metrics.shutdown()` before a mover exits.
# Ok(())
# }
```

## See also

- [`docs/dev/observability.md`](../../docs/dev/observability.md) — the full
  architecture, the `kopiur_*` metric catalog, Helm knobs, and the OTLP collector
  recipe.
- [ADR-0003](../../docs/adr/0003-kopiur-rust-operator.md) — the canonical design
  for the operator this crate instruments.

[`opentelemetry-prometheus`]: https://docs.rs/opentelemetry-prometheus
[`prometheus::Registry`]: https://docs.rs/prometheus/latest/prometheus/struct.Registry.html
[`PeriodicReader`]: https://docs.rs/opentelemetry_sdk/latest/opentelemetry_sdk/metrics/struct.PeriodicReader.html
