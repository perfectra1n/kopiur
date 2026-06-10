# kopiur-webhook

An [`axum`] 0.8 + `rustls` Kubernetes admission webhook for the Kopiur CRDs —
the enforcement point for the cross-field rules the type system can't express.

## Role in the workspace

`kopiur-webhook` is the admission half of the operator (ADR-0003 §5.3). Kopiur's
core thesis is that "exactly one of" surfaces are Rust `enum`s, so most invalid
states are unrepresentable. The webhook covers the remainder — the _cross-field_
rules a structural schema cannot encode (ADR §2.2 principle 8):
mutually-exclusive fields, malformed cron, `ClusterRepository` tenancy, and
origin-aware `deletionPolicy`.

It is both a **validating** and a **mutating** webhook served from a single
endpoint: it denies invalid objects and, in the same pass, applies safe
defaulting patches (origin-aware `deletionPolicy`, the
`kopiur.home-operations.com/snapshot-cleanup` finalizer, GitOps-safe schedule
defaults). See [`routes`] for the single-endpoint rationale.

**One validator, two callers.** The webhook does not fork validation logic. Each
handler is a thin adapter over the **same** [`kopiur_api::validate`] validators
the controller calls defensively before reconcile (ADR §5.1). Validation
behavior is therefore identical across both call sites — a rule cannot be
enforced at admission but silently skipped at reconcile (or vice versa).

## Key modules and types

- [`routes::app`] — builds the [`axum::Router`]; [`routes::AppState`] carries the
  optional [`kube::Client`] (used for tenancy lookups) and metrics.
- [`handlers::dispatch`] — routes an `AdmissionReview` to the per-kind handler,
  each of which calls into [`kopiur_api::validate`].
- [`handlers::SNAPSHOT_CLEANUP_FINALIZER`] — the finalizer the mutating pass adds.
- [`tenancy`] — [`tenancy::evaluate_tenancy`] / [`tenancy::TenancyDecision`]:
  `ClusterRepository` consumer-allow-list enforcement.
- [`metrics::WebhookMetrics`] — admission outcome counters (Prometheus pull).
- [`config`] — every webhook env var name + default (listen addr, TLS paths).

## Example

The server needs TLS + a cluster, so booting it is shown `no_run`:

```rust,no_run
# fn doc() {
use kopiur_webhook::app;

// `app(None)` builds the router with no kube::Client (tenancy lookups that
// need the API server are skipped); pass `Some(client)` in production.
let router = app(None);
let _ = router; // serve it with axum + rustls in real deployments.
# }
```

Because the webhook shares the controller's validators, the actual admission
logic is pure and runs without a cluster. Here is the cron check that backs the
`SnapshotSchedule` handler:

```rust
use kopiur_api::validate::validate_cron;
use kopiur_api::ValidationError;

// Valid 5-field crons pass — including Jenkins-style `H` (jitter resolved later).
assert!(validate_cron("0 2 * * *").is_ok());
assert!(validate_cron("H 2 * * *").is_ok());

// Garbage is rejected at apply time, not at first reconcile (ADR §4.1).
assert!(matches!(
    validate_cron("not a cron"),
    Err(ValidationError::InvalidCron { .. }),
));
```

## See also

- [ADR-0003](https://github.com/perfectra1n/kopiur/blob/main/docs/adr/0003-kopiur-rust-operator.md)
  §5.1 ("one validator, two callers") and §5.3 (webhook) — the canonical design.
- [`kopiur-api`](kopiur_api) — the validators this webhook shares with the controller.
