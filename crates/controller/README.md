# kopiur-controller

The kube-rs operator runtime for Kopiur — one [`kube::runtime::Controller`] per
top-level CRD, each with its owned-resource watches, a kind-aware `error_policy`,
and long-running kopia work delegated to mover `Job`s.

## Role in the workspace

`kopiur-controller` is the reconcile half of the operator (ADR-0003 §5.2). It
runs **seven** controllers concurrently — one per top-level CRD in API group
`kopiur.home-operations.com/v1alpha1`:

- [`repository`] / [`cluster_repository`] — validate and project a repository;
  default-manage an owned [`Maintenance`](kopiur_api::Maintenance) CR.
- [`snapshot_policy`] — the backup _recipe_; enforces GFS retention by watching
  the `Snapshot`s it parents.
- [`snapshot_schedule`] — turns a cron + jitter into `Snapshot` _invocations_ it owns.
- [`snapshot`] — one invocation; owns its mover `Job` + `ConfigMap` and ties the
  kopia snapshot's lifecycle to the CR via a finalizer + `deletionPolicy`.
- [`restore`] — drives a restore mover and the PVC populator handshake.
- [`maintenance`] — runs scheduled kopia maintenance.

Two design properties are load-bearing:

- **Long kopia ops run in mover `Job`s, not in-process** (ADR §5.4). The
  controller only makes short, idempotent kopia calls; mover `Job`s carry the
  snapshot/restore subprocesses so a controller restart never strands a kopia
  process. The pure `Job`/`ConfigMap` builder lives in [`jobs`].
- **Shared informer over per-reconcile `Api::list`.** Cross-resource membership
  reads (e.g. "is a `Maintenance` configured for this repository?") come from a
  single reflector-backed [`reflector::Store`](kube::runtime::reflector::Store)
  the repo reconcilers share, never an `Api::list` per reconcile. See [`run`]
  for how the informer is driven.

The reconcilers keep their **decision logic pure** so the type-safety thesis is
unit-tested without a cluster; the kube IO ([`io`]) is thin and exercised by the
feature-gated integration tests.

## Key modules and types

- [`context::Context`] — shared reconcile context: client, kopia client factory,
  metrics, event recorder, the shared `Maintenance` informer.
- [`error::Error`] — the controller error enum, classified by [`error::error_policy_for`]
  into transient (short requeue) vs structural (long requeue) outcomes.
- [`metrics::Metrics`] — OTel instruments (Prometheus pull + optional OTLP push,
  via `kopiur-telemetry`).
- [`config`] — every controller env var name + fixed config value in one place.
- [`jobs`] — the pure mover `Job`/`ConfigMap` builder.
- Pure decision helpers, unit-tested cluster-free:
  [`snapshot::plan_deletion`], [`snapshot_schedule::next_fire`],
  [`snapshot_schedule::concurrency_allows`], [`snapshot_policy::backups_to_delete`].

## Example

Reconcilers need a live [`kube::Client`], so the entrypoint is shown `no_run`:

```rust,no_run
# async fn doc() -> anyhow::Result<()> {
// Builds every controller plus the /metrics + /healthz + /readyz server and
// runs them until shutdown.
kopiur_controller::run().await?;
# Ok(())
# }
```

The pure decision helpers run without a cluster. Here is the concurrency gate a
`SnapshotSchedule` uses to decide whether a new run may start:

```rust
use kopiur_api::ConcurrencyPolicy;
use kopiur_controller::snapshot_schedule::concurrency_allows;

// `Forbid` blocks a new run while one is already active...
assert!(!concurrency_allows(ConcurrencyPolicy::Forbid, true));
assert!(concurrency_allows(ConcurrencyPolicy::Forbid, false));

// ...while `Allow` and `Replace` always let the slot proceed.
assert!(concurrency_allows(ConcurrencyPolicy::Allow, true));
assert!(concurrency_allows(ConcurrencyPolicy::Replace, true));
```

## See also

- [ADR-0003](https://github.com/perfectra1n/kopiur/blob/main/docs/adr/0003-kopiur-rust-operator.md)
  §5.2 (controller runtime) and §5.4 (kopia interaction) — the canonical design.
- [`kopiur-api`](kopiur_api) — the CRD types and validators the controller reconciles.
