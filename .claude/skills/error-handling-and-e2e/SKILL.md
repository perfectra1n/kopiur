---
name: error-handling-and-e2e
description: How Kopiur does strongly-typed, actionable error handling and end-to-end testing. Use when adding or changing any error type, error path, or fallible operation (kopia calls, kube IO, OTLP/telemetry init, validators, the mover), or when writing/extending tests — especially e2e scenarios in crates/e2e. Encodes the thiserror exhaustive-enum + classification pattern, the what/why/fix message rule, degrade-not-crash for non-critical subsystems, and the crates/e2e harness conventions (feature = "e2e", mise //crates/e2e:test, World::ensure(Need) provisioning, wait_phase/wait_until, assert real operator output, never a real cluster).
---

# Errors & e2e testing in Kopiur

Two linked disciplines. Strong typed errors make invalid states unrepresentable
and tell the operator _exactly_ how to recover; e2e tests prove the whole
pipeline reaches the user-visible success condition. Both exist because this is
**data-protection software** — a silently-swallowed error or an untested path can
lose backups.

Read alongside `[[kopiur-design]]` (the type-safety thesis) and `CLAUDE.md`.

## Part 1 — Strongly-typed, actionable errors

### The pattern (mirror `crates/controller/src/error.rs`)

1. **One `thiserror` enum per crate/domain**, `#[derive(Debug, thiserror::Error)]`.
   One variant per distinct failure mode. Use `#[from]` to wrap upstream errors
   (`kube::Error`, `KopiaError`, `serde_json::Error`) so `?` just works.
2. **Exhaustive classification, not catch-alls.** If the error drives behavior
   (requeue timing, fatal-vs-degrade), express that as a method with an
   exhaustive `match` and **no `_ =>` arm** — a new variant must fail to compile
   until it is classified. See `Error::class() -> ErrorClass`
   (Transient/Structural → backoff). A `_` arm here is a latent data-loss bug.
3. **`pub type Result<T, E = Error>`** alias per crate; reconcile/fallible fns
   return it.
4. **Test the classification AND the message.** Unit-test that each variant maps
   to the right class/behavior (`error.rs` tests) and — for any message a human
   acts on — assert on `to_string()` so the actionable text can't silently rot.

### The message rule: what failed, why, how to fix

Every `#[error("…")]` an operator might read states **what** failed, the likely
**why**, and the concrete **fix** (expected value, env var, command). This is the
type-safety thesis extended to UX: homelabbers run this; a cryptic error wastes
time and can mask a data-protection gap.

```rust
#[error(
    "OTEL_EXPORTER_OTLP_ENDPOINT='{value}' is not a valid URL. Use scheme+host+port, \
     e.g. http://otel-collector:4317 (OTLP/gRPC) or :4318 (OTLP/HTTP); unset it to disable OTLP"
)]
InvalidOtlpEndpoint { value: String, #[source] source: url::ParseError },
```

Prefer `{named}` fields over positional `{0}` when the message interpolates user
input — it reads better and survives reordering. Keep the underlying cause via
`#[source]`/`#[from]` so the chain is inspectable; don't bury it inside a string.

### Degrade, don't crash, for non-critical subsystems

Reconciliation and data movement are critical; observability, OTLP export, and
similar side-channels are not. A misconfigured non-critical subsystem must **log
the actionable error and continue**, never abort the operator:

- `init_telemetry()` returns `Result`; on error the binary logs it at `error`
  and falls back (fmt-only logs + the always-on Prometheus pull). An optional
  `KOPIUR_OTEL_STRICT=true` makes it fail-fast for those who want it.
- Critical paths still fail loudly: a bad CRD spec is `Structural` (long requeue,
  surfaced on `.status` + an Event), a kopia/API outage is `Transient` (short
  requeue). Both go through `error_policy_for`, which records
  `controller_reconcile_errors_total{kind,class}` and requeues by class.

### Surfacing errors to users

Reconcile errors should reach the user, not just the log: set a `Condition` /
phase on `.status` and emit a kube `Event` via the `Recorder` (Context already
carries one). The metric + log + status + Event together make a failure visible
in `kubectl get`, dashboards, and alerts.

## Part 2 — e2e testing (`crates/e2e`)

### When to write an e2e test

Per `[[regression-test-every-bugfix]]`, every fix ships with a test at the
cheapest tier that exercises the broken path. Reach for e2e (not a unit test)
when the failure **only appears against a live operator**: a reconcile that never
reaches its terminal phase, a missing runtime dependency, an RBAC/SA gap, a
dropped option that survives serialization but breaks at runtime, or — for the
observability work — metrics that must actually be emitted and scrapable.

### Harness conventions (mirror `crates/e2e/tests/lifecycle.rs`)

- **Gate:** `#![cfg(all(unix, feature = "e2e"))]` at the top of the test file,
  and `#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]` on
  each `#[tokio::test]`. So the suite compiles everywhere and is skipped without a
  cluster; it only runs under the harness.
- **Run it:** `mise run //crates/e2e:test`. The host-level steps are mise tasks
  in `crates/e2e/mise.toml` (a monorepo subproject): build + load the images into
  kind, seed the node's hostPath dirs, and `helm upgrade --install` with
  `deploy/e2e/values.yaml` (webhook disabled — covered by unit/integration tiers).
- **Declare cluster prerequisites as data.** Each scenario opens with
  `let Some(world) = World::connect().await else { return; };` then
  `world.ensure(&[Need::Filesystem /* | Need::Minio | Need::WorkloadNs */]).await?`.
  `World` (crates/e2e/src/world.rs) provisions namespaces/Secrets/PV-PVCs/MinIO/
  buckets idempotently via the type-safe `Fixture` apply dispatch — never via
  `kubectl` in a shell. Add a new fixture kind by extending the `Need`/`Fixture`
  enums (exhaustive `match`).
- **NEVER target a real cluster.** `scripts/with-kind.sh` (integration tier) and
  the `//crates/e2e:*` tasks pin an isolated kubeconfig under `target/e2e/` and
  tear the cluster down; they never touch the homelab kubecontext. Do not add a
  test that reads the ambient `KUBECONFIG`.
- **Helpers live in `kopiur_e2e`** (`crates/e2e/src/lib.rs`): `E2E_NAMESPACE`,
  `try_client()` (returns `None` when no cluster → skip gracefully),
  `wait_until(...)`, `default_timeout()`, `poll_interval()`. Reuse them; don't
  re-roll polling.

### Assert real operator output, and make the test fail on the bug

Assert the **user-visible success condition**, not an intermediate detail:

- A `Repository` reaching `Ready`; a `Backup` reaching `Succeeded` **with a real
  `kopiaSnapshotID`**; a `Restore` `Completed`; a schedule actually creating a
  `Backup`; a finalizer deleting the snapshot; a `Maintenance` lease claimed.
- Use the `wait_phase(&api, name, "Succeeded")` pattern (poll status to a target
  phase with a timeout). A regression test must be written so it **times out /
  fails on the buggy code** and passes on the fix — see
  `cluster_repository_backup_lifecycle` (added for the "ClusterRepository refs
  ignored" bug: it would hang at `wait_phase(... "Succeeded")` before the fix).

### Validating metrics in e2e (observability)

When the assertion is "a metric is emitted," drive the real lifecycle, then
**scrape `/metrics` and parse the exposition** rather than trusting a code path:

- curl the controller `:8080/metrics` (and webhook `/metrics`); assert the
  expected families/labels are present with sane values
  (`controller_reconciliations_total{kind=…} > 0`,
  `kopiur_resource_phase{kind=Backup,phase=Succeeded} == 1`, backup
  size/files/duration `> 0`, error/consecutive-failure counters reflect an
  induced failure).
- Assert the body still **parses as valid Prometheus text** — a regression guard
  for the OTel→Prometheus name rewrite (`_total` suffixes, `otel_scope_*`,
  `target_info`).
- Also assert `/healthz` + `/readyz` return 200 (real endpoints, not the old
  any-path listener).

## Checklist before claiming an error/test change done

- [ ] New error variants classified in an exhaustive `match` (no `_ =>`).
- [ ] User-facing messages say what/why/fix; `#[source]` preserved.
- [ ] Non-critical subsystem degrades-and-logs; critical path fails loud +
      surfaces on status/Event/metric.
- [ ] Message text unit-tested where a human acts on it.
- [ ] Bug fix has a test that fails without the fix (unit if possible, e2e if the
      bug is pipeline-only).
- [ ] e2e gated `feature = "e2e"` + `#[ignore]`, uses `kopiur_e2e` helpers,
      asserts the user-visible success condition, never a real cluster.
- [ ] `cargo test --workspace` + `clippy -D warnings` + `fmt --check` green;
      record the bug + guard in the `operator-bugs-fixed-by-e2e` memory.
