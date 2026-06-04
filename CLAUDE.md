# CLAUDE.md — Kopiur

Guidance for Claude Code (and humans) working in this repository.

## What this is

**Kopiur** is a Kopia-native Kubernetes backup operator written in **Rust** on
[`kube-rs`](https://github.com/kube-rs/kube). It is the implementation of
**ADR-0003** (`docs/adr/0003-kopiur-rust-operator.md`), which supersedes two
earlier Go-flavored drafts (ADR-0001 onedr0p, ADR-0002 bo0tzz). Read ADR-0003
first — it is the canonical source of truth for the CRD surface, UX, and design
decisions. ADR-0001 §3.2–§3.7 holds the authoritative _field-by-field_ CRD YAML.

The operator exposes **7 CRDs** in API group `kopiur.home-operations.com`, version `v1alpha1`:
`Repository` (ns), `ClusterRepository` (cluster), `BackupConfig`, `Backup`,
`BackupSchedule`, `Restore`, `Maintenance` (ns). It separates **recipe**
(`BackupConfig`) from **invocation** (`Backup`) from **schedule**
(`BackupSchedule`), makes the repository a first-class resource, and ties a
kopia snapshot's lifecycle to its `Backup` CR via a finalizer + `deletionPolicy`.

## The one load-bearing idea: type-safety end-to-end (ADR §5.5)

Every "exactly one of" surface in the CRDs is a Rust `enum`, so an invalid state
is unrepresentable and reconcilers `match` exhaustively. A new variant cannot
compile until every handler accounts for it. For backup software — where a
silently-unhandled case can lose user data — this is the whole reason we chose
Rust over Go. **Preserve this property in every change.** Prefer an `enum` +
exhaustive `match` over `if let`/`_ =>` catch-alls in reconcile paths.

## Workspace layout

```
crates/
  api/         CRD types, validators, identity/jitter/retention logic. NO controller deps.
  kopia/       Typed `kopia --json` models + tokio::process client.
  webhook/     axum + rustls admission webhook (imports api::validate).
  controller/  Per-CRD kube::runtime::Controller reconcilers, finalizers.
  mover/       Per-Backup/Restore Job binary (musl, distroless).
  xtask/       Codegen: `cargo xtask gen-crds|gen-rbac|gen-all` → deploy/crds, deploy/helm.
deploy/        Generated CRDs, Helm chart, example manifests.
docs/adr/      Architecture Decision Records (0003 is canonical).
docs/dev/      Developer conventions (READ docs/dev/api-conventions.md before editing crates/api).
```

The `api` ↔ `controller` split is deliberate (ADR §5.1): `kopiur-api` must stay
free of `tokio`/`kube::Client` so downstream tools can depend on the types alone.
Do not add controller-runtime dependencies to `crates/api`.

## Non-negotiable conventions (full detail in docs/dev/api-conventions.md)

1. **Discriminated unions are externally-tagged enums** (`backend: { s3: {...} }`),
   NOT `#[serde(tag = "...")]`. Internally-tagged enums break Kubernetes
   structural-schema generation (kube's rewriter panics on the differing tag
   property). External tagging keeps full type-safety AND generates valid CRDs.
2. **No `Eq` on structs that embed `k8s-openapi` types** (`LabelSelector`,
   `ResourceRequirements`, `SecurityContext`, `JobSpec`, `Condition`, …) — they
   are `PartialEq` only. Reuse these types; don't re-invent them.
3. **Sub-objects, not leaf fields**, for every credential/policy/identity/schedule
   surface, so future fields slot in without API breakage (ADR §4.11).
4. **Optionals**: `#[serde(default, skip_serializing_if = "Option::is_none")]`.
   Status always pins `resolved.*` values (identity resolved at admission, never
   re-rendered — ADR §4.2).
5. **Tests parse YAML the cluster's way**: YAML → `serde_json::Value` → typed
   (see `crates/api/src/lib.rs::testutil::from_yaml`). Never `serde_yaml::from_str`
   directly into a typed value — serde_yaml 0.9 mis-encodes externally-tagged enums.

## Locked technical decisions

| Concern             | Choice                                                                                                                                                                                                                        |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cron                | `croner` (deterministic `H`/jitter from `(scheduleUID, slot)`; wall-clock anchor)                                                                                                                                             |
| Identity templating | `tera` (Jinja2-compatible), rendered at admission, pinned to status                                                                                                                                                           |
| Webhook server      | `axum` 0.8 + `rustls`; validators shared with controller via `api::validate`                                                                                                                                                  |
| kopia invocation    | `tokio::process::Command`, JSON streamed line-by-line; long ops in mover, short idempotent ops in controller                                                                                                                  |
| CRD/schema          | `kube::CustomResource` derive + `schemars` 1; CRDs generated by `xtask`, checked into `deploy/crds/`                                                                                                                          |
| Observability       | `kopiur-telemetry` crate: instrument once on the OTel API → Prometheus pull (`/metrics`) + optional OTLP push; all metrics `kopiur_*`; OTLP env-gated/off by default; one span per reconcile. See `docs/dev/observability.md` |
| API version         | `v1alpha1` only; no conversion webhooks yet (ADR §8)                                                                                                                                                                          |
| Retention           | GFS-only (`BackupConfig.spec.retention`); failures bounded by flat `failedJobsHistoryLimit`; NO `successfulJobsHistoryLimit`                                                                                                  |
| Deletion            | `Backup` CR owns its snapshot via finalizer; `deletionPolicy: Delete`(default produced) / `Retain`(forced for discovered) / `Orphan`                                                                                          |
| Maintenance         | Default-managed: `Repository`/`ClusterRepository` `spec.maintenance` (default-on) is projected into an *owned* `Maintenance` CR; an externally-authored `Maintenance` is always honored (never duplicated), even with `enabled: false`. ClusterRepo placement: `spec.maintenance.namespace` else `KOPIUR_NAMESPACE`. ADR §3.7 |

Pinned dep triad (Rust 1.95): `kube` 3.1, `k8s-openapi` 0.27 (feature `v1_33`,
`schemars` on), `schemars` 1.2, `axum` 0.8, `croner` 2, `tera` 1.

## Build / test / verify

Tasks live in `.mise/config.toml`; `mise tasks` lists them, `mise run <task>` runs
one. Mise is mandatory and pins the Rust toolchain plus required components.

```bash
mise run build
mise run test                  # hermetic: unit/serde/validation. No cluster, no network.
mise run clippy
mise run fmt-check
mise run gen                   # regenerate deploy/crds + RBAC (M3+)
mise run gen-check             # CI drift guard: fails if checked-in artifacts are stale

# Cluster-dependent integration tests are #[ignore] + feature-gated, ephemeral kind only:
mise run test-int              # wraps scripts/with-kind.sh
```

Integration tests must **never** target the user's real clusters. Use
`scripts/with-kind.sh`, which isolates kubeconfig and tears the cluster down.

## Working style here

- This is a phased build (see the plan and `docs/adr/0003`). Land one milestone,
  verify it (`cargo test` + `clippy` green), then start the next. Don't claim a
  milestone done without showing the passing test output.
- When a milestone is large and well-specified, it's fine to delegate to a
  subagent — but always independently re-run `cargo test`/`clippy` before
  trusting the result.
- Commit/push only when the user asks. Work happens on a feature branch.
