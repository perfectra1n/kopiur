---
name: kopiur-design
description: Design norms and locked decisions for the Kopiur Kopia-native Kubernetes backup operator (Rust/kube-rs). Use when adding or modifying CRD types, reconcilers, the admission webhook, the kopia client/mover, validators, or codegen in this repo — anything under crates/ or deploy/. Encodes the type-safety thesis, the externally-tagged-enum rule, k8s-openapi Eq constraints, the shared-validator pattern, retention/deletion semantics, and the build/test discipline so changes stay consistent with ADR-0003.
---

# Kopiur design norms & decisions

Kopiur implements **ADR-0003** (`docs/adr/0003-kopiur-rust-operator.md`): a
Kopia-native Kubernetes backup operator in Rust on `kube-rs`. Read ADR-0003 for
intent; ADR-0001 §3.2–§3.7 for the authoritative CRD field surface; `CLAUDE.md`
for the quick map; `docs/dev/api-conventions.md` for the encoding rulebook.

## The thesis you must protect (ADR §5.5)

Invalid states are unrepresentable; reconcilers handle every variant. Concretely:

- Model every "exactly one of" surface as a Rust `enum`, never a bag of mutually
  exclusive `Option` fields + a runtime check.
- In reconcile/finalizer paths, use exhaustive `match` — avoid `_ =>` catch-alls
  and `if let ... else { /* ignore */ }`. A new enum variant should fail to
  compile until handled. This is the entire reason the project is Rust, not Go.

## Hard rules (violating these breaks the build or the CRDs)

1. **Externally-tagged enums for discriminated unions.** `backend: { s3: {...} }`,
   not `backend: { kind: S3, ... }`. Do NOT use `#[serde(tag = "...")]`:
   internally-tagged enums make kube's structural-schema rewriter panic
   (`property "kind" ... must be identical`) because each variant needs a
   distinct tag const. External tagging keeps the type-safety guarantee AND
   produces valid structural schemas. Provide a `kind_str()` helper for
   status/metrics/print-columns. Applies to `Backend`, `AllowedNamespaces`,
   `RestoreSource`, `RestoreTarget`, `Hook`, etc.
2. **No `Eq` when embedding `k8s-openapi` types.** `LabelSelector`,
   `ResourceRequirements`, `SecurityContext`, `JobSpec`, `Condition`, `PodSpec`
   are `PartialEq` but not `Eq`. Derive `PartialEq` only on any struct that
   contains one (directly or transitively). Reuse these types — never re-declare
   them. The `schemars` feature is on for k8s-openapi so they derive `JsonSchema`.
3. **`crates/api` has no controller-runtime deps.** No `tokio`, no
   `kube::Client`. It's the shared types + pure logic crate (ADR §5.1). The
   webhook and controller both import its validators so validation is identical.
4. **One validator, two callers.** Cross-field rules live in `api::validate` as
   pure functions returning a typed error; the webhook calls them at admission
   and the controller calls them defensively. Never fork validation logic.
5. **Tests use the cluster's parse path.** `from_yaml` = YAML → `serde_json::Value`
   → typed (`crates/api/src/lib.rs::testutil`). Direct `serde_yaml::from_str::<T>`
   mis-encodes externally-tagged enums (serde_yaml 0.9 `!Variant` syntax) and is
   not representative of the real wire format.

## Semantic decisions to honor

- **Retention is GFS-only.** `BackupConfig.spec.retention` is the sole successful-
  retention driver (operator prunes `Backup` CRs). Failures use a flat
  `BackupSchedule.spec.failedJobsHistoryLimit`. There is deliberately **no**
  `successfulJobsHistoryLimit`. (ADR §4.4 — resolves the onedr0p/bo0tzz split.)
- **Snapshot lifecycle = CR lifecycle.** Every `Backup` carries the
  `kopiur.home-operations.com/snapshot-cleanup` finalizer. `deletionPolicy`:
  `Delete` (default for produced) / `Retain` (FORCED for `origin: discovered`,
  webhook-rejected otherwise) / `Orphan`. Match all three exhaustively; the
  `kopiur.home-operations.com/skip-snapshot-cleanup` annotation is the repo-offline escape hatch.
- **Identity** defaults to `username=name`, `hostname=namespace`,
  `sourcePath=/pvc/<name>`; `ClusterRepository.identityDefaults` templates render
  via `tera` at admission and are pinned to `status.resolved.identity` — never
  re-rendered after admission.
- **Scheduling**: wall-clock anchor (`cron(now)`), deterministic jitter seeded by
  `(scheduleUID, slot_start)` (no RNG — must be identical across HA replicas and
  restarts). `runOnCreate: false` and `concurrencyPolicy: Forbid` are defaults.
- **Restores fail closed** (`onMissingSnapshot: Fail`) except `source.fromConfig`,
  which defaults to `Continue` for the GitOps deploy-or-restore pattern.

## Build & verify discipline

```bash
cargo test --workspace                 # hermetic; must be green before claiming done
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask gen-all --check            # generated CRDs/RBAC must match checked-in
scripts/with-kind.sh cargo test --workspace --features integration -- --include-ignored
```

- Integration tests are `#[ignore]` + `--features integration`, run only on an
  ephemeral `kind` cluster via `scripts/with-kind.sh`. **Never** point them at a
  real/homelab cluster.
- Land one milestone, prove it green, then move on. Show the passing output —
  don't assert completion without evidence (a `T::crd()` call doubles as the
  schema-generation smoke test; it panics if an enum is mis-encoded).
- Delegating a milestone to a subagent is encouraged when it's well-specified,
  but always re-run `cargo test`/`clippy` yourself before trusting the result.

## Every change ships with tests (non-negotiable)

This is **data-protection software**: an untested path can silently lose backups.
So **every new feature AND every bug fix ships with tests** — you have NOT
finished until the work is proven by tests that would fail without it.

### New features: cover every tier the feature touches

A new feature (a CRD field, a backend, a reconciler path, a mover op) is not done
until it has, **at minimum**:

1. **Unit/serde tests** for the leaf logic — validators, the externally-tagged
   wire shape (`from_yaml` round-trip), pure decision functions, and any
   `match`-on-enum mapping. Cheapest tier; catches the most. Also test the
   controller _glue_ that maps a CRD field to a mover input (e.g. the function
   that turns `source.nfs` into a mover volume mount) — extract it so it's
   callable without a cluster, then assert on its output.
2. **An e2e scenario that exercises the feature end to end** against a live
   operator and asserts the user-visible success condition (a `Backup` reaching
   `Succeeded` with a real `kopiaSnapshotID`, a `Repository` `Ready`, a `Restore`
   `Completed`). If the feature needs a new piece of cluster infrastructure to
   test (an NFS server, an SFTP server, a new backend), **stand it up in the e2e
   `World`** (`Need`/`Fixture`) — see `[[error-handling-and-e2e]]`.
3. Integration tier (`#[ignore]` + `--features integration`) when the surface is
   an API-server interaction (admission, RBAC) that doesn't need the mover images.

**No e2e test is "too large" or "too expensive" to include.** This tool guards
real data; thorough end-to-end proof is the requirement, not a nice-to-have. If a
feature _can_ be exercised against a live operator, it gets an e2e scenario — even
when that means provisioning a server, building an image, or a multi-minute run.
The only acceptable reason to skip an e2e is that the feature genuinely has no
runtime-observable behavior (a pure type/refactor) — and then say so explicitly.

### Bug fixes: a regression test that fails without the fix

When you fix a bug — especially one a user hit at runtime — you have NOT finished
until a test would fail without your fix and passes with it. A fix without a test
is an invitation for the same bug to return. Default to writing the test _first_
(reproduce the failure), then fix.

Pick the cheapest tier that actually exercises the broken path:

1. **Hermetic unit test (preferred).** If the bug lives in a decision, extract that
   decision into a pure function and unit-test it — the codebase's "thin IO over a
   tested pure fn" idiom (ADR §5.2/§5.4). Example: the "ClusterRepository refs are
   ignored" bug (controller resolved every `repository` ref as a namespaced
   `Repository` regardless of `kind`) became `io::repo_lookup(&RepositoryRef, …) ->
RepoLookup` with unit tests asserting `kind: ClusterRepository` maps to a
   cluster-scoped lookup, never a namespaced get. Runs in `cargo test`, no cluster.
2. **e2e test for whole-pipeline bugs.** If the failure only shows up against a live
   operator (a reconcile that never reaches `Succeeded`, a missing dependency, an
   RBAC/SA gap, a dropped option), add a scenario to `crates/e2e/tests/lifecycle.rs`
   that reproduces the _exact_ user-visible symptom and asserts the success
   condition (e.g. Backup reaching `Succeeded` with a real `kopiaSnapshotID`). Write
   the test so it would have _timed out / failed_ on the buggy code. See
   `cluster_repository_backup_lifecycle` for the template.
3. Integration tier (`#[ignore]` + `--features integration`) for API-server
   interactions that don't need the mover images.

Then record the bug + its guard in the `operator-bugs-fixed-by-e2e` auto-memory so
the class of failure stays visible across sessions.
