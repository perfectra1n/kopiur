# ADR-0005 — CRD Feature Improvements

- **Status:** Proposed
- **Date:** 2026-06-09
- **Deciders:** kopiur maintainers
- **Builds on:** ADR-0004 (renamed surface — `SnapshotPolicy`/`Snapshot`/`SnapshotSchedule`, `policyRef`, `moverDefaults`, the `cel-rust`/`*Expr` foundation)
- **Scope:** `kopiur.home-operations.com/v1alpha1` — additive capabilities (a few carry a breaking default change)

> **Pre-release principle (shared with ADR-0004).** No aliases or deprecation windows; the breaking default changes here land in the **same single cut** as ADR-0004. Everything below uses ADR-0004's renamed surface. Per the split rule, where ADR-0004 carries the *minimal* form of a shared mechanism — the `moverDefaults` base (0004 §1) and the `cel-rust`/`*Expr` foundation (0004 §5) — this ADR carries its **further development**.

## Context

ADR-0004 reshapes the existing surface; this ADR adds capability. The core already matches Kopia, but several higher-value things are missing: **data-integrity guarantees** a backup tool owes its users (is a snapshot restorable? does `kubectl delete ns` destroy history?), **GitOps fidelity** (does the resource go green in Flux/Argo, validate in CI, stay drift-free?), and Kopia's **power-feature tail** (ECC, replication, throttling, …). These were observed running kopiur across a real multi-namespace GitOps cluster.

## Decision

### A. Defaulting, status & GitOps fidelity

#### §1 — Static OpenAPI defaults for unconditional fields

Fields whose default is independent of any other carry a real OpenAPI `default:` (`#[serde(default)]` + schemars): `copyMethod=Snapshot`, `source.fromPolicy.offset=0`, `schedule.runOnCreate=false`. They then appear in `kubectl explain` and the stored object, and GitOps engines stop diff-thrashing on a controller-materialized field. **Conditional** defaults stay controller/webhook-resolved and pinned to status: `policy.onMissingSnapshot` (depends on source kind), identity (ADR-0003 §4.2).

#### §2 — kstatus-compliant status + `observedGeneration`

Every reconciled CRD exposes standard `metav1.Condition`s (`Ready`, plus `Reconciling`/`Stalled` where useful) and `status.observedGeneration`. This makes Flux `wait`/`healthChecks`, Argo CD health, and `kubectl wait --for=condition=Ready` work natively against `Repository`/`SnapshotPolicy`/`Restore`/`Maintenance` — and lets dependents gate cleanly (e.g. a `SnapshotPolicy`'s namespace won't reconcile before its `Repository` is `Ready`).

#### §3 — Printer columns + surfaced timestamps

`additionalPrinterColumns` + the backing status fields: `SnapshotPolicy` → `REPOSITORY`, `LAST-SNAPSHOT` (`status.lastSuccessfulSnapshot`), `AGE`; `SnapshotSchedule` → `SCHEDULE`, `NEXT` (`status.nextRun`), `LAST`, `SUSPENDED`; `Restore` → `PHASE`, `SOURCE`, `AGE`. `lastSuccessfulSnapshot` also backs the `prometheusRule` staleness alert.

### B. Data-integrity guarantees

#### §4 — First-class backup verification

`Maintenance` covers GC/compaction but not *restorability*. Add verification as an opt-in capability: periodic `kopia snapshot verify` (blob-level) and/or a scratch-restore test (restore the latest snapshot to an ephemeral PVC, checksum, discard). Surface `status.lastVerified` and a `kopiur_snapshot_verified_timestamp` metric. Schedulable like maintenance (quick verify often, deep restore-test rarely).

#### §5 — Namespace-deletion cascade policy

Deleting a namespace must not silently destroy off-site history or hang on N `kopia snapshot delete` calls. Repository-level `onNamespaceDelete: Orphan|Delete` (and/or a workload-namespace annotation), **default `Orphan`** (fail-safe): finalizers release ownership without deleting snapshots unless explicitly opted into `Delete`.

#### §6 — Identity-collision detection at admission

The webhook pins identity at admission (ADR-0003 §4.2); extend it to **reject** a `SnapshotPolicy` whose resolved `username@hostname:path` collides with an existing policy's identity in the same repository, naming the conflict. Prevents two recipes interleaving snapshots into one kopia identity.

#### §7 — Enforce create-time immutability

`encryption`/`splitter`/`hash` (fixed at repository creation) and pinned identity become **webhook-immutable**: edits are rejected with an actionable message ("immutable after creation; create a new Repository") rather than silently ignored.

### C. Multi-tenancy authorization

#### §8 — Repository-gated credential projection

Today projection is consumer-gated only (`credentialProjection.enabled` + the operator's cluster-wide `secrets` RBAC), so any tenant in a `ClusterRepository`'s `allowedNamespaces` can copy the shared repo password into its own namespace. Add `credentialProjection.allowed` (default **`false`**) to `ClusterRepository`: projection requires repository-owner allow **and** consumer opt-in **and** operator RBAC — fail-closed. Namespaced `Repository` is exempt (repo + secret co-reside; ADR-0003 §4.10).

### D. Lifecycle & ergonomics

#### §9 — Explicit Restore populator mode

Populator mode is `spec.target.populator: {}`. The empty-`target` form is **removed**: the webhook rejects a `Restore` with neither `target.pvc`/`target.pvcRef` nor `target.populator`, and rejects `inheritSecurityContextFrom` in populator mode (no workload pod exists at provision time; ADR-0003 §4.7), pointing at `moverDefaults` (ADR-0004 §1) / explicit `securityContext`.

#### §10 — `policySelector` on `SnapshotSchedule`

Allow a `SnapshotSchedule` to target many recipes via `spec.policySelector` (a label selector over `SnapshotPolicy` objects), mutually exclusive with `policyRef`. Mirrors the `pvcSelector` pattern; "back up everything tagged `tier=critical` nightly" becomes one object. (Named `policySelector` at birth, following ADR-0004 §3–§4; if it ever grows an expression form it takes the `*MatchExpr` shape of §15.)

#### §11 — Repository `ReadOnly` mode

`Repository.spec.mode: ReadWrite|ReadOnly` (default `ReadWrite`) — maps to Kopia's read-only repository connection. A `ReadOnly` repo serves restores only (no backups, no maintenance), for decommissioning a backend or migrating between repositories without risking writes.

#### §12 — Mover Job TTL

Mover Jobs set `ttlSecondsAfterFinished` (sane default, overridable via `moverDefaults`, ADR-0004 §1) so finished backup/restore/maintenance Jobs and their pods self-GC.

### E. Kopia feature parity

#### §13 — Durability, DR & throughput capabilities

Surface Kopia capabilities not yet exposed — additive (new optional fields, or one new CRD):

- **(a) ECC.** `kopia repository create --ecc=REED-SOLOMON-CRC32 --ecc-overhead-percent=N` — Reed-Solomon parity guarding repo blobs against backend bit-rot. `Repository.create.ecc{algorithm, overheadPercent}`, create-time-fixed (immutable under §7).
- **(b) Backup-side error handling.** Kopia's error-handling policy (`--ignore-file-errors`, `--ignore-dir-errors`, `--ignore-unknown-types`) lets a snapshot complete-with-errors. `SnapshotPolicy.spec.errorHandling.*` — the missing **backup-side analog** of restore's `ignorePermissionErrors`.
- **(c) Snapshot pinning.** `kopia snapshot pin` exempts a snapshot from expiry. `Snapshot.spec.pin` (or `retain: Forever`); GFS retention skips pinned snapshots — for pre-migration / compliance holds.
- **(d) Repository replication.** `kopia repository sync-to` mirrors blobs to a second backend (the "2" in 3-2-1). A small `RepositoryReplication` (sourceRef + destination backend + schedule) — the one net-new CRD and the heaviest lift.
- **(e) Throttling.** `kopia repository throttle` caps upload/download bytes-per-sec and ops-per-sec. **Extends `moverDefaults` (ADR-0004 §1)** so a run doesn't saturate the NFS link or hammer an object store.
- **(f) Upload parallelism.** Kopia's upload policy (`--parallel`, max-parallel-file-reads). On the flattened `SnapshotPolicy` (`upload.{maxParallelSnapshots, maxParallelFileReads}`) and/or `moverDefaults`.

### F. GitOps ergonomics (Flux / Argo)

#### §14 — First-class GitOps citizens

§1–§3 cover defaults/health/columns; the rest is packaging, drift hygiene, and validation:

- **(a) Publish CRD JSON schemas** (home-operations schema server and/or `datreeio/CRDs-catalog`) so `flux build | kubeconform` validates kopiur CRs in CI without a cluster, and `# yaml-language-server: $schema=` editor hints resolve. *(Observed: those schema URLs 404 today.)*
- **(b) Standalone CRD artifact** (CRD-only OCI artifact / kustomize component, separate from the operator release) so consumers apply CRDs in an early Argo **sync-wave** / Flux **`dependsOn`** wave, removing the "CR before its CRD" race.
- **(c) Label + own every operator-created object** — mover Jobs, the minted `kopiur-mover` SA/RoleBinding, the cache PVC, CSI `VolumeSnapshot`s, and the **projected credential Secret (§8)** — with `app.kubernetes.io/managed-by: kopiur` + ownerReferences, so Argo/Flux don't report them `OutOfSync` or prune them. Ship the Argo `resource.exclusions`/Flux snippet.
- **(d) Invariant — status-only writes.** The controller writes only `.status`, never user `spec` (identity is pinned to status, ADR-0003 §4.2). A write-back into spec makes Argo/Flux perpetually `OutOfSync`; new fields default via OpenAPI (§1), never via spec mutation.
- **(e) Consistent `suspend`** across `Repository`/`SnapshotPolicy` (today only `SnapshotSchedule`) — pause via one declarative field, surfaced in a §3 column.
- **(f) Webhook scope & failure mode** — keep it scoped to kopiur kinds so a `failurePolicy: Fail` outage never wedges unrelated GitOps applies; CRD apply bypasses the webhook (so (b) still bootstraps during operator downtime).
- **(g) Ship health rules** — an Argo health-check Lua customization and a Flux `healthCheckExprs` (CEL) example for `Repository`/`SnapshotPolicy`/`Restore`.
- **(h) Document the populator cutover** — a bound PVC's `dataSourceRef` is immutable, so migrating an existing app onto the populator (§9) is a snapshot-gated delete-and-repopulate, not a silent `git push`.

Highest-leverage near-term: **(a)** schemas, **(b)** the standalone CRD artifact, **(c)** managed-by/ownerRefs.

### G. Expressions (CEL) — further uses

#### §15 — Extend the `*Expr` convention beyond identity

Building on the `cel-rust`/`*Expr` foundation established in **ADR-0004 §5** (sandboxed, typed, per-field environment + cost budget), apply the same convention where expressions add value — none decided here, all reusing the convention:

- **`successExpr` on verification (§4)** — a pass/fail predicate over the verify/restore result (e.g. `stats.files > 0`, killing the silent "0 files" success).
- **`*MatchExpr` selectors** — `pvcMatchExpr`, `namespaceMatchExpr` (the `allowedNamespaces` selector), `policyMatchExpr` (§10) — richer than label selectors.
- **`pinExpr` / `whenExpr` / `tagsExpr`** — conditional pinning, hook gating, computed tags (speculative; ship on demand).
- **`x-kubernetes-validations`** — operator-authored CEL in the CRD *schema* for cross-field invariants (exactly-one-of `{pvc,pvcSelector,nfs}`, `target.populator` XOR `target.pvc`) and §7 immutability via transition rules (`self.encryption == oldSelf.encryption`), validating in the apiserver and CI — which also shrinks the validating webhook and tightens §14's PR gate.

## Breaking changes (same single cut as ADR-0004)

A handful of these features change defaults; they ride ADR-0004's cut:

- **`credentialProjection` is off until the repository owner opts in** (§8) — `allowed` defaults `false`.
- **Namespace deletion no longer cascade-deletes snapshots by default** (§5) — `Orphan`; opt into `Delete`.
- **A `Restore` with no `target` is invalid** — populator intent must be `target.populator: {}` (§9).
- **`copyMethod`/`offset`/`runOnCreate` are materialized into stored specs** (§1) — a one-time, semantically-noop change.

## Consequences

**Positive.** Backups are provably restorable (§4); `kubectl delete ns` can't quietly nuke history (§5); GitOps tools go green natively (§2) and stay drift-free (§14); shared-credential owners control projection (§8); the repo gains Kopia's durability/DR tail (§13).

**Costs.** §4 (verification) and §13(d) (replication, a new CRD) are the heaviest net-new surface. Four breaking default changes (above). Several webhook validations (§6/§7/§9) and `x-kubernetes-validations` rules (§15) to maintain.

**Neutral.** The remainder is additive optional fields; existing recipes keep working.

## Alternatives considered

- **Verification via `Maintenance` only.** Rejected — kopia maintenance is GC/compaction, not a restorability check (§4).
- **Cascade-delete on namespace deletion (status quo default).** Rejected — a backup tool must not make `kubectl delete ns` a data-loss event; opt-in only (§5).
- **Lean entirely on External Secrets / a credential-sync CRD for projection.** Rejected — a repository-side `allowed` gate is sufficient (§8).
- **Apply CEL reflexively across the surface.** Rejected — the convention is established (ADR-0004 §5) and extended only where it earns its keep (§15); `pinExpr`/`whenExpr`/`tagsExpr` are explicitly deferred.
