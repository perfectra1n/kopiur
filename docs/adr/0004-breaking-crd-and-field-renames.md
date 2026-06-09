# ADR-0004 — Breaking CRD & Field Renames

- **Status:** Proposed
- **Date:** 2026-06-09
- **Deciders:** kopiur maintainers
- **Supersedes:** ADR-0003 §3.1 (cache defaults), §4.2 (identity templating)
- **Companion:** ADR-0005 (feature improvements built on this renamed surface)
- **Scope:** `kopiur.home-operations.com/v1alpha1` — breaking renames & the reshaping they require

> **Pre-release principle.** kopiur is `v1alpha1` with no compatibility guarantee. We take breaking changes in a single cut — no aliases, deprecation windows, or fallbacks. This ADR is the **reshape**: kind/field renames and the minimal restructuring they require. Net-new capabilities live in **ADR-0005**. Per the split rule, where a 0005 feature is a *prerequisite* for a rename here, the minimal form of it is included below and its further development stays in 0005.

## Context

Two reshape needs surfaced, independent of any new capability:

- **Mover config is per-recipe, repetitive, and replace-not-merge.** `securityContext`/`podSecurityContext`/`resources`/`cache` are set on every `BackupConfig`/`Restore`/`Maintenance`, are identical per repository, and drift (observed: a maintenance mover with `seccompProfile`, a sibling restore mover without). The connect/create (bootstrap) Job has *no* override path (`Repository` exposes only `maintenance.mover`), so a filesystem/NFS repo on a non-`65532`-owned directory can't bootstrap. And `mover.securityContext` *replaces* the hardened default (`build_job` does `unwrap_or_else(default_security_context)`), so a partial override silently drops `capabilities.drop:[ALL]`/`seccompProfile` — the doc comment claims "merged," but the code doesn't.
- **Names and field layout diverged from Kopia.** Three kinds already use Kopia's vocabulary (`Repository`/`Restore`/`Maintenance`); the `Backup*` kinds are generic. Reference fields (`configRef`/`backupRef`/`fromConfig`) name the old kinds. `BackupConfig` nests `compression`/`ignore`/`splitter` under an inner `policy` block (a future `policy.policy`) and carries a `policy.splitter` with no Kopia equivalent (the object splitter is repository-global). Identity templating uses a bespoke Jinja2 engine (ADR-0003 §4.2) rather than CEL.

## Decision

### A. Mover configuration restructure

#### §1 — `moverDefaults` (rename `cacheDefaults` → `moverDefaults` + inheritable mover config)

`Repository.spec.cacheDefaults` is **removed** and replaced by a `moverDefaults` block on `Repository`/`ClusterRepository`:

```yaml
spec:
  moverDefaults:
    securityContext: {...}       # container
    podSecurityContext: {...}    # pod (fsGroup, …)
    resources: {...}
    cache: {...}                 # the former cacheDefaults
    nodeSelector / tolerations / affinity: {...}
```

`moverDefaults` is the **base for every mover the repository spawns — bootstrap, backup, restore, maintenance** — overridable per-recipe via `mover`. This closes the bootstrap gap (the connect/create Job inherits it, so a filesystem repo on a non-`65532`-owned directory is bootstrappable with no special-case knob). It is the minimal feature the `cacheDefaults` rename requires to be meaningful; **further `moverDefaults` fields (throttle, upload parallelism) are ADR-0005.**

#### §2 — Layered, field-wise merge of mover security contexts

`securityContext`/`podSecurityContext` resolve by **field-wise merge**, lowest→highest: `built-in hardened default ⊂ repo.moverDefaults ⊂ recipe.mover`. Each explicitly-set field wins; unset fields inherit the layer below; the privileged-mover gate (ADR-0003 §4.11/§G16) runs on the merged result. This is **required for §1's inheritance to compose** — under replace, a recipe setting one SC field would wipe `moverDefaults`. The `unwrap_or_else(default_security_context)` path is deleted and the doc comment made accurate.

### B. Naming

#### §3 — CRD kind names follow Kopia nomenclature

kopiur already names three kinds after Kopia (`Repository`, `Restore`, `Maintenance`); the `Backup*` kinds are generic. Align them so the surface speaks one language and maps onto the `kopia` CLI:

| Current | Kopia term | New kind |
|---|---|---|
| `Repository` | repository | `Repository` *(unchanged)* |
| `ClusterRepository` | repository (+ k8s cluster scope) | `ClusterRepository` *(unchanged)* |
| `BackupConfig` | **policy** | **`SnapshotPolicy`** |
| `Backup` | **snapshot** | **`Snapshot`** |
| `BackupSchedule` | (Kopia folds schedule into policy) | **`SnapshotSchedule`** |
| `Restore` | restore | `Restore` *(unchanged)* |
| `Maintenance` | maintenance | `Maintenance` *(unchanged)* |

`kubectl get snapshots` ↔ `kopia snapshot list`, `kubectl get snapshotpolicies` ↔ `kopia policy list`.

- **`Snapshot` vs CSI `VolumeSnapshot`** — no API collision (`snapshots.kopiur.home-operations.com` vs `volumesnapshots.snapshot.storage.k8s.io`), only colloquial overlap. We own the term; `KopiaSnapshot` was rejected as uglier for no benefit.
- **`SnapshotPolicy`, not bare `Policy`** — `Policy` is overloaded in Kubernetes; `SnapshotPolicy` disambiguates.
- **Keep the schedule split** — Kopia embeds scheduling in the policy; kopiur's recipe/invocation/schedule split (ADR-0003) is kept, so `SnapshotSchedule` stays its own kind.

Trade-off: this optimizes for Kopia-fluent users over the Velero/k8up `Backup` convention — the right bet for a Kopia-native operator.

#### §4 — Field names follow the renamed kinds & Kopia's policy layout

A full `api`-crate pass confirms most fields already match Kopia and **stay unchanged** — `retention.keep*`, `cache.{content,metadata}CacheSizeMb`, the eight backends and their fields, `maintenance.schedule.{quick,full}`, `tags`/`kopiaSnapshotID`, restore `writeFilesAtomically`/`ignorePermissionErrors`, identity `username`/`hostname`/`sourcePath`/`snapshotID`, repo `encryption`/`splitter`/`hash`. Two groups change:

**(a) Reference fields follow the renamed kinds (§3).** `configRef`/`ConfigRef` → `policyRef`/`PolicyRef`; `Restore.source.backupRef` → `snapshotRef`; `Restore.source.fromConfig`/`FromConfig` → `fromPolicy`/`FromPolicy`. Status mirrors move too: `ScheduleRef.backupRef`, `ResolvedRestore.backupRef` → `snapshotRef`. *(The `configSelector → policySelector` rename rides the `policySelector` feature in ADR-0005 — that field doesn't exist yet, so it's named correctly at birth there, not renamed here.)*

**(b) `SnapshotPolicy` mirrors Kopia's flat policy sub-structure.** The inner `policy` block (which would read as `policy.policy`) is flattened to siblings of `retention`: `policy.compression → compression`, `policy.ignore → files` (with `paths → ignoreRules`, `cacheDirs → ignoreCacheDirs` — Kopia's *files* policy), `policy.extraArgs → extraArgs`. **`policy.splitter` is removed** — the object splitter is a repository property (fixed at creation, global) and already lives at `Repository.create.splitter`; a per-policy splitter has no Kopia equivalent and can't take effect.

#### §5 — Identity templating moves to CEL (the `*Expr` convention)

`ClusterRepository.identityDefaults.{hostnameTemplate, usernameTemplate}` (today Jinja2, ADR-0003 §4.2) become **`{hostnameExpr, usernameExpr}`** — CEL evaluated in-controller via [`cel-rust`](https://github.com/cel-rust/cel-rust), suffixed `*Expr` (the kromgo `valueExpr`/`colorExpr` convention):

```yaml
identityDefaults:
  hostnameExpr: "namespace"
  usernameExpr: "namespace + '-' + configName"
```

This drops the Jinja2 dependency / injection surface and gains conditionals + label access (`has(labels.team) ? labels.team : namespace`). It establishes the minimal CEL foundation: each `*Expr` documents its CEL **environment** (here `namespace`, `configName`, the policy's `labels`/`annotations`), validated at admission, with evaluation bounded by CEL's **cost budget**; CEL is sandboxed (no I/O, no arbitrary code). **Broader CEL uses — `successExpr`, `*MatchExpr` selectors, `x-kubernetes-validations` — are ADR-0005**, per the split rule: the foundation the identity rename needs is here; its further development is in 0005.

## Breaking changes (single cut)

All land together; no aliases or fallbacks.

- `cacheDefaults` removed → `moverDefaults.cache` (§1).
- `mover.securityContext`/`podSecurityContext` now **merge** over the hardened baseline instead of replacing it (§2) — can only tighten.
- Kinds renamed: `BackupConfig`/`Backup`/`BackupSchedule` → `SnapshotPolicy`/`Snapshot`/`SnapshotSchedule` (§3).
- Reference fields renamed: `configRef → policyRef`, `source.backupRef → snapshotRef`, `source.fromConfig → fromPolicy` (§4).
- `SnapshotPolicy` inner `policy` flattened: `compression`/`files`(was `ignore`: `paths→ignoreRules`, `cacheDirs→ignoreCacheDirs`)/`extraArgs` top-level; `policy.splitter` removed (§4).
- Identity templating Jinja2 → CEL: `identityDefaults.{hostnameTemplate→hostnameExpr, usernameTemplate→usernameExpr}` (§5).

## Consequences

**Positive.** One place per repository defines mover identity/hardening/resources/cache (§1); hardening is safe-by-default and can only tighten (§2); the bootstrap-mover gap closes with no bespoke field; the surface speaks Kopia end-to-end with CLI-mirroring ergonomics (§3–§4); and §5 establishes the `cel-rust`/`*Expr` foundation ADR-0005 builds on.

**Costs.** Everything here is breaking — acceptable pre-`v1`, landed in one cut. §5 adds a `cel-rust` dependency.

**Neutral.** `moverDefaults` is structurally a generalization of `cacheDefaults`; recipes that set everything inline keep working (modulo the merge in §2).

## Alternatives considered

- **Keep per-recipe mover config / replace semantics + an admission warning.** Rejected — drift, the un-fixable bootstrap mover, and silent de-hardening are the motivation; merge makes the safe outcome the default.
- **Deprecated aliases for `cacheDefaults`/the old kinds.** Rejected on the pre-release principle — break cleanly now rather than carry two spellings into `v1`.
- **Bare `Policy`; fold schedule into the policy (Kopia-internal).** Rejected — `Policy` is overloaded; the recipe/invocation/schedule split is a deliberate improvement over Kopia.
- **Keep Jinja2 identity templating.** Rejected — one sandboxed, k8s-native expression language (CEL) over a bespoke template engine.
