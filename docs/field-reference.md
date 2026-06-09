# Field reference

Every spec and status field of all eight CRDs in
`kopiur.home-operations.com/v1alpha1`, with its type, default, whether it is
immutable after creation, and a one-line meaning. This is the exhaustive companion
to the task-oriented guides — cross-checked against `crates/api` and the generated
`deploy/crds/`.

/// info | Conventions

- **Type** uses the CRD/YAML shape. `enum(A|B)` lists the allowed values; the
  **bold** one is the default. Sub-objects link to their own table.
- **Default** is the value when the field is absent. "—" means no default (the
  field is optional and simply unset). A value in `code` is materialized into the
  stored object (visible in `kubectl explain`) per ADR-0005 §1.
- **Required** fields have no default and must be present, or admission fails.
- **Immutable** fields are rejected on edit by the webhook *and* CRD
  `x-kubernetes-validations` transition rules (ADR-0005 §7).
- Externally-tagged unions (`backend`, `source`, `target`, `allowedNamespaces`,
  hooks) select a variant by **which key you set**, never a `kind:` field.

///

---

## Repository (namespaced)

Short name `kopiarepo`. Print columns: `PHASE`, `BACKEND`, `AGE`.

### `spec`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `backend` | externally-tagged [Backend](#backend) | **required** | Exactly one storage backend. |
| `encryption.passwordSecretRef` | [SecretKeyRef](#secretkeyref) | **required** | The kopia repository password Secret. **Immutable.** |
| `create` | [CreateBehavior](#createbehavior) | — | Initialize the repo if absent (off by default). |
| `moverDefaults` | [MoverDefaults](#moverdefaults) | — | Base config every mover inherits (bootstrap/backup/restore/maintenance). |
| `catalog` | [CatalogBounds](#catalogbounds) | — | Bounds materialization of `discovered` `Snapshot` CRs. |
| `maintenance` | [RepositoryMaintenanceSpec](#repositorymaintenancespec) | default-on | Default-managed `Maintenance` projection. |
| `onNamespaceDelete` | enum(**`Orphan`**\|`Delete`) | `Orphan` | What a consuming-namespace deletion does to snapshots. §5 |
| `mode` | enum(**`ReadWrite`**\|`ReadOnly`) | `ReadWrite` | `ReadOnly` serves restores only (no backups/maintenance). §11 |
| `suspend` | bool | `false` | Pause connect/bootstrap + maintenance projection. §14(e) |

Immutable after creation: `encryption`, `create.splitter`, `create.hash`,
`create.encryption`, `create.ecc`.

### `status`

| Field | Type | Notes |
| --- | --- | --- |
| `phase` | enum(`Pending`\|`Initializing`\|`Ready`\|`Degraded`\|`Failed`) | Lifecycle. |
| `observedGeneration` | int | Last reconciled `metadata.generation` (kstatus). |
| `uniqueId` | string | kopia repository unique ID. |
| `backend` | string | Backend discriminant (mirrors `spec.backend`), for the column. |
| `storageStats` | {`snapshotCount`,`totalSize`,`lastObservedAt`} | From the last catalog scan. |
| `catalog` | {`discoveredBackupCount`,`lastRefreshAt`} | Catalog-materialization status. |
| `conditions` | []Condition | `Ready`/`Reconciling`/`Stalled`, `Connected`, `MaintenanceOwned`, … |

---

## ClusterRepository (cluster-scoped)

Short name `kopiacrepo`. Same backend/encryption/create surface as `Repository`,
plus tenancy + identity. Print columns: `PHASE`, `BACKEND`, `NAMESPACES`, `AGE`.
Because it is cluster-scoped, **every** Secret reference in it (`backend.*.auth`,
`encryption.passwordSecretRef`) MUST carry an explicit `namespace` (webhook-enforced).

### `spec`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `backend` | externally-tagged [Backend](#backend) | **required** | Exactly one backend. |
| `encryption.passwordSecretRef` | [SecretKeyRef](#secretkeyref) (with `namespace`) | **required** | Repo password. **Immutable.** |
| `create` | [CreateBehavior](#createbehavior) | — | Same as `Repository`. |
| `moverDefaults` | [MoverDefaults](#moverdefaults) | — | Inherited by every mover (including consumer backup/restore). |
| `catalog` | [CatalogBounds](#catalogbounds) | — | Adds `fallbackNamespace` for discovered snapshots. |
| `allowedNamespaces` | externally-tagged [AllowedNamespaces](#allowednamespaces) | **required** | Tenancy gate. |
| `identityDefaults` | [IdentityDefaults](#identitydefaults) | — | Per-tenant identity CEL `*Expr`. |
| `maintenance` | [RepositoryMaintenanceSpec](#repositorymaintenancespec) | default-on | `maintenance.namespace` picks where the `Maintenance` lands. |
| `onNamespaceDelete` | enum(**`Orphan`**\|`Delete`) | `Orphan` | §5 |
| `credentialProjection.allowed` | bool | `false` | **Owner gate** for credential projection. §8 |
| `mode` | enum(**`ReadWrite`**\|`ReadOnly`) | `ReadWrite` | §11 |
| `suspend` | bool | `false` | §14(e) |

Same immutable set as `Repository`.

### `status`

Mirrors `RepositoryStatus` plus `allowedNamespaceCount` (int — namespaces currently
resolved by `allowedNamespaces`).

---

## SnapshotPolicy (namespaced) — the recipe

Short name `kopiasp`, plural `snapshotpolicies`. Print columns: `REPOSITORY`,
`LAST-SNAPSHOT` (`status.lastSuccessfulSnapshot`), `LAST-VERIFIED`
(`status.lastVerified`), `SUSPENDED` (`spec.suspend`), `AGE`.

### `spec`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `repository` | [RepositoryRef](#repositoryref) | **required** | The `Repository`/`ClusterRepository` to write to. |
| `identity` | {`username`?,`hostname`?} | — | Override the resolved `username@hostname`. |
| `sources` | [][Source](#source) | — | What to back up (≥1, webhook-enforced). |
| `copyMethod` | enum(**`Snapshot`**\|`Clone`\|`Direct`) | `Snapshot` | How the volume is captured (materialized default). §1 |
| `volumeSnapshotClassName` | string | — | `VolumeSnapshotClass` for `Snapshot`/`Clone`. |
| `groupBy` | enum(**`VolumeGroupSnapshot`**\|`None`) | `VolumeGroupSnapshot` | Multi-PVC consistency; `None` must be set explicitly. |
| `retention` | [Retention](#retention) | — | GFS — the only successful-retention driver. |
| `defaultDeletionPolicy` | enum(`Delete`\|`Retain`\|`Orphan`) | `Delete` (effective) | Default `deletionPolicy` for child `Snapshot`s. |
| `compression` | {`compressor`?,`neverCompress`[]} | — | kopia compressor + per-glob opt-out. |
| `files` | {`ignoreRules`[],`ignoreCacheDirs`,`ignoreIdenticalSnapshots`} | — | kopia files policy: ignore globs, honor `CACHEDIR.TAG`, skip identical re-snapshots. |
| `extraArgs` | []string | — | Escape hatch for kopia flags. |
| `errorHandling` | {`ignoreFileErrors`,`ignoreDirErrors`,`ignoreUnknownTypes`} | all `false` | Let a snapshot complete-with-errors. §13(b) |
| `upload` | {`maxParallelSnapshots`?,`maxParallelFileReads`?} | — | Upload parallelism. §13(f) |
| `verification` | [Verification](#verification) | — | Opt-in restorability checks. §4 |
| `suspend` | bool | `false` | Skip this recipe entirely. §14(e) |
| `hooks` | {`beforeSnapshot`[],`afterSnapshot`[]} of [Hook](#hook) | — | Pre/post-snapshot workload hooks. |
| `mover` | [MoverSpec](#moverspec) | — | Per-recipe mover overrides (merged over `moverDefaults`). |
| `credentialProjection.enabled` | bool | `false` | Consumer opt-in to project the repo Secret. |

### `status`

| Field | Type | Notes |
| --- | --- | --- |
| `observedGeneration` | int | kstatus. |
| `resolved` | {`identity`,`sources`[]} | Pinned at admission; never re-rendered. |
| `retention` | {`activeSnapshotCount`,`lastPruneAt`,`lastPruneDeleted`} | Last GFS prune summary. |
| `lastSuccessfulSnapshot` | RFC3339 | Backs `LAST-SNAPSHOT` + the staleness alert. |
| `lastVerified` | RFC3339 | Backs `LAST-VERIFIED` + `kopiur_snapshot_verified_timestamp`. §4 |
| `conditions` | []Condition | `Ready`/`Reconciling`/`Stalled`, `RepositoryReachable`, … |

---

## Snapshot (namespaced) — one snapshot

Short name `kopiasnap`, plural `snapshots`. Print columns: `PHASE`, `ORIGIN`,
`SNAPSHOT` (`status.snapshot.kopiaSnapshotID`), `AGE`. For `discovered` snapshots
the whole `spec` is empty.

### `spec`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `policyRef` | [PolicyRef](#policyref) | — (absent for discovered) | The recipe to run. |
| `tags` | map | — | Arbitrary kopia snapshot tags. |
| `failurePolicy` | {`backoffLimit`?,`activeDeadlineSeconds`?} | — | Mover Job retry/deadline. |
| `deletionPolicy` | enum(`Delete`\|`Retain`\|`Orphan`) | `Delete` (scheduled/manual), forced `Retain` (discovered) | What happens to the snapshot when this CR is deleted. §4.5 |
| `pin` | bool | `false` | Exempt this snapshot from GFS retention. §13(c) |

### `status`

| Field | Type | Notes |
| --- | --- | --- |
| `phase` | enum(`Pending`\|`Running`\|`Succeeded`\|`Failed`\|`Deleting`\|`Discovered`) | Lifecycle. |
| `origin` | enum(`scheduled`\|`manual`\|`discovered`) | Canonical origin (mirrors the `origin` label). |
| `observedGeneration` | int | kstatus. |
| `snapshot` | {`kopiaSnapshotID`,`identity`} | The kopia artifact this CR owns. |
| `timing` | {`startTime`,`endTime`,`durationSeconds`} | Run timing. |
| `stats` | {`sizeBytes`,`bytesNew`,`filesNew`,`filesModified`,`filesUnchanged`} | From kopia JSON. |
| `job` | {`name`,`attempts`} | Mover Job (absent for discovered). |
| `resolved` | {`repository`,`sources`[]} | Frozen recipe values at run time. |
| `conditions` | []Condition | `SourcesQuiesced`, `SnapshotCreated`, … |
| `logTail` | string | Capped ~4KB; full logs in the Job pod. |
| `pinned` | bool? | Observed kopia-side pin state (vs `spec.pin`). |

---

## SnapshotSchedule (namespaced) — the cron

Short name `kopiasched`, plural `snapshotschedules`. Print columns: `CONFIG`
(`spec.policyRef.name`), `SCHEDULE` (`spec.schedule.cron`), `SUSPENDED`
(`spec.schedule.suspend`), `AGE`. `policyRef` **XOR** `policySelector` (exactly one;
webhook + CRD validation).

### `spec`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `policyRef` | [PolicyRef](#policyref) | — | A single `SnapshotPolicy`. Mutually exclusive with `policySelector`. |
| `policySelector` | LabelSelector | — | Fan-out: many `SnapshotPolicy`s in this namespace. §10 |
| `schedule` | [ScheduleSpec](#schedulespec) | **required** | Cron + jitter + concurrency. |
| `failedJobsHistoryLimit` | uint | — | Bounds *failed* child `Snapshot`s. (No `successfulJobsHistoryLimit` — GFS only.) |

### `status`

| Field | Type | Notes |
| --- | --- | --- |
| `observedGeneration` | int | kstatus. |
| `lastSchedule` | [ScheduleRef](#scheduleref) | Most recent firing (pinned). |
| `nextSchedule` | [ScheduleRef](#scheduleref) | Computed next firing (`status.nextSchedule.at`). |
| `lastSuccessfulSchedule` | [ScheduleRef](#scheduleref) | Most recent firing whose `Snapshot` succeeded. |
| `consecutiveFailures` | int | Back-to-back failures; resets on success. |
| `conditions` | []Condition | Schedule health. |

---

## Restore (namespaced)

Short name `kopiarestore`. Print columns: `PHASE`, `SOURCE` (`status.sourceKind`),
`AGE`. Exactly one of `target.pvc`/`target.pvcRef`/`target.populator` (webhook +
CRD validation).

### `spec`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `repository` | [RepositoryRef](#repositoryref) | — (inferred from `source`) | **Required** for an `identity` source. |
| `source` | externally-tagged [RestoreSource](#restoresource) | **required** | Where to read from. |
| `target` | externally-tagged [RestoreTarget](#restoretarget) | **required** | Where to write — `pvc`/`pvcRef`/`populator: {}`. §9 |
| `options` | [RestoreOptions](#restoreoptions) | — | kopia write behavior. |
| `policy` | {`onMissingSnapshot`?,`waitTimeout`?} | `Fail`/`Continue` by source | Missing-snapshot handling. |
| `credentialProjection.enabled` | bool | `false` | Project the repo Secret into the mover namespace. |
| `mover` | [MoverSpec](#moverspec) | — | Same surface a backup gets. |
| `failurePolicy` | {`backoffLimit`?,`activeDeadlineSeconds`?} | — | Mover Job retry/deadline. |

### `status`

| Field | Type | Notes |
| --- | --- | --- |
| `phase` | enum(`Pending`\|`Resolving`\|`Restoring`\|`Completed`\|`Failed`) | Lifecycle. |
| `sourceKind` | string | `SnapshotRef`/`FromPolicy`/`Identity`; backs `SOURCE`. |
| `observedGeneration` | int | kstatus. |
| `resolved` | {`snapshotRef`,`repository`,`pinnedAt`,`identity`} | Pinned source. |
| `target` | {`pvcPrime`,`pvcRef`} | Resolved target. |
| `timing` | {`startTime`,`endTime`} | — |
| `progress` | {`bytesRestored`,`filesRestored`} | Live mover progress. |
| `conditions` | []Condition | `Ready`/`Reconciling`/`Stalled`, reason text. |

---

## Maintenance (namespaced)

Short name `kopiamaint`. Print columns: `REPOSITORY`, `OWNER`
(`status.ownership.owner`), `AGE`. At most one per repository (ownership lease).
Usually default-managed via the repository's `spec.maintenance` — see
[Maintenance](maintenance.md).

### `spec`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `repository` | [RepositoryRef](#repositoryref) | **required** | The repo to maintain. |
| `schedule.quick` / `schedule.full` | [CronSpec](#cronspec) | **required** | quick (cheap) / full (`--full`, reclamation). |
| `schedule.timezone` | string | — | IANA tz for both crons. |
| `ownership` | {`owner`,`takeoverPolicy`} | `takeoverPolicy`=`Never` | Lease holder + takeover (`Never`/`PromptCondition`/`Force`). |
| `mover` | [MoverSpec](#moverspec) | — | Object-store repos typically tune this. |
| `failurePolicy` | {`backoffLimit`?,`activeDeadlineSeconds`?} | — | Failed-run handling. |
| `credentialProjection.enabled` | bool | `false` | Project the repo Secret for the maintenance mover. |

### `status`

| Field | Type | Notes |
| --- | --- | --- |
| `observedGeneration` | int | kstatus. |
| `ownership` | {`owner`,`claimedAt`} | Current lease holder. |
| `quick` / `full` | [RunStatus](#runstatus) | Per-kind run state; `full.lastContentReclaimedBytes` is the only place reclamation is surfaced. |
| `conditions` | []Condition | Maintenance health. |

---

## RepositoryReplication (namespaced)

Short name `kopiarepl`, plural `repositoryreplications`. The off-site mirror —
`kopia repository sync-to`. Print columns: `SOURCE` (`spec.sourceRef.name`),
`DESTINATION` (`status.destinationBackend`), `SCHEDULE` (`spec.schedule.cron`),
`LAST` (`status.lastReplicated`), `AGE`. See [Repository replication](replication.md). §13(d)

### `spec`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `sourceRef` | [RepositoryRef](#repositoryref) | **required** | The repository to mirror from. |
| `destination` | externally-tagged [Backend](#backend) | **required** | Mirror to — must differ from the source backend. |
| `destinationEncryption.passwordSecretRef` | [SecretKeyRef](#secretkeyref) | — | Omit to reuse the source password (true mirror). |
| `schedule` | [CronSpec](#cronspec) | **required** | Cron + jitter for replication runs. |
| `mover` | [MoverSpec](#moverspec) | — | Inherits the source repo's `moverDefaults`. |
| `suspend` | bool | `false` | §14(e) |

### `status`

| Field | Type | Notes |
| --- | --- | --- |
| `phase` | enum(`Pending`\|`Replicating`\|`Succeeded`\|`Failed`\|`Suspended`) | Lifecycle. |
| `observedGeneration` | int | kstatus. |
| `destinationBackend` | string | Backs `DESTINATION`. |
| `lastReplicated` / `nextScheduledAt` | RFC3339 | Backs `LAST`. |
| `lastReplicatedBytes` / `lastReplicatedBlobs` | int | Best-effort from kopia output. |
| `conditions` | []Condition | `Ready`/`Reconciling`/`Stalled`. |

---

## Shared sub-objects

### Backend

Externally tagged — set exactly one of: `s3`, `azure`, `gcs`, `b2`, `filesystem`,
`sftp`, `webDav`, `rclone`. See [Backend configuration](backends.md) for each
backend's fields and Secret keys.

### SecretKeyRef

`{ name, namespace?, key? }` — a key within a `Secret`. `namespace` defaults to the
referrer's namespace (required on cluster-scoped CRs).

### RepositoryRef

`{ kind, name, namespace? }` — `kind` is `enum(`**`Repository`**`|ClusterRepository)`
(defaults `Repository`). `namespace` is forbidden when `kind: ClusterRepository`
(webhook-enforced).

### PolicyRef

`{ name, namespace? }` — reference to a `SnapshotPolicy`. May cross namespaces
(subject to RBAC).

### CreateBehavior

`{ enabled, encryption?, splitter?, hash?, ecc? }` — `enabled` defaults `false`.
`encryption`/`splitter`/`hash` are kopia algorithm names consulted only at creation;
`ecc` is `{ algorithm?, overheadPercent? }` (Reed-Solomon parity). All of
`splitter`/`hash`/`encryption`/`ecc` are **immutable** after creation. §13(a)

### MoverDefaults

| Field | Type | Notes |
| --- | --- | --- |
| `securityContext` | core/v1 SecurityContext | Container SC base for every mover (where you set the mover's `runAsUser`/`runAsGroup`). |
| `podSecurityContext` | core/v1 PodSecurityContext | Pod SC base (notably `fsGroup`). |
| `resources` | core/v1 ResourceRequirements | Mover container resources base. |
| `cache` | [CacheDefaults](#cachedefaults) | kopia cache backing every mover. |
| `nodeSelector` / `tolerations` / `affinity` | core/v1 | Pod scheduling for every mover. |
| `ttlSecondsAfterFinished` | int | Finished mover Jobs self-GC (built-in default `3600`). §12 |
| `throttle` | {`uploadBytesPerSecond`?,`downloadBytesPerSecond`?,`readOpsPerSecond`?,`writeOpsPerSecond`?} | kopia repository throttle. §13(e) |

`securityContext`/`podSecurityContext`/`resources`/`cache` merge field-wise:
`hardened ⊂ moverDefaults ⊂ recipe.mover`. See
[`moverDefaults` on the Repositories page](repositories.md).

### MoverSpec

Per-recipe override (on `SnapshotPolicy`/`Restore`/`Maintenance`/`RepositoryReplication`).
`{ resources?, cache?, securityContext?, podSecurityContext?, privilegedMode?,
inheritSecurityContextFrom?, ttlSecondsAfterFinished? }`. `securityContext` and
`inheritSecurityContextFrom` are mutually exclusive (webhook). Merges *over*
`moverDefaults`; a partial override can only tighten the hardened baseline.

### CacheDefaults

`{ capacity?, storageClassName?, metadataCacheSizeMb?, contentCacheSizeMb?,
mode? }` — `mode` is `enum(`**`Ephemeral`**`|Persistent)`.

### CatalogBounds

`{ retain?: { perIdentity?, maxAgeDays? }, refreshInterval?, fallbackNamespace? }` —
bounds materialized `discovered` `Snapshot`s. `fallbackNamespace` is
ClusterRepository-only.

### RepositoryMaintenanceSpec

`{ enabled, schedule?, mover?, failurePolicy?, takeoverPolicy?, namespace? }` —
`enabled` defaults **`true`** (default-on). `namespace` is ClusterRepository-only
(where the managed `Maintenance` lands). See [Maintenance](maintenance.md).

### AllowedNamespaces

Externally tagged — exactly one of `list: [..]`, `selector: <LabelSelector>`, or
`all: true`.

### IdentityDefaults

`{ hostnameExpr?, usernameExpr? }` — **CEL** expressions returning strings, env
`namespace`/`policyName`/`labels`/`annotations`, validated at admission, ~1 KiB cap.
§5 (ADR-0004)

### Retention

`{ keepLatest?, keepHourly?, keepDaily?, keepWeekly?, keepMonthly?, keepAnnual? }`
(GFS; all uint). Set only the buckets you want.

### Source

`{ pvc? | pvcSelector? | nfs? , sourcePathOverride?, sourcePathStrategy? }` — exactly
one of `pvc`/`pvcSelector`/`nfs` (CRD `x-kubernetes-validations` + webhook).
`sourcePathStrategy` is `enum(`**`PvcName`**`|PvcNamespacedName)`.

### Hook

Externally tagged — `workloadExec` / `runJob` / `httpRequest`. Each carries
`continueOnFailure` (default `false` — a hook failure aborts the snapshot).

### Verification

`{ quick?: CronSpec, deep?: { schedule, storageClassName?, capacity? }, successExpr?,
verifyFilesPercent? }` — `successExpr` is a CEL bool predicate over
`stats{files,bytes,errors}`/`snapshot`/`restored{files,checksumMatches}`, validated
at admission. §4

### RestoreSource

Externally tagged — `snapshotRef: ObjectRef` / `fromPolicy: { name, namespace?, asOf?,
offset }` (`offset` materialized default `0`) / `identity: { username, hostname,
sourcePath?, snapshotID?, asOf?, offset? }`.

### RestoreTarget

Externally tagged — `pvc: { name, storageClassName?, capacity?, accessModes[] }` /
`pvcRef: { name, namespace? }` / `populator: {}`.

### RestoreOptions

`{ enableFileDeletion (default false), ignorePermissionErrors? (default true),
writeFilesAtomically? (default true) }`.

### ScheduleSpec

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `cron` | string | **required** | Cron with Jenkins-style `H`. |
| `jitter` | duration | — | Deterministic per-schedule jitter. |
| `timezone` | string | — | IANA tz. |
| `runOnCreate` | bool | `false` | Materialized default; do not fire on apply. §1 |
| `suspend` | bool | `false` | Skip future firings. |
| `concurrencyPolicy` | enum(**`Forbid`**\|`Allow`\|`Replace`) | `Forbid` | Materialized default. §1 |
| `startingDeadlineSeconds` | int | — | Skip a slot missed by more than this. |

### ScheduleRef

`{ at, snapshotRef? }` — `at` is the RFC3339 slot instant (accepts the `scheduledAt`
alias on the wire). `snapshotRef` is `{ name }`.

### CronSpec

`{ cron, jitter? }` — used by `Maintenance` quick/full, `Verification`, and
`RepositoryReplication`.

### RunStatus

`{ lastRunAt?, nextScheduledAt?, consecutiveFailures?, lastContentReclaimedBytes? }`.

## See also

- [Backups & schedules](backups.md), [Restores](restores.md), [Repositories](repositories.md) — task-oriented prose.
- [API reference (rustdoc)](api-reference.md) — the generated Rust type docs.
