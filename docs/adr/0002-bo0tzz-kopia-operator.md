# ADR-0001: A Kopia-Native Backup Operator for Kubernetes

- **Status:** Proposed
- **Date:** 2026-05-24
- **Inspired by:** [`backube/volsync`](https://github.com/backube/volsync) and the kopia fork [`perfectra1n/volsync`](https://github.com/perfectra1n/volsync) (especially PR [`backube/volsync#1723`](https://github.com/backube/volsync/pull/1723) and the trigger-redesign proposal [`backube/volsync#1559`](https://github.com/backube/volsync/issues/1559)). The triggering model also draws on [CloudNativePG](https://cloudnative-pg.io/) (`Cluster` / `ScheduledBackup` / `Backup`) and Tekton (`Task` / `TaskRun`).

> Scope: this ADR covers **CRD shape, user experience, and high-level design choices**. It deliberately does not specify Go package layout, controller-runtime indexes, leader-election lease IDs, or other implementation mechanics — those belong to follow-up ADRs once the API surface is agreed.

---

## 1. Context

VolSync is the de-facto Kubernetes-native mover for PVCs. Its design is mature and battle-tested, but it has accreted around restic's model. As soon as you try to add a non-restic mover (kopia, rustic, borg, …) several deep design choices push back. The community fork `perfectra1n/volsync` proves out a kopia mover and ships a usable image — but its PR has been open ~13 months without merging, the upstream maintainers are capacity-constrained, and many users have switched to running the fork in production.

The fork's existence and the volume of feature requests around kopia/restic locking, multi-PVC backup, scheduling jitter, restore UX, trigger separation, and "stop running on apply" suggest something stronger than "land kopia in volsync" is warranted. A **kopia-native operator** can:

1. Drop the multi-mover abstraction entirely. Kopia is the only mover, so every CRD field can be expressive without leaking through a generic shape.
2. Make a repository a first-class Kubernetes resource. Kopia repos are designed to be shared across many writers — a fact volsync cannot express cleanly.
3. Separate **recipe**, **invocation**, and **schedule** so backups can be triggered by any source (cron, `kubectl create`, Argo Events, button-in-Grafana). Volsync's `trigger` field couples all three.
4. Use kopia's native identity model (`username@hostname:path`) deliberately rather than as an accident of `metadata.name`/`metadata.namespace`.
5. Treat `kopia maintenance` and snapshot lifecycle as first-class operator concerns rather than retrofits.
6. Surface kopia's snapshot catalog through CRDs so restore is "browse and reference," not "construct an `restoreAsOf` timestamp and hope."
7. Address the long backlog of papercuts as design decisions, not bug fixes.

We refer to the project as **`kopia-operator`** in this document; final naming is out of scope. The API group is **`kopia.io`** with initial version `v1alpha1`.

### 1.1 The most important gaps we are addressing

| # | Gap | volsync refs |
|---|---|---|
| G1 | Repository is not a Kubernetes resource; cannot be shared/reused cleanly | implicit; perfectra1n CRD shape |
| G2 | One `ReplicationSource` = one PVC | `#1115`, `#1116`, `#320` |
| G3 | First reconcile triggers an immediate backup, no GitOps-friendly "skip first run" | `#627` |
| G4 | No cron jitter / `H` substitution, no timezone | `#1421`, `#702` |
| G5 | Restic repo locking / piling-up jobs | `#1042`, `#1429`, `#646` |
| G6 | No retry-limit / backoffLimit override | `#1228`, `#1042` |
| G7 | Restore proceeds with empty PVC if no snapshot found | `#1211` |
| G8 | Snapshot selection is restic-format `restoreAsOf` only; no browse | `#7`, `#1211` |
| G9 | `latestImage` always wins — no immutable restore source | `disc #1115` |
| G10 | Volume populator + Direct copyMethod incompatibility | `disc #1115`, `#1129` |
| G11 | Maintenance ownership is implicit & runs in the same pod as backup | perfectra1n fork redesigned this three times |
| G12 | Policy passthrough is brittle: every kopia knob needs CRD/jq script changes | fork `#13`, `#23` |
| G13 | Snapshot actions run in mover, not workload | fork `#22` |
| G14 | OOMs unpredictable; no resource guidance | `#626`, `#707`, `#1228` |
| G15 | Mover image is `:latest` by default | volsync `restic/builder.go:42` |
| G16 | Restricted PSA / OpenShift SCC / unprivileged-mode `lost+found` papercuts | `#367`, `#1033`, `#1889`, `#1430` |
| G17 | Trigger semantics are baked into the source CR — no manual/external trigger path | `#1559` |
| G18 | Mover-pod lifecycle (zombie pods, stuck jobs) | fork `#8`, volsync `#1415` |
| G19 | Maintainers' explicit door-closing on new movers | `#1743`, `#1029`, `#320` |

---

## 2. Decision

### 2.1 Topology

Five CRDs in `kopia.io/v1alpha1`, all namespaced:

| CRD | Layer | Purpose |
|---|---|---|
| **`Repository`** | Storage | A kopia repository: credentials, backend, encryption, optional catalog-materialization bounds. Many `BackupConfig`s/`Restore`s reference one. |
| **`BackupConfig`** | Recipe | *What* to back up: PVC selector, identity, retention, policy, hooks. Idempotent — doesn't run anything on its own. |
| **`Backup`** | Invocation + Catalog | A single kopia snapshot as a Kubernetes object. Created by `BackupSchedule`, `kubectl create`, or any other trigger source. Also materialized by the operator from the kopia catalog for snapshots it didn't produce (foreign or pre-install). |
| **`BackupSchedule`** | Cron | *When* it runs: cron (with jitter + timezone) + `configRef`. Creates `Backup` CRs. |
| **`Restore`** | Operation | A restore from a snapshot/identity to a PVC. Used directly, or referenced by `PVC.spec.dataSourceRef`. |
| **`Maintenance`** | Lifecycle | One per `Repository`: schedules `kopia maintenance run` quick + full, manages ownership lease. |

The three-layer split (recipe / invocation / schedule) for backups is the deliberate response to volsync `#1559`. It means:

- A `Backup` can be created from anywhere — `kubectl create`, Argo Events, a Tekton pipeline, a webhook handler.
- A `BackupSchedule` is just one source of `Backup` CRs. Removing or pausing a schedule does not affect already-running or already-completed runs.
- A `BackupConfig` change applies to subsequent invocations; the operator snapshots resolved values into each `Backup.status.resolved...` for traceability.

`Backup` is also the single canonical representation of a kopia snapshot — both ones we produced and ones we discover in the repo. Two retention drivers cover the lifecycle:

- `BackupSchedule.spec.successfulJobsHistoryLimit` GCs schedule-spawned `Backup`s.
- `Repository.spec.catalog.retain` GCs `origin: discovered` `Backup`s, bounding etcd footprint for large repos.
- Manual `Backup` CRs are user-owned and never auto-GC'd.

Dedup key is `(Repository.UID, kopiaSnapshotID)` — the operator will not create a discovered `Backup` for a snapshot already represented by an operator-initiated one.

Restore stays as a single CR (it's an operation, not a recurring thing). For the `dataSourceRef`-driven populator pattern, a `Restore` is left in passive mode (no `target`) and consumed by zero-or-more PVCs.

### 2.2 Anchoring principles

1. **Repositories are objects.** Identity, lifecycle, and maintenance hang off them.
2. **Triggering is decoupled.** `BackupConfig` says *what*; `Backup` says *that*; `BackupSchedule` says *when*. Any of the three can be authored or automated independently.
3. **A `Backup` is a kopia snapshot.** Operator-initiated, manually-applied, and discovered snapshots are all the same kind.
4. **Restores are explicit.** No silent "empty PVC because no snapshots existed yet" by default. The "deploy-or-restore" GitOps pattern is opt-in via a specific source mode + `onMissingSnapshot: Continue`.
5. **Maintenance is a first-class lifecycle concern**, with its own CRD and explicit ownership lease.
6. **The mover is a thin shim.** A Go-native controller invokes `kopia --json` and parses results. No 2,500-line bash scripts. The image carries `kopia` and nothing else.
7. **Validation is webhook-enforced.** Mutually exclusive fields, missing repository references, malformed schedules — rejected at admission.
8. **Identity is explicit and overridable.** Defaults derive from object name/namespace; every component is overridable; the resolved identity always appears in `status`.
9. **Forward-compatible by construction.** Every credential, policy, and rotation surface is a sub-object, so future fields slot in without API breakage (see §4.10).

### 2.3 Where Backup CRs live

| Origin | Namespace |
|---|---|
| `operator` — created by `BackupSchedule` | The `BackupConfig`'s namespace (so the owning team sees their backups with `kubectl get backup -n <team>`). |
| `manual` — created by `kubectl create` or external automation | Whichever namespace the user applies it to. The `configRef` may cross namespaces, subject to RBAC. |
| `discovered` — materialized from the kopia catalog | The `Repository`'s namespace. The operator has no reliable way to attribute a foreign snapshot to a `BackupConfig`, so it stays with the repo. |

`Restore.spec.source.backupRef` carries `{ name, namespace }` for cross-namespace references.

---

## 3. CRD Design

### 3.1 `Repository`

Owns credentials, encryption, and repository-wide settings. Catalog materialization for discovered `Backup` CRs is configured here.

```yaml
apiVersion: kopia.io/v1alpha1
kind: Repository
metadata:
  name: nas-primary
  namespace: backups
spec:
  # Exactly one backend block. Webhook-enforced.
  backend:
    s3:
      bucket: my-backups
      prefix: clusters/prod/
      endpoint: s3.us-east-1.amazonaws.com
      region: us-east-1
      auth:
        secretRef:
          name: nas-primary-creds       # keys: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, ...
        # Optional advanced auth — workloadIdentity supported but not the default.
        # workloadIdentity:
        #   serviceAccountName: kopia-s3
      tls:
        caBundleRef:
          configMapName: corp-ca
          key: ca.crt
        insecureSkipVerify: false

  encryption:
    passwordSecretRef:                  # always a Secret ref; never inline
      name: nas-primary-creds
      key: KOPIA_PASSWORD
    # Future fields (rotation, previousPasswords, ...) slot in here.

  create:
    enabled: true                       # if repo missing, create it
    encryption: AES256-GCM-HMAC-SHA256
    splitter: DYNAMIC-4M-BUZHASH
    hash: BLAKE3-256

  cacheDefaults:                        # inherited by Backup/Restore unless overridden
    capacity: 8Gi
    storageClassName: fast-ssd
    metadataCacheSizeMB: 5000
    contentCacheSizeMB: 2000

  catalog:                              # bounds materialization of `origin: discovered` Backup CRs
    retain:
      perIdentity: 100                  # most recent N per username@hostname:path
      maxAgeDays: 90                    # nothing older than this gets a Backup CR
    refreshInterval: 5m
    # Older snapshots remain in kopia; restorable via Restore.source.identity.snapshotID

status:
  phase: Ready                          # Pending | Initializing | Ready | Degraded | Failed
  observedGeneration: 7
  uniqueID: "fb6e...c41a"               # kopia repo unique ID
  conditions:
    - type: Connected
      status: "True"
      reason: ConnectFromConfig
    - type: MaintenanceOwned
      status: "True"
      message: "kopia-operator/nas-primary"
  storageStats:
    snapshotCount: 1284
    totalSize: 412Gi
    lastObservedAt: 2026-05-24T17:00:01Z
  catalog:
    discoveredBackupCount: 412          # how many Backup CRs materialized from the catalog scan
    lastRefreshAt: 2026-05-24T17:01:11Z
```

**Why:** addresses **G1** (repo as a resource), **G15** (digest pinning belongs on the operator image, not embedded per recipe), and provides the catalog-bounds knob that keeps `Backup` CRs from blowing up etcd while still giving the K8s-native view of kopia history. `encryption` is a sub-object so future rotation fields fit without API breakage (**§4.10**).

### 3.2 `BackupConfig`

The recipe. Idempotent. Apply once; reference from many `Backup`s or one `BackupSchedule`.

```yaml
apiVersion: kopia.io/v1alpha1
kind: BackupConfig
metadata:
  name: postgres-data
  namespace: billing
spec:
  repository:
    name: nas-primary
    namespace: backups                  # cross-ns ref; RBAC-gated

  # Identity — what kopia sees. Defaults shown.
  identity:
    username: "postgres-data"           # default: <BackupConfig.metadata.name>
    hostname: "billing"                 # default: <BackupConfig.metadata.namespace>

  # Sources — what to back up.
  sources:
    - pvc: { name: postgres-data }
      sourcePathOverride: /data         # what kopia records (default: /pvc/<name>)
    # Or a selector for multi-PVC:
    # - pvcSelector:
    #     namespaceSelector: { matchNames: [billing, billing-staging] }
    #     labelSelector: { matchLabels: { backup: include } }
    #   sourcePathStrategy: PVCName     # PVCName | PVCNamespacedName

  copyMethod: Snapshot                  # Snapshot (default, PiT) | Clone | Direct
  volumeSnapshotClassName: csi-snap-class
  groupBy: VolumeGroupSnapshot          # default for multi-PVC sources; None opts into per-PVC

  retention:
    keepLatest: 10
    keepHourly: 24
    keepDaily: 14
    keepWeekly: 8
    keepMonthly: 12
    keepAnnual: 5

  policy:                               # typed fields — not opaque JSON parsed by jq
    compression:
      compressor: zstd
      neverCompress: ["*.zip", "*.gz", "*.mp4"]
    splitter: DYNAMIC-4M-BUZHASH
    ignore:
      paths: ["*.tmp", "*/cache/*", "lost+found"]
      cacheDirs: true                   # honor CACHEDIR.TAG
      ignoreIdenticalSnapshots: true    # fork issue #13
    extraArgs: []                       # escape hatch for kopia flags we don't model yet

  hooks:                                # G13 — runs in the workload, not the mover
    beforeSnapshot:
      - workloadExec:
          podSelector: { matchLabels: { app: postgres } }
          container: postgres
          command: ["pg_start_backup", "snap"]
          timeout: 2m
    afterSnapshot:
      - workloadExec:
          podSelector: { matchLabels: { app: postgres } }
          container: postgres
          command: ["pg_stop_backup"]
          timeout: 2m

  mover:                                # per-recipe overrides
    resources:
      requests: { cpu: 250m, memory: 512Mi }
      limits:   { cpu: "2",  memory: 4Gi }
    cache:
      capacity: 16Gi
      storageClassName: fast-ssd
    securityContext: {}                 # override; default: nonRoot uid 65534
    # privilegedMode: true              # opt-in, namespace-gated; preserves UID/GID on restore
    # inheritSecurityContextFrom:       # opt-in: copy SC from a live workload pod
    #   podSelector: { matchLabels: { app: postgres } }

status:
  resolved:                             # what would be passed to kopia
    identity:
      username: "postgres-data"
      hostname: "billing"
      sources:
        - pvc: billing/postgres-data
          sourcePath: /data
  conditions:
    - type: RepositoryReachable
      status: "True"
    - type: GroupSnapshotSupported
      status: "True"
```

**Why:** addresses **G2** (selector + VolumeGroupSnapshot default), **G12** (typed policy + escape hatch), **G13** (hook types), **G14** (explicit resource defaults), **G16** (security-context controls without forcing privileged-by-default). The identity sub-object makes the second-biggest perfectra1n papercut (fork **#7**) impossible.

### 3.3 `Backup`

A single kopia snapshot as a Kubernetes object. Three origins:

- `operator` — created by a `BackupSchedule`. Spec has `configRef`; lives in the `BackupConfig`'s namespace.
- `manual` — created by `kubectl create` or external automation. Spec has `configRef`; lives wherever the user applied it.
- `discovered` — materialized by the operator's catalog scan for snapshots it didn't produce. Spec is empty; lives in the `Repository`'s namespace.

```yaml
apiVersion: kopia.io/v1alpha1
kind: Backup
metadata:
  name: postgres-data-20260524-021300
  namespace: billing
  labels:
    # Operator-managed labels — canonical values live in status; these mirror
    # for kubectl-selectability.
    kopia.io/repository: nas-primary
    kopia.io/backup-config: postgres-data
    kopia.io/origin: operator
    kopia.io/identity-hash: "a3f1..."
spec:
  # Operator-initiated and manual: configRef + optional overrides.
  # Discovered: spec is empty/absent.
  configRef: { name: postgres-data }
  tags:
    reason: "scheduled-nightly"
  # parameters:                         # optional per-run overrides on the recipe
  #   compressionOverride: none
  failurePolicy:                        # G6 — per-run, not hard-coded
    backoffLimit: 2
    activeDeadlineSeconds: 7200

status:
  phase: Succeeded                      # Pending | Running | Succeeded | Failed | Discovered
  origin: operator                      # operator | manual | discovered — canonical
  snapshot:                             # the kopia artifact
    kopiaSnapshotID: k1f1ec0a8
    identity:
      username: "postgres-data"
      hostname: "billing"
      sourcePath: /data
  timing:
    startTime: 2026-05-24T02:13:00Z
    endTime:   2026-05-24T02:18:42Z
    durationSeconds: 342
  stats:                                # populated from kopia's JSON output
    sizeBytes: 4321098765
    bytesNew: 12345678
    filesNew: 1233
    filesModified: 22
    filesUnchanged: 998111
  job:                                  # operator/manual only; absent for discovered
    name: backup-postgres-data-20260524-021300
    attempts: 1
  resolved:                             # frozen recipe values at run time (operator/manual)
    repository: { name: nas-primary, namespace: backups }
    sources:
      - pvc: billing/postgres-data
        sourcePath: /data
  conditions:
    - type: SourcesQuiesced
      status: "True"
    - type: SnapshotCreated
      status: "True"
  logTail: |                            # capped at ~4KB; full logs in the Job pod
    Snapshot created: k1f1ec0a8
    Total bytes: 4321098765
```

`kubectl get backup` shows everything — runs in flight, historical successes, failed attempts, and the discovered catalog — distinguished by the `kopia.io/origin` label and `status.phase`.

**Why:** addresses **G17** (invocations as first-class, any trigger source), folds in the catalog representation cleanly, and means restores reference one kind of thing. Logs are bounded; full logs live in the Job pod where users expect them. Failed `Backup` CRs are durable evidence — `failedJobsHistoryLimit` on the schedule controls how many we keep.

### 3.4 `BackupSchedule`

Creates `Backup` CRs on a schedule in the `BackupConfig`'s namespace.

```yaml
apiVersion: kopia.io/v1alpha1
kind: BackupSchedule
metadata:
  name: postgres-data-nightly
  namespace: billing
spec:
  configRef:
    name: postgres-data
  schedule:
    cron: "H 2 * * *"                   # G4 — Jenkins-style 'H' substitution
    jitter: 30m
    timezone: "America/Los_Angeles"
    runOnCreate: false                  # G3 — GitOps-friendly default
    suspend: false
    concurrencyPolicy: Forbid           # Forbid | Allow | Replace — G18
    startingDeadlineSeconds: 600
  successfulJobsHistoryLimit: 10        # GC bound for origin: operator Backups from this schedule
  failedJobsHistoryLimit: 3

status:
  lastSchedule:
    scheduledAt: 2026-05-24T02:13:00Z   # cron + jitter, pinned (predictable for alerting)
    backupRef: { name: postgres-data-20260524-021300 }
  nextSchedule:
    at: 2026-05-25T02:21:00Z
  lastSuccessfulSchedule:
    at: 2026-05-24T02:13:00Z
    backupRef: { name: postgres-data-20260524-021300 }
  consecutiveFailures: 0
  conditions:
    - type: ConfigResolvable
      status: "True"
```

**Why:** mirrors `CronJob` semantics exactly (G4, G18). Schedule anchoring is wall-clock (`cron(now)`), not `cron(lastSyncTime)` — fixes volsync's drift behavior. Pinned `scheduledAt` lets ops alerts say "you missed the 02:13 slot" without ambiguity.

### 3.5 `Restore`

A restore from a `Backup` (or raw kopia identity) to a PVC.

```yaml
apiVersion: kopia.io/v1alpha1
kind: Restore
metadata:
  name: postgres-restore-2026-05-23
  namespace: billing
spec:
  # Optional. Derived from `source` when omitted (the Backup CR / BackupConfig CR
  # knows its Repository). Required only with `source.identity`.
  # repository: { name: nas-primary, namespace: backups }

  # Exactly one of the following. Webhook-enforced.
  source:
    # Preferred: a Backup CR (operator-initiated, manual, or discovered — all same kind).
    backupRef: { name: postgres-data-20260524-021300, namespace: billing }
    # Or a BackupConfig CR — resolves via identity against the repo, even if no Backup
    # CR has ever been created in this cluster (deploy-or-restore on a fresh cluster
    # against an existing repo).
    # fromConfig:
    #   name: postgres-data
    #   asOf: 2026-05-23T20:00:00Z
    #   offset: 0                       # 0 = latest, 1 = previous, ...
    # Or a raw kopia identity (works for foreign writers or snapshots that have
    # aged out of the K8s-side catalog window).
    # identity:
    #   username: postgres-data
    #   hostname: billing
    #   sourcePath: /data
    #   snapshotID: k1f1ec0a8           # or asOf / offset
    # spec.repository is REQUIRED when using `identity`.

  # Optional. Three modes.
  target:
    # Mode 1: operator creates the PVC.
    pvc:
      name: postgres-data-restored
      storageClassName: fast-ssd
      capacity: 100Gi
      accessModes: [ReadWriteOnce]
    # Mode 2: write into an existing PVC.
    # pvcRef: { name: postgres-data-restored }
    # Mode 3: omit `target` entirely — passive. A PVC with
    #         spec.dataSourceRef -> this Restore kicks off the populator handshake.

  options:
    enableFileDeletion: false
    ignorePermissionErrors: true
    writeFilesAtomically: true

  policy:
    onMissingSnapshot: Fail             # default for explicit sources (backupRef/identity)
    # For source.fromConfig the default is Continue (see §4.4 for the deploy-or-restore pattern).
    waitTimeout: 5m

status:
  phase: Restoring                      # Pending | Resolving | Restoring | Completed | Failed
  resolved:                             # pinned at admission
    backupRef: { name: postgres-data-20260524-021300, namespace: billing }
    repository: { name: nas-primary, namespace: backups }
    pinnedAt: 2026-05-24T17:33:11Z
    identity:
      username: postgres-data
      hostname: billing
      sourcePath: /data
  target:                               # what the operator is writing into
    pvcPrime: pvc-prime-9f8e2c1b        # populator handshake (passive / pvc-create modes)
    pvcRef: { name: postgres-data-restored }
  timing:
    startTime: 2026-05-24T17:33:14Z
  progress:
    bytesRestored: 8123456789
    filesRestored: 998111
```

**Why:** addresses **G7** (fail-closed defaults), **G8/G9** (admission-time resolution, no drift on re-apply), **G10** (single restore path covers populator and in-place uniformly). Three source modes cover the spectrum: K8s-native reference (`backupRef`), recipe-driven (`fromConfig` — the GitOps deploy-or-restore pattern), and raw kopia identity (foreign writers, aged-out catalog). `spec.repository` is derivable for the first two; required only when raw identity is the source.

### 3.6 `Maintenance`

```yaml
apiVersion: kopia.io/v1alpha1
kind: Maintenance
metadata: { name: nas-primary, namespace: backups }
spec:
  repository: { name: nas-primary }
  schedule:
    quick: { cron: "0 */6 * * *", jitter: 30m }
    full:  { cron: "0 3 * * 0",  jitter: 1h }
    timezone: UTC
  ownership:
    owner: "kopia-operator/nas-primary"
    takeoverPolicy: PromptCondition     # Never | PromptCondition | Force
  mover:
    resources: { requests: { cpu: 250m, memory: 1Gi }, limits: { cpu: "2", memory: 4Gi } }
  failurePolicy:
    backoffLimit: 1
    activeDeadlineSeconds: 14400

status:
  ownership:
    owner: "kopia-operator/nas-primary"
    claimedAt: 2026-05-12T08:14:02Z
  quick:
    lastRunAt: 2026-05-24T12:00:11Z
    nextScheduledAt: 2026-05-24T18:00:00Z
    consecutiveFailures: 0
  full:
    lastRunAt: 2026-05-19T03:01:42Z
    nextScheduledAt: 2026-05-26T03:00:00Z
    consecutiveFailures: 0
  conditions:
    - type: OwnershipClaimed
      status: "True"
```

**Why:** at most one `Maintenance` per `Repository` (webhook-enforced) — kills the perfectra1n cross-namespace first-writer-wins race by making the conflict unrepresentable.

---

## 4. Key behaviors

### 4.1 Scheduling

- CronJob-style wall-clock anchoring (`cron(now)`), not last-completion anchoring. Fixes volsync's drift.
- `jitter` derives deterministically from `BackupSchedule.UID + scheduledAt` so HA operator replicas compute the same fire time.
- `cron: "H * * * *"` literal `H` substitution; result pinned in `status.lastSchedule.scheduledAt`.
- `runOnCreate: false` is the default (**G3**).
- `concurrencyPolicy: Forbid` is the default; skipped runs surface a condition rather than silently piling up (**G5/G18**).

### 4.2 Identity model

For a `BackupConfig` `C` in namespace `N` backing up PVC `P`:

- `username` defaults to `C.metadata.name`; `hostname` defaults to `N`.
- `sourcePath` defaults to `/pvc/<P>`; `sourcePathStrategy: PVCNamespacedName` is available for multi-namespace selectors.
- Resolved identity always appears in `BackupConfig.status.resolved.identity` and `Backup.status.identity`.

This is the part where a kopia-native operator can do better than volsync's accidental design — identity is the API, not an internal detail.

### 4.3 Repository sharing

Many `BackupConfig`s point at one `Repository`. Each writes under its own identity, so snapshots never collide. The repo is created lazily on first connect failure; the race is mediated by kopia's own object-store guarantees plus a per-repo lease in the operator. The `RESTIC_HOST="volsync"` anti-pattern doesn't apply.

### 4.4 Restore resolution & semantics

`Restore.spec.source` resolution at admission:

| Source mode | Resolution | `spec.repository` required? | Default `onMissingSnapshot` |
|---|---|---|---|
| `backupRef` | Look up the `Backup` CR; derive repository from it | No | `Fail` |
| `fromConfig` | Resolve identity from `BackupConfig`, query repo directly | No (derived from the BackupConfig) | **`Continue`** |
| `identity` | Direct kopia query | **Yes** | `Fail` |

**`fromConfig` + `Continue` is the deploy-or-restore pattern:** apply `Repository` + `BackupConfig` + `BackupSchedule` + `Restore` + workload PVC together. Fresh cluster against an existing repo → PVC restored from latest. Fresh cluster against empty repo → PVC binds empty, `BackupSchedule` starts producing `Backup`s under the same identity, and a future redeploy restores from there. No manifest changes between the two cases.

`writeFilesAtomically: true` is the default. `ignorePermissionErrors: true` is the default (and surfaces a condition if any errors occurred — non-silent).

### 4.5 Volume populator

To clarify the field's status on modern Kubernetes:

- `PVC.spec.dataSourceRef` is GA since 1.24 via the `AnyVolumeDataSource` feature gate (default-on).
- A populator controller (this operator) watches PVCs whose `dataSourceRef` references its kind and runs the `pvc-prime` + `claimRef`-rebind handshake.
- `kubernetes-csi/volume-data-source-validator` (which ships the `populator.storage.k8s.io/VolumePopulator` CRD) is **optional**. Without it, PVCs that mistype their populator ref hang `Pending`. With it, they're rejected at admission. The actual data-moving machinery works either way.

**Our position:** the populator path works on any cluster ≥ 1.24 without installing anything extra. If the `VolumePopulator` CRD is present at operator startup, we register ourselves for the better UX; if absent, we log it and carry on. No hard dependency.

This addresses **G10** by making the populator path uniform (passive `Restore`) and never gating it on copy-method.

### 4.6 Hooks

`hooks.beforeSnapshot[]` / `hooks.afterSnapshot[]` accept one of:

- `workloadExec` — `kubectl exec`-style into a matched workload pod/container (the default and most-requested form, fork **#22**).
- `runJob: { jobSpec: ... }` — full `JobSpec` to run as a one-shot Job (k8up-style `PreBackupPod` analog). Named `runJob` to make the materialization explicit.
- `httpRequest` — typed POST to a URL for cross-system orchestration.

Hook failures abort the backup by default; `continueOnFailure: true` is opt-in per hook.

### 4.7 Multi-PVC consistency

`groupBy: VolumeGroupSnapshot` is the default for multi-PVC sources. If the chosen `volumeSnapshotClass`'s driver doesn't support VGS, the `BackupConfig` reports a `GroupSnapshotUnsupported` condition and refuses to run. Silently falling back to per-PVC snapshots would mean inconsistent backups — the same data-integrity hazard as **#1211**. To intentionally accept per-PVC snapshots, set `groupBy: None` explicitly.

### 4.8 Observability

| Surface | Volsync | This operator |
|---|---|---|
| Per-PVC metrics labels | absent (`#518`) | always present (`pvc`, `pvc_namespace`, `backup_config`, `repository`) |
| Stale-metrics-on-delete | yes (`#1194`) | metrics are CR-scoped, deleted with the CR |
| `lastSuccessAt` | only derivable | `BackupSchedule.status.lastSuccessfulSchedule` first-class |
| Snapshot count | exec into pod | `kubectl get backup -l kopia.io/backup-config=...` |
| Logs | tail of last pod | small tail (~4KB) in `Backup.status.logTail`; full logs in Job pod |
| Repo storage stats | absent | `Repository.status.storageStats` |

SLO-friendly metrics:

- `kopia_operator_backup_last_success_timestamp_seconds{backup_config,namespace}` — gauge.
- `kopia_operator_backup_consecutive_failures{backup_config,namespace}` — gauge.
- `kopia_operator_restore_duration_seconds{...}` — summary (p50/p90/p99).

### 4.9 Security & RBAC

- Operator is namespaced by default; cluster-scoped install is opt-in via Helm value.
- Mover pods run as `runAsNonRoot: true`, `runAsUser: 65534` (nobody) by default. Files restored may not match original ownership — documented trade-off, not a hidden surprise.
- `mover.securityContext: {...}` — explicit override.
- `mover.inheritSecurityContextFrom: { podSelector }` — opt-in best-effort copy from a live consumer; fails loud (condition) if no pod matches at backup time. Not a default because the workload may be scaled to zero.
- `mover.privilegedMode: true` (namespace-gated by `kopia.io/allow-privileged-movers: "true"`) — runs with `CHOWN`/`FOWNER` for clean ownership restoration. Explicit double opt-in.
- `lost+found` and similar system entries are skipped by default (fork **#1033**/`#1889`).

### 4.10 Mover pods & failure handling

- Jobs use `restartPolicy: Never` and `backoffLimit: spec.failurePolicy.backoffLimit` (default `2`).
- `concurrencyPolicy: Forbid` default; missed slots produce a `BackupSkipped` condition.
- `activeDeadlineSeconds` default `7200`.
- Completed mover pods are reaped on the same reconcile that observes their terminal status — no zombie pods (fork **#8**).

### 4.11 Forward compatibility

Every credential, schedule, policy, and identity surface is a **sub-object** rather than a leaf field, so future fields slot in without changing the basic shape. Specifically deferred but accommodated:

- `Repository.spec.encryption.rotation` — password rotation flow.
- `Repository.spec.access.readOnly` — read-only repo mode for restore-only operators.
- `Repository.spec.backend.<type>.auth.workloadIdentity` — IRSA/WIF (already structurally present; deprioritized for the homelab default).
- `Backup.spec.parameters` — typed run-time overrides beyond just tags.

The `kopia.io` API group itself is `v1alpha1`; webhook conversions to `v1beta1`/`v1` will be additive only.

The **API-server-dependency-on-the-operator's-webhook** concern is bounded: webhooks intercept only `kopia.io/*` CRDs. PVC admission, populator dispatch, and in-flight restore reconciliation never depend on the webhook being up. Standard `failurePolicy: Fail` is appropriate.

---

## 5. Usage walkthroughs

### 5.1 Single PVC, scheduled daily

```yaml
apiVersion: v1
kind: Secret
metadata: { name: nas-primary-creds, namespace: backups }
stringData:
  AWS_ACCESS_KEY_ID: ...
  AWS_SECRET_ACCESS_KEY: ...
  KOPIA_PASSWORD: choose-something-long
---
apiVersion: kopia.io/v1alpha1
kind: Repository
metadata: { name: nas-primary, namespace: backups }
spec:
  backend:
    s3:
      bucket: my-backups
      prefix: prod/
      endpoint: s3.us-east-1.amazonaws.com
      region: us-east-1
      auth: { secretRef: { name: nas-primary-creds } }
  encryption: { passwordSecretRef: { name: nas-primary-creds, key: KOPIA_PASSWORD } }
  create: { enabled: true }
---
apiVersion: kopia.io/v1alpha1
kind: BackupConfig
metadata: { name: postgres-data, namespace: billing }
spec:
  repository: { name: nas-primary, namespace: backups }
  sources: [{ pvc: { name: postgres-data } }]
  retention: { keepDaily: 14, keepWeekly: 4 }
---
apiVersion: kopia.io/v1alpha1
kind: BackupSchedule
metadata: { name: postgres-data-nightly, namespace: billing }
spec:
  configRef: { name: postgres-data }
  schedule:
    cron: "H 2 * * *"
    jitter: 30m
    runOnCreate: false
```

Maintenance is implicit — a default `Maintenance` is created on first reference to a `Repository` unless one already exists.

### 5.2 Restore by picking a backup

```bash
kubectl get backup -n billing \
  -l kopia.io/backup-config=postgres-data \
  --sort-by=.status.startTime
```

```yaml
apiVersion: kopia.io/v1alpha1
kind: Restore
metadata: { name: postgres-restore-yesterday, namespace: billing }
spec:
  source:
    backupRef: { name: postgres-data-20260523-021300, namespace: billing }
  target:
    pvc:
      name: postgres-data-restored
      storageClassName: fast-ssd
      capacity: 100Gi
      accessModes: [ReadWriteOnce]
```

### 5.3 Multi-PVC selector

```yaml
apiVersion: kopia.io/v1alpha1
kind: BackupConfig
metadata: { name: app-bundle, namespace: billing }
spec:
  repository: { name: nas-primary, namespace: backups }
  identity: { username: app-bundle, hostname: billing }
  sources:
    - pvcSelector:
        labelSelector: { matchLabels: { backup: include } }
      sourcePathStrategy: PVCName
  groupBy: VolumeGroupSnapshot
  retention: { keepDaily: 14 }
```

### 5.4 Deploy-or-restore (GitOps)

The headline pattern. Apply everything together; on a fresh cluster against an existing repo, the PVC restores; on a fresh repo, it comes up empty and gets backed up going forward.

```yaml
apiVersion: kopia.io/v1alpha1
kind: BackupConfig
metadata: { name: postgres-data, namespace: billing }
spec:
  repository: { name: nas-primary, namespace: backups }
  sources: [{ pvc: { name: postgres-data } }]
---
apiVersion: kopia.io/v1alpha1
kind: BackupSchedule
metadata: { name: postgres-data-nightly, namespace: billing }
spec:
  configRef: { name: postgres-data }
  schedule: { cron: "H 2 * * *", jitter: 30m, runOnCreate: false }
---
apiVersion: kopia.io/v1alpha1
kind: Restore
metadata: { name: postgres-data-restore, namespace: billing }
spec:
  source: { fromConfig: { name: postgres-data, offset: 0 } }
  policy: { onMissingSnapshot: Continue }     # default for fromConfig — explicit here for clarity
  # No target — passive. The PVC below references this Restore.
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: postgres-data, namespace: billing }
spec:
  storageClassName: fast-ssd
  resources: { requests: { storage: 100Gi } }
  accessModes: [ReadWriteOnce]
  dataSourceRef:
    apiGroup: kopia.io
    kind: Restore
    name: postgres-data-restore
```

### 5.5 Manual one-shot backup

```yaml
apiVersion: kopia.io/v1alpha1
kind: Backup
metadata: { name: postgres-data-pre-migration, namespace: billing }
spec:
  configRef: { name: postgres-data }
  tags: { reason: "pre-schema-migration" }
```

Equivalently from any external system: Argo Events Sensor, Tekton Task, GitHub Actions, webhook handler. The `Backup` CR is the universal entry point.

### 5.6 Restore from a discovered (foreign / pre-install) backup

```bash
# Discovered Backups live in the Repository's namespace because the operator
# has no reliable way to attribute them to a BackupConfig.
kubectl get backup -n backups -l kopia.io/origin=discovered
```

```yaml
apiVersion: kopia.io/v1alpha1
kind: Restore
metadata: { name: rescue-restore, namespace: billing }
spec:
  source:
    backupRef: { name: kopia-disc-9c2a1f, namespace: backups }
  target: { pvc: { name: rescue-pvc, storageClassName: fast-ssd, capacity: 50Gi, accessModes: [ReadWriteOnce] } }
```

### 5.7 Suspending a schedule via GitOps

```yaml
spec:
  schedule:
    suspend: true        # apply via PR; un-suspend in a follow-up PR
```

In-flight `Backup`s are unaffected; only future cron firings are skipped.

---

## 6. Consequences

### 6.1 Positive

- Kopia-native ergonomics: identity, policy, hooks, snapshot listing all map to user mental models 1:1 with kopia.
- Three-layer triggering — any source can fire a `Backup`. Solves `#1559` and `#627` together.
- GitOps deploy-or-restore is a single-manifest pattern, not a runbook.
- `kubectl get backup` is the one place to look — runs, history, and the catalog all live there.
- Multi-PVC, multi-namespace, one-repo: first-class.
- VolumeGroupSnapshot on by default; degrades loudly, never silently.
- No bash mover scripts.

### 6.2 Negative / trade-offs

- Five CRDs to volsync's two — discoverability cost. Mitigated by hiding `Maintenance` from the typical first-time user flow (the simple case in §5.1 doesn't reference it explicitly) and by overloading `Backup` to cover both the "operator made this" and "we found this in the repo" cases.
- `origin: discovered` `Backup` CRs add etcd load. Mitigated by `Repository.spec.catalog.retain` bounds.
- Webhook resolution of `Restore.source.backupRef` means restoring a snapshot just outside the catalog window requires the raw-identity escape hatch. Documented.
- Kopia version pinning is a single choice baked into the operator image; mitigated by a `Repository.spec.kopiaImageOverride` escape hatch for advanced users.
- Coexists with volsync rather than supplanting it; users wanting rsync/syncthing keep volsync.

---

## 7. References

- VolSync upstream: <https://github.com/backube/volsync>
- VolSync kopia fork (perfectra1n): <https://github.com/perfectra1n/volsync>
- Kopia mover PR: <https://github.com/backube/volsync/pull/1723>
- Trigger redesign proposal: <https://github.com/backube/volsync/issues/1559>
- Kopia: <https://kopia.io/>
- CloudNativePG (Backup/ScheduledBackup pattern): <https://cloudnative-pg.io/documentation/current/backup/>
- KEP-1495 AnyVolumeDataSource: <https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/1495-volume-populators>
- `kubernetes-csi/volume-data-source-validator`: <https://github.com/kubernetes-csi/volume-data-source-validator>
- `kubernetes-csi/lib-volume-populator`: <https://github.com/kubernetes-csi/lib-volume-populator>

---

## Appendix A: Field-by-field comparison vs volsync

| Concern | volsync | this operator |
|---|---|---|
| Repo as a resource | Secret reference | `Repository` CRD |
| Triggering layers | one (`trigger` field on source) | three (`BackupConfig` / `Backup` / `BackupSchedule`) |
| Manual / external trigger | `trigger.manual: <value>` string-change | `kubectl create backup` (G17, `#1559`) |
| Multi-PVC | not supported | `pvcSelector` + `groupBy: VolumeGroupSnapshot` default |
| Multi-PVC consistency on unsupported drivers | n/a | fail loud (`GroupSnapshotUnsupported`); `groupBy: None` opts into per-PVC |
| First-sync skip | not supported (`#627`) | `runOnCreate: false` default |
| Cron jitter | not supported (`#1421`) | `jitter` + `H` substitution |
| Cron timezone | not supported (`#702`) | `schedule.timezone` |
| Schedule anchor | last-completion | wall-clock (CronJob-style) |
| Concurrency policy | implicit | `concurrencyPolicy: Forbid` default |
| BackoffLimit | hard-coded 8 | per-`Backup` `failurePolicy.backoffLimit` |
| Snapshot as a K8s object | absent | `Backup` CR (operator-initiated, manual, or discovered) |
| Restore by snapshot reference | no — only `restoreAsOf` | `source.backupRef` |
| Restore on a fresh cluster against an existing repo | manual runbook | `source.fromConfig` resolves via identity |
| Missing snapshot at restore | silently succeeds with empty PVC | `Fail` default; `Continue` for `fromConfig` (deploy-or-restore) |
| Maintenance | embedded in mover pod | own `Maintenance` CRD |
| Maintenance ownership | implicit | explicit lease + status |
| Snapshot catalog | exec into pod | `Backup` CRs (`origin: discovered`), bounded materialization |
| Hooks | shell-string in mover | typed `workloadExec` / `runJob` / `httpRequest` |
| Policy passthrough | restic flags only | typed `policy.*` + `extraArgs` escape hatch |
| Per-PVC metrics | absent (`#518`) | first-class labels |
| Stale metrics | observed (`#1194`) | metric-per-CR, cleared on delete |
| Mover image default tag | `:latest` | digest-pinned per operator release |
| Zombie mover pods | observed (fork `#8`) | reaped on terminal-status reconcile |
| Lost+found root files | break unprivileged restore (`#1033`) | skipped by default |
| Restore target modes | `destinationPVC` (in-place) OR populator | `target.pvcRef` / `target.pvc` / passive (populator) — uniform |
| Populator dependency | requires reading the docs to know | clarified: works on any 1.24+ cluster; volume-data-source-validator is an optional UX nicety |
