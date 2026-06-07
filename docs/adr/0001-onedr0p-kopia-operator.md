# ADR-0001: A Kopia-Native Backup Operator for Kubernetes

- **Status:** Proposed
- **Date:** 2026-05-24
- **Inspired by:** [`backube/volsync`](https://github.com/backube/volsync) and the kopia fork [`perfectra1n/volsync`](https://github.com/perfectra1n/volsync) (especially PR [`backube/volsync#1723`](https://github.com/backube/volsync/pull/1723) and the trigger-redesign proposal [`backube/volsync#1559`](https://github.com/backube/volsync/issues/1559)). The triggering model also draws on [CloudNativePG](https://cloudnative-pg.io/) (`Cluster` / `ScheduledBackup` / `Backup`) and Tekton (`Task` / `TaskRun`).

> Scope: this ADR covers **CRD shape, user experience, and high-level design choices**. It deliberately does not specify Go package layout, controller-runtime indexes, leader-election lease IDs, the cron library, or other implementation mechanics — those belong to follow-up ADRs once the API surface is agreed.

---

## 1. Context

VolSync is the de-facto Kubernetes-native mover for PVCs. Its design is mature and battle-tested, but it has accreted around restic's model. As soon as you try to add a non-restic mover (kopia, rustic, borg, …) several deep design choices push back. The community fork `perfectra1n/volsync` proves out a kopia mover and ships a usable image — but its PR has been open ~13 months without merging, the upstream maintainers are capacity-constrained, and many users have switched to running the fork in production.

The fork's existence and the volume of feature requests around kopia/restic locking, multi-PVC backup, scheduling jitter, restore UX, trigger separation, snapshot lifecycle, and "stop running on apply" suggest something stronger than "land kopia in volsync" is warranted. A **kopia-native operator** can:

1. Drop the multi-mover abstraction entirely. Kopia is the only mover, so every CRD field can be expressive without leaking through a generic shape.
2. Make a repository a first-class Kubernetes resource — at both namespace and cluster scope. Kopia repos are designed to be shared across many writers, including across namespaces.
3. Separate **recipe**, **invocation**, and **schedule** so backups can be triggered by any source (cron, `kubectl create`, Argo Events, button-in-Grafana). Volsync's `trigger` field couples all three.
4. Use kopia's native identity model (`username@hostname:path`) deliberately rather than as an accident of `metadata.name`/`metadata.namespace`.
5. Treat `kopia maintenance` and snapshot lifecycle as first-class operator concerns rather than retrofits.
6. **Tie the lifecycle of a Kopia snapshot to the lifecycle of its `Backup` CR** by default, with explicit opt-outs — addressing the persistent volsync confusion that deleting a `ReplicationSource` has no effect on snapshots in the repository.
7. Surface kopia's snapshot catalog through CRDs so restore is "browse and reference," not "construct an `restoreAsOf` timestamp and hope."
8. Address the long backlog of papercuts as design decisions, not bug fixes.

We refer to the project as **`kopia-operator`** in this document; final naming is out of scope. The API group is **`kopia.io`** with initial version `v1alpha1`.

### 1.1 The most important gaps we are addressing

| #   | Gap                                                                               | volsync refs                                 |
| --- | --------------------------------------------------------------------------------- | -------------------------------------------- |
| G1  | Repository is not a Kubernetes resource; cannot be shared/reused cleanly          | implicit; perfectra1n CRD shape              |
| G2  | One `ReplicationSource` = one PVC                                                 | `#1115`, `#1116`, `#320`                     |
| G3  | First reconcile triggers an immediate backup, no GitOps-friendly "skip first run" | `#627`                                       |
| G4  | No cron jitter / `H` substitution, no timezone                                    | `#1421`, `#702`                              |
| G5  | Restic repo locking / piling-up jobs                                              | `#1042`, `#1429`, `#646`                     |
| G6  | No retry-limit / backoffLimit override                                            | `#1228`, `#1042`                             |
| G7  | Restore proceeds with empty PVC if no snapshot found                              | `#1211`                                      |
| G8  | Snapshot selection is restic-format `restoreAsOf` only; no browse                 | `#7`, `#1211`                                |
| G9  | `latestImage` always wins — no immutable restore source                           | `disc #1115`                                 |
| G10 | Volume populator + Direct copyMethod incompatibility                              | `disc #1115`, `#1129`                        |
| G11 | Maintenance ownership is implicit & runs in the same pod as backup                | perfectra1n fork redesigned this three times |
| G12 | Policy passthrough is brittle: every kopia knob needs CRD/jq script changes       | fork `#13`, `#23`                            |
| G13 | Snapshot actions run in mover, not workload                                       | fork `#22`                                   |
| G14 | OOMs unpredictable; no resource guidance                                          | `#626`, `#707`, `#1228`                      |
| G15 | Mover image is `:latest` by default                                               | volsync `restic/builder.go:42`               |
| G16 | Restricted PSA / OpenShift SCC / unprivileged-mode `lost+found` papercuts         | `#367`, `#1033`, `#1889`, `#1430`            |
| G17 | Trigger semantics are baked into the source CR — no manual/external trigger path  | `#1559`                                      |
| G18 | Mover-pod lifecycle (zombie pods, stuck jobs)                                     | fork `#8`, volsync `#1415`                   |
| G19 | Maintainers' explicit door-closing on new movers                                  | `#1743`, `#1029`, `#320`                     |
| G20 | Deleting the source CR doesn't delete snapshots from the repository               | implicit                                     |

---

## 2. Decision

### 2.1 Topology

Seven CRDs in `kopia.io/v1alpha1`. Six are namespaced; **`ClusterRepository`** is cluster-scoped.

| CRD                     | Scope      | Layer                | Purpose                                                                                                                                                                                                                                        |
| ----------------------- | ---------- | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`Repository`**        | Namespaced | Storage              | A kopia repository owned by one namespace: credentials, backend, encryption, optional catalog-materialization bounds. Many `BackupConfig`s/`Restore`s reference one.                                                                           |
| **`ClusterRepository`** | Cluster    | Storage              | A shared kopia repository operated by the platform team, referenceable from allow-listed namespaces. Identity defaults are templated per consumer namespace.                                                                                   |
| **`BackupConfig`**      | Namespaced | Recipe               | _What_ to back up: PVC selector, identity, retention, policy, hooks. Idempotent — doesn't run anything on its own.                                                                                                                             |
| **`Backup`**            | Namespaced | Invocation + Catalog | A single kopia snapshot as a Kubernetes object. Created by `BackupSchedule`, `kubectl create`, or any other trigger source. Also materialized by the operator from the kopia catalog for snapshots it didn't produce (foreign or pre-install). |
| **`BackupSchedule`**    | Namespaced | Cron                 | _When_ it runs: cron (with jitter + timezone) + `configRef`. Creates `Backup` CRs.                                                                                                                                                             |
| **`Restore`**           | Namespaced | Operation            | A restore from a snapshot/identity to a PVC. Used directly, or referenced by `PVC.spec.dataSourceRef`.                                                                                                                                         |
| **`Maintenance`**       | Namespaced | Lifecycle            | One per `Repository`/`ClusterRepository`: schedules `kopia maintenance run` quick + full, manages ownership lease.                                                                                                                             |

The three-layer split (recipe / invocation / schedule) for backups is the deliberate response to volsync `#1559`. It means:

- A `Backup` can be created from anywhere — `kubectl create`, Argo Events, a Tekton pipeline, a webhook handler.
- A `BackupSchedule` is just one source of `Backup` CRs. Removing or pausing a schedule does not affect already-running or already-completed runs.
- A `BackupConfig` change applies to subsequent invocations; the operator snapshots resolved values into each `Backup.status.resolved...` for traceability.

`Backup` is also the single canonical representation of a kopia snapshot — both ones we produced and ones we discover in the repo. Three retention drivers cover the lifecycle:

- **`BackupConfig.spec.retention`** (GFS — `keepLatest`/`keepHourly`/`keepDaily`/...) is the primary mechanism. The operator periodically computes the retention set for each `(BackupConfig identity, source)` tuple and deletes `Backup` CRs outside it. Each deleted CR's `deletionPolicy` determines whether the underlying kopia snapshot goes with it. Details in §4.4.
- **`BackupSchedule.spec.failedJobsHistoryLimit`** bounds _failed_ `Backup` CRs from a schedule (GFS doesn't apply to failures).
- **`Repository.spec.catalog.retain` / `ClusterRepository.spec.catalog.retain`** bounds the `origin: discovered` `Backup` CR set, keeping etcd footprint sane for large repos. Discovered `Backup`s always have `deletionPolicy: Retain` so this never deletes real snapshots (§4.5).

Manual `Backup` CRs (`origin: manual` with no schedule parent) are user-owned and not auto-GC'd; their snapshots are tied to their `deletionPolicy`.

Dedup key is `(Repository.UID, kopiaSnapshotID)` — the operator will not create a discovered `Backup` for a snapshot already represented by an operator-initiated one.

Restore stays as a single CR (it's an operation, not a recurring thing). For the `dataSourceRef`-driven populator pattern, a `Restore` is left in passive mode (no `target`) and consumed by zero-or-more PVCs.

### 2.2 Anchoring principles

1. **Repositories are objects, at both namespace and cluster scope.** Identity, lifecycle, maintenance, and tenancy gating hang off them.
2. **Triggering is decoupled.** `BackupConfig` says _what_; `Backup` says _that_; `BackupSchedule` says _when_. Any of the three can be authored or automated independently.
3. **A `Backup` is a kopia snapshot.** Operator-initiated, manually-applied, and discovered snapshots are all the same kind.
4. **A `Backup` CR owns the lifecycle of its kopia snapshot by default.** Deleting the CR deletes the snapshot from the repository, governed by a `deletionPolicy` field. Discovered backups are forced to `Retain` so the operator never deletes data it didn't create.
5. **Restores are explicit.** No silent "empty PVC because no snapshots existed yet" by default. The "deploy-or-restore" GitOps pattern is opt-in via a specific source mode + `onMissingSnapshot: Continue`.
6. **Maintenance is a first-class lifecycle concern**, with its own CRD and explicit ownership lease.
7. **The mover is a thin shim.** A Go-native controller invokes `kopia --json` and parses results. No 2,500-line bash scripts. The image carries `kopia` and nothing else.
8. **Validation is webhook-enforced.** Mutually exclusive fields, missing repository references, malformed schedules, cross-tenant references — rejected at admission.
9. **Identity is explicit and overridable.** Defaults derive from object name/namespace; every component is overridable; the resolved identity always appears in `status`.
10. **Forward-compatible by construction.** Every credential, policy, and rotation surface is a sub-object, so future fields slot in without API breakage (see §4.13).

### 2.3 Where Backup CRs live

| Origin                                                        | Namespace                                                                                                                                                                                                                                                         |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `operator` — created by `BackupSchedule`                      | The `BackupConfig`'s namespace (so the owning team sees their backups with `kubectl get backup -n <team>`).                                                                                                                                                       |
| `manual` — created by `kubectl create` or external automation | Whichever namespace the user applies it to. The `configRef` may cross namespaces, subject to RBAC.                                                                                                                                                                |
| `discovered` — materialized from the kopia catalog            | The `Repository`'s namespace, or — for snapshots discovered under a `ClusterRepository` — the namespace named in the snapshot's identity, if it exists and is in the `allowedNamespaces` set. Falls back to a configurable `catalog.fallbackNamespace` otherwise. |

`Restore.spec.source.backupRef` carries `{ name, namespace }` for cross-namespace references.

---

## 3. CRD Design

### 3.1 `Repository`

Owns credentials, encryption, and repository-wide settings for a single namespace. Catalog materialization for discovered `Backup` CRs is configured here.

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
                    name: nas-primary-creds # keys: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, ...
                # Optional advanced auth — workloadIdentity supported but not the default.
                # workloadIdentity:
                #   serviceAccountName: kopia-s3
            tls:
                caBundleRef:
                    configMapName: corp-ca
                    key: ca.crt
                insecureSkipVerify: false

    encryption:
        passwordSecretRef: # always a Secret ref; never inline
            name: nas-primary-creds
            key: KOPIA_PASSWORD
        # Future fields (rotation, previousPasswords, ...) slot in here.

    create:
        enabled: true # if repo missing, create it
        encryption: AES256-GCM-HMAC-SHA256
        splitter: DYNAMIC-4M-BUZHASH
        hash: BLAKE3-256

    cacheDefaults: # inherited by Backup/Restore unless overridden
        capacity: 8Gi
        storageClassName: fast-ssd
        metadataCacheSizeMB: 5000
        contentCacheSizeMB: 2000

    catalog: # bounds materialization of `origin: discovered` Backup CRs
        retain:
            perIdentity: 100 # most recent N per username@hostname:path
            maxAgeDays: 90 # nothing older than this gets a Backup CR
        refreshInterval: 5m
        # Older snapshots remain in kopia; restorable via Restore.source.identity.snapshotID

status:
    phase: Ready # Pending | Initializing | Ready | Degraded | Failed
    observedGeneration: 7
    uniqueID: "fb6e...c41a" # kopia repo unique ID
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
        discoveredBackupCount: 412 # how many Backup CRs materialized from the catalog scan
        lastRefreshAt: 2026-05-24T17:01:11Z
```

**Why:** addresses **G1** (repo as a resource), **G15** (digest pinning belongs on the operator image, not embedded per recipe), and provides the catalog-bounds knob that keeps `Backup` CRs from blowing up etcd while still giving the K8s-native view of kopia history. `encryption` is a sub-object so future rotation fields fit without API breakage (**§4.13**).

### 3.2 `ClusterRepository`

The cluster-scoped counterpart for shared infrastructure repositories operated by a platform team. Same spec surface as `Repository`, plus tenancy gating and per-namespace identity templating.

```yaml
apiVersion: kopia.io/v1alpha1
kind: ClusterRepository
metadata:
    name: shared-primary
spec:
    # Same backend/encryption/cacheDefaults/create/catalog blocks as Repository.
    backend:
        s3:
            bucket: org-kopia-repo
            prefix: "" # bucket root maximizes dedup across tenants
            endpoint: s3.us-east-1.amazonaws.com
            region: us-east-1
            auth:
                secretRef:
                    name: kopia-platform-creds
                    namespace: kopia-system # REQUIRED on cluster-scoped CRs
    encryption:
        passwordSecretRef:
            name: kopia-platform-creds
            namespace: kopia-system # REQUIRED
            key: KOPIA_PASSWORD
    create:
        enabled: true
        encryption: AES256-GCM-HMAC-SHA256

    # Tenancy gate — webhook-enforced on every consumer CR.
    allowedNamespaces:
        # Exactly one of:
        list: [production, staging, billing]
        # selector:
        #   matchLabels: { kopia.io/tier: enterprise }
        # all: true

    # Identity defaults applied when consumers don't override.
    identityDefaults:
        hostnameTemplate: "{{ .Namespace }}"
        usernameTemplate: "{{ .Namespace }}-{{ .ConfigName }}"

    catalog:
        retain:
            perIdentity: 50
            maxAgeDays: 60
        refreshInterval: 5m
        # Where to materialize discovered Backup CRs whose identity hostname
        # does not match an allowed namespace.
        fallbackNamespace: kopia-system

status:
    phase: Ready
    uniqueID: "0a91...8a3f"
    allowedNamespaceCount: 3
    conditions:
        - type: Connected
          status: "True"
        - type: TenancyEnforced
          status: "True"
```

Consumer CRDs (`BackupConfig`, `Backup`, `Restore`, `Maintenance`) accept a discriminated `repository` reference:

```yaml
repository:
    kind: ClusterRepository # Repository (default) | ClusterRepository
    name: shared-primary
    # namespace: ...                      # ignored when kind=ClusterRepository
```

The validating admission webhook rejects a consumer CR whose namespace is not in the `ClusterRepository.spec.allowedNamespaces` set. This avoids the "secret accessible from any namespace" anti-pattern and gives platform teams a single object tenants can't shadow.

**Why:** the cross-namespace `Repository` ref pattern covers most cases but has two real shortcomings — tenants can create their own `Repository` with the same name as a platform one (no shadow protection), and tenancy is expressed in RBAC rules rather than as a first-class allow list. `ClusterRepository` fixes both. The shared-prefix backend layout (`prefix: ""`) also maximizes deduplication across all tenant namespaces, which is the operational reason platform teams want a shared repo in the first place.

### 3.3 `BackupConfig`

The recipe. Idempotent. Apply once; reference from many `Backup`s or one `BackupSchedule`.

```yaml
apiVersion: kopia.io/v1alpha1
kind: BackupConfig
metadata:
    name: postgres-data
    namespace: billing
spec:
    repository:
        kind: Repository # Repository | ClusterRepository
        name: nas-primary
        namespace: backups # cross-ns Repository; ignored for ClusterRepository

    # Identity — what kopia sees. Defaults shown.
    # For ClusterRepository consumers, the repository's identityDefaults templates apply
    # unless overridden here.
    identity:
        username: "postgres-data" # default: <BackupConfig.metadata.name>
        hostname: "billing" # default: <BackupConfig.metadata.namespace>

    # Sources — what to back up.
    sources:
        - pvc: { name: postgres-data }
          sourcePathOverride: /data # what kopia records (default: /pvc/<name>)
        # Or a selector for multi-PVC:
        # - pvcSelector:
        #     namespaceSelector: { matchNames: [billing, billing-staging] }
        #     labelSelector: { matchLabels: { backup: include } }
        #   sourcePathStrategy: PVCName     # PVCName | PVCNamespacedName

    copyMethod: Snapshot # Snapshot (default, PiT) | Clone | Direct
    volumeSnapshotClassName: csi-snap-class
    groupBy: VolumeGroupSnapshot # default for multi-PVC sources; None opts into per-PVC

    retention: # GFS — enforced by operator pruning Backup CRs (§4.4)
        keepLatest: 10
        keepHourly: 24
        keepDaily: 14
        keepWeekly: 8
        keepMonthly: 12
        keepAnnual: 5

    # Default deletion policy for Backup CRs created against this config.
    # Per-Backup override available on Backup.spec.deletionPolicy.
    defaultDeletionPolicy: Delete # Delete | Retain | Orphan

    policy: # typed fields — not opaque JSON parsed by jq
        compression:
            compressor: zstd
            neverCompress: ["*.zip", "*.gz", "*.mp4"]
        splitter: DYNAMIC-4M-BUZHASH
        ignore:
            paths: ["*.tmp", "*/cache/*", "lost+found"]
            cacheDirs: true # honor CACHEDIR.TAG
            ignoreIdenticalSnapshots: true # fork issue #13
        extraArgs: [] # escape hatch for kopia flags we don't model yet

    hooks: # G13 — runs in the workload, not the mover
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

    mover: # per-recipe overrides
        resources:
            requests: { cpu: 250m, memory: 512Mi }
            limits: { cpu: "2", memory: 4Gi }
        cache:
            capacity: 16Gi
            storageClassName: fast-ssd
        securityContext: {} # override; default: nonRoot uid 65534
        # privilegedMode: true              # opt-in, namespace-gated; preserves UID/GID on restore
        # inheritSecurityContextFrom:       # opt-in: copy SC from a live workload pod
        #   podSelector: { matchLabels: { app: postgres } }

status:
    resolved: # what would be passed to kopia
        identity:
            username: "postgres-data"
            hostname: "billing"
            sources:
                - pvc: billing/postgres-data
                  sourcePath: /data
    retention:
        activeBackupCount: 47 # CRs currently inside the GFS window
        lastPruneAt: 2026-05-24T03:00:00Z
        lastPruneDeleted: 2
    conditions:
        - type: RepositoryReachable
          status: "True"
        - type: GroupSnapshotSupported
          status: "True"
```

**Why:** addresses **G2** (selector + VolumeGroupSnapshot default), **G12** (typed policy + escape hatch), **G13** (hook types), **G14** (explicit resource defaults), **G16** (security-context controls without forcing privileged-by-default). The identity sub-object makes the second-biggest perfectra1n papercut (fork **#7**) impossible.

### 3.4 `Backup`

A single kopia snapshot as a Kubernetes object. Three origins:

- `operator` — created by a `BackupSchedule`. Spec has `configRef`; lives in the `BackupConfig`'s namespace.
- `manual` — created by `kubectl create` or external automation. Spec has `configRef`; lives wherever the user applied it.
- `discovered` — materialized by the operator's catalog scan for snapshots it didn't produce. Spec is empty/absent; lives in the `Repository`'s namespace (see §2.3).

```yaml
apiVersion: kopia.io/v1alpha1
kind: Backup
metadata:
    name: postgres-data-20260524-021300
    namespace: billing
    finalizers:
        - kopia.io/snapshot-cleanup # §4.5
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
    failurePolicy: # G6 — per-run, not hard-coded
        backoffLimit: 2
        activeDeadlineSeconds: 7200

    # Lifecycle of the underlying kopia snapshot when this CR is deleted.
    # Defaults are origin-aware (§4.5):
    #   operator:    Delete (or inherits BackupConfig.spec.defaultDeletionPolicy)
    #   manual:      Delete (or inherits BackupConfig.spec.defaultDeletionPolicy)
    #   discovered:  Retain (FORCED — webhook rejects other values)
    deletionPolicy: Delete # Delete | Retain | Orphan

status:
    phase: Succeeded # Pending | Running | Succeeded | Failed | Deleting | Discovered
    origin: operator # operator | manual | discovered — canonical
    snapshot: # the kopia artifact
        kopiaSnapshotID: k1f1ec0a8
        identity:
            username: "postgres-data"
            hostname: "billing"
            sourcePath: /data
    timing:
        startTime: 2026-05-24T02:13:00Z
        endTime: 2026-05-24T02:18:42Z
        durationSeconds: 342
    stats: # populated from kopia's JSON output
        sizeBytes: 4321098765
        bytesNew: 12345678
        filesNew: 1233
        filesModified: 22
        filesUnchanged: 998111
    job: # operator/manual only; absent for discovered
        name: backup-postgres-data-20260524-021300
        attempts: 1
    resolved: # frozen recipe values at run time (operator/manual)
        repository: { kind: Repository, name: nas-primary, namespace: backups }
        sources:
            - pvc: billing/postgres-data
              sourcePath: /data
    conditions:
        - type: SourcesQuiesced
          status: "True"
        - type: SnapshotCreated
          status: "True"
    logTail: | # capped at ~4KB; full logs in the Job pod
        Snapshot created: k1f1ec0a8
        Total bytes: 4321098765
```

`kubectl get backup` shows everything — runs in flight, historical successes, failed attempts, and the discovered catalog — distinguished by the `kopia.io/origin` label and `status.phase`.

**Spec immutability.** The validating webhook freezes `spec` once `status.phase != Pending`, with two exceptions:

- `spec.deletionPolicy` and `spec.failurePolicy` remain editable post-completion (users may decide after the fact to retain a snapshot, or extend a retry budget).
- Discovered `Backup`s have no spec to mutate; only `deletionPolicy: Retain` is permitted via the webhook.

**Why:** addresses **G17** (invocations as first-class, any trigger source) and **G20** (snapshot lifecycle = CR lifecycle, configurable). Folds in the catalog representation cleanly; restores reference one kind of thing. Logs are bounded; full logs live in the Job pod where users expect them.

### 3.5 `BackupSchedule`

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
        cron: "H 2 * * *" # G4 — Jenkins-style 'H' substitution
        jitter: 30m # deterministic; see §4.1
        timezone: "America/Los_Angeles"
        runOnCreate: false # G3 — GitOps-friendly default
        suspend: false
        concurrencyPolicy: Forbid # Forbid | Allow | Replace — G18
        startingDeadlineSeconds: 600
    failedJobsHistoryLimit:
        3 # successful Backup retention is governed by
        # BackupConfig.spec.retention (§4.4)

status:
    lastSchedule:
        scheduledAt: 2026-05-24T02:13:00Z # cron + jitter, pinned (predictable for alerting)
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

Note the deliberate absence of `successfulJobsHistoryLimit`: successful retention is GFS-driven on `BackupConfig.spec.retention`, not flat-count on the schedule. See §4.4 for the rationale.

**Why:** mirrors `CronJob` semantics for the parts that matter (G4, G18). Schedule anchoring is wall-clock (`cron(now)`), not `cron(lastSyncTime)` — fixes volsync's drift behavior. Pinned `scheduledAt` lets ops alerts say "you missed the 02:13 slot" without ambiguity.

### 3.6 `Restore`

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
    # repository: { kind: Repository, name: nas-primary, namespace: backups }

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
        onMissingSnapshot: Fail # default for explicit sources (backupRef/identity)
        # For source.fromConfig the default is Continue (see §4.6 for the deploy-or-restore pattern).
        waitTimeout: 5m

status:
    phase: Restoring # Pending | Resolving | Restoring | Completed | Failed
    resolved: # pinned at admission
        backupRef: { name: postgres-data-20260524-021300, namespace: billing }
        repository: { kind: Repository, name: nas-primary, namespace: backups }
        pinnedAt: 2026-05-24T17:33:11Z
        identity:
            username: postgres-data
            hostname: billing
            sourcePath: /data
    target: # what the operator is writing into
        pvcPrime: pvc-prime-9f8e2c1b # populator handshake (passive / pvc-create modes)
        pvcRef: { name: postgres-data-restored }
    timing:
        startTime: 2026-05-24T17:33:14Z
    progress:
        bytesRestored: 8123456789
        filesRestored: 998111
```

**Why:** addresses **G7** (fail-closed defaults), **G8/G9** (admission-time resolution, no drift on re-apply), **G10** (single restore path covers populator and in-place uniformly). Three source modes cover the spectrum: K8s-native reference (`backupRef`), recipe-driven (`fromConfig` — the GitOps deploy-or-restore pattern), and raw kopia identity (foreign writers, aged-out catalog). `spec.repository` is derivable for the first two; required only when raw identity is the source.

### 3.7 `Maintenance`

```yaml
apiVersion: kopia.io/v1alpha1
kind: Maintenance
metadata: { name: nas-primary, namespace: backups }
spec:
    repository:
        kind: Repository # Repository | ClusterRepository
        name: nas-primary
    schedule:
        quick: { cron: "0 */6 * * *", jitter: 30m }
        full: { cron: "0 3 * * 0", jitter: 1h }
        timezone: UTC
    ownership:
        owner: "kopia-operator/nas-primary"
        takeoverPolicy: PromptCondition # Never | PromptCondition | Force
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
        lastContentReclaimedBytes: 1234567
    full:
        lastRunAt: 2026-05-19T03:01:42Z
        nextScheduledAt: 2026-05-26T03:00:00Z
        consecutiveFailures: 0
        lastContentReclaimedBytes: 89456789012
    conditions:
        - type: OwnershipClaimed
          status: "True"
```

**Why:** at most one `Maintenance` per `Repository`/`ClusterRepository` (webhook-enforced) — kills the perfectra1n cross-namespace first-writer-wins race by making the conflict unrepresentable. `lastContentReclaimedBytes` is the _only_ place storage reclamation is surfaced; per-`Backup` deletion only marks manifests for GC (§4.5).

---

## 4. Key behaviors

### 4.1 Scheduling

- CronJob-style wall-clock anchoring (`cron(now)`), not last-completion anchoring. Fixes volsync's drift.
- `jitter` is **deterministic**, derived from `BackupSchedule.UID + base scheduledAt`. HA operator replicas compute identical fire times without coordination; controller restarts re-derive the same value without persisting it. No "re-roll on restart" hazard.
- `cron: "H * * * *"` literal `H` substitution; result pinned in `status.lastSchedule.scheduledAt`.
- `runOnCreate: false` is the default (**G3**).
- `concurrencyPolicy: Forbid` is the default; skipped runs surface a condition rather than silently piling up (**G5/G18**).
- Validating webhook parses the cron expression with the same parser the controller uses at runtime — bad expressions rejected at apply time, not at first reconcile.

### 4.2 Identity model

For a `BackupConfig` `C` in namespace `N` backing up PVC `P`:

- `username` defaults to `C.metadata.name`; `hostname` defaults to `N`.
- For `ClusterRepository` consumers, the repository's `identityDefaults` templates apply unless `BackupConfig.spec.identity` overrides them.
- `sourcePath` defaults to `/pvc/<P>`; `sourcePathStrategy: PVCNamespacedName` is available for multi-namespace selectors.
- Resolved identity always appears in `BackupConfig.status.resolved.identity` and `Backup.status.snapshot.identity`.

This is the part where a kopia-native operator can do better than volsync's accidental design — identity is the API, not an internal detail.

### 4.3 Repository sharing

Many `BackupConfig`s point at one `Repository` or `ClusterRepository`. Each writes under its own identity, so snapshots never collide. The repo is created lazily on first connect failure; the race is mediated by kopia's own object-store guarantees plus a per-repo lease in the operator. The `RESTIC_HOST="volsync"` anti-pattern doesn't apply.

For `ClusterRepository`, the `allowedNamespaces` gate is enforced at admission on `BackupConfig`, `Backup` (manual), `Restore`, and `Maintenance`. A namespace that loses its allow-list entry retains its existing `Backup` CRs (no retroactive deletion) but cannot create new ones.

### 4.4 Retention enforcement

`BackupConfig.spec.retention` is the **only** retention mechanism for successful operator-initiated and manual `Backup`s. It is enforced operator-side by pruning `Backup` CRs; each pruned CR's `deletionPolicy` then drives what happens to the underlying snapshot.

**Algorithm.** On every `Backup` completion under a `BackupConfig`, and on a periodic timer per `BackupConfig`:

1. List all `Backup` CRs in the operator's cache where `status.resolved.repository` and `status.snapshot.identity` match this `BackupConfig`'s resolved values, and `origin ∈ {operator, manual}`.
2. Sort by `status.timing.endTime` descending.
3. Apply the GFS retention buckets in order: `keepLatest`, `keepHourly`, `keepDaily`, `keepWeekly`, `keepMonthly`, `keepAnnual`. A `Backup` qualifies for a bucket if its `endTime` is the most recent within that bucket's window.
4. Any `Backup` not selected by any bucket is deleted.
5. Deletion runs through the standard `kopia.io/snapshot-cleanup` finalizer (§4.5).

**Failed `Backup`s** are governed by `BackupSchedule.spec.failedJobsHistoryLimit` (operator-origin failures) or are user-managed (manual-origin failures). They are not subject to GFS.

**Discovered `Backup`s** are governed by `Repository.spec.catalog.retain` / `ClusterRepository.spec.catalog.retain`. When a discovered CR ages out of the catalog window, the operator deletes the CR; the forced `deletionPolicy: Retain` ensures the underlying snapshot remains in the repository (§4.5).

**Exclusivity with Kopia-side retention policies.** The operator does not invoke `kopia policy set --keep-*`. Repository-level retention policies set by users running the `kopia` CLI directly against an operator-managed repository will conflict with CR-driven retention and may cause double-deletion. The validating webhook on `Repository` rejects inline policy fields that would set retention at the repo level. This is documented as unsupported.

**Why not also a flat-count cap on `BackupSchedule`?** Two retention drivers for the same set of objects creates rule-precedence questions and surprises (a flat cap can silently undercut a GFS policy that should have retained an annual snapshot). GFS alone, enforced consistently, is the simpler model. Users who want a hard cap can set `keepLatest` low.

### 4.5 Backup deletion semantics

A `Backup` CR owns the lifecycle of its kopia snapshot by default. The finalizer `kopia.io/snapshot-cleanup` is added to every `Backup` at admission and is the load-bearing mechanism.

**`deletionPolicy` defaults by origin:**

| Origin       | Default                                                              | Other values allowed?                      |
| ------------ | -------------------------------------------------------------------- | ------------------------------------------ |
| `operator`   | `Delete` (inherits `BackupConfig.spec.defaultDeletionPolicy` if set) | Yes                                        |
| `manual`     | `Delete` (inherits `BackupConfig.spec.defaultDeletionPolicy` if set) | Yes                                        |
| `discovered` | `Retain`                                                             | **No** — webhook rejects `Delete`/`Orphan` |

The discovered restriction prevents data loss: the operator did not create those snapshots and may not be the only writer. Aging out a discovered `Backup` CR via `catalog.retain` is a Kubernetes-side cleanup, not a repository-side one.

**Behaviour on CR deletion:**

| `deletionPolicy` | Action                                                                                                                                                                       | Final state                                                                                                              |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `Delete`         | Operator spawns a one-shot mover pod that runs `kopia snapshot delete --delete <kopiaSnapshotID>`. On success, the finalizer is removed and the CR disappears.               | Manifest deleted; content reclaimed at next maintenance run.                                                             |
| `Retain`         | Finalizer is removed immediately; CR disappears.                                                                                                                             | Snapshot remains in the repository, still discoverable via the catalog (and may rematerialize as `origin: discovered`).  |
| `Orphan`         | Operator removes tracking labels (`kopia.io/backup-config`, `kopia.io/identity-hash`, …) so the snapshot is no longer surfaced under this config. Finalizer is then removed. | Snapshot remains; will be visible only via raw identity or as a discovered backup if it falls inside the catalog window. |

**Failure during `Delete`.** If `kopia snapshot delete` fails, the CR stays in `phase: Deleting` with a `SnapshotDeletionFailed` condition and an exponential-backoff retry. The CR is **not** silently dropped — operators want to see "your snapshot wasn't actually deleted."

**Force-delete escape hatch.** When the repository is unreachable and the user needs the CR gone:

```bash
kubectl annotate backup postgres-data-20260524-021300 \
  kopia.io/skip-snapshot-cleanup=true --overwrite
kubectl delete backup postgres-data-20260524-021300
```

The annotation causes the finalizer to remove itself without running the delete pod. The controller emits a warn-level log line and a `SnapshotOrphaned` Event recording the kopia snapshot ID for audit. The same annotation works on stuck `Maintenance` runs.

**Manifest deletion vs. content reclamation.** Kopia marks manifests deleted immediately, but on-disk content is reclaimed only during maintenance. The model honors this:

- `Backup.status.phase` transitions `Succeeded → Deleting → (CR removed)`.
- The byte-level storage drop appears on `Maintenance.status.{quick,full}.lastContentReclaimedBytes`, never on any `Backup` field.

This asymmetry is called out in user-facing documentation because it is the kind of thing that causes "I deleted the backup, why is my bucket the same size?" support questions.

### 4.6 Restore resolution & semantics

`Restore.spec.source` resolution at admission:

| Source mode  | Resolution                                                | `spec.repository` required?        | Default `onMissingSnapshot` |
| ------------ | --------------------------------------------------------- | ---------------------------------- | --------------------------- |
| `backupRef`  | Look up the `Backup` CR; derive repository from it        | No                                 | `Fail`                      |
| `fromConfig` | Resolve identity from `BackupConfig`, query repo directly | No (derived from the BackupConfig) | **`Continue`**              |
| `identity`   | Direct kopia query                                        | **Yes**                            | `Fail`                      |

**`fromConfig` + `Continue` is the deploy-or-restore pattern:** apply `Repository` + `BackupConfig` + `BackupSchedule` + `Restore` + workload PVC together. Fresh cluster against an existing repo → PVC restored from latest. Fresh cluster against empty repo → PVC binds empty, `BackupSchedule` starts producing `Backup`s under the same identity, and a future redeploy restores from there. No manifest changes between the two cases.

`writeFilesAtomically: true` is the default. `ignorePermissionErrors: true` is the default (and surfaces a condition if any errors occurred — non-silent).

### 4.7 Volume populator

To clarify the field's status on modern Kubernetes:

- `PVC.spec.dataSourceRef` is GA since 1.24 via the `AnyVolumeDataSource` feature gate (default-on).
- A populator controller (this operator) watches PVCs whose `dataSourceRef` references its kind and runs the `pvc-prime` + `claimRef`-rebind handshake.
- `kubernetes-csi/volume-data-source-validator` (which ships the `populator.storage.k8s.io/VolumePopulator` CRD) is **optional**. Without it, PVCs that mistype their populator ref hang `Pending`. With it, they're rejected at admission. The actual data-moving machinery works either way.

**Our position:** the populator path works on any cluster ≥ 1.24 without installing anything extra. If the `VolumePopulator` CRD is present at operator startup, we register ourselves for the better UX; if absent, we log it and carry on. No hard dependency.

This addresses **G10** by making the populator path uniform (passive `Restore`) and never gating it on copy-method.

### 4.8 Hooks

`hooks.beforeSnapshot[]` / `hooks.afterSnapshot[]` accept one of:

- `workloadExec` — `kubectl exec`-style into a matched workload pod/container (the default and most-requested form, fork **#22**).
- `runJob: { jobSpec: ... }` — full `JobSpec` to run as a one-shot Job (k8up-style `PreBackupPod` analog). Named `runJob` to make the materialization explicit.
- `httpRequest` — typed POST to a URL for cross-system orchestration.

Hook failures abort the backup by default; `continueOnFailure: true` is opt-in per hook.

### 4.9 Multi-PVC consistency

`groupBy: VolumeGroupSnapshot` is the default for multi-PVC sources. If the chosen `volumeSnapshotClass`'s driver doesn't support VGS, the `BackupConfig` reports a `GroupSnapshotUnsupported` condition and refuses to run. Silently falling back to per-PVC snapshots would mean inconsistent backups — the same data-integrity hazard as **#1211**. To intentionally accept per-PVC snapshots, set `groupBy: None` explicitly.

### 4.10 Observability

| Surface                 | Volsync          | This operator                                                          |
| ----------------------- | ---------------- | ---------------------------------------------------------------------- |
| Per-PVC metrics labels  | absent (`#518`)  | always present (`pvc`, `pvc_namespace`, `backup_config`, `repository`) |
| Stale-metrics-on-delete | yes (`#1194`)    | metrics are CR-scoped, deleted with the CR                             |
| `lastSuccessAt`         | only derivable   | `BackupSchedule.status.lastSuccessfulSchedule` first-class             |
| Snapshot count          | exec into pod    | `kubectl get backup -l kopia.io/backup-config=...`                     |
| Logs                    | tail of last pod | small tail (~4KB) in `Backup.status.logTail`; full logs in Job pod     |
| Repo storage stats      | absent           | `Repository.status.storageStats`                                       |
| Content reclamation     | absent           | `Maintenance.status.*.lastContentReclaimedBytes`                       |

SLO-friendly metrics:

- `kopia_operator_backup_last_success_timestamp_seconds{backup_config,namespace}` — gauge.
- `kopia_operator_backup_consecutive_failures{backup_config,namespace}` — gauge.
- `kopia_operator_restore_duration_seconds{...}` — summary (p50/p90/p99).
- `kopia_operator_snapshot_deletion_failures_total{repository}` — counter; alert on rate.
- `kopia_operator_orphaned_snapshots_total{repository}` — counter; incremented by `skip-snapshot-cleanup` escape hatch.

### 4.11 Security & RBAC

- Operator is namespaced by default; cluster-scoped install is opt-in via Helm value. The `ClusterRepository` CRD is registered regardless (it's the shape; whether tenants can read it is RBAC).
- Mover pods run as `runAsNonRoot: true`, `runAsUser: 65534` (nobody) by default. Files restored may not match original ownership — documented trade-off, not a hidden surprise.
- `mover.securityContext: {...}` — explicit override.
- `mover.inheritSecurityContextFrom: { podSelector }` — opt-in best-effort copy from a live consumer; fails loud (condition) if no pod matches at backup time. Not a default because the workload may be scaled to zero.
- `mover.privilegedMode: true` (namespace-gated by `kopia.io/allow-privileged-movers: "true"`) — runs with `CHOWN`/`FOWNER` for clean ownership restoration. Explicit double opt-in.
- `lost+found` and similar system entries are skipped by default (fork **#1033**/`#1889`).

### 4.12 Mover pods & failure handling

- Jobs use `restartPolicy: Never` and `backoffLimit: spec.failurePolicy.backoffLimit` (default `2`).
- `concurrencyPolicy: Forbid` default; missed slots produce a `BackupSkipped` condition.
- `activeDeadlineSeconds` default `7200`.
- Completed mover pods are reaped on the same reconcile that observes their terminal status — no zombie pods (fork **#8**).

### 4.13 Forward compatibility

Every credential, schedule, policy, and identity surface is a **sub-object** rather than a leaf field, so future fields slot in without changing the basic shape. Specifically deferred but accommodated:

- `Repository.spec.encryption.rotation` / `ClusterRepository.spec.encryption.rotation` — password rotation flow.
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
    repository: { kind: Repository, name: nas-primary, namespace: backups }
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

### 5.2 Shared platform repository

```yaml
apiVersion: kopia.io/v1alpha1
kind: ClusterRepository
metadata: { name: shared-primary }
spec:
    backend:
        s3:
            bucket: org-kopia-repo
            prefix: "" # bucket root, maximum dedup
            endpoint: s3.us-east-1.amazonaws.com
            region: us-east-1
            auth:
                secretRef: { name: kopia-platform-creds, namespace: kopia-system }
    encryption:
        passwordSecretRef:
            { name: kopia-platform-creds, namespace: kopia-system, key: KOPIA_PASSWORD }
    allowedNamespaces:
        list: [billing, payments, identity]
    identityDefaults:
        hostnameTemplate: "{{ .Namespace }}"
        usernameTemplate: "{{ .Namespace }}-{{ .ConfigName }}"
---
# In the tenant namespace — no need to know the secret name or platform details
apiVersion: kopia.io/v1alpha1
kind: BackupConfig
metadata: { name: postgres-data, namespace: billing }
spec:
    repository: { kind: ClusterRepository, name: shared-primary }
    sources: [{ pvc: { name: postgres-data } }]
    retention: { keepDaily: 14 }
```

Identity resolves to `billing-postgres-data@billing:/pvc/postgres-data` via the templates.

### 5.3 Restore by picking a backup

```bash
kubectl get backup -n billing \
  -l kopia.io/backup-config=postgres-data \
  --sort-by=.status.timing.startTime
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

### 5.4 Multi-PVC selector

```yaml
apiVersion: kopia.io/v1alpha1
kind: BackupConfig
metadata: { name: app-bundle, namespace: billing }
spec:
    repository: { kind: Repository, name: nas-primary, namespace: backups }
    identity: { username: app-bundle, hostname: billing }
    sources:
        - pvcSelector:
              labelSelector: { matchLabels: { backup: include } }
          sourcePathStrategy: PVCName
    groupBy: VolumeGroupSnapshot
    retention: { keepDaily: 14 }
```

### 5.5 Deploy-or-restore (GitOps)

The headline pattern. Apply everything together; on a fresh cluster against an existing repo, the PVC restores; on a fresh repo, it comes up empty and gets backed up going forward.

```yaml
apiVersion: kopia.io/v1alpha1
kind: BackupConfig
metadata: { name: postgres-data, namespace: billing }
spec:
    repository: { kind: Repository, name: nas-primary, namespace: backups }
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
    policy: { onMissingSnapshot: Continue } # default for fromConfig — explicit here for clarity
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

### 5.6 Manual one-shot backup

```yaml
apiVersion: kopia.io/v1alpha1
kind: Backup
metadata: { name: postgres-data-pre-migration, namespace: billing }
spec:
    configRef: { name: postgres-data }
    tags: { reason: "pre-schema-migration" }
    deletionPolicy: Retain # I want this one to survive my next prune
```

Equivalently from any external system: Argo Events Sensor, Tekton Task, GitHub Actions, webhook handler. The `Backup` CR is the universal entry point.

### 5.7 Restore from a discovered (foreign / pre-install) backup

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
    target:
        {
            pvc:
                {
                    name: rescue-pvc,
                    storageClassName: fast-ssd,
                    capacity: 50Gi,
                    accessModes: [ReadWriteOnce],
                },
        }
```

### 5.8 Forcing CR removal when the repo is offline

```bash
kubectl annotate backup postgres-data-pre-migration -n billing \
  kopia.io/skip-snapshot-cleanup=true --overwrite
kubectl delete backup postgres-data-pre-migration -n billing
# Snapshot remains in the repo and will rematerialize as `origin: discovered`
# once Repository is healthy and within the catalog window.
```

### 5.9 Suspending a schedule via GitOps

```yaml
spec:
    schedule:
        suspend: true # apply via PR; un-suspend in a follow-up PR
```

In-flight `Backup`s are unaffected; only future cron firings are skipped.

---

## 6. Consequences

### 6.1 Positive

- Kopia-native ergonomics: identity, policy, hooks, snapshot listing all map to user mental models 1:1 with kopia.
- Three-layer triggering — any source can fire a `Backup`. Solves `#1559` and `#627` together.
- GitOps deploy-or-restore is a single-manifest pattern, not a runbook.
- `kubectl get backup` is the one place to look — runs, history, and the catalog all live there.
- Snapshot lifecycle = CR lifecycle, configurable. The "I deleted my `ReplicationSource` but my snapshots are still there" volsync confusion is structurally impossible.
- Cluster-scoped and namespace-scoped repositories are both first-class. Platform teams get a single-object shared repository; app teams get private repositories without cross-namespace RBAC plumbing.
- Multi-PVC, multi-namespace, one-repo: first-class.
- VolumeGroupSnapshot on by default; degrades loudly, never silently.
- No bash mover scripts.

### 6.2 Negative / trade-offs

- Seven CRDs to volsync's two — discoverability cost. Mitigated by hiding `Maintenance` from the typical first-time user flow (the simple case in §5.1 doesn't reference it explicitly) and by overloading `Backup` to cover both the "operator made this" and "we found this in the repo" cases.
- `origin: discovered` `Backup` CRs add etcd load. Mitigated by `catalog.retain` bounds.
- Webhook resolution of `Restore.source.backupRef` means restoring a snapshot just outside the catalog window requires the raw-identity escape hatch. Documented.
- Kopia version pinning is a single choice baked into the operator image; mitigated by a `Repository.spec.kopiaImageOverride` escape hatch for advanced users.
- The default `deletionPolicy: Delete` on operator/manual backups is a sharp edge for users coming from volsync, where deleting a `ReplicationSource` is a safe operation. Documentation must lead with this difference and offer `defaultDeletionPolicy: Retain` on `BackupConfig` as the conservative migration default.
- "Manifest deleted ≠ storage reclaimed" asymmetry will generate support questions. Mitigated by exposing `lastContentReclaimedBytes` on `Maintenance` and by metrics, but it remains an unavoidable property of kopia.
- Coexists with volsync rather than supplanting it; users wanting rsync/syncthing keep volsync.

---

## 7. Deferred / open questions

These are real questions we've punted on. Each warrants its own ADR before implementation.

1. **Cron library implementation.** Out of scope per the ADR header. Requirements include: deterministic jitter (derived from a stable seed), IANA TZ database, manual-trigger primitive, runtime schedule updates, missed-run policies on operator restart, ISC-style DST handling.
2. **`BackupWorkflow` / `dependsOn`.** Backup→verify→cleanup pipelines (e.g., automatic restore-into-scratch verification) are a natural follow-up. The trigger schema does not preclude this. Likely v1alpha2.
3. **`RestoreSchedule`.** Scheduled restore verification can be done with `CronJob` applying `Restore` CRs today; whether to first-class it is deferred. We will ship a documented example.
4. **Repository password rotation.** `Repository.spec.encryption` is a sub-object so the surface exists; the flow (rolling write to all repo blobs, coordinated with maintenance) needs its own design.
5. **Cross-cluster restoration.** Identity model accommodates per-cluster hostname prefixes; the operational surface (presenting one cluster's repo to another) is out of scope for v1alpha1.
6. **VolSync migration tooling.** Likely a `kubectl` plugin that translates `ReplicationSource` + `ReplicationDestination` into `BackupConfig` + `BackupSchedule` + `Restore`. Separate ADR.
7. **Performance characterization at high CR counts.** A workload backed up hourly with 14-day daily / 8-week weekly retention will hold ~50 `Backup` CRs per workload at steady state — manageable, but warrants benchmarks at 10k `Backup` CRs per namespace before GA.
8. **Discovered backup attribution.** When a discovered snapshot's identity matches a known `BackupConfig`, should the operator place the discovered `Backup` in the `BackupConfig`'s namespace instead of the `Repository`'s? Improves locality but adds attribution complexity. Deferred.

---

## 8. References

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

| Concern                                             | volsync                                  | this operator                                                                                |
| --------------------------------------------------- | ---------------------------------------- | -------------------------------------------------------------------------------------------- |
| Repo as a resource                                  | Secret reference                         | `Repository` + `ClusterRepository` CRDs                                                      |
| Cluster-scoped shared repo                          | not expressible                          | `ClusterRepository` with `allowedNamespaces`                                                 |
| Triggering layers                                   | one (`trigger` field on source)          | three (`BackupConfig` / `Backup` / `BackupSchedule`)                                         |
| Manual / external trigger                           | `trigger.manual: <value>` string-change  | `kubectl create backup` (G17, `#1559`)                                                       |
| Snapshot lifecycle on CR delete                     | unaffected (G20)                         | `deletionPolicy: Delete` (default) / `Retain` / `Orphan`; finalizer-driven                   |
| Force-delete escape hatch                           | n/a                                      | `kopia.io/skip-snapshot-cleanup: "true"` annotation                                          |
| Discovered / foreign snapshots                      | exec into mover pod                      | `Backup` CR with `origin: discovered`; forced `Retain`                                       |
| Multi-PVC                                           | not supported                            | `pvcSelector` + `groupBy: VolumeGroupSnapshot` default                                       |
| Multi-PVC consistency on unsupported drivers        | n/a                                      | fail loud (`GroupSnapshotUnsupported`); `groupBy: None` opts into per-PVC                    |
| First-sync skip                                     | not supported (`#627`)                   | `runOnCreate: false` default                                                                 |
| Cron jitter                                         | not supported (`#1421`)                  | deterministic `jitter` + `H` substitution                                                    |
| Cron timezone                                       | not supported (`#702`)                   | `schedule.timezone`                                                                          |
| Schedule anchor                                     | last-completion                          | wall-clock (CronJob-style)                                                                   |
| Concurrency policy                                  | implicit                                 | `concurrencyPolicy: Forbid` default                                                          |
| BackoffLimit                                        | hard-coded 8                             | per-`Backup` `failurePolicy.backoffLimit`                                                    |
| Retention                                           | restic `forget` flags                    | GFS on `BackupConfig`, operator prunes CRs; CR-driven exclusive                              |
| Snapshot as a K8s object                            | absent                                   | `Backup` CR (operator-initiated, manual, or discovered)                                      |
| Restore by snapshot reference                       | no — only `restoreAsOf`                  | `source.backupRef`                                                                           |
| Restore on a fresh cluster against an existing repo | manual runbook                           | `source.fromConfig` resolves via identity                                                    |
| Missing snapshot at restore                         | silently succeeds with empty PVC         | `Fail` default; `Continue` for `fromConfig` (deploy-or-restore)                              |
| Maintenance                                         | embedded in mover pod                    | own `Maintenance` CRD                                                                        |
| Maintenance ownership                               | implicit                                 | explicit lease + status                                                                      |
| Snapshot catalog                                    | exec into pod                            | `Backup` CRs (`origin: discovered`), bounded materialization                                 |
| Hooks                                               | shell-string in mover                    | typed `workloadExec` / `runJob` / `httpRequest`                                              |
| Policy passthrough                                  | restic flags only                        | typed `policy.*` + `extraArgs` escape hatch                                                  |
| Per-PVC metrics                                     | absent (`#518`)                          | first-class labels                                                                           |
| Stale metrics                                       | observed (`#1194`)                       | metric-per-CR, cleared on delete                                                             |
| Content reclamation visibility                      | absent                                   | `Maintenance.status.*.lastContentReclaimedBytes` + metric                                    |
| Mover image default tag                             | `:latest`                                | digest-pinned per operator release                                                           |
| Zombie mover pods                                   | observed (fork `#8`)                     | reaped on terminal-status reconcile                                                          |
| Lost+found root files                               | break unprivileged restore (`#1033`)     | skipped by default                                                                           |
| Restore target modes                                | `destinationPVC` (in-place) OR populator | `target.pvcRef` / `target.pvc` / passive (populator) — uniform                               |
| Populator dependency                                | requires reading the docs to know        | clarified: works on any 1.24+ cluster; volume-data-source-validator is an optional UX nicety |
