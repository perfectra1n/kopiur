# Restores

A `Restore` reads a snapshot back into a PVC. At its core it answers three questions: **from where** (`source`), **to where** (`target`), and **how** (`options`/`policy`). For real-world restores it also exposes the same mover knobs a backup has — UID/GID, kopia cache, and a Job retry/deadline policy — covered in [Mover, cache & failure policy](#mover-cache--failure-policy) below.

/// tip | The shape of a Restore

```yaml
spec:
    source: { <one of three>: ... } # FROM: which snapshot
    target: { <one of three>: ... } # TO: pvc | pvcRef | populator: {}  (REQUIRED)
    options: { ... } # HOW kopia writes (file deletion, permissions)
    policy: { ... } # what to do if the snapshot is missing
```

`source` **and** `target` are required (ADR-0005 §9); `options`/`policy` are optional with safe defaults.

///

Restore is "pick a row, write it somewhere" — there's no timestamp arithmetic in the common case. A `Restore` resolves its source **once at admission** and pins it to status, so it never silently retargets a different snapshot later.

## Where to restore _from_ — `source`

Exactly one of three modes (externally tagged — you set one key):

### `snapshotRef` — restore a specific Snapshot (the default)

You browsed the catalog and picked a `Snapshot` CR. No timestamps — just reference it. See [example 03](examples.md#example-03--restore-by-picking-a-snapshot).

```yaml
source:
    snapshotRef:
        name: postgres-data-20260524-021300
        namespace: billing # optional; defaults to the Restore's namespace
```

To find candidates:

```console
$ kubectl get snapshots -n billing \
    -l kopiur.home-operations.com/snapshot-policy=postgres-data \
    --sort-by=.status.timing.startTime
```

### `fromPolicy` — resolve via a SnapshotPolicy's identity

Restore the latest (or an offset/point-in-time) snapshot for a `SnapshotPolicy`'s identity — **even when no `Snapshot` CR exists yet**. This is what powers deploy-or-restore (see below) and point-in-time rollback ([example 14](examples.md#example-14--point-in-time--offset-restore), [scenario 07](scenarios/point-in-time-rollback.md)). Defaults to `onMissingSnapshot: Continue`.

```yaml
source:
    fromPolicy:
        name: postgres-data
        namespace: billing # optional; defaults to the Restore's namespace
        offset: 0 # 0 = latest, 1 = previous, ...
        # asOf: 2026-05-01T00:00:00Z   # or: newest snapshot at/before this instant
```

`asOf` (newest snapshot at/before an RFC3339 instant) and `offset` (count back from latest) are alternatives — set one. `asOf` is the "roll back to a known-good time" knob; `offset` is "the previous one."

### `identity` — a raw kopia identity

For snapshots written by a foreign kopia client, or ones that have aged out of the catalog ([example 13](examples.md#example-13--restore-by-raw-kopia-identity)). You give the raw `username@hostname:path`. This mode **requires** an explicit `spec.repository` (there's no `Snapshot`/`SnapshotPolicy` to infer it from).

```yaml
repository: { kind: Repository, name: primary, namespace: backups }
source:
    identity:
        username: postgres-data
        hostname: billing
        sourcePath: /data
        snapshotID: k1f1ec0a8 # pin an exact snapshot, or use asOf / offset
```

## Where to restore _to_ — `target`

### `pvc` — create a new PVC

The operator creates the PVC and restores into it. Best for verification restores (restore alongside the original and compare):

```yaml
target:
    pvc:
        name: postgres-data-restored
        storageClassName: fast-ssd # optional; cluster default otherwise
        capacity: 100Gi
        accessModes: [ReadWriteOnce]
```

### `pvcRef` — write into an existing PVC

```yaml
target:
    pvcRef:
        name: postgres-data # an existing PVC in this namespace
```

### `populator: {}` — passive populator mode

Set `target.populator: {}` and the `Restore` becomes a **passive volume-populator source**: it doesn't act on its own; instead a PVC's `spec.dataSourceRef` points at it, and the snapshot is restored as that PVC is provisioned. This is the GitOps deploy-or-restore pattern (next section).

```yaml
target:
    populator: {} # explicit passive-populator mode (ADR-0005 §9)
```

/// warning | `target` is required — the empty-`target` form is gone

As of ADR-0005 §9, a `Restore` with **no** `target` is rejected by the webhook. Populator intent must be the **explicit** `target.populator: {}` (not an omitted `target`). Also, `inheritSecurityContextFrom` is invalid in populator mode — there's no workload pod at provision time, so the webhook rejects it and points you at `moverDefaults` / an explicit `securityContext`.

///

## How to write — `options` and `policy`

```yaml
options:
    enableFileDeletion: false # default: additive restore (don't delete extra files in the target)
    ignorePermissionErrors: true # default true
    writeFilesAtomically: true # default true
policy:
    onMissingSnapshot: Fail # see table below
    waitTimeout: 5m # how long to wait for the source snapshot to appear
```

/// warning | `enableFileDeletion` makes the target a mirror

By default a restore is **additive** — it writes the snapshot's files and leaves anything else in the target alone. `enableFileDeletion: true` deletes files in the target that aren't in the snapshot, making it an exact mirror. Use it deliberately.

///

### `onMissingSnapshot` — fail-closed vs proceed

| Value      | Behavior                                                                      | Default for                                  |
| ---------- | ----------------------------------------------------------------------------- | -------------------------------------------- |
| `Fail`     | No matching snapshot ⇒ the restore fails.                                     | `snapshotRef` / `identity` (explicit sources). |
| `Continue` | No matching snapshot ⇒ proceed without restoring (the volume comes up empty). | `fromPolicy`.                                |

The defaults are the point: an _explicit_ restore that finds nothing is an error you want surfaced; a _deploy-or-restore_ that finds nothing should let the app start with a fresh volume.

## Mover, cache & failure policy

A restore writes data **into** a PVC, so the mover that does the writing has the same concerns a backup's does. `Restore.spec.mover` is the same `MoverSpec` a `SnapshotPolicy` exposes, and `Restore.spec.failurePolicy` mirrors `Snapshot.spec.failurePolicy`. See the full manifest in [example 12](examples.md#example-12--restore-mover-cache--failure-policy).

```yaml
spec:
    mover:
        securityContext: { runAsUser: 1000, runAsGroup: 1000, ... } # CONTAINER: own the restored files
        podSecurityContext: { fsGroup: 1000 } # POD: make a fresh volume writable
        # inheritSecurityContextFrom: { podSelector: {...} }        # ...or copy securityContext from a live pod
        cache: { capacity: 16Gi, mode: Persistent, contentCacheSizeMb: 10000 }
    failurePolicy:
        backoffLimit: 4
        activeDeadlineSeconds: 7200
```

- **`mover.securityContext`** — run the restore mover (its **container**) as the UID/GID that should own the restored files. Without it the mover runs as the hardened default (UID 65532), which may write files the app can't read. This is the fix for "the restore mover had no UID control".
- **`mover.podSecurityContext.fsGroup`** — a **pod**-level `fsGroup` that makes a freshly-provisioned target volume group-writable, so an **unprivileged** `runAsUser: 1000` mover can populate it on restore (instead of needing a root mover just to write the new volume). The headline case for restoring into a brand-new PVC as non-root. See [Security context → fsGroup](security-context.md).
- **`mover.inheritSecurityContextFrom`** — instead of hard-coding them, copy **both** the container `securityContext` **and** the pod-level `securityContext` (so the restore mover gets the app's UID *and* its `fsGroup`) from a live workload pod (by label selector). Mutually exclusive with both `securityContext` and `podSecurityContext` (combining is webhook-rejected). See [Security context → Inherit it from the workload](security-context.md#2-inherit-it-from-the-workload) and [example 18](examples.md#example-18--inherit-the-mover-security-context-from-a-workload).
- **`mover.cache`** — size the kopia cache for a large restore. `mode: Ephemeral` (default) gives a fresh per-run volume sized by `capacity` (or an `emptyDir` when unset); `mode: Persistent` keeps a controller-owned cache PVC and reuses it across runs for a warm cache. `contentCacheSizeMb` / `metadataCacheSizeMb` pass kopia's `--content/metadata-cache-size-mb` budgets. A repository's `moverDefaults.cache` are inherited and overlaid by `mover.cache`.
- **`failurePolicy`** — the restore Job's `backoffLimit` and `activeDeadlineSeconds`. Absent uses the defaults (2 retries, no deadline).

/// warning | An elevated restore mover needs the namespace to opt in

A restore mover that runs as root (`runAsUser: 0`), with added capabilities, or `privilegedMode: true` — including one **inherited** from a root workload pod — is refused with `MoverPermitted=False` until the restore's namespace opts in, exactly like a backup:

```console
$ kubectl annotate namespace billing kopiur.home-operations.com/privileged-movers=true
```

See [Permissions](permissions.md) for how to choose the UID/GID and when a privileged mover is warranted.

///

## Deploy-or-restore (GitOps)

The headline pattern: commit one bundle and apply it to **any** cluster. On a fresh cluster pointed at an existing repository, the PVC restores the latest snapshot before the app starts; on a brand-new repository, the PVC comes up empty and is backed up going forward. No "is this a new install or a recovery?" branching.

The mechanism is a **passive `Restore`** (`source.fromPolicy`, `target.populator: {}`, `onMissingSnapshot: Continue`) consumed by a PVC's `dataSourceRef` as a volume populator. The full manifest is [example 05](examples.md#example-05--deploy-or-restore-gitops).

/// note | Kubernetes ≥ 1.24

The volume-populator handshake relies on the `AnyVolumeDataSource` feature (GA from 1.24). The optional `volume-data-source-validator` surfaces a malformed `dataSourceRef` as an event instead of a silently-stuck PVC.

///

## Restoring a snapshot Kopiur didn't create

Snapshots written by a foreign kopia client, or predating your install, are materialized as **discovered** `Snapshot` CRs (`origin=discovered`, forced `deletionPolicy: Retain`) in the repository's namespace. Restore them two ways (see [example 07](examples.md#example-07--restore-a-discovered-backup)):

- **(A)** reference the discovered `Snapshot` CR with `source.snapshotRef` — same as any other backup; or
- **(B)** use `source.identity` with the raw kopia identity (requires `spec.repository`), for snapshots that aged out of the catalog.

```console
$ kubectl get snapshots -n backups -l kopiur.home-operations.com/origin=discovered
```

## Watching a restore

```console
$ kubectl get restore -n billing -w
NAME              PHASE        AGE
postgres-verify   Resolving    2s
postgres-verify   Restoring    9s
postgres-verify   Completed    41s
```

Phases: `Pending` → `Resolving` (pinning the source snapshot) → `Restoring` (mover writing data) → `Completed` / `Failed`. Live byte/file progress is in `status.progress`; the resolved snapshot and target PVC are in `status.resolved` / `status.target`. If it won't progress, `kubectl describe restore <name>` shows the reason on the conditions and as an Event — see [Troubleshooting](troubleshooting.md).

## Credentials in a fresh namespace — `credentialProjection`

A restore mover loads the repository credentials via `envFrom` from a Secret **in its own namespace**. Restoring into a namespace that has never run a backup (disaster recovery, a clone target) won't have one. Set `credentialProjection.enabled: true` and the operator copies the referenced repository's Secret into the mover's namespace for the run — owned by the `Restore`, garbage-collected with it ([example 17](examples.md#example-17--restore-from-a-shared-repo-projection)):

```yaml
spec:
    repository: { kind: ClusterRepository, name: platform-shared }
    credentialProjection:
        enabled: true # off by default; needs Helm secretProjection.enabled
```

It's **off by default** (cross-namespace Secret copying is opt-in) and needs the operator's Secret-projection RBAC (Helm `secretProjection.enabled`). The alternative is placing the Secret in the namespace yourself. See [Movers → credential projection](movers.md#let-kopiur-project-the-credentials-secret-recommended-for-shared-repos).

## Field reference — every value, and when to change it

The full `Restore` surface, with the examples that exercise each. `source` is the only required field.

| Field | What it does | When to set it |
| --- | --- | --- |
| `repository` | The repository to read from (`{ kind, name, namespace? }`). Inferred from `source` for `snapshotRef`/`fromPolicy`; **required** for `identity`. | Cross-namespace / cluster restores, or any `identity` source. ([13](examples.md#example-13--restore-by-raw-kopia-identity), [16](examples.md#example-16--cross-namespace-clone-restore)) |
| `source.snapshotRef` | Restore a specific `Snapshot` CR (`{ name, namespace? }`). | The common case — you picked a row from the catalog. ([03](examples.md#example-03--restore-by-picking-a-snapshot), [16](examples.md#example-16--cross-namespace-clone-restore)) |
| `source.fromPolicy` | Resolve via a `SnapshotPolicy`'s identity (`{ name, namespace?, asOf?, offset? }`). | No `Snapshot` CR (deploy-or-restore), or point-in-time (`asOf`) / positional (`offset`) recovery. ([05](examples.md#example-05--deploy-or-restore-gitops), [14](examples.md#example-14--point-in-time--offset-restore)) |
| `source.identity` | Raw kopia identity (`{ username, hostname, sourcePath?, snapshotID?, asOf?, offset? }`). | Foreign / aged-out snapshots; needs `repository`. ([13](examples.md#example-13--restore-by-raw-kopia-identity)) |
| `target.pvc` | Create a new PVC and restore into it (`{ name, storageClassName?, capacity?, accessModes? }`). | The safe default — restore beside the original, verify, cut over. ([03](examples.md#example-03--restore-by-picking-a-snapshot)) |
| `target.pvcRef` | Restore into an **existing** PVC (`{ name }`). | In-place restore (scale the app down first). ([15](examples.md#example-15--in-place-mirror-restore)) |
| `target.populator` | Explicit passive volume-populator source (`populator: {}`). | GitOps deploy-or-restore via a PVC `dataSourceRef`. ([05](examples.md#example-05--deploy-or-restore-gitops)) |
| `options.enableFileDeletion` | Delete target files not in the snapshot (exact **mirror**). Default `false` (additive). | A faithful in-place restore — destructive, use deliberately. ([15](examples.md#example-15--in-place-mirror-restore)) |
| `options.ignorePermissionErrors` | Complete and _report_ permission problems vs. fail hard. Default `true`. | `false` to fail-closed when exact permissions matter. |
| `options.writeFilesAtomically` | Write via a temp file + rename. Default `true`. | Rarely changed. |
| `policy.onMissingSnapshot` | `Fail` (explicit sources) vs `Continue` (fromPolicy default). | `Fail` for deliberate recoveries; `Continue` for deploy-or-restore. |
| `policy.waitTimeout` | How long to wait for the source snapshot to appear. | Sources that may lag behind the Restore being applied. |
| `mover.securityContext` / `podSecurityContext` | Container UID/GID, and the pod-level `fsGroup` that makes a fresh target volume writable. | Own restored files as the app's UID; populate a fresh PVC as non-root (`fsGroup`). See [Mover, cache & failure policy](#mover-cache--failure-policy). ([12](examples.md#example-12--restore-mover-cache--failure-policy)) |
| `mover.cache` / `resources` / `inheritSecurityContextFrom` | Cache sizing/mode, mover resources, inherit-from-pod. | Large-restore cache, resource limits, run-as-the-app. ([12](examples.md#example-12--restore-mover-cache--failure-policy)) |
| `failurePolicy` | Restore Job `backoffLimit` / `activeDeadlineSeconds`. | Retry/deadline control for big or flaky restores. ([12](examples.md#example-12--restore-mover-cache--failure-policy)) |
| `credentialProjection` | Project the repo Secret into the mover's namespace. | Restoring into a fresh namespace from a shared repo. ([17](examples.md#example-17--restore-from-a-shared-repo-projection)) |

## See also

- [Backups & schedules](backups.md) — producing the snapshots you restore.
- [Repositories & backends](repositories.md) — where the snapshots live.
- [Permissions](permissions.md) — choosing the mover's UID/GID and the privileged-movers opt-in (applies to restores too).
- [Scenarios](scenarios/index.md) — [02 recover lost data](scenarios/recover-lost-data.md), [07 point-in-time rollback](scenarios/point-in-time-rollback.md), [08 clone to another namespace](scenarios/clone-app-to-namespace.md).
- [Examples](examples.md) — [03 by Snapshot](examples.md#example-03--restore-by-picking-a-snapshot), [05 deploy-or-restore](examples.md#example-05--deploy-or-restore-gitops), [07 discovered](examples.md#example-07--restore-a-discovered-backup), [12 mover/cache/failure policy](examples.md#example-12--restore-mover-cache--failure-policy), [13 by identity](examples.md#example-13--restore-by-raw-kopia-identity), [14 point-in-time](examples.md#example-14--point-in-time--offset-restore), [15 in-place mirror](examples.md#example-15--in-place-mirror-restore), [16 cross-namespace](examples.md#example-16--cross-namespace-clone-restore), [17 shared-repo projection](examples.md#example-17--restore-from-a-shared-repo-projection).
