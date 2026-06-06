# Restores

A `Restore` reads a snapshot back into a PVC. It answers three questions, and its whole spec is just those three: **from where** (`source`), **to where** (`target`), and **how** (`options`/`policy`).

```admonish tip title="The shape of a Restore"
~~~yaml
spec:
  source: { <one of three>: ... }  # FROM: which snapshot
  target: { <one of two>: ... }    # TO: which PVC  (omit entirely = passive populator)
  options: { ... }                 # HOW kopia writes (file deletion, permissions)
  policy: { ... }                  # what to do if the snapshot is missing
~~~
`source` is required; everything else is optional with safe defaults.
```

Restore is "pick a row, write it somewhere" — there's no timestamp arithmetic in the common case. A `Restore` resolves its source **once at admission** and pins it to status, so it never silently retargets a different snapshot later.

## Where to restore *from* — `source`

Exactly one of three modes (externally tagged — you set one key):

### `backupRef` — restore a specific Backup (the default)

You browsed the catalog and picked a `Backup` CR. No timestamps — just reference it. See [example 03](examples.md#example-03--restore-by-picking-a-backup).

```yaml
source:
  backupRef:
    name: postgres-data-20260524-021300
    namespace: billing # optional; defaults to the Restore's namespace
```

To find candidates:

```console
$ kubectl get backup -n billing \
    -l kopiur.home-operations.com/backup-config=postgres-data \
    --sort-by=.status.timing.startTime
```

### `fromConfig` — resolve via a BackupConfig's identity

Restore the latest (or an offset/point-in-time) snapshot for a `BackupConfig`'s identity — **even when no `Backup` CR exists yet**. This is what powers deploy-or-restore (see below). Defaults to `onMissingSnapshot: Continue`.

```yaml
source:
  fromConfig:
    name: postgres-data
    offset: 0 # 0 = latest, 1 = previous, ...
    # asOf: 2026-05-01T00:00:00Z   # or: newest snapshot at/before this instant
```

### `identity` — a raw kopia identity

For snapshots written by a foreign kopia client, or ones that have aged out of the catalog. You give the raw `username@hostname:path`. This mode **requires** an explicit `spec.repository` (there's no `Backup`/`BackupConfig` to infer it from).

```yaml
repository: { kind: Repository, name: primary, namespace: backups }
source:
  identity:
    username: postgres-data
    hostname: billing
    sourcePath: /data
    snapshotID: k1f1ec0a8 # pin an exact snapshot, or use asOf / offset
```

## Where to restore *to* — `target`

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

### No `target` — passive populator mode

Omit `target` entirely and the `Restore` becomes a **passive volume-populator source**: it doesn't act on its own; instead a PVC's `spec.dataSourceRef` points at it, and the snapshot is restored as that PVC is provisioned. This is the GitOps deploy-or-restore pattern (next section).

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

```admonish warning title="`enableFileDeletion` makes the target a mirror"
By default a restore is **additive** — it writes the snapshot's files and leaves anything else in the target alone. `enableFileDeletion: true` deletes files in the target that aren't in the snapshot, making it an exact mirror. Use it deliberately.
```

### `onMissingSnapshot` — fail-closed vs proceed

| Value | Behavior | Default for |
|---|---|---|
| `Fail` | No matching snapshot ⇒ the restore fails. | `backupRef` / `identity` (explicit sources). |
| `Continue` | No matching snapshot ⇒ proceed without restoring (the volume comes up empty). | `fromConfig`. |

The defaults are the point: an *explicit* restore that finds nothing is an error you want surfaced; a *deploy-or-restore* that finds nothing should let the app start with a fresh volume.

## Deploy-or-restore (GitOps)

The headline pattern: commit one bundle and apply it to **any** cluster. On a fresh cluster pointed at an existing repository, the PVC restores the latest snapshot before the app starts; on a brand-new repository, the PVC comes up empty and is backed up going forward. No "is this a new install or a recovery?" branching.

The mechanism is a **passive `Restore`** (`source.fromConfig`, no `target`, `onMissingSnapshot: Continue`) consumed by a PVC's `dataSourceRef` as a volume populator. The full manifest is [example 05](examples.md#example-05--deploy-or-restore-gitops).

```admonish note title="Kubernetes ≥ 1.24"
The volume-populator handshake relies on the `AnyVolumeDataSource` feature (GA from 1.24). The optional `volume-data-source-validator` surfaces a malformed `dataSourceRef` as an event instead of a silently-stuck PVC.
```

## Restoring a snapshot Kopiur didn't create

Snapshots written by a foreign kopia client, or predating your install, are materialized as **discovered** `Backup` CRs (`origin=discovered`, forced `deletionPolicy: Retain`) in the repository's namespace. Restore them two ways (see [example 07](examples.md#example-07--restore-a-discovered-backup)):

- **(A)** reference the discovered `Backup` CR with `source.backupRef` — same as any other backup; or
- **(B)** use `source.identity` with the raw kopia identity (requires `spec.repository`), for snapshots that aged out of the catalog.

```console
$ kubectl get backup -n backups -l kopiur.home-operations.com/origin=discovered
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

## See also

- [Backups & schedules](backups.md) — producing the snapshots you restore.
- [Repositories & backends](repositories.md) — where the snapshots live.
- [Examples](examples.md) — [03 restore by Backup](examples.md#example-03--restore-by-picking-a-backup), [05 deploy-or-restore](examples.md#example-05--deploy-or-restore-gitops), [07 discovered](examples.md#example-07--restore-a-discovered-backup).
