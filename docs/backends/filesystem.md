# Filesystem (PVC or inline NFS)

The filesystem backend stores the kopia repository on a **local path** Kopiur
mounts into the mover. That path is backed by either a `PersistentVolumeClaim` or
an **inline NFS export** (`volume.nfs` — no PVC; see [below](#inline-nfs-no-pvc)),
typically a NAS/NFS share. There are **no object-store credentials**: the only
secret is `KOPIA_PASSWORD`. The thing that bites people here is **ownership**, not
auth.

Reach for this when your "off-site" is an on-prem NAS or any `ReadWriteMany`
volume. For a remote server reached over SSH, see [SFTP](sftp.md).

## Provider prerequisites

- Storage the mover can mount **read-write**: either a **`PersistentVolumeClaim`**
  or an **NFS export**. For a PVC, use `ReadWriteMany` (NFS/NAS) so that backup,
  restore, _and_ maintenance movers — which may run as different Jobs at the same
  time — can all mount it. The example bundles a PVC; the
  [inline-NFS variant](#inline-nfs-no-pvc) needs no PVC at all.
- The repository path must be **writable by the UID the mover runs as** (default
  `65532`). See Troubleshooting.

## The Secret shape

Filesystem backends need **only** the repository encryption password.

| Secret key       | Required | What it is                                                  |
| ---------------- | -------- | ----------------------------------------------------------- |
| `KOPIA_PASSWORD` | **yes**  | The repository encryption password. No backend `auth` keys. |

```yaml
stringData:
    KOPIA_PASSWORD: "choose-something-long-and-random"
```

/// warning | Lose the password, lose the backups

Even though the data sits on your own NAS, kopia still encrypts it with
`KOPIA_PASSWORD`. Lose the password and the repository is unrecoverable. Store it
outside the cluster and back up the Secret. See [Encryption](../repositories.md#encryption-and-repository-creation).

///

## The Repository

```yaml
--8<-- "deploy/examples/backends/filesystem.yaml"
```

## Fields reference (`backend.filesystem`)

| Field               | Required | Default | Example           | What it controls                                                                                                                |
| ------------------- | -------- | ------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `path`              | yes      | —       | `/repo`           | **Mount path inside the mover pod** where kopia writes the repository. Not a path on your NAS — the `volume` decides what's behind it. |
| `volume`            | no       | —       | —                 | What backs `path`: **exactly one of** `pvc` or `nfs`. Omit entirely only if `path` already exists on the node/image (hostPath).  |
| `volume.pvc.name`   | —        | —       | `nas-repo`        | The `PersistentVolumeClaim` mounted read-write at `path`. Must be `ReadWriteMany` (movers overlap).                              |
| `volume.nfs.server` | —        | —       | `nas.lan`         | NFS server hostname or IP — for an inline NFS export with no PVC (see below).                                                    |
| `volume.nfs.path`   | —        | —       | `/export/kopia`   | The absolute export path **on the NFS server** (what `showmount -e nas.lan` lists).                                              |

/// note | `volume` is an "exactly one of" choice

`volume: { pvc: … }` and `volume: { nfs: … }` are externally-tagged variants — set
one, never both. An empty/absent `volume` means "the path is already present in
the mover" (a `hostPath`/baked-in mount; mainly the e2e harness).

///

## Customization — the values you actually change

- **`volume.pvc.name`** — the PVC to mount (its size/StorageClass live on the PVC, not here).
- **`volume.nfs`** — point straight at an NFS export instead of a PVC ([below](#inline-nfs-no-pvc)).
- **`path`** — the in-pod mount point; `/repo` is a fine default.
- **mover `securityContext`** — set `runAsUser`/`fsGroup` on the consuming
  `SnapshotPolicy` to match the share's ownership; see [Permissions](../permissions.md).
- **`create.enabled`** — initialize the repository if missing.

### Sizing the PVC

The bundled example requests `500Gi` as a placeholder — size yours to the
**deduplicated, compressed** repository, not the raw source data. kopia
content-addresses everything, so N daily snapshots of slowly-changing data cost
roughly one full copy plus the churn, not N copies. A reasonable starting point
is 1–1.5× the source data; watch actual usage after the first retention cycle
and resize (most NAS-backed StorageClasses support volume expansion). Running
the volume completely full is the failure mode to avoid — kopia maintenance
needs headroom to rewrite and compact blobs.

### Preparing the export (NFS-side ownership)

The mover runs as UID `65532` by default and is **not root**, so classic
`root_squash` on the export is irrelevant — what matters is that UID `65532`
can write the directory:

```console
# on the NAS / NFS server
$ mkdir -p /export/kopia
$ chown -R 65532:65532 /export/kopia
```

If your NAS forces all clients to one identity (`all_squash` /
"map all users"), point that mapping (`anonuid`/`anongid`) at the directory
owner instead — or set the mover's `runAsUser` to whatever UID the NAS expects.
The full decision table is in [Permissions, UID & GID](../permissions.md).

## Inline NFS (no PVC) { #inline-nfs-no-pvc }

kopia has **no native NFS backend** — NFS is reached _through_ the filesystem
backend by mounting the export at `path`. Instead of pre-creating a
`ReadWriteMany` PVC, name an NFS export directly under `volume.nfs` and the
operator synthesizes a Kubernetes inline `nfs` volume on every mover Job
(bootstrap, backup, restore, maintenance):

```yaml
--8<-- "deploy/examples/backends/nfs.yaml"
```

This is the lowest-friction path to an on-prem NAS repository: no PVC, no
StorageClass, no provisioner — just a `server` and an absolute `path`. The same
`volume.nfs` shape works on a `ClusterRepository`. To back up an NFS export
_as a source_ (rather than as the repository), see
[Example 10](../examples.md#example-10--nfs-source-no-pvc).

/// note | A volume-backed repo bootstraps in a mover Job

A bare-path filesystem repo is reachable from the controller and is
connected/created in-process. A PVC- or NFS-backed repo is **not** reachable from
the controller, so the operator runs the connect/create in a short mover Job that
mounts the volume — the same path object stores use. The Repository moves
`Initializing` → `Ready` as that Job completes.

///

## As a `ClusterRepository`

A `ClusterRepository` may also use a filesystem backend. For a PVC, the claim and
its `ReadWriteMany` reach must be available in whatever namespace the movers run
in — see [Movers](../movers.md). An [inline NFS export](#inline-nfs-no-pvc) sees
the same reach from any mover namespace (it's named, not claimed), which can make
it simpler than a cross-namespace PVC — though a cloud/object backend is usually
the better fit for a shared platform repository.

## Back up and restore against this repository

The lifecycle is backend-independent. Once `Ready`, add a `SnapshotPolicy` +
`SnapshotSchedule` ([Backups & schedules](../backups.md),
[Example 01](../examples.md#example-01--single-pvc-scheduled)) and restore by
picking a `Snapshot` ([Restores](../restores.md),
[Example 03](../examples.md#example-03--restore-by-picking-a-snapshot)).

/// note | ReadWriteMany matters (for PVCs)

Backup, restore, and maintenance run as separate mover Jobs and may overlap. A
`ReadWriteOnce` volume can only attach to one node at a time and will block the
others; use `ReadWriteMany`. An [inline NFS export](#inline-nfs-no-pvc) sidesteps
this — NFS is inherently multi-mount, so concurrent movers all reach it.

///

## Troubleshooting

/// warning | It's ownership, not a credential

The most common filesystem failure is **permission denied on the repository
path** — the path isn't writable by the mover's UID (default `65532`). Kopiur's
Warning Event names the exact UID and the `chown -R <uid> <path>` to run on the
NAS. Either `chown` the path or match the mover's UID/GID to the share owner via
the mover `securityContext`. Full story: [Permissions, UID & GID](../permissions.md).

///

- **`permission denied`** on create/connect — `chown -R 65532 <path>` (or set the
  mover UID to the owner). See above.
- **Mover Job pending** — for a PVC, it isn't bound or isn't `ReadWriteMany`; check
  the PVC and StorageClass. For NFS, the pod can't mount the export — confirm the
  `server`/`path` are reachable from the cluster nodes and the export permits them.

## See also

- [Permissions, UID & GID](../permissions.md) — the ownership story this backend lives and dies by.
- [Repositories & backends](../repositories.md) — concepts: scope, encryption, creation.
- [Movers, RBAC & credentials](../movers.md) — mover Jobs and volume mounts.
- Sibling backend: [SFTP](sftp.md) — same NAS, reached over SSH instead.
