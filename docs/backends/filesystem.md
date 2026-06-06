# Filesystem (PVC-backed)

The filesystem backend stores the kopia repository on a **local path** that
Kopiur mounts into the mover from a `PersistentVolumeClaim` — typically a NAS/NFS
share. There are **no object-store credentials**: the only secret is
`KOPIA_PASSWORD`. The thing that bites people here is **ownership**, not auth.

Reach for this when your "off-site" is an on-prem NAS or any `ReadWriteMany`
volume. For a remote server reached over SSH, see [SFTP](sftp.md).

## Provider prerequisites

- A **`PersistentVolumeClaim`** the mover can mount **read-write**. Use
  `ReadWriteMany` (NFS/NAS) so that backup, restore, *and* maintenance movers —
  which may run as different Jobs at the same time — can all mount it. The example
  bundles a PVC.
- The repository path must be **writable by the UID the mover runs as** (default
  `65532`). See Troubleshooting.

## The Secret shape

Filesystem backends need **only** the repository encryption password.

| Secret key | Required | What it is |
|---|---|---|
| `KOPIA_PASSWORD` | **yes** | The repository encryption password. No backend `auth` keys. |

```yaml
stringData:
  KOPIA_PASSWORD: "choose-something-long-and-random"
```

```admonish warning title="Lose the password, lose the backups"
Even though the data sits on your own NAS, kopia still encrypts it with
`KOPIA_PASSWORD`. Lose the password and the repository is unrecoverable. Store it
outside the cluster and back up the Secret. See [Encryption](../repositories.md#encryption-and-repository-creation).
```

## The Repository

```yaml
{{#include ../../deploy/examples/backends/filesystem.yaml}}
```

## Fields reference (`backend.filesystem`)

| Field | Required | Default | What it controls |
|---|---|---|---|
| `path` | yes | — | **Mount path inside the mover pod** where kopia writes the repository (e.g. `/repo`). |
| `pvcName` | no | — | The `PersistentVolumeClaim` mounted at `path`. Omit only if `path` already exists on the node/image. |

## Customization — the values you actually change

- **`pvcName`** — the PVC to mount (its size/StorageClass live on the PVC, not here).
- **`path`** — the in-pod mount point; `/repo` is a fine default.
- **mover `securityContext`** — set `runAsUser`/`fsGroup` on the consuming
  `BackupConfig` to match the share's ownership; see [Permissions](../permissions.md).
- **`create.enabled`** — initialize the repository if missing.

## As a `ClusterRepository`

A `ClusterRepository` may also use a filesystem backend, but note the PVC and its
`ReadWriteMany` reach must be available in whatever namespace the movers run in —
see [Movers](../movers.md). A cloud/object backend is usually a better fit for a
shared platform repository.

## Back up and restore against this repository

The lifecycle is backend-independent. Once `Ready`, add a `BackupConfig` +
`BackupSchedule` ([Backups & schedules](../backups.md),
[Example 01](../examples.md#example-01--single-pvc-scheduled)) and restore by
picking a `Backup` ([Restores](../restores.md),
[Example 03](../examples.md#example-03--restore-by-picking-a-backup)).

```admonish note title="ReadWriteMany matters"
Backup, restore, and maintenance run as separate mover Jobs and may overlap. A
`ReadWriteOnce` volume can only attach to one node at a time and will block the
others; use `ReadWriteMany`.
```

## Troubleshooting

```admonish warning title="It's ownership, not a credential"
The most common filesystem failure is **permission denied on the repository
path** — the path isn't writable by the mover's UID (default `65532`). Kopiur's
Warning Event names the exact UID and the `chown -R <uid> <path>` to run on the
NAS. Either `chown` the path or match the mover's UID/GID to the share owner via
the mover `securityContext`. Full story: [Permissions, UID & GID](../permissions.md).
```

- **`permission denied`** on create/connect — `chown -R 65532 <path>` (or set the
  mover UID to the owner). See above.
- **Mover Job pending** — the PVC isn't bound or isn't `ReadWriteMany`; check the
  PVC and StorageClass.

## See also

- [Permissions, UID & GID](../permissions.md) — the ownership story this backend lives and dies by.
- [Repositories & backends](../repositories.md) — concepts: scope, encryption, creation.
- [Movers, RBAC & credentials](../movers.md) — mover Jobs and volume mounts.
- Sibling backend: [SFTP](sftp.md) — same NAS, reached over SSH instead.
