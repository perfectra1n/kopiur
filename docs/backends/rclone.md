# rclone (everything else)

kopia shells out to [`rclone`](https://rclone.org), so the rclone backend reaches
**any rclone-supported provider** — Google Drive, OneDrive, Dropbox, pCloud, Box,
Mega, and dozens more. This is the escape hatch for providers without a native
kopia backend.

If a native backend exists for your provider ([S3](s3.md), [Azure](azure.md),
[GCS](gcs.md), [B2](b2.md), [WebDAV](webdav.md)), prefer it — it's simpler and has
fewer moving parts. Use rclone for everything else.

## Provider prerequisites

- A working **`rclone.conf`** that defines the remote you'll reference. Build and
  test it locally first:

  ```console
  $ rclone config            # interactively create a remote, e.g. named "mydrive"
  $ rclone ls mydrive:       # confirm it works before putting it in a Secret
  ```

- The **remote name** in your `remotePath` (`mydrive:` below) must match a section
  header (`[mydrive]`) in that `rclone.conf`.

## The Secret shape

rclone is one of the three **file-delivered** backends. The mover reads the whole
config from a well-known env key, writes it to a private (`0600`) file, and runs
rclone with `--config`.

| Secret key | Required | What it is |
|---|---|---|
| `KOPIA_RCLONE_CONFIG` | yes | The **entire `rclone.conf`**, verbatim. Materialized to a file → rclone `--config`. |
| `KOPIA_PASSWORD` | **yes** | The repository encryption password. |

```yaml
stringData:
  KOPIA_RCLONE_CONFIG: |
    [mydrive]
    type = drive
    scope = drive
    token = {"access_token":"REPLACE_ME", ...}
  KOPIA_PASSWORD: "choose-something-long-and-random"
```

```admonish info title="Why KOPIA_RCLONE_CONFIG and not rclone.conf"
The mover loads credentials with `envFrom`, and a dotted key like `rclone.conf` is
not a valid environment-variable name — Kubernetes drops it. So the config goes
under `KOPIA_RCLONE_CONFIG`; the mover writes it to `rclone.conf` for you.
```

## The Repository

```yaml
{{#include ../../deploy/examples/backends/rclone.yaml}}
```

```admonish warning title="rclone uses `configSecretRef`, not `auth`"
Unlike the object-store backends, rclone references its config via
`backend.rclone.configSecretRef` — there is **no** `auth` block. The remote name in
`remotePath` must match a section in the `rclone.conf`.
```

## Fields reference (`backend.rclone`)

| Field | Required | Default | What it controls |
|---|---|---|---|
| `remotePath` | yes | — | rclone path in `remote:path` form (e.g. `mydrive:backups/kopia`). |
| `configSecretRef` | no¹ | — | Secret holding the `rclone.conf` (under `KOPIA_RCLONE_CONFIG`). **Not** `auth`. |

¹ Optional in the schema, but in practice required: rclone can't reach a remote
without its config.

## Customization — the values you actually change

- **`remotePath`** — the `remote:path`. The remote name must exist in the config.
- **`KOPIA_RCLONE_CONFIG`** — the config contents; re-paste when tokens rotate.
- **`create.enabled`** — initialize the repository if missing.

## As a `ClusterRepository`

The same `backend.rclone` stanza works on a cluster-scoped
[`ClusterRepository`](../repositories.md#clusterrepository-a-shared-repository); the
`configSecretRef` (and the `encryption.passwordSecretRef`) must carry an explicit
`namespace:`, and the Secret must exist where the movers run — see [Movers](../movers.md).

## Back up and restore against this repository

The lifecycle is backend-independent. Once `Ready`, add a `BackupConfig` +
`BackupSchedule` ([Backups & schedules](../backups.md),
[Example 01](../examples.md#example-01--single-pvc-scheduled)) and restore by
picking a `Backup` ([Restores](../restores.md),
[Example 03](../examples.md#example-03--restore-by-picking-a-backup)).

## Troubleshooting

```admonish warning title="Remote name must match"
`directory not found` / `didn't find section in config file` almost always means
the remote name in `remotePath` doesn't match a `[section]` in your `rclone.conf`.
Keep them identical.
```

- **Auth/token errors** — tokens in `rclone.conf` expire or get revoked; re-run
  `rclone config reconnect <remote>:` locally and re-paste the config.
- **`configSecretRef` ignored** — make sure you used `configSecretRef`, not
  `auth`; rclone has no `auth` block.
- Always validate with `rclone ls <remote>:` locally before applying.

## See also

- [Repositories & backends](../repositories.md) — concepts: scope, encryption, creation.
- [Movers, RBAC & credentials](../movers.md) — where the config Secret must live.
- Native backends (prefer where available): [S3](s3.md) · [Azure](azure.md) · [GCS](gcs.md) · [B2](b2.md) · [WebDAV](webdav.md).
