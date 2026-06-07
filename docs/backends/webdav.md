# WebDAV

The WebDAV backend stores the kopia repository at a **WebDAV collection URL** with
HTTP basic auth — Nextcloud, Apache `mod_dav`, or any WebDAV server.

Reach for WebDAV when your target speaks WebDAV but not S3. For Nextcloud you can
also reach it as an [rclone](rclone.md) remote, but native WebDAV is simpler.

## Provider prerequisites

- A WebDAV **collection URL** for the repository (e.g.
  `https://dav.example.com/kopia` or a Nextcloud `/remote.php/dav/files/<user>/...` path).
- **Basic-auth** credentials with write access to that collection.

## The Secret shape

Loaded with `envFrom`; the keys reach kopia as environment variables.

| Secret key              | Required | What it is                                                          |
| ----------------------- | -------- | ------------------------------------------------------------------- |
| `KOPIA_WEBDAV_USERNAME` | yes      | Basic-auth username.                                                |
| `KOPIA_WEBDAV_PASSWORD` | yes      | Basic-auth password (an app password where the server supports it). |
| `KOPIA_PASSWORD`        | **yes**  | The repository encryption password.                                 |

```yaml
stringData:
    KOPIA_WEBDAV_USERNAME: "REPLACE_ME"
    KOPIA_WEBDAV_PASSWORD: "REPLACE_ME"
    KOPIA_PASSWORD: "choose-something-long-and-random"
```

/// warning | Lose the password, lose the backups

`KOPIA_PASSWORD` encrypts the repository and cannot be recovered if lost. It is
**separate** from the WebDAV login password. Store it outside the cluster and back
up the Secret. See [Encryption](../repositories.md#encryption-and-repository-creation).

///

## The Repository

```yaml
--8<-- "deploy/examples/backends/webdav.yaml"
```

## Fields reference (`backend.webDav`)

Note the spec key is `webDav` (camelCase, capital D).

| Field            | Required | Default | What it controls                                  |
| ---------------- | -------- | ------- | ------------------------------------------------- |
| `url`            | yes      | —       | The WebDAV collection URL holding the repository. |
| `auth.secretRef` | no       | —       | Names the basic-auth Secret above.                |

## Customization — the values you actually change

- **`url`** — the collection URL. Include the full path to the repository folder.
- **`create.enabled`** — initialize the repository if missing.
- **`cacheDefaults`** — mover cache sizing ([movers](../movers.md)).

## As a `ClusterRepository`

The same `backend.webDav` stanza works on a cluster-scoped
[`ClusterRepository`](../repositories.md#clusterrepository-a-shared-repository); every
Secret reference must carry an explicit `namespace:` and the Secret must exist
where the movers run — see [Movers](../movers.md).

## Back up and restore against this repository

The lifecycle is backend-independent. Once `Ready`, add a `BackupConfig` +
`BackupSchedule` ([Backups & schedules](../backups.md),
[Example 01](../examples.md#example-01--single-pvc-scheduled)) and restore by
picking a `Backup` ([Restores](../restores.md),
[Example 03](../examples.md#example-03--restore-by-picking-a-backup)).

## Troubleshooting

- **`401 Unauthorized`** — wrong basic-auth username/password, or the server
  requires an **app password** (Nextcloud) rather than the account password.
- **`404` / `409`** — the collection URL doesn't exist or isn't a writable
  collection. Create the folder and point `url` at it.
- **TLS errors** — the WebDAV backend speaks HTTPS; use a valid certificate (the
  `tls` overrides available on S3 are not part of `backend.webDav`).

## See also

- [Repositories & backends](../repositories.md) — concepts: scope, encryption, creation.
- [Movers, RBAC & credentials](../movers.md) — where the credential Secret must live.
- Sibling backends: [S3](s3.md) · [rclone](rclone.md) (for Nextcloud and many others).
