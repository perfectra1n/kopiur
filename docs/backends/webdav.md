# WebDAV

The WebDAV backend stores the kopia repository at a **WebDAV collection URL** with
HTTP basic auth — Nextcloud, Apache `mod_dav`, or any WebDAV server.

Reach for WebDAV when your target speaks WebDAV but not S3. For Nextcloud you can
also reach it as an [rclone](rclone.md) remote, but native WebDAV is simpler.

## Provider prerequisites

- A WebDAV **collection URL** for the repository (e.g.
  `https://dav.example.com/kopia` or a Nextcloud `/remote.php/dav/files/<user>/...` path).
- **Basic-auth** credentials with write access to that collection.

### Finding your collection URL

The URL must point at a **folder (collection) that already exists** and is
writable; the path shape is server-specific:

| Server               | Collection URL shape                                              | Notes                                                                |
| -------------------- | ----------------------------------------------------------------- | -------------------------------------------------------------------- |
| **Nextcloud**        | `https://cloud.example.com/remote.php/dav/files/<username>/kopia` | `<username>` is the login name, and it appears **in the path**.      |
| **Apache `mod_dav`** | `https://dav.example.com/kopia`                                   | Whatever `<Location>` the DAV block serves, plus your folder.        |
| **Caddy (webdav)**   | `https://dav.example.com/kopia`                                   | The route prefix configured for the webdav handler, plus the folder. |

/// example | Nextcloud: app password + the exact URL

1. Create the target folder (`kopia` here) in the Files app.
2. _Personal settings → Security → Devices & sessions_ → **Create new app
   password**. Use it as `KOPIA_WEBDAV_PASSWORD` — with two-factor enabled the
   account password will not work for WebDAV at all, and an app password is
   revocable on its own either way.
3. The URL is the user-specific DAV path, **not** the share link and **not** the
   bare hostname: `https://cloud.example.com/remote.php/dav/files/alice/kopia`
   (for login `alice`). The deprecated `/remote.php/webdav/…` form still works
   but the `dav/files/<user>` form is canonical.

///

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

| Field            | Required | Default | Example                                                      | What it controls                                                                                                 |
| ---------------- | -------- | ------- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| `url`            | yes      | —       | `https://cloud.example.com/remote.php/dav/files/alice/kopia` | The WebDAV collection URL holding the repository — full scheme + path to an **existing, writable** folder.       |
| `auth.secretRef` | no       | —       | `{ name: webdav-repo-creds }`                                | Names the basic-auth Secret above. Same namespace as the `Repository`; a `ClusterRepository` adds `namespace:`.   |

WebDAV has no cloud-IAM federation, so its `auth` is **Secret-only** — there is
no `workloadIdentity` here. A stray `auth.workloadIdentity` is silently
**pruned** by the API server (it's not in the schema), not rejected.

## Customization — the values you actually change

- **`url`** — the collection URL. Include the full path to the repository folder.
- **`create.enabled`** — initialize the repository if missing.
- **`moverDefaults.cache`** — mover cache sizing ([movers](../movers.md)).

## As a `ClusterRepository`

The same `backend.webDav` stanza works on a cluster-scoped
[`ClusterRepository`](../repositories.md#clusterrepository-a-shared-repository); every
Secret reference must carry an explicit `namespace:` and the Secret must exist
where the movers run — see [Movers](../movers.md).

## Back up and restore against this repository

The lifecycle is backend-independent. Once `Ready`, add a `SnapshotPolicy` +
`SnapshotSchedule` ([Backups & schedules](../backups.md),
[Example 01](../examples.md#example-01--single-pvc-scheduled)) and restore by
picking a `Snapshot` ([Restores](../restores.md),
[Example 03](../examples.md#example-03--restore-by-picking-a-snapshot)).

## Troubleshooting

- **`401 Unauthorized`** — wrong basic-auth username/password, or the server
  requires an **app password** (Nextcloud with two-factor) rather than the
  account password.
- **`404` / `409`** — the collection URL doesn't exist or isn't a writable
  collection. Create the folder and point `url` at it. On Nextcloud, also check
  the username **in the path** matches the login the credentials belong to.
- **TLS errors** — the WebDAV backend speaks HTTPS; use a valid certificate (the
  `tls` overrides available on S3 are not part of `backend.webDav`). For an
  internal server with a private CA, terminate with a publicly-trusted cert or
  use a different backend.
- **Slow backups** — WebDAV is the chattiest backend (one HTTP request per
  blob operation, no multipart). Fine for modest datasets; for large or
  high-churn sources prefer an object store.

## See also

- [Repositories & backends](../repositories.md) — concepts: scope, encryption, creation.
- [Movers, RBAC & credentials](../movers.md) — where the credential Secret must live.
- Sibling backends: [S3](s3.md) · [rclone](rclone.md) (for Nextcloud and many others).
