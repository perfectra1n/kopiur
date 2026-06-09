# Backend configuration

This is the **index** for Kopiur's storage backends. Each backend has its own
dedicated page with provider prerequisites, the exact Secret shape, a
field-by-field reference, customization knobs, and backend-specific
troubleshooting. Start here for the cross-cutting rules (the mental model, the
credential-key conventions), then jump to your backend below. For the _concepts_
behind a repository (the namespaced-vs-cluster split, encryption, repository
creation) read [Repositories & backends](repositories.md) first.

/// tip | The mental model (read this once)

A **`Repository`** is _where_ snapshots live. It splits cleanly into two halves:

- **Non-secret connection identifiers** — bucket, container, endpoint, host, path. These go in `spec.backend.<kind>`.
- **Secrets** — backend access keys and the kopia encryption password. These live in a Kubernetes `Secret`, read by **well-known keys**, and are passed to kopia as environment variables (never on argv, never in status).

`SnapshotPolicy` / `Snapshot` / `Restore` never repeat any of this — they point at the `Repository` by name.

///

/// warning | Externally tagged — there is no `kind:` field

A backend is chosen by **which key you set** (`backend.s3`, `backend.azure`, …), not a `kind:` discriminator. `backend: { kind: S3 }` will **not** admit. Exactly one backend key is allowed. (Why: [API conventions](dev/api-conventions.md).)

///

## Pick your backend

| Backend                                          | Covers                                                       | Spec key             |
| ------------------------------------------------ | ------------------------------------------------------------ | -------------------- |
| [S3 & S3-compatible](backends/s3.md)             | Amazon S3, MinIO, RustFS, Ceph RGW, Wasabi, Cloudflare R2, … | `backend.s3`         |
| [Azure Blob Storage](backends/azure.md)          | Azure Blob containers (key or SAS token)                     | `backend.azure`      |
| [Google Cloud Storage](backends/gcs.md)          | GCS buckets (service-account key)                            | `backend.gcs`        |
| [Backblaze B2](backends/b2.md)                   | Backblaze B2 native API                                      | `backend.b2`         |
| [Filesystem (PVC / NFS)](backends/filesystem.md) | A NAS/PVC or inline NFS export mounted into the mover        | `backend.filesystem` |
| [SFTP](backends/sftp.md)                         | Any server reachable over SSH/SFTP                           | `backend.sftp`       |
| [WebDAV](backends/webdav.md)                     | Nextcloud, Apache `mod_dav`, …                               | `backend.webDav`     |
| [rclone](backends/rclone.md)                     | Google Drive, OneDrive, Dropbox, and dozens more             | `backend.rclone`     |

## How to use these pages

1. Find your backend above.
2. **Provider prerequisites** — create the bucket/container/share and a scoped credential on the provider side.
3. Copy the manifest on the page, fill in every `REPLACE_ME`, set a long random `KOPIA_PASSWORD`, and `kubectl apply -f`.
4. Watch it become `Ready` (see [Watching a repository](repositories.md#watching-a-repository)).

/// warning | Lose the password, lose the backups

`KOPIA_PASSWORD` encrypts the repository. kopia cannot decrypt without it and there is no recovery. Use a long random value, store it **outside** the cluster (a password manager / external secret store), and back up the Secret itself. The three create-time tunables (`encryption`/`splitter`/`hash`) are fixed forever at creation — see [Encryption and repository creation](repositories.md#encryption-and-repository-creation).

///

## Credential keys at a glance

The mover reads these **exact** key names from the Secret you reference and feeds them to kopia. `KOPIA_PASSWORD` is required for **every** backend.

| Backend                                 | Secret keys (besides `KOPIA_PASSWORD`)                                    | Spec key             |
| --------------------------------------- | ------------------------------------------------------------------------- | -------------------- |
| [S3 / S3-compatible](backends/s3.md)    | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, _(opt)_ `AWS_SESSION_TOKEN` | `backend.s3`         |
| [Azure](backends/azure.md)              | `AZURE_STORAGE_KEY` **or** `AZURE_STORAGE_SAS_TOKEN`                      | `backend.azure`      |
| [Google Cloud Storage](backends/gcs.md) | `KOPIA_GCS_CREDENTIALS` (the SA-key JSON)                                 | `backend.gcs`        |
| [Backblaze B2](backends/b2.md)          | `B2_KEY_ID`, `B2_KEY`                                                     | `backend.b2`         |
| [Filesystem](backends/filesystem.md)    | _(none — only `KOPIA_PASSWORD`)_                                          | `backend.filesystem` |
| [SFTP](backends/sftp.md)                | `KOPIA_SFTP_KEY_DATA`, `KOPIA_SFTP_KNOWN_HOSTS`                           | `backend.sftp`       |
| [WebDAV](backends/webdav.md)            | `KOPIA_WEBDAV_USERNAME`, `KOPIA_WEBDAV_PASSWORD`                          | `backend.webDav`     |
| [rclone](backends/rclone.md)            | `KOPIA_RCLONE_CONFIG` _(via `configSecretRef`)_                           | `backend.rclone`     |

/// info | Env-delivered vs. file-delivered credentials

Most backends authenticate via environment variables kopia reads directly — the mover loads the Secret with `envFrom`, so the keys above become env vars. Three backends need their credentials as **files** instead (kopia's SFTP/GCS/rclone flags have no env form, and a Secret key like `ssh-privatekey` isn't a valid env-var name so `envFrom` would silently drop it). For those, the mover reads a well-known env key and writes it to a private (`0600`) file, then points kopia at the path:

| Secret key               | Becomes                       | kopia flag           |
| ------------------------ | ----------------------------- | -------------------- |
| `KOPIA_SFTP_KEY_DATA`    | the SSH private key file      | `--keyfile`          |
| `KOPIA_SFTP_KNOWN_HOSTS` | the `known_hosts` file        | `--known-hosts`      |
| `KOPIA_GCS_CREDENTIALS`  | the service-account JSON file | `--credentials-file` |
| `KOPIA_RCLONE_CONFIG`    | the `rclone.conf` file        | rclone `--config`    |

You don't manage the files — just put the value under the right key; the secret never lands on kopia's argv.

///

/// note | ClusterRepository: Secret refs need a namespace

The per-backend pages use a namespaced `Repository`. For a cluster-scoped `ClusterRepository` the same backend stanzas apply, but **every** Secret reference must carry an explicit `namespace:` (webhook-enforced), and the credential Secret must also reach each workload namespace — either replicate it yourself or turn on [credential projection](movers.md#let-kopiur-project-the-credentials-secret-recommended-for-shared-repos) (recommended for shared repos). A worked S3 example is on the [S3 page](backends/s3.md#as-a-clusterrepository).

///

## See also

- [Repositories & backends](repositories.md) — the concepts: scope, encryption, creation, `ClusterRepository`.
- [Permissions, UID & GID](permissions.md) — the filesystem/SFTP ownership story and the mover UID knob.
- [Movers, RBAC & credentials](movers.md) — where the credential Secret must live, and why.
