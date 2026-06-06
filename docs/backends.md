# Backend configuration

This page is the **per-backend setup cookbook**: for each storage backend Kopiur supports, what you provision on the provider side, which keys the credential Secret needs, and a complete apply-ready `Repository` you can copy. For the *concepts* behind a repository (the namespaced-vs-cluster split, encryption, repository creation) read [Repositories & backends](repositories.md) first; this page is the hands-on companion to it.

```admonish tip title="The mental model (read this once)"
A **`Repository`** is *where* snapshots live. It splits cleanly into two halves:

- **Non-secret connection identifiers** — bucket, container, endpoint, host, path. These go in `spec.backend.<kind>`.
- **Secrets** — backend access keys and the kopia encryption password. These live in a Kubernetes `Secret`, read by **well-known keys**, and are passed to kopia as environment variables (never on argv, never in status).

`BackupConfig` / `Backup` / `Restore` never repeat any of this — they point at the `Repository` by name.
```

```admonish warning title="Externally tagged — there is no `kind:` field"
A backend is chosen by **which key you set** (`backend.s3`, `backend.azure`, …), not a `kind:` discriminator. `backend: { kind: S3 }` will **not** admit. Exactly one backend key is allowed. (Why: [API conventions](dev/api-conventions.md).)
```

## How to use this page

1. Find your backend below.
2. **Provider prerequisites** — create the bucket/container/share and a scoped credential on the provider side.
3. Copy the manifest, fill in every `REPLACE_ME`, set a long random `KOPIA_PASSWORD`, and `kubectl apply -f`.
4. Watch it become `Ready` (see [Watching a repository](repositories.md#watching-a-repository)).

```admonish warning title="Lose the password, lose the backups"
`KOPIA_PASSWORD` encrypts the repository. kopia cannot decrypt without it and there is no recovery. Use a long random value, store it **outside** the cluster (a password manager / external secret store), and back up the Secret itself. The three create-time tunables (`encryption`/`splitter`/`hash`) are fixed forever at creation — see [Encryption and repository creation](repositories.md#encryption-and-repository-creation).
```

## Credential keys at a glance

The mover reads these **exact** key names from the Secret you reference and feeds them to kopia. `KOPIA_PASSWORD` is required for **every** backend.

| Backend | Secret keys (besides `KOPIA_PASSWORD`) | Spec key |
|---|---|---|
| [S3 / S3-compatible](#s3--s3-compatible) | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, *(opt)* `AWS_SESSION_TOKEN` | `backend.s3` |
| [Azure](#azure-blob-storage) | `AZURE_STORAGE_KEY` **or** `AZURE_STORAGE_SAS_TOKEN` | `backend.azure` |
| [Google Cloud Storage](#google-cloud-storage) | `KOPIA_GCS_CREDENTIALS` (the SA-key JSON) | `backend.gcs` |
| [Backblaze B2](#backblaze-b2) | `B2_KEY_ID`, `B2_KEY` | `backend.b2` |
| [Filesystem](#filesystem-pvc-backed) | *(none — only `KOPIA_PASSWORD`)* | `backend.filesystem` |
| [SFTP](#sftp) | `KOPIA_SFTP_KEY_DATA`, `KOPIA_SFTP_KNOWN_HOSTS` | `backend.sftp` |
| [WebDAV](#webdav) | `KOPIA_WEBDAV_USERNAME`, `KOPIA_WEBDAV_PASSWORD` | `backend.webDav` |
| [rclone](#rclone-everything-else) | `KOPIA_RCLONE_CONFIG` *(via `configSecretRef`)* | `backend.rclone` |

```admonish info title="Env-delivered vs. file-delivered credentials"
Most backends authenticate via environment variables kopia reads directly — the mover loads the Secret with `envFrom`, so the keys above become env vars. Three backends need their credentials as **files** instead (kopia's SFTP/GCS/rclone flags have no env form, and a Secret key like `ssh-privatekey` isn't a valid env-var name so `envFrom` would silently drop it). For those, the mover reads a well-known env key and writes it to a private (`0600`) file, then points kopia at the path:

| Secret key | Becomes | kopia flag |
|---|---|---|
| `KOPIA_SFTP_KEY_DATA` | the SSH private key file | `--keyfile` |
| `KOPIA_SFTP_KNOWN_HOSTS` | the `known_hosts` file | `--known-hosts` |
| `KOPIA_GCS_CREDENTIALS` | the service-account JSON file | `--credentials-file` |
| `KOPIA_RCLONE_CONFIG` | the `rclone.conf` file | rclone `--config` |

You don't manage the files — just put the value under the right key; the secret never lands on kopia's argv.
```

```admonish note title="ClusterRepository: Secret refs need a namespace"
Everything below uses a namespaced `Repository`. For a cluster-scoped `ClusterRepository` the same backend stanzas apply, but **every** Secret reference must carry an explicit `namespace:` (webhook-enforced), and the credential Secret must also be replicated into each workload namespace — see [Movers → the ClusterRepository gotcha](movers.md#the-credentials-secret-yours-to-place).
```

---

## S3 / S3-compatible

Works for Amazon S3 and any S3-compatible store — MinIO, RustFS, Ceph RGW, Wasabi, and friends.

**Provider prerequisites**

- A **bucket** (the manifest does not create it).
- An access key with read/write/list/delete on that bucket. Prefer a key scoped to the one bucket over a root key.
- For AWS: the bucket's **region**. For a compatible store: its **endpoint** host.

The values you'll actually change: `bucket`, `prefix`, `region`, and (for non-AWS) `endpoint` + `tls`.

```yaml
{{#include ../deploy/examples/backends/s3.yaml}}
```

```admonish tip title="In-cluster MinIO / RustFS over HTTP"
kopia's S3 path assumes HTTPS. For a plain-HTTP in-cluster endpoint set `tls.disableTls: true`. For a self-signed HTTPS endpoint, prefer pointing `tls.caBundleRef` at a `ConfigMap` holding the CA over `tls.insecureSkipVerify: true`. The endpoint is a bare host[:port] — no scheme.
```

---

## Azure Blob Storage

**Provider prerequisites**

- A **storage account** and a **blob container** within it.
- A credential for that container: either the account **access key** (`AZURE_STORAGE_KEY`) or a **SAS token** (`AZURE_STORAGE_SAS_TOKEN`) scoped to the container. Provide exactly one.

The values you'll actually change: `container`, `prefix`, `storageAccount`.

```yaml
{{#include ../deploy/examples/backends/azure.yaml}}
```

---

## Google Cloud Storage

**Provider prerequisites**

- A **GCS bucket**.
- A **service account** with object admin on that bucket, and a **JSON key** for it. The whole key JSON goes into the Secret under `GOOGLE_APPLICATION_CREDENTIALS`; the mover writes it to a file and points kopia at it.

The values you'll actually change: `bucket`, `prefix`.

```yaml
{{#include ../deploy/examples/backends/gcs.yaml}}
```

---

## Backblaze B2

**Provider prerequisites**

- A **B2 bucket**.
- An **application key** (keyID + key). Prefer a key scoped to the one bucket over the master key.

The values you'll actually change: `bucket`, `prefix`.

```yaml
{{#include ../deploy/examples/backends/b2.yaml}}
```

---

## Filesystem (PVC-backed)

A NAS/PVC repository: the operator mounts a PVC into the mover at `path` and kopia writes the repository there. No object-store keys — only `KOPIA_PASSWORD`.

**Provider prerequisites**

- A **`PersistentVolumeClaim`** the mover can mount read-write (typically `ReadWriteMany` NFS/NAS so backup, restore, and maintenance movers can all reach it). The example includes one.

The values you'll actually change: `path` (mount path inside the mover), `pvcName`.

```yaml
{{#include ../deploy/examples/backends/filesystem.yaml}}
```

```admonish warning title="Filesystem repos are about permissions, not credentials"
The most common filesystem failure is **ownership**, not a wrong key: the repository path must be writable by the UID the mover runs as (default `65532`). If create/connect fails with "permission denied", Kopiur's Warning Event names the exact UID and the `chown -R <uid> <path>` to run. See the [Permissions guide](permissions.md).
```

---

## SFTP

**Provider prerequisites**

- An SFTP account and a **path** on the server for the repository.
- An **SSH private key** for that account (key-based auth is recommended over a password).
- The server's host key, so you can pin `known_hosts` instead of trusting on first use: `ssh-keyscan -p 22 <host>`.

The values you'll actually change: `host`, `port`, `path`, `username`.

```yaml
{{#include ../deploy/examples/backends/sftp.yaml}}
```

---

## WebDAV

**Provider prerequisites**

- A WebDAV **collection URL** (e.g. Nextcloud, Apache `mod_dav`).
- **Basic-auth** username/password with write access to that collection.

The values you'll actually change: `url`.

```yaml
{{#include ../deploy/examples/backends/webdav.yaml}}
```

---

## rclone (everything else)

kopia shells out to `rclone`, so any rclone-supported provider works — Google Drive, OneDrive, Dropbox, pCloud, and dozens more. This is the escape hatch for providers without a native kopia backend.

**Provider prerequisites**

- A working **`rclone.conf`** that defines the remote you'll reference. Generate it locally with `rclone config`, test it with `rclone ls <remote>:`, then put the `rclone.conf` into a Secret.

The values you'll actually change: `remotePath` (rclone's `remote:path`), and the `rclone.conf` contents.

```admonish warning title="rclone uses `configSecretRef`, not `auth`"
Unlike the object-store backends, rclone references its config via `backend.rclone.configSecretRef` — there is no `auth` block. The remote name in `remotePath` (`mydrive:` below) must match a section in the `rclone.conf`.
```

```yaml
{{#include ../deploy/examples/backends/rclone.yaml}}
```

## See also

- [Repositories & backends](repositories.md) — the concepts: scope, encryption, creation, `ClusterRepository`.
- [Permissions, UID & GID](permissions.md) — the filesystem/SFTP ownership story and the mover UID knob.
- [Movers, RBAC & credentials](movers.md) — where the credential Secret must live, and why.
