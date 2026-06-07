# Repositories & backends

A **`Repository`** is _where_ your snapshots live. It is the one resource that holds the storage backend, the encryption password, and the credentials — everything `BackupConfig`/`Backup`/`Restore` need but shouldn't have to repeat. Get this right and the rest of Kopiur just points at it by name.

There are two flavors:

| CRD                     | Scope      | Use it when…                                                                                                                            |
| ----------------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| **`Repository`**        | Namespaced | One namespace owns the backups (the common case). The repo and its credential Secret live together in that namespace.                   |
| **`ClusterRepository`** | Cluster    | A platform team owns one shared repository that **many** tenant namespaces back up to, without each tenant knowing the backend details. |

Both have the **same** backend/encryption/create surface; `ClusterRepository` adds a tenancy gate (`allowedNamespaces`) and identity templating. See [ClusterRepository](#clusterrepository-a-shared-repository) below.

/// tip | One shared repository is the recommended default

Point as many backups as you can at **one** repository. Kopia deduplicates by content hash across every writer, so pooling workloads into a single repository stores their common content once — and each `BackupConfig` writes under its own identity, so their snapshots never collide. See [Recommended: one shared repository](concepts/how-kopia-works.md#recommended-one-shared-repository) for the mechanism and the trade-offs.

///

/// tip | Anatomy of a Repository

```yaml
spec:
    backend: { <one of eight>: { ... } } # WHERE: storage. Exactly one backend.
    encryption: { passwordSecretRef: ... } # the kopia repo password (Secret ref)
    create: { enabled: true } # initialize the repo if absent (default: off)
    cacheDefaults: { ... } # mover cache sizing (optional)
    catalog: { ... } # bounds "discovered" snapshot materialization
    maintenance: { ... } # default-managed; see the Maintenance guide
```

Only `backend` and `encryption` are required. The rest have sane defaults.

///

## The two things every backend needs: identifiers + a Secret

Kopiur deliberately splits a backend into **non-secret connection identifiers** (bucket, endpoint, host, path — these go in the `Repository` spec) and **secrets** (access keys, the encryption password — these live in a Kubernetes `Secret` and are passed to kopia as environment variables, never on the command line or in status). So every object-store backend looks like:

```yaml
spec:
    backend:
        s3:
            bucket: my-backups # ← identifiers in the spec
            auth:
                secretRef:
                    name: repo-creds # ← a Secret holding the access keys
    encryption:
        passwordSecretRef:
            name: repo-creds # ← (can be the same Secret) holding KOPIA_PASSWORD
            key: KOPIA_PASSWORD
```

/// warning | Externally tagged — no `kind:` field

A backend is selected by **which key you set** (`backend.s3`, `backend.azure`, …), not a `kind:` discriminator. `backend: { kind: S3 }` will **not** admit. This is the type-safety design: exactly one backend is representable. (See the [API conventions](dev/api-conventions.md).)

///

### Credential Secret keys by backend

The mover reads these **well-known keys** from the Secret you reference and feeds them to kopia. Put your credentials under these exact key names. `KOPIA_PASSWORD` (the repository encryption password) is required for **every** backend. See [Backend configuration](backends.md#credential-keys-at-a-glance) for the full per-backend setup and the env-vs-file credential detail.

| Backend        | Secret keys the mover reads                                               | Notes                                                                                                       |
| -------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| **S3**         | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, _(opt)_ `AWS_SESSION_TOKEN` | Works for AWS and any S3-compatible store (MinIO, RustFS, Ceph RGW).                                        |
| **Azure**      | `AZURE_STORAGE_KEY` **or** `AZURE_STORAGE_SAS_TOKEN`                      | Account name can come from `spec.backend.azure.storageAccount`.                                             |
| **GCS**        | `KOPIA_GCS_CREDENTIALS` (service-account JSON)                            | The mover writes the JSON to a file and passes `--credentials-file`.                                        |
| **B2**         | `B2_KEY_ID`, `B2_KEY`                                                     | Backblaze application key ID + key.                                                                         |
| **SFTP**       | `KOPIA_SFTP_KEY_DATA`, `KOPIA_SFTP_KNOWN_HOSTS`                           | Key-based auth; the mover writes both to files (`--keyfile`/`--known-hosts`). See [SFTP](backends/sftp.md). |
| **WebDAV**     | `KOPIA_WEBDAV_USERNAME`, `KOPIA_WEBDAV_PASSWORD`                          | HTTP basic auth, via `auth.secretRef`.                                                                      |
| **rclone**     | `KOPIA_RCLONE_CONFIG` (the `rclone.conf`)                                 | Referenced by `backend.rclone.configSecretRef`, not `auth`.                                                 |
| **filesystem** | _(none — local path)_                                                     | Only `KOPIA_PASSWORD` is needed.                                                                            |

/// note | ClusterRepository Secret references need a namespace

Because a `ClusterRepository` is cluster-scoped it has no namespace of its own, so **every** Secret reference in it (`auth.secretRef`, `encryption.passwordSecretRef`) **must** carry an explicit `namespace:` (webhook-enforced). And remember the credential Secret also has to exist in each **workload** namespace — see [Movers → the ClusterRepository gotcha](movers.md#the-credentials-secret-yours-to-place).

///

## The eight backends

Kopiur supports eight backends; each is selected by the `spec.backend.<key>` you set.

| Backend                                                               | `spec.backend` key | Where it goes             |
| --------------------------------------------------------------------- | ------------------ | ------------------------- |
| Amazon S3 + any S3-compatible store (MinIO, RustFS, Ceph RGW, Wasabi) | `s3`               | a bucket                  |
| Azure Blob Storage                                                    | `azure`            | a container               |
| Google Cloud Storage                                                  | `gcs`              | a bucket                  |
| Backblaze B2                                                          | `b2`               | a bucket                  |
| Filesystem (NAS/PVC or inline NFS)                                    | `filesystem`       | a mounted PVC or NFS path |
| SFTP                                                                  | `sftp`             | a path on an SSH server   |
| WebDAV                                                                | `webDav`           | a collection URL          |
| rclone (everything else)                                              | `rclone`           | any rclone remote         |

/// tip | Per-backend setup lives on its own page

For each backend — the **provider prerequisites**, the exact **Secret keys**, the knobs you'll actually change, and a **complete apply-ready manifest** (Secret + Repository in one file) — see [**Backend configuration**](backends.md). That page is the hands-on cookbook; this one is the concepts.

///

## Encryption and repository creation

```yaml
spec:
    encryption:
        passwordSecretRef:
            name: repo-creds
            key: KOPIA_PASSWORD # which key inside the Secret holds the password
    create:
        enabled: true # create the repo if it doesn't exist yet (default: false)
        # The three below are consulted ONLY at creation time, then fixed forever:
        encryption: AES256-GCM-HMAC-SHA256
        splitter: DYNAMIC-4M-BUZHASH
        hash: BLAKE2B-256-128
```

/// warning | Lose the password, lose the backups

`KOPIA_PASSWORD` encrypts the repository. kopia cannot decrypt without it — there is no recovery. Store it outside the cluster (password manager / external secret store), and back up the Secret itself.

///

/// note | `create.enabled` is off by default — on purpose

With creation disabled, a typo in `bucket`/`endpoint` surfaces as a connect failure instead of silently spinning up a brand-new empty repository at the wrong address. Enable it for a genuinely new repo; leave it off to require that one already exists.

///

## Watching a repository

```console
$ kubectl get repository -n demo
NAME      PHASE   BACKEND   AGE
primary   Ready   S3        4m

$ kubectl describe repository primary -n demo # Conditions + Events explain Pending/Failed
```

Phases: `Pending` → `Initializing` → **`Ready`** (healthy). `Degraded` means reachable but a sub-operation (e.g. maintenance) is failing; `Failed` means connect/create failed — the actionable reason is on the conditions. `BackupConfig`/`Backup`/`Restore`/`Maintenance` all wait for `Ready` before doing anything, so this is the first thing to check when a backup won't start.

## ClusterRepository: a shared repository

A `ClusterRepository` is the same backend/encryption surface, made cluster-scoped and shared. A platform team defines it once; tenant namespaces reference it by name (`repository: { kind: ClusterRepository, name: … }`) without seeing the backend or credentials. Two extra fields make sharing safe:

### `allowedNamespaces` — who may use it

A tenancy gate, enforced on every consumer CR (externally tagged — exactly one form):

```yaml
spec:
    allowedNamespaces: { list: ["billing", "media", "wiki"] } # explicit names
    # or:  allowedNamespaces: { selector: { matchLabels: { backups: "yes" } } }
    # or:  allowedNamespaces: { all: true } # any namespace
```

### `identityDefaults` — per-tenant identity

kopia records every snapshot under `username@hostname:path`. For a shared repo you usually want each tenant's snapshots distinguishable. Templates are rendered (Jinja2-compatible) at admission; a consumer's explicit `spec.identity` always wins over the template. Available variables: `Namespace`, `ConfigName`.

```yaml
spec:
    identityDefaults:
        hostnameTemplate: "{{ .Namespace }}"
        usernameTemplate: "{{ .Namespace }}-{{ .ConfigName }}"
```

For namespace `billing` + config `postgres-data`, that resolves to `billing-postgres-data@billing:/pvc/…`. (Both the Go-style `{{ .Namespace }}` and the native tera `{{ Namespace }}` spellings work.)

/// warning | Two requirements for ClusterRepository backups

1. Install the operator with **`installScope=cluster`** (otherwise `ClusterRepository` is never reconciled — see [Installation → scope](install.md#install-scope)).
2. Replicate the credential Secret into each **workload** namespace; Kopiur deliberately does not copy the shared repo's root credentials for you. See [Movers → the ClusterRepository gotcha](movers.md#the-credentials-secret-yours-to-place).

///

A complete, apply-ready example is [`deploy/examples/02-cluster-repository.yaml`](examples.md#example-02--shared-platform-repository).

## The values you'll actually change

| Field                                                  | What it does                                      |
| ------------------------------------------------------ | ------------------------------------------------- |
| `backend.<kind>.bucket` / `container` / `path` / `url` | The storage location.                             |
| `backend.<kind>.prefix`                                | Key prefix so several repos can share one bucket. |
| `backend.s3.endpoint` / `region`                       | Non-AWS endpoint and region.                      |
| `backend.<kind>.auth.secretRef.name`                   | The Secret holding the backend keys.              |
| `encryption.passwordSecretRef.{name,key}`              | Where the kopia password lives.                   |
| `create.enabled`                                       | Whether to initialize a new repository.           |
| `backend.s3.tls.disableTls`                            | Plain-HTTP endpoints (in-cluster MinIO/RustFS).   |
| `allowedNamespaces` _(ClusterRepository)_              | Which namespaces may use the repo.                |
| `identityDefaults` _(ClusterRepository)_               | Per-tenant snapshot identity.                     |

## See also

- [How Kopia works](concepts/how-kopia-works.md) — dedup, the identity model, and why one shared repository maximizes it.
- [Backend configuration](backends.md) — per-backend setup cookbook (prereqs, Secret keys, apply-ready manifests).
- [Movers, RBAC & credentials](movers.md) — where the credential Secret must live.
- [Maintenance](maintenance.md) — the default-managed space reclamation per repo.
- [`deploy/examples/01-single-pvc-scheduled.yaml`](examples.md#example-01--single-pvc-scheduled) — S3 `Repository`, end to end.
- [`deploy/examples/02-cluster-repository.yaml`](examples.md#example-02--shared-platform-repository) — `ClusterRepository`.
