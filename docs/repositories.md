# Repositories & backends

A **`Repository`** is _where_ your snapshots live. It is the one resource that holds the storage backend, the encryption password, and the credentials — everything `SnapshotPolicy`/`Snapshot`/`Restore` need but shouldn't have to repeat. Get this right and the rest of Kopiur just points at it by name.

There are two flavors:

| CRD                     | Scope      | Use it when…                                                                                                                            |
| ----------------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| **`Repository`**        | Namespaced | One namespace owns the backups (the common case). The repo and its credential Secret live together in that namespace.                   |
| **`ClusterRepository`** | Cluster    | A platform team owns one shared repository that **many** tenant namespaces back up to, without each tenant knowing the backend details. |

Both have the **same** backend/encryption/create surface; `ClusterRepository` adds a tenancy gate (`allowedNamespaces`) and per-tenant identity CEL expressions. See [ClusterRepository](#clusterrepository-a-shared-repository) below.

/// tip | One shared repository is the recommended default

Point as many backups as you can at **one** repository. Kopia deduplicates by content hash across every writer, so pooling workloads into a single repository stores their common content once — and each `SnapshotPolicy` writes under its own identity, so their snapshots never collide. See [Recommended: one shared repository](concepts/how-kopia-works.md#recommended-one-shared-repository) for the mechanism and the trade-offs.

///

/// tip | Anatomy of a Repository

```yaml
spec:
    backend: { <one of eight>: { ... } } # WHERE: storage. Exactly one backend.
    encryption: { passwordSecretRef: ... } # the kopia repo password (Secret ref)
    create: { enabled: true, ecc: { ... } } # initialize the repo if absent (default: off)
    moverDefaults: { ... } # base config for EVERY mover (SC, resources, cache, ...)
    catalog: { ... } # bounds "discovered" snapshot materialization
    maintenance: { ... } # default-managed; see the Maintenance guide
    onNamespaceDelete: Orphan # Orphan (default) | Delete — kubectl delete ns behavior
    mode: ReadWrite # ReadWrite (default) | ReadOnly
    suspend: false # pause connect/bootstrap + maintenance
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

Because a `ClusterRepository` is cluster-scoped it has no namespace of its own, so **every** Secret reference in it (`auth.secretRef`, `encryption.passwordSecretRef`) **must** carry an explicit `namespace:` (webhook-enforced). The credential Secret also has to exist in each **workload** namespace a mover runs in — either place it there yourself, or (recommended for a shared repo) turn on [**credential projection**](movers.md#let-kopiur-project-the-credentials-secret-recommended-for-shared-repos) and let Kopiur copy it for you.

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
        # All of the below are consulted ONLY at creation time, then fixed forever
        # (webhook- AND apiserver-immutable: editing them is rejected, ADR-0005 §7):
        encryption: AES256-GCM-HMAC-SHA256
        splitter: DYNAMIC-4M-BUZHASH
        hash: BLAKE2B-256-128
        ecc: # Reed-Solomon parity guarding blobs against backend bit-rot (ADR-0005 §13(a))
            algorithm: REED-SOLOMON-CRC32
            overheadPercent: 2
```

/// note | Create-time settings are immutable

`create.{splitter,hash,encryption,ecc}` and the pinned identity are fixed at repository creation. Editing them is **rejected** (by the webhook and by CRD `x-kubernetes-validations` transition rules) with an actionable message — create a new `Repository` instead of mutating these.

The `encryption.passwordSecretRef` is **not** in this set: you may rename or repoint the Secret freely (e.g. a GitOps secret rename) as long as it still resolves to the **same password value** — kopia bakes only the resolved value into the repository format, not the reference. Point it at a *different* password and kopia simply fails to open the repository at connect time (a recoverable runtime error, not an admission rejection).

///

/// warning | Lose the password, lose the backups

`KOPIA_PASSWORD` encrypts the repository. kopia cannot decrypt without it — there is no recovery. Store it outside the cluster (password manager / external secret store), and back up the Secret itself.

///

/// note | `create.enabled` is off by default — on purpose

With creation disabled, a typo in `bucket`/`endpoint` surfaces as a connect failure instead of silently spinning up a brand-new empty repository at the wrong address. Enable it for a genuinely new repo; leave it off to require that one already exists.

If the backend holds **no** repository yet and `create.enabled` is off, the `Repository`/`ClusterRepository` goes `Failed` with a `Bootstrapped=False`, `reason: RepositoryNotInitialized` condition whose message tells you exactly what to do — set `create.enabled: true` to initialize it, or point the backend at an existing repository. (This is distinct from a wrong-password connect, which fails `AuthFailure` and **never** recreates over the existing data — see [Safe by construction](#safe-by-construction) below.)

///

### Safe by construction

`create.enabled: true` does **not** mean "always create". Every bootstrap **connects first** and only creates as a fallback, gated so that an existing repository is never overwritten:

- **A repository already exists and the password is correct** → kopiur *connects and adopts* it (and materializes any snapshots already in the store as `discovered` Snapshots). No creation happens.
- **A repository already exists but the password is wrong** → connect fails `AuthFailure`; kopiur **does not** create (that would risk a second repository or mask the wrong-password error). The `Repository` goes `Failed` and the existing repository is left untouched.
- **No repository exists** → kopiur creates one (only because `create.enabled` is on). As a final backstop, kopia's own `repository create` refuses to overwrite an existing repository, so even a misclassified connect cannot clobber your data.

So enabling `create.enabled` for a repository that turns out to already exist is safe: kopiur will adopt it, not re-initialize it.

## `moverDefaults` — one place to configure every mover

A repository spawns movers for **everything** — bootstrap (connect/create), backup, restore, maintenance. `moverDefaults` is the single base they all inherit (ADR-0004 §1); a per-recipe `mover` block (on `SnapshotPolicy`/`Restore`/`Maintenance`) overlays it **field-wise** (the recipe wins, the default fills, the hardened security baseline sits underneath — so a partial override can only tighten, never drop `drop:[ALL]`/seccomp).

```yaml
spec:
    moverDefaults:
        securityContext: # container SC — runAsUser/runAsGroup, caps, seccomp
            runAsUser: 1000 # the UID every mover runs as
            runAsGroup: 1000 # the GID every mover runs as
        podSecurityContext: # pod SC — notably fsGroup
            fsGroup: 1000 # make fresh restore volumes group-writable, set once here
        resources: { requests: { cpu: 250m, memory: 512Mi } }
        cache: # kopia cache backing every mover (capacity/class/budgets)
            capacity: 10Gi
            storageClassName: fast-ssd
        nodeSelector: { kubernetes.io/arch: amd64 }
        tolerations: [{ key: backup, operator: Exists }]
        affinity: { ... }
        ttlSecondsAfterFinished: 3600 # finished mover Jobs self-GC (ADR-0005 §12)
        throttle: # cap kopia's bandwidth/ops so a run doesn't saturate the link
            uploadBytesPerSecond: 10485760
            downloadBytesPerSecond: 10485760
```

/// tip | Set the mover UID/GID once, for every mover

Set the UID/GID **once** on `moverDefaults.securityContext.runAsUser/runAsGroup` (and `podSecurityContext.fsGroup`), and every mover the repository spawns — **including the bootstrap (connect/create) Job** — inherits it. That means a filesystem/NFS repository on a directory not owned by `65532` is bootstrappable with no special-case knob: the bootstrap mover runs as the UID you set here. A per-recipe `mover` block can still tighten any of these for an individual `SnapshotPolicy`/`Restore`/`Maintenance`. See [example 09](examples.md#example-09--mover-uidgid--permissions) and [Permissions](permissions.md).

`podSecurityContext.fsGroup` already **defaults to `65532`** (the mover image's GID), so the operator-managed kopia cache is writable out of the box — you only set it here to match a non-default mover UID. Note `fsGroup` can't fix a root-squashed **NFS** cache StorageClass; keep `moverDefaults.cache` unset (node-local `emptyDir`) or use a block class for a sized cache (see [Security context](security-context.md#the-default-hardened-context)).

///

## `onNamespaceDelete` — what `kubectl delete ns` does to snapshots

`Orphan` (default) or `Delete` (ADR-0005 §5). A backup tool must not make deleting a namespace a silent data-loss event, so the default is fail-safe:

| Value | On namespace deletion |
| --- | --- |
| `Orphan` _(default)_ | Release ownership (drop the `Snapshot` finalizers) **without** deleting the kopia snapshots — off-site history survives. |
| `Delete` | Cascade: each `Snapshot`'s own `deletionPolicy` applies (produced snapshots are `kopia snapshot delete`d). Opt-in. |

This is distinct from a single `kubectl delete snapshot`, which always honors that one `Snapshot`'s `deletionPolicy`.

## `mode` — ReadWrite or ReadOnly

`mode: ReadWrite` (default) or `ReadOnly` (ADR-0005 §11). A `ReadOnly` repository connects read-only and serves **restores only** — the operator refuses backup Jobs and skips maintenance projection. Use it to decommission a backend or migrate between repositories without any risk of writes.

## `suspend` — pause a repository

`suspend: true` (ADR-0005 §14(e)) pauses connect/bootstrap and maintenance projection declaratively, without deleting the `Repository`. Surfaced via a condition. `suspend` is consistent across `Repository`/`ClusterRepository`/`SnapshotPolicy`/`RepositoryReplication`.

## Watching a repository

```console
$ kubectl get repository -n demo
NAME      PHASE   BACKEND   AGE
primary   Ready   S3        4m

$ kubectl describe repository primary -n demo # Conditions + Events explain Pending/Failed
```

Phases: `Pending` → `Initializing` → **`Ready`** (healthy). `Degraded` means reachable but a sub-operation (e.g. maintenance) is failing; `Failed` means connect/create failed — the actionable reason is on the conditions. `SnapshotPolicy`/`Snapshot`/`Restore`/`Maintenance` all wait for `Ready` before doing anything, so this is the first thing to check when a backup won't start.

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

### `identityDefaults` — per-tenant identity (CEL)

kopia records every snapshot under `username@hostname:path`. For a shared repo you usually want each tenant's snapshots distinguishable. `identityDefaults` are **CEL expressions** (`*Expr`, the kromgo `valueExpr`/`colorExpr` convention), evaluated at admission and pinned to status; a consumer's explicit `spec.identity` always wins.

```yaml
spec:
    identityDefaults:
        hostnameExpr: "namespace"
        usernameExpr: "namespace + '-' + policyName"
```

For namespace `billing` + policy `postgres-data`, that resolves to `billing-postgres-data@billing:/pvc/…`.

The CEL **environment** is the consuming `SnapshotPolicy`'s metadata:

| Variable | Type | Is |
| --- | --- | --- |
| `namespace` | string | the SnapshotPolicy's namespace |
| `policyName` | string | the SnapshotPolicy's name |
| `labels` | map | `metadata.labels` |
| `annotations` | map | `metadata.annotations` |

Each `*Expr` must return a **string**. Conditionals and map access come for free:

```yaml
identityDefaults:
    hostnameExpr: "'team' in labels ? labels['team'] : namespace"
    usernameExpr: "namespace + '-' + policyName + (labels['env'] == 'prod' ? '-prod' : '')"
```

/// note | How `*Expr` evaluation is bounded

Each `*Expr` is a CEL expression returning a **string**. CEL is sandboxed (no I/O, no arbitrary code) and the expression is **validated at admission** — a syntax error, a wrong return type, or a reference to a variable outside the documented environment (`namespace`/`policyName`/`labels`/`annotations`) is rejected on `kubectl apply`, not discovered at backup time. Evaluation is bounded by CEL's cost budget, and each expression is capped at ~1 KiB.

///

### `credentialProjection.allowed` — the owner gate for shared creds

By default a `ClusterRepository` will **not** let its credential Secret be projected into a foreign consumer namespace (`credentialProjection.allowed` defaults `false`, ADR-0005 §8). Projection is fail-closed: it requires the repository owner's `allowed: true` **and** the consumer's `credentialProjection.enabled: true` **and** the operator's `secrets` RBAC. A namespaced `Repository` has no such gate (its repo and Secret co-reside).

```yaml
spec:
    credentialProjection:
        allowed: true # owner permits projection; consumers still opt in per-CR
```

See [Movers → credential projection](movers.md#let-kopiur-project-the-credentials-secret-recommended-for-shared-repos).

/// warning | Two requirements for ClusterRepository backups

1. Install the operator with **`installScope=cluster`** (otherwise `ClusterRepository` is never reconciled — see [Installation → scope](install.md#install-scope)).
2. Get the credential Secret into each **workload** namespace a mover runs in. The easy way: set [`credentialProjection.enabled: true`](movers.md#let-kopiur-project-the-credentials-secret-recommended-for-shared-repos) on the `SnapshotPolicy`/`Restore`/`Maintenance` that uses this repository, and Kopiur copies it for you (off by default, recommended for shared repos). Otherwise replicate it yourself.

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
| `identityDefaults` _(ClusterRepository)_               | Per-tenant snapshot identity (CEL `*Expr`).       |
| `moverDefaults`                                        | Base security context / resources / cache for every mover. |
| `onNamespaceDelete`                                    | `Orphan` (default) / `Delete` on namespace delete.|
| `mode`                                                 | `ReadWrite` (default) / `ReadOnly`.               |
| `suspend`                                              | Pause connect/bootstrap + maintenance.            |

## See also

- [How Kopia works](concepts/how-kopia-works.md) — dedup, the identity model, and why one shared repository maximizes it.
- [Backend configuration](backends.md) — per-backend setup cookbook (prereqs, Secret keys, apply-ready manifests).
- [Movers, RBAC & credentials](movers.md) — where the credential Secret must live.
- [Maintenance](maintenance.md) — the default-managed space reclamation per repo.
- [`deploy/examples/01-single-pvc-scheduled.yaml`](examples.md#example-01--single-pvc-scheduled) — S3 `Repository`, end to end.
- [`deploy/examples/02-cluster-repository.yaml`](examples.md#example-02--shared-platform-repository) — `ClusterRepository`.
