# Repositories & backends

A **`Repository`** is *where* your snapshots live. It is the one resource that holds the storage backend, the encryption password, and the credentials — everything `BackupConfig`/`Backup`/`Restore` need but shouldn't have to repeat. Get this right and the rest of Kopiur just points at it by name.

There are two flavors:

| CRD | Scope | Use it when… |
|---|---|---|
| **`Repository`** | Namespaced | One namespace owns the backups (the common case). The repo and its credential Secret live together in that namespace. |
| **`ClusterRepository`** | Cluster | A platform team owns one shared repository that **many** tenant namespaces back up to, without each tenant knowing the backend details. |

Both have the **same** backend/encryption/create surface; `ClusterRepository` adds a tenancy gate (`allowedNamespaces`) and identity templating. See [ClusterRepository](#clusterrepository-a-shared-repository) below.

```admonish tip title="Anatomy of a Repository"
~~~yaml
spec:
  backend: { <one of eight>: { ... } } # WHERE: storage. Exactly one backend.
  encryption: { passwordSecretRef: ... } # the kopia repo password (Secret ref)
  create: { enabled: true } # initialize the repo if absent (default: off)
  cacheDefaults: { ... } # mover cache sizing (optional)
  catalog: { ... } # bounds "discovered" snapshot materialization
  maintenance: { ... } # default-managed; see the Maintenance guide
~~~
Only `backend` and `encryption` are required. The rest have sane defaults.
```

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

```admonish warning title="Externally tagged — no `kind:` field"
A backend is selected by **which key you set** (`backend.s3`, `backend.azure`, …), not a `kind:` discriminator. `backend: { kind: S3 }` will **not** admit. This is the type-safety design: exactly one backend is representable. (See the [API conventions](dev/api-conventions.md).)
```

### Credential Secret keys by backend

The mover reads these **well-known keys** from the Secret you reference and exports them as the environment variables kopia expects. Put your credentials under these exact key names. `KOPIA_PASSWORD` (the repository encryption password) is required for **every** backend.

| Backend | Secret keys the mover reads | Notes |
|---|---|---|
| **S3** | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, *(opt)* `AWS_SESSION_TOKEN` | Works for AWS and any S3-compatible store (MinIO, RustFS, Ceph RGW). |
| **Azure** | `AZURE_STORAGE_KEY` **or** `AZURE_STORAGE_SAS_TOKEN` | Account name can come from `spec.backend.azure.storageAccount`. |
| **GCS** | `GOOGLE_APPLICATION_CREDENTIALS` (service-account JSON) | The Secret holds the SA key JSON; kopia reads it as the credentials file. |
| **B2** | `B2_KEY_ID`, `B2_KEY` | Backblaze application key ID + key. |
| **SFTP** | SSH private key / password | Supplied via `auth.secretRef`; see [SFTP](#sftp). |
| **WebDAV** | basic-auth username / password | Supplied via `auth.secretRef`. |
| **rclone** | `rclone.conf` | Referenced by `backend.rclone.configSecretRef`, not `auth`. |
| **filesystem** | *(none — local path)* | Only `KOPIA_PASSWORD` is needed. |

```admonish note title="ClusterRepository Secret references need a namespace"
Because a `ClusterRepository` is cluster-scoped it has no namespace of its own, so **every** Secret reference in it (`auth.secretRef`, `encryption.passwordSecretRef`) **must** carry an explicit `namespace:` (webhook-enforced). And remember the credential Secret also has to exist in each **workload** namespace — see [Movers → the ClusterRepository gotcha](movers.md#the-credentials-secret-yours-to-place).
```

## The eight backends

Each block below is the `spec.backend` stanza. Drop it into a `Repository` (or `ClusterRepository`) with an `encryption` block and you're done. A complete, apply-ready S3 example is [`deploy/examples/01-single-pvc-scheduled.yaml`](examples.md#example-01--single-pvc-scheduled); filesystem appears in the [Maintenance guide](maintenance.md).

### S3 / S3-compatible

```yaml
backend:
  s3:
    bucket: my-backups
    prefix: clusters/prod/ # optional; lets several repos share one bucket
    endpoint: s3.us-east-1.amazonaws.com # OMIT for AWS default; SET for MinIO/RustFS/Ceph
    region: us-east-1 # required by AWS and some compatible providers
    auth:
      secretRef: { name: repo-creds }
    tls: # optional — for self-signed or HTTP-only endpoints
      disableTls: false # true ⇒ plain HTTP (in-cluster MinIO/RustFS)
      insecureSkipVerify: false
```

```admonish tip title="In-cluster MinIO / RustFS over HTTP"
kopia's S3 path assumes HTTPS. For a plain-HTTP in-cluster endpoint set `tls.disableTls: true`. For a self-signed HTTPS endpoint, prefer pointing `tls.caBundleRef` at a `ConfigMap` with the CA over `insecureSkipVerify: true`.
```

### Azure Blob Storage

```yaml
backend:
  azure:
    container: kopia-backups
    prefix: prod/ # optional
    storageAccount: mystorageacct # when not inferred from credentials
    auth:
      secretRef: { name: repo-creds } # AZURE_STORAGE_KEY or AZURE_STORAGE_SAS_TOKEN
```

### Google Cloud Storage

```yaml
backend:
  gcs:
    bucket: my-kopia-backups
    prefix: prod/ # optional
    auth:
      secretRef: { name: repo-creds } # GOOGLE_APPLICATION_CREDENTIALS (SA JSON)
```

### Backblaze B2

```yaml
backend:
  b2:
    bucket: my-kopia-backups
    prefix: prod/ # optional
    auth:
      secretRef: { name: repo-creds } # B2_KEY_ID + B2_KEY
```

### Filesystem (PVC-backed)

For a NAS/PVC repository: the operator mounts a PVC into the mover at `path` and kopia writes the repository there. No object-store keys — only `KOPIA_PASSWORD`.

```yaml
backend:
  filesystem:
    path: /repo # mount path inside the mover pod
    pvcName: nas-repo # the PVC mounted at `path` (omit if the path is on the image/node)
```

### SFTP

```yaml
backend:
  sftp:
    host: nas.lan
    port: 22 # optional; defaults to 22
    path: /volume1/kopia
    username: backup
    auth:
      secretRef: { name: repo-creds } # SSH private key / known-hosts / password
```

### WebDAV

```yaml
backend:
  webDav:
    url: https://dav.example.com/kopia
    auth:
      secretRef: { name: repo-creds } # HTTP basic-auth username/password
```

### rclone (everything else)

kopia shells out to `rclone`, so any rclone-supported provider works. The `remotePath` is rclone's `remote:path` form, and the remote must be defined in the `rclone.conf` you supply via a Secret (note: `configSecretRef`, **not** `auth`).

```yaml
backend:
  rclone:
    remotePath: mydrive:backups/kopia
    configSecretRef:
      name: rclone-config # Secret holding the rclone.conf that defines `mydrive`
```

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

```admonish warning title="Lose the password, lose the backups"
`KOPIA_PASSWORD` encrypts the repository. kopia cannot decrypt without it — there is no recovery. Store it outside the cluster (password manager / external secret store), and back up the Secret itself.
```

```admonish note title="`create.enabled` is off by default — on purpose"
With creation disabled, a typo in `bucket`/`endpoint` surfaces as a connect failure instead of silently spinning up a brand-new empty repository at the wrong address. Enable it for a genuinely new repo; leave it off to require that one already exists.
```

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

```admonish warning title="Two requirements for ClusterRepository backups"
1. Install the operator with **`installScope=cluster`** (otherwise `ClusterRepository` is never reconciled — see [Installation → scope](install.md#install-scope)).
2. Replicate the credential Secret into each **workload** namespace; Kopiur deliberately does not copy the shared repo's root credentials for you. See [Movers → the ClusterRepository gotcha](movers.md#the-credentials-secret-yours-to-place).
```

A complete, apply-ready example is [`deploy/examples/02-cluster-repository.yaml`](examples.md#example-02--shared-platform-repository).

## The values you'll actually change

| Field | What it does |
|---|---|
| `backend.<kind>.bucket` / `container` / `path` / `url` | The storage location. |
| `backend.<kind>.prefix` | Key prefix so several repos can share one bucket. |
| `backend.s3.endpoint` / `region` | Non-AWS endpoint and region. |
| `backend.<kind>.auth.secretRef.name` | The Secret holding the backend keys. |
| `encryption.passwordSecretRef.{name,key}` | Where the kopia password lives. |
| `create.enabled` | Whether to initialize a new repository. |
| `backend.s3.tls.disableTls` | Plain-HTTP endpoints (in-cluster MinIO/RustFS). |
| `allowedNamespaces` *(ClusterRepository)* | Which namespaces may use the repo. |
| `identityDefaults` *(ClusterRepository)* | Per-tenant snapshot identity. |

## See also

- [Movers, RBAC & credentials](movers.md) — where the credential Secret must live.
- [Maintenance](maintenance.md) — the default-managed space reclamation per repo.
- [`deploy/examples/01-single-pvc-scheduled.yaml`](examples.md#example-01--single-pvc-scheduled) — S3 `Repository`, end to end.
- [`deploy/examples/02-cluster-repository.yaml`](examples.md#example-02--shared-platform-repository) — `ClusterRepository`.
