# Azure Blob Storage

The Azure backend stores the kopia repository in an **Azure Blob Storage**
container. Reach for it when your storage is Azure; for an S3-compatible store use
[S3](s3.md) instead.

## Provider prerequisites

- A **storage account** and a **blob container** within it (Kopiur does not create them).
- A credential for that container, **exactly one** of:
    - the storage-account **access key** (`AZURE_STORAGE_KEY`), or
    - a **SAS token** (`AZURE_STORAGE_SAS_TOKEN`) scoped to the container — least privilege.

## The Secret shape

Loaded with `envFrom`; the keys reach kopia as environment variables.

| Secret key                | Required | What it is                                            |
| ------------------------- | -------- | ----------------------------------------------------- |
| `AZURE_STORAGE_KEY`       | one of¹  | The storage-account access key.                       |
| `AZURE_STORAGE_SAS_TOKEN` | one of¹  | A SAS token scoped to the container (no leading `?`). |
| `KOPIA_PASSWORD`          | **yes**  | The repository encryption password.                   |

¹ Provide **exactly one** of the key or the SAS token. kopia uses whichever is set.

```yaml
stringData:
    AZURE_STORAGE_KEY: "REPLACE_ME" # OR AZURE_STORAGE_SAS_TOKEN, not both
    KOPIA_PASSWORD: "choose-something-long-and-random"
```

/// warning | Lose the password, lose the backups

`KOPIA_PASSWORD` encrypts the repository and cannot be recovered if lost. Store it
outside the cluster and back up the Secret. See [Encryption](../repositories.md#encryption-and-repository-creation).

///

## The Repository

```yaml
--8<-- "deploy/examples/backends/azure.yaml"
```

## Fields reference (`backend.azure`)

| Field            | Required | Default        | What it controls                                                                     |
| ---------------- | -------- | -------------- | ------------------------------------------------------------------------------------ |
| `container`      | yes      | —              | The blob container holding the repository.                                           |
| `prefix`         | no       | container root | Blob-name prefix so several repos can share one container.                           |
| `storageAccount` | no       | inferred       | Account name; set it when not encoded in the credential (SAS tokens don't carry it). |
| `auth.secretRef` | no       | —              | Names the credential Secret above.                                                   |

## Customization — the values you actually change

- **`container` / `prefix`** — where snapshots land.
- **`storageAccount`** — usually required; SAS tokens don't encode the account.
- **Key vs. SAS** — switch by which Secret key you set (see the SAS variant below).
- **`create.enabled`** — initialize the repository if missing. Creation-time
  `encryption`/`splitter`/`hash` are fixed forever — see [creation](../repositories.md#encryption-and-repository-creation).

### SAS-token auth (least privilege)

A SAS token scoped to the container, time-limited, avoids handing the mover the
full account key:

```yaml
--8<-- "deploy/examples/backends/azure-sas.yaml"
```

## As a `ClusterRepository`

The same `backend.azure` stanza works on a cluster-scoped
[`ClusterRepository`](../repositories.md#clusterrepository-a-shared-repository); every
Secret reference must carry an explicit `namespace:` and the Secret must be
present where the movers run — see [Movers](../movers.md).

## Back up and restore against this repository

The lifecycle is backend-independent. Once `Ready`, add a `BackupConfig` +
`BackupSchedule` ([Backups & schedules](../backups.md),
[Example 01](../examples.md#example-01--single-pvc-scheduled)) and restore by
picking a `Backup` ([Restores](../restores.md),
[Example 03](../examples.md#example-03--restore-by-picking-a-backup)).

## Troubleshooting

/// warning | Provide exactly one credential

Setting **both** `AZURE_STORAGE_KEY` and `AZURE_STORAGE_SAS_TOKEN` is ambiguous.
Provide one. A SAS token must be pasted **without** a leading `?` and must grant
read/write/list/delete on the container.

///

- **`AuthenticationFailed`** — wrong key, expired SAS token, or a SAS scoped to the
  wrong container. Regenerate scoped to _this_ container.
- **`ContainerNotFound`** — create the container first; Kopiur won't.

## See also

- [Repositories & backends](../repositories.md) — concepts: scope, encryption, creation.
- [Movers, RBAC & credentials](../movers.md) — where the credential Secret must live.
- Sibling backends: [S3](s3.md) · [GCS](gcs.md) · [B2](b2.md).
