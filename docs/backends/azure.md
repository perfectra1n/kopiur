# Azure Blob Storage

The Azure backend stores the kopia repository in an **Azure Blob Storage**
container. Reach for it when your storage is Azure; for an S3-compatible store use
[S3](s3.md) instead.

## Provider prerequisites

- A **storage account** and a **blob container** within it (Kopiur does not create them).
- A credential for that container, **exactly one** of:
    - the storage-account **access key** (`AZURE_STORAGE_KEY`), or
    - a **SAS token** (`AZURE_STORAGE_SAS_TOKEN`) scoped to the container — least privilege.

/// example | Creating the container and credential with the Azure CLI

```console
$ az storage container create \
    --account-name mystorageacct --name kopia-backups

# Option A — the account access key (full-account access):
$ az storage account keys list \
    --account-name mystorageacct --query "[0].value" -o tsv

# Option B — a container-scoped SAS token (least privilege; note the expiry):
$ az storage container generate-sas \
    --account-name mystorageacct --name kopia-backups \
    --permissions racwdl --expiry 2027-06-12 -o tsv
```

The SAS `--permissions` must include **r**ead, **a**dd, **c**reate, **w**rite,
**d**elete, and **l**ist (`racwdl`) — kopia lists and deletes blobs during
retention and [maintenance](../maintenance.md), not just at backup time. Paste
the CLI output as-is: it has no leading `?`.

///

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

| Field            | Required | Default        | Example                      | What it controls                                                                                                |
| ---------------- | -------- | -------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `container`      | yes      | —              | `kopia-backups`              | The blob container holding the repository. The container name only — not a URL, not `account/container`.        |
| `prefix`         | no       | container root | `prod/`                      | Blob-name prefix so several repos can share one container. End it with `/`.                                     |
| `storageAccount` | no       | inferred       | `mystorageacct`              | Account name. In practice set it always: SAS tokens don't carry it, and being explicit costs nothing with a key. |
| `auth.secretRef` | no¹      | —              | `{ name: azure-repo-creds }` | Names the credential Secret above. Same namespace as the `Repository`; a `ClusterRepository` adds `namespace:`. Mutually exclusive with `workloadIdentity`. |
| `auth.workloadIdentity.serviceAccountName` | no¹ | — | `backup-mover`        | Run the mover Jobs as this (user-created, Entra-federated) ServiceAccount instead of a key/SAS — see [Workload identity](#workload-identity-aks). Requires `storageAccount`. |

¹ Set **exactly one** of `auth.secretRef` or `auth.workloadIdentity`
(webhook-enforced). `auth` itself may be omitted when the `AZURE_*` key rides
the encryption-password Secret.

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

## Workload identity (AKS) { #workload-identity-aks }

On AKS with the workload-identity add-on (or any cluster running the
azure-workload-identity webhook), you can drop the storage key and SAS token
entirely: set `auth.workloadIdentity.serviceAccountName` and every mover Job
runs **as that ServiceAccount**. Kopiur stamps the mover pods with the
`azure.workload.identity/use: "true"` label, the Azure webhook injects
`AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_FEDERATED_TOKEN_FILE`, and kopia
authenticates with the federated token. The only secret left in the cluster is
`KOPIA_PASSWORD`.

What you provide:

1. A **managed identity** (or app registration) with
   `Storage Blob Data Contributor` on the container, plus a **federated
   credential** for `system:serviceaccount:<namespace>:<sa-name>`.
2. A **ServiceAccount** annotated `azure.workload.identity/client-id: <id>`,
   present in every namespace mover Jobs run in.
3. `auth.workloadIdentity.serviceAccountName` on the backend — and
   **`storageAccount` becomes required** (webhook-enforced): the identity
   webhook injects the tenant, client id, and token, but not the account name.

```yaml
--8<-- "deploy/examples/backends/azure-workload-identity.yaml"
```

/// warning | Replication: don't mix static and workload-identity Azure pairs

A `RepositoryReplication` between two Azure backends must use the same auth
style on both sides (or both `workloadIdentity` with the same ServiceAccount) —
a mixed pair is rejected at admission, because the replication pod's env would
carry the static side's `AZURE_*` credentials where the federated side's
env-driven flags would pick them up.

///

## As a `ClusterRepository`

The same `backend.azure` stanza works on a cluster-scoped
[`ClusterRepository`](../repositories.md#clusterrepository-a-shared-repository); every
Secret reference must carry an explicit `namespace:` and the Secret must be
present where the movers run — see [Movers](../movers.md).

## Back up and restore against this repository

The lifecycle is backend-independent. Once `Ready`, add a `SnapshotPolicy` +
`SnapshotSchedule` ([Backups & schedules](../backups.md),
[Example 01](../examples.md#example-01--single-pvc-scheduled)) and restore by
picking a `Snapshot` ([Restores](../restores.md),
[Example 03](../examples.md#example-03--restore-by-picking-a-snapshot)).

## Troubleshooting

/// warning | Provide exactly one credential

Setting **both** `AZURE_STORAGE_KEY` and `AZURE_STORAGE_SAS_TOKEN` is ambiguous.
Provide one. A SAS token must be pasted **without** a leading `?` and must grant
read/write/list/delete on the container.

///

/// note | SAS tokens expire — and take your backups offline with them

A SAS token carries an expiry (`se=` in the token). When it lapses, every mover
run starts failing with `AuthenticationFailed` even though nothing in the cluster
changed. Pick an expiry you'll actually rotate before, put the rotation in your
calendar, and update the Secret in place — the operator
[watches the Secret](../repositories.md) and re-verifies the repository without
you touching the `Repository` object.

///

- **`AuthenticationFailed`** — wrong key, expired SAS token (check the `se=`
  timestamp inside the token), or a SAS scoped to the wrong container.
  Regenerate scoped to _this_ container.
- **`ContainerNotFound`** — create the container first; Kopiur won't.
- **Works with the key, fails with SAS** — the SAS is missing a permission
  (needs `racwdl`) or `storageAccount` is unset; a SAS token doesn't encode the
  account name.

## See also

- [Repositories & backends](../repositories.md) — concepts: scope, encryption, creation.
- [Movers, RBAC & credentials](../movers.md) — where the credential Secret must live.
- Sibling backends: [S3](s3.md) · [GCS](gcs.md) · [B2](b2.md).
