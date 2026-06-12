# Google Cloud Storage

The GCS backend stores the kopia repository in a **Google Cloud Storage** bucket,
authenticating with a service-account key. GCS credentials are delivered as a
**file**, not an environment variable (see below).

## Provider prerequisites

- A **GCS bucket** (Kopiur does not create it).
- A **service account** with object admin on that bucket
  (`roles/storage.objectAdmin`, scoped to the bucket where possible), and a **JSON
  key** for it.

/// example | The full provider-side setup with gcloud

```console
# 1. The bucket (uniform bucket-level access, so IAM is the only ACL surface):
$ gcloud storage buckets create gs://my-kopia-backups \
    --location europe-west4 --uniform-bucket-level-access

# 2. A dedicated service account for the movers:
$ gcloud iam service-accounts create kopia \
    --display-name "kopia repository access"

# 3. Object admin on THIS bucket only (not project-wide):
$ gcloud storage buckets add-iam-policy-binding gs://my-kopia-backups \
    --member serviceAccount:kopia@PROJECT.iam.gserviceaccount.com \
    --role roles/storage.objectAdmin

# 4. The JSON key — this file's contents go under KOPIA_GCS_CREDENTIALS:
$ gcloud iam service-accounts keys create key.json \
    --iam-account kopia@PROJECT.iam.gserviceaccount.com
```

`roles/storage.objectAdmin` is the right role: kopia needs to create, read,
list, **and delete** objects (retention and [maintenance](../maintenance.md)
delete expired blobs). The read-only and creator roles both break maintenance.

///

## The Secret shape

GCS is one of the three **file-delivered** backends. kopia's GCS path wants a
credentials _file_, and the SDK env var `GOOGLE_APPLICATION_CREDENTIALS` holds a
_path_, not the JSON — so Kopiur reads the JSON body from a well-known env key and
the mover writes it to a private (`0600`) file, then passes `--credentials-file`.

| Secret key              | Required | What it is                                                                                            |
| ----------------------- | -------- | ----------------------------------------------------------------------------------------------------- |
| `KOPIA_GCS_CREDENTIALS` | yes      | The **full service-account key JSON, verbatim**. Materialized to a file → kopia `--credentials-file`. |
| `KOPIA_PASSWORD`        | **yes**  | The repository encryption password.                                                                   |

```yaml
stringData:
    KOPIA_GCS_CREDENTIALS: |
        { "type": "service_account", "project_id": "...", ... }
    KOPIA_PASSWORD: "choose-something-long-and-random"
```

/// info | Why KOPIA_GCS_CREDENTIALS and not GOOGLE_APPLICATION_CREDENTIALS

`GOOGLE_APPLICATION_CREDENTIALS` is, by Google's own convention, a **path** to a
key file — putting JSON under that name would be misread. Kopiur takes the JSON
**body** under `KOPIA_GCS_CREDENTIALS`, writes it to a `0600` file in the mover,
and points kopia at the path. The secret never lands on kopia's argv.

///

## The Repository

```yaml
--8<-- "deploy/examples/backends/gcs.yaml"
```

## Fields reference (`backend.gcs`)

| Field            | Required | Default     | Example                    | What it controls                                                                                                 |
| ---------------- | -------- | ----------- | -------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `bucket`         | yes      | —           | `my-kopia-backups`         | The GCS bucket holding the repository. The bare name — no `gs://`.                                                |
| `prefix`         | no       | bucket root | `clusters/prod/`           | Object-name prefix so several repos can share one bucket. End it with `/`.                                        |
| `auth.secretRef` | no       | —           | `{ name: gcs-repo-creds }` | Names the credential Secret above. Same namespace as the `Repository`; a `ClusterRepository` adds `namespace:`.   |

## Customization — the values you actually change

- **`bucket` / `prefix`** — where snapshots land.
- **`create.enabled`** — initialize the repository if missing. Creation-time
  algorithms are fixed forever — see [creation](../repositories.md#encryption-and-repository-creation).
- **`moverDefaults.cache`** — mover cache sizing ([movers](../movers.md)).

## As a `ClusterRepository`

The same `backend.gcs` stanza works on a cluster-scoped
[`ClusterRepository`](../repositories.md#clusterrepository-a-shared-repository); every
Secret reference must carry an explicit `namespace:` and the credential Secret
must exist where the movers run — see [Movers](../movers.md).

## Back up and restore against this repository

The lifecycle is backend-independent. Once `Ready`, add a `SnapshotPolicy` +
`SnapshotSchedule` ([Backups & schedules](../backups.md),
[Example 01](../examples.md#example-01--single-pvc-scheduled)) and restore by
picking a `Snapshot` ([Restores](../restores.md),
[Example 03](../examples.md#example-03--restore-by-picking-a-snapshot)).

## Troubleshooting

/// warning | Paste the JSON body, not a path

A common mistake is putting a filename or path under `KOPIA_GCS_CREDENTIALS`. It
must be the **entire key JSON**, verbatim (including the `BEGIN PRIVATE KEY`
block). The mover writes that body to the credentials file for you.

///

- **`403` / permission denied** — the service account lacks object admin on the
  bucket. Grant `roles/storage.objectAdmin` scoped to the bucket. If the bucket
  predates uniform bucket-level access, a legacy object ACL can also deny the SA —
  prefer turning uniform access on.
- **Malformed JSON** — a clipped or re-indented key fails to parse; copy the file
  contents unchanged. The `private_key` field must keep its embedded `\n` escapes.
- **Key rejected after rotation** — a disabled or deleted service-account key
  fails like a wrong key. Mint a new one (`gcloud iam service-accounts keys
  create`) and update the Secret in place; the operator re-verifies on the
  Secret change.

## See also

- [Repositories & backends](../repositories.md) — concepts: scope, encryption, creation.
- [Movers, RBAC & credentials](../movers.md) — where the credential Secret must live.
- Sibling backends: [S3](s3.md) · [Azure](azure.md) · [rclone](rclone.md) (for Google Drive).
