# Google Cloud Storage

The GCS backend stores the kopia repository in a **Google Cloud Storage** bucket,
authenticating with a service-account key. GCS credentials are delivered as a
**file**, not an environment variable (see below).

## Provider prerequisites

- A **GCS bucket** (Kopiur does not create it).
- A **service account** with object admin on that bucket
  (`roles/storage.objectAdmin`, scoped to the bucket where possible), and a **JSON
  key** for it. Create one with:

    ```console
    $ gcloud iam service-accounts keys create key.json \
        --iam-account kopia@PROJECT.iam.gserviceaccount.com
    ```

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

| Field            | Required | Default     | What it controls                                          |
| ---------------- | -------- | ----------- | --------------------------------------------------------- |
| `bucket`         | yes      | —           | The GCS bucket holding the repository.                    |
| `prefix`         | no       | bucket root | Object-name prefix so several repos can share one bucket. |
| `auth.secretRef` | no       | —           | Names the credential Secret above.                        |

## Customization — the values you actually change

- **`bucket` / `prefix`** — where snapshots land.
- **`create.enabled`** — initialize the repository if missing. Creation-time
  algorithms are fixed forever — see [creation](../repositories.md#encryption-and-repository-creation).
- **`cacheDefaults`** — mover cache sizing ([movers](../movers.md)).

## As a `ClusterRepository`

The same `backend.gcs` stanza works on a cluster-scoped
[`ClusterRepository`](../repositories.md#clusterrepository-a-shared-repository); every
Secret reference must carry an explicit `namespace:` and the credential Secret
must exist where the movers run — see [Movers](../movers.md).

## Back up and restore against this repository

The lifecycle is backend-independent. Once `Ready`, add a `BackupConfig` +
`BackupSchedule` ([Backups & schedules](../backups.md),
[Example 01](../examples.md#example-01--single-pvc-scheduled)) and restore by
picking a `Backup` ([Restores](../restores.md),
[Example 03](../examples.md#example-03--restore-by-picking-a-backup)).

## Troubleshooting

/// warning | Paste the JSON body, not a path

A common mistake is putting a filename or path under `KOPIA_GCS_CREDENTIALS`. It
must be the **entire key JSON**, verbatim (including the `BEGIN PRIVATE KEY`
block). The mover writes that body to the credentials file for you.

///

- **`403` / permission denied** — the service account lacks object admin on the
  bucket. Grant `roles/storage.objectAdmin` scoped to the bucket.
- **Malformed JSON** — a clipped or re-indented key fails to parse; copy the file
  contents unchanged.

## See also

- [Repositories & backends](../repositories.md) — concepts: scope, encryption, creation.
- [Movers, RBAC & credentials](../movers.md) — where the credential Secret must live.
- Sibling backends: [S3](s3.md) · [Azure](azure.md) · [rclone](rclone.md) (for Google Drive).
