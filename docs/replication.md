# Repository replication

A **`RepositoryReplication`** mirrors a repository's blobs to a **second backend** on a schedule — `kopia repository sync-to` wrapped as a Kubernetes resource (ADR-0005 §13(d)). It is the off-site copy that turns one repository into a 3-2-1 strategy: the same data on a second medium / in a second location.

/// tip | When to reach for it

You already have a primary `Repository` and you want a durable copy elsewhere — a second cloud, a different region, or an on-prem NAS — kept in sync automatically. The mirror is restore-ready: point a `Repository`/`Restore` at the destination backend if the primary is ever lost.

///

## How it works

- **Namespaced**, living alongside its source repository (like `Maintenance`). It references a `Repository` or `ClusterRepository` via `sourceRef`.
- The controller schedules a per-slot mover Job (croner + deterministic jitter, single-flight, repo-ready gate) — the same scheduling kernel `Maintenance` uses. The mover inherits the source repository's `moverDefaults`.
- `destination` is exactly one backend (the same externally-tagged `Backend` shape `Repository` uses) and **must differ** from the source's backend (webhook-enforced).

## Minimal manifest

```yaml
apiVersion: kopiur.home-operations.com/v1alpha1
kind: RepositoryReplication
metadata:
    name: nas-primary-offsite
    namespace: billing
spec:
    sourceRef: # mirror FROM (kind defaults to Repository)
        kind: Repository
        name: nas-primary
    destination: # mirror TO — exactly one backend; must differ from the source
        s3:
            bucket: offsite-mirror
            region: us-west-2
            auth: { secretRef: { name: offsite-mirror-creds } }
    schedule:
        cron: "0 5 * * *" # nightly, after backups land
        jitter: 1h
    suspend: false
```

The full apply-ready manifest is [`deploy/examples/19-repository-replication.yaml`](examples.md#example-19--repository-replication).

## The fields you'll change

| Field | What it does |
| --- | --- |
| `sourceRef` | The repository to mirror from (`Repository`/`ClusterRepository`; `kind` defaults to `Repository`). |
| `destination` | The backend to mirror to. Externally tagged (`destination.s3`, `destination.filesystem`, …). Must differ from the source backend. |
| `destinationEncryption` | A distinct password for the destination repository. **Omit it** to reuse the source repository's password — the common case for a true mirror, where `sync-to` copies blobs verbatim and the format (including encryption material) is identical. |
| `schedule.cron` / `jitter` | When replication runs (Jenkins-style `H` supported, like a `SnapshotSchedule`). |
| `mover` | Per-run mover overrides (resources, scheduling, security context). Inherits the source repository's `moverDefaults`. |
| `suspend` | Pause replication without deleting the CR (ADR-0005 §14(e)). |

## Watching it

```console
$ kubectl get repositoryreplications -n billing
NAME                  SOURCE        DESTINATION   SCHEDULE    LAST   AGE
nas-primary-offsite   nas-primary   s3            0 5 * * *   8h     6d
```

`status` surfaces `lastReplicated`, `nextScheduledAt`, and best-effort `lastReplicatedBytes`/`lastReplicatedBlobs`, plus standard `Ready`/`Reconciling`/`Stalled` conditions (ADR-0005 §2) for `kubectl wait`.

## See also

- [`deploy/examples/19-repository-replication.yaml`](examples.md#example-19--repository-replication)
- [Repositories & backends](repositories.md)
- [Disaster recovery scenario](scenarios/disaster-recovery.md)
