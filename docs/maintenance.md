# Maintenance

Kopia repositories need periodic **maintenance** to stay healthy: compacting indexes, advancing epochs, and — most importantly — **reclaiming storage** by garbage-collecting content that deleted snapshots no longer reference. Without it, a repository keeps growing even as you expire old backups.

Kopiur makes maintenance a first-class, **default-managed** concern. You don't have to remember to schedule it: every `Repository` and `ClusterRepository` gets a `Maintenance` resource automatically, and the operator runs `kopia maintenance` on a schedule for **every backend** — filesystem and object stores (S3, Azure, GCS, B2, …) alike.

/// info | How it runs

Each scheduled run executes in a short-lived **mover Job** (the same mechanism used for backups and restores), so maintenance works identically whether your repository lives on a PVC or in an object store the operator can't reach directly.

///

## Quick vs. full

kopia has two maintenance passes, and Kopiur schedules them independently:

| Pass      | kopia command                     | What it does                                                                       | Default schedule                        |
| --------- | --------------------------------- | ---------------------------------------------------------------------------------- | --------------------------------------- |
| **Quick** | `kopia maintenance run --no-full` | Cheap, frequent: index compaction, epoch advance.                                  | every 6h (`0 */6 * * *`), 30m jitter    |
| **Full**  | `kopia maintenance run --full`    | Heavier: content garbage-collection + rewrite — this is what **reclaims storage**. | daily at 03:00 (`0 3 * * *`), 1h jitter |

A **full** run subsumes a **quick** run, so when both are due at once the operator runs full and advances both clocks.

## The default-managed model

Maintenance is **on by default**. For every `Repository`/`ClusterRepository`, the operator projects a `Maintenance` resource (named after the repository) with the default schedule above. You can see it with:

```console
$ kubectl get maintenance -A
NAMESPACE   NAME          REPOSITORY    OWNER                          AGE
billing     nas-primary   nas-primary   kopiur/billing/nas-primary     4h44m
```

There are three ways to control it, in increasing order of explicitness.

### 1. Tune it inline on the repository

Set `spec.maintenance` on the `Repository`/`ClusterRepository` to override the schedule (or other knobs) while keeping it operator-managed:

```yaml
apiVersion: kopiur.home-operations.com/v1alpha1
kind: Repository
metadata:
    name: nas-primary
    namespace: billing
spec:
    backend: { filesystem: { path: /repo, pvcName: nas-primary } }
    encryption:
        passwordSecretRef: { name: nas-primary-kopia, key: KOPIA_PASSWORD }
    maintenance:
        enabled: true
        schedule:
            quick: { cron: "0 */6 * * *", jitter: 30m }
            full: { cron: "0 3 * * *", jitter: 1h }
```

`spec.maintenance` fields:

| Field            | Purpose                                                                                                                   |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `enabled`        | Default `true`. Set `false` to opt out (see [Disabling](#disabling-maintenance)).                                         |
| `schedule`       | Override `quick`/`full` cron + jitter (and `timezone`). Absent ⇒ the defaults above.                                      |
| `mover`          | Pod overrides for the maintenance Job (resources, scheduling, security context).                                          |
| `failurePolicy`  | `backoffLimit` / `activeDeadlineSeconds` for the Job.                                                                     |
| `takeoverPolicy` | Ownership-lease policy (see [Ownership](#ownership-and-shared-repositories)).                                             |
| `namespace`      | **`ClusterRepository` only** — which namespace the managed `Maintenance` lives in (defaults to the operator's namespace). |

### 2. Author a standalone `Maintenance`

For fine-grained control — a custom ownership identity or takeover policy — author a `Maintenance` directly. When one references a repository, the operator **defers to it and never creates a duplicate**, even if `spec.maintenance` is otherwise default-on.

```yaml
apiVersion: kopiur.home-operations.com/v1alpha1
kind: Maintenance
metadata:
    name: nas-primary-maintenance
    namespace: billing
spec:
    repository: { kind: Repository, name: nas-primary }
    schedule:
        quick: { cron: "0 */6 * * *", jitter: 30m }
        full: { cron: "0 3 * * 0", jitter: 1h }
        timezone: UTC
    ownership:
        owner: "kopia-operator/nas-primary"
        takeoverPolicy: PromptCondition
    mover:
        resources:
            requests: { cpu: 250m, memory: 1Gi }
            limits: { cpu: "2", memory: 4Gi }
    failurePolicy:
        backoffLimit: 1
        activeDeadlineSeconds: 14400
```

### Disabling maintenance

Set `spec.maintenance.enabled: false` on the repository. The operator stops managing a `Maintenance` for it.

/// warning | Disabling stops space reclamation

With no maintenance, the repository never garbage-collects, so storage grows without bound even as you expire backups. Only disable it if something else runs `kopia maintenance` against the repository. Note that `enabled: false` only tells the operator not to create _its own_ `Maintenance` — an externally-authored one referencing the repository is always honored.

///

## Ownership and shared repositories

kopia tracks a single **maintenance owner** per repository. When several clusters (or operators) share one repository, only one should run maintenance at a time. `spec.ownership` encodes who that is and what to do on conflict:

- `owner` — a stable identity string for this `Maintenance`.
- `takeoverPolicy` — a closed enum:

| Policy                      | Behavior when another owner holds the lease                    |
| --------------------------- | -------------------------------------------------------------- |
| `Never` _(default, safest)_ | Do nothing; surface that the lease is held elsewhere and wait. |
| `PromptCondition`           | Set a condition asking an operator to decide; don't seize it.  |
| `Force`                     | Forcibly claim the lease and run.                              |

The lease is read inside the maintenance Job (which is the only place with repository access for object stores). If the policy declines to take over, the run is a successful no-op that records why on the resource's conditions.

## Inspecting status

```console
$ kubectl get maintenance nas-primary -n billing -o yaml
```

Key `status` fields:

| Field                                                                | Meaning                                                                                                                                       |
| -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `ownership.owner` / `ownership.claimedAt`                            | Current lease holder and when it was claimed.                                                                                                 |
| `quick.lastRunAt` / `full.lastRunAt`                                 | Timestamp of the most recent run of each pass.                                                                                                |
| `quick.lastContentReclaimedBytes` / `full.lastContentReclaimedBytes` | Storage reclaimed — **the only place this is surfaced.**                                                                                      |
| `conditions[type=LeaseOwned]`                                        | `True` when this resource holds the lease and is running; `False` (with a reason) when waiting on the repository, a held lease, or a failure. |

The running mover Jobs are labeled, so you can watch them directly:

```console
$ kubectl get jobs -n billing -l app.kubernetes.io/component=maintenance
```

/// note | Reclaimed bytes currently reports 0

`kopia maintenance run` does not emit a machine-readable reclaimed-bytes figure, so `lastContentReclaimedBytes` is reported as `0` today even though the run does reclaim space. The field exists and round-trips; populating it precisely is a planned enhancement.

///

## Behavior you can rely on

- **Runs at the scheduled time.** Spawning is gated on the same cron + jitter logic as `BackupSchedule`, seeded deterministically per resource, so two replicas agree and the run lands in its window — it is not "every reconcile".
- **Waits for the repository.** Maintenance only runs once the target repository reports `Ready` (an object-store repository must finish connecting or being created first). Until then the resource shows `LeaseOwned=False, reason=WaitingForRepository`.
- **One run at a time.** The operator never starts a second maintenance Job for a repository while one is in flight.
- **Catches up after downtime — once.** If the operator is down across several scheduled slots, it runs a single catch-up pass on recovery, not a storm of missed runs.
- **Self-cleaning Jobs.** Finished maintenance Jobs are removed automatically (`ttlSecondsAfterFinished`); a failed run is retried with backoff.

## See also

- [`deploy/examples/08-maintenance.yaml`](https://github.com/home-operations/kopiur/blob/main/deploy/examples/08-maintenance.yaml) — a standalone `Maintenance`.
- [`deploy/examples/01-single-pvc-scheduled.yaml`](https://github.com/home-operations/kopiur/blob/main/deploy/examples/01-single-pvc-scheduled.yaml) — inline `spec.maintenance`.
- [ADR-0003 §4.5](adr/0003-kopiur-rust-operator.md) — the design rationale.
