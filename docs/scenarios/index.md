# Scenarios

The [Examples](../examples.md) page is a ladder of **component** manifests — one
CRD capability per file. **Scenarios** are the layer above: end-to-end, problem-
driven walkthroughs for a real situation ("the cluster is gone — get my data
back"), tying several resources, the right `kubectl` commands, and the
verification steps together.

Each scenario is backed by a single apply-ready bundle under
[`deploy/examples/scenarios/`](https://github.com/home-operations/kopiur/tree/main/deploy/examples/scenarios)
— copy it, replace the `REPLACE_ME` values, and `kubectl apply -f`.

/// tip | The mental model (read this first if you're new)

Kopiur splits one job into separate resources so each can change independently:

- a **`Repository`** is _where_ snapshots live (an S3 bucket, a NAS, B2…);
- a **`SnapshotPolicy`** is the **recipe** — _what_ to back up. It runs nothing on its own;
- a **`Snapshot`** is one **invocation** — a single snapshot as a Kubernetes object;
- a **`SnapshotSchedule`** is the **cron** — _when_ the recipe runs;
- a **`Restore`** reads a snapshot back into a PVC;
- a **`Maintenance`** reclaims space in the repository.

The load-bearing detail in the recovery/migration scenarios is **identity**:
kopia stores each snapshot under `username@hostname:path`, defaulting to
`<backup-config-name>@<namespace>:/pvc/<pvcName>`. Resolving an _existing_
snapshot means matching that identity. See [How Kopia works](../concepts/how-kopia-works.md).

///

| #   | Scenario                                                            | When you reach for it                                                 | Complexity |
| --- | ------------------------------------------------------------------ | --------------------------------------------------------------------- | ---------- |
| 01  | [Protect a stateful app](protect-stateful-app.md)                  | Nightly, application-consistent backups of a database on a PVC.       | Simple     |
| 02  | [Recover from accidental data loss](recover-lost-data.md)          | Someone deleted/corrupted data; restore the last good snapshot safely.| Simple     |
| 03  | [Disaster recovery on a fresh cluster](disaster-recovery.md)       | The cluster is gone; rebuild the app + its data from the repository.  | Advanced   |
| 04  | [Migrate an app across clusters / namespaces](migrate-across-clusters.md) | Move a stateful app to a new namespace or cluster, data and all.| Advanced   |
| 05  | [Adopt an existing kopia repository](adopt-existing-repo.md)       | You already have a kopia repo; take it over without re-uploading.     | Advanced   |
| 06  | [Backup verification / restore drills](verification-drills.md)     | Prove backups are restorable on a schedule, and alert when one isn't. | Advanced   |
| 07  | [Point-in-time rollback](point-in-time-rollback.md)                | Roll a volume back to a specific known-good moment (`asOf`/`offset`).  | Simple     |
| 08  | [Clone an app into another namespace](clone-app-to-namespace.md)   | Copy prod data into staging/preview to reproduce a bug or seed an env.| Advanced   |

/// warning | Alpha

These use API group `kopiur.home-operations.com`, version `v1alpha1`. Backends
are **externally tagged** (the bucket lives under `backend.s3`, not
`backend: { kind: S3 }`).

///

## See also

- [Getting started](../getting-started.md) — the install-to-first-restore walkthrough.
- [Examples](../examples.md) — the per-capability manifest ladder.
- [Backups & schedules](../backups.md) and [Restores](../restores.md) — the field-level reference for the resources these scenarios combine.
- [Troubleshooting](../troubleshooting.md) — when a step doesn't go green.
