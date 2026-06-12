# Examples

A walkthrough of the manifests in [`deploy/examples/`](https://github.com/home-operations/kopiur/tree/main/deploy/examples). Each is a complete, apply-ready manifest; copy one, replace the `REPLACE_ME` secrets and PVC/bucket names, and `kubectl apply -f`.

/// tip | Looking for a whole workflow, not one capability?

This page is a ladder of **per-capability** manifests. For end-to-end, problem-driven walkthroughs — protect a database, recover deleted data, disaster recovery, cross-cluster migration, adopting an existing repo, verification drills — see [**Scenarios**](scenarios/index.md).

///

/// info | Single source

The YAML below is pulled directly from the manifests in `deploy/examples/` at build time (MkDocs snippets), so the docs never drift from the files you actually apply. Each manifest carries its own inline comments.

///

/// tip | The mental model

Kopiur separates the backup **recipe** (`SnapshotPolicy`) from its **invocation** (`Snapshot`) from its **schedule** (`SnapshotSchedule`). A `SnapshotPolicy` runs nothing on its own — a `SnapshotSchedule` (cron) or a `Snapshot` (manual / external trigger) is what produces a snapshot. The [`Repository`](repositories.md) holds the storage, and [`Maintenance`](maintenance.md) reclaims it. You can apply a whole bundle at once; the operator resolves the ordering.

///

| #   | Example                                                                 | Demonstrates                                                            |
| --- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| 01  | [Single PVC, scheduled](#example-01--single-pvc-scheduled)              | The canonical first backup: Repository → SnapshotPolicy → SnapshotSchedule. |
| 02  | [Shared platform repository](#example-02--shared-platform-repository)   | A cluster-scoped `ClusterRepository` tenants reference without secrets. |
| 03  | [Restore by picking a Snapshot](#example-03--restore-by-picking-a-snapshot) | Restore is "pick a row" from the catalog — no timestamp math.           |
| 04  | [Multi-PVC selector](#example-04--multi-pvc-selector)                   | Back up every PVC matching a label as one consistent group.             |
| 05  | [Deploy-or-restore (GitOps)](#example-05--deploy-or-restore-gitops)     | One bundle that restores on a fresh cluster, backs up otherwise.        |
| 06  | [Manual one-shot backup](#example-06--manual-one-shot-backup)           | A `Snapshot` CR as the universal trigger.                                 |
| 07  | [Restore a discovered backup](#example-07--restore-a-discovered-backup) | Restore foreign / pre-install snapshots.                                |
| 08  | [Maintenance](#example-08--maintenance)                                 | A standalone `Maintenance` for fine-grained control.                    |
| 09  | [Mover UID/GID & permissions](#example-09--mover-uidgid--permissions)   | Match the mover's UID/GID to the data owner so it can read it.          |
| 10  | [NFS source (no PVC)](#example-10--nfs-source-no-pvc)                   | Back up a NAS export directly — no PersistentVolumeClaim.               |
| 11  | [Credential projection](#example-11--credential-projection)             | Let the operator copy the repo Secret into each mover namespace.        |
| 12  | [Restore mover, cache & failure policy](#example-12--restore-mover-cache--failure-policy) | Give a `Restore` the same UID/GID, cache, and retry knobs a backup has. |
| 13  | [Restore by raw kopia identity](#example-13--restore-by-raw-kopia-identity) | Restore a foreign / aged-out snapshot by `username@hostname:path`. |
| 14  | [Point-in-time / offset restore](#example-14--point-in-time--offset-restore) | "Roll back to Tuesday 2am" — restore via `asOf` / `offset`. |
| 15  | [In-place mirror restore](#example-15--in-place-mirror-restore) | Restore into an existing PVC and make it an exact mirror. |
| 16  | [Cross-namespace clone restore](#example-16--cross-namespace-clone-restore) | Clone one namespace's snapshot into another (prod → staging). |
| 17  | [Restore from a shared repo (projection)](#example-17--restore-from-a-shared-repo-projection) | Restore from a `ClusterRepository` into a fresh namespace, creds projected. |
| 18  | [Inherit the mover security context](#example-18--inherit-the-mover-security-context-from-a-workload) | Run the mover as "whatever the app runs as" by selecting the workload. |
| 19  | [Repository replication](#example-19--repository-replication)           | Mirror a repository to a second backend (the "2" in 3-2-1).             |
| 20  | [Quiesce with hooks](#example-20--quiesce-with-hooks)                    | Run workloadExec/httpRequest hooks around the snapshot for app-consistent backups. |

/// tip | Looking for a specific storage backend?

Each backend (S3, Azure, GCS, B2, filesystem, SFTP, WebDAV, rclone) has its own dedicated page — provider prerequisites, the exact Secret shape, a field-by-field reference, and a complete apply-ready manifest — starting from the [Backend configuration](backends/index.md) index. The apply-ready manifests themselves live under [`deploy/examples/backends/`](https://github.com/home-operations/kopiur/tree/main/deploy/examples/backends). The numbered ladder below is the task/workflow tutorial.

///

/// warning | Alpha

These use API group `kopiur.home-operations.com`, version `v1alpha1`. Backends are **externally tagged** (the bucket lives under `backend.s3`, not `backend: { kind: S3 }`).

///

---

## Example 01 — Single PVC, scheduled

The canonical first backup: one `Repository` (S3), one `SnapshotPolicy` (the idempotent recipe), one `SnapshotSchedule` (the cron that creates `Snapshot` CRs). Maintenance is implicit — a default `Maintenance` is created for the repository unless you override or disable it.

```yaml
--8<-- "deploy/examples/01-single-pvc-scheduled.yaml"
```

---

## Example 02 — Shared platform repository

A platform team owns a cluster-scoped `ClusterRepository`; tenant namespaces reference it without knowing the secret name or backend details. Per-consumer identity is templated at admission.

/// note

Requires the operator installed with `installScope=cluster`. Because `ClusterRepository` is cluster-scoped, its Secret references **must** carry an explicit `namespace` (webhook-enforced).

///

```yaml
--8<-- "deploy/examples/02-cluster-repository.yaml"
```

---

## Example 03 — Restore by picking a Snapshot

Browse the catalog, then reference a specific `Snapshot` CR. No timestamp math — restore is "pick a row". `source` and `target` are externally-tagged (`target.pvc` creates a PVC; `target.pvcRef` writes into an existing one).

```console
# list candidate snapshots for a policy, newest last:
$ kubectl get snapshots -n billing \
    -l kopiur.home-operations.com/config=postgres-data \
    --sort-by=.status.timing.startTime
```

```yaml
--8<-- "deploy/examples/03-restore-by-backup.yaml"
```

---

## Example 04 — Multi-PVC selector

Back up every PVC matching a label as one consistent group (one `VolumeGroupSnapshot` across all matched PVCs). A `Source` has mutually-exclusive `pvc` and `pvcSelector` (webhook-enforced).

```yaml
--8<-- "deploy/examples/04-multi-pvc-selector.yaml"
```

---

## Example 05 — Deploy-or-restore (GitOps)

The headline GitOps pattern. Apply everything together: on a **fresh cluster against an existing repo**, the PVC restores the latest snapshot before the app starts; on a **fresh repo**, the PVC comes up empty and is backed up going forward. The trick is a **passive `Restore`** (`target.populator: {}`, `source.fromPolicy`, `onMissingSnapshot: Continue`) consumed by a PVC's `dataSourceRef` as a volume populator.

/// note

The volume-populator handshake needs Kubernetes ≥ 1.24 (`AnyVolumeDataSource`).

///

```yaml
--8<-- "deploy/examples/05-deploy-or-restore-gitops.yaml"
```

---

## Example 06 — Manual one-shot backup

A `Snapshot` CR is the universal trigger — created by a `SnapshotSchedule`, by `kubectl create`, or by any external system (Argo Events, Tekton, CI). The trigger is separable from the recipe. `deletionPolicy` is `Delete` (default for produced) | `Retain` | `Orphan`.

```yaml
--8<-- "deploy/examples/06-manual-backup.yaml"
```

---

## Example 07 — Restore a discovered backup

Snapshots the operator did **not** produce (a foreign kopia writer, or snapshots predating the install) are materialized as `Snapshot` CRs with `origin=discovered` in the repository's namespace, forced to `deletionPolicy: Retain`. Restore one **(A)** by referencing the discovered `Snapshot` CR, or **(B)** by a raw kopia identity (which requires an explicit `spec.repository`).

```console
# list discovered snapshots in the repo namespace:
$ kubectl get snapshots -n backups -l kopiur.home-operations.com/origin=discovered
```

```yaml
--8<-- "deploy/examples/07-restore-discovered.yaml"
```

---

## Example 08 — Maintenance

Maintenance is default-managed (see the [Maintenance guide](maintenance.md)), but you can author a standalone `Maintenance` for fine-grained control — a custom ownership identity or takeover policy. When a user-authored `Maintenance` references a repository, the operator defers to it and never creates a duplicate.

```yaml
--8<-- "deploy/examples/08-maintenance.yaml"
```

---

## Example 09 — Mover UID/GID & permissions

The mover Job is a separate pod that mounts and reads your PVC, so it must run as a UID/GID that can read the data (and, on restore, write the target). This `SnapshotPolicy` sets `spec.mover.securityContext.runAsUser/runAsGroup` to match the owning user, and comments the root-mover variant for data you can't match. See the [Permissions guide](permissions.md) for how to find the right numbers.

```yaml
--8<-- "deploy/examples/09-mover-permissions.yaml"
```

---

## Example 10 — NFS source (no PVC)

Back up a NAS export directly: `source.nfs` names an NFS `server` + `path` instead of a `pvc`, and the operator mounts the export read-only into the backup mover for kopia to snapshot — no `PersistentVolumeClaim`, no StorageClass. kopia records the export `path` as the snapshot source path by default (override with `sourcePathOverride`). An NFS source is mutually exclusive with `pvc`/`pvcSelector` (webhook-enforced) and works with **any** repository backend. The repository here is itself an [inline-NFS filesystem repo](backends/filesystem.md#inline-nfs-no-pvc), but that's independent of the source.

```yaml
--8<-- "deploy/examples/10-nfs-source.yaml"
```

## Example 11 — Credential projection

A shared `ClusterRepository` keeps its credential Secret in the operator namespace, but movers run in workload namespaces and load creds via namespace-local `envFrom` — so normally you copy that Secret into every namespace yourself. Setting `credentialProjection.enabled: true` on the **`SnapshotPolicy`** (also available on `Restore`/`Maintenance`) opts out of that chore: before each run the operator projects the repository's Secret into the mover's namespace, owned by the consuming `Snapshot`/`Restore`/`Maintenance` (garbage-collected with it) and refreshed from source each run. It's **off by default** (cross-namespace copying is opt-in) but is the **recommended** path for a shared repository spanning several namespaces. It needs the operator's cluster-wide `secrets` create/patch RBAC (Helm `secretProjection.enabled`, **off by default** — set it when you opt a consumer into projection); see [Movers, RBAC & credentials](movers.md#let-kopiur-project-the-credentials-secret-recommended-for-shared-repos) for the security trade-off.

```yaml
--8<-- "deploy/examples/11-credential-projection.yaml"
```

## Example 12 — Restore mover, cache & failure policy

A `Restore` writes data **into** a PVC, so it has the same mover concerns a backup does. `spec.mover` matches the UID/GID that should own the restored files (or `inheritSecurityContextFrom` copies it from a live workload pod — the two are mutually exclusive), `spec.mover.cache` sizes the kopia cache (`mode: Persistent` keeps a warm cache PVC across runs), and `spec.failurePolicy` sets the restore Job's `backoffLimit`/`activeDeadlineSeconds`. An elevated restore mover (root / `privilegedMode`) is gated by the same per-namespace `privileged-movers` opt-in a backup uses. See [Restores → Mover, cache & failure policy](restores.md#mover-cache--failure-policy) and [Permissions](permissions.md).

```yaml
--8<-- "deploy/examples/12-restore-mover-cache.yaml"
```

## Example 13 — Restore by raw kopia identity

No `Snapshot` CR to point at — a snapshot written by a foreign kopia client, or one that aged out of the catalog? Restore by the raw kopia identity (`username@hostname:path`). This mode **requires** an explicit `spec.repository` (there's nothing to infer it from). Pin an exact `snapshotID`, or select with `asOf` / `offset`.

```yaml
--8<-- "deploy/examples/13-restore-by-identity.yaml"
```

## Example 14 — Point-in-time / offset restore

"Roll back to Tuesday 2am" without hunting for the exact `Snapshot` CR. `source.fromPolicy` resolves through the `SnapshotPolicy`'s identity and takes `asOf` (newest snapshot at/before an instant) or `offset` (0 = latest, 1 = previous, …) — so it works even when the matching `Snapshot` CR has aged out. Restore into a side-by-side PVC and compare; see [scenario 07](scenarios/point-in-time-rollback.md).

```yaml
--8<-- "deploy/examples/14-restore-point-in-time.yaml"
```

## Example 15 — In-place mirror restore

Restore straight into an **existing** PVC (`target.pvcRef`) and make it an **exact mirror** of the snapshot with `options.enableFileDeletion: true` (files not in the snapshot are deleted). The faithful "put it back exactly how it was" restore — use it deliberately, and scale the app down first.

/// warning | `enableFileDeletion` is destructive

By default a restore is additive. `enableFileDeletion: true` deletes target files that aren't in the snapshot — point it at the wrong PVC and it wipes the extras. Scale the workload to zero so nothing writes the target mid-restore.

///

```yaml
--8<-- "deploy/examples/15-restore-in-place-mirror.yaml"
```

## Example 16 — Cross-namespace clone restore

Restore a snapshot taken in one namespace **into another** — e.g. clone production data into `staging` to reproduce a bug against real data. The `snapshotRef` carries the **source** namespace; the `Restore` and its target PVC live in the **destination**. The mover runs in the destination, so the repo credentials must be readable there (a shared `ClusterRepository` + `credentialProjection` handles that — example 17). See [scenario 08](scenarios/clone-app-to-namespace.md).

```yaml
--8<-- "deploy/examples/16-restore-cross-namespace.yaml"
```

## Example 17 — Restore from a shared repo (projection)

Restoring from a shared `ClusterRepository` into a namespace that has never run a backup hits a chicken-and-egg: the mover loads the repo creds from a Secret in **its** namespace, which isn't there yet. `credentialProjection.enabled: true` has the operator copy the repository's Secret into the mover's namespace for the run (owned by the `Restore`, GC'd with it). Needs the operator's Secret-projection RBAC (Helm `secretProjection.enabled`).

```yaml
--8<-- "deploy/examples/17-restore-shared-repo-projection.yaml"
```

## Example 18 — Inherit the mover security context from a workload

Instead of hard-coding the mover's UID/GID (example 09), `spec.mover.inheritSecurityContextFrom` copies the `securityContext` from a live workload pod onto the mover, so it runs as "whatever the app runs as." You select the workload by **label** (Kubernetes can't look up which pod mounts a PVC), and the same knob works on a `Restore`. Mutually exclusive with `securityContext`; an inherited *root* context is still gated by the `privileged-movers` opt-in. See [Security context → Inherit it from the workload](security-context.md#2-inherit-it-from-the-workload).

```yaml
--8<-- "deploy/examples/18-inherit-security-context.yaml"
```

## Example 19 — Repository replication

Mirror a repository's blobs to a **second** backend on a schedule (`kopia repository sync-to`) — the off-site copy that makes a 3-2-1 strategy. A `RepositoryReplication` is namespaced, references its source via `sourceRef`, and writes to a `destination` backend that must differ from the source. Omit `destinationEncryption` to reuse the source repo's password (a true mirror). See [Repository replication](replication.md).

```yaml
--8<-- "deploy/examples/19-repository-replication.yaml"
```

## Example 20 — Quiesce with hooks

App-consistent backups: `spec.hooks` runs commands **in the workload** (the controller execs into your pod — the mover never runs hooks) before and after the snapshot. Here PostgreSQL is put into backup mode (`beforeSnapshot` workloadExec), resumed afterwards, and a notifier is called (`httpRequest` with `continueOnFailure: true` so a flaky notifier can't fail the backup). The `runJob` form (a full one-shot Job, the k8up `PreBackupPod` analog) is shown in a comment. A failing hook **aborts** the backup unless that hook opts out; `afterSnapshot` hooks run whether the backup succeeded or failed, so a resume can't be skipped. See [Backups → hooks](backups.md#hooks--quiesce-the-app-around-the-snapshot).

```yaml
--8<-- "deploy/examples/20-backup-with-hooks.yaml"
```
