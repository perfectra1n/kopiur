# Examples

A walkthrough of the manifests in [`deploy/examples/`](https://github.com/home-operations/kopiur/tree/main/deploy/examples). Each is a complete, apply-ready manifest; copy one, replace the `REPLACE_ME` secrets and PVC/bucket names, and `kubectl apply -f`.

```admonish info title="Single source"
The YAML below is pulled directly from the manifests in `deploy/examples/` at build time (mdBook `{{#include}}`), so the docs never drift from the files you actually apply. Each manifest carries its own inline comments.
```

```admonish tip title="The mental model"
Kopiur separates the backup **recipe** (`BackupConfig`) from its **invocation** (`Backup`) from its **schedule** (`BackupSchedule`). A `BackupConfig` runs nothing on its own — a `BackupSchedule` (cron) or a `Backup` (manual / external trigger) is what produces a snapshot. The [`Repository`](introduction.md) holds the storage, and [`Maintenance`](maintenance.md) reclaims it. You can apply a whole bundle at once; the operator resolves the ordering.
```

| # | Example | Demonstrates |
|---|---|---|
| 01 | [Single PVC, scheduled](#example-01--single-pvc-scheduled) | The canonical first backup: Repository → BackupConfig → BackupSchedule. |
| 02 | [Shared platform repository](#example-02--shared-platform-repository) | A cluster-scoped `ClusterRepository` tenants reference without secrets. |
| 03 | [Restore by picking a Backup](#example-03--restore-by-picking-a-backup) | Restore is "pick a row" from the catalog — no timestamp math. |
| 04 | [Multi-PVC selector](#example-04--multi-pvc-selector) | Back up every PVC matching a label as one consistent group. |
| 05 | [Deploy-or-restore (GitOps)](#example-05--deploy-or-restore-gitops) | One bundle that restores on a fresh cluster, backs up otherwise. |
| 06 | [Manual one-shot backup](#example-06--manual-one-shot-backup) | A `Backup` CR as the universal trigger. |
| 07 | [Restore a discovered backup](#example-07--restore-a-discovered-backup) | Restore foreign / pre-install snapshots. |
| 08 | [Maintenance](#example-08--maintenance) | A standalone `Maintenance` for fine-grained control. |
| 09 | [Mover UID/GID & permissions](#example-09--mover-uidgid--permissions) | Match the mover's UID/GID to the data owner so it can read it. |

```admonish tip title="Looking for a specific storage backend?"
Each backend (S3, Azure, GCS, B2, filesystem, SFTP, WebDAV, rclone) has its own dedicated page — provider prerequisites, the exact Secret shape, a field-by-field reference, and a complete apply-ready manifest — starting from the [Backend configuration](backends.md) index. The apply-ready manifests themselves live under [`deploy/examples/backends/`](https://github.com/home-operations/kopiur/tree/main/deploy/examples/backends). The numbered ladder below is the task/workflow tutorial.
```

```admonish warning title="Alpha"
These use API group `kopiur.home-operations.com`, version `v1alpha1`. Backends are **externally tagged** (the bucket lives under `backend.s3`, not `backend: { kind: S3 }`).
```

---

## Example 01 — Single PVC, scheduled

The canonical first backup: one `Repository` (S3), one `BackupConfig` (the idempotent recipe), one `BackupSchedule` (the cron that creates `Backup` CRs). Maintenance is implicit — a default `Maintenance` is created for the repository unless you override or disable it.

```yaml
{{#include ../deploy/examples/01-single-pvc-scheduled.yaml}}
```

---

## Example 02 — Shared platform repository

A platform team owns a cluster-scoped `ClusterRepository`; tenant namespaces reference it without knowing the secret name or backend details. Per-consumer identity is templated at admission.

```admonish note
Requires the operator installed with `installScope=cluster`. Because `ClusterRepository` is cluster-scoped, its Secret references **must** carry an explicit `namespace` (webhook-enforced).
```

```yaml
{{#include ../deploy/examples/02-cluster-repository.yaml}}
```

---

## Example 03 — Restore by picking a Backup

Browse the catalog, then reference a specific `Backup` CR. No timestamp math — restore is "pick a row". `source` and `target` are externally-tagged (`target.pvc` creates a PVC; `target.pvcRef` writes into an existing one).

```console
# list candidate snapshots for a config, newest last:
$ kubectl get backup -n billing \
    -l kopiur.home-operations.com/backup-config=postgres-data \
    --sort-by=.status.timing.startTime
```

```yaml
{{#include ../deploy/examples/03-restore-by-backup.yaml}}
```

---

## Example 04 — Multi-PVC selector

Back up every PVC matching a label as one consistent group (one `VolumeGroupSnapshot` across all matched PVCs). A `Source` has mutually-exclusive `pvc` and `pvcSelector` (webhook-enforced).

```yaml
{{#include ../deploy/examples/04-multi-pvc-selector.yaml}}
```

---

## Example 05 — Deploy-or-restore (GitOps)

The headline GitOps pattern. Apply everything together: on a **fresh cluster against an existing repo**, the PVC restores the latest snapshot before the app starts; on a **fresh repo**, the PVC comes up empty and is backed up going forward. The trick is a **passive `Restore`** (no `target`, `source.fromConfig`, `onMissingSnapshot: Continue`) consumed by a PVC's `dataSourceRef` as a volume populator.

```admonish note
The volume-populator handshake needs Kubernetes ≥ 1.24 (`AnyVolumeDataSource`).
```

```yaml
{{#include ../deploy/examples/05-deploy-or-restore-gitops.yaml}}
```

---

## Example 06 — Manual one-shot backup

A `Backup` CR is the universal trigger — created by a `BackupSchedule`, by `kubectl create`, or by any external system (Argo Events, Tekton, CI). The trigger is separable from the recipe. `deletionPolicy` is `Delete` (default for produced) | `Retain` | `Orphan`.

```yaml
{{#include ../deploy/examples/06-manual-backup.yaml}}
```

---

## Example 07 — Restore a discovered backup

Snapshots the operator did **not** produce (a foreign kopia writer, or snapshots predating the install) are materialized as `Backup` CRs with `origin=discovered` in the repository's namespace, forced to `deletionPolicy: Retain`. Restore one **(A)** by referencing the discovered `Backup` CR, or **(B)** by a raw kopia identity (which requires an explicit `spec.repository`).

```console
# list discovered snapshots in the repo namespace:
$ kubectl get backup -n backups -l kopiur.home-operations.com/origin=discovered
```

```yaml
{{#include ../deploy/examples/07-restore-discovered.yaml}}
```

---

## Example 08 — Maintenance

Maintenance is default-managed (see the [Maintenance guide](maintenance.md)), but you can author a standalone `Maintenance` for fine-grained control — a custom ownership identity or takeover policy. When a user-authored `Maintenance` references a repository, the operator defers to it and never creates a duplicate.

```yaml
{{#include ../deploy/examples/08-maintenance.yaml}}
```

---

## Example 09 — Mover UID/GID & permissions

The mover Job is a separate pod that mounts and reads your PVC, so it must run as a UID/GID that can read the data (and, on restore, write the target). This `BackupConfig` sets `spec.mover.securityContext.runAsUser/runAsGroup` to match the owning user, and comments the root-mover variant for data you can't match. See the [Permissions guide](permissions.md) for how to find the right numbers.

```yaml
{{#include ../deploy/examples/09-mover-permissions.yaml}}
```
