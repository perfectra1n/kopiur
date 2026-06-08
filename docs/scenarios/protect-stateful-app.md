# Scenario 01 — Protect a stateful app, consistently

**The everyday case.** You run a database on a PVC and want nightly,
application-consistent backups with a sane retention window. This is the
canonical reason to install Kopiur, and it's four resources in one namespace.

It goes one step past [example 01](../examples.md#example-01--single-pvc-scheduled):
it adds **hooks** that quiesce the database around the snapshot, so the captured
bytes are not just crash-consistent but application-consistent.

/// info | What you'll deploy

A `Secret` (backend creds + repo password), a `Repository` (a new S3 repo for
this app), a `BackupConfig` (the recipe — source PVC, copy method, hooks,
retention), and a `BackupSchedule` (the nightly cron). All in the app's
namespace, because that's where the mover Job runs.

///

## The values you'll actually change

| Field | Where | What it does |
| --- | --- | --- |
| `backend.s3.bucket` / `prefix` / `endpoint` / `region` | `Repository` | Points at your object store. |
| `KOPIA_PASSWORD` | the `Secret` | Encrypts the repository. **Lose it, lose the backups.** |
| `sources[].pvc.name` | `BackupConfig` | The PVC to back up. |
| `hooks.*.workloadExec` | `BackupConfig` | The commands that quiesce/unquiesce your app. Swap the Postgres ones for your database's equivalents. |
| `retention.keep*` | `BackupConfig` | The GFS window — how many daily/weekly/monthly snapshots to keep. |
| `schedule.cron` / `jitter` | `BackupSchedule` | When it runs. `H` picks a stable minute for you. |

/// tip | Why hooks instead of just a snapshot?

`copyMethod: Snapshot` (the default) already gives you a crash-consistent
point-in-time copy. The `beforeSnapshot`/`afterSnapshot` hooks add **application
consistency**: they run _inside the workload_ to tell the database a backup is
starting, so what's on disk at snapshot time is a clean, restorable state. A hook
failure aborts the backup unless you set `continueOnFailure: true` (we set it on
the _after_ hook so the database never gets stuck in backup mode).

///

```yaml
--8<-- "deploy/examples/scenarios/01-protect-stateful-app.yaml"
```

## Verify it worked

The `Repository` should reach `Ready`, then fire one backup by hand to confirm
the whole chain (recipe → mover → snapshot) before trusting the schedule:

```console
$ kubectl get repository -n billing
NAME               PHASE   AGE
postgres-primary   Ready   30s

$ kubectl create -f - <<'EOF'
apiVersion: kopiur.home-operations.com/v1alpha1
kind: Backup
metadata: { generateName: postgres-data-test-, namespace: billing }
spec: { configRef: { name: postgres-data } }
EOF

$ kubectl get backup -n billing -w
NAME                       PHASE       ORIGIN   SNAPSHOT    AGE
postgres-data-test-x9f     Running     manual               7s
postgres-data-test-x9f     Succeeded   manual   k1f1ec0a8   44s
```

A `SNAPSHOT` id on a `Succeeded` backup means the data is in the repository. From
here the `BackupSchedule` takes over nightly.

## See also

- [Backups & schedules](../backups.md) — every field on these three resources, including the other hook forms (`runJob`, `httpRequest`) and `copyMethod`.
- [Scenario 06 — verification drills](verification-drills.md) — prove these backups actually restore.
- [Movers, RBAC & credentials](../movers.md) — where the mover runs and what it needs.
