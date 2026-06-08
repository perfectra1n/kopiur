# Scenario 02 — Recover from accidental data loss

**Something deleted the data.** A bad migration dropped a table, a script did
`rm -rf` in the wrong directory, an app bug truncated a file. You have nightly
backups ([scenario 01](protect-stateful-app.md)) and want yesterday's data back.

The golden rule: **don't overwrite the live volume first.** Restore the chosen
snapshot into a _new, side-by-side_ PVC, verify it, then cut over. An in-place
overwrite into a mounted, running database is a corruption hazard — and if you
picked the wrong snapshot, the original is gone too.

## Step 1 — Pick the snapshot from before the incident

Restore is "pick a row" — no timestamp math. List the candidate `Backup` CRs for
the recipe, newest last, and choose the last one from _before_ things broke:

```console
$ kubectl get backup -n billing \
    -l kopiur.home-operations.com/backup-config=postgres-data \
    --sort-by=.status.timing.startTime
NAME                          PHASE       ORIGIN      SNAPSHOT    AGE
postgres-data-20260522-021300 Succeeded   scheduled   k1a9...     2d
postgres-data-20260523-021300 Succeeded   scheduled   k1f1...     1d   # <- last good
postgres-data-20260524-021300 Succeeded   scheduled   k2c4...     2h   # incident was here
```

## Step 2 — Restore beside the original

The bundle below restores the chosen `Backup` into a fresh PVC
(`postgres-data-recovered`). The live `postgres-data` PVC is untouched. The
in-place variant — for once you've _decided_ the live data is unrecoverable — is
included as a commented block at the bottom of the file.

/// warning | `enableFileDeletion` turns the target into a mirror

A restore is **additive** by default: it writes the snapshot's files and leaves
anything else in the target alone. `enableFileDeletion: true` _deletes_ files in
the target that aren't in the snapshot, making it an exact mirror — necessary for
a faithful in-place restore, dangerous if you point it at the wrong PVC. Use it
deliberately.

///

```yaml
--8<-- "deploy/examples/scenarios/02-recover-lost-data.yaml"
```

## Step 3 — Watch it, then cut over

```console
$ kubectl get restore postgres-recover-pre-incident -n billing -w
NAME                            PHASE        AGE
postgres-recover-pre-incident   Resolving    2s
postgres-recover-pre-incident   Restoring    9s
postgres-recover-pre-incident   Completed    41s
```

Now point the app at `postgres-data-recovered` (or copy the rows you need out of
it), confirm the data is what you expected, and only then retire the original.
Explicit restores **fail-closed** (`onMissingSnapshot: Fail`) — if the snapshot
you named isn't there, the restore errors instead of silently producing an empty
volume.

## See also

- [Restores](../restores.md) — the full `source` / `target` / `options` / `policy` reference, including `pvcRef` (in-place) and the `identity` source.
- [Scenario 03 — disaster recovery](disaster-recovery.md) — when it's not one volume but the whole cluster.
- [Troubleshooting](../troubleshooting.md) — if a restore won't progress.
