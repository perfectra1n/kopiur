# Scenario 07 — Point-in-time rollback

**You need a specific moment, not "yesterday."** A bad deploy at 14:30 quietly
corrupted data over the next hour. You want the volume exactly as it was at **14:00**
— just before things went wrong — and you don't want to scroll through `Backup` CRs
guessing which one that was.

This is what `source.fromConfig` with `asOf` is for: it resolves through the
`BackupConfig`'s **identity** (so it works even if the relevant `Backup` CR has aged
out of the catalog) and picks the newest snapshot **at or before** an instant. As
always, restore into a **side-by-side** PVC and verify before cutting over.

## Step 1 — Choose the instant

You don't list snapshots — you name the time. `asOf` takes an RFC3339 timestamp and
resolves to the newest snapshot at or before it. (Prefer counting backwards? Use
`offset`: `0` = latest, `1` = previous, and so on.)

/// tip | `asOf` vs `offset`

Use **`asOf`** when you know _when_ things were good ("just before the 14:30 deploy").
Use **`offset`** when you know _how many snapshots back_ ("the one before last"). Set
one, not both.

///

## Step 2 — Restore into a clone and verify

The bundle restores into a fresh `postgres-data-1400` PVC; the live volume is
untouched. Because `fromConfig` defaults to `onMissingSnapshot: Continue`
(deploy-or-restore), a deliberate rollback sets it to **`Fail`** so an instant with no
snapshot is a loud error, not a silent empty volume.

```yaml
--8<-- "deploy/examples/scenarios/07-point-in-time-rollback.yaml"
```

```console
$ kubectl get restore postgres-rollback-1400 -n billing -w
NAME                     PHASE        AGE
postgres-rollback-1400   Resolving    2s
postgres-rollback-1400   Restoring    9s
postgres-rollback-1400   Completed    44s
```

Point a throwaway client at the clone PVC and confirm the data is the moment you
wanted.

## Step 3 — Cut over

Once you trust the clone, scale the app down and either repoint it at the clone, or do
an **in-place mirror** restore into the live PVC — `target.pvcRef` +
`options.enableFileDeletion: true` to make the live volume an exact mirror of the
chosen instant (see [example 15](../examples.md#example-15--in-place-mirror-restore)).
The in-place form is shown commented at the bottom of the bundle.

/// warning | Never roll back in place on the first try

Restoring over a mounted, running database is a corruption hazard, and if you pick the
wrong instant the original is gone too. Clone, verify, _then_ cut over.

///

## See also

- [Restores → point-in-time](../restores.md#fromconfig--resolve-via-a-backupconfigs-identity) — the `asOf` / `offset` reference.
- [Example 14 — point-in-time / offset restore](../examples.md#example-14--point-in-time--offset-restore) and [example 15 — in-place mirror](../examples.md#example-15--in-place-mirror-restore).
- [Scenario 02 — recover lost data](recover-lost-data.md) — the same safe clone-and-verify habit for a known `Backup` CR.
