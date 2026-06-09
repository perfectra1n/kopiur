# Scenario 06 — Backup verification / restore drills

**An untested backup is a hope, not a guarantee.** A backup you have never
restored might be encrypted with a lost password, pointed at a dead bucket, or
quietly capturing an empty volume — and you find out during the outage. A
verification drill catches that on _your_ schedule: periodically restore the
latest snapshot into a **throwaway** PVC, assert it completed, then clean up.

There are **two** layers of verification, and they answer different questions:

| Layer | What it proves | How |
| --- | --- | --- |
| **Built-in `verification`** on the `SnapshotPolicy` | The repository blobs are intact and (optionally) a scratch-restore of the latest snapshot succeeds. | A field on the recipe — the operator runs it on its own cron. Start here. |
| **A full restore drill** (this scenario) | An end-to-end `Restore` into a real PVC completes, mountable and app-checkable. | A `CronJob` that creates `Restore` CRs. The deepest, app-level proof. |

Start with the built-in capability; reach for the drill when you want a true
end-to-end restore (and an app-level check on the restored data).

## Built-in verification (`SnapshotPolicy.spec.verification`)

Kopiur has first-class, opt-in verification (ADR-0005 §4). Add a `verification`
block to the recipe and the operator runs it on a schedule — no `CronJob`, no
extra RBAC:

```yaml
spec:
    verification:
        quick: { cron: "0 4 * * *", jitter: 30m } # blob-level `kopia snapshot verify`, often
        deep: # scratch-restore the latest snapshot to an ephemeral PVC, rarely
            schedule: { cron: "0 5 * * 0", jitter: 1h }
            capacity: 100Gi
            storageClassName: fast-ssd
        successExpr: "stats.files > 0 && stats.errors == 0" # CEL pass/fail predicate
        verifyFilesPercent: 10 # how much of each file `quick` actually reads
```

- **`quick`** is a cheap, frequent blob-level integrity check; **`deep`** is a rare
  full scratch-restore into a throwaway PVC (then discarded). Schedule each
  independently.
- **`successExpr`** is a CEL predicate over the result (`stats{files,bytes,errors}`,
  `snapshot`, and — deep only — `restored{files,checksumMatches}`) — it kills the
  silent "0 files" success. It is validated at admission, so a typo is rejected on
  `kubectl apply`.
- **`verifyFilesPercent`** sets how much of each file `quick` reads in full (the
  rest is checked at the index/blob level).

The most recent successful verify lands in `status.lastVerified`, shows in the
`LAST-VERIFIED` printer column, and exports the `kopiur_snapshot_verified_timestamp`
metric — alert on its staleness exactly like `kopiur_snapshot_last_success_timestamp_seconds`.
The full field reference is in [Backups → verification](../backups.md#verification--prove-the-snapshots-are-restorable).

## The full-restore drill

When you want the deepest, app-level proof — a real `Restore` into a real PVC you
can mount and check — run a drill. Kopiur has no `RestoreSchedule` kind (restores
are one-shot operations), so the cadence is a tiny `CronJob` that creates `Restore`
CRs. The bundle has two halves you can use independently.

### Half A — run one drill by hand

The first object in the file is a plain `Restore` you can `kubectl apply` right
now: latest snapshot → throwaway PVC, with `onMissingSnapshot: Fail` so a drill
that finds nothing is a _failed_ drill (that's the alarm). Watch it, eyeball the
result, delete the PVC.

### Half B — the automated nightly drill

The rest of the file is a `ServiceAccount` + `Role` + `RoleBinding` + `CronJob`.
Each night the CronJob creates a timestamped drill `Restore`, waits for it to
reach `Completed`, then deletes both the `Restore` and its throwaway PVC. If the
restore fails or times out, the Job fails — which is what your monitoring alerts
on.

/// note | Least-privilege RBAC

The drill runner can only `create`/`get`/`delete` `Restore` CRs and delete PVCs
**in its own namespace** — it is not the operator and holds none of the
operator's repository or mover permissions. The `CronJob` image is the upstream
`registry.k8s.io/kubectl` (any image with `kubectl ≥ 1.23` works — it uses
`kubectl wait --for=jsonpath`).

///

```yaml
--8<-- "deploy/examples/scenarios/06-verification-drill.yaml"
```

## Alert on the operator's metrics

The drill proves a _full restore_ works. Pair it with cheap, always-on alerts on
the operator's Prometheus metrics (all `kopiur_*`, scraped from `/metrics` — see
[Observability](../dev/observability.md)) so you also catch a backup that simply
**stopped running**:

```promql
# A backup hasn't succeeded in over 26h (a missed nightly + margin).
time() - kopiur_snapshot_last_success_timestamp_seconds > 26 * 3600

# A schedule is racking up consecutive failures.
kopiur_snapshot_consecutive_failures > 2

# Built-in `verification` hasn't passed in over a week (deep verify is weekly + margin).
time() - kopiur_snapshot_verified_timestamp > 8 * 24 * 3600
```

And alert on the drill itself by watching the `CronJob`'s Job failures (e.g.
`kube_job_status_failed{job_name=~"kopiur-restore-drill.*"} > 0` if you run
kube-state-metrics), or on the drill restore's duration via
`kopiur_restore_duration_seconds`.

/// tip | What "verified" should mean to you

`Completed` proves kopia could decrypt the repo and write the bytes back. For the
strongest guarantee, go one level further: have the drill (or a follow-on `Job`)
mount the restored PVC and run an app-level check — `pg_verifybackup`, a checksum
of known files, a test query. A restore that completes but produces unreadable
data is rare, but a drill that _opens_ the data rules it out entirely.

///

## See also

- [Backups → verification](../backups.md#verification--prove-the-snapshots-are-restorable) — the built-in `quick`/`deep`/`successExpr` field reference.
- [Observability](../dev/observability.md) — the full `kopiur_*` metric surface and how to scrape it.
- [Restores](../restores.md) — `fromPolicy`, `onMissingSnapshot`, and restore phases.
- [Scenario 02 — recover from data loss](recover-lost-data.md) — the real restore your drills are rehearsing.
