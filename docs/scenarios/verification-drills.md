# Scenario 06 — Backup verification / restore drills

**An untested backup is a hope, not a guarantee.** A backup you have never
restored might be encrypted with a lost password, pointed at a dead bucket, or
quietly capturing an empty volume — and you find out during the outage. A
verification drill catches that on _your_ schedule: periodically restore the
latest snapshot into a **throwaway** PVC, assert it completed, then clean up.

Kopiur has no `RestoreSchedule` kind — restores are one-shot operations — so the
cadence is a tiny `CronJob` that creates `Restore` CRs. The bundle has two halves
you can use independently.

## Half A — run one drill by hand

The first object in the file is a plain `Restore` you can `kubectl apply` right
now: latest snapshot → throwaway PVC, with `onMissingSnapshot: Fail` so a drill
that finds nothing is a _failed_ drill (that's the alarm). Watch it, eyeball the
result, delete the PVC.

## Half B — the automated nightly drill

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

## Half C — alert on the operator's metrics

The drill proves a _full restore_ works. Pair it with cheap, always-on alerts on
the operator's Prometheus metrics (all `kopiur_*`, scraped from `/metrics` — see
[Observability](../dev/observability.md)) so you also catch a backup that simply
**stopped running**:

```promql
# A backup hasn't succeeded in over 26h (a missed nightly + margin).
time() - kopiur_snapshot_last_success_timestamp_seconds > 26 * 3600

# A schedule is racking up consecutive failures.
kopiur_snapshot_consecutive_failures > 2
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

- [Observability](../dev/observability.md) — the full `kopiur_*` metric surface and how to scrape it.
- [Restores](../restores.md) — `fromPolicy`, `onMissingSnapshot`, and restore phases.
- [Scenario 02 — recover from data loss](recover-lost-data.md) — the real restore your drills are rehearsing.
