# Migrating from VolSync

Translate VolSync **restic** `ReplicationSource`/`ReplicationDestination`
objects into kopiur `SnapshotPolicy`/`SnapshotSchedule`/`Restore` manifests
(printed as apply-ready YAML; `--apply` creates them via server-side apply).
All [global flags](index.md#global-flags) apply.

/// danger | Config translation ONLY — no data is migrated
A VolSync **restic** repository is NOT a kopia repository. The kopiur
repository the translated policies point at starts **empty** and fills as
kopiur takes its own snapshots. Keep VolSync (and its repository) running
until kopiur's retention coverage is sufficient for your recovery needs.

///

```console
$ kubectl kopiur migrate volsync -n media --repository nas --apply
```

| Flag | Effect |
|---|---|
| `--name NAME` | Translate one ReplicationSource (default: every one in the namespace). |
| `--repository NAME [--repository-kind …]` | Point the translated policies at an EXISTING kopiur repository. |
| `--resolve-secrets` | Instead, parse each restic Secret's `RESTIC_REPOSITORY` and EMIT a kopiur `Repository` + credential Secrets. The kopia password is a `REPLACE_ME` placeholder **you must set** (a kopia repo needs its own new password); `--apply` refuses while any placeholder remains. |
| `--include-destinations` | Also translate ReplicationDestinations into deploy-or-restore `Restore`s (`fromPolicy` + `onMissingSnapshot: Continue`). |
| `--strict` | Exit 1 (emitting nothing) when any field has no kopiur equivalent. |
| `--apply` | Server-side-apply the translated objects. |

Every VolSync field the translator reads is accounted for on stderr as
`mapped` (with the kopiur destination), `UNMAPPABLE` (with why and what to do
instead — e.g. restic's `retain.within` has no kopia equivalent), or
`ignored` (with why it isn't needed — e.g. `pruneIntervalDays`: kopiur
maintenance is default-managed). Nothing is silently dropped.
