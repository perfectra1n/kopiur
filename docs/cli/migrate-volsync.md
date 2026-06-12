# Migrating from VolSync

Translate VolSync `ReplicationSource`/`ReplicationDestination` objects into
kopiur `SnapshotPolicy`/`SnapshotSchedule`/`Restore` manifests (printed as
apply-ready YAML; `--apply` creates them via server-side apply). All
[global flags](index.md#global-flags) apply.

Two VolSync movers are supported, with **very different data semantics** —
the command auto-detects which one each object uses (`spec.restic` vs
`spec.kopia`), and a mixed namespace translates in one run:

| Mover | Where it comes from | What migration means |
|---|---|---|
| **restic** | upstream VolSync | Config translation ONLY — the repository formats are incompatible, so the kopiur repository starts empty. |
| **kopia** | the [perfectra1n/volsync fork](https://github.com/perfectra1n/volsync) | The repository **is** a kopia repository: kopiur **adopts it in place** — all existing snapshots are preserved and history continues. |

```console
$ kubectl kopiur migrate volsync -n media --resolve-secrets --apply
```

| Flag | Effect |
|---|---|
| `--name NAME` | Translate one ReplicationSource (default: every one in the namespace). |
| `--repository NAME [--repository-kind …]` | Point the translated policies at an EXISTING kopiur repository. |
| `--resolve-secrets` | Instead, parse each repository Secret and EMIT a kopiur `Repository` derived from it. restic: + credential Secrets, with a `REPLACE_ME` kopia password **you must set** (a kopia repo needs its own new password); `--apply` refuses while any placeholder remains. kopia (fork): the existing repository is **adopted** — the Secret is referenced in place, no placeholder, so `--apply` works in one shot. |
| `--include-destinations` | Also translate ReplicationDestinations into `Restore`s. restic (and kopia without an identity): deploy-or-restore `fromPolicy` + `onMissingSnapshot: Continue`. kopia with `sourceIdentity` or `username`/`hostname`: a raw-identity restore (`source.identity`), no policy pairing needed. |
| `--strict` | Exit 1 (emitting nothing) when any field has no kopiur equivalent. A minimal fork-kopia source is fully mappable and passes. |
| `--apply` | Server-side-apply the translated objects. |

Every VolSync field the translator reads is accounted for on stderr as
`mapped` (with the kopiur destination), `UNMAPPABLE` (with why and what to do
instead — e.g. restic's `retain.within` has no kopia equivalent), or
`ignored` (with why it isn't needed — e.g. `pruneIntervalDays`: kopiur
maintenance is default-managed). Nothing is silently dropped.

## restic sources (upstream VolSync)

/// danger | Config translation ONLY — no data is migrated
A VolSync **restic** repository is NOT a kopia repository. The kopiur
repository the translated policies point at starts **empty** and fills as
kopiur takes its own snapshots. Keep VolSync (and its repository) running
until kopiur's retention coverage is sufficient for your recovery needs.

///

```console
$ kubectl kopiur migrate volsync -n media --repository nas --apply
```

With `--resolve-secrets`, the restic Secret's `RESTIC_REPOSITORY` URL is
parsed into a kopiur `Repository` backend (s3/b2/azure/gcs/filesystem) and the
backend credentials are carried into a new `<secret>-kopiur-creds` Secret
under **kopia's** env names (restic's differ for B2/Azure). The kopia
password is emitted as a `REPLACE_ME` placeholder you must replace — a restic
password cannot initialize a kopia repository's encryption.

## kopia sources (perfectra1n/volsync fork)

/// tip | Repository ADOPTED in place — data and history are preserved
The fork's mover writes a real kopia repository. The emitted `Repository`
connects to it **as-is** (same backend, same password) with **no `create`
block** — it must already exist, so a mis-parsed backend can never silently
initialize a fresh empty repository. Every existing snapshot is preserved and
surfaces as an `origin: discovered` [Snapshot](../repositories.md).

///

```console
$ kubectl kopiur migrate volsync -n media --resolve-secrets --apply
```

What the translation does, and why it is safe to switch over:

- **Identity continuity (the load-bearing part).** The fork records snapshots
  as `<sanitized-name>@<sanitized-namespace>:/data` (or your explicit
  `username`/`hostname`/`sourcePathOverride`). Every translated
  `SnapshotPolicy` pins `spec.identity` + `sources[0].sourcePathOverride` to
  exactly that identity, so the next kopiur snapshot continues the same
  history — retention sees old and new snapshots as one series. The pinned
  identity is shown in the accounting (`(fork snapshot identity)` line);
  verify it matches `kopia snapshot list` before applying.
- **Secrets are referenced in place, never copied.** The `Repository`'s
  `encryption.passwordSecretRef` points at the existing VolSync Secret's
  `KOPIA_PASSWORD`, and (for S3, whose `AWS_*` env names already match) the
  backend `auth.secretRef` does too. Only key names kopia does not read get a
  small derived `<secret>-kopiur-creds` rename-Secret: B2
  (`B2_ACCOUNT_ID`/`B2_APPLICATION_KEY` → `B2_KEY_ID`/`B2_KEY`), legacy Azure
  (`AZURE_ACCOUNT_KEY` → `AZURE_STORAGE_KEY`), WebDAV
  (`WEBDAV_USERNAME`/`WEBDAV_PASSWORD` → `KOPIA_WEBDAV_*`), and SFTP
  known-hosts data.
- **`KOPIA_REPOSITORY` URL forms** `s3://`, `gcs://`, `azure://`, `b2://`,
  `filesystem://` (the repository PVC is inferred from `moverVolumes`),
  `sftp://`, `webdav://`, and `rclone://` all translate, including the fork's
  Secret-key overrides (`KOPIA_S3_BUCKET` beats the URL bucket,
  `AWS_S3_ENDPOINT`, `*_DISABLE_TLS`, …). Parity quirks are preserved: only
  `s3://bucket/prefix` carries a prefix (with the fork's trailing slash); for
  `gcs`/`azure`/`b2` the fork always **ignored** the URL's path portion, so
  the repository is adopted at the bucket/container root and the dropped path
  is called out in the accounting. `gdrive://` has no kopiur backend — author
  that `Repository` by hand.
- **Retention maps 1:1** (`retain.latest` → `keepLatest`, `retain.yearly` →
  `keepAnnual`, the rest by name), as do `compression`, `parallelism`
  (→ `upload.maxParallelFileReads`), `additionalArgs` (→ `extraArgs`), and
  the cache size limits (→ `mover.cache.*`).

/// warning | KEEP the VolSync Secret(s)
kopiur reads the repository password (and, where the names match, the backend
credentials) **from the original VolSync Secret, in place**. When you
decommission VolSync, delete its CRDs and `ReplicationSource`s — but keep the
repository Secret, or the adopted `Repository` loses its credentials.

///

/// warning | Retire the fork's `KopiaMaintenance` objects
kopiur manages repository maintenance itself and takes over kopia's
maintenance ownership (`kopia maintenance set --owner`) on its first run. A
fork `KopiaMaintenance` left running will fight kopiur over ownership —
delete it once the adopted repository is `Ready`.

///

A few fork fields have **no kopiur equivalent** and are called out in the
accounting instead of being silently dropped: `actions.beforeSnapshot`/
`afterSnapshot` run in the fork's *mover* pod, while kopiur
[hooks](../backups.md) run in the *workload* pod — rewrite them for
that context; `policyConfig` raw policy files are replaced by the typed
`SnapshotPolicy` fields; `shallow` restore windows have no analog (use
`asOf`/`offset`/`snapshotID`).

### Suggested cut-over

1. Suspend or delete the fork's `ReplicationSource` (so the two operators
   don't snapshot concurrently), keep its Secret.
2. `kubectl kopiur migrate volsync -n <ns> --resolve-secrets` — review the
   accounting, especially the pinned identity line.
3. Re-run with `--apply`; wait for the `Repository` to reach `Ready` and the
   old snapshots to appear: `kubectl kopiur snapshots list -n <ns>`.
4. Prove continuity: `kubectl kopiur snapshot now --policy <name> --wait`,
   then confirm the new snapshot lists under the same identity.
5. Delete the fork's `KopiaMaintenance` objects and, when satisfied, the
   VolSync install — **not** the repository Secret.
