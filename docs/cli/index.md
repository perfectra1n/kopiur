# kubectl plugin (`kubectl kopiur`)

Kopiur ships a kubectl plugin that wraps the day-to-day operations — suspending
and resuming resources, inspecting snapshots, triggering backups and restores,
running maintenance, browsing (and reading files out of) snapshot contents, and
migrating from VolSync — so you don't have to hand-write CR YAML for routine
tasks.

The plugin is a single static binary named `kubectl-kopiur`. kubectl discovers
plugins by binary name: any executable called `kubectl-kopiur` on your `PATH`
makes `kubectl kopiur …` work. It talks to the cluster with the **same
configuration kubectl uses** — `$KUBECONFIG`, `~/.kube/config`, or in-cluster
credentials — and never needs anything besides API-server access.

/// note | Alpha, like the operator
The plugin tracks the `v1alpha1` CRDs and is versioned with the operator.
A plugin build talking to a much older/newer operator may not know fields
the other side uses; keep them on the same release.

///

## Install

Via [krew](https://krew.sigs.k8s.io/) — the kopiur repository doubles as its
own [custom index](https://krew.sigs.k8s.io/docs/developer-guide/custom-indexes/)
(the `plugins/` directory at the repo root; the plugin is not yet in the
official krew-index — that submission waits for kopiur to leave heavy
development):

```console
$ kubectl krew index add kopiur https://github.com/home-operations/kopiur.git
$ kubectl krew install kopiur/kopiur
$ kubectl kopiur --version
```

`kubectl krew upgrade` picks up new releases after a `kubectl krew update`
(which pulls the index).

Without krew: every GitHub release attaches per-platform archives
(`kopiur-cli_<os>_<arch>.tar.gz` for linux/darwin amd64+arm64,
`…windows_amd64.zip` best-effort) with `.sha256` files, plus the rendered
krew manifest `kopiur.yaml` — `kubectl krew install --manifest-url
<that asset's URL>` also works. Or drop the `kubectl-kopiur` binary anywhere
on your `PATH`.

From source (requires the repo and [mise](https://mise.jdx.dev/)):

```console
$ mise run build
$ install -m 0755 target/debug/kubectl-kopiur ~/.local/bin/kubectl-kopiur
```

## Global flags

Every subcommand accepts the kubectl-alike connection and output flags:

| Flag | Meaning |
|---|---|
| `--kubeconfig PATH` | Use this kubeconfig instead of `$KUBECONFIG` / `~/.kube/config`. |
| `--context NAME` | Use this kubeconfig context instead of the current one. |
| `-n, --namespace NS` | Operate in this namespace (default: the context's namespace). |
| `-A, --all-namespaces` | List across all namespaces (list commands). |
| `-o, --output FORMAT` | `table` (default), `wide`, `yaml`, `json`, or `name`. |
| `-v` / `-vv` | Debug / trace diagnostics on stderr (`KOPIUR_LOG` accepts a full filter). |

`-o yaml|json` always emits the **verbatim Kubernetes objects** (a `v1/List`
for list commands), so the output is pipeable to `kubectl apply`, `jq`, or
`yq` — the table is just one rendering of the same data.

## `snapshot now`

Run a SnapshotPolicy immediately: the plugin creates a manual `Snapshot` CR
(the same thing a `SnapshotSchedule` does on a cron slot, labeled
`origin: manual`) and, with `--wait`, follows it to its terminal phase —
exit 0 on `Succeeded` with a one-line stats summary, exit 1 on `Failed` with
the failure class, kopia stderr tail, and log tail on stderr.

```console
$ kubectl kopiur snapshot now --policy nightly -n media --tag reason=pre-upgrade --wait
snapshot.kopiur.home-operations.com/nightly-manual-20260611030012 created
snapshot nightly-manual-20260611030012 succeeded: kopia id a1b2c3d4e5f6, 5.0 GiB, took 300s
```

| Flag | Effect |
|---|---|
| `--policy NAME` | The SnapshotPolicy (recipe) to run. Checked up front — a typo fails fast with a fix hint. |
| `--name NAME` | Name the Snapshot (default `<policy>-manual-<timestamp>`). |
| `--tag KEY=VALUE` | kopia snapshot tag; repeatable. |
| `--deletion-policy delete\|retain\|orphan` | What happens to the kopia snapshot when the CR is deleted. |
| `--pin` | Exempt this snapshot from GFS retention until unpinned. |
| `--backoff-limit N` / `--active-deadline-seconds SECS` | Mover Job retry budget / wall-clock cap. |
| `--wait` | Watch until `Succeeded` (exit 0) or `Failed` (exit 1). |
| `--logs` | Stream the mover's logs while waiting (implies `--wait`). |
| `--timeout DURATION` | Give up waiting after e.g. `90s`, `30m`, `1h` (default 30m). Timing out stops the *waiting*, not the run. |

Without `--wait` the command returns as soon as the CR is admitted —
`-o yaml|json` then prints the created object, `-o name` just its name.

/// tip | A suspended policy still admits a manual Snapshot
If the policy is suspended the plugin warns but proceeds — the operator is
authoritative about what suspension means. Resume the policy if the run
doesn't start.

///

## `restore`

The `Restore` CRD's 3-sources × 3-targets matrix as one command line. Exactly
one source and exactly one target are required — the plugin enforces it at
parse time, the webhook enforces it again at admission:

| Source | Meaning |
|---|---|
| `--from-snapshot NAME [--snapshot-namespace NS]` | An explicit Snapshot CR (scheduled, manual, or discovered). |
| `--from-policy NAME [--policy-namespace NS] [--as-of RFC3339] [--offset N]` | Resolve via the SnapshotPolicy's identity — works with no Snapshot CR present (the GitOps deploy-or-restore pattern). |
| `--identity USER@HOST[:PATH] [--snapshot-id ID] [--as-of] [--offset]` | A raw kopia identity, for foreign writers or snapshots aged out of the catalog. Requires `--repository`. |

| Target | Meaning |
|---|---|
| `--to-pvc NAME` | Write into an existing PVC. |
| `--create-pvc NAME --size 10Gi [--storage-class X] [--access-mode RWO]…` | The operator creates the PVC. `--size` is required — kopiur never guesses a capacity. |
| `--populator` | Passive mode: the restore is claimed later by a PVC's `dataSourceRef`. |

```console
$ kubectl kopiur restore --from-policy nightly --create-pvc data-restored --size 10Gi -n media --wait
restore.kopiur.home-operations.com/restore-nightly-20260611120001 created
restore restore-nightly-20260611120001 completed: kopia id a1b2c3d4e5f6, 5.0 GiB / 1000 files into pvc/data-restored
```

Every `spec.options`/`spec.policy` knob has a flag: `--enable-file-deletion`
(exact-mirror restore; default is additive), `--ignore-permission-errors
true|false`, `--write-files-atomically true|false`, `--on-missing-snapshot
fail|continue`, `--wait-timeout 5m`, plus `--backoff-limit` /
`--active-deadline-seconds` for the mover Job. `--wait`, `--logs`, and
`--timeout` behave exactly as in `snapshot now` (Completed → exit 0 with a
summary; Failed → exit 1 with the failure block).

/// warning | Restores fail closed
With an explicit source (`--from-snapshot`/`--identity`), a missing snapshot
fails the restore (`onMissingSnapshot: Fail`) rather than silently no-opping.
Only `--from-policy` defaults to `continue`, for deploy-or-restore.

///

/// warning | Identity-based sources need a filesystem backend (today)
The operator resolves `--from-policy` and un-pinned `--identity` sources by
listing the repository's snapshots in-process, which is currently implemented
only for **filesystem-backed** repositories. Against an object-store backend
(S3/GCS/Azure/B2) the Restore stays `Pending` with an `InvalidSpec` warning
event. Use `--from-snapshot` (any backend), or pin the exact snapshot with
`--identity … --snapshot-id <ID>`.

///

## `logs`

Stream the mover Job's logs for a Snapshot or Restore without chasing
Job/pod names yourself:

```console
$ kubectl kopiur logs snapshot nightly-manual-20260611030012 -n media -f
```

The kind is explicit (`logs snapshot …` / `logs restore …`) because the two
CRs may share names. `-f/--follow`, `--tail N`, and `--previous` behave like
`kubectl logs`. A retried Job's **newest** pod is selected. When the Job and
its pods are already garbage-collected, the plugin prints the tail the
operator recorded in `status.logTail` (plus the structured failure block)
and says so honestly — it never pretends rotated logs are complete.

## `maintenance run`

Trigger an out-of-band maintenance run, by Maintenance name or by the
repository it covers (the operator default-manages one per repository). The
plugin stamps the `run-requested`/`run-mode` annotations (also usable from
bare `kubectl annotate` — see [Maintenance](../maintenance.md)); the operator
runs it through the same lease and single-flight path as the cron slots and
answers in `status.manualRun`.

```console
$ kubectl kopiur maintenance run --repository nas --full -n media --wait
maintenance.kopiur.home-operations.com/nas full run requested (2026-06-11T12:00:00Z)
maintenance nas full run completed at 2026-06-11T12:01:42Z
```

`--full` selects the full (compaction + reclamation) pass; the default is
quick. `--wait` exits 0 on `Succeeded` and 1 on `Failed`.

## `status`

A one-screen health overview — repositories (with the `Ready` condition
message inlined for anything not Ready), policies (last successful snapshot /
last verification), schedules (last/next fire, consecutive failures),
in-flight work counts, and anything reporting `Stalled=True`:

```console
$ kubectl kopiur status -n media
REPOSITORIES
KIND        NAME  NAMESPACE  PHASE  BACKEND  MODE       SUSPENDED  MAINTENANCE
Repository  nas   media      Ready  S3       ReadWrite  false      configured

POLICIES
NAME     NAMESPACE  REPOSITORY      SUSPENDED  LAST-SNAPSHOT  LAST-VERIFIED
nightly  media      Repository/nas  false      9h ago         -
…
IN FLIGHT: 0 snapshot(s), 0 restore(s)
```

`--repository NAME [--repository-kind …]` narrows everything to one
repository and the policies/schedules/work attached to it. `-o yaml|json`
emits the full typed report for dashboards/scripts.

## `doctor`

Diagnoses an installation and exits 1 if anything failed: the 8 CRDs are
installed and serve `v1alpha1`, the controller (and webhook, when installed)
Deployments are ready, a **live dry-run admission probe** (an intentionally
invalid SnapshotPolicy that must be denied — zero cluster mutation), every
repository is `Ready`, every repository's credential Secrets resolve, nothing
has been Pending/Running longer than `--stuck-threshold` (default `1h`), and
recent Warning events are summarized.

```console
$ kubectl kopiur doctor -n media
  ok    CRDs installed
  ok    controller running
  ok    webhook running
  ok    webhook admission (live dry-run probe)
  ok    repositories ready
  FAIL  credential secrets present: Repository/nas: secret media/kopia-creds not found
        why: movers load credentials via namespace-local envFrom; a missing Secret fails every run against that repository
        fix: create the Secret in the named namespace (or enable credentialProjection where supported)
  ok    no stuck snapshots/restores
  ok    recent warning events

8 check(s): 1 failed, 0 warning(s)
```

Checks the user lacks RBAC for degrade to warnings naming the missing grant —
doctor never crashes on a restricted kubeconfig.

## `migrate volsync`

Translate VolSync **restic** `ReplicationSource`/`ReplicationDestination`
objects into kopiur `SnapshotPolicy`/`SnapshotSchedule`/`Restore` manifests
(printed as apply-ready YAML; `--apply` creates them via server-side apply).

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

## `suspend` / `resume`

Pause and unpause reconciliation declaratively. Suspending a
**SnapshotSchedule** stops it firing; suspending a **SnapshotPolicy** makes
schedules skip it; suspending a **Repository**/**ClusterRepository** pauses
all work against that repository; suspending a **RepositoryReplication**
pauses replication runs. This is the same `suspend` field you can set in
GitOps — the plugin just flips it for you (and is idempotent: re-suspending
prints `unchanged`).

```console
$ kubectl kopiur suspend schedule nightly -n media
snapshotschedule.kopiur.home-operations.com/nightly suspended

$ kubectl kopiur resume schedule nightly -n media
snapshotschedule.kopiur.home-operations.com/nightly resumed
```

The kind is one of `policy`, `schedule`, `repository`,
`cluster-repository` (alias `clusterrepo`), `replication`.

/// tip | GitOps users: this is a spec edit
`suspend` patches `spec` (with field manager `kubectl-kopiur`), so a GitOps
controller that owns the object will revert it on the next sync. For a
durable pause, set `suspend: true` in Git instead — the plugin is for the
interactive "stop the bleeding now" moment.

///

## `snapshots list`

A richer `kubectl get snapshots`: policy, origin, phase, the kopia snapshot ID,
real size/file counts from the run's stats, and start time — sorted newest
first, filterable, and cross-namespace with `-A`.

```console
$ kubectl kopiur snapshots list -n media
NAME                        POLICY   ORIGIN     PHASE      SNAPSHOT-ID   SIZE     FILES  START                 AGE
nightly-20260611-030012     nightly  scheduled  Succeeded  a1b2c3d4e5f6  5.0 GiB  1000   2026-06-11T03:00:12Z  9h
```

Filters (combinable):

| Flag | Effect |
|---|---|
| `--policy NAME` | Only snapshots produced from this SnapshotPolicy. |
| `--origin scheduled\|manual\|discovered` | Only this origin. `discovered` = found in the repository by the catalog scan, not produced by this cluster. |
| `--repository NAME` | Only snapshots stored in this repository. Matches produced snapshots through their pinned `status.resolved.repository` and discovered ones through the repository-UID label. |
| `--repository-kind repository\|cluster-repository` | Which kind `--repository` names (default `repository`). |
| `--repository-namespace NS` | Where the `--repository` lives, when it differs from the query namespace. |

`-o wide` adds the kopia identity (`username@hostname:path`), the
`deletionPolicy`, and the observed pin state.

## `ls` / `cat` / `download` / `browse`

Read a snapshot's **files** without restoring anything — answer "is the file I
need actually in last night's backup?" in seconds:

```console
$ kubectl kopiur ls nightly-20260611-030012 -n media
NAME       TYPE  SIZE     MODIFIED
config/    dir   1.2 MiB  2026-06-10 21:14:02
movies.db  file  4.8 GiB  2026-06-11 02:59:31

$ kubectl kopiur ls nightly-20260611-030012 config -n media
$ kubectl kopiur cat nightly-20260611-030012 config/app.yaml -n media
$ kubectl kopiur download nightly-20260611-030012 movies.db ./movies.db -n media
$ kubectl kopiur browse nightly-20260611-030012 -n media   # interactive ls/cd/cat/get
```

All four take a **Snapshot object name** (scheduled, manual, or discovered —
anything `snapshots list` shows with a kopia snapshot ID) and an optional path
**relative to the snapshot root**. `cat` streams the file to stdout
(binary-safe — pipe it wherever); `download` writes it locally (defaulting to
the file's own name) and verifies the byte count against the snapshot
manifest, deleting a partial file rather than leaving a truncated one behind.
`browse` opens a small read-only REPL (`ls`, `cd`, `cat`, `get`, `pwd`,
`help`, `quit`). `ls -o wide` adds the kopia object ID column; `ls -o json`
emits the kopia directory manifest verbatim.

### The session-pod model

The first command against a repository starts a **session pod**: a mover `Job`
in the snapshot's namespace that connects to the repository **read-only** and
then idles. That first command takes a few seconds (pod start + connect);
every further `ls`/`cat`/`download` against the same repository reuses the
warm session and answers instantly. The session expires on its own after
`--session-ttl` (default **15m**) and the cluster garbage-collects the
finished Job a minute later — an abandoned browse can never hold a repository
connection (or a pod) open forever. `browse` ends its session on exit unless
you pass `--keep`.

| Flag | Effect |
|---|---|
| `--session-ttl DURATION` | How long the session pod stays warm (default `15m`). |
| `--local` | Skip the session pod; read with a **local kopia binary** (below). |
| `--kopia-bin PATH` | Which local kopia to run (`--local` only; default `$KOPIUR_KOPIA_BINARY`, then `kopia` on `PATH`). |
| `--keep` | (`browse` only) keep the session warm on exit. |

There is deliberately **no `--image` flag**: the session pod runs the exact
mover image the operator's controller Deployment is configured with
(`KOPIUR_MOVER_IMAGE`), falling back to the release default — what browses
your repository is always what backs it up.

### The security model

Read-only is enforced twice, but understand what each layer is:

- The session connects with kopia's `--readonly`, and the CLI can only ever
  exec a **closed, typed set** of kopia read commands (snapshot list, object
  show) — a mutating verb is structurally impossible in the client, and the
  read-only connection refuses writes even if someone execs into the pod by
  hand. This is an **anti-footgun, not a security boundary**.
- The actual boundary is RBAC: anyone who can create pods that reference the
  repository's credential `Secret` in that namespace can already read the
  repository. Browsing needs exactly that power (create the session Job, exec
  into it) — **and notably does NOT need to read Secrets**: the pod loads the
  credentials itself; they never pass through the user's hands.

The chart ships an opt-in ClusterRole with exactly the browse permission set —
set `rbac.browseRole: true` and bind `<release>-browse` to the humans who
should browse (a namespaced RoleBinding scopes them to one namespace). See the
[RBAC reference](../rbac.md#browsing-snapshots-rbacbrowserole).

/// warning | `--local` moves the credentials to your machine
`--local` is for clusters you can't run pods in (or backends only reachable
from your workstation). It fetches the repository's credential Secret(s) onto
your machine — which requires `get secrets` RBAC the session path never needs
— stages them in a private temp dir (removed afterwards), and runs a local
`kopia` binary (install one, or point `--kopia-bin` at it) with a read-only
connect. The backend endpoint must be reachable **from your machine**; an
in-cluster MinIO needs a `kubectl port-forward` and a Repository endpoint
your workstation can resolve.
///

/// note | Sessions are shared per repository
One warm session serves every command against the same repository. Exiting
`browse` without `--keep` ends that shared session — a second terminal
mid-read will see its next command fail and start a fresh session.

///

## `session end`

End a warm browse session early (it expires by TTL anyway):

```console
$ kubectl kopiur session end nightly-20260611-030012 -n media   # via a snapshot in that repo
$ kubectl kopiur session end --repository nas -n media          # or the repository directly
session kopiur-browse-nas-1a2b3c4d ended (Job + work-spec ConfigMap deleted)
```

Deletes the session Job and its work-spec ConfigMap. When no session is open
it says so and exits 0 — safe to run from cleanup scripts. Sessions are also
labeled (`kopiur.home-operations.com/session=browse`) so a plain
`kubectl delete job -l kopiur.home-operations.com/session=browse` works too.
