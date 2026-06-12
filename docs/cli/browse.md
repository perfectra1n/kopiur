# Inspecting & browsing snapshots

List what's in the catalog, then read a snapshot's **files** without restoring
anything — answer "is the file I need actually in last night's backup?" in
seconds. All [global flags](index.md#global-flags) apply.

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

Read a snapshot's **files** without restoring anything:

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
