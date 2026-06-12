# Operations: status, doctor, maintenance, suspend

The day-2 commands: a one-screen health overview, an installation diagnostic,
out-of-band maintenance, and the declarative pause switch. All
[global flags](index.md#global-flags) apply.

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
