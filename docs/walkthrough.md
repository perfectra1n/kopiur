# Complete walkthrough

[Getting started](getting-started.md) is the 15-minute first run. **This page is the long version**: the same journey — install → repository → policy → schedule → snapshot → restore — but with the _reasoning_ behind every choice and value, a second track for NAS users, and the [`kubectl kopiur` plugin](cli/index.md) woven in as the day-2 interface. Read it when you're past "does it work" and into "what should _my_ setup look like, and why".

One paragraph of mental model (the [Concepts page](concepts/how-kopia-works.md) has the full story): a **`Repository`** is _where_ snapshots live; a **`SnapshotPolicy`** is the **recipe** — _what_ to back up, it runs nothing on its own; a **`Snapshot`** is one **invocation** of a recipe, as a Kubernetes object; a **`SnapshotSchedule`** is the **cron** that creates those invocations; a **`Restore`** reads a snapshot back into a PVC. Everything below is those five pieces, in order.

/// note | Pick your track

Every manifest step below has two tabs: **S3** (AWS, MinIO, RustFS, Ceph RGW, …) and **Filesystem (NAS)** (an NFS export on your NAS). Pick one in any tab and the whole page follows — the selection is linked and persists. The two tracks are deliberately near-identical: **only the `Repository` backend and one restore detail change.** Using Azure, GCS, B2, SFTP, WebDAV, or rclone instead? Follow the S3 track and swap the `Repository` from your [backend's page](backends/index.md).

///

## What you'll build

| Stage | Resource | Day-2 CLI verb |
| ----------------------- | ------------------------ | ------------------------------------- |
| Where snapshots live | `Repository` + a Secret | `kubectl kopiur status` / `doctor` |
| What to back up | `SnapshotPolicy` | `kubectl kopiur snapshot now` |
| When it runs | `SnapshotSchedule` | `kubectl kopiur suspend` / `resume` |
| One backup | `Snapshot` (created for you) | `kubectl kopiur snapshots list` / `logs` |
| Proof it works | `Restore` | `kubectl kopiur restore` / `ls` / `browse` |

Both tracks ship as one apply-ready bundle each — [`deploy/examples/walkthrough/s3.yaml`](https://github.com/home-operations/kopiur/blob/main/deploy/examples/walkthrough/s3.yaml) and [`deploy/examples/walkthrough/nas.yaml`](https://github.com/home-operations/kopiur/blob/main/deploy/examples/walkthrough/nas.yaml) — and every YAML block below is pulled from them at build time, stage by stage. You can follow along step-wise, or fill in the `REPLACE_ME`s and apply a whole bundle at once; the operator resolves the ordering.

You need a cluster (**≥ 1.24**), Helm, `kubectl`, and a PVC with data in it — the walkthrough assumes one named `app-data` in namespace `demo` (the [Getting started prerequisites](getting-started.md#what-you-need) show how to create a throwaway one).

## Step 0 — Choices before you install

The Helm install itself is two commands. The decisions worth making _consciously_ first:

**Install scope.** The default `installScope=namespaced` confines the operator to objects in its own release namespace — the safe, least-privilege starting point (RBAC is a `Role`, not a `ClusterRole`). Choose `cluster` when you want one operator watching every namespace, and it's **required** for [`ClusterRepository`](repositories.md) — the shared-repo pattern where a platform team owns the storage and tenant namespaces reference it without ever seeing credentials. Start namespaced; moving to cluster scope later is a Helm upgrade, not a migration. Details: [Installation → Install scope](install.md#install-scope).

**Webhook TLS.** Kopiur validates and defaults your resources through an admission webhook, which needs a serving certificate. The default `webhook.tls.mode=self` means the operator mints and rotates it itself — zero dependencies, the right answer unless you already run cert-manager (then `cert-manager` keeps all your certs in one system). `manual` is for clusters where certificates must come from your own PKI. Details: [Installation → Webhook TLS](install.md#webhook-tls).

**CRD lifecycle.** `installCRDs=true` (default) templates the CRDs into the release so chart upgrades also upgrade schemas. Set it to `false` only if something else (a GitOps CRD pipeline) owns them. Details: [Installation → CRD lifecycle](install.md#crd-lifecycle).

With the defaults chosen deliberately, install:

```console
$ kubectl create namespace kopiur-system
$ helm install kopiur deploy/helm/kopiur --namespace kopiur-system
$ kubectl -n kopiur-system rollout status deploy/kopiur-controller
$ kubectl -n kopiur-system rollout status deploy/kopiur-webhook
```

Every other knob (images by digest, resources, replicas for HA, observability) is in [Helm chart values](configuration.md) — none of them block a first run.

## Step 1 — Install the kubectl plugin

Everything in this walkthrough _can_ be done with raw YAML and `kubectl get -w`. The plugin exists because day-2 operations are imperative by nature — "back up **now**", "what failed?", "show me the files in that snapshot" — and deserve verbs with `--wait`, streamed logs, and meaningful exit codes instead of hand-rolled watch loops. Via [krew](https://krew.sigs.k8s.io/) (the kopiur repo doubles as its own index):

```console
$ kubectl krew index add kopiur https://github.com/home-operations/kopiur.git
$ kubectl krew install kopiur/kopiur
```

Smoke-test it — on a fresh install this prints an empty-but-healthy overview:

```console
$ kubectl kopiur status -n demo
```

No krew? Each GitHub release attaches per-platform binaries — see [the plugin page](cli/index.md#install).

## Step 2 — Credentials

The mover Job that actually runs kopia reads its secrets from a `Secret` **in the same namespace as the data** (`demo` here) — it's loaded with `envFrom`, which is namespace-local, so credentials never transit the operator. What goes in it differs by track, and this is the first place the two tracks teach different lessons:

/// tab | S3

```yaml
--8<-- "deploy/examples/walkthrough/s3.yaml:secret"
```

///

/// tab | Filesystem (NAS)

```yaml
--8<-- "deploy/examples/walkthrough/nas.yaml:secret"
```

///

The S3 track carries the object-store keys (`AWS_*`, read by well-known names — the [per-backend key table](repositories.md#credential-secret-keys-by-backend) covers the other backends). The NAS track needs **only** `KOPIA_PASSWORD`: there is no storage account to authenticate to, but kopia still encrypts everything it writes — your NAS holds ciphertext either way.

/// warning | Lose the password, lose the backups

`KOPIA_PASSWORD` encrypts the repository. If you lose it, the backups are **unrecoverable** — kopia cannot decrypt without it. Generate something long and random and store it in your password manager or secret store, not only in the cluster (a backup password that only exists in the cluster it's backing up defeats the purpose).

///

## Step 3 — The Repository (where)

/// tab | S3

```yaml
--8<-- "deploy/examples/walkthrough/s3.yaml:repository"
```

///

/// tab | Filesystem (NAS)

```yaml
--8<-- "deploy/examples/walkthrough/nas.yaml:repository"
```

///

The values worth pausing on:

- **One bucket, many repositories** (S3: `prefix`; NAS: a subdirectory per repo). A repository is an encryption boundary _and_ a blast-radius boundary — a leaked password for `demo/` reads nothing from `prod/`. The trade-off: kopia deduplicates **within** a repository, so two namespaces backing up similar data into separate repos store it twice. Guidance on drawing that line: [Repositories & backends](repositories.md).
- **`create.enabled: true`** initializes a brand-new kopia repository when the target is empty. When you're _adopting_ an existing repository (e.g. one written by another cluster, or by VolSync — see [the migration guide](cli/migrate-volsync.md)), set it `false` so a typo'd bucket name fails loudly instead of quietly initializing an empty repo.
- **`maintenance` is default-managed.** Omit the block entirely and the operator creates an owned [`Maintenance`](maintenance.md) with exactly the values shown (quick every 6 h, full daily). It's spelled out in the bundle so you can see the knobs — most users should delete the block and take the default.
- **NAS only — permissions**: the export must be writable by the mover's UID (default 65532). If it isn't, the operator's Warning Event names the exact `chown` to run. See [Permissions, UID & GID](permissions.md).

Apply, then wait for `Ready` — this is the gate everything else waits on, and every Kopiur CRD exposes a standard `Ready` condition for exactly this:

```console
$ kubectl -n demo wait --for=condition=Ready repository/primary --timeout=2m
```

Stuck? `kubectl -n demo describe repository primary` shows conditions and events with the actual cause (wrong keys, unreachable endpoint/NAS, missing repo with `create.enabled: false`), or jump ahead to [`kubectl kopiur doctor`](cli/operations.md#doctor).

## Step 4 — The SnapshotPolicy (what, and for how long)

/// tab | S3

```yaml
--8<-- "deploy/examples/walkthrough/s3.yaml:policy"
```

///

/// tab | Filesystem (NAS)

```yaml
--8<-- "deploy/examples/walkthrough/nas.yaml:policy"
```

///

Identical in both tracks — the policy doesn't care where bytes land. The reasoning:

- **Retention is GFS** (grandfather–father–son) and is the _only_ thing that prunes successful backups. Don't pick numbers; answer recovery questions. `keepDaily: 14` = "I can restore any day from the last two weeks" (covers "we noticed the corruption a week later"). `keepWeekly: 8` = "any week from the last two months" (covers slow-burn mistakes). `keepMonthly: 6` = "any month from the last half year" (compliance / archaeology). Thanks to deduplication, the marginal cost of the older tiers is small — they share unchanged data with newer snapshots.
- **`copyMethod: Direct`** reads the live volume — no CSI requirements, and the right default for data that doesn't rewrite in place. The moment a database is involved, reach for `Snapshot` (a CSI VolumeSnapshot taken first, so kopia reads a crash-consistent point in time) — it's also the only way to back up `ReadWriteOncePod` volumes. `Clone` suits drivers that clone faster than they snapshot. Decision table: [Copy methods](copy-methods.md#which-should-i-use); for application-_consistent_ backups (quiesce first), see [hooks](backups.md).
- **`defaultDeletionPolicy: Delete`** ties each produced `Snapshot` CR to its kopia snapshot: delete the CR, the data goes too, and cluster state stays truthful. `Retain`/`Orphan` decouple them — useful, but read [the deletionPolicy section](backups.md#deletionpolicy--what-happens-to-the-snapshot) before choosing, because "I deleted the CR but the repo kept growing" and "I deleted the CR and lost the snapshot" are both surprises with the wrong setting.

This policy backs up one PVC. Label-selector sources (every PVC matching `app=web`, grouped consistently) and NFS sources are the same resource with a different `sources` entry — [Backups & schedules](backups.md) covers them.

## Step 5 — The SnapshotSchedule (when)

/// tab | S3

```yaml
--8<-- "deploy/examples/walkthrough/s3.yaml:schedule"
```

///

/// tab | Filesystem (NAS)

```yaml
--8<-- "deploy/examples/walkthrough/nas.yaml:schedule"
```

///

Also identical in both tracks. Why these values:

- **`cron: "H 2 * * *"`** — `H` is a Jenkins-style placeholder: a deterministic minute derived from the schedule's identity, so this fires at, say, 02:17 _every_ night. Fifty teams writing "nightly at 2" stop stampeding the repository at 02:00:00 without anyone coordinating. The resolved next firing is pinned to `status.nextSchedule.at`.
- **`jitter: 30m`** spreads the start within a window on top of `H` — kinder to the backend when many policies share it, and it costs you nothing for a nightly backup.
- **`runOnCreate: false`** (the default) means applying this manifest does **not** immediately fire a backup — exactly what you want under GitOps, where a re-applied manifest shouldn't mean a surprise snapshot at 3 pm. The trade-off: your first backup waits for tonight, which is why the next step triggers one manually.
- **`concurrencyPolicy: Forbid`** (the default) skips a firing if the previous one is somehow still running, rather than stacking movers on the same PVC.

```console
$ kubectl -n demo get snapshotschedule app-data-nightly \
    -o jsonpath='{.status.nextSchedule.at}'
2026-06-13T02:17:00Z
```

## Step 6 — First snapshot, the day-2 way

The schedule will produce `Snapshot` CRs nightly. Don't wait for it — trigger the recipe now, watch it run, and stream the mover's logs, in one command:

```console
$ kubectl kopiur snapshot now --policy app-data -n demo --wait --logs
```

This creates a `Snapshot` CR with `origin: manual` — _exactly_ the object the schedule creates nightly with `origin: scheduled`, so what you just verified is what runs unattended from now on. (`--tag reason=walkthrough` attaches searchable kopia tags; `--pin` exempts a snapshot from GFS retention — handy before risky migrations.) `--wait` exits 0 on `Succeeded` and 1 on `Failed`, so the same command drops into CI and scripts.

Then look at what exists:

```console
$ kubectl kopiur snapshots list -n demo
NAME                            POLICY    ORIGIN  PHASE      SNAPSHOT-ID   SIZE     FILES  START                 AGE
app-data-manual-20260612140012  app-data  manual  Succeeded  a1b2c3d4e5f6  148 MiB  412    2026-06-12T14:00:12Z  1m
```

Richer than `kubectl get snapshots`, but the CRs are still ordinary objects — both views work. For a failed run, `kubectl kopiur logs snapshot <name> -n demo` replays the mover's logs even after the Job is gone ([details](cli/backup-restore.md#logs)).

## Step 7 — Look inside the repository

Before trusting a restore to a 2 a.m. incident, look at what's actually in a snapshot — read-only, without restoring anything:

```console
$ kubectl kopiur ls app-data-manual-20260612140012 -n demo
$ kubectl kopiur cat app-data-manual-20260612140012 etc/config.yaml -n demo
$ kubectl kopiur browse app-data-manual-20260612140012 -n demo   # interactive: ls/cd/cat/get
```

These run through a short-lived in-cluster **session pod** that mounts nothing from your workloads and can only read the repository — the [session-pod model](cli/browse.md#the-session-pod-model) explains the security boundary. Browsing is gated behind its own opt-in RBAC (`rbac.browse=true` in the chart, the default) so platform teams can switch it off wholesale.

## Step 8 — Restore (prove the round trip)

A backup you've never restored is a hope, not a backup. Restore into a **fresh** PVC and compare, so the original is never touched. Here the tracks differ for the second and last time:

/// tab | S3

Object-store backends name an explicit snapshot. As a one-liner — pick the snapshot from `snapshots list`, restore it, follow until done:

```console
$ kubectl kopiur restore --from-snapshot app-data-manual-20260612140012 \
    --create-pvc app-data-restored --size 1Gi -n demo --wait
```

Or declaratively, the same thing as a manifest (paste the `Snapshot` name into `snapshotRef`):

```yaml
--8<-- "deploy/examples/walkthrough/s3.yaml:restore"
```

///

/// tab | Filesystem (NAS)

Filesystem repositories can resolve "the **latest** snapshot for this policy" at restore time — no snapshot name to paste:

```yaml
--8<-- "deploy/examples/walkthrough/nas.yaml:restore"
```

This `fromPolicy` source is what powers **deploy-or-restore**: the same manifests restore data on a fresh cluster and back it up everywhere else ([example 05](examples.md#example-05--deploy-or-restore-gitops), [GitOps guide](gitops.md)). The CLI equivalent: `kubectl kopiur restore --from-policy app-data --create-pvc app-data-restored --size 1Gi -n demo --wait`.

///

/// warning | `fromPolicy` needs a filesystem repository

Resolving "latest for a policy" lists snapshots in-process, which requires a repo the controller can mount — so it works for **filesystem** backends only. On S3 and the other object-store backends, name the snapshot explicitly (`snapshotRef` / `--from-snapshot`) or pin an exact ID via the [`identity` source](restores.md#identity--a-raw-kopia-identity); the operator fails loudly with that exact fix if you try.

///

```console
$ kubectl -n demo wait --for=jsonpath='{.status.phase}'=Completed restore/walkthrough-verify --timeout=5m
```

`Completed` means the data is in `app-data-restored` — mount it in a pod and diff against the original; that's the real proof. The restored PVC is deliberately **not** owned by the `Restore`, so deleting the CR afterwards keeps the data. If your app runs with an `fsGroup` and the restored files come out unreadable, see [restore-side permissions](permissions.md#restore-side-permissions).

## Step 9 — Day-2 operations

The verbs you'll actually use after today, each one line here and detailed in [CLI → Operations](cli/operations.md):

- **`kubectl kopiur status -n demo`** — one screen: repositories, policies, schedules, in-flight work, last/next runs. The morning-coffee view.
- **`kubectl kopiur doctor -n demo`** — when something is red: checks CRDs, operator, webhook, repository connectivity, credentials, and stuck work, and exits 1 if any check fails (CI-friendly).
- **`kubectl kopiur suspend schedule app-data-nightly -n demo`** / **`resume`** — pause firings around upgrades or maintenance windows, declaratively (it sets `spec.suspend`, so GitOps sees the change).
- **`kubectl kopiur maintenance run --repository primary -n demo --wait`** — out-of-band compaction/pruning. You normally never need it: maintenance is default-managed (Step 3). It exists for "I just deleted a terabyte and want the space back now".

## Teardown

```console
$ kubectl -n demo delete snapshotschedule app-data-nightly
$ kubectl -n demo delete restore walkthrough-verify     # the restored PVC stays
$ kubectl -n demo delete snapshot --all
$ kubectl -n demo delete snapshotpolicy app-data
$ kubectl -n demo delete repository primary
$ helm uninstall kopiur -n kopiur-system
```

/// warning | Deleting a Snapshot deletes its snapshot

With `deletionPolicy: Delete` (the produced default — chosen in Step 4), removing a `Snapshot` CR runs `kopia snapshot delete` via a finalizer. That's the lock-step behavior you opted into; use `Retain`/`Orphan` per-snapshot if a CR must go but the data must stay — see [deletionPolicy](backups.md#deletionpolicy--what-happens-to-the-snapshot).

///

## Where to go next

- **[Scenarios](scenarios/index.md)** — the same machinery aimed at specific problems: protect a database (hooks), recover deleted data, disaster recovery, cross-cluster migration, restore drills.
- **[Repositories & backends](repositories.md)** — `ClusterRepository` for one shared repo across namespaces, and the other six backends.
- **[Repository replication](replication.md)** — mirror the repository to a second backend; the "2" in 3-2-1.
- **[Examples](examples.md)** — the per-capability manifest ladder when you need one specific pattern.
- **[Troubleshooting](troubleshooting.md)** — when a step above doesn't go green.
