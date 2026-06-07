# Getting started

This is the **end-to-end walkthrough**: from a cluster with nothing installed to a verified backup _and_ a verified restore. It hand-holds every step and shows you exactly what to look for so you know each one worked. Budget ~15 minutes.

If you only want the install reference (every Helm value, scopes, cert options), see [Installation](install.md). This page is the guided first run.

/// tip | The mental model — read this first

Kopiur splits one job into three resources so each can change independently:

- a **`Repository`** is _where_ snapshots are stored (your S3 bucket, NAS, B2…);
- a **`BackupConfig`** is the **recipe** — _what_ to back up. It is idempotent and **runs nothing on its own**;
- a **`Backup`** is an **invocation** — one snapshot, as a Kubernetes object. It is the universal trigger (created by a schedule, by `kubectl`, or by automation);
- a **`BackupSchedule`** is the **cron** — _when_ the recipe runs. It creates `Backup` CRs for you.

A `Restore` reads a snapshot back into a PVC. That's the whole model. Everything below is just those pieces in order.

For the full picture — how Kopia dedups, the `username@hostname:path` identity model, and why these are separate resources — see [Concepts](concepts/how-kopia-works.md).

///

## What you need

- A Kubernetes cluster (**≥ 1.24**) and `kubectl` pointed at it.
- **Helm 3 or 4.**
- A storage backend kopia can reach. This guide uses **S3 / S3-compatible** (AWS S3, MinIO, RustFS, Ceph RGW…). Any of the [eight backends](repositories.md) works the same way — only the `Repository` changes.
- A **PersistentVolumeClaim** with some data in it to back up. The walkthrough assumes one named `app-data` in a namespace called `demo`.

/// note | No spare PVC?

Create a throwaway one to follow along:

```console
$ kubectl create namespace demo
$ kubectl -n demo apply -f - <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: app-data }
spec:
  accessModes: [ReadWriteOnce]
  resources: { requests: { storage: 1Gi } }
EOF
```

Mount it in a throwaway pod and write a file if you want to see real data move.

///

## Step 1 — Install the operator

Install the chart into its own namespace. The simplest path lets **cert-manager** mint the webhook's serving certificate; if you don't run cert-manager, see [Installation → Without cert-manager](install.md#without-cert-manager).

```console
$ kubectl create namespace kopiur-system
$ helm install kopiur deploy/helm/kopiur \
    --namespace kopiur-system \
    --set webhook.certManager.enabled=true
```

**Verify** the operator is up and the 7 CRDs are registered:

```console
$ kubectl -n kopiur-system rollout status deploy/kopiur-controller
$ kubectl -n kopiur-system rollout status deploy/kopiur-webhook
$ kubectl get crd -l app.kubernetes.io/part-of=kopiur
NAME                                              CREATED AT
backupconfigs.kopiur.home-operations.com          ...
backups.kopiur.home-operations.com                ...
backupschedules.kopiur.home-operations.com        ...
clusterrepositories.kopiur.home-operations.com    ...
maintenances.kopiur.home-operations.com           ...
repositories.kopiur.home-operations.com           ...
restores.kopiur.home-operations.com               ...
```

Seven CRDs and two ready Deployments means the operator is live.

## Step 2 — Give it credentials

The mover Job that runs kopia reads two things from a Secret: your **backend access keys** and the **repository encryption password**. That Secret must live in the **same namespace as the data you back up** (`demo` here) — the mover loads it with `envFrom`, which is namespace-local. See [Movers, RBAC & credentials](movers.md) for the full why.

```console
$ kubectl -n demo create secret generic repo-creds \
    --from-literal=AWS_ACCESS_KEY_ID='REPLACE_ME' \
    --from-literal=AWS_SECRET_ACCESS_KEY='REPLACE_ME' \
    --from-literal=KOPIA_PASSWORD="$(openssl rand -base64 24)"
```

/// warning | Save the KOPIA_PASSWORD

The `KOPIA_PASSWORD` encrypts the repository. **If you lose it, the backups are unrecoverable** — kopia cannot decrypt without it. Store it in your password manager / secret store, not just in the cluster. The backend keys (`AWS_*`) are your object-store credentials; the well-known key names per backend are in the [Repositories reference](repositories.md#credential-secret-keys-by-backend).

///

## Step 3 — Create the Repository

Tell Kopiur where to store snapshots. `create.enabled: true` lets the operator _initialize_ a brand-new kopia repository in the bucket; drop it (or set `false`) to require that one already exists.

```yaml
apiVersion: kopiur.home-operations.com/v1alpha1
kind: Repository
metadata:
    name: primary
    namespace: demo
spec:
    backend:
        s3: # externally tagged — the bucket lives under `s3`
            bucket: my-kopia-bucket
            prefix: demo/ # optional: share one bucket across repos
            endpoint: s3.us-east-1.amazonaws.com # omit for AWS default; set for MinIO/RustFS
            region: us-east-1
            auth:
                secretRef:
                    name: repo-creds # the backend keys from Step 2
    encryption:
        passwordSecretRef:
            name: repo-creds
            key: KOPIA_PASSWORD # which key in the Secret holds the password
    create:
        enabled: true # create the repo if the bucket is empty
```

Apply it, then **wait for `Ready`** — this is the gate everything else waits on:

```console
$ kubectl apply -f repository.yaml
$ kubectl -n demo get repository primary -w
NAME      PHASE          BACKEND   AGE
primary   Initializing   S3        5s
primary   Ready          S3        12s
```

If it sticks in `Pending`/`Failed`, read the reason — Kopiur tells you exactly what's wrong:

```console
$ kubectl -n demo describe repository primary    # see Conditions + Events
```

(Common causes: wrong keys, unreachable endpoint, or a bucket that doesn't exist with `create.enabled: false`. See [Troubleshooting](troubleshooting.md).)

## Step 4 — Write the recipe (BackupConfig)

Now describe _what_ to back up and _how long to keep it_. Retention is **GFS** (grandfather-father-son) and is the only thing that prunes successful backups.

```yaml
apiVersion: kopiur.home-operations.com/v1alpha1
kind: BackupConfig
metadata:
    name: app-data
    namespace: demo
spec:
    repository:
        name: primary # kind defaults to Repository (same namespace)
    sources:
        - pvc:
              name: app-data # the PVC to snapshot
    retention:
        keepDaily: 7
        keepWeekly: 4
```

```console
$ kubectl apply -f backupconfig.yaml
$ kubectl -n demo get backupconfig
NAME       REPOSITORY   AGE
app-data   primary      3s
```

A `BackupConfig` runs nothing yet — it's the recipe. Next we invoke it.

## Step 5 — Take your first backup (and watch it work)

Trigger one snapshot by creating a `Backup` that references the recipe:

```yaml
apiVersion: kopiur.home-operations.com/v1alpha1
kind: Backup
metadata:
    generateName: app-data-manual- # let the API server pick a unique name
    namespace: demo
spec:
    configRef:
        name: app-data
```

```console
$ kubectl create -f backup.yaml
backup.kopiur.home-operations.com/app-data-manual-abc12 created

$ kubectl -n demo get backup -w
NAME                    PHASE       ORIGIN   SNAPSHOT     AGE
app-data-manual-abc12   Pending     manual                2s
app-data-manual-abc12   Running     manual                6s
app-data-manual-abc12   Succeeded   manual   k8f3c1a90    41s
```

`Succeeded` with a `SNAPSHOT` ID means the data is in your repository. Inspect the details:

```console
$ kubectl -n demo get backup app-data-manual-abc12 -o jsonpath='{.status.stats}'
{"sizeBytes":...,"bytesNew":...,"filesNew":...}
```

If it stays `Pending` with no Job, the mover is blocked on a precondition (usually credentials) — the cause is on the `Backup`'s conditions and as an Event. See [Movers → Troubleshooting](movers.md#troubleshooting).

## Step 6 — Restore it (the half people forget to test)

A backup you've never restored is a hope, not a backup. Restore the snapshot you just made into a **new** PVC so you can compare it without touching the original:

```yaml
apiVersion: kopiur.home-operations.com/v1alpha1
kind: Restore
metadata:
    name: app-data-verify
    namespace: demo
spec:
    source:
        backupRef:
            name: app-data-manual-abc12 # the Backup from Step 5
    target:
        pvc: # operator creates this PVC
            name: app-data-restored
            capacity: 1Gi
            accessModes: [ReadWriteOnce]
```

```console
$ kubectl apply -f restore.yaml
$ kubectl -n demo get restore -w
NAME              PHASE        AGE
app-data-verify   Resolving    2s
app-data-verify   Restoring    8s
app-data-verify   Completed    37s
```

`Completed` means the data landed in `app-data-restored`. Mount that PVC in a pod and confirm your files are there — that's the real proof the round-trip works.

## Step 7 — Put it on a schedule

Manual backups prove the pipeline; a `BackupSchedule` makes it routine. It creates `Backup` CRs on a cron, with deterministic jitter so replicas agree and load spreads.

```yaml
apiVersion: kopiur.home-operations.com/v1alpha1
kind: BackupSchedule
metadata:
    name: app-data-nightly
    namespace: demo
spec:
    configRef:
        name: app-data
    schedule:
        cron: "H 2 * * *" # "H" = a deterministic per-schedule minute, ~02:00 nightly
        jitter: 30m # spread the start over a 30-minute window
        runOnCreate: false # GitOps-friendly: don't fire the instant you apply this
```

```console
$ kubectl apply -f backupschedule.yaml
$ kubectl -n demo get backupschedule
NAME               CONFIG     SCHEDULE    SUSPENDED   AGE
app-data-nightly   app-data   H 2 * * *   false       3s

# the controller pins the next firing into status:
$ kubectl -n demo get backupschedule app-data-nightly \
    -o jsonpath='{.status.nextSchedule.at}'
2026-06-07T02:17:00Z
```

That's a complete, recurring, restore-tested backup. 🎉

## What just happened (and where to go next)

You created a **Repository** (where), a **BackupConfig** (what), invoked it with a **Backup** (one snapshot), proved it with a **Restore**, and automated it with a **BackupSchedule** (when). Maintenance — the periodic `kopia maintenance` that reclaims space — was set up for you automatically the moment the repository existed; you don't have to do anything for it.

From here:

- **[Concepts](concepts/how-kopia-works.md)** — the _why_ behind what you just did: dedup, the identity model, the three-resource split, and one-shared-repository guidance.
- **[Repositories & backends](repositories.md)** — point Kopiur at Azure, GCS, B2, a NAS (filesystem/SFTP/WebDAV), or rclone; and share one repo across namespaces with `ClusterRepository`.
- **[Backups & schedules](backups.md)** — multi-PVC selectors, hooks (quiesce a database before snapshotting), retention tuning, `deletionPolicy`.
- **[Restores](restores.md)** — point-in-time restore, deploy-or-restore (GitOps), and restoring snapshots Kopiur didn't create.
- **[Maintenance](maintenance.md)** — what runs, when, and how shared repositories coordinate.
- **[Examples](examples.md)** — eight complete, apply-ready manifests covering the patterns above.
- **[Troubleshooting](troubleshooting.md)** — when a step above doesn't go green.

## Tearing down the walkthrough

```console
$ kubectl -n demo delete backupschedule app-data-nightly
$ kubectl -n demo delete restore app-data-verify
$ kubectl -n demo delete backup --all          # finalizer also deletes the snapshots
$ kubectl -n demo delete backupconfig app-data
$ kubectl -n demo delete repository primary
```

/// warning | Deleting a Backup deletes its snapshot

A scheduled/manual `Backup` defaults to `deletionPolicy: Delete`, so removing the CR runs `kopia snapshot delete` via a finalizer. Use `Retain` (or `Orphan`) if you want the CR gone but the snapshot kept — see [Backups → deletionPolicy](backups.md#deletionpolicy--what-happens-to-the-snapshot).

///
