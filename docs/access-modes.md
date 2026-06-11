# PVC access modes (RWO, RWX, RWOP)

Kopiur's movers are ordinary pods: a backup mover must **mount** the volume it reads, and a restore mover must mount the volume it writes. Whether Kubernetes lets a *second* pod do that while your application is running is governed by the PVC's **access mode** — so the access mode, together with the [copy method](copy-methods.md), decides how (and whether) a backup or restore can run alongside your app.

Everything on this page is automatic — there is nothing to install or enable. The behavior is driven by `moverDefaults.sourceColocation` (default [`Auto`](repositories.md#sourcecolocation-avoid-the-rwo-multi-attach-error)); you only touch that knob to opt out.

## Compatibility at a glance

| Access mode | `Direct` backup (mounts the **live** PVC) | `Snapshot` / `Clone` backup (mounts a **staged copy**) | Restore **into** the PVC |
| --- | --- | --- | --- |
| `ReadWriteMany` / `ReadOnlyMany` | ✅ mover schedules freely | ✅ | ✅ |
| `ReadWriteOnce` (RWO) | ✅ mover **co-locates** onto the attach node automatically | ✅ | ✅ co-locates automatically |
| `ReadWriteOncePod` (RWOP) | ⚠️ only while **no pod holds** the volume; a held volume fails fast with guidance | ✅ **works with no downtime** — recommended | ⚠️ only while no pod holds the volume |

## `ReadWriteMany` / `ReadOnlyMany` — nothing to think about

The volume can be attached to many nodes and mounted by many pods at once. The mover schedules wherever the cluster likes, alongside your running app. No pinning, no restrictions.

## `ReadWriteOnce` — handled automatically

An RWO volume attaches to **one node at a time**, but any number of pods *on that node* may mount it. Kopiur detects the node your app holds the volume on and pins the mover there, so backups and restores of in-use RWO PVCs just work — this is the default `sourceColocation.mode: Auto` behavior, and it avoids the Kubernetes *Multi-Attach error*. Full detail (discovery order, the `Required`/`Disabled` modes, RBAC needs) is in [Repositories → `sourceColocation`](repositories.md#sourcecolocation-avoid-the-rwo-multi-attach-error).

## `ReadWriteOncePod` — exclusive to one pod, so pick the right copy method

`ReadWriteOncePod` (RWOP, GA since Kubernetes 1.29, CSI volumes only) hardens RWO's guarantee: the volume can be mounted by **a single pod cluster-wide**. That single-pod exclusivity is exactly what makes it attractive for databases — and exactly what a backup tool has to plan around, because the mover *is* a second pod. Unlike RWO, **co-locating the mover on the same node cannot help**: the kubelet refuses the second mount even there.

What Kopiur does about it, per situation:

### Backing up an RWOP volume with `Snapshot` or `Clone` — no downtime (recommended)

With [`copyMethod: Snapshot` (or `Clone`)](copy-methods.md), the mover **never mounts your live volume**. Kopiur takes a CSI VolumeSnapshot (or CSI clone) of the source — a storage-layer operation that the RWOP mount exclusivity does not restrict — provisions a temporary staged PVC from it, and runs kopia against that stage. The staged PVC inherits your source's access modes, RWOP included, but the mover is its **only** pod, so the exclusivity is satisfied. Your app keeps running, untouched.

This is the recommended way to back up RWOP volumes, and you almost certainly already have what it needs: RWOP itself requires a CSI driver, and most CSI drivers that ship RWOP support also ship snapshots.

```yaml
--8<-- "deploy/examples/24-rwop-snapshot-backup.yaml"
```

/// note | `Clone` of an in-use volume is driver-dependent

CSI **snapshots** of attached volumes are universally supported. CSI **clones** of an attached volume are up to the driver — some refuse and leave the staged PVC `Pending`. If that happens, prefer `copyMethod: Snapshot`.

///

### Backing up an RWOP volume with `Direct` — only while nothing holds it

`copyMethod: Direct` mounts the live PVC into the mover, so it can only work when **no pod currently holds the volume** (then the mover is the sole pod, which RWOP permits — Kopiur schedules it freely). If a running pod *does* hold the volume, Kopiur does not leave a mover stuck `Pending` forever: the backup **fails immediately** with an actionable message —

```text
PVC `ns/data` is ReadWriteOncePod and is currently held by a running pod; a second
pod (the backup mover) cannot mount it even on the same node — scale the workload
down before backing it up, switch the PVC to ReadWriteMany, or set
moverDefaults.sourceColocation.mode=Disabled
```

Your options, in order of preference:

1. **Switch to `copyMethod: Snapshot`** (above) — no downtime, point-in-time, app-decoupled.
2. **Scale the workload down** for the backup window (`kubectl scale deploy/<app> --replicas=0`). With the volume released, `Direct` works; scale back up afterwards.
3. **Change the PVC's access mode** to `ReadWriteOnce` if you don't actually need single-*pod* exclusivity — RWO still guarantees single-*node* attachment, and Kopiur co-locates the mover automatically.

### Restoring into an RWOP volume

A restore mover **writes into** the target PVC, so the same rule applies: the target must not be held by a running pod. Restoring into a **freshly created** `target.pvc` always works (the mover is the sole pod). Restoring into an **existing** RWOP PVC (`target.pvcRef`) requires scaling the workload down first — which you generally want during a restore anyway, so the app doesn't read or write data mid-rewrite. A held RWOP target fails fast with the same actionable message as above.

### Escape hatch: `sourceColocation.mode: Disabled`

Setting `moverDefaults.sourceColocation.mode: Disabled` skips the access-mode checks entirely (along with RWO node pinning) and schedules the mover with only your explicit `nodeSelector`/`affinity`/`tolerations`. Use it only when you manage placement and volume hand-offs yourself — e.g. an external system that releases the volume right before the backup window. If a pod still holds the RWOP volume when the mover starts, the mover pod will sit `Pending` on a mount conflict instead of failing with guidance.

/// warning | RWOP failures are structural, not transient

A held-RWOP failure is reported as a validation failure (the run's condition and an Event carry the message above) and will recur on every run until **you** change something — scale the holder down, switch the copy method, or change the access mode. Kopiur won't retry its way out of it. See [Troubleshooting](troubleshooting.md#mover-pod-stuck-with-multi-attach-error-rwo-pvc).

///
