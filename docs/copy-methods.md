# Copy methods: `Snapshot`, `Clone`, `Direct`

`SnapshotPolicy.spec.copyMethod` chooses **how Kopiur captures your data before kopia reads it**. Kopia always backs up files from a mounted volume — the copy method only decides *which* volume the backup mover mounts:

| Method | What the mover reads | Point-in-time? | Decoupled from the app's node? | Requires |
| --- | --- | --- | --- | --- |
| **`Snapshot`** _(default)_ | A temporary PVC restored from a CSI **VolumeSnapshot** of your source | ✅ yes | ✅ yes | CSI snapshot stack + a `VolumeSnapshotClass` |
| **`Clone`** | A temporary **CSI clone** of your source PVC | ✅ yes (at clone time) | ✅ yes | CSI driver with volume-clone support |
| **`Direct`** | Your **live** source PVC, read-only | ❌ no (crash-consistent live read) | ❌ no (co-located with the app) | Nothing — works on any storage |

## Which should I use?

```text
Do you need a point-in-time, app-decoupled backup (e.g. a database)?
│
├─ No  ────────────────────────────────────────────►  Direct
│         (config, media, file shares; simplest; works everywhere)
│
└─ Yes
    │
    ├─ Does your CSI driver support VolumeSnapshots?  ──►  Snapshot   (preferred)
    │
    └─ Only volume cloning, not snapshots?  ───────────►  Clone
```

- **Start with `Direct`** if you don't have (or don't want to maintain) the CSI snapshot stack — it just works.
- **Use `Snapshot`** for databases and anything where you want a consistent, point-in-time capture that doesn't tie the backup to the node your app runs on.
- **Use `Clone`** only if your driver does cloning but not snapshots (uncommon).

!!! note "`Snapshot` is the default"
    `copyMethod` defaults to `Snapshot` because point-in-time is the safer backup. If your cluster has no CSI snapshot stack, either install it (below) or set `copyMethod: Direct` explicitly.

---

## `Snapshot` — point-in-time CSI snapshot (default)

When a backup runs, Kopiur:

1. Creates a CSI **`VolumeSnapshot`** of your source PVC (after any `beforeSnapshot` hooks, so a quiesced app yields a consistent capture).
2. Waits for the snapshot to become `readyToUse`.
3. Provisions a temporary **staged PVC** from the snapshot.
4. Runs the kopia mover against the **staged PVC** — never the live volume.
5. **Cleans everything up** (staged PVC + VolumeSnapshot) when the backup finishes.

The staged PVC is brand-new and unheld, so the backup mover **schedules freely** — it is fully decoupled from the node your application runs on (unlike `Direct`, which must co-locate).

### What it requires

`Snapshot` needs the cluster's **CSI snapshot stack**, which your cluster administrator installs once:

- The **external-snapshotter** — the `snapshot-controller` Deployment **and** the `VolumeSnapshot`/`VolumeSnapshotContent`/`VolumeSnapshotClass` CRDs (see the [kubernetes-csi external-snapshotter docs](https://kubernetes-csi.github.io/docs/snapshot-controller.html)). Many managed distributions (EKS, GKE, AKS, Talos, k3s add-ons) ship or offer this.
- A **`VolumeSnapshotClass`** whose `driver` matches the CSI provisioner of your source PVC's `StorageClass`.

If any of this is missing, the backup **fails with a clear condition** telling you exactly what to do — Kopiur never silently downgrades a `Snapshot` backup to a live read. See [Troubleshooting](#troubleshooting) below.

### Choosing the `VolumeSnapshotClass`

```yaml
spec:
    copyMethod: Snapshot
    # Optional. Leave unset to auto-select your driver's DEFAULT class.
    volumeSnapshotClassName: csi-rbd-snapclass
```

- **Set it explicitly** to pin a specific class.
- **Leave it unset** and Kopiur picks the **default `VolumeSnapshotClass` for your source's driver** (the one annotated `snapshot.storage.kubernetes.io/is-default-class: "true"`). If exactly one class exists for the driver it's used even without the annotation.
- If **no** class matches your driver, or **several** match with no single default, the backup fails asking you to create/annotate a class or name one explicitly.

```yaml
--8<-- "deploy/examples/21-copy-method-snapshot.yaml"
```

---

## `Clone` — CSI volume clone

`Clone` provisions the staged PVC directly from your source PVC (`dataSource: PersistentVolumeClaim`) — a CSI **volume clone** — with no intermediate VolumeSnapshot. Like `Snapshot`, the mover reads the clone and the clone is cleaned up afterward.

Use it when your CSI driver supports cloning (`CLONE_VOLUME`) but not snapshots. It needs no `VolumeSnapshotClass`.

```yaml
--8<-- "deploy/examples/22-copy-method-clone.yaml"
```

!!! warning "Clone requires driver support"
    If your driver can't clone the volume, the staged PVC stays `Pending` and the backup never starts. If you see that, check the staged PVC's events (`kubectl describe pvc <snapshot-name>-src`) and use `Snapshot` or `Direct` instead.

---

## `Direct` — read the live volume

`Direct` mounts your **live** source PVC into the mover, read-only, and kopia reads it in place. No snapshot, no clone, no extra storage — it works on **any** storage, including `local-path`/hostPath that has no snapshot support.

Because the live volume is mounted, Kopiur **co-locates** the mover on the node already holding the PVC (for `ReadWriteOnce` volumes), avoiding the Kubernetes *Multi-Attach error*. See [Repositories → `sourceColocation`](repositories.md#sourcecolocation-avoid-the-rwo-multi-attach-error).

```yaml
--8<-- "deploy/examples/23-copy-method-direct.yaml"
```

`Direct` reads a **live filesystem**, so the backup is *crash-consistent* — fine for most file data, but for a busy database prefer `Snapshot`, or quiesce the app with hooks (below).

---

## Consistency: what each method guarantees

- `Snapshot` / `Clone` capture a **point-in-time** image at the block level — *crash-consistent* (like a power-cut: the filesystem is intact, in-flight writes may not be flushed).
- `Direct` reads files **while the app may be writing** — also crash-consistent, but spread across the read rather than a single instant.

For **application consistency** (a database flushed and quiesced), use `SnapshotPolicy.spec.hooks` to quiesce before the capture and resume after. With `Snapshot`, the VolumeSnapshot is taken **after** your `beforeSnapshot` hooks, so a `FLUSH`/`fsfreeze` hook yields a consistent snapshot. See [Backups → hooks](backups.md).

## Cleanup & cost

Kopiur reaps the staged PVC and VolumeSnapshot when the backup reaches a terminal state (and again if you delete the `Snapshot`). To avoid the well-known leak where a **`Retain`** StorageClass leaves the staged PV (and its backend volume) behind, Kopiur flips a bound staged PV's reclaim policy to `Delete` before removing it. A `Retain` **`VolumeSnapshotClass`** keeps the *underlying* storage snapshot after the VolumeSnapshot object is deleted — prefer a `Delete` deletion policy for the class you point Kopiur at, unless you want to keep raw storage snapshots yourself.

`status.staged` on the `Snapshot` records what was created (the VolumeSnapshot + staged PVC names) for visibility.

## Troubleshooting

| Condition / symptom | Cause | Fix |
| --- | --- | --- |
| `SourceStaged=False`, reason **`SnapshotStackMissing`** | No `VolumeSnapshotClass` API — the external-snapshotter isn't installed. | Install the [snapshot-controller + CRDs](https://kubernetes-csi.github.io/docs/snapshot-controller.html) and a `VolumeSnapshotClass`, or set `copyMethod: Direct`. |
| `SourceStaged=False`, reason **`NoVolumeSnapshotClass`** | No class matches your source PVC's driver (or several do with no single default). | Create/annotate a `VolumeSnapshotClass` for the driver, set `volumeSnapshotClassName` explicitly, or use `Direct`. |
| `SourceStaged=False`, reason **`VolumeSnapshotFailed`** | The CSI driver reported an error creating the snapshot. | Read the message (it includes the driver's error); check the class/driver, then re-create the `Snapshot`. |
| `SourceStaged=False`, reason **`SourceNotCSIProvisioned`** | The source PVC has no `StorageClass` (a static/hostPath volume) — nothing to snapshot. | Use a CSI-provisioned PVC, or `copyMethod: Direct`. |
| Backup stuck `Pending`, staged PVC `Pending` | `WaitForFirstConsumer` (normal — binds when the mover starts) **or** the driver can't clone (for `Clone`). | If it never binds, `kubectl describe pvc <name>-src` for the driver event; switch method if cloning is unsupported. |

See also [Troubleshooting → Multi-Attach](troubleshooting.md) for the `Direct`-mode co-location path.
