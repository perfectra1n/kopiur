# Permissions, UID & GID

The single most common reason a backup runs but reads **nothing** — or a restore writes files the app then can't open — is a **UID/GID mismatch**. This page shows how to find the right numbers, how to set them, and how to verify it worked, without guesswork.

/// tip | The mental model: the mover is a separate pod

A backup does not run inside your app's pod. Kopiur launches a short-lived **mover** Job that mounts your PVC and runs kopia. Linux file permissions don't care that it's "your" data — they only see the **UID/GID the mover process runs as**. So the rule is simply:

- **Backup** — the mover's UID/GID must be able to **read** every file in the source PVC.
- **Restore** — the mover's UID/GID must be able to **write** into the target PVC.

Get the numbers to line up and permissions stop being a problem.

///

/// info | Looking for the field reference?

This page is the **task** guide — how to find the owning UID and make a stuck backup/restore work. For the full `securityContext` reference (every field, the hardened default, inheriting from a workload, root/privileged movers, and the awkward cases like RWX volumes and preserving ownership on restore), see [**The mover security context**](security-context.md).

///

## What the mover runs as by default

Out of the box the mover runs **unprivileged** as the mover image's user — **UID `65532`** (distroless `nonroot`) — with a hardened security context: `runAsNonRoot: true`, `allowPrivilegeEscalation: false`, all Linux capabilities dropped, seccomp `RuntimeDefault`.

That default reads data that is **world-readable** or **owned by `65532`**. If your app writes files `0600`/`0640` owned by some other UID (very common — most images run as `1000`, `1001`, `999`, …), an unprivileged mover at `65532` gets **permission denied** on those files. You then have three options, in order of preference:

1. Run the mover as the **same UID/GID** that owns the data (best).
2. Run the mover with a **GID** that matches a group the files are readable by.
3. Run the mover as **root** — reads anything, but is _elevated_ and needs an admin opt-in (last resort).

The rest of this page is how to do (1)/(2) reliably, and when to reach for (3).

## Step 1 — Find the UID/GID that owns your data

You want the **numeric** owner of the files in the PVC. Numeric, not names — the mover image has no `/etc/passwd` entry for your app's user, so `ls -l` showing a name is misleading. Use `-n` for numeric.

**If the workload is running** — read it straight from the app pod:

```console
$ kubectl exec -n app deploy/myapp -- id
uid=1000(app) gid=1000(app) groups=1000(app)

$ kubectl exec -n app deploy/myapp -- ls -ln /data
drwxr-xr-x 2 1000 1000 4096 Jun  6 12:00 .
-rw------- 1 1000 1000  512 Jun  6 12:00 secret.key   # 0600, owner-only
```

Here the data is owned by `1000:1000` and some files are owner-only (`0600`) — so the mover **must** run as UID `1000` (matching the group is not enough for `0600` files).

**If nothing is mounting the PVC** (e.g. a fresh restore target, or a scaled-down app) — spin up a throwaway pod that mounts it read-only and inspect:

```console
$ kubectl run pvc-inspect -n app --rm -it --restart=Never \
    --image=busybox --overrides='
{
  "spec": {
    "containers": [{
      "name": "x", "image": "busybox", "command": ["sh"], "stdin": true, "tty": true,
      "volumeMounts": [{"name": "d", "mountPath": "/data", "readOnly": true}]
    }],
    "volumes": [{"name": "d", "persistentVolumeClaim": {"claimName": "app-data"}}]
  }
}' -- sh

/ # stat -c '%u %g %a %n' /data /data/*    # numeric uid, gid, mode, name
1000 1000 755 /data
1000 1000 600 /data/secret.key
```

Note the **lowest common denominator**: if any file you need is `0600` owned by `1000`, the mover has to be UID `1000`. If everything is at least group-readable (`0640`/`0750`) and shares a GID, matching the **GID** is enough.

## Step 2 — Set the mover's UID/GID in the `SnapshotPolicy`

Set it per-recipe under `spec.mover.securityContext` (a standard Kubernetes container `SecurityContext`). Match what you found in Step 1:

```yaml
spec:
    mover:
        securityContext:
            runAsUser: 1000 # the UID that owns the data
            runAsGroup: 1000 # the GID that owns the data
            runAsNonRoot: true # keep the unprivileged guarantee
            allowPrivilegeEscalation: false
            capabilities:
                drop: ["ALL"]
            seccompProfile:
                type: RuntimeDefault
```

A complete, apply-ready example (Repository + SnapshotPolicy with this block, plus the root-mover variant commented out) is [Example 09](examples.md#example-09--mover-uidgid--permissions):

/// tip | `fsGroup` lives on `mover.podSecurityContext`

`runAsUser`/`runAsGroup` above are **container**-level (`spec.mover.securityContext`). `fsGroup` is **pod**-level, so it has its own sibling field — `spec.mover.podSecurityContext.fsGroup`. It's the right tool when an unprivileged mover must **write a freshly-provisioned restore volume** (the kubelet makes the mount group-writable by that GID). For *reading* source data, prefer matching the owning `runAsUser`/`runAsGroup`. See [The mover security context → fsGroup](security-context.md).

///

## Step 3 — Verify it worked

Re-run the backup and confirm it actually read files, rather than silently snapshotting an empty/partial tree:

```console
# the mover Job's exact name lives on the Snapshot:
$ kubectl get snapshot <snapshot-name> -n app -o jsonpath='{.status.job.name}'
app-data-manual-abc12-snap

# its pod (by the standard Job-managed pod label), then the container's effective UID:
$ kubectl get pods -n app --selector=job-name=app-data-manual-abc12-snap
$ kubectl get pod <mover-pod> -n app \
    -o jsonpath='{.spec.containers[0].securityContext.runAsUser}{"\n"}'
1000

# permission errors, if any, surface in the mover log and on the Snapshot status:
$ kubectl logs <mover-pod> -n app | grep -i "permission denied"
$ kubectl get snapshot <snapshot-name> -n app -o jsonpath='{.status.conditions}'
```

A healthy backup ends `Succeeded` with non-zero files/bytes in `status`. A backup that "succeeded" but shows **zero files** is the classic sign the mover couldn't read the data — recheck the UID.

## When you can't match the UID: the root mover

If the data is owned by **assorted UIDs you can't match** (a `lost+found`, a multi-user volume, or an app that writes as root), a **root mover** reads everything. Set:

```yaml
spec:
    mover:
        securityContext:
            runAsUser: 0
            runAsNonRoot: false
        privilegedMode: true # also preserves UID/GID ownership on RESTORE
```

A root (or otherwise elevated) mover is a **privileged mover**, and granting it is a per-namespace admin decision. If the namespace hasn't opted in, the `Snapshot` is refused with a clear `MoverPermitted=False` condition telling you the exact command:

```console
$ kubectl annotate namespace app kopiur.home-operations.com/privileged-movers=true
```

Anything that trips the "privileged" detector needs that opt-in: `runAsUser: 0`, `privileged: true`, `allowPrivilegeEscalation: true`, added Linux capabilities, `runAsNonRoot: false`, or `privilegedMode: true`. Full detail and the revoke path are in [Movers → Privileged movers](movers.md#privileged-movers).

/// tip | Prefer matching the UID over going root

A root mover widens the blast radius of the minted mover ServiceAccount. Reach for it only when you genuinely can't match the owning UID/GID. Most single-app PVCs back up fine as their app's UID.

///

## Filesystem repositories: the _other_ permission

The UID/GID story above is about reading **source data**. A [filesystem repository](backends/filesystem.md) (PVC- or NFS-backed) adds a second surface: the **repository path itself must be writable** by the operator/mover UID.

When create/connect can't write the repo path, Kopiur does not hang — it emits a Warning Event (and a `Bootstrapped=False` condition) naming the **actual** UID it runs as and the fix:

```console
$ kubectl describe repository nas-primary -n backups
...
Warning  PermissionDenied  the repository path is not writable by the operator's UID (65532) —
  fix its ownership/mode (e.g. `chown -R 65532 /repo`) and reconcile again.
```

The UID in that message is the operator's real effective UID (it varies with the chart's `podSecurityContext.runAsUser`), so the `chown` it prints is always correct for your install. Run it on the NAS/host backing the PVC, then reconcile.

## Restore-side permissions

A restore writes files into the **target** PVC, so the same rules apply in reverse — and a `Restore` has the **same `spec.mover`** surface a `SnapshotPolicy` does:

- **`Restore.spec.mover.securityContext`** — set the UID/GID the restore mover writes as, so the restored files land owned correctly and the mover can write the target. (Before this existed the restore mover always ran as UID `65532`.) For a freshly created target PVC (`target.pvc`) the default is usually fine; for an existing PVC (`target.pvcRef`) match the UID that owns it.
- **`Restore.spec.mover.inheritSecurityContextFrom`** — copy the `securityContext` from a live workload pod by label selector instead of hard-coding it (mutually exclusive with `securityContext`). Handy for "restore as whatever the app runs as" — full treatment in [Security context → Inherit it from the workload](security-context.md#2-inherit-it-from-the-workload).
- **Preserving original ownership** — kopia restores files with the UID/GID they had when snapshotted. Reproducing that ownership requires a privileged (root) mover with `privilegedMode: true`; an unprivileged mover writes files owned by its own UID instead. An elevated restore mover (root / `privilegedMode`, or one inherited from a root pod) is gated by the same `privileged-movers` namespace opt-in a backup uses.
- **`spec.options.ignorePermissionErrors`** (default `true`) lets a restore complete and _report_ permission problems via a condition rather than failing hard. Set it `false` to fail-closed when exact permissions matter.

See [Restores → Mover, cache & failure policy](restores.md#mover-cache--failure-policy) for the full restore mover surface, and [example 12](examples.md#example-12--restore-mover-cache--failure-policy).

## Troubleshooting

| Symptom                                        | Where it shows                 | Cause                                                      | Fix                                                                                                                |
| ---------------------------------------------- | ------------------------------ | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Backup `Succeeded` but **0 files / 0 bytes**   | `Snapshot` `.status`             | Mover UID can't read the source files.                     | Match `spec.mover.securityContext.runAsUser/Group` to the data owner (Steps 1–2).                                  |
| Mover log: `permission denied` reading source  | Mover pod logs                 | Same as above — partial read.                              | Same as above; or a root mover if UIDs can't be matched.                                                           |
| Backup stuck `Pending`, `MoverPermitted=False` | `Snapshot` condition / Event     | Mover requests privilege; namespace not opted in.          | `kubectl annotate namespace <ns> kopiur.home-operations.com/privileged-movers=true`, or drop the elevated context. |
| `Repository` `Failed`, `PermissionDenied`      | `Repository` Event / condition | Filesystem repo path not writable by the operator UID.     | `chown -R <uid> <path>` (the Event names the UID), then reconcile.                                                 |
| Restored files unreadable by the app           | After restore                  | Files restored as the mover's UID, not the original owner. | Set `Restore.spec.mover.securityContext.runAsUser/Group` to the app's UID (or `inheritSecurityContextFrom`); use a root mover with `privilegedMode: true` to preserve the original ownership exactly. |
| Restore stuck `Pending`, `MoverPermitted=False` | `Restore` condition / Event   | Restore mover requests privilege; namespace not opted in.  | `kubectl annotate namespace <ns> kopiur.home-operations.com/privileged-movers=true`, or drop the elevated context. |

## Quick reference

| Thing                            | Value                                                                               |
| -------------------------------- | ----------------------------------------------------------------------------------- |
| Default mover UID                | `65532` (distroless `nonroot`), `runAsNonRoot: true`; pod `fsGroup: 65532` so the kopia cache is writable on PVC-backed storage |
| Set the mover UID/GID            | `SnapshotPolicy.spec.mover.securityContext.runAsUser` / `runAsGroup` (same on `Restore.spec.mover` / `Maintenance.spec.mover`) |
| Inherit UID/GID from a workload  | `spec.mover.inheritSecurityContextFrom.podSelector` (mutually exclusive with `securityContext`) — see [Security context](security-context.md#2-inherit-it-from-the-workload) |
| Mover cache size / warm cache    | `spec.mover.cache` (`capacity`, `storageClassName`, `mode: Ephemeral`/`Persistent`, `content`/`metadataCacheSizeMb`); inherits `Repository.spec.moverDefaults.cache` |
| `fsGroup`                        | `spec.mover.podSecurityContext.fsGroup` — **defaults to `65532`** (keeps the cache writable); override to make a fresh restore volume writable by a mover running as a different UID |
| Root / preserve-ownership        | `runAsUser: 0` + `privilegedMode: true` (needs the namespace opt-in)                |
| Privileged-mover opt-in          | `kubectl annotate namespace <ns> kopiur.home-operations.com/privileged-movers=true` |
| Filesystem repo not writable     | Event prints `chown -R <uid> <path>` with the real operator UID                     |
| Restore ignore/permission errors | `Restore.spec.options.ignorePermissionErrors` (default `true`)                      |

## See also

- [Movers, RBAC & credentials](movers.md) — privileged movers, the minted ServiceAccount, credential placement.
- [Backend configuration](backends/index.md) — filesystem & SFTP backends, where ownership matters most.
- [Restores](restores.md) — restore targets and options.
