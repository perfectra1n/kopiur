# The mover security context

Every backup and restore in Kopiur runs in a short-lived **mover** pod, and that pod's **security context** decides which files it can read and write. This page explains what the security context is, the fields that matter, the three ways to set it, how to work out the right values, and how to handle the awkward cases (mixed ownership, RWX volumes, preserving ownership on restore, restricted namespaces).

/// tip | The mental model: the mover is a separate pod

A backup or restore does **not** run inside your app's pod. Kopiur launches a short-lived **mover** Job that mounts the PVC and runs kopia. Linux file permissions don't care that it's "your" data — they only see the **UID/GID the mover process runs as**. The security context is how you control that identity.

- **Backup** — the mover must be able to **read** every file in the source.
- **Restore** — the mover must be able to **write** into the target (and, ideally, land files owned correctly for the app).

///

## What "security context" means here

A Kubernetes [`SecurityContext`](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/) is the block that sets a container's Linux identity and privileges: the UID/GID it runs as, whether it may escalate, which capabilities it holds, its seccomp profile, and so on. Kopiur exposes the standard, unmodified `core/v1` `SecurityContext` on every kind that runs a mover:

| Kind | Field |
| --- | --- |
| `BackupConfig` | `spec.mover.securityContext` |
| `Restore` | `spec.mover.securityContext` |
| `Maintenance` | `spec.mover.securityContext` |

Kopiur applies it at the **container** level (on the mover container), not the pod level. That single design choice has one important consequence:

/// warning | `fsGroup` is not available

`fsGroup` is a **pod-level** setting (`PodSecurityContext`), and Kopiur deliberately does not set a pod-level security context — so `fsGroup` (which would recursively `chgrp` the volume on mount) has no effect on the mover. Make ownership line up with `runAsUser`/`runAsGroup` instead: match the owning UID, match a GID the files are group-readable by, or use a root mover. This keeps the mover's privileges auditable in exactly one place.

///

## The default (hardened) context

If you set nothing, the mover runs **unprivileged** as the mover image's user — **UID `65532`** (distroless `nonroot`) — with a hardened context:

```yaml
securityContext:
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: false
  capabilities:
    drop: ["ALL"]
  seccompProfile:
    type: RuntimeDefault
```

This default is compatible with the Pod Security Admission **`restricted`** profile, so it runs in locked-down namespaces out of the box. What it can **read** is limited: files that are world-readable, or owned by UID `65532`. Most app images run as some other UID (`1000`, `1001`, `999`, …) and write files `0600`/`0640`, so the default mover gets **permission denied** on real app data. Whenever the source isn't world-readable, you'll set the context to match the data — read on.

## How to decide what to set

The whole problem reduces to one number (sometimes two): the **numeric** UID/GID that owns the data.

1. **Find the owner.** Read it from the running app (`kubectl exec … -- id` / `ls -ln`) or, if nothing mounts the PVC, from a throwaway inspection pod. The step-by-step recipe — including the **lowest-common-denominator** rule (if any file you need is `0600` owned by `1000`, the mover must be UID `1000`; if everything is at least group-readable and shares a GID, matching the GID is enough) — lives in the [Permissions guide → Find the UID/GID](permissions.md#step-1--find-the-uidgid-that-owns-your-data).
2. **Decide backup vs restore intent:**
    - **Backup** — pick a UID/GID that can **read** the source.
    - **Restore** — pick the UID/GID that should **own** the restored files, so the app can read them afterward. (To reproduce the *original* ownership exactly, see [Preserving ownership on restore](#preserving-original-ownership-on-restore).)
3. **Choose how to express it** — one of the three approaches below.

## Three ways to set the context

### 1. Set it explicitly

The most direct: hard-code the UID/GID under `spec.mover.securityContext`, keeping the rest of the hardened context.

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

Full, apply-ready example:

```yaml
--8<-- "deploy/examples/09-mover-permissions.yaml"
```

### 2. Inherit it from the workload

If you'd rather "run as **whatever the app runs as**" than track a UID, `inheritSecurityContextFrom` copies the security context from a live workload pod onto the mover. This is the answer to *"back up / restore as the pod that mounts this PVC."*

```yaml
spec:
  mover:
    inheritSecurityContextFrom:
      podSelector:
        matchLabels:
          app.kubernetes.io/name: app # the workload that owns the PVC
      container: app # optional; defaults to the pod's first container
```

/// note | You select the workload by label — Kubernetes can't look it up from the PVC

There is no Kubernetes API to ask "which pod mounts PVC X" (pods aren't field-selectable by claim name). So `inheritSecurityContextFrom` takes a **label selector** and you point it at the workload that owns the PVC — typically the same labels the app already carries. This is selection, not auto-discovery. To find the right labels, list the pods that mount the claim and read their labels:

```console
$ kubectl get pods -n app -o json \
    | jq -r '.items[]
        | select(.spec.volumes[]?.persistentVolumeClaim.claimName=="app-data")
        | .metadata.name'
app-7c9d8f5b6-h2k4p

$ kubectl get pod app-7c9d8f5b6-h2k4p -n app --show-labels
```

///

How it resolves: the controller lists pods matching the selector, prefers a **Running** one, picks the named container (or the pod's first), and copies that container's `securityContext`. If no pod matches, the selector is empty, or the chosen container sets no `securityContext`, the Backup/Restore is held with an actionable `MissingDependency`-style condition telling you exactly what to fix. The matched workload must be **running** so its identity can be read.

Two constraints to remember:

- **Mutually exclusive with `securityContext`.** Setting both is rejected by the admission webhook.
- **Inheriting a *root* workload is still elevated.** The *resolved* context is what's evaluated, so inheriting from a pod that runs as root (or with added capabilities) trips the [privileged-mover gate](#privileged-and-root-movers) exactly like an explicit root context would.

Full, apply-ready example (BackupConfig + the same knob on a `Restore`):

```yaml
--8<-- "deploy/examples/18-inherit-security-context.yaml"
```

### 3. Go root (privileged mover)

When the data is owned by **assorted UIDs you can't match** (a `lost+found`, a multi-user volume, an app that writes as root), a root mover reads everything — and on restore it can reproduce original ownership. It is **elevated** and gated (see below):

```yaml
spec:
  mover:
    securityContext:
      runAsUser: 0
      runAsNonRoot: false
    privilegedMode: true # also preserves UID/GID ownership on RESTORE
```

/// tip | Prefer matching the UID over going root

A root mover widens the blast radius of the minted mover ServiceAccount. Reach for it only when you genuinely can't match the owning UID/GID. Most single-app PVCs back up fine as their app's UID.

///

## Privileged and root movers

Anything that makes the mover's **effective** context elevated requires a per-namespace admin opt-in. The "elevated" detector trips on any of:

- `runAsUser: 0` (root)
- `privileged: true`
- `allowPrivilegeEscalation: true`
- added Linux `capabilities`
- `runAsNonRoot: false`
- `privilegedMode: true`

…and it evaluates the **resolved** context, so an inherited-from-root mover counts too. If the namespace hasn't opted in, the Backup/Restore is refused with a clear `MoverPermitted=False` condition and a Warning Event naming the exact fix:

```console
$ kubectl annotate namespace <ns> kopiur.home-operations.com/privileged-movers=true
```

Why the gate exists, and the revoke path, are covered in [Movers → Privileged movers](movers.md#privileged-movers). The rationale mirrors VolSync's `privileged-movers` model: the operator mints a mover ServiceAccount in the workload namespace, and a tenant there could otherwise reuse it at the mover's privilege.

## Complex circumstances

### Preserving original ownership on restore

kopia records each file's original UID/GID in the snapshot. An **unprivileged** restore mover writes everything owned by its own UID instead — fine when one UID owns everything, wrong for multi-user data. To restore files with their **original** ownership, the mover must be able to `chown` to arbitrary UIDs, which needs root:

```yaml
spec:
  mover:
    securityContext: { runAsUser: 0, runAsNonRoot: false }
    privilegedMode: true
```

This is the same elevation the gate covers, so the restore namespace must opt in. There's an inherent trade: *"preserve arbitrary ownership exactly"* and *"run unprivileged"* are largely mutually exclusive.

### ReadWriteMany / multi-writer volumes

On an RWX volume you can't lean on `fsGroup` ownership-remapping (Kopiur doesn't set it, and it doesn't apply to RWX the way it does to RWO anyway). Match the owning UID/GID directly, or — if the volume holds files from several UIDs — use a root mover to read/write regardless of owner.

### Mixed ownership, `lost+found`, root-written data

If `stat` shows several different owners and some files are owner-only (`0600`), no single non-root UID can read them all. A root mover is the pragmatic answer; pair it with `privilegedMode: true` if you also need restores to land with the original ownership.

### NFS sources and filesystem repositories

- **NFS exports** often apply `root_squash` (root is remapped to `nobody`) and their own UID mapping server-side. A root mover may *not* help there; match the UID the NFS server expects, or relax the export.
- A **filesystem repository** adds a second, separate permission surface: the **repository path** must be writable by the operator/mover UID. That's not a `securityContext` knob — see [Permissions → Filesystem repositories](permissions.md#filesystem-repositories-the-other-permission).

### Restricted namespaces (Pod Security Admission)

The hardened default satisfies the `restricted` PSA profile, so unprivileged movers run anywhere. A **root/elevated** mover violates `restricted` — beyond Kopiur's own opt-in annotation, the namespace's PSA level (and any OpenShift SCC) must also permit it, or the pod won't schedule.

## Backup vs Restore at a glance

| | Backup | Restore |
| --- | --- | --- |
| The mover must… | **read** the source PVC | **write** the target PVC |
| Set the UID/GID to… | an identity that can read the data | the identity that should **own** the restored files |
| Default if unset | UID `65532` (reads world-readable / `65532`-owned only) | UID `65532` (files land owned by `65532`) |
| Preserve original ownership | n/a (kopia records it) | needs root + `privilegedMode: true` |
| Inherit from workload | `BackupConfig.spec.mover.inheritSecurityContextFrom` | `Restore.spec.mover.inheritSecurityContextFrom` |
| Elevated context | namespace `privileged-movers` opt-in | same opt-in |
| Tolerate permission errors | fails on unreadable files | `spec.options.ignorePermissionErrors` (default `true`) reports instead of failing |

## Verify what the mover actually ran as

After a run, confirm the mover's effective identity and that it actually moved data:

```console
# the mover pod for this backup/restore:
$ kubectl get pods -n app -l kopiur.home-operations.com/backup=<backup-name>

# the container's effective UID (sanity-check it matches the data owner):
$ kubectl get pod <mover-pod> -n app \
    -o jsonpath='{.spec.containers[0].securityContext.runAsUser}{"\n"}'
1000

# permission errors, if any:
$ kubectl logs <mover-pod> -n app | grep -i "permission denied"
```

A backup that reports **`Succeeded` but zero files/bytes** is the classic sign the mover couldn't read the source — recheck the UID. The full verification workflow (status conditions, what a healthy run looks like) is in [Permissions → Verify it worked](permissions.md#step-3--verify-it-worked).

## Quick reference

| Thing | Value |
| --- | --- |
| Where to set it | `spec.mover.securityContext` on `BackupConfig` / `Restore` / `Maintenance` |
| Level applied | **container** (no pod-level context → no `fsGroup`) |
| Default | UID `65532`, `runAsNonRoot: true`, drop ALL caps, seccomp `RuntimeDefault`, no escalation |
| Set the UID/GID | `securityContext.runAsUser` / `runAsGroup` (match the data owner) |
| Inherit from a workload | `inheritSecurityContextFrom.podSelector` (+ optional `container`); mutually exclusive with `securityContext` |
| Root / preserve ownership | `runAsUser: 0` + `runAsNonRoot: false` (+ `privilegedMode: true` for restore ownership) |
| Privileged-mover opt-in | `kubectl annotate namespace <ns> kopiur.home-operations.com/privileged-movers=true` |
| Find the owning UID | [Permissions → Find the UID/GID](permissions.md#step-1--find-the-uidgid-that-owns-your-data) |

## See also

- [Permissions, UID & GID](permissions.md) — the task-oriented "my backup reads nothing / my restore is unreadable" workflow.
- [Movers, RBAC & credentials](movers.md) — privileged movers, the minted ServiceAccount, credential placement.
- [Restores](restores.md) — restore targets, options, and `ignorePermissionErrors`.
- [Example 09](examples.md#example-09--mover-uidgid--permissions) · [Example 18](examples.md#example-18--inherit-the-mover-security-context-from-a-workload).
