# Movers, RBAC & credentials

Every backup, restore, snapshot deletion, and repository bootstrap runs in a
short-lived Kubernetes **Job** called a **mover**. The mover is where kopia
actually executes: it mounts your data, connects to the repository, and streams
snapshots. Understanding *where* a mover runs — and what it needs to be there —
explains the two things you must get right for a backup to succeed: a
**ServiceAccount** and a **credentials Secret**, both in the workload namespace.

```admonish info title="The one rule to remember"
A mover Job runs in the **same namespace as the data it backs up** — not in the
operator's namespace. So everything the mover needs (its ServiceAccount and the
repository's credential Secret) must exist **in that workload namespace**. Kopiur
mints the ServiceAccount for you; the credential Secret is yours to place.
```

## Why movers run in the workload namespace

A backup reads a `PersistentVolumeClaim`, and a PVC is namespaced — a Job can
only mount a PVC in its own namespace. So a `Backup` in namespace `media` runs
its mover Job in `media`, even when the repository it targets is a cluster-scoped
`ClusterRepository` whose definition and credentials live in `kopiur-system`.

That split is the source of the two requirements below.

## The mover ServiceAccount (minted for you)

The mover patches its owning `Backup`/`Restore` `.status`, so it needs a
ServiceAccount with RBAC — and that SA must exist in the workload namespace. The
operator's own ServiceAccount lives only in the operator namespace, so before
each mover Job the controller **mints**, in the Job's namespace:

- a **`kopiur-mover` ServiceAccount**, and
- a **RoleBinding** tying it to the **`kopiur-mover`** ClusterRole.

```admonish info title="Least privilege"
The `kopiur-mover` role grants only what a mover actually uses: `patch` on the
owning CRDs' `/status` subresource and on the bootstrap-result ConfigMap. It does
**not** grant Secrets, Jobs, Pods, or PVCs — a far smaller surface than the
operator's own role. A namespace tenant who can read that SA's token can do
almost nothing with it (see [Privileged movers](#privileged-movers) for the one
exception that needs your sign-off).
```

You don't create or manage these — they're applied idempotently on every
reconcile, labelled `app.kubernetes.io/managed-by: kopiur`. To see them:

```console
$ kubectl get serviceaccount,rolebinding -n media -l app.kubernetes.io/component=mover
NAME                            SECRETS   AGE
serviceaccount/kopiur-mover     0         3m

NAME                                              ROLE                      AGE
rolebinding/kopiur-mover   ClusterRole/kopiur-mover   3m
```

```admonish note title="Names are chart-derived"
The names above assume the default Helm release. The chart passes the real names
to the controller via `KOPIUR_MOVER_SERVICE_ACCOUNT` and `KOPIUR_MOVER_CLUSTERROLE`,
so a release-prefixed install (e.g. `myrel-mover`) stays consistent.
```

## The credentials Secret (yours to place)

The mover reads the repository password (and any object-store keys) from a
Secret, mounted into the Job with `envFrom`. **`envFrom` is namespace-local** —
it can only reference a Secret in the Job's own namespace. So the credential
Secret must exist in the **workload** namespace.

How that plays out depends on the repository kind:

| Repository kind | Where the Secret lives | What you do |
|---|---|---|
| `Repository` (namespaced) | The repo and its Secret are already in the workload namespace. | Nothing extra — the natural layout already satisfies the rule. |
| `ClusterRepository` (cluster-scoped) | The repo's Secret is referenced with an explicit `namespace` (typically `kopiur-system`). | You must **also place a Secret of the same name in each workload namespace** that backs up to it. |

```admonish warning title="ClusterRepository: the common gotcha"
A `ClusterRepository` is shared infrastructure: its credential Secret stays in
the operator namespace, and Kopiur **deliberately does not copy it** into your
workload namespaces (that would leak the shared repository's root credentials to
every tenant). You replicate it yourself — `kubectl`, a templating tool, or a
secret-sync controller (External Secrets, Reloader, `kubernetes-replicator`,
etc.). This mirrors how VolSync requires the repository Secret in the same
namespace as the workload.
```

Example — make the credentials available in the `media` namespace:

```console
$ kubectl get secret kopia-rustfs-creds -n kopiur-system -o yaml \
    | sed 's/namespace: kopiur-system/namespace: media/' \
    | kubectl apply -n media -f -
```

When the Secret is missing, the `Backup` does **not** silently hang. It stays
`Pending` and reports exactly what's wrong:

```console
$ kubectl get backup my-backup -n media \
    -o jsonpath='{.status.conditions[?(@.type=="CredentialsAvailable")].message}'
credentials Secret `kopia-rustfs-creds` does not exist in namespace `media`,
where the mover Job runs and loads it via envFrom — Kubernetes envFrom is
namespace-local and cannot read a Secret from another namespace. The referenced
ClusterRepository `rustfs-primary` keeps that Secret in namespace `kopiur-system`...
Fix: create a Secret named `kopia-rustfs-creds` in namespace `media`...
```

Place the Secret and the condition clears to `CredentialsAvailable=True` on the
next reconcile; the backup proceeds.

## Privileged movers

By default movers run unprivileged. Some workloads need an elevated mover — most
commonly a **root** mover (`spec.mover.securityContext.runAsUser: 0`) to read
files an unprivileged user can't. Because the controller mints a ServiceAccount
in the workload namespace, a tenant with access there could reuse it to run pods
at the mover's privilege. Granting that is therefore a **per-namespace admin
decision**, gated by an annotation — exactly like VolSync's
`volsync.backube/privileged-movers`.

If a `BackupConfig`'s `spec.mover` requests privilege (any of `runAsUser: 0`,
`privileged: true`, `allowPrivilegeEscalation: true`, added Linux capabilities,
`runAsNonRoot: false`, or `privilegedMode: true`) and the namespace has **not**
opted in, the `Backup` is refused with a clear condition:

```console
$ kubectl get backup my-backup -n media \
    -o jsonpath='{.status.conditions[?(@.type=="MoverPermitted")]}'
{"type":"MoverPermitted","status":"False","reason":"PrivilegedMoverNotPermitted",
 "message":"BackupConfig `my-config` requests a privileged mover ... namespace
 `media` has not opted in ... kubectl annotate namespace media
 kopiur.home-operations.com/privileged-movers=true ..."}
```

A cluster admin opts the namespace in:

```console
$ kubectl annotate namespace media kopiur.home-operations.com/privileged-movers=true
```

On the next reconcile `MoverPermitted` clears to `True` and the privileged mover
runs. To revoke, remove the annotation (or drop the elevated `securityContext`
from the `BackupConfig`).

```admonish tip title="Prefer unprivileged when you can"
Reach for a privileged mover only when a workload genuinely needs it (e.g. an
app that writes files as root). Many sources back up fine unprivileged, and an
unprivileged mover keeps the minted ServiceAccount's blast radius minimal.
```

## Putting it together: a ClusterRepository backup in a workload namespace

To back up a PVC in `media` to a shared `ClusterRepository` whose Secret lives in
`kopiur-system`, with a root mover:

1. **Credentials** — place the repo Secret in `media`:
   ```console
   $ kubectl get secret kopia-rustfs-creds -n kopiur-system -o yaml \
       | sed 's/namespace: kopiur-system/namespace: media/' \
       | kubectl apply -n media -f -
   ```
2. **Privilege opt-in** (only if the mover runs as root):
   ```console
   $ kubectl annotate namespace media kopiur.home-operations.com/privileged-movers=true
   ```
3. **Apply** your `BackupConfig` + `Backup` (or `BackupSchedule`) in `media`. The
   controller mints `kopiur-mover` SA + RoleBinding, both gates pass, and the
   mover Job runs.
4. **Watch it**:
   ```console
   $ kubectl get backup -n media -w        # Pending → Running → Succeeded
   ```

## Troubleshooting

The mover preconditions surface on the `Backup`/`Restore` status as conditions
**and** as `Warning` Events (visible in `kubectl describe`), so you never have to
read controller logs to find out why a backup didn't start.

| Symptom | Condition / Event | Cause | Fix |
|---|---|---|---|
| Backup stuck `Pending`, no Job | `CredentialsAvailable=False` / `MissingCredentialsSecret` | The credential Secret isn't in the workload namespace. | Create the Secret there (replicate it for a `ClusterRepository`). |
| Backup stuck `Pending`, no Job | `MoverPermitted=False` / `PrivilegedMoverNotPermitted` | The mover requests privilege but the namespace hasn't opted in. | Annotate the namespace, or drop the elevated `securityContext`. |
| Job created but pod never appears, `FailedCreate: serviceaccount ... not found` | (pre-fix only) | The mover SA isn't in the namespace. | Upgrade the operator — it now mints the SA automatically. |

```admonish info title="Where to look"
- `kubectl describe backup <name> -n <ns>` — conditions **and** Events in one place.
- `kubectl get serviceaccount,rolebinding -n <ns> -l app.kubernetes.io/component=mover` — confirm the mover RBAC was minted.
- `kubectl get jobs,pods -n <ns> -l kopiur.home-operations.com/backup=<name>` — the mover Job and its pod.
```

## Quick reference

| Thing | Value |
|---|---|
| Minted mover ServiceAccount / role | `kopiur-mover` (release-prefixed) |
| Privileged-mover opt-in annotation | `kopiur.home-operations.com/privileged-movers: "true"` (on the **Namespace**) |
| Credentials condition | `CredentialsAvailable` (reason `MissingCredentialsSecret`) |
| Privilege condition | `MoverPermitted` (reason `PrivilegedMoverNotPermitted`) |
| Operator RBAC needed to mint | `serviceaccounts: [create, get]`, `rolebindings: [get, create, update, patch]`, `namespaces: [get]` (cluster install) |
