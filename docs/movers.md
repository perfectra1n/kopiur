# Movers, RBAC & credentials

Every backup, restore, snapshot deletion, and repository bootstrap runs in a short-lived Kubernetes **Job** called a **mover**. The mover is where kopia actually executes: it mounts your data, connects to the repository, and streams snapshots. Understanding _where_ a mover runs — and what it needs to be there — explains the two things you must get right for a backup to succeed: a **ServiceAccount** and a **credentials Secret**, both in the workload namespace.

/// info | The one rule to remember

A mover Job runs in the **same namespace as the data it backs up** — not in the operator's namespace. So everything the mover needs (its ServiceAccount and the repository's credential Secret) must exist **in that workload namespace**. Kopiur mints the ServiceAccount for you. The credential Secret is yours to place by default — but you don't have to: flip on [**credential projection**](#let-kopiur-project-the-credentials-secret-recommended-for-shared-repos) and Kopiur copies it into each mover namespace for you.

///

## Why movers run in the workload namespace

A backup reads a `PersistentVolumeClaim`, and a PVC is namespaced — a Job can only mount a PVC in its own namespace. So a `Backup` in namespace `media` runs its mover Job in `media`, even when the repository it targets is a cluster-scoped `ClusterRepository` whose definition and credentials live in `kopiur-system`.

That split is the source of the two requirements below.

## The mover ServiceAccount (minted for you)

The mover patches its owning `Backup`/`Restore` `.status`, so it needs a ServiceAccount with RBAC — and that SA must exist in the workload namespace. The operator's own ServiceAccount lives only in the operator namespace, so before each mover Job the controller **mints**, in the Job's namespace:

- a **`kopiur-mover` ServiceAccount**, and
- a **RoleBinding** tying it to the **`kopiur-mover`** ClusterRole.

/// info | Least privilege

The `kopiur-mover` role grants only what a mover actually uses: `patch` on the owning CRDs' `/status` subresource and on the bootstrap-result ConfigMap. It does **not** grant Secrets, Jobs, Pods, or PVCs — a far smaller surface than the operator's own role. A namespace tenant who can read that SA's token can do almost nothing with it (see [Privileged movers](#privileged-movers) for the one exception that needs your sign-off).

///

You don't create or manage these — they're applied idempotently on every reconcile, labelled `app.kubernetes.io/managed-by: kopiur`. To see them:

```console
$ kubectl get serviceaccount,rolebinding -n media -l app.kubernetes.io/component=mover
NAME                            SECRETS   AGE
serviceaccount/kopiur-mover     0         3m

NAME                                              ROLE                      AGE
rolebinding/kopiur-mover   ClusterRole/kopiur-mover   3m
```

/// note | Names are chart-derived

The names above assume the default Helm release. The chart passes the real names to the controller via `KOPIUR_MOVER_SERVICE_ACCOUNT` and `KOPIUR_MOVER_CLUSTERROLE`, so a release-prefixed install (e.g. `myrel-mover`) stays consistent.

///

## The credentials Secret

The mover reads the repository password (and any object-store keys) from a Secret, mounted into the Job with `envFrom`. **`envFrom` is namespace-local** — it can only reference a Secret in the Job's own namespace. So the credential Secret must exist in the **workload** namespace. You have two ways to get it there:

| Repository kind                      | Self-managed (default)                                                                       | Projection (recommended for shared repos)                                                                       |
| ------------------------------------ | -------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `Repository` (namespaced)            | Nothing extra — the repo and its Secret are already in the workload namespace.               | Not needed (no-op): the Secret is already where the mover runs.                                                 |
| `ClusterRepository` (cluster-scoped) | Place a Secret of the same name in **each** workload namespace that backs up to it.          | Set `credentialProjection.enabled: true` on the `BackupConfig`/`Restore`/`Maintenance` that uses it.            |

/// tip | Don't hand-copy Secrets — turn on projection

If you run a shared `ClusterRepository` across more than a namespace or two, **use credential projection** instead of replicating the Secret by hand. It's one field on the consumer (`BackupConfig`/`Restore`/`Maintenance`) and you never touch the credential Secret in a workload namespace again. It's **off by default** (a namespace is a trust boundary, so cross-namespace copying is opt-in), but for the multi-namespace shared-repo case it's the intended path — see [below](#let-kopiur-project-the-credentials-secret-recommended-for-shared-repos).

///

### Let Kopiur project the credentials Secret (recommended for shared repos)

Set `spec.credentialProjection.enabled: true` on the **consumer** — the `BackupConfig` (also on `Restore` and `Maintenance`), not the repository. The namespace owner opts in, rather than the shared repository pushing its creds everywhere. Before each mover run, Kopiur reads the referenced repository's credential Secret(s) from their source namespace and writes a copy into the mover Job's namespace — so the `envFrom` resolves without you placing anything there:

```yaml
# on the BackupConfig in your workload namespace (Restore/Maintenance take the same field)
apiVersion: kopiur.home-operations.com/v1alpha1
kind: BackupConfig
metadata:
  name: my-data
  namespace: media
spec:
  repository: { kind: ClusterRepository, name: shared-primary }
  sources:
    - pvc: { name: my-data }
  credentialProjection:
    enabled: true # off by default; flip this on to stop hand-copying Secrets
```

`Backup`s produced from this config (manual, scheduled, or discovered) inherit the setting.

How the projected copies behave:

- **Per run, owned by the consuming CR.** Each mover gets its own copy named `<run>-creds-N`, with an `ownerReference` to the `Backup`/`Restore`/`Maintenance` that created it. Deleting that CR garbage-collects the copy — no orphaned Secrets.
- **Always fresh.** The copy is re-read from source on every run, so rotating the source Secret takes effect on the next backup. There is no long-lived shadow copy to drift.
- **A no-op when not needed.** For a namespaced `Repository` whose Secret already lives in the workload namespace, projection copies nothing — it just verifies the Secret is present, exactly like the self-managed path. It only copies for the genuine cross-namespace case.
- **Labeled kopiur-managed.** Copies carry `app.kubernetes.io/managed-by=kopiur` and `app.kubernetes.io/component=credentials`, plus a `kopiur.home-operations.com/projected-from` annotation recording the source — don't edit them by hand.

/// warning | Projection needs the operator's `secrets` create/patch RBAC

To write Secrets into workload namespaces, the operator needs cluster-wide `secrets` `create`/`patch`. The Helm value `secretProjection.enabled` (**off by default**) grants it. Projection is itself opt-in per-consumer, so the chart withholds this broader RBAC until you set `secretProjection.enabled: true`. The trade-off: `create` cannot be scoped to a Secret name, so the operator can write a Secret in any namespace it manages. While it stays at the default `false`, `secrets` access is read-only — and a projection-enabled `BackupConfig`/`Restore`/`Maintenance` surfaces an actionable `403` telling you to enable it. A projected copy in namespace `X` is readable by anything that can read Secrets in `X` — exactly as it would be if you placed it there yourself.

///

### Or manage the Secret yourself

The self-managed default: place the credential Secret in each workload namespace by hand, with `kubectl`, a templating tool, or a secret-sync controller (External Secrets, Reflector, `kubernetes-replicator`). For example, copy it into the `media` namespace:

```console
$ kubectl get secret kopia-rustfs-creds -n kopiur-system -o yaml \
    | sed 's/namespace: kopiur-system/namespace: media/' \
    | kubectl apply -n media -f -
```

When the Secret is missing — projection off and you haven't placed it, or projection on but the **source** Secret doesn't exist — the `Backup` does **not** silently hang. It stays `Pending` and reports exactly what's wrong:

```console
$ kubectl get backup my-backup -n media \
    -o jsonpath='{.status.conditions[?(@.type=="CredentialsAvailable")].message}'
credentials Secret `kopia-rustfs-creds` does not exist in namespace `media`,
where the mover Job runs and loads it via envFrom — Kubernetes envFrom is
namespace-local and cannot read a Secret from another namespace. The referenced
ClusterRepository `rustfs-primary` keeps that Secret in namespace `kopiur-system`...
Fix: create a Secret named `kopia-rustfs-creds` in namespace `media`...
```

Place the Secret and the condition clears to `CredentialsAvailable=True` on the next reconcile; the backup proceeds.

## Privileged movers

By default movers run unprivileged. Some workloads need an elevated mover — most commonly a **root** mover (`spec.mover.securityContext.runAsUser: 0`) to read files an unprivileged user can't. Because the controller mints a ServiceAccount in the workload namespace, a tenant with access there could reuse it to run pods at the mover's privilege. Granting that is therefore a **per-namespace admin decision**, gated by an annotation — exactly like VolSync's `volsync.backube/privileged-movers`.

For the full mover `securityContext` surface — the hardened default, setting or inheriting the UID/GID, and the complex cases — see [The mover security context](security-context.md).

The gate applies to **every** kind that runs a mover — a `BackupConfig`'s `spec.mover`, a `Restore`'s `spec.mover`, and a `Maintenance`'s `spec.mover` alike — including a context **inherited** from a workload pod via `inheritSecurityContextFrom` (the resolved context is what's checked, so an inherited-root mover is gated too). If `spec.mover` requests privilege (any of `runAsUser: 0`, `privileged: true`, `allowPrivilegeEscalation: true`, added Linux capabilities, `runAsNonRoot: false`, or `privilegedMode: true`) and the namespace has **not** opted in, the `Backup`/`Restore`/`Maintenance` is refused with a clear condition:

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

On the next reconcile `MoverPermitted` clears to `True` and the privileged mover runs. To revoke, remove the annotation (or drop the elevated `securityContext` from the `BackupConfig`/`Restore`/`Maintenance`).

/// tip | Prefer unprivileged when you can

Reach for a privileged mover only when a workload genuinely needs it (e.g. an app that writes files as root). Many sources back up fine unprivileged, and an unprivileged mover keeps the minted ServiceAccount's blast radius minimal. Before going root, try matching the mover's UID/GID to the data owner — see [Permissions, UID & GID](permissions.md).

///

## Putting it together: a ClusterRepository backup in a workload namespace

To back up a PVC in `media` to a shared `ClusterRepository` whose Secret lives in `kopiur-system`, with a root mover:

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
3. **Apply** your `BackupConfig` + `Backup` (or `BackupSchedule`) in `media`. The controller mints `kopiur-mover` SA + RoleBinding, both gates pass, and the mover Job runs.
4. **Watch it**:
    ```console
    $ kubectl get backup -n media -w        # Pending → Running → Succeeded
    ```

## Troubleshooting

The mover preconditions surface on the `Backup`/`Restore` status as conditions **and** as `Warning` Events (visible in `kubectl describe`), so you never have to read controller logs to find out why a backup didn't start.

| Symptom                                                                         | Condition / Event                                         | Cause                                                           | Fix                                                               |
| ------------------------------------------------------------------------------- | --------------------------------------------------------- | --------------------------------------------------------------- | ----------------------------------------------------------------- |
| Backup stuck `Pending`, no Job                                                  | `CredentialsAvailable=False` / `MissingCredentialsSecret` | The credential Secret isn't in the workload namespace.          | Create the Secret there (replicate it for a `ClusterRepository`). |
| Backup stuck `Pending`, no Job                                                  | `MoverPermitted=False` / `PrivilegedMoverNotPermitted`    | The mover requests privilege but the namespace hasn't opted in. | Annotate the namespace, or drop the elevated `securityContext`.   |
| Job created but pod never appears, `FailedCreate: serviceaccount ... not found` | (pre-fix only)                                            | The mover SA isn't in the namespace.                            | Upgrade the operator — it now mints the SA automatically.         |

/// info | Where to look

- `kubectl describe backup <name> -n <ns>` — conditions **and** Events in one place.
- `kubectl get serviceaccount,rolebinding -n <ns> -l app.kubernetes.io/component=mover` — confirm the mover RBAC was minted.
- `kubectl get jobs,pods -n <ns> -l kopiur.home-operations.com/backup=<name>` — the mover Job and its pod.

///

## Quick reference

| Thing                              | Value                                                                                                                 |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Minted mover ServiceAccount / role | `kopiur-mover` (release-prefixed)                                                                                     |
| Privileged-mover opt-in annotation | `kopiur.home-operations.com/privileged-movers: "true"` (on the **Namespace**)                                         |
| Credentials condition              | `CredentialsAvailable` (reason `MissingCredentialsSecret`)                                                            |
| Privilege condition                | `MoverPermitted` (reason `PrivilegedMoverNotPermitted`)                                                               |
| Operator RBAC needed to mint       | `serviceaccounts: [create, get]`, `rolebindings: [get, create, update, patch]`, `namespaces: [get]` (cluster install) |
