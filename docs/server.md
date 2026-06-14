# Web UI (kopia server)

Kopia ships a built-in **web UI** — an HTML view of a repository's snapshots,
policies, sources, and tasks. Kopiur exposes it declaratively: set `spec.server`
on a `Repository` (or `ClusterRepository`) and the operator runs `kopia server
start` in a `Deployment` and puts a `Service` in front of it. There is **no
`enabled` bool** — the presence of the `spec.server` block is what turns it on,
and removing the block tears everything back down.

Kopiur creates the workload and the `Service` only. Routing the Service to the
outside world (an `Ingress`/`HTTPRoute`) is yours to wire — see
[Exposing the Service](#exposing-the-service).

## When would you use this?

The UI is an **interactive** surface for a human. Reach for it when you want to:

- **Browse and verify** snapshots, policies, and sources visually, without the
  [kubectl plugin](cli/index.md).
- **Restore ad hoc** through the UI — pick a snapshot, mount it, pull a file.
- Give an operator a point-and-click view of a repository's contents.

You do **not** need it for normal operation. Scheduled backups, restores, and
maintenance all run headless in short-lived mover Jobs — the UI is never on that
path. Because it is a **long-lived pod that holds the repository decryption key**
(see the warning below), only run it where you actually want interactive access,
and tear it down when you're done.

/// warning | The kopia UI has no read-only mode

Anyone who can reach the UI can **read, create, and delete** backups, and the
server pod holds the repository **decryption key**. There is no view-only mode in
kopia's server. Treat exposing the UI exactly like exposing the repository
itself: keep it `ClusterIP` (the default), put authentication in front of it, and
restrict who can reach the `Service` with a `NetworkPolicy`.

///

## The `spec.server` surface

| Field | Type | Default | What it does |
| --- | --- | --- | --- |
| `auth` | externally-tagged [enum](#authentication) (`generate` \| `secretRef` \| `insecure`) | `generate` | UI login. Omitted ⇒ operator-generated credentials. **Never** defaults to no-auth. |
| `service.type` | enum(**`ClusterIP`**\|`NodePort`\|`LoadBalancer`) | `ClusterIP` | How the `Service` is exposed. Routing outside the cluster is your job. |
| `service.port` | int | `51515` | Listen + `Service` port. |
| `service.annotations` | map | — | Applied to the `Service` — the seam for your ingress/LB controller. |
| `resources` | [ResourceRequirements](field-reference.md) | — | Requests/limits for the server pod. |
| `securityContext` | [SecurityContext](security-context.md) | hardened default | Override the default hardened container security context. |
| `namespace` | string | — | **`ClusterRepository` only, required** — which namespace the server objects land in (a cluster-scoped owner has no implicit namespace). |

There is no `enabled` field: **presence of `spec.server` is "on"**, absence is
"off". See the [full field reference](field-reference.md#serverspec).

## How to deploy it

`spec.server` is just a field on a `Repository`, so it deploys like any other CRD
edit — `kubectl apply`, or through GitOps ([Flux/Argo](gitops.md)). The smallest
form adds the block to a repository and takes the safe defaults (operator-minted
credentials, `ClusterIP`):

```yaml
apiVersion: kopiur.home-operations.com/v1alpha1
kind: Repository
metadata:
    name: nas-primary
    namespace: apps
spec:
    backend:
        s3: { bucket: my-backups, endpoint: s3.amazonaws.com, region: us-east-1, auth: { secretRef: { name: nas-primary-creds } } }
    encryption:
        passwordSecretRef: { name: nas-primary-creds, key: KOPIA_PASSWORD }
    server:
        auth: { generate: {} } # operator mints UI credentials into an owned Secret
```

### What the operator creates for you

Once the repository is `Ready`, the controller materializes — all named
`<repo>-kopia-ui` and labeled `app.kubernetes.io/name=kopiur-server`,
`app.kubernetes.io/instance=<repo>`:

| Object | Name | Purpose |
| --- | --- | --- |
| `Deployment` | `<repo>-kopia-ui` | Runs `kopia server start` (1 replica, `Recreate` strategy, mover image, TCP readiness/liveness probes). |
| `Service` | `<repo>-kopia-ui` | Fronts the Deployment on the configured port. |
| `ConfigMap` | `<repo>-kopia-ui` | The server's work spec (which repo, port, auth mode). |
| `Secret` | `<repo>-kopia-ui-auth` | **`generate` mode only** — the minted UI credentials (`username`/`password`). |

```console
$ kubectl get deploy,svc,cm,secret -n apps \
    -l app.kubernetes.io/name=kopiur-server,app.kubernetes.io/instance=nas-primary
```

The controller manages `Deployments`/`Services`/`ConfigMaps`/`Secrets` for this
feature; the RBAC for it ships with the chart (see
[Installation](install.md#install-scope)). For a namespaced `Repository` the
objects carry an `ownerReference` to the repository; a `ClusterRepository` cleans
them up via a finalizer instead (see [ClusterRepository](#clusterrepository-server)).

/// info | The server runs without in-pod TLS

The operator starts kopia with `--insecure` — i.e. plain HTTP inside the pod.
That is deliberate: TLS termination belongs at your ingress/load balancer, not in
the server pod. The credentials still protect the UI; just don't expose the raw
`Service` to an untrusted network without TLS in front.

///

## Authentication

`spec.server.auth` is an externally-tagged enum — you set exactly one of three
keys. It defaults to `generate` (never to no-auth).

| Mode | Shape | When to use |
| --- | --- | --- |
| **`generate`** _(default)_ | `generate: { username? }` | Let the operator mint a random password. The simplest safe choice. |
| **`secretRef`** | `secretRef: { name, usernameKey, passwordKey }` | You manage the UI credentials yourself (e.g. a shared/SSO-fronted password). |
| **`insecure`** | `insecure: { acknowledgeInsecure: true }` | **No login at all.** A footgun; for throwaway/lab use only. |

### `generate` — operator-minted credentials (recommended)

The operator creates a `Secret` `<repo>-kopia-ui-auth` once (keys `username`,
`password`), pins its reference to `status.server.generatedSecretRef`, and
**never rotates it** on later reconciles. The username defaults to `kopia`; set
`generate: { username: alice }` to change it. Read the password with:

```console
$ kubectl get secret nas-primary-kopia-ui-auth -n apps \
    -o jsonpath='{.data.password}' | base64 -d; echo
```

### `secretRef` — bring your own credentials

Point at a `Secret` you own; all three keys are required:

```yaml
server:
    auth:
        secretRef: { name: my-ui-creds, usernameKey: username, passwordKey: password }
```

### `insecure` — no authentication

Disables the UI login entirely. It demands an explicit acknowledgement, so you
can't reach it by accident:

```yaml
server:
    auth:
        insecure: { acknowledgeInsecure: true } # required — the webhook rejects it otherwise
```

/// danger | `insecure` exposes the whole repository with no login

With `insecure`, anyone who can reach the `Service` has full read/write/**delete**
of every backup. The admission webhook rejects the mode unless you set
`acknowledgeInsecure: true`. Only use it on an isolated network you fully trust,
and pair it with a `NetworkPolicy`.

///

## Exposing the Service

`spec.server.service` controls the `Service`; `port` defaults to `51515`.

| `service.type` | Reach it from | Notes |
| --- | --- | --- |
| **`ClusterIP`** _(default)_ | inside the cluster | Use `kubectl port-forward` or your own ingress. The safe default. |
| `NodePort` | each node's IP | A static high port on every node. |
| `LoadBalancer` | an external IP | Provisioned by your cloud/LB controller. |

**Kopiur creates the `Service` only — it never creates an `Ingress` or
`HTTPRoute`.** Point your own router at `Service` `<repo>-kopia-ui` on the
configured port, and put TLS + (ideally) an additional auth layer there. The
[full example](#full-example) carries commented `HTTPRoute` and `NetworkPolicy`
templates you can adapt. Use `service.annotations` to feed your ingress/LB
controller (e.g. an `external-dns` hostname or an LB class).

## Accessing the UI

For a quick look, port-forward the `Service` and open it locally:

```console
$ kubectl port-forward -n apps svc/nas-primary-kopia-ui 51515:51515
# then browse http://localhost:51515 and log in with the credentials above
```

For ongoing access, route an `Ingress`/`HTTPRoute` to the `Service` (with TLS),
and strongly consider a `NetworkPolicy` restricting who may reach it.

## ClusterRepository server { #clusterrepository-server }

A `ClusterRepository` is cluster-scoped and has no implicit namespace, so its
`spec.server` block **requires** a `namespace` (the fields are otherwise
identical, flattened in):

```yaml
apiVersion: kopiur.home-operations.com/v1alpha1
kind: ClusterRepository
metadata:
    name: platform
spec:
    # backend / encryption / allowedNamespaces …
    server:
        namespace: kopiur-system # required: where the server objects land
        auth: { generate: {} }
        service: { type: ClusterIP, port: 51515 }
```

Because a cluster-scoped object can't own namespaced children via an
`ownerReference`, the controller tracks and cleans up the server objects with a
**finalizer + labels** instead. If the repository credentials Secret lives in a
different namespace than the server, the operator mirrors it next to the server
pod (`envFrom` can't cross namespaces). Changing `server.namespace` moves the
server: the operator deletes the objects in the old namespace and recreates them
in the new one (it tracks the last-applied namespace in `status.server.namespace`).

## Filesystem backends require ReadWriteMany

For an **object-store** backend (S3, Azure, GCS, B2, …) the server connects over
the network — no volume constraint. For a **filesystem** backend the server pod
must mount the repository volume, and it is long-lived:

/// warning | A filesystem-backed server needs a ReadWriteMany repo PVC

A long-lived server holding a `ReadWriteOnce` repo PVC would block every
backup/restore/maintenance mover that needs the same volume. The operator
therefore **requires the repository PVC to be `ReadWriteMany`** when `spec.server`
is set on a filesystem `Repository`, and rejects the reconcile otherwise. Use an
RWX-capable StorageClass (or an inline NFS export) for the repository volume, or
keep the UI on an object-store repository.

///

## Inspecting status

The reconciler pins a `status.server` block (it never stores a password):

```console
$ kubectl get repository nas-primary -n apps -o jsonpath='{.status.server}' | jq
```

| Field | Meaning |
| --- | --- |
| `endpoint` | In-cluster address, `<service>.<namespace>.svc:<port>`. |
| `namespace` | Namespace the server objects were last applied to (used to detect a `namespace` change). |
| `authMode` | Resolved auth discriminant — `Generate` / `SecretRef` / `Insecure`. |
| `generatedSecretRef` | **`generate` mode only** — the operator-owned Secret holding the UI credentials. |

When the server is disabled, `status.server` is cleared to null.

## Disabling it

Remove the `spec.server` block (or patch it to null). The operator deletes the
Deployment, Service, ConfigMap, and any generated Secret it owns:

```console
$ kubectl patch repository nas-primary -n apps --type merge -p '{"spec":{"server":null}}'
```

## Full example

A complete, apply-ready `Repository` with `spec.server` (S3 backend, `generate`
auth, plus commented `HTTPRoute` + `NetworkPolicy` templates):

```yaml
--8<-- "deploy/examples/25-repository-server-ui.yaml"
```

## See also

- [Repositories & backends](repositories.md) — the `Repository`/`ClusterRepository` surface this feature sits on.
- [Security context](security-context.md) — the hardened default the server pod runs under, and how to override it.
- [Installation](install.md) — install scope and the RBAC the controller needs to manage the server objects.
- [GitOps (Flux / Argo)](gitops.md) — deploying the field through a GitOps pipeline.
- [`deploy/examples/25-repository-server-ui.yaml`](#full-example) — the apply-ready example above.
- [ADR-0003 §4.14](adr/0003-kopiur-rust-operator.md) — the design rationale.
</content>
