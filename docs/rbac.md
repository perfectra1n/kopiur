# RBAC reference

Everything Kopiur is allowed to do in your cluster, in one place — for security
review, for scoping a namespaced install, and for debugging `Forbidden` errors.

Kopiur runs as **two principals**:

| ServiceAccount | Who uses it | Bound to |
| --- | --- | --- |
| `kopiur-controller` | The controller Deployment **and** the admission webhook Deployment (they share one ServiceAccount) | `kopiur-controller` ClusterRole (cluster scope) or Role (namespaced scope) |
| `kopiur-mover` | Every mover `Job` (snapshot, restore, bootstrap, maintenance, verify, replicate, pin, delete) | `kopiur-mover` ClusterRole/Role |

The authoritative definitions are **generated** by `cargo xtask gen-rbac` into
`deploy/rbac/` (`operator-clusterrole.yaml`, `operator-role.yaml`,
`mover-clusterrole.yaml`, `mover-role.yaml`). The Helm chart templates
(`deploy/helm/kopiur/templates/{clusterrole,role,clusterrole-mover,role-mover}.yaml`)
are hand-synced copies of those files — if this page and the chart ever disagree,
`deploy/rbac/` wins; regenerate with `mise run gen` and check drift with
`mise run gen-check`.

## The controller / webhook (`kopiur-controller`)

What each rule is **for**, grouped by purpose:

| API group → resources | Verbs | Why |
| --- | --- | --- |
| `kopiur.home-operations.com` → all 8 CRDs (`repositories`, `snapshotpolicies`, `snapshots`, `snapshotschedules`, `restores`, `maintenances`, `repositoryreplications`, `clusterrepositories`†) | get, list, watch, create, update, patch, delete | Reconcile every kind; schedules **create** `Snapshot` CRs; repositories **create** owned `Maintenance` CRs; retention **deletes** pruned `Snapshot` CRs. |
| same group → each CRD's `/status` and `/finalizers` | get, update, patch | Status is written via server-side apply (**a PATCH — `patch` is required, not just `update`**); finalizers gate snapshot deletion. |
| core → `pods`, `persistentvolumeclaims`, `configmaps` | get, list, watch, create, update, patch, delete | Resolve workload pods for `inheritSecurityContextFrom` and hooks; create restore-target / cache PVCs; write the mover work-spec ConfigMap. |
| core → `pods/exec` | create, get | Run `hooks.beforeSnapshot/afterSnapshot` `workloadExec` commands inside the workload pod (quiesce/resume). |
| core → `events` **and** `events.k8s.io` → `events` | create, patch | The kube `Recorder` writes `events.k8s.io/v1` Events; **both** groups are needed (a common gotcha — without `events.k8s.io` every event write 403s). |
| core → `secrets` | get, list, watch, create, patch | Read repository credential Secrets (and re-reconcile when they change); **create/patch** is the credential-projection feature (copying a repo's Secret into a consumer namespace) and the self-managed webhook TLS Secret. |
| `batch` → `jobs` | get, list, watch, create, update, patch, delete | Create and track the mover Jobs; reap them per `failedJobsHistoryLimit`. |
| `snapshot.storage.k8s.io` → `volumesnapshots`; `groupsnapshot.storage.k8s.io` → `volumegroupsnapshots` | get, list, watch, create, delete | CSI snapshot / group-snapshot copy methods (`SnapshotPolicy.spec.copyMethod`). |
| core → `serviceaccounts`; `rbac.authorization.k8s.io` → `rolebindings` | get, create, update, patch | Mint the per-namespace mover ServiceAccount + RoleBinding on demand (see below). |
| core → `namespaces` | get, list, watch | Read the `kopiur.home-operations.com/privileged-movers` annotation (the elevated-mover opt-in) and drive `pvcSelector` namespace selection. *(Cluster scope only.)* |
| `admissionregistration.k8s.io` → `validatingwebhookconfigurations`, `mutatingwebhookconfigurations` (names `kopiur-validating` / `kopiur-mutating` only) | get, patch | Inject the self-managed CA bundle into the webhook configurations (`webhook.tls.mode: self`). *(Cluster scope only.)* |
| core → `secrets` (name `kopiur-webhook-tls` only) | update, patch | Rotate the self-managed webhook serving certificate. |

† In a **namespaced install** (`installScope: namespaced`), the Role drops
`clusterrepositories` (a cluster-scoped kind), the webhook-configuration rule, and
the `namespaces` rule — which is also why the privileged-mover gate fails *open*
there: the operator can't read namespace annotations, and the install is already
confined to admin-chosen namespaces.

## The mover (`kopiur-mover`)

The mover is deliberately tiny — it can **only** report its result:

| API group → resources | Verbs | Why |
| --- | --- | --- |
| `kopiur.home-operations.com` → every CRD's `/status` | get, patch | Patch progress and the terminal result (snapshot id, stats, timing, `logTail`, `failure`) onto the CR that owns the Job. |
| core → `configmaps` | get, patch | Read its work-spec; write bootstrap results back to the result ConfigMap. |

It cannot read Secrets (credentials arrive via `envFrom` on the Job), cannot
create or delete anything, and cannot touch other namespaces.

### The runtime-minted per-namespace mover identity

Mover Jobs run in the **workload's** namespace, so before creating a Job there the
controller mints (idempotently, via server-side apply):

1. a `kopiur-mover` ServiceAccount in that namespace, and
2. a RoleBinding from it to the `kopiur-mover` ClusterRole (or Role).

This is what the `serviceaccounts` + `rolebindings` rules above are for. A tenant
in that namespace could create pods that *use* the mover ServiceAccount — which is
exactly why an **elevated** mover (root UID, `privilegedMode`, added capabilities)
additionally requires the namespace to opt in with the
`kopiur.home-operations.com/privileged-movers: "true"` annotation
(see [Movers, RBAC & credentials](movers.md)).

/// note | Auditing tip
`kubectl auth can-i --list --as=system:serviceaccount:kopiur-system:kopiur-controller`
shows the effective permissions on a live cluster; diff it against
`deploy/rbac/operator-clusterrole.yaml` if something looks off.
///

## Browsing snapshots (`rbac.browseRole`)

The `kubectl kopiur ls/cat/download/browse` data-plane runs as the **human's**
kubeconfig identity, not a ServiceAccount. The chart can render an opt-in
ClusterRole (`rbac.browseRole: true` → `<release>-browse`) carrying exactly
what browsing needs — you bind it yourself (a namespaced RoleBinding limits a
user to browsing snapshots in that one namespace):

| API group → resources | Verbs | Why |
| --- | --- | --- |
| `kopiur.home-operations.com` → `snapshots`, `repositories` | get, list | Resolve the Snapshot → repository chain. |
| `kopiur.home-operations.com` → `clusterrepositories` | get | Same chain for cluster-scoped repositories. |
| `apps` → `deployments` | get, list | Resolve the mover image from the controller Deployment (sessions run exactly what the operator runs). |
| `batch` → `jobs` | create, get, list, delete | Find-or-create the read-only session Job; `session end`. |
| core → `configmaps` | create, get, delete | The session's work-spec ConfigMap (owned by the Job). |
| core → `pods` | get, list, watch | Wait for the session pod to become Ready. |
| core → `pods/log` | get | Surface the pod's logs when the session fails to start. |
| core → `pods/exec` | create | The read path: exec the closed kopia read-command set. |

Deliberately **no `secrets` access**: the session pod loads the repository
credentials itself via `envFrom`, so a browsing user never reads them. Note
the binding's blast radius honestly: `pods/exec` and `jobs delete` are
namespace-wide once bound — RBAC cannot scope exec to session pods only, so
bind this role only where that is acceptable. The
`--local` flag is the exception — it copies the credentials to the user's
machine and therefore additionally needs `get` on `secrets`; grant that
separately and consciously.
