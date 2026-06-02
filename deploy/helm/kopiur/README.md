# kopiur Helm chart

Deploys the **Kopiur** Kopia-native Kubernetes backup operator: the controller,
the admission webhook, the 7 `kopia.io/v1alpha1` CRDs, and the RBAC to run them.
Implements [ADR-0003](../../../docs/adr/0003-kopiur-rust-operator.md).

- Chart type: `application`
- Chart version: `0.1.0` · App version: `0.1.0`
- Requires Kubernetes **>= 1.24** (CSI volume-populator path, ADR §4.7)

## TL;DR

```bash
# namespaced install (default), self-managed webhook cert disabled-by-default:
helm install kopiur deploy/helm/kopiur \
  --namespace kopiur-system --create-namespace \
  --set webhook.certManager.enabled=true   # easiest: let cert-manager mint the cert
```

See [`docs/install.md`](../../../docs/install.md) for the full quickstart and
prerequisites.

## Install modes

### Scope: `namespaced` (default) vs `cluster`

| `installScope` | RBAC | What it manages | `ClusterRepository` |
|---|---|---|---|
| `namespaced` | `Role` + `RoleBinding` | `kopia.io` objects in the **release namespace** only | not reconciled |
| `cluster` | `ClusterRole` + `ClusterRoleBinding` | `kopia.io` objects **cluster-wide** | reconciled |

`namespaced` is the safer default (ADR §4.11). Choose `cluster` when a platform
team runs a shared backup tier (a `ClusterRepository` referenced by many tenant
namespaces).

```bash
helm install kopiur deploy/helm/kopiur --set installScope=cluster ...
```

The RBAC rules are **synced from `cargo xtask gen-rbac`** (the checked-in
`deploy/rbac/operator-*.yaml`), which derives the `kopia.io` permissions from the
kube-rs `Resource` traits. The xtask is the source of truth; the chart templates
carry a header comment to that effect and own only the names/labels.

### Webhook TLS: cert-manager vs self-managed

The admission webhook always serves TLS. Two options:

- **`webhook.certManager.enabled=true`** — the chart provisions a cert-manager
  `Certificate` (+ a self-signed `Issuer`, unless you point `webhook.certManager.issuerRef`
  at your own) and lets cert-manager's `ca-injector` populate the `caBundle` on
  both webhook configurations. Requires cert-manager installed in the cluster.
- **`webhook.certManager.enabled=false`** (default) — you supply the serving cert
  yourself: create the `Secret` named by `webhook.tls.secretName` (type
  `kubernetes.io/tls`) and set `webhook.caBundle` (base64 PEM) so the API server
  trusts the webhook.

Disable the webhook entirely with `webhook.enabled=false` (validation then falls
back to the controller's defensive checks only — not recommended).

### CRD install toggle

`installCRDs: true` (default) renders the 7 CRDs as **templates** (not via Helm's
special `crds/` directory) so the flag actually works and `helm upgrade`
re-applies schema changes. Trade-off: `helm uninstall` will then delete the CRDs
**and every `kopia.io` object in the cluster**. For decoupled CRD lifecycle (e.g.
GitOps), set `installCRDs: false` and apply `deploy/crds/*.yaml` out of band.

## Values

| Key | Default | Description |
|---|---|---|
| `nameOverride` / `fullnameOverride` | `""` | Name overrides. |
| `image.registry` | `ghcr.io` | Shared registry. |
| `image.pullPolicy` | `IfNotPresent` | Controller/webhook pull policy. |
| `image.controller.repository` | `perfectra1n/kopiur-controller` | Controller image repo. |
| `image.controller.tag` / `.digest` | `""` | Tag (defaults to appVersion) / digest (wins). |
| `image.webhook.repository` | `perfectra1n/kopiur-webhook` | Webhook image repo. |
| `image.webhook.tag` / `.digest` | `""` | Tag / digest. |
| `image.mover.repository` | `perfectra1n/kopiur-mover` | Per-Job mover image repo. |
| `image.mover.digest` | `""` | **Recommended** digest pin (ADR §G15); wins over tag. |
| `image.mover.pullPolicy` | `IfNotPresent` | Mover Job pull policy. |
| `imagePullSecrets` | `[]` | Pull secrets for pods and mover Jobs. |
| `installCRDs` | `true` | Render the 7 CRDs as templates (toggleable). |
| `installScope` | `namespaced` | `namespaced` or `cluster`. |
| `serviceAccount.create` | `true` | Create the ServiceAccount. |
| `serviceAccount.name` | `""` | SA name (defaults to fullname). |
| `serviceAccount.annotations` | `{}` | e.g. IRSA / Workload Identity. |
| `controller.replicaCount` | `1` | `>1` = HA via leader election. |
| `controller.leaderElection.enabled` | `true` | Required for `replicaCount > 1`. |
| `controller.logLevel` | `info` | `RUST_LOG` value. |
| `controller.resources` | requests 50m/64Mi, limits 500m/256Mi | Controller resources. |
| `controller.nodeSelector` / `tolerations` / `affinity` | `{}` / `[]` / `{}` | Scheduling. |
| `controller.priorityClassName` | `""` | Priority class. |
| `controller.probePort` | `8080` | Health + `/metrics` port. |
| `webhook.enabled` | `true` | Deploy webhook + configs. |
| `webhook.replicaCount` | `1` | Webhook replicas. |
| `webhook.failurePolicy` | `Fail` | `Fail` (fail-closed) or `Ignore`. |
| `webhook.timeoutSeconds` | `10` | Admission timeout. |
| `webhook.listenAddr` | `0.0.0.0:8443` | `KOPIUR_WEBHOOK_ADDR`. |
| `webhook.containerPort` | `8443` | TLS container port. |
| `webhook.tls.secretName` | `kopiur-webhook-tls` | Serving cert Secret. |
| `webhook.certManager.enabled` | `false` | Provision cert-manager Certificate/Issuer + ca-injection. |
| `webhook.certManager.issuerRef.{name,kind}` | `""` / `Issuer` | Use an existing issuer. |
| `webhook.caBundle` | `""` | Base64 PEM CA (when certManager disabled). |
| `webhook.resources` / scheduling | see values.yaml | Webhook pod tuning. |
| `metrics.enabled` | `true` | Create the metrics Service. |
| `metrics.port` | `8080` | Metrics Service port. |
| `metrics.serviceMonitor.enabled` | `false` | Create a Prometheus-Operator ServiceMonitor. |
| `metrics.serviceMonitor.{interval,scrapeTimeout,labels,...}` | see values.yaml | ServiceMonitor tuning. |
| `podSecurityContext` | runAsNonRoot, uid/gid/fsGroup 65534, RuntimeDefault | Pod security (ADR §4.9/§4.11). |
| `securityContext` | drop ALL, no privilege escalation, read-only rootfs | Container security. |

## Verify a render locally

```bash
helm lint deploy/helm/kopiur
helm template kopiur deploy/helm/kopiur --set installScope=cluster --set webhook.certManager.enabled=true
```
