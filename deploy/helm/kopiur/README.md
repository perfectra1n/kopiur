# kopiur

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.1.0](https://img.shields.io/badge/AppVersion-0.1.0-informational?style=flat-square)

Kopiur — a Kopia-native Kubernetes backup operator written in Rust.
Installs the controller, admission webhook, the 7 kopiur.home-operations.com/v1alpha1 CRDs,
and the RBAC required to run them. Implements ADR-0003.

**Homepage:** <https://github.com/home-operations/kopiur>

Requires Kubernetes **>= 1.24** — the CSI volume-populator path (`Restore` +
`PVC.dataSourceRef`) needs the `AnyVolumeDataSource` machinery GA-gated from
1.24+ (ADR §4.7).

## TL;DR

```bash
# namespaced install (default), self-managed webhook cert disabled-by-default:
helm install kopiur deploy/helm/kopiur \
  --namespace kopiur-system --create-namespace \
  --set webhook.certManager.enabled=true   # easiest: let cert-manager mint the cert
```

See [`docs/install.md`](../../../docs/install.md) for the full quickstart and prerequisites.

## Install modes

### Scope: `namespaced` (default) vs `cluster`

| `installScope` | RBAC | What it manages | `ClusterRepository` |
|---|---|---|---|
| `namespaced` | `Role` + `RoleBinding` | `kopiur.home-operations.com` objects in the **release namespace** only | not reconciled |
| `cluster` | `ClusterRole` + `ClusterRoleBinding` | `kopiur.home-operations.com` objects **cluster-wide** | reconciled |

`namespaced` is the safer default (ADR §4.11). Choose `cluster` when a platform team runs a shared backup tier (a `ClusterRepository` referenced by many tenant namespaces).

```bash
helm install kopiur deploy/helm/kopiur --set installScope=cluster ...
```

The RBAC rules are **synced from `cargo xtask gen-rbac`** (the checked-in `deploy/rbac/operator-*.yaml`), which derives the `kopiur.home-operations.com` permissions from the kube-rs `Resource` traits. The xtask is the source of truth; the chart templates carry a header comment to that effect and own only the names/labels.

### Webhook TLS: cert-manager vs self-managed

The admission webhook always serves TLS. Two options:

- **`webhook.certManager.enabled=true`** — the chart provisions a cert-manager `Certificate` (+ a self-signed `Issuer`, unless you point `webhook.certManager.issuerRef` at your own) and lets cert-manager's `ca-injector` populate the `caBundle` on both webhook configurations. Requires cert-manager installed in the cluster.
- **`webhook.certManager.enabled=false`** (default) — you supply the serving cert yourself: create the `Secret` named by `webhook.tls.secretName` (type `kubernetes.io/tls`) and set `webhook.caBundle` (base64 PEM) so the API server trusts the webhook.

Disable the webhook entirely with `webhook.enabled=false` (validation then falls back to the controller's defensive checks only — not recommended).

### CRD install toggle

`installCRDs: true` (default) renders the 7 CRDs as **templates** (not via Helm's special `crds/` directory) so the flag actually works and `helm upgrade` re-applies schema changes. Trade-off: `helm uninstall` will then delete the CRDs **and every `kopiur.home-operations.com` object in the cluster**. For decoupled CRD lifecycle (e.g. GitOps), set `installCRDs: false` and apply `deploy/crds/*.yaml` out of band.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| controller.affinity | object | `{}` |  |
| controller.extraArgs | list | `[]` | Extra CLI args appended to the controller container. |
| controller.extraEnv | list | `[]` | Extra environment variables for the controller container. |
| controller.extraVolumeMounts | list | `[]` | Extra volume mounts on the controller container (pairs with extraVolumes). |
| controller.extraVolumes | list | `[]` | Extra volumes on the controller pod. Use this to make a filesystem-backend repository reachable in-process (hostPath/NFS/PVC) so the controller can run short idempotent kopia ops (ADR §5.4). The e2e harness uses a hostPath here. |
| controller.leaderElection | object | `{"enabled":true}` | Enable leader election. Required when replicaCount > 1; harmless at 1. |
| controller.logLevel | string | `"info"` | DEPRECATED: use the top-level `logging.level` instead. Kept as a fallback for existing values files — `logging.level` wins when both are set. Applies to the controller and webhook (RUST_LOG style: error|warn|info|debug|trace). |
| controller.nodeSelector | object | `{}` | Scheduling controls. |
| controller.podAnnotations | object | `{}` |  |
| controller.podLabels | object | `{}` | Extra pod labels / annotations. |
| controller.priorityClassName | string | `""` | Pod-level priority class. |
| controller.probePort | int | `8080` | Liveness/readiness HTTP probe port on the controller (serves /healthz, /readyz, /metrics). |
| controller.replicaCount | int | `1` | Number of controller replicas. >1 enables HA via leader election; only the elected leader reconciles, so deterministic jitter (ADR §4.1) keeps schedules identical across replicas and across failover. |
| controller.resources | object | `{"limits":{"memory":"1Gi"},"requests":{"cpu":"50m","memory":"128Mi"}}` | Resource requests/limits for the controller pod. No CPU limit (CPU throttling on an operator only adds reconcile latency; the request reserves a fair share). The memory limit must cover the *burst* on startup/restart, not just steady state (~120Mi): on (re)start the controller reconciles every existing resource at once, spawning concurrent in-process `kopia` subprocesses (whose RSS counts against this container's cgroup) to list/connect a repository that may hold many snapshots. With the OpenTelemetry/OTLP stack linked in, 256Mi was too tight — the burst OOMKilled the controller, which then crash-looped (OOM -> restart -> re-reconcile burst -> OOM). 1Gi gives ample headroom. See crates/e2e/tests/lifecycle.rs. |
| controller.tolerations | list | `[]` |  |
| fullnameOverride | string | `""` | Override the full release-qualified name (defaults to "<release>-kopiur"). |
| grafanaDashboard | object | `{"enabled":false,"folder":"","folderAnnotation":"","label":"grafana_dashboard","labelValue":"1"}` | Grafana dashboard for the kopiur fleet. Shipped as a ConfigMap labeled for the Grafana sidecar to auto-discover; the same JSON also lives in deploy/dashboards/kopiur.json for manual import. |
| grafanaDashboard.enabled | bool | `false` | Create the dashboard ConfigMap. |
| grafanaDashboard.folderAnnotation | string | `""` | Annotation setting the Grafana folder for the dashboard (optional). |
| grafanaDashboard.label | string | `"grafana_dashboard"` | Label the Grafana sidecar watches for (key: value). Adjust to your stack. |
| image.controller.digest | string | `""` | Pin by digest (e.g. "sha256:..."); takes precedence over tag. |
| image.controller.repository | string | `"home-operations/kopiur-controller"` |  |
| image.controller.tag | string | `""` | Defaults to .Chart.AppVersion when empty. |
| image.mover.digest | string | `""` | STRONGLY RECOMMENDED in production. Digest pin for the mover Job image. |
| image.mover.pullPolicy | string | `"IfNotPresent"` | Pull policy used on the mover Job pods. |
| image.mover.repository | string | `"home-operations/kopiur-mover"` |  |
| image.mover.tag | string | `""` |  |
| image.pullPolicy | string | `"IfNotPresent"` | Default image pull policy for controller and webhook. |
| image.registry | string | `"ghcr.io"` | Container registry shared by all three images unless overridden per-image. |
| image.webhook.digest | string | `""` |  |
| image.webhook.repository | string | `"home-operations/kopiur-webhook"` |  |
| image.webhook.tag | string | `""` |  |
| imagePullSecrets | list | `[]` | Image pull secrets applied to controller/webhook pods and mover Jobs. |
| installCRDs | bool | `true` | Install the 7 kopiur.home-operations.com CRDs as part of this release. When true the CRDs are rendered under templates/ (guarded by this flag) so `helm uninstall` removes them and `installCRDs: false` actually omits them. Trade-off: templated CRDs are subject to Helm's normal apply ordering and (unlike the special `crds/` directory) are NOT skipped on `helm upgrade`. That is the behavior we want for an alpha API. If you manage CRDs out of band (GitOps applies deploy/crds/*.yaml directly), set this to false. |
| installScope | string | `"namespaced"` | "namespaced" (default) or "cluster".   namespaced — RBAC is a namespace-scoped Role/RoleBinding. The operator     manages kopiur.home-operations.com resources only in its own namespace. ClusterRepository     is NOT reconciled (it is a cluster-scoped kind and out of a Role's reach).   cluster    — RBAC is a ClusterRole/ClusterRoleBinding. The operator manages     kopiur.home-operations.com resources cluster-wide AND reconciles ClusterRepository. Per ADR §4.11 namespaced is the safer default; cluster scope is an explicit opt-in for platform teams running a shared backup tier. |
| logging.format | string | `"text"` | Console format: "text" (human-readable, default) or "json" (one structured object per line for Loki/ELK/Datadog). Unknown values degrade to text. |
| logging.level | string | `""` | Log level / filter directive (RUST_LOG style: error|warn|info|debug|trace; per-target works too, e.g. "info,kopia=debug" to see kopia's own progress in mover logs). When empty, falls back to the deprecated `controller.logLevel`. |
| metrics.enabled | bool | `true` | Expose a Service for the controller's /metrics endpoint. |
| metrics.port | int | `8080` | Service port for /metrics. |
| metrics.prometheusRule.backupStaleAfterSeconds | int | `172800` | Age (seconds) after which a BackupConfig's last success is "stale". |
| metrics.prometheusRule.enabled | bool | `false` | Create a Prometheus-Operator PrometheusRule with kopiur alerts. |
| metrics.prometheusRule.labels | object | `{}` | Extra labels (e.g. to match your Prometheus ruleSelector). |
| metrics.serviceMonitor.enabled | bool | `false` | Create a Prometheus-Operator ServiceMonitor. Requires the CRD to exist. |
| metrics.serviceMonitor.interval | string | `"30s"` | Scrape interval. |
| metrics.serviceMonitor.labels | object | `{}` | Extra labels (e.g. to match your Prometheus serviceMonitorSelector). |
| metrics.serviceMonitor.metricRelabelings | list | `[]` |  |
| metrics.serviceMonitor.relabelings | list | `[]` | Relabelings / metricRelabelings passed through verbatim. |
| metrics.serviceMonitor.scrapeTimeout | string | `"10s"` | Scrape timeout. |
| nameOverride | string | `""` | Override the chart name used in resource names (defaults to .Chart.Name = "kopiur"). |
| observability.otlp.enabled | bool | `false` | Enable OTLP export (sets OTEL_EXPORTER_OTLP_ENDPOINT on all components). |
| observability.otlp.endpoint | string | `"http://otel-collector.observability.svc:4317"` | Collector gRPC endpoint. Required when enabled. Only gRPC is compiled in. |
| observability.otlp.extraEnv | list | `[]` | Extra raw env (e.g. OTEL_TRACES_SAMPLER) added to every component. |
| observability.otlp.headers | string | `""` | OTEL_EXPORTER_OTLP_HEADERS, e.g. "authorization=Bearer xyz". Empty to omit. |
| observability.otlp.protocol | string | `"grpc"` | OTEL_EXPORTER_OTLP_PROTOCOL (only "grpc" is supported by this build). |
| observability.otlp.strict | bool | `false` | Fail-fast on telemetry misconfiguration instead of degrading to fmt+pull. |
| podSecurityContext.fsGroup | int | `65534` |  |
| podSecurityContext.runAsGroup | int | `65534` |  |
| podSecurityContext.runAsNonRoot | bool | `true` |  |
| podSecurityContext.runAsUser | int | `65534` |  |
| podSecurityContext.seccompProfile.type | string | `"RuntimeDefault"` |  |
| securityContext.allowPrivilegeEscalation | bool | `false` |  |
| securityContext.capabilities.drop[0] | string | `"ALL"` |  |
| securityContext.readOnlyRootFilesystem | bool | `true` |  |
| serviceAccount.annotations | object | `{}` | Extra annotations (e.g. IRSA / Workload Identity role bindings). |
| serviceAccount.create | bool | `true` | Create the ServiceAccount. Disable to bring your own. |
| serviceAccount.name | string | `""` | Name to use; defaults to the chart fullname when empty. |
| webhook.affinity | object | `{}` |  |
| webhook.caBundle | string | `""` | Base64-encoded PEM CA bundle injected into the webhook configurations when certManager.enabled is false. Required for the API server to trust a self-managed serving cert. Ignored when certManager.enabled is true. |
| webhook.certManager.enabled | bool | `false` | Provision a cert-manager Certificate + self-signed Issuer for the webhook, and let cert-manager's ca-injector populate caBundle on the webhook configurations (via the cert-manager.io/inject-ca-from annotation). When false you must supply webhook.tls.secretName yourself and set webhook.caBundle (base64 PEM) so the API server trusts the webhook. |
| webhook.certManager.issuerRef | object | `{"kind":"Issuer","name":""}` | Use an existing Issuer/ClusterIssuer instead of the self-signed Issuer this chart creates. Leave name empty to use the chart-managed Issuer. |
| webhook.containerPort | int | `8443` | Port the webhook container listens on (must match listenAddr above). |
| webhook.enabled | bool | `true` | Deploy the webhook (Deployment + Service + Validating/Mutating configs). When false, validation falls back to the controller's defensive checks only. |
| webhook.failurePolicy | string | `"Fail"` | failurePolicy for both webhook configurations: Fail (fail-closed, recommended for a backup operator — ADR §7.2) or Ignore. |
| webhook.listenAddr | string | `"0.0.0.0:8443"` | Address the webhook server binds to (env KOPIUR_WEBHOOK_ADDR). |
| webhook.nodeSelector | object | `{}` |  |
| webhook.podAnnotations | object | `{}` |  |
| webhook.podLabels | object | `{}` |  |
| webhook.priorityClassName | string | `""` |  |
| webhook.replicaCount | int | `1` |  |
| webhook.resources.limits.memory | string | `"512Mi"` |  |
| webhook.resources.requests.cpu | string | `"25m"` |  |
| webhook.resources.requests.memory | string | `"64Mi"` |  |
| webhook.serviceMonitor.enabled | bool | `false` | Create a ServiceMonitor scraping the webhook's /metrics over HTTPS. |
| webhook.serviceMonitor.insecureSkipVerify | bool | `true` | The webhook serves a self-signed cert, so skip verification by default. |
| webhook.serviceMonitor.interval | string | `"30s"` |  |
| webhook.serviceMonitor.labels | object | `{}` |  |
| webhook.serviceMonitor.scrapeTimeout | string | `"10s"` |  |
| webhook.timeoutSeconds | int | `10` | timeoutSeconds for admission requests (1..30). |
| webhook.tls.secretName | string | `"kopiur-webhook-tls"` | Name of the Secret holding tls.crt / tls.key (and optionally ca.crt). When certManager.enabled is true this is the Secret cert-manager writes. Otherwise YOU must create this Secret before install (kubernetes.io/tls). |
| webhook.tolerations | list | `[]` |  |

### Observability

Metrics are always available on the controller's `/metrics` (also `/healthz`, `/readyz`); enable `metrics.serviceMonitor` to scrape them. Turning on `observability.otlp` additionally exports **traces, logs, and a metrics push** over OTLP from the controller, webhook, and mover Jobs (the controller passes the `OTEL_*` env through to the Jobs it creates) — set `observability.otlp.endpoint` to your collector's gRPC port. All metrics are under the `kopiur_` namespace; see [`docs/dev/observability.md`](../../../docs/dev/observability.md) for the full metric list, env vars, and a sample collector config. A ready-made values overlay that turns everything on is at `deploy/observability-values.yaml`. The dashboard JSON also lives at `deploy/dashboards/kopiur.json` for manual Grafana import.

## Verify a render locally

```bash
helm lint deploy/helm/kopiur
helm template kopiur deploy/helm/kopiur --set installScope=cluster --set webhook.certManager.enabled=true
```

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| kopiur maintainers |  |  |

## Source Code

* <https://github.com/home-operations/kopiur>

## Requirements

Kubernetes: `>=1.24.0-0`

---

_This README is generated by [helm-docs](https://github.com/norwoodj/helm-docs) from `Chart.yaml` and `values.yaml`. Edit those (or `README.md.gotmpl`) and run `mise run helm-docs`._
