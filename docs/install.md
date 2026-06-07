# Installing Kopiur

Kopiur is a Kopia-native Kubernetes backup operator (Rust / kube-rs). This guide covers installing the operator with the bundled Helm chart and verifying it.

> Status: **alpha** — API group `kopiur.home-operations.com`, version `v1alpha1`. The CRD surface may still change between releases.

## Prerequisites

- **Kubernetes >= 1.24.** The deploy-or-restore volume-populator path (`Restore` + `PVC.spec.dataSourceRef`) relies on the `AnyVolumeDataSource` feature, available from 1.24 (ADR §4.7).
- **Helm 3 or 4.**
- A **kopia repository backend** you can reach: S3/MinIO, Azure Blob, GCS, B2, filesystem (PVC), SFTP, WebDAV, or rclone.
- _(Optional)_ **cert-manager** — only if you prefer it to manage the admission webhook's certificate. **It is not required**: by default the operator manages the webhook cert itself (see [Webhook TLS](#webhook-tls)).
- _(Optional)_ **volume-data-source-validator** — recommended alongside CSI populators so a malformed `dataSourceRef` is surfaced as an event rather than a silently-stuck PVC (ADR §4.7).
- _(Optional)_ **Prometheus Operator** — if you want the chart's `ServiceMonitor`.

## Quickstart

```bash
# 1. Create the operator namespace.
kubectl create namespace kopiur-system

# 2. Install the chart. No extra flags needed — the webhook cert is
#    self-managed by default (no cert-manager required).
helm install kopiur deploy/helm/kopiur \
  --namespace kopiur-system

# 3. Wait for rollout. (The webhook pod stays in ContainerCreating until the
#    controller mints its serving Secret — a few seconds after the controller
#    becomes ready.)
kubectl -n kopiur-system rollout status deploy/kopiur-controller
kubectl -n kopiur-system rollout status deploy/kopiur-webhook

# 4. Confirm the 7 CRDs are registered.
kubectl get crd -l app.kubernetes.io/part-of=kopiur
```

## Webhook TLS

The admission webhook always serves TLS, and the API server must trust it. `webhook.tls.mode` chooses how the serving certificate is provisioned:

| `webhook.tls.mode` | What happens | Needs cert-manager? |
| ------------------ | ------------ | ------------------- |
| `self` (default)   | The operator mints its own CA + serving cert, writes the Secret, injects the `caBundle`, and auto-rotates the cert (zero-downtime hot-reload). | No |
| `cert-manager`     | cert-manager issues the cert; its `ca-injector` populates the `caBundle`. | Yes |
| `manual`           | You pre-create the Secret and supply `webhook.caBundle`. | No |

### `self` (default)

Nothing to configure — this is the `helm install` above. The operator grants itself the minimal extra RBAC to write its one serving Secret and `patch` the `caBundle` of its two webhook configurations (resourceName-scoped).

### `cert-manager`

```bash
helm install kopiur deploy/helm/kopiur \
  --namespace kopiur-system \
  --set webhook.tls.mode=cert-manager
# Optionally point at your own Issuer/ClusterIssuer instead of the chart's
# self-signed one: --set webhook.certManager.issuerRef.name=my-issuer
```

### `manual`

```bash
# create a kubernetes.io/tls Secret named per webhook.tls.secretName, then:
helm install kopiur deploy/helm/kopiur \
  --namespace kopiur-system \
  --set webhook.tls.mode=manual \
  --set webhook.tls.secretName=kopiur-webhook-tls \
  --set webhook.caBundle="$(base64 -w0 ca.crt)"
```

Or disable the webhook entirely (validation then relies on the controller's defensive checks only — not recommended):

```bash
helm install kopiur deploy/helm/kopiur -n kopiur-system --set webhook.enabled=false
```

## Install scope

| Mode                 | `--set installScope=` | RBAC        | Manages                | `ClusterRepository` |
| -------------------- | --------------------- | ----------- | ---------------------- | ------------------- |
| Namespaced (default) | `namespaced`          | Role        | release namespace only | not reconciled      |
| Cluster              | `cluster`             | ClusterRole | cluster-wide           | reconciled          |

Use **cluster** scope for a shared platform repository (`ClusterRepository`) referenced by many tenant namespaces. See `deploy/examples/02-cluster-repository.yaml`.

## CRD lifecycle

`installCRDs: true` (default) installs the 7 CRDs as Helm **templates**, so the flag is honored and `helm upgrade` re-applies schema changes.

> Caution: with templated CRDs, `helm uninstall kopiur` deletes the CRDs **and
> every `kopiur.home-operations.com` object in the cluster** (Repositories, Backups, ...). For an
> alpha API this is the intended, predictable behavior. To decouple CRD lifecycle
> from the release (e.g. GitOps), install with `--set installCRDs=false` and apply
> the generated CRDs out of band:
>
> ```bash
> # Server-side apply is required: the BackupConfig CRD embeds a full JobSpec
> # (runJob hook) and is too large for the client-side last-applied annotation.
> kubectl apply --server-side -f deploy/crds/all-crds.yaml
> ```

The CRDs and RBAC shipped by the chart are **generated** by `cargo xtask gen-crds` / `cargo xtask gen-rbac` and checked in under `deploy/crds/` and `deploy/rbac/`. Those xtasks are the source of truth.

## First backup

After install, create a repository and start backing up a PVC. The smallest end-to-end example is `deploy/examples/01-single-pvc-scheduled.yaml`:

```bash
kubectl apply -f deploy/examples/01-single-pvc-scheduled.yaml
kubectl get repositories,backupconfigs,backupschedules -n billing
```

Eight runnable walkthroughs live in `deploy/examples/`:

| File                               | Pattern                                             |
| ---------------------------------- | --------------------------------------------------- |
| `01-single-pvc-scheduled.yaml`     | Single PVC, scheduled daily                         |
| `02-cluster-repository.yaml`       | Shared platform `ClusterRepository` (cluster scope) |
| `03-restore-by-backup.yaml`        | Restore by picking a `Backup`                       |
| `04-multi-pvc-selector.yaml`       | Multi-PVC label selector + group snapshot           |
| `05-deploy-or-restore-gitops.yaml` | Deploy-or-restore (PVC `dataSourceRef`)             |
| `06-manual-backup.yaml`            | Manual one-shot `Backup`                            |
| `07-restore-discovered.yaml`       | Restore a discovered / foreign snapshot             |
| `08-maintenance.yaml`              | `kopia maintenance` schedule + ownership lease      |

## Observability

- The controller serves `/metrics`, `/healthz`, and `/readyz` on its probe port (`:8080`); the webhook serves `/metrics` on its TLS port. All metrics are under the `kopiur_` namespace.
- `metrics.enabled=true` (default) creates a metrics `Service`.
- `metrics.serviceMonitor.enabled=true` creates a Prometheus-Operator `ServiceMonitor` (requires the Prometheus-Operator CRDs); `metrics.prometheusRule.enabled=true` ships the kopiur alert rules.
- `grafanaDashboard.enabled=true` ships the Grafana dashboard as a sidecar-discoverable `ConfigMap` (source: `deploy/dashboards/kopiur.json`). Set `grafanaDashboard.grafanaOperator.enabled=true` to instead render a [grafana-operator](https://grafana.github.io/grafana-operator/) `GrafanaDashboard` CR from the same JSON (use `grafanaDashboard.grafanaOperator.matchLabels` to select the Grafana instance).
- `observability.otlp.enabled=true` (with `observability.otlp.endpoint`) additionally exports OTLP **traces, logs, and a metrics push** from the controller, webhook, and mover Jobs. Off by default.

Turn it all on with the ready-made overlay:

```bash
helm upgrade kopiur deploy/helm/kopiur -n kopiur-system \
  -f deploy/observability-values.yaml
```

See [`docs/dev/observability.md`](dev/observability.md) for the full metric list, OTLP details, and a sample collector config.

## Upgrade / uninstall

```bash
helm upgrade kopiur deploy/helm/kopiur -n kopiur-system   # re-applies CRD schema
helm uninstall kopiur -n kopiur-system                     # see CRD caution above
```

## See also

- Design: [`docs/adr/0003-kopiur-rust-operator.md`](adr/0003-kopiur-rust-operator.md)
- Chart values & modes: [`deploy/helm/kopiur/README.md`](https://github.com/home-operations/kopiur/blob/main/deploy/helm/kopiur/README.md)
