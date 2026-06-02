# Installing Kopiur

Kopiur is a Kopia-native Kubernetes backup operator (Rust / kube-rs). This guide
covers installing the operator with the bundled Helm chart and verifying it.

> Status: **alpha** — API group `kopia.io`, version `v1alpha1`. The CRD surface
> may still change between releases.

## Prerequisites

- **Kubernetes >= 1.24.** The deploy-or-restore volume-populator path
  (`Restore` + `PVC.spec.dataSourceRef`) relies on the `AnyVolumeDataSource`
  feature, available from 1.24 (ADR §4.7).
- **Helm 3 or 4.**
- A **kopia repository backend** you can reach: S3/MinIO, Azure Blob, GCS, B2,
  filesystem (PVC), SFTP, WebDAV, or rclone.
- *(Optional)* **cert-manager** — the simplest way to provision the admission
  webhook's serving certificate. Without it you provide the cert yourself.
- *(Optional)* **volume-data-source-validator** — recommended alongside CSI
  populators so a malformed `dataSourceRef` is surfaced as an event rather than a
  silently-stuck PVC (ADR §4.7).
- *(Optional)* **Prometheus Operator** — if you want the chart's `ServiceMonitor`.

## Quickstart

```bash
# 1. Create the operator namespace.
kubectl create namespace kopiur-system

# 2. Install the chart. Easiest path: let cert-manager mint the webhook cert.
helm install kopiur deploy/helm/kopiur \
  --namespace kopiur-system \
  --set webhook.certManager.enabled=true

# 3. Wait for rollout.
kubectl -n kopiur-system rollout status deploy/kopiur-controller
kubectl -n kopiur-system rollout status deploy/kopiur-webhook

# 4. Confirm the 7 CRDs are registered.
kubectl get crd -l app.kubernetes.io/part-of=kopiur
```

### Without cert-manager

The webhook serves TLS and the API server must trust it. If you are not using
cert-manager, create the serving Secret and pass the CA bundle:

```bash
# create a kubernetes.io/tls Secret named per webhook.tls.secretName, then:
helm install kopiur deploy/helm/kopiur \
  --namespace kopiur-system \
  --set webhook.certManager.enabled=false \
  --set webhook.tls.secretName=kopiur-webhook-tls \
  --set webhook.caBundle="$(base64 -w0 ca.crt)"
```

Or disable the webhook entirely (validation then relies on the controller's
defensive checks only — not recommended):

```bash
helm install kopiur deploy/helm/kopiur -n kopiur-system --set webhook.enabled=false
```

## Install scope

| Mode | `--set installScope=` | RBAC | Manages | `ClusterRepository` |
|---|---|---|---|---|
| Namespaced (default) | `namespaced` | Role | release namespace only | not reconciled |
| Cluster | `cluster` | ClusterRole | cluster-wide | reconciled |

Use **cluster** scope for a shared platform repository (`ClusterRepository`)
referenced by many tenant namespaces. See `deploy/examples/02-cluster-repository.yaml`.

## CRD lifecycle

`installCRDs: true` (default) installs the 7 CRDs as Helm **templates**, so the
flag is honored and `helm upgrade` re-applies schema changes.

> Caution: with templated CRDs, `helm uninstall kopiur` deletes the CRDs **and
> every `kopia.io` object in the cluster** (Repositories, Backups, ...). For an
> alpha API this is the intended, predictable behavior. To decouple CRD lifecycle
> from the release (e.g. GitOps), install with `--set installCRDs=false` and apply
> the generated CRDs out of band:
>
> ```bash
> # Server-side apply is required: the BackupConfig CRD embeds a full JobSpec
> # (runJob hook) and is too large for the client-side last-applied annotation.
> kubectl apply --server-side -f deploy/crds/all-crds.yaml
> ```

The CRDs and RBAC shipped by the chart are **generated** by
`cargo xtask gen-crds` / `cargo xtask gen-rbac` and checked in under
`deploy/crds/` and `deploy/rbac/`. Those xtasks are the source of truth.

## First backup

After install, create a repository and start backing up a PVC. The smallest
end-to-end example is `deploy/examples/01-single-pvc-scheduled.yaml`:

```bash
kubectl apply -f deploy/examples/01-single-pvc-scheduled.yaml
kubectl get repositories,backupconfigs,backupschedules -n billing
```

Eight runnable walkthroughs live in `deploy/examples/`:

| File | Pattern |
|---|---|
| `01-single-pvc-scheduled.yaml` | Single PVC, scheduled daily |
| `02-cluster-repository.yaml` | Shared platform `ClusterRepository` (cluster scope) |
| `03-restore-by-backup.yaml` | Restore by picking a `Backup` |
| `04-multi-pvc-selector.yaml` | Multi-PVC label selector + group snapshot |
| `05-deploy-or-restore-gitops.yaml` | Deploy-or-restore (PVC `dataSourceRef`) |
| `06-manual-backup.yaml` | Manual one-shot `Backup` |
| `07-restore-discovered.yaml` | Restore a discovered / foreign snapshot |
| `08-maintenance.yaml` | `kopia maintenance` schedule + ownership lease |

## Observability

- Controller metrics are served on the controller's probe port (`/metrics`).
- `metrics.enabled=true` (default) creates a metrics `Service`.
- `metrics.serviceMonitor.enabled=true` creates a Prometheus-Operator
  `ServiceMonitor` (requires the Prometheus-Operator CRDs).

## Upgrade / uninstall

```bash
helm upgrade kopiur deploy/helm/kopiur -n kopiur-system   # re-applies CRD schema
helm uninstall kopiur -n kopiur-system                     # see CRD caution above
```

## See also

- Design: [`docs/adr/0003-kopiur-rust-operator.md`](adr/0003-kopiur-rust-operator.md)
- Chart values & modes: [`deploy/helm/kopiur/README.md`](../deploy/helm/kopiur/README.md)
