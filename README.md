# kopiur

**Kopiur** (Kopia + Rust) is a Kopia-native Kubernetes backup operator written in
Rust on [`kube-rs`](https://github.com/kube-rs/kube). It makes a kopia repository
a first-class Kubernetes resource and separates the backup **recipe** from its
**invocation** from its **schedule**, so backups can be triggered by cron,
`kubectl create`, Argo Events, or a Helm hook — and a kopia snapshot's lifecycle
is tied to its `Backup` CR by a finalizer + `deletionPolicy`. The whole CRD
surface is modeled as Rust enums so invalid states are unrepresentable and
reconcilers handle every variant at compile time. See
[ADR-0003](docs/adr/0003-kopiur-rust-operator.md) for the full design.

> Status: **alpha** — API group `kopiur.home-operations.com`, version `v1alpha1`. The CRD surface
> may still change between releases.

## The 7 CRDs (`kopiur.home-operations.com/v1alpha1`)

| CRD | Scope | Layer | Purpose |
|---|---|---|---|
| `Repository` | Namespaced | Storage | A kopia repository owned by one namespace: backend, encryption, credentials. |
| `ClusterRepository` | Cluster | Storage | A shared repository for platform teams, gated by `allowedNamespaces`. |
| `BackupConfig` | Namespaced | Recipe | *What* to back up: PVC sources, identity, retention, policy, hooks. Idempotent. |
| `Backup` | Namespaced | Invocation + Catalog | One kopia snapshot as a Kubernetes object. The universal trigger entry point. |
| `BackupSchedule` | Namespaced | Cron | *When* it runs: cron + jitter + timezone; creates `Backup` CRs. |
| `Restore` | Namespaced | Operation | Restore a snapshot to a PVC, or act as a passive volume-populator source. |
| `Maintenance` | Namespaced | Lifecycle | Schedules `kopia maintenance` quick + full with an ownership lease. |

## Quickstart

```bash
kubectl create namespace kopiur-system
helm install kopiur deploy/helm/kopiur \
  --namespace kopiur-system \
  --set webhook.certManager.enabled=true
kubectl get crd -l app.kubernetes.io/part-of=kopiur
```

Then apply a worked example:

```bash
kubectl apply -f deploy/examples/01-single-pvc-scheduled.yaml
```

Full install guide, prerequisites (k8s >= 1.24, optional cert-manager), install
modes, and the CRD-lifecycle caveat: **[docs/install.md](docs/install.md)**.

## Layout

```
crates/          Rust workspace (api, kopia, webhook, controller, mover, xtask)
deploy/crds/     Generated CRDs (cargo xtask gen-crds) — checked in
deploy/rbac/     Generated RBAC (cargo xtask gen-rbac) — checked in
deploy/helm/     Helm chart (deploy/helm/kopiur)
deploy/examples/ 8 runnable usage walkthroughs
docs/adr/        Architecture Decision Records (0003 is canonical)
```

## Documentation

📖 **Docs site: <https://kopiur.home-operations.com/>** — user guide, ADRs,
and the generated [Rust API reference](https://kopiur.home-operations.com/rustdoc/).

- [Install guide](docs/install.md)
- [Helm chart values & modes](deploy/helm/kopiur/README.md)
- [ADR-0003 — Kopiur, a Kopia-native backup operator in Rust](docs/adr/0003-kopiur-rust-operator.md)
- [Example manifests](deploy/examples/)

## License

[AGPL-3.0-only](LICENSE)

