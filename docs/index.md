# Kopiur

**Kopiur** (Kopia + Rust) is a Kopia-native Kubernetes backup operator written in Rust on [`kube-rs`](https://github.com/kube-rs/kube). It makes a kopia repository a first-class Kubernetes resource and separates the backup **recipe** from its **invocation** from its **schedule**, so backups can be triggered by cron, `kubectl create`, Argo Events, or a Helm hook — and a kopia snapshot's lifecycle is tied to its `Backup` CR by a finalizer + `deletionPolicy`.

The whole CRD surface is modeled as Rust enums so invalid states are unrepresentable and reconcilers handle every variant at compile time. For the high-level mental model start with [Concepts](concepts/how-kopia-works.md); see [ADR-0003](adr/0003-kopiur-rust-operator.md) for the full design.

/// warning | Alpha

API group `kopiur.home-operations.com`, version `v1alpha1`. The CRD surface may still change between releases.

///

## The 7 CRDs (`kopiur.home-operations.com/v1alpha1`)

| CRD                 | Scope      | Layer                | Purpose                                                                         |
| ------------------- | ---------- | -------------------- | ------------------------------------------------------------------------------- |
| `Repository`        | Namespaced | Storage              | A kopia repository owned by one namespace: backend, encryption, credentials.    |
| `ClusterRepository` | Cluster    | Storage              | A shared repository for platform teams, gated by `allowedNamespaces`.           |
| `BackupConfig`      | Namespaced | Recipe               | _What_ to back up: PVC sources, identity, retention, policy, hooks. Idempotent. |
| `Backup`            | Namespaced | Invocation + Catalog | One kopia snapshot as a Kubernetes object. The universal trigger entry point.   |
| `BackupSchedule`    | Namespaced | Cron                 | _When_ it runs: cron + jitter + timezone; creates `Backup` CRs.                 |
| `Restore`           | Namespaced | Operation            | Restore a snapshot to a PVC, or act as a passive volume-populator source.       |
| `Maintenance`       | Namespaced | Lifecycle            | Schedules `kopia maintenance` quick + full with an ownership lease.             |

## Where to next

- **[How Kopia works](concepts/how-kopia-works.md)** — content-addressable dedup, snapshots, the `username@hostname:path` identity model, encryption, maintenance — and why one shared repository is the recommended layout.
- **[Why Kopiur is designed this way](concepts/why-kopiur.md)** — the recipe/invocation/schedule split, repository-as-resource, the type-safety thesis, and snapshot-lifecycle-tied-to-CR.
- **[Getting started](getting-started.md)** — the end-to-end walkthrough: install, first backup, and a verified restore in ~15 minutes.
- **[Installation](install.md)** — prerequisites, install modes, and the CRD-lifecycle caveat.
- **[Repositories & backends](repositories.md)** — point Kopiur at S3, Azure, GCS, B2, a NAS, or rclone.
- **[Backups & schedules](backups.md)** and **[Restores](restores.md)** — the recipe/invocation/schedule model and reading data back.
- **[Troubleshooting](troubleshooting.md)** — when something doesn't go green.
- **[API reference (rustdoc)](api-reference.md)** — the generated Rust API docs for every crate in the workspace.
- **[API conventions](dev/api-conventions.md)** and **[Observability](dev/observability.md)** — developer notes.
- **[ADR-0003](adr/0003-kopiur-rust-operator.md)** — the canonical design document.
