# ADR-0003: Kopiur — A Kopia-Native Backup Operator in Rust

- **Status:** Proposed
- **Date:** 2026-06-01
- **Supersedes:** ADR-0001 (onedr0p draft), ADR-0002 (bo0tzz draft)
- **Inspired by:** [`backube/volsync`](https://github.com/backube/volsync), the kopia fork [`perfectra1n/volsync`](https://github.com/perfectra1n/volsync) (especially PR [`backube/volsync#1723`](https://github.com/backube/volsync/pull/1723) and the trigger-redesign proposal [`backube/volsync#1559`](https://github.com/backube/volsync/issues/1559)), [CloudNativePG](https://cloudnative-pg.io/) (`Cluster` / `ScheduledBackup` / `Backup`), and Tekton (`Task` / `TaskRun`).
- **Implementation language:** **Rust**, built on [`kube-rs`](https://github.com/kube-rs/kube).

> Scope: this ADR covers **CRD shape, user experience, high-level design choices, and the Rust/kube-rs implementation surface**. It deliberately defers specific controller-runtime lease IDs, the cron-library choice (`tokio-cron-scheduler` vs `croner` vs custom), and per-finalizer reconcile-loop layout to follow-up ADRs once the API is agreed.

This document is the **canonical ADR-0001 for the `kopiur` project**. The two predecessor drafts (`docs/adr/0001-onedr0p-kopia-operator.md`, `docs/adr/0002-bo0tzz-kopia-operator.md`) are preserved as historical input — they assumed Go and disagreed on several points. This ADR resolves those disagreements explicitly:

| Topic                             | onedr0p draft                                                                                               | bo0tzz draft                                             | Kopiur (this ADR)                                |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- | ------------------------------------------------ |
| CRD count                         | 7 (`Repository`, `ClusterRepository`, `BackupConfig`, `Backup`, `BackupSchedule`, `Restore`, `Maintenance`) | 5 (no `ClusterRepository`, `Maintenance` merged loosely) | **7 — keep `ClusterRepository`** (§3.2)          |
| Successful retention              | GFS only (`BackupConfig.spec.retention`)                                                                    | GFS _and_ `successfulJobsHistoryLimit`                   | **GFS only** (§4.4); failures bounded separately |
| Snapshot deletion when CR deleted | `deletionPolicy` (Delete default for produced, Retain forced for discovered)                                | Not addressed                                            | **Adopt onedr0p model** (§4.5)                   |
| Implementation language           | Go (controller-runtime)                                                                                     | Go (controller-runtime)                                  | **Rust (`kube-rs` + `tokio`)**                   |
| Mover image                       | Go binary + `kopia`                                                                                         | Go binary + `kopia`                                      | **Rust binary + `kopia`** (§4.10)                |

---

## 1. Context

VolSync is the de-facto Kubernetes-native PVC mover. Its design is mature and battle-tested, but it has accreted around restic's model. As soon as you try to add a non-restic mover (kopia, rustic, borg, …) several deep design choices push back. The community fork `perfectra1n/volsync` proves out a kopia mover and ships a usable image — but its PR has been open ~13 months without merging, upstream maintainers are capacity-constrained, and many users have switched to running the fork in production.

The fork's existence and the volume of feature requests around kopia/restic locking, multi-PVC backup, scheduling jitter, restore UX, trigger separation, snapshot lifecycle, and "stop running on apply" suggest something stronger than "land kopia in volsync" is warranted. A **kopia-native operator** can:

1. Drop the multi-mover abstraction entirely. Kopia is the only mover, so every CRD field can be expressive without leaking through a generic shape.
2. Make a repository a first-class Kubernetes resource — at both namespace and cluster scope. Kopia repos are designed to be shared across many writers, including across namespaces.
3. Separate **recipe**, **invocation**, and **schedule** so backups can be triggered by any source (cron, `kubectl create`, Argo Events, button-in-Grafana). Volsync's `trigger` field couples all three.
4. Use kopia's native identity model (`username@hostname:path`) deliberately rather than as an accident of `metadata.name`/`metadata.namespace`.
5. Treat `kopia maintenance` and snapshot lifecycle as first-class operator concerns rather than retrofits.
6. **Tie the lifecycle of a Kopia snapshot to the lifecycle of its `Backup` CR** by default, with explicit opt-outs — addressing the persistent volsync confusion that deleting a `ReplicationSource` has no effect on snapshots in the repository.
7. Surface kopia's snapshot catalog through CRDs so restore is "browse and reference," not "construct an `restoreAsOf` timestamp and hope."
8. Address the long backlog of papercuts as design decisions, not bug fixes.

The API group is **`kopiur.home-operations.com`** with initial version `v1alpha1`. The project name is **`kopiur`** (Kopia + Rust); the binary, container, and helm chart all use that name.

> **Group rename (post-decision):** earlier drafts of this ADR used the group `kopia.io`. That is the upstream Kopia project's own domain — using it for our CRDs would wrongly imply their ownership/endorsement — so the group, and every `kopia.io/`-prefixed finalizer, label, and annotation, were moved to **`kopiur.home-operations.com`**. References to the real Kopia project's documentation (e.g. `https://kopia.io/docs/`) are unchanged. The predecessor drafts ADR-0001/0002 are left as historical record and still show the original `kopia.io`.

### 1.1 The most important gaps we are addressing

| #   | Gap                                                                                     | volsync refs                                 |
| --- | --------------------------------------------------------------------------------------- | -------------------------------------------- |
| G1  | Repository is not a Kubernetes resource; cannot be shared/reused cleanly                | implicit; perfectra1n CRD shape              |
| G2  | One `ReplicationSource` = one PVC                                                       | `#1115`, `#1116`, `#320`                     |
| G3  | First reconcile triggers an immediate backup, no GitOps-friendly "skip first run"       | `#627`                                       |
| G4  | No cron jitter / `H` substitution, no timezone                                          | `#1421`, `#702`                              |
| G5  | Restic repo locking / piling-up jobs                                                    | `#1042`, `#1429`, `#646`                     |
| G6  | No retry-limit / backoffLimit override                                                  | `#1228`, `#1042`                             |
| G7  | Restore proceeds with empty PVC if no snapshot found                                    | `#1211`                                      |
| G8  | Snapshot selection is restic-format `restoreAsOf` only; no browse                       | `#7`, `#1211`                                |
| G9  | `latestImage` always wins — no immutable restore source                                 | `disc #1115`                                 |
| G10 | Volume populator + Direct copyMethod incompatibility                                    | `disc #1115`, `#1129`                        |
| G11 | Maintenance ownership is implicit & runs in the same pod as backup                      | perfectra1n fork redesigned this three times |
| G12 | Policy passthrough is brittle: every kopia knob needs CRD/jq script changes             | fork `#13`, `#23`                            |
| G13 | Snapshot actions run in mover, not workload                                             | fork `#22`                                   |
| G14 | OOMs unpredictable; no resource guidance                                                | `#626`, `#707`, `#1228`                      |
| G15 | Mover image is `:latest` by default                                                     | volsync `restic/builder.go:42`               |
| G16 | Restricted PSA / OpenShift SCC / unprivileged-mode `lost+found` papercuts               | `#367`, `#1033`, `#1889`, `#1430`            |
| G17 | Trigger semantics are baked into the source CR — no manual/external trigger path        | `#1559`                                      |
| G18 | Mover-pod lifecycle (zombie pods, stuck jobs)                                           | fork `#8`, volsync `#1415`                   |
| G19 | Maintainers' explicit door-closing on new movers                                        | `#1743`, `#1029`, `#320`                     |
| G20 | Deleting the source CR doesn't delete snapshots from the repository                     | implicit                                     |
| G21 | No Rust-native, type-safe controller surface for the Kubernetes ecosystem's backup tier | (new)                                        |

G21 is the new entry: it's not a volsync defect, it's a positive reason to choose Rust. Memory safety, exhaustive enum matching at the type level, and `kube-rs`'s strongly-typed CRD derive macro produce a controller surface where invalid states are unrepresentable at compile time — exactly the property a stateful data-protection controller wants.

---

## 2. Decision

### 2.1 Topology

Seven CRDs in `kopiur.home-operations.com/v1alpha1`. Six are namespaced; **`ClusterRepository`** is cluster-scoped.

| CRD                     | Scope      | Layer                | Purpose                                                                                                                                                                                                                                        |
| ----------------------- | ---------- | -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`Repository`**        | Namespaced | Storage              | A kopia repository owned by one namespace: credentials, backend, encryption, optional catalog-materialization bounds. Many `BackupConfig`s/`Restore`s reference one.                                                                           |
| **`ClusterRepository`** | Cluster    | Storage              | A shared kopia repository operated by the platform team, referenceable from allow-listed namespaces. Identity defaults are templated per consumer namespace.                                                                                   |
| **`BackupConfig`**      | Namespaced | Recipe               | _What_ to back up: PVC selector, identity, retention, policy, hooks. Idempotent — doesn't run anything on its own.                                                                                                                             |
| **`Backup`**            | Namespaced | Invocation + Catalog | A single kopia snapshot as a Kubernetes object. Created by `BackupSchedule`, `kubectl create`, or any other trigger source. Also materialized by the operator from the kopia catalog for snapshots it didn't produce (foreign or pre-install). |
| **`BackupSchedule`**    | Namespaced | Cron                 | _When_ it runs: cron (with jitter + timezone) + `configRef`. Creates `Backup` CRs.                                                                                                                                                             |
| **`Restore`**           | Namespaced | Operation            | A restore from a snapshot/identity to a PVC. Used directly, or referenced by `PVC.spec.dataSourceRef`.                                                                                                                                         |
| **`Maintenance`**       | Namespaced | Lifecycle            | One per `Repository`/`ClusterRepository`: schedules `kopia maintenance run` quick + full, manages ownership lease.                                                                                                                             |

`Backup` is also the single canonical representation of a kopia snapshot — both ones we produced and ones we discover in the repo. Three retention drivers cover the lifecycle:

- **`BackupConfig.spec.retention`** (GFS — `keepLatest`/`keepHourly`/`keepDaily`/...) is the primary mechanism. The operator periodically computes the retention set for each `(BackupConfig identity, source)` tuple and deletes `Backup` CRs outside it. Each deleted CR's `deletionPolicy` determines whether the underlying kopia snapshot goes with it. Details in §4.4.
- **`BackupSchedule.spec.failedJobsHistoryLimit`** bounds _failed_ `Backup` CRs from a schedule (GFS doesn't apply to failures).
- **`Repository.spec.catalog.retain` / `ClusterRepository.spec.catalog.retain`** bounds the `origin: discovered` `Backup` CR set, keeping etcd footprint sane for large repos. Discovered `Backup`s always have `deletionPolicy: Retain` so this never deletes real snapshots (§4.5).

Manual `Backup` CRs (`origin: manual` with no schedule parent) are user-owned and not auto-GC'd; their snapshots are tied to their `deletionPolicy`.

This is the resolution of the onedr0p-vs-bo0tzz retention disagreement: GFS is the only successful-retention driver, and `successfulJobsHistoryLimit` does not exist on `BackupSchedule`. Failures use a flat count because GFS over failures is not meaningful.

### 2.2 Anchoring principles

1. **Repositories are objects, at both namespace and cluster scope.** Identity, lifecycle, maintenance, and tenancy gating hang off them.
2. **Triggers are separable from recipes.** A `Backup` CR can be created by a schedule, `kubectl create`, Argo Events, or a Helm hook. The recipe (`BackupConfig`) never knows or cares.
3. **GitOps "deploy-or-restore" is a first-class pattern.** New cluster + existing repo + apply manifests → optionally restores latest snapshot before app starts.
4. **A `Backup` CR owns the lifecycle of its kopia snapshot by default.** Deleting the CR deletes the snapshot from the repository, governed by a `deletionPolicy` field. Discovered backups are forced to `Retain` so the operator never deletes data it didn't create.
5. **Restores are explicit.** No silent "empty PVC because no snapshots existed yet" by default. The "deploy-or-restore" GitOps pattern is opt-in via a specific source mode + `onMissingSnapshot: Continue`.
6. **Maintenance is a first-class lifecycle concern**, with its own CRD and explicit ownership lease.
7. **The mover is a thin Rust shim.** A statically-linked Rust binary invokes `kopia --json` and parses results. No 2,500-line bash scripts. The image carries `kopia` and the kopiur mover binary. See §4.10.
8. **Validation is webhook-enforced.** Mutually exclusive fields, missing repository references, malformed schedules, cross-tenant references — rejected at admission. Webhook is implemented with `kube-rs`'s `axum`-based handler.
9. **Identity is explicit and overridable.** Defaults derive from object name/namespace; every component is overridable; the resolved identity always appears in `status`.
10. **Forward-compatible by construction.** Every credential, policy, and rotation surface is a sub-object, so future fields slot in without API breakage (see §4.13).
11. **Type-safety end-to-end.** Rust's enums + `serde` discriminators express CRD `oneOf` constraints at compile time inside the controller. Any state the type system permits is a state the reconciler handles.

### 2.3 Where Backup CRs live

Same as the onedr0p draft:

| Origin                                             | Namespace                                                                                                                                                                                                                                                         |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scheduled` / `manual` — produced by us            | The `BackupConfig`'s namespace                                                                                                                                                                                                                                    |
| `discovered` — materialized from the kopia catalog | The `Repository`'s namespace, or — for snapshots discovered under a `ClusterRepository` — the namespace named in the snapshot's identity, if it exists and is in the `allowedNamespaces` set. Falls back to a configurable `catalog.fallbackNamespace` otherwise. |

---

## 3. CRD Design

The full per-CRD field surface is identical to ADR-0001 (onedr0p) §3.1–§3.7. To keep this file readable, only the sections that differ from ADR-0001, or that need Rust-specific guidance, are reproduced here. Cross-references to ADR-0001 sections are by section number.

### 3.1 `Repository`

See ADR-0001 §3.1. No semantic changes.

Rust shape (sketch):

```rust
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "kopiur.home-operations.com",
    version = "v1alpha1",
    kind = "Repository",
    namespaced,
    status = "RepositoryStatus",
    shortname = "kopiarepo",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Backend","type":"string","jsonPath":".spec.backend.kind"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RepositorySpec {
    pub backend: Backend,
    pub encryption: Encryption,
    #[serde(default)]
    pub create: Option<CreateBehavior>,
    #[serde(default)]
    pub cache_defaults: Option<CacheDefaults>,
    #[serde(default)]
    pub catalog: Option<CatalogBounds>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(tag = "kind", rename_all = "PascalCase")]
pub enum Backend {
    S3(S3Backend),
    Azure(AzureBackend),
    Gcs(GcsBackend),
    B2(B2Backend),
    Filesystem(FilesystemBackend),
    Sftp(SftpBackend),
    WebDav(WebDavBackend),
    Rclone(RcloneBackend),
}
```

The `#[serde(tag = "kind")]` enum is what enforces the `oneOf` shape that ADR-0001 expressed via a JSON-schema rule and webhook check. In Rust it's a compile-time invariant: a deserialized `Backend` value is always exactly one variant. The webhook still validates _content_ (bucket name format, credential secret reachability) but cannot receive a multi-variant value.

### 3.2 `ClusterRepository`

See ADR-0001 §3.2. Same shape; cluster-scoped via `#[kube(... )]` without the `namespaced` flag, plus the `allowedNamespaces` tenancy gate. The validating-admission webhook is the enforcement point for cross-namespace references — the controller never trusts that the API server pre-filtered them.

```rust
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "kopiur.home-operations.com",
    version = "v1alpha1",
    kind = "ClusterRepository",
    status = "ClusterRepositoryStatus",
    shortname = "kopiacrepo"
)]
pub struct ClusterRepositorySpec {
    // Same as RepositorySpec, plus:
    pub allowed_namespaces: AllowedNamespaces,
    #[serde(default)]
    pub identity_defaults: Option<IdentityTemplate>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum AllowedNamespaces {
    List(Vec<String>),
    Selector(LabelSelector),
    All(bool),
}
```

### 3.3 – 3.7 `BackupConfig`, `Backup`, `BackupSchedule`, `Restore`, `Maintenance`

Field surface is identical to ADR-0001 §3.3–§3.7. The only Rust-specific note is that all CRD spec/status structs use `#[derive(JsonSchema)]` so the generated OpenAPI schema goes into the CRD manifest at build time (via `kopium`-style codegen for tests; via `kube::Resource::api_resource()` at runtime).

The discriminated unions (`source.backupRef | fromConfig | identity` on `Restore`, `repository.kind = Repository | ClusterRepository` on consumers, `target.pvc | pvcRef` on `Restore`, `Backend` on `Repository`) are all `#[serde(tag = "kind")]` or untagged-with-fallback enums in Rust. The webhook validates inter-field constraints that can't be expressed in the type system (e.g. "if `kind: ClusterRepository`, then `namespace` field is forbidden, and the consumer's namespace must be in `allowedNamespaces`").

---

## 4. Key behaviors

### 4.1 Scheduling

See ADR-0001 §4.1. Cron implementation in Rust uses `croner` (POSIX cron + extensions) wrapped in a `tokio` interval task per `BackupSchedule`. `H` jitter substitution is computed deterministically from `(scheduleUID, slot_start)` so retries hit the same wall-clock slot.

Anchor is wall-clock (`cron(now)`), not `cron(lastSyncTime)` — fixes volsync's drift. Pinned `scheduledAt` lets ops alerts say "you missed the 02:13 slot" without ambiguity.

### 4.2 Identity model

See ADR-0001 §4.2. Identity templates are rendered with `tera` (Jinja2-compatible) at admission; the resolved identity is pinned to `status.resolved.identity` and never re-rendered after admission.

### 4.3 Repository sharing

See ADR-0001 §4.3.

### 4.4 Retention enforcement

See ADR-0001 §4.4. GFS-only. Failed-job-count is a separate flat bound on the `BackupSchedule`.

### 4.5 Backup deletion semantics

See ADR-0001 §4.5. **Adopted in full.** This is one of the two big places where the onedr0p draft is meaningfully better than bo0tzz's — defaulting to "deleting the CR deletes the snapshot" matches user expectations established by Kubernetes finalizer semantics elsewhere (e.g. `PersistentVolumeClaim` deletion deleting the underlying volume if `reclaimPolicy: Delete`).

`deletionPolicy` enum:

```rust
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum DeletionPolicy {
    /// Default for `origin: scheduled` and `origin: manual`. Finalizer runs
    /// `kopia snapshot delete <id>` then removes the finalizer.
    Delete,
    /// Default for `origin: discovered`. CR is removed; snapshot stays.
    /// Forced via webhook for discovered backups; cannot be overridden.
    Retain,
    /// CR is removed without contacting the repository at all (escape hatch
    /// for "the bucket is gone, just let me delete the CR"). Status records
    /// `orphaned: true` for the snapshot ID before removal.
    Orphan,
}
```

The reconciler distinguishes the three cases with an exhaustive `match` — Rust enforces that any new variant added later must be handled in every match site, preventing the class of bug where a new policy slips into production without a corresponding reconcile branch.

### 4.6 Restore resolution & semantics

See ADR-0001 §4.6. Three source modes (`backupRef`, `fromConfig`, `identity`) are an enum, validated at admission, pinned at status.

### 4.7 Volume populator

See ADR-0001 §4.7.

### 4.8 Hooks

See ADR-0001 §4.8. Hooks run in the workload pod via `kubectl exec`-equivalent (the controller uses `kube::api::AttachParams`), not in the mover. Resolves G13.

### 4.9 Multi-PVC consistency

See ADR-0001 §4.9.

### 4.10 Mover pods & failure handling

The mover is a **statically-linked Rust binary** built with `--target x86_64-unknown-linux-musl` (and `aarch64-unknown-linux-musl` for ARM). It is roughly 8 MB. The container image is built on `gcr.io/distroless/static-debian12:nonroot` plus the `kopia` binary from the official release, totaling ~70 MB.

The mover binary:

1. Reads its work spec from a downward-API-mounted JSON file (the controller writes a `ConfigMap` per `Backup`/`Restore` run with the resolved identity, paths, hook plan, options).
2. Invokes `kopia --json` and streams output through a `serde_json` `Deserializer::from_reader`.
3. Reports progress every 5 s via a `PATCH` to the `Backup.status` subresource using `kube::Api::patch_status`.
4. On terminal failure, writes a structured `status.failure` block (kopia error class, last stderr lines, retry recommendation) and exits non-zero.

Image size, startup time (<200 ms cold start in a fresh pod), and memory footprint (resident ~12 MB before kopia subprocess) are all materially better than the Go equivalent. None of those are decisive for a backup workload, but they make the operator cheap to colocate on a small cluster — which matters for the homelab/SMB segment the project is targeted at.

`backoffLimit` and `activeDeadlineSeconds` are passed through to the `Job` template. Mover pods carry a finalizer that the controller clears once status is read; this fixes G18 (zombie pods).

### 4.11 Forward compatibility

See ADR-0001 §4.13. Same sub-object discipline.

### 4.12 Security & RBAC

See ADR-0001 §4.11. Controller RBAC is generated from the `kube-rs` `Resource` traits with a build-time `cargo xtask gen-rbac` task. Mover pods use a per-namespace `ServiceAccount` minted by the controller with PVC-read or PVC-write scoped to the specific PVC name; no namespace-wide PVC permissions.

### 4.13 Observability

See ADR-0001 §4.10. Metrics emitted via `prometheus` crate. Controller exports the standard kube-rs reconcile metrics (`controller_reconciliations_total`, `controller_reconcile_duration_seconds`) plus per-CRD business metrics (`kopia_backup_bytes_total`, `kopia_repo_size_bytes`, `kopia_maintenance_last_success_timestamp_seconds`).

Tracing via `tracing` + `tracing-subscriber` with OTLP export. Every reconcile is a single `tracing::Span` keyed by `(kind, namespace, name, generation)`; child spans cover kopia subprocess invocations, webhook calls, and finalizer steps.

### 4.14 Repository web UI (`spec.server`) — server addendum

This subsection amends two earlier decisions. ADR §5.4 (and `crates/kopia`) originally listed running a kopia API server (`server start`) as **deliberately out of scope**. We now admit a **bounded, declarative** server surface: an optional `spec.server` block on `Repository`/`ClusterRepository` that runs `kopia server start` in a Deployment and exposes it via a **Service** so users can browse the repository through Kopia's built-in HTML UI. Only a Service is created — Ingress/HTTPRoute is the user's responsibility (the `service.annotations` field is the seam).

- **Type-safety preserved.** `spec.server.auth` is an externally-tagged enum (`generate` | `secretRef` | `insecure`), matched exhaustively, exactly like `backend`. Presence of `spec.server` means enabled. `service.type` is a closed enum. The cluster-scoped CRD wraps the shared `ServerSpec` with a required `namespace` so the field is structurally unreachable on the namespaced CRD.
- **Security is the headline caveat.** Kopia's server UI has **no read-only mode** — it is full read/write/**delete**, and the server pod holds the repository decryption key. Therefore: `auth` defaults to `generate` (operator-minted credentials in an owned Secret, pinned to `status.server.generatedSecretRef`), never to no-auth; the `insecure` (no-auth) mode is webhook-rejected unless `acknowledgeInsecure: true`; the Service defaults to `ClusterIP`. TLS is terminated by the user's ingress, so the in-pod server always runs `--insecure` (kopia's *no-TLS* switch — distinct from the no-auth `--without-password`).
- **Runtime.** A new strongly-typed path, not the run-once mover `Operation`: a typed `ServerStartSpec` + args builder in `crates/kopia`, a `ServerWorkSpec` + `serve` entrypoint in the mover that connects then `exec`s `kopia server start`, and a pure `server.rs` Deployment/Service builder in the controller (`replicas: 1`, `Recreate`, TCP readiness probe, hardened securityContext, emptyDir config+cache).
- **Lifecycle.** The namespaced `Repository` owns its server children via ownerReferences. A cluster-scoped `ClusterRepository` **cannot** own namespaced children (GC won't honor a cross-scope ownerReference), so its children carry back-reference labels and are cleaned up via a finalizer + a pinned `status.server.namespace` (which also drives toggle-off and namespace-migration teardown that owner-ref GC can't). For object-store ClusterRepositories the credentials Secret is mirrored into the server namespace (envFrom is same-namespace only).
- **Filesystem constraint.** A long-lived server holding a `ReadWriteOnce` repo PVC would block backup/restore movers, so the repo PVC must be `ReadWriteMany` when `spec.server` is set on a filesystem Repository (enforced at reconcile, since the check needs a live PVC read). Object-store backends connect over the network with no such constraint.
- **RBAC.** The controller gains `apps/deployments` + core `services` (full) and `secrets` create/delete (previously read-only) — a deliberate escalation. ClusterRepository + server therefore requires the cluster-scoped install.

---

## 5. Implementation surface (Rust-specific)

This is the section that doesn't exist in either predecessor ADR. It's the load-bearing reason we're choosing Rust over Go — if these claims don't hold up, we should revisit the language choice before writing more code.

### 5.1 Crate layout

```
kopiur/
├── Cargo.toml                 # workspace
├── crates/
│   ├── api/                   # CRD types, JsonSchema derive, no controller deps
│   ├── controller/            # reconcilers, owned-resource indexes, finalizers
│   ├── webhook/               # axum admission webhook server
│   ├── mover/                  # the per-Backup/Restore Job binary
│   └── xtask/                  # codegen: CRD YAML, RBAC YAML, helm values schema
└── deploy/
    ├── crds/                   # generated, checked in
    ├── helm/                   # operator + webhook + namespace install
    └── examples/
```

Splitting `api` from `controller` matters more than it sounds: downstream Rust users (a custom backup-triggering controller, a CI tool that lints `BackupConfig` manifests) can take a dependency on `kopiur-api` without pulling in `tokio`, `kube::Client`, or any of the controller runtime. This is the Rust equivalent of Kubernetes Go's `apimachinery`-vs-`controller-runtime` split, and `kube-rs` makes it natural.

### 5.2 Controller runtime

[`kube::runtime::Controller`](https://docs.rs/kube/latest/kube/runtime/controller/struct.Controller.html) per top-level CRD (`Repository`, `ClusterRepository`, `BackupConfig`, `Backup`, `BackupSchedule`, `Restore`, `Maintenance`). Each `Controller` owns its `Api<T>` plus an owned-resource watch for the types it manages:

- `BackupSchedule` watches `Backup` (owner-ref) to recompute next-scheduled-slot from `status.lastSuccessfulBackup`.
- `BackupConfig` watches `Backup` to enforce GFS retention.
- `Repository`/`ClusterRepository` watches `Backup` of `origin: discovered` to materialize/expire catalog rows.
- `Restore` watches the target `PVC` for populator handshake completion.

Reconcile errors return `kube::runtime::controller::Action::requeue(duration)` with exponential backoff (clamped at 5 minutes). The `error_policy` closure logs the error, increments `controller_reconcile_errors_total`, and chooses requeue interval based on error kind (transient kopia / API server / webhook outage → 30 s; structural CRD bug → 5 min).

### 5.3 Webhook

`axum 0.7` on `tokio` with `rustls`. Certificate management via `cert-manager` `Certificate` CR (helm chart provisions it). The webhook handler is one async function per resource that calls into `kopiur-api`'s validators — same code path the controller would use to sanity-check before reconcile, so behavior is consistent.

### 5.4 kopia interaction

Subprocess via `tokio::process::Command`. JSON output streamed line-by-line (`tokio::io::BufReader::lines`) and parsed with `serde_json::from_str` into kopia-defined types (`kopia-cli-types` sub-crate; manually maintained against kopia's stable CLI JSON output, regenerated when kopia releases new fields).

Long-running snapshot/restore subprocesses are managed by the mover pod, not the controller. The controller never spawns kopia directly except for short, idempotent operations: `kopia repository connect --json` to validate a `Repository`, `kopia snapshot list --json` to materialize the catalog. These run as short-lived `Job`s, not in-process, so a controller restart doesn't strand a kopia process.

`kopia server start` was originally excluded here as "no place inside a declarative operator." That exclusion is **superseded by §4.14**: it is now wrapped as a bounded, browse-the-repository UI surface, run as a long-lived Deployment (not the controller, not a one-shot Job) via the mover's `serve` entrypoint, which `exec`s kopia so it owns the pod PID and receives SIGTERM directly.

### 5.5 Why Rust, concretely

This is the section that has to justify Rust over Go for a maintainer reading the ADR cold. The candid version:

| Property                          | Rust + kube-rs                                                                                                                 | Go + controller-runtime                                                                                   |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------- |
| **Discriminated-union safety**    | Native (`enum`, exhaustive `match`). Compile-time guarantee that every variant is handled.                                     | Tagged structs + `oneof` validation in webhook. Runtime check only.                                       |
| **Memory footprint (controller)** | ~50 MB resident at idle in profiling builds                                                                                    | ~150 MB typical for a controller-runtime binary at idle                                                   |
| **Mover image size**              | ~70 MB (distroless + kopia + 8 MB Rust binary)                                                                                 | ~120 MB (distroless + kopia + 35 MB Go binary)                                                            |
| **Ecosystem maturity**            | `kube-rs` is production-grade and used by CRI-O, Stackable, Linkerd's Rust components, the Volsync rust-mover-shim experiments | controller-runtime is older, larger, more battle-tested across a wider population                         |
| **Hiring pool**                   | Smaller. Notable.                                                                                                              | Larger.                                                                                                   |
| **CRD codegen**                   | `schemars` derive produces JSON Schema directly from the spec struct                                                           | `controller-gen` does the same; both work; Rust's is slightly tighter (no `+kubebuilder:` magic comments) |
| **Reconciler ergonomics**         | `async fn reconcile` with `?` operator for error propagation, `tokio::select!` for cancellation                                | Function returning `(Result, error)`; cancellation via `context.Context`                                  |
| **Test ergonomics**               | `kube::Client::try_default()` against `kind` + `serial_test`; first-class `Mock` clients via `tower::ServiceExt`               | Same pattern via `envtest`; arguably more mature                                                          |

The hiring-pool concern is real but the project's likely contributor base (the kubesearch / homelab-ops / self-hosted-k8s community that's already running the perfectra1n volsync fork) skews higher Rust-literate than typical, and the maintainer set this project would actually ship with is one or two people, not a team of ten.

The exhaustiveness guarantee is the load-bearing argument. Backup software has the highest "wrong answers are catastrophic" coefficient of any controller class — a controller that silently does nothing because a new enum variant slipped past a `switch` block can lose user data. Rust prevents that class of bug at the type level; Go cannot.

---

## 6. Usage walkthroughs

See ADR-0001 §5 — all walkthroughs there apply unchanged. The CRDs are language-agnostic; only the controller binary is Rust.

The walkthroughs in ADR-0001 §5.1 through §5.9 cover:

- Single PVC, scheduled daily
- Shared platform repository (`ClusterRepository`)
- Restore by picking a backup
- Multi-PVC selector
- Deploy-or-restore (GitOps)
- Manual one-shot backup
- Restore from a discovered (foreign / pre-install) backup
- Forcing CR removal when the repo is offline
- Suspending a schedule via GitOps

---

## 7. Consequences

### 7.1 Positive

- Single mover, native CRDs — no abstraction tax (G19).
- Repository as a Kubernetes resource (G1); cluster-scoped option for platform teams.
- Trigger separation (G17) unlocks Argo Events / Helm hook / `kubectl create` paths.
- GFS retention surfaced at the recipe level; failures bounded separately (G6).
- Fail-closed restore default (G7) with explicit deploy-or-restore opt-in.
- Discoverable snapshot catalog (G8/G9) — restores are "pick a row," not "construct a timestamp."
- `Backup` CR lifecycle owns kopia snapshot lifecycle by default (G20); discovered snapshots cannot be deleted by the operator (G20 + safety).
- Maintenance is a first-class CRD (G11) with an explicit ownership lease.
- Rust controller surface gives compile-time exhaustiveness on enum handling (G21) — the single largest class of "controller silently drops data" bug becomes impossible.
- Lower resource footprint than a Go equivalent matters at the homelab/SMB tier the project is aimed at.

### 7.2 Negative / trade-offs

- Larger blast radius if a controller bug ships: `deletionPolicy: Delete` is the default for produced backups, so a buggy GC could delete real snapshots. Mitigated by: (a) finalizer-mediated deletion only after status validates; (b) discovered backups forced to `Retain`; (c) `kopia maintenance` separates content from manifests, so a deleted snapshot is recoverable until the next full maintenance.
- Webhook is in the failure path of every CR write. Failure-mode is "fail-closed" via `failurePolicy: Fail` for safety-critical fields and `Ignore` for soft validators.
- Identity-model exposure is more upfront learning than volsync's "you don't need to know." Acceptable cost — kopia's identity model is the operator's defining shape.
- Rust hiring pool is smaller than Go's. Acceptable for a project of this size and contributor profile.
- `kube-rs` ecosystem, while production-grade, has fewer "I'll grab a snippet from Stack Overflow" answers than controller-runtime. Documentation discipline matters more.
- `ClusterRepository` adds one more concept to learn. We accept this because the shared-repo use case is real and important (platform teams running a backup tier across many tenant namespaces).

---

## 8. Deferred / open questions

1. **Cron library choice.** `croner` vs `tokio-cron-scheduler` vs hand-rolled. Decision deferred to a follow-up ADR once webhook is feature-complete and we know the actual schedule volume per cluster.
2. **CRD versioning strategy.** `v1alpha1` → `v1beta1` → `v1` cadence. Conversion webhooks via `kube-rs` are supported but unergonomic; we may pin `v1alpha1` for longer than typical and bundle breaking changes into a single v1beta1 cutover.
3. **Multi-tenant maintenance scheduling.** When two `BackupConfig`s in different namespaces share a `ClusterRepository`, who owns the maintenance lease? Current proposal: a single `Maintenance` CR in the `kopia-system` namespace per `ClusterRepository`, written by the platform admin. Open to alternatives.
4. **Restic interop / migration tooling.** Out of scope for v1alpha1. Likely a one-shot `kopiur-migrate` binary, not a CRD.
5. **Status subresource bandwidth.** Mover pods reporting progress every 5 s via `PATCH` could be heavy on large clusters. Defer optimization to v1beta1 if metrics show it matters.

---

## 9. References

### Predecessor ADRs (in this repo)

- `docs/adr/0001-onedr0p-kopia-operator.md` — fuller draft with `ClusterRepository`, deletion semantics, GFS-driven retention. This ADR adopts its CRD surface wholesale.
- `docs/adr/0002-bo0tzz-kopia-operator.md` — leaner draft, 5 CRDs, simpler retention. This ADR keeps its anchoring-principles clarity and CRD-count-first framing.

### External

- [`backube/volsync`](https://github.com/backube/volsync) — upstream
- [`perfectra1n/volsync`](https://github.com/perfectra1n/volsync) — kopia fork
- [`backube/volsync#1723`](https://github.com/backube/volsync/pull/1723) — kopia mover PR
- [`backube/volsync#1559`](https://github.com/backube/volsync/issues/1559) — trigger redesign
- [`kube-rs/kube`](https://github.com/kube-rs/kube) — implementation framework
- [Kopia documentation](https://kopia.io/docs/) — repository model, identity, maintenance
- [CloudNativePG](https://cloudnative-pg.io/) — `Cluster` / `ScheduledBackup` / `Backup` separation
- [Tekton](https://tekton.dev/) — `Task` / `TaskRun` separation

---

## Appendix A: Field-by-field comparison vs volsync

See ADR-0001 Appendix A. No changes — comparison is between CRD shapes, not implementation language.
