# kopiur-api

Strongly-typed CRD definitions and shared, controller-free logic for **Kopiur**,
the Kopia-native Kubernetes backup operator ([ADR-0003]).

## Role in the workspace

This crate holds the **7 CRD types** in API group `kopiur.home-operations.com`,
version `v1alpha1` — [`Repository`] (ns), [`ClusterRepository`] (cluster),
[`BackupConfig`], [`Backup`], [`BackupSchedule`], [`Restore`], and
[`Maintenance`] — together with the **shared pure logic** every consumer needs:
validation ([`validate`]), identity resolution ([`resolve_identity`]), schedule
jitter ([`jitter`]), and GFS retention ([`select_kept`]).

It deliberately has **no controller-runtime dependencies** — no `kube::Client`,
no `tokio`. Downstream tools (a custom backup-triggering controller, a CI linter
for `BackupConfig` manifests, a dashboard) can depend on the API types and shared
logic *alone*, without pulling in the async runtime or the cluster client
(ADR §5.1). The webhook and the controller both import the same `validate`/
`identity`/`retention` functions, so validation and resolution behave identically
across call sites ("one validator, two callers").

## The load-bearing idea: type-safety end-to-end (ADR §5.5)

Every discriminated union in the CRD surface is a Rust `enum`:
[`Backend`], [`AllowedNamespaces`], [`DeletionPolicy`],
[`RestoreSource`], [`Hook`], and friends.

A deserialized value is always *exactly one* variant — an invalid "two backends
at once" or "no backend" state is unrepresentable — and reconcilers `match`
exhaustively. A new variant added later **cannot compile** until every handler
accounts for it. For backup software, where a silently-unhandled case can lose
user data, this eliminates the highest-severity class of "controller silently
dropped data" bugs. This is the whole reason Kopiur is Rust and not Go; preserve
this property in every change (prefer an `enum` + exhaustive `match` over
`if let` / `_ =>` catch-alls).

## Key types

| Type | Purpose |
| --- | --- |
| [`Repository`] / [`ClusterRepository`] | The kopia repository as a first-class resource (namespaced / cluster-scoped). |
| [`Backend`] | The storage backend union (`s3`, `azure`, `gcs`, `b2`, `filesystem`, `sftp`, `webDav`, `rclone`). |
| [`BackupConfig`] | The backup *recipe* (sources, retention, hooks). |
| [`Backup`] | A single backup *invocation*, owning its snapshot via finalizer. |
| [`BackupSchedule`] | The *schedule* that emits `Backup`s on a cron. |
| [`Restore`] | A restore request ([`RestoreSource`] / [`RestoreTarget`]). |
| [`Maintenance`] | `kopia maintenance` as a first-class, default-managed concern. |
| [`DeletionPolicy`] | `Delete` / `Retain` / `Orphan` — ties snapshot lifecycle to the CR. |

Shared pure logic (no controller deps):

| Function | Purpose |
| --- | --- |
| [`resolve_identity`] | Render the kopia `username@hostname:path` identity, pinned to status at admission. |
| [`select_kept`] | GFS retention: decide which backups to keep. |
| [`jitter_offset`] / [`substitute_h`] | Deterministic `H`/jitter from `(scheduleUID, slot)`. |

## Conventions

Before editing this crate, read [`docs/dev/api-conventions.md`]. The load-bearing
rules:

- **Discriminated unions are externally-tagged enums** (`backend: { s3: {...} }`),
  *not* `#[serde(tag = "...")]` — internally-tagged enums break Kubernetes
  structural-schema generation.
- **No `Eq`** on structs that embed `k8s-openapi` types (`LabelSelector`,
  `ResourceRequirements`, `SecurityContext`, …) — they are `PartialEq` only.
- **Sub-objects, not leaf fields**, for every credential/policy/identity/schedule
  surface, so future fields slot in without API breakage.

## Usage

Construct or deserialize a [`Backend`] the way the API server does (JSON value →
typed) and `match` it exhaustively:

```rust
use kopiur_api::Backend;

// External tagging: the variant key selects exactly one backend.
let backend: Backend = serde_json::from_value(serde_json::json!({
    "s3": { "bucket": "my-backups", "region": "us-east-1" }
}))
.unwrap();

// A deserialized Backend is always exactly one variant -> exhaustive match.
let summary = match &backend {
    Backend::S3(s3) => format!("s3://{}", s3.bucket),
    Backend::Azure(_) => "azure".into(),
    Backend::Gcs(_) => "gcs".into(),
    Backend::B2(_) => "b2".into(),
    Backend::Filesystem(_) => "filesystem".into(),
    Backend::Sftp(_) => "sftp".into(),
    Backend::WebDav(_) => "webdav".into(),
    Backend::Rclone(_) => "rclone".into(),
};
assert_eq!(summary, "s3://my-backups");

// The stable discriminant is independent of the camelCase wire key.
assert_eq!(backend.kind_str(), "S3");
```

> Note: deserialize via `serde_json` (the API-server path), never `serde_yaml`
> directly into a typed value — serde_yaml 0.9 mis-encodes externally-tagged
> enums. For YAML tests, go YAML → `serde_json::Value` → typed.

## See also

- [ADR-0003] — the canonical source of truth for the CRD surface, UX, and design.
- [`docs/dev/api-conventions.md`] — how to encode the ADR's fields in Rust.

[ADR-0003]: ../../docs/adr/0003-kopiur-rust-operator.md
[`docs/dev/api-conventions.md`]: ../../docs/dev/api-conventions.md
