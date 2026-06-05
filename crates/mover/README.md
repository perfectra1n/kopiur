# kopiur-mover

The per-`Backup`/`Restore` Job binary that drives `kopia` inside a pod — and the
pure data library that defines its contract with the controller.

## Role in the workspace

The mover is primarily a **binary** (`main.rs`): a statically-linked
musl/distroless Rust executable (~8 MB) that ships in an image alongside the
`kopia` binary (ADR §4.10). At runtime it:

1. reads its [`workspec::MoverWorkSpec`] from a mounted JSON file written by the
   controller as a `ConfigMap`;
2. invokes `kopia --json` (via [`kopiur-kopia`](../kopia)) for the long-running
   snapshot / restore / delete / bootstrap operation;
3. `PATCH`es progress and a terminal [`status::StatusUpdate`] onto the target
   CR's status subresource;
4. on failure writes a structured [`status::FailureBlock`] and exits non-zero.

But the mover's **pure data modules are exposed as a library** so the controller
can construct a [`workspec::MoverWorkSpec`] — the controller↔mover JSON contract
(ADR §4.10) — and unit-test that construction without a cluster or a kopia
subprocess. Only the cluster-free layers are public:

- [`workspec`] — the work-spec contract (operation, resolved identity, repository
  connect info, target ref, options). Round-trips losslessly through serde_json.
- [`status`] — the pure `kopia`-result → CR-status mapping ([`status::StatusUpdate`],
  [`status::FailureBlock`], [`status::MoverPhase`]).
- [`bootstrap`] — the repository connect-vs-create decision ([`bootstrap::BootstrapResult`],
  [`bootstrap::should_attempt_create`]).
- [`env`] — the mover's environment-variable contract.

The kube `PATCH` path lives in `main.rs` and is intentionally *not* part of the
library surface.

## The contract: `MoverWorkSpec`

The spec carries **resolved values only** — identity already rendered, repository
connect info concrete (ADR §4.2). The mover never re-derives anything; it executes
exactly what the controller decided. Both the [`workspec::Operation`] selector and
the [`workspec::RepositoryConnect`] backend selector are **externally-tagged
enums** (`{ "backup": {...} }`, `{ "filesystem": {...} }`), mirroring the api
crate's enum discipline: a new operation or backend cannot compile until every
`match` handles it. Credentials are *not* in the spec — they arrive as env vars
from a mounted Secret, so they never land in a ConfigMap.

## Key types

| Type | What it is |
| --- | --- |
| [`workspec::MoverWorkSpec`] | The full controller→mover JSON contract |
| [`workspec::Operation`] | Backup / Restore / SnapshotDelete / BootstrapRepository |
| [`workspec::RepositoryConnect`] | Serializable backend selector (mirrors `kopiur_kopia::ConnectSpec`) |
| [`workspec::ResolvedIdentity`] | The pinned `username@hostname:path` identity |
| [`status::StatusUpdate`] / [`status::FailureBlock`] / [`status::MoverPhase`] | Pure result → CR-status mapping |
| [`bootstrap::BootstrapResult`] | Outcome of a repository bootstrap run |

## Example

Construct a backup `MoverWorkSpec` the way the controller does and round-trip it
through serde_json, confirming the externally-tagged wire shape:

```rust
use std::collections::BTreeMap;
use kopiur_mover::workspec::*;

let spec = MoverWorkSpec {
    version: 1,
    operation: Operation::Backup(BackupOp {
        source_path: "/data".into(),
        tags: BTreeMap::new(),
    }),
    identity: ResolvedIdentity {
        username: "mydb".into(),
        hostname: "prod".into(),
        source_path: "/data".into(),
    },
    repository: RepositoryConnect::Filesystem { path: "/repo".into() },
    target_ref: TargetRef {
        api_version: "kopiur.home-operations.com/v1alpha1".into(),
        kind: "Backup".into(),
        name: "mydb-20260601".into(),
        namespace: "prod".into(),
    },
    hook_plan: HookPlanSummary::default(),
    options: MoverOptions::default(),
};

// Round-trips through serde_json unchanged.
let json = serde_json::to_string(&spec).unwrap();
let back: MoverWorkSpec = serde_json::from_str(&json).unwrap();
assert_eq!(back, spec);

// Externally tagged on the wire (camelCase keys).
let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
assert_eq!(v["operation"]["backup"]["sourcePath"], "/data");
assert_eq!(v["repository"]["filesystem"]["path"], "/repo");
assert_eq!(spec.operation.kind_str(), "Backup");
```

The actual `kopia` invocation and the kube `PATCH` happen in the binary against a
real repository and cluster, so they are not runnable doctests.

## See also

- [ADR-0003](../../docs/adr/0003-kopiur-rust-operator.md) — §4.10 (mover pods &
  failure handling, the work-spec contract) and §5.4 (kopia interaction).
