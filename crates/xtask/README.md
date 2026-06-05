# xtask

Kopiur's codegen library + binary: generate the CRDs, RBAC, and dashboards that ship under `deploy/`.

## Role in the workspace

`xtask` is Kopiur's developer-tooling crate, invoked as `cargo xtask <cmd>`. It is
the single source that turns the typed CRD definitions (from `kopiur-api`'s
`kube::CustomResource` derives) and the hand-written RBAC/dashboard sources into
the checked-in deploy artifacts:

- `gen-crds` → structural-schema CRD YAML under `deploy/crds/`
- `gen-rbac` → the controller/webhook RBAC manifests under `deploy/`
- `gen-all` → CRDs + RBAC + the Grafana dashboard copy under
  `deploy/helm/kopiur/files/dashboards/`

Each subcommand also takes a `--check` mode (`mise run gen-check`) that re-renders
everything in memory and compares it against the checked-in files **without
writing**, so CI fails on drift instead of silently shipping stale YAML.

The generation logic deliberately lives in the **library** (`xtask::`), not in
`main.rs`: a binary crate's modules aren't importable, so keeping it in the lib
lets the integration tests under `tests/` exercise it directly.

> Note: the package name is `xtask` (not `kopiur-xtask`); the import path is
> `xtask::`.

## Key modules / types

| Item | Role |
|---|---|
| [`collect`] | Returns the [`artifact::Artifact`]s a subcommand (`gen-crds` / `gen-rbac` / `gen-all`) is responsible for. |
| [`run`] | Drives a subcommand end-to-end: writes the artifacts, or in `--check` mode reports drift and returns the process exit code. |
| [`artifact::Artifact`] | One generated file: a `deploy/`-relative path + its full content (including the generated-file header). |
| [`artifact::write_all`] / [`artifact::check_all`] | Write every artifact to disk / compare against the checked-in files (the drift guard). |
| [`paths::workspace_root`] / [`paths::deploy_dir`] | Deterministic workspace-root resolution and the `deploy/` directory under it. |
| [`crds`] / [`rbac`] / [`dashboards`] | The per-kind artifact generators. |

## Example

[`artifact::Artifact`] is a pure value type — constructing one and reading its
relative path needs no filesystem, cluster, or codegen:

```rust
use xtask::artifact::Artifact;

let a = Artifact::new("crds/repositories.yaml".into(), "# GENERATED\n".into());
assert_eq!(a.rel_path, "crds/repositories.yaml");
assert!(a.content.starts_with("# GENERATED"));
```

Running a subcommand (touches the filesystem under `deploy/`, so `no_run`):

```rust,no_run
# fn main() -> anyhow::Result<()> {
// `false` = write mode; `true` = --check drift guard (returns exit code 1 on drift).
let exit_code = xtask::run("gen-all", false)?;
std::process::exit(exit_code);
# }
```

From the command line:

```text
cargo xtask gen-all           # regenerate deploy/crds + RBAC + dashboard
cargo xtask gen-all --check   # CI drift guard: nonzero exit if artifacts are stale
mise run gen                  # the same, via the pinned task runner
mise run gen-check
```

## See also

- [ADR-0003](../../docs/adr/0003-kopiur-rust-operator.md) — the canonical CRD
  surface these artifacts are generated from.
