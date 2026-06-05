# kopiur-kopia

Typed models for kopia's `--json` output plus a `tokio::process`-based client — the
operator's single, controller-agnostic gateway to the `kopia` CLI.

## Role in the workspace

`kopiur-kopia` is the only crate in Kopiur that knows how to talk to the `kopia`
binary. It is split into three layers:

- **Typed JSON models** ([`model`]) — Rust structs modeled against the *actual*
  JSON kopia 0.23 emits (captured by round-tripping a real repository), using
  `camelCase` and tolerating unknown fields so kopia version skew never panics
  the operator.
- **Structured errors** ([`error`]) — a [`KopiaError`] enum rich enough to build a
  `status.failure` block (exit code, stderr tail, and a best-effort
  [`KopiaErrorClass`] with a retryable/terminal verdict).
- **A subprocess client** ([`client`]) — [`KopiaClient`] / [`KopiaClientBuilder`]
  spawn `kopia --json` via `tokio::process::Command`, parse stdout, and retain
  stderr for diagnostics.

Crucially this crate is **controller-agnostic**: it has *no* `kube` or
`k8s-openapi` dependency (ADR §5.1). That deliberate split lets both sides of the
operator depend on it:

- the **controller** uses it for short, idempotent operations it runs in-process
  or as tiny Jobs — `repository connect` to validate a `Repository`, `snapshot
  list` to materialize the catalog (ADR §5.4);
- the **mover** ([`kopiur-mover`](../mover)) uses it for the long-running
  snapshot/restore/maintenance subprocesses that must not be stranded by a
  controller restart.

## Key types

| Type | What it is |
| --- | --- |
| [`KopiaClient`] / [`KopiaClientBuilder`] | The `tokio::process` client and its builder |
| [`ConnectSpec`] | Externally-tagged backend selector (filesystem, s3, azure, gcs, b2, sftp, webdav, rclone, …) |
| [`RestoreOptions`] / [`VerifyOptions`] / [`PolicyArgs`] | Typed option bundles for the corresponding verbs |
| [`SnapshotCreateResult`] / [`SnapshotListEntry`] / [`SnapshotSource`] | Snapshot models with convenience accessors |
| [`RepositoryStatus`] / [`MaintenanceInfo`] | Repository status + maintenance JSON models |
| [`KopiaError`] / [`KopiaErrorClass`] | Structured failure with retry classification |

## stdout vs stderr

kopia prints progress (`Snapshotting ...`, `Restored N files`) to **stderr** and
the machine-readable `--json` result to **stdout**. The client parses stdout and
retains stderr only for diagnostics on failure.

## Deliberately out of scope

Commands with no place inside a declarative Kubernetes operator are not wrapped:
running a kopia API server (`server start`), FUSE `mount`, `notification`
profiles, `repository change-password`, `benchmark`, and the low-level
`blob`/`content`/`index`/`manifest`/`acl`/`users` plumbing.

## Example

Parse a representative `kopia snapshot create --json` result and read the
convenience accessors that pull aggregate counts from `rootEntry.summ`:

```rust
use kopiur_kopia::SnapshotCreateResult;

let json = r#"{
    "id": "k9c0ffee",
    "source": {"host": "prod", "userName": "mydb", "path": "/data"},
    "startTime": "2026-06-02T03:13:59Z",
    "endTime": "2026-06-02T03:14:00Z",
    "rootEntry": {
        "name": "data", "type": "d", "obj": "k1",
        "summ": {"size": 4096, "files": 12, "dirs": 3, "numFailed": 0}
    }
}"#;

let result: SnapshotCreateResult = serde_json::from_str(json).unwrap();
assert_eq!(result.id, "k9c0ffee");
assert_eq!(result.source.identity(), "mydb@prod:/data");
assert_eq!(result.total_bytes(), 4096);
assert_eq!(result.file_count(), 12);
```

Anything that spawns the `kopia` subprocess belongs behind [`KopiaClient`] and is
not shown as a runnable doctest:

```rust,no_run
# async fn run() -> Result<(), kopiur_kopia::KopiaError> {
use kopiur_kopia::KopiaClient;

let client = KopiaClient::builder().build();
let status = client.repository_status().await?;
println!("repo unique id: {}", status.unique_id_hex);
# Ok(())
# }
```

## See also

- [ADR-0003](../../docs/adr/0003-kopiur-rust-operator.md) — §5.4 (kopia
  interaction) is the canonical description of the subprocess contract; §5.1
  explains the `api`/`controller`/`kopia` dependency split.
