# kopiur-e2e

End-to-end test harness: drive the **fully deployed** Kopiur operator against an ephemeral kind cluster.

## Role in the workspace

Unlike the per-crate `integration` tests (which call reconcile helpers or POST
admission reviews directly), `kopiur-e2e` exercises the operator the way a user
deploys it. The work splits cleanly in two:

- **Host-level setup is mise tasks** (`mise run test-e2e` and its `e2e-*`
  sub-tasks): build + load the `:e2e` images into a throwaway `kind` cluster,
  seed the node's hostPath fixtures, and `helm upgrade --install` the chart with
  `deploy/e2e/values.yaml`. These own only what has no Kubernetes API.
- **In-cluster provisioning is Rust** ([`World`]): each scenario declares the
  cluster state it needs as data — `world.ensure(&[Need::Minio])` — and the
  harness applies the namespaces, Secrets, PV/PVCs, MinIO, and buckets
  idempotently (type-safe [`apply::Fixture`] dispatch, [`wait`] conditions).

The scenarios then create real `kopiur.home-operations.com` CRs and assert on the
cluster state the operator produces — real mover `Job`s, real kopia snapshots,
real restored bytes.

Everything is gated behind the **`e2e` cargo feature *and* `#[ignore]`**, and
[`World::connect`] skips gracefully when no cluster is reachable. So the hermetic
`cargo test --workspace` never compiles a cluster body or touches a cluster, and
even `--features e2e` runs e2e only with `-- --include-ignored`.

> Safety: per the project conventions, these tests must **only** target the
> throwaway `kopiur-e2e` kind cluster. The mise `e2e-*` tasks pin an isolated
> `KUBECONFIG` under `target/e2e/` and tear the cluster down on exit — a real
> kubecontext is never touched.

## Key items

| Item | Role |
|---|---|
| [`World`] / [`Need`] | Declarative provisioning: a scenario calls `World::ensure(&[Need::…])` to bring up exactly the cluster state it needs (filesystem fixtures, MinIO, the workload namespace), idempotently. |
| [`E2E_NAMESPACE`] | The namespace the harness installs the operator and runs scenarios in (matches `deploy/e2e/values.yaml`). |
| [`try_client`] | Connects to a cluster and probes the API server; returns `None` (printing a skip notice) when unreachable, so an e2e test is a clean no-op off-cluster. |
| [`wait_until`] | Generic poll helper: re-runs a closure every `interval` until it yields `Ok(Some(value))`, failing with a `what`-tagged error on timeout. `Ok(None)` = keep waiting; `Err` = hard failure. |
| [`default_timeout`] / [`poll_interval`] | Sensible poll budget (180s) and interval (3s) generous enough for a cold kind node pulling images and running kopia. |

## Running it

The host-level steps are a mise monorepo subproject (`crates/e2e/mise.toml`).

```text
mise run //crates/e2e:test    # full pipeline: build+load images, kind up, helm install, run, tear down

# Individual steps (each runnable on its own):
mise run //crates/e2e:cluster-create   # create/reuse the kind cluster
mise run //crates/e2e:images           # build the :e2e images
mise run //crates/e2e:images-load      # load them into the cluster
mise run //crates/e2e:helm             # helm upgrade --install
mise run //crates/e2e:down             # tear the cluster down

# Knobs: KOPIUR_E2E_SKIP_BUILD=1 (reuse images), KOPIUR_KEEP_KIND=1 (leave the
# cluster up), KOPIUR_E2E_TESTFILTER=<name> (run a subset).

# The underlying cargo invocation the pipeline runs (feature-gated + ignored):
cargo test -p kopiur-e2e --features e2e -- --include-ignored
```

Off-cluster the suite still passes as a no-op: [`World::connect`] prints a `SKIP`
notice and each scenario returns early.

## See also

- [ADR-0003](../../docs/adr/0003-kopiur-rust-operator.md) — the operator design
  and CRD lifecycle these scenarios validate.
