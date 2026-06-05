# kopiur-e2e

End-to-end test harness: drive the **fully deployed** Kopiur operator against an ephemeral kind cluster.

## Role in the workspace

Unlike the per-crate `integration` tests (which call reconcile helpers or POST
admission reviews directly), `kopiur-e2e` exercises the operator the way a user
deploys it. `scripts/with-e2e.sh` builds the controller + mover images, loads them
into a throwaway `kind` cluster, and installs the Helm chart. The scenarios in
`tests/lifecycle.rs` then create real `kopiur.home-operations.com` CRs and assert
on the cluster state the operator produces — real mover `Job`s, real kopia
snapshots, real restored bytes.

Everything is gated behind the **`e2e` cargo feature *and* `#[ignore]`**, and the
client probe skips gracefully when no cluster is reachable. So the hermetic
`cargo test --workspace` never compiles a cluster body or touches a cluster, and
even `--features e2e` runs e2e only with `-- --include-ignored`.

> Safety: per the project conventions, these tests must **only** target the
> throwaway kind cluster created by `scripts/with-e2e.sh` — never a real cluster.
> `scripts/with-e2e.sh` isolates the kubeconfig and tears the cluster down.

## Key items

| Item | Role |
|---|---|
| [`E2E_NAMESPACE`] | The namespace the harness installs the operator and runs scenarios in (matches `scripts/with-e2e.sh`). |
| [`try_client`] | Connects to a cluster and probes the API server; returns `None` (printing a skip notice) when unreachable, so an e2e test is a clean no-op off-cluster. |
| [`wait_until`] | Generic poll helper: re-runs a closure every `interval` until it yields `Ok(Some(value))`, failing with a `what`-tagged error on timeout. `Ok(None)` = keep waiting; `Err` = hard failure. |
| [`default_timeout`] / [`poll_interval`] | Sensible poll budget (180s) and interval (3s) generous enough for a cold kind node pulling images and running kopia. |

## Running it

```text
mise run test-e2e             # wraps scripts/with-e2e.sh: build images, kind up, helm install, run, tear down

# Or directly:
scripts/with-e2e.sh

# The underlying cargo invocation the script runs (feature-gated + ignored):
cargo test -p kopiur-e2e --features e2e -- --include-ignored
```

Off-cluster the suite still passes as a no-op: [`try_client`] prints a `SKIP`
notice and each scenario returns early.

## See also

- [ADR-0003](../../docs/adr/0003-kopiur-rust-operator.md) — the operator design
  and CRD lifecycle these scenarios validate.
