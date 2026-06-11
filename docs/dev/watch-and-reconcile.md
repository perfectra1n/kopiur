# Watches & reconcile triggers

How a change to *anything a CR depends on* turns into a reconcile of that CR —
and how a terminally-failed object un-sticks. The implementation lives in
`crates/controller/src/watch.rs` (mappers) and `crates/controller/src/lib.rs::run`
(wiring); the terminal gate is `crates/controller/src/io/apply.rs::terminal_gate_holds`.

## Why referent watches exist

A kube `Controller` re-reconciles its primary kind and the children it owns —
nothing else. But most Kopiur reconcilers *read* objects they don't own: a
credential `Secret`, a TLS-CA `ConfigMap`, a `Repository`/`ClusterRepository`, a
`SnapshotPolicy`. Editing a Secret's *content* does not even bump any referrer's
`metadata.generation`, so without extra wiring a fixed password would sit unnoticed
until a multi-minute requeue. The referent watches close that gap: a referent
change re-triggers its referrers within seconds.

## Mapper architecture

Each `Controller::watches(Api<Referent>, …, mapper)` call installs a mapper that
answers "which referrers care about this referent event?". Two rules:

1. **Scan a shared reflector `Store`, never `Api::list`.** Every mapper reads the
   referrer controller's own store (shared via `Controller::store()`) and does one
   in-memory pass (`select()` in `watch.rs`). A per-event API list would hammer the
   API server on every Secret change in the cluster; the store scan is free, and a
   non-matching event (most Secrets) maps to the empty set.
2. **Matching is pure and unit-tested.** The predicates
   (`repo_references_secret`, `ref_matches_repository`, …) reproduce the exact
   namespace-defaulting the reconcilers use, so the watch can't disagree with the
   read path.

## Referent → referrer matrix

| When this changes… | …these re-reconcile (mapper) |
| --- | --- |
| `Secret` | `Repository` (`secret_to_repositories`), `ClusterRepository` (`secret_to_cluster_repositories`), `RepositoryReplication` destination creds (`secret_to_replications`) |
| `ConfigMap` (TLS CA bundle) | `Repository` (`configmap_to_repositories`), `ClusterRepository` (`configmap_to_cluster_repositories`) |
| `Repository` / `ClusterRepository` | `SnapshotPolicy` (`*_to_policies`), `Restore` (`*_to_restores`), `Maintenance` (`*_to_maintenances`), `RepositoryReplication` source (`*_to_replications`) |
| `SnapshotPolicy` | `Snapshot` (`policy_to_snapshots`), `SnapshotSchedule` (`policy_to_schedules`) |

A source-repository Secret fix thus propagates transitively: Secret →
`Repository` reconciles to `Ready` → the repository watch re-triggers its
policies/restores/maintenances/replications.

(Owned children — mover `Job`s, projected Secrets, the `Maintenance` a repository
projects — are covered by the ordinary `owns()` relation, not these mappers.)

## The terminal-failure gate (and why it keys on two things)

A non-retryable backend failure (wrong password, permission denied) parks the
object `Failed` so the operator stops hammering the backend — re-driven only by a
~30-minute heartbeat. The gate that keeps it parked, `terminal_gate_holds`, holds
only while **both**:

1. `status.observedGeneration == metadata.generation` (the spec is unchanged), and
2. the credential Secret's live `resourceVersion` still equals the
   `status.resolvedCredentialVersion` recorded at the failed attempt.

Keying on generation alone is the historical bug this design fixes: a Secret
content edit bumps neither generation nor any spec field, so a *fixed* password
would never reopen the gate. With (2), the Secret watch fires, the gate sees a new
`resourceVersion`, and the repository retries immediately. The e2e guard is
`fixed_credential_secret_unsticks_failed_repository` (`crates/e2e/tests/repository_lifecycle.rs`).

## Status-write discipline (don't self-trigger)

Watches make status churn dangerous: a reconcile that writes volatile content to
its own status (a fresh `now()`, a kopia temp filename) re-triggers itself in a
hot loop. House rules, enforced across reconcilers:

- Timestamps (`pinnedAt`, `lastTransitionTime`) are written **once per
  transition**, guarded by a presence/equality check — never re-stamped on a
  no-op reconcile.
- Phase writes are guarded by phase-equality checks.
- Condition updates go through `io::upsert_condition`, which preserves
  `lastTransitionTime` when the status is unchanged, so an identical patch is a
  server-side no-op.

## Field mutability stance

Spec fields are mutable unless immutability protects data (e.g. a pinned
resolution, a discovered snapshot's forced `Retain`). The guiding rule: **an edit
to a spec or credential must always be able to un-stick a `Failed` object** —
defensive immutability that forces delete-and-recreate is worse than the bug it
prevents. Anything resolved-then-pinned lives in `status.resolved.*`, not in spec.
