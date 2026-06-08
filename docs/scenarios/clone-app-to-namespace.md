# Scenario 08 — Clone an app's data into another namespace

**Reproduce a prod bug against real data, or seed staging.** You want production's
data in a `staging` (or preview) namespace — not a hand-exported dump, the actual
latest snapshot — without touching the production volume.

The mechanism is a **cross-namespace restore**: the `Restore` lives in the
destination namespace, while the source reference carries the **source** namespace.
The only wrinkle is credentials — the restore mover runs in the destination, so the
repository's credential Secret has to be reachable there.

## Step 1 — Make credentials reachable in the destination

The restore mover loads the repo password (and any backend creds) via `envFrom` from
a Secret **in its own namespace**. A brand-new `staging` namespace doesn't have one.
Two options:

- **Shared `ClusterRepository` + projection (recommended).** With a cluster-scoped
  repository, set `credentialProjection.enabled: true` on the `Restore` and the
  operator copies the repository's Secret into `staging` for the run (owned by the
  `Restore`, garbage-collected with it). Needs the operator's Secret-projection RBAC
  (Helm `secretProjection.enabled`, off by default).
- **Place the Secret yourself.** Copy the repo's credential Secret into `staging`
  ahead of time and skip `credentialProjection`.

## Step 2 — Restore prod's snapshot into the destination

The bundle creates the `staging` namespace and a `Restore` that resolves prod's
latest snapshot (`source.fromConfig` with `namespace: billing`) into a new
`postgres-data-clone` PVC in `staging`. The mover's `securityContext` matches the
staging app's UID so the cloned files are usable. (To clone a _specific_ snapshot
instead of the latest, use `source.backupRef` with the prod `Backup`'s name +
`namespace` — see [example 16](../examples.md#example-16--cross-namespace-clone-restore).)

```yaml
--8<-- "deploy/examples/scenarios/08-clone-app-to-namespace.yaml"
```

```console
$ kubectl get restore clone-prod-postgres -n staging -w
NAME                  PHASE        AGE
clone-prod-postgres   Resolving    3s
clone-prod-postgres   Restoring    11s
clone-prod-postgres   Completed    52s
```

## Step 3 — Mount the clone

Point your staging `Deployment`/`StatefulSet` at `postgres-data-clone` and you have
production data in staging. The production namespace and its volume are never read
for _write_ and never modified.

/// note | Identity, not magic

`fromConfig` resolves the snapshot by kopia identity
(`<backupConfigName>@<namespace>:/pvc/<pvcName>`), which is why it needs the source
namespace. The clone lands under a new identity for its own future backups. See
[How Kopia works](../concepts/how-kopia-works.md).

///

## See also

- [Example 16 — cross-namespace clone restore](../examples.md#example-16--cross-namespace-clone-restore) and [example 17 — restore from a shared repo with projection](../examples.md#example-17--restore-from-a-shared-repo-projection).
- [Movers, RBAC & credentials](../movers.md) — credential projection and the minted mover ServiceAccount.
- [Scenario 04 — migrate across clusters / namespaces](migrate-across-clusters.md) — when it's a permanent move, not a clone.
