# Scenario 04 — Migrate an app across clusters or namespaces

**Move a stateful app — data and all — somewhere new.** Unlike
[disaster recovery](disaster-recovery.md), where the app keeps its name and
namespace, a migration changes the app's _coordinate_: a new namespace
(`billing` → `payments`), or a whole new cluster, reusing the same repository.

That coordinate change is the catch.

/// warning | Why you can't just use `fromPolicy` in the destination

kopia stores each snapshot under `username@hostname:path`, and **`hostname`
defaults to the source namespace**. In the destination namespace, a `fromPolicy`
restore would compute the _destination's_ identity and find nothing. So the
one-time data carry restores by the **raw `source.identity`** (the source's
`username` + `hostname`), which is exactly what `identity` mode is for.

///

## The flow

```mermaid
flowchart LR
  subgraph src[Source — namespace billing]
    S[(snapshots<br/>postgres-data@billing)]
  end
  subgraph dst[Destination — namespace payments]
    REPO[Repository<br/>same bucket, connect] --> RST[Restore<br/>by raw identity]
    RST --> PVC[PVC postgres-data]
    BC[SnapshotPolicy<br/>identity pinned to billing] --> SCH[SnapshotSchedule]
  end
  S -. restore by identity .-> RST
```

Applied in the destination namespace, the bundle: connects a `Repository` to the
same bucket, restores the source's latest snapshot by identity into a new PVC,
then sets up a `SnapshotPolicy` + `SnapshotSchedule` to protect the app going forward.

## The decision that matters: continue or fork the lineage

The new `SnapshotPolicy`'s `identity` determines whether the destination's future
snapshots **extend the original timeline** or **start a fresh one**:

| Choice | How | Result |
| --- | --- | --- |
| **Continue** the lineage | pin `identity` to the **original** `username`/`hostname` (the bundle does this) | new snapshots dedup against the carried-over history; one logical timeline. |
| **Fork** a fresh lineage | delete the `identity` block; let it default to the destination namespace | a clean new timeline under the new coordinate. |

/// danger | Continue-lineage is for a MOVE, not active-active

If you pin the destination to the original identity, **decommission the source
first.** Two clusters writing the _same_ `username@hostname:path` concurrently
corrupt the snapshot timeline. Pin-to-original means "the app lives _here_ now,"
not "the app runs in both places."

///

```yaml
--8<-- "deploy/examples/scenarios/04-migrate-across-clusters.yaml"
```

## Verify the carry-over

```console
$ kubectl get restore postgres-migrate-in -n payments -w
NAME                  PHASE       AGE
postgres-migrate-in   Resolving   3s
postgres-migrate-in   Restoring   11s
postgres-migrate-in   Completed   38s

$ kubectl get pvc postgres-data -n payments
NAME            STATUS   VOLUME    CAPACITY   AGE
postgres-data   Bound    pvc-...   100Gi      40s
```

Start the app in `payments` against the restored PVC, confirm it, then tear down
the source. The first scheduled backup in `payments` will dedup against the
existing data rather than re-uploading it.

/// tip | Finding the source's exact identity

If you're unsure what the source recorded, read it off a source `Snapshot`'s
status, or `kopia snapshot list` against the repo. For a PVC source it's
`<config-name>@<namespace>:/pvc/<pvcName>` unless the source pinned a custom
`identity`.

///

## See also

- [Restores → `identity` source](../restores.md#identity--a-raw-kopia-identity) — the raw-identity restore mode.
- [Backups → identity](../backups.md#identity--what-kopia-records-usernamehostnamepath) — how the default identity is computed and pinned.
- [Scenario 05 — adopt an existing repo](adopt-existing-repo.md) — a close cousin when the "source" is foreign tooling rather than another Kopiur cluster.
