# GitOps with Kopiur

Kopiur is built to be a well-behaved Flux/Argo citizen: every reconciled resource
reports standard health, every operator-created object is labeled and owned, the
controller never writes back into your `spec`, and cross-field invariants are
validated in the apiserver (so CI catches them too). This page is the reference for
those guarantees — `kubectl wait`, health checks, drift hygiene, and CI validation.

## kstatus conditions + `observedGeneration` (ADR-0005 §2)

Every reconciled CRD exposes standard Kubernetes
[`Condition`s](https://kubernetes.io/docs/reference/using-api/api-concepts/) and a
`status.observedGeneration`, following the
[kstatus](https://github.com/kubernetes-sigs/cli-utils/blob/master/pkg/kstatus/README.md)
convention. That makes the resources first-class for `kubectl wait`, Flux
`healthChecks`/`healthCheckExprs`, and Argo CD health — without a custom health
plugin.

| Condition      | Meaning |
| -------------- | --- |
| `Ready`        | The resource is reconciled and healthy (repository connected, schedule armed, restore complete, …). The one condition to gate on. |
| `Reconciling`  | The controller is actively working toward the desired state (transient). |
| `Stalled`      | Progress is blocked on something that won't resolve by retrying (e.g. a missing dependency, a terminal kopia error) — look at the message. |

`observedGeneration` is the `metadata.generation` the status reflects; when it lags
`metadata.generation`, the controller hasn't caught up to your latest edit yet.

### `kubectl wait`

```console
# Block until a repository is connected before applying policies that use it
$ kubectl wait --for=condition=Ready repository/primary -n billing --timeout=120s

# Wait for a one-shot restore to finish
$ kubectl wait --for=condition=Ready restore/postgres-verify -n billing --timeout=30m
```

Each kind also surfaces kind-specific conditions alongside the kstatus three —
e.g. a `Repository` adds `Connected` and `MaintenanceOwned`; a `SnapshotPolicy`
adds `RepositoryReachable`; a `Snapshot` adds `SnapshotCreated`. `kubectl describe`
shows the full set with human-readable messages.

### Flux

```yaml
# A Kustomization can gate on a Repository going Ready
spec:
    healthChecks:
        - apiVersion: kopiur.home-operations.com/v1alpha1
          kind: Repository
          name: primary
          namespace: billing
    # …or the CEL form (Flux ≥ 2.x):
    healthCheckExprs:
        - apiVersion: kopiur.home-operations.com/v1alpha1
          kind: SnapshotPolicy
          inProgress: "status.conditions.filter(c, c.type == 'Reconciling').exists(c, c.status == 'True')"
          failed: "status.conditions.filter(c, c.type == 'Stalled').exists(c, c.status == 'True')"
          current: "status.conditions.filter(c, c.type == 'Ready').exists(c, c.status == 'True')"
```

### Dependency gating

Because conditions are standard, dependents gate cleanly: a `SnapshotPolicy` in a
tenant namespace won't usefully reconcile until its `Repository` is `Ready`, and
you can express that with a Flux `dependsOn` (on the Kustomization that contains the
`Repository`) or an Argo sync-wave.

## Materialized defaults — no diff-thrash (ADR-0005 §1)

Fields whose default is unconditional now carry a real OpenAPI `default:` and are
written into the stored object:

| Field | Materialized default |
| --- | --- |
| `SnapshotPolicy.spec.copyMethod` | `Snapshot` |
| `Restore.spec.source.fromPolicy.offset` | `0` |
| `SnapshotSchedule.spec.schedule.runOnCreate` | `false` |
| `SnapshotSchedule.spec.schedule.concurrencyPolicy` | `Forbid` |
| `Repository.spec.mode` / `ClusterRepository.spec.mode` | `ReadWrite` |
| `Repository.spec.onNamespaceDelete` / `ClusterRepository…` | `Orphan` |

They appear in `kubectl explain` and round-trip in the stored spec, so Flux/Argo
don't report an `OutOfSync` diff against a controller-set value. *Conditional*
defaults (identity, `policy.onMissingSnapshot`) stay controller/webhook-resolved and
pinned to **status**, never written into `spec`.

## Status-only writes (ADR-0005 §14(d))

The controller writes only `.status`, **never** your `spec`. The pinned identity
lives in `status.resolved.identity`; new fields default via OpenAPI, never by spec
mutation. A write-back into spec would make Argo/Flux perpetually `OutOfSync` — so
Kopiur doesn't do it. This is an invariant, not a best-effort.

## managed-by + ownerReferences (ADR-0005 §14(c))

Every object Kopiur creates carries `app.kubernetes.io/managed-by: kopiur` **and** an
`ownerReference` to the CR that caused it:

- mover Jobs (backup/restore/maintenance/replication),
- the minted `kopiur-mover` ServiceAccount + RoleBinding,
- the cache PVC (when `cache.mode: Persistent`),
- CSI `VolumeSnapshot`s,
- the projected credential Secret (§8).

So Argo/Flux don't report them `OutOfSync` or prune them, and they garbage-collect
with their owner. Tell your GitOps engine to ignore operator-managed children:

```yaml title="Argo CD — resource.exclusions / ignore"
# argocd-cm: ignore kopiur-managed children by label
resource.customizations.ignoreDifferences.all: |
    managedFieldsManagers: [kopiur]
```

```yaml title="Flux — exclude by label in the Kustomization source, or"
# A Kustomization that only owns the kopiur CRs (not their children) needs no
# special config: the children carry ownerReferences and are not in Git.
```

## Suspend — one declarative pause (ADR-0005 §14(e))

`suspend: true` is available consistently across `Repository`, `ClusterRepository`,
`SnapshotPolicy`, `SnapshotSchedule` (`schedule.suspend`), and
`RepositoryReplication`. Pause via Git, surfaced in a `SUSPENDED` print column where
applicable. No imperative `kubectl` dance.

## CRD-schema validation (`x-kubernetes-validations`, ADR-0005 §15)

Cross-field invariants are operator-authored CEL **in the CRD schema**, so they are
enforced by the apiserver *and* by `kubeconform`/`flux build` in CI — not only by the
webhook. This shrinks the webhook and means a bad manifest fails your PR gate, not
production.

| Kind | Rule |
| --- | --- |
| `SnapshotPolicy` | each `source` is exactly one of `pvc`/`pvcSelector`/`nfs`. |
| `SnapshotSchedule` | exactly one of `policyRef` / `policySelector`. |
| `Restore` | exactly one of `target.pvc` / `target.pvcRef` / `target.populator`. |
| `Repository` / `ClusterRepository` | `create.{splitter,hash,encryption,ecc}` are immutable (transition rules). The `encryption.passwordSecretRef` reference is mutable — rename/repoint freely as long as it resolves to the same password value. |

/// tip | Validate before you push

```console
$ flux build kustomization apps --dry-run | kubeconform -strict -schema-location default \
    -schema-location 'https://…/kopiur.home-operations.com/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'
```

The published CRD JSON schemas (home-operations schema server / `datreeio/CRDs-catalog`)
let `kubeconform` validate kopiur CRs without a cluster, and make
`# yaml-language-server: $schema=` editor hints resolve.

///

## Webhook failure mode (ADR-0005 §14(f))

The admission webhook is scoped to kopiur kinds only, so a `failurePolicy: Fail`
outage never wedges unrelated GitOps applies. **CRD applies bypass the webhook**, so
a CRD-only sync-wave/`dependsOn` still bootstraps even during operator downtime —
apply CRDs in an early wave so a CR never races ahead of its CRD.

## The populator cutover caveat (ADR-0005 §14(h))

A bound PVC's `dataSourceRef` is **immutable**. Migrating an existing app onto the
volume-populator restore path (`target.populator`, see [Restores → deploy-or-restore](restores.md#deploy-or-restore-gitops))
is therefore a snapshot-gated **delete-and-repopulate**, not a silent `git push`:
back up, delete the old PVC, then let the populator-backed PVC provision. Plan the
cutover deliberately.

## See also

- [Restores → deploy-or-restore](restores.md#deploy-or-restore-gitops) — the one-bundle GitOps pattern.
- [Field reference](field-reference.md) — the conditions and status fields per kind.
- [Observability](dev/observability.md) — metrics + the `resource_phase` gauge.
- [ADR-0005 §14](adr/0005-crd-feature-improvements.md) — the full GitOps-citizen decision.
