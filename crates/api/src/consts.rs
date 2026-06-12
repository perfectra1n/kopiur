//! Well-known wire-contract strings: the finalizer, labels, annotations, and
//! condition types that form kopiur's public Kubernetes surface (ADR §4.5,
//! ADR-0005 §2/§14(c)).
//!
//! These live in `kopiur-api` — not the controller — because they are part of
//! the API contract itself: external tooling (the `kubectl kopiur` plugin,
//! GitOps health checks, user automation) must agree on them byte-for-byte
//! with the operator. Controller-internal reasons/actions/deadlines stay in
//! `kopiur-controller`'s own `consts` module.

/// The finalizer every `Snapshot` carries so the operator can run snapshot
/// cleanup before the CR is removed (ADR §4.5 / SKILL "Snapshot lifecycle =
/// CR lifecycle").
pub const SNAPSHOT_CLEANUP_FINALIZER: &str = "kopiur.home-operations.com/snapshot-cleanup";

/// Repo-offline escape hatch: when present, the finalizer is removed *without*
/// contacting the repository, the snapshot is recorded orphaned, and a
/// `SnapshotOrphaned` event is emitted (ADR §4.5).
pub const SKIP_SNAPSHOT_CLEANUP_ANNOTATION: &str =
    "kopiur.home-operations.com/skip-snapshot-cleanup";

/// Label mirroring a `Snapshot`'s origin (`scheduled`/`manual`/`discovered`).
pub const ORIGIN_LABEL: &str = "kopiur.home-operations.com/origin";
/// Label keying a discovered `Snapshot` to its kopia snapshot id (dedup, §2.1).
pub const SNAPSHOT_ID_LABEL: &str = "kopiur.home-operations.com/snapshot-id";
/// Label keying a discovered `Snapshot` to the owning Repository UID (dedup).
pub const REPOSITORY_UID_LABEL: &str = "kopiur.home-operations.com/repository-uid";
/// Label naming the `SnapshotPolicy` a `Snapshot` was produced from.
pub const CONFIG_LABEL: &str = "kopiur.home-operations.com/config";

/// Label naming the `SnapshotSchedule` that fired a scheduled `Snapshot`
/// (selector for a schedule's own children, distinct from [`CONFIG_LABEL`]
/// under `policySelector` fan-out).
pub const SCHEDULE_LABEL: &str = "kopiur.home-operations.com/schedule";

/// Label naming the operation a mover `Job` performs, for Jobs whose owning CR
/// doesn't record the Job name in status (e.g. `Restore`). Values:
/// [`OP_RESTORE`], [`OP_RESTORE_TARGET`].
pub const OP_LABEL: &str = "kopiur.home-operations.com/op";
/// [`OP_LABEL`] value for a `Restore`'s mover Job.
pub const OP_RESTORE: &str = "restore";
/// [`OP_LABEL`] value for a `Restore`'s operator-created target PVC.
pub const OP_RESTORE_TARGET: &str = "restore-target";

/// Annotation requesting an out-of-band `Maintenance` run NOW (Flux-style
/// reconcile trigger). Value: an RFC3339 timestamp; a NEW timestamp requests a
/// new run (re-applying the same value is a no-op once handled). Usable from
/// bare `kubectl annotate` or `kubectl kopiur maintenance run`.
pub const RUN_REQUESTED_ANNOTATION: &str = "kopiur.home-operations.com/run-requested";
/// Companion annotation selecting the run kind: `quick` (default) or `full`
/// (see `kopiur_api::maintenance::ManualRunMode`).
pub const RUN_MODE_ANNOTATION: &str = "kopiur.home-operations.com/run-mode";

/// The API version string for kopiur CRDs (used in mover `TargetRef`s and
/// `kubectl -o name`-style output).
pub const API_VERSION: &str = "kopiur.home-operations.com/v1alpha1";

/// The standard `app.kubernetes.io/managed-by` label key. Stamped on **every**
/// operator-created object (mover Jobs, work-spec ConfigMaps, cache PVC, minted
/// mover SA/RoleBinding, projected credential Secret, CSI VolumeSnapshots) so
/// Argo/Flux recognize them as controller-owned and neither prune nor report them
/// `OutOfSync` (ADR-0005 §14(c)).
pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
/// The [`MANAGED_BY_LABEL`] value identifying kopiur-managed objects.
pub const MANAGED_BY_VALUE: &str = "kopiur";

/// kstatus-compliant standard condition types (ADR-0005 §2) so `kubectl wait
/// --for=condition=Ready` and Flux/Argo health checks work natively against every
/// reconciled kopiur CRD.
/// The headline readiness condition.
pub const READY_CONDITION: &str = "Ready";
/// Set `True` while a reconcile is making progress toward Ready.
pub const RECONCILING_CONDITION: &str = "Reconciling";
/// Set `True` when the resource is stuck and won't progress without intervention
/// (mapped from a terminal `ErrorClass::Terminal` failure).
pub const STALLED_CONDITION: &str = "Stalled";

/// `Repository`/`ClusterRepository` condition recording whether a `Maintenance`
/// covers it (ADR §3.7). Wire-visible: GitOps health checks and the kubectl
/// plugin's `status` read it.
pub const MAINTENANCE_CONFIGURED_CONDITION: &str = "MaintenanceConfigured";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_version_is_group_slash_version() {
        // The duplicated literal must never drift from the canonical pair.
        assert_eq!(API_VERSION, format!("{}/{}", crate::GROUP, crate::VERSION));
    }

    #[test]
    fn well_known_strings_are_group_prefixed() {
        // Finalizers/labels/annotations on the kopiur API surface live under the
        // API group domain; a typo'd prefix would silently break selectors.
        for s in [
            SNAPSHOT_CLEANUP_FINALIZER,
            SKIP_SNAPSHOT_CLEANUP_ANNOTATION,
            ORIGIN_LABEL,
            SNAPSHOT_ID_LABEL,
            REPOSITORY_UID_LABEL,
            CONFIG_LABEL,
            SCHEDULE_LABEL,
            OP_LABEL,
            RUN_REQUESTED_ANNOTATION,
            RUN_MODE_ANNOTATION,
        ] {
            assert!(s.starts_with(crate::GROUP), "{s} must be group-prefixed");
        }
    }
}
