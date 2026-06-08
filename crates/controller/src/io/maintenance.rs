use super::*;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use k8s_openapi::api::core::v1::ObjectReference;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, OwnerReference};
use kube::api::DeleteParams;
use kube::runtime::events::{Event, EventType};
use kube::runtime::reflector::Store;
use kube::{Api, Resource};
use serde::de::DeserializeOwned;

use kopiur_api::Maintenance;
use kopiur_api::common::{RepositoryKind, RepositoryRef};
use kopiur_api::maintenance::{
    MaintenanceSpec, Ownership, RepositoryMaintenanceSpec, default_maintenance_schedule,
};

use crate::consts::{
    CHECK_MAINTENANCE_ACTION, MAINTENANCE_CONFIGURED_CONDITION, MAINTENANCE_CONFIGURED_REASON,
    MAINTENANCE_DISABLED_REASON, MAINTENANCE_NAMESPACE_UNRESOLVED_REASON,
};
use crate::context::Context;

/// True if any `Maintenance` in the shared informer store references the given
/// repository. **Synchronous** — reads the reflector cache built by the
/// Maintenance controller, so a Repository reconcile answers "is maintenance
/// configured for me?" without an `Api::list` round-trip. `namespace` is `None`
/// for a cluster-scoped `ClusterRepository`. Matching is the pure, exhaustive
/// [`RepositoryRef::resolves_to`].
pub fn repository_has_maintenance(
    store: &Store<Maintenance>,
    kind: RepositoryKind,
    name: &str,
    namespace: Option<&str>,
) -> bool {
    store.state().iter().any(|m| {
        let owner_ns = m.metadata.namespace.as_deref().unwrap_or_default();
        m.spec
            .repository
            .resolves_to(owner_ns, kind, name, namespace)
    })
}

/// True if `m` is an operator-*managed* `Maintenance` owned by the repository
/// `(owner_kind, repo_name)` — i.e. it carries a controller `ownerReference` back
/// to that repository. Managed CRs are projected from `spec.maintenance`; a CR a
/// user hand-authored has no such owner reference and is treated as *foreign*.
pub(crate) fn is_managed_by(m: &Maintenance, owner_kind: &str, repo_name: &str) -> bool {
    m.metadata
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|o| o.kind == owner_kind && o.name == repo_name && o.controller == Some(true))
}

/// What the reconciler should do with the operator-managed `Maintenance` for a
/// repository, given the inputs. A closed enum matched exhaustively so a new
/// state can't slip past a reconcile branch (ADR §5.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceAction {
    /// Create or converge the operator-managed `Maintenance`.
    Manage,
    /// Remove the operator-managed `Maintenance` (it exists but is no longer wanted).
    Unmanage,
    /// Nothing to do (not wanted, and none is managed).
    Leave,
    /// Wanted, but the (cluster-repo) placement namespace is unresolved.
    Unresolved,
}

/// Pure decision for the managed `Maintenance` (the design matrix in the plan):
/// the operator manages its own only when `enabled` AND no `foreign`
/// (user-authored) `Maintenance` already covers the repo. When it shouldn't,
/// any previously-`managed` one is removed. A wanted-but-unplaceable cluster repo
/// is `Unresolved`.
pub fn maintenance_action(
    enabled: bool,
    foreign: bool,
    managed_exists: bool,
    placement_resolved: bool,
) -> MaintenanceAction {
    if enabled && !foreign {
        if placement_resolved {
            MaintenanceAction::Manage
        } else {
            MaintenanceAction::Unresolved
        }
    } else if managed_exists {
        MaintenanceAction::Unmanage
    } else {
        MaintenanceAction::Leave
    }
}

/// Partition the `Maintenance` CRs referencing repository `(kind, name)` into
/// "is there a *foreign* (user-authored) one?" and "the operator-*managed* one,
/// if present". Pure over an iterator of `Maintenance` so it is unit-tested
/// without a cluster. `match_namespace` is the repository's namespace (`None` for
/// a cluster-scoped `ClusterRepository`); `owner_kind` is the literal CR kind
/// (`"Repository"`/`"ClusterRepository"`) used to recognize our own owner ref.
pub fn classify_maintenance(
    items: impl IntoIterator<Item = Maintenance>,
    kind: RepositoryKind,
    owner_kind: &str,
    name: &str,
    match_namespace: Option<&str>,
) -> (bool, Option<Maintenance>) {
    let mut foreign = false;
    let mut managed = None;
    for m in items {
        let owner_ns = m.metadata.namespace.as_deref().unwrap_or_default();
        if !m
            .spec
            .repository
            .resolves_to(owner_ns, kind, name, match_namespace)
        {
            continue;
        }
        if is_managed_by(&m, owner_kind, name) {
            managed = Some(m);
        } else {
            foreign = true;
        }
    }
    (foreign, managed)
}

/// Build the operator-managed `Maintenance` CR projected from a repository's
/// `spec.maintenance` (ADR §3.7). Pure — the reconciler server-side-applies the
/// result. Naming is 1:1 with the repository (at most one `Maintenance` per
/// repository); the `ownership.owner` lease string is deterministic so the same
/// repository always claims the same lease.
///
/// `placement_namespace` is where the (namespaced) `Maintenance` lives: the
/// repository's own namespace for a `Repository`, or the resolved placement
/// namespace for a `ClusterRepository`. The `repository` ref omits a namespace —
/// a `Repository` ref resolves via the Maintenance's own namespace, and a
/// `ClusterRepository` ref must not carry one.
pub fn build_managed_maintenance(
    kind: RepositoryKind,
    name: &str,
    placement_namespace: &str,
    spec: &RepositoryMaintenanceSpec,
    owner: OwnerReference,
) -> Maintenance {
    let owner_lease = match kind {
        RepositoryKind::Repository => format!("kopiur/{placement_namespace}/{name}"),
        RepositoryKind::ClusterRepository => format!("kopiur/clusterrepository/{name}"),
    };
    let mut m = Maintenance::new(
        name,
        MaintenanceSpec {
            repository: RepositoryRef {
                kind,
                name: name.to_string(),
                namespace: None,
            },
            schedule: spec
                .schedule
                .clone()
                .unwrap_or_else(default_maintenance_schedule),
            ownership: Ownership {
                owner: owner_lease,
                takeover_policy: spec.takeover_policy.unwrap_or_default(),
            },
            mover: spec.mover.clone(),
            failure_policy: spec.failure_policy.clone(),
            // Repo-managed maintenance runs where the repository's Secret already
            // lives (its own / the operator namespace), so it never needs projection.
            credential_projection: None,
        },
    );
    m.metadata = child_meta(name, placement_namespace, BTreeMap::new(), Some(owner));
    m
}

/// Project a repository's `spec.maintenance` into an operator-managed
/// `Maintenance` CR, honoring an externally-authored one, and surface the
/// `MaintenanceConfigured` status condition + `kopiur_repository_maintenance_configured`
/// gauge. The replacement for the old "warn when missing" check: maintenance is
/// **default-managed** (ADR §3.7), so the common path creates a `Maintenance`
/// rather than nagging.
///
/// Behavior (see also the design matrix in the plan):
/// - `enabled` (default) **and no foreign Maintenance** → server-side-apply the
///   managed `Maintenance` (create or converge). Condition `True`.
/// - a **foreign** (user-authored) `Maintenance` referencing the repo exists →
///   defer to it; delete any stale managed one. Condition `True`. This holds
///   regardless of `enabled` — `enabled: false` never ignores a user's Maintenance.
/// - `enabled: false` and **no** Maintenance covers it → delete any managed one;
///   condition `False` (reason `MaintenanceDisabled`), **no Warning event** (a
///   deliberate opt-out).
/// - `ClusterRepository` whose managed Maintenance has no resolvable placement
///   namespace → condition `False` + Warning (`MaintenanceNamespaceUnresolved`).
///
/// Degrade-not-crash: if the shared informer store has not synced yet, the whole
/// step is skipped (the `.watches` trigger + periodic requeue re-run it warm), so
/// a cold cache never deletes a managed CR or emits a false signal. `metric_kind`
/// doubles as the owner-reference kind (`"Repository"`/`"ClusterRepository"`);
/// `match_namespace` is the repository's namespace for ref-matching (`None` for a
/// `ClusterRepository`); `placement_namespace` is where the namespaced managed
/// `Maintenance` lives (`None` → unresolved, only possible for a `ClusterRepository`).
#[allow(clippy::too_many_arguments)]
pub async fn ensure_maintenance<K>(
    ctx: &Context,
    api: &Api<K>,
    obj: &K,
    regarding: &ObjectReference,
    kind: RepositoryKind,
    metric_kind: &str,
    metric_namespace: &str,
    match_namespace: Option<&str>,
    placement_namespace: Option<&str>,
    name: &str,
    maintenance: Option<&RepositoryMaintenanceSpec>,
    existing_conditions: &[Condition],
    observed_generation: Option<i64>,
) where
    K: Resource<DynamicType = ()> + DeserializeOwned + Clone + std::fmt::Debug,
{
    if !ctx.maintenance_synced.load(Ordering::Relaxed) {
        return;
    }

    let spec = maintenance.cloned().unwrap_or_default();
    let enabled = spec.enabled;

    let (foreign, managed) = classify_maintenance(
        ctx.maintenance_store.state().iter().map(|m| (**m).clone()),
        kind,
        metric_kind,
        name,
        match_namespace,
    );

    let mut covered = foreign;
    let mut unresolved = false;

    // Exhaustive match on the pure decision so every state is handled (ADR §5.5).
    match maintenance_action(
        enabled,
        foreign,
        managed.is_some(),
        placement_namespace.is_some(),
    ) {
        MaintenanceAction::Manage => {
            let ns = placement_namespace.expect("Manage implies a resolved placement namespace");
            match owner_ref_for(obj, metric_kind) {
                Ok(owner) => {
                    let desired = build_managed_maintenance(kind, name, ns, &spec, owner);
                    let mapi: Api<Maintenance> = Api::namespaced(ctx.client.clone(), ns);
                    match apply(&mapi, name, &desired).await {
                        Ok(_) => covered = true,
                        Err(e) => {
                            tracing::warn!(error = %e, repo = %name, namespace = %ns, "failed to apply managed Maintenance")
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, repo = %name, "cannot build owner reference for managed Maintenance")
                }
            }
        }
        MaintenanceAction::Unmanage => {
            // Disabled, or a foreign Maintenance now covers the repo: remove the
            // operator-managed one (idempotent; ignore NotFound).
            if let Some(existing) = &managed {
                let mns = existing
                    .metadata
                    .namespace
                    .as_deref()
                    .unwrap_or(metric_namespace);
                let mapi: Api<Maintenance> = Api::namespaced(ctx.client.clone(), mns);
                if let Err(e) = mapi.delete(name, &DeleteParams::default()).await
                    && !matches!(&e, kube::Error::Api(ae) if ae.code == 404)
                {
                    tracing::warn!(error = %e, repo = %name, "failed to delete managed Maintenance");
                }
            }
        }
        MaintenanceAction::Unresolved => unresolved = true,
        MaintenanceAction::Leave => {}
    }

    ctx.metrics
        .set_repository_maintenance_configured(metric_kind, metric_namespace, name, covered);

    let (status, reason, message, warn) = if unresolved {
        (
            false,
            MAINTENANCE_NAMESPACE_UNRESOLVED_REASON,
            format!(
                "managed Maintenance for {metric_kind} {name} cannot be placed: set \
                 spec.maintenance.namespace, or the operator's KOPIUR_NAMESPACE, so the namespaced \
                 Maintenance CR has a home"
            ),
            true,
        )
    } else if covered {
        let msg = if foreign {
            format!("an externally-authored Maintenance references {metric_kind} {name}")
        } else {
            format!("the operator manages a Maintenance for {metric_kind} {name}")
        };
        (true, MAINTENANCE_CONFIGURED_REASON, msg, false)
    } else {
        (
            false,
            MAINTENANCE_DISABLED_REASON,
            format!(
                "maintenance is disabled for {metric_kind} {name} (spec.maintenance.enabled: \
                 false) and no Maintenance references it; kopia storage will not be reclaimed"
            ),
            false,
        )
    };

    if warn
        && let Err(e) = ctx
            .recorder
            .publish(
                &Event {
                    type_: EventType::Warning,
                    reason: reason.into(),
                    note: Some(message.clone()),
                    action: CHECK_MAINTENANCE_ACTION.into(),
                    secondary: None,
                },
                regarding,
            )
            .await
    {
        tracing::warn!(error = %e, repo = %name, "failed to publish {reason} event");
    }

    let conditions = upsert_condition(
        existing_conditions,
        MAINTENANCE_CONFIGURED_CONDITION,
        status,
        reason,
        &message,
        observed_generation,
    );
    if let Err(e) = patch_status(api, name, serde_json::json!({ "conditions": conditions })).await {
        tracing::warn!(error = %e, repo = %name, "failed to patch MaintenanceConfigured condition");
    }
}
