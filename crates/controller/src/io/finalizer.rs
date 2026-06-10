use super::*;

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, OwnerReference, Time};
use kube::api::{Patch, PatchParams};
use kube::core::ObjectMeta;
use kube::{Api, Resource, ResourceExt};
use serde::de::DeserializeOwned;

use crate::consts::API_VERSION;
use crate::error::{Error, Result};

/// Build an [`OwnerReference`] to a kopiur CR `obj` of the given `kind`.
/// `controller: true`, `blockOwnerDeletion: false` so children (Job/ConfigMap)
/// are reaped by GC with the CR but never block its deletion (§4.10).
pub fn owner_ref_for<K: Resource<DynamicType = ()>>(obj: &K, kind: &str) -> Result<OwnerReference> {
    let name = obj.meta().name.clone().ok_or_else(|| {
        Error::Invariant(format!("{kind} has no metadata.name for owner reference"))
    })?;
    let uid = obj.meta().uid.clone().ok_or_else(|| {
        Error::Invariant(format!("{kind} has no metadata.uid for owner reference"))
    })?;
    Ok(OwnerReference {
        api_version: API_VERSION.to_string(),
        kind: kind.to_string(),
        name,
        uid,
        controller: Some(true),
        block_owner_deletion: Some(false),
    })
}

/// Ensure `finalizer` is present on the object, patching it in if absent.
/// Returns `true` if a patch was issued (the caller should requeue rather than
/// proceed, so the next reconcile observes the finalizer).
pub async fn ensure_finalizer<K>(api: &Api<K>, obj: &K, finalizer: &str) -> Result<bool>
where
    K: Resource + DeserializeOwned + Clone + std::fmt::Debug,
{
    if obj.finalizers().iter().any(|f| f == finalizer) {
        return Ok(false);
    }
    let name = obj
        .meta()
        .name
        .clone()
        .ok_or_else(|| Error::Invariant("object has no name".into()))?;
    let mut finalizers = obj.finalizers().to_vec();
    finalizers.push(finalizer.to_string());
    let patch = serde_json::json!({ "metadata": { "finalizers": finalizers } });
    api.patch(
        &name,
        &PatchParams::apply(FIELD_MANAGER),
        &Patch::Merge(&patch),
    )
    .await?;
    Ok(true)
}

/// Remove `finalizer` from the object (the last finalizer cleared lets the API
/// server complete deletion). A no-op if it is already absent.
pub async fn remove_finalizer<K>(api: &Api<K>, obj: &K, finalizer: &str) -> Result<()>
where
    K: Resource + DeserializeOwned + Clone + std::fmt::Debug,
{
    if !obj.finalizers().iter().any(|f| f == finalizer) {
        return Ok(());
    }
    let name = obj
        .meta()
        .name
        .clone()
        .ok_or_else(|| Error::Invariant("object has no name".into()))?;
    let finalizers: Vec<String> = obj
        .finalizers()
        .iter()
        .filter(|f| *f != finalizer)
        .cloned()
        .collect();
    // A JSON-merge `null` would clear nothing extra; set the explicit array.
    let patch = serde_json::json!({ "metadata": { "finalizers": finalizers } });
    api.patch(
        &name,
        &PatchParams::apply(FIELD_MANAGER),
        &Patch::Merge(&patch),
    )
    .await?;
    Ok(())
}

/// Standard kopiur labels for a child object (origin/config/snapshot). ALWAYS
/// includes `app.kubernetes.io/managed-by=kopiur` (ADR-0005 §14(c)) so every
/// operator-created object is recognized as controller-owned by Argo/Flux (and so
/// is never pruned / reported `OutOfSync`). `extra` overlays additional labels.
pub fn child_labels(extra: &[(&str, &str)]) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(
        crate::consts::MANAGED_BY_LABEL.to_string(),
        crate::consts::MANAGED_BY_VALUE.to_string(),
    );
    for (k, v) in extra {
        labels.insert(k.to_string(), v.to_string());
    }
    labels
}

/// Build a bare [`ObjectMeta`] with name+namespace+labels+owner (helper for
/// reconcilers creating child CRs like scheduled/discovered Snapshots).
pub fn child_meta(
    name: &str,
    namespace: &str,
    labels: BTreeMap<String, String>,
    owner: Option<OwnerReference>,
) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(namespace.to_string()),
        labels: if labels.is_empty() {
            None
        } else {
            Some(labels)
        },
        owner_references: owner.map(|o| vec![o]),
        ..Default::default()
    }
}

/// Upsert a status condition by `type_`, returning the full conditions vector to
/// patch. An existing condition of the same `type_` keeps its
/// `lastTransitionTime` while its `status` is unchanged (the timestamp marks the
/// last real transition, per the Kubernetes condition convention) and gets a
/// fresh one on a flip or first set. Other conditions are preserved unchanged.
pub fn upsert_condition(
    existing: &[Condition],
    type_: &str,
    status: bool,
    reason: &str,
    message: &str,
    observed_generation: Option<i64>,
) -> Vec<Condition> {
    let status_str = if status { "True" } else { "False" };
    let prior = existing.iter().find(|c| c.type_ == type_);
    let last_transition_time = match prior {
        Some(c) if c.status == status_str => c.last_transition_time.clone(),
        _ => Time(k8s_openapi::jiff::Timestamp::now()),
    };
    let updated = Condition {
        type_: type_.to_string(),
        status: status_str.to_string(),
        reason: reason.to_string(),
        message: message.to_string(),
        last_transition_time,
        observed_generation,
    };
    existing
        .iter()
        .filter(|c| c.type_ != type_)
        .cloned()
        .chain(std::iter::once(updated))
        .collect()
}

/// The kstatus outcome a reconcile reports via [`set_ready`] (ADR-0005 §2).
/// Closed enum so the Ready/Reconciling/Stalled mapping is exhaustive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyOutcome {
    /// The resource reached its desired state: `Ready=True`, `Reconciling=False`,
    /// `Stalled=False`.
    Ready,
    /// A reconcile is in progress (not yet Ready, but not stuck): `Ready=False`,
    /// `Reconciling=True`, `Stalled=False`.
    Reconciling,
    /// A terminal error: the resource won't progress without a spec change
    /// (mapped from `ErrorClass::Terminal`). `Ready=False`, `Reconciling=False`,
    /// `Stalled=True`.
    Stalled,
}

/// Upsert the standard kstatus conditions (`Ready`, `Reconciling`, `Stalled`) for
/// `outcome` onto `existing`, preserving each condition's transition time when its
/// status is unchanged and flipping it on a real change (delegating to
/// [`upsert_condition`]). This is what makes `kubectl wait --for=condition=Ready`
/// and Flux/Argo health checks work against every kopiur CRD (ADR-0005 §2). The
/// `observedGeneration` is stamped on each condition. `reason`/`message` describe
/// the current state for humans (and machine-readable `reason`).
///
/// Every reconciled CRD calls this at the end of a reconcile with the outcome
/// derived from its phase (where one exists) or its domain conditions.
pub fn set_ready(
    existing: &[Condition],
    generation: Option<i64>,
    outcome: ReadyOutcome,
    reason: &str,
    message: &str,
) -> Vec<Condition> {
    let (ready, reconciling, stalled) = match outcome {
        ReadyOutcome::Ready => (true, false, false),
        ReadyOutcome::Reconciling => (false, true, false),
        ReadyOutcome::Stalled => (false, false, true),
    };
    use crate::consts::{READY_CONDITION, RECONCILING_CONDITION, STALLED_CONDITION};
    // Reason strings for the non-headline conditions are derived from the outcome so
    // they're always non-empty (the Condition contract requires a reason).
    let conds = upsert_condition(
        existing,
        READY_CONDITION,
        ready,
        reason,
        message,
        generation,
    );
    let conds = upsert_condition(
        &conds,
        RECONCILING_CONDITION,
        reconciling,
        if reconciling { reason } else { "Settled" },
        message,
        generation,
    );
    upsert_condition(
        &conds,
        STALLED_CONDITION,
        stalled,
        if stalled { reason } else { "NotStalled" },
        message,
        generation,
    )
}
