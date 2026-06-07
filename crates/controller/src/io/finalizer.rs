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

/// Standard kopiur labels for a child object (origin/config/snapshot).
pub fn child_labels(extra: &[(&str, &str)]) -> BTreeMap<String, String> {
    extra
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Build a bare [`ObjectMeta`] with name+namespace+labels+owner (helper for
/// reconcilers creating child CRs like scheduled/discovered Backups).
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
