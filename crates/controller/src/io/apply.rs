use kube::api::{Patch, PatchParams};
use kube::{Api, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::Result;

/// The field-manager used for every server-side apply the controller performs.
pub const FIELD_MANAGER: &str = "kopiur.home-operations.com/controller";

/// Server-side apply an object into the given namespaced API. Idempotent: the
/// controller owns the fields it sets; reapplying converges. Uses
/// [`FIELD_MANAGER`] with `force` so the controller reliably re-takes ownership
/// of fields after a restart (ADR §5.2).
pub async fn apply<K>(api: &Api<K>, name: &str, obj: &K) -> Result<K>
where
    K: Resource + Serialize + DeserializeOwned + Clone + std::fmt::Debug,
{
    let pp = PatchParams::apply(FIELD_MANAGER).force();
    Ok(api.patch(name, &pp, &Patch::Apply(obj)).await?)
}

/// Patch an object's `.status` subresource with a strategic-merge body.
pub async fn patch_status<K>(api: &Api<K>, name: &str, status: serde_json::Value) -> Result<()>
where
    K: Resource + DeserializeOwned + Clone + std::fmt::Debug,
{
    let body = serde_json::json!({ "status": status });
    let pp = PatchParams::apply(FIELD_MANAGER);
    api.patch_status(name, &pp, &Patch::Merge(&body)).await?;
    Ok(())
}

/// Whether merge-patching `desired` over `current` would be a no-op — i.e. every
/// key in `desired` already holds the same value in `current`. `current` is the
/// object's existing `.status` serialized to JSON (or `None` when there is no
/// status yet, which is never a no-op).
///
/// This is the predicate behind [`patch_status_if_changed`]. It deliberately only
/// inspects the keys present in `desired` (a strategic merge never removes the
/// keys it omits), so a reconciler that patches a *subset* of status compares only
/// that subset.
pub fn status_patch_is_noop(
    current: Option<&serde_json::Value>,
    desired: &serde_json::Value,
) -> bool {
    let (Some(current), Some(desired_obj)) = (current, desired.as_object()) else {
        return false;
    };
    let Some(current_obj) = current.as_object() else {
        return false;
    };
    desired_obj
        .iter()
        .all(|(k, v)| current_obj.get(k) == Some(v))
}

/// Idempotent status patch: skip the PATCH entirely when `desired` matches the
/// object's existing status (`current`), returning `false`; otherwise merge-patch
/// and return `true`.
///
/// This is what breaks the reconcile hot-loop: a controller that re-writes an
/// unchanged `Failed` status would bump `resourceVersion`, emit a watch event, and
/// re-trigger itself in a tight loop. Skipping the no-op write means no event, no
/// re-trigger. For this to hold, the `desired` status must be byte-stable across
/// repeated identical failures — hence the condition message comes from
/// [`kopiur_kopia::KopiaErrorClass::summary`] (volatile-free) and
/// [`crate::io::upsert_condition`] preserves `lastTransitionTime` while the status is
/// unchanged. The returned bool lets the caller fire its Warning Event only on a
/// real transition.
pub async fn patch_status_if_changed<K>(
    api: &Api<K>,
    name: &str,
    current: Option<&serde_json::Value>,
    desired: serde_json::Value,
) -> Result<bool>
where
    K: Resource + DeserializeOwned + Clone + std::fmt::Debug,
{
    if status_patch_is_noop(current, &desired) {
        return Ok(false);
    }
    patch_status(api, name, desired).await?;
    Ok(true)
}

/// Whether `status` already records a **terminal** `Failed` for the given spec
/// `generation` — i.e. the reconciler hard-stopped on a non-retryable failure and
/// nothing in the spec has changed since (`observedGeneration == generation`).
///
/// A repository reconciler checks this before re-reading secrets or re-connecting
/// to the backend: once terminal for the current generation, it returns a quiet
/// heartbeat instead of re-hitting a backend that cannot succeed until the user
/// edits the CR (which bumps `metadata.generation` and reopens the gate). Only
/// `Failed` is treated as terminal — `Degraded` (a *retryable* failure) keeps
/// retrying on the transient cadence.
pub fn is_terminal_for_generation(
    phase: Option<kopiur_api::RepositoryPhase>,
    observed_generation: Option<i64>,
    generation: Option<i64>,
) -> bool {
    generation.is_some()
        && phase == Some(kopiur_api::RepositoryPhase::Failed)
        && observed_generation == generation
}

/// Whether the terminal-failure hard-stop still holds — i.e. we should return a
/// quiet heartbeat instead of re-attempting the backend.
///
/// Extends [`is_terminal_for_generation`] with a *credential* check: a terminal
/// failure means "won't succeed until an **input** changes", and the inputs are
/// the spec (`generation`) **and** the referenced password Secret. A Secret
/// content edit does NOT bump `metadata.generation`, so gating on generation alone
/// parks the object forever even after the user fixes the credential. We therefore
/// also reopen the gate when the password Secret's `resourceVersion` differs from
/// the one (`recorded_version`) observed at the last failed connect — `current_version`
/// is the Secret's live `resourceVersion`, read cheaply before this check.
///
/// Holds (skip the backend) only when BOTH are unchanged: terminal for this
/// generation AND the credential is byte-for-byte the same Secret revision. Any
/// difference (including a first failure that recorded no version) reopens it.
pub fn terminal_gate_holds(
    phase: Option<kopiur_api::RepositoryPhase>,
    observed_generation: Option<i64>,
    generation: Option<i64>,
    recorded_version: Option<&str>,
    current_version: &str,
) -> bool {
    is_terminal_for_generation(phase, observed_generation, generation)
        && recorded_version == Some(current_version)
}
