//! One validating + mutating admission handler per CRD (ADR §5.3, §2.2 principle 8).
//!
//! Each handler follows the same shape, so behavior is consistent and every handler
//! is a thin adapter over the shared `kopiur_api::validate` validators — **no forked
//! validation logic** (SKILL hard-rule 4):
//!
//! 1. Decode the typed spec from the incoming `DynamicObject` (`object.data["spec"]`).
//!    A decode failure denies with a clear message (fail closed).
//! 2. Run the corresponding `validate_*` aggregate. A non-empty `Vec<ValidationError>`
//!    denies with **all** messages joined, so a user sees every problem in one apply.
//! 3. Apply mutating defaults as a JSON patch (RFC 6902) on the `AdmissionResponse`
//!    via `AdmissionResponse::with_patch`.
//! 4. For consumer CRs referencing a `ClusterRepository`, enforce the
//!    `allowedNamespaces` tenancy gate (fail closed) — see [`crate::tenancy`].

use kopiur_api as api;

use api::cluster_repository::ClusterRepositorySpec;
use api::common::{DeletionPolicy, RepositoryKind, RepositoryRef};
use api::error::ValidationError;
use api::maintenance::MaintenanceSpec;
use api::repository::RepositorySpec;
use api::repository_replication::RepositoryReplicationSpec;
use api::restore::RestoreSpec;
use api::snapshot::{Origin, SnapshotSpec};
use api::snapshot_policy::SnapshotPolicySpec;
use api::snapshot_schedule::SnapshotScheduleSpec;

use crate::error::{AdmissionError, AdmissionResult};
use crate::tenancy::{self, TenancyDecision, TenancyDenial};
use json_patch::{AddOperation, Patch, PatchOperation, jsonptr::PointerBuf};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Client;
use kube::core::DynamicObject;
use kube::core::admission::{AdmissionRequest, AdmissionResponse, Operation};
use serde_json::{Value, json};

/// The finalizer that ties a kopia snapshot's lifecycle to its `Snapshot` CR (ADR §4.5).
pub const SNAPSHOT_CLEANUP_FINALIZER: &str = "kopiur.home-operations.com/snapshot-cleanup";

/// Dispatch a decoded `AdmissionRequest` to the handler for its `kind`.
///
/// Unknown kinds are allowed (the webhook only registers for kopiur.home-operations.com kinds; an
/// unexpected kind reaching us is not a reason to block the cluster). The `client`
/// is used only for `ClusterRepository` tenancy resolution; `None` forces those
/// checks to fail closed.
///
/// This is the **single deny choke point** (ADR §5.5): every handler returns
/// `Result<AdmissionResponse, AdmissionError>`, and only this `match` turns a
/// typed [`AdmissionError`] into `AdmissionResponse::deny` — grep for `.deny(`
/// and this is the one production call. The denial is logged with its stable
/// [`AdmissionError::reason`] label.
pub async fn dispatch(
    req: &AdmissionRequest<DynamicObject>,
    client: Option<&Client>,
) -> AdmissionResponse {
    let base = AdmissionResponse::from(req);
    let result = match req.kind.kind.as_str() {
        "SnapshotPolicy" => handle_snapshot_policy(req, base, client).await,
        "Snapshot" => handle_snapshot(req, base),
        "SnapshotSchedule" => handle_snapshot_schedule(req, base),
        "Restore" => handle_restore(req, base, client).await,
        "Maintenance" => handle_maintenance(req, base, client).await,
        "RepositoryReplication" => handle_repository_replication(req, base, client).await,
        "ClusterRepository" => handle_cluster_repository(req, base),
        "Repository" => handle_repository(req, base),
        other => {
            tracing::warn!(
                kind = other,
                "admission request for unregistered kind; allowing"
            );
            Ok(base)
        }
    };
    match result {
        Ok(resp) => resp,
        Err(err) => {
            tracing::info!(
                kind = %req.kind.kind,
                name = %req.name,
                namespace = req.namespace.as_deref().unwrap_or(""),
                reason = err.reason(),
                error = %err,
                "denying admission"
            );
            AdmissionResponse::from(req).deny(err.to_string())
        }
    }
}

// --- decode helpers ---------------------------------------------------------

/// Extract the incoming object from the request, denying if absent.
///
/// Note: a `DynamicObject` splits the wire object into `metadata` (typed
/// [`ObjectMeta`]) and `data` (everything else: `spec`, `status`). `apiVersion`/
/// `kind` land in `types`. So spec lives in `obj.data["spec"]` and labels/finalizers
/// live in `obj.metadata`, NOT in `data`.
fn raw_object(req: &AdmissionRequest<DynamicObject>) -> AdmissionResult<&DynamicObject> {
    match &req.object {
        Some(obj) => Ok(obj),
        None => Err(AdmissionError::MissingObject),
    }
}

/// Deserialize `object.data["spec"]` into a typed spec `T`. A missing `spec`
/// deserializes from `null`/`{}` so specs that are entirely optional (e.g. a
/// discovered `Snapshot`) still decode.
fn decode_spec<T: serde::de::DeserializeOwned>(data: &Value) -> Result<T, serde_json::Error> {
    let spec = data
        .get("spec")
        .cloned()
        .unwrap_or(Value::Object(Default::default()));
    serde_json::from_value(spec)
}

/// Decode the OLD object's typed spec `T` from an UPDATE admission request, if
/// present. `None` for CREATE (no old object) or when the old object carries no
/// decodable spec. Used by the create-time-immutability checks (ADR-0005 §7), which
/// only run on UPDATE.
fn decode_old_spec<T: serde::de::DeserializeOwned>(
    req: &AdmissionRequest<DynamicObject>,
) -> Option<T> {
    let old = req.old_object.as_ref()?;
    decode_spec(&old.data).ok()
}

/// Apply a JSON patch to a response; a serialization failure is the typed
/// [`AdmissionError::InternalPatch`] (fail closed at the dispatch choke point).
fn with_patch(resp: AdmissionResponse, ops: Vec<PatchOperation>) -> AdmissionResult {
    if ops.is_empty() {
        return Ok(resp);
    }
    resp.with_patch(Patch(ops))
        .map_err(|source| AdmissionError::InternalPatch { source })
}

fn ptr(path: &str) -> PointerBuf {
    PointerBuf::parse(path).expect("static JSON pointer is valid")
}

/// `metadata.finalizers` may be absent. Build a patch op that appends the snapshot
/// finalizer without clobbering existing finalizers.
///
/// Never (re-)add the finalizer to an object already being deleted
/// (`deletionTimestamp` set): the controller's deletion path PATCHes the object to
/// REMOVE this finalizer, and that PATCH is itself an UPDATE admission — re-adding
/// here would immediately undo the removal, so the snapshot-cleanup finalizer
/// could never clear and the `Snapshot` CR would never be garbage-collected.
fn ensure_finalizer_ops(meta: &ObjectMeta, ops: &mut Vec<PatchOperation>) {
    if meta.deletion_timestamp.is_some() {
        return;
    }
    match &meta.finalizers {
        None => {
            // No finalizers array at all: create it with our finalizer.
            ops.push(PatchOperation::Add(AddOperation {
                path: ptr("/metadata/finalizers"),
                value: json!([SNAPSHOT_CLEANUP_FINALIZER]),
            }));
        }
        Some(existing) => {
            if !existing.iter().any(|f| f == SNAPSHOT_CLEANUP_FINALIZER) {
                // Append to the end of the existing array (RFC 6902 "-" token).
                ops.push(PatchOperation::Add(AddOperation {
                    path: ptr("/metadata/finalizers/-"),
                    value: json!(SNAPSHOT_CLEANUP_FINALIZER),
                }));
            }
        }
    }
}

// --- SnapshotPolicy -----------------------------------------------------------

async fn handle_snapshot_policy(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
    client: Option<&Client>,
) -> AdmissionResult {
    let obj = raw_object(req)?;
    let spec: SnapshotPolicySpec =
        decode_spec(&obj.data).map_err(|source| AdmissionError::SpecDecode {
            kind: "SnapshotPolicy",
            source,
        })?;

    let errs = api::validate::validate_backup_config(&spec);
    if !errs.is_empty() {
        return Err(AdmissionError::Invalid(errs));
    }

    // ClusterRepository tenancy (fail closed). namespace comes from the request.
    if let TenancyDecision::Deny(denial) =
        tenancy_for(&spec.repository, req.namespace.as_deref(), client).await
    {
        return Err(denial.into());
    }

    // Identity-collision detection (ADR-0005 §6): reject a SnapshotPolicy whose
    // resolved `username@hostname[:path]` identity collides with an already-admitted
    // policy's identity in the same repository — two recipes must not interleave
    // snapshots into one kopia identity. The IO is best-effort (fails open), so a
    // transient list/get error never wedges an apply.
    if let Some(ns) = req.namespace.as_deref() {
        let name = obj.metadata.name.as_deref().unwrap_or(req.name.as_str());
        if let Some(collision) = crate::identity_collision::check_identity_collision(
            client,
            name,
            ns,
            &spec,
            obj.metadata.labels.as_ref(),
            obj.metadata.annotations.as_ref(),
        )
        .await
        {
            return Err(AdmissionError::Invalid(vec![
                ValidationError::IdentityCollision {
                    identity: collision.identity,
                    conflict: collision.conflict,
                },
            ]));
        }
    }

    Ok(resp)
}

// --- Snapshot -----------------------------------------------------------------

fn handle_snapshot(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
) -> AdmissionResult {
    let obj = raw_object(req)?;
    let spec: SnapshotSpec =
        decode_spec(&obj.data).map_err(|source| AdmissionError::SpecDecode {
            kind: "Snapshot",
            source,
        })?;

    let origin = backup_origin(&obj.metadata, &obj.data);

    let errs = api::validate::validate_backup(&spec, origin);
    if !errs.is_empty() {
        return Err(AdmissionError::Invalid(errs));
    }

    // Manual backups carrying a policyRef to a ClusterRepository are not expressible
    // here (Snapshot.spec has no repository field — it derives from the policyRef), so
    // there is no inline ClusterRepository ref to gate on a Snapshot. Tenancy for the
    // recipe is enforced on the SnapshotPolicy. We still default deletionPolicy and the
    // finalizer.

    let mut ops = Vec::new();

    // Origin-aware default deletionPolicy when absent (ADR §4.5):
    //   discovered → forced Retain; produced (scheduled/manual) → Delete.
    // (SnapshotPolicy.defaultDeletionPolicy inheritance is the controller's job once it
    // resolves the policyRef; the webhook only sets the safe origin-aware default.)
    if spec.deletion_policy.is_none() {
        let default = match origin {
            Origin::Discovered => DeletionPolicy::Retain,
            Origin::Scheduled | Origin::Manual => DeletionPolicy::Delete,
        };
        ops.push(set_spec_field(
            &obj.data,
            "deletionPolicy",
            serde_json::to_value(default).expect("DeletionPolicy serializes"),
        ));
    }

    // Every Snapshot carries the snapshot-cleanup finalizer (ADR §4.5).
    ensure_finalizer_ops(&obj.metadata, &mut ops);

    with_patch(resp, ops)
}

/// Resolve a `Snapshot`'s origin from the `kopiur.home-operations.com/origin` label (canonical) or
/// `status.origin`, defaulting to `manual` for user-created backups with no marker.
fn backup_origin(meta: &ObjectMeta, data: &Value) -> Origin {
    let from_label = meta
        .labels
        .as_ref()
        .and_then(|l| l.get("kopiur.home-operations.com/origin"))
        .map(|s| s.as_str());
    let from_status = data
        .get("status")
        .and_then(|s| s.get("origin"))
        .and_then(|v| v.as_str());
    match from_label.or(from_status) {
        Some("discovered") => Origin::Discovered,
        Some("scheduled") => Origin::Scheduled,
        // A user `kubectl create`-ing a Snapshot with no origin marker is manual.
        _ => Origin::Manual,
    }
}

// --- SnapshotSchedule ---------------------------------------------------------

fn handle_snapshot_schedule(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
) -> AdmissionResult {
    let obj = raw_object(req)?;
    let data = &obj.data;
    let spec: SnapshotScheduleSpec =
        decode_spec(data).map_err(|source| AdmissionError::SpecDecode {
            kind: "SnapshotSchedule",
            source,
        })?;

    let errs = api::validate::validate_backup_schedule(&spec);
    if !errs.is_empty() {
        return Err(AdmissionError::Invalid(errs));
    }

    // No spec-mutating defaulting here: `schedule.runOnCreate` (false) and
    // `schedule.concurrencyPolicy` (Forbid) now carry real OpenAPI `default:`s in the
    // CRD schema (ADR-0005 §1), so the apiserver materializes them. The webhook writes
    // no user spec (the status-only-write invariant, ADR-0005 §14(d)) — a write-back
    // into spec makes Argo/Flux perpetually `OutOfSync`.
    Ok(resp)
}

// --- Restore ----------------------------------------------------------------

async fn handle_restore(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
    client: Option<&Client>,
) -> AdmissionResult {
    let obj = raw_object(req)?;
    let spec: RestoreSpec =
        decode_spec(&obj.data).map_err(|source| AdmissionError::SpecDecode {
            kind: "Restore",
            source,
        })?;

    let errs = api::validate::validate_restore_spec(&spec);
    if !errs.is_empty() {
        return Err(AdmissionError::Invalid(errs));
    }

    if let Some(repo) = &spec.repository
        && let TenancyDecision::Deny(denial) =
            tenancy_for(repo, req.namespace.as_deref(), client).await
    {
        return Err(denial.into());
    }

    Ok(resp)
}

// --- Maintenance ------------------------------------------------------------

async fn handle_maintenance(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
    client: Option<&Client>,
) -> AdmissionResult {
    let obj = raw_object(req)?;
    let spec: MaintenanceSpec =
        decode_spec(&obj.data).map_err(|source| AdmissionError::SpecDecode {
            kind: "Maintenance",
            source,
        })?;

    let errs = api::validate::validate_maintenance(&spec);
    if !errs.is_empty() {
        return Err(AdmissionError::Invalid(errs));
    }

    if let TenancyDecision::Deny(denial) =
        tenancy_for(&spec.repository, req.namespace.as_deref(), client).await
    {
        return Err(denial.into());
    }

    Ok(resp)
}

// --- RepositoryReplication --------------------------------------------------

async fn handle_repository_replication(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
    client: Option<&Client>,
) -> AdmissionResult {
    let obj = raw_object(req)?;
    let spec: RepositoryReplicationSpec =
        decode_spec(&obj.data).map_err(|source| AdmissionError::SpecDecode {
            kind: "RepositoryReplication",
            source,
        })?;

    let errs = api::validate::validate_repository_replication(&spec);
    if !errs.is_empty() {
        return Err(AdmissionError::Invalid(errs));
    }

    // Tenancy: a ClusterRepository sourceRef is gated against allowedNamespaces.
    if let TenancyDecision::Deny(denial) =
        tenancy_for(&spec.source_ref, req.namespace.as_deref(), client).await
    {
        return Err(denial.into());
    }

    // §13(d): the destination must differ from the source's backend (no self-mirror).
    // Resolve the source backend via the client and compare. Best-effort — a missing
    // client / unresolvable source skips this one check (the structural validations
    // above already ran), mirroring how tenancy degrades when inputs are unavailable.
    if let Some(client) = client
        && let Some(source_backend) =
            resolve_source_backend(client, &spec.source_ref, req.namespace.as_deref()).await
        && !api::validate::replication_destination_differs(&source_backend, &spec.destination)
    {
        return Err(AdmissionError::Invalid(vec![
            ValidationError::ReplicationDestinationSameAsSource {
                backend: spec.destination.kind_str().to_string(),
            },
        ]));
    }

    Ok(resp)
}

/// Resolve a replication source's backend from its `RepositoryRef` (a namespaced
/// `Repository` or a cluster-scoped `ClusterRepository`). Returns `None` when the
/// repo can't be fetched (so the differs check is skipped rather than guessed).
async fn resolve_source_backend(
    client: &Client,
    source: &RepositoryRef,
    consumer_namespace: Option<&str>,
) -> Option<api::backend::Backend> {
    use kube::Api;
    match source.kind {
        RepositoryKind::Repository => {
            let ns = source.namespace.as_deref().or(consumer_namespace)?;
            let api: Api<api::Repository> = Api::namespaced(client.clone(), ns);
            api.get_opt(&source.name)
                .await
                .ok()
                .flatten()
                .map(|r| r.spec.backend)
        }
        RepositoryKind::ClusterRepository => {
            let api: Api<api::ClusterRepository> = Api::all(client.clone());
            api.get_opt(&source.name)
                .await
                .ok()
                .flatten()
                .map(|r| r.spec.backend)
        }
    }
}

// --- ClusterRepository ------------------------------------------------------

fn handle_cluster_repository(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
) -> AdmissionResult {
    let obj = raw_object(req)?;
    let spec: ClusterRepositorySpec =
        decode_spec(&obj.data).map_err(|source| AdmissionError::SpecDecode {
            kind: "ClusterRepository",
            source,
        })?;

    let mut errs = api::validate::validate_cluster_repository(&spec);
    // Create-time immutability (ADR-0005 §7): on UPDATE, reject changes to
    // `encryption`/`create.{splitter,hash,encryption}` — kopia bakes them into the
    // repository format. CREATE has no old object, so the check is UPDATE-only.
    if req.operation == Operation::Update
        && let Some(old) = decode_old_spec::<ClusterRepositorySpec>(req)
    {
        errs.extend(api::validate::validate_cluster_repository_immutability(
            &old, &spec,
        ));
    }
    if !errs.is_empty() {
        return Err(AdmissionError::Invalid(errs));
    }
    Ok(resp)
}

// --- Repository -------------------------------------------------------------

fn handle_repository(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
) -> AdmissionResult {
    let obj = raw_object(req)?;
    let spec: RepositorySpec =
        decode_spec(&obj.data).map_err(|source| AdmissionError::SpecDecode {
            kind: "Repository",
            source,
        })?;

    let mut errs = api::validate::validate_repository(&spec);
    // Create-time immutability (ADR-0005 §7), UPDATE-only.
    if req.operation == Operation::Update
        && let Some(old) = decode_old_spec::<RepositorySpec>(req)
    {
        errs.extend(api::validate::validate_repository_immutability(&old, &spec));
    }
    if !errs.is_empty() {
        return Err(AdmissionError::Invalid(errs));
    }
    Ok(resp)
}

// --- shared tenancy adapter -------------------------------------------------

/// Gate a consumer's `RepositoryRef` against `ClusterRepository` tenancy.
///
/// - `Repository` refs are not gated here (cross-namespace `Repository` references
///   are allowed and RBAC-gated elsewhere).
/// - `ClusterRepository` refs go through the **fail-closed** resolver
///   ([`tenancy::resolve_tenancy_inputs`]): it fetches the `ClusterRepository` + the
///   consumer namespace's labels and evaluates the gate. No client / unresolvable
///   inputs → deny.
async fn tenancy_for(
    repo: &RepositoryRef,
    consumer_namespace: Option<&str>,
    client: Option<&Client>,
) -> TenancyDecision {
    match repo.kind {
        RepositoryKind::Repository => TenancyDecision::Allow,
        RepositoryKind::ClusterRepository => {
            // validate_repository_ref already rejected a set namespace; the consumer's
            // own namespace is what the gate is evaluated against.
            let Some(ns) = consumer_namespace else {
                return TenancyDecision::Deny(TenancyDenial::NoConsumerNamespace);
            };
            tenancy::resolve_tenancy_inputs(client, ns, &repo.name).await
        }
    }
}

/// Build a JSON-patch op that sets `spec.<field>`, creating `/spec` first if the raw
/// object had no spec object at all (an empty discovered `Snapshot`). We use a `test`
/// guard only when `/spec` is known present to keep patches minimal.
fn set_spec_field(data: &Value, field: &str, value: Value) -> PatchOperation {
    if data.get("spec").and_then(|s| s.as_object()).is_some() {
        PatchOperation::Add(AddOperation {
            path: ptr(&format!("/spec/{field}")),
            value,
        })
    } else {
        // No spec object: add the whole spec with just this field.
        PatchOperation::Add(AddOperation {
            path: ptr("/spec"),
            value: json!({ field: value }),
        })
    }
}
