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

use crate::tenancy::{self, TenancyDecision};
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
pub async fn dispatch(
    req: &AdmissionRequest<DynamicObject>,
    client: Option<&Client>,
) -> AdmissionResponse {
    let base = AdmissionResponse::from(req);
    match req.kind.kind.as_str() {
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
            base
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
fn raw_object(req: &AdmissionRequest<DynamicObject>) -> Result<&DynamicObject, &'static str> {
    match &req.object {
        Some(obj) => Ok(obj),
        None => Err("admission request carried no object to validate"),
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

/// Join a validation-error vec into a single user-facing rejection message.
fn join_errors(errs: &[ValidationError]) -> String {
    errs.iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

/// Apply a JSON patch to a response, denying (fail closed) if serialization fails.
fn with_patch_or_deny(resp: AdmissionResponse, ops: Vec<PatchOperation>) -> AdmissionResponse {
    if ops.is_empty() {
        return resp;
    }
    match resp.clone().with_patch(Patch(ops)) {
        Ok(r) => r,
        Err(e) => resp.deny(format!("internal error building admission patch: {e}")),
    }
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
) -> AdmissionResponse {
    let obj = match raw_object(req) {
        Ok(o) => o,
        Err(m) => return resp.deny(m),
    };
    let spec: SnapshotPolicySpec = match decode_spec(&obj.data) {
        Ok(s) => s,
        Err(e) => return resp.deny(format!("failed to decode SnapshotPolicy spec: {e}")),
    };

    let errs = api::validate::validate_backup_config(&spec);
    if !errs.is_empty() {
        return resp.deny(join_errors(&errs));
    }

    // ClusterRepository tenancy (fail closed). namespace comes from the request.
    if let TenancyDecision::Deny(msg) =
        tenancy_for(&spec.repository, req.namespace.as_deref(), client).await
    {
        return resp.deny(msg);
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
            return resp.deny(
                ValidationError::IdentityCollision {
                    identity: collision.identity,
                    conflict: collision.conflict,
                }
                .to_string(),
            );
        }
    }

    resp
}

// --- Snapshot -----------------------------------------------------------------

fn handle_snapshot(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
) -> AdmissionResponse {
    let obj = match raw_object(req) {
        Ok(o) => o,
        Err(m) => return resp.deny(m),
    };
    let spec: SnapshotSpec = match decode_spec(&obj.data) {
        Ok(s) => s,
        Err(e) => return resp.deny(format!("failed to decode Snapshot spec: {e}")),
    };

    let origin = backup_origin(&obj.metadata, &obj.data);

    let errs = api::validate::validate_backup(&spec, origin);
    if !errs.is_empty() {
        return resp.deny(join_errors(&errs));
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

    with_patch_or_deny(resp, ops)
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
) -> AdmissionResponse {
    let obj = match raw_object(req) {
        Ok(o) => o,
        Err(m) => return resp.deny(m),
    };
    let data = &obj.data;
    let spec: SnapshotScheduleSpec = match decode_spec(data) {
        Ok(s) => s,
        Err(e) => return resp.deny(format!("failed to decode SnapshotSchedule spec: {e}")),
    };

    let errs = api::validate::validate_backup_schedule(&spec);
    if !errs.is_empty() {
        return resp.deny(join_errors(&errs));
    }

    // No spec-mutating defaulting here: `schedule.runOnCreate` (false) and
    // `schedule.concurrencyPolicy` (Forbid) now carry real OpenAPI `default:`s in the
    // CRD schema (ADR-0005 §1), so the apiserver materializes them. The webhook writes
    // no user spec (the status-only-write invariant, ADR-0005 §14(d)) — a write-back
    // into spec makes Argo/Flux perpetually `OutOfSync`.
    resp
}

// --- Restore ----------------------------------------------------------------

async fn handle_restore(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
    client: Option<&Client>,
) -> AdmissionResponse {
    let obj = match raw_object(req) {
        Ok(o) => o,
        Err(m) => return resp.deny(m),
    };
    let spec: RestoreSpec = match decode_spec(&obj.data) {
        Ok(s) => s,
        Err(e) => return resp.deny(format!("failed to decode Restore spec: {e}")),
    };

    let errs = api::validate::validate_restore_spec(&spec);
    if !errs.is_empty() {
        return resp.deny(join_errors(&errs));
    }

    if let Some(repo) = &spec.repository
        && let TenancyDecision::Deny(msg) =
            tenancy_for(repo, req.namespace.as_deref(), client).await
    {
        return resp.deny(msg);
    }

    resp
}

// --- Maintenance ------------------------------------------------------------

async fn handle_maintenance(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
    client: Option<&Client>,
) -> AdmissionResponse {
    let obj = match raw_object(req) {
        Ok(o) => o,
        Err(m) => return resp.deny(m),
    };
    let spec: MaintenanceSpec = match decode_spec(&obj.data) {
        Ok(s) => s,
        Err(e) => return resp.deny(format!("failed to decode Maintenance spec: {e}")),
    };

    let errs = api::validate::validate_maintenance(&spec);
    if !errs.is_empty() {
        return resp.deny(join_errors(&errs));
    }

    if let TenancyDecision::Deny(msg) =
        tenancy_for(&spec.repository, req.namespace.as_deref(), client).await
    {
        return resp.deny(msg);
    }

    resp
}

// --- RepositoryReplication --------------------------------------------------

async fn handle_repository_replication(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
    client: Option<&Client>,
) -> AdmissionResponse {
    let obj = match raw_object(req) {
        Ok(o) => o,
        Err(m) => return resp.deny(m),
    };
    let spec: RepositoryReplicationSpec = match decode_spec(&obj.data) {
        Ok(s) => s,
        Err(e) => return resp.deny(format!("failed to decode RepositoryReplication spec: {e}")),
    };

    let errs = api::validate::validate_repository_replication(&spec);
    if !errs.is_empty() {
        return resp.deny(join_errors(&errs));
    }

    // Tenancy: a ClusterRepository sourceRef is gated against allowedNamespaces.
    if let TenancyDecision::Deny(msg) =
        tenancy_for(&spec.source_ref, req.namespace.as_deref(), client).await
    {
        return resp.deny(msg);
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
        return resp.deny(
            ValidationError::ReplicationDestinationSameAsSource {
                backend: spec.destination.kind_str().to_string(),
            }
            .to_string(),
        );
    }

    resp
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
) -> AdmissionResponse {
    let obj = match raw_object(req) {
        Ok(o) => o,
        Err(m) => return resp.deny(m),
    };
    let spec: ClusterRepositorySpec = match decode_spec(&obj.data) {
        Ok(s) => s,
        Err(e) => return resp.deny(format!("failed to decode ClusterRepository spec: {e}")),
    };

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
        return resp.deny(join_errors(&errs));
    }
    resp
}

// --- Repository -------------------------------------------------------------

fn handle_repository(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
) -> AdmissionResponse {
    let obj = match raw_object(req) {
        Ok(o) => o,
        Err(m) => return resp.deny(m),
    };
    let spec: RepositorySpec = match decode_spec(&obj.data) {
        Ok(s) => s,
        Err(e) => return resp.deny(format!("failed to decode Repository spec: {e}")),
    };

    let mut errs = api::validate::validate_repository(&spec);
    // Create-time immutability (ADR-0005 §7), UPDATE-only.
    if req.operation == Operation::Update
        && let Some(old) = decode_old_spec::<RepositorySpec>(req)
    {
        errs.extend(api::validate::validate_repository_immutability(&old, &spec));
    }
    if !errs.is_empty() {
        return resp.deny(join_errors(&errs));
    }
    resp
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
                return TenancyDecision::Deny(
                    "consumer namespace was not provided in the admission request; cannot \
                     evaluate ClusterRepository tenancy (fail-closed)"
                        .to_string(),
                );
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
