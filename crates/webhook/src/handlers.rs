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

use api::backup::{BackupSpec, Origin};
use api::backup_config::BackupConfigSpec;
use api::backup_schedule::BackupScheduleSpec;
use api::cluster_repository::ClusterRepositorySpec;
use api::common::{DeletionPolicy, RepositoryKind, RepositoryRef};
use api::error::ValidationError;
use api::maintenance::MaintenanceSpec;
use api::repository::RepositorySpec;
use api::restore::RestoreSpec;

use crate::tenancy::{self, TenancyDecision};
use json_patch::{jsonptr::PointerBuf, AddOperation, Patch, PatchOperation};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::core::admission::{AdmissionRequest, AdmissionResponse};
use kube::core::DynamicObject;
use kube::Client;
use serde_json::{json, Value};

/// The finalizer that ties a kopia snapshot's lifecycle to its `Backup` CR (ADR §4.5).
pub const SNAPSHOT_CLEANUP_FINALIZER: &str = "kopia.io/snapshot-cleanup";

/// Dispatch a decoded `AdmissionRequest` to the handler for its `kind`.
///
/// Unknown kinds are allowed (the webhook only registers for kopia.io kinds; an
/// unexpected kind reaching us is not a reason to block the cluster). The `client`
/// is used only for `ClusterRepository` tenancy resolution; `None` forces those
/// checks to fail closed.
pub async fn dispatch(
    req: &AdmissionRequest<DynamicObject>,
    client: Option<&Client>,
) -> AdmissionResponse {
    let base = AdmissionResponse::from(req);
    match req.kind.kind.as_str() {
        "BackupConfig" => handle_backup_config(req, base, client).await,
        "Backup" => handle_backup(req, base),
        "BackupSchedule" => handle_backup_schedule(req, base),
        "Restore" => handle_restore(req, base, client).await,
        "Maintenance" => handle_maintenance(req, base, client).await,
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
/// discovered `Backup`) still decode.
fn decode_spec<T: serde::de::DeserializeOwned>(data: &Value) -> Result<T, serde_json::Error> {
    let spec = data
        .get("spec")
        .cloned()
        .unwrap_or(Value::Object(Default::default()));
    serde_json::from_value(spec)
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
fn ensure_finalizer_ops(meta: &ObjectMeta, ops: &mut Vec<PatchOperation>) {
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

// --- BackupConfig -----------------------------------------------------------

async fn handle_backup_config(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
    client: Option<&Client>,
) -> AdmissionResponse {
    let obj = match raw_object(req) {
        Ok(o) => o,
        Err(m) => return resp.deny(m),
    };
    let spec: BackupConfigSpec = match decode_spec(&obj.data) {
        Ok(s) => s,
        Err(e) => return resp.deny(format!("failed to decode BackupConfig spec: {e}")),
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

    resp
}

// --- Backup -----------------------------------------------------------------

fn handle_backup(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
) -> AdmissionResponse {
    let obj = match raw_object(req) {
        Ok(o) => o,
        Err(m) => return resp.deny(m),
    };
    let spec: BackupSpec = match decode_spec(&obj.data) {
        Ok(s) => s,
        Err(e) => return resp.deny(format!("failed to decode Backup spec: {e}")),
    };

    let origin = backup_origin(&obj.metadata, &obj.data);

    let errs = api::validate::validate_backup(&spec, origin);
    if !errs.is_empty() {
        return resp.deny(join_errors(&errs));
    }

    // Manual backups carrying a configRef to a ClusterRepository are not expressible
    // here (Backup.spec has no repository field — it derives from the configRef), so
    // there is no inline ClusterRepository ref to gate on a Backup. Tenancy for the
    // recipe is enforced on the BackupConfig. We still default deletionPolicy and the
    // finalizer.

    let mut ops = Vec::new();

    // Origin-aware default deletionPolicy when absent (ADR §4.5):
    //   discovered → forced Retain; produced (scheduled/manual) → Delete.
    // (BackupConfig.defaultDeletionPolicy inheritance is the controller's job once it
    // resolves the configRef; the webhook only sets the safe origin-aware default.)
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

    // Every Backup carries the snapshot-cleanup finalizer (ADR §4.5).
    ensure_finalizer_ops(&obj.metadata, &mut ops);

    with_patch_or_deny(resp, ops)
}

/// Resolve a `Backup`'s origin from the `kopia.io/origin` label (canonical) or
/// `status.origin`, defaulting to `manual` for user-created backups with no marker.
fn backup_origin(meta: &ObjectMeta, data: &Value) -> Origin {
    let from_label = meta
        .labels
        .as_ref()
        .and_then(|l| l.get("kopia.io/origin"))
        .map(|s| s.as_str());
    let from_status = data
        .get("status")
        .and_then(|s| s.get("origin"))
        .and_then(|v| v.as_str());
    match from_label.or(from_status) {
        Some("discovered") => Origin::Discovered,
        Some("scheduled") => Origin::Scheduled,
        // A user `kubectl create`-ing a Backup with no origin marker is manual.
        _ => Origin::Manual,
    }
}

// --- BackupSchedule ---------------------------------------------------------

fn handle_backup_schedule(
    req: &AdmissionRequest<DynamicObject>,
    resp: AdmissionResponse,
) -> AdmissionResponse {
    let obj = match raw_object(req) {
        Ok(o) => o,
        Err(m) => return resp.deny(m),
    };
    let data = &obj.data;
    let spec: BackupScheduleSpec = match decode_spec(data) {
        Ok(s) => s,
        Err(e) => return resp.deny(format!("failed to decode BackupSchedule spec: {e}")),
    };

    let errs = api::validate::validate_backup_schedule(&spec);
    if !errs.is_empty() {
        return resp.deny(join_errors(&errs));
    }

    // Make the GitOps-safe defaults explicit on the object (ADR §4.1, G3/G5):
    //   schedule.runOnCreate = false, schedule.concurrencyPolicy = Forbid.
    // serde already defaults these when deserializing, but they are skip-serialized,
    // so they are absent on the stored object unless we pin them via the patch.
    let mut ops = Vec::new();
    let schedule = data.get("spec").and_then(|s| s.get("schedule"));
    let run_on_create_absent = schedule
        .map(|s| s.get("runOnCreate").is_none())
        .unwrap_or(true);
    let concurrency_absent = schedule
        .map(|s| s.get("concurrencyPolicy").is_none())
        .unwrap_or(true);

    // spec.schedule is required by the schema; if it somehow decoded but is absent in
    // raw JSON we skip defaulting (validation above would have caught a missing cron).
    if schedule.is_some() {
        if run_on_create_absent {
            ops.push(PatchOperation::Add(AddOperation {
                path: ptr("/spec/schedule/runOnCreate"),
                value: json!(false),
            }));
        }
        if concurrency_absent {
            ops.push(PatchOperation::Add(AddOperation {
                path: ptr("/spec/schedule/concurrencyPolicy"),
                value: json!("Forbid"),
            }));
        }
    }

    with_patch_or_deny(resp, ops)
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

    if let Some(repo) = &spec.repository {
        if let TenancyDecision::Deny(msg) =
            tenancy_for(repo, req.namespace.as_deref(), client).await
        {
            return resp.deny(msg);
        }
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

    let errs = api::validate::validate_cluster_repository(&spec);
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

    if let Err(e) = api::validate::validate_repository_no_inline_retention(&spec) {
        return resp.deny(e.to_string());
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
/// object had no spec object at all (an empty discovered `Backup`). We use a `test`
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
