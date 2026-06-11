//! CSI VolumeSnapshot / clone **staging** for backups (`copyMethod: Snapshot` /
//! `Clone`, ADR §3.3).
//!
//! Instead of mounting the **live** source PVC, a staged backup captures a
//! point-in-time copy and runs the mover against *that*:
//!
//! - **`Snapshot`**: create a CSI `VolumeSnapshot` of the source PVC, wait for it to be
//!   `readyToUse`, then provision a temporary "staged" PVC whose `dataSource` is the
//!   snapshot. The mover mounts the staged PVC.
//! - **`Clone`**: provision the staged PVC directly from the source PVC
//!   (`dataSource: PersistentVolumeClaim`, a CSI volume clone) — no VolumeSnapshot.
//! - **`Direct`** (and any NFS source): no staging — the caller mounts the live source.
//!
//! Staging **decouples the backup from the source node** (the staged PVC is fresh and
//! unheld, so [`super::colocation`] resolves to "no pin") and gives a point-in-time,
//! crash-consistent capture (app-consistency uses `SnapshotPolicy.spec.hooks`).
//!
//! The cluster has no typed Rust binding for `snapshot.storage.k8s.io` (those are CRDs,
//! not core API), so VolumeSnapshots are created/read as [`kube::core::DynamicObject`]s
//! (the same approach the mover uses for status patches). Every **decision** is a pure,
//! unit-tested function ([`decide_class`], [`staged_pvc_size`], [`cleanup_plan`], the
//! builders); the async wrappers do only cluster IO and return a [`StagingOutcome`] the
//! reconciler maps to status/conditions/events — so this module stays free of status
//! side effects (mirroring [`super::colocation`]).

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    PersistentVolume, PersistentVolumeClaim, PersistentVolumeClaimSpec, Pod,
    TypedLocalObjectReference, VolumeResourceRequirements,
};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::api::{DeleteParams, ListParams, Patch, PatchParams};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind, ObjectMeta};
use kube::{Api, ResourceExt};

use kopiur_api::{CopyMethod, SnapshotPolicy};

use super::apply::{FIELD_MANAGER, apply};
use super::child_labels;
use crate::error::{Error, Result};

/// The `snapshot.storage.k8s.io` API group.
pub const SNAPSHOT_GROUP: &str = "snapshot.storage.k8s.io";
/// The well-known annotation marking the default `VolumeSnapshotClass` for a driver.
pub const DEFAULT_CLASS_ANNOTATION: &str = "snapshot.storage.kubernetes.io/is-default-class";

/// `ApiResource` for `VolumeSnapshot` (namespaced).
fn volume_snapshot_ar() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(SNAPSHOT_GROUP, "v1", "VolumeSnapshot"),
        "volumesnapshots",
    )
}

/// `ApiResource` for `VolumeSnapshotClass` (cluster-scoped).
fn volume_snapshot_class_ar() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(SNAPSHOT_GROUP, "v1", "VolumeSnapshotClass"),
        "volumesnapshotclasses",
    )
}

fn volume_snapshot_api(client: &kube::Client, ns: &str) -> Api<DynamicObject> {
    Api::namespaced_with(client.clone(), ns, &volume_snapshot_ar())
}

// ---------------------------------------------------------------------------
// Outcome / decision types
// ---------------------------------------------------------------------------

/// The staged source the mover should mount instead of the live PVC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedSource {
    /// Name of the staged PVC (mounted in place of the live source).
    pub pvc_name: String,
    /// Name of the `VolumeSnapshot` backing the stage (`Snapshot` mode; `None` for `Clone`).
    pub volume_snapshot_name: Option<String>,
    /// The resolved capture method (`Snapshot`/`Clone`) — recorded to status.
    pub copy_method: &'static str,
}

/// What [`resolve_staging`] decided. The reconciler maps this to status/conditions/
/// events — this module performs **no** status side effects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StagingOutcome {
    /// No staging — `Direct`, an NFS source, or a multi-PVC selector. Mount the live source.
    NotApplicable,
    /// The stage is provisioned and ready; mount [`StagedSource::pvc_name`].
    Ready(StagedSource),
    /// The VolumeSnapshot is not `readyToUse` yet — requeue (transient). The message is
    /// for a `SourceStaged=False` / `Pending` condition.
    Waiting(String),
    /// The stage cannot be produced (no snapshot stack / no class / VolumeSnapshot
    /// errored / source not CSI-provisioned). The reconciler fails the Snapshot with this
    /// `reason` + actionable `message` and re-drives on the structural cadence (so it
    /// recovers once the cluster is fixed — e.g. a class is installed).
    Failed {
        /// kstatus condition reason (a stable PascalCase token).
        reason: &'static str,
        /// Actionable what/why/fix message.
        message: String,
    },
}

/// Condition reason: the CSI snapshot stack (snapshot-controller / CRDs) is absent.
pub const REASON_STACK_MISSING: &str = "SnapshotStackMissing";
/// Condition reason: no usable `VolumeSnapshotClass` for the source's driver.
pub const REASON_NO_CLASS: &str = "NoVolumeSnapshotClass";
/// Condition reason: the created `VolumeSnapshot` reported an error.
pub const REASON_VS_FAILED: &str = "VolumeSnapshotFailed";
/// Condition reason: the source PVC isn't CSI-provisioned (no StorageClass), so it
/// can't be snapshotted/cloned.
pub const REASON_SOURCE_NOT_CSI: &str = "SourceNotCSIProvisioned";

/// A `VolumeSnapshotClass` as far as class selection cares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassInfo {
    /// `metadata.name`.
    pub name: String,
    /// `.driver` (the CSI driver the class snapshots).
    pub driver: String,
    /// Whether it carries the default-class annotation.
    pub is_default: bool,
}

/// The pure outcome of picking a `VolumeSnapshotClass` for a source provisioner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassDecision {
    /// Set this class name explicitly on the VolumeSnapshot.
    Use(String),
    /// An explicit `volumeSnapshotClassName` was given but no such class exists.
    ExplicitNotFound(String),
    /// No class's driver matches the source provisioner.
    NoneForDriver(String),
    /// Several classes match the driver and none (or more than one) is the default —
    /// the user must name one explicitly.
    Ambiguous {
        /// The source provisioner the candidates matched.
        driver: String,
        /// The candidate class names.
        candidates: Vec<String>,
    },
}

/// Pick the `VolumeSnapshotClass` for a source `provisioner`. **Pure** over the listed
/// classes (the cluster read is the caller's). Precedence: an explicit name wins (if it
/// exists); otherwise the single class whose driver matches; otherwise — among several
/// matches — the unique default-annotated one; otherwise `Ambiguous`/`NoneForDriver`.
pub fn decide_class(
    classes: &[ClassInfo],
    provisioner: &str,
    explicit: Option<&str>,
) -> ClassDecision {
    if let Some(name) = explicit {
        return if classes.iter().any(|c| c.name == name) {
            ClassDecision::Use(name.to_string())
        } else {
            ClassDecision::ExplicitNotFound(name.to_string())
        };
    }
    let candidates: Vec<&ClassInfo> = classes.iter().filter(|c| c.driver == provisioner).collect();
    match candidates.as_slice() {
        [] => ClassDecision::NoneForDriver(provisioner.to_string()),
        [only] => ClassDecision::Use(only.name.clone()),
        many => {
            let defaults: Vec<&&ClassInfo> = many.iter().filter(|c| c.is_default).collect();
            if let [the_default] = defaults.as_slice() {
                ClassDecision::Use(the_default.name.clone())
            } else {
                ClassDecision::Ambiguous {
                    driver: provisioner.to_string(),
                    candidates: many.iter().map(|c| c.name.clone()).collect(),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Pure builders
// ---------------------------------------------------------------------------

/// Kubernetes object-name length cap.
const MAX_NAME_LEN: usize = 63;

/// A deterministic child-object name `<base>-<suffix>`, bounded to 63 chars (a short
/// stable hash of the base replaces the overflow so two long Snapshot names never
/// collide). Deterministic so a crashed reconcile re-derives the **same** name and
/// never orphans or double-creates.
pub fn staged_child_name(base: &str, suffix: &str) -> String {
    let full = format!("{base}-{suffix}");
    if full.len() <= MAX_NAME_LEN {
        return full;
    }
    // Stable FNV-1a hash of the base → 8 hex chars; keep the suffix legible.
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in base.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let tag = format!("{:08x}", (hash & 0xffff_ffff) as u32);
    let keep = MAX_NAME_LEN - tag.len() - suffix.len() - 2; // two '-'
    format!("{}-{tag}-{suffix}", &base[..keep.min(base.len())])
}

/// The VolumeSnapshot name for a Snapshot CR.
pub fn volume_snapshot_name(snapshot_cr: &str) -> String {
    staged_child_name(snapshot_cr, "snap")
}

/// The staged-PVC name for a Snapshot CR.
pub fn staged_pvc_name(snapshot_cr: &str) -> String {
    staged_child_name(snapshot_cr, "src")
}

/// Build the `VolumeSnapshot` `DynamicObject` for `source_claim`, owner-referenced so GC
/// reaps it with the Snapshot CR and labelled managed-by. `class` (when `Some`) pins
/// `spec.volumeSnapshotClassName`.
pub fn build_volume_snapshot(
    name: &str,
    ns: &str,
    source_claim: &str,
    class: Option<&str>,
    owner: k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference,
) -> DynamicObject {
    let mut spec = serde_json::json!({
        "source": { "persistentVolumeClaimName": source_claim },
    });
    if let Some(class) = class {
        spec["volumeSnapshotClassName"] = serde_json::Value::String(class.to_string());
    }
    let mut obj = DynamicObject::new(name, &volume_snapshot_ar())
        .within(ns)
        .data(serde_json::json!({ "spec": spec }));
    obj.metadata.owner_references = Some(vec![owner]);
    obj.metadata.labels = Some(child_labels(&[(
        "kopiur.home-operations.com/component",
        "staged-source-snapshot",
    )]));
    obj
}

/// The `dataSource` for the staged PVC: the VolumeSnapshot (`Snapshot`) or the source
/// PVC itself (`Clone`, a CSI volume clone).
pub fn data_source_for(
    copy_method: CopyMethod,
    source_pvc: &str,
    volume_snapshot: &str,
) -> TypedLocalObjectReference {
    match copy_method {
        CopyMethod::Clone => TypedLocalObjectReference {
            api_group: None,
            kind: "PersistentVolumeClaim".to_string(),
            name: source_pvc.to_string(),
        },
        // Snapshot (and any non-Direct default) stages from the VolumeSnapshot.
        _ => TypedLocalObjectReference {
            api_group: Some(SNAPSHOT_GROUP.to_string()),
            kind: "VolumeSnapshot".to_string(),
            name: volume_snapshot.to_string(),
        },
    }
}

/// Parse a `Quantity` like `"10Gi"` to bytes (best-effort; binary + decimal SI). Returns
/// `None` when unparseable, so sizing falls back conservatively.
fn quantity_to_bytes(q: &str) -> Option<i128> {
    let q = q.trim();
    let (num, mult): (&str, i128) = if let Some(p) = q.strip_suffix("Ki") {
        (p, 1 << 10)
    } else if let Some(p) = q.strip_suffix("Mi") {
        (p, 1 << 20)
    } else if let Some(p) = q.strip_suffix("Gi") {
        (p, 1 << 30)
    } else if let Some(p) = q.strip_suffix("Ti") {
        (p, 1i128 << 40)
    } else if let Some(p) = q.strip_suffix("Pi") {
        (p, 1i128 << 50)
    } else if let Some(p) = q.strip_suffix('k') {
        (p, 1_000)
    } else if let Some(p) = q.strip_suffix('M') {
        (p, 1_000_000)
    } else if let Some(p) = q.strip_suffix('G') {
        (p, 1_000_000_000)
    } else if let Some(p) = q.strip_suffix('T') {
        (p, 1_000_000_000_000)
    } else {
        (q, 1)
    };
    num.trim().parse::<i128>().ok().map(|n| n * mult)
}

/// The storage request for the staged PVC: the larger of the snapshot's `restoreSize`
/// (often `0`/absent) and the source PVC's request — the external-provisioner rejects a
/// staged PVC smaller than `restoreSize`. Returns the chosen `Quantity` string, or
/// `None` when neither is known (let the provisioner default).
pub fn staged_pvc_size(restore_size: Option<&str>, source_request: Option<&str>) -> Option<String> {
    let r = restore_size.and_then(quantity_to_bytes).unwrap_or(0);
    let s = source_request.and_then(quantity_to_bytes).unwrap_or(0);
    match (restore_size, source_request) {
        (None, None) => None,
        // Keep the original string of whichever is larger (preserves the unit the user
        // wrote, and avoids re-rendering bytes the provisioner then rounds).
        _ if r >= s => restore_size
            .filter(|_| r > 0)
            .map(str::to_string)
            .or_else(|| source_request.map(str::to_string)),
        _ => source_request.map(str::to_string),
    }
}

/// Build the staged PVC: copies the source PVC's `accessModes`/`storageClassName`/
/// `volumeMode`, sets `dataSource`, requests `max(restoreSize, source)` storage, and is
/// owner-referenced + managed-by labelled.
pub fn build_staged_pvc(
    name: &str,
    ns: &str,
    source_pvc: &PersistentVolumeClaim,
    data_source: TypedLocalObjectReference,
    restore_size: Option<&str>,
    owner: k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference,
) -> PersistentVolumeClaim {
    let src = source_pvc.spec.clone().unwrap_or_default();
    let source_request = src
        .resources
        .as_ref()
        .and_then(|r| r.requests.as_ref())
        .and_then(|m| m.get("storage"))
        .map(|q| q.0.clone());
    let size = staged_pvc_size(restore_size, source_request.as_deref());
    let resources = size.map(|s| VolumeResourceRequirements {
        requests: Some(BTreeMap::from([("storage".to_string(), Quantity(s))])),
        limits: None,
    });
    PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            owner_references: Some(vec![owner]),
            labels: Some(child_labels(&[(
                "kopiur.home-operations.com/component",
                "staged-source",
            )])),
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: src.access_modes,
            storage_class_name: src.storage_class_name,
            volume_mode: src.volume_mode,
            data_source: Some(data_source),
            resources,
            ..Default::default()
        }),
        status: None,
    }
}

/// What cleanup must do for a staged PVC: whether its **bound** PV needs a
/// `Retain → Delete` reclaim patch before the PVC is deleted (else a `Retain`
/// StorageClass leaks the PV + backend volume), plus the bound PV name.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CleanupPlan {
    /// The bound PV to patch `Retain → Delete` before deleting the PVC (`None` when the
    /// PVC is unbound — nothing to leak).
    pub reclaim_pv: Option<String>,
}

/// Decide the PV-reclaim step for a staged PVC. The PVC is bound iff it has a
/// `spec.volumeName`; only then can a `Retain` PV outlive it.
pub fn cleanup_plan(staged_pvc: &PersistentVolumeClaim) -> CleanupPlan {
    CleanupPlan {
        reclaim_pv: staged_pvc
            .spec
            .as_ref()
            .and_then(|s| s.volume_name.clone())
            .filter(|v| !v.is_empty()),
    }
}

// ---------------------------------------------------------------------------
// Cluster IO
// ---------------------------------------------------------------------------

/// List the cluster's `VolumeSnapshotClass`es, or `None` when the snapshot CRDs are not
/// installed (the API group is absent → the list 404s / "could not find the requested
/// resource"). Distinguishing absence from a transient error lets the caller emit the
/// precise `SnapshotStackMissing` guidance.
pub async fn list_snapshot_classes(client: &kube::Client) -> Result<Option<Vec<ClassInfo>>> {
    let api: Api<DynamicObject> = Api::all_with(client.clone(), &volume_snapshot_class_ar());
    match api.list(&ListParams::default()).await {
        Ok(list) => Ok(Some(
            list.items
                .into_iter()
                .map(|o| ClassInfo {
                    driver: o
                        .data
                        .get("driver")
                        .and_then(|d| d.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    is_default: o
                        .annotations()
                        .get(DEFAULT_CLASS_ANNOTATION)
                        .map(|v| v == "true")
                        .unwrap_or(false),
                    name: o.name_any(),
                })
                .collect(),
        )),
        // The CRD/API group isn't served → the stack isn't installed.
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
        Err(e) => Err(Error::Kube(e)),
    }
}

/// The CSI provisioner backing a PVC: its `StorageClass.provisioner`. `None` when the
/// PVC has no `storageClassName` (a statically-provisioned / hostPath volume that can't
/// be CSI-snapshotted) or the class is missing.
pub async fn source_provisioner(
    client: &kube::Client,
    source_pvc: &PersistentVolumeClaim,
) -> Result<Option<String>> {
    let Some(class) = source_pvc
        .spec
        .as_ref()
        .and_then(|s| s.storage_class_name.clone())
        .filter(|c| !c.is_empty())
    else {
        return Ok(None);
    };
    let api: Api<StorageClass> = Api::all(client.clone());
    Ok(api.get_opt(&class).await?.map(|sc| sc.provisioner))
}

/// Read a VolumeSnapshot's readiness: `(ready_to_use, restore_size, error_message)`.
async fn read_volume_snapshot(
    client: &kube::Client,
    ns: &str,
    name: &str,
) -> Result<Option<(bool, Option<String>, Option<String>)>> {
    let api = volume_snapshot_api(client, ns);
    let Some(vs) = api.get_opt(name).await? else {
        return Ok(None);
    };
    let status = vs.data.get("status");
    let ready = status
        .and_then(|s| s.get("readyToUse"))
        .and_then(|r| r.as_bool())
        .unwrap_or(false);
    let restore_size = status.and_then(|s| s.get("restoreSize")).and_then(|r| {
        r.as_str()
            .map(str::to_string)
            .or_else(|| r.as_i64().map(|n| n.to_string()))
    });
    let error = status
        .and_then(|s| s.get("error"))
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .map(str::to_string);
    Ok(Some((ready, restore_size, error)))
}

/// Resolve staging for a backup. Performs the cluster IO (preflight, create
/// VolumeSnapshot, wait `readyToUse`, create staged PVC) and returns a [`StagingOutcome`]
/// — **no** status side effects (the reconciler maps the outcome). Idempotent: every
/// create is SSA over a deterministic name.
pub async fn resolve_staging(
    client: &kube::Client,
    policy: &SnapshotPolicy,
    ns: &str,
    snapshot_cr: &str,
    owner: &k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference,
) -> Result<StagingOutcome> {
    let copy_method = policy.spec.copy_method;
    // Direct → never stage. Staging only applies to a single explicit `pvc` source; NFS
    // and multi-PVC selectors mount the live source (the latter is already rejected
    // downstream).
    if copy_method == CopyMethod::Direct {
        return Ok(StagingOutcome::NotApplicable);
    }
    let Some(source) = policy.spec.sources.first() else {
        return Ok(StagingOutcome::NotApplicable);
    };
    let Some(pvc_ref) = source.pvc.as_ref() else {
        // NFS / pvcSelector — nothing to snapshot.
        return Ok(StagingOutcome::NotApplicable);
    };
    let source_name = &pvc_ref.name;

    // Read the source PVC (needed for sizing + provisioner + shape).
    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), ns);
    let Some(source_pvc) = pvc_api.get_opt(source_name).await? else {
        return Ok(StagingOutcome::Failed {
            reason: REASON_SOURCE_NOT_CSI,
            message: format!(
                "source PVC `{ns}/{source_name}` was not found; copyMethod {copy_method:?} needs an \
                 existing CSI-provisioned PVC to snapshot — create the PVC, or set copyMethod: Direct"
            ),
        });
    };

    let staged_pvc = staged_pvc_name(snapshot_cr);
    let vs_name = volume_snapshot_name(snapshot_cr);

    // `Snapshot` needs a VolumeSnapshotClass + a ready VolumeSnapshot; `Clone` stages
    // straight from the source PVC.
    if copy_method != CopyMethod::Clone {
        // Provisioner (CSI driver) of the source — required to match a class.
        let Some(provisioner) = source_provisioner(client, &source_pvc).await? else {
            return Ok(StagingOutcome::Failed {
                reason: REASON_SOURCE_NOT_CSI,
                message: format!(
                    "source PVC `{ns}/{source_name}` has no StorageClass / CSI provisioner, so it \
                     cannot be CSI-snapshotted; use a CSI StorageClass, or set copyMethod: Direct"
                ),
            });
        };
        // Preflight the snapshot stack + pick the class.
        let Some(classes) = list_snapshot_classes(client).await? else {
            return Ok(StagingOutcome::Failed {
                reason: REASON_STACK_MISSING,
                message: format!(
                    "the CSI snapshot stack is not installed (no VolumeSnapshotClass API), so \
                     copyMethod: Snapshot cannot run; install the external-snapshotter \
                     (snapshot-controller + CRDs) and a VolumeSnapshotClass for driver \
                     `{provisioner}`, or set copyMethod: Direct"
                ),
            });
        };
        let class = match decide_class(
            &classes,
            &provisioner,
            policy.spec.volume_snapshot_class_name.as_deref(),
        ) {
            ClassDecision::Use(name) => name,
            ClassDecision::ExplicitNotFound(name) => {
                return Ok(StagingOutcome::Failed {
                    reason: REASON_NO_CLASS,
                    message: format!(
                        "volumeSnapshotClassName `{name}` does not exist; create it (driver \
                         `{provisioner}`), pick an existing class, or set copyMethod: Direct"
                    ),
                });
            }
            ClassDecision::NoneForDriver(driver) => {
                return Ok(StagingOutcome::Failed {
                    reason: REASON_NO_CLASS,
                    message: format!(
                        "no VolumeSnapshotClass has driver `{driver}` (the source PVC's \
                         provisioner); create a VolumeSnapshotClass for it (optionally annotate it \
                         {DEFAULT_CLASS_ANNOTATION}=true), set volumeSnapshotClassName explicitly, \
                         or set copyMethod: Direct"
                    ),
                });
            }
            ClassDecision::Ambiguous { driver, candidates } => {
                return Ok(StagingOutcome::Failed {
                    reason: REASON_NO_CLASS,
                    message: format!(
                        "multiple VolumeSnapshotClasses match driver `{driver}` ({}) and none is \
                         the unique default; set volumeSnapshotClassName explicitly to choose one",
                        candidates.join(", ")
                    ),
                });
            }
        };

        // Create (SSA, idempotent) the VolumeSnapshot and wait for readyToUse.
        let vs = build_volume_snapshot(&vs_name, ns, source_name, Some(&class), owner.clone());
        apply(&volume_snapshot_api(client, ns), &vs_name, &vs).await?;

        let restore_size = match read_volume_snapshot(client, ns, &vs_name).await? {
            Some((true, restore_size, _)) => restore_size,
            Some((false, _, Some(err))) => {
                return Ok(StagingOutcome::Failed {
                    reason: REASON_VS_FAILED,
                    message: format!(
                        "VolumeSnapshot `{ns}/{vs_name}` failed: {err}; check the \
                         VolumeSnapshotClass `{class}` / CSI driver, then re-create the Snapshot"
                    ),
                });
            }
            // Not ready yet (or status not populated) → requeue.
            _ => {
                return Ok(StagingOutcome::Waiting(format!(
                    "waiting for VolumeSnapshot `{ns}/{vs_name}` to become readyToUse"
                )));
            }
        };

        let data_source = data_source_for(copy_method, source_name, &vs_name);
        let staged = build_staged_pvc(
            &staged_pvc,
            ns,
            &source_pvc,
            data_source,
            restore_size.as_deref(),
            owner.clone(),
        );
        apply(&pvc_api, &staged_pvc, &staged).await?;
        return Ok(StagingOutcome::Ready(StagedSource {
            pvc_name: staged_pvc,
            volume_snapshot_name: Some(vs_name),
            copy_method: "Snapshot",
        }));
    }

    // Clone: stage straight from the source PVC (CSI volume clone). No VolumeSnapshot.
    let data_source = data_source_for(CopyMethod::Clone, source_name, &vs_name);
    let staged = build_staged_pvc(
        &staged_pvc,
        ns,
        &source_pvc,
        data_source,
        None,
        owner.clone(),
    );
    apply(&pvc_api, &staged_pvc, &staged).await?;
    Ok(StagingOutcome::Ready(StagedSource {
        pvc_name: staged_pvc,
        volume_snapshot_name: None,
        copy_method: "Clone",
    }))
}

/// Reap the staged objects a backup created (idempotent; 404-tolerant). Deletes the
/// finished mover **pod** first (a completed Job pod still references the staged PVC via
/// `spec.volumes`, so `kubernetes.io/pvc-protection` blocks the PVC's deletion until the
/// pod is gone — but the Job object is left intact so the reconcile's terminal-state
/// detection still sees a completed Job and does NOT re-run the backup). Then patches a
/// bound staged PV `Retain → Delete` (so a `Retain` StorageClass doesn't leak the PV +
/// backend volume), and deletes the staged PVC and the VolumeSnapshot. Safe to call on
/// every terminal reconcile — once the objects are gone it is a no-op.
pub async fn cleanup_staged_source(
    client: &kube::Client,
    ns: &str,
    snapshot_cr: &str,
) -> Result<()> {
    let staged_pvc = staged_pvc_name(snapshot_cr);
    let vs_name = volume_snapshot_name(snapshot_cr);

    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), ns);
    if let Some(pvc) = pvc_api.get_opt(&staged_pvc).await? {
        // Release pvc-protection: delete the finished mover pod(s) (the backup Job is
        // named after the Snapshot CR). The Job object itself is preserved. Use
        // list + delete-by-name (not delete_collection), since the controller is granted
        // the `delete`/`list` verbs but NOT the distinct `deletecollection` verb.
        let pod_api: Api<Pod> = Api::namespaced(client.clone(), ns);
        let mover_pods = pod_api
            .list(
                &ListParams::default()
                    .labels(&format!("batch.kubernetes.io/job-name={snapshot_cr}")),
            )
            .await
            .map(|l| l.items)
            .unwrap_or_default();
        for pod in mover_pods {
            if let Some(pod_name) = pod.metadata.name {
                delete_ignore_404(&pod_api, &pod_name).await?;
            }
        }
        // Patch the bound PV to Delete so the underlying volume is reclaimed even on a
        // Retain StorageClass (the #82 OpenEBS-ZFS leak).
        if let Some(pv_name) = cleanup_plan(&pvc).reclaim_pv {
            let pv_api: Api<PersistentVolume> = Api::all(client.clone());
            let patch =
                serde_json::json!({ "spec": { "persistentVolumeReclaimPolicy": "Delete" } });
            match pv_api
                .patch(
                    &pv_name,
                    &PatchParams::apply(FIELD_MANAGER),
                    &Patch::Merge(&patch),
                )
                .await
            {
                Ok(_) => {}
                Err(kube::Error::Api(e)) if e.code == 404 => {}
                Err(e) => return Err(Error::Kube(e)),
            }
        }
        delete_ignore_404(&pvc_api, &staged_pvc).await?;
    }

    let vs_api = volume_snapshot_api(client, ns);
    if vs_api.get_opt(&vs_name).await?.is_some() {
        delete_ignore_404(&vs_api, &vs_name).await?;
    }
    Ok(())
}

async fn delete_ignore_404<K>(api: &Api<K>, name: &str) -> Result<()>
where
    K: kube::Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug,
{
    match api.delete(name, &DeleteParams::background()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(Error::Kube(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class(name: &str, driver: &str, default: bool) -> ClassInfo {
        ClassInfo {
            name: name.into(),
            driver: driver.into(),
            is_default: default,
        }
    }

    // --- decide_class ---

    #[test]
    fn decide_class_explicit_wins_when_present() {
        let classes = [class("csi-a", "ebs.csi.aws.com", false)];
        assert_eq!(
            decide_class(&classes, "ebs.csi.aws.com", Some("csi-a")),
            ClassDecision::Use("csi-a".into())
        );
        assert_eq!(
            decide_class(&classes, "ebs.csi.aws.com", Some("nope")),
            ClassDecision::ExplicitNotFound("nope".into())
        );
    }

    #[test]
    fn decide_class_single_match_is_used() {
        let classes = [
            class("csi-a", "ebs.csi.aws.com", false),
            class("other", "pd.csi.storage.gke.io", false),
        ];
        assert_eq!(
            decide_class(&classes, "ebs.csi.aws.com", None),
            ClassDecision::Use("csi-a".into())
        );
    }

    #[test]
    fn decide_class_no_match_for_driver() {
        let classes = [class("csi-a", "ebs.csi.aws.com", false)];
        assert_eq!(
            decide_class(&classes, "zfs.csi.openebs.io", None),
            ClassDecision::NoneForDriver("zfs.csi.openebs.io".into())
        );
        // Empty cluster.
        assert_eq!(
            decide_class(&[], "any", None),
            ClassDecision::NoneForDriver("any".into())
        );
    }

    #[test]
    fn decide_class_many_picks_unique_default_else_ambiguous() {
        let driver = "rbd.csi.ceph.com";
        let many = [
            class("a", driver, false),
            class("b", driver, true),
            class("c", driver, false),
        ];
        assert_eq!(
            decide_class(&many, driver, None),
            ClassDecision::Use("b".into())
        );
        // No default among several → ambiguous.
        let no_default = [class("a", driver, false), class("b", driver, false)];
        match decide_class(&no_default, driver, None) {
            ClassDecision::Ambiguous { candidates, .. } => {
                assert_eq!(candidates, vec!["a".to_string(), "b".to_string()])
            }
            other => panic!("expected Ambiguous, got {other:?}"),
        }
        // Two defaults → also ambiguous (no unique default).
        let two_defaults = [class("a", driver, true), class("b", driver, true)];
        assert!(matches!(
            decide_class(&two_defaults, driver, None),
            ClassDecision::Ambiguous { .. }
        ));
    }

    // --- staged_child_name ---

    #[test]
    fn staged_names_are_deterministic_and_bounded() {
        assert_eq!(volume_snapshot_name("db"), "db-snap");
        assert_eq!(staged_pvc_name("db"), "db-src");
        let long = "a".repeat(80);
        let n = staged_pvc_name(&long);
        assert!(n.len() <= MAX_NAME_LEN, "bounded to 63: {} ", n.len());
        assert!(n.ends_with("-src"));
        // Deterministic.
        assert_eq!(staged_pvc_name(&long), n);
    }

    // --- data_source_for ---

    #[test]
    fn data_source_snapshot_vs_clone() {
        let snap = data_source_for(CopyMethod::Snapshot, "src", "vs");
        assert_eq!(snap.api_group.as_deref(), Some(SNAPSHOT_GROUP));
        assert_eq!(snap.kind, "VolumeSnapshot");
        assert_eq!(snap.name, "vs");
        let clone = data_source_for(CopyMethod::Clone, "src", "vs");
        assert_eq!(clone.api_group, None);
        assert_eq!(clone.kind, "PersistentVolumeClaim");
        assert_eq!(clone.name, "src");
    }

    // --- staged_pvc_size: max(restoreSize, source) with restoreSize 0/absent ---

    #[test]
    fn staged_size_takes_the_larger() {
        assert_eq!(
            staged_pvc_size(Some("20Gi"), Some("10Gi")).as_deref(),
            Some("20Gi")
        );
        assert_eq!(
            staged_pvc_size(Some("5Gi"), Some("10Gi")).as_deref(),
            Some("10Gi")
        );
        // restoreSize 0/absent → fall back to source.
        assert_eq!(
            staged_pvc_size(Some("0"), Some("10Gi")).as_deref(),
            Some("10Gi")
        );
        assert_eq!(staged_pvc_size(None, Some("10Gi")).as_deref(), Some("10Gi"));
        // Neither known → let the provisioner default.
        assert_eq!(staged_pvc_size(None, None), None);
    }

    // --- build_staged_pvc copies source shape + dataSource ---

    #[test]
    fn staged_pvc_copies_source_and_sets_data_source() {
        let source = PersistentVolumeClaim {
            spec: Some(PersistentVolumeClaimSpec {
                access_modes: Some(vec!["ReadWriteOnce".into()]),
                storage_class_name: Some("fast".into()),
                volume_mode: Some("Filesystem".into()),
                resources: Some(VolumeResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".into(),
                        Quantity("10Gi".into()),
                    )])),
                    limits: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let ds = data_source_for(CopyMethod::Snapshot, "src", "vs");
        let owner = k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference::default();
        let staged = build_staged_pvc("db-src", "ns", &source, ds, Some("0"), owner);
        let spec = staged.spec.unwrap();
        assert_eq!(
            spec.access_modes.as_deref(),
            Some(["ReadWriteOnce".to_string()].as_slice())
        );
        assert_eq!(spec.storage_class_name.as_deref(), Some("fast"));
        assert_eq!(spec.volume_mode.as_deref(), Some("Filesystem"));
        // restoreSize 0 → falls back to the source's 10Gi request.
        assert_eq!(
            spec.resources
                .unwrap()
                .requests
                .unwrap()
                .get("storage")
                .unwrap()
                .0,
            "10Gi"
        );
        let ds = spec.data_source.unwrap();
        assert_eq!(ds.kind, "VolumeSnapshot");
        assert_eq!(ds.name, "vs");
    }

    // --- cleanup_plan ---

    #[test]
    fn cleanup_plan_only_reclaims_a_bound_pv() {
        let mut pvc = PersistentVolumeClaim {
            spec: Some(PersistentVolumeClaimSpec {
                volume_name: Some("pv-123".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(cleanup_plan(&pvc).reclaim_pv.as_deref(), Some("pv-123"));
        // Unbound (no volumeName) → nothing to reclaim.
        pvc.spec.as_mut().unwrap().volume_name = None;
        assert_eq!(cleanup_plan(&pvc).reclaim_pv, None);
    }

    // --- build_volume_snapshot wire shape ---

    #[test]
    fn volume_snapshot_has_source_and_class() {
        let owner = k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference::default();
        let vs = build_volume_snapshot("db-snap", "ns", "db", Some("csi-class"), owner);
        assert_eq!(vs.data["spec"]["source"]["persistentVolumeClaimName"], "db");
        assert_eq!(vs.data["spec"]["volumeSnapshotClassName"], "csi-class");
        // Omitting the class leaves it unset (controller auto-selects the default).
        let owner = k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference::default();
        let vs2 = build_volume_snapshot("db-snap", "ns", "db", None, owner);
        assert!(vs2.data["spec"].get("volumeSnapshotClassName").is_none());
    }
}
