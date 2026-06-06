//! Shared cluster-IO helpers for the reconcilers (the "thin IO calling tested
//! pure fns" layer, ADR §5.2/§5.4).
//!
//! These wrap the repetitive `kube::Api` mechanics — server-side apply with a
//! stable field manager, finalizer add/remove, status subresource patches, and
//! resolving the credentials Secret for a repository — so each reconciler stays
//! focused on its decision logic. The decision logic itself lives in the
//! per-reconciler pure functions (which remain unit-tested without a cluster).

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, ObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, OwnerReference, Time};
use kube::api::{DeleteParams, Patch, PatchParams};
use kube::core::ObjectMeta;
use kube::runtime::events::{Event, EventType};
use kube::runtime::reflector::Store;
use kube::{Api, Resource, ResourceExt};
use serde::Serialize;
use serde::de::DeserializeOwned;

use kopiur_api::backend::Backend;
use kopiur_api::common::{Encryption, RepositoryKind, RepositoryRef};
use kopiur_api::maintenance::{
    MaintenanceSpec, Ownership, RepositoryMaintenanceSpec, default_maintenance_schedule,
};
use kopiur_api::{ClusterRepository, Maintenance, Repository};
use kopiur_kopia::KopiaErrorClass;

use crate::consts::{
    API_VERSION, CHECK_BACKEND_ACTION, CHECK_CREDENTIALS_ACTION, CHECK_MAINTENANCE_ACTION,
    CHECK_PERMISSIONS_ACTION, MAINTENANCE_CONFIGURED_CONDITION, MAINTENANCE_CONFIGURED_REASON,
    MAINTENANCE_DISABLED_REASON, MAINTENANCE_NAMESPACE_UNRESOLVED_REASON,
};
use crate::context::Context;
use crate::error::{Error, Result};

/// The field-manager used for every server-side apply the controller performs.
pub const FIELD_MANAGER: &str = "kopiur.home-operations.com/controller";

/// Default key within the encryption password Secret when unset.
pub const DEFAULT_PASSWORD_KEY: &str = "KOPIA_PASSWORD";

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
/// [`upsert_condition`] preserves `lastTransitionTime` while the status is
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

/// The credentials a mover Job needs, sourced from a repository's
/// `encryption.passwordSecretRef`. The Secret is mounted as env (`envFrom`), so
/// here we only need its name; the key resolution is documented for callers that
/// read the password in-process.
#[derive(Debug, Clone)]
pub struct RepoCredentials {
    /// Secret name holding `KOPIA_PASSWORD` (+ optional backend creds).
    pub secret_name: String,
    /// The key within the Secret holding the repository password.
    pub password_key: String,
    /// Optional explicit namespace (cluster-scoped repos require it).
    pub namespace: Option<String>,
}

/// The repository surface a backup/restore/maintenance run needs, resolved from
/// either a namespaced [`Repository`] or a cluster-scoped [`ClusterRepository`].
///
/// Both CRDs expose the same `backend`/`encryption` shape; the reconcilers only
/// need those two fields to connect to kopia, so we normalize once at resolution
/// time. The discriminated [`RepositoryKind`] is `match`ed exhaustively in
/// [`resolve_repository_ref`] (ADR §5.5) — a future variant cannot compile until
/// it is handled here.
#[derive(Debug, Clone)]
pub struct ResolvedRepository {
    /// The repository's storage backend (normalized from either CRD).
    pub backend: Backend,
    /// The repository's encryption/password configuration.
    pub encryption: Encryption,
}

/// Which API a [`RepositoryRef`] resolves against, derived purely from `kind`.
///
/// Extracting this from the IO lets the namespaced-vs-cluster decision be
/// unit-tested without a cluster. It is the regression guard for the class of
/// bug where a `kind: ClusterRepository` ref silently fell through to a
/// namespaced `Repository` lookup and produced `missing dependency: Repository
/// <ns>/<name>` for cluster-backed `BackupConfig`s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepoLookup {
    /// Namespaced `Repository` get in `namespace`.
    Namespaced {
        /// Namespace to perform the `Repository` get in.
        namespace: String,
        /// Name of the `Repository` to get.
        name: String,
    },
    /// Cluster-scoped `ClusterRepository` get (`Api::all`).
    Cluster {
        /// Name of the `ClusterRepository` to get.
        name: String,
    },
}

/// Pure mapping from a consumer's [`RepositoryRef`] (+ the default namespace to
/// use when the ref omits one) to the API lookup it implies. Exhaustive over
/// [`RepositoryKind`] (ADR §5.5): a new variant cannot compile until handled.
///
/// ```
/// use kopiur_controller::io::{repo_lookup, RepoLookup};
/// use kopiur_api::common::{RepositoryKind, RepositoryRef};
///
/// // A namespaced ref with no explicit namespace falls back to `default_ns`.
/// let r = RepositoryRef { kind: RepositoryKind::Repository, name: "nas".into(), namespace: None };
/// assert_eq!(
///     repo_lookup(&r, "billing"),
///     RepoLookup::Namespaced { namespace: "billing".into(), name: "nas".into() },
/// );
///
/// // A ClusterRepository ref ignores any namespace and resolves cluster-wide.
/// let c = RepositoryRef {
///     kind: RepositoryKind::ClusterRepository,
///     name: "shared".into(),
///     namespace: Some("ignored".into()),
/// };
/// assert_eq!(repo_lookup(&c, "billing"), RepoLookup::Cluster { name: "shared".into() });
/// ```
pub fn repo_lookup(repo_ref: &RepositoryRef, default_ns: &str) -> RepoLookup {
    match repo_ref.kind {
        RepositoryKind::Repository => RepoLookup::Namespaced {
            namespace: repo_ref
                .namespace
                .as_deref()
                .unwrap_or(default_ns)
                .to_string(),
            name: repo_ref.name.clone(),
        },
        // Cluster-scoped: `ref.namespace` is forbidden (webhook-enforced) and
        // deliberately ignored here.
        RepositoryKind::ClusterRepository => RepoLookup::Cluster {
            name: repo_ref.name.clone(),
        },
    }
}

/// Resolve a consumer CR's [`RepositoryRef`] to its backend + encryption,
/// honoring `kind` via [`repo_lookup`]:
///
/// - [`RepositoryKind::Repository`]: namespaced lookup, cross-namespace allowed
///   via `ref.namespace` (defaulting to `default_ns`, usually the consumer's
///   namespace).
/// - [`RepositoryKind::ClusterRepository`]: cluster-scoped lookup (`Api::all`).
pub async fn resolve_repository_ref(
    client: &kube::Client,
    repo_ref: &RepositoryRef,
    default_ns: &str,
) -> Result<ResolvedRepository> {
    match repo_lookup(repo_ref, default_ns) {
        RepoLookup::Namespaced { namespace, name } => {
            let api: Api<Repository> = Api::namespaced(client.clone(), &namespace);
            let repo = api.get_opt(&name).await?.ok_or_else(|| {
                Error::MissingDependency(format!("Repository {namespace}/{name}"))
            })?;
            Ok(ResolvedRepository {
                backend: repo.spec.backend,
                encryption: repo.spec.encryption,
            })
        }
        RepoLookup::Cluster { name } => {
            let api: Api<ClusterRepository> = Api::all(client.clone());
            let repo = api
                .get_opt(&name)
                .await?
                .ok_or_else(|| Error::MissingDependency(format!("ClusterRepository {name}")))?;
            Ok(ResolvedRepository {
                backend: repo.spec.backend,
                encryption: repo.spec.encryption,
            })
        }
    }
}

/// Whether the referenced repository is connected and healthy
/// (`status.phase == Ready`). Maintenance gates Job spawning on this: an
/// object-store repository must be bootstrapped (connected/created) before
/// `kopia maintenance` can reach it, so spawning a maintenance Job earlier just
/// produces a doomed pod (ADR §3.7, G7). Honors `kind` via [`repo_lookup`].
pub async fn repository_ready(
    client: &kube::Client,
    repo_ref: &RepositoryRef,
    default_ns: &str,
) -> Result<bool> {
    let ready = Some(kopiur_api::RepositoryPhase::Ready);
    match repo_lookup(repo_ref, default_ns) {
        RepoLookup::Namespaced { namespace, name } => {
            let api: Api<Repository> = Api::namespaced(client.clone(), &namespace);
            let repo = api.get_opt(&name).await?.ok_or_else(|| {
                Error::MissingDependency(format!("Repository {namespace}/{name}"))
            })?;
            Ok(repo.status.and_then(|s| s.phase) == ready)
        }
        RepoLookup::Cluster { name } => {
            let api: Api<ClusterRepository> = Api::all(client.clone());
            let repo = api
                .get_opt(&name)
                .await?
                .ok_or_else(|| Error::MissingDependency(format!("ClusterRepository {name}")))?;
            Ok(repo.status.and_then(|s| s.phase) == ready)
        }
    }
}

/// Resolve the credentials Secret reference from a repository's encryption block.
pub fn repo_credentials(enc: &Encryption) -> RepoCredentials {
    let r = &enc.password_secret_ref;
    RepoCredentials {
        secret_name: r.name.clone(),
        password_key: r
            .key
            .clone()
            .unwrap_or_else(|| DEFAULT_PASSWORD_KEY.to_string()),
        namespace: r.namespace.clone(),
    }
}

/// Read the repository password value from its Secret (used by the in-process
/// kopia path for filesystem repos — connect/create/status/snapshot list).
pub async fn read_repo_password(
    client: &kube::Client,
    namespace: &str,
    creds: &RepoCredentials,
) -> Result<String> {
    use k8s_openapi::api::core::v1::Secret;
    let ns = creds.namespace.as_deref().unwrap_or(namespace);
    let api: Api<Secret> = Api::namespaced(client.clone(), ns);
    let secret = api.get(&creds.secret_name).await.map_err(|e| {
        Error::MissingDependency(format!(
            "credentials secret {ns}/{} not found: {e}",
            creds.secret_name
        ))
    })?;
    let data = secret.data.unwrap_or_default();
    let raw = data.get(&creds.password_key).ok_or_else(|| {
        Error::MissingDependency(format!(
            "secret {ns}/{} missing key {}",
            creds.secret_name, creds.password_key
        ))
    })?;
    String::from_utf8(raw.0.clone())
        .map_err(|e| Error::Invariant(format!("password not valid utf-8: {e}")))
}

/// The backend credentials Secret name for an object-store backend, if any.
///
/// Exhaustive over [`Backend`] (ADR §5.5): a new backend cannot compile until its
/// credential source is decided here. Object stores read keys (e.g.
/// `AWS_ACCESS_KEY_ID`) from `auth.secretRef`; Rclone reads its config from
/// `configSecretRef`; Filesystem has no backend credentials. This Secret is
/// mounted into the mover Job alongside the encryption-password Secret so kopia
/// can reach the store (the in-process filesystem path never needs it).
pub fn backend_auth_secret_ref(backend: &Backend) -> Option<&kopiur_api::common::SecretRef> {
    match backend {
        Backend::S3(b) => b.auth.as_ref().and_then(|a| a.secret_ref.as_ref()),
        Backend::Azure(b) => b.auth.as_ref().and_then(|a| a.secret_ref.as_ref()),
        Backend::Gcs(b) => b.auth.as_ref().and_then(|a| a.secret_ref.as_ref()),
        Backend::B2(b) => b.auth.as_ref().and_then(|a| a.secret_ref.as_ref()),
        Backend::Sftp(b) => b.auth.as_ref().and_then(|a| a.secret_ref.as_ref()),
        Backend::WebDav(b) => b.auth.as_ref().and_then(|a| a.secret_ref.as_ref()),
        Backend::Rclone(b) => b.config_secret_ref.as_ref(),
        Backend::Filesystem(_) => None,
    }
}

/// The distinct credential Secret names a mover Job for `backend` + `encryption`
/// needs as `envFrom`: always the encryption-password Secret, plus the backend
/// `auth` Secret when present and different. Deduped, order-stable (password
/// first). The common single-secret setup (password + keys in one Secret)
/// collapses to one entry.
pub fn mover_creds_secrets(backend: &Backend, enc: &Encryption) -> Vec<String> {
    let mut names = vec![enc.password_secret_ref.name.clone()];
    if let Some(auth) = backend_auth_secret_ref(backend)
        && !names.contains(&auth.name)
    {
        names.push(auth.name.clone());
    }
    names
}

/// The filesystem repo path for a `Filesystem` backend, or `None` for object
/// stores. Used to decide whether to mount a repo PVC and run kopia in-process.
pub fn filesystem_repo_path(backend: &Backend) -> Option<String> {
    match backend {
        Backend::Filesystem(f) => Some(f.path.clone()),
        _ => None,
    }
}

/// The repo PVC name for a `Filesystem` backend, if any (mounted read-write).
pub fn filesystem_repo_pvc(backend: &Backend) -> Option<String> {
    match backend {
        Backend::Filesystem(f) => f.pvc_name.clone(),
        _ => None,
    }
}

/// Apply both the work-spec `ConfigMap` and the mover `Job` (server-side).
/// Both carry the owner reference so GC reaps them with the CR (§4.10).
pub async fn apply_mover_objects(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    config_map: &ConfigMap,
    job: &Job,
) -> Result<()> {
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    apply(&cm_api, name, config_map).await?;
    let job_api: Api<Job> = Api::namespaced(client.clone(), namespace);
    apply(&job_api, name, job).await?;
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
fn is_managed_by(m: &Maintenance, owner_kind: &str, repo_name: &str) -> bool {
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
        },
    );
    m.metadata = child_meta(name, placement_namespace, BTreeMap::new(), Some(owner));
    m
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

/// Kubernetes caps an Event's `note` at 1024 bytes (the apiserver validates with
/// Go's `len`, i.e. bytes). A longer note is rejected with a 422, so the
/// *actionable* warning never reaches `kubectl describe`. We clamp every
/// composed note to this.
const EVENT_NOTE_MAX_BYTES: usize = 1024;

/// Budget for the kopia error message embedded *inside* a note. Capping the
/// message before composing keeps the surrounding remediation text — the part a
/// user actually acts on — from being eaten by a huge kopia stderr tail when the
/// whole note is finally clamped to [`EVENT_NOTE_MAX_BYTES`].
const EVENT_MESSAGE_BUDGET_BYTES: usize = 512;

/// Appended to a string that was truncated, signalling the cut to readers.
const TRUNCATION_MARKER: &str = "…";

/// Truncate `s` to at most `max` bytes on a UTF-8 char boundary, appending
/// [`TRUNCATION_MARKER`] when anything was dropped. The result is always
/// `<= max` bytes (assuming `max >= TRUNCATION_MARKER.len()`).
fn truncate_for_note(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max.saturating_sub(TRUNCATION_MARKER.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{TRUNCATION_MARKER}", &s[..end])
}

/// Map a kopia failure class to the `(action, note)` of the Warning Event we
/// surface on the repository CR. The Event `reason` is the class label itself
/// (`class.as_str()`, set by the caller) so it matches the `Bootstrapped=False`
/// condition reason and is machine-readable; only the remediation hint and the
/// human note vary here. Exhaustive on `KopiaErrorClass` so a new class forces an
/// explicit decision (ADR §5.5).
///
/// The kopia `message` (which carries the stderr tail, up to ~4 KB) is truncated
/// to [`EVENT_MESSAGE_BUDGET_BYTES`] before composing and the whole note is
/// clamped to [`EVENT_NOTE_MAX_BYTES`], so the Event always publishes and the
/// remediation hint always survives.
///
/// `uid` is the operator's effective UID (from [`operator_uid`]) — reported
/// verbatim in the `PermissionDenied` hint so the `chown` advice names the real
/// UID the operator runs as, not a hardcoded guess (it varies with the chart's
/// `podSecurityContext.runAsUser`).
fn backend_failure_event(
    class: KopiaErrorClass,
    message: &str,
    uid: u32,
) -> (&'static str, String) {
    let message = truncate_for_note(message, EVENT_MESSAGE_BUDGET_BYTES);
    let (action, note) = match class {
        KopiaErrorClass::AccessDenied => (
            CHECK_CREDENTIALS_ACTION,
            format!(
                "the storage backend denied access: {message}. The credentials Secret may lack \
                 permission, or the configured bucket/container/path does not exist (some backends \
                 report a missing bucket as \"Access Denied\"). Verify the credentials Secret and \
                 that the bucket/path exists and is reachable."
            ),
        ),
        KopiaErrorClass::PermissionDenied => (
            CHECK_PERMISSIONS_ACTION,
            format!(
                "the repository path is not writable by the operator: {message}. The filesystem \
                 export or PVC must be writable by the operator's UID ({uid}) — fix its \
                 ownership/mode (e.g. `chown -R {uid} <path>`) and reconcile again."
            ),
        ),
        KopiaErrorClass::AuthFailure => (
            CHECK_CREDENTIALS_ACTION,
            format!(
                "the repository password was rejected: {message}. Check the encryption password \
                 Secret (the `KOPIA_PASSWORD` key) referenced by this repository."
            ),
        ),
        KopiaErrorClass::RepositoryUnavailable
        | KopiaErrorClass::NotFound
        | KopiaErrorClass::Locked
        | KopiaErrorClass::SourceError
        | KopiaErrorClass::Unknown => (
            CHECK_BACKEND_ACTION,
            format!("repository backend error ({}): {message}", class.as_str()),
        ),
    };
    (action, truncate_for_note(&note, EVENT_NOTE_MAX_BYTES))
}

/// The operator's effective UID — the identity that writes a filesystem repo in
/// the controller's in-process kopia ops, and (by default) the mover pods' UID.
/// Surfaced in the `PermissionDenied` remediation hint so it names the real UID
/// rather than a hardcoded constant.
fn operator_uid() -> u32 {
    // SAFETY: geteuid() is always-succeeds and thread-safe; it has no
    // preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

/// Surface a repository connect/create failure as a Warning Event on the CR, so
/// *what* the backend rejected (e.g. S3 "Access Denied") is visible from
/// `kubectl get events` / `describe` and not buried in a status condition. The
/// Event `reason` is the kopia class (matching the `Bootstrapped=False`
/// condition). Best-effort: a failed publish is logged, never fatal.
pub async fn publish_backend_failure(
    ctx: &Context,
    regarding: &ObjectReference,
    name: &str,
    class: KopiaErrorClass,
    message: &str,
) {
    let reason = class.as_str();
    let (action, note) = backend_failure_event(class, message, operator_uid());
    if let Err(e) = ctx
        .recorder
        .publish(
            &Event {
                type_: EventType::Warning,
                reason: reason.into(),
                note: Some(note),
                action: action.into(),
                secondary: None,
            },
            regarding,
        )
        .await
    {
        tracing::warn!(error = %e, repo = %name, "failed to publish {reason} event");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::backend::FilesystemBackend;
    use kopiur_api::common::SecretKeyRef;

    /// A representative operator UID for the pure-function tests. Deliberately
    /// NOT the old hardcoded 65534, so the assertions prove the UID is now
    /// interpolated from the argument rather than baked into the message.
    const TEST_UID: u32 = 65532;

    // --- backend_failure_event: the typed kopia class drives the Event's
    // remediation `action` + human note; the `reason` (asserted at the call site)
    // is the class label itself, so it matches the `Bootstrapped=False` condition.
    // (regression: S3 Access Denied used to land as Unknown, only visible via
    // `kubectl describe`.)
    #[test]
    fn backend_failure_access_denied_points_at_credentials_and_bucket() {
        let (action, note) = backend_failure_event(
            KopiaErrorClass::AccessDenied,
            "error retrieving storage config from bucket \"kopiur\": Access Denied",
            TEST_UID,
        );
        assert_eq!(action, CHECK_CREDENTIALS_ACTION);
        assert!(note.contains("denied access"));
        assert!(note.contains("credentials Secret"));
        assert!(note.contains("bucket/path"));
    }

    #[test]
    fn backend_failure_permission_denied_points_at_the_live_uid() {
        // Regression: the hint used to hardcode "commonly 65534"; it must now
        // report the operator's actual UID (here the e2e/distroless 65532) so the
        // `chown` advice is correct under any `podSecurityContext.runAsUser`.
        let (action, note) = backend_failure_event(
            KopiaErrorClass::PermissionDenied,
            "unable to create directory /repo: permission denied",
            TEST_UID,
        );
        assert_eq!(action, CHECK_PERMISSIONS_ACTION);
        assert!(note.contains("not writable"));
        assert!(
            note.contains("65532"),
            "note should name the live UID: {note}"
        );
        assert!(
            note.contains("chown -R 65532"),
            "the chown example should use the live UID: {note}"
        );
        assert!(
            !note.contains("65534"),
            "the old hardcoded UID must be gone: {note}"
        );
    }

    #[test]
    fn backend_failure_other_classes_stay_generic_with_class_and_message() {
        let (action, note) = backend_failure_event(
            KopiaErrorClass::RepositoryUnavailable,
            "connection refused",
            TEST_UID,
        );
        assert_eq!(action, CHECK_BACKEND_ACTION);
        assert!(note.contains("RepositoryUnavailable"));
        assert!(note.contains("connection refused"));
    }

    // --- note truncation: a huge kopia stderr tail must not blow past the
    // Kubernetes 1024-byte Event note limit (regression: the apiserver rejected
    // the Event with a 422 "can have at most 1024 characters", so the actionable
    // PermissionDenied warning never reached `kubectl describe`). ---

    #[test]
    fn backend_failure_note_is_clamped_to_the_event_limit() {
        // A kopia error several KB long (the /nonexistent cache spam + real error)
        // across every class — none may exceed the Event note limit, and the
        // oversized message must visibly carry the truncation marker.
        let huge = "x".repeat(5000);
        for class in [
            KopiaErrorClass::AccessDenied,
            KopiaErrorClass::PermissionDenied,
            KopiaErrorClass::AuthFailure,
            KopiaErrorClass::RepositoryUnavailable,
            KopiaErrorClass::NotFound,
            KopiaErrorClass::Locked,
            KopiaErrorClass::SourceError,
            KopiaErrorClass::Unknown,
        ] {
            let (_, note) = backend_failure_event(class, &huge, TEST_UID);
            assert!(
                note.len() <= EVENT_NOTE_MAX_BYTES,
                "{class:?} note is {} bytes, exceeds the {EVENT_NOTE_MAX_BYTES}-byte Event limit",
                note.len()
            );
            assert!(
                note.contains(TRUNCATION_MARKER),
                "{class:?} note should carry the truncation marker for the cut message"
            );
        }
    }

    #[test]
    fn backend_failure_truncation_keeps_the_remediation_hint() {
        // Even with an oversized message, the static remediation text (the part a
        // user acts on) must survive — the message budget protects it, not just
        // the final clamp.
        let huge = "x".repeat(5000);
        let (action, note) =
            backend_failure_event(KopiaErrorClass::PermissionDenied, &huge, TEST_UID);
        assert_eq!(action, CHECK_PERMISSIONS_ACTION);
        assert!(
            note.contains("not writable"),
            "remediation hint lost to truncation: {note}"
        );
        assert!(
            note.contains("65532"),
            "remediation hint lost to truncation: {note}"
        );
    }

    #[test]
    fn truncate_for_note_is_a_noop_under_budget() {
        let s = "short message";
        assert_eq!(truncate_for_note(s, EVENT_NOTE_MAX_BYTES), s);
    }

    #[test]
    fn truncate_for_note_clamps_and_marks_when_over_budget() {
        let s = "x".repeat(5000);
        let out = truncate_for_note(&s, EVENT_NOTE_MAX_BYTES);
        assert_eq!(out.len(), EVENT_NOTE_MAX_BYTES);
        assert!(out.ends_with(TRUNCATION_MARKER));
    }

    #[test]
    fn truncate_for_note_respects_utf8_boundaries() {
        // A multibyte char straddling the cut must not panic or produce invalid
        // UTF-8 — the result is always valid and within budget.
        let s = "é".repeat(100); // each 'é' is 2 bytes
        let out = truncate_for_note(&s, 51);
        assert!(out.len() <= 51);
        assert!(out.ends_with(TRUNCATION_MARKER));
    }

    fn ref_of(kind: RepositoryKind, name: &str, namespace: Option<&str>) -> RepositoryRef {
        RepositoryRef {
            kind,
            name: name.into(),
            namespace: namespace.map(str::to_string),
        }
    }

    // --- repo_lookup: the regression guard for "ClusterRepository references are
    // ignored" (controller logged `missing dependency: Repository <ns>/<name>`
    // for a `kind: ClusterRepository` config). A ClusterRepository ref MUST map
    // to a cluster-scoped lookup, never a namespaced Repository get. ---

    #[test]
    fn repo_lookup_namespaced_uses_ref_namespace() {
        let r = ref_of(RepositoryKind::Repository, "nas", Some("backups"));
        assert_eq!(
            repo_lookup(&r, "consumer-ns"),
            RepoLookup::Namespaced {
                namespace: "backups".into(),
                name: "nas".into(),
            }
        );
    }

    #[test]
    fn repo_lookup_namespaced_defaults_to_consumer_namespace() {
        let r = ref_of(RepositoryKind::Repository, "nas", None);
        assert_eq!(
            repo_lookup(&r, "consumer-ns"),
            RepoLookup::Namespaced {
                namespace: "consumer-ns".into(),
                name: "nas".into(),
            }
        );
    }

    #[test]
    fn repo_lookup_cluster_is_cluster_scoped_not_namespaced() {
        // This is the bug the user hit: a config referencing
        // `{ kind: ClusterRepository, name: hetzner }` was resolved as a
        // namespaced Repository in the consumer's namespace and never found.
        let r = ref_of(RepositoryKind::ClusterRepository, "hetzner", None);
        assert_eq!(
            repo_lookup(&r, "selfhosted"),
            RepoLookup::Cluster {
                name: "hetzner".into(),
            }
        );
    }

    #[test]
    fn repo_lookup_cluster_ignores_a_stray_namespace() {
        // Even if `namespace` somehow slips through (webhook normally forbids it),
        // a ClusterRepository ref still resolves cluster-scoped — never namespaced.
        let r = ref_of(RepositoryKind::ClusterRepository, "hetzner", Some("oops"));
        assert_eq!(
            repo_lookup(&r, "selfhosted"),
            RepoLookup::Cluster {
                name: "hetzner".into(),
            }
        );
    }

    #[test]
    fn repo_credentials_defaults_password_key() {
        let enc = Encryption {
            password_secret_ref: SecretKeyRef {
                name: "creds".into(),
                namespace: None,
                key: None,
            },
        };
        let c = repo_credentials(&enc);
        assert_eq!(c.secret_name, "creds");
        assert_eq!(c.password_key, "KOPIA_PASSWORD");
    }

    #[test]
    fn repo_credentials_honors_explicit_key_and_namespace() {
        let enc = Encryption {
            password_secret_ref: SecretKeyRef {
                name: "creds".into(),
                namespace: Some("kopia-system".into()),
                key: Some("pw".into()),
            },
        };
        let c = repo_credentials(&enc);
        assert_eq!(c.password_key, "pw");
        assert_eq!(c.namespace.as_deref(), Some("kopia-system"));
    }

    #[test]
    fn filesystem_path_and_pvc_extracted() {
        let b = Backend::Filesystem(FilesystemBackend {
            path: "/repo".into(),
            pvc_name: Some("repo-pvc".into()),
        });
        assert_eq!(filesystem_repo_path(&b).as_deref(), Some("/repo"));
        assert_eq!(filesystem_repo_pvc(&b).as_deref(), Some("repo-pvc"));
    }

    #[test]
    fn s3_backend_has_no_filesystem_path() {
        use kopiur_api::backend::S3Backend;
        let b = Backend::S3(S3Backend {
            bucket: "b".into(),
            prefix: None,
            endpoint: None,
            region: None,
            auth: None,
            tls: None,
        });
        assert_eq!(filesystem_repo_path(&b), None);
        assert_eq!(filesystem_repo_pvc(&b), None);
    }

    #[test]
    fn backend_auth_secret_for_s3_and_none_for_filesystem() {
        use kopiur_api::backend::{BackendAuth, S3Backend};
        use kopiur_api::common::SecretRef;
        let s3 = Backend::S3(S3Backend {
            bucket: "b".into(),
            prefix: None,
            endpoint: None,
            region: None,
            auth: Some(BackendAuth {
                secret_ref: Some(SecretRef {
                    name: "s3-creds".into(),
                    namespace: Some("kopiur-system".into()),
                }),
                workload_identity: None,
            }),
            tls: None,
        });
        assert_eq!(
            backend_auth_secret_ref(&s3).map(|s| s.name.as_str()),
            Some("s3-creds")
        );
        let fs = Backend::Filesystem(FilesystemBackend {
            path: "/repo".into(),
            pvc_name: None,
        });
        assert!(backend_auth_secret_ref(&fs).is_none());
    }

    #[test]
    fn mover_creds_dedupe_when_password_and_backend_share_a_secret() {
        use kopiur_api::backend::{BackendAuth, S3Backend};
        use kopiur_api::common::{SecretKeyRef, SecretRef};
        let enc = Encryption {
            password_secret_ref: SecretKeyRef {
                name: "kopia-rustfs-creds".into(),
                namespace: Some("kopiur-system".into()),
                key: None,
            },
        };
        // Same secret holds password + AWS keys (the homelab layout) -> one entry.
        let same = Backend::S3(S3Backend {
            bucket: "b".into(),
            prefix: None,
            endpoint: None,
            region: None,
            auth: Some(BackendAuth {
                secret_ref: Some(SecretRef {
                    name: "kopia-rustfs-creds".into(),
                    namespace: Some("kopiur-system".into()),
                }),
                workload_identity: None,
            }),
            tls: None,
        });
        assert_eq!(mover_creds_secrets(&same, &enc), vec!["kopia-rustfs-creds"]);

        // Separate secrets -> both, password first.
        let split = Backend::S3(S3Backend {
            bucket: "b".into(),
            prefix: None,
            endpoint: None,
            region: None,
            auth: Some(BackendAuth {
                secret_ref: Some(SecretRef {
                    name: "s3-creds".into(),
                    namespace: Some("kopiur-system".into()),
                }),
                workload_identity: None,
            }),
            tls: None,
        });
        assert_eq!(
            mover_creds_secrets(&split, &enc),
            vec!["kopia-rustfs-creds", "s3-creds"]
        );
    }

    #[test]
    fn child_meta_omits_empty_labels() {
        let m = child_meta("n", "ns", BTreeMap::new(), None);
        assert_eq!(m.name.as_deref(), Some("n"));
        assert!(m.labels.is_none());
    }

    // --- upsert_condition ---------------------------------------------------

    #[test]
    fn upsert_condition_inserts_new_and_preserves_others() {
        let other = Condition {
            type_: "Ready".into(),
            status: "True".into(),
            reason: "Ok".into(),
            message: "ready".into(),
            last_transition_time: Time(k8s_openapi::jiff::Timestamp::now()),
            observed_generation: Some(1),
        };
        let out = upsert_condition(
            std::slice::from_ref(&other),
            "MaintenanceConfigured",
            false,
            "MaintenanceNotConfigured",
            "no maintenance",
            Some(2),
        );
        assert_eq!(out.len(), 2);
        // Pre-existing condition is untouched.
        assert!(out.iter().any(|c| c.type_ == "Ready" && c.status == "True"));
        let m = out
            .iter()
            .find(|c| c.type_ == "MaintenanceConfigured")
            .unwrap();
        assert_eq!(m.status, "False");
        assert_eq!(m.reason, "MaintenanceNotConfigured");
        assert_eq!(m.observed_generation, Some(2));
    }

    #[test]
    fn upsert_condition_preserves_transition_time_when_status_unchanged() {
        let t0 = Time(k8s_openapi::jiff::Timestamp::from_second(1_700_000_000).unwrap());
        let existing = vec![Condition {
            type_: "MaintenanceConfigured".into(),
            status: "False".into(),
            reason: "MaintenanceNotConfigured".into(),
            message: "old".into(),
            last_transition_time: t0.clone(),
            observed_generation: Some(1),
        }];
        // Same status (still False) -> timestamp must NOT move, but message updates.
        let out = upsert_condition(
            &existing,
            "MaintenanceConfigured",
            false,
            "MaintenanceNotConfigured",
            "new message",
            Some(2),
        );
        let m = &out[0];
        assert_eq!(m.last_transition_time, t0, "timestamp moved on no-op");
        assert_eq!(m.message, "new message");
        assert_eq!(m.observed_generation, Some(2));
    }

    #[test]
    fn upsert_condition_bumps_transition_time_on_flip() {
        let t0 = Time(k8s_openapi::jiff::Timestamp::from_second(1_700_000_000).unwrap());
        let existing = vec![Condition {
            type_: "MaintenanceConfigured".into(),
            status: "False".into(),
            reason: "MaintenanceNotConfigured".into(),
            message: "old".into(),
            last_transition_time: t0.clone(),
            observed_generation: Some(1),
        }];
        // Flip False -> True: timestamp must advance.
        let out = upsert_condition(
            &existing,
            "MaintenanceConfigured",
            true,
            "MaintenanceConfigured",
            "now configured",
            Some(2),
        );
        let m = &out[0];
        assert_eq!(m.status, "True");
        assert_ne!(
            m.last_transition_time, t0,
            "timestamp did not advance on flip"
        );
    }

    // --- idempotent status writes (the hot-loop fix) -------------------------

    #[test]
    fn status_patch_noop_when_subset_unchanged() {
        let current = serde_json::json!({
            "phase": "Failed",
            "backend": "Filesystem",
            "observedGeneration": 3,
            "conditions": [{ "type": "Bootstrapped", "status": "False", "reason": "PermissionDenied" }],
            "uniqueId": "abc",            // an extra key the desired doesn't touch
        });
        // Desired is a subset that matches → no-op (a merge patch never removes the
        // keys it omits, so we only compare the keys we'd write).
        let desired = serde_json::json!({
            "phase": "Failed",
            "backend": "Filesystem",
            "observedGeneration": 3,
            "conditions": [{ "type": "Bootstrapped", "status": "False", "reason": "PermissionDenied" }],
        });
        assert!(status_patch_is_noop(Some(&current), &desired));
    }

    #[test]
    fn status_patch_not_noop_on_reason_or_generation_or_absent() {
        let current = serde_json::json!({
            "phase": "Failed",
            "observedGeneration": 3,
            "conditions": [{ "type": "Bootstrapped", "status": "False", "reason": "PermissionDenied" }],
        });
        // A new generation must write (the spec changed → re-attempt).
        let newer_gen = serde_json::json!({ "phase": "Failed", "observedGeneration": 4 });
        assert!(!status_patch_is_noop(Some(&current), &newer_gen));
        // A different condition reason must write.
        let new_reason = serde_json::json!({
            "conditions": [{ "type": "Bootstrapped", "status": "False", "reason": "AuthFailure" }],
        });
        assert!(!status_patch_is_noop(Some(&current), &new_reason));
        // No status at all (first reconcile) is never a no-op.
        assert!(!status_patch_is_noop(None, &newer_gen));
        assert!(!status_patch_is_noop(
            Some(&serde_json::Value::Null),
            &newer_gen
        ));
    }

    #[test]
    fn status_patch_noop_ignores_volatile_message_only_when_message_matches() {
        // The condition message is now class-derived (stable). If two desired
        // payloads carry the SAME stable message + same reason/generation, the
        // second is a no-op. (A volatile message would differ here and force a
        // write — which is exactly the loop we removed by switching to summary().)
        let stable = "repository path is not writable by the operator's UID";
        let current = serde_json::json!({
            "phase": "Failed",
            "observedGeneration": 2,
            "conditions": [{ "type": "Bootstrapped", "status": "False", "reason": "PermissionDenied", "message": stable }],
        });
        let desired = serde_json::json!({
            "phase": "Failed",
            "observedGeneration": 2,
            "conditions": [{ "type": "Bootstrapped", "status": "False", "reason": "PermissionDenied", "message": stable }],
        });
        assert!(status_patch_is_noop(Some(&current), &desired));
    }

    #[test]
    fn terminal_gate_only_on_failed_at_current_generation() {
        use kopiur_api::RepositoryPhase;
        // Failed at the current generation → terminal (hard-stop).
        assert!(is_terminal_for_generation(
            Some(RepositoryPhase::Failed),
            Some(5),
            Some(5)
        ));
        // Failed but the spec moved on (gen bumped) → gate reopens, re-attempt.
        assert!(!is_terminal_for_generation(
            Some(RepositoryPhase::Failed),
            Some(5),
            Some(6)
        ));
        // Degraded (a retryable failure) is never terminal — keep retrying.
        assert!(!is_terminal_for_generation(
            Some(RepositoryPhase::Degraded),
            Some(5),
            Some(5)
        ));
        // No generation yet / no observed generation → not terminal.
        assert!(!is_terminal_for_generation(
            Some(RepositoryPhase::Failed),
            None,
            Some(5)
        ));
        assert!(!is_terminal_for_generation(
            Some(RepositoryPhase::Failed),
            Some(5),
            None
        ));
    }

    // --- managed Maintenance projection (ADR §3.7, default-on) ---------------

    fn dummy_owner(kind: &str, name: &str) -> OwnerReference {
        OwnerReference {
            api_version: API_VERSION.into(),
            kind: kind.into(),
            name: name.into(),
            uid: "uid-1".into(),
            controller: Some(true),
            block_owner_deletion: Some(false),
        }
    }

    #[test]
    fn build_managed_maintenance_for_namespaced_repository() {
        let spec = RepositoryMaintenanceSpec::default();
        let m = build_managed_maintenance(
            RepositoryKind::Repository,
            "nas",
            "apps",
            &spec,
            dummy_owner("Repository", "nas"),
        );
        // 1:1 naming, lives in the repository's namespace, owned by the repo.
        assert_eq!(m.metadata.name.as_deref(), Some("nas"));
        assert_eq!(m.metadata.namespace.as_deref(), Some("apps"));
        assert!(is_managed_by(&m, "Repository", "nas"));
        // Same-namespace ref (namespace omitted), default schedule, default lease.
        assert_eq!(m.spec.repository.kind, RepositoryKind::Repository);
        assert_eq!(m.spec.repository.name, "nas");
        assert!(m.spec.repository.namespace.is_none());
        assert_eq!(m.spec.schedule, default_maintenance_schedule());
        assert_eq!(m.spec.ownership.owner, "kopiur/apps/nas");
        assert_eq!(
            m.spec.ownership.takeover_policy,
            kopiur_api::TakeoverPolicy::Never
        );
    }

    #[test]
    fn build_managed_maintenance_for_cluster_repository_uses_overrides() {
        use kopiur_api::common::CronSpec;
        use kopiur_api::{MaintenanceSchedule, TakeoverPolicy};
        let spec = RepositoryMaintenanceSpec {
            enabled: true,
            schedule: Some(MaintenanceSchedule {
                quick: CronSpec {
                    cron: "0 */2 * * *".into(),
                    jitter: None,
                },
                full: CronSpec {
                    cron: "0 1 * * *".into(),
                    jitter: None,
                },
                timezone: Some("UTC".into()),
            }),
            takeover_policy: Some(TakeoverPolicy::Force),
            namespace: Some("kopia-system".into()),
            ..Default::default()
        };
        let m = build_managed_maintenance(
            RepositoryKind::ClusterRepository,
            "hetzner",
            "kopia-system",
            &spec,
            dummy_owner("ClusterRepository", "hetzner"),
        );
        assert_eq!(m.metadata.namespace.as_deref(), Some("kopia-system"));
        assert_eq!(m.spec.repository.kind, RepositoryKind::ClusterRepository);
        // Cluster ref must never carry a namespace.
        assert!(m.spec.repository.namespace.is_none());
        assert_eq!(m.spec.schedule.quick.cron, "0 */2 * * *");
        assert_eq!(m.spec.ownership.owner, "kopiur/clusterrepository/hetzner");
        assert_eq!(m.spec.ownership.takeover_policy, TakeoverPolicy::Force);
    }

    #[test]
    fn maintenance_action_covers_the_matrix() {
        use MaintenanceAction::*;
        // enabled, no foreign, placement resolved -> manage.
        assert_eq!(maintenance_action(true, false, false, true), Manage);
        assert_eq!(maintenance_action(true, false, true, true), Manage);
        // enabled, no foreign, placement UNresolved -> unresolved.
        assert_eq!(maintenance_action(true, false, false, false), Unresolved);
        // foreign present -> never manage; remove a stale managed one.
        assert_eq!(maintenance_action(true, true, true, true), Unmanage);
        assert_eq!(maintenance_action(true, true, false, true), Leave);
        // disabled -> remove managed if any, else leave (never warns/ignores foreign).
        assert_eq!(maintenance_action(false, false, true, true), Unmanage);
        assert_eq!(maintenance_action(false, false, false, true), Leave);
        assert_eq!(maintenance_action(false, true, true, true), Unmanage);
        assert_eq!(maintenance_action(false, true, false, true), Leave);
    }

    fn maint_referencing(
        name: &str,
        ns: &str,
        r: RepositoryRef,
        owner: Option<OwnerReference>,
    ) -> Maintenance {
        let mut m = Maintenance::new(
            name,
            MaintenanceSpec {
                repository: r,
                schedule: default_maintenance_schedule(),
                ownership: Ownership {
                    owner: "lease".into(),
                    takeover_policy: Default::default(),
                },
                mover: None,
                failure_policy: None,
            },
        );
        m.metadata.namespace = Some(ns.into());
        m.metadata.owner_references = owner.map(|o| vec![o]);
        m
    }

    #[test]
    fn classify_maintenance_distinguishes_managed_foreign_and_unrelated() {
        let managed = maint_referencing(
            "nas",
            "apps",
            ref_of(RepositoryKind::Repository, "nas", None),
            Some(dummy_owner("Repository", "nas")),
        );
        let foreign = maint_referencing(
            "user-maint",
            "apps",
            ref_of(RepositoryKind::Repository, "nas", None),
            None,
        );
        let unrelated = maint_referencing(
            "other",
            "apps",
            ref_of(RepositoryKind::Repository, "different", None),
            None,
        );

        // Managed only.
        let (f, m) = classify_maintenance(
            vec![managed.clone(), unrelated.clone()],
            RepositoryKind::Repository,
            "Repository",
            "nas",
            Some("apps"),
        );
        assert!(!f);
        assert_eq!(
            m.as_ref().and_then(|m| m.metadata.name.as_deref()),
            Some("nas")
        );

        // Foreign only.
        let (f, m) = classify_maintenance(
            vec![foreign.clone()],
            RepositoryKind::Repository,
            "Repository",
            "nas",
            Some("apps"),
        );
        assert!(f);
        assert!(m.is_none());

        // Both present: foreign flagged AND managed found (so a stale managed one
        // is removed while deferring to the user's).
        let (f, m) = classify_maintenance(
            vec![managed, foreign],
            RepositoryKind::Repository,
            "Repository",
            "nas",
            Some("apps"),
        );
        assert!(f);
        assert!(m.is_some());
    }

    #[test]
    fn classify_maintenance_matches_cluster_repository_by_owner_ref() {
        let managed = maint_referencing(
            "hetzner",
            "kopia-system",
            ref_of(RepositoryKind::ClusterRepository, "hetzner", None),
            Some(dummy_owner("ClusterRepository", "hetzner")),
        );
        let (f, m) = classify_maintenance(
            vec![managed],
            RepositoryKind::ClusterRepository,
            "ClusterRepository",
            "hetzner",
            None,
        );
        assert!(!f);
        assert!(m.is_some());
    }
}
