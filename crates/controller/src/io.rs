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
use kube::api::{Patch, PatchParams};
use kube::core::ObjectMeta;
use kube::runtime::events::{Event, EventType};
use kube::runtime::reflector::Store;
use kube::{Api, Resource, ResourceExt};
use serde::Serialize;
use serde::de::DeserializeOwned;

use kopiur_api::backend::Backend;
use kopiur_api::common::{Encryption, RepositoryKind, RepositoryRef};
use kopiur_api::{ClusterRepository, Maintenance, Repository};

use crate::consts::{
    API_VERSION, CHECK_MAINTENANCE_ACTION, MAINTENANCE_CONFIGURED_CONDITION,
    MAINTENANCE_CONFIGURED_REASON, MAINTENANCE_NOT_CONFIGURED_REASON,
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
    pub backend: Backend,
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
    Namespaced { namespace: String, name: String },
    /// Cluster-scoped `ClusterRepository` get (`Api::all`).
    Cluster { name: String },
}

/// Pure mapping from a consumer's [`RepositoryRef`] (+ the default namespace to
/// use when the ref omits one) to the API lookup it implies. Exhaustive over
/// [`RepositoryKind`] (ADR §5.5): a new variant cannot compile until handled.
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

/// Surface whether a `Maintenance` references this repository (ADR §3.7): a
/// Warning Event when none does, the `MaintenanceConfigured` status condition,
/// and the `kopiur_repository_maintenance_configured` gauge. Membership is read
/// from the shared informer store ([`repository_has_maintenance`]).
///
/// Degrade-not-crash: if the store has not yet synced, the check is skipped (the
/// `.watches` trigger and the periodic requeue re-evaluate once warm), so a cold
/// cache never emits a false "not configured" warning. `metric_namespace` is the
/// gauge's `namespace` label (empty for a `ClusterRepository`); `match_namespace`
/// is the repository's namespace for ref-matching (`None` for `ClusterRepository`).
#[allow(clippy::too_many_arguments)]
pub async fn check_maintenance<K>(
    ctx: &Context,
    api: &Api<K>,
    regarding: &ObjectReference,
    kind: RepositoryKind,
    metric_kind: &str,
    metric_namespace: &str,
    match_namespace: Option<&str>,
    name: &str,
    existing_conditions: &[Condition],
    observed_generation: Option<i64>,
) where
    K: Resource + DeserializeOwned + Clone + std::fmt::Debug,
{
    if !ctx.maintenance_synced.load(Ordering::Relaxed) {
        return;
    }

    let configured =
        repository_has_maintenance(&ctx.maintenance_store, kind, name, match_namespace);

    ctx.metrics.set_repository_maintenance_configured(
        metric_kind,
        metric_namespace,
        name,
        configured,
    );

    let (reason, message) = if configured {
        (
            MAINTENANCE_CONFIGURED_REASON,
            format!("a Maintenance resource references {metric_kind} {name}"),
        )
    } else {
        let msg = format!(
            "no Maintenance resource references {metric_kind} {name}; kopia storage will not be \
             reclaimed — create a Maintenance referencing this repository"
        );
        if let Err(e) = ctx
            .recorder
            .publish(
                &Event {
                    type_: EventType::Warning,
                    reason: MAINTENANCE_NOT_CONFIGURED_REASON.into(),
                    note: Some(msg.clone()),
                    action: CHECK_MAINTENANCE_ACTION.into(),
                    secondary: None,
                },
                regarding,
            )
            .await
        {
            tracing::warn!(error = %e, repo = %name, "failed to publish MaintenanceNotConfigured event");
        }
        (MAINTENANCE_NOT_CONFIGURED_REASON, msg)
    };

    let conditions = upsert_condition(
        existing_conditions,
        MAINTENANCE_CONFIGURED_CONDITION,
        configured,
        reason,
        &message,
        observed_generation,
    );
    if let Err(e) = patch_status(api, name, serde_json::json!({ "conditions": conditions })).await {
        tracing::warn!(error = %e, repo = %name, "failed to patch MaintenanceConfigured condition");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::backend::FilesystemBackend;
    use kopiur_api::common::SecretKeyRef;

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
}
