//! Shared cluster-IO helpers for the reconcilers (the "thin IO calling tested
//! pure fns" layer, ADR §5.2/§5.4).
//!
//! These wrap the repetitive `kube::Api` mechanics — server-side apply with a
//! stable field manager, finalizer add/remove, status subresource patches, and
//! resolving the credentials Secret for a repository — so each reconciler stays
//! focused on its decision logic. The decision logic itself lives in the
//! per-reconciler pure functions (which remain unit-tested without a cluster).

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{Patch, PatchParams};
use kube::core::ObjectMeta;
use kube::{Api, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use serde::Serialize;

use kopiur_api::backend::Backend;
use kopiur_api::common::Encryption;

use crate::consts::API_VERSION;
use crate::error::{Error, Result};

/// The field-manager used for every server-side apply the controller performs.
pub const FIELD_MANAGER: &str = "kopiur.dev/controller";

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

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::backend::FilesystemBackend;
    use kopiur_api::common::SecretKeyRef;

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
    fn child_meta_omits_empty_labels() {
        let m = child_meta("n", "ns", BTreeMap::new(), None);
        assert_eq!(m.name.as_deref(), Some("n"));
        assert!(m.labels.is_none());
    }
}
