//! Cluster-IO helpers for the kopia **web-UI server** (`spec.server`): the thin
//! `kube::Api` mechanics the pure builders in [`crate::server`] don't do —
//! create-once Secrets, cross-namespace credential mirroring, PVC access-mode
//! reads, and idempotent apply/delete of the Deployment/Service/ConfigMap.

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Resource};
use serde::de::DeserializeOwned;

use super::apply;
use crate::error::{Error, Result};

/// Generate a random alphanumeric UI password (used by `Generate` auth, written
/// once into an operator-owned Secret).
pub fn random_password() -> String {
    use rand::Rng;
    use rand::distr::Alphanumeric;
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

/// Read a single key's value from a Secret as a UTF-8 string.
pub async fn read_secret_value(
    client: &kube::Client,
    namespace: &str,
    secret_name: &str,
    key: &str,
) -> Result<String> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let secret = api.get(secret_name).await.map_err(|e| {
        Error::MissingDependency(format!("secret {namespace}/{secret_name} not found: {e}"))
    })?;
    let data = secret.data.unwrap_or_default();
    let raw = data.get(key).ok_or_else(|| {
        Error::MissingDependency(format!(
            "secret {namespace}/{secret_name} missing key {key}"
        ))
    })?;
    String::from_utf8(raw.0.clone())
        .map_err(|e| Error::Invariant(format!("secret value not valid utf-8: {e}")))
}

/// Create a Secret **once**, never overwriting existing data. Returns `Ok` whether
/// it created the Secret or found it already present — so the `Generate` auth path
/// never rotates the UI password on a subsequent reconcile (which a force-apply would).
pub async fn ensure_secret_once(
    client: &kube::Client,
    namespace: &str,
    secret: &Secret,
) -> Result<()> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let name = secret
        .metadata
        .name
        .clone()
        .ok_or_else(|| Error::Invariant("generated secret has no name".into()))?;
    if api.get_opt(&name).await?.is_some() {
        return Ok(());
    }
    match api.create(&PostParams::default(), secret).await {
        Ok(_) => Ok(()),
        // Lost a create race with another replica/reconcile — the Secret exists now.
        Err(kube::Error::Api(ae)) if ae.code == 409 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Mirror a source Secret's data into `dst` (server-side apply). Used to project a
/// `ClusterRepository`'s credentials Secret into the (possibly different) server
/// namespace, since `envFrom` can only reference a Secret in the pod's own namespace.
pub async fn mirror_secret(
    client: &kube::Client,
    src_namespace: &str,
    src_name: &str,
    mut dst: Secret,
) -> Result<()> {
    let src_api: Api<Secret> = Api::namespaced(client.clone(), src_namespace);
    let src = src_api.get(src_name).await.map_err(|e| {
        Error::MissingDependency(format!(
            "credentials secret {src_namespace}/{src_name} not found: {e}"
        ))
    })?;
    dst.data = src.data;
    dst.type_ = src.type_;
    let dst_ns = dst
        .metadata
        .namespace
        .clone()
        .ok_or_else(|| Error::Invariant("mirror secret has no namespace".into()))?;
    let dst_name = dst
        .metadata
        .name
        .clone()
        .ok_or_else(|| Error::Invariant("mirror secret has no name".into()))?;
    let api: Api<Secret> = Api::namespaced(client.clone(), &dst_ns);
    apply(&api, &dst_name, &dst).await?;
    Ok(())
}

/// The access modes declared on a PVC (`ReadWriteOnce`, `ReadWriteMany`, …).
pub async fn pvc_access_modes(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> Result<Vec<String>> {
    use k8s_openapi::api::core::v1::PersistentVolumeClaim;
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    let pvc = api
        .get_opt(name)
        .await?
        .ok_or_else(|| Error::MissingDependency(format!("PVC {namespace}/{name}")))?;
    Ok(pvc.spec.and_then(|s| s.access_modes).unwrap_or_default())
}

/// Apply the server's `ConfigMap` + `Deployment` + `Service` (server-side).
pub async fn apply_server_objects(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    config_map: &ConfigMap,
    deployment: &Deployment,
    service: &Service,
) -> Result<()> {
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    apply(&cm_api, name, config_map).await?;
    let dep_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    apply(&dep_api, name, deployment).await?;
    let svc_api: Api<Service> = Api::namespaced(client.clone(), namespace);
    apply(&svc_api, name, service).await?;
    Ok(())
}

/// Delete a named object, treating `404 NotFound` as success (idempotent cleanup).
async fn delete_if_present<K>(api: &Api<K>, name: &str) -> Result<()>
where
    K: Resource + DeserializeOwned + Clone + std::fmt::Debug,
{
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ae)) if ae.code == 404 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Delete the server's `Deployment`, `Service`, `ConfigMap`, and (optionally) the
/// generated-auth `Secret` by name in `namespace`. Idempotent. Used for toggle-off,
/// namespace migration, and `ClusterRepository` finalizer cleanup (where owner-ref
/// GC cannot reach a namespaced child of a cluster-scoped owner).
pub async fn delete_server_objects(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    generated_secret: Option<&str>,
) -> Result<()> {
    let dep_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    delete_if_present(&dep_api, name).await?;
    let svc_api: Api<Service> = Api::namespaced(client.clone(), namespace);
    delete_if_present(&svc_api, name).await?;
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    delete_if_present(&cm_api, name).await?;
    if let Some(secret) = generated_secret {
        let secret_api: Api<Secret> = Api::namespaced(client.clone(), namespace);
        delete_if_present(&secret_api, secret).await?;
    }
    Ok(())
}

/// Delete a single Secret by name, treating `404` as success.
pub async fn delete_secret_if_present(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> Result<()> {
    let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    delete_if_present(&api, name).await
}
