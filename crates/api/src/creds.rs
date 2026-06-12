//! Pure credential/volume metadata over the CRD types: which Secrets a
//! repository's mover needs. Shared by the controller (envFrom projection,
//! referent watches) and external tooling (`kubectl kopiur doctor`), so the
//! "what credentials does this backend reference" answer cannot fork.

use crate::backend::{Backend, WorkloadIdentity};
use crate::common::Encryption;

/// The backend credentials Secret name for an object-store backend, if any.
///
/// Exhaustive over [`Backend`] (ADR §5.5): a new backend cannot compile until its
/// credential source is decided here. Object stores read keys (e.g.
/// `AWS_ACCESS_KEY_ID`) from `auth.secretRef`; Rclone reads its config from
/// `configSecretRef`; Filesystem has no backend credentials. This Secret is
/// mounted into the mover Job alongside the encryption-password Secret so kopia
/// can reach the store (the in-process filesystem path never needs it).
pub fn backend_auth_secret_ref(backend: &Backend) -> Option<&crate::common::SecretRef> {
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

/// Which cloud IAM plane a workload-identity backend federates with. Drives the
/// cloud-specific mover wiring (the Azure pod label; docs/messages naming the
/// right SA annotation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadIdentityCloud {
    /// AWS: IRSA (web-identity token) or EKS Pod Identity via the minio-go
    /// credential chain.
    S3,
    /// Azure Workload Identity: the azure-workload-identity webhook injects
    /// `AZURE_TENANT_ID`/`AZURE_CLIENT_ID`/`AZURE_FEDERATED_TOKEN_FILE` into
    /// pods carrying the opt-in label and running as the federated SA.
    Azure,
    /// GKE Workload Identity: ambient ADC via the GKE metadata server.
    Gcs,
}

/// The backend's workload-identity binding, if any, with its cloud plane.
///
/// Exhaustive over [`Backend`] (ADR §5.5): only S3/Azure/GCS can carry one —
/// the other backends' auth types make it unrepresentable, and a new backend
/// cannot compile until its arm is decided here.
pub fn backend_workload_identity(
    backend: &Backend,
) -> Option<(&WorkloadIdentity, WorkloadIdentityCloud)> {
    match backend {
        Backend::S3(b) => b
            .auth
            .as_ref()
            .and_then(|a| a.workload_identity.as_ref())
            .map(|wi| (wi, WorkloadIdentityCloud::S3)),
        Backend::Azure(b) => b
            .auth
            .as_ref()
            .and_then(|a| a.workload_identity.as_ref())
            .map(|wi| (wi, WorkloadIdentityCloud::Azure)),
        Backend::Gcs(b) => b
            .auth
            .as_ref()
            .and_then(|a| a.workload_identity.as_ref())
            .map(|wi| (wi, WorkloadIdentityCloud::Gcs)),
        Backend::B2(_)
        | Backend::Sftp(_)
        | Backend::WebDav(_)
        | Backend::Rclone(_)
        | Backend::Filesystem(_) => None,
    }
}

/// A credential Secret a mover Job needs, with the namespace it is sourced from.
/// `namespace` is the resolved *source* namespace (where the operator reads the
/// Secret when projecting), not the Job's namespace. `None` only when neither the
/// reference nor the repository carries one — which projection treats as an
/// actionable error (a `ClusterRepository` reference must pin a namespace).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredsSecretRef {
    /// Name of the credential `Secret`.
    pub name: String,
    /// Resolved source namespace, if known.
    pub namespace: Option<String>,
}

/// The distinct credential Secrets a mover Job for `backend` + `encryption` needs
/// as `envFrom`, each with its resolved *source* namespace: always the
/// encryption-password Secret, plus the backend `auth` Secret when present and
/// differently named. Deduped by name, order-stable (password first).
///
/// `repo_namespace` is the referencing repository's own namespace (a namespaced
/// `Repository`), used as the source-namespace fallback when a reference omits
/// one; pass `None` for a cluster-scoped `ClusterRepository`, whose references
/// pin their own namespace. This is the single source of the dedup/order contract
/// that [`mover_creds_secrets`] (names only) is built on.
pub fn mover_creds_secret_refs(
    backend: &Backend,
    enc: &Encryption,
    repo_namespace: Option<&str>,
) -> Vec<CredsSecretRef> {
    let source_ns = |ns: Option<String>| ns.or_else(|| repo_namespace.map(str::to_string));
    let mut refs = vec![CredsSecretRef {
        name: enc.password_secret_ref.name.clone(),
        namespace: source_ns(enc.password_secret_ref.namespace.clone()),
    }];
    if let Some(auth) = backend_auth_secret_ref(backend)
        && !refs.iter().any(|r| r.name == auth.name)
    {
        refs.push(CredsSecretRef {
            name: auth.name.clone(),
            namespace: source_ns(auth.namespace.clone()),
        });
    }
    refs
}

/// The distinct credential Secret names a mover Job for `backend` + `encryption`
/// needs as `envFrom`: always the encryption-password Secret, plus the backend
/// `auth` Secret when present and different. Deduped, order-stable (password
/// first). The common single-secret setup (password + keys in one Secret)
/// collapses to one entry. Names-only projection of [`mover_creds_secret_refs`].
pub fn mover_creds_secrets(backend: &Backend, enc: &Encryption) -> Vec<String> {
    mover_creds_secret_refs(backend, enc, None)
        .into_iter()
        .map(|r| r.name)
        .collect()
}
