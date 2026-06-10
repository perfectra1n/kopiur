use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::Api;

use kopiur_api::backend::{Backend, RepoVolume};
use kopiur_api::cluster_repository::IdentityDefaults;
use kopiur_api::common::{
    Encryption, MoverDefaults, NamespaceDeletePolicy, RepositoryKind, RepositoryMode, RepositoryRef,
};
use kopiur_api::{ClusterRepository, Repository};

use crate::error::{Error, Result};
use crate::jobs::MountSource;

/// Default key within the encryption password Secret when unset.
pub const DEFAULT_PASSWORD_KEY: &str = "KOPIA_PASSWORD";

/// Resolve the mover work-spec throttle from a repository's `moverDefaults.throttle`
/// (ADR-0005 §13(e)). Thin wrapper over the single tested mapping so the field is
/// never inert; an absent throttle yields an empty spec (the mover skips the call).
pub fn throttle_spec(defaults: Option<&MoverDefaults>) -> kopiur_mover::workspec::ThrottleSpec {
    kopiur_mover::workspec::ThrottleSpec::from_mover_defaults(defaults)
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
    /// The repository's own namespace (`Some` for a namespaced [`Repository`],
    /// `None` for a cluster-scoped [`ClusterRepository`]). Used as the *source*
    /// namespace fallback when a credential Secret reference omits one.
    pub repo_namespace: Option<String>,
    /// The repository's `moverDefaults` — the base for every mover it spawns
    /// (security context, pod security context, resources, cache, node placement,
    /// Job TTL), merged field-wise under each run's `mover` (ADR-0004 §1/§2).
    pub mover_defaults: Option<MoverDefaults>,
    /// The `ClusterRepository`'s `identityDefaults` (CEL `*Expr`), applied when a
    /// consumer doesn't override its identity (ADR-0004 §5). `None` for a namespaced
    /// `Repository` (which has no identity defaults).
    pub identity_defaults: Option<IdentityDefaults>,
    /// The repository's namespace-deletion cascade policy (ADR-0005 §5): `Orphan`
    /// (keep history) or `Delete` (cascade). Consulted by the `Snapshot` finalizer
    /// when the owning namespace is terminating.
    pub on_namespace_delete: NamespaceDeletePolicy,
    /// The `ClusterRepository`-owner credential-projection allow gate
    /// (`credentialProjection.allowed`, ADR-0005 §8). `false` for a namespaced
    /// `Repository` (which has no such gate — projection there is a same-namespace
    /// no-op, so the owner gate is irrelevant).
    pub credential_projection_allowed: bool,
    /// The repository's access mode (`ReadWrite`/`ReadOnly`, ADR-0005 §11). A
    /// `ReadOnly` repo refuses backup Jobs and maintenance (restores allowed); the
    /// `Snapshot` reconciler gates on [`RepositoryMode::allows_writes`].
    pub mode: RepositoryMode,
    /// An `OwnerReference` to the repository CR itself. The namespace-deletion
    /// cascade runs its `SnapshotDelete` Job *outside* the terminating namespace
    /// (ADR-0005 §5), where the deleting `Snapshot` cannot own it (cross-namespace
    /// owner references are invalid) — the repository, which outlives the
    /// namespace, owns the Job instead so GC still reaps it.
    pub owner_ref: OwnerReference,
}

/// Which API a [`RepositoryRef`] resolves against, derived purely from `kind`.
///
/// Extracting this from the IO lets the namespaced-vs-cluster decision be
/// unit-tested without a cluster. It is the regression guard for the class of
/// bug where a `kind: ClusterRepository` ref silently fell through to a
/// namespaced `Repository` lookup and produced `missing dependency: Repository
/// <ns>/<name>` for cluster-backed `SnapshotPolicy`s.
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
            let owner_ref = super::owner_ref_for(&repo, "Repository")?;
            Ok(ResolvedRepository {
                repo_namespace: Some(namespace),
                backend: repo.spec.backend,
                encryption: repo.spec.encryption,
                mover_defaults: repo.spec.mover_defaults,
                // A namespaced Repository has no identity defaults.
                identity_defaults: None,
                on_namespace_delete: repo.spec.on_namespace_delete,
                // A namespaced Repository has no owner-side projection gate: its
                // credential Secret co-resides with the consumer, so projection is a
                // same-namespace no-op. Treat the gate as not-applicable (false).
                credential_projection_allowed: false,
                mode: repo.spec.mode,
                owner_ref,
            })
        }
        RepoLookup::Cluster { name } => {
            let api: Api<ClusterRepository> = Api::all(client.clone());
            let repo = api
                .get_opt(&name)
                .await?
                .ok_or_else(|| Error::MissingDependency(format!("ClusterRepository {name}")))?;
            let owner_ref = super::owner_ref_for(&repo, "ClusterRepository")?;
            Ok(ResolvedRepository {
                repo_namespace: None,
                backend: repo.spec.backend,
                encryption: repo.spec.encryption,
                mover_defaults: repo.spec.mover_defaults,
                identity_defaults: repo.spec.identity_defaults,
                on_namespace_delete: repo.spec.on_namespace_delete,
                credential_projection_allowed: repo
                    .spec
                    .credential_projection
                    .map(|p| p.allowed)
                    .unwrap_or(false),
                mode: repo.spec.mode,
                owner_ref,
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

/// Whether the named `Namespace` is being torn down — its `deletionTimestamp` is
/// set, or its `status.phase` is `Terminating`. Used by the `Snapshot` finalizer to
/// decide the namespace-deletion cascade (ADR-0005 §5). A missing Namespace (already
/// gone) counts as terminating — the namespace is clearly being removed. A transient
/// read error propagates so the caller can back off rather than guess.
pub async fn namespace_is_terminating(client: &kube::Client, namespace: &str) -> Result<bool> {
    use k8s_openapi::api::core::v1::Namespace;
    let api: Api<Namespace> = Api::all(client.clone());
    match api.get_opt(namespace).await? {
        None => Ok(true),
        Some(ns) => {
            let has_ts = ns.metadata.deletion_timestamp.is_some();
            let terminating = ns
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .map(|p| p == "Terminating")
                .unwrap_or(false);
            Ok(has_ts || terminating)
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

/// The filesystem repo path for a `Filesystem` backend, or `None` for object
/// stores. Used to decide whether to mount a repo PVC and run kopia in-process.
pub fn filesystem_repo_path(backend: &Backend) -> Option<String> {
    match backend {
        Backend::Filesystem(f) => Some(f.path.clone()),
        _ => None,
    }
}

/// The repo volume source for a `Filesystem` backend, if any — a PVC or an inline
/// NFS export the mover mounts at [`filesystem_repo_path`]. `None` for object
/// stores and for a bare-path filesystem repo (a `hostPath`/baked-in mount).
pub fn filesystem_repo_mount_source(backend: &Backend) -> Option<MountSource> {
    match backend {
        Backend::Filesystem(f) => f.volume.as_ref().map(|v| match v {
            RepoVolume::Pvc(p) => MountSource::Pvc {
                claim_name: p.name.clone(),
            },
            RepoVolume::Nfs(n) => MountSource::Nfs {
                server: n.server.clone(),
                path: n.path.clone(),
            },
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::backend::BackendAuth;
    use kopiur_api::backend::{FilesystemBackend, S3Backend};
    use kopiur_api::common::{SecretKeyRef, SecretRef};

    fn enc(name: &str, ns: Option<&str>) -> Encryption {
        Encryption {
            password_secret_ref: SecretKeyRef {
                name: name.to_string(),
                namespace: ns.map(str::to_string),
                key: None,
            },
        }
    }

    #[test]
    fn refs_resolve_source_ns_from_repo_namespace_fallback() {
        // Namespaced Repository: password ref omits namespace → falls back to the
        // repository's own namespace.
        let backend = Backend::Filesystem(FilesystemBackend {
            path: "/repo".into(),
            volume: None,
        });
        let refs = mover_creds_secret_refs(&backend, &enc("repo-pw", None), Some("team-a"));
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "repo-pw");
        assert_eq!(refs[0].namespace.as_deref(), Some("team-a"));
    }

    #[test]
    fn refs_keep_explicit_ref_namespace_over_fallback() {
        // ClusterRepository: ref pins its own namespace; no repo namespace fallback.
        let refs = mover_creds_secret_refs(
            &Backend::Filesystem(FilesystemBackend {
                path: "/r".into(),
                volume: None,
            }),
            &enc("repo-pw", Some("kopiur-system")),
            None,
        );
        assert_eq!(refs[0].namespace.as_deref(), Some("kopiur-system"));
    }

    #[test]
    fn refs_include_distinct_backend_secret_password_first() {
        let backend = Backend::S3(S3Backend {
            bucket: "b".into(),
            prefix: None,
            endpoint: None,
            region: None,
            auth: Some(BackendAuth {
                secret_ref: Some(SecretRef {
                    name: "s3-keys".into(),
                    namespace: Some("kopiur-system".into()),
                }),
                workload_identity: None,
            }),
            tls: None,
        });
        let refs = mover_creds_secret_refs(&backend, &enc("repo-pw", Some("kopiur-system")), None);
        let names: Vec<_> = refs.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["repo-pw", "s3-keys"]); // password first, order-stable
    }

    #[test]
    fn mover_creds_secrets_is_names_only_projection() {
        let backend = Backend::Filesystem(FilesystemBackend {
            path: "/r".into(),
            volume: None,
        });
        assert_eq!(
            mover_creds_secrets(&backend, &enc("repo-pw", Some("x"))),
            vec!["repo-pw"]
        );
    }
}
