//! Snapshot → browse-target resolution: which kopia snapshot to read, which
//! repository holds it, and where (and with which Secrets) a session pod may
//! run. The decisions are pure functions over the fetched CRs; the single IO
//! fn just fetches and delegates.

use kopiur_api::backend::Backend;
use kopiur_api::common::{Encryption, RepositoryKind};
use kopiur_api::creds::mover_creds_secret_refs;
use kopiur_api::{ClusterRepository, Repository, Snapshot};
use kube::Api;
use kube::ResourceExt;

use crate::context::KubeCtx;
use crate::error::{CliError, classify_kube};

/// Everything the browse data-plane needs about one repository: identity (for
/// the session Job's name/labels/owner), backend + encryption (for the work
/// spec and credentials), and which namespace the repository itself lives in
/// (`None` for the cluster-scoped kind).
pub struct RepoHandle {
    /// Which repository CRD kind this is.
    pub kind: RepositoryKind,
    /// The repository object's name.
    pub name: String,
    /// Its UID (the session Job's ownerReference, so cluster GC reaps sessions
    /// with the repository).
    pub uid: String,
    /// The repository's own namespace (`None` for `ClusterRepository`).
    pub namespace: Option<String>,
    /// The storage backend.
    pub backend: Backend,
    /// The encryption (password Secret) settings.
    pub encryption: Encryption,
}

/// A fully-resolved browse target: the kopia snapshot id plus its repository.
pub struct BrowseTarget {
    /// The Snapshot CR name (for messages).
    pub snapshot: String,
    /// The namespace the Snapshot lives in — and where the session pod runs.
    pub namespace: String,
    /// The kopia snapshot manifest id pinned in the Snapshot's status.
    pub kopia_snapshot_id: String,
    /// The repository holding the snapshot.
    pub repo: RepoHandle,
}

/// Extract the kopia snapshot id a Snapshot pins in status, or say exactly why
/// there is none. Pure.
pub fn kopia_id_of(snap: &Snapshot) -> Result<String, CliError> {
    let name = snap.name_any();
    match snap.status.as_ref().and_then(|s| s.snapshot.as_ref()) {
        Some(info) if !info.kopia_snapshot_id.is_empty() => Ok(info.kopia_snapshot_id.clone()),
        _ => {
            let phase = snap
                .status
                .as_ref()
                .and_then(|s| s.phase)
                .map(|p| format!("phase is {}", serde_json::json!(p).as_str().unwrap_or("?")))
                .unwrap_or_else(|| "it has no status yet".to_string());
            Err(CliError::SnapshotNotBrowsable {
                name,
                reason: format!("{phase} and no kopia snapshot id is recorded"),
            })
        }
    }
}

/// The credential Secret names a session pod in `session_namespace` loads via
/// `envFrom` — refusing (actionably) any Secret that lives in a different
/// namespace, since a pod cannot load one across namespaces. Pure.
pub fn session_creds_secrets(
    repo: &RepoHandle,
    session_namespace: &str,
) -> Result<Vec<String>, CliError> {
    let refs = mover_creds_secret_refs(&repo.backend, &repo.encryption, repo.namespace.as_deref());
    let mut names = Vec::new();
    for r in refs {
        match r.namespace.as_deref() {
            Some(ns) if ns == session_namespace => names.push(r.name),
            Some(ns) => {
                return Err(CliError::CredsOutsideSessionNamespace {
                    secret: r.name,
                    secret_namespace: ns.to_string(),
                    session_namespace: session_namespace.to_string(),
                });
            }
            // Only reachable for a ClusterRepository reference that pins no
            // namespace (the webhook normally refuses this).
            None => {
                return Err(CliError::ClusterRepoSecretNamespaceMissing {
                    secret: r.name,
                    repository: repo.name.clone(),
                });
            }
        }
    }
    Ok(names)
}

/// Resolve `snapshot_name` (in the context namespace) into a [`BrowseTarget`]:
/// fetch the Snapshot, derive its repository via the shared
/// [`kopiur_api::snapshot::repository_ref_for`] rule, then fetch the
/// Repository/ClusterRepository for backend + credentials.
pub async fn resolve(ctx: &KubeCtx, snapshot_name: &str) -> Result<BrowseTarget, CliError> {
    let ns = ctx.namespace.clone();
    let snaps: Api<Snapshot> = Api::namespaced(ctx.client.clone(), &ns);
    let snap = snaps.get(snapshot_name).await.map_err(|e| {
        classify_kube(
            "get",
            "Snapshot",
            "snapshots",
            Some(&ns),
            Some(snapshot_name),
            e,
        )
    })?;
    let kopia_snapshot_id = kopia_id_of(&snap)?;
    let rref = kopiur_api::snapshot::repository_ref_for(&snap).ok_or_else(|| {
        CliError::RepositoryUnderivable {
            snapshot: snapshot_name.to_string(),
        }
    })?;

    // Exhaustive over the repository kinds (ADR §5.5): a new kind must decide
    // its fetch + namespace story here before it compiles.
    let repo = match rref.kind {
        RepositoryKind::Repository => {
            // An absent ref namespace means "the Snapshot's own namespace".
            let repo_ns = rref.namespace.clone().unwrap_or_else(|| ns.clone());
            // The session Job lives in the SNAPSHOT's namespace and is owned
            // by the Repository — a cross-namespace ownerReference is invalid
            // (Kubernetes GC treats the owner as missing and reaps the Job
            // mid-read). Refuse up front with the --local escape hatch.
            if repo_ns != ns {
                return Err(CliError::RepoOutsideSessionNamespace {
                    repo: format!("Repository/{}", rref.name),
                    repo_namespace: repo_ns,
                    session_namespace: ns,
                });
            }
            let api: Api<Repository> = Api::namespaced(ctx.client.clone(), &repo_ns);
            let r = api.get(&rref.name).await.map_err(|e| {
                classify_kube(
                    "get",
                    "Repository",
                    "repositories",
                    Some(&repo_ns),
                    Some(&rref.name),
                    e,
                )
            })?;
            RepoHandle {
                kind: RepositoryKind::Repository,
                name: r.name_any(),
                uid: r.uid().unwrap_or_default(),
                namespace: Some(repo_ns),
                backend: r.spec.backend.clone(),
                encryption: r.spec.encryption.clone(),
            }
        }
        RepositoryKind::ClusterRepository => {
            let api: Api<ClusterRepository> = Api::all(ctx.client.clone());
            let r = api.get(&rref.name).await.map_err(|e| {
                classify_kube(
                    "get",
                    "ClusterRepository",
                    "clusterrepositories",
                    None,
                    Some(&rref.name),
                    e,
                )
            })?;
            RepoHandle {
                kind: RepositoryKind::ClusterRepository,
                name: r.name_any(),
                uid: r.uid().unwrap_or_default(),
                namespace: None,
                backend: r.spec.backend.clone(),
                encryption: r.spec.encryption.clone(),
            }
        }
    };

    Ok(BrowseTarget {
        snapshot: snapshot_name.to_string(),
        namespace: ns,
        kopia_snapshot_id,
        repo,
    })
}

/// Fetch just the [`RepoHandle`] for an explicitly-named repository (the
/// `session end --repository` path).
pub async fn resolve_repo(
    ctx: &KubeCtx,
    kind: RepositoryKind,
    name: &str,
) -> Result<RepoHandle, CliError> {
    match kind {
        RepositoryKind::Repository => {
            let ns = ctx.namespace.clone();
            let api: Api<Repository> = Api::namespaced(ctx.client.clone(), &ns);
            let r = api.get(name).await.map_err(|e| {
                classify_kube(
                    "get",
                    "Repository",
                    "repositories",
                    Some(&ns),
                    Some(name),
                    e,
                )
            })?;
            Ok(RepoHandle {
                kind,
                name: r.name_any(),
                uid: r.uid().unwrap_or_default(),
                namespace: Some(ns),
                backend: r.spec.backend.clone(),
                encryption: r.spec.encryption.clone(),
            })
        }
        RepositoryKind::ClusterRepository => {
            let api: Api<ClusterRepository> = Api::all(ctx.client.clone());
            let r = api.get(name).await.map_err(|e| {
                classify_kube(
                    "get",
                    "ClusterRepository",
                    "clusterrepositories",
                    None,
                    Some(name),
                    e,
                )
            })?;
            Ok(RepoHandle {
                kind,
                name: r.name_any(),
                uid: r.uid().unwrap_or_default(),
                namespace: None,
                backend: r.spec.backend.clone(),
                encryption: r.spec.encryption.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(v: serde_json::Value) -> Snapshot {
        serde_json::from_value(v).expect("snapshot fixture")
    }

    #[test]
    fn kopia_id_extracted_from_a_succeeded_snapshot() {
        let s = snap(serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Snapshot",
            "metadata": { "name": "db-1", "namespace": "media" },
            "spec": {},
            "status": {
                "phase": "Succeeded",
                "snapshot": {
                    "kopiaSnapshotID": "kdeadbeef",
                    "identity": { "username": "db", "hostname": "media", "sourcePath": "/data" }
                }
            }
        }));
        assert_eq!(kopia_id_of(&s).unwrap(), "kdeadbeef");
    }

    #[test]
    fn running_or_statusless_snapshot_is_not_browsable_with_the_phase_named() {
        let running = snap(serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Snapshot",
            "metadata": { "name": "db-1", "namespace": "media" },
            "spec": {},
            "status": { "phase": "Running" }
        }));
        let msg = kopia_id_of(&running).unwrap_err().to_string();
        assert!(msg.contains("phase is Running"), "{msg}");

        let bare = snap(serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Snapshot",
            "metadata": { "name": "db-2", "namespace": "media" },
            "spec": {}
        }));
        let msg = kopia_id_of(&bare).unwrap_err().to_string();
        assert!(msg.contains("no status yet"), "{msg}");
    }

    fn repo_handle(kind: RepositoryKind, ns: Option<&str>, secret_ns: Option<&str>) -> RepoHandle {
        use kopiur_api::backend::S3Backend;
        use kopiur_api::common::SecretKeyRef;
        RepoHandle {
            kind,
            name: "nas".into(),
            uid: "u1".into(),
            namespace: ns.map(str::to_string),
            backend: Backend::S3(S3Backend {
                bucket: "b".into(),
                prefix: None,
                endpoint: None,
                region: None,
                auth: None,
                tls: None,
            }),
            encryption: Encryption {
                password_secret_ref: SecretKeyRef {
                    name: "creds".into(),
                    key: Some("KOPIA_PASSWORD".into()),
                    namespace: secret_ns.map(str::to_string),
                },
            },
        }
    }

    #[test]
    fn same_namespace_secret_is_allowed() {
        let repo = repo_handle(RepositoryKind::Repository, Some("media"), None);
        let names = session_creds_secrets(&repo, "media").unwrap();
        assert_eq!(names, vec!["creds".to_string()]);
    }

    #[test]
    fn cross_namespace_secret_is_refused_with_the_namespaces_named() {
        // Repository in `backups`, session (snapshot) in `media`: the secret
        // falls back to the repository's namespace, which the session pod
        // cannot envFrom.
        let repo = repo_handle(RepositoryKind::Repository, Some("backups"), None);
        let err = session_creds_secrets(&repo, "media").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("namespace backups"), "{msg}");
        assert!(msg.contains("--local"), "{msg}");
    }

    #[test]
    fn cluster_repo_pinned_secret_matches_or_is_refused() {
        // Pinned to the session namespace: fine.
        let repo = repo_handle(RepositoryKind::ClusterRepository, None, Some("media"));
        assert_eq!(
            session_creds_secrets(&repo, "media").unwrap(),
            vec!["creds".to_string()]
        );
        // Pinned elsewhere: actionable refusal.
        let repo = repo_handle(RepositoryKind::ClusterRepository, None, Some("backups"));
        assert!(matches!(
            session_creds_secrets(&repo, "media"),
            Err(CliError::CredsOutsideSessionNamespace { .. })
        ));
        // No namespace at all on a cluster repo ref: names the CRD field.
        let repo = repo_handle(RepositoryKind::ClusterRepository, None, None);
        let msg = session_creds_secrets(&repo, "media")
            .unwrap_err()
            .to_string();
        assert!(msg.contains("secretRef.namespace"), "{msg}");
    }
}
