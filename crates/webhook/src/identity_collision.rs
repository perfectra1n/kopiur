//! Identity-collision detection at admission (ADR-0005 §6).
//!
//! Kopia records every snapshot under `username@hostname:sourcePath`. Two
//! `SnapshotPolicy`s that resolve to the *same* identity in the *same* repository
//! would interleave snapshots into one kopia identity — corrupting the snapshot
//! history. The webhook pins identity at admission (ADR-0003 §4.2); this extends it
//! to **reject** a `SnapshotPolicy` whose resolved identity collides with an
//! already-admitted one, naming the conflict.
//!
//! ## Pure core + thin IO (mirrors the tenancy module)
//!
//! - [`repo_key`] and [`policy_identity_string`] are **pure** (no cluster) and
//!   unit-tested. They turn a policy + its repository ref into the comparable
//!   `(identity, repo_key)` pair.
//! - The api-crate [`api::validate::detect_identity_collision`] is the pure
//!   decision over a candidate pair + a list of existing pairs.
//! - [`check_identity_collision`] is the **thin IO caller**: it lists every
//!   `SnapshotPolicy` cluster-wide, fetches each referenced `ClusterRepository`'s
//!   `identityDefaults` (cached), resolves identities, and calls the pure decision.
//!   It **fails open on IO errors** (a transient list/get failure must not wedge
//!   unrelated applies) — the collision guard is a best-effort admission check, not
//!   a security boundary, and the controller would still pin distinct status.

use std::collections::BTreeMap;

use kopiur_api as api;

use api::cluster_repository::IdentityDefaults;
use api::common::{RepositoryKind, RepositoryRef};
use api::validate::ExistingIdentity;
use api::{ClusterRepository, IdentityInputs, SnapshotPolicy, SnapshotPolicySpec};
use kube::{Api, Client, ResourceExt};

/// A normalized, comparable repository key for a consumer's [`RepositoryRef`]
/// resolved from `owner_namespace` (the consuming policy's namespace). Two policies
/// are "the same repository" only when their keys match. Pure + exhaustive over
/// [`RepositoryKind`].
///
/// - `Repository` → `"Repository/<effective-ns>/<name>"` (effective-ns is
///   `ref.namespace` or the owner's namespace).
/// - `ClusterRepository` → `"ClusterRepository/<name>"` (namespace-free).
pub fn repo_key(repo: &RepositoryRef, owner_namespace: &str) -> String {
    match repo.kind {
        RepositoryKind::Repository => {
            let ns = repo.namespace.as_deref().unwrap_or(owner_namespace);
            format!("Repository/{ns}/{}", repo.name)
        }
        RepositoryKind::ClusterRepository => format!("ClusterRepository/{}", repo.name),
    }
}

/// Resolve a `SnapshotPolicy`'s kopia identity string (`username@hostname[:path]`),
/// reusing the api-crate kernel ([`api::resolve_identity`] + [`api::identity_string`]).
/// `defaults` is the referenced `ClusterRepository`'s `identityDefaults` (CEL `*Expr`),
/// `None` for a namespaced `Repository`. Returns `None` if an expression fails to
/// resolve (the per-field validators already reject those; here we just skip the
/// collision check for an unresolvable identity rather than panic).
pub fn policy_identity_string(
    name: &str,
    namespace: &str,
    spec: &SnapshotPolicySpec,
    labels: Option<&BTreeMap<String, String>>,
    annotations: Option<&BTreeMap<String, String>>,
    defaults: Option<&IdentityDefaults>,
) -> Option<String> {
    let first = spec.sources.first();
    let pvc_name = first.and_then(|s| s.pvc.as_ref().map(|p| p.name.clone()));
    let nfs_source_path = first.and_then(|s| s.nfs.as_ref().map(|n| n.path.clone()));
    let source_path_override = first.and_then(|s| s.source_path_override.clone());
    let inputs = IdentityInputs {
        object_name: name,
        namespace,
        overrides: spec.identity.as_ref(),
        defaults,
        labels,
        annotations,
        pvc_name: pvc_name.as_deref(),
        default_source_path: nfs_source_path.as_deref(),
        source_path_override: source_path_override.as_deref(),
    };
    api::resolve_identity(&inputs)
        .ok()
        .map(|r| api::identity_string(&r))
}

/// Look up the `identityDefaults` of the `ClusterRepository` a policy references, if
/// any (`None` for a namespaced `Repository` or when the lookup fails). Cached by
/// repo name so listing N policies that share a ClusterRepository does one get.
async fn cluster_repo_defaults(
    client: &Client,
    repo: &RepositoryRef,
    cache: &mut BTreeMap<String, Option<IdentityDefaults>>,
) -> Option<IdentityDefaults> {
    if repo.kind != RepositoryKind::ClusterRepository {
        return None;
    }
    if let Some(cached) = cache.get(&repo.name) {
        return cached.clone();
    }
    let api: Api<ClusterRepository> = Api::all(client.clone());
    let defaults = api
        .get_opt(&repo.name)
        .await
        .ok()
        .flatten()
        .and_then(|c| c.spec.identity_defaults);
    cache.insert(repo.name.clone(), defaults.clone());
    defaults
}

/// Resolve a policy's `(identity, repo_key)` pair for collision comparison, using
/// the (cached) ClusterRepository identity defaults. `None` when the identity can't
/// be resolved (skip — the per-field validators handle malformed expressions).
async fn policy_pair(
    client: &Client,
    name: &str,
    namespace: &str,
    spec: &SnapshotPolicySpec,
    labels: Option<&BTreeMap<String, String>>,
    annotations: Option<&BTreeMap<String, String>>,
    cache: &mut BTreeMap<String, Option<IdentityDefaults>>,
) -> Option<(String, String)> {
    let defaults = cluster_repo_defaults(client, &spec.repository, cache).await;
    let identity = policy_identity_string(
        name,
        namespace,
        spec,
        labels,
        annotations,
        defaults.as_ref(),
    )?;
    Some((identity, repo_key(&spec.repository, namespace)))
}

/// A detected identity collision: the conflicting policy's `namespace/name` and the
/// resolved identity string that collided (so the rejection message is exact).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Collision {
    /// `namespace/name` of the already-admitted policy with the same identity.
    pub conflict: String,
    /// The resolved `username@hostname[:path]` identity that collided.
    pub identity: String,
}

/// Check the incoming `SnapshotPolicy` for an identity collision with an
/// already-admitted policy in the same repository (ADR-0005 §6). Returns the
/// [`Collision`] when found, else `None`.
///
/// Thin IO: lists every `SnapshotPolicy` cluster-wide, resolves each one's
/// `(identity, repo_key)` pair (using the exact ClusterRepository identity defaults),
/// and calls the pure [`api::validate::detect_identity_collision`]. Self (same
/// `namespace/name`) is skipped. **Fails open** if the client is absent or the list
/// fails — a transient IO error must not wedge applies, and this is a best-effort
/// guard (the controller still pins distinct status).
pub async fn check_identity_collision(
    client: Option<&Client>,
    self_name: &str,
    self_namespace: &str,
    self_spec: &SnapshotPolicySpec,
    self_labels: Option<&BTreeMap<String, String>>,
    self_annotations: Option<&BTreeMap<String, String>>,
) -> Option<Collision> {
    let client = client?;
    let mut cache: BTreeMap<String, Option<IdentityDefaults>> = BTreeMap::new();

    let (self_identity, self_repo_key) = policy_pair(
        client,
        self_name,
        self_namespace,
        self_spec,
        self_labels,
        self_annotations,
        &mut cache,
    )
    .await?;

    let api: Api<SnapshotPolicy> = Api::all(client.clone());
    let policies = api.list(&Default::default()).await.ok()?;

    let self_full = format!("{self_namespace}/{self_name}");
    let mut existing: Vec<ExistingIdentity> = Vec::new();
    for p in policies {
        let Some(ns) = p.namespace() else { continue };
        let name = p.name_any();
        let full = format!("{ns}/{name}");
        if full == self_full {
            continue; // self
        }
        if let Some((identity, key)) = policy_pair(
            client,
            &name,
            &ns,
            &p.spec,
            p.metadata.labels.as_ref(),
            p.metadata.annotations.as_ref(),
            &mut cache,
        )
        .await
        {
            existing.push(ExistingIdentity {
                identity,
                repo_key: key,
                name: full,
            });
        }
    }

    api::validate::detect_identity_collision(&self_identity, &self_repo_key, &self_full, &existing)
        .map(|conflict| Collision {
            conflict,
            identity: self_identity,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use api::common::{Identity, RepositoryRef};
    use api::snapshot_policy::{PvcSource, Source};

    fn spec_with(repo: RepositoryRef, identity: Option<Identity>, pvc: &str) -> SnapshotPolicySpec {
        SnapshotPolicySpec {
            repository: repo,
            identity,
            sources: vec![Source {
                pvc: Some(PvcSource { name: pvc.into() }),
                pvc_selector: None,
                nfs: None,
                source_path_override: None,
                source_path_strategy: None,
            }],
            copy_method: Default::default(),
            volume_snapshot_class_name: None,
            group_by: None,
            retention: None,
            default_deletion_policy: None,
            compression: None,
            files: None,
            extra_args: vec![],
            error_handling: None,
            upload: None,
            verification: None,
            suspend: false,
            hooks: None,
            mover: None,
            credential_projection: None,
        }
    }

    #[test]
    fn repo_key_namespaced_uses_effective_namespace() {
        let r = RepositoryRef {
            kind: RepositoryKind::Repository,
            name: "nas".into(),
            namespace: None,
        };
        assert_eq!(repo_key(&r, "billing"), "Repository/billing/nas");
        let explicit = RepositoryRef {
            kind: RepositoryKind::Repository,
            name: "nas".into(),
            namespace: Some("backups".into()),
        };
        assert_eq!(repo_key(&explicit, "billing"), "Repository/backups/nas");
    }

    #[test]
    fn repo_key_cluster_is_namespace_free() {
        let r = RepositoryRef {
            kind: RepositoryKind::ClusterRepository,
            name: "shared".into(),
            namespace: None,
        };
        assert_eq!(repo_key(&r, "billing"), "ClusterRepository/shared");
    }

    #[test]
    fn policy_identity_uses_name_namespace_and_pvc_path() {
        let spec = spec_with(
            RepositoryRef {
                kind: RepositoryKind::Repository,
                name: "nas".into(),
                namespace: None,
            },
            None,
            "data",
        );
        let id = policy_identity_string("pg", "billing", &spec, None, None, None).unwrap();
        assert_eq!(id, "pg@billing:/pvc/data");
    }

    #[test]
    fn policy_identity_honors_explicit_override() {
        let spec = spec_with(
            RepositoryRef {
                kind: RepositoryKind::Repository,
                name: "nas".into(),
                namespace: None,
            },
            Some(Identity {
                username: Some("custom".into()),
                hostname: Some("host".into()),
            }),
            "data",
        );
        let id = policy_identity_string("pg", "billing", &spec, None, None, None).unwrap();
        assert_eq!(id, "custom@host:/pvc/data");
    }
}
