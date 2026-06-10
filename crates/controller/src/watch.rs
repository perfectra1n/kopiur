//! Cross-resource watch mappers: turn a change to a *referenced* object into the
//! set of *referrer* CRs that must re-reconcile.
//!
//! The kube `Controller` only re-reconciles its primary kind and its owned
//! children. Every reconciler that *reads* an external referent it does not own —
//! a credential `Secret`, a TLS-CA `ConfigMap`, a `Repository`/`ClusterRepository`,
//! a `SnapshotPolicy` — would otherwise not react when that referent changes
//! (a `Secret` content edit does not even bump the referrer's `generation`). These
//! mappers, wired via `Controller::watches` in [`crate::run`], close that gap so a
//! fixed credential or a repository that just became `Ready` re-triggers its
//! consumers within seconds instead of waiting out a multi-minute requeue.
//!
//! Each mapper reads a reflector [`Store`] of the *referrer* kind (the controller's
//! own store, shared via `Controller::store()`) and scans it in memory — never an
//! `Api::list` per event (the "prefer a shared informer over a list" rule). A
//! non-matching event (most cluster `Secret`s) yields an empty set and triggers
//! nothing.

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::ResourceExt;
use kube::runtime::reflector::{ObjectRef, Store};

use kopiur_api::backend::Backend;
use kopiur_api::common::{Encryption, PolicyRef, RepositoryKind, RepositoryRef};
use kopiur_api::{
    ClusterRepository, Maintenance, Repository, RepositoryReplication, Restore, Snapshot,
    SnapshotPolicy, SnapshotSchedule,
};

use crate::io;
use crate::snapshot_schedule::policy_matches_selector;

// --- predicates -------------------------------------------------------------

/// Does a repository with `backend`/`encryption` (whose own namespace is
/// `repo_ns`, `None` for cluster-scoped) reference the Secret `(sec_ns, sec_name)`?
/// Covers the encryption-password Secret and the backend `auth`/rclone-config
/// Secret, with the same namespace-defaulting the mover uses
/// ([`io::mover_creds_secret_refs`]).
fn repo_references_secret(
    backend: &Backend,
    encryption: &Encryption,
    repo_ns: Option<&str>,
    sec_ns: &str,
    sec_name: &str,
) -> bool {
    io::mover_creds_secret_refs(backend, encryption, repo_ns)
        .iter()
        .any(|r| r.name == sec_name && r.namespace.as_deref() == Some(sec_ns))
}

/// Whether a consumer's [`RepositoryRef`] targets the namespaced `Repository`
/// `(repo_ns, repo_name)`. `consumer_ns` is the referrer's own namespace, used
/// when the ref omits one. A `ClusterRepository` ref never matches a `Repository`.
fn ref_targets_repository(
    r: &RepositoryRef,
    consumer_ns: Option<&str>,
    repo_ns: &str,
    repo_name: &str,
) -> bool {
    matches!(r.kind, RepositoryKind::Repository)
        && r.name == repo_name
        && r.namespace.as_deref().or(consumer_ns) == Some(repo_ns)
}

/// Whether a consumer's [`RepositoryRef`] targets the `ClusterRepository` `name`
/// (cluster-scoped: namespace is meaningless and ignored).
fn ref_targets_cluster(r: &RepositoryRef, name: &str) -> bool {
    matches!(r.kind, RepositoryKind::ClusterRepository) && r.name == name
}

/// Whether a consumer's [`PolicyRef`] (in `consumer_ns`) targets the namespaced
/// `SnapshotPolicy` `(policy_ns, policy_name)`.
fn ref_targets_policy(
    r: &PolicyRef,
    consumer_ns: Option<&str>,
    policy_ns: &str,
    policy_name: &str,
) -> bool {
    r.name == policy_name && r.namespace.as_deref().or(consumer_ns) == Some(policy_ns)
}

// --- generic store scans ----------------------------------------------------

/// Scan a referrer `Store` and return an [`ObjectRef`] for each referrer for which
/// `matches` holds. The single in-memory pass shared by every mapper below.
fn select<K, F>(store: &Store<K>, matches: F) -> Vec<ObjectRef<K>>
where
    K: kube::Resource<DynamicType = ()> + Clone + 'static,
    F: Fn(&K) -> bool,
{
    store
        .state()
        .iter()
        .filter(|obj| matches(obj.as_ref()))
        .map(|obj| ObjectRef::from_obj(obj.as_ref()))
        .collect()
}

// --- M2: Secret / ConfigMap -> repository -----------------------------------

/// Repositories that reference the changed `secret` (password or backend auth).
pub fn secret_to_repositories(
    store: &Store<Repository>,
    secret: &Secret,
) -> Vec<ObjectRef<Repository>> {
    let (Some(sec_ns), sec_name) = (secret.namespace(), secret.name_any()) else {
        return Vec::new();
    };
    select(store, |r: &Repository| {
        repo_references_secret(
            &r.spec.backend,
            &r.spec.encryption,
            r.namespace().as_deref(),
            &sec_ns,
            &sec_name,
        )
    })
}

/// ClusterRepositories that reference the changed `secret`.
pub fn secret_to_cluster_repositories(
    store: &Store<ClusterRepository>,
    secret: &Secret,
) -> Vec<ObjectRef<ClusterRepository>> {
    let (Some(sec_ns), sec_name) = (secret.namespace(), secret.name_any()) else {
        return Vec::new();
    };
    select(store, |r: &ClusterRepository| {
        // Cluster-scoped: refs pin their own namespace (repo_ns = None).
        repo_references_secret(
            &r.spec.backend,
            &r.spec.encryption,
            None,
            &sec_ns,
            &sec_name,
        )
    })
}

/// RepositoryReplications that reference the changed `secret` as a *destination*
/// credential (the destination encryption password and/or destination backend
/// auth). A change to the *source* repository's Secret instead re-reconciles the
/// source `Repository`/`ClusterRepository`, which re-triggers this replication via
/// the source-ref watch ([`repository_to_replications`]).
pub fn secret_to_replications(
    store: &Store<RepositoryReplication>,
    secret: &Secret,
) -> Vec<ObjectRef<RepositoryReplication>> {
    let (Some(sec_ns), sec_name) = (secret.namespace(), secret.name_any()) else {
        return Vec::new();
    };
    select(store, |repl: &RepositoryReplication| {
        let repl_ns = repl.namespace();
        match repl.spec.destination_encryption.as_ref() {
            Some(enc) => repo_references_secret(
                &repl.spec.destination,
                enc,
                repl_ns.as_deref(),
                &sec_ns,
                &sec_name,
            ),
            // No destination password: match only the destination backend's auth Secret.
            None => io::backend_auth_secret_ref(&repl.spec.destination).is_some_and(|a| {
                a.name == sec_name
                    && a.namespace.as_deref().or(repl_ns.as_deref()) == Some(sec_ns.as_str())
            }),
        }
    })
}

/// Repositories that reference the changed TLS-CA `configmap`. A `ConfigMapKeyRef`
/// carries no namespace, so for a namespaced `Repository` we require the ConfigMap
/// to live in the repo's namespace.
pub fn configmap_to_repositories(
    store: &Store<Repository>,
    configmap: &ConfigMap,
) -> Vec<ObjectRef<Repository>> {
    let (Some(cm_ns), cm_name) = (configmap.namespace(), configmap.name_any()) else {
        return Vec::new();
    };
    select(store, |r: &Repository| {
        io::backend_tls_ca_configmap(&r.spec.backend) == Some(cm_name.as_str())
            && r.namespace().as_deref() == Some(cm_ns.as_str())
    })
}

/// ClusterRepositories that reference the changed TLS-CA `configmap`. A cluster
/// repo's `caBundleRef` has no namespace, so this matches by ConfigMap *name* only
/// (an over-trigger at worst re-runs an idempotent reconcile).
pub fn configmap_to_cluster_repositories(
    store: &Store<ClusterRepository>,
    configmap: &ConfigMap,
) -> Vec<ObjectRef<ClusterRepository>> {
    let cm_name = configmap.name_any();
    select(store, |r: &ClusterRepository| {
        io::backend_tls_ca_configmap(&r.spec.backend) == Some(cm_name.as_str())
    })
}

// --- M3: repository -> consumers --------------------------------------------

/// SnapshotPolicies whose `spec.repository` targets the changed `Repository`.
pub fn repository_to_policies(
    store: &Store<SnapshotPolicy>,
    repo: &Repository,
) -> Vec<ObjectRef<SnapshotPolicy>> {
    let (Some(ns), name) = (repo.namespace(), repo.name_any()) else {
        return Vec::new();
    };
    select(store, |c: &SnapshotPolicy| {
        ref_targets_repository(&c.spec.repository, c.namespace().as_deref(), &ns, &name)
    })
}

/// SnapshotPolicies whose `spec.repository` targets the changed `ClusterRepository`.
pub fn cluster_repository_to_policies(
    store: &Store<SnapshotPolicy>,
    repo: &ClusterRepository,
) -> Vec<ObjectRef<SnapshotPolicy>> {
    let name = repo.name_any();
    select(store, |c: &SnapshotPolicy| {
        ref_targets_cluster(&c.spec.repository, &name)
    })
}

/// Restores whose `spec.repository` targets the changed `Repository`.
pub fn repository_to_restores(
    store: &Store<Restore>,
    repo: &Repository,
) -> Vec<ObjectRef<Restore>> {
    let (Some(ns), name) = (repo.namespace(), repo.name_any()) else {
        return Vec::new();
    };
    select(store, |c: &Restore| {
        c.spec
            .repository
            .as_ref()
            .is_some_and(|r| ref_targets_repository(r, c.namespace().as_deref(), &ns, &name))
    })
}

/// Restores whose `spec.repository` targets the changed `ClusterRepository`.
pub fn cluster_repository_to_restores(
    store: &Store<Restore>,
    repo: &ClusterRepository,
) -> Vec<ObjectRef<Restore>> {
    let name = repo.name_any();
    select(store, |c: &Restore| {
        c.spec
            .repository
            .as_ref()
            .is_some_and(|r| ref_targets_cluster(r, &name))
    })
}

/// Maintenances whose `spec.repository` targets the changed `Repository`.
pub fn repository_to_maintenances(
    store: &Store<Maintenance>,
    repo: &Repository,
) -> Vec<ObjectRef<Maintenance>> {
    let (Some(ns), name) = (repo.namespace(), repo.name_any()) else {
        return Vec::new();
    };
    select(store, |c: &Maintenance| {
        ref_targets_repository(&c.spec.repository, c.namespace().as_deref(), &ns, &name)
    })
}

/// Maintenances whose `spec.repository` targets the changed `ClusterRepository`.
pub fn cluster_repository_to_maintenances(
    store: &Store<Maintenance>,
    repo: &ClusterRepository,
) -> Vec<ObjectRef<Maintenance>> {
    let name = repo.name_any();
    select(store, |c: &Maintenance| {
        ref_targets_cluster(&c.spec.repository, &name)
    })
}

/// RepositoryReplications whose `spec.sourceRef` targets the changed `Repository`.
pub fn repository_to_replications(
    store: &Store<RepositoryReplication>,
    repo: &Repository,
) -> Vec<ObjectRef<RepositoryReplication>> {
    let (Some(ns), name) = (repo.namespace(), repo.name_any()) else {
        return Vec::new();
    };
    select(store, |c: &RepositoryReplication| {
        ref_targets_repository(&c.spec.source_ref, c.namespace().as_deref(), &ns, &name)
    })
}

/// RepositoryReplications whose `spec.sourceRef` targets the changed `ClusterRepository`.
pub fn cluster_repository_to_replications(
    store: &Store<RepositoryReplication>,
    repo: &ClusterRepository,
) -> Vec<ObjectRef<RepositoryReplication>> {
    let name = repo.name_any();
    select(store, |c: &RepositoryReplication| {
        ref_targets_cluster(&c.spec.source_ref, &name)
    })
}

// --- M3: SnapshotPolicy -> Snapshot / SnapshotSchedule ----------------------

/// Snapshots whose `spec.policyRef` targets the changed `SnapshotPolicy`.
pub fn policy_to_snapshots(
    store: &Store<Snapshot>,
    policy: &SnapshotPolicy,
) -> Vec<ObjectRef<Snapshot>> {
    let (Some(ns), name) = (policy.namespace(), policy.name_any()) else {
        return Vec::new();
    };
    select(store, |s: &Snapshot| {
        s.spec
            .policy_ref
            .as_ref()
            .is_some_and(|r| ref_targets_policy(r, s.namespace().as_deref(), &ns, &name))
    })
}

/// SnapshotSchedules that select the changed `SnapshotPolicy` — by `policyRef`
/// (single) or by `policySelector` (fan-out; same-namespace label match, reusing
/// [`policy_matches_selector`]).
pub fn policy_to_schedules(
    store: &Store<SnapshotSchedule>,
    policy: &SnapshotPolicy,
) -> Vec<ObjectRef<SnapshotSchedule>> {
    let (Some(ns), name) = (policy.namespace(), policy.name_any()) else {
        return Vec::new();
    };
    let labels = policy.labels().clone();
    select(store, |sched: &SnapshotSchedule| {
        let sched_ns = sched.namespace();
        if let Some(r) = sched.spec.policy_ref.as_ref()
            && ref_targets_policy(r, sched_ns.as_deref(), &ns, &name)
        {
            return true;
        }
        // A selector only matches policies in the schedule's own namespace.
        sched.spec.policy_selector.as_ref().is_some_and(|sel| {
            sched_ns.as_deref() == Some(ns.as_str()) && policy_matches_selector(&labels, sel)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_api::backend::{BackendAuth, FilesystemBackend, S3Backend};
    use kopiur_api::common::{SecretKeyRef, SecretRef};

    fn rref(kind: RepositoryKind, name: &str, ns: Option<&str>) -> RepositoryRef {
        RepositoryRef {
            kind,
            name: name.into(),
            namespace: ns.map(Into::into),
        }
    }

    #[test]
    fn repository_ref_matching_honors_kind_name_and_namespace_default() {
        // Explicit namespace on the ref wins.
        let r = rref(RepositoryKind::Repository, "nas", Some("billing"));
        assert!(ref_targets_repository(&r, Some("other"), "billing", "nas"));
        assert!(!ref_targets_repository(
            &r,
            Some("other"),
            "billing",
            "wrong-name"
        ));
        assert!(!ref_targets_repository(
            &r,
            Some("other"),
            "other-ns",
            "nas"
        ));
        // No explicit namespace → falls back to the consumer's own namespace.
        let r = rref(RepositoryKind::Repository, "nas", None);
        assert!(ref_targets_repository(
            &r,
            Some("billing"),
            "billing",
            "nas"
        ));
        assert!(!ref_targets_repository(
            &r,
            Some("billing"),
            "other-ns",
            "nas"
        ));
        // A ClusterRepository ref never matches a namespaced Repository event.
        let cr = rref(RepositoryKind::ClusterRepository, "nas", None);
        assert!(!ref_targets_repository(
            &cr,
            Some("billing"),
            "billing",
            "nas"
        ));
    }

    #[test]
    fn cluster_repository_ref_matching_ignores_namespace() {
        let cr = rref(RepositoryKind::ClusterRepository, "shared", None);
        assert!(ref_targets_cluster(&cr, "shared"));
        assert!(!ref_targets_cluster(&cr, "other"));
        // A namespaced Repository ref never matches a ClusterRepository event.
        let r = rref(RepositoryKind::Repository, "shared", Some("ns"));
        assert!(!ref_targets_cluster(&r, "shared"));
    }

    #[test]
    fn policy_ref_matching_uses_namespace_default() {
        let p = PolicyRef {
            name: "daily".into(),
            namespace: None,
        };
        assert!(ref_targets_policy(&p, Some("apps"), "apps", "daily"));
        assert!(!ref_targets_policy(&p, Some("apps"), "other", "daily"));
        let p = PolicyRef {
            name: "daily".into(),
            namespace: Some("apps".into()),
        };
        assert!(ref_targets_policy(&p, Some("ignored"), "apps", "daily"));
    }

    fn enc(name: &str, ns: Option<&str>) -> Encryption {
        Encryption {
            password_secret_ref: SecretKeyRef {
                name: name.into(),
                namespace: ns.map(Into::into),
                key: None,
            },
        }
    }

    #[test]
    fn repo_references_secret_covers_password_and_backend_auth() {
        // Namespaced repo: password Secret defaults to the repo's namespace.
        let fs = Backend::Filesystem(FilesystemBackend {
            path: "/repo".into(),
            volume: None,
        });
        let e = enc("kopia-creds", None);
        assert!(repo_references_secret(
            &fs,
            &e,
            Some("billing"),
            "billing",
            "kopia-creds"
        ));
        // Wrong namespace / wrong name → no match.
        assert!(!repo_references_secret(
            &fs,
            &e,
            Some("billing"),
            "other",
            "kopia-creds"
        ));
        assert!(!repo_references_secret(
            &fs,
            &e,
            Some("billing"),
            "billing",
            "nope"
        ));

        // S3 backend with a separate auth Secret → both the password and the auth
        // Secret are referents.
        let s3 = Backend::S3(S3Backend {
            bucket: "b".into(),
            prefix: None,
            endpoint: None,
            region: None,
            auth: Some(BackendAuth {
                secret_ref: Some(SecretRef {
                    name: "s3-creds".into(),
                    namespace: None,
                }),
                workload_identity: None,
            }),
            tls: None,
        });
        assert!(repo_references_secret(
            &s3,
            &e,
            Some("billing"),
            "billing",
            "kopia-creds"
        ));
        assert!(repo_references_secret(
            &s3,
            &e,
            Some("billing"),
            "billing",
            "s3-creds"
        ));
        assert!(!repo_references_secret(
            &s3,
            &e,
            Some("billing"),
            "billing",
            "unrelated"
        ));
    }

    #[test]
    fn cluster_repo_references_secret_requires_explicit_namespace() {
        // Cluster-scoped (repo_ns = None): the password ref must pin its own namespace.
        let fs = Backend::Filesystem(FilesystemBackend {
            path: "/repo".into(),
            volume: None,
        });
        let e = enc("kopia-creds", Some("kopia-system"));
        assert!(repo_references_secret(
            &fs,
            &e,
            None,
            "kopia-system",
            "kopia-creds"
        ));
        assert!(!repo_references_secret(
            &fs,
            &e,
            None,
            "elsewhere",
            "kopia-creds"
        ));
        // A ref with no namespace and no repo namespace cannot resolve → never matches.
        let e_no_ns = enc("kopia-creds", None);
        assert!(!repo_references_secret(
            &fs,
            &e_no_ns,
            None,
            "kopia-system",
            "kopia-creds"
        ));
    }
}
