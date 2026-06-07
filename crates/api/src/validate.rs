//! Cross-field validation the type system can't express (ADR §2.2 principle 8).
//!
//! These are the rules a single struct's types can't enforce: "field X is
//! forbidden only when sibling Y has a particular variant," "this string must
//! parse as a cron," "a discovered backup may only Retain." They live here as pure
//! functions so the **webhook calls them at admission and the controller calls them
//! defensively** — one validator, two callers (SKILL hard-rule 4). No `kube::Client`,
//! no `tokio`.
//!
//! ## Fail-fast vs. accumulate (see [`crate::error`])
//!
//! Single-rule helpers return [`ValidationResult`] (fail-fast — first problem).
//! The per-CRD aggregate validators (`validate_backup_config`, …) return
//! `Vec<ValidationError>` so a user sees every independent problem in one apply.
//! An empty vec means valid.

use crate::backend::NfsVolume;
use crate::backup::{BackupSpec, Origin};
use crate::backup_config::{BackupConfigSpec, Source};
use crate::backup_schedule::BackupScheduleSpec;
use crate::cluster_repository::{AllowedNamespaces, ClusterRepositorySpec};
use crate::common::{DeletionPolicy, RepositoryKind, RepositoryRef};
use crate::error::{ValidationError, ValidationResult};
use crate::maintenance::{MaintenanceSpec, RepositoryMaintenanceSpec};
use crate::repository::RepositorySpec;
use crate::restore::{RestoreSource, RestoreSpec, RestoreTarget};
use std::collections::BTreeMap;

/// A `RepositoryRef` is well-formed: a `ClusterRepository` reference is by name
/// only, so `namespace` MUST be absent (ADR §3.2/§3.3). A namespaced `Repository`
/// reference may carry a namespace (cross-namespace references are allowed).
///
/// ```
/// use kopiur_api::common::RepositoryRef;
/// use kopiur_api::validate::validate_repository_ref;
/// use kopiur_api::ValidationError;
///
/// // OK: a namespaced Repository reference may name a namespace.
/// let ok: RepositoryRef = serde_json::from_value(serde_json::json!({
///     "kind": "Repository", "name": "nas-primary", "namespace": "backups",
/// }))
/// .unwrap();
/// assert!(validate_repository_ref(&ok).is_ok());
///
/// // Err: a ClusterRepository is referenced by name alone — a namespace is forbidden.
/// let bad: RepositoryRef = serde_json::from_value(serde_json::json!({
///     "kind": "ClusterRepository", "name": "shared", "namespace": "oops",
/// }))
/// .unwrap();
/// assert_eq!(
///     validate_repository_ref(&bad).unwrap_err(),
///     ValidationError::ClusterRepoNamespaceForbidden { namespace: "oops".to_string() },
/// );
/// ```
pub fn validate_repository_ref(r: &RepositoryRef) -> ValidationResult {
    match r.kind {
        RepositoryKind::ClusterRepository => match &r.namespace {
            Some(ns) => Err(ValidationError::ClusterRepoNamespaceForbidden {
                namespace: ns.clone(),
            }),
            None => Ok(()),
        },
        RepositoryKind::Repository => Ok(()),
    }
}

/// A consumer namespace is permitted by a `ClusterRepository`'s tenancy gate
/// (ADR §3.2/§4.3).
///
/// - `List`     → membership test.
/// - `All(true)`→ always allowed; `All(false)` is meaningless and denies.
/// - `Selector` → matched against `labels` (the consumer namespace's labels). The
///   `crates/api` crate cannot fetch a `Namespace` object, so the caller (webhook)
///   must supply the labels. **If `labels` is `None` we fail closed** with
///   [`ValidationError::SelectorLabelsUnavailable`] rather than guess — the webhook
///   never trusts unfiltered input (ADR §3.2). Selector matching here is a simple
///   `matchLabels` superset test (the common case); `matchExpressions` is treated
///   as "no constraint" for now and documented as such.
pub fn validate_consumer_against_cluster_repo(
    consumer_namespace: &str,
    repo_name: &str,
    allowed: &AllowedNamespaces,
    labels: Option<&BTreeMap<String, String>>,
) -> ValidationResult {
    match allowed {
        AllowedNamespaces::All(true) => Ok(()),
        AllowedNamespaces::All(false) => Err(ValidationError::ConsumerNamespaceNotAllowed {
            namespace: consumer_namespace.to_string(),
            repo: repo_name.to_string(),
        }),
        AllowedNamespaces::List(names) => {
            if names.iter().any(|n| n == consumer_namespace) {
                Ok(())
            } else {
                Err(ValidationError::ConsumerNamespaceNotAllowed {
                    namespace: consumer_namespace.to_string(),
                    repo: repo_name.to_string(),
                })
            }
        }
        AllowedNamespaces::Selector(sel) => {
            let Some(labels) = labels else {
                return Err(ValidationError::SelectorLabelsUnavailable {
                    namespace: consumer_namespace.to_string(),
                    repo: repo_name.to_string(),
                });
            };
            let match_labels = sel.match_labels.clone().unwrap_or_default();
            // Every required label must be present with the required value.
            let matches = match_labels
                .iter()
                .all(|(k, v)| labels.get(k).map(|got| got == v).unwrap_or(false));
            if matches {
                Ok(())
            } else {
                Err(ValidationError::ConsumerNamespaceNotAllowed {
                    namespace: consumer_namespace.to_string(),
                    repo: repo_name.to_string(),
                })
            }
        }
    }
}

/// A `Backup`'s `deletionPolicy` is legal for its origin (ADR §4.5).
///
/// `origin: discovered` forces `Retain`: `None` (defaults to `Retain`) and an
/// explicit `Retain` pass; `Delete`/`Orphan` are rejected. Other origins accept any
/// policy.
pub fn validate_backup_deletion_policy(
    origin: Origin,
    policy: Option<DeletionPolicy>,
) -> ValidationResult {
    if origin != Origin::Discovered {
        return Ok(());
    }
    match policy {
        None | Some(DeletionPolicy::Retain) => Ok(()),
        Some(other) => Err(ValidationError::DiscoveredMustRetain {
            got: format!("{other:?}"),
        }),
    }
}

/// A single backup `Source` is well-formed: **exactly one** of `pvc`,
/// `pvcSelector`, or `nfs` is set (ADR §3.3 — modeled as sibling Options because
/// the forms share `sourcePath*` keys, so it's a webhook check, not an enum). When
/// the source is `nfs`, its server/path are also validated.
pub fn validate_source(source: &Source) -> ValidationResult {
    let set: Vec<&str> = [
        ("pvc", source.pvc.is_some()),
        ("pvcSelector", source.pvc_selector.is_some()),
        ("nfs", source.nfs.is_some()),
    ]
    .into_iter()
    .filter_map(|(name, present)| present.then_some(name))
    .collect();

    match set.as_slice() {
        [] => Err(ValidationError::MissingRequiredField {
            field: "source.pvc, source.pvcSelector, or source.nfs".to_string(),
        }),
        [first, second, ..] => Err(ValidationError::MutuallyExclusive {
            a: (*first).to_string(),
            b: (*second).to_string(),
            context: "backup source".to_string(),
        }),
        [_only] => match &source.nfs {
            Some(nfs) => validate_nfs_volume(nfs, "backup source"),
            None => Ok(()),
        },
    }
}

/// An inline [`NfsVolume`] is well-formed: a non-empty server and an absolute
/// export path. The structural schema can't express either, so the webhook does.
/// `context` names where it appears (e.g. `"backup source"`, `"filesystem repo"`)
/// for an actionable message.
pub fn validate_nfs_volume(nfs: &NfsVolume, context: &str) -> ValidationResult {
    if nfs.server.trim().is_empty() {
        return Err(ValidationError::MissingRequiredField {
            field: format!("{context} nfs.server"),
        });
    }
    if !nfs.path.starts_with('/') {
        return Err(ValidationError::InvalidFieldValue {
            field: format!("{context} nfs.path"),
            reason: format!(
                "must be an absolute export path beginning with '/' (got {:?})",
                nfs.path
            ),
        });
    }
    Ok(())
}

/// A `Restore` spec is internally consistent (ADR §3.6/§4.6).
///
/// The externally-tagged `RestoreSource`/`RestoreTarget` enums already guarantee
/// **exactly one** variant — that is a compile-time/serde invariant, not re-checked
/// here. We validate the cross-field rules the enums can't express:
/// - `source.identity` requires `spec.repository` (nothing else can derive it).
/// - if `target: pvc`, the template must name the PVC (`name` non-empty).
pub fn validate_restore(spec: &RestoreSpec) -> ValidationResult {
    // Exactly-one-variant on `source`/`target` is guaranteed by the enum + Option;
    // see RestoreSource / Option<RestoreTarget>.
    if matches!(spec.source, RestoreSource::Identity(_)) && spec.repository.is_none() {
        return Err(ValidationError::RestoreSourceRepositoryRequired);
    }
    if let Some(RestoreTarget::Pvc(t)) = &spec.target
        && t.name.trim().is_empty()
    {
        return Err(ValidationError::MissingRequiredField {
            field: "restore.target.pvc.name".to_string(),
        });
    }
    Ok(())
}

/// A `Repository` spec does not carry kopia-side (repo-level) retention policy,
/// which would conflict with CR-driven GFS retention (ADR §4.4 exclusivity).
///
/// The current [`RepositorySpec`] deliberately models no inline retention field, so
/// this **always passes today**. It exists as the enforcement hook so that if a
/// future field (e.g. `spec.policy.keepDaily`) is ever added, wiring it here is the
/// one obvious place — and the rule is already named and tested. Be pragmatic: we
/// do not invent a field to reject.
pub fn validate_repository_no_inline_retention(_spec: &RepositorySpec) -> ValidationResult {
    // No inline-retention field exists on RepositorySpec. If one is added later,
    // return Err(ValidationError::InlineRetentionForbidden { field: "<name>" }) here.
    Ok(())
}

/// Validate a `spec.maintenance` block on a `Repository`/`ClusterRepository`,
/// accumulating problems (ADR §3.7):
/// - any override schedule's quick/full crons must parse (same parser as runtime);
/// - `namespace` is **cluster-scope only** — it selects where the namespaced
///   managed `Maintenance` lands for a `ClusterRepository`, and is forbidden on a
///   namespaced `Repository` (whose `Maintenance` always lives in its own ns).
///
/// `cluster_scoped` is the only thing that differs between the two repository
/// kinds, so one validator serves both call sites.
pub fn validate_repository_maintenance(
    maintenance: &RepositoryMaintenanceSpec,
    cluster_scoped: bool,
) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let Some(schedule) = &maintenance.schedule {
        if let Err(e) = validate_cron(&schedule.quick.cron) {
            errs.push(e);
        }
        if let Err(e) = validate_cron(&schedule.full.cron) {
            errs.push(e);
        }
    }
    if !cluster_scoped && let Some(ns) = &maintenance.namespace {
        errs.push(ValidationError::MaintenanceNamespaceOnNamespacedRepo {
            namespace: ns.clone(),
        });
    }
    errs
}

/// A cron expression parses with the same parser the controller uses at runtime, so
/// bad expressions are rejected at apply time, not at first reconcile (ADR §4.1).
///
/// `croner` 2.x does not implement Jenkins-style `H`. Since kopiur resolves `H`
/// deterministically in [`crate::jitter::substitute_h`] (not in the parser), we
/// substitute every `H` field with the fixed placeholder `0` purely to validate the
/// expression's *shape* here. The real `H` spread is produced at scheduling time.
///
/// ```
/// use kopiur_api::validate::validate_cron;
/// use kopiur_api::ValidationError;
///
/// // Valid 5-field crons pass — including Jenkins-style `H` (resolved later).
/// assert!(validate_cron("0 2 * * *").is_ok());
/// assert!(validate_cron("H 2 * * *").is_ok());
///
/// // Garbage is rejected at apply time, not at first reconcile (ADR §4.1).
/// assert!(matches!(
///     validate_cron("not a cron"),
///     Err(ValidationError::InvalidCron { .. }),
/// ));
/// ```
pub fn validate_cron(expr: &str) -> ValidationResult {
    let probe = expr
        .split_whitespace()
        .map(|f| if f == "H" { "0" } else { f })
        .collect::<Vec<_>>()
        .join(" ");
    match croner::Cron::new(&probe).parse() {
        Ok(_) => Ok(()),
        Err(e) => Err(ValidationError::InvalidCron {
            expr: expr.to_string(),
            reason: e.to_string(),
        }),
    }
}

// --- Per-CRD aggregate validators (accumulate every problem) ----------------

/// Validate a `BackupConfig` spec, accumulating all problems.
pub fn validate_backup_config(spec: &BackupConfigSpec) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let Err(e) = validate_repository_ref(&spec.repository) {
        errs.push(e);
    }
    if spec.sources.is_empty() {
        errs.push(ValidationError::MissingRequiredField {
            field: "spec.sources (at least one source required)".to_string(),
        });
    }
    for source in &spec.sources {
        if let Err(e) = validate_source(source) {
            errs.push(e);
        }
    }
    errs
}

/// Validate a `Backup` spec for a given origin, accumulating all problems.
///
/// `origin` is supplied by the caller because the canonical value lives in
/// `status.origin` / the `kopiur.home-operations.com/origin` label, not in `spec` (ADR §3.4).
pub fn validate_backup(spec: &BackupSpec, origin: Origin) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let Err(e) = validate_backup_deletion_policy(origin, spec.deletion_policy) {
        errs.push(e);
    }
    errs
}

/// Validate a `BackupSchedule` spec, accumulating all problems.
pub fn validate_backup_schedule(spec: &BackupScheduleSpec) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let Err(e) = validate_cron(&spec.schedule.cron) {
        errs.push(e);
    }
    errs
}

/// Validate a `Repository` spec, accumulating all problems (ADR §3.1).
pub fn validate_repository(spec: &RepositorySpec) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let Err(e) = validate_repository_no_inline_retention(spec) {
        errs.push(e);
    }
    if let Err(e) = validate_backend(&spec.backend) {
        errs.push(e);
    }
    if let Some(m) = &spec.maintenance {
        errs.extend(validate_repository_maintenance(m, false));
    }
    errs
}

/// Validate backend *content* the structural schema can't express. Today this is
/// the inline-NFS volume on a `Filesystem` backend; object stores carry their own
/// (bucket/credential) checks elsewhere. Exhaustive `match` so a new `Backend`
/// variant must be considered here before it compiles.
pub fn validate_backend(backend: &crate::backend::Backend) -> ValidationResult {
    use crate::backend::{Backend, RepoVolume};
    match backend {
        Backend::Filesystem(fs) => match &fs.volume {
            Some(RepoVolume::Nfs(nfs)) => validate_nfs_volume(nfs, "filesystem repo"),
            Some(RepoVolume::Pvc(_)) | None => Ok(()),
        },
        Backend::S3(_)
        | Backend::Azure(_)
        | Backend::Gcs(_)
        | Backend::B2(_)
        | Backend::Sftp(_)
        | Backend::WebDav(_)
        | Backend::Rclone(_) => Ok(()),
    }
}

/// Validate a `Maintenance` spec, accumulating all problems (ADR §3.7).
pub fn validate_maintenance(spec: &MaintenanceSpec) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let Err(e) = validate_repository_ref(&spec.repository) {
        errs.push(e);
    }
    if let Err(e) = validate_cron(&spec.schedule.quick.cron) {
        errs.push(e);
    }
    if let Err(e) = validate_cron(&spec.schedule.full.cron) {
        errs.push(e);
    }
    errs
}

/// Validate a `ClusterRepository` spec, accumulating all problems (ADR §3.2).
///
/// `All(false)` is rejected as meaningless (SKILL: "`false` is rejected by webhook").
pub fn validate_cluster_repository(spec: &ClusterRepositorySpec) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let AllowedNamespaces::All(false) = spec.allowed_namespaces {
        errs.push(ValidationError::MissingRequiredField {
            field: "allowedNamespaces.all must be true to grant access (false is meaningless)"
                .to_string(),
        });
    }
    if let Err(e) = validate_backend(&spec.backend) {
        errs.push(e);
    }
    if let Some(m) = &spec.maintenance {
        errs.extend(validate_repository_maintenance(m, true));
    }
    errs
}

/// Validate a `Restore` spec, accumulating all problems (wraps the fail-fast
/// [`validate_restore`] for caller symmetry).
pub fn validate_restore_spec(spec: &RestoreSpec) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let Some(r) = &spec.repository
        && let Err(e) = validate_repository_ref(r)
    {
        errs.push(e);
    }
    if let Err(e) = validate_restore(spec) {
        errs.push(e);
    }
    errs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Identity;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;

    fn repo_ref(kind: RepositoryKind, ns: Option<&str>) -> RepositoryRef {
        RepositoryRef {
            kind,
            name: "r".to_string(),
            namespace: ns.map(String::from),
        }
    }

    // --- validate_repository_ref ---

    #[test]
    fn cluster_repo_ref_forbids_namespace() {
        let err = validate_repository_ref(&repo_ref(RepositoryKind::ClusterRepository, Some("x")))
            .unwrap_err();
        assert_eq!(
            err,
            ValidationError::ClusterRepoNamespaceForbidden {
                namespace: "x".to_string()
            }
        );
    }

    #[test]
    fn cluster_repo_ref_without_namespace_ok() {
        assert!(
            validate_repository_ref(&repo_ref(RepositoryKind::ClusterRepository, None)).is_ok()
        );
    }

    #[test]
    fn namespaced_repo_ref_allows_namespace() {
        assert!(
            validate_repository_ref(&repo_ref(RepositoryKind::Repository, Some("other"))).is_ok()
        );
        assert!(validate_repository_ref(&repo_ref(RepositoryKind::Repository, None)).is_ok());
    }

    // --- validate_consumer_against_cluster_repo ---

    #[test]
    fn consumer_allowed_via_list() {
        let allowed = AllowedNamespaces::List(vec!["billing".into(), "staging".into()]);
        assert!(validate_consumer_against_cluster_repo("billing", "repo", &allowed, None).is_ok());
    }

    #[test]
    fn consumer_denied_via_list() {
        let allowed = AllowedNamespaces::List(vec!["billing".into()]);
        let err =
            validate_consumer_against_cluster_repo("evil", "repo", &allowed, None).unwrap_err();
        assert_eq!(
            err,
            ValidationError::ConsumerNamespaceNotAllowed {
                namespace: "evil".to_string(),
                repo: "repo".to_string()
            }
        );
    }

    #[test]
    fn consumer_allowed_via_all_true_denied_via_all_false() {
        assert!(
            validate_consumer_against_cluster_repo(
                "any",
                "repo",
                &AllowedNamespaces::All(true),
                None
            )
            .is_ok()
        );
        assert!(
            validate_consumer_against_cluster_repo(
                "any",
                "repo",
                &AllowedNamespaces::All(false),
                None
            )
            .is_err()
        );
    }

    #[test]
    fn consumer_allowed_via_selector_match() {
        let sel = LabelSelector {
            match_labels: Some(BTreeMap::from([(
                "kopiur.home-operations.com/tier".to_string(),
                "enterprise".to_string(),
            )])),
            ..Default::default()
        };
        let allowed = AllowedNamespaces::Selector(sel);
        let labels = BTreeMap::from([(
            "kopiur.home-operations.com/tier".to_string(),
            "enterprise".to_string(),
        )]);
        assert!(
            validate_consumer_against_cluster_repo("ns", "repo", &allowed, Some(&labels)).is_ok()
        );
    }

    #[test]
    fn consumer_denied_via_selector_mismatch() {
        let sel = LabelSelector {
            match_labels: Some(BTreeMap::from([(
                "kopiur.home-operations.com/tier".to_string(),
                "enterprise".to_string(),
            )])),
            ..Default::default()
        };
        let allowed = AllowedNamespaces::Selector(sel);
        let labels = BTreeMap::from([(
            "kopiur.home-operations.com/tier".to_string(),
            "free".to_string(),
        )]);
        assert!(
            validate_consumer_against_cluster_repo("ns", "repo", &allowed, Some(&labels)).is_err()
        );
    }

    #[test]
    fn selector_without_labels_fails_closed() {
        let allowed = AllowedNamespaces::Selector(LabelSelector::default());
        let err = validate_consumer_against_cluster_repo("ns", "repo", &allowed, None).unwrap_err();
        assert_eq!(
            err,
            ValidationError::SelectorLabelsUnavailable {
                namespace: "ns".to_string(),
                repo: "repo".to_string()
            }
        );
    }

    // --- validate_backup_deletion_policy ---

    #[test]
    fn discovered_accepts_none_and_retain() {
        assert!(validate_backup_deletion_policy(Origin::Discovered, None).is_ok());
        assert!(
            validate_backup_deletion_policy(Origin::Discovered, Some(DeletionPolicy::Retain))
                .is_ok()
        );
    }

    #[test]
    fn discovered_rejects_delete_and_orphan() {
        assert_eq!(
            validate_backup_deletion_policy(Origin::Discovered, Some(DeletionPolicy::Delete))
                .unwrap_err(),
            ValidationError::DiscoveredMustRetain {
                got: "Delete".to_string()
            }
        );
        assert!(matches!(
            validate_backup_deletion_policy(Origin::Discovered, Some(DeletionPolicy::Orphan)),
            Err(ValidationError::DiscoveredMustRetain { .. })
        ));
    }

    #[test]
    fn produced_origins_accept_any_policy() {
        for o in [Origin::Scheduled, Origin::Manual] {
            for p in [
                None,
                Some(DeletionPolicy::Delete),
                Some(DeletionPolicy::Retain),
                Some(DeletionPolicy::Orphan),
            ] {
                assert!(validate_backup_deletion_policy(o, p).is_ok());
            }
        }
    }

    // --- validate_source ---

    #[test]
    fn source_with_both_pvc_and_selector_is_rejected() {
        use crate::backup_config::{PvcSelector, PvcSource};
        let src = Source {
            pvc: Some(PvcSource { name: "p".into() }),
            pvc_selector: Some(PvcSelector {
                namespace_selector: None,
                label_selector: None,
            }),
            nfs: None,
            source_path_override: None,
            source_path_strategy: None,
        };
        assert!(matches!(
            validate_source(&src),
            Err(ValidationError::MutuallyExclusive { .. })
        ));
    }

    #[test]
    fn source_with_neither_is_rejected() {
        let src = Source {
            pvc: None,
            pvc_selector: None,
            nfs: None,
            source_path_override: None,
            source_path_strategy: None,
        };
        assert!(matches!(
            validate_source(&src),
            Err(ValidationError::MissingRequiredField { .. })
        ));
    }

    #[test]
    fn nfs_source_alone_is_accepted() {
        let src = Source {
            pvc: None,
            pvc_selector: None,
            nfs: Some(NfsVolume {
                server: "nas.lan".into(),
                path: "/export/media".into(),
            }),
            source_path_override: None,
            source_path_strategy: None,
        };
        assert!(validate_source(&src).is_ok());
    }

    #[test]
    fn nfs_source_with_pvc_is_mutually_exclusive() {
        use crate::backup_config::PvcSource;
        let src = Source {
            pvc: Some(PvcSource { name: "p".into() }),
            pvc_selector: None,
            nfs: Some(NfsVolume {
                server: "nas.lan".into(),
                path: "/export/media".into(),
            }),
            source_path_override: None,
            source_path_strategy: None,
        };
        assert!(matches!(
            validate_source(&src),
            Err(ValidationError::MutuallyExclusive { .. })
        ));
    }

    #[test]
    fn nfs_source_with_relative_path_is_rejected() {
        let src = Source {
            pvc: None,
            pvc_selector: None,
            nfs: Some(NfsVolume {
                server: "nas.lan".into(),
                path: "export/media".into(),
            }),
            source_path_override: None,
            source_path_strategy: None,
        };
        assert!(matches!(
            validate_source(&src),
            Err(ValidationError::InvalidFieldValue { .. })
        ));
    }

    // --- validate_backend (filesystem inline-NFS repo content) ---

    #[test]
    fn filesystem_nfs_repo_volume_valid_passes() {
        use crate::backend::{Backend, FilesystemBackend, NfsVolume, RepoVolume};
        let b = Backend::Filesystem(FilesystemBackend {
            path: "/repo".into(),
            volume: Some(RepoVolume::Nfs(NfsVolume {
                server: "nas.lan".into(),
                path: "/export/kopia".into(),
            })),
        });
        assert!(validate_backend(&b).is_ok());
    }

    #[test]
    fn filesystem_nfs_repo_volume_relative_path_is_rejected() {
        use crate::backend::{Backend, FilesystemBackend, NfsVolume, RepoVolume};
        let b = Backend::Filesystem(FilesystemBackend {
            path: "/repo".into(),
            volume: Some(RepoVolume::Nfs(NfsVolume {
                server: "nas.lan".into(),
                path: "export/kopia".into(), // not absolute
            })),
        });
        assert!(matches!(
            validate_backend(&b),
            Err(ValidationError::InvalidFieldValue { .. })
        ));
    }

    #[test]
    fn filesystem_pvc_and_object_backends_need_no_content_check() {
        use crate::backend::{Backend, FilesystemBackend, PvcVolume, RepoVolume, S3Backend};
        let pvc = Backend::Filesystem(FilesystemBackend {
            path: "/repo".into(),
            volume: Some(RepoVolume::Pvc(PvcVolume {
                name: "repo-pvc".into(),
            })),
        });
        let bare = Backend::Filesystem(FilesystemBackend {
            path: "/repo".into(),
            volume: None,
        });
        let s3 = Backend::S3(S3Backend {
            bucket: "b".into(),
            prefix: None,
            endpoint: None,
            region: None,
            auth: None,
            tls: None,
        });
        assert!(validate_backend(&pvc).is_ok());
        assert!(validate_backend(&bare).is_ok());
        assert!(validate_backend(&s3).is_ok());
    }

    #[test]
    fn nfs_source_with_empty_server_is_rejected() {
        let src = Source {
            pvc: None,
            pvc_selector: None,
            nfs: Some(NfsVolume {
                server: "  ".into(),
                path: "/export/media".into(),
            }),
            source_path_override: None,
            source_path_strategy: None,
        };
        assert!(matches!(
            validate_source(&src),
            Err(ValidationError::MissingRequiredField { .. })
        ));
    }

    // --- validate_restore ---

    fn restore_with(source: RestoreSource, repo: Option<RepositoryRef>) -> RestoreSpec {
        RestoreSpec {
            repository: repo,
            source,
            target: None,
            options: None,
            policy: None,
        }
    }

    #[test]
    fn restore_identity_requires_repository() {
        use crate::restore::IdentitySource;
        let spec = restore_with(
            RestoreSource::Identity(IdentitySource {
                username: "u".into(),
                hostname: "h".into(),
                source_path: None,
                snapshot_id: None,
                as_of: None,
                offset: None,
            }),
            None,
        );
        assert_eq!(
            validate_restore(&spec).unwrap_err(),
            ValidationError::RestoreSourceRepositoryRequired
        );
    }

    #[test]
    fn restore_identity_with_repository_ok() {
        use crate::restore::IdentitySource;
        let spec = restore_with(
            RestoreSource::Identity(IdentitySource {
                username: "u".into(),
                hostname: "h".into(),
                source_path: None,
                snapshot_id: None,
                as_of: None,
                offset: None,
            }),
            Some(repo_ref(RepositoryKind::Repository, Some("backups"))),
        );
        assert!(validate_restore(&spec).is_ok());
    }

    #[test]
    fn restore_backup_ref_does_not_require_repository() {
        use crate::common::ObjectRef;
        let spec = restore_with(
            RestoreSource::BackupRef(ObjectRef {
                name: "b".into(),
                namespace: None,
            }),
            None,
        );
        assert!(validate_restore(&spec).is_ok());
    }

    #[test]
    fn restore_pvc_target_requires_name() {
        use crate::common::ObjectRef;
        use crate::restore::PvcTemplate;
        let mut spec = restore_with(
            RestoreSource::BackupRef(ObjectRef {
                name: "b".into(),
                namespace: None,
            }),
            None,
        );
        spec.target = Some(RestoreTarget::Pvc(PvcTemplate {
            name: "  ".into(),
            storage_class_name: None,
            capacity: None,
            access_modes: vec![],
        }));
        assert!(matches!(
            validate_restore(&spec),
            Err(ValidationError::MissingRequiredField { .. })
        ));
    }

    // --- validate_cron ---

    #[test]
    fn valid_cron_expressions_pass() {
        for expr in ["0 2 * * *", "*/15 * * * *", "0 0 1 1 *", "0 */6 * * *"] {
            assert!(validate_cron(expr).is_ok(), "{expr} should be valid");
        }
    }

    #[test]
    fn jenkins_h_cron_passes_via_placeholder() {
        // H is substituted to 0 for shape-validation; real spread is in jitter.
        assert!(validate_cron("H 2 * * *").is_ok());
        assert!(validate_cron("H H * * *").is_ok());
    }

    #[test]
    fn malformed_cron_is_rejected() {
        for expr in ["not a cron", "99 99 99 99 99", ""] {
            assert!(
                matches!(
                    validate_cron(expr),
                    Err(ValidationError::InvalidCron { .. })
                ),
                "{expr} should be rejected"
            );
        }
    }

    // --- validate_repository_no_inline_retention ---

    #[test]
    fn repository_inline_retention_hook_passes_today() {
        use crate::backend::{Backend, FilesystemBackend};
        use crate::common::{Encryption, SecretKeyRef};
        let spec = RepositorySpec {
            backend: Backend::Filesystem(FilesystemBackend {
                path: "/repo".into(),
                volume: None,
            }),
            encryption: Encryption {
                password_secret_ref: SecretKeyRef {
                    name: "s".into(),
                    namespace: None,
                    key: None,
                },
            },
            create: None,
            cache_defaults: None,
            catalog: None,
            maintenance: None,
        };
        assert!(validate_repository_no_inline_retention(&spec).is_ok());
    }

    // --- aggregate validators ---

    #[test]
    fn backup_config_aggregate_collects_multiple_errors() {
        let spec = BackupConfigSpec {
            repository: repo_ref(RepositoryKind::ClusterRepository, Some("forbidden")),
            identity: Some(Identity::default()),
            sources: vec![], // missing required
            copy_method: None,
            volume_snapshot_class_name: None,
            group_by: None,
            retention: None,
            default_deletion_policy: None,
            policy: None,
            hooks: None,
            mover: None,
        };
        let errs = validate_backup_config(&spec);
        // Both: ClusterRepo namespace forbidden + missing sources.
        assert_eq!(errs.len(), 2);
        assert!(
            errs.iter()
                .any(|e| matches!(e, ValidationError::ClusterRepoNamespaceForbidden { .. }))
        );
        assert!(
            errs.iter()
                .any(|e| matches!(e, ValidationError::MissingRequiredField { .. }))
        );
    }

    #[test]
    fn backup_config_valid_spec_has_no_errors() {
        use crate::backup_config::{PvcSource, Source};
        let spec = BackupConfigSpec {
            repository: repo_ref(RepositoryKind::Repository, Some("backups")),
            identity: None,
            sources: vec![Source {
                pvc: Some(PvcSource {
                    name: "data".into(),
                }),
                pvc_selector: None,
                nfs: None,
                source_path_override: None,
                source_path_strategy: None,
            }],
            copy_method: None,
            volume_snapshot_class_name: None,
            group_by: None,
            retention: None,
            default_deletion_policy: None,
            policy: None,
            hooks: None,
            mover: None,
        };
        assert!(validate_backup_config(&spec).is_empty());
    }

    #[test]
    fn backup_aggregate_rejects_discovered_delete() {
        let spec = BackupSpec {
            config_ref: None,
            tags: None,
            failure_policy: None,
            deletion_policy: Some(DeletionPolicy::Delete),
        };
        let errs = validate_backup(&spec, Origin::Discovered);
        assert_eq!(errs.len(), 1);
        assert!(matches!(
            errs[0],
            ValidationError::DiscoveredMustRetain { .. }
        ));
    }

    #[test]
    fn backup_schedule_aggregate_rejects_bad_cron() {
        use crate::backup_schedule::ScheduleSpec;
        use crate::common::ConfigRef;
        let spec = BackupScheduleSpec {
            config_ref: ConfigRef {
                name: "c".into(),
                namespace: None,
            },
            schedule: ScheduleSpec {
                cron: "totally bad".into(),
                jitter: None,
                timezone: None,
                run_on_create: false,
                suspend: false,
                concurrency_policy: Default::default(),
                starting_deadline_seconds: None,
            },
            failed_jobs_history_limit: None,
        };
        let errs = validate_backup_schedule(&spec);
        assert!(
            errs.iter()
                .any(|e| matches!(e, ValidationError::InvalidCron { .. }))
        );
    }

    // --- validate_repository_maintenance / validate_repository ---

    fn repo_spec_with_maintenance(m: Option<RepositoryMaintenanceSpec>) -> RepositorySpec {
        use crate::backend::{Backend, FilesystemBackend};
        use crate::common::{Encryption, SecretKeyRef};
        RepositorySpec {
            backend: Backend::Filesystem(FilesystemBackend {
                path: "/repo".into(),
                volume: None,
            }),
            encryption: Encryption {
                password_secret_ref: SecretKeyRef {
                    name: "s".into(),
                    namespace: None,
                    key: None,
                },
            },
            create: None,
            cache_defaults: None,
            catalog: None,
            maintenance: m,
        }
    }

    #[test]
    fn repository_default_managed_maintenance_is_valid() {
        // Absent `maintenance` (default-on) and an empty block both pass.
        assert!(validate_repository(&repo_spec_with_maintenance(None)).is_empty());
        assert!(
            validate_repository(&repo_spec_with_maintenance(Some(
                RepositoryMaintenanceSpec::default()
            )))
            .is_empty()
        );
    }

    #[test]
    fn repository_maintenance_namespace_rejected_on_namespaced_repo() {
        let m = RepositoryMaintenanceSpec {
            namespace: Some("kopia-system".into()),
            ..Default::default()
        };
        let errs = validate_repository(&repo_spec_with_maintenance(Some(m)));
        assert_eq!(
            errs,
            vec![ValidationError::MaintenanceNamespaceOnNamespacedRepo {
                namespace: "kopia-system".into()
            }]
        );
    }

    #[test]
    fn repository_maintenance_namespace_allowed_on_cluster_repo() {
        let m = RepositoryMaintenanceSpec {
            namespace: Some("kopia-system".into()),
            ..Default::default()
        };
        // cluster_scoped = true: the namespace field is the placement selector.
        assert!(validate_repository_maintenance(&m, true).is_empty());
    }

    #[test]
    fn repository_maintenance_bad_override_cron_is_rejected() {
        use crate::common::CronSpec;
        use crate::maintenance::MaintenanceSchedule;
        let m = RepositoryMaintenanceSpec {
            schedule: Some(MaintenanceSchedule {
                quick: CronSpec {
                    cron: "totally bad".into(),
                    jitter: None,
                },
                full: CronSpec {
                    cron: "0 3 * * *".into(),
                    jitter: None,
                },
                timezone: None,
            }),
            ..Default::default()
        };
        let errs = validate_repository_maintenance(&m, false);
        assert!(
            errs.iter()
                .any(|e| matches!(e, ValidationError::InvalidCron { .. }))
        );
    }

    #[test]
    fn cluster_repository_rejects_all_false() {
        use crate::backend::{Backend, FilesystemBackend};
        use crate::common::{Encryption, SecretKeyRef};
        let spec = ClusterRepositorySpec {
            backend: Backend::Filesystem(FilesystemBackend {
                path: "/r".into(),
                volume: None,
            }),
            encryption: Encryption {
                password_secret_ref: SecretKeyRef {
                    name: "s".into(),
                    namespace: Some("kopia-system".into()),
                    key: None,
                },
            },
            create: None,
            cache_defaults: None,
            catalog: None,
            allowed_namespaces: AllowedNamespaces::All(false),
            identity_defaults: None,
            maintenance: None,
        };
        assert!(!validate_cluster_repository(&spec).is_empty());
    }
}
