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
use crate::cluster_repository::{AllowedNamespaces, ClusterRepositorySpec};
use crate::common::{DeletionPolicy, MoverSpec, RepositoryKind, RepositoryRef};
use crate::error::{ValidationError, ValidationResult};
use crate::maintenance::{MaintenanceSpec, RepositoryMaintenanceSpec};
use crate::repository::RepositorySpec;
use crate::repository_replication::RepositoryReplicationSpec;
use crate::restore::{RestoreSource, RestoreSpec, RestoreTarget};
use crate::snapshot::{Origin, SnapshotSpec};
use crate::snapshot_policy::{Hook, SnapshotPolicySpec, Source};
use crate::snapshot_schedule::SnapshotScheduleSpec;
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

/// A `Snapshot`'s `deletionPolicy` is legal for its origin (ADR §4.5).
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
            context: "snapshot source".to_string(),
        }),
        [_only] => match &source.nfs {
            Some(nfs) => validate_nfs_volume(nfs, "snapshot source"),
            None => Ok(()),
        },
    }
}

/// An inline [`NfsVolume`] is well-formed: a non-empty server and an absolute
/// export path. The structural schema can't express either, so the webhook does.
/// `context` names where it appears (e.g. `"snapshot source"`, `"filesystem repo"`)
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

/// A `Restore` spec is internally consistent (ADR §3.6/§4.6 / ADR-0005 §9).
///
/// The externally-tagged `RestoreSource`/`RestoreTarget` enums already guarantee
/// **exactly one** variant — that is a compile-time/serde invariant, not re-checked
/// here (a `Restore` with no `target` now fails to deserialize entirely, ADR-0005 §9).
/// We validate the cross-field rules the enums can't express:
/// - `source.identity` requires `spec.repository` (nothing else can derive it).
/// - if `target: pvc`, the template must name the PVC (`name` non-empty).
/// - `target: populator` forbids `mover.inheritSecurityContextFrom`: no workload pod
///   exists at provision time to inherit from (ADR-0005 §9 / ADR §4.7) — point the
///   user at `moverDefaults` / an explicit `securityContext` instead.
pub fn validate_restore(spec: &RestoreSpec) -> ValidationResult {
    // Exactly-one-variant on `source`/`target` is guaranteed by the enums; see
    // RestoreSource / RestoreTarget (both required, externally tagged).
    if matches!(spec.source, RestoreSource::Identity(_)) && spec.repository.is_none() {
        return Err(ValidationError::RestoreSourceRepositoryRequired);
    }
    // `asOf` / `waitTimeout` are parsed at reconcile time with the SAME parsers
    // used here, so a value the webhook admits can never fail to parse later.
    // Exhaustive over the source so a new variant must declare its rules.
    match &spec.source {
        RestoreSource::SnapshotRef(_) => {}
        RestoreSource::FromPolicy(c) => {
            validate_as_of("restore.source.fromPolicy.asOf", c.as_of.as_deref())?;
        }
        RestoreSource::Identity(i) => {
            validate_as_of("restore.source.identity.asOf", i.as_of.as_deref())?;
            // `snapshotID` pins an exact snapshot — combining it with the
            // relative selectors would silently ignore one of them.
            if i.snapshot_id.is_some() && i.as_of.is_some() {
                return Err(ValidationError::MutuallyExclusive {
                    a: "source.identity.snapshotID".to_string(),
                    b: "source.identity.asOf".to_string(),
                    context: "snapshotID pins an exact snapshot; asOf selects by time".to_string(),
                });
            }
            if i.snapshot_id.is_some() && i.offset.is_some_and(|o| o != 0) {
                return Err(ValidationError::MutuallyExclusive {
                    a: "source.identity.snapshotID".to_string(),
                    b: "source.identity.offset".to_string(),
                    context: "snapshotID pins an exact snapshot; offset selects by position"
                        .to_string(),
                });
            }
        }
    }
    if let Some(wt) = spec.policy.as_ref().and_then(|p| p.wait_timeout.as_deref())
        && crate::duration::parse_go_duration(wt).is_none()
    {
        return Err(ValidationError::InvalidFieldValue {
            field: "restore.policy.waitTimeout".to_string(),
            reason: format!(
                "{wt:?} is not a valid Go-style duration; use a positive number with an \
                 s/m/h suffix (e.g. 90s, 5m, 1h) — how long the restore waits for the \
                 source snapshot to appear before applying onMissingSnapshot"
            ),
        });
    }
    match &spec.target {
        RestoreTarget::Pvc(t) if t.name.trim().is_empty() => {
            return Err(ValidationError::MissingRequiredField {
                field: "restore.target.pvc.name".to_string(),
            });
        }
        // `target.pvc` makes the operator CREATE the PVC, so it must know the
        // size — a guessed default could be smaller than the restored data.
        RestoreTarget::Pvc(t) if t.capacity.as_deref().is_none_or(|c| c.trim().is_empty()) => {
            return Err(ValidationError::MissingRequiredField {
                field: "restore.target.pvc.capacity".to_string(),
            });
        }
        RestoreTarget::Populator(_) => {
            if let Some(m) = &spec.mover
                && m.inherit_security_context_from.is_some()
            {
                return Err(ValidationError::InvalidFieldValue {
                    field: "restore.mover.inheritSecurityContextFrom".to_string(),
                    reason: "is not allowed with target.populator: no workload pod exists at \
                             provision time to inherit a security context from; set \
                             mover.securityContext explicitly or rely on the repository's \
                             moverDefaults instead"
                        .to_string(),
                });
            }
        }
        RestoreTarget::Pvc(_) | RestoreTarget::PvcRef(_) => {}
    }
    if let Some(m) = &spec.mover {
        validate_mover(m, "Restore mover")?;
    }
    Ok(())
}

/// An `asOf` point-in-time selector must be a valid RFC3339 timestamp — the
/// reconciler parses it with `chrono::DateTime::parse_from_rfc3339`, so the
/// webhook rejects anything that parser would choke on, with a fix in the message.
fn validate_as_of(field: &str, as_of: Option<&str>) -> ValidationResult {
    if let Some(s) = as_of
        && chrono::DateTime::parse_from_rfc3339(s).is_err()
    {
        return Err(ValidationError::InvalidFieldValue {
            field: field.to_string(),
            reason: format!(
                "{s:?} is not an RFC3339 timestamp; use e.g. 2026-05-01T00:00:00Z \
                 (the newest snapshot at or before this instant is restored)"
            ),
        });
    }
    Ok(())
}

/// Validate a `MoverSpec`. `inheritSecurityContextFrom` copies **both** the workload
/// pod's container and pod security contexts, so it is **mutually exclusive** with
/// **both** explicit `securityContext` and `podSecurityContext`: the mover's effective
/// contexts must have a single, unambiguous source so the privileged-mover gate runs on
/// exactly one. `context` names the owning resource for the message (e.g. `"Restore
/// mover"`).
pub fn validate_mover(mover: &MoverSpec, context: &str) -> ValidationResult {
    if mover.inherit_security_context_from.is_some() {
        if mover.security_context.is_some() {
            return Err(ValidationError::MutuallyExclusive {
                a: "mover.securityContext".to_string(),
                b: "mover.inheritSecurityContextFrom".to_string(),
                context: context.to_string(),
            });
        }
        if mover.pod_security_context.is_some() {
            return Err(ValidationError::MutuallyExclusive {
                a: "mover.podSecurityContext".to_string(),
                b: "mover.inheritSecurityContextFrom".to_string(),
                context: context.to_string(),
            });
        }
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

/// Accumulate every create-time-immutable field that changed between `old` and
/// `new` repository specs (ADR-0005 §7). Shared by both repository kinds via the
/// thin [`validate_repository_immutability`] / [`validate_cluster_repository_immutability`]
/// wrappers, which pass the `encryption` password ref + the `create.{splitter,hash,
/// encryption}` algorithms — the fields kopia bakes into the repository format.
///
/// Pure: the webhook supplies `old`/`new` from the admission request's old/new
/// objects; CREATE has no old object, so this is only wired into the UPDATE path.
fn diff_immutable_repo_fields(
    old_create: Option<&crate::common::CreateBehavior>,
    new_create: Option<&crate::common::CreateBehavior>,
) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    // NOTE: `encryption` (the password Secret *reference*) is deliberately NOT immutable.
    // kopia bakes only the resolved password *value* and the `create.*` algorithms into
    // the repository format — never the Secret name/namespace/key. Locking the reference
    // was both over-strict (a Secret rename with identical content was rejected, breaking
    // GitOps) and under-strict (editing a Secret's content in place — the actual password
    // change kopia would reject — sailed through). kopia also supports `change-password`,
    // so the password is operationally mutable; a genuinely wrong ref surfaces at connect
    // time, not at admission. We only enforce the create-time algorithms below.
    // The create-time kopia algorithms. Compared field-wise so the message names the
    // exact field. `create` itself may be absent on either side (absent ⇒ None algos).
    let old_splitter = old_create.and_then(|c| c.splitter.as_deref());
    let new_splitter = new_create.and_then(|c| c.splitter.as_deref());
    if old_splitter != new_splitter {
        errs.push(ValidationError::Immutable {
            field: "create.splitter".to_string(),
        });
    }
    let old_hash = old_create.and_then(|c| c.hash.as_deref());
    let new_hash = new_create.and_then(|c| c.hash.as_deref());
    if old_hash != new_hash {
        errs.push(ValidationError::Immutable {
            field: "create.hash".to_string(),
        });
    }
    let old_enc = old_create.and_then(|c| c.encryption.as_deref());
    let new_enc = new_create.and_then(|c| c.encryption.as_deref());
    if old_enc != new_enc {
        errs.push(ValidationError::Immutable {
            field: "create.encryption".to_string(),
        });
    }
    // ECC (Reed-Solomon parity) is baked into the repository format at create time
    // (ADR-0005 §13(a)) — immutable post-create like the other create knobs.
    let old_ecc = old_create.and_then(|c| c.ecc.as_ref());
    let new_ecc = new_create.and_then(|c| c.ecc.as_ref());
    if old_ecc != new_ecc {
        errs.push(ValidationError::Immutable {
            field: "create.ecc".to_string(),
        });
    }
    errs
}

/// Reject changes to create-time-immutable `Repository` fields on UPDATE (ADR-0005
/// §7): `create.splitter`, `create.hash`, `create.encryption`, `create.ecc`. Returns
/// every changed field so a user sees them all at once. Empty ⇒ no immutable change.
///
/// `encryption` (the password Secret reference) is intentionally NOT in this set — only
/// the resolved password value is fixed in the kopia format, and the reference is not a
/// reliable proxy for it (see [`diff_immutable_repo_fields`]). Renaming the Secret is fine.
///
/// ```
/// use kopiur_api::repository::RepositorySpec;
/// use kopiur_api::validate::validate_repository_immutability;
/// # use kopiur_api::backend::{Backend, FilesystemBackend};
/// # use kopiur_api::common::{CreateBehavior, Encryption, SecretKeyRef};
/// # fn spec(splitter: Option<&str>) -> RepositorySpec {
/// #     RepositorySpec {
/// #         backend: Backend::Filesystem(FilesystemBackend { path: "/r".into(), volume: None }),
/// #         encryption: Encryption { password_secret_ref: SecretKeyRef { name: "s".into(), namespace: None, key: None } },
/// #         create: Some(CreateBehavior { enabled: true, encryption: None, splitter: splitter.map(String::from), hash: None, ecc: None }),
/// #         mover_defaults: None, catalog: None, maintenance: None, on_namespace_delete: Default::default(), mode: Default::default(), suspend: false,
/// #     }
/// # }
/// // Unchanged splitter → accepted.
/// assert!(validate_repository_immutability(&spec(Some("FIXED-4M")), &spec(Some("FIXED-4M"))).is_empty());
/// // Changed splitter → rejected.
/// assert!(!validate_repository_immutability(&spec(Some("FIXED-4M")), &spec(Some("DYNAMIC"))).is_empty());
/// ```
pub fn validate_repository_immutability(
    old: &RepositorySpec,
    new: &RepositorySpec,
) -> Vec<ValidationError> {
    diff_immutable_repo_fields(old.create.as_ref(), new.create.as_ref())
}

/// Reject changes to create-time-immutable `ClusterRepository` fields on UPDATE
/// (ADR-0005 §7). Same field set as [`validate_repository_immutability`].
pub fn validate_cluster_repository_immutability(
    old: &ClusterRepositorySpec,
    new: &ClusterRepositorySpec,
) -> Vec<ValidationError> {
    diff_immutable_repo_fields(old.create.as_ref(), new.create.as_ref())
}

/// An already-admitted `SnapshotPolicy`'s identity, keyed for collision detection
/// (ADR-0005 §6). `repo_key` is a normalized repository identity (e.g.
/// `"ClusterRepository/shared"` or `"Repository/backups/nas"`) so two policies are
/// "the same repository" only when their keys match; `name` is the policy's
/// `namespace/name` for the actionable message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingIdentity {
    /// The other policy's resolved `username@hostname[:path]` identity string.
    pub identity: String,
    /// The other policy's normalized repository key.
    pub repo_key: String,
    /// `namespace/name` of the other policy (for the conflict message).
    pub name: String,
}

/// Detect whether a `SnapshotPolicy`'s resolved identity collides with an
/// already-admitted policy's identity **in the same repository** (ADR-0005 §6).
/// Pure so the decision is unit-tested; the webhook does the IO (list policies,
/// resolve each identity) and calls this. Returns the conflicting `namespace/name`
/// or `None`.
///
/// - `self_name` is the candidate's own `namespace/name`, skipped so a re-apply of
///   the same object never collides with itself.
/// - A collision requires BOTH the same `repo_key` AND the same `identity` string.
///
/// ```
/// use kopiur_api::validate::{detect_identity_collision, ExistingIdentity};
///
/// let existing = vec![ExistingIdentity {
///     identity: "pg@billing:/pvc/data".into(),
///     repo_key: "ClusterRepository/shared".into(),
///     name: "billing/pg-a".into(),
/// }];
/// // Same identity + same repo, different policy → collision.
/// assert_eq!(
///     detect_identity_collision("pg@billing:/pvc/data", "ClusterRepository/shared", "billing/pg-b", &existing),
///     Some("billing/pg-a".to_string()),
/// );
/// // Same identity but a DIFFERENT repository → no collision (separate snapshot history).
/// assert_eq!(
///     detect_identity_collision("pg@billing:/pvc/data", "Repository/billing/nas", "billing/pg-b", &existing),
///     None,
/// );
/// // Self (same name) is skipped.
/// assert_eq!(
///     detect_identity_collision("pg@billing:/pvc/data", "ClusterRepository/shared", "billing/pg-a", &existing),
///     None,
/// );
/// ```
pub fn detect_identity_collision(
    self_identity: &str,
    self_repo_key: &str,
    self_name: &str,
    existing: &[ExistingIdentity],
) -> Option<String> {
    existing
        .iter()
        .find(|e| e.name != self_name && e.repo_key == self_repo_key && e.identity == self_identity)
        .map(|e| e.name.clone())
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

/// Validate a `SnapshotPolicy` spec, accumulating all problems.
pub fn validate_backup_config(spec: &SnapshotPolicySpec) -> Vec<ValidationError> {
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
    // `volumeSnapshotClassName` only applies when a PVC source is CSI-snapshotted/cloned
    // (`copyMethod: Snapshot`/`Clone`). An NFS source has no PVC to snapshot, so pairing
    // it with an explicit class is a configuration mistake — reject it at admission with
    // an actionable message rather than silently ignoring the class. (`copyMethod` itself
    // can't be rejected for NFS: it defaults to `Snapshot` implicitly and an NFS source
    // is simply read directly.)
    if spec.volume_snapshot_class_name.is_some() && spec.sources.iter().any(|s| s.nfs.is_some()) {
        errs.push(ValidationError::InvalidFieldValue {
            field: "spec.volumeSnapshotClassName".to_string(),
            reason: "an NFS source cannot be CSI-snapshotted, so volumeSnapshotClassName is \
                     meaningless with it; remove volumeSnapshotClassName (NFS is read directly), \
                     or use a PVC source for copyMethod: Snapshot/Clone"
                .to_string(),
        });
    }
    if let Some(m) = &spec.mover
        && let Err(e) = validate_mover(m, "SnapshotPolicy mover")
    {
        errs.push(e);
    }
    // Verification (ADR-0005 §4): override schedules must parse, and the optional
    // `successExpr` (ADR-0005 §15) must compile + trial-evaluate to a bool with no
    // out-of-scope variable — rejected at admission rather than at first verify run.
    if let Some(v) = &spec.verification {
        if let Some(q) = &v.quick
            && let Err(e) = validate_cron(&q.cron)
        {
            errs.push(e);
        }
        if let Some(d) = &v.deep
            && let Err(e) = validate_cron(&d.schedule.cron)
        {
            errs.push(e);
        }
        if let Some(expr) = &v.success_expr
            && let Err(e) = crate::success_expr::validate_success_expr(expr)
        {
            errs.push(e);
        }
    }
    // Hooks (ADR §4.8): per-hook shape problems are caught at admission rather
    // than at the first backup run (where a quiesce hook failing on a typo would
    // abort the backup).
    if let Some(h) = &spec.hooks {
        for (list, hooks) in [
            ("beforeSnapshot", &h.before_snapshot),
            ("afterSnapshot", &h.after_snapshot),
        ] {
            for (i, hook) in hooks.iter().enumerate() {
                if let Err(e) = validate_hook(list, i, hook) {
                    errs.push(e);
                }
            }
        }
    }
    errs
}

/// Validate one hook entry — the controller executes these with the SAME parsers
/// (Go-style `timeout`, URL/method for `httpRequest`), so a value admitted here
/// can never fail to parse at run time. Exhaustive over [`Hook`].
fn validate_hook(list: &str, index: usize, hook: &Hook) -> ValidationResult {
    let field = |leaf: &str| format!("spec.hooks.{list}[{index}].{leaf}");
    let check_timeout = |leaf: &str, t: Option<&str>| -> ValidationResult {
        if let Some(t) = t
            && crate::duration::parse_go_duration(t).is_none()
        {
            return Err(ValidationError::InvalidFieldValue {
                field: field(leaf),
                reason: format!(
                    "{t:?} is not a valid Go-style duration; use a positive number with an \
                     s/m/h suffix (e.g. 90s, 2m) — how long the hook may run before it is \
                     treated as failed"
                ),
            });
        }
        Ok(())
    };
    match hook {
        Hook::WorkloadExec(h) => {
            if h.command.is_empty() {
                return Err(ValidationError::MissingRequiredField {
                    field: field("workloadExec.command"),
                });
            }
            check_timeout("workloadExec.timeout", h.timeout.as_deref())
        }
        Hook::RunJob(h) => check_timeout("runJob.timeout", h.timeout.as_deref()),
        Hook::HttpRequest(h) => {
            if !(h.url.starts_with("http://") || h.url.starts_with("https://")) {
                return Err(ValidationError::InvalidFieldValue {
                    field: field("httpRequest.url"),
                    reason: format!(
                        "{:?} must be an absolute http:// or https:// URL the controller can \
                         reach (e.g. http://notifier.tools.svc:8080/fire)",
                        h.url
                    ),
                });
            }
            if let Some(m) = &h.method {
                const METHODS: [&str; 7] =
                    ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"];
                if !METHODS.contains(&m.to_ascii_uppercase().as_str()) {
                    return Err(ValidationError::InvalidFieldValue {
                        field: field("httpRequest.method"),
                        reason: format!(
                            "{m:?} is not an HTTP method; use one of GET, POST (default), PUT, \
                             PATCH, DELETE, HEAD, OPTIONS"
                        ),
                    });
                }
            }
            check_timeout("httpRequest.timeout", h.timeout.as_deref())
        }
    }
}

/// Validate a `Snapshot` spec for a given origin, accumulating all problems.
///
/// `origin` is supplied by the caller because the canonical value lives in
/// `status.origin` / the `kopiur.home-operations.com/origin` label, not in `spec` (ADR §3.4).
pub fn validate_backup(spec: &SnapshotSpec, origin: Origin) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let Err(e) = validate_backup_deletion_policy(origin, spec.deletion_policy) {
        errs.push(e);
    }
    errs
}

/// Exactly one of `policyRef` / `policySelector` is set on a `SnapshotSchedule`
/// (ADR-0005 §10). Neither ⇒ `MissingRequiredField`; both ⇒ `MutuallyExclusive`.
/// Pure so the XOR decision is unit-tested directly.
pub fn validate_schedule_policy_target(spec: &SnapshotScheduleSpec) -> ValidationResult {
    match (spec.policy_ref.is_some(), spec.policy_selector.is_some()) {
        (true, true) => Err(ValidationError::MutuallyExclusive {
            a: "policyRef".to_string(),
            b: "policySelector".to_string(),
            context: "SnapshotSchedule".to_string(),
        }),
        (false, false) => Err(ValidationError::MissingRequiredField {
            field: "exactly one of spec.policyRef or spec.policySelector".to_string(),
        }),
        _ => Ok(()),
    }
}

/// Validate a `SnapshotSchedule` spec, accumulating all problems.
pub fn validate_backup_schedule(spec: &SnapshotScheduleSpec) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let Err(e) = validate_schedule_policy_target(spec) {
        errs.push(e);
    }
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

/// Validate a `RepositoryReplication` spec, accumulating all problems (ADR-0005
/// §13(d)): the `sourceRef` is well-formed, the schedule cron parses, the
/// destination backend's content is valid, and (when a mover is set) it's
/// well-formed. The "destination differs from source" rule needs the resolved
/// source backend, which this pure validator cannot fetch — the webhook resolves it
/// and calls [`replication_destination_differs`] separately.
pub fn validate_repository_replication(spec: &RepositoryReplicationSpec) -> Vec<ValidationError> {
    let mut errs = Vec::new();
    if let Err(e) = validate_repository_ref(&spec.source_ref) {
        errs.push(e);
    }
    if let Err(e) = validate_cron(&spec.schedule.cron) {
        errs.push(e);
    }
    if let Err(e) = validate_backend(&spec.destination) {
        errs.push(e);
    }
    if let Some(m) = &spec.mover
        && let Err(e) = validate_mover(m, "RepositoryReplication mover")
    {
        errs.push(e);
    }
    errs
}

/// Whether a replication's `destination` backend differs from its source
/// repository's backend (ADR-0005 §13(d)). Replicating a repository to *itself* is a
/// no-op (or a loop), so the webhook rejects it. Pure so the decision is unit-tested;
/// the webhook resolves the source backend (it has a client) and calls this. A
/// "same" destination is detected structurally: same backend kind AND the same
/// identifying target (bucket+prefix / path / container / host+path / url / remote).
///
/// ```
/// use kopiur_api::backend::{Backend, FilesystemBackend, S3Backend};
/// use kopiur_api::validate::replication_destination_differs;
///
/// let fs_a = Backend::Filesystem(FilesystemBackend { path: "/a".into(), volume: None });
/// let fs_b = Backend::Filesystem(FilesystemBackend { path: "/b".into(), volume: None });
/// // Different paths → differ.
/// assert!(replication_destination_differs(&fs_a, &fs_b));
/// // Same path → same target (would be a self-replication).
/// assert!(!replication_destination_differs(&fs_a, &fs_a));
/// // Different backend kinds always differ.
/// let s3 = Backend::S3(S3Backend { bucket: "b".into(), prefix: None, endpoint: None, region: None, auth: None, tls: None });
/// assert!(replication_destination_differs(&fs_a, &s3));
/// ```
pub fn replication_destination_differs(
    source: &crate::backend::Backend,
    dest: &crate::backend::Backend,
) -> bool {
    backend_target_key(source) != backend_target_key(dest)
}

/// A structural identity key for a backend (kind + identifying target), used by
/// [`replication_destination_differs`] to decide whether two backends point at the
/// same storage. Exhaustive over [`crate::backend::Backend`] so a new backend cannot
/// compile until its key is defined.
fn backend_target_key(backend: &crate::backend::Backend) -> String {
    use crate::backend::Backend;
    let kind = backend.kind_str();
    let target = match backend {
        Backend::Filesystem(f) => f.path.clone(),
        Backend::S3(s) => format!("{}/{}", s.bucket, s.prefix.clone().unwrap_or_default()),
        Backend::Azure(a) => format!("{}/{}", a.container, a.prefix.clone().unwrap_or_default()),
        Backend::Gcs(g) => format!("{}/{}", g.bucket, g.prefix.clone().unwrap_or_default()),
        Backend::B2(b) => format!("{}/{}", b.bucket, b.prefix.clone().unwrap_or_default()),
        Backend::Sftp(s) => format!("{}:{}", s.host, s.path),
        Backend::WebDav(w) => w.url.clone(),
        Backend::Rclone(r) => r.remote_path.clone(),
    };
    format!("{kind}:{target}")
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
    if let Some(m) = &spec.mover
        && let Err(e) = validate_mover(m, "Maintenance mover")
    {
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
    // Identity CEL expressions must compile + trial-evaluate to a string at admission
    // (ADR-0004 §5), so a typo / out-of-scope variable is rejected on `kubectl apply`.
    if let Some(id) = &spec.identity_defaults {
        if let Some(expr) = &id.hostname_expr
            && let Err(e) = crate::identity::validate_identity_expr(expr)
        {
            errs.push(e);
        }
        if let Some(expr) = &id.username_expr
            && let Err(e) = crate::identity::validate_identity_expr(expr)
        {
            errs.push(e);
        }
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
        use crate::snapshot_policy::{PvcSelector, PvcSource};
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
        use crate::snapshot_policy::PvcSource;
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
        use crate::common::ObjectRef;
        RestoreSpec {
            repository: repo,
            source,
            // A benign existing-PVC target (target is required, ADR-0005 §9); the
            // populator-specific rules are exercised in dedicated tests.
            target: RestoreTarget::PvcRef(ObjectRef {
                name: "tgt".into(),
                namespace: None,
            }),
            options: None,
            policy: None,
            credential_projection: None,
            mover: None,
            failure_policy: None,
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
            RestoreSource::SnapshotRef(ObjectRef {
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
            RestoreSource::SnapshotRef(ObjectRef {
                name: "b".into(),
                namespace: None,
            }),
            None,
        );
        spec.target = RestoreTarget::Pvc(PvcTemplate {
            name: "  ".into(),
            storage_class_name: None,
            capacity: None,
            access_modes: vec![],
        });
        assert!(matches!(
            validate_restore(&spec),
            Err(ValidationError::MissingRequiredField { .. })
        ));
    }

    #[test]
    fn restore_pvc_target_requires_capacity() {
        use crate::common::ObjectRef;
        use crate::restore::PvcTemplate;
        let mut spec = restore_with(
            RestoreSource::SnapshotRef(ObjectRef {
                name: "b".into(),
                namespace: None,
            }),
            None,
        );
        // The operator creates this PVC, so it must be told the size.
        spec.target = RestoreTarget::Pvc(PvcTemplate {
            name: "restored".into(),
            storage_class_name: None,
            capacity: None,
            access_modes: vec![],
        });
        assert!(matches!(
            validate_restore(&spec),
            Err(ValidationError::MissingRequiredField { field }) if field.contains("capacity")
        ));
        spec.target = RestoreTarget::Pvc(PvcTemplate {
            name: "restored".into(),
            storage_class_name: None,
            capacity: Some("10Gi".into()),
            access_modes: vec![],
        });
        assert!(validate_restore(&spec).is_ok());
    }

    #[test]
    fn restore_as_of_must_be_rfc3339_and_message_says_how_to_fix() {
        use crate::restore::FromPolicy;
        let spec = restore_with(
            RestoreSource::FromPolicy(FromPolicy {
                name: "pg".into(),
                namespace: None,
                as_of: Some("yesterday".into()),
                offset: 0,
            }),
            None,
        );
        let err = validate_restore(&spec).unwrap_err();
        assert!(matches!(err, ValidationError::InvalidFieldValue { .. }));
        // The message a human acts on: names the field, the bad value, and the fix.
        let msg = err.to_string();
        assert!(msg.contains("restore.source.fromPolicy.asOf"), "{msg}");
        assert!(msg.contains("yesterday"), "{msg}");
        assert!(msg.contains("RFC3339"), "{msg}");
        assert!(msg.contains("2026-05-01T00:00:00Z"), "{msg}");

        // A valid RFC3339 instant (with offset) is accepted.
        let ok = restore_with(
            RestoreSource::FromPolicy(FromPolicy {
                name: "pg".into(),
                namespace: None,
                as_of: Some("2026-05-01T00:00:00+02:00".into()),
                offset: 1,
            }),
            None,
        );
        assert!(validate_restore(&ok).is_ok());
    }

    #[test]
    fn restore_identity_snapshot_id_excludes_as_of_and_offset() {
        use crate::restore::IdentitySource;
        let base = IdentitySource {
            username: "u".into(),
            hostname: "h".into(),
            source_path: None,
            snapshot_id: Some("k1f1ec0a8".into()),
            as_of: None,
            offset: None,
        };
        let with_as_of = restore_with(
            RestoreSource::Identity(IdentitySource {
                as_of: Some("2026-05-01T00:00:00Z".into()),
                ..base.clone()
            }),
            Some(repo_ref(RepositoryKind::Repository, None)),
        );
        assert!(matches!(
            validate_restore(&with_as_of),
            Err(ValidationError::MutuallyExclusive { .. })
        ));
        let with_offset = restore_with(
            RestoreSource::Identity(IdentitySource {
                offset: Some(1),
                ..base.clone()
            }),
            Some(repo_ref(RepositoryKind::Repository, None)),
        );
        assert!(matches!(
            validate_restore(&with_offset),
            Err(ValidationError::MutuallyExclusive { .. })
        ));
        // An explicit offset: 0 is the "latest" default — not a conflict.
        let with_zero = restore_with(
            RestoreSource::Identity(IdentitySource {
                offset: Some(0),
                ..base
            }),
            Some(repo_ref(RepositoryKind::Repository, None)),
        );
        assert!(validate_restore(&with_zero).is_ok());
    }

    #[test]
    fn restore_wait_timeout_must_parse_as_go_duration() {
        use crate::common::ObjectRef;
        use crate::restore::RestorePolicy;
        let mut spec = restore_with(
            RestoreSource::SnapshotRef(ObjectRef {
                name: "b".into(),
                namespace: None,
            }),
            None,
        );
        spec.policy = Some(RestorePolicy {
            on_missing_snapshot: None,
            wait_timeout: Some("soon".into()),
        });
        let err = validate_restore(&spec).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("restore.policy.waitTimeout"), "{msg}");
        assert!(msg.contains("5m"), "{msg}");

        spec.policy = Some(RestorePolicy {
            on_missing_snapshot: None,
            wait_timeout: Some("5m".into()),
        });
        assert!(validate_restore(&spec).is_ok());
    }

    #[test]
    fn hooks_are_validated_at_admission_with_actionable_messages() {
        use crate::common::PodSelector;
        use crate::snapshot_policy::{Hooks, HttpRequestHook, WorkloadExecHook};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
        let base: SnapshotPolicySpec = crate::testutil::from_yaml(
            "repository: { kind: Repository, name: r }\nsources: [ { pvc: { name: data } } ]\n",
        );
        let selector = PodSelector {
            pod_selector: LabelSelector::default(),
            container: None,
        };

        // workloadExec with no command → missing required field, with the path.
        let mut spec = base.clone();
        spec.hooks = Some(Hooks {
            before_snapshot: vec![Hook::WorkloadExec(WorkloadExecHook {
                selector: selector.clone(),
                command: vec![],
                timeout: None,
                continue_on_failure: false,
            })],
            after_snapshot: vec![],
        });
        let errs = validate_backup_config(&spec);
        assert!(
            errs.iter().any(|e| e
                .to_string()
                .contains("spec.hooks.beforeSnapshot[0].workloadExec.command")),
            "{errs:?}"
        );

        // httpRequest: relative URL and an unparseable timeout, both rejected
        // with the fix in the message.
        let mut spec = base.clone();
        spec.hooks = Some(Hooks {
            before_snapshot: vec![],
            after_snapshot: vec![Hook::HttpRequest(HttpRequestHook {
                url: "notifier.tools/fire".into(),
                method: Some("FETCH".into()),
                body: None,
                timeout: Some("soon".into()),
                continue_on_failure: false,
            })],
        });
        let errs = validate_backup_config(&spec);
        let all = errs
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(all.contains("http://"), "{all}");

        // A well-formed hook set passes (lowercase method is normalized).
        let mut spec = base;
        spec.hooks = Some(Hooks {
            before_snapshot: vec![Hook::WorkloadExec(WorkloadExecHook {
                selector,
                command: vec!["sh".into(), "-c".into(), "sync".into()],
                timeout: Some("2m".into()),
                continue_on_failure: false,
            })],
            after_snapshot: vec![Hook::HttpRequest(HttpRequestHook {
                url: "https://notifier.tools.svc/fire".into(),
                method: Some("post".into()),
                body: Some("done".into()),
                timeout: Some("30s".into()),
                continue_on_failure: true,
            })],
        });
        assert!(validate_backup_config(&spec).is_empty());
    }

    #[test]
    fn volume_snapshot_class_with_nfs_source_is_rejected() {
        // An NFS source can't be CSI-snapshotted, so an explicit volumeSnapshotClassName
        // alongside it is a config mistake — rejected with an actionable message.
        let spec: SnapshotPolicySpec = crate::testutil::from_yaml(
            "repository: { kind: Repository, name: r }\n\
             volumeSnapshotClassName: csi-class\n\
             sources: [ { nfs: { server: nas.lan, path: /export/data } } ]\n",
        );
        let errs = validate_backup_config(&spec);
        let msg = errs
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(msg.contains("spec.volumeSnapshotClassName"), "{msg}");
        assert!(msg.contains("NFS"), "{msg}");

        // A PVC source with a class is fine; an NFS source WITHOUT a class is fine.
        let pvc: SnapshotPolicySpec = crate::testutil::from_yaml(
            "repository: { kind: Repository, name: r }\n\
             volumeSnapshotClassName: csi-class\n\
             sources: [ { pvc: { name: data } } ]\n",
        );
        assert!(validate_backup_config(&pvc).is_empty());
        let nfs: SnapshotPolicySpec = crate::testutil::from_yaml(
            "repository: { kind: Repository, name: r }\n\
             sources: [ { nfs: { server: nas.lan, path: /export/data } } ]\n",
        );
        assert!(validate_backup_config(&nfs).is_empty());
    }

    // --- validate_mover: inheritSecurityContextFrom XOR explicit (container OR pod) ---

    #[test]
    fn mover_inherit_is_mutually_exclusive_with_both_explicit_contexts() {
        use crate::common::ObjectRef;
        use crate::common::{MoverSpec, PodSelector};
        use k8s_openapi::api::core::v1::{PodSecurityContext, SecurityContext};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;

        let inherit = || {
            Some(PodSelector {
                pod_selector: LabelSelector::default(),
                container: None,
            })
        };

        // inherit + container securityContext → rejected.
        let with_container = MoverSpec {
            security_context: Some(SecurityContext {
                run_as_user: Some(1000),
                ..Default::default()
            }),
            inherit_security_context_from: inherit(),
            ..Default::default()
        };
        assert!(matches!(
            validate_mover(&with_container, "Restore mover"),
            Err(ValidationError::MutuallyExclusive { .. })
        ));

        // inherit + POD securityContext → also rejected (inherit copies the pod context too).
        let with_pod = MoverSpec {
            pod_security_context: Some(PodSecurityContext {
                fs_group: Some(1000),
                ..Default::default()
            }),
            inherit_security_context_from: inherit(),
            ..Default::default()
        };
        assert!(matches!(
            validate_mover(&with_pod, "Restore mover"),
            Err(ValidationError::MutuallyExclusive { .. })
        ));

        // Surfaced through the Restore validator.
        let mut spec = restore_with(
            RestoreSource::SnapshotRef(ObjectRef {
                name: "b".into(),
                namespace: None,
            }),
            None,
        );
        spec.mover = Some(with_container);
        assert!(matches!(
            validate_restore(&spec),
            Err(ValidationError::MutuallyExclusive { .. })
        ));

        // inherit alone, or explicit container+pod together (no inherit), are both fine.
        let inherit_only = MoverSpec {
            inherit_security_context_from: inherit(),
            ..Default::default()
        };
        assert!(validate_mover(&inherit_only, "Restore mover").is_ok());
        let explicit_both = MoverSpec {
            security_context: Some(SecurityContext {
                run_as_user: Some(1000),
                ..Default::default()
            }),
            pod_security_context: Some(PodSecurityContext {
                fs_group: Some(1000),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(validate_mover(&explicit_both, "Restore mover").is_ok());
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
            mover_defaults: None,
            catalog: None,
            maintenance: None,
            on_namespace_delete: Default::default(),
            mode: Default::default(),
            suspend: false,
        };
        assert!(validate_repository_no_inline_retention(&spec).is_ok());
    }

    // --- aggregate validators ---

    #[test]
    fn backup_config_aggregate_collects_multiple_errors() {
        let spec = SnapshotPolicySpec {
            repository: repo_ref(RepositoryKind::ClusterRepository, Some("forbidden")),
            identity: Some(Identity::default()),
            sources: vec![], // missing required
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
        use crate::snapshot_policy::{PvcSource, Source};
        let spec = SnapshotPolicySpec {
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
        };
        assert!(validate_backup_config(&spec).is_empty());
    }

    #[test]
    fn backup_aggregate_rejects_discovered_delete() {
        let spec = SnapshotSpec {
            policy_ref: None,
            tags: None,
            failure_policy: None,
            deletion_policy: Some(DeletionPolicy::Delete),
            pin: false,
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
        use crate::common::PolicyRef;
        use crate::snapshot_schedule::ScheduleSpec;
        let spec = SnapshotScheduleSpec {
            policy_ref: Some(PolicyRef {
                name: "c".into(),
                namespace: None,
            }),
            policy_selector: None,
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

    // --- §10 policyRef XOR policySelector ---

    #[test]
    fn schedule_requires_exactly_one_policy_target() {
        use crate::common::PolicyRef;
        use crate::snapshot_schedule::ScheduleSpec;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
        let base_schedule = || ScheduleSpec {
            cron: "0 2 * * *".into(),
            jitter: None,
            timezone: None,
            run_on_create: false,
            suspend: false,
            concurrency_policy: Default::default(),
            starting_deadline_seconds: None,
        };
        let pref = || {
            Some(PolicyRef {
                name: "pg".into(),
                namespace: None,
            })
        };
        let sel = || Some(LabelSelector::default());

        // Neither → MissingRequiredField.
        let neither = SnapshotScheduleSpec {
            policy_ref: None,
            policy_selector: None,
            schedule: base_schedule(),
            failed_jobs_history_limit: None,
        };
        assert!(matches!(
            validate_schedule_policy_target(&neither),
            Err(ValidationError::MissingRequiredField { .. })
        ));

        // Both → MutuallyExclusive.
        let both = SnapshotScheduleSpec {
            policy_ref: pref(),
            policy_selector: sel(),
            schedule: base_schedule(),
            failed_jobs_history_limit: None,
        };
        assert!(matches!(
            validate_schedule_policy_target(&both),
            Err(ValidationError::MutuallyExclusive { .. })
        ));

        // Exactly one (either form) → ok.
        let only_ref = SnapshotScheduleSpec {
            policy_ref: pref(),
            policy_selector: None,
            schedule: base_schedule(),
            failed_jobs_history_limit: None,
        };
        let only_sel = SnapshotScheduleSpec {
            policy_ref: None,
            policy_selector: sel(),
            schedule: base_schedule(),
            failed_jobs_history_limit: None,
        };
        assert!(validate_schedule_policy_target(&only_ref).is_ok());
        assert!(validate_schedule_policy_target(&only_sel).is_ok());
        // The aggregate validator surfaces the XOR problem too.
        assert!(
            validate_backup_schedule(&neither)
                .iter()
                .any(|e| matches!(e, ValidationError::MissingRequiredField { .. }))
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
            mover_defaults: None,
            catalog: None,
            maintenance: m,
            on_namespace_delete: Default::default(),
            mode: Default::default(),
            suspend: false,
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
            mover_defaults: None,
            catalog: None,
            allowed_namespaces: AllowedNamespaces::All(false),
            identity_defaults: None,
            maintenance: None,
            on_namespace_delete: Default::default(),
            mode: Default::default(),
            suspend: false,
            credential_projection: None,
        };
        assert!(!validate_cluster_repository(&spec).is_empty());
    }

    #[test]
    fn cluster_repository_rejects_bad_identity_expr() {
        use crate::backend::{Backend, FilesystemBackend};
        use crate::cluster_repository::IdentityDefaults;
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
            mover_defaults: None,
            catalog: None,
            allowed_namespaces: AllowedNamespaces::All(true),
            // `namspace` is an out-of-scope typo → rejected at admission (ADR-0004 §5).
            identity_defaults: Some(IdentityDefaults {
                hostname_expr: Some("namspace".into()),
                username_expr: None,
            }),
            maintenance: None,
            on_namespace_delete: Default::default(),
            mode: Default::default(),
            suspend: false,
            credential_projection: None,
        };
        let errs = validate_cluster_repository(&spec);
        assert!(
            errs.iter()
                .any(|e| matches!(e, ValidationError::IdentityExprEval { .. })),
            "expected IdentityExprEval, got {errs:?}"
        );
    }

    // --- create-time immutability (ADR-0005 §7) -----------------------------

    fn repo_spec_create(
        enc_secret: &str,
        splitter: Option<&str>,
        hash: Option<&str>,
        create_enc: Option<&str>,
    ) -> RepositorySpec {
        use crate::backend::{Backend, FilesystemBackend};
        use crate::common::{CreateBehavior, Encryption, SecretKeyRef};
        RepositorySpec {
            backend: Backend::Filesystem(FilesystemBackend {
                path: "/repo".into(),
                volume: None,
            }),
            encryption: Encryption {
                password_secret_ref: SecretKeyRef {
                    name: enc_secret.into(),
                    namespace: None,
                    key: None,
                },
            },
            create: Some(CreateBehavior {
                enabled: true,
                encryption: create_enc.map(String::from),
                splitter: splitter.map(String::from),
                hash: hash.map(String::from),
                ecc: None,
            }),
            mover_defaults: None,
            catalog: None,
            maintenance: None,
            on_namespace_delete: Default::default(),
            mode: Default::default(),
            suspend: false,
        }
    }

    #[test]
    fn repository_immutability_accepts_unchanged_fields() {
        let old = repo_spec_create("pw", Some("FIXED-4M"), Some("BLAKE2B-256"), Some("AES256"));
        let new = old.clone();
        assert!(validate_repository_immutability(&old, &new).is_empty());
    }

    #[test]
    fn repository_immutability_allows_changed_password_secret_ref() {
        // Renaming/repointing the password Secret is NOT an immutable change: kopia
        // fixes only the resolved password value, never the Secret reference, so a
        // rename with identical content must pass admission (regression: a GitOps
        // Secret rename used to wedge the whole Kustomization).
        let old = repo_spec_create("kopia-creds", None, None, None);
        let new = repo_spec_create("kopia-creds-renamed", None, None, None);
        assert!(
            validate_repository_immutability(&old, &new).is_empty(),
            "changing only the password Secret ref must be allowed"
        );
    }

    #[test]
    fn cluster_repository_immutability_allows_changed_password_secret_ref() {
        use crate::backend::{Backend, FilesystemBackend};
        use crate::common::{CreateBehavior, Encryption, SecretKeyRef};
        let mk = |secret: &str| ClusterRepositorySpec {
            backend: Backend::Filesystem(FilesystemBackend {
                path: "/r".into(),
                volume: None,
            }),
            encryption: Encryption {
                password_secret_ref: SecretKeyRef {
                    name: secret.into(),
                    namespace: Some("kopia-system".into()),
                    key: None,
                },
            },
            create: Some(CreateBehavior {
                enabled: true,
                encryption: None,
                splitter: Some("FIXED-4M".into()),
                hash: None,
                ecc: None,
            }),
            mover_defaults: None,
            catalog: None,
            allowed_namespaces: AllowedNamespaces::All(true),
            identity_defaults: None,
            maintenance: None,
            on_namespace_delete: Default::default(),
            mode: Default::default(),
            suspend: false,
            credential_projection: None,
        };
        assert!(
            validate_cluster_repository_immutability(&mk("creds"), &mk("creds-renamed")).is_empty(),
            "changing only the password Secret ref must be allowed"
        );
    }

    #[test]
    fn repository_immutability_rejects_changed_ecc() {
        use crate::common::Ecc;
        let mut old = repo_spec_create("pw", None, None, None);
        let mut new = old.clone();
        if let Some(c) = old.create.as_mut() {
            c.ecc = Some(Ecc {
                algorithm: Some("REED-SOLOMON-CRC32".into()),
                overhead_percent: Some(2),
            });
        }
        if let Some(c) = new.create.as_mut() {
            c.ecc = Some(Ecc {
                algorithm: Some("REED-SOLOMON-CRC32".into()),
                overhead_percent: Some(5), // changed overhead → immutable
            });
        }
        let errs = validate_repository_immutability(&old, &new);
        assert!(errs.contains(&ValidationError::Immutable {
            field: "create.ecc".to_string()
        }));
        // Unchanged ECC → no error.
        assert!(validate_repository_immutability(&old, &old.clone()).is_empty());
    }

    #[test]
    fn repository_immutability_rejects_changed_splitter_hash_and_create_encryption() {
        let old = repo_spec_create("pw", Some("FIXED-4M"), Some("BLAKE2B-256"), Some("AES256"));
        let new = repo_spec_create("pw", Some("DYNAMIC"), Some("HMAC-SHA256"), Some("CHACHA20"));
        let errs = validate_repository_immutability(&old, &new);
        assert!(errs.contains(&ValidationError::Immutable {
            field: "create.splitter".to_string()
        }));
        assert!(errs.contains(&ValidationError::Immutable {
            field: "create.hash".to_string()
        }));
        assert!(errs.contains(&ValidationError::Immutable {
            field: "create.encryption".to_string()
        }));
        // Unchanged encryption secret → no `encryption` immutable error.
        assert!(!errs.contains(&ValidationError::Immutable {
            field: "encryption".to_string()
        }));
    }

    #[test]
    fn repository_immutability_tolerates_absent_create_on_both_sides() {
        // create absent ⇒ no algos pinned; unchanged ⇒ no immutable errors.
        let mut old = repo_spec_create("pw", None, None, None);
        old.create = None;
        let new = old.clone();
        assert!(validate_repository_immutability(&old, &new).is_empty());
    }

    #[test]
    fn cluster_repository_immutability_rejects_changed_splitter() {
        use crate::backend::{Backend, FilesystemBackend};
        use crate::common::{CreateBehavior, Encryption, SecretKeyRef};
        let mk = |splitter: &str| ClusterRepositorySpec {
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
            create: Some(CreateBehavior {
                enabled: true,
                encryption: None,
                splitter: Some(splitter.into()),
                hash: None,
                ecc: None,
            }),
            mover_defaults: None,
            catalog: None,
            allowed_namespaces: AllowedNamespaces::All(true),
            identity_defaults: None,
            maintenance: None,
            on_namespace_delete: Default::default(),
            mode: Default::default(),
            suspend: false,
            credential_projection: None,
        };
        let old = mk("FIXED-4M");
        let same = mk("FIXED-4M");
        assert!(validate_cluster_repository_immutability(&old, &same).is_empty());
        let changed = mk("DYNAMIC");
        assert!(
            validate_cluster_repository_immutability(&old, &changed).contains(
                &ValidationError::Immutable {
                    field: "create.splitter".to_string()
                }
            )
        );
    }

    // --- identity-collision detection (ADR-0005 §6) -------------------------

    #[test]
    fn identity_collision_same_repo_same_identity_is_detected() {
        let existing = vec![ExistingIdentity {
            identity: "pg@billing:/pvc/data".into(),
            repo_key: "ClusterRepository/shared".into(),
            name: "billing/pg-a".into(),
        }];
        assert_eq!(
            detect_identity_collision(
                "pg@billing:/pvc/data",
                "ClusterRepository/shared",
                "billing/pg-b",
                &existing
            ),
            Some("billing/pg-a".to_string())
        );
    }

    #[test]
    fn identity_collision_different_repo_is_allowed() {
        let existing = vec![ExistingIdentity {
            identity: "pg@billing:/pvc/data".into(),
            repo_key: "ClusterRepository/shared".into(),
            name: "billing/pg-a".into(),
        }];
        // Same identity but a different repository → no collision (separate history).
        assert_eq!(
            detect_identity_collision(
                "pg@billing:/pvc/data",
                "Repository/billing/nas",
                "billing/pg-b",
                &existing
            ),
            None
        );
    }

    #[test]
    fn identity_collision_skips_self() {
        let existing = vec![ExistingIdentity {
            identity: "pg@billing:/pvc/data".into(),
            repo_key: "ClusterRepository/shared".into(),
            name: "billing/pg-a".into(),
        }];
        // A re-apply of the same object (same name) must not collide with itself.
        assert_eq!(
            detect_identity_collision(
                "pg@billing:/pvc/data",
                "ClusterRepository/shared",
                "billing/pg-a",
                &existing
            ),
            None
        );
    }

    #[test]
    fn identity_collision_different_identity_is_allowed() {
        let existing = vec![ExistingIdentity {
            identity: "pg@billing:/pvc/data".into(),
            repo_key: "ClusterRepository/shared".into(),
            name: "billing/pg-a".into(),
        }];
        assert_eq!(
            detect_identity_collision(
                "redis@billing:/pvc/cache",
                "ClusterRepository/shared",
                "billing/redis",
                &existing
            ),
            None
        );
    }

    // --- §13(d) RepositoryReplication ---

    fn replication_spec(
        source: RepositoryRef,
        dest: crate::backend::Backend,
        cron: &str,
    ) -> RepositoryReplicationSpec {
        use crate::common::CronSpec;
        RepositoryReplicationSpec {
            source_ref: source,
            destination: dest,
            destination_encryption: None,
            schedule: CronSpec {
                cron: cron.into(),
                jitter: None,
            },
            mover: None,
            suspend: false,
        }
    }

    #[test]
    fn replication_valid_spec_has_no_errors() {
        use crate::backend::{Backend, S3Backend};
        let spec = replication_spec(
            repo_ref(RepositoryKind::Repository, None),
            Backend::S3(S3Backend {
                bucket: "mirror".into(),
                prefix: None,
                endpoint: None,
                region: None,
                auth: None,
                tls: None,
            }),
            "0 5 * * *",
        );
        assert!(validate_repository_replication(&spec).is_empty());
    }

    #[test]
    fn replication_rejects_bad_cron_and_bad_clusterrepo_ref() {
        use crate::backend::{Backend, S3Backend};
        // A ClusterRepository sourceRef with a namespace + a bad cron → two errors.
        let spec = replication_spec(
            repo_ref(RepositoryKind::ClusterRepository, Some("oops")),
            Backend::S3(S3Backend {
                bucket: "mirror".into(),
                prefix: None,
                endpoint: None,
                region: None,
                auth: None,
                tls: None,
            }),
            "not a cron",
        );
        let errs = validate_repository_replication(&spec);
        assert!(
            errs.iter()
                .any(|e| matches!(e, ValidationError::ClusterRepoNamespaceForbidden { .. }))
        );
        assert!(
            errs.iter()
                .any(|e| matches!(e, ValidationError::InvalidCron { .. }))
        );
    }

    #[test]
    fn replication_rejects_invalid_destination_backend_content() {
        use crate::backend::{Backend, FilesystemBackend, NfsVolume, RepoVolume};
        // A filesystem destination with a relative NFS repo path is invalid content.
        let spec = replication_spec(
            repo_ref(RepositoryKind::Repository, None),
            Backend::Filesystem(FilesystemBackend {
                path: "/mirror".into(),
                volume: Some(RepoVolume::Nfs(NfsVolume {
                    server: "nas".into(),
                    path: "relative/path".into(),
                })),
            }),
            "0 5 * * *",
        );
        let errs = validate_repository_replication(&spec);
        assert!(
            errs.iter()
                .any(|e| matches!(e, ValidationError::InvalidFieldValue { .. }))
        );
    }

    #[test]
    fn replication_destination_differs_decision() {
        use crate::backend::{Backend, FilesystemBackend, S3Backend};
        let fs_a = Backend::Filesystem(FilesystemBackend {
            path: "/a".into(),
            volume: None,
        });
        let fs_a2 = Backend::Filesystem(FilesystemBackend {
            path: "/a".into(),
            volume: None,
        });
        let fs_b = Backend::Filesystem(FilesystemBackend {
            path: "/b".into(),
            volume: None,
        });
        // Same path → same target (self-replication).
        assert!(!replication_destination_differs(&fs_a, &fs_a2));
        // Different path → differ.
        assert!(replication_destination_differs(&fs_a, &fs_b));
        // S3 differs from filesystem.
        let s3 = Backend::S3(S3Backend {
            bucket: "b".into(),
            prefix: None,
            endpoint: None,
            region: None,
            auth: None,
            tls: None,
        });
        assert!(replication_destination_differs(&fs_a, &s3));
        // Same S3 bucket+prefix → same target.
        let s3b = Backend::S3(S3Backend {
            bucket: "b".into(),
            prefix: None,
            endpoint: None,
            region: None,
            auth: None,
            tls: None,
        });
        assert!(!replication_destination_differs(&s3, &s3b));
    }
}
