use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::Api;
use kube::core::ObjectMeta;

use kopiur_api::common::RepositoryKind;

use crate::consts::PROJECTED_FROM_ANNOTATION;
use crate::error::{Error, Result};

use super::{CredsSecretRef, ResolvedRepository, apply, mover_creds_secret_refs};

/// Context for the missing-credentials message: which Secrets the mover Job needs
/// in its namespace, and where the referencing repository keeps them (so the
/// message can name the cross-namespace mismatch).
pub struct CredsContext<'a> {
    /// Secret names the mover Job loads via `envFrom`, required in the Job's ns.
    pub secret_names: &'a [String],
    /// `Repository` or `ClusterRepository` — the kind of the referencing repo.
    pub repo_kind: &'a str,
    /// Name of the referencing repository.
    pub repo_name: &'a str,
    /// Namespace the repository's credential Secret lives in, when explicit (a
    /// `ClusterRepository` pins it, e.g. `kopiur-system`). `None` ⇒ same-namespace
    /// reference (a namespaced `Repository`).
    pub repo_secret_namespace: Option<&'a str>,
}

/// The actionable message for a credentials Secret missing from the mover Job's
/// namespace (ADR §4.12; the project's what/why/how-to-fix rule). Pure so the
/// exact text is unit-asserted. Names the missing Secret and namespace, explains
/// why (the mover loads it via namespace-local `envFrom`), states where the repo
/// currently keeps it, and gives concrete fixes.
pub fn missing_creds_message(secret: &str, job_ns: &str, ctx: &CredsContext) -> String {
    let mut msg = format!(
        "credentials Secret `{secret}` does not exist in namespace `{job_ns}`, where the mover \
         Job runs and loads it via envFrom — Kubernetes envFrom is namespace-local and cannot \
         read a Secret from another namespace."
    );
    match ctx.repo_secret_namespace {
        // Cross-namespace mismatch (typically a ClusterRepository whose Secret is
        // pinned to the operator namespace): name both ends and offer both fixes.
        Some(src) if src != job_ns => {
            msg.push_str(&format!(
                " The referenced {kind} `{name}` keeps that Secret in namespace `{src}`. \
                 Fix: create a Secret named `{secret}` in namespace `{job_ns}` carrying the same \
                 keys (e.g. `KOPIA_PASSWORD`, plus backend keys like `AWS_ACCESS_KEY_ID`/\
                 `AWS_SECRET_ACCESS_KEY`), or back up with a namespaced Repository whose secret \
                 lives in `{job_ns}`.",
                kind = ctx.repo_kind,
                name = ctx.repo_name,
            ));
        }
        // Same-namespace reference: the Secret simply isn't there yet.
        _ => {
            msg.push_str(&format!(
                " The {kind} `{name}` references it from namespace `{job_ns}`. \
                 Fix: create a Secret named `{secret}` in namespace `{job_ns}` carrying the \
                 repository credentials (e.g. `KOPIA_PASSWORD`, plus any backend keys).",
                kind = ctx.repo_kind,
                name = ctx.repo_name,
            ));
        }
    }
    msg
}

/// The `repo_kind` string for a [`RepositoryKind`] (for [`CredsContext`] messages).
pub fn repo_kind_str(kind: RepositoryKind) -> &'static str {
    match kind {
        RepositoryKind::Repository => "Repository",
        RepositoryKind::ClusterRepository => "ClusterRepository",
    }
}

/// The actionable message for the first credential Secret missing from the mover
/// Job's namespace, or `None` if all are present. Lets a caller surface the
/// blocking condition + Event before requeueing (see [`crate::io::publish_missing_creds_event`]).
pub async fn first_missing_cred(
    client: &kube::Client,
    job_ns: &str,
    ctx: &CredsContext<'_>,
) -> Result<Option<String>> {
    let api: Api<Secret> = Api::namespaced(client.clone(), job_ns);
    for name in ctx.secret_names {
        if api.get_opt(name).await?.is_none() {
            return Ok(Some(missing_creds_message(name, job_ns, ctx)));
        }
    }
    Ok(None)
}

/// Verify every credential Secret the mover Job needs exists in its namespace,
/// before launching a Job that would otherwise hang on a missing-Secret `envFrom`.
/// Returns an actionable [`Error::MissingDependency`] (Transient — a GitOps apply
/// may add the Secret shortly) naming the first missing Secret. Used by the
/// bootstrap paths (repository/cluster-repository), whose Secret is same-namespace;
/// the Snapshot/Restore paths use [`first_missing_cred`] to also surface a condition.
pub async fn ensure_creds_present(
    client: &kube::Client,
    job_ns: &str,
    ctx: &CredsContext<'_>,
) -> Result<()> {
    match first_missing_cred(client, job_ns, ctx).await? {
        Some(msg) => Err(Error::MissingDependency(msg)),
        None => Ok(()),
    }
}

/// Deterministic name of the projected copy of the `idx`-th credential Secret for
/// a mover Job named `job_name`. Per-Job and per-source so each copy is uniquely
/// owned by (and garbage-collected with) its consuming CR, mirroring the per-Job
/// work-spec ConfigMap. Distinct enough not to collide with a user-owned Secret.
pub fn projected_creds_name(job_name: &str, idx: usize) -> String {
    format!("{job_name}-creds-{idx}")
}

/// Build a kopiur-managed copy of a source credential `Secret` for `job_ns`,
/// owned by the consuming CR (`owner`) so native GC reaps it with that CR — the
/// owner and the copy are always in the same namespace, so the ownerRef is valid
/// (cross-namespace ownerRefs are forbidden by Kubernetes). Pure (no IO) so the
/// shape is unit-testable. Copies `data`/`stringData` verbatim and preserves the
/// source `type`; records the source in [`PROJECTED_FROM_ANNOTATION`]. Not marked
/// immutable — it is re-applied (refreshed from source) on every run.
pub fn build_projected_secret(
    name: &str,
    job_ns: &str,
    owner: OwnerReference,
    src: &Secret,
) -> Secret {
    let src_ns = src.metadata.namespace.clone().unwrap_or_default();
    let src_name = src.metadata.name.clone().unwrap_or_default();
    let labels = BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            "kopiur".to_string(),
        ),
        (
            "app.kubernetes.io/component".to_string(),
            "credentials".to_string(),
        ),
    ]);
    let annotations = BTreeMap::from([(
        PROJECTED_FROM_ANNOTATION.to_string(),
        format!("{src_ns}/{src_name}"),
    )]);
    Secret {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(job_ns.to_string()),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: Some(vec![owner]),
            ..Default::default()
        },
        data: src.data.clone(),
        string_data: src.string_data.clone(),
        type_: src.type_.clone().or_else(|| Some("Opaque".to_string())),
        immutable: None,
    }
}

/// The decision for projecting a repository credential Secret across namespaces
/// (ADR-0005 §8). Pure + exhaustive so the fail-closed authorization model is
/// tested in one place. A same-namespace Secret is never a "projection" — it is a
/// verify-in-place, decided separately by the caller (`src_ns == job_ns`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionDecision {
    /// Copy the source Secret into the Job's namespace.
    Project,
    /// Do not project: surface the reason. Carries which gate was unmet.
    Deny(ProjectionDenyReason),
}

/// Why a cross-namespace credential projection was denied (ADR-0005 §8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionDenyReason {
    /// The consumer never opted in (`credentialProjection.enabled` is false/absent).
    ConsumerNotOptedIn,
    /// The repository owner has not allowed projection
    /// (`ClusterRepository.credentialProjection.allowed` is false/absent).
    OwnerNotAllowed,
}

/// Decide whether to project a repository credential Secret into a **foreign**
/// consumer namespace (ADR-0005 §8). Fail-closed: projection requires BOTH the
/// consumer opt-in (`enabled`) AND the repository-owner allow (`allowed`); operator
/// RBAC is enforced separately at apply time (a `403` → actionable error). Pure +
/// exhaustively matched so the authorization model lives in one tested place.
///
/// (RBAC is the third gate but it is an apply-time IO outcome, not a pure input.)
pub fn projection_decision(consumer_enabled: bool, owner_allowed: bool) -> ProjectionDecision {
    match (consumer_enabled, owner_allowed) {
        (true, true) => ProjectionDecision::Project,
        (false, _) => ProjectionDecision::Deny(ProjectionDenyReason::ConsumerNotOptedIn),
        (true, false) => ProjectionDecision::Deny(ProjectionDenyReason::OwnerNotAllowed),
    }
}

/// The credential Secret names a mover Job should load via `envFrom`, plus how
/// many of them the operator actually projected (copied cross-namespace) this run.
pub struct MoverCreds {
    /// Names to put in the Job's `envFrom`, in order.
    pub names: Vec<String>,
    /// How many `names` are freshly-projected cross-namespace copies (for the
    /// `kopiur_secrets_projected` metric). Same-namespace Secrets are not counted.
    pub projected: u64,
}

/// Resolve the credential Secret names a mover Job should load via `envFrom`,
/// handling both the self-managed default and gated cross-namespace projection
/// (ADR-0005 §8).
///
/// `consumer_enabled` is the consumer opt-in (`credentialProjection.enabled` on the
/// `SnapshotPolicy`/`Restore`); `owner_allowed` is the repository-owner allow
/// (`ClusterRepository.credentialProjection.allowed`). Per-ref:
///
/// - **Same-namespace** (`src_ns == job_ns`, the common namespaced-`Repository`
///   layout): the Secret is already where the mover needs it. Verify it is present
///   and use its original name — no projection, no gate (there is nothing to copy
///   across a trust boundary). A missing one yields an actionable error.
/// - **Cross-namespace** (a shared `ClusterRepository`): projection is gated by
///   [`projection_decision`] — it requires BOTH `consumer_enabled` AND
///   `owner_allowed`, else it **fails closed** with an actionable
///   [`Error::MissingDependency`] naming the unmet gate. When permitted, read the
///   source Secret and apply a per-Job copy (named [`projected_creds_name`], owned
///   by `owner`) into `job_ns`. A missing source Secret / unresolvable source
///   namespace yields an actionable error; a `403` on apply maps to the Helm RBAC
///   toggle hint (degrade-not-crash — projection needs cluster-wide `secrets`
///   create/patch, the third gate).
///
/// Re-reading the source and re-applying on every run keeps copies fresh, so there
/// is no drift to reconcile and no source-watch to maintain.
#[allow(clippy::too_many_arguments)]
pub async fn resolve_mover_creds(
    client: &kube::Client,
    job_ns: &str,
    job_name: &str,
    owner: &OwnerReference,
    refs: &[CredsSecretRef],
    consumer_enabled: bool,
    owner_allowed: bool,
    ctx: &CredsContext<'_>,
) -> Result<MoverCreds> {
    let dst: Api<Secret> = Api::namespaced(client.clone(), job_ns);
    let mut names = Vec::with_capacity(refs.len());
    let mut projected = 0u64;
    for (idx, r) in refs.iter().enumerate() {
        let src_ns = match r.namespace.as_deref() {
            Some(ns) => ns,
            // No resolvable source namespace. If the Secret happens to already be in
            // the Job's namespace we'd have matched the same-namespace branch via an
            // explicit ns; with none resolvable we can neither verify-in-place nor
            // project. When the consumer didn't ask for projection, fall back to the
            // self-managed verify in job_ns; otherwise it's the unresolved-source error.
            None => {
                if consumer_enabled {
                    return Err(Error::MissingDependency(projection_unresolved_ns_message(
                        &r.name, ctx,
                    )));
                }
                if dst.get_opt(&r.name).await?.is_none() {
                    return Err(Error::MissingDependency(missing_creds_message(
                        &r.name, job_ns, ctx,
                    )));
                }
                names.push(r.name.clone());
                continue;
            }
        };
        // Already in the mover's namespace (the common namespaced-Repository case):
        // nothing to copy across a trust boundary. Verify it is present — exactly the
        // self-managed path — and use its original name. No owner/consumer gate here.
        if src_ns == job_ns {
            if dst.get_opt(&r.name).await?.is_none() {
                return Err(Error::MissingDependency(missing_creds_message(
                    &r.name, job_ns, ctx,
                )));
            }
            names.push(r.name.clone());
            continue;
        }
        // Cross-namespace. If the consumer never opted in, this is the self-managed
        // path: the user is expected to have placed the Secret in the mover namespace
        // themselves (e.g. a hand-copied ClusterRepository password). Verify it is
        // present in `job_ns` and use its name — never silently project without opt-in.
        if !consumer_enabled {
            if dst.get_opt(&r.name).await?.is_none() {
                return Err(Error::MissingDependency(missing_creds_message(
                    &r.name, job_ns, ctx,
                )));
            }
            names.push(r.name.clone());
            continue;
        }
        // Consumer opted in: projection is gated. Fail closed unless the repository
        // owner also allows it (ADR-0005 §8). (RBAC is the third gate, enforced at
        // apply time below as a 403 → actionable error.)
        match projection_decision(consumer_enabled, owner_allowed) {
            ProjectionDecision::Project => {}
            ProjectionDecision::Deny(reason) => {
                return Err(Error::MissingDependency(projection_denied_message(
                    &r.name, src_ns, job_ns, reason, ctx,
                )));
            }
        }
        // Permitted: project a per-Job copy owned by the consuming CR.
        let src_api: Api<Secret> = Api::namespaced(client.clone(), src_ns);
        let src = src_api.get_opt(&r.name).await?.ok_or_else(|| {
            Error::MissingDependency(projection_source_missing_message(
                &r.name, src_ns, job_ns, ctx,
            ))
        })?;
        let proj_name = projected_creds_name(job_name, idx);
        let secret = build_projected_secret(&proj_name, job_ns, owner.clone(), &src);
        apply(&dst, &proj_name, &secret)
            .await
            .map_err(|e| map_projection_apply_error(e, &proj_name, job_ns))?;
        names.push(proj_name);
        projected += 1;
    }
    Ok(MoverCreds { names, projected })
}

/// Resolve the mover Job's `envFrom` credential Secret names for a consumer run
/// (Snapshot/Restore/Maintenance) against a [`ResolvedRepository`]. Convenience over
/// [`resolve_mover_creds`] that derives the credential references (with their
/// source namespaces) and the [`CredsContext`] from the repository. `owner` is the
/// consuming CR's owner reference, applied to any projected Secret so GC reaps it
/// with that CR.
///
/// `consumer_enabled` is the consumer opt-in (`spec.credentialProjection.enabled` on
/// the `SnapshotPolicy`/`Restore`/`Maintenance`); the owner gate
/// (`ClusterRepository.credentialProjection.allowed`) is read from the resolved
/// repository (`repo.credential_projection_allowed`) — a namespaced `Repository`
/// reports `false`, which is harmless because its projection is always a
/// same-namespace no-op (ADR-0005 §8). `repo_kind`/`repo_name` only label the
/// actionable messages (a `Restore` may infer its repository from the source
/// config, so they are plain strings rather than a `RepositoryRef`).
#[allow(clippy::too_many_arguments)]
pub async fn resolve_mover_creds_for(
    client: &kube::Client,
    job_ns: &str,
    job_name: &str,
    owner: &OwnerReference,
    repo: &ResolvedRepository,
    consumer_enabled: bool,
    repo_kind: &str,
    repo_name: &str,
) -> Result<MoverCreds> {
    let refs = mover_creds_secret_refs(
        &repo.backend,
        &repo.encryption,
        repo.repo_namespace.as_deref(),
    );
    let names: Vec<String> = refs.iter().map(|r| r.name.clone()).collect();
    let ctx = CredsContext {
        secret_names: &names,
        repo_kind,
        repo_name,
        repo_secret_namespace: repo.encryption.password_secret_ref.namespace.as_deref(),
    };
    resolve_mover_creds(
        client,
        job_ns,
        job_name,
        owner,
        &refs,
        consumer_enabled,
        repo.credential_projection_allowed,
        &ctx,
    )
    .await
}

/// Actionable message when a cross-namespace credential projection is denied by the
/// fail-closed §8 gate. Names the unmet gate (consumer opt-in vs. repository-owner
/// allow), the Secret + namespaces, and the concrete fix. The what/why/fix rule.
fn projection_denied_message(
    secret: &str,
    src_ns: &str,
    job_ns: &str,
    reason: ProjectionDenyReason,
    ctx: &CredsContext,
) -> String {
    let (why, fix) = match reason {
        ProjectionDenyReason::ConsumerNotOptedIn => (
            "the consumer has not opted in to credential projection",
            "set `spec.credentialProjection.enabled: true` on this SnapshotPolicy/Restore \
             (and ensure the ClusterRepository owner sets `credentialProjection.allowed: true`), \
             or create the Secret in the mover namespace yourself",
        ),
        ProjectionDenyReason::OwnerNotAllowed => (
            "the ClusterRepository owner has not allowed credential projection",
            "ask the repository owner to set `credentialProjection.allowed: true` on the \
             ClusterRepository, or create the Secret in the mover namespace yourself",
        ),
    };
    format!(
        "credential Secret `{secret}` lives in namespace `{src_ns}` but the mover Job runs in \
         `{job_ns}`, and projecting it across namespaces is not permitted: {why}. The referenced \
         {kind} `{name}` is the source. Fix: {fix}.",
        kind = ctx.repo_kind,
        name = ctx.repo_name,
    )
}

/// Actionable message when projection cannot read a source Secret because its
/// source namespace is unresolvable (a `ClusterRepository` reference that omits
/// `namespace`). The what/why/fix rule (ADR §4.12).
fn projection_unresolved_ns_message(secret: &str, ctx: &CredsContext) -> String {
    format!(
        "credential Secret `{secret}` for {kind} `{name}` has no resolvable source namespace, so \
         credential projection cannot read it to copy into the mover Job's namespace. Fix: set an \
         explicit `namespace` on the Secret reference (a {kind} reference must pin one), or disable \
         `spec.credentialProjection` and manage the Secret in each mover namespace yourself.",
        kind = ctx.repo_kind,
        name = ctx.repo_name,
    )
}

/// Actionable message when projection is enabled but the *source* Secret is absent
/// from its source namespace (so there is nothing to copy). The what/why/fix rule.
fn projection_source_missing_message(
    secret: &str,
    src_ns: &str,
    job_ns: &str,
    ctx: &CredsContext,
) -> String {
    format!(
        "credential Secret `{secret}` was not found in source namespace `{src_ns}`, so the \
         {kind} `{name}` cannot project it into namespace `{job_ns}` where the mover Job runs. \
         Fix: create Secret `{secret}` in `{src_ns}` carrying the repository credentials (e.g. \
         `KOPIA_PASSWORD`, plus backend keys like `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`).",
        kind = ctx.repo_kind,
        name = ctx.repo_name,
    )
}

/// Map a credential-projection apply failure to an actionable error. A `403`
/// means the operator lacks the cluster-wide `secrets` create/patch RBAC that
/// projection requires; point the admin at the Helm toggle that grants it.
/// Other errors pass through unchanged. Transient (re-driven once RBAC is fixed).
fn map_projection_apply_error(e: Error, proj_name: &str, job_ns: &str) -> Error {
    if let Error::Kube(kube::Error::Api(resp)) = &e
        && resp.code == 403
    {
        return Error::MissingDependency(format!(
            "the operator is not permitted to write the projected credentials Secret \
             `{proj_name}` in namespace `{job_ns}` (HTTP 403). Credential projection needs \
             cluster-wide `secrets` create/patch RBAC. Fix: set `secretProjection.enabled: true` \
             in the Helm chart (grants the operator ClusterRole those verbs), or disable \
             `spec.credentialProjection` on the repository and manage the Secret in `{job_ns}`."
        ));
    }
    e
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::core::Status;

    fn api_error(code: u16) -> Error {
        Error::Kube(kube::Error::Api(Box::new(Status {
            code,
            message: "boom".into(),
            reason: "Forbidden".into(),
            ..Default::default()
        })))
    }

    fn owner(name: &str) -> OwnerReference {
        OwnerReference {
            api_version: "kopiur.home-operations.com/v1alpha1".into(),
            kind: "Snapshot".into(),
            name: name.into(),
            uid: "uid-123".into(),
            controller: Some(true),
            block_owner_deletion: Some(false),
        }
    }

    fn ctx() -> CredsContext<'static> {
        CredsContext {
            secret_names: &[],
            repo_kind: "ClusterRepository",
            repo_name: "shared",
            repo_secret_namespace: Some("kopiur-system"),
        }
    }

    // --- projection_decision: the §8 fail-closed authorization gate ----------

    #[test]
    fn projection_allowed_only_when_consumer_and_owner_both_agree() {
        assert_eq!(projection_decision(true, true), ProjectionDecision::Project);
    }

    #[test]
    fn projection_denied_when_owner_disallows_even_if_consumer_enabled() {
        // The headline §8 fix: a tenant opting in cannot copy the shared repo password
        // unless the ClusterRepository owner allows it.
        assert_eq!(
            projection_decision(true, false),
            ProjectionDecision::Deny(ProjectionDenyReason::OwnerNotAllowed)
        );
    }

    #[test]
    fn projection_denied_when_consumer_not_opted_in() {
        assert_eq!(
            projection_decision(false, true),
            ProjectionDecision::Deny(ProjectionDenyReason::ConsumerNotOptedIn)
        );
        assert_eq!(
            projection_decision(false, false),
            ProjectionDecision::Deny(ProjectionDenyReason::ConsumerNotOptedIn)
        );
    }

    #[test]
    fn projection_denied_message_names_the_unmet_gate() {
        let owner_msg = projection_denied_message(
            "repo-pw",
            "kopiur-system",
            "team-a",
            ProjectionDenyReason::OwnerNotAllowed,
            &ctx(),
        );
        assert!(owner_msg.contains("owner has not allowed"));
        assert!(owner_msg.contains("credentialProjection.allowed: true"));
        assert!(owner_msg.contains("`repo-pw`"));

        let consumer_msg = projection_denied_message(
            "repo-pw",
            "kopiur-system",
            "team-a",
            ProjectionDenyReason::ConsumerNotOptedIn,
            &ctx(),
        );
        assert!(consumer_msg.contains("consumer has not opted in"));
        assert!(consumer_msg.contains("credentialProjection.enabled: true"));
    }

    #[test]
    fn projected_name_is_per_job_and_index() {
        assert_eq!(
            projected_creds_name("nightly-1700", 0),
            "nightly-1700-creds-0"
        );
        assert_eq!(
            projected_creds_name("nightly-1700", 1),
            "nightly-1700-creds-1"
        );
    }

    #[test]
    fn projected_secret_copies_data_and_is_owned_and_labeled() {
        let mut src = Secret {
            metadata: ObjectMeta {
                name: Some("repo-pw".into()),
                namespace: Some("kopiur-system".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        src.data = Some(BTreeMap::from([(
            "KOPIA_PASSWORD".to_string(),
            k8s_openapi::ByteString(b"hunter2".to_vec()),
        )]));

        let s = build_projected_secret("job-creds-0", "team-a", owner("job"), &src);

        assert_eq!(s.metadata.name.as_deref(), Some("job-creds-0"));
        assert_eq!(s.metadata.namespace.as_deref(), Some("team-a"));
        // Owned by the consuming CR in the SAME namespace → valid ownerRef, native GC.
        let owners = s.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].kind, "Snapshot");
        assert_eq!(owners[0].controller, Some(true));
        // Data copied verbatim; type defaulted to Opaque; not immutable (refreshable).
        assert_eq!(s.data, src.data);
        assert_eq!(s.type_.as_deref(), Some("Opaque"));
        assert_eq!(s.immutable, None);
        // Managed labels + source annotation for discoverability.
        let labels = s.metadata.labels.as_ref().unwrap();
        assert_eq!(
            labels
                .get("app.kubernetes.io/managed-by")
                .map(String::as_str),
            Some("kopiur")
        );
        assert_eq!(
            labels
                .get("app.kubernetes.io/component")
                .map(String::as_str),
            Some("credentials")
        );
        let ann = s.metadata.annotations.as_ref().unwrap();
        assert_eq!(
            ann.get(PROJECTED_FROM_ANNOTATION).map(String::as_str),
            Some("kopiur-system/repo-pw")
        );
    }

    #[test]
    fn source_missing_message_is_actionable() {
        let msg = projection_source_missing_message("repo-pw", "kopiur-system", "team-a", &ctx());
        // names what, where (source), why (cannot project to job ns), and how to fix.
        assert!(msg.contains("`repo-pw`"));
        assert!(msg.contains("`kopiur-system`"));
        assert!(msg.contains("`team-a`"));
        assert!(msg.contains("ClusterRepository `shared`"));
        assert!(msg.contains("KOPIA_PASSWORD"));
    }

    #[test]
    fn unresolved_ns_message_points_at_explicit_namespace() {
        let msg = projection_unresolved_ns_message("repo-pw", &ctx());
        assert!(msg.contains("no resolvable source namespace"));
        assert!(msg.contains("explicit `namespace`"));
        assert!(msg.contains("spec.credentialProjection"));
    }

    #[test]
    fn apply_403_maps_to_rbac_toggle_hint() {
        let mapped = map_projection_apply_error(api_error(403), "job-creds-0", "team-a");
        match mapped {
            Error::MissingDependency(m) => {
                assert!(m.contains("HTTP 403"));
                assert!(m.contains("secretProjection.enabled: true"));
                assert!(m.contains("`job-creds-0`"));
            }
            other => panic!("expected MissingDependency, got {other:?}"),
        }
    }

    #[test]
    fn non_403_error_passes_through_unchanged() {
        assert!(matches!(
            map_projection_apply_error(api_error(500), "job-creds-0", "team-a"),
            Error::Kube(_)
        ));
    }
}
