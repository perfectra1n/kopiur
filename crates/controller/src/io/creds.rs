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
/// the Backup/Restore paths use [`first_missing_cred`] to also surface a condition.
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
/// handling both the self-managed default and opt-in projection.
///
/// - `project == false` (the default): verify each `refs` Secret already exists in
///   `job_ns`; a missing one yields an actionable [`Error::MissingDependency`].
///   Returns the original names, `projected: 0`.
/// - `project == true` (opted in): for each ref, if its source namespace **is**
///   `job_ns` the Secret is already where the mover needs it — verify it is present
///   (identical to the opt-out path) and use its original name, copying nothing. If
///   the source namespace **differs** (a shared `ClusterRepository`), read the
///   source Secret and apply a per-Job copy (named [`projected_creds_name`], owned
///   by `owner`) into `job_ns`, and use the projected name. A missing source Secret
///   (or an unresolvable source namespace) yields an actionable
///   [`Error::MissingDependency`]; a `403` on apply is mapped to a message pointing
///   at the Helm RBAC toggle (degrade-not-crash — projection needs cluster-wide
///   `secrets` create/patch).
///
/// Re-reading the source and re-applying on every run keeps copies fresh, so there
/// is no drift to reconcile and no source-watch to maintain.
pub async fn resolve_mover_creds(
    client: &kube::Client,
    job_ns: &str,
    job_name: &str,
    owner: &OwnerReference,
    refs: &[CredsSecretRef],
    project: bool,
    ctx: &CredsContext<'_>,
) -> Result<MoverCreds> {
    if !project {
        ensure_creds_present(client, job_ns, ctx).await?;
        return Ok(MoverCreds {
            names: refs.iter().map(|r| r.name.clone()).collect(),
            projected: 0,
        });
    }

    let dst: Api<Secret> = Api::namespaced(client.clone(), job_ns);
    let mut names = Vec::with_capacity(refs.len());
    let mut projected = 0u64;
    for (idx, r) in refs.iter().enumerate() {
        let src_ns = r.namespace.as_deref().ok_or_else(|| {
            Error::MissingDependency(projection_unresolved_ns_message(&r.name, ctx))
        })?;
        // Already in the mover's namespace (the common namespaced-Repository case):
        // nothing to copy. Verify it is present — exactly the self-managed path —
        // and use its original name, so default-on projection is a no-op here.
        if src_ns == job_ns {
            if dst.get_opt(&r.name).await?.is_none() {
                return Err(Error::MissingDependency(missing_creds_message(
                    &r.name, job_ns, ctx,
                )));
            }
            names.push(r.name.clone());
            continue;
        }
        // Cross-namespace: project a per-Job copy owned by the consuming CR.
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
/// (Backup/Restore/Maintenance) against a [`ResolvedRepository`]. Convenience over
/// [`resolve_mover_creds`] that derives the credential references (with their
/// source namespaces) and the [`CredsContext`] from the repository. `owner` is the
/// consuming CR's owner reference, applied to any projected Secret so GC reaps it
/// with that CR. `project` is the consumer's opt-in
/// (`spec.credentialProjection.enabled` on the `BackupConfig`/`Restore`/
/// `Maintenance`) — projection is a consumer-side decision, not a repository one.
/// `repo_kind`/`repo_name` only label the actionable messages (a `Restore` may
/// infer its repository from the source config, so they are plain strings rather
/// than a `RepositoryRef`).
#[allow(clippy::too_many_arguments)]
pub async fn resolve_mover_creds_for(
    client: &kube::Client,
    job_ns: &str,
    job_name: &str,
    owner: &OwnerReference,
    repo: &ResolvedRepository,
    project: bool,
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
    resolve_mover_creds(client, job_ns, job_name, owner, &refs, project, &ctx).await
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
            kind: "Backup".into(),
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
        assert_eq!(owners[0].kind, "Backup");
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
