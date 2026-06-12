//! `kubectl kopiur migrate volsync` — translate VolSync restic
//! ReplicationSource/ReplicationDestination objects into kopiur
//! SnapshotPolicy/SnapshotSchedule/Restore (+ optionally a Repository and
//! credential Secrets derived from the restic Secret).
//!
//! **Config translation ONLY.** A VolSync restic repository is NOT a kopia
//! repository: no backup data is migrated, and the new kopiur repository
//! starts empty. Keep VolSync running until kopiur's retention coverage
//! suffices.

pub mod translate;
pub mod volsync_types;

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Secret;
use kube::api::{Api, DynamicObject, GroupVersionKind, ListParams, Patch, PatchParams};
use kube::discovery::ApiResource;

use crate::CmdOutput;
use crate::cli::MigrateVolsyncArgs;
use crate::context::KubeCtx;
use crate::error::{CliError, classify_kube};
use translate::{FieldNote, PASSWORD_PLACEHOLDER, Translation};
use volsync_types::{ReplicationDestinationSpec, ReplicationSourceSpec};

/// The banner that heads the emitted YAML (and goes to stderr). Loud on
/// purpose: the most dangerous misunderstanding of this command is thinking
/// it moves data.
const BANNER: &str = "\
# ============================================================================
# kopiur migrate volsync — CONFIG TRANSLATION ONLY
#
# A VolSync restic repository is NOT a kopia repository. NO backup data is
# migrated; the kopiur repository referenced below starts EMPTY and fills as
# kopiur takes its own snapshots. Keep VolSync (and its repository) until
# kopiur's retention coverage is sufficient for your recovery needs.
#
# Review the per-field accounting printed on stderr before applying.
# ============================================================================";

fn volsync_api(ctx: &KubeCtx, kind: &str) -> Api<DynamicObject> {
    let gvk = GroupVersionKind::gvk("volsync.backube", "v1alpha1", kind);
    let resource = ApiResource::from_gvk(&gvk);
    Api::namespaced_with(ctx.client.clone(), &ctx.namespace, &resource)
}

/// One translated VolSync object, for reporting.
struct Translated {
    source: String,
    translation: Translation,
}

/// Build the Repository + Secrets from a restic repository Secret
/// (`--resolve-secrets`). Returns `(repository_ref, objects)`.
fn emit_repository_objects(
    namespace: &str,
    restic_secret_name: &str,
    restic_secret: &Secret,
) -> Result<(serde_json::Value, Vec<serde_json::Value>, Vec<FieldNote>), CliError> {
    let data = restic_secret.data.clone().unwrap_or_default();
    let get = |key: &str| -> Option<String> {
        data.get(key)
            .and_then(|v| String::from_utf8(v.0.clone()).ok())
    };
    let url = get("RESTIC_REPOSITORY").ok_or_else(|| CliError::MigrationInput {
        what: format!(
            "restic Secret {namespace}/{restic_secret_name} has no RESTIC_REPOSITORY key"
        ),
        fix: "point --repository at an existing kopiur Repository instead, or fix the Secret"
            .into(),
    })?;

    let creds_name = format!("{restic_secret_name}-kopiur-creds");
    let password_name = format!("{restic_secret_name}-kopiur-password");
    let repo_name = format!("{restic_secret_name}-kopiur");

    let (mut backend, scheme) = translate::backend_from_restic_repository(&url, &creds_name)
        .map_err(|reason| CliError::MigrationInput {
            what: reason,
            fix: "author the kopiur Repository by hand and pass it via --repository".into(),
        })?;

    let mut notes = vec![FieldNote {
        field: format!("secret/{restic_secret_name}.RESTIC_REPOSITORY"),
        disposition: translate::Disposition::Mapped {
            to: format!("Repository/{repo_name}.spec.backend"),
        },
    }];

    let mut objects = Vec::new();
    // Carry credentials the way kopiur's MOVER reads them (kopia's env names
    // and backend fields — not restic's names; see translate::cred_plan).
    let mut creds_data = BTreeMap::new();
    for carry in translate::cred_plan(scheme) {
        match carry {
            translate::CredCarry::Env { from, to } => {
                if let Some(value) = get(from) {
                    creds_data.insert(to.to_string(), value);
                    notes.push(FieldNote {
                        field: format!("secret/{restic_secret_name}.{from}"),
                        disposition: translate::Disposition::Mapped {
                            to: format!("Secret/{creds_name}.{to}"),
                        },
                    });
                }
            }
            translate::CredCarry::BackendField { from, pointer } => {
                if let Some(value) = get(from) {
                    if let Some(slot) = backend.pointer_mut(pointer) {
                        *slot = serde_json::json!(value);
                    } else {
                        // Parent object exists (the scheme just built it);
                        // insert the leaf key.
                        let (parent, key) = pointer.rsplit_once('/').expect("pointer has a key");
                        if let Some(obj) =
                            backend.pointer_mut(parent).and_then(|v| v.as_object_mut())
                        {
                            obj.insert(key.to_string(), serde_json::json!(value));
                        }
                    }
                    notes.push(FieldNote {
                        field: format!("secret/{restic_secret_name}.{from}"),
                        disposition: translate::Disposition::Mapped {
                            to: format!("Repository/{repo_name}.spec.backend{pointer}"),
                        },
                    });
                }
            }
        }
    }
    if matches!(scheme, translate::BackendScheme::Gcs) {
        notes.push(FieldNote {
            field: format!("secret/{restic_secret_name}.GOOGLE_APPLICATION_CREDENTIALS"),
            disposition: translate::Disposition::Unmappable {
                reason: format!(
                    "restic stores a file PATH; kopiur needs the service-account JSON CONTENT \
                     under {creds_name}.KOPIA_GCS_CREDENTIALS — add it yourself before applying"
                ),
            },
        });
    }
    if !creds_data.is_empty() {
        objects.push(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": creds_name, "namespace": namespace },
            "stringData": creds_data,
        }));
    }
    // The kopia password CANNOT be the restic password's hash-of-use — and
    // reusing the same passphrase across tools is its own hazard. Emit an
    // explicitly-invalid placeholder; --apply refuses it.
    notes.push(FieldNote {
        field: format!("secret/{restic_secret_name}.RESTIC_PASSWORD"),
        disposition: translate::Disposition::Unmappable {
            reason: format!(
                "a kopia repository needs its OWN new password; {password_name} carries a \
                 REPLACE_ME placeholder you must set before applying"
            ),
        },
    });
    objects.push(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": password_name, "namespace": namespace },
        "stringData": { "KOPIA_PASSWORD": PASSWORD_PLACEHOLDER },
    }));
    objects.push(serde_json::json!({
        "apiVersion": kopiur_api::consts::API_VERSION,
        "kind": "Repository",
        "metadata": { "name": repo_name, "namespace": namespace },
        "spec": {
            "backend": backend,
            "encryption": { "passwordSecretRef": { "name": password_name, "key": "KOPIA_PASSWORD" } },
            "create": { "enabled": true }
        }
    }));
    let repo_ref = serde_json::json!({ "kind": "Repository", "name": repo_name });
    Ok((repo_ref, objects, notes))
}

/// Render the accounting for stderr. Pure.
fn render_notes(translated: &[Translated]) -> String {
    let mut out = String::new();
    for t in translated {
        out.push_str(&format!("{}:\n", t.source));
        for note in &t.translation.notes {
            let line = match &note.disposition {
                translate::Disposition::Mapped { to } => {
                    format!("  mapped      {} -> {to}\n", note.field)
                }
                translate::Disposition::Unmappable { reason } => {
                    format!("  UNMAPPABLE  {}: {reason}\n", note.field)
                }
                translate::Disposition::Ignored { reason } => {
                    format!("  ignored     {}: {reason}\n", note.field)
                }
            };
            out.push_str(&line);
        }
    }
    out
}

/// Does any emitted object still carry a REPLACE_ME marker? Pure.
fn has_placeholder(objects: &[serde_json::Value]) -> bool {
    objects
        .iter()
        .any(|o| serde_json::to_string(o).is_ok_and(|s| s.contains("REPLACE_ME")))
}

/// Render the emitted objects as a multi-doc YAML stream headed by the banner.
fn render_yaml(objects: &[serde_json::Value]) -> Result<String, CliError> {
    let mut out = String::from(BANNER);
    out.push('\n');
    for obj in objects {
        out.push_str("---\n");
        out.push_str(
            &serde_yaml::to_string(obj).map_err(|e| CliError::Serialization {
                what: "translated manifest",
                source: e.into(),
            })?,
        );
    }
    Ok(out)
}

/// Run `migrate volsync`.
pub async fn run(ctx: &KubeCtx, args: &MigrateVolsyncArgs) -> Result<CmdOutput, CliError> {
    if matches!(ctx.scope, crate::context::Scope::All) {
        return Err(CliError::AllNamespacesNotApplicable {
            command: "migrate volsync",
        });
    }
    let ns = ctx.namespace.clone();

    // --- list the VolSync sources ---
    let sources_api = volsync_api(ctx, "ReplicationSource");
    let listed = sources_api
        .list(&ListParams::default())
        .await
        .map_err(|e| match &e {
            kube::Error::Api(ae) if ae.code == 404 => CliError::MigrationInput {
                what: "the cluster has no volsync.backube/v1alpha1 ReplicationSource resource"
                    .into(),
                fix: "is VolSync installed on this cluster? Nothing to migrate without its CRDs"
                    .into(),
            },
            _ => classify_kube(
                "list",
                "ReplicationSource",
                "replicationsources",
                Some(&ns),
                None,
                e,
            ),
        })?;
    let mut sources: Vec<DynamicObject> = listed.items;
    if let Some(name) = &args.name {
        sources.retain(|o| o.metadata.name.as_deref() == Some(name));
        if sources.is_empty() {
            return Err(CliError::NotFound {
                kind: "ReplicationSource",
                plural: "replicationsources",
                name: name.clone(),
                scope: crate::error::scope_suffix(Some(&ns)),
                scope_flag: format!(" -n {ns}"),
            });
        }
    }
    if sources.is_empty() {
        return Ok(CmdOutput::ok(format!(
            "no ReplicationSources found in namespace {ns}; nothing to migrate\n"
        )));
    }

    let mut objects: Vec<serde_json::Value> = Vec::new();
    let mut translated: Vec<Translated> = Vec::new();
    // restic Secret name -> kopiur repository ref (emitted once per Secret).
    let mut emitted_repos: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    // policy name per restic Secret, for destination pairing.
    let mut policies_by_secret: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for source in &sources {
        let name = source.metadata.name.clone().unwrap_or_default();
        let spec: ReplicationSourceSpec = serde_json::from_value(
            source.data.get("spec").cloned().unwrap_or_default(),
        )
        .map_err(|e| CliError::MigrationInput {
            what: format!("ReplicationSource {ns}/{name}: cannot decode spec: {e}"),
            fix: "check the object; only volsync.backube/v1alpha1 shapes are supported".into(),
        })?;

        let restic_secret_name = spec
            .restic
            .as_ref()
            .and_then(|r| r.repository.clone())
            .unwrap_or_default();

        // Which kopiur repository the policy points at.
        let repo_ref = if let Some(existing) = &args.repository {
            let kind: kopiur_api::common::RepositoryKind = args.repository_kind.into();
            serde_json::json!({ "kind": format!("{kind:?}"), "name": existing })
        } else {
            // --resolve-secrets: derive (and emit, once per restic Secret).
            match emitted_repos.get(&restic_secret_name) {
                Some(r) => r.clone(),
                None => {
                    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
                    let secret = secrets
                        .get_opt(&restic_secret_name)
                        .await
                        .map_err(|e| {
                            classify_kube(
                                "get",
                                "Secret",
                                "secrets",
                                Some(&ns),
                                Some(&restic_secret_name),
                                e,
                            )
                        })?
                        .ok_or_else(|| CliError::MigrationInput {
                            what: format!(
                                "ReplicationSource {ns}/{name} references restic Secret \
                                 {restic_secret_name:?}, which does not exist"
                            ),
                            fix: "fix the VolSync object or pass --repository to skip secret \
                                  resolution"
                                .into(),
                        })?;
                    let (repo_ref, repo_objects, notes) =
                        emit_repository_objects(&ns, &restic_secret_name, &secret)?;
                    objects.extend(repo_objects);
                    translated.push(Translated {
                        source: format!("secret/{restic_secret_name}"),
                        translation: Translation {
                            objects: Vec::new(),
                            notes,
                            has_unmappable: true, // the password placeholder
                        },
                    });
                    emitted_repos.insert(restic_secret_name.clone(), repo_ref.clone());
                    repo_ref
                }
            }
        };

        let translation =
            translate::translate_source(&name, &ns, &spec, &repo_ref).map_err(|what| {
                CliError::MigrationInput {
                    what,
                    fix: "only restic-mover ReplicationSources are translatable".into(),
                }
            })?;
        objects.extend(translation.objects.iter().cloned());
        policies_by_secret
            .entry(restic_secret_name)
            .or_default()
            .push(name.clone());
        translated.push(Translated {
            source: format!("replicationsource/{name}"),
            translation,
        });
    }

    // --- destinations (opt-in) ---
    if args.include_destinations {
        let dests_api = volsync_api(ctx, "ReplicationDestination");
        let listed = dests_api.list(&ListParams::default()).await.map_err(|e| {
            classify_kube(
                "list",
                "ReplicationDestination",
                "replicationdestinations",
                Some(&ns),
                None,
                e,
            )
        })?;
        for dest in listed.items {
            let name = dest.metadata.name.clone().unwrap_or_default();
            let spec: ReplicationDestinationSpec =
                serde_json::from_value(dest.data.get("spec").cloned().unwrap_or_default())
                    .map_err(|e| CliError::MigrationInput {
                        what: format!(
                            "ReplicationDestination {ns}/{name}: cannot decode spec: {e}"
                        ),
                        fix: "check the object".into(),
                    })?;
            // Pair with a source policy via the shared restic Secret.
            let dest_secret = spec
                .restic
                .as_ref()
                .and_then(|r| r.repository.clone())
                .unwrap_or_default();
            let policy = match policies_by_secret.get(&dest_secret).map(Vec::as_slice) {
                Some([only]) => only.clone(),
                _ => {
                    // Ambiguous/unknown: emit with a loud placeholder the user
                    // must edit (and --apply refuses).
                    "REPLACE_ME-policy".to_string()
                }
            };
            let mut translation = translate::translate_destination(&name, &ns, &spec, &policy)
                .map_err(|what| CliError::MigrationInput {
                    what,
                    fix: "only restic-mover ReplicationDestinations are translatable".into(),
                })?;
            if policy == "REPLACE_ME-policy" {
                translation.has_unmappable = true;
                translation.notes.push(FieldNote {
                    field: "spec.restic.repository".into(),
                    disposition: translate::Disposition::Unmappable {
                        reason: format!(
                            "could not pair Secret {dest_secret:?} with exactly one translated \
                             source policy; edit fromPolicy.name before applying"
                        ),
                    },
                });
            }
            objects.extend(translation.objects.iter().cloned());
            translated.push(Translated {
                source: format!("replicationdestination/{name}"),
                translation,
            });
        }
    }

    // --- report + emit ---
    eprintln!(
        "CONFIG TRANSLATION ONLY — no backup data is migrated; the kopiur repository starts \
         empty. Keep VolSync until kopiur's retention coverage suffices.\n"
    );
    eprint!("{}", render_notes(&translated));

    let any_unmappable = translated.iter().any(|t| t.translation.has_unmappable);
    if args.strict && any_unmappable {
        eprintln!("\n--strict: unmappable fields present (see above); not emitting");
        return Ok(CmdOutput {
            text: String::new(),
            exit: 1,
        });
    }

    if args.apply {
        if has_placeholder(&objects) {
            return Err(CliError::MigrationInput {
                what: "the translated manifests still contain REPLACE_ME placeholders \
                       (kopia password and/or an unpaired restore policy)"
                    .into(),
                fix: "run without --apply, edit the placeholders in the emitted YAML, then \
                      `kubectl apply -f` it yourself"
                    .into(),
            });
        }
        let params = PatchParams::apply(crate::consts::FIELD_MANAGER);
        for obj in &objects {
            let kind = obj["kind"].as_str().unwrap_or("?");
            let name = obj["metadata"]["name"].as_str().unwrap_or("?");
            let gvk = match obj["apiVersion"].as_str().unwrap_or_default() {
                "v1" => GroupVersionKind::gvk("", "v1", kind),
                _ => GroupVersionKind::gvk(kopiur_api::GROUP, kopiur_api::VERSION, kind),
            };
            let resource = ApiResource::from_gvk(&gvk);
            let api: Api<DynamicObject> = Api::namespaced_with(ctx.client.clone(), &ns, &resource);
            api.patch(name, &params, &Patch::Apply(obj.clone()))
                .await
                .map_err(|e| {
                    classify_kube(
                        "patch",
                        "translated object",
                        "objects",
                        Some(&ns),
                        Some(name),
                        e,
                    )
                })?;
            eprintln!("applied {kind}/{name}");
        }
    }

    Ok(CmdOutput::ok(render_yaml(&objects)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_scan_catches_password_and_policy_markers() {
        let clean = vec![serde_json::json!({ "kind": "Secret", "stringData": { "k": "v" } })];
        assert!(!has_placeholder(&clean));
        let with_password = vec![serde_json::json!({
            "stringData": { "KOPIA_PASSWORD": PASSWORD_PLACEHOLDER }
        })];
        assert!(has_placeholder(&with_password));
        let with_policy = vec![serde_json::json!({
            "spec": { "source": { "fromPolicy": { "name": "REPLACE_ME-policy" } } }
        })];
        assert!(has_placeholder(&with_policy));
    }

    #[test]
    fn banner_is_loud_about_config_only() {
        let yaml = render_yaml(&[serde_json::json!({ "kind": "SnapshotPolicy" })]).unwrap();
        assert!(yaml.starts_with("# ===="), "{yaml}");
        assert!(yaml.contains("CONFIG TRANSLATION ONLY"), "{yaml}");
        assert!(yaml.contains("NO backup data is"), "{yaml}");
        assert!(yaml.contains("---\nkind: SnapshotPolicy"), "{yaml}");
    }

    #[test]
    fn emit_repository_objects_builds_repo_and_secrets_with_placeholder_password() {
        let secret: Secret = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": { "name": "restic-cfg", "namespace": "media" },
            "data": {
                "RESTIC_REPOSITORY": base64("s3:https://minio.local:9000/bucket/app"),
                "RESTIC_PASSWORD": base64("old-restic-pass"),
                "AWS_ACCESS_KEY_ID": base64("AK"),
                "AWS_SECRET_ACCESS_KEY": base64("SK")
            }
        }))
        .unwrap();
        let (repo_ref, objects, notes) =
            emit_repository_objects("media", "restic-cfg", &secret).unwrap();
        assert_eq!(repo_ref["name"], "restic-cfg-kopiur");
        // creds secret carries AWS keys but NEVER the restic password.
        let creds = objects
            .iter()
            .find(|o| o["metadata"]["name"] == "restic-cfg-kopiur-creds")
            .expect("creds secret");
        assert_eq!(creds["stringData"]["AWS_ACCESS_KEY_ID"], "AK");
        assert!(creds["stringData"].get("RESTIC_PASSWORD").is_none());
        // password secret is an explicit placeholder.
        let password = objects
            .iter()
            .find(|o| o["metadata"]["name"] == "restic-cfg-kopiur-password")
            .expect("password secret");
        assert_eq!(
            password["stringData"]["KOPIA_PASSWORD"],
            PASSWORD_PLACEHOLDER
        );
        // repository points at the parsed backend.
        let repo = objects
            .iter()
            .find(|o| o["kind"] == "Repository")
            .expect("repository");
        assert_eq!(repo["spec"]["backend"]["s3"]["bucket"], "bucket");
        assert_eq!(repo["spec"]["backend"]["s3"]["prefix"], "app/");
        // …and the accounting flags the password as unmappable.
        assert!(notes.iter().any(|n| matches!(
            &n.disposition,
            translate::Disposition::Unmappable { reason } if reason.contains("REPLACE_ME")
        )));
        // The emitted Repository spec parses as the REAL kopiur type.
        let _typed: kopiur_api::RepositorySpec =
            serde_json::from_value(repo["spec"].clone()).expect("valid RepositorySpec");
    }

    fn base64(s: &str) -> String {
        use std::fmt::Write;
        // tiny local base64 (test-only; avoids a dev-dependency)
        const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let bytes = s.as_bytes();
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b = [
                chunk[0],
                chunk.get(1).copied().unwrap_or(0),
                chunk.get(2).copied().unwrap_or(0),
            ];
            let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
            let _ = write!(
                out,
                "{}{}",
                TABLE[(n >> 18) as usize & 63] as char,
                TABLE[(n >> 12) as usize & 63] as char
            );
            out.push(if chunk.len() > 1 {
                TABLE[(n >> 6) as usize & 63] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                TABLE[n as usize & 63] as char
            } else {
                '='
            });
        }
        out
    }
}
