//! Kopia identity resolution (ADR §4.2).
//!
//! Kopia records every snapshot under `username@hostname:sourcePath`. Kopiur makes
//! that identity an explicit, overridable part of the API rather than an accident
//! of `metadata.name`/`metadata.namespace` (ADR §2.2 principle 9). This module is
//! the single place the defaulting + templating rules live; the webhook calls it at
//! admission and pins the result into `status.resolved.identity`, which is **never
//! re-rendered** afterwards (ADR §4.2).
//!
//! ## Defaults (ADR §4.2)
//! - `username` ← `BackupConfig.metadata.name`
//! - `hostname` ← namespace
//! - `sourcePath` ← `/pvc/<pvcName>`
//!
//! ## ClusterRepository identity templates
//!
//! A [`crate::cluster_repository::IdentityTemplate`] supplies
//! `hostnameTemplate`/`usernameTemplate`, rendered with `tera` (Jinja2-compatible).
//! A consumer's explicit [`Identity`] override **always wins** over the template.
//!
//! ### Template syntax decision
//!
//! The ADR example is written Go-template-style with a leading dot
//! (`hostnameTemplate: "{{ .Namespace }}"`), but `tera` uses `{{ Namespace }}`.
//! Rather than force users to learn that `kopiur` templates are tera, we
//! **preprocess** the leading dot away: `{{ .Foo }}` → `{{ Foo }}` (see
//! `strip_leading_dots`). Both spellings therefore work, and the exact ADR
//! example renders correctly (proven in tests). Context variables exposed:
//! `Namespace` and `ConfigName`.

use crate::cluster_repository::IdentityTemplate;
use crate::common::{Identity, ResolvedIdentity};
use crate::error::{ValidationError, ValidationResult};
use tera::{Context, Tera};

/// Inputs to identity resolution. Grouped into a struct so call sites are readable
/// and future inputs (e.g. extra template vars) slot in without churning the
/// signature.
#[derive(Debug, Clone)]
pub struct IdentityInputs<'a> {
    /// The consumer object's `metadata.name` (default `username`).
    pub object_name: &'a str,
    /// The consumer object's namespace (default `hostname`, and the `Namespace`
    /// template var).
    pub namespace: &'a str,
    /// Explicit overrides from `BackupConfig.spec.identity`, if any.
    pub overrides: Option<&'a Identity>,
    /// `ClusterRepository.spec.identityDefaults`, if the consumer targets one.
    pub template: Option<&'a IdentityTemplate>,
    /// The PVC name backing `sourcePath`'s `/pvc/<name>` default. `None` for
    /// surfaces without a single PVC (a non-PVC source like NFS, or a maintenance
    /// identity). When set it takes precedence over [`Self::default_source_path`].
    pub pvc_name: Option<&'a str>,
    /// The `sourcePath` default for a non-PVC source (e.g. an NFS export's path),
    /// used when there is no `pvc_name` and no override. `None` leaves `sourcePath`
    /// unset (kopia's identity-only `username@hostname` form).
    pub default_source_path: Option<&'a str>,
    /// An explicit `sourcePathOverride` (ADR §3.3), which beats every default.
    pub source_path_override: Option<&'a str>,
}

/// Rewrite Go-template leading-dot variables (`{{ .Foo }}`, `{{.Foo}}`) into tera
/// syntax (`{{ Foo }}`). Only touches `.` immediately following `{{` and optional
/// whitespace, so it never disturbs method calls or literals elsewhere.
fn strip_leading_dots(tmpl: &str) -> String {
    let mut out = String::with_capacity(tmpl.len());
    let bytes = tmpl.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            out.push_str("{{");
            i += 2;
            // Skip whitespace after `{{`, preserving it in the output.
            while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
                out.push(bytes[i] as char);
                i += 1;
            }
            // Drop a single leading dot if present.
            if i < bytes.len() && bytes[i] == b'.' {
                i += 1;
            }
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn render(tmpl: &str, ctx: &Context) -> ValidationResult<String> {
    let prepared = strip_leading_dots(tmpl);
    Tera::one_off(&prepared, ctx, false).map_err(|e| ValidationError::IdentityTemplateRender {
        // tera errors nest the real cause one level down; surface it.
        reason: e
            .source()
            .map(|s| s.to_string())
            .unwrap_or_else(|| e.to_string()),
    })
}

// Local trait import for `.source()` on the tera error.
use std::error::Error as _;

/// Resolve a [`ResolvedIdentity`] from defaults, an optional `ClusterRepository`
/// identity template, and explicit consumer overrides (ADR §4.2).
///
/// Precedence per component: **explicit override > template > default**. Returns a
/// [`ValidationError::IdentityTemplateRender`] if a supplied template fails to
/// render (so the webhook rejects it at admission rather than pinning garbage).
///
/// ```
/// use kopiur_api::{IdentityInputs, resolve_identity, identity_string};
///
/// // Bare defaults: username <- object name, hostname <- namespace,
/// // sourcePath <- /pvc/<pvcName> (ADR §4.2).
/// let inputs = IdentityInputs {
///     object_name: "postgres-data",
///     namespace: "billing",
///     overrides: None,
///     template: None,
///     pvc_name: Some("postgres-data"),
///     default_source_path: None,
///     source_path_override: None,
/// };
/// let id = resolve_identity(&inputs).unwrap();
/// assert_eq!(id.username, "postgres-data");
/// assert_eq!(id.hostname, "billing");
/// assert_eq!(id.source_path.as_deref(), Some("/pvc/postgres-data"));
/// assert_eq!(identity_string(&id), "postgres-data@billing:/pvc/postgres-data");
/// ```
pub fn resolve_identity(inputs: &IdentityInputs<'_>) -> ValidationResult<ResolvedIdentity> {
    let mut ctx = Context::new();
    ctx.insert("Namespace", inputs.namespace);
    ctx.insert("ConfigName", inputs.object_name);

    let override_username = inputs.overrides.and_then(|o| o.username.as_deref());
    let override_hostname = inputs.overrides.and_then(|o| o.hostname.as_deref());

    let username = match override_username {
        Some(u) => u.to_string(),
        None => match inputs.template.and_then(|t| t.username_template.as_deref()) {
            Some(tmpl) => render(tmpl, &ctx)?,
            None => inputs.object_name.to_string(),
        },
    };

    let hostname = match override_hostname {
        Some(h) => h.to_string(),
        None => match inputs.template.and_then(|t| t.hostname_template.as_deref()) {
            Some(tmpl) => render(tmpl, &ctx)?,
            None => inputs.namespace.to_string(),
        },
    };

    let source_path = match inputs.source_path_override {
        Some(p) => Some(p.to_string()),
        None => inputs
            .pvc_name
            .map(|n| format!("/pvc/{n}"))
            .or_else(|| inputs.default_source_path.map(String::from)),
    };

    Ok(ResolvedIdentity {
        username,
        hostname,
        source_path,
    })
}

/// Format a kopia identity string. With a source path: `username@hostname:path`;
/// without one: `username@hostname` (kopia's identity-only form, used for catalog
/// queries that aren't pinned to a path).
///
/// ```
/// use kopiur_api::{IdentityInputs, resolve_identity, identity_string};
///
/// // No PVC => no sourcePath => kopia's identity-only `username@hostname` form.
/// let inputs = IdentityInputs {
///     object_name: "cfg",
///     namespace: "ns",
///     overrides: None,
///     template: None,
///     pvc_name: None,
///     default_source_path: None,
///     source_path_override: None,
/// };
/// let id = resolve_identity(&inputs).unwrap();
/// assert_eq!(id.source_path, None);
/// assert_eq!(identity_string(&id), "cfg@ns");
/// ```
pub fn identity_string(id: &ResolvedIdentity) -> String {
    match &id.source_path {
        Some(p) => format!("{}@{}:{}", id.username, id.hostname, p),
        None => format!("{}@{}", id.username, id.hostname),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs<'a>(
        name: &'a str,
        ns: &'a str,
        overrides: Option<&'a Identity>,
        template: Option<&'a IdentityTemplate>,
        pvc: Option<&'a str>,
    ) -> IdentityInputs<'a> {
        IdentityInputs {
            object_name: name,
            namespace: ns,
            overrides,
            template,
            pvc_name: pvc,
            default_source_path: None,
            source_path_override: None,
        }
    }

    #[test]
    fn nfs_source_uses_default_source_path() {
        // No PVC, but an NFS export supplies the sourcePath default.
        let r = resolve_identity(&IdentityInputs {
            object_name: "media",
            namespace: "default",
            overrides: None,
            template: None,
            pvc_name: None,
            default_source_path: Some("/mnt/eros/Media"),
            source_path_override: None,
        })
        .unwrap();
        assert_eq!(r.source_path.as_deref(), Some("/mnt/eros/Media"));
        assert_eq!(identity_string(&r), "media@default:/mnt/eros/Media");
    }

    #[test]
    fn override_beats_default_source_path() {
        let r = resolve_identity(&IdentityInputs {
            object_name: "media",
            namespace: "default",
            overrides: None,
            template: None,
            pvc_name: None,
            default_source_path: Some("/mnt/eros/Media"),
            source_path_override: Some("/data"),
        })
        .unwrap();
        assert_eq!(r.source_path.as_deref(), Some("/data"));
    }

    #[test]
    fn defaults_use_name_namespace_and_pvc_path() {
        let r = resolve_identity(&inputs(
            "postgres-data",
            "billing",
            None,
            None,
            Some("postgres-data"),
        ))
        .unwrap();
        assert_eq!(r.username, "postgres-data");
        assert_eq!(r.hostname, "billing");
        assert_eq!(r.source_path.as_deref(), Some("/pvc/postgres-data"));
    }

    #[test]
    fn adr_cluster_repository_template_example() {
        // ADR §3.2 example, verbatim Go-template dot syntax:
        //   hostnameTemplate: "{{ .Namespace }}"
        //   usernameTemplate: "{{ .Namespace }}-{{ .ConfigName }}"
        // For namespace `billing`, config `postgres-data`, must render to
        //   username = billing-postgres-data, hostname = billing.
        let tmpl = IdentityTemplate {
            hostname_template: Some("{{ .Namespace }}".to_string()),
            username_template: Some("{{ .Namespace }}-{{ .ConfigName }}".to_string()),
        };
        let r = resolve_identity(&inputs(
            "postgres-data",
            "billing",
            None,
            Some(&tmpl),
            Some("data"),
        ))
        .unwrap();
        assert_eq!(r.username, "billing-postgres-data");
        assert_eq!(r.hostname, "billing");
    }

    #[test]
    fn tera_native_syntax_also_works() {
        // Same render without the leading dot.
        let tmpl = IdentityTemplate {
            hostname_template: Some("{{ Namespace }}".to_string()),
            username_template: Some("{{ Namespace }}-{{ ConfigName }}".to_string()),
        };
        let r = resolve_identity(&inputs(
            "postgres-data",
            "billing",
            None,
            Some(&tmpl),
            Some("data"),
        ))
        .unwrap();
        assert_eq!(r.username, "billing-postgres-data");
        assert_eq!(r.hostname, "billing");
    }

    #[test]
    fn override_beats_template() {
        let tmpl = IdentityTemplate {
            hostname_template: Some("{{ .Namespace }}".to_string()),
            username_template: Some("{{ .Namespace }}-{{ .ConfigName }}".to_string()),
        };
        let ovr = Identity {
            username: Some("custom-user".to_string()),
            hostname: Some("custom-host".to_string()),
        };
        let r = resolve_identity(&inputs("cfg", "ns", Some(&ovr), Some(&tmpl), Some("p"))).unwrap();
        assert_eq!(r.username, "custom-user");
        assert_eq!(r.hostname, "custom-host");
    }

    #[test]
    fn partial_override_falls_through_to_template_for_the_other_field() {
        let tmpl = IdentityTemplate {
            hostname_template: Some("{{ .Namespace }}".to_string()),
            username_template: Some("{{ .Namespace }}-{{ .ConfigName }}".to_string()),
        };
        // Only hostname overridden; username still comes from the template.
        let ovr = Identity {
            username: None,
            hostname: Some("pinned-host".to_string()),
        };
        let r = resolve_identity(&inputs(
            "postgres-data",
            "billing",
            Some(&ovr),
            Some(&tmpl),
            Some("d"),
        ))
        .unwrap();
        assert_eq!(r.hostname, "pinned-host");
        assert_eq!(r.username, "billing-postgres-data");
    }

    #[test]
    fn source_path_override_beats_default() {
        let mut i = inputs("cfg", "ns", None, None, Some("vol"));
        i.source_path_override = Some("/data");
        let r = resolve_identity(&i).unwrap();
        assert_eq!(r.source_path.as_deref(), Some("/data"));
    }

    #[test]
    fn no_pvc_yields_no_source_path() {
        let r = resolve_identity(&inputs("cfg", "ns", None, None, None)).unwrap();
        assert_eq!(r.source_path, None);
    }

    #[test]
    fn identity_string_with_and_without_path() {
        let with = ResolvedIdentity {
            username: "postgres-data".into(),
            hostname: "billing".into(),
            source_path: Some("/data".into()),
        };
        assert_eq!(identity_string(&with), "postgres-data@billing:/data");
        let without = ResolvedIdentity {
            source_path: None,
            ..with
        };
        assert_eq!(identity_string(&without), "postgres-data@billing");
    }

    #[test]
    fn malformed_template_is_rejected() {
        let tmpl = IdentityTemplate {
            hostname_template: Some("{{ .Namespace ".to_string()), // unterminated
            username_template: None,
        };
        let err = resolve_identity(&inputs("c", "n", None, Some(&tmpl), Some("p"))).unwrap_err();
        assert!(matches!(
            err,
            ValidationError::IdentityTemplateRender { .. }
        ));
    }
}
