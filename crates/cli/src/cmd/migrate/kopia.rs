//! Pure fork-kopia → kopiur translation (`perfectra1n/volsync`'s kopia mover).
//!
//! Unlike restic, the fork's repository **IS a kopia repository**: translation
//! adopts it in place — same backend, same `KOPIA_PASSWORD` (referenced from
//! the existing Secret, never copied), and every existing snapshot preserved.
//! The one property that makes adoption seamless is **identity continuity**:
//! the fork records snapshots as `<sanitized-name>@<sanitized-namespace>:/data`
//! (or explicit overrides), so every translated `SnapshotPolicy` pins
//! `spec.identity` + `sources[0].sourcePathOverride` to exactly that identity.
//! Omitting the pin would silently fork the snapshot history.
//!
//! Same accounting contract as [`super::translate`]: every fork field the
//! translator reads lands in an explicit [`Disposition`] — nothing is silently
//! dropped.

use std::collections::BTreeMap;

use super::translate::{Disposition, FieldNote, Translation};
use super::volsync_types::{MoverVolume, ReplicationDestinationSpec, ReplicationSourceSpec};

/// The fork's fallback identity when sanitization leaves nothing
/// (`builder.go` `defaultUsername`).
pub const FORK_DEFAULT_IDENTITY: &str = "volsync-default";

/// The fork's username length cap (`builder.go` `maxUsernameLength`).
const FORK_MAX_USERNAME_LEN: usize = 50;

/// The path the fork's mover snapshots when no `sourcePathOverride` is set
/// (its PVC mount point).
pub const FORK_DEFAULT_SOURCE_PATH: &str = "/data";

/// Port of the fork's `internal/controller/mover/kopia/builder.go::sanitizeForIdentifier`.
/// Keeps `[a-zA-Z0-9-]`; `_` is kept when `allow_underscore` else mapped to `-`;
/// `.` is kept only when `allow_dots` (otherwise DROPPED); everything else is
/// dropped. Then trims leading/trailing `-` (+`_` when allowed, +`.` when
/// allowed). **Bug-for-bug parity is required**: the emitted identity must equal
/// what the fork wrote into the kopia repository, or snapshot history forks.
fn sanitize_for_identifier(input: &str, allow_underscore: bool, allow_dots: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' => out.push(c),
            '_' if allow_underscore => out.push(c),
            '_' => out.push('-'),
            '.' if allow_dots => out.push(c),
            _ => {}
        }
    }
    let trim: &[char] = match (allow_underscore, allow_dots) {
        (true, true) => &['-', '_', '.'],
        (true, false) => &['-', '_'],
        (false, true) => &['-', '.'],
        (false, false) => &['-'],
    };
    out.trim_matches(trim).to_string()
}

/// The fork's default kopia **username** for a ReplicationSource: its
/// `metadata.name` through `sanitizeForIdentifier(name, true, false)` (keeps
/// `_`, drops `.`), trimmed, then truncated to 50 bytes — truncation happens
/// AFTER trimming, with **no re-trim**, so a truncated result may end in `-`/`_`
/// (parity with `builder.go::generateUsername`). Empty → `volsync-default`.
pub fn sanitize_username(object_name: &str) -> String {
    let valid = sanitize_for_identifier(object_name, true, false);
    if valid.is_empty() {
        return FORK_DEFAULT_IDENTITY.to_string();
    }
    if valid.len() > FORK_MAX_USERNAME_LEN {
        return valid[..FORK_MAX_USERNAME_LEN].to_string();
    }
    valid
}

/// The fork's default kopia **hostname**: the namespace through
/// `sanitizeForIdentifier(ns, false, true)` (`_`→`-`, dots KEPT), with NO length
/// cap; empty falls back to the sanitized object name, then `volsync-default`
/// (parity with `builder.go::generateHostname`/`sanitizeForHostname`).
pub fn sanitize_hostname(namespace: &str, fallback_name: &str) -> String {
    let ns = sanitize_for_identifier(namespace, false, true);
    if !ns.is_empty() {
        return ns;
    }
    let name = sanitize_for_identifier(fallback_name, false, true);
    if !name.is_empty() {
        return name;
    }
    FORK_DEFAULT_IDENTITY.to_string()
}

/// Resolve the kopia identity the fork recorded for a ReplicationSource:
/// explicit overrides are used **verbatim** (the fork applies no sanitization
/// to them); defaults are the sanitized name/namespace.
pub fn fork_identity(
    rs_name: &str,
    namespace: &str,
    username_override: Option<&str>,
    hostname_override: Option<&str>,
) -> (String, String) {
    let username = match username_override {
        Some(u) if !u.is_empty() => u.to_string(),
        _ => sanitize_username(rs_name),
    };
    let hostname = match hostname_override {
        Some(h) if !h.is_empty() => h.to_string(),
        _ => sanitize_hostname(namespace, rs_name),
    };
    (username, hostname)
}

/// Which backend a fork `KOPIA_REPOSITORY` URL maps to. Closed enum — the
/// credential planning `match`es it exhaustively. (`gdrive://` is an error,
/// not a variant: kopiur has no Google Drive backend.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KopiaScheme {
    /// `s3://bucket[/prefix]`
    S3,
    /// `gcs://bucket[/path]`
    Gcs,
    /// `azure://container[/path]`
    Azure,
    /// `b2://bucket[/path]`
    B2,
    /// `filesystem:///path`
    Filesystem,
    /// `sftp://[user@]host[:port]/path`
    Sftp,
    /// `webdav://host/path`
    WebDav,
    /// `rclone://remote:/path`
    Rclone,
}

/// One credential-key rename from the fork's Secret into the derived
/// `-kopiur-creds` Secret (the kopiur mover reads kopia's env names, which
/// differ from the fork's for B2/Azure/WebDAV/SFTP).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredRename {
    /// The fork Secret key to read.
    pub from: &'static str,
    /// The kopiur-mover env key to write.
    pub to: &'static str,
}

/// The rename plan per scheme, given the keys actually present in the Secret.
/// Empty means the existing Secret is referenced **in place** (its key names
/// already match the kopiur mover's env contract).
pub fn rename_plan(scheme: KopiaScheme, data: &BTreeMap<String, String>) -> Vec<CredRename> {
    let present = |k: &str| data.contains_key(k);
    match scheme {
        // AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN are
        // exactly what the kopiur mover reads.
        KopiaScheme::S3 => vec![],
        // GCS needs JSON CONTENT under KOPIA_GCS_CREDENTIALS — the fork's
        // GOOGLE_APPLICATION_CREDENTIALS is a file PATH; nothing renames.
        KopiaScheme::Gcs => vec![],
        // The kopiur mover reads AZURE_STORAGE_KEY / AZURE_STORAGE_SAS_TOKEN.
        // When the fork Secret already carries those, reference in place;
        // otherwise rename the legacy AZURE_ACCOUNT_* forms.
        KopiaScheme::Azure => {
            let mut plan = Vec::new();
            if !present("AZURE_STORAGE_KEY") && present("AZURE_ACCOUNT_KEY") {
                plan.push(CredRename {
                    from: "AZURE_ACCOUNT_KEY",
                    to: "AZURE_STORAGE_KEY",
                });
            }
            if !present("AZURE_STORAGE_SAS_TOKEN") && present("AZURE_ACCOUNT_SAS") {
                plan.push(CredRename {
                    from: "AZURE_ACCOUNT_SAS",
                    to: "AZURE_STORAGE_SAS_TOKEN",
                });
            }
            plan
        }
        // kopia's B2 env names differ from the fork's restic-legacy names.
        KopiaScheme::B2 => {
            let mut plan = Vec::new();
            if present("B2_ACCOUNT_ID") {
                plan.push(CredRename {
                    from: "B2_ACCOUNT_ID",
                    to: "B2_KEY_ID",
                });
            }
            if present("B2_APPLICATION_KEY") {
                plan.push(CredRename {
                    from: "B2_APPLICATION_KEY",
                    to: "B2_KEY",
                });
            }
            plan
        }
        KopiaScheme::Filesystem => vec![],
        // The kopiur mover materializes SFTP key/known-hosts from
        // KOPIA_SFTP_KEY_DATA / KOPIA_SFTP_KNOWN_HOSTS (content, not paths).
        KopiaScheme::Sftp => {
            let mut plan = Vec::new();
            if present("SFTP_KNOWN_HOSTS_DATA") {
                plan.push(CredRename {
                    from: "SFTP_KNOWN_HOSTS_DATA",
                    to: "KOPIA_SFTP_KNOWN_HOSTS",
                });
            }
            plan
        }
        KopiaScheme::WebDav => {
            let mut plan = Vec::new();
            if present("WEBDAV_USERNAME") {
                plan.push(CredRename {
                    from: "WEBDAV_USERNAME",
                    to: "KOPIA_WEBDAV_USERNAME",
                });
            }
            if present("WEBDAV_PASSWORD") {
                plan.push(CredRename {
                    from: "WEBDAV_PASSWORD",
                    to: "KOPIA_WEBDAV_PASSWORD",
                });
            }
            plan
        }
        KopiaScheme::Rclone => vec![], // RCLONE_CONFIG handled separately (content sniff)
    }
}

/// The backend translated from a fork repository Secret.
#[derive(Debug)]
pub struct KopiaBackendPlan {
    /// The kopiur `spec.backend` JSON (externally-tagged).
    pub backend: serde_json::Value,
    /// Which scheme the URL selected.
    pub scheme: KopiaScheme,
    /// A derived rename-Secret to emit (`name`, `stringData`), only when key
    /// names had to change; `None` means the fork Secret serves as-is.
    pub derived_creds: Option<(String, BTreeMap<String, String>)>,
    /// Per-key accounting for the Secret's backend keys.
    pub notes: Vec<FieldNote>,
}

/// Inputs to [`backend_from_kopia_repository`], grouped for readability.
#[derive(Debug)]
pub struct KopiaBackendInput<'a> {
    /// The Secret's `KOPIA_REPOSITORY` value.
    pub url: &'a str,
    /// The fork Secret's name (referenced in place where possible).
    pub secret_name: &'a str,
    /// The kopiur Repository name being emitted (for accounting lines).
    pub repo_name: &'a str,
    /// The decoded Secret data.
    pub data: &'a BTreeMap<String, String>,
    /// The source's `moverVolumes` (a PVC here is how the fork reaches a
    /// `filesystem://` repository).
    pub mover_volumes: Option<&'a [MoverVolume]>,
}

/// Push a `Mapped` accounting note (free fn, not a closure, so call sites can
/// also push to `notes` directly without fighting the borrow checker).
fn note_mapped(notes: &mut Vec<FieldNote>, field: String, to: String) {
    notes.push(FieldNote {
        field,
        disposition: Disposition::Mapped { to },
    });
}

/// The fork ignores the URL's path portion for gcs/azure/b2 — record that
/// loudly so a user who *believed* the path mattered learns the repository
/// actually lives at the bucket/container root.
fn note_ignored_url_path(notes: &mut Vec<FieldNote>, url: &str, path: &str, backend: &str) {
    notes.push(FieldNote {
        field: format!("KOPIA_REPOSITORY ({url})"),
        disposition: Disposition::Ignored {
            reason: format!(
                "the fork's entry.sh passes NO --prefix for {backend} — the URL path {path:?} \
                 was always ignored and the repository lives at the root; kopiur adopts it there"
            ),
        },
    });
}

/// "true"/"1"/"yes", case-insensitive — how the fork's entry.sh reads its
/// boolean-ish env values.
fn truthy(v: &str) -> bool {
    matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes")
}

/// kopia prefixes are literal strings; a missing trailing slash concatenates
/// the prefix with object names. The fork's `entry.sh` appends one for S3
/// ("Added trailing slash to S3 prefix for proper directory separation") —
/// this MUST match it, or adoption points at the wrong blobs.
fn prefixify(p: &str) -> Option<String> {
    let trimmed = p.trim_matches('/');
    (!trimmed.is_empty()).then(|| format!("{trimmed}/"))
}

/// Split `bucket[/path…]` into (bucket, prefix). Only S3 USES the prefix —
/// see [`split_bucket_only`] for the backends whose URL path the fork ignores.
fn split_bucket_path(rest: &str) -> (String, Option<String>) {
    match rest.split_once('/') {
        Some((b, p)) => (b.to_string(), prefixify(p)),
        None => (rest.trim_end_matches('/').to_string(), None),
    }
}

/// gcs/azure/b2 bucket extraction. PARITY: the fork's `entry.sh` passes NO
/// `--prefix` for these backends — the URL's path portion is silently IGNORED
/// and the repository lives at the bucket/container ROOT. Carrying the path
/// into a kopiur prefix would adopt a location the fork never wrote to, so it
/// is dropped here with an explicit accounting note from the caller.
fn split_bucket_only(rest: &str) -> (String, Option<String>) {
    match rest.split_once('/') {
        Some((b, p)) => {
            let ignored = p.trim_matches('/');
            (
                b.to_string(),
                (!ignored.is_empty()).then(|| ignored.to_string()),
            )
        }
        None => (rest.trim_end_matches('/').to_string(), None),
    }
}

/// Parse a fork `KOPIA_REPOSITORY` URL (+ the Secret's backend env keys) into a
/// kopiur Backend plan. The fork's URL forms are kopia-native (`s3://bucket`),
/// NOT restic's (`s3:endpoint/bucket`) — see the fork's `mover-kopia/entry.sh`.
/// Secret keys can override URL parts (notably `KOPIA_S3_BUCKET` beats the URL
/// bucket); each consumed key gets an accounting note.
#[allow(clippy::too_many_lines)] // one arm per scheme; splitting hides the symmetry
pub fn backend_from_kopia_repository(
    input: &KopiaBackendInput<'_>,
) -> Result<KopiaBackendPlan, String> {
    let KopiaBackendInput {
        url,
        secret_name,
        repo_name,
        data,
        mover_volumes,
    } = input;
    let get = |key: &str| data.get(key).map(String::as_str);
    let mut notes: Vec<FieldNote> = Vec::new();
    let derived_name = format!("{secret_name}-kopiur-creds");
    let in_place_auth = serde_json::json!({ "secretRef": { "name": secret_name } });
    let derived_auth = serde_json::json!({ "secretRef": { "name": derived_name } });

    let (backend, scheme, mut derived) = if let Some(rest) = url.strip_prefix("s3://") {
        let (url_bucket, prefix) = split_bucket_path(rest);
        // KOPIA_S3_BUCKET beats the URL bucket (fork entry.sh precedence).
        let bucket = match get("KOPIA_S3_BUCKET") {
            Some(b) if !b.is_empty() => {
                note_mapped(
                    &mut notes,
                    format!("secret/{secret_name}.KOPIA_S3_BUCKET"),
                    format!(
                        "Repository/{repo_name}.spec.backend.s3.bucket (overrides URL bucket \
                         {url_bucket:?}, matching the fork's precedence)"
                    ),
                );
                b.to_string()
            }
            _ => url_bucket,
        };
        if bucket.is_empty() {
            return Err(format!(
                "KOPIA_REPOSITORY {url:?}: no bucket in the URL or KOPIA_S3_BUCKET key"
            ));
        }
        let mut s3 = serde_json::json!({ "bucket": bucket, "auth": in_place_auth });
        if let Some(p) = prefix {
            s3["prefix"] = serde_json::json!(p);
        }
        let mut disable_tls = false;
        let endpoint_key = ["KOPIA_S3_ENDPOINT", "AWS_S3_ENDPOINT"]
            .into_iter()
            .find(|k| get(k).is_some_and(|v| !v.is_empty()));
        if let Some(key) = endpoint_key {
            let raw = get(key).expect("checked");
            let host = if let Some(h) = raw.strip_prefix("http://") {
                disable_tls = true;
                h
            } else {
                raw.strip_prefix("https://").unwrap_or(raw)
            };
            s3["endpoint"] = serde_json::json!(host.trim_end_matches('/'));
            note_mapped(
                &mut notes,
                format!("secret/{secret_name}.{key}"),
                format!("Repository/{repo_name}.spec.backend.s3.endpoint"),
            );
        }
        for key in ["KOPIA_S3_DISABLE_TLS", "AWS_S3_DISABLE_TLS"] {
            if get(key).is_some_and(truthy) {
                disable_tls = true;
                note_mapped(
                    &mut notes,
                    format!("secret/{secret_name}.{key}"),
                    format!("Repository/{repo_name}.spec.backend.s3.tls.disableTls"),
                );
            }
        }
        if disable_tls {
            s3["tls"] = serde_json::json!({ "disableTls": true });
        }
        if let Some(key) = ["AWS_REGION", "AWS_DEFAULT_REGION"]
            .into_iter()
            .find(|k| get(k).is_some_and(|v| !v.is_empty()))
        {
            s3["region"] = serde_json::json!(get(key).expect("checked"));
            note_mapped(
                &mut notes,
                format!("secret/{secret_name}.{key}"),
                format!("Repository/{repo_name}.spec.backend.s3.region"),
            );
        }
        for key in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
        ] {
            if get(key).is_some() {
                note_mapped(
                    &mut notes,
                    format!("secret/{secret_name}.{key}"),
                    format!(
                        "referenced IN PLACE by Repository/{repo_name}.spec.backend.s3.auth.secretRef \
                         (kopia reads the same env name)"
                    ),
                );
            }
        }
        (serde_json::json!({ "s3": s3 }), KopiaScheme::S3, None)
    } else if let Some(rest) = url.strip_prefix("gcs://") {
        let (url_bucket, ignored_path) = split_bucket_only(rest);
        let bucket = match ["KOPIA_GCS_BUCKET", "GCS_BUCKET"]
            .into_iter()
            .find(|k| get(k).is_some_and(|v| !v.is_empty()))
        {
            Some(key) => {
                note_mapped(
                    &mut notes,
                    format!("secret/{secret_name}.{key}"),
                    format!(
                        "Repository/{repo_name}.spec.backend.gcs.bucket (overrides URL bucket)"
                    ),
                );
                get(key).expect("checked").to_string()
            }
            None => url_bucket,
        };
        if bucket.is_empty() {
            return Err(format!(
                "KOPIA_REPOSITORY {url:?}: no bucket in the URL or KOPIA_GCS_BUCKET key"
            ));
        }
        if let Some(path) = ignored_path {
            note_ignored_url_path(&mut notes, url, &path, "gcs");
        }
        let gcs = serde_json::json!({ "bucket": bucket, "auth": derived_auth });
        notes.push(FieldNote {
            field: format!("secret/{secret_name}.GOOGLE_APPLICATION_CREDENTIALS"),
            disposition: Disposition::Unmappable {
                reason: format!(
                    "the fork stores a file PATH inside its mover pod; kopiur needs the \
                     service-account JSON CONTENT under {derived_name}.KOPIA_GCS_CREDENTIALS — \
                     add it yourself before applying"
                ),
            },
        });
        (serde_json::json!({ "gcs": gcs }), KopiaScheme::Gcs, None)
    } else if let Some(rest) = url.strip_prefix("azure://") {
        let (url_container, ignored_path) = split_bucket_only(rest);
        let container = match get("KOPIA_AZURE_CONTAINER") {
            Some(c) if !c.is_empty() => {
                note_mapped(
                    &mut notes,
                    format!("secret/{secret_name}.KOPIA_AZURE_CONTAINER"),
                    format!(
                        "Repository/{repo_name}.spec.backend.azure.container (overrides URL \
                         container)"
                    ),
                );
                c.to_string()
            }
            _ => url_container,
        };
        if container.is_empty() {
            return Err(format!(
                "KOPIA_REPOSITORY {url:?}: no container in the URL or KOPIA_AZURE_CONTAINER key"
            ));
        }
        if let Some(path) = ignored_path {
            note_ignored_url_path(&mut notes, url, &path, "azure");
        }
        let plan = rename_plan(KopiaScheme::Azure, data);
        let auth = if plan.is_empty() {
            for key in ["AZURE_STORAGE_KEY", "AZURE_STORAGE_SAS_TOKEN"] {
                if get(key).is_some() {
                    note_mapped(
                        &mut notes,
                        format!("secret/{secret_name}.{key}"),
                        format!(
                            "referenced IN PLACE by \
                             Repository/{repo_name}.spec.backend.azure.auth.secretRef"
                        ),
                    );
                }
            }
            in_place_auth.clone()
        } else {
            derived_auth.clone()
        };
        let mut azure = serde_json::json!({ "container": container, "auth": auth });
        if let Some(key) = ["AZURE_ACCOUNT_NAME", "AZURE_STORAGE_ACCOUNT"]
            .into_iter()
            .find(|k| get(k).is_some_and(|v| !v.is_empty()))
        {
            // kopia takes the account NAME as a backend field, not env.
            azure["storageAccount"] = serde_json::json!(get(key).expect("checked"));
            note_mapped(
                &mut notes,
                format!("secret/{secret_name}.{key}"),
                format!("Repository/{repo_name}.spec.backend.azure.storageAccount"),
            );
        }
        let derived = build_derived(&plan, &derived_name, secret_name, data, &mut notes);
        (
            serde_json::json!({ "azure": azure }),
            KopiaScheme::Azure,
            derived,
        )
    } else if let Some(rest) = url.strip_prefix("b2://") {
        let (url_bucket, ignored_path) = split_bucket_only(rest);
        let bucket = match get("KOPIA_B2_BUCKET") {
            Some(b) if !b.is_empty() => {
                note_mapped(
                    &mut notes,
                    format!("secret/{secret_name}.KOPIA_B2_BUCKET"),
                    format!("Repository/{repo_name}.spec.backend.b2.bucket (overrides URL bucket)"),
                );
                b.to_string()
            }
            _ => url_bucket,
        };
        if bucket.is_empty() {
            return Err(format!(
                "KOPIA_REPOSITORY {url:?}: no bucket in the URL or KOPIA_B2_BUCKET key"
            ));
        }
        if let Some(path) = ignored_path {
            note_ignored_url_path(&mut notes, url, &path, "b2");
        }
        let plan = rename_plan(KopiaScheme::B2, data);
        let auth = if plan.is_empty() {
            in_place_auth.clone()
        } else {
            derived_auth.clone()
        };
        let b2 = serde_json::json!({ "bucket": bucket, "auth": auth });
        let derived = build_derived(&plan, &derived_name, secret_name, data, &mut notes);
        (serde_json::json!({ "b2": b2 }), KopiaScheme::B2, derived)
    } else if let Some(rest) = url.strip_prefix("filesystem://") {
        // filesystem:///path → rest = "/path".
        if !rest.starts_with('/') {
            return Err(format!(
                "KOPIA_REPOSITORY {url:?}: filesystem URLs must carry an absolute path \
                 (filesystem:///path)"
            ));
        }
        let mut fs = serde_json::json!({ "path": rest });
        // The fork reaches a filesystem repo through a moverVolumes PVC mounted
        // at /mnt/<mountPath>; kopiur mounts the PVC via backend.filesystem.volume.
        let pvcs: Vec<&str> = mover_volumes
            .map(|vols| {
                vols.iter()
                    .filter_map(|v| {
                        v.volume_source
                            .as_ref()?
                            .persistent_volume_claim
                            .as_ref()?
                            .claim_name
                            .as_deref()
                    })
                    .collect()
            })
            .unwrap_or_default();
        match pvcs.as_slice() {
            [claim] => {
                fs["volume"] = serde_json::json!({ "pvc": { "name": claim } });
                note_mapped(
                    &mut notes,
                    "spec.kopia.moverVolumes[pvc]".to_string(),
                    format!(
                        "Repository/{repo_name}.spec.backend.filesystem.volume.pvc.name \
                         (the PVC backing the repository path)"
                    ),
                );
            }
            _ => {
                fs["volume"] = serde_json::json!({ "pvc": { "name": "REPLACE_ME-repo-pvc" } });
                notes.push(FieldNote {
                    field: "spec.kopia.moverVolumes".to_string(),
                    disposition: Disposition::Unmappable {
                        reason: format!(
                            "could not infer exactly one repository PVC from moverVolumes \
                             (found {}); set backend.filesystem.volume.pvc.name before applying",
                            pvcs.len()
                        ),
                    },
                });
            }
        }
        (
            serde_json::json!({ "filesystem": fs }),
            KopiaScheme::Filesystem,
            None,
        )
    } else if let Some(rest) = url.strip_prefix("sftp://") {
        // sftp://[user@]host[:port]/path — Secret SFTP_* keys override URL parts.
        let (authority, url_path) = match rest.split_once('/') {
            Some((a, p)) => (a, format!("/{p}")),
            None => (rest, String::new()),
        };
        let (url_user, hostport) = match authority.split_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, authority),
        };
        let (url_host, url_port) = match hostport.rsplit_once(':') {
            Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
                let port = p.parse::<u16>().map_err(|_| {
                    format!("KOPIA_REPOSITORY {url:?}: SFTP port {p:?} is not a valid TCP port")
                })?;
                (h, Some(port))
            }
            _ => (hostport, None),
        };
        let pick = |key: &str, fallback: Option<&str>| -> Option<(String, bool)> {
            match get(key) {
                Some(v) if !v.is_empty() => Some((v.to_string(), true)),
                _ => fallback.map(|f| (f.to_string(), false)),
            }
        };
        let Some((host, host_from_secret)) =
            pick("SFTP_HOST", Some(url_host).filter(|h| !h.is_empty()))
        else {
            return Err(format!(
                "KOPIA_REPOSITORY {url:?}: no SFTP host in URL or SFTP_HOST key"
            ));
        };
        if host_from_secret {
            note_mapped(
                &mut notes,
                format!("secret/{secret_name}.SFTP_HOST"),
                format!("Repository/{repo_name}.spec.backend.sftp.host"),
            );
        }
        let Some((path, path_from_secret)) = pick(
            "SFTP_PATH",
            Some(url_path.as_str()).filter(|p| !p.is_empty()),
        ) else {
            return Err(format!(
                "KOPIA_REPOSITORY {url:?}: no SFTP path in URL or SFTP_PATH key"
            ));
        };
        if path_from_secret {
            note_mapped(
                &mut notes,
                format!("secret/{secret_name}.SFTP_PATH"),
                format!("Repository/{repo_name}.spec.backend.sftp.path"),
            );
        }
        let mut sftp = serde_json::json!({ "host": host, "path": path, "auth": derived_auth });
        if let Some((port, from_secret)) = pick("SFTP_PORT", None)
            .map(|(v, s)| (v.parse::<u16>().ok(), s))
            .and_then(|(p, s)| p.map(|p| (p, s)))
            .or(url_port.map(|p| (p, false)))
        {
            sftp["port"] = serde_json::json!(port);
            if from_secret {
                note_mapped(
                    &mut notes,
                    format!("secret/{secret_name}.SFTP_PORT"),
                    format!("Repository/{repo_name}.spec.backend.sftp.port"),
                );
            }
        }
        if let Some((user, from_secret)) = pick("SFTP_USERNAME", url_user) {
            sftp["username"] = serde_json::json!(user);
            if from_secret {
                note_mapped(
                    &mut notes,
                    format!("secret/{secret_name}.SFTP_USERNAME"),
                    format!("Repository/{repo_name}.spec.backend.sftp.username"),
                );
            }
        }
        for (key, why) in [
            (
                "SFTP_KEY_FILE",
                format!(
                    "a file PATH inside the fork's mover; kopiur needs the private-key CONTENT \
                     under {derived_name}.KOPIA_SFTP_KEY_DATA — add it yourself before applying"
                ),
            ),
            (
                "SFTP_KNOWN_HOSTS",
                format!(
                    "a file PATH; put the known_hosts CONTENT under \
                     {derived_name}.KOPIA_SFTP_KNOWN_HOSTS instead"
                ),
            ),
            (
                "SFTP_PASSWORD",
                "kopiur's SFTP backend is key-based (no password auth); switch the server to an \
                 SSH key and put it under KOPIA_SFTP_KEY_DATA"
                    .to_string(),
            ),
        ] {
            if get(key).is_some() {
                notes.push(FieldNote {
                    field: format!("secret/{secret_name}.{key}"),
                    disposition: Disposition::Unmappable { reason: why },
                });
            }
        }
        let plan = rename_plan(KopiaScheme::Sftp, data);
        let derived = build_derived(&plan, &derived_name, secret_name, data, &mut notes);
        // SFTP always points auth at the derived Secret (key data is content-
        // based there); emit it even when only KOPIA_SFTP_KEY_DATA remains for
        // the user to fill in. An empty derived Secret is still emitted so the
        // reference resolves.
        let derived = derived.or_else(|| Some((derived_name.clone(), BTreeMap::new())));
        (
            serde_json::json!({ "sftp": sftp }),
            KopiaScheme::Sftp,
            derived,
        )
    } else if let Some(rest) = url.strip_prefix("webdav://") {
        let (webdav_url, from_secret) = match get("WEBDAV_URL") {
            Some(u) if !u.is_empty() => (u.to_string(), true),
            _ => (format!("https://{rest}"), false),
        };
        if from_secret {
            note_mapped(
                &mut notes,
                format!("secret/{secret_name}.WEBDAV_URL"),
                format!("Repository/{repo_name}.spec.backend.webDav.url"),
            );
        } else {
            note_mapped(
                &mut notes,
                format!("KOPIA_REPOSITORY ({url})"),
                format!(
                    "Repository/{repo_name}.spec.backend.webDav.url (assumed https://; edit if \
                     the server is plain HTTP)"
                ),
            );
        }
        let plan = rename_plan(KopiaScheme::WebDav, data);
        let auth = if plan.is_empty() {
            in_place_auth.clone()
        } else {
            derived_auth.clone()
        };
        let webdav = serde_json::json!({ "url": webdav_url, "auth": auth });
        let derived = build_derived(&plan, &derived_name, secret_name, data, &mut notes);
        (
            serde_json::json!({ "webDav": webdav }),
            KopiaScheme::WebDav,
            derived,
        )
    } else if let Some(rest) = url.strip_prefix("rclone://") {
        let remote_path = match get("RCLONE_REMOTE_PATH") {
            Some(p) if !p.is_empty() => {
                note_mapped(
                    &mut notes,
                    format!("secret/{secret_name}.RCLONE_REMOTE_PATH"),
                    format!("Repository/{repo_name}.spec.backend.rclone.remotePath"),
                );
                p.to_string()
            }
            _ => rest.to_string(),
        };
        let mut rclone = serde_json::json!({ "remotePath": remote_path });
        let mut derived_map = BTreeMap::new();
        match get("RCLONE_CONFIG") {
            // An rclone.conf has ini section headers; a path does not.
            Some(cfg) if cfg.contains('[') => {
                derived_map.insert("KOPIA_RCLONE_CONFIG".to_string(), cfg.to_string());
                rclone["configSecretRef"] = serde_json::json!({ "name": derived_name });
                note_mapped(
                    &mut notes,
                    format!("secret/{secret_name}.RCLONE_CONFIG"),
                    format!(
                        "Secret/{derived_name}.KOPIA_RCLONE_CONFIG (+ \
                         Repository/{repo_name}.spec.backend.rclone.configSecretRef)"
                    ),
                );
            }
            Some(_) => {
                notes.push(FieldNote {
                    field: format!("secret/{secret_name}.RCLONE_CONFIG"),
                    disposition: Disposition::Unmappable {
                        reason: format!(
                            "looks like a file PATH, not rclone.conf content; put the config \
                             CONTENT under {derived_name}.KOPIA_RCLONE_CONFIG and set \
                             backend.rclone.configSecretRef"
                        ),
                    },
                });
            }
            None => {}
        }
        if get("RCLONE_EXE").is_some() {
            notes.push(FieldNote {
                field: format!("secret/{secret_name}.RCLONE_EXE"),
                disposition: Disposition::Ignored {
                    reason: "the kopiur mover image ships its own rclone binary".to_string(),
                },
            });
        }
        let derived = (!derived_map.is_empty()).then(|| (derived_name.clone(), derived_map));
        (
            serde_json::json!({ "rclone": rclone }),
            KopiaScheme::Rclone,
            derived,
        )
    } else {
        let scheme = url.split(':').next().unwrap_or(url);
        return Err(format!(
            "KOPIA_REPOSITORY {url:?}: scheme {scheme:?} is not translatable (kopiur supports \
             s3/gcs/azure/b2/filesystem/sftp/webdav/rclone; notably there is no Google Drive \
             backend); author the kopiur Repository by hand"
        ));
    };

    if get("KOPIA_MANUAL_CONFIG").is_some() {
        notes.push(FieldNote {
            field: format!("secret/{secret_name}.KOPIA_MANUAL_CONFIG"),
            disposition: Disposition::Unmappable {
                reason: "raw kopia repository-config JSON has no kopiur equivalent; express the \
                         overrides via SnapshotPolicy fields (compression, retention, …) or \
                         author the Repository by hand"
                    .to_string(),
            },
        });
    }
    // GCS: auth points at the derived Secret the user must complete; make sure
    // a (possibly empty) derived Secret rides along so the reference resolves.
    if matches!(scheme, KopiaScheme::Gcs) && derived.is_none() {
        derived = Some((derived_name, BTreeMap::new()));
    }

    Ok(KopiaBackendPlan {
        backend,
        scheme,
        derived_creds: derived,
        notes,
    })
}

/// Apply a [`rename_plan`] against the Secret data: build the derived Secret's
/// `stringData` and the per-key Mapped notes. Returns `None` when the plan is
/// empty (reference-in-place).
fn build_derived(
    plan: &[CredRename],
    derived_name: &str,
    secret_name: &str,
    data: &BTreeMap<String, String>,
    notes: &mut Vec<FieldNote>,
) -> Option<(String, BTreeMap<String, String>)> {
    if plan.is_empty() {
        return None;
    }
    let mut out = BTreeMap::new();
    for rename in plan {
        if let Some(value) = data.get(rename.from) {
            out.insert(rename.to.to_string(), value.clone());
            notes.push(FieldNote {
                field: format!("secret/{secret_name}.{}", rename.from),
                disposition: Disposition::Mapped {
                    to: format!("Secret/{derived_name}.{}", rename.to),
                },
            });
        }
    }
    Some((derived_name.to_string(), out))
}

/// Translate one fork-kopia ReplicationSource into a SnapshotPolicy (+ optional
/// SnapshotSchedule), pinning the fork's snapshot identity so history continues
/// in the adopted repository. `repository_ref` is the kopiur repository the
/// policy points at.
pub fn translate_source(
    name: &str,
    namespace: &str,
    spec: &ReplicationSourceSpec,
    repository_ref: &serde_json::Value,
) -> Result<Translation, String> {
    let kopia = spec
        .kopia
        .as_ref()
        .ok_or_else(|| format!("ReplicationSource {namespace}/{name} has no spec.kopia block"))?;
    let mut t = Translation::default();

    let source_pvc = spec.source_pvc.clone().ok_or_else(|| {
        format!("ReplicationSource {namespace}/{name} has no spec.sourcePVC; nothing to back up")
    })?;
    t.mapped("spec.sourcePVC", "SnapshotPolicy.spec.sources[0].pvc.name");

    // --- identity continuity (the load-bearing part of kopia adoption) ---
    let (username, hostname) = fork_identity(
        name,
        namespace,
        kopia.username.as_deref(),
        kopia.hostname.as_deref(),
    );
    if kopia.username.as_deref().is_some_and(|u| !u.is_empty()) {
        t.mapped(
            "spec.kopia.username",
            "SnapshotPolicy.spec.identity.username (verbatim, as the fork used it)",
        );
    }
    if kopia.hostname.as_deref().is_some_and(|h| !h.is_empty()) {
        t.mapped(
            "spec.kopia.hostname",
            "SnapshotPolicy.spec.identity.hostname (verbatim, as the fork used it)",
        );
    }
    let source_path = kopia
        .source_path_override
        .clone()
        .unwrap_or_else(|| FORK_DEFAULT_SOURCE_PATH.to_string());
    if kopia.source_path_override.is_some() {
        t.mapped(
            "spec.kopia.sourcePathOverride",
            "SnapshotPolicy.spec.sources[0].sourcePathOverride",
        );
    }
    t.mapped(
        "(fork snapshot identity)",
        &format!(
            "SnapshotPolicy.spec.identity + sources[0].sourcePathOverride pinned to \
             {username}@{hostname}:{source_path} — the existing snapshot history continues \
             under kopiur"
        ),
    );

    let mut policy_spec = serde_json::json!({
        "repository": repository_ref,
        "identity": { "username": username, "hostname": hostname },
        "sources": [ { "pvc": { "name": source_pvc }, "sourcePathOverride": source_path } ],
    });

    // copyMethod: VolSync None/Direct → Direct; Snapshot/Clone 1:1.
    if let Some(method) = &kopia.copy_method {
        let kopiur_method = match method.as_str() {
            "Snapshot" => Some("Snapshot"),
            "Clone" => Some("Clone"),
            "Direct" | "None" => Some("Direct"),
            other => {
                t.unmappable(
                    "spec.kopia.copyMethod",
                    &format!("unknown VolSync copyMethod {other:?}; defaulting to kopiur's Direct"),
                );
                None
            }
        };
        if let Some(m) = kopiur_method {
            policy_spec["copyMethod"] = serde_json::json!(m);
            t.mapped("spec.kopia.copyMethod", "SnapshotPolicy.spec.copyMethod");
        }
    }
    if let Some(class) = &kopia.volume_snapshot_class_name {
        policy_spec["volumeSnapshotClassName"] = serde_json::json!(class);
        t.mapped(
            "spec.kopia.volumeSnapshotClassName",
            "SnapshotPolicy.spec.volumeSnapshotClassName",
        );
    }

    // Retention: the fork's counts are already kopia GFS semantics.
    if let Some(retain) = &kopia.retain {
        let mut retention = serde_json::Map::new();
        let mut map = |t: &mut Translation, vs: &str, val: Option<u32>, kopiur: &str| {
            if let Some(v) = val {
                retention.insert(kopiur.to_string(), serde_json::json!(v));
                t.mapped(
                    &format!("spec.kopia.retain.{vs}"),
                    &format!("SnapshotPolicy.spec.retention.{kopiur}"),
                );
            }
        };
        map(&mut t, "latest", retain.latest, "keepLatest");
        map(&mut t, "hourly", retain.hourly, "keepHourly");
        map(&mut t, "daily", retain.daily, "keepDaily");
        map(&mut t, "weekly", retain.weekly, "keepWeekly");
        map(&mut t, "monthly", retain.monthly, "keepMonthly");
        map(&mut t, "yearly", retain.yearly, "keepAnnual");
        if !retention.is_empty() {
            policy_spec["retention"] = serde_json::Value::Object(retention);
        }
    }

    if let Some(compression) = &kopia.compression {
        policy_spec["compression"] = serde_json::json!({ "compressor": compression });
        t.mapped(
            "spec.kopia.compression",
            "SnapshotPolicy.spec.compression.compressor",
        );
    }
    if let Some(parallel) = kopia.parallelism {
        // The fork passes `kopia snapshot create --parallel=N` — kopia's
        // file-read parallelism, i.e. the upload policy's maxParallelFileReads.
        policy_spec["upload"] = serde_json::json!({ "maxParallelFileReads": parallel });
        t.mapped(
            "spec.kopia.parallelism",
            "SnapshotPolicy.spec.upload.maxParallelFileReads",
        );
    }
    if let Some(args) = &kopia.additional_args
        && !args.is_empty()
    {
        policy_spec["extraArgs"] = serde_json::json!(args);
        t.mapped("spec.kopia.additionalArgs", "SnapshotPolicy.spec.extraArgs");
    }

    // Mover knobs.
    let mut mover = serde_json::Map::new();
    let mut cache = serde_json::Map::new();
    if let Some(cap) = &kopia.cache_capacity {
        cache.insert("capacity".into(), serde_json::json!(cap));
        t.mapped(
            "spec.kopia.cacheCapacity",
            "SnapshotPolicy.spec.mover.cache.capacity",
        );
    }
    if let Some(class) = &kopia.cache_storage_class_name {
        cache.insert("storageClassName".into(), serde_json::json!(class));
        t.mapped(
            "spec.kopia.cacheStorageClassName",
            "SnapshotPolicy.spec.mover.cache.storageClassName",
        );
    }
    if let Some(mb) = kopia.metadata_cache_size_limit_mb {
        cache.insert("metadataCacheSizeMb".into(), serde_json::json!(mb));
        t.mapped(
            "spec.kopia.metadataCacheSizeLimitMB",
            "SnapshotPolicy.spec.mover.cache.metadataCacheSizeMb",
        );
    }
    if let Some(mb) = kopia.content_cache_size_limit_mb {
        cache.insert("contentCacheSizeMb".into(), serde_json::json!(mb));
        t.mapped(
            "spec.kopia.contentCacheSizeLimitMB",
            "SnapshotPolicy.spec.mover.cache.contentCacheSizeMb",
        );
    }
    if !cache.is_empty() {
        mover.insert("cache".into(), serde_json::Value::Object(cache));
    }
    if let Some(resources) = &kopia.mover_resources {
        mover.insert("resources".into(), resources.clone());
        t.mapped(
            "spec.kopia.moverResources",
            "SnapshotPolicy.spec.mover.resources",
        );
    }
    if let Some(sc) = &kopia.mover_security_context {
        mover.insert("podSecurityContext".into(), sc.clone());
        t.mapped(
            "spec.kopia.moverSecurityContext",
            "SnapshotPolicy.spec.mover.podSecurityContext",
        );
    }
    if !mover.is_empty() {
        policy_spec["mover"] = serde_json::Value::Object(mover);
    }

    // No kopiur equivalent / deliberately not carried.
    if let Some(actions) = &kopia.actions {
        for (field, present) in [
            ("beforeSnapshot", actions.before_snapshot.is_some()),
            ("afterSnapshot", actions.after_snapshot.is_some()),
        ] {
            if present {
                t.unmappable(
                    &format!("spec.kopia.actions.{field}"),
                    "the fork runs this shell IN ITS MOVER POD; kopiur hooks \
                     (SnapshotPolicy.spec.hooks) run in the WORKLOAD pod or as a Job — \
                     rewrite the action for that execution context by hand",
                );
            }
        }
    }
    if kopia.policy_config.is_some() {
        t.unmappable(
            "spec.kopia.policyConfig",
            "kopiur models kopia policy as typed CRD fields (retention/compression/files/\
             upload/extraArgs); there is no raw policy-file passthrough — port the file's \
             settings into the SnapshotPolicy spec",
        );
    }
    if kopia.mover_service_account.is_some() {
        t.unmappable(
            "spec.kopia.moverServiceAccount",
            "kopiur mints a least-privilege per-namespace mover ServiceAccount itself",
        );
    }
    if kopia.mover_pod_labels.is_some() {
        t.unmappable(
            "spec.kopia.moverPodLabels",
            "kopiur has no per-recipe mover pod-label surface",
        );
    }
    if kopia.mover_affinity.is_some() {
        t.unmappable(
            "spec.kopia.moverAffinity",
            "mover affinity lives on the Repository's spec.moverDefaults (repo-wide), not \
             per-policy; set it there",
        );
    }
    if kopia.mover_volumes.is_some() {
        t.ignored(
            "spec.kopia.moverVolumes",
            "kopiur movers mount only source/cache/repository volumes; a PVC backing a \
             filesystem:// repository is carried into Repository.spec.backend.filesystem.volume \
             instead (see the repository accounting)",
        );
    }
    if kopia.custom_ca.is_some() {
        t.unmappable(
            "spec.kopia.customCA",
            "set the kopiur Repository's backend tls.caBundleRef (S3; a ConfigMap) instead",
        );
    }
    if let Some(class) = &kopia.storage_class_name {
        t.unmappable(
            "spec.kopia.storageClassName",
            &format!(
                "kopiur stages Snapshot/Clone copies with the source PVC's StorageClass; \
                 there is no per-policy staging-class override (was {class:?})"
            ),
        );
    }
    if kopia.access_modes.is_some() {
        t.unmappable(
            "spec.kopia.accessModes",
            "kopiur derives staging access modes from the source PVC",
        );
    }
    if kopia.cache_access_modes.is_some() {
        t.unmappable(
            "spec.kopia.cacheAccessModes",
            "kopiur's mover cache has no access-mode override (it is a per-run \
             ephemeral or controller-owned persistent volume)",
        );
    }
    if kopia.cleanup_cache_pvc.is_some() {
        t.ignored(
            "spec.kopia.cleanupCachePVC",
            "kopiur cache lifecycle is mover.cache.mode (Ephemeral default, or Persistent for \
             a warm cache)",
        );
    }

    t.objects.push(serde_json::json!({
        "apiVersion": kopiur_api::consts::API_VERSION,
        "kind": "SnapshotPolicy",
        "metadata": { "name": name, "namespace": namespace },
        "spec": policy_spec,
    }));

    // --- SnapshotSchedule (cron trigger only) ---
    match spec.trigger.as_ref() {
        Some(trigger) if trigger.schedule.is_some() => {
            let cron = trigger.schedule.clone().expect("checked");
            t.mapped(
                "spec.trigger.schedule",
                "SnapshotSchedule.spec.schedule.cron",
            );
            t.objects.push(serde_json::json!({
                "apiVersion": kopiur_api::consts::API_VERSION,
                "kind": "SnapshotSchedule",
                "metadata": { "name": name, "namespace": namespace },
                "spec": {
                    "policyRef": { "name": name },
                    "schedule": { "cron": cron }
                }
            }));
        }
        Some(trigger) if trigger.manual.is_some() => {
            t.ignored(
                "spec.trigger.manual",
                "manual triggers map to `kubectl kopiur snapshot now --policy <name>`; \
                 no SnapshotSchedule emitted",
            );
        }
        _ => {
            t.ignored(
                "spec.trigger",
                "no trigger: no SnapshotSchedule emitted (run manually or add one later)",
            );
        }
    }

    Ok(t)
}

/// Translate one fork-kopia ReplicationDestination into a kopiur Restore.
/// Prefers a raw-identity source (`source.identity`, which needs
/// `spec.repository`) — the fork's `sourceIdentity` / explicit
/// `username`+`hostname` map directly. Without either, falls back to
/// `source.fromPolicy` against `policy_name` (the caller pairs it via the
/// shared Secret, exactly like the restic path).
pub fn translate_destination(
    name: &str,
    namespace: &str,
    spec: &ReplicationDestinationSpec,
    repository_ref: &serde_json::Value,
    policy_name: &str,
) -> Result<Translation, String> {
    let kopia = spec.kopia.as_ref().ok_or_else(|| {
        format!("ReplicationDestination {namespace}/{name} has no spec.kopia block")
    })?;
    let mut t = Translation::default();

    // --- pick the restore source form ---
    enum SourceForm {
        Identity {
            username: String,
            hostname: String,
            source_path: String,
        },
        FromPolicy,
    }
    let form = if let Some(si) = kopia
        .source_identity
        .as_ref()
        .filter(|si| si.source_name.as_deref().is_some_and(|n| !n.is_empty()))
    {
        let source_name = si.source_name.as_deref().expect("checked");
        let source_ns = si.source_namespace.as_deref().unwrap_or(namespace);
        let source_path = si
            .source_path_override
            .clone()
            .unwrap_or_else(|| FORK_DEFAULT_SOURCE_PATH.to_string());
        t.mapped(
            "spec.kopia.sourceIdentity",
            "Restore.spec.source.identity (fork-sanitized username/hostname + sourcePath)",
        );
        if si.source_pvc_name.is_some() {
            t.ignored(
                "spec.kopia.sourceIdentity.sourcePVCName",
                "only feeds the fork's path inference; kopiur pins sourcePath explicitly",
            );
        }
        SourceForm::Identity {
            username: sanitize_username(source_name),
            hostname: sanitize_hostname(source_ns, source_name),
            source_path,
        }
    } else {
        let user = kopia.username.as_deref().filter(|s| !s.is_empty());
        let host = kopia.hostname.as_deref().filter(|s| !s.is_empty());
        match (user, host) {
            (Some(u), Some(h)) => {
                t.mapped(
                    "spec.kopia.username/hostname",
                    "Restore.spec.source.identity (verbatim, as the fork used them)",
                );
                SourceForm::Identity {
                    username: u.to_string(),
                    hostname: h.to_string(),
                    source_path: FORK_DEFAULT_SOURCE_PATH.to_string(),
                }
            }
            (Some(_), None) | (None, Some(_)) => {
                t.unmappable(
                    "spec.kopia.username/hostname",
                    "the fork requires username and hostname TOGETHER; only one is set — \
                     falling back to fromPolicy resolution",
                );
                SourceForm::FromPolicy
            }
            (None, None) => SourceForm::FromPolicy,
        }
    };

    let mut restore_spec = serde_json::json!({});
    match form {
        SourceForm::Identity {
            username,
            hostname,
            source_path,
        } => {
            let mut identity = serde_json::json!({
                "username": username,
                "hostname": hostname,
                "sourcePath": source_path,
            });
            if let Some(as_of) = &kopia.restore_as_of {
                identity["asOf"] = serde_json::json!(as_of);
                t.mapped(
                    "spec.kopia.restoreAsOf",
                    "Restore.spec.source.identity.asOf",
                );
            }
            if let Some(previous) = kopia.previous {
                identity["offset"] = serde_json::json!(previous);
                t.mapped("spec.kopia.previous", "Restore.spec.source.identity.offset");
            }
            // A raw-identity source REQUIRES spec.repository.
            restore_spec["repository"] = repository_ref.clone();
            restore_spec["source"] = serde_json::json!({ "identity": identity });
            // Fail-closed default for onMissingSnapshot: an explicit identity
            // restore should not silently no-op.
        }
        SourceForm::FromPolicy => {
            let mut from_policy = serde_json::json!({ "name": policy_name });
            if let Some(as_of) = &kopia.restore_as_of {
                from_policy["asOf"] = serde_json::json!(as_of);
                t.mapped(
                    "spec.kopia.restoreAsOf",
                    "Restore.spec.source.fromPolicy.asOf",
                );
            }
            if let Some(previous) = kopia.previous {
                from_policy["offset"] = serde_json::json!(previous);
                t.mapped(
                    "spec.kopia.previous",
                    "Restore.spec.source.fromPolicy.offset",
                );
            }
            restore_spec["source"] = serde_json::json!({ "fromPolicy": from_policy });
            restore_spec["policy"] = serde_json::json!({ "onMissingSnapshot": "Continue" });
        }
    }

    // --- target (same shape as the restic path) ---
    let target = match (&kopia.destination_pvc, &kopia.capacity) {
        (Some(existing), _) => {
            t.mapped(
                "spec.kopia.destinationPVC",
                "Restore.spec.target.pvcRef.name",
            );
            // Provisioning knobs are moot when restoring into an existing PVC.
            for (field, present) in [
                ("capacity", kopia.capacity.is_some()),
                ("accessModes", kopia.access_modes.is_some()),
                ("storageClassName", kopia.storage_class_name.is_some()),
            ] {
                if present {
                    t.ignored(
                        &format!("spec.kopia.{field}"),
                        "destinationPVC takes precedence; the restore writes into the \
                         existing PVC and provisions nothing",
                    );
                }
            }
            serde_json::json!({ "pvcRef": { "name": existing } })
        }
        (None, Some(capacity)) => {
            let mut pvc = serde_json::json!({
                "name": format!("{name}-restored"),
                "capacity": capacity,
            });
            t.mapped("spec.kopia.capacity", "Restore.spec.target.pvc.capacity");
            if let Some(modes) = &kopia.access_modes {
                pvc["accessModes"] = serde_json::json!(modes);
                t.mapped(
                    "spec.kopia.accessModes",
                    "Restore.spec.target.pvc.accessModes",
                );
            }
            if let Some(class) = &kopia.storage_class_name {
                pvc["storageClassName"] = serde_json::json!(class);
                t.mapped(
                    "spec.kopia.storageClassName",
                    "Restore.spec.target.pvc.storageClassName",
                );
            }
            serde_json::json!({ "pvc": pvc })
        }
        (None, None) => {
            return Err(format!(
                "ReplicationDestination {namespace}/{name} has neither destinationPVC nor \
                 capacity; kopiur needs an explicit restore target"
            ));
        }
    };
    restore_spec["target"] = target;

    if let Some(enabled) = kopia.enable_file_deletion {
        restore_spec["options"] = serde_json::json!({ "enableFileDeletion": enabled });
        t.mapped(
            "spec.kopia.enableFileDeletion",
            "Restore.spec.options.enableFileDeletion",
        );
    }
    if let Some(shallow) = kopia.shallow {
        t.unmappable(
            "spec.kopia.shallow",
            &format!(
                "kopiur has no newest-N selection window (was {shallow}); pick the snapshot \
                 via asOf/offset or snapshotID instead"
            ),
        );
    }
    if let Some(method) = &kopia.copy_method {
        t.ignored(
            "spec.kopia.copyMethod",
            &format!(
                "kopiur restores write directly into the target PVC; VolSync's destination \
                 copyMethod ({method:?}) has no role"
            ),
        );
    }

    // Destination mover cache knobs carry to the restore's mover.
    let mut cache = serde_json::Map::new();
    if let Some(cap) = &kopia.cache_capacity {
        cache.insert("capacity".into(), serde_json::json!(cap));
        t.mapped(
            "spec.kopia.cacheCapacity",
            "Restore.spec.mover.cache.capacity",
        );
    }
    if let Some(class) = &kopia.cache_storage_class_name {
        cache.insert("storageClassName".into(), serde_json::json!(class));
        t.mapped(
            "spec.kopia.cacheStorageClassName",
            "Restore.spec.mover.cache.storageClassName",
        );
    }
    if let Some(mb) = kopia.metadata_cache_size_limit_mb {
        cache.insert("metadataCacheSizeMb".into(), serde_json::json!(mb));
        t.mapped(
            "spec.kopia.metadataCacheSizeLimitMB",
            "Restore.spec.mover.cache.metadataCacheSizeMb",
        );
    }
    if let Some(mb) = kopia.content_cache_size_limit_mb {
        cache.insert("contentCacheSizeMb".into(), serde_json::json!(mb));
        t.mapped(
            "spec.kopia.contentCacheSizeLimitMB",
            "Restore.spec.mover.cache.contentCacheSizeMb",
        );
    }
    if !cache.is_empty() {
        restore_spec["mover"] = serde_json::json!({ "cache": serde_json::Value::Object(cache) });
    }
    if kopia.cleanup_cache_pvc.is_some() {
        t.ignored(
            "spec.kopia.cleanupCachePVC",
            "kopiur restore caches are run-scoped (Ephemeral default)",
        );
    }
    if kopia.custom_ca.is_some() {
        t.unmappable(
            "spec.kopia.customCA",
            "set the kopiur Repository's backend tls.caBundleRef (S3; a ConfigMap) instead",
        );
    }
    if spec.trigger.is_some() {
        t.ignored(
            "spec.trigger",
            "kopiur Restores are one-shot objects; create one per restore instead of a \
             recurring destination trigger",
        );
    }

    t.objects.push(serde_json::json!({
        "apiVersion": kopiur_api::consts::API_VERSION,
        "kind": "Restore",
        "metadata": { "name": name, "namespace": namespace },
        "spec": restore_spec,
    }));
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source_spec(v: serde_json::Value) -> ReplicationSourceSpec {
        serde_json::from_value(v).unwrap()
    }

    fn dest_spec(v: serde_json::Value) -> ReplicationDestinationSpec {
        serde_json::from_value(v).unwrap()
    }

    fn repo_ref() -> serde_json::Value {
        serde_json::json!({ "kind": "Repository", "name": "adopted" })
    }

    fn data(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn plan_for(url: &str, d: &BTreeMap<String, String>) -> KopiaBackendPlan {
        backend_from_kopia_repository(&KopiaBackendInput {
            url,
            secret_name: "vs-secret",
            repo_name: "vs-secret-kopiur",
            data: d,
            mover_volumes: None,
        })
        .unwrap()
    }

    /// Every emitted backend must parse as the REAL kopiur Backend enum.
    fn assert_valid_backend(plan: &KopiaBackendPlan) {
        let _typed: kopiur_api::backend::Backend =
            serde_json::from_value(plan.backend.clone()).expect("valid Backend");
    }

    // --- identity parity with the fork's builder.go (bug-for-bug REQUIRED) ---

    #[test]
    fn username_parity_with_fork_sanitize_for_identifier() {
        // Plain k8s names pass through.
        assert_eq!(sanitize_username("vs-app"), "vs-app");
        // Dots are DROPPED (not replaced) for usernames.
        assert_eq!(sanitize_username("app.v2"), "appv2");
        // Underscores are KEPT for usernames.
        assert_eq!(sanitize_username("my_app"), "my_app");
        // Leading/trailing '-'/'_' trimmed.
        assert_eq!(sanitize_username("-_app_-"), "app");
        // Empty / all-invalid falls back to the fork's default.
        assert_eq!(sanitize_username(""), FORK_DEFAULT_IDENTITY);
        assert_eq!(sanitize_username("..."), FORK_DEFAULT_IDENTITY);
    }

    #[test]
    fn username_truncates_to_50_after_trim_with_no_retrim() {
        // builder.go order: sanitize+trim FIRST, then validObjectName[:50] with
        // no re-trim — a truncated username may legally end in '-'.
        let name = format!("{}-{}", "a".repeat(49), "b".repeat(10));
        let got = sanitize_username(&name);
        assert_eq!(got.len(), 50);
        assert_eq!(got, format!("{}-", "a".repeat(49)));
        assert!(got.ends_with('-'), "no re-trim after truncation: {got}");
    }

    #[test]
    fn hostname_parity_with_fork_sanitize_for_hostname() {
        assert_eq!(sanitize_hostname("media", "x"), "media");
        // Dots are KEPT for hostnames.
        assert_eq!(sanitize_hostname("ns.prod", "x"), "ns.prod");
        // Underscores map to '-' for hostnames.
        assert_eq!(sanitize_hostname("my_ns", "x"), "my-ns");
        // Hostnames have NO length cap.
        let long = "n".repeat(80);
        assert_eq!(sanitize_hostname(&long, "x"), long);
        // Empty namespace falls back to the sanitized NAME, then the default.
        assert_eq!(sanitize_hostname("", "app.v2"), "app.v2");
        assert_eq!(sanitize_hostname("", ""), FORK_DEFAULT_IDENTITY);
    }

    #[test]
    fn fork_identity_overrides_bypass_sanitization() {
        // The fork uses explicit username/hostname AS-IS — so must we.
        let (u, h) = fork_identity("name", "ns", Some("Weird.User_"), Some("Weird.Host"));
        assert_eq!(u, "Weird.User_");
        assert_eq!(h, "Weird.Host");
        // Defaults: sanitized name@namespace.
        let (u, h) = fork_identity("app.v2", "my_ns", None, None);
        assert_eq!(u, "appv2");
        assert_eq!(h, "my-ns");
        // Empty-string overrides are treated as unset (Go `*s != ""` guard).
        let (u, _) = fork_identity("app", "ns", Some(""), None);
        assert_eq!(u, "app");
    }

    // --- backend parsing per scheme ---

    #[test]
    fn s3_url_with_endpoint_region_and_in_place_auth() {
        let d = data(&[
            ("AWS_ACCESS_KEY_ID", "AK"),
            ("AWS_SECRET_ACCESS_KEY", "SK"),
            ("AWS_S3_ENDPOINT", "http://minio.ns.svc:9000"),
            ("AWS_REGION", "us-east-1"),
        ]);
        let plan = plan_for("s3://bucket/pre/fix", &d);
        assert_eq!(plan.scheme, KopiaScheme::S3);
        assert_eq!(plan.backend["s3"]["bucket"], "bucket");
        assert_eq!(plan.backend["s3"]["prefix"], "pre/fix/");
        assert_eq!(plan.backend["s3"]["endpoint"], "minio.ns.svc:9000");
        // http:// endpoint ⇒ plain-HTTP repository ⇒ disableTls.
        assert_eq!(plan.backend["s3"]["tls"]["disableTls"], true);
        assert_eq!(plan.backend["s3"]["region"], "us-east-1");
        // AWS env names already match the kopiur mover: reference IN PLACE.
        assert_eq!(plan.backend["s3"]["auth"]["secretRef"]["name"], "vs-secret");
        assert!(plan.derived_creds.is_none(), "S3 needs no rename secret");
        assert_valid_backend(&plan);
    }

    #[test]
    fn s3_kopia_s3_bucket_overrides_url_bucket() {
        let d = data(&[("KOPIA_S3_BUCKET", "real-bucket")]);
        let plan = plan_for("s3://url-bucket", &d);
        assert_eq!(plan.backend["s3"]["bucket"], "real-bucket");
        // The accounting must say which value won.
        let note = plan
            .notes
            .iter()
            .find(|n| n.field.ends_with("KOPIA_S3_BUCKET"))
            .expect("override note");
        assert!(
            matches!(&note.disposition, Disposition::Mapped { to } if to.contains("url-bucket")),
            "{note:?}"
        );
    }

    #[test]
    fn s3_disable_tls_keys_force_disable_tls() {
        let d = data(&[
            ("KOPIA_S3_ENDPOINT", "https://s3.example"),
            ("KOPIA_S3_DISABLE_TLS", "true"),
        ]);
        let plan = plan_for("s3://b", &d);
        assert_eq!(plan.backend["s3"]["endpoint"], "s3.example");
        assert_eq!(plan.backend["s3"]["tls"]["disableTls"], true);
    }

    #[test]
    fn azure_account_name_is_a_backend_field_and_legacy_key_renames() {
        // Legacy AZURE_ACCOUNT_KEY only ⇒ derived rename secret.
        let d = data(&[("AZURE_ACCOUNT_NAME", "acct"), ("AZURE_ACCOUNT_KEY", "key")]);
        let plan = plan_for("azure://cont/path", &d);
        assert_eq!(plan.scheme, KopiaScheme::Azure);
        assert_eq!(plan.backend["azure"]["container"], "cont");
        // PARITY: the fork passes NO --prefix for azure — the URL path was
        // always ignored, so the repository lives at the container ROOT.
        assert!(plan.backend["azure"].get("prefix").is_none());
        assert!(plan.notes.iter().any(|n| matches!(
            &n.disposition,
            Disposition::Ignored { reason } if reason.contains("NO --prefix")
        )));
        assert_eq!(plan.backend["azure"]["storageAccount"], "acct");
        let (name, derived) = plan.derived_creds.as_ref().expect("rename secret");
        assert_eq!(name, "vs-secret-kopiur-creds");
        assert_eq!(
            derived.get("AZURE_STORAGE_KEY").map(String::as_str),
            Some("key")
        );
        assert_eq!(
            plan.backend["azure"]["auth"]["secretRef"]["name"],
            "vs-secret-kopiur-creds"
        );
        assert_valid_backend(&plan);

        // AZURE_STORAGE_KEY already present ⇒ reference in place, no rename.
        let d = data(&[
            ("AZURE_STORAGE_ACCOUNT", "acct"),
            ("AZURE_STORAGE_KEY", "key"),
        ]);
        let plan = plan_for("azure://cont", &d);
        assert!(plan.derived_creds.is_none());
        assert_eq!(
            plan.backend["azure"]["auth"]["secretRef"]["name"],
            "vs-secret"
        );
        assert_eq!(plan.backend["azure"]["storageAccount"], "acct");
    }

    #[test]
    fn b2_keys_rename_to_kopias_names() {
        let d = data(&[("B2_ACCOUNT_ID", "id"), ("B2_APPLICATION_KEY", "key")]);
        let plan = plan_for("b2://bkt/p", &d);
        assert_eq!(plan.scheme, KopiaScheme::B2);
        assert_eq!(plan.backend["b2"]["bucket"], "bkt");
        // PARITY: the fork ignores the b2 URL path (no --prefix) — root it is.
        assert!(plan.backend["b2"].get("prefix").is_none());
        let (_, derived) = plan.derived_creds.as_ref().expect("rename secret");
        // kopia reads B2_KEY_ID/B2_KEY — the fork's restic-legacy names are inert.
        assert_eq!(derived.get("B2_KEY_ID").map(String::as_str), Some("id"));
        assert_eq!(derived.get("B2_KEY").map(String::as_str), Some("key"));
        assert_valid_backend(&plan);
    }

    #[test]
    fn gcs_path_credentials_are_unmappable_content() {
        let d = data(&[("GOOGLE_APPLICATION_CREDENTIALS", "/credentials/key.json")]);
        let plan = plan_for("gcs://bkt", &d);
        assert_eq!(plan.scheme, KopiaScheme::Gcs);
        // Auth points at the derived Secret the user must complete...
        assert_eq!(
            plan.backend["gcs"]["auth"]["secretRef"]["name"],
            "vs-secret-kopiur-creds"
        );
        // ...which is emitted (possibly empty) so the reference resolves.
        assert!(plan.derived_creds.is_some());
        // ...and the accounting says WHY (file PATH vs JSON CONTENT).
        assert!(
            plan.notes.iter().any(|n| matches!(
                &n.disposition,
                Disposition::Unmappable { reason }
                    if reason.contains("KOPIA_GCS_CREDENTIALS") && reason.contains("CONTENT")
            )),
            "{:?}",
            plan.notes
        );
        assert_valid_backend(&plan);
    }

    #[test]
    fn filesystem_infers_the_repo_pvc_from_mover_volumes() {
        let vols: Vec<MoverVolume> = serde_json::from_value(serde_json::json!([
            { "mountPath": "kopia-repo",
              "volumeSource": { "persistentVolumeClaim": { "claimName": "repo-pvc" } } },
            { "mountPath": "scratch", "volumeSource": { "secret": { "secretName": "x" } } }
        ]))
        .unwrap();
        let d = data(&[]);
        let plan = backend_from_kopia_repository(&KopiaBackendInput {
            url: "filesystem:///mnt/kopia-repo",
            secret_name: "vs-secret",
            repo_name: "vs-secret-kopiur",
            data: &d,
            mover_volumes: Some(&vols),
        })
        .unwrap();
        assert_eq!(plan.scheme, KopiaScheme::Filesystem);
        assert_eq!(plan.backend["filesystem"]["path"], "/mnt/kopia-repo");
        assert_eq!(
            plan.backend["filesystem"]["volume"]["pvc"]["name"],
            "repo-pvc"
        );
        assert_valid_backend(&plan);
    }

    #[test]
    fn filesystem_without_an_inferable_pvc_blocks_apply_with_placeholder() {
        let plan = plan_for("filesystem:///repo", &data(&[]));
        assert_eq!(
            plan.backend["filesystem"]["volume"]["pvc"]["name"],
            "REPLACE_ME-repo-pvc"
        );
        assert!(plan.notes.iter().any(|n| matches!(
            &n.disposition,
            Disposition::Unmappable { reason } if reason.contains("moverVolumes")
        )));
    }

    #[test]
    fn sftp_parses_authority_and_renames_content_keys() {
        let d = data(&[
            ("SFTP_KNOWN_HOSTS_DATA", "nas ssh-ed25519 AAAA"),
            ("SFTP_PASSWORD", "hunter2"),
            ("SFTP_KEY_FILE", "/keys/id_ed25519"),
        ]);
        let plan = plan_for("sftp://backup@nas.lan:2222/srv/kopia", &d);
        assert_eq!(plan.scheme, KopiaScheme::Sftp);
        assert_eq!(plan.backend["sftp"]["host"], "nas.lan");
        assert_eq!(plan.backend["sftp"]["port"], 2222);
        assert_eq!(plan.backend["sftp"]["username"], "backup");
        assert_eq!(plan.backend["sftp"]["path"], "/srv/kopia");
        let (_, derived) = plan.derived_creds.as_ref().expect("derived secret");
        assert_eq!(
            derived.get("KOPIA_SFTP_KNOWN_HOSTS").map(String::as_str),
            Some("nas ssh-ed25519 AAAA")
        );
        // Password auth and key file PATHS have no kopiur equivalent.
        for key in ["SFTP_PASSWORD", "SFTP_KEY_FILE"] {
            assert!(
                plan.notes.iter().any(|n| n.field.ends_with(key)
                    && matches!(n.disposition, Disposition::Unmappable { .. })),
                "missing unmappable for {key}: {:?}",
                plan.notes
            );
        }
        assert_valid_backend(&plan);
    }

    #[test]
    fn webdav_prefers_the_secret_url_and_renames_creds() {
        let d = data(&[
            ("WEBDAV_URL", "https://dav.example/kopia"),
            ("WEBDAV_USERNAME", "u"),
            ("WEBDAV_PASSWORD", "p"),
        ]);
        let plan = plan_for("webdav://dav.example/kopia", &d);
        assert_eq!(plan.backend["webDav"]["url"], "https://dav.example/kopia");
        let (_, derived) = plan.derived_creds.as_ref().expect("rename secret");
        assert_eq!(
            derived.get("KOPIA_WEBDAV_USERNAME").map(String::as_str),
            Some("u")
        );
        assert_eq!(
            derived.get("KOPIA_WEBDAV_PASSWORD").map(String::as_str),
            Some("p")
        );
        assert_valid_backend(&plan);

        // No WEBDAV_URL: derive from the kopia URL, assuming https.
        let plan = plan_for("webdav://dav.example/kopia", &data(&[]));
        assert_eq!(plan.backend["webDav"]["url"], "https://dav.example/kopia");
    }

    #[test]
    fn rclone_sniffs_config_content_vs_path() {
        // Content (has an ini section) ⇒ derived KOPIA_RCLONE_CONFIG.
        let d = data(&[("RCLONE_CONFIG", "[remote]\ntype = s3\n")]);
        let plan = plan_for("rclone://remote:/backups", &d);
        assert_eq!(plan.backend["rclone"]["remotePath"], "remote:/backups");
        assert_eq!(
            plan.backend["rclone"]["configSecretRef"]["name"],
            "vs-secret-kopiur-creds"
        );
        let (_, derived) = plan.derived_creds.as_ref().expect("config secret");
        assert!(derived.contains_key("KOPIA_RCLONE_CONFIG"));
        assert_valid_backend(&plan);

        // A path ⇒ unmappable with guidance.
        let d = data(&[("RCLONE_CONFIG", "/etc/rclone/rclone.conf")]);
        let plan = plan_for("rclone://remote:/backups", &d);
        assert!(plan.notes.iter().any(|n| matches!(
            &n.disposition,
            Disposition::Unmappable { reason } if reason.contains("CONTENT")
        )));
    }

    #[test]
    fn s3_prefix_carries_the_fork_trailing_slash_and_gcs_path_is_ignored() {
        // S3 is the ONLY scheme whose URL path becomes a prefix, and the fork's
        // entry.sh appends a trailing slash — parity on both counts.
        let plan = plan_for("s3://bkt/pre/fix", &data(&[]));
        assert_eq!(plan.backend["s3"]["prefix"], "pre/fix/");
        let plan = plan_for("gcs://bkt/some/path", &data(&[]));
        assert!(plan.backend["gcs"].get("prefix").is_none());
        assert!(plan.notes.iter().any(|n| matches!(
            &n.disposition,
            Disposition::Ignored { reason } if reason.contains("NO --prefix")
        )));
    }

    #[test]
    fn empty_bucket_and_bad_sftp_port_are_explicit_errors() {
        let err = backend_from_kopia_repository(&KopiaBackendInput {
            url: "s3://",
            secret_name: "s",
            repo_name: "s-kopiur",
            data: &data(&[]),
            mover_volumes: None,
        })
        .unwrap_err();
        assert!(err.contains("no bucket"), "{err}");

        let err = backend_from_kopia_repository(&KopiaBackendInput {
            url: "sftp://host:99999/path",
            secret_name: "s",
            repo_name: "s-kopiur",
            data: &data(&[]),
            mover_volumes: None,
        })
        .unwrap_err();
        assert!(err.contains("not a valid TCP port"), "{err}");
    }

    #[test]
    fn gdrive_is_an_explicit_error() {
        let err = backend_from_kopia_repository(&KopiaBackendInput {
            url: "gdrive://folder-id",
            secret_name: "s",
            repo_name: "s-kopiur",
            data: &data(&[]),
            mover_volumes: None,
        })
        .unwrap_err();
        assert!(err.contains("Google Drive"), "{err}");
        assert!(err.contains("by hand"), "{err}");
    }

    #[test]
    fn kopia_manual_config_is_surfaced_as_unmappable() {
        let d = data(&[("KOPIA_MANUAL_CONFIG", "{\"compression\":{}}")]);
        let plan = plan_for("s3://b", &d);
        assert!(
            plan.notes
                .iter()
                .any(|n| n.field.ends_with("KOPIA_MANUAL_CONFIG")
                    && matches!(n.disposition, Disposition::Unmappable { .. }))
        );
    }

    // --- translate_source ---

    #[test]
    fn full_kopia_source_translates_with_identity_pinned() {
        let spec = source_spec(serde_json::json!({
            "sourcePVC": "data",
            "trigger": { "schedule": "0 3 * * *" },
            "kopia": {
                "repository": "vs-secret",
                "copyMethod": "Snapshot",
                "volumeSnapshotClassName": "csi-snapclass",
                "retain": { "hourly": 6, "daily": 7, "weekly": 4, "monthly": 6, "yearly": 1, "latest": 3 },
                "compression": "zstd",
                "parallelism": 4,
                "cacheCapacity": "2Gi",
                "cacheStorageClassName": "fast",
                "metadataCacheSizeLimitMB": 700,
                "contentCacheSizeLimitMB": 200,
                "additionalArgs": ["--one-file-system"],
                "moverResources": { "limits": { "memory": "1Gi" } },
                "moverSecurityContext": { "fsGroup": 1000 },
                "actions": { "beforeSnapshot": "sync" },
                "policyConfig": { "repositoryConfig": "{}" },
                "moverServiceAccount": "custom-sa",
                "moverPodLabels": { "a": "b" },
                "moverAffinity": { "nodeAffinity": {} },
                "cleanupCachePVC": true
            }
        }));
        let t = translate_source("app.v2", "my_ns", &spec, &repo_ref()).unwrap();

        assert_eq!(t.objects.len(), 2);
        let policy = &t.objects[0];
        assert_eq!(policy["kind"], "SnapshotPolicy");
        // THE load-bearing property: the fork's identity is pinned so the
        // adopted repository's history continues (dots dropped from username,
        // underscore→dash in hostname, path defaulted to /data).
        assert_eq!(policy["spec"]["identity"]["username"], "appv2");
        assert_eq!(policy["spec"]["identity"]["hostname"], "my-ns");
        assert_eq!(policy["spec"]["sources"][0]["sourcePathOverride"], "/data");
        assert_eq!(policy["spec"]["sources"][0]["pvc"]["name"], "data");
        assert_eq!(policy["spec"]["copyMethod"], "Snapshot");
        // Fork retention names: latest→keepLatest, yearly→keepAnnual.
        assert_eq!(policy["spec"]["retention"]["keepLatest"], 3);
        assert_eq!(policy["spec"]["retention"]["keepAnnual"], 1);
        assert_eq!(policy["spec"]["retention"]["keepHourly"], 6);
        assert_eq!(policy["spec"]["compression"]["compressor"], "zstd");
        assert_eq!(policy["spec"]["upload"]["maxParallelFileReads"], 4);
        assert_eq!(policy["spec"]["extraArgs"][0], "--one-file-system");
        assert_eq!(policy["spec"]["mover"]["cache"]["capacity"], "2Gi");
        assert_eq!(policy["spec"]["mover"]["cache"]["metadataCacheSizeMb"], 700);
        assert_eq!(policy["spec"]["mover"]["cache"]["contentCacheSizeMb"], 200);
        assert_eq!(
            policy["spec"]["mover"]["resources"]["limits"]["memory"],
            "1Gi"
        );
        assert_eq!(
            policy["spec"]["mover"]["podSecurityContext"]["fsGroup"],
            1000
        );

        let schedule = &t.objects[1];
        assert_eq!(schedule["kind"], "SnapshotSchedule");
        assert_eq!(schedule["spec"]["schedule"]["cron"], "0 3 * * *");

        // Accounting: mover-pod actions / policyConfig / SA / labels / affinity
        // are unmappable with reasons; cleanupCachePVC is merely ignored.
        let note = |field: &str| {
            t.notes
                .iter()
                .find(|n| n.field == field)
                .unwrap_or_else(|| panic!("no note for {field}: {:?}", t.notes))
        };
        assert!(t.has_unmappable);
        for field in [
            "spec.kopia.actions.beforeSnapshot",
            "spec.kopia.policyConfig",
            "spec.kopia.moverServiceAccount",
            "spec.kopia.moverPodLabels",
            "spec.kopia.moverAffinity",
        ] {
            assert!(
                matches!(note(field).disposition, Disposition::Unmappable { .. }),
                "{field} should be unmappable"
            );
        }
        assert!(matches!(
            note("spec.kopia.cleanupCachePVC").disposition,
            Disposition::Ignored { .. }
        ));
        assert!(matches!(
            note("(fork snapshot identity)").disposition,
            Disposition::Mapped { .. }
        ));

        // The emitted policy parses as the REAL kopiur type (admission shape).
        let spec_typed: kopiur_api::SnapshotPolicySpec =
            serde_json::from_value(policy["spec"].clone()).expect("valid SnapshotPolicySpec");
        assert_eq!(
            spec_typed
                .identity
                .as_ref()
                .and_then(|i| i.username.as_deref()),
            Some("appv2")
        );
        assert_eq!(
            spec_typed.sources[0].source_path_override.as_deref(),
            Some("/data")
        );
    }

    #[test]
    fn source_path_and_identity_overrides_carry_verbatim() {
        let spec = source_spec(serde_json::json!({
            "sourcePVC": "data",
            "kopia": {
                "repository": "vs-secret",
                "username": "Custom_User",
                "hostname": "custom.host",
                "sourcePathOverride": "/data/app"
            }
        }));
        let t = translate_source("app", "media", &spec, &repo_ref()).unwrap();
        let policy = &t.objects[0];
        assert_eq!(policy["spec"]["identity"]["username"], "Custom_User");
        assert_eq!(policy["spec"]["identity"]["hostname"], "custom.host");
        assert_eq!(
            policy["spec"]["sources"][0]["sourcePathOverride"],
            "/data/app"
        );
        // A minimal kopia source has nothing unmappable: --strict must pass.
        assert!(!t.has_unmappable, "{:?}", t.notes);
    }

    #[test]
    fn non_kopia_source_is_an_explicit_error() {
        let spec = source_spec(serde_json::json!({ "sourcePVC": "data" }));
        let err = translate_source("app", "media", &spec, &repo_ref()).unwrap_err();
        assert!(err.contains("no spec.kopia"), "{err}");
    }

    // --- translate_destination ---

    #[test]
    fn destination_source_identity_maps_to_identity_restore() {
        let spec = dest_spec(serde_json::json!({
            "trigger": { "manual": "once" },
            "kopia": {
                "repository": "vs-secret",
                "destinationPVC": "data",
                "sourceIdentity": {
                    "sourceName": "web.app",
                    "sourceNamespace": "prod_ns",
                    "sourcePVCName": "web-data"
                },
                "restoreAsOf": "2026-06-01T00:00:00Z",
                "previous": 1,
                "enableFileDeletion": true,
                "cacheCapacity": "1Gi",
                "shallow": 5
            }
        }));
        let t = translate_destination("app-dst", "media", &spec, &repo_ref(), "unused").unwrap();
        let restore = &t.objects[0];
        assert_eq!(restore["kind"], "Restore");
        // Raw-identity restores REQUIRE spec.repository.
        assert_eq!(restore["spec"]["repository"]["name"], "adopted");
        // Fork sanitization applies to the derived identity.
        assert_eq!(restore["spec"]["source"]["identity"]["username"], "webapp");
        assert_eq!(restore["spec"]["source"]["identity"]["hostname"], "prod-ns");
        assert_eq!(restore["spec"]["source"]["identity"]["sourcePath"], "/data");
        assert_eq!(
            restore["spec"]["source"]["identity"]["asOf"],
            "2026-06-01T00:00:00Z"
        );
        assert_eq!(restore["spec"]["source"]["identity"]["offset"], 1);
        assert_eq!(restore["spec"]["target"]["pvcRef"]["name"], "data");
        assert_eq!(restore["spec"]["options"]["enableFileDeletion"], true);
        assert_eq!(restore["spec"]["mover"]["cache"]["capacity"], "1Gi");
        // Identity restores keep the fail-closed onMissingSnapshot default.
        assert!(restore["spec"].get("policy").is_none());
        // shallow has no kopiur equivalent.
        assert!(t.notes.iter().any(|n| n.field == "spec.kopia.shallow"
            && matches!(n.disposition, Disposition::Unmappable { .. })));

        // Real type round-trip, with an exhaustive match on the source form
        // (type-safety thesis: a new RestoreSource variant must be decided here).
        let typed: kopiur_api::RestoreSpec =
            serde_json::from_value(restore["spec"].clone()).expect("valid RestoreSpec");
        match typed.source {
            kopiur_api::restore::RestoreSource::Identity(id) => {
                assert_eq!(id.username, "webapp");
                assert_eq!(id.hostname, "prod-ns");
                assert_eq!(id.source_path.as_deref(), Some("/data"));
                assert_eq!(id.offset, Some(1));
            }
            kopiur_api::restore::RestoreSource::SnapshotRef(_)
            | kopiur_api::restore::RestoreSource::FromPolicy(_) => {
                panic!("sourceIdentity must produce an identity source")
            }
        }
    }

    #[test]
    fn destination_explicit_identity_pair_is_verbatim() {
        let spec = dest_spec(serde_json::json!({
            "kopia": {
                "repository": "vs-secret",
                "destinationPVC": "data",
                "username": "Custom_User",
                "hostname": "custom.host"
            }
        }));
        let t = translate_destination("dst", "media", &spec, &repo_ref(), "unused").unwrap();
        let restore = &t.objects[0];
        assert_eq!(
            restore["spec"]["source"]["identity"]["username"],
            "Custom_User"
        );
        assert_eq!(
            restore["spec"]["source"]["identity"]["hostname"],
            "custom.host"
        );
        assert_eq!(restore["spec"]["source"]["identity"]["sourcePath"], "/data");
    }

    #[test]
    fn destination_pvc_precedence_accounts_for_dropped_provisioning_knobs() {
        // destinationPVC wins over capacity/accessModes/storageClassName —
        // the losers must land in the accounting, not vanish.
        let spec = dest_spec(serde_json::json!({
            "kopia": {
                "repository": "vs-secret",
                "destinationPVC": "data",
                "capacity": "10Gi",
                "storageClassName": "fast",
                "username": "u",
                "hostname": "h"
            }
        }));
        let t = translate_destination("dst", "media", &spec, &repo_ref(), "unused").unwrap();
        for field in ["spec.kopia.capacity", "spec.kopia.storageClassName"] {
            assert!(
                t.notes
                    .iter()
                    .any(|n| n.field == field
                        && matches!(&n.disposition, Disposition::Ignored { .. })),
                "missing ignored note for {field}: {:?}",
                t.notes
            );
        }
    }

    #[test]
    fn destination_without_identity_falls_back_to_from_policy() {
        let spec = dest_spec(serde_json::json!({
            "kopia": {
                "repository": "vs-secret",
                "capacity": "10Gi",
                "restoreAsOf": "2026-06-01T00:00:00Z"
            }
        }));
        let t = translate_destination("dst", "media", &spec, &repo_ref(), "app").unwrap();
        let restore = &t.objects[0];
        assert_eq!(restore["spec"]["source"]["fromPolicy"]["name"], "app");
        assert_eq!(
            restore["spec"]["source"]["fromPolicy"]["asOf"],
            "2026-06-01T00:00:00Z"
        );
        assert_eq!(restore["spec"]["policy"]["onMissingSnapshot"], "Continue");
        assert_eq!(restore["spec"]["target"]["pvc"]["capacity"], "10Gi");
        let _typed: kopiur_api::RestoreSpec =
            serde_json::from_value(restore["spec"].clone()).expect("valid RestoreSpec");
    }

    #[test]
    fn destination_partial_identity_pair_is_unmappable_and_falls_back() {
        let spec = dest_spec(serde_json::json!({
            "kopia": {
                "repository": "vs-secret",
                "destinationPVC": "data",
                "username": "only-user"
            }
        }));
        let t = translate_destination("dst", "media", &spec, &repo_ref(), "app").unwrap();
        assert!(t.has_unmappable);
        let restore = &t.objects[0];
        assert_eq!(restore["spec"]["source"]["fromPolicy"]["name"], "app");
    }
}
