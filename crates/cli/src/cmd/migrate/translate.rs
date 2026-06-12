//! Pure VolSync→kopiur translation. Every VolSync field the translator reads
//! lands in an explicit [`Disposition`] — `Mapped`, `Unmappable`, or `Ignored`
//! with a reason — so nothing is ever silently dropped. CONFIG ONLY: a restic
//! repository is not a kopia repository; no data moves.

use serde::Serialize;

use super::volsync_types::{ReplicationDestinationSpec, ReplicationSourceSpec};

/// What happened to one VolSync field. Closed enum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "disposition")]
pub enum Disposition {
    /// Translated into the kopiur object(s); `to` names the kopiur field.
    Mapped {
        /// The kopiur destination field.
        to: String,
    },
    /// Has NO kopiur equivalent; carries the why.
    Unmappable {
        /// Why there is no equivalent, and what to do instead.
        reason: String,
    },
    /// Deliberately not carried over; carries the why.
    Ignored {
        /// Why it is not needed in kopiur.
        reason: String,
    },
}

/// One accounting line: `field` (VolSync path) → what happened.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldNote {
    /// The VolSync field path.
    pub field: String,
    /// Its disposition.
    #[serde(flatten)]
    pub disposition: Disposition,
}

/// Translation result for one VolSync object.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Translation {
    /// The kopiur manifests (plain JSON values; rendered as YAML docs).
    pub objects: Vec<serde_json::Value>,
    /// Per-field accounting — review this before applying.
    pub notes: Vec<FieldNote>,
    /// True when any field was `Unmappable` (drives `--strict`).
    pub has_unmappable: bool,
}

impl Translation {
    pub(super) fn mapped(&mut self, field: &str, to: &str) {
        self.notes.push(FieldNote {
            field: field.into(),
            disposition: Disposition::Mapped { to: to.into() },
        });
    }
    pub(super) fn unmappable(&mut self, field: &str, reason: &str) {
        self.has_unmappable = true;
        self.notes.push(FieldNote {
            field: field.into(),
            disposition: Disposition::Unmappable {
                reason: reason.into(),
            },
        });
    }
    pub(super) fn ignored(&mut self, field: &str, reason: &str) {
        self.notes.push(FieldNote {
            field: field.into(),
            disposition: Disposition::Ignored {
                reason: reason.into(),
            },
        });
    }
}

/// Translate one restic ReplicationSource into a SnapshotPolicy (+ optional
/// SnapshotSchedule). `repository_ref` is the kopiur repository the policy
/// should point at (an existing one via `--repository`, or the one
/// `--resolve-secrets`/`--emit-repository` emits).
pub fn translate_source(
    name: &str,
    namespace: &str,
    spec: &ReplicationSourceSpec,
    repository_ref: &serde_json::Value,
) -> Result<Translation, String> {
    let restic = spec
        .restic
        .as_ref()
        .ok_or_else(|| format!("ReplicationSource {namespace}/{name} has no spec.restic block (a non-restic mover); kopiur migration covers restic sources only"))?;
    let mut t = Translation::default();

    // --- SnapshotPolicy ---
    let source_pvc = spec.source_pvc.clone().ok_or_else(|| {
        format!("ReplicationSource {namespace}/{name} has no spec.sourcePVC; nothing to back up")
    })?;
    t.mapped("spec.sourcePVC", "SnapshotPolicy.spec.sources[0].pvc.name");

    let mut policy_spec = serde_json::json!({
        "repository": repository_ref,
        "sources": [ { "pvc": { "name": source_pvc } } ],
    });

    // copyMethod: VolSync None/Direct → Direct; Snapshot/Clone 1:1.
    if let Some(method) = &restic.copy_method {
        let kopiur_method = match method.as_str() {
            "Snapshot" => Some("Snapshot"),
            "Clone" => Some("Clone"),
            "Direct" | "None" => Some("Direct"),
            other => {
                t.unmappable(
                    "spec.restic.copyMethod",
                    &format!("unknown VolSync copyMethod {other:?}; defaulting to kopiur's Direct"),
                );
                None
            }
        };
        if let Some(m) = kopiur_method {
            policy_spec["copyMethod"] = serde_json::json!(m);
            t.mapped("spec.restic.copyMethod", "SnapshotPolicy.spec.copyMethod");
        }
    }
    if let Some(class) = &restic.volume_snapshot_class_name {
        policy_spec["volumeSnapshotClassName"] = serde_json::json!(class);
        t.mapped(
            "spec.restic.volumeSnapshotClassName",
            "SnapshotPolicy.spec.volumeSnapshotClassName",
        );
    }

    // Retention: restic forget → kopiur GFS. `last` IS keepLatest; `within`
    // has no kopia equivalent.
    if let Some(retain) = &restic.retain {
        let mut retention = serde_json::Map::new();
        let mut map = |t: &mut Translation, vs: &str, val: Option<u32>, kopiur: &str| {
            if let Some(v) = val {
                retention.insert(kopiur.to_string(), serde_json::json!(v));
                t.mapped(
                    &format!("spec.restic.retain.{vs}"),
                    &format!("SnapshotPolicy.spec.retention.{kopiur}"),
                );
            }
        };
        map(&mut t, "last", retain.last, "keepLatest");
        map(&mut t, "hourly", retain.hourly, "keepHourly");
        map(&mut t, "daily", retain.daily, "keepDaily");
        map(&mut t, "weekly", retain.weekly, "keepWeekly");
        map(&mut t, "monthly", retain.monthly, "keepMonthly");
        map(&mut t, "yearly", retain.yearly, "keepAnnual");
        if let Some(within) = &retain.within {
            t.unmappable(
                "spec.restic.retain.within",
                &format!(
                    "kopia has no keep-within ({within:?}); approximate with keepHourly/keepDaily \
                     counts covering the same window"
                ),
            );
        }
        if !retention.is_empty() {
            policy_spec["retention"] = serde_json::Value::Object(retention);
        }
    }

    // Mover knobs.
    let mut mover = serde_json::Map::new();
    if let Some(cap) = &restic.cache_capacity {
        mover.entry("cache").or_insert(serde_json::json!({}))["capacity"] = serde_json::json!(cap);
        t.mapped(
            "spec.restic.cacheCapacity",
            "SnapshotPolicy.spec.mover.cache.capacity",
        );
    }
    if let Some(class) = &restic.cache_storage_class_name {
        mover.entry("cache").or_insert(serde_json::json!({}))["storageClassName"] =
            serde_json::json!(class);
        t.mapped(
            "spec.restic.cacheStorageClassName",
            "SnapshotPolicy.spec.mover.cache.storageClassName",
        );
    }
    if let Some(resources) = &restic.mover_resources {
        mover.insert("resources".into(), resources.clone());
        t.mapped(
            "spec.restic.moverResources",
            "SnapshotPolicy.spec.mover.resources",
        );
    }
    if let Some(sc) = &restic.mover_security_context {
        mover.insert("podSecurityContext".into(), sc.clone());
        t.mapped(
            "spec.restic.moverSecurityContext",
            "SnapshotPolicy.spec.mover.podSecurityContext",
        );
    }
    if !mover.is_empty() {
        policy_spec["mover"] = serde_json::Value::Object(mover);
    }

    // Deliberately not carried.
    if restic.prune_interval_days.is_some() {
        t.ignored(
            "spec.restic.pruneIntervalDays",
            "kopiur maintenance is default-managed per repository (quick 6h / full daily); \
             tune the repository's spec.maintenance instead",
        );
    }
    if restic.unlock.is_some() {
        t.ignored(
            "spec.restic.unlock",
            "kopia has no restic-style repo lock to clear",
        );
    }
    if restic.mover_service_account.is_some() {
        t.unmappable(
            "spec.restic.moverServiceAccount",
            "kopiur mints a least-privilege per-namespace mover ServiceAccount itself",
        );
    }
    if restic.custom_ca.is_some() {
        t.unmappable(
            "spec.restic.customCA",
            "set the kopiur Repository's backend tls.caBundleRef (S3) instead",
        );
    }
    if let Some(class) = &restic.storage_class_name {
        t.unmappable(
            "spec.restic.storageClassName",
            &format!(
                "kopiur stages Snapshot/Clone copies with the source PVC's StorageClass; \
                 there is no per-policy staging-class override (was {class:?})"
            ),
        );
    }
    if restic.access_modes.is_some() {
        t.unmappable(
            "spec.restic.accessModes",
            "kopiur derives staging access modes from the source PVC",
        );
    }
    if restic.cache_access_modes.is_some() {
        t.unmappable(
            "spec.restic.cacheAccessModes",
            "kopiur's mover cache has no access-mode override (it is a per-run \
             ephemeral or controller-owned persistent volume)",
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

/// Translate one restic ReplicationDestination into a kopiur Restore
/// (deploy-or-restore: `source.fromPolicy` + `onMissingSnapshot: Continue`).
/// `policy_name` is the translated SnapshotPolicy the restore resolves through.
pub fn translate_destination(
    name: &str,
    namespace: &str,
    spec: &ReplicationDestinationSpec,
    policy_name: &str,
) -> Result<Translation, String> {
    let restic = spec.restic.as_ref().ok_or_else(|| {
        format!("ReplicationDestination {namespace}/{name} has no spec.restic block")
    })?;
    let mut t = Translation::default();

    let mut source = serde_json::json!({ "fromPolicy": { "name": policy_name } });
    if let Some(as_of) = &restic.restore_as_of {
        source["fromPolicy"]["asOf"] = serde_json::json!(as_of);
        t.mapped(
            "spec.restic.restoreAsOf",
            "Restore.spec.source.fromPolicy.asOf",
        );
    }
    if let Some(previous) = restic.previous {
        source["fromPolicy"]["offset"] = serde_json::json!(previous);
        t.mapped(
            "spec.restic.previous",
            "Restore.spec.source.fromPolicy.offset",
        );
    }

    let target = match (&restic.destination_pvc, &restic.capacity) {
        (Some(existing), _) => {
            t.mapped(
                "spec.restic.destinationPVC",
                "Restore.spec.target.pvcRef.name",
            );
            // Provisioning knobs are moot when restoring into an existing PVC.
            for (field, present) in [
                ("capacity", restic.capacity.is_some()),
                ("accessModes", restic.access_modes.is_some()),
                ("storageClassName", restic.storage_class_name.is_some()),
            ] {
                if present {
                    t.ignored(
                        &format!("spec.restic.{field}"),
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
            t.mapped("spec.restic.capacity", "Restore.spec.target.pvc.capacity");
            if let Some(modes) = &restic.access_modes {
                pvc["accessModes"] = serde_json::json!(modes);
                t.mapped(
                    "spec.restic.accessModes",
                    "Restore.spec.target.pvc.accessModes",
                );
            }
            if let Some(class) = &restic.storage_class_name {
                pvc["storageClassName"] = serde_json::json!(class);
                t.mapped(
                    "spec.restic.storageClassName",
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

    if let Some(method) = &restic.copy_method {
        t.ignored(
            "spec.restic.copyMethod",
            &format!(
                "kopiur restores write directly into the target PVC; VolSync's destination \
                 copyMethod ({method:?}) has no role"
            ),
        );
    }
    // restic --delete ⇒ exact-mirror restore.
    let options = restic.enable_file_deletion.map(|enabled| {
        t.mapped(
            "spec.restic.enableFileDeletion",
            "Restore.spec.options.enableFileDeletion",
        );
        serde_json::json!({ "enableFileDeletion": enabled })
    });
    if spec.trigger.is_some() {
        t.ignored(
            "spec.trigger",
            "kopiur Restores are one-shot objects; create one per restore instead of a \
             recurring destination trigger",
        );
    }

    let mut restore_spec = serde_json::json!({
        "source": source,
        "target": target,
        "policy": { "onMissingSnapshot": "Continue" }
    });
    if let Some(options) = options {
        restore_spec["options"] = options;
    }
    t.objects.push(serde_json::json!({
        "apiVersion": kopiur_api::consts::API_VERSION,
        "kind": "Restore",
        "metadata": { "name": name, "namespace": namespace },
        "spec": restore_spec,
    }));
    Ok(t)
}

/// The placeholder a `--resolve-secrets` password Secret carries. A restic
/// password CANNOT initialize a kopia repository's encryption — the user must
/// choose a NEW kopia password; `--apply` refuses to create this value.
pub const PASSWORD_PLACEHOLDER: &str = "REPLACE_ME-choose-a-new-kopia-password";

/// Which backend a `RESTIC_REPOSITORY` URL maps to. Closed enum — the
/// credential carry-over below `match`es it exhaustively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendScheme {
    /// `s3:…`
    S3,
    /// `b2:bucket[:prefix]`
    B2,
    /// `azure:container[:path]`
    Azure,
    /// `gs:bucket[:path]`
    Gcs,
    /// A local/absolute path.
    Filesystem,
}

/// Parse a `RESTIC_REPOSITORY` URL into a kopiur Backend JSON + its scheme.
/// Supported: `s3:`/`b2:`/`azure:`/`gs:`/local paths; anything else is an
/// error naming the scheme. kopia prefixes are LITERAL strings — the kopiur
/// convention is a trailing slash, normalized here for every scheme.
pub fn backend_from_restic_repository(
    url: &str,
    creds_secret: &str,
) -> Result<(serde_json::Value, BackendScheme), String> {
    let prefixify = |p: &str| -> Option<String> {
        let trimmed = p.trim_matches('/');
        (!trimmed.is_empty()).then(|| format!("{trimmed}/"))
    };
    if let Some(rest) = url.strip_prefix("s3:") {
        // Forms: s3:https://endpoint/bucket/prefix… or s3:s3.amazonaws.com/bucket/prefix…
        let (endpoint, path) = if let Some(stripped) = rest
            .strip_prefix("https://")
            .or_else(|| rest.strip_prefix("http://"))
        {
            let insecure = rest.starts_with("http://");
            let (host, path) = stripped.split_once('/').ok_or_else(|| {
                format!("RESTIC_REPOSITORY {url:?}: no bucket after the endpoint")
            })?;
            ((host.to_string(), insecure), path)
        } else {
            let (host, path) = rest.split_once('/').ok_or_else(|| {
                format!("RESTIC_REPOSITORY {url:?}: no bucket after the endpoint")
            })?;
            ((host.to_string(), false), path)
        };
        let (bucket, prefix) = match path.split_once('/') {
            Some((b, p)) => (b.to_string(), prefixify(p)),
            None => (path.trim_end_matches('/').to_string(), None),
        };
        let mut s3 = serde_json::json!({
            "bucket": bucket,
            "endpoint": endpoint.0,
            "auth": { "secretRef": { "name": creds_secret } }
        });
        if let Some(p) = prefix {
            s3["prefix"] = serde_json::json!(p);
        }
        if endpoint.1 {
            s3["tls"] = serde_json::json!({ "disableTls": true });
        }
        Ok((serde_json::json!({ "s3": s3 }), BackendScheme::S3))
    } else if let Some(rest) = url.strip_prefix("b2:") {
        let (bucket, prefix) = match rest.split_once(':') {
            Some((b, p)) => (b.to_string(), prefixify(p)),
            None => (rest.to_string(), None),
        };
        let mut b2 = serde_json::json!({
            "bucket": bucket,
            "auth": { "secretRef": { "name": creds_secret } }
        });
        if let Some(p) = prefix {
            b2["prefix"] = serde_json::json!(p);
        }
        Ok((serde_json::json!({ "b2": b2 }), BackendScheme::B2))
    } else if let Some(rest) = url.strip_prefix("azure:") {
        let (container, prefix) = match rest.split_once(':') {
            Some((c, p)) => (c.to_string(), prefixify(p)),
            None => (rest.to_string(), None),
        };
        let mut azure = serde_json::json!({
            "container": container,
            "auth": { "secretRef": { "name": creds_secret } }
        });
        if let Some(p) = prefix {
            azure["prefix"] = serde_json::json!(p);
        }
        Ok((serde_json::json!({ "azure": azure }), BackendScheme::Azure))
    } else if let Some(rest) = url.strip_prefix("gs:") {
        let (bucket, prefix) = match rest.split_once(':') {
            Some((b, p)) => (b.to_string(), prefixify(p)),
            None => (rest.trim_end_matches('/').to_string(), None),
        };
        let mut gcs = serde_json::json!({
            "bucket": bucket,
            "auth": { "secretRef": { "name": creds_secret } }
        });
        if let Some(p) = prefix {
            gcs["prefix"] = serde_json::json!(p);
        }
        Ok((serde_json::json!({ "gcs": gcs }), BackendScheme::Gcs))
    } else if url.starts_with('/') {
        Ok((
            serde_json::json!({ "filesystem": { "path": url } }),
            BackendScheme::Filesystem,
        ))
    } else {
        let scheme = url.split(':').next().unwrap_or(url);
        Err(format!(
            "RESTIC_REPOSITORY {url:?}: scheme {scheme:?} is not translatable (kopiur supports \
             s3/b2/azure/gcs/filesystem here); author the kopiur Repository by hand"
        ))
    }
}

/// How one restic credential key carries into the kopiur world.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredCarry {
    /// Copy the VALUE into the kopiur creds Secret under `to` (kopia's env
    /// names differ from restic's for several backends).
    Env {
        /// The restic Secret key to read.
        from: &'static str,
        /// The kopia env key to write.
        to: &'static str,
    },
    /// The VALUE belongs in a backend FIELD, not the Secret.
    BackendField {
        /// The restic Secret key to read.
        from: &'static str,
        /// JSON pointer into the backend object (e.g. `/azure/storageAccount`).
        pointer: &'static str,
    },
}

/// The credential plan per scheme — what kopiur's OWN mover actually reads
/// (`crates/mover/src/credentials.rs` / kopia env contract), which is NOT
/// restic's env surface for B2/Azure/GCS.
pub fn cred_plan(scheme: BackendScheme) -> Vec<CredCarry> {
    match scheme {
        BackendScheme::S3 => vec![
            CredCarry::Env {
                from: "AWS_ACCESS_KEY_ID",
                to: "AWS_ACCESS_KEY_ID",
            },
            CredCarry::Env {
                from: "AWS_SECRET_ACCESS_KEY",
                to: "AWS_SECRET_ACCESS_KEY",
            },
            CredCarry::Env {
                from: "AWS_SESSION_TOKEN",
                to: "AWS_SESSION_TOKEN",
            },
            // kopia takes the region as a backend field, not this env var.
            CredCarry::BackendField {
                from: "AWS_DEFAULT_REGION",
                pointer: "/s3/region",
            },
        ],
        BackendScheme::B2 => vec![
            // restic names vs kopia names differ.
            CredCarry::Env {
                from: "B2_ACCOUNT_ID",
                to: "B2_KEY_ID",
            },
            CredCarry::Env {
                from: "B2_ACCOUNT_KEY",
                to: "B2_KEY",
            },
        ],
        BackendScheme::Azure => vec![
            CredCarry::Env {
                from: "AZURE_ACCOUNT_KEY",
                to: "AZURE_STORAGE_KEY",
            },
            // kopia takes the account NAME as a backend field.
            CredCarry::BackendField {
                from: "AZURE_ACCOUNT_NAME",
                pointer: "/azure/storageAccount",
            },
        ],
        // GCS has no carry: restic's GOOGLE_APPLICATION_CREDENTIALS is a file
        // PATH inside the VolSync pod; kopiur needs the JSON CONTENT under
        // KOPIA_GCS_CREDENTIALS. Surfaced as an Unmappable note by the caller.
        BackendScheme::Gcs => vec![],
        BackendScheme::Filesystem => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source_spec(v: serde_json::Value) -> ReplicationSourceSpec {
        serde_json::from_value(v).unwrap()
    }

    fn repo_ref() -> serde_json::Value {
        serde_json::json!({ "kind": "Repository", "name": "migrated" })
    }

    #[test]
    fn full_source_translates_with_complete_accounting() {
        let spec = source_spec(serde_json::json!({
            "sourcePVC": "data",
            "trigger": { "schedule": "0 3 * * *" },
            "restic": {
                "repository": "restic-secret",
                "copyMethod": "Snapshot",
                "volumeSnapshotClassName": "csi-snapclass",
                "retain": { "hourly": 6, "daily": 7, "weekly": 4, "monthly": 6, "yearly": 1, "last": 3, "within": "3d" },
                "cacheCapacity": "2Gi",
                "moverResources": { "limits": { "memory": "1Gi" } },
                "pruneIntervalDays": 7,
                "moverServiceAccount": "custom-sa"
            }
        }));
        let t = translate_source("app", "media", &spec, &repo_ref()).unwrap();

        // Two objects: policy + schedule.
        assert_eq!(t.objects.len(), 2);
        let policy = &t.objects[0];
        assert_eq!(policy["kind"], "SnapshotPolicy");
        assert_eq!(policy["spec"]["sources"][0]["pvc"]["name"], "data");
        assert_eq!(policy["spec"]["copyMethod"], "Snapshot");
        assert_eq!(policy["spec"]["volumeSnapshotClassName"], "csi-snapclass");
        assert_eq!(policy["spec"]["retention"]["keepLatest"], 3);
        assert_eq!(policy["spec"]["retention"]["keepHourly"], 6);
        assert_eq!(policy["spec"]["retention"]["keepAnnual"], 1);
        assert_eq!(policy["spec"]["mover"]["cache"]["capacity"], "2Gi");
        assert_eq!(
            policy["spec"]["mover"]["resources"]["limits"]["memory"],
            "1Gi"
        );
        let schedule = &t.objects[1];
        assert_eq!(schedule["kind"], "SnapshotSchedule");
        assert_eq!(schedule["spec"]["schedule"]["cron"], "0 3 * * *");
        assert_eq!(schedule["spec"]["policyRef"]["name"], "app");

        // Accounting: within is unmappable, prune ignored, SA unmappable.
        assert!(t.has_unmappable);
        let note = |field: &str| {
            t.notes
                .iter()
                .find(|n| n.field == field)
                .unwrap_or_else(|| panic!("no note for {field}: {:?}", t.notes))
        };
        assert!(matches!(
            note("spec.restic.retain.within").disposition,
            Disposition::Unmappable { .. }
        ));
        assert!(matches!(
            note("spec.restic.pruneIntervalDays").disposition,
            Disposition::Ignored { .. }
        ));
        assert!(matches!(
            note("spec.restic.moverServiceAccount").disposition,
            Disposition::Unmappable { .. }
        ));
        assert!(matches!(
            note("spec.restic.retain.last").disposition,
            Disposition::Mapped { .. }
        ));
        // The emitted policy parses as the REAL kopiur type (admission shape).
        let spec_typed: kopiur_api::SnapshotPolicySpec =
            serde_json::from_value(policy["spec"].clone()).expect("valid SnapshotPolicySpec");
        assert_eq!(spec_typed.sources.len(), 1);
    }

    #[test]
    fn non_restic_source_is_an_explicit_error() {
        let spec = source_spec(serde_json::json!({ "sourcePVC": "data" }));
        let err = translate_source("app", "media", &spec, &repo_ref()).unwrap_err();
        assert!(err.contains("non-restic mover"), "{err}");
    }

    #[test]
    fn destination_maps_to_deploy_or_restore() {
        let spec: ReplicationDestinationSpec = serde_json::from_value(serde_json::json!({
            "trigger": { "manual": "once" },
            "restic": {
                "repository": "restic-secret",
                "destinationPVC": "data",
                "restoreAsOf": "2026-06-01T00:00:00Z",
                "previous": 1
            }
        }))
        .unwrap();
        let t = translate_destination("app-dst", "media", &spec, "app").unwrap();
        let restore = &t.objects[0];
        assert_eq!(restore["kind"], "Restore");
        assert_eq!(restore["spec"]["source"]["fromPolicy"]["name"], "app");
        assert_eq!(
            restore["spec"]["source"]["fromPolicy"]["asOf"],
            "2026-06-01T00:00:00Z"
        );
        assert_eq!(restore["spec"]["source"]["fromPolicy"]["offset"], 1);
        assert_eq!(restore["spec"]["target"]["pvcRef"]["name"], "data");
        assert_eq!(restore["spec"]["policy"]["onMissingSnapshot"], "Continue");
        // Real type round-trip.
        let _typed: kopiur_api::RestoreSpec =
            serde_json::from_value(restore["spec"].clone()).expect("valid RestoreSpec");
    }

    #[test]
    fn restic_repository_urls_parse_into_backends() {
        let (backend, scheme) =
            backend_from_restic_repository("s3:https://minio.local:9000/bucket/pre/fix", "creds")
                .unwrap();
        assert_eq!(scheme, BackendScheme::S3);
        assert_eq!(backend["s3"]["bucket"], "bucket");
        assert_eq!(backend["s3"]["endpoint"], "minio.local:9000");
        assert_eq!(backend["s3"]["prefix"], "pre/fix/");

        // Trailing slash on the prefix must not double up.
        let (backend, _) =
            backend_from_restic_repository("s3:https://e/bucket/pre/", "creds").unwrap();
        assert_eq!(backend["s3"]["prefix"], "pre/");

        let (backend, _) =
            backend_from_restic_repository("s3:s3.amazonaws.com/just-bucket", "creds").unwrap();
        assert_eq!(backend["s3"]["bucket"], "just-bucket");
        assert!(backend["s3"].get("prefix").is_none());

        // b2: prefix normalized to the kopiur trailing-slash convention;
        // a trailing colon is an empty prefix, not part of the bucket.
        let (backend, scheme) = backend_from_restic_repository("b2:bkt:pre", "creds").unwrap();
        assert_eq!(scheme, BackendScheme::B2);
        assert_eq!(backend["b2"]["bucket"], "bkt");
        assert_eq!(backend["b2"]["prefix"], "pre/");
        let (backend, _) = backend_from_restic_repository("b2:bkt:", "creds").unwrap();
        assert_eq!(backend["b2"]["bucket"], "bkt");
        assert!(backend["b2"].get("prefix").is_none());

        // azure/gs paths land in prefix instead of being dropped.
        let (backend, _) =
            backend_from_restic_repository("azure:cont:/some/path", "creds").unwrap();
        assert_eq!(backend["azure"]["container"], "cont");
        assert_eq!(backend["azure"]["prefix"], "some/path/");
        let (backend, _) = backend_from_restic_repository("gs:bkt:/p", "creds").unwrap();
        assert_eq!(backend["gcs"]["bucket"], "bkt");
        assert_eq!(backend["gcs"]["prefix"], "p/");

        let (backend, scheme) = backend_from_restic_repository("/mnt/repo", "creds").unwrap();
        assert_eq!(scheme, BackendScheme::Filesystem);
        assert_eq!(backend["filesystem"]["path"], "/mnt/repo");

        let err = backend_from_restic_repository("sftp:user@host:/x", "creds").unwrap_err();
        assert!(err.contains("not translatable"), "{err}");
        assert!(err.contains("by hand"), "{err}");
    }

    #[test]
    fn cred_plans_use_kopias_env_names_not_restics() {
        // The kopiur mover reads kopia's env surface (B2_KEY_ID/B2_KEY,
        // AZURE_STORAGE_KEY, region/storageAccount as backend FIELDS) — NOT
        // restic's names. Carrying restic names verbatim produces inert vars.
        let b2 = cred_plan(BackendScheme::B2);
        assert!(b2.contains(&CredCarry::Env {
            from: "B2_ACCOUNT_ID",
            to: "B2_KEY_ID"
        }));
        assert!(b2.contains(&CredCarry::Env {
            from: "B2_ACCOUNT_KEY",
            to: "B2_KEY"
        }));
        let azure = cred_plan(BackendScheme::Azure);
        assert!(azure.contains(&CredCarry::Env {
            from: "AZURE_ACCOUNT_KEY",
            to: "AZURE_STORAGE_KEY"
        }));
        assert!(azure.contains(&CredCarry::BackendField {
            from: "AZURE_ACCOUNT_NAME",
            pointer: "/azure/storageAccount"
        }));
        let s3 = cred_plan(BackendScheme::S3);
        assert!(s3.contains(&CredCarry::BackendField {
            from: "AWS_DEFAULT_REGION",
            pointer: "/s3/region"
        }));
        assert!(s3.contains(&CredCarry::Env {
            from: "AWS_SESSION_TOKEN",
            to: "AWS_SESSION_TOKEN"
        }));
        // GCS: no carry (file PATH vs JSON CONTENT mismatch — caller notes it).
        assert!(cred_plan(BackendScheme::Gcs).is_empty());
    }
}
