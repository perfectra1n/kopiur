//! `kubectl kopiur suspend|resume <kind> <name>` — toggle the declarative
//! suspend field (ADR-0005 §14(e)) on any kind that has one.

use kopiur_api::{
    ClusterRepository, Repository, RepositoryReplication, SnapshotPolicy, SnapshotSchedule,
};
use kube::api::{Api, Patch, PatchParams};
use serde::de::DeserializeOwned;

use crate::cli::{SuspendArgs, SuspendableKind};
use crate::context::KubeCtx;
use crate::error::{CliError, classify_kube};
use crate::output::OutputFormat;

/// Identity strings for one suspendable kind, used in messages and `-o name`.
#[derive(Debug, Clone, Copy)]
pub struct KindMeta {
    /// CamelCase kind, for messages.
    pub kind: &'static str,
    /// Lowercase singular, for `-o name` (`<singular>.<group>/<name>`).
    pub singular: &'static str,
    /// Lowercase plural, for RBAC hints and `kubectl get` remediation.
    pub plural: &'static str,
}

/// Resolve the naming for each suspendable kind. Exhaustive.
pub fn kind_meta(kind: SuspendableKind) -> KindMeta {
    match kind {
        SuspendableKind::Policy => KindMeta {
            kind: "SnapshotPolicy",
            singular: "snapshotpolicy",
            plural: "snapshotpolicies",
        },
        SuspendableKind::Schedule => KindMeta {
            kind: "SnapshotSchedule",
            singular: "snapshotschedule",
            plural: "snapshotschedules",
        },
        SuspendableKind::Repository => KindMeta {
            kind: "Repository",
            singular: "repository",
            plural: "repositories",
        },
        SuspendableKind::ClusterRepository => KindMeta {
            kind: "ClusterRepository",
            singular: "clusterrepository",
            plural: "clusterrepositories",
        },
        SuspendableKind::Replication => KindMeta {
            kind: "RepositoryReplication",
            singular: "repositoryreplication",
            plural: "repositoryreplications",
        },
    }
}

/// The merge patch that sets the suspend field for this kind. The path is the
/// only thing that varies: `SnapshotSchedule` nests it under `spec.schedule`
/// (it is a schedule property there), every other kind has `spec.suspend`.
pub fn patch_for(kind: SuspendableKind, desired: bool) -> serde_json::Value {
    match kind {
        SuspendableKind::Schedule => {
            serde_json::json!({ "spec": { "schedule": { "suspend": desired } } })
        }
        SuspendableKind::Policy
        | SuspendableKind::Repository
        | SuspendableKind::ClusterRepository
        | SuspendableKind::Replication => {
            serde_json::json!({ "spec": { "suspend": desired } })
        }
    }
}

/// Outcome of a suspend/resume, with everything the renderer needs.
#[derive(Debug, serde::Serialize)]
pub struct SuspendReport {
    /// Kind naming (not serialized as-is; flattened into the fields below).
    #[serde(skip)]
    pub meta: KindMeta,
    /// CamelCase kind.
    pub kind: &'static str,
    /// Object name.
    pub name: String,
    /// Namespace, absent for ClusterRepository.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Suspend value before this command ran.
    pub previous: bool,
    /// Suspend value requested (and now in effect).
    pub desired: bool,
    /// The full object after patching (verbatim CR for `-o yaml|json`).
    pub object: serde_json::Value,
}

/// Render the report for the requested output format. Pure.
pub fn render(report: &SuspendReport, output: OutputFormat) -> Result<String, CliError> {
    let resource = format!(
        "{}.{}/{}",
        report.meta.singular,
        kopiur_api::GROUP,
        report.name
    );
    match output {
        OutputFormat::Table | OutputFormat::Wide => {
            let verb = if report.desired {
                "suspended"
            } else {
                "resumed"
            };
            if report.previous == report.desired {
                Ok(format!("{resource} unchanged (already {verb})\n"))
            } else {
                Ok(format!("{resource} {verb}\n"))
            }
        }
        OutputFormat::Yaml => {
            serde_yaml::to_string(&report.object).map_err(|e| CliError::Serialization {
                what: "patched object",
                source: e.into(),
            })
        }
        OutputFormat::Json => {
            let mut s = serde_json::to_string_pretty(&report.object).map_err(|e| {
                CliError::Serialization {
                    what: "patched object",
                    source: e.into(),
                }
            })?;
            s.push('\n');
            Ok(s)
        }
        OutputFormat::Name => Ok(format!("{resource}\n")),
    }
}

/// Toggle one object's suspend field: get (for the previous value and a real
/// not-found message), merge-patch only when it would change, return the
/// resulting object. Idempotent by construction.
async fn toggle<K>(
    api: Api<K>,
    meta: KindMeta,
    namespace: Option<&str>,
    name: &str,
    kind: SuspendableKind,
    desired: bool,
    current: impl Fn(&K) -> bool,
) -> Result<SuspendReport, CliError>
where
    K: kube::Resource + Clone + std::fmt::Debug + DeserializeOwned + serde::Serialize,
{
    let obj = api
        .get(name)
        .await
        .map_err(|e| classify_kube("get", meta.kind, meta.plural, namespace, Some(name), e))?;
    let previous = current(&obj);
    let patched = if previous == desired {
        obj
    } else {
        let params = PatchParams {
            field_manager: Some(crate::consts::FIELD_MANAGER.to_string()),
            ..Default::default()
        };
        api.patch(name, &params, &Patch::Merge(patch_for(kind, desired)))
            .await
            .map_err(|e| classify_kube("patch", meta.kind, meta.plural, namespace, Some(name), e))?
    };
    let object = serde_json::to_value(&patched).map_err(|e| CliError::Serialization {
        what: "patched object",
        source: e.into(),
    })?;
    Ok(SuspendReport {
        meta,
        kind: meta.kind,
        name: name.to_string(),
        namespace: namespace.map(str::to_string),
        previous,
        desired,
        object,
    })
}

/// Entry point for both `suspend` (desired=true) and `resume` (desired=false).
pub async fn run(
    ctx: &KubeCtx,
    args: &SuspendArgs,
    desired: bool,
    output: OutputFormat,
) -> Result<String, CliError> {
    if matches!(ctx.scope, crate::context::Scope::All) {
        return Err(CliError::AllNamespacesNotApplicable {
            command: if desired { "suspend" } else { "resume" },
        });
    }
    let meta = kind_meta(args.kind);
    let ns = ctx.namespace.as_str();
    let client = ctx.client.clone();
    let report = match args.kind {
        SuspendableKind::Policy => {
            let api: Api<SnapshotPolicy> = Api::namespaced(client, ns);
            toggle(api, meta, Some(ns), &args.name, args.kind, desired, |o| {
                o.spec.suspend
            })
            .await?
        }
        SuspendableKind::Schedule => {
            let api: Api<SnapshotSchedule> = Api::namespaced(client, ns);
            toggle(api, meta, Some(ns), &args.name, args.kind, desired, |o| {
                o.spec.schedule.suspend
            })
            .await?
        }
        SuspendableKind::Repository => {
            let api: Api<Repository> = Api::namespaced(client, ns);
            toggle(api, meta, Some(ns), &args.name, args.kind, desired, |o| {
                o.spec.suspend
            })
            .await?
        }
        SuspendableKind::ClusterRepository => {
            let api: Api<ClusterRepository> = Api::all(client);
            toggle(api, meta, None, &args.name, args.kind, desired, |o| {
                o.spec.suspend
            })
            .await?
        }
        SuspendableKind::Replication => {
            let api: Api<RepositoryReplication> = Api::namespaced(client, ns);
            toggle(api, meta, Some(ns), &args.name, args.kind, desired, |o| {
                o.spec.suspend
            })
            .await?
        }
    };
    render(&report, output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_patches_the_nested_schedule_suspend_path() {
        let p = patch_for(SuspendableKind::Schedule, true);
        assert_eq!(p["spec"]["schedule"]["suspend"], true);
        assert!(p["spec"].get("suspend").is_none());
    }

    #[test]
    fn flat_kinds_patch_spec_suspend() {
        for kind in [
            SuspendableKind::Policy,
            SuspendableKind::Repository,
            SuspendableKind::ClusterRepository,
            SuspendableKind::Replication,
        ] {
            let p = patch_for(kind, false);
            assert_eq!(p["spec"]["suspend"], false, "{kind:?}");
            assert!(p["spec"].get("schedule").is_none(), "{kind:?}");
        }
    }

    fn report(previous: bool, desired: bool) -> SuspendReport {
        SuspendReport {
            meta: kind_meta(SuspendableKind::Policy),
            kind: "SnapshotPolicy",
            name: "nightly".into(),
            namespace: Some("media".into()),
            previous,
            desired,
            object: serde_json::json!({"kind": "SnapshotPolicy"}),
        }
    }

    #[test]
    fn table_render_states_the_transition_or_noop() {
        let changed = render(&report(false, true), OutputFormat::Table).unwrap();
        assert_eq!(
            changed,
            "snapshotpolicy.kopiur.home-operations.com/nightly suspended\n"
        );
        let resumed = render(&report(true, false), OutputFormat::Table).unwrap();
        assert_eq!(
            resumed,
            "snapshotpolicy.kopiur.home-operations.com/nightly resumed\n"
        );
        let noop = render(&report(true, true), OutputFormat::Table).unwrap();
        assert_eq!(
            noop,
            "snapshotpolicy.kopiur.home-operations.com/nightly unchanged (already suspended)\n"
        );
    }

    #[test]
    fn name_render_matches_kubectl_o_name() {
        let out = render(&report(false, true), OutputFormat::Name).unwrap();
        assert_eq!(out, "snapshotpolicy.kopiur.home-operations.com/nightly\n");
    }

    #[test]
    fn yaml_and_json_render_the_object_verbatim() {
        let r = report(false, true);
        let yaml = render(&r, OutputFormat::Yaml).unwrap();
        assert!(yaml.contains("kind: SnapshotPolicy"));
        let json = render(&r, OutputFormat::Json).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["kind"], "SnapshotPolicy");
    }
}
