//! `kubectl kopiur maintenance run` — trigger an out-of-band maintenance run
//! by stamping the `run-requested`/`run-mode` annotations; the operator routes
//! it through the SAME mover/lease/single-flight path as the cron slots and
//! answers in `status.manualRun`.

use chrono::{DateTime, SecondsFormat, Utc};
use kopiur_api::common::RepositoryKind;
use kopiur_api::consts::{RUN_MODE_ANNOTATION, RUN_REQUESTED_ANNOTATION};
use kopiur_api::{Maintenance, ManualRunMode, ManualRunPhase};
use kube::ResourceExt;
use kube::api::{Api, ListParams, Patch, PatchParams};

use crate::CmdOutput;
use crate::cli::MaintenanceRunArgs;
use crate::context::KubeCtx;
use crate::error::{CliError, classify_kube};
use crate::wait::{DEFAULT_WAIT_TIMEOUT, wait_for};

/// Does this Maintenance cover the wanted repository? An absent ref namespace
/// means "same as the Maintenance" for a namespaced Repository. Pure.
pub fn covers_repository(maint: &Maintenance, kind: RepositoryKind, name: &str) -> bool {
    let rref = &maint.spec.repository;
    if rref.kind != kind || rref.name != name {
        return false;
    }
    match kind {
        RepositoryKind::ClusterRepository => true,
        // Same-namespace semantics: the CLI lists Maintenances in ONE
        // namespace, so an absent ref namespace is that namespace.
        RepositoryKind::Repository => {
            let effective = rref
                .namespace
                .as_deref()
                .or(maint.metadata.namespace.as_deref());
            effective == maint.metadata.namespace.as_deref()
        }
    }
}

/// The annotation merge-patch for one run request. BOTH annotations are always
/// set — leaving a stale `run-mode: full` behind would silently upgrade the
/// next quick request. Pure.
pub fn run_patch(requested_at: &str, mode: ManualRunMode) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "annotations": {
                RUN_REQUESTED_ANNOTATION: requested_at,
                RUN_MODE_ANNOTATION: mode.label(),
            }
        }
    })
}

/// Did `status.manualRun` answer THIS request (matching `requestedAt`)?
/// Exhaustive over [`ManualRunPhase`].
pub fn answered(
    maint: &Maintenance,
    requested_at: &str,
) -> Option<Result<Box<Maintenance>, Box<Maintenance>>> {
    let manual = maint.status.as_ref()?.manual_run.as_ref()?;
    if manual.requested_at.as_deref() != Some(requested_at) {
        return None;
    }
    match manual.phase? {
        ManualRunPhase::Running => None,
        ManualRunPhase::Succeeded => Some(Ok(Box::new(maint.clone()))),
        ManualRunPhase::Failed => Some(Err(Box::new(maint.clone()))),
    }
}

/// Failure detail from the conditions (the manual-run status itself has no
/// failure block; the reconciler writes the reason into conditions).
pub fn failure_detail(maint: &Maintenance, requested_at: &str) -> String {
    let name = maint.metadata.name.as_deref().unwrap_or("?");
    let condition_msg = maint
        .status
        .as_ref()
        .map(|s| s.conditions.as_slice())
        .unwrap_or_default()
        .iter()
        .find(|c| c.status == "False" || c.reason == "ManualRunOutcomeLost")
        .map(|c| format!(" ({}): {}", c.reason, c.message))
        .unwrap_or_default();
    format!(
        "maintenance {name} manual run (requested {requested_at}) failed{condition_msg}\n\
         check `kubectl kopiur logs` is not applicable here — maintenance Jobs are found with \
         `kubectl get jobs -l kopiur.home-operations.com/maintenance={name}`\n"
    )
}

/// The lease-yield note for a "successful" run that did not actually run
/// (the mover yields when another owner holds the kopia maintenance lease and
/// takeoverPolicy forbids seizing it). Pure.
pub fn yield_note(maint: &Maintenance) -> Option<String> {
    let c = maint
        .status
        .as_ref()
        .map(|s| s.conditions.as_slice())
        .unwrap_or_default()
        .iter()
        .find(|c| {
            c.reason == kopiur_api::maintenance::LEASE_HELD_BY_OTHER_REASON
                || c.reason == kopiur_api::maintenance::LEASE_TAKEOVER_PROMPT_REASON
        })?;
    Some(format!(
        "note: the Job succeeded by YIELDING the maintenance lease — no maintenance ran. {} \
         Set spec.ownership.takeoverPolicy=Force to claim it.",
        c.message
    ))
}

/// Resolve the target Maintenance by name or by the repository it covers.
async fn resolve(ctx: &KubeCtx, args: &MaintenanceRunArgs) -> Result<Maintenance, CliError> {
    let ns = ctx.namespace.as_str();
    let api: Api<Maintenance> = Api::namespaced(ctx.client.clone(), ns);
    if let Some(name) = &args.name {
        return api.get(name).await.map_err(|e| {
            classify_kube(
                "get",
                "Maintenance",
                "maintenances",
                Some(ns),
                Some(name),
                e,
            )
        });
    }
    let repo = args.repository.as_deref().expect("clap group");
    let kind: RepositoryKind = args.repository_kind.into();
    // A ClusterRepository's managed Maintenance is usually placed in the
    // OPERATOR namespace, not the caller's — search every namespace for that
    // kind (covers_repository ignores namespace for cluster repos).
    let listed = match kind {
        RepositoryKind::Repository => api
            .list(&ListParams::default())
            .await
            .map_err(|e| classify_kube("list", "Maintenance", "maintenances", Some(ns), None, e))?,
        RepositoryKind::ClusterRepository => {
            let all: Api<Maintenance> = Api::all(ctx.client.clone());
            all.list(&ListParams::default())
                .await
                .map_err(|e| classify_kube("list", "Maintenance", "maintenances", None, None, e))?
        }
    };
    let mut covering: Vec<Maintenance> = listed
        .items
        .into_iter()
        .filter(|m| covers_repository(m, kind, repo))
        .collect();
    match covering.len() {
        1 => Ok(covering.remove(0)),
        0 => Err(CliError::NotFound {
            kind: "Maintenance",
            plural: "maintenances",
            name: format!("(covering {kind:?}/{repo})"),
            scope: crate::error::scope_suffix(Some(ns)),
            scope_flag: format!(" -n {ns}"),
        }),
        n => Err(CliError::AmbiguousTarget {
            what: format!("{n} Maintenance objects cover {kind:?}/{repo}"),
            candidates: covering
                .iter()
                .map(|m| m.name_any())
                .collect::<Vec<_>>()
                .join(", "),
        }),
    }
}

/// Run `maintenance run`.
pub async fn run(
    ctx: &KubeCtx,
    args: &MaintenanceRunArgs,
    now: DateTime<Utc>,
) -> Result<CmdOutput, CliError> {
    if matches!(ctx.scope, crate::context::Scope::All) {
        return Err(CliError::AllNamespacesNotApplicable {
            command: "maintenance run",
        });
    }
    let maint = resolve(ctx, args).await?;
    let name = maint.name_any();
    // Operate where the resolved Maintenance LIVES (a ClusterRepository's
    // managed Maintenance is typically in the operator namespace).
    let maint_ns = maint
        .metadata
        .namespace
        .clone()
        .unwrap_or_else(|| ctx.namespace.clone());
    let ns = maint_ns.as_str();
    let api: Api<Maintenance> = Api::namespaced(ctx.client.clone(), ns);

    let mode = if args.full {
        ManualRunMode::Full
    } else {
        ManualRunMode::Quick
    };
    let requested_at = now.to_rfc3339_opts(SecondsFormat::Secs, true);
    let params = PatchParams {
        field_manager: Some(crate::consts::FIELD_MANAGER.to_string()),
        ..Default::default()
    };
    api.patch(
        &name,
        &params,
        &Patch::Merge(run_patch(&requested_at, mode)),
    )
    .await
    .map_err(|e| {
        classify_kube(
            "patch",
            "Maintenance",
            "maintenances",
            Some(ns),
            Some(&name),
            e,
        )
    })?;

    let requested_line = format!(
        "maintenance.{}/{name} {} run requested ({requested_at})\n",
        kopiur_api::GROUP,
        mode.label()
    );
    if !args.wait {
        return Ok(CmdOutput::ok(requested_line));
    }

    eprint!("{requested_line}");
    let timeout = args.timeout.unwrap_or(DEFAULT_WAIT_TIMEOUT);
    let requested_for_check = requested_at.clone();
    let verdict = wait_for(
        &api,
        &name,
        format!("maintenance {name} manual run"),
        format!(
            "watch it with `kubectl get maintenance {name} -n {ns} -o jsonpath='{{.status.manualRun}}'`, \
             or raise --timeout"
        ),
        timeout,
        move |m: &Maintenance| answered(m, &requested_for_check),
    )
    .await;

    match verdict? {
        Ok(done) => {
            let completed = done
                .status
                .as_ref()
                .and_then(|s| s.manual_run.as_ref())
                .and_then(|m| m.completed_at.clone())
                .unwrap_or_default();
            // Honesty: a Job that YIELDED the lease succeeded without running
            // any maintenance — the user explicitly asked for a run, so say so.
            if let Some(note) = yield_note(&done) {
                eprintln!("{note}");
            }
            Ok(CmdOutput::ok(format!(
                "maintenance {name} {} run completed at {completed}\n",
                mode.label()
            )))
        }
        Err(failed) => {
            eprint!("{}", failure_detail(&failed, &requested_at));
            Ok(CmdOutput {
                text: String::new(),
                exit: 1,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn maint(v: serde_json::Value) -> Maintenance {
        serde_json::from_value(v).unwrap()
    }

    fn base(name: &str, repo_kind: &str, repo: &str) -> Maintenance {
        maint(serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Maintenance",
            "metadata": { "name": name, "namespace": "media" },
            "spec": {
                "repository": { "kind": repo_kind, "name": repo },
                "schedule": { "quick": { "cron": "0 */6 * * *" }, "full": { "cron": "0 3 * * *" } },
                "ownership": { "owner": "kopiur" }
            }
        }))
    }

    #[test]
    fn covers_repository_matches_kind_and_name() {
        let m = base("m", "Repository", "nas");
        assert!(covers_repository(&m, RepositoryKind::Repository, "nas"));
        assert!(!covers_repository(&m, RepositoryKind::Repository, "other"));
        assert!(!covers_repository(
            &m,
            RepositoryKind::ClusterRepository,
            "nas"
        ));
        let c = base("m", "ClusterRepository", "shared");
        assert!(covers_repository(
            &c,
            RepositoryKind::ClusterRepository,
            "shared"
        ));
    }

    #[test]
    fn run_patch_always_sets_both_annotations() {
        // A stale run-mode=full must never leak into a later quick request.
        let p = run_patch("2026-06-11T12:00:00Z", ManualRunMode::Quick);
        assert_eq!(
            p["metadata"]["annotations"]["kopiur.home-operations.com/run-requested"],
            "2026-06-11T12:00:00Z"
        );
        assert_eq!(
            p["metadata"]["annotations"]["kopiur.home-operations.com/run-mode"],
            "quick"
        );
        let p = run_patch("2026-06-11T12:00:00Z", ManualRunMode::Full);
        assert_eq!(
            p["metadata"]["annotations"]["kopiur.home-operations.com/run-mode"],
            "full"
        );
    }

    #[test]
    fn answered_matches_only_this_request_and_is_exhaustive() {
        let with_manual = |requested: &str, phase: &str| {
            let mut m = base("m", "Repository", "nas");
            m.status = Some(kopiur_api::MaintenanceStatus {
                manual_run: Some(
                    serde_json::from_value(serde_json::json!({
                        "requestedAt": requested,
                        "mode": "quick",
                        "phase": phase
                    }))
                    .unwrap(),
                ),
                ..Default::default()
            });
            m
        };
        let ts = "2026-06-11T12:00:00Z";
        assert!(answered(&with_manual(ts, "Running"), ts).is_none());
        assert!(matches!(
            answered(&with_manual(ts, "Succeeded"), ts),
            Some(Ok(_))
        ));
        assert!(matches!(
            answered(&with_manual(ts, "Failed"), ts),
            Some(Err(_))
        ));
        // A PREVIOUS request's terminal answer must not satisfy this one.
        assert!(answered(&with_manual("2026-06-11T11:00:00Z", "Succeeded"), ts).is_none());
        // No manual status at all: keep waiting.
        assert!(answered(&base("m", "Repository", "nas"), ts).is_none());
    }

    #[test]
    fn failure_detail_carries_the_condition_reason() {
        let mut m = base("m", "Repository", "nas");
        m.status = Some(kopiur_api::MaintenanceStatus {
            conditions: vec![k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition {
                type_: "Ready".into(),
                status: "False".into(),
                reason: "MaintenanceFailed".into(),
                message: "manual maintenance Job failed; see the Job/pod logs".into(),
                last_transition_time: k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                    "2026-06-11T12:00:00Z".parse().unwrap(),
                ),
                observed_generation: None,
            }],
            ..Default::default()
        });
        let detail = failure_detail(&m, "2026-06-11T12:00:00Z");
        assert!(
            detail.contains("manual run (requested 2026-06-11T12:00:00Z) failed"),
            "{detail}"
        );
        assert!(detail.contains("MaintenanceFailed"), "{detail}");
        assert!(
            detail.contains("kopiur.home-operations.com/maintenance=m"),
            "{detail}"
        );
    }
}
