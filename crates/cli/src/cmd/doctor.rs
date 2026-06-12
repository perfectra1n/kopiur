//! `kubectl kopiur doctor` — diagnose an installation: CRDs, operator and
//! webhook health (including a live admission probe), repository readiness,
//! credential Secrets, stuck work, and recent warnings. Every check lands in a
//! closed enum and renders Pass / Warn / Fail{what, why, fix}; the exit code is
//! 1 iff anything failed. RBAC the user lacks degrades a check to Warn (with
//! the missing grant named), never a crash.

use chrono::{DateTime, Utc};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::api::events::v1::Event;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kopiur_api::common::RepositoryKind;
use kopiur_api::creds::mover_creds_secret_refs;
use kopiur_api::{
    ClusterRepository, Repository, Restore, RestorePhase, Snapshot, SnapshotPhase, SnapshotPolicy,
};
use kube::ResourceExt;
use kube::api::{Api, ListParams, PostParams};
use kube::core::CustomResourceExt as KubeCustomResourceExt;
use serde::Serialize;

use crate::cli::DoctorArgs;
use crate::context::{KubeCtx, Scope};
use crate::error::CliError;
use crate::output::OutputFormat;

/// Every check doctor performs. Closed enum: adding a check forces the runner
/// and the renderer to handle it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DoctorCheck {
    /// All 8 kopiur CRDs are installed and serve `v1alpha1`.
    CrdsInstalled,
    /// The controller Deployment exists and has ready replicas.
    ControllerRunning,
    /// The webhook Deployment exists and has ready replicas (when installed).
    WebhookRunning,
    /// A live dry-run admission probe: an invalid SnapshotPolicy must be denied.
    WebhookAdmits,
    /// Every Repository/ClusterRepository is phase Ready.
    RepositoriesReady,
    /// Every repository's credential Secret(s) resolve.
    CredentialsPresent,
    /// No Snapshot/Restore has been non-terminal longer than the threshold.
    NoStuckWork,
    /// No recent Warning events on kopiur objects.
    RecentWarnings,
}

impl DoctorCheck {
    /// Human title for the report line.
    pub fn title(self) -> &'static str {
        match self {
            Self::CrdsInstalled => "CRDs installed",
            Self::ControllerRunning => "controller running",
            Self::WebhookRunning => "webhook running",
            Self::WebhookAdmits => "webhook admission (live dry-run probe)",
            Self::RepositoriesReady => "repositories ready",
            Self::CredentialsPresent => "credential secrets present",
            Self::NoStuckWork => "no stuck snapshots/restores",
            Self::RecentWarnings => "recent warning events",
        }
    }
}

/// Outcome of one check.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Outcome {
    /// All good.
    Pass,
    /// Could not fully verify, or a non-fatal observation; message says why.
    Warn(String),
    /// Something is wrong; what/why/fix.
    Fail {
        /// What is broken.
        what: String,
        /// Why it matters / why it happens.
        why: String,
        /// How to fix it.
        fix: String,
    },
}

/// One line of the doctor report.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckResult {
    /// Which check.
    pub check: DoctorCheck,
    /// What happened.
    pub outcome: Outcome,
}

/// The full report (`-o json|yaml` emits this).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DoctorReport {
    /// All check results, in run order.
    pub checks: Vec<CheckResult>,
}

impl DoctorReport {
    /// Exit code: 1 iff any check failed (warnings don't fail the run).
    pub fn exit_code(&self) -> u8 {
        let failed = self
            .checks
            .iter()
            .any(|c| matches!(c.outcome, Outcome::Fail { .. }));
        u8::from(failed)
    }
}

/// Render the human report. Pure.
pub fn render(report: &DoctorReport) -> String {
    let mut out = String::new();
    for result in &report.checks {
        let line = match &result.outcome {
            Outcome::Pass => format!("  ok    {}\n", result.check.title()),
            Outcome::Warn(msg) => format!("  warn  {}: {}\n", result.check.title(), msg),
            Outcome::Fail { what, why, fix } => format!(
                "  FAIL  {}: {}\n        why: {}\n        fix: {}\n",
                result.check.title(),
                what,
                why,
                fix
            ),
        };
        out.push_str(&line);
    }
    let failed = report
        .checks
        .iter()
        .filter(|c| matches!(c.outcome, Outcome::Fail { .. }))
        .count();
    let warned = report
        .checks
        .iter()
        .filter(|c| matches!(c.outcome, Outcome::Warn(_)))
        .count();
    out.push_str(&format!(
        "\n{} check(s): {} failed, {} warning(s)\n",
        report.checks.len(),
        failed,
        warned
    ));
    out
}

/// Map a kube error on an optional check to a Warn naming the missing access.
fn warn_for(verb: &str, resource: &str, e: &kube::Error) -> Outcome {
    match e {
        kube::Error::Api(ae) if ae.code == 403 => Outcome::Warn(format!(
            "cannot {verb} {resource} (RBAC); grant `{verb}` on `{resource}` or run \
             with a more privileged kubeconfig to enable this check"
        )),
        other => Outcome::Warn(format!("cannot {verb} {resource}: {other}")),
    }
}

/// The 8 CRD names doctor expects, from the same types the plugin is built
/// against (so "installed" means "this plugin's schema vintage exists").
fn expected_crds() -> Vec<(String, CustomResourceDefinition)> {
    fn entry<K: KubeCustomResourceExt>() -> (String, CustomResourceDefinition) {
        let crd = K::crd();
        (crd.metadata.name.clone().unwrap_or_default(), crd)
    }
    vec![
        entry::<Repository>(),
        entry::<ClusterRepository>(),
        entry::<SnapshotPolicy>(),
        entry::<Snapshot>(),
        entry::<kopiur_api::SnapshotSchedule>(),
        entry::<Restore>(),
        entry::<kopiur_api::Maintenance>(),
        entry::<kopiur_api::RepositoryReplication>(),
    ]
}

async fn check_crds(ctx: &KubeCtx) -> Outcome {
    let api: Api<CustomResourceDefinition> = Api::all(ctx.client.clone());
    let mut missing = Vec::new();
    for (name, _) in expected_crds() {
        match api.get_opt(&name).await {
            Ok(Some(crd)) => {
                let serves_v1alpha1 = crd
                    .spec
                    .versions
                    .iter()
                    .any(|v| v.name == kopiur_api::VERSION && v.served);
                if !serves_v1alpha1 {
                    return Outcome::Fail {
                        what: format!("CRD {name} does not serve {}", kopiur_api::VERSION),
                        why: "this plugin (and the operator) speak v1alpha1 only".into(),
                        fix: "upgrade/reinstall the kopiur CRDs (helm upgrade, or apply deploy/crds/)"
                            .into(),
                    };
                }
            }
            Ok(None) => missing.push(name),
            Err(e) => return warn_for("get", "customresourcedefinitions", &e),
        }
    }
    if missing.is_empty() {
        Outcome::Pass
    } else {
        Outcome::Fail {
            what: format!("missing CRD(s): {}", missing.join(", ")),
            why: "without the CRDs the API server rejects every kopiur object".into(),
            fix:
                "install kopiur (helm install kopiur oci://ghcr.io/home-operations/charts/kopiur) \
                  or apply deploy/crds/"
                    .into(),
        }
    }
}

/// Find kopiur Deployments by the chart's labels, across namespaces. Returns
/// the outcome plus whether the Deployment EXISTS at all (the admission probe
/// gates on existence — a present-but-unready webhook should be probed, since
/// that is exactly when "failed calling webhook" surfaces).
async fn check_deployment(ctx: &KubeCtx, component: &str, required: bool) -> (Outcome, bool) {
    let api: Api<Deployment> = Api::all(ctx.client.clone());
    let selector = format!("app.kubernetes.io/name=kopiur,app.kubernetes.io/component={component}");
    let listed = match api.list(&ListParams::default().labels(&selector)).await {
        Ok(l) => l,
        Err(e) => return (warn_for("list", "deployments", &e), true),
    };
    let Some(deploy) = listed.items.first() else {
        if !required {
            return (
                Outcome::Warn(format!(
                    "no {component} Deployment found (label {selector}); skipped if not installed"
                )),
                false,
            );
        }
        return (
            Outcome::Fail {
                what: format!("no {component} Deployment found (label {selector})"),
                why: "without the controller nothing reconciles — backups will not run".into(),
                fix: "install kopiur (helm install …) or check the release's namespace".into(),
            },
            false,
        );
    };
    let ready = deploy
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);
    let outcome = if ready >= 1 {
        Outcome::Pass
    } else {
        Outcome::Fail {
            what: format!(
                "{component} Deployment {}/{} has 0 ready replicas",
                deploy.metadata.namespace.clone().unwrap_or_default(),
                deploy.name_any()
            ),
            why: "the pods are not Ready (crash loop, image pull, scheduling, …)".into(),
            fix: format!(
                "kubectl -n {} describe deploy/{} and check the pod events/logs",
                deploy.metadata.namespace.clone().unwrap_or_default(),
                deploy.name_any()
            ),
        }
    };
    (outcome, true)
}

/// Live admission probe: a dry-run create of a deliberately-invalid
/// SnapshotPolicy. Denied = the webhook intercepts (healthy); admitted = it is
/// not intercepting; transport error = broken wiring. Zero cluster mutation
/// (server-side dryRun).
async fn check_webhook_admission(ctx: &KubeCtx, webhook_installed: bool) -> Outcome {
    if !webhook_installed {
        return Outcome::Warn(
            "webhook not installed; admission-time validation is off (the controller still \
             validates defensively)"
                .into(),
        );
    }
    let ns = ctx.namespace.as_str();
    let api: Api<SnapshotPolicy> = Api::namespaced(ctx.client.clone(), ns);
    // Invalid on purpose: a ClusterRepository ref must not carry a namespace
    // (api::validate refuses it; shared by webhook and controller).
    let invalid: SnapshotPolicy = serde_json::from_value(serde_json::json!({
        "apiVersion": kopiur_api::consts::API_VERSION,
        "kind": "SnapshotPolicy",
        "metadata": { "name": "kopiur-doctor-probe", "namespace": ns },
        "spec": {
            "repository": { "kind": "ClusterRepository", "name": "x", "namespace": "not-allowed" },
            "sources": [ { "pvc": { "name": "x" } } ]
        }
    }))
    .expect("probe fixture");
    let params = PostParams {
        dry_run: true,
        field_manager: Some(crate::consts::FIELD_MANAGER.to_string()),
    };
    match api.create(&params, &invalid).await {
        // Denied BY KOPIUR's webhook: reachable and validating. Healthy. (The
        // apiserver names the denying webhook; a Kyverno/OPA denial must not
        // mask a broken kopiur webhook behind failurePolicy: Ignore.)
        Err(kube::Error::Api(ae))
            if ae.message.contains("denied the request") && ae.message.contains("kopiur") =>
        {
            Outcome::Pass
        }
        Err(kube::Error::Api(ae)) if ae.message.contains("denied the request") => {
            Outcome::Warn(format!(
                "the probe was denied by a NON-kopiur webhook, so kopiur's own validation \
                 could not be confirmed: {}",
                ae.message
            ))
        }
        // Admitted: the webhook did NOT intercept an invalid object.
        Ok(_) => Outcome::Fail {
            what: "an invalid SnapshotPolicy passed admission (dry-run)".into(),
            why: "the validating webhook is not intercepting kopiur objects — bad specs will \
                  land and fail later at reconcile time"
                .into(),
            fix: "check the ValidatingWebhookConfiguration, the webhook Service endpoints, and \
                  the webhook pod logs"
                .into(),
        },
        // Webhook wired but unreachable: the failurePolicy surfaces as an error.
        Err(kube::Error::Api(ae)) if ae.message.contains("failed calling webhook") => {
            Outcome::Fail {
                what: "the API server cannot call the kopiur webhook".into(),
                why: format!(
                    "admission requests error instead of validating: {}",
                    ae.message
                ),
                fix: "check the webhook Service/EndpointSlices, the CA bundle, and the webhook \
                      pod (kubectl -n <ns> logs deploy/<release>-webhook)"
                    .into(),
            }
        }
        Err(kube::Error::Api(ae)) if ae.code == 403 => Outcome::Warn(
            "cannot dry-run create snapshotpolicies (RBAC); grant `create` (dryRun) to enable \
             the admission probe"
                .into(),
        ),
        Err(e) => Outcome::Warn(format!("admission probe inconclusive: {e}")),
    }
}

struct RepoSummary {
    kind: RepositoryKind,
    name: String,
    namespace: Option<String>,
    phase: Option<String>,
    ready_message: Option<String>,
    backend: kopiur_api::Backend,
    encryption: kopiur_api::common::Encryption,
}

async fn list_repos(ctx: &KubeCtx) -> Result<Vec<RepoSummary>, Outcome> {
    use kopiur_api::common::PhaseLabel;
    let mut repos = Vec::new();
    let api: Api<Repository> = match &ctx.scope {
        Scope::All => Api::all(ctx.client.clone()),
        Scope::Namespace(ns) => Api::namespaced(ctx.client.clone(), ns),
    };
    match api.list(&ListParams::default()).await {
        Ok(listed) => {
            for r in listed.items {
                repos.push(RepoSummary {
                    kind: RepositoryKind::Repository,
                    name: r.name_any(),
                    namespace: r.metadata.namespace.clone(),
                    phase: r
                        .status
                        .as_ref()
                        .and_then(|s| s.phase)
                        .map(|p| p.label().to_string()),
                    ready_message: r
                        .status
                        .as_ref()
                        .map(|s| s.conditions.as_slice())
                        .unwrap_or_default()
                        .iter()
                        .find(|c| c.type_ == kopiur_api::consts::READY_CONDITION)
                        .map(|c| c.message.clone()),
                    backend: r.spec.backend.clone(),
                    encryption: r.spec.encryption.clone(),
                });
            }
        }
        Err(e) => return Err(warn_for("list", "repositories", &e)),
    }
    let api: Api<ClusterRepository> = Api::all(ctx.client.clone());
    match api.list(&ListParams::default()).await {
        Ok(listed) => {
            for r in listed.items {
                repos.push(RepoSummary {
                    kind: RepositoryKind::ClusterRepository,
                    name: r.name_any(),
                    namespace: None,
                    phase: r
                        .status
                        .as_ref()
                        .and_then(|s| s.phase)
                        .map(|p| p.label().to_string()),
                    ready_message: r
                        .status
                        .as_ref()
                        .map(|s| s.conditions.as_slice())
                        .unwrap_or_default()
                        .iter()
                        .find(|c| c.type_ == kopiur_api::consts::READY_CONDITION)
                        .map(|c| c.message.clone()),
                    backend: r.spec.backend.clone(),
                    encryption: r.spec.encryption.clone(),
                });
            }
        }
        Err(e) => return Err(warn_for("list", "clusterrepositories", &e)),
    }
    Ok(repos)
}

fn check_repos_ready(repos: &[RepoSummary]) -> Outcome {
    let not_ready: Vec<String> = repos
        .iter()
        .filter(|r| r.phase.as_deref() != Some("Ready"))
        .map(|r| {
            format!(
                "{:?}/{} ({}{})",
                r.kind,
                r.name,
                r.phase.as_deref().unwrap_or("no status"),
                r.ready_message
                    .as_deref()
                    .map(|m| format!(": {m}"))
                    .unwrap_or_default()
            )
        })
        .collect();
    if not_ready.is_empty() {
        Outcome::Pass
    } else {
        Outcome::Fail {
            what: format!("repositories not Ready: {}", not_ready.join("; ")),
            why: "backups/restores against an unready repository cannot run".into(),
            fix: "the Ready condition message above is the operator's diagnosis; \
                  `kubectl describe` the repository for events"
                .into(),
        }
    }
}

async fn check_credentials(ctx: &KubeCtx, repos: &[RepoSummary]) -> Outcome {
    let mut missing = Vec::new();
    let mut unverifiable = Vec::new();
    for repo in repos {
        let default_ns = repo.namespace.as_deref();
        for cred in mover_creds_secret_refs(&repo.backend, &repo.encryption, default_ns) {
            let Some(ns) = cred.namespace.clone().or(default_ns.map(str::to_string)) else {
                missing.push(format!(
                    "{:?}/{}: secret {:?} has no resolvable namespace (a ClusterRepository \
                     reference must pin one)",
                    repo.kind, repo.name, cred.name
                ));
                continue;
            };
            let api: Api<Secret> = Api::namespaced(ctx.client.clone(), &ns);
            match api.get_opt(&cred.name).await {
                Ok(Some(_)) => {}
                Ok(None) => missing.push(format!(
                    "{:?}/{}: secret {}/{} not found",
                    repo.kind, repo.name, ns, cred.name
                )),
                // Don't let one unreadable Secret discard confirmed misses.
                Err(e) => unverifiable.push(format!("{}/{}: {e}", ns, cred.name)),
            }
        }
    }
    if missing.is_empty() && !unverifiable.is_empty() {
        return Outcome::Warn(format!(
            "could not verify {} secret(s): {}",
            unverifiable.len(),
            unverifiable.join("; ")
        ));
    }
    if missing.is_empty() {
        Outcome::Pass
    } else {
        Outcome::Fail {
            what: format!("missing credential Secret(s): {}", missing.join("; ")),
            why: "movers load credentials via namespace-local envFrom; a missing Secret \
                  fails every run against that repository"
                .into(),
            fix: "create the Secret in the named namespace (or enable credentialProjection \
                  where supported)"
                .into(),
        }
    }
}

async fn check_stuck(ctx: &KubeCtx, threshold: std::time::Duration, now: DateTime<Utc>) -> Outcome {
    fn age_of(meta: &kube::core::ObjectMeta, now: DateTime<Utc>) -> Option<chrono::Duration> {
        let t = meta.creation_timestamp.as_ref()?;
        let created = DateTime::from_timestamp(t.0.as_second(), 0)?;
        Some(now - created)
    }
    let threshold_label = format!("{}s", threshold.as_secs());
    let threshold = chrono::Duration::from_std(threshold).unwrap_or(chrono::Duration::hours(1));
    let mut stuck = Vec::new();

    let api: Api<Snapshot> = match &ctx.scope {
        Scope::All => Api::all(ctx.client.clone()),
        Scope::Namespace(ns) => Api::namespaced(ctx.client.clone(), ns),
    };
    match api.list(&ListParams::default()).await {
        Ok(listed) => {
            for s in listed.items {
                let in_flight = matches!(
                    s.status.as_ref().and_then(|st| st.phase),
                    Some(SnapshotPhase::Pending | SnapshotPhase::Running) | None
                );
                if in_flight && age_of(&s.metadata, now).is_some_and(|a| a > threshold) {
                    stuck.push(format!(
                        "snapshot {}/{}",
                        s.metadata.namespace.clone().unwrap_or_default(),
                        s.name_any()
                    ));
                }
            }
        }
        Err(e) => return warn_for("list", "snapshots", &e),
    }
    let api: Api<Restore> = match &ctx.scope {
        Scope::All => Api::all(ctx.client.clone()),
        Scope::Namespace(ns) => Api::namespaced(ctx.client.clone(), ns),
    };
    match api.list(&ListParams::default()).await {
        Ok(listed) => {
            for r in listed.items {
                let in_flight = matches!(
                    r.status.as_ref().and_then(|st| st.phase),
                    Some(RestorePhase::Pending | RestorePhase::Resolving | RestorePhase::Restoring)
                        | None
                );
                if in_flight && age_of(&r.metadata, now).is_some_and(|a| a > threshold) {
                    stuck.push(format!(
                        "restore {}/{}",
                        r.metadata.namespace.clone().unwrap_or_default(),
                        r.name_any()
                    ));
                }
            }
        }
        Err(e) => return warn_for("list", "restores", &e),
    }
    if stuck.is_empty() {
        Outcome::Pass
    } else {
        Outcome::Fail {
            what: format!(
                "non-terminal for longer than {} (measured from creation): {}",
                threshold_label,
                stuck.join("; ")
            ),
            why: "a Snapshot/Restore should reach a terminal phase; a long Pending/Running \
                  usually means an unschedulable mover pod, missing PVC, or unreachable backend"
                .into(),
            fix: "kubectl kopiur logs snapshot|restore <name> and `kubectl describe` the \
                  object for its conditions/events; if this is a legitimately long run \
                  (e.g. a large initial backup), raise --stuck-threshold"
                .into(),
        }
    }
}

async fn check_warnings(ctx: &KubeCtx, now: DateTime<Utc>) -> Outcome {
    let api: Api<Event> = match &ctx.scope {
        Scope::All => Api::all(ctx.client.clone()),
        Scope::Namespace(ns) => Api::namespaced(ctx.client.clone(), ns),
    };
    let listed = match api
        .list(&ListParams::default().fields("type=Warning"))
        .await
    {
        Ok(l) => l,
        Err(e) => return warn_for("list", "events.events.k8s.io", &e),
    };
    let cutoff = now - chrono::Duration::hours(1);
    let mut recent: Vec<String> = listed
        .items
        .iter()
        .filter(|e| {
            e.regarding
                .as_ref()
                .and_then(|r| r.api_version.as_deref())
                .map(|v| v.starts_with(kopiur_api::GROUP))
                .unwrap_or(false)
        })
        .filter(|e| {
            let at = e
                .series
                .as_ref()
                .map(|s| s.last_observed_time.0)
                .or_else(|| e.event_time.as_ref().map(|t| t.0))
                // core/v1-emitted aggregated events: lastTimestamp tracks
                // recurrence; creationTimestamp is only the FIRST occurrence.
                .or_else(|| e.deprecated_last_timestamp.as_ref().map(|t| t.0))
                .or_else(|| e.metadata.creation_timestamp.as_ref().map(|t| t.0));
            at.and_then(|t| DateTime::from_timestamp(t.as_second(), 0))
                .is_some_and(|t| t > cutoff)
        })
        .map(|e| {
            format!(
                "{} {}/{}: {}",
                e.reason.clone().unwrap_or_default(),
                e.regarding
                    .as_ref()
                    .and_then(|r| r.namespace.clone())
                    .unwrap_or_default(),
                e.regarding
                    .as_ref()
                    .and_then(|r| r.name.clone())
                    .unwrap_or_default(),
                e.note.clone().unwrap_or_default()
            )
        })
        .collect();
    recent.sort();
    recent.dedup();
    if recent.is_empty() {
        Outcome::Pass
    } else {
        // Warnings are informational here — the specific checks above turn the
        // actionable ones into Fails; this is the catch-all surface.
        Outcome::Warn(format!(
            "{} warning(s) on kopiur objects in the last hour: {}",
            recent.len(),
            recent.join(" | ")
        ))
    }
}

/// Run all checks in order.
pub async fn run(
    ctx: &KubeCtx,
    args: &DoctorArgs,
    output: OutputFormat,
    now: DateTime<Utc>,
) -> Result<crate::CmdOutput, CliError> {
    let mut checks = Vec::new();
    checks.push(CheckResult {
        check: DoctorCheck::CrdsInstalled,
        outcome: check_crds(ctx).await,
    });
    let (controller, _) = check_deployment(ctx, "controller", true).await;
    checks.push(CheckResult {
        check: DoctorCheck::ControllerRunning,
        outcome: controller,
    });
    let (webhook, webhook_installed) = check_deployment(ctx, "webhook", false).await;
    checks.push(CheckResult {
        check: DoctorCheck::WebhookRunning,
        outcome: webhook,
    });
    checks.push(CheckResult {
        check: DoctorCheck::WebhookAdmits,
        outcome: check_webhook_admission(ctx, webhook_installed).await,
    });
    match list_repos(ctx).await {
        Ok(repos) => {
            checks.push(CheckResult {
                check: DoctorCheck::RepositoriesReady,
                outcome: check_repos_ready(&repos),
            });
            checks.push(CheckResult {
                check: DoctorCheck::CredentialsPresent,
                outcome: check_credentials(ctx, &repos).await,
            });
        }
        Err(warn) => {
            checks.push(CheckResult {
                check: DoctorCheck::RepositoriesReady,
                outcome: warn,
            });
            checks.push(CheckResult {
                check: DoctorCheck::CredentialsPresent,
                outcome: Outcome::Warn("skipped (repositories not listable)".into()),
            });
        }
    }
    checks.push(CheckResult {
        check: DoctorCheck::NoStuckWork,
        outcome: check_stuck(ctx, args.stuck_threshold, now).await,
    });
    checks.push(CheckResult {
        check: DoctorCheck::RecentWarnings,
        outcome: check_warnings(ctx, now).await,
    });

    let report = DoctorReport { checks };
    let exit = report.exit_code();
    let text = match output {
        OutputFormat::Table | OutputFormat::Wide => render(&report),
        OutputFormat::Yaml => {
            let value = serde_json::to_value(&report).map_err(|e| CliError::Serialization {
                what: "doctor report",
                source: e.into(),
            })?;
            serde_yaml::to_string(&value).map_err(|e| CliError::Serialization {
                what: "doctor report",
                source: e.into(),
            })?
        }
        OutputFormat::Json => {
            let mut s =
                serde_json::to_string_pretty(&report).map_err(|e| CliError::Serialization {
                    what: "doctor report",
                    source: e.into(),
                })?;
            s.push('\n');
            s
        }
        OutputFormat::Name => {
            return Err(CliError::Serialization {
                what: "doctor report as -o name (doctor is a report, not a resource; use -o json)",
                source: Box::new(std::io::Error::other("unsupported output format")),
            });
        }
    };
    Ok(crate::CmdOutput { text, exit })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(outcomes: Vec<Outcome>) -> DoctorReport {
        DoctorReport {
            checks: outcomes
                .into_iter()
                .map(|outcome| CheckResult {
                    check: DoctorCheck::CrdsInstalled,
                    outcome,
                })
                .collect(),
        }
    }

    #[test]
    fn exit_code_is_one_iff_any_fail() {
        assert_eq!(report(vec![Outcome::Pass]).exit_code(), 0);
        assert_eq!(
            report(vec![Outcome::Pass, Outcome::Warn("x".into())]).exit_code(),
            0,
            "warnings must not fail the run"
        );
        assert_eq!(
            report(vec![
                Outcome::Pass,
                Outcome::Fail {
                    what: "w".into(),
                    why: "y".into(),
                    fix: "f".into()
                }
            ])
            .exit_code(),
            1
        );
    }

    #[test]
    fn render_carries_what_why_fix_for_failures() {
        let text = render(&report(vec![
            Outcome::Pass,
            Outcome::Warn("cannot list deployments (RBAC)".into()),
            Outcome::Fail {
                what: "missing CRD(s): snapshots.kopiur.home-operations.com".into(),
                why: "without the CRDs the API server rejects every kopiur object".into(),
                fix: "install kopiur".into(),
            },
        ]));
        assert!(text.contains("ok    CRDs installed"), "{text}");
        assert!(
            text.contains("warn  CRDs installed: cannot list deployments"),
            "{text}"
        );
        assert!(
            text.contains("FAIL  CRDs installed: missing CRD(s)"),
            "{text}"
        );
        assert!(text.contains("why: without the CRDs"), "{text}");
        assert!(text.contains("fix: install kopiur"), "{text}");
        assert!(
            text.contains("3 check(s): 1 failed, 1 warning(s)"),
            "{text}"
        );
    }

    #[test]
    fn rbac_misses_degrade_to_warn_with_the_grant_named() {
        let e = kube::Error::Api(
            kube::core::Status::failure("forbidden", "Forbidden")
                .with_code(403)
                .boxed(),
        );
        let Outcome::Warn(msg) = warn_for("list", "secrets", &e) else {
            panic!("403 must degrade to Warn");
        };
        assert!(msg.contains("grant `list` on `secrets`"), "{msg}");
    }

    #[test]
    fn expected_crds_covers_all_eight_kinds() {
        let names: Vec<String> = expected_crds().into_iter().map(|(n, _)| n).collect();
        assert_eq!(names.len(), 8);
        for n in &names {
            assert!(n.ends_with(".kopiur.home-operations.com"), "{n}");
        }
    }
}
