//! `kubectl kopiur status` — a one-screen health overview: repositories (with
//! readiness detail), policies (last snapshot / last verify), schedules (last /
//! next fire), and in-flight or stalled work.

use chrono::{DateTime, Utc};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kopiur_api::common::{PhaseLabel, RepositoryKind};
use kopiur_api::consts::{MAINTENANCE_CONFIGURED_CONDITION, READY_CONDITION, STALLED_CONDITION};
use kopiur_api::{
    ClusterRepository, Repository, Restore, RestorePhase, Snapshot, SnapshotPhase, SnapshotPolicy,
    SnapshotSchedule,
};
use kube::ResourceExt;
use kube::api::{Api, ListParams};
use serde::Serialize;

use crate::cli::StatusArgs;
use crate::cmd::snapshots::{RepoFilter, matches_repository, resolve_repo_filter_for};
use crate::context::{KubeCtx, Scope};
use crate::error::{CliError, classify_kube};
use crate::output::{EMPTY_CELL, OutputFormat, Table, human_age};

/// The typed report `-o yaml|json` emits (and the table renders).
#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct StatusReport {
    /// Repositories (namespaced + cluster-scoped), with readiness detail.
    pub repositories: Vec<RepoRow>,
    /// SnapshotPolicies with their last snapshot / verification.
    pub policies: Vec<PolicyRow>,
    /// SnapshotSchedules with firing detail.
    pub schedules: Vec<ScheduleRow>,
    /// Snapshots/Restores currently in a non-terminal phase.
    pub in_flight: InFlight,
    /// Objects reporting the kstatus `Stalled=True` condition.
    pub stalled: Vec<StalledRow>,
}

/// One repository line.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepoRow {
    /// `Repository` or `ClusterRepository`.
    pub kind: &'static str,
    /// Object name.
    pub name: String,
    /// Namespace; absent for ClusterRepository.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// `status.phase` as reported.
    pub phase: String,
    /// Backend discriminant (`S3`, `Filesystem`, …).
    pub backend: String,
    /// `spec.mode` (ReadWrite/ReadOnly).
    pub mode: String,
    /// `spec.suspend`.
    pub suspended: bool,
    /// Whether a Maintenance covers it (`MaintenanceConfigured` condition).
    pub maintenance: String,
    /// The `Ready` condition message when the repo is NOT Ready.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub problem: Option<String>,
}

/// One policy line.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyRow {
    /// Object name.
    pub name: String,
    /// Namespace.
    pub namespace: String,
    /// The referenced repository (`kind/name`).
    pub repository: String,
    /// `spec.suspend`.
    pub suspended: bool,
    /// `status.lastSuccessfulSnapshot` (RFC3339).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_snapshot: Option<String>,
    /// `status.lastVerified` (RFC3339).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_verified: Option<String>,
}

/// One schedule line.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleRow {
    /// Object name.
    pub name: String,
    /// Namespace.
    pub namespace: String,
    /// `policyRef.name` or `selector` for policySelector fan-out.
    pub policy: String,
    /// The cron expression.
    pub cron: String,
    /// `spec.schedule.suspend`.
    pub suspended: bool,
    /// `status.lastSchedule.at`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_fire: Option<String>,
    /// `status.nextSchedule.at`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_fire: Option<String>,
    /// `status.consecutiveFailures` when non-zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consecutive_failures: Option<i64>,
}

/// Counts of non-terminal work.
#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InFlight {
    /// Snapshots in Pending/Running.
    pub snapshots: usize,
    /// Restores in Pending/Resolving/Restoring.
    pub restores: usize,
}

/// One stalled object (kstatus `Stalled=True`).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StalledRow {
    /// Kind of the stalled object.
    pub kind: &'static str,
    /// `namespace/name`.
    pub object: String,
    /// The Stalled condition's message.
    pub message: String,
}

fn condition<'a>(conditions: &'a [Condition], type_: &str) -> Option<&'a Condition> {
    conditions.iter().find(|c| c.type_ == type_)
}

/// Is a Snapshot non-terminal? Exhaustive.
fn snapshot_in_flight(phase: Option<SnapshotPhase>) -> bool {
    match phase {
        Some(SnapshotPhase::Pending | SnapshotPhase::Running) | None => true,
        Some(
            SnapshotPhase::Succeeded
            | SnapshotPhase::Failed
            | SnapshotPhase::Deleting
            | SnapshotPhase::Discovered,
        ) => false,
    }
}

/// Is a Restore non-terminal? Exhaustive.
fn restore_in_flight(phase: Option<RestorePhase>) -> bool {
    match phase {
        Some(RestorePhase::Pending | RestorePhase::Resolving | RestorePhase::Restoring) | None => {
            true
        }
        Some(RestorePhase::Completed | RestorePhase::Failed) => false,
    }
}

/// Does this repository row match the resolved `--repository` filter?
/// Namespace-aware: two Repositories named `nas` in different namespaces are
/// different repositories.
fn repo_matches(
    filter: Option<&RepoFilter>,
    kind: RepositoryKind,
    name: &str,
    namespace: Option<&str>,
) -> bool {
    match filter {
        None => true,
        Some(f) => {
            f.kind == kind
                && f.name == name
                && match kind {
                    RepositoryKind::ClusterRepository => true,
                    RepositoryKind::Repository => f.namespace.as_deref() == namespace,
                }
        }
    }
}

/// Does a policy's repository ref match the filter? An absent ref namespace
/// means "same as the policy" for a namespaced Repository.
fn policy_matches(filter: Option<&RepoFilter>, policy: &SnapshotPolicy) -> bool {
    let Some(f) = filter else { return true };
    let rref = &policy.spec.repository;
    if rref.kind != f.kind || rref.name != f.name {
        return false;
    }
    match f.kind {
        RepositoryKind::ClusterRepository => true,
        RepositoryKind::Repository => {
            let effective = rref
                .namespace
                .as_deref()
                .or(policy.metadata.namespace.as_deref());
            effective == f.namespace.as_deref()
        }
    }
}

/// Does a Restore belong to the filtered repository? Matches the pinned
/// `status.resolved.repository`, the explicit `spec.repository`, or — for a
/// fromPolicy source — a kept policy `(namespace, name)`.
fn restore_matches(
    filter: Option<&RepoFilter>,
    kept_policies: &std::collections::BTreeSet<(String, String)>,
    restore: &Restore,
) -> bool {
    let Some(f) = filter else { return true };
    let restore_ns = restore.metadata.namespace.as_deref();
    let ref_matches = |rref: &kopiur_api::common::RepositoryRef| {
        rref.kind == f.kind
            && rref.name == f.name
            && match f.kind {
                RepositoryKind::ClusterRepository => true,
                RepositoryKind::Repository => {
                    rref.namespace.as_deref().or(restore_ns) == f.namespace.as_deref()
                }
            }
    };
    if let Some(rref) = restore
        .status
        .as_ref()
        .and_then(|s| s.resolved.as_ref())
        .and_then(|r| r.repository.as_ref())
        && ref_matches(rref)
    {
        return true;
    }
    if let Some(rref) = restore.spec.repository.as_ref()
        && ref_matches(rref)
    {
        return true;
    }
    if let kopiur_api::RestoreSource::FromPolicy(c) = &restore.spec.source {
        let policy_ns = c
            .namespace
            .clone()
            .or_else(|| restore.metadata.namespace.clone())
            .unwrap_or_default();
        return kept_policies.contains(&(policy_ns, c.name.clone()));
    }
    false
}

/// Build the repository row from a typed object's pieces. Pure.
#[allow(clippy::too_many_arguments)]
fn repo_row(
    kind: &'static str,
    name: String,
    namespace: Option<String>,
    phase: Option<String>,
    backend: Option<String>,
    mode: String,
    suspended: bool,
    conditions: &[Condition],
) -> RepoRow {
    let phase = phase.unwrap_or_else(|| EMPTY_CELL.into());
    let maintenance = condition(conditions, MAINTENANCE_CONFIGURED_CONDITION)
        .map(|c| {
            if c.status == "True" {
                "configured".to_string()
            } else {
                c.reason.clone()
            }
        })
        .unwrap_or_else(|| EMPTY_CELL.into());
    let problem = match phase.as_str() {
        "Ready" => None,
        _ => condition(conditions, READY_CONDITION)
            .filter(|c| c.status != "True")
            .map(|c| c.message.clone()),
    };
    RepoRow {
        kind,
        name,
        namespace,
        phase,
        backend: backend.unwrap_or_else(|| EMPTY_CELL.into()),
        mode,
        suspended,
        maintenance,
        problem,
    }
}

/// Gather everything and build the typed report.
async fn gather(
    ctx: &KubeCtx,
    args: &StatusArgs,
    now: DateTime<Utc>,
) -> Result<StatusReport, CliError> {
    let _ = now;
    let mut report = StatusReport::default();

    let repo_filter = match &args.repository {
        None => None,
        Some(name) => Some(
            resolve_repo_filter_for(
                ctx,
                name,
                args.repository_kind.into(),
                args.repository_namespace.as_deref(),
            )
            .await?,
        ),
    };
    let repo_filter = repo_filter.as_ref();

    macro_rules! list {
        ($ty:ty, $kind:literal, $plural:literal) => {{
            let api: Api<$ty> = match &ctx.scope {
                Scope::All => Api::all(ctx.client.clone()),
                Scope::Namespace(ns) => Api::namespaced(ctx.client.clone(), ns),
            };
            let ns = match &ctx.scope {
                Scope::All => None,
                Scope::Namespace(ns) => Some(ns.as_str()),
            };
            api.list(&ListParams::default())
                .await
                .map_err(|e| classify_kube("list", $kind, $plural, ns, None, e))?
                .items
        }};
    }

    // Repositories (namespaced) + ClusterRepositories (always cluster-scoped).
    for repo in list!(Repository, "Repository", "repositories") {
        if !repo_matches(
            repo_filter,
            RepositoryKind::Repository,
            &repo.name_any(),
            repo.metadata.namespace.as_deref(),
        ) {
            continue;
        }
        let status = repo.status.as_ref();
        report.repositories.push(repo_row(
            "Repository",
            repo.name_any(),
            repo.metadata.namespace.clone(),
            status.and_then(|s| s.phase).map(|p| p.label().to_string()),
            status.and_then(|s| s.backend.clone()),
            format!("{:?}", repo.spec.mode),
            repo.spec.suspend,
            status.map(|s| s.conditions.as_slice()).unwrap_or_default(),
        ));
    }
    {
        let api: Api<ClusterRepository> = Api::all(ctx.client.clone());
        let cluster_repos = api
            .list(&ListParams::default())
            .await
            .map_err(|e| {
                classify_kube(
                    "list",
                    "ClusterRepository",
                    "clusterrepositories",
                    None,
                    None,
                    e,
                )
            })?
            .items;
        for repo in cluster_repos {
            if !repo_matches(
                repo_filter,
                RepositoryKind::ClusterRepository,
                &repo.name_any(),
                None,
            ) {
                continue;
            }
            let status = repo.status.as_ref();
            report.repositories.push(repo_row(
                "ClusterRepository",
                repo.name_any(),
                None,
                status.and_then(|s| s.phase).map(|p| p.label().to_string()),
                status.and_then(|s| s.backend.clone()),
                format!("{:?}", repo.spec.mode),
                repo.spec.suspend,
                status.map(|s| s.conditions.as_slice()).unwrap_or_default(),
            ));
        }
    }

    // Policies + their schedules.
    let policies = list!(SnapshotPolicy, "SnapshotPolicy", "snapshotpolicies");
    let kept_policies: Vec<&SnapshotPolicy> = policies
        .iter()
        .filter(|p| policy_matches(repo_filter, p))
        .collect();
    // Keyed by (namespace, name): policyRefs are namespace-local, and two
    // namespaces may both have a policy named `nightly`.
    let kept_keys: std::collections::BTreeSet<(String, String)> = kept_policies
        .iter()
        .map(|p| {
            (
                p.metadata.namespace.clone().unwrap_or_default(),
                p.name_any(),
            )
        })
        .collect();
    for policy in &kept_policies {
        let status = policy.status.as_ref();
        report.policies.push(PolicyRow {
            name: policy.name_any(),
            namespace: policy.metadata.namespace.clone().unwrap_or_default(),
            repository: format!(
                "{:?}/{}",
                policy.spec.repository.kind, policy.spec.repository.name
            ),
            suspended: policy.spec.suspend,
            last_snapshot: status.and_then(|s| s.last_successful_snapshot.clone()),
            last_verified: status.and_then(|s| s.last_verified.clone()),
        });
        if let Some(c) = status
            .map(|s| s.conditions.as_slice())
            .unwrap_or_default()
            .iter()
            .find(|c| c.type_ == STALLED_CONDITION && c.status == "True")
        {
            report.stalled.push(StalledRow {
                kind: "SnapshotPolicy",
                object: format!(
                    "{}/{}",
                    policy.metadata.namespace.clone().unwrap_or_default(),
                    policy.name_any()
                ),
                message: c.message.clone(),
            });
        }
    }
    for schedule in list!(SnapshotSchedule, "SnapshotSchedule", "snapshotschedules") {
        let policy = schedule
            .spec
            .policy_ref
            .as_ref()
            .map(|p| p.name.clone())
            .unwrap_or_else(|| "(selector)".to_string());
        // Under --repository, keep a schedule only when its policy was kept
        // (selector-based schedules are kept — their fan-out is dynamic).
        if repo_filter.is_some()
            && schedule.spec.policy_ref.is_some()
            && !kept_keys.contains(&(
                schedule.metadata.namespace.clone().unwrap_or_default(),
                policy.clone(),
            ))
        {
            continue;
        }
        let status = schedule.status.as_ref();
        report.schedules.push(ScheduleRow {
            name: schedule.name_any(),
            namespace: schedule.metadata.namespace.clone().unwrap_or_default(),
            policy,
            cron: schedule.spec.schedule.cron.clone(),
            suspended: schedule.spec.schedule.suspend,
            last_fire: status
                .and_then(|s| s.last_schedule.as_ref())
                .and_then(|l| l.at.clone()),
            next_fire: status
                .and_then(|s| s.next_schedule.as_ref())
                .and_then(|n| n.at.clone()),
            consecutive_failures: status
                .and_then(|s| s.consecutive_failures)
                .filter(|&n| n > 0),
        });
    }

    // In-flight + stalled work.
    for snap in list!(Snapshot, "Snapshot", "snapshots") {
        let status = snap.status.as_ref();
        // UID-label (discovered) / pinned-resolved-ref (produced) matching —
        // the same logic `snapshots list --repository` uses.
        if let Some(f) = repo_filter
            && !matches_repository(&snap, f)
        {
            continue;
        }
        if snapshot_in_flight(status.and_then(|s| s.phase)) {
            report.in_flight.snapshots += 1;
        }
        if let Some(c) = status
            .map(|s| s.conditions.as_slice())
            .unwrap_or_default()
            .iter()
            .find(|c| c.type_ == STALLED_CONDITION && c.status == "True")
        {
            report.stalled.push(StalledRow {
                kind: "Snapshot",
                object: format!(
                    "{}/{}",
                    snap.metadata.namespace.clone().unwrap_or_default(),
                    snap.name_any()
                ),
                message: c.message.clone(),
            });
        }
    }
    for restore in list!(Restore, "Restore", "restores") {
        if !restore_matches(repo_filter, &kept_keys, &restore) {
            continue;
        }
        let status = restore.status.as_ref();
        if restore_in_flight(status.and_then(|s| s.phase)) {
            report.in_flight.restores += 1;
        }
        if let Some(c) = status
            .map(|s| s.conditions.as_slice())
            .unwrap_or_default()
            .iter()
            .find(|c| c.type_ == STALLED_CONDITION && c.status == "True")
        {
            report.stalled.push(StalledRow {
                kind: "Restore",
                object: format!(
                    "{}/{}",
                    restore.metadata.namespace.clone().unwrap_or_default(),
                    restore.name_any()
                ),
                message: c.message.clone(),
            });
        }
    }

    Ok(report)
}

/// Render the report as the human one-screen overview. Pure.
pub fn render(report: &StatusReport, now: DateTime<Utc>) -> String {
    let mut out = String::new();

    out.push_str("REPOSITORIES\n");
    if report.repositories.is_empty() {
        out.push_str("  (none)\n");
    } else {
        let mut t = Table::new(vec![
            "KIND",
            "NAME",
            "NAMESPACE",
            "PHASE",
            "BACKEND",
            "MODE",
            "SUSPENDED",
            "MAINTENANCE",
        ]);
        for r in &report.repositories {
            t.push(vec![
                r.kind.to_string(),
                r.name.clone(),
                r.namespace.clone().unwrap_or_else(|| EMPTY_CELL.into()),
                r.phase.clone(),
                r.backend.clone(),
                r.mode.clone(),
                r.suspended.to_string(),
                r.maintenance.clone(),
            ]);
        }
        out.push_str(&t.render());
        for r in &report.repositories {
            if let Some(problem) = &r.problem {
                out.push_str(&format!("  ! {}/{}: {}\n", r.kind, r.name, problem));
            }
        }
    }

    out.push_str("\nPOLICIES\n");
    if report.policies.is_empty() {
        out.push_str("  (none)\n");
    } else {
        let mut t = Table::new(vec![
            "NAME",
            "NAMESPACE",
            "REPOSITORY",
            "SUSPENDED",
            "LAST-SNAPSHOT",
            "LAST-VERIFIED",
        ]);
        for p in &report.policies {
            t.push(vec![
                p.name.clone(),
                p.namespace.clone(),
                p.repository.clone(),
                p.suspended.to_string(),
                humanize_rfc3339(p.last_snapshot.as_deref(), now),
                humanize_rfc3339(p.last_verified.as_deref(), now),
            ]);
        }
        out.push_str(&t.render());
    }

    out.push_str("\nSCHEDULES\n");
    if report.schedules.is_empty() {
        out.push_str("  (none)\n");
    } else {
        let mut t = Table::new(vec![
            "NAME",
            "NAMESPACE",
            "POLICY",
            "CRON",
            "SUSPENDED",
            "LAST-FIRE",
            "NEXT-FIRE",
            "FAILURES",
        ]);
        for s in &report.schedules {
            t.push(vec![
                s.name.clone(),
                s.namespace.clone(),
                s.policy.clone(),
                s.cron.clone(),
                s.suspended.to_string(),
                humanize_rfc3339(s.last_fire.as_deref(), now),
                s.next_fire.clone().unwrap_or_else(|| EMPTY_CELL.into()),
                s.consecutive_failures
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| EMPTY_CELL.into()),
            ]);
        }
        out.push_str(&t.render());
    }

    out.push_str(&format!(
        "\nIN FLIGHT: {} snapshot(s), {} restore(s)\n",
        report.in_flight.snapshots, report.in_flight.restores
    ));
    if !report.stalled.is_empty() {
        out.push_str("\nSTALLED (won't progress without intervention):\n");
        for s in &report.stalled {
            out.push_str(&format!("  ! {} {}: {}\n", s.kind, s.object, s.message));
        }
    }
    out
}

/// `2026-06-11T03:00:12Z` → `9h ago`, for the relative columns.
fn humanize_rfc3339(ts: Option<&str>, now: DateTime<Utc>) -> String {
    ts.and_then(|t| DateTime::parse_from_rfc3339(t).ok())
        .map(|t| format!("{} ago", human_age(t.with_timezone(&Utc), now)))
        .unwrap_or_else(|| EMPTY_CELL.into())
}

/// Run `status`.
pub async fn run(
    ctx: &KubeCtx,
    args: &StatusArgs,
    output: OutputFormat,
    now: DateTime<Utc>,
) -> Result<String, CliError> {
    let report = gather(ctx, args, now).await?;
    match output {
        OutputFormat::Table | OutputFormat::Wide => Ok(render(&report, now)),
        OutputFormat::Yaml => {
            let value = serde_json::to_value(&report).map_err(|e| CliError::Serialization {
                what: "status report",
                source: e.into(),
            })?;
            serde_yaml::to_string(&value).map_err(|e| CliError::Serialization {
                what: "status report",
                source: e.into(),
            })
        }
        OutputFormat::Json => {
            let mut s =
                serde_json::to_string_pretty(&report).map_err(|e| CliError::Serialization {
                    what: "status report",
                    source: e.into(),
                })?;
            s.push('\n');
            Ok(s)
        }
        // There is no single resource to name; the report is the output.
        OutputFormat::Name => Err(CliError::Serialization {
            what: "status report as -o name (status is a report, not a resource; use -o json)",
            source: Box::new(std::io::Error::other("unsupported output format")),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 11, 12, 0, 0).unwrap()
    }

    fn sample() -> StatusReport {
        StatusReport {
            repositories: vec![
                RepoRow {
                    kind: "Repository",
                    name: "nas".into(),
                    namespace: Some("media".into()),
                    phase: "Ready".into(),
                    backend: "S3".into(),
                    mode: "ReadWrite".into(),
                    suspended: false,
                    maintenance: "configured".into(),
                    problem: None,
                },
                RepoRow {
                    kind: "ClusterRepository",
                    name: "offsite".into(),
                    namespace: None,
                    phase: "Failed".into(),
                    backend: "B2".into(),
                    mode: "ReadWrite".into(),
                    suspended: false,
                    maintenance: "-".into(),
                    problem: Some("credentials rejected; fix the Secret".into()),
                },
            ],
            policies: vec![PolicyRow {
                name: "nightly".into(),
                namespace: "media".into(),
                repository: "Repository/nas".into(),
                suspended: false,
                last_snapshot: Some("2026-06-11T03:00:12Z".into()),
                last_verified: None,
            }],
            schedules: vec![ScheduleRow {
                name: "nightly".into(),
                namespace: "media".into(),
                policy: "nightly".into(),
                cron: "0 3 * * *".into(),
                suspended: false,
                last_fire: Some("2026-06-11T03:00:00Z".into()),
                next_fire: Some("2026-06-12T03:00:00Z".into()),
                consecutive_failures: Some(2),
            }],
            in_flight: InFlight {
                snapshots: 1,
                restores: 0,
            },
            stalled: vec![StalledRow {
                kind: "Snapshot",
                object: "media/oops".into(),
                message: "terminal kopia failure".into(),
            }],
        }
    }

    #[test]
    fn render_shows_sections_problems_and_stalled() {
        let text = render(&sample(), now());
        assert!(text.contains("REPOSITORIES"), "{text}");
        assert!(text.contains("Repository         nas"), "{text}");
        // A non-Ready repo carries its Ready-condition message inline.
        assert!(
            text.contains("! ClusterRepository/offsite: credentials rejected"),
            "{text}"
        );
        assert!(text.contains("LAST-SNAPSHOT"), "{text}");
        assert!(text.contains("9h ago"), "{text}");
        assert!(
            text.contains("IN FLIGHT: 1 snapshot(s), 0 restore(s)"),
            "{text}"
        );
        assert!(text.contains("STALLED"), "{text}");
        assert!(
            text.contains("! Snapshot media/oops: terminal kopia failure"),
            "{text}"
        );
        // consecutiveFailures surfaces in the FAILURES column.
        assert!(
            text.lines()
                .any(|l| l.contains("0 3 * * *") && l.ends_with('2')),
            "{text}"
        );
    }

    #[test]
    fn restore_filter_matches_resolved_spec_or_kept_policy() {
        let filter = RepoFilter {
            uid: "u1".into(),
            name: "nas".into(),
            kind: RepositoryKind::Repository,
            namespace: Some("media".into()),
        };
        let kept: std::collections::BTreeSet<(String, String)> =
            [("media".to_string(), "nightly".to_string())].into();
        let restore = |v: serde_json::Value| -> Restore { serde_json::from_value(v).unwrap() };

        // Pinned resolved repository matches (ref ns absent = restore ns).
        let pinned = restore(serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1", "kind": "Restore",
            "metadata": { "name": "r", "namespace": "media" },
            "spec": { "source": { "snapshotRef": { "name": "s" } }, "target": { "pvcRef": { "name": "d" } } },
            "status": { "resolved": { "repository": { "kind": "Repository", "name": "nas" } } }
        }));
        assert!(restore_matches(Some(&filter), &kept, &pinned));

        // fromPolicy source matches through the kept (namespace, name) key…
        let from_policy = restore(serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1", "kind": "Restore",
            "metadata": { "name": "r", "namespace": "media" },
            "spec": { "source": { "fromPolicy": { "name": "nightly" } }, "target": { "pvcRef": { "name": "d" } } }
        }));
        assert!(restore_matches(Some(&filter), &kept, &from_policy));

        // …but the SAME policy name in another namespace must NOT match.
        let other_ns = restore(serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1", "kind": "Restore",
            "metadata": { "name": "r", "namespace": "other" },
            "spec": { "source": { "fromPolicy": { "name": "nightly" } }, "target": { "pvcRef": { "name": "d" } } }
        }));
        assert!(!restore_matches(Some(&filter), &kept, &other_ns));

        // No filter keeps everything.
        assert!(restore_matches(None, &kept, &other_ns));
    }

    #[test]
    fn in_flight_classification_is_exhaustive() {
        assert!(snapshot_in_flight(Some(SnapshotPhase::Pending)));
        assert!(snapshot_in_flight(Some(SnapshotPhase::Running)));
        assert!(!snapshot_in_flight(Some(SnapshotPhase::Succeeded)));
        assert!(!snapshot_in_flight(Some(SnapshotPhase::Discovered)));
        assert!(restore_in_flight(Some(RestorePhase::Resolving)));
        assert!(!restore_in_flight(Some(RestorePhase::Completed)));
    }

    #[test]
    fn report_serializes_camel_case_for_machine_output() {
        let v = serde_json::to_value(sample()).unwrap();
        assert_eq!(v["inFlight"]["snapshots"], 1);
        assert_eq!(v["policies"][0]["lastSnapshot"], "2026-06-11T03:00:12Z");
        assert_eq!(
            v["repositories"][1]["problem"],
            "credentials rejected; fix the Secret"
        );
    }
}
