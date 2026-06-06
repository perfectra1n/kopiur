//! The `Maintenance` CRD — schedules `kopia maintenance run` quick + full and
//! manages the ownership lease. At most one per repository. ADR-0001 §3.7.

use crate::common::{CronSpec, FailurePolicy, MoverSpec, RepositoryRef};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The schedule an operator-managed `Maintenance` uses when the owning
/// `Repository`/`ClusterRepository` does not override it: quick every 6h (30m
/// jitter), full daily at 03:00 (1h jitter). Shared by the webhook (defaulting),
/// the controller (projection), and tests, so the default lives in exactly one
/// place. ADR §3.7.
///
/// ```
/// use kopiur_api::default_maintenance_schedule;
///
/// let s = default_maintenance_schedule();
/// assert_eq!(s.quick.cron, "0 */6 * * *");
/// assert_eq!(s.quick.jitter.as_deref(), Some("30m"));
/// assert_eq!(s.full.cron, "0 3 * * *");
/// assert_eq!(s.full.jitter.as_deref(), Some("1h"));
/// assert!(s.timezone.is_none());
/// ```
pub fn default_maintenance_schedule() -> MaintenanceSchedule {
    MaintenanceSchedule {
        quick: CronSpec {
            cron: "0 */6 * * *".to_string(),
            jitter: Some("30m".to_string()),
        },
        full: CronSpec {
            cron: "0 3 * * *".to_string(),
            jitter: Some("1h".to_string()),
        },
        timezone: None,
    }
}

/// Maintenance schedule + ownership lease for one `Repository`/`ClusterRepository`. ADR §3.7.
///
/// Not `Eq`: `mover` transitively embeds k8s-openapi types.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "kopiur.home-operations.com",
    version = "v1alpha1",
    kind = "Maintenance",
    namespaced,
    status = "MaintenanceStatus",
    shortname = "kopiamaint",
    category = "kopiur",
    printcolumn = r#"{"name":"Repository","type":"string","jsonPath":".spec.repository.name"}"#,
    printcolumn = r#"{"name":"Owner","type":"string","jsonPath":".status.ownership.owner"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceSpec {
    /// Discriminated reference to a `Repository` or `ClusterRepository`. ADR §3.2.
    pub repository: RepositoryRef,
    /// Quick + full cron schedules (with a shared timezone) for `kopia
    /// maintenance run`. ADR §3.7.
    pub schedule: MaintenanceSchedule,
    /// Ownership-lease configuration; at most one `Maintenance` may own a
    /// repository at a time. ADR §3.7.
    pub ownership: Ownership,
    /// Mover (Job pod) overrides for the maintenance run — resources, scheduling,
    /// etc. Object-store repositories typically tune this. ADR §3.7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mover: Option<MoverSpec>,
    /// How a failed maintenance run is retried/bounded (backoff, deadline). ADR §3.7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_policy: Option<FailurePolicy>,
}

/// Quick + full cron schedules plus a shared timezone. ADR §3.7.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceSchedule {
    /// Cron + jitter for `kopia maintenance run` (quick = cheap index/log work).
    pub quick: CronSpec,
    /// Cron + jitter for `kopia maintenance run --full` (content reclamation).
    pub full: CronSpec,
    /// IANA timezone both crons are evaluated in; absent means controller default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

/// Ownership-lease configuration. At most one `Maintenance` may own a repository. ADR §3.7.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Ownership {
    /// Stable lease holder identity (e.g. `kopia-operator/nas-primary`). Two
    /// `Maintenance` CRs claiming the same repository compare this. ADR §3.7.
    pub owner: String,
    /// What to do if the lease is already held by a different `owner`. ADR §3.7.
    #[serde(default)]
    pub takeover_policy: TakeoverPolicy,
}

/// What to do when another owner already holds the lease. Closed enum. ADR §3.7.
///
/// ```
/// use kopiur_api::TakeoverPolicy;
///
/// // The safest default: never seize a lease another owner holds.
/// assert_eq!(TakeoverPolicy::default(), TakeoverPolicy::Never);
/// assert_eq!(
///     serde_json::to_value(TakeoverPolicy::PromptCondition).unwrap(),
///     serde_json::json!("PromptCondition"),
/// );
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum TakeoverPolicy {
    /// Never take over an existing lease (default — safest).
    #[default]
    Never,
    /// Surface a condition prompting an operator to decide.
    PromptCondition,
    /// Forcibly claim the lease.
    Force,
}

/// What to do about the ownership lease, decided from the takeover policy and
/// whether another owner currently holds it (ADR §3.7). Exhaustive over
/// [`TakeoverPolicy`].
///
/// Lives in `kopiur-api` (not the controller) because the lease decision is made
/// in the mover for object-store repositories — only something with repo access
/// can read `kopia maintenance info` to learn the current holder. Keeping the
/// pure decision here gives the controller (filesystem) and the mover
/// (object-store) one shared, exhaustively-matched source of truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseAction {
    /// Claim the lease (we hold it or it is free).
    Claim,
    /// Forcibly take the lease from the current holder.
    Takeover,
    /// Surface a condition prompting a human to decide; do not claim.
    Prompt,
    /// Another owner holds it and policy is `Never`: do nothing, requeue.
    Yield,
}

/// Decide the lease action. `held_by_other` is true when a *different* owner
/// currently holds the maintenance lease for this repository.
///
/// ```
/// use kopiur_api::{lease_action, LeaseAction, TakeoverPolicy};
///
/// // Free (or already ours) → always claim, regardless of policy.
/// assert_eq!(lease_action(TakeoverPolicy::Never, false), LeaseAction::Claim);
/// // Held by another → dispatch on policy.
/// assert_eq!(lease_action(TakeoverPolicy::Never, true), LeaseAction::Yield);
/// assert_eq!(lease_action(TakeoverPolicy::Force, true), LeaseAction::Takeover);
/// ```
pub fn lease_action(policy: TakeoverPolicy, held_by_other: bool) -> LeaseAction {
    if !held_by_other {
        // Free or already ours → just (re)claim.
        return LeaseAction::Claim;
    }
    match policy {
        TakeoverPolicy::Never => LeaseAction::Yield,
        TakeoverPolicy::PromptCondition => LeaseAction::Prompt,
        TakeoverPolicy::Force => LeaseAction::Takeover,
    }
}

/// Inline maintenance control on a `Repository`/`ClusterRepository`
/// (`spec.maintenance`). ADR §3.1/§3.7.
///
/// Maintenance is **default-managed**: when this is absent (or `enabled: true`),
/// the repository reconciler projects it into an *owned* `Maintenance` child CR,
/// so kopia storage is reclaimed without the user remembering to author a
/// separate `Maintenance`. The reconciler honors an externally-authored
/// `Maintenance` referencing the repository regardless of `enabled` — setting
/// `enabled: false` only tells the operator not to create its own; it never
/// deletes, ignores, or warns about a user-managed one.
///
/// Not `Eq`: `mover` transitively embeds k8s-openapi types.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryMaintenanceSpec {
    /// Whether the operator manages a `Maintenance` CR for this repository.
    /// Defaults to `true` (default-on). When `false`, the operator does not
    /// create or manage one — but an externally-authored `Maintenance` is still
    /// honored.
    #[serde(default = "crate::common::default_true")]
    pub enabled: bool,
    /// Schedule override. When absent, the operator uses
    /// [`default_maintenance_schedule`] (quick 6h / full daily).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<MaintenanceSchedule>,
    /// Mover overrides for the managed `Maintenance` (object-store repositories).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mover: Option<MoverSpec>,
    /// Failure handling (backoff/deadline) for the managed `Maintenance` run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_policy: Option<FailurePolicy>,
    /// Lease takeover policy for the managed `Maintenance`. Defaults to
    /// [`TakeoverPolicy::Never`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub takeover_policy: Option<TakeoverPolicy>,
    /// **ClusterRepository only** — namespace the managed (namespaced)
    /// `Maintenance` CR is created in. Defaults to the operator's own namespace.
    /// Forbidden on a namespaced `Repository` (its `Maintenance` always lives in
    /// the repository's namespace), rejected by the admission webhook.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

impl Default for RepositoryMaintenanceSpec {
    /// Default-on with no overrides. `enabled` is `true` here to match the serde
    /// `default_true` so a constructed default and a deserialized `{}` agree.
    fn default() -> Self {
        Self {
            enabled: true,
            schedule: None,
            mover: None,
            failure_policy: None,
            takeover_policy: None,
            namespace: None,
        }
    }
}

/// Observed maintenance state: lease holder plus per-kind run results. ADR §3.7.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceStatus {
    /// The `metadata.generation` this status reflects, for staleness detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Current lease holder, if the lease has been claimed. ADR §3.7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership: Option<OwnershipStatus>,
    /// Last/next-run state for the quick maintenance schedule. ADR §3.7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quick: Option<RunStatus>,
    /// Last/next-run state for the full maintenance schedule. ADR §3.7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full: Option<RunStatus>,
    /// Standard Kubernetes conditions surfacing maintenance health. ADR §5.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Observed ownership-lease state: who holds it and since when. ADR §3.7.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OwnershipStatus {
    /// The current lease holder's identity (matches `Ownership.owner`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// RFC3339 instant the lease was claimed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<String>,
}

/// Per-kind (quick/full) run status. ADR §3.7.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunStatus {
    /// RFC3339 instant of the most recent run of this kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<String>,
    /// RFC3339 instant of the next scheduled run of this kind (cron + jitter, pinned).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_scheduled_at: Option<String>,
    /// Count of back-to-back failed runs of this kind; resets on success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consecutive_failures: Option<i64>,
    /// The ONLY place storage reclamation is surfaced (ADR §3.7/§4.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_content_reclaimed_bytes: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::RepositoryKind;
    use crate::testutil::from_yaml;
    use kube::core::CustomResourceExt;

    #[test]
    fn maintenance_crd_metadata_is_correct() {
        let crd = Maintenance::crd();
        assert_eq!(crd.spec.group, "kopiur.home-operations.com");
        assert_eq!(crd.spec.names.kind, "Maintenance");
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn maintenance_roundtrip_matches_adr_shape() {
        // Mirrors ADR-0001 §3.7.
        let yaml = r#"
repository:
  kind: Repository
  name: nas-primary
schedule:
  quick: { cron: "0 */6 * * *", jitter: 30m }
  full:  { cron: "0 3 * * 0", jitter: 1h }
  timezone: UTC
ownership:
  owner: "kopia-operator/nas-primary"
  takeoverPolicy: PromptCondition
mover:
  resources: { requests: { cpu: 250m, memory: 1Gi }, limits: { cpu: "2", memory: 4Gi } }
failurePolicy:
  backoffLimit: 1
  activeDeadlineSeconds: 14400
"#;
        let spec: MaintenanceSpec = from_yaml(yaml);
        assert_eq!(spec.repository.kind, RepositoryKind::Repository);
        assert_eq!(spec.schedule.quick.cron, "0 */6 * * *");
        assert_eq!(spec.schedule.quick.jitter.as_deref(), Some("30m"));
        assert_eq!(spec.schedule.full.cron, "0 3 * * 0");
        assert_eq!(spec.schedule.timezone.as_deref(), Some("UTC"));
        assert_eq!(spec.ownership.owner, "kopia-operator/nas-primary");
        assert_eq!(
            spec.ownership.takeover_policy,
            TakeoverPolicy::PromptCondition
        );
        assert_eq!(
            spec.failure_policy
                .as_ref()
                .unwrap()
                .active_deadline_seconds,
            Some(14400)
        );

        let json = serde_json::to_value(&spec).expect("serialize");
        let reparsed: MaintenanceSpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn maintenance_status_roundtrips() {
        // Mirrors ADR-0001 §3.7 status block.
        let yaml = r#"
ownership:
  owner: "kopia-operator/nas-primary"
  claimedAt: 2026-05-12T08:14:02Z
quick:
  lastRunAt: 2026-05-24T12:00:11Z
  nextScheduledAt: 2026-05-24T18:00:00Z
  consecutiveFailures: 0
  lastContentReclaimedBytes: 1234567
full:
  lastRunAt: 2026-05-19T03:01:42Z
  nextScheduledAt: 2026-05-26T03:00:00Z
  consecutiveFailures: 0
  lastContentReclaimedBytes: 89456789012
"#;
        let status: MaintenanceStatus = from_yaml(yaml);
        assert_eq!(
            status.ownership.as_ref().unwrap().owner.as_deref(),
            Some("kopia-operator/nas-primary")
        );
        assert_eq!(
            status.quick.as_ref().unwrap().last_content_reclaimed_bytes,
            Some(1234567)
        );
        assert_eq!(
            status.full.as_ref().unwrap().last_content_reclaimed_bytes,
            Some(89456789012)
        );

        let json = serde_json::to_value(&status).unwrap();
        let reparsed: MaintenanceStatus = serde_json::from_value(json).unwrap();
        assert_eq!(status, reparsed);
    }

    #[test]
    fn repository_maintenance_defaults_to_enabled() {
        // An empty `spec.maintenance: {}` is default-on with no overrides.
        let m: RepositoryMaintenanceSpec = from_yaml("{}\n");
        assert!(
            m.enabled,
            "absent `enabled` must default to true (default-on)"
        );
        assert!(m.schedule.is_none());
        assert!(m.namespace.is_none());
        assert!(m.takeover_policy.is_none());
        // The constructed Default agrees with the deserialized `{}`.
        assert_eq!(m, RepositoryMaintenanceSpec::default());
    }

    #[test]
    fn repository_maintenance_roundtrip_with_overrides() {
        let yaml = r#"
enabled: false
schedule:
  quick: { cron: "0 */4 * * *", jitter: 20m }
  full:  { cron: "30 2 * * *", jitter: 45m }
  timezone: America/Chicago
takeoverPolicy: Force
namespace: kopia-system
failurePolicy:
  backoffLimit: 2
"#;
        let m: RepositoryMaintenanceSpec = from_yaml(yaml);
        assert!(!m.enabled);
        let s = m.schedule.as_ref().expect("schedule");
        assert_eq!(s.quick.cron, "0 */4 * * *");
        assert_eq!(s.full.jitter.as_deref(), Some("45m"));
        assert_eq!(s.timezone.as_deref(), Some("America/Chicago"));
        assert_eq!(m.takeover_policy, Some(TakeoverPolicy::Force));
        assert_eq!(m.namespace.as_deref(), Some("kopia-system"));
        assert_eq!(m.failure_policy.as_ref().unwrap().backoff_limit, Some(2));

        let json = serde_json::to_value(&m).expect("serialize");
        let reparsed: RepositoryMaintenanceSpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(m, reparsed);
    }

    #[test]
    fn default_maintenance_schedule_is_quick_6h_full_daily() {
        let s = default_maintenance_schedule();
        assert_eq!(s.quick.cron, "0 */6 * * *");
        assert_eq!(s.quick.jitter.as_deref(), Some("30m"));
        assert_eq!(s.full.cron, "0 3 * * *");
        assert_eq!(s.full.jitter.as_deref(), Some("1h"));
        assert!(s.timezone.is_none());
    }

    #[test]
    fn free_lease_is_claimed_regardless_of_policy() {
        for p in [
            TakeoverPolicy::Never,
            TakeoverPolicy::PromptCondition,
            TakeoverPolicy::Force,
        ] {
            assert_eq!(lease_action(p, false), LeaseAction::Claim);
        }
    }

    #[test]
    fn held_lease_dispatches_by_policy() {
        assert_eq!(
            lease_action(TakeoverPolicy::Never, true),
            LeaseAction::Yield
        );
        assert_eq!(
            lease_action(TakeoverPolicy::PromptCondition, true),
            LeaseAction::Prompt
        );
        assert_eq!(
            lease_action(TakeoverPolicy::Force, true),
            LeaseAction::Takeover
        );
    }

    #[test]
    fn takeover_policy_serializes_to_expected_strings() {
        assert_eq!(
            serde_json::to_value(TakeoverPolicy::Never).unwrap(),
            "Never"
        );
        assert_eq!(
            serde_json::to_value(TakeoverPolicy::PromptCondition).unwrap(),
            "PromptCondition"
        );
        assert_eq!(
            serde_json::to_value(TakeoverPolicy::Force).unwrap(),
            "Force"
        );
        assert_eq!(TakeoverPolicy::default(), TakeoverPolicy::Never);
    }
}
