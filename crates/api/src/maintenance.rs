//! The `Maintenance` CRD — schedules `kopia maintenance run` quick + full and
//! manages the ownership lease. At most one per repository. ADR-0001 §3.7.

use crate::common::{CredentialProjection, CronSpec, FailurePolicy, MoverSpec, RepositoryRef};
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
    /// Opt-in credential-Secret projection for this maintenance run's mover
    /// (default off). When `enabled: true`, the operator copies the referenced
    /// repository's credential Secret(s) into the namespace this `Maintenance`
    /// runs in (a no-op when they already live there) — useful when maintaining a
    /// shared `ClusterRepository` from a namespace that lacks the Secret. ADR §4.11.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_projection: Option<CredentialProjection>,
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

/// The logical maintenance-lease string the operator uses for a repository's
/// DEFAULT-MANAGED `Maintenance` (ADR §3.7). Single derivation, shared by the
/// managed-Maintenance projection and the bootstrap mover's initial kopia
/// owner stamp, so they cannot drift.
///
/// ```
/// use kopiur_api::common::RepositoryKind;
/// use kopiur_api::maintenance::managed_lease;
///
/// assert_eq!(managed_lease(RepositoryKind::Repository, "media", "nas"), "kopiur/media/nas");
/// assert_eq!(
///     managed_lease(RepositoryKind::ClusterRepository, "ignored", "shared"),
///     "kopiur/clusterrepository/shared"
/// );
/// ```
pub fn managed_lease(kind: crate::common::RepositoryKind, namespace: &str, name: &str) -> String {
    match kind {
        crate::common::RepositoryKind::Repository => format!("kopiur/{namespace}/{name}"),
        crate::common::RepositoryKind::ClusterRepository => {
            format!("kopiur/clusterrepository/{name}")
        }
    }
}

/// The mover-owned condition recording lease state on `Maintenance.status`:
/// `True` (lease claimed, run proceeded) or `False` with one of the reasons
/// below. Written by the mover, matched by the controller (Ready degradation)
/// and the kubectl plugin — one definition so the producers and readers cannot
/// drift.
pub const LEASE_OWNED_CONDITION: &str = "LeaseOwned";
/// `LeaseOwned=False` reason: a foreign owner holds the lease and
/// `takeoverPolicy: Never` — the run yielded.
pub const LEASE_HELD_BY_OTHER_REASON: &str = "LeaseHeldByOther";
/// `LeaseOwned=False` reason: a foreign owner holds the lease and
/// `takeoverPolicy: PromptCondition` — the run yielded, prompting the operator
/// to set `Force`.
pub const LEASE_TAKEOVER_PROMPT_REASON: &str = "LeaseTakeoverPrompt";

/// The STABLE kopia client identity a maintenance mover assumes for `lease`
/// (`(username, hostname)`); the mover sets it with `kopia repository
/// set-client` so kopia's designated-owner check compares something stable —
/// the pod's own identity is ephemeral (a new hostname every run), which is
/// why comparing kopia's recorded owner against it can never work.
///
/// ```
/// use kopiur_api::maintenance::kopia_lease_identity;
///
/// assert_eq!(
///     kopia_lease_identity("kopiur/media/nas"),
///     ("kopiur".to_string(), "kopiur-media-nas".to_string())
/// );
/// ```
pub fn kopia_lease_identity(lease: &str) -> (String, String) {
    // Hostname-safe: lowercase, [a-z0-9-], everything else collapses to '-',
    // trimmed and capped at 63 chars (a DNS label).
    let mut host: String = lease
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    while host.contains("--") {
        host = host.replace("--", "-");
    }
    let host = host.trim_matches('-');
    let host: String = host.chars().take(63).collect();
    let host = host.trim_end_matches('-').to_string();
    ("kopiur".to_string(), host)
}

/// The full `user@hostname` owner string kopia records for `lease` — what the
/// mover compares `maintenance info`'s owner against, and what the bootstrap
/// stamps on a repository it CREATES.
///
/// ```
/// use kopiur_api::maintenance::kopia_owner_for_lease;
///
/// assert_eq!(kopia_owner_for_lease("kopiur/media/nas"), "kopiur@kopiur-media-nas");
/// ```
pub fn kopia_owner_for_lease(lease: &str) -> String {
    let (user, host) = kopia_lease_identity(lease);
    format!("{user}@{host}")
}

/// Parse the `run-requested`/`run-mode` annotations into a manual-run request.
/// `Ok(None)` = no request; `Err` = the annotations are present but malformed
/// (the messages say how to fix). Shared by the admission webhook and the
/// controller so validation cannot fork (SKILL "one validator, two callers").
pub fn parse_run_annotations(
    annotations: Option<&std::collections::BTreeMap<String, String>>,
) -> Result<Option<(chrono::DateTime<chrono::Utc>, ManualRunMode)>, String> {
    let Some(raw) = annotations.and_then(|a| a.get(crate::consts::RUN_REQUESTED_ANNOTATION)) else {
        return Ok(None);
    };
    let at = chrono::DateTime::parse_from_rfc3339(raw)
        .map_err(|e| {
            format!(
                "annotation {} must be an RFC3339 timestamp (got {raw:?}): {e}. \
                 Fix: re-annotate with e.g. $(date -u +%Y-%m-%dT%H:%M:%SZ), or use \
                 `kubectl kopiur maintenance run`",
                crate::consts::RUN_REQUESTED_ANNOTATION
            )
        })?
        .with_timezone(&chrono::Utc);
    let mode = match annotations.and_then(|a| a.get(crate::consts::RUN_MODE_ANNOTATION)) {
        None => ManualRunMode::Quick,
        Some(raw_mode) => ManualRunMode::parse(raw_mode).ok_or_else(|| {
            format!(
                "annotation {} must be `quick` or `full` (got {raw_mode:?}). \
                 Fix: re-annotate with a valid mode",
                crate::consts::RUN_MODE_ANNOTATION
            )
        })?,
    };
    Ok(Some((at, mode)))
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
    /// State of the most recent annotation-requested out-of-band run
    /// (`kopiur.home-operations.com/run-requested`); absent until one is requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual_run: Option<ManualRunStatus>,
}

/// Which maintenance kind a manual (annotation-requested) run performs. Closed
/// enum; the wire values are the `run-mode` annotation values.
///
/// ```
/// use kopiur_api::maintenance::ManualRunMode;
///
/// assert_eq!(ManualRunMode::default(), ManualRunMode::Quick);
/// assert_eq!(ManualRunMode::parse("full"), Some(ManualRunMode::Full));
/// assert_eq!(ManualRunMode::parse("FULL"), None); // exact, lowercase
/// assert_eq!(serde_json::to_value(ManualRunMode::Quick).unwrap(), "quick");
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ManualRunMode {
    /// `kopia maintenance run` (quick). The default when `run-mode` is absent.
    #[default]
    Quick,
    /// `kopia maintenance run --full`.
    Full,
}

impl ManualRunMode {
    /// Parse a `run-mode` annotation value. Exact-match, lowercase — the same
    /// strings serde uses on the wire.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "quick" => Some(Self::Quick),
            "full" => Some(Self::Full),
            _ => None,
        }
    }

    /// The stable wire/annotation string.
    pub fn label(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Full => "full",
        }
    }
}

/// Lifecycle of a manual run. Closed enum.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
pub enum ManualRunPhase {
    /// The mover Job for this request is in flight.
    Running,
    /// The run finished successfully (or yielded the lease cleanly — see the
    /// `LeaseOwned` condition for which).
    Succeeded,
    /// The run's Job failed; conditions carry the detail.
    Failed,
}

/// Bookkeeping for the most recent annotation-requested run. `requestedAt`
/// pins WHICH request this status answers, so re-applying the same annotation
/// value is a no-op and a new timestamp starts a new run.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ManualRunStatus {
    /// The `run-requested` annotation value this status reflects (RFC3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_at: Option<String>,
    /// The run kind that was performed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<ManualRunMode>,
    /// Where the run is in its lifecycle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<ManualRunPhase>,
    /// RFC3339 instant the run reached a terminal phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
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
    /// RFC3339 instant the controller last observed one of this kind's per-slot
    /// Jobs reach terminal success — the mover either ran maintenance (which
    /// also advances `lastRunAt`) or deliberately *yielded* the lease (which
    /// does not). The scheduler measures the next slot from
    /// `max(lastRunAt, lastHandledAt)`, so a handled slot never re-fires after
    /// its Job self-reaps via `ttlSecondsAfterFinished`. Without this, a yielded
    /// slot's only record was the (self-reaping) Job itself, and the same slot
    /// respawned a yield Job every TTL period, forever. The stamp is the
    /// *observation instant*, not the (possibly year-old, first-ever) slot
    /// itself — anchoring at the slot would make a yield-only Maintenance march
    /// through the entire backlog of historic slots one Job at a time. Same
    /// catch-up-once semantics as the mover's `lastRunAt = now`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_handled_at: Option<String>,
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
    fn lease_identity_is_hostname_safe_and_stable() {
        let (user, host) = kopia_lease_identity("kopiur/media/My_App.x");
        assert_eq!(user, "kopiur");
        assert_eq!(host, "kopiur-media-my-app-x");
        // Long leases cap at a DNS label and never end with '-'.
        let (_, host) = kopia_lease_identity(&format!("kopiur/{}/x", "n".repeat(100)));
        assert!(host.len() <= 63, "{host}");
        assert!(!host.ends_with('-'), "{host}");
        // Deterministic.
        assert_eq!(
            kopia_owner_for_lease("kopiur/media/nas"),
            kopia_owner_for_lease("kopiur/media/nas")
        );
    }

    #[test]
    fn parse_run_annotations_covers_ok_default_and_garbage() {
        use std::collections::BTreeMap;
        assert_eq!(parse_run_annotations(None), Ok(None));
        let mut a = BTreeMap::new();
        a.insert(
            crate::consts::RUN_REQUESTED_ANNOTATION.to_string(),
            "2026-06-11T12:00:00Z".to_string(),
        );
        let (_, mode) = parse_run_annotations(Some(&a)).unwrap().unwrap();
        assert_eq!(mode, ManualRunMode::Quick, "mode defaults to quick");
        a.insert(
            crate::consts::RUN_MODE_ANNOTATION.to_string(),
            "full".to_string(),
        );
        let (_, mode) = parse_run_annotations(Some(&a)).unwrap().unwrap();
        assert_eq!(mode, ManualRunMode::Full);
        a.insert(
            crate::consts::RUN_REQUESTED_ANNOTATION.to_string(),
            "yesterday".to_string(),
        );
        let err = parse_run_annotations(Some(&a)).unwrap_err();
        assert!(err.contains("must be an RFC3339 timestamp"), "{err}");
        assert!(err.contains("kubectl kopiur maintenance run"), "{err}");
    }

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
  securityContext: { runAsUser: 1000, runAsNonRoot: true }
  podSecurityContext: { fsGroup: 1000 }
failurePolicy:
  backoffLimit: 1
  activeDeadlineSeconds: 14400
"#;
        let spec: MaintenanceSpec = from_yaml(yaml);
        assert_eq!(spec.repository.kind, RepositoryKind::Repository);
        // The mover security contexts (container + pod) round-trip on Maintenance too.
        let mover = spec.mover.as_ref().expect("mover");
        assert_eq!(
            mover.security_context.as_ref().and_then(|s| s.run_as_user),
            Some(1000)
        );
        assert_eq!(
            mover.pod_security_context.as_ref().and_then(|p| p.fs_group),
            Some(1000)
        );
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
