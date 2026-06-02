//! The `Maintenance` CRD — schedules `kopia maintenance run` quick + full and
//! manages the ownership lease. At most one per repository. ADR-0001 §3.7.

use crate::common::{CronSpec, FailurePolicy, MoverSpec, RepositoryRef};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Maintenance schedule + ownership lease for one `Repository`/`ClusterRepository`. ADR §3.7.
///
/// Not `Eq`: `mover` transitively embeds k8s-openapi types.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "kopia.io",
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
    pub schedule: MaintenanceSchedule,
    pub ownership: Ownership,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mover: Option<MoverSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_policy: Option<FailurePolicy>,
}

/// Quick + full cron schedules plus a shared timezone. ADR §3.7.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceSchedule {
    pub quick: CronSpec,
    pub full: CronSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

/// Ownership-lease configuration. At most one `Maintenance` may own a repository. ADR §3.7.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Ownership {
    pub owner: String,
    #[serde(default)]
    pub takeover_policy: TakeoverPolicy,
}

/// What to do when another owner already holds the lease. Closed enum. ADR §3.7.
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership: Option<OwnershipStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quick: Option<RunStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full: Option<RunStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OwnershipStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<String>,
}

/// Per-kind (quick/full) run status. ADR §3.7.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_scheduled_at: Option<String>,
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
        assert_eq!(crd.spec.group, "kopia.io");
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
