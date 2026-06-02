//! The `BackupSchedule` CRD — *when* a backup runs. Creates `Backup` CRs on a
//! cron schedule in the `BackupConfig`'s namespace. ADR-0001 §3.5, ADR-0003 §4.4.

use crate::common::ConfigRef;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Cron + `configRef`. One source of `Backup` CRs; pausing it doesn't affect
/// in-flight or completed runs. ADR §3.5.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "kopiur.dev",
    version = "v1alpha1",
    kind = "BackupSchedule",
    namespaced,
    status = "BackupScheduleStatus",
    shortname = "kopiasched",
    category = "kopiur",
    printcolumn = r#"{"name":"Config","type":"string","jsonPath":".spec.configRef.name"}"#,
    printcolumn = r#"{"name":"Schedule","type":"string","jsonPath":".spec.schedule.cron"}"#,
    printcolumn = r#"{"name":"Suspended","type":"boolean","jsonPath":".spec.schedule.suspend"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BackupScheduleSpec {
    pub config_ref: ConfigRef,
    pub schedule: ScheduleSpec,
    /// Bounds *failed* `Backup` CRs from this schedule. Successful retention is
    /// GFS-driven on `BackupConfig.spec.retention` — there is deliberately NO
    /// `successfulJobsHistoryLimit` (ADR-0003 §4.4, ADR-0001 §4.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_jobs_history_limit: Option<u32>,
}

/// Cron schedule with deterministic jitter, timezone, and concurrency controls. ADR §3.5/§4.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleSpec {
    /// Cron expression with Jenkins-style `H` substitution. ADR §4.1 (G4).
    pub cron: String,
    /// Deterministic jitter (Go-style duration), derived from `(scheduleUID, slot)`. ADR §4.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// GitOps-friendly default: do NOT fire immediately on create. ADR §4.1 (G3).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub run_on_create: bool,
    /// Skip future firings while true. ADR §5.9.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspend: bool,
    #[serde(default)]
    pub concurrency_policy: ConcurrencyPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starting_deadline_seconds: Option<i64>,
}

/// What to do when a previous run is still in flight. Closed enum, default `Forbid`. ADR §4.1 (G5/G18).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum ConcurrencyPolicy {
    /// Skip the new run; surface a condition rather than pile up (default).
    #[default]
    Forbid,
    Allow,
    Replace,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupScheduleStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// Most recent firing (cron + jitter, pinned). ADR §3.5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_schedule: Option<ScheduleRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_schedule: Option<ScheduleRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_successful_schedule: Option<ScheduleRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consecutive_failures: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// A pinned schedule slot and (optionally) the `Backup` it created. ADR §3.5.
///
/// `at`/`scheduledAt` are both accepted on the wire: ADR uses `scheduledAt` for
/// `lastSchedule` and `at` for `next`/`lastSuccessful`. We model both as the single
/// `at` field with a serde alias so either spelling round-trips.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleRef {
    #[serde(
        default,
        alias = "scheduledAt",
        skip_serializing_if = "Option::is_none"
    )]
    pub at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_ref: Option<BackupReference>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupReference {
    pub name: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::from_yaml;
    use kube::core::CustomResourceExt;

    #[test]
    fn backup_schedule_crd_metadata_is_correct() {
        let crd = BackupSchedule::crd();
        assert_eq!(crd.spec.group, "kopiur.dev");
        assert_eq!(crd.spec.names.kind, "BackupSchedule");
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn backup_schedule_roundtrip_matches_adr_shape() {
        // Mirrors ADR-0001 §3.5.
        let yaml = r#"
configRef:
  name: postgres-data
schedule:
  cron: "H 2 * * *"
  jitter: 30m
  timezone: "America/Los_Angeles"
  runOnCreate: false
  suspend: false
  concurrencyPolicy: Forbid
  startingDeadlineSeconds: 600
failedJobsHistoryLimit: 3
"#;
        let spec: BackupScheduleSpec = from_yaml(yaml);
        assert_eq!(spec.config_ref.name, "postgres-data");
        assert_eq!(spec.schedule.cron, "H 2 * * *");
        assert_eq!(spec.schedule.jitter.as_deref(), Some("30m"));
        assert_eq!(spec.schedule.concurrency_policy, ConcurrencyPolicy::Forbid);
        assert!(!spec.schedule.run_on_create);
        assert_eq!(spec.failed_jobs_history_limit, Some(3));

        let json = serde_json::to_value(&spec).expect("serialize");
        let reparsed: BackupScheduleSpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn schedule_defaults_are_gitops_friendly() {
        // Mirrors ADR-0001 §5.1: minimal schedule.
        let spec: BackupScheduleSpec = from_yaml(
            "configRef: { name: postgres-data }\nschedule: { cron: \"H 2 * * *\", jitter: 30m }\n",
        );
        // runOnCreate and suspend default false; concurrency defaults Forbid.
        assert!(!spec.schedule.run_on_create);
        assert!(!spec.schedule.suspend);
        assert_eq!(spec.schedule.concurrency_policy, ConcurrencyPolicy::Forbid);
        // No successfulJobsHistoryLimit exists on the type at all (ADR-0003 §4.4).
    }

    #[test]
    fn concurrency_policy_serializes_to_expected_strings() {
        assert_eq!(
            serde_json::to_value(ConcurrencyPolicy::Forbid).unwrap(),
            "Forbid"
        );
        assert_eq!(
            serde_json::to_value(ConcurrencyPolicy::Allow).unwrap(),
            "Allow"
        );
        assert_eq!(
            serde_json::to_value(ConcurrencyPolicy::Replace).unwrap(),
            "Replace"
        );
        assert_eq!(ConcurrencyPolicy::default(), ConcurrencyPolicy::Forbid);
    }

    #[test]
    fn schedule_status_accepts_both_at_and_scheduled_at() {
        // ADR §3.5 uses `scheduledAt` on lastSchedule and `at` on next/lastSuccessful.
        let status: BackupScheduleStatus = from_yaml(
            r#"
lastSchedule:
  scheduledAt: 2026-05-24T02:13:00Z
  backupRef: { name: postgres-data-20260524-021300 }
nextSchedule:
  at: 2026-05-25T02:21:00Z
lastSuccessfulSchedule:
  at: 2026-05-24T02:13:00Z
  backupRef: { name: postgres-data-20260524-021300 }
consecutiveFailures: 0
"#,
        );
        assert_eq!(
            status.last_schedule.as_ref().unwrap().at.as_deref(),
            Some("2026-05-24T02:13:00Z")
        );
        assert_eq!(
            status.next_schedule.as_ref().unwrap().at.as_deref(),
            Some("2026-05-25T02:21:00Z")
        );
        // Round-trips (serializes back as `at`).
        let json = serde_json::to_value(&status).unwrap();
        let reparsed: BackupScheduleStatus = serde_json::from_value(json).unwrap();
        assert_eq!(status, reparsed);
    }
}
