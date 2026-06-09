//! The `RepositoryReplication` CRD — mirror a repository's blobs to a second
//! backend on a schedule (ADR-0005 §13(d)). The one net-new CRD: it is the "2" in
//! 3-2-1 backup, wrapping `kopia repository sync-to`.
//!
//! It is **namespaced** (it lives alongside its source repository, mirroring
//! `Maintenance`) and references either a namespaced `Repository` or a cluster-scoped
//! `ClusterRepository` via a [`RepositoryRef`]. The controller schedules a per-slot
//! mover Job (croner + deterministic jitter, single-flight, repo-ready gate,
//! transition-guarded status) exactly like `Maintenance`.

use crate::backend::Backend;
use crate::common::{CronSpec, Encryption, MoverSpec, RepositoryRef};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Mirror a source repository's blobs to a destination backend on a schedule
/// (`kopia repository sync-to`), ADR-0005 §13(d).
///
/// Not `Eq`: `mover` transitively embeds k8s-openapi types.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[kube(
    group = "kopiur.home-operations.com",
    version = "v1alpha1",
    kind = "RepositoryReplication",
    plural = "repositoryreplications",
    namespaced,
    status = "RepositoryReplicationStatus",
    shortname = "kopiarepl",
    category = "kopiur",
    printcolumn = r#"{"name":"Source","type":"string","jsonPath":".spec.sourceRef.name"}"#,
    printcolumn = r#"{"name":"Destination","type":"string","jsonPath":".status.destinationBackend"}"#,
    printcolumn = r#"{"name":"Schedule","type":"string","jsonPath":".spec.schedule.cron"}"#,
    printcolumn = r#"{"name":"Last","type":"date","jsonPath":".status.lastReplicated"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryReplicationSpec {
    /// The repository to mirror *from* — a `Repository` or `ClusterRepository`
    /// reference (ADR §3.2). Credentials/connect are resolved from it.
    pub source_ref: RepositoryRef,
    /// The backend to mirror *to* (`kopia repository sync-to <destination>`).
    /// Exactly one backend by construction (the externally-tagged `Backend` enum,
    /// reused). Must differ from the source's backend (webhook-enforced).
    pub destination: Backend,
    /// Encryption/password for the destination repository when it needs its own
    /// (e.g. a freshly-created mirror with a distinct password). Absent ⇒ the
    /// destination uses the **source** repository's password — the common case for a
    /// true mirror, where `sync-to` copies blobs verbatim and the format (including
    /// the encryption material) is identical. Document this in the example.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_encryption: Option<Encryption>,
    /// Cron + deterministic jitter for the replication runs (ADR §3.7 scheduling
    /// kernel, shared with `Maintenance`).
    pub schedule: CronSpec,
    /// Mover (Job pod) overrides for the replication run — resources, scheduling,
    /// security context. Inherits the source repository's `moverDefaults` underneath
    /// (ADR-0004 §1/§2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mover: Option<MoverSpec>,
    /// Pause this replication declaratively (ADR-0005 §14(e)): a suspended
    /// `RepositoryReplication` is skipped by its own reconcile (no sync runs),
    /// surfaced via a condition. Default `false`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub suspend: bool,
}

/// Lifecycle phase of a replication. Closed enum. ADR-0005 §13(d).
///
/// ```
/// use kopiur_api::repository_replication::RepositoryReplicationPhase;
///
/// assert_eq!(RepositoryReplicationPhase::default(), RepositoryReplicationPhase::Pending);
/// assert_eq!(
///     serde_json::to_value(RepositoryReplicationPhase::Replicating).unwrap(),
///     "Replicating"
/// );
/// ```
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum RepositoryReplicationPhase {
    /// Admitted, not yet run (also the default).
    #[default]
    Pending,
    /// A replication mover Job is in flight.
    Replicating,
    /// The most recent replication completed successfully (idle until the next slot).
    Succeeded,
    /// The most recent replication run failed; see conditions.
    Failed,
    /// Suspended via `spec.suspend`.
    Suspended,
}

impl crate::common::PhaseLabel for RepositoryReplicationPhase {
    const ALL: &'static [Self] = &[
        Self::Pending,
        Self::Replicating,
        Self::Succeeded,
        Self::Failed,
        Self::Suspended,
    ];
    fn label(&self) -> &'static str {
        match self {
            Self::Pending => "Pending",
            Self::Replicating => "Replicating",
            Self::Succeeded => "Succeeded",
            Self::Failed => "Failed",
            Self::Suspended => "Suspended",
        }
    }
}

/// Observed state of a [`RepositoryReplication`]. ADR-0005 §13(d).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryReplicationStatus {
    /// Current lifecycle phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<RepositoryReplicationPhase>,
    /// `metadata.generation` last reconciled, for staleness detection / kstatus.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The destination backend kind (mirror of `spec.destination` discriminant),
    /// for the `DESTINATION` print column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_backend: Option<String>,
    /// RFC3339 timestamp of the most recent successful replication run. Backs the
    /// `LAST` print column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_replicated: Option<String>,
    /// RFC3339 timestamp of the next scheduled replication run (cron + jitter, pinned).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_scheduled_at: Option<String>,
    /// Bytes replicated by the last successful run (best-effort from kopia output).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_replicated_bytes: Option<i64>,
    /// Blobs replicated by the last successful run (best-effort).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_replicated_blobs: Option<i64>,
    /// Standard Kubernetes conditions (`Ready`, `Reconciling`, `Stalled`). ADR-0005 §2.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::RepositoryKind;
    use crate::testutil::from_yaml;
    use kube::core::CustomResourceExt;

    #[test]
    fn repository_replication_crd_metadata_is_correct() {
        let crd = RepositoryReplication::crd();
        assert_eq!(crd.spec.group, "kopiur.home-operations.com");
        assert_eq!(crd.spec.names.kind, "RepositoryReplication");
        assert_eq!(crd.spec.names.plural, "repositoryreplications");
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn repository_replication_roundtrip() {
        // sourceRef + destination (externally-tagged backend) + schedule.
        let yaml = r#"
sourceRef:
  kind: Repository
  name: nas-primary
destination:
  s3:
    bucket: offsite-mirror
    region: us-east-1
    auth:
      secretRef:
        name: offsite-creds
destinationEncryption:
  passwordSecretRef:
    name: offsite-creds
    key: KOPIA_PASSWORD
schedule:
  cron: "0 5 * * *"
  jitter: 1h
suspend: false
"#;
        let spec: RepositoryReplicationSpec = from_yaml(yaml);
        assert_eq!(spec.source_ref.kind, RepositoryKind::Repository);
        assert_eq!(spec.source_ref.name, "nas-primary");
        // Destination is exactly one backend variant (the type guarantees it).
        match &spec.destination {
            Backend::S3(s3) => assert_eq!(s3.bucket, "offsite-mirror"),
            other => panic!("expected S3 destination, got {}", other.kind_str()),
        }
        assert_eq!(spec.schedule.cron, "0 5 * * *");
        assert_eq!(spec.schedule.jitter.as_deref(), Some("1h"));
        assert!(spec.destination_encryption.is_some());
        assert!(!spec.suspend);

        let json = serde_json::to_value(&spec).expect("serialize");
        // Externally tagged destination backend.
        assert_eq!(json["destination"]["s3"]["bucket"], "offsite-mirror");
        let reparsed: RepositoryReplicationSpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn destination_encryption_is_optional() {
        // A true mirror reuses the source password — destinationEncryption absent.
        let yaml = r#"
sourceRef: { name: nas-primary }
destination: { filesystem: { path: /mirror } }
schedule: { cron: "0 6 * * 0" }
"#;
        let spec: RepositoryReplicationSpec = from_yaml(yaml);
        assert!(spec.destination_encryption.is_none());
        // sourceRef.kind defaults to Repository.
        assert_eq!(spec.source_ref.kind, RepositoryKind::Repository);
        let json = serde_json::to_value(&spec).unwrap();
        assert!(json.get("destinationEncryption").is_none());
        assert!(json.get("suspend").is_none());
    }

    #[test]
    fn replication_phase_all_covers_every_variant() {
        use crate::common::PhaseLabel;
        let labels: Vec<&str> = RepositoryReplicationPhase::ALL
            .iter()
            .map(|p| p.label())
            .collect();
        assert_eq!(RepositoryReplicationPhase::ALL.len(), 5);
        assert!(labels.iter().all(|l| !l.is_empty()));
    }

    #[test]
    fn status_roundtrips() {
        let status: RepositoryReplicationStatus = from_yaml(
            "phase: Succeeded\ndestinationBackend: s3\nlastReplicated: 2026-06-09T05:00:00Z\nlastReplicatedBytes: 12345\n",
        );
        assert_eq!(status.phase, Some(RepositoryReplicationPhase::Succeeded));
        assert_eq!(status.destination_backend.as_deref(), Some("s3"));
        assert_eq!(status.last_replicated_bytes, Some(12345));
        let json = serde_json::to_value(&status).unwrap();
        let reparsed: RepositoryReplicationStatus = serde_json::from_value(json).unwrap();
        assert_eq!(status, reparsed);
    }
}
