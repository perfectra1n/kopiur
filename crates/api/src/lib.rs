#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

pub mod backend;
pub mod cluster_repository;
pub mod common;
pub mod consts;
pub mod maintenance;
pub mod repository;
pub mod repository_replication;
pub mod restore;
pub mod snapshot;
pub mod snapshot_policy;
pub mod snapshot_schedule;

// Shared pure-logic modules (no controller-runtime deps). The webhook and the
// controller both import these, so validation/resolution behavior is identical
// across the two call sites (ADR §5.1, SKILL "one validator, two callers").
pub mod creds;
pub mod duration;
pub mod error;
pub mod identity;
pub mod jitter;
pub mod retention;
pub mod success_expr;
pub mod validate;

pub use backend::{Backend, NfsVolume, PvcVolume, RepoVolume};
pub use cluster_repository::{
    AllowedNamespaces, ClusterRepoCredentialProjection, ClusterRepository, ClusterRepositorySpec,
    ClusterRepositoryStatus, IdentityDefaults,
};
pub use common::{
    CacheDefaults, CacheVolumeMode, CronSpec, DeletionPolicy, MoverDefaults, NamespaceDeletePolicy,
    ObjectRef, PhaseLabel, PolicyRef, ResolvedMover, SourceColocation, SourceColocationMode,
    hardened_security_context, merge_pod_security_context, merge_resources, merge_security_context,
    resolve_mover,
};
pub use maintenance::{
    LeaseAction, Maintenance, MaintenanceSchedule, MaintenanceSpec, MaintenanceStatus,
    ManualRunMode, ManualRunPhase, ManualRunStatus, Ownership, RepositoryMaintenanceSpec,
    TakeoverPolicy, default_maintenance_schedule, kopia_lease_identity, kopia_owner_for_lease,
    lease_action, managed_lease, parse_run_annotations,
};
pub use repository::{Repository, RepositoryPhase, RepositorySpec, RepositoryStatus};
pub use repository_replication::{
    RepositoryReplication, RepositoryReplicationPhase, RepositoryReplicationSpec,
    RepositoryReplicationStatus,
};
pub use restore::{
    OnMissingSnapshot, PopulatorTarget, Restore, RestorePhase, RestoreSource, RestoreSpec,
    RestoreStatus, RestoreTarget,
};
pub use snapshot::{
    Origin, Snapshot, SnapshotPhase, SnapshotSpec, SnapshotStats, SnapshotStatus, SnapshotTiming,
    StagedSources,
};
pub use snapshot_policy::{
    CopyMethod, DeepVerification, GroupBy, Hook, SnapshotPolicy, SnapshotPolicySpec,
    SnapshotPolicyStatus, SourcePathStrategy, Verification,
};
pub use snapshot_schedule::{
    ConcurrencyPolicy, ScheduleSpec, SnapshotSchedule, SnapshotScheduleSpec, SnapshotScheduleStatus,
};

// Shared logic re-exports.
pub use duration::parse_go_duration;
pub use error::{ValidationError, ValidationResult};
pub use identity::{IdentityInputs, identity_string, resolve_identity, validate_identity_expr};
pub use jitter::{offset as jitter_offset, substitute_h};
pub use retention::{KeptSet, SnapshotLike, select_kept};
pub use success_expr::{
    RestoredStats, SuccessExprInputs, VerifyStats, eval_success_expr, validate_success_expr,
};

/// The CRD API group for all kopiur resources.
pub const GROUP: &str = "kopiur.home-operations.com";
/// The current (and only, per ADR §8) API version.
pub const VERSION: &str = "v1alpha1";

/// Shared test helper: parse a YAML manifest the way the cluster does
/// (YAML → JSON value → typed), reused by every CRD module's round-trip tests.
///
/// `kubectl` converts YAML to JSON before sending to the API server, and `kube`
/// (de)serializes exclusively via `serde_json`. Going straight through `serde_yaml`
/// would instead exercise its non-standard `!Variant` encoding of externally-tagged
/// enums, which the real wire format never uses — so this is the representative path.
#[cfg(test)]
pub(crate) mod testutil {
    pub(crate) fn from_yaml<T: serde::de::DeserializeOwned>(yaml: &str) -> T {
        let value: serde_json::Value = serde_yaml::from_str(yaml).expect("yaml -> json value");
        serde_json::from_value(value).expect("json value -> typed")
    }
}

#[cfg(test)]
mod roundtrip_tests {
    //! Proves the `CustomResource` derive + schemars-1 + k8s-openapi-type-reuse
    //! pattern works end to end against the exact YAML shapes in ADR §3.1.
    use super::*;
    use crate::testutil::from_yaml;
    use kube::core::CustomResourceExt;

    #[test]
    fn repository_crd_metadata_is_correct() {
        let crd = Repository::crd();
        assert_eq!(crd.spec.group, "kopiur.home-operations.com");
        assert_eq!(crd.spec.names.kind, "Repository");
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.versions[0].name, "v1alpha1");
    }

    #[test]
    fn repository_s3_roundtrip_matches_adr_shape() {
        // Mirrors ADR §3.1 / §5.1.
        let yaml = r#"
backend:
  s3:
    bucket: my-backups
    prefix: prod/
    endpoint: s3.us-east-1.amazonaws.com
    region: us-east-1
    auth:
      secretRef:
        name: nas-primary-creds
encryption:
  passwordSecretRef:
    name: nas-primary-creds
    key: KOPIA_PASSWORD
create:
  enabled: true
"#;
        let spec: RepositorySpec = from_yaml(yaml);
        // The backend is exactly one variant — the type system guarantees it.
        match &spec.backend {
            Backend::S3(s3) => {
                assert_eq!(s3.bucket, "my-backups");
                assert_eq!(s3.prefix.as_deref(), Some("prod/"));
            }
            other => panic!("expected S3 backend, got {}", other.kind_str()),
        }
        // Round-trip: serialize back and re-parse, assert structural equality.
        let json = serde_json::to_value(&spec).expect("serialize");
        let reparsed: RepositorySpec = serde_json::from_value(json).expect("reparse");
        assert_eq!(spec, reparsed);
    }

    #[test]
    fn backend_is_externally_tagged() {
        let spec: RepositorySpec = from_yaml(
            "backend:\n  filesystem:\n    path: /repo\nencryption:\n  passwordSecretRef:\n    name: s\n",
        );
        assert_eq!(spec.backend.kind_str(), "Filesystem");
        let v = serde_json::to_value(&spec.backend).unwrap();
        assert_eq!(v["filesystem"]["path"], "/repo");
    }

    #[test]
    fn filesystem_repo_volume_pvc_is_externally_tagged() {
        // `volume: { pvc: { name } }` — the externally-tagged RepoVolume wire shape.
        let spec: RepositorySpec = from_yaml(
            "backend:\n  filesystem:\n    path: /repo\n    volume:\n      pvc:\n        name: nas-repo\nencryption:\n  passwordSecretRef:\n    name: s\n",
        );
        let Backend::Filesystem(fs) = &spec.backend else {
            panic!("expected filesystem backend");
        };
        match fs.volume.as_ref().expect("volume present") {
            RepoVolume::Pvc(p) => assert_eq!(p.name, "nas-repo"),
            other => panic!("expected pvc volume, got {}", other.kind_str()),
        }
        // Round-trips through JSON under the camelCase `pvc` key.
        let v = serde_json::to_value(&spec.backend).unwrap();
        assert_eq!(v["filesystem"]["volume"]["pvc"]["name"], "nas-repo");
    }

    #[test]
    fn filesystem_repo_volume_nfs_is_externally_tagged() {
        // `volume: { nfs: { server, path } }` — inline NFS repo, no PVC.
        let spec: RepositorySpec = from_yaml(
            "backend:\n  filesystem:\n    path: /repo\n    volume:\n      nfs:\n        server: nas.lan\n        path: /export/kopia\nencryption:\n  passwordSecretRef:\n    name: s\n",
        );
        let Backend::Filesystem(fs) = &spec.backend else {
            panic!("expected filesystem backend");
        };
        match fs.volume.as_ref().expect("volume present") {
            RepoVolume::Nfs(n) => {
                assert_eq!(n.server, "nas.lan");
                assert_eq!(n.path, "/export/kopia");
            }
            other => panic!("expected nfs volume, got {}", other.kind_str()),
        }
        let v = serde_json::to_value(&spec.backend).unwrap();
        assert_eq!(v["filesystem"]["volume"]["nfs"]["server"], "nas.lan");
        assert_eq!(v["filesystem"]["volume"]["nfs"]["path"], "/export/kopia");
    }

    #[test]
    fn backup_config_nfs_source_roundtrips() {
        use crate::SnapshotPolicySpec;
        let spec: SnapshotPolicySpec = from_yaml(
            "repository:\n  name: repo\nsources:\n  - nfs:\n      server: expanse.internal\n      path: /mnt/eros/Media\n",
        );
        let src = &spec.sources[0];
        let nfs = src.nfs.as_ref().expect("nfs source present");
        assert_eq!(nfs.server, "expanse.internal");
        assert_eq!(nfs.path, "/mnt/eros/Media");
        assert!(src.pvc.is_none() && src.pvc_selector.is_none());
    }

    #[test]
    fn unknown_backend_variant_is_rejected() {
        let value: serde_json::Value = serde_yaml::from_str("dropbox:\n  bucket: x\n").unwrap();
        let err = serde_json::from_value::<Backend>(value);
        assert!(
            err.is_err(),
            "unknown backend variant must fail to deserialize"
        );
    }
}
