//! # kopiur-api
//!
//! Strongly-typed CRD definitions and shared logic for **Kopiur**, the
//! Kopia-native Kubernetes backup operator (ADR-0003).
//!
//! This crate deliberately has *no* controller-runtime dependencies (no
//! `kube::Client`, no `tokio`) so downstream tools — a custom backup-triggering
//! controller, a CI linter for `BackupConfig` manifests — can depend on the API
//! types alone (ADR §5.1).
//!
//! ## Type-safety thesis (ADR §5.5)
//!
//! Every discriminated union in the CRD surface is a Rust `enum`:
//! [`backend::Backend`], [`cluster_repository::AllowedNamespaces`],
//! [`common::DeletionPolicy`], [`restore::RestoreSource`], [`backup_config::Hook`],
//! etc. A deserialized value is always exactly one variant, and reconcilers `match`
//! exhaustively — so a new variant added later *cannot* compile until every handler
//! accounts for it. For backup software this eliminates the highest-severity class of
//! "controller silently dropped data" bugs (gap G21).

pub mod backend;
pub mod backup;
pub mod backup_config;
pub mod backup_schedule;
pub mod cluster_repository;
pub mod common;
pub mod maintenance;
pub mod repository;
pub mod restore;

// Shared pure-logic modules (no controller-runtime deps). The webhook and the
// controller both import these, so validation/resolution behavior is identical
// across the two call sites (ADR §5.1, SKILL "one validator, two callers").
pub mod error;
pub mod identity;
pub mod jitter;
pub mod retention;
pub mod validate;

pub use backend::Backend;
pub use backup::{Backup, BackupPhase, BackupSpec, BackupStatus, Origin};
pub use backup_config::{
    BackupConfig, BackupConfigSpec, BackupConfigStatus, CopyMethod, GroupBy, Hook,
    SourcePathStrategy,
};
pub use backup_schedule::{
    BackupSchedule, BackupScheduleSpec, BackupScheduleStatus, ConcurrencyPolicy, ScheduleSpec,
};
pub use cluster_repository::{
    AllowedNamespaces, ClusterRepository, ClusterRepositorySpec, ClusterRepositoryStatus,
    IdentityTemplate,
};
pub use common::{ConfigRef, CronSpec, DeletionPolicy, ObjectRef};
pub use maintenance::{Maintenance, MaintenanceSpec, MaintenanceStatus, TakeoverPolicy};
pub use repository::{Repository, RepositorySpec, RepositoryStatus};
pub use restore::{
    OnMissingSnapshot, Restore, RestorePhase, RestoreSource, RestoreSpec, RestoreStatus,
    RestoreTarget,
};

// Shared logic re-exports.
pub use error::{ValidationError, ValidationResult};
pub use identity::{IdentityInputs, identity_string, resolve_identity};
pub use jitter::{offset as jitter_offset, substitute_h};
pub use retention::{BackupLike, KeptSet, select_kept};

/// The CRD API group for all kopiur resources.
pub const GROUP: &str = "kopiur.dev";
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
        assert_eq!(crd.spec.group, "kopiur.dev");
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
    fn unknown_backend_variant_is_rejected() {
        let value: serde_json::Value = serde_yaml::from_str("dropbox:\n  bucket: x\n").unwrap();
        let err = serde_json::from_value::<Backend>(value);
        assert!(
            err.is_err(),
            "unknown backend variant must fail to deserialize"
        );
    }
}
