//! The mover work spec: the JSON contract between the controller and a mover
//! pod.
//!
//! Per ADR §4.10, the controller writes a `ConfigMap` per `Backup`/`Restore`
//! run with the resolved identity, paths, hook plan, and options; the mover
//! reads it from a downward-API-mounted file. This module is **pure data** plus
//! serde — no kube, no kopia subprocess. It is exhaustively round-trip tested.
//!
//! The spec carries *resolved* values only (identity already rendered, repo
//! connect info concrete). The mover never re-derives anything: it executes
//! exactly what the controller decided.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Which operation this mover run performs. Externally tagged so exactly one
/// operation payload is representable (mirrors the api crate's enum discipline;
/// a new variant cannot compile until every `match` handles it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Operation {
    /// Create a kopia snapshot of `source` and report stats back to the Backup.
    Backup(BackupOp),
    /// Restore a snapshot's contents into `target`.
    Restore(RestoreOp),
    /// Delete a snapshot from the repository (finalizer path, deletionPolicy:
    /// Delete).
    SnapshotDelete(SnapshotDeleteOp),
}

impl Operation {
    /// Stable discriminant string for logging/metrics.
    pub fn kind_str(&self) -> &'static str {
        match self {
            Operation::Backup(_) => "Backup",
            Operation::Restore(_) => "Restore",
            Operation::SnapshotDelete(_) => "SnapshotDelete",
        }
    }
}

/// Payload for a backup run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupOp {
    /// Absolute path inside the mover pod to snapshot (e.g. `/data`).
    pub source_path: String,
    /// Tags to attach to the snapshot (`key:value` pairs).
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

/// Payload for a restore run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreOp {
    /// The snapshot manifest id to restore from. Resolved by the controller
    /// (browse-and-reference, not a timestamp).
    pub snapshot_id: String,
    /// Absolute path inside the mover pod to restore into (e.g. `/data`).
    pub target_path: String,
}

/// Payload for a snapshot-delete run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotDeleteOp {
    /// The snapshot manifest id to delete.
    pub snapshot_id: String,
}

/// The resolved kopia identity (`username@hostname:path`). Pinned by the
/// controller at admission and never re-derived (ADR §4.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedIdentity {
    /// kopia username component.
    pub username: String,
    /// kopia hostname component.
    pub hostname: String,
    /// kopia source path component.
    pub source_path: String,
}

/// How to reach the repository. Externally tagged: exactly one backend.
///
/// This mirrors `kopiur_kopia::ConnectSpec` but is a *serializable* wire type
/// (the kopia client's `ConnectSpec` is intentionally not serde). The mover
/// converts one to the other. Credentials are NOT here: they arrive as env vars
/// (mounted Secret) so they never land in a ConfigMap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RepositoryConnect {
    /// Filesystem backend at a path.
    Filesystem {
        /// Absolute path to the repository root.
        path: String,
    },
    /// S3-compatible backend.
    S3 {
        /// Bucket name.
        bucket: String,
        /// Optional custom endpoint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        /// Optional key prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Optional region.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        region: Option<String>,
    },
}

impl RepositoryConnect {
    /// Convert to the kopia client's connect spec.
    pub fn to_connect_spec(&self) -> kopiur_kopia::ConnectSpec {
        match self {
            RepositoryConnect::Filesystem { path } => {
                kopiur_kopia::ConnectSpec::Filesystem { path: path.into() }
            }
            RepositoryConnect::S3 {
                bucket,
                endpoint,
                prefix,
                region,
            } => kopiur_kopia::ConnectSpec::S3 {
                bucket: bucket.clone(),
                endpoint: endpoint.clone(),
                prefix: prefix.clone(),
                region: region.clone(),
            },
        }
    }
}

/// A reference to the `Backup` or `Restore` CR whose `.status` the mover
/// PATCHes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetRef {
    /// The CR's `apiVersion` (e.g. `kopia.io/v1alpha1`).
    pub api_version: String,
    /// The CR kind (`Backup` or `Restore`).
    pub kind: String,
    /// The CR name.
    pub name: String,
    /// The CR namespace.
    pub namespace: String,
}

/// A summary of the hook plan the workload pod will execute. The mover does
/// *not* run hooks (ADR §4.8 — hooks run in the workload pod); it carries this
/// summary only for status/observability.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HookPlanSummary {
    /// Names of pre-hooks (executed by the controller in the workload pod).
    #[serde(default)]
    pub pre: Vec<String>,
    /// Names of post-hooks.
    #[serde(default)]
    pub post: Vec<String>,
}

/// Tunable options for the run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoverOptions {
    /// How often (seconds) to PATCH progress to the CR status. ADR §4.13 uses
    /// ~5s; configurable here.
    #[serde(default = "default_progress_interval_secs")]
    pub progress_interval_secs: u64,
    /// Overall timeout (seconds) for the kopia operation; `None` = no timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_timeout_secs: Option<u64>,
}

fn default_progress_interval_secs() -> u64 {
    5
}

impl Default for MoverOptions {
    fn default() -> Self {
        MoverOptions {
            progress_interval_secs: default_progress_interval_secs(),
            operation_timeout_secs: None,
        }
    }
}

/// The full work spec the controller writes for one mover run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoverWorkSpec {
    /// Schema version for forward compatibility.
    #[serde(default = "default_spec_version")]
    pub version: u32,
    /// The operation to perform.
    pub operation: Operation,
    /// The resolved kopia identity.
    pub identity: ResolvedIdentity,
    /// How to connect to the repository.
    pub repository: RepositoryConnect,
    /// The CR to PATCH status onto.
    pub target_ref: TargetRef,
    /// Hook plan summary (informational).
    #[serde(default)]
    pub hook_plan: HookPlanSummary,
    /// Run options.
    #[serde(default)]
    pub options: MoverOptions,
}

fn default_spec_version() -> u32 {
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_identity() -> ResolvedIdentity {
        ResolvedIdentity {
            username: "mydb".into(),
            hostname: "prod".into(),
            source_path: "/pvc/mydb".into(),
        }
    }

    fn sample_target() -> TargetRef {
        TargetRef {
            api_version: "kopia.io/v1alpha1".into(),
            kind: "Backup".into(),
            name: "mydb-20260601".into(),
            namespace: "prod".into(),
        }
    }

    fn roundtrip(spec: &MoverWorkSpec) -> MoverWorkSpec {
        let json = serde_json::to_string_pretty(spec).unwrap();
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn backup_roundtrip() {
        let mut tags = BTreeMap::new();
        tags.insert("app".into(), "mydb".into());
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::Backup(BackupOp {
                source_path: "/data".into(),
                tags,
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::Filesystem {
                path: "/repo".into(),
            },
            target_ref: sample_target(),
            hook_plan: HookPlanSummary {
                pre: vec!["fsfreeze".into()],
                post: vec!["fsunfreeze".into()],
            },
            options: MoverOptions::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "Backup");
    }

    #[test]
    fn restore_roundtrip() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::Restore(RestoreOp {
                snapshot_id: "abc123".into(),
                target_path: "/data".into(),
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::S3 {
                bucket: "backups".into(),
                endpoint: Some("https://minio.local".into()),
                prefix: Some("kopiur/".into()),
                region: None,
            },
            target_ref: TargetRef {
                kind: "Restore".into(),
                ..sample_target()
            },
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions {
                progress_interval_secs: 10,
                operation_timeout_secs: Some(3600),
            },
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "Restore");
    }

    #[test]
    fn snapshot_delete_roundtrip() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::SnapshotDelete(SnapshotDeleteOp {
                snapshot_id: "todelete".into(),
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::Filesystem {
                path: "/repo".into(),
            },
            target_ref: sample_target(),
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "SnapshotDelete");
    }

    #[test]
    fn externally_tagged_operation_shape() {
        // Assert the wire shape is externally tagged: { "backup": {...} }.
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::Backup(BackupOp {
                source_path: "/data".into(),
                tags: BTreeMap::new(),
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::Filesystem {
                path: "/repo".into(),
            },
            target_ref: sample_target(),
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
        };
        let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
        assert!(v["operation"]["backup"].is_object());
        assert!(v["operation"]["backup"]["sourcePath"].is_string());
        // Repository is externally tagged too.
        assert!(v["repository"]["filesystem"]["path"].is_string());
    }

    #[test]
    fn defaults_fill_in_when_absent() {
        // A minimal spec: omit version, hookPlan, options entirely.
        let json = r#"{
            "operation": {"snapshotDelete": {"snapshotId": "x"}},
            "identity": {"username": "u", "hostname": "h", "sourcePath": "/p"},
            "repository": {"filesystem": {"path": "/repo"}},
            "targetRef": {"apiVersion": "kopia.io/v1alpha1", "kind": "Backup", "name": "n", "namespace": "ns"}
        }"#;
        let spec: MoverWorkSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.version, 1);
        assert_eq!(spec.options.progress_interval_secs, 5);
        assert_eq!(spec.options.operation_timeout_secs, None);
        assert!(spec.hook_plan.pre.is_empty());
    }

    #[test]
    fn connect_spec_conversion() {
        let fs = RepositoryConnect::Filesystem {
            path: "/repo".into(),
        };
        assert_eq!(
            fs.to_connect_spec(),
            kopiur_kopia::ConnectSpec::Filesystem {
                path: "/repo".into()
            }
        );
        let s3 = RepositoryConnect::S3 {
            bucket: "b".into(),
            endpoint: None,
            prefix: None,
            region: Some("r".into()),
        };
        assert_eq!(
            s3.to_connect_spec(),
            kopiur_kopia::ConnectSpec::S3 {
                bucket: "b".into(),
                endpoint: None,
                prefix: None,
                region: Some("r".into()),
            }
        );
    }
}
