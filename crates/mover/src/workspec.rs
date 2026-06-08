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
    /// Bootstrap a repository: connect (adopt an existing repo), or — when
    /// `autoCreate` and the backend is reachable with valid creds — create it,
    /// then report its identity + catalog back to the controller. The
    /// connect/create lifecycle for object-store backends the controller cannot
    /// reach in-process (ADR §5.4). Result is written to the work-spec ConfigMap,
    /// not the CR status (the controller owns the Repository status).
    BootstrapRepository(BootstrapRepositoryOp),
    /// Run `kopia maintenance run` (quick or full) for a repository the
    /// controller cannot reach in-process. The mover reads the ownership lease,
    /// applies the takeover policy, runs maintenance when it holds the lease, and
    /// PATCHes the `Maintenance` `.status` directly (ADR §3.7/§5.4).
    Maintenance(MaintenanceOp),
}

impl Operation {
    /// Stable discriminant string for logging/metrics.
    pub fn kind_str(&self) -> &'static str {
        match self {
            Operation::Backup(_) => "Backup",
            Operation::Restore(_) => "Restore",
            Operation::SnapshotDelete(_) => "SnapshotDelete",
            Operation::BootstrapRepository(_) => "BootstrapRepository",
            Operation::Maintenance(_) => "Maintenance",
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
    /// `--[no-]ignore-permission-errors` (Restore CRD `options`; kopia default
    /// true). `None` lets kopia use its default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_permission_errors: Option<bool>,
    /// `--[no-]write-files-atomically` (Restore CRD `options`). `None` lets kopia
    /// use its default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_files_atomically: Option<bool>,
}

impl RestoreOp {
    /// Translate the carried restore flags into the kopia client's options.
    ///
    /// ```
    /// use kopiur_mover::workspec::RestoreOp;
    ///
    /// let op = RestoreOp {
    ///     snapshot_id: "k1".into(),
    ///     target_path: "/data".into(),
    ///     ignore_permission_errors: Some(false),
    ///     write_files_atomically: Some(true),
    /// };
    /// let opts = op.restore_options();
    /// assert_eq!(opts.ignore_permission_errors, Some(false));
    /// assert_eq!(opts.write_files_atomically, Some(true));
    /// ```
    pub fn restore_options(&self) -> kopiur_kopia::RestoreOptions {
        kopiur_kopia::RestoreOptions {
            ignore_permission_errors: self.ignore_permission_errors,
            write_files_atomically: self.write_files_atomically,
            ..Default::default()
        }
    }
}

/// Payload for a snapshot-delete run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotDeleteOp {
    /// The snapshot manifest id to delete.
    pub snapshot_id: String,
}

/// Payload for a repository-bootstrap run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapRepositoryOp {
    /// Create the repository when connect fails AND the backend is reachable
    /// with valid credentials (mirrors `Repository.spec.create.enabled`). The
    /// connect-first ordering means an existing repo is always adopted, never
    /// recreated; create is gated so a wrong password / locked repo is surfaced
    /// instead of silently spawning a second repository.
    #[serde(default)]
    pub auto_create: bool,
    /// Run `snapshot list` and return the entries so the controller can
    /// materialize `origin: discovered` Backup CRs. The snapshot *count* is
    /// always reported; the entries are only returned when this is set (the
    /// controller sets it for namespaced `Repository`, not `ClusterRepository`,
    /// whose cross-namespace placement is a separate concern).
    #[serde(default)]
    pub scan_catalog: bool,
}

/// Payload for a maintenance run.
///
/// The controller decides *which* pass is due (full subsumes quick) and passes
/// the lease parameters down; the mover makes the lease decision because reading
/// the current holder requires repo access (`kopia maintenance info`), which the
/// controller does not have for object stores. ADR §3.7.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceOp {
    /// Which pass to run when the lease is held: quick (index/log) or full
    /// (content reclamation).
    pub mode: kopiur_kopia::MaintenanceMode,
    /// This `Maintenance`'s configured lease holder identity
    /// (`spec.ownership.owner`); compared against the repo's current holder.
    pub owner: String,
    /// What to do if the lease is held by a *different* owner. ADR §3.7.
    #[serde(default)]
    pub takeover_policy: kopiur_api::TakeoverPolicy,
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
///
/// The variants mirror the eight CRD `Backend` kinds one-to-one, so the
/// controller's `Backend -> RepositoryConnect` map is exhaustive (a new backend
/// cannot compile until it is wired through to the mover).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
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
        /// Talk plain HTTP (`--disable-tls`) for HTTP-only endpoints.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        disable_tls: bool,
        /// Skip TLS certificate verification (`--disable-tls-verification`).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        disable_tls_verification: bool,
    },
    /// Azure Blob Storage backend.
    Azure {
        /// Blob container name.
        container: String,
        /// Storage account name (when not supplied via env).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        storage_account: Option<String>,
        /// Optional object prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
    },
    /// Google Cloud Storage backend.
    Gcs {
        /// Bucket name.
        bucket: String,
        /// Optional object prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
    },
    /// Backblaze B2 backend.
    B2 {
        /// Bucket name.
        bucket: String,
        /// Optional object prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
    },
    /// SFTP/SSH backend.
    Sftp {
        /// Server hostname.
        host: String,
        /// Path to the repository on the server.
        path: String,
        /// Server port (defaults to 22 when absent).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
        /// SSH username.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        username: Option<String>,
        /// Path to a private key file inside the mover pod.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        keyfile: Option<String>,
    },
    /// WebDAV backend.
    WebDav {
        /// WebDAV server URL.
        url: String,
    },
    /// Rclone backend.
    Rclone {
        /// Rclone `remote:path`.
        remote_path: String,
    },
}

impl RepositoryConnect {
    /// Stable backend discriminant for logging. Exhaustive: a new backend
    /// variant fails to compile until handled.
    pub fn kind_str(&self) -> &'static str {
        match self {
            RepositoryConnect::Filesystem { .. } => "Filesystem",
            RepositoryConnect::S3 { .. } => "S3",
            RepositoryConnect::Azure { .. } => "Azure",
            RepositoryConnect::Gcs { .. } => "Gcs",
            RepositoryConnect::B2 { .. } => "B2",
            RepositoryConnect::Sftp { .. } => "Sftp",
            RepositoryConnect::WebDav { .. } => "WebDav",
            RepositoryConnect::Rclone { .. } => "Rclone",
        }
    }

    /// Convert to the kopia client's connect spec. Exhaustive: a new backend
    /// variant fails to compile until handled.
    ///
    /// ```
    /// use kopiur_mover::workspec::RepositoryConnect;
    /// use kopiur_kopia::ConnectSpec;
    ///
    /// let wire = RepositoryConnect::Filesystem { path: "/repo".into() };
    /// assert_eq!(wire.kind_str(), "Filesystem");
    /// assert_eq!(
    ///     wire.to_connect_spec(),
    ///     ConnectSpec::Filesystem { path: "/repo".into() },
    /// );
    /// ```
    pub fn to_connect_spec(&self) -> kopiur_kopia::ConnectSpec {
        use kopiur_kopia::ConnectSpec;
        match self {
            RepositoryConnect::Filesystem { path } => ConnectSpec::Filesystem { path: path.into() },
            RepositoryConnect::S3 {
                bucket,
                endpoint,
                prefix,
                region,
                disable_tls,
                disable_tls_verification,
            } => ConnectSpec::S3 {
                bucket: bucket.clone(),
                endpoint: endpoint.clone(),
                prefix: prefix.clone(),
                region: region.clone(),
                disable_tls: *disable_tls,
                disable_tls_verification: *disable_tls_verification,
            },
            RepositoryConnect::Azure {
                container,
                storage_account,
                prefix,
            } => ConnectSpec::Azure {
                container: container.clone(),
                storage_account: storage_account.clone(),
                prefix: prefix.clone(),
            },
            RepositoryConnect::Gcs { bucket, prefix } => ConnectSpec::Gcs {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                // The service-account JSON path is materialized by the mover from
                // the credentials Secret at runtime (see `crate::credentials`).
                credentials_file: None,
            },
            RepositoryConnect::B2 { bucket, prefix } => ConnectSpec::B2 {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
            },
            RepositoryConnect::Sftp {
                host,
                path,
                port,
                username,
                keyfile,
            } => ConnectSpec::Sftp {
                host: host.clone(),
                path: path.clone(),
                port: *port,
                username: username.clone(),
                keyfile: keyfile.clone(),
                // keyfile/known_hosts are materialized by the mover from the
                // credentials Secret at runtime (see `crate::credentials`).
                known_hosts: None,
            },
            RepositoryConnect::WebDav { url } => ConnectSpec::WebDav { url: url.clone() },
            RepositoryConnect::Rclone { remote_path } => ConnectSpec::Rclone {
                remote_path: remote_path.clone(),
                // rclone.conf is materialized by the mover from the config Secret
                // at runtime (see `crate::credentials`).
                config_file: None,
            },
        }
    }
}

/// A reference to the `Backup` or `Restore` CR whose `.status` the mover
/// PATCHes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetRef {
    /// The CR's `apiVersion` (e.g. `kopiur.home-operations.com/v1alpha1`).
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
///
/// This is the controller↔mover JSON contract (ADR §4.10): the controller
/// serializes it into a `ConfigMap`, the mover deserializes it from a mounted
/// file. It round-trips losslessly, and externally-tagged enums keep the wire
/// shape `{ "backup": {...} }` / `{ "filesystem": {...} }`:
///
/// ```
/// use std::collections::BTreeMap;
/// use kopiur_mover::workspec::*;
///
/// let spec = MoverWorkSpec {
///     version: 1,
///     operation: Operation::Backup(BackupOp {
///         source_path: "/data".into(),
///         tags: BTreeMap::new(),
///     }),
///     identity: ResolvedIdentity {
///         username: "mydb".into(),
///         hostname: "prod".into(),
///         source_path: "/data".into(),
///     },
///     repository: RepositoryConnect::Filesystem { path: "/repo".into() },
///     target_ref: TargetRef {
///         api_version: "kopiur.home-operations.com/v1alpha1".into(),
///         kind: "Backup".into(),
///         name: "mydb-20260601".into(),
///         namespace: "prod".into(),
///     },
///     hook_plan: HookPlanSummary::default(),
///     options: MoverOptions::default(),
///     cache: kopiur_kopia::CacheTuning::default(),
/// };
///
/// // Round-trips through serde_json unchanged.
/// let json = serde_json::to_string(&spec).unwrap();
/// let back: MoverWorkSpec = serde_json::from_str(&json).unwrap();
/// assert_eq!(back, spec);
///
/// // Externally tagged on the wire (camelCase keys).
/// let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
/// assert_eq!(v["operation"]["backup"]["sourcePath"], "/data");
/// assert_eq!(v["repository"]["filesystem"]["path"], "/repo");
/// assert_eq!(spec.operation.kind_str(), "Backup");
/// ```
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
    /// kopia cache budgets applied when this mover connects to the repository
    /// (`--content-cache-size-mb` / `--metadata-cache-size-mb`). The controller
    /// resolves these from the repository's `cacheDefaults` overlaid by the run's
    /// `mover.cache`. Unset leaves kopia's defaults.
    #[serde(default)]
    pub cache: kopiur_kopia::CacheTuning,
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
            api_version: "kopiur.home-operations.com/v1alpha1".into(),
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
            cache: Default::default(),
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
                ignore_permission_errors: Some(true),
                write_files_atomically: Some(false),
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::S3 {
                bucket: "backups".into(),
                endpoint: Some("https://minio.local".into()),
                prefix: Some("kopiur/".into()),
                region: None,
                disable_tls: false,
                disable_tls_verification: false,
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
            cache: Default::default(),
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
            cache: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "SnapshotDelete");
    }

    #[test]
    fn bootstrap_repository_roundtrip_and_wire_shape() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::BootstrapRepository(BootstrapRepositoryOp {
                auto_create: true,
                scan_catalog: true,
            }),
            identity: sample_identity(),
            repository: RepositoryConnect::S3 {
                bucket: "b".into(),
                endpoint: Some("minio:9000".into()),
                prefix: None,
                region: None,
                disable_tls: true,
                disable_tls_verification: false,
            },
            target_ref: TargetRef {
                kind: "Repository".into(),
                ..sample_target()
            },
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
            cache: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "BootstrapRepository");
        let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
        // Externally tagged: { "bootstrapRepository": { "autoCreate": true, ... } }.
        assert_eq!(v["operation"]["bootstrapRepository"]["autoCreate"], true);
        assert_eq!(v["operation"]["bootstrapRepository"]["scanCatalog"], true);
        // S3 disable-tls flows on the wire (camelCase, omitted when false).
        assert_eq!(v["repository"]["s3"]["disableTls"], true);
        assert!(
            v["repository"]["s3"]
                .get("disableTlsVerification")
                .is_none()
        );
    }

    #[test]
    fn maintenance_roundtrip_and_wire_shape() {
        let spec = MoverWorkSpec {
            version: 1,
            operation: Operation::Maintenance(MaintenanceOp {
                mode: kopiur_kopia::MaintenanceMode::Full,
                owner: "kopiur/prod/nas-primary".into(),
                takeover_policy: kopiur_api::TakeoverPolicy::Force,
            }),
            identity: ResolvedIdentity {
                username: "kopiur-maintenance".into(),
                hostname: "prod".into(),
                source_path: String::new(),
            },
            repository: RepositoryConnect::S3 {
                bucket: "b".into(),
                endpoint: Some("minio:9000".into()),
                prefix: None,
                region: None,
                disable_tls: true,
                disable_tls_verification: false,
            },
            target_ref: TargetRef {
                kind: "Maintenance".into(),
                ..sample_target()
            },
            hook_plan: HookPlanSummary::default(),
            options: MoverOptions::default(),
            cache: Default::default(),
        };
        assert_eq!(roundtrip(&spec), spec);
        assert_eq!(spec.operation.kind_str(), "Maintenance");
        let v: serde_json::Value = serde_json::to_value(&spec).unwrap();
        // Externally tagged: { "maintenance": { "mode": "full", "owner": ... } }.
        assert_eq!(v["operation"]["maintenance"]["mode"], "full");
        assert_eq!(
            v["operation"]["maintenance"]["owner"],
            "kopiur/prod/nas-primary"
        );
        assert_eq!(v["operation"]["maintenance"]["takeoverPolicy"], "Force");
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
            cache: Default::default(),
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
            "targetRef": {"apiVersion": "kopiur.home-operations.com/v1alpha1", "kind": "Backup", "name": "n", "namespace": "ns"}
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
            disable_tls: false,
            disable_tls_verification: false,
        };
        assert_eq!(
            s3.to_connect_spec(),
            kopiur_kopia::ConnectSpec::S3 {
                bucket: "b".into(),
                endpoint: None,
                prefix: None,
                region: Some("r".into()),
                disable_tls: false,
                disable_tls_verification: false,
            }
        );
    }

    #[test]
    fn object_store_backends_convert_and_roundtrip() {
        use kopiur_kopia::ConnectSpec;
        // One representative per non-trivial backend: assert both the wire
        // round-trip and the conversion to the kopia client spec.
        let cases: Vec<(RepositoryConnect, ConnectSpec)> = vec![
            (
                RepositoryConnect::Azure {
                    container: "c".into(),
                    storage_account: Some("acct".into()),
                    prefix: None,
                },
                ConnectSpec::Azure {
                    container: "c".into(),
                    storage_account: Some("acct".into()),
                    prefix: None,
                },
            ),
            (
                RepositoryConnect::Gcs {
                    bucket: "b".into(),
                    prefix: Some("p/".into()),
                },
                ConnectSpec::Gcs {
                    bucket: "b".into(),
                    prefix: Some("p/".into()),
                    credentials_file: None,
                },
            ),
            (
                RepositoryConnect::B2 {
                    bucket: "b".into(),
                    prefix: None,
                },
                ConnectSpec::B2 {
                    bucket: "b".into(),
                    prefix: None,
                },
            ),
            (
                RepositoryConnect::Sftp {
                    host: "h".into(),
                    path: "/r".into(),
                    port: Some(2222),
                    username: Some("u".into()),
                    keyfile: Some("/k".into()),
                },
                ConnectSpec::Sftp {
                    host: "h".into(),
                    path: "/r".into(),
                    port: Some(2222),
                    username: Some("u".into()),
                    keyfile: Some("/k".into()),
                    known_hosts: None,
                },
            ),
            (
                RepositoryConnect::WebDav {
                    url: "https://dav".into(),
                },
                ConnectSpec::WebDav {
                    url: "https://dav".into(),
                },
            ),
            (
                RepositoryConnect::Rclone {
                    remote_path: "r:bucket".into(),
                },
                ConnectSpec::Rclone {
                    remote_path: "r:bucket".into(),
                    config_file: None,
                },
            ),
        ];
        for (wire, expected_spec) in cases {
            // Wire round-trip (externally tagged, camelCase).
            let json = serde_json::to_string(&wire).unwrap();
            let back: RepositoryConnect = serde_json::from_str(&json).unwrap();
            assert_eq!(back, wire, "round-trip for {json}");
            // Conversion to the kopia client spec.
            assert_eq!(wire.to_connect_spec(), expected_spec);
        }
    }

    #[test]
    fn restore_op_maps_options_and_defaults_absent() {
        // Options present → mapped onto the kopia client options.
        let op = RestoreOp {
            snapshot_id: "s".into(),
            target_path: "/data".into(),
            ignore_permission_errors: Some(false),
            write_files_atomically: Some(true),
        };
        let opts = op.restore_options();
        assert_eq!(opts.ignore_permission_errors, Some(false));
        assert_eq!(opts.write_files_atomically, Some(true));

        // Older wire payload without the option fields still deserializes
        // (forward/backward compatible), mapping to kopia defaults (None).
        let json = r#"{"snapshotId":"s","targetPath":"/data"}"#;
        let parsed: RestoreOp = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.ignore_permission_errors, None);
        assert_eq!(parsed.restore_options().write_files_atomically, None);
    }

    #[test]
    fn azure_wire_shape_is_external_camel_case() {
        let wire = RepositoryConnect::Azure {
            container: "c".into(),
            storage_account: Some("acct".into()),
            prefix: None,
        };
        let v: serde_json::Value = serde_json::to_value(&wire).unwrap();
        assert!(v["azure"]["container"].is_string());
        assert_eq!(v["azure"]["storageAccount"], "acct");
        // prefix omitted when None.
        assert!(v["azure"].get("prefix").is_none());
    }
}
