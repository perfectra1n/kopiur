//! The mover's typed error surface (ADR §5.5, [[actionable-error-messages]]).
//!
//! One `thiserror` enum per failure domain instead of stringly `anyhow`
//! wrapping: the structured [`kopiur_kopia::KopiaError`] (class, stderr tail,
//! exit code) survives **all the way to the status PATCH** — stringification
//! happens only at the [`FailureBlock`](crate::status::FailureBlock) /
//! log-line surface, never before. Every kopia call site names which
//! invocation failed via [`KopiaOp`], so a `kubectl logs` line or a
//! `status.failure.message` always says *what* failed, not just *that* kopia
//! exited non-zero.

use std::path::PathBuf;

use kopiur_kopia::{KopiaError, KopiaErrorClass};

/// Which kopia invocation a [`MoverError::Kopia`] failure came from. Stable,
/// human-greppable labels for messages and logs; exhaustive — a new mover flow
/// must name its operations here before it can fail (ADR §5.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KopiaOp {
    /// `repository connect` on the generic backup/restore/delete/pin path.
    RepositoryConnect,
    /// `repository throttle set` (moverDefaults.throttle).
    ThrottleSet,
    /// `policy set` against the snapshot's source identity.
    PolicySet,
    /// `snapshot create`.
    SnapshotCreate,
    /// `snapshot restore`.
    SnapshotRestore,
    /// `snapshot delete` (the Snapshot finalizer path).
    SnapshotDelete,
    /// `snapshot pin`/`unpin` reconciliation.
    SnapshotPin,
    /// `repository connect` for a maintenance run.
    MaintenanceConnect,
    /// `maintenance info` (lease holder read).
    MaintenanceInfo,
    /// `maintenance set-owner` (kopia's per-connection owner guard).
    MaintenanceSetOwner,
    /// `maintenance run`.
    MaintenanceRun,
    /// `repository connect` for a verification run.
    VerifyConnect,
    /// `snapshot verify` (quick tier).
    SnapshotVerify,
    /// `snapshot list` while resolving the deep-verify restore candidate.
    DeepVerifySnapshotList,
    /// The deep-verify scratch restore.
    DeepVerifyRestore,
    /// `repository connect` to the replication *source*.
    ReplicateConnect,
    /// `repository sync-to` the replication destination.
    RepositorySyncTo,
    /// `repository connect --readonly` for a browse session.
    BrowseConnect,
}

impl KopiaOp {
    /// The stable label used in messages/logs (matches the historical
    /// `"<op> failed (class …)"` strings, so logs stay greppable).
    pub fn as_str(&self) -> &'static str {
        match self {
            KopiaOp::RepositoryConnect => "repository connect",
            KopiaOp::ThrottleSet => "repository throttle set",
            KopiaOp::PolicySet => "policy set",
            KopiaOp::SnapshotCreate => "snapshot create",
            KopiaOp::SnapshotRestore => "snapshot restore",
            KopiaOp::SnapshotDelete => "snapshot delete",
            KopiaOp::SnapshotPin => "snapshot pin",
            KopiaOp::MaintenanceConnect => "maintenance connect",
            KopiaOp::MaintenanceInfo => "maintenance info",
            KopiaOp::MaintenanceSetOwner => "maintenance set-owner",
            KopiaOp::MaintenanceRun => "maintenance run",
            KopiaOp::VerifyConnect => "verify connect",
            KopiaOp::SnapshotVerify => "snapshot verify",
            KopiaOp::DeepVerifySnapshotList => "deep verify snapshot list",
            KopiaOp::DeepVerifyRestore => "deep verify restore",
            KopiaOp::ReplicateConnect => "replication connect",
            KopiaOp::RepositorySyncTo => "repository sync-to",
            KopiaOp::BrowseConnect => "browse session connect",
        }
    }
}

/// Everything the mover binary can fail on. Replaces the old `anyhow` paths so
/// the typed cause (and the kopia class behind it) is preserved until the
/// status PATCH / process exit.
#[derive(Debug, thiserror::Error)]
pub enum MoverError {
    /// A kopia subprocess call failed. Names the invocation and keeps the full
    /// [`KopiaError`] (class, stderr tail, exit code) as the source.
    #[error("{} failed (class {}): {}", .op.as_str(), .source.class(), .source)]
    Kopia {
        /// Which kopia invocation failed.
        op: KopiaOp,
        /// The structured kopia failure.
        #[source]
        source: KopiaError,
    },

    /// A repository bootstrap ended unsuccessfully; the class/message are read
    /// back from the persisted [`BootstrapResult`](crate::bootstrap::BootstrapResult)
    /// failure block (the class arrives as its stable label).
    #[error("repository bootstrap failed (class {class}): {message}")]
    BootstrapFailed {
        /// The kopia error class the bootstrap recorded.
        class: KopiaErrorClass,
        /// The bootstrap's persisted failure message.
        message: String,
    },

    /// No work-spec path was provided at all.
    #[error(
        "no work spec path: pass it as the first arg or set {}",
        crate::env::WORK_SPEC_PATH
    )]
    WorkSpecPathMissing,

    /// The work-spec file could not be read.
    #[error(
        "failed to read the work spec at {}: {source}. The controller mounts it via the \
         work-spec ConfigMap — check the Job's volume mount and {}",
        .path.display(),
        crate::env::WORK_SPEC_PATH
    )]
    WorkSpecRead {
        /// The path that could not be read.
        path: PathBuf,
        /// The underlying IO error.
        #[source]
        source: std::io::Error,
    },

    /// The work-spec file is not valid `MoverWorkSpec` JSON.
    #[error(
        "failed to parse the work spec at {}: {source}. The controller and mover image versions \
         may be skewed — redeploy so both run the same kopiur version",
        .path.display()
    )]
    WorkSpecParse {
        /// The path holding the malformed spec.
        path: PathBuf,
        /// The underlying JSON error.
        #[source]
        source: serde_json::Error,
    },

    /// The credential staging directory could not be created.
    #[error(
        "failed to create the credential staging dir {}: {source}. The kopia-cache emptyDir must \
         be mounted and writable by the mover's UID",
        .path.display()
    )]
    CredentialStagingDir {
        /// The staging directory.
        path: PathBuf,
        /// The underlying IO error.
        #[source]
        source: std::io::Error,
    },

    /// A file-based backend credential (SFTP key, GCS JSON, rclone.conf) could
    /// not be written from its environment variable.
    #[error(
        "failed to write the credential file {} (from ${env_key}): {source}. Check the \
         credentials Secret key and that the kopia-cache emptyDir is writable",
        .path.display()
    )]
    CredentialWrite {
        /// The env var the credential came from.
        env_key: &'static str,
        /// The destination file.
        path: PathBuf,
        /// The underlying IO error.
        #[source]
        source: std::io::Error,
    },

    /// The browse-session readiness marker could not be written, so the
    /// session pod would never turn Ready and the CLI would hang waiting.
    #[error(
        "failed to write the browse-session readiness marker {}: {source}. The kopia-cache \
         emptyDir must be mounted at /var/cache/kopia and writable by the mover's UID",
        .path.display()
    )]
    ReadyMarkerWrite {
        /// The marker path ([`crate::env::READY_MARKER`]).
        path: PathBuf,
        /// The underlying IO error.
        #[source]
        source: std::io::Error,
    },

    /// Deep verification found no snapshot to scratch-restore.
    #[error(
        "deep verify found no snapshot to restore for source path {source_path:?}; run a backup \
         first or set verify.deep.snapshotID explicitly"
    )]
    VerifyNoSnapshot {
        /// The identity source path the lookup keyed on.
        source_path: String,
    },

    /// The user's verification `successExpr` evaluated to `false`.
    #[error("verification successExpr evaluated false: {expr:?}")]
    SuccessExprFalse {
        /// The CEL expression that rejected the run.
        expr: String,
    },

    /// The verification `successExpr` could not be evaluated at all.
    #[error("verification successExpr failed to evaluate: {source}")]
    SuccessExprEval {
        /// The evaluation error (bad expression / non-bool result).
        #[source]
        source: kopiur_api::ValidationError,
    },

    /// A kube client could not be built (the side-channel status PATCHes need
    /// in-cluster ServiceAccount credentials).
    #[error(
        "failed to build a kube client: {source}. In-cluster ServiceAccount credentials are \
         required for status PATCHes"
    )]
    KubeClient {
        /// The underlying kube error (boxed: `kube::Error` is large and this
        /// enum rides in every mover `Result`).
        #[source]
        source: Box<kube::Error>,
    },

    /// A CR status PATCH failed.
    #[error("failed to PATCH the status of {kind} {namespace}/{name}: {source}")]
    StatusPatch {
        /// The target CR kind.
        kind: String,
        /// The target CR namespace.
        namespace: String,
        /// The target CR name.
        name: String,
        /// The underlying kube error (boxed, see [`MoverError::KubeClient`]).
        #[source]
        source: Box<kube::Error>,
    },

    /// The bootstrap result could not be serialized.
    #[error("failed to serialize the bootstrap result: {source}")]
    ResultSerialize {
        /// The underlying JSON error.
        #[source]
        source: serde_json::Error,
    },

    /// The bootstrap result could not be written into the work-spec ConfigMap.
    #[error(
        "failed to write the bootstrap result into ConfigMap {namespace}/{configmap}: {source}. \
         The controller cannot read the outcome — check the mover Role's ConfigMap patch \
         permission"
    )]
    ResultConfigMapPatch {
        /// The ConfigMap name.
        configmap: String,
        /// The ConfigMap namespace.
        namespace: String,
        /// The underlying kube error (boxed, see [`MoverError::KubeClient`]).
        #[source]
        source: Box<kube::Error>,
    },

    /// Telemetry init failed under `KOPIUR_OTEL_STRICT` (without strict mode it
    /// degrades inside `init_tracing` and never reaches here).
    #[error(transparent)]
    Telemetry(#[from] kopiur_telemetry::TelemetryError),
}

impl MoverError {
    /// The kopia error class this failure maps to — what
    /// [`FailureBlock`](crate::status::FailureBlock) persists and the
    /// controller keys retry decisions on. Exhaustive `match`, no `_ =>`
    /// (ADR §5.5): a new variant cannot compile until it is classified.
    ///
    /// Kopia/bootstrap failures delegate to the real class. Everything else is
    /// environmental/config — re-running the same pod will not help — so it
    /// maps to [`KopiaErrorClass::Unknown`] (non-retryable), matching how
    /// [`KopiaError::Spawn`] is treated.
    pub fn kopia_class(&self) -> KopiaErrorClass {
        match self {
            MoverError::Kopia { source, .. } => source.class(),
            MoverError::BootstrapFailed { class, .. } => *class,
            MoverError::WorkSpecPathMissing
            | MoverError::WorkSpecRead { .. }
            | MoverError::WorkSpecParse { .. }
            | MoverError::CredentialStagingDir { .. }
            | MoverError::CredentialWrite { .. }
            | MoverError::ReadyMarkerWrite { .. }
            | MoverError::VerifyNoSnapshot { .. }
            | MoverError::SuccessExprFalse { .. }
            | MoverError::SuccessExprEval { .. }
            | MoverError::KubeClient { .. }
            | MoverError::StatusPatch { .. }
            | MoverError::ResultSerialize { .. }
            | MoverError::ResultConfigMapPatch { .. }
            | MoverError::Telemetry(_) => KopiaErrorClass::Unknown,
        }
    }

    /// Whether the operator should retry the same operation (delegates to the
    /// class's own hint).
    pub fn retry_recommended(&self) -> bool {
        self.kopia_class().is_retryable()
    }
}

/// Result alias for mover code.
pub type Result<T, E = MoverError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kopia_op_labels_are_stable_for_every_variant() {
        // Loop over every op: the label must be non-empty, lowercase-stable,
        // and distinct (these strings are greppable log/message anchors).
        let all = [
            KopiaOp::RepositoryConnect,
            KopiaOp::ThrottleSet,
            KopiaOp::PolicySet,
            KopiaOp::SnapshotCreate,
            KopiaOp::SnapshotRestore,
            KopiaOp::SnapshotDelete,
            KopiaOp::SnapshotPin,
            KopiaOp::MaintenanceConnect,
            KopiaOp::MaintenanceInfo,
            KopiaOp::MaintenanceSetOwner,
            KopiaOp::MaintenanceRun,
            KopiaOp::VerifyConnect,
            KopiaOp::SnapshotVerify,
            KopiaOp::DeepVerifySnapshotList,
            KopiaOp::DeepVerifyRestore,
            KopiaOp::ReplicateConnect,
            KopiaOp::RepositorySyncTo,
            KopiaOp::BrowseConnect,
        ];
        let mut seen = std::collections::BTreeSet::new();
        for op in all {
            let label = op.as_str();
            assert!(!label.is_empty());
            assert!(seen.insert(label), "duplicate op label {label}");
        }
    }

    #[test]
    fn kopia_class_delegates_to_the_source_class() {
        // A retryable kopia failure (locked repo) stays retryable through the
        // mover wrapper; an auth failure stays terminal.
        let locked = MoverError::Kopia {
            op: KopiaOp::MaintenanceRun,
            source: KopiaError::NonZeroExit {
                args: "maintenance run".into(),
                code: Some(1),
                class: KopiaErrorClass::Locked,
                stderr_tail: "repository is locked".into(),
            },
        };
        assert_eq!(locked.kopia_class(), KopiaErrorClass::Locked);
        assert!(locked.retry_recommended());

        let auth = MoverError::BootstrapFailed {
            class: KopiaErrorClass::AuthFailure,
            message: "invalid repository password".into(),
        };
        assert_eq!(auth.kopia_class(), KopiaErrorClass::AuthFailure);
        assert!(!auth.retry_recommended());
    }

    #[test]
    fn environmental_failures_classify_unknown_and_non_retryable() {
        // Config/environment problems don't fix themselves on a blind re-run.
        let parse = MoverError::WorkSpecParse {
            path: PathBuf::from("/spec/work.json"),
            source: serde_json::from_str::<serde_json::Value>("{").unwrap_err(),
        };
        assert_eq!(parse.kopia_class(), KopiaErrorClass::Unknown);
        assert!(!parse.retry_recommended());

        let cred = MoverError::CredentialWrite {
            env_key: "KOPIA_SFTP_KEY_DATA",
            path: PathBuf::from("/kopia-cache/creds/sftp_key"),
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        };
        assert_eq!(cred.kopia_class(), KopiaErrorClass::Unknown);
    }

    // --- message texts a human acts on (the what/why/fix rule) ---

    #[test]
    fn kopia_message_preserves_the_historical_op_failed_shape() {
        let err = MoverError::Kopia {
            op: KopiaOp::MaintenanceConnect,
            source: KopiaError::NonZeroExit {
                args: "repository connect".into(),
                code: Some(1),
                class: KopiaErrorClass::RepositoryUnavailable,
                stderr_tail: "dial tcp: connection refused".into(),
            },
        };
        let msg = err.to_string();
        assert!(
            msg.starts_with("maintenance connect failed (class RepositoryUnavailable):"),
            "{msg}"
        );
        assert!(msg.contains("connection refused"), "{msg}");
    }

    #[test]
    fn bootstrap_failed_message_is_byte_identical_to_the_historical_string() {
        // The controller and e2e logs grep for this exact shape; it must not
        // drift when the anyhow! call became a typed variant.
        let err = MoverError::BootstrapFailed {
            class: KopiaErrorClass::AccessDenied,
            message: "Access Denied".into(),
        };
        assert_eq!(
            err.to_string(),
            "repository bootstrap failed (class AccessDenied): Access Denied"
        );
    }

    #[test]
    fn work_spec_messages_name_the_env_var_and_path() {
        assert!(
            MoverError::WorkSpecPathMissing
                .to_string()
                .contains("KOPIUR_WORK_SPEC_PATH")
        );
        let read = MoverError::WorkSpecRead {
            path: PathBuf::from("/spec/work.json"),
            source: std::io::Error::from(std::io::ErrorKind::NotFound),
        };
        let msg = read.to_string();
        assert!(msg.contains("/spec/work.json"), "{msg}");
        assert!(msg.contains("work-spec ConfigMap"), "{msg}");
    }

    #[test]
    fn credential_write_names_the_env_key_and_the_fix() {
        let err = MoverError::CredentialWrite {
            env_key: "KOPIA_GCS_CREDENTIALS",
            path: PathBuf::from("/kopia-cache/creds/gcs-credentials.json"),
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        };
        let msg = err.to_string();
        assert!(msg.contains("$KOPIA_GCS_CREDENTIALS"), "{msg}");
        assert!(msg.contains("credentials Secret"), "{msg}");
        assert!(msg.contains("emptyDir is writable"), "{msg}");
    }

    #[test]
    fn ready_marker_write_names_the_path_and_the_fix() {
        let err = MoverError::ReadyMarkerWrite {
            path: PathBuf::from(crate::env::READY_MARKER),
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("/var/cache/kopia/.kopiur-session-ready"),
            "{msg}"
        );
        assert!(msg.contains("writable by the mover's UID"), "{msg}");
        assert_eq!(err.kopia_class(), KopiaErrorClass::Unknown);
    }

    #[test]
    fn success_expr_messages_match_the_historical_patch_bodies() {
        let f = MoverError::SuccessExprFalse {
            expr: "stats.files > 0".into(),
        };
        assert_eq!(
            f.to_string(),
            "verification successExpr evaluated false: \"stats.files > 0\""
        );
    }

    #[test]
    fn source_chain_is_preserved() {
        let err = MoverError::Kopia {
            op: KopiaOp::SnapshotCreate,
            source: KopiaError::EmptyOutput {
                context: "snapshot create".into(),
            },
        };
        assert!(
            std::error::Error::source(&err).is_some(),
            "the KopiaError source must stay inspectable"
        );
    }
}
