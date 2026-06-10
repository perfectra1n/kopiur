//! Error types for kopia subprocess invocation and JSON parsing.
//!
//! A terminal [`KopiaError`] must carry enough structured detail for the mover
//! to build a `status.failure` block (ADR §4.10): exit code, the last lines of
//! stderr, and a best-effort *error class* derived from kopia's stderr so the
//! controller can decide whether a retry is worthwhile.

use std::fmt;

/// How many trailing lines of stderr we retain on a failed invocation. Kopia
/// can print a lot of progress to stderr; the tail is where the actual error
/// message lands.
pub const STDERR_TAIL_LINES: usize = 20;

/// A best-effort classification of a kopia failure, derived by inspecting the
/// captured stderr. This is intentionally coarse — it exists to drive the
/// "should we retry?" decision in the mover, not to be exhaustive. Unknown
/// failures map to [`KopiaErrorClass::Unknown`] and are treated as
/// non-retryable by default.
///
/// Classification reads kopia's stderr; the class then drives the retry hint and
/// round-trips through its stable label:
///
/// ```
/// use kopiur_kopia::KopiaErrorClass;
///
/// // A backend down / unreachable error is transient → worth a retry.
/// let class = KopiaErrorClass::classify("ERROR error connecting to repository: dial tcp");
/// assert_eq!(class, KopiaErrorClass::RepositoryUnavailable);
/// assert!(class.is_retryable());
///
/// // A wrong repository password is not retryable without a config change.
/// let auth = KopiaErrorClass::classify("invalid repository password");
/// assert_eq!(auth, KopiaErrorClass::AuthFailure);
/// assert!(!auth.is_retryable());
///
/// // The stable label round-trips through from_label/as_str.
/// assert_eq!(class.as_str(), "RepositoryUnavailable");
/// assert_eq!(KopiaErrorClass::from_label("RepositoryUnavailable"), class);
/// // An unrecognized label degrades to Unknown.
/// assert_eq!(KopiaErrorClass::from_label("bogus"), KopiaErrorClass::Unknown);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KopiaErrorClass {
    /// Repository could not be reached / opened (network, backend down,
    /// bad endpoint). Typically transient → retry.
    RepositoryUnavailable,
    /// Authentication / password / credential failure (wrong repository
    /// password). Not retryable without a config change.
    AuthFailure,
    /// The storage backend **denied access** to the bucket/container/object
    /// (e.g. S3/B2/GCS "Access Denied", HTTP 403). The credentials usually
    /// authenticate fine but lack permission — or the bucket/path doesn't exist
    /// and the backend masks that as access-denied (RustFS/S3 do this). Not
    /// retryable without a credentials/permission/bucket fix.
    AccessDenied,
    /// The repository **path is not writable by this process** — e.g. a
    /// filesystem repo whose PVC/NFS export is not writable by the operator's
    /// UID ("permission denied" / EACCES when connecting or creating). Not
    /// retryable without fixing ownership/mode.
    PermissionDenied,
    /// The requested source path / snapshot / target was not found.
    NotFound,
    /// A repository lock is held by another writer. Often transient → retry.
    Locked,
    /// Source filesystem error during upload (I/O, prepare failure).
    SourceError,
    /// Anything we could not classify.
    Unknown,
}

impl KopiaErrorClass {
    /// Stable string form for status fields / metrics labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            KopiaErrorClass::RepositoryUnavailable => "RepositoryUnavailable",
            KopiaErrorClass::AuthFailure => "AuthFailure",
            KopiaErrorClass::AccessDenied => "AccessDenied",
            KopiaErrorClass::PermissionDenied => "PermissionDenied",
            KopiaErrorClass::NotFound => "NotFound",
            KopiaErrorClass::Locked => "Locked",
            KopiaErrorClass::SourceError => "SourceError",
            KopiaErrorClass::Unknown => "Unknown",
        }
    }

    /// Inverse of [`as_str`](Self::as_str): reconstruct the class from its stable
    /// label. Used when only the persisted string is available (the controller
    /// reads `result.failure.kopiaErrorClass` from a bootstrap Job's ConfigMap).
    /// An unrecognized label maps to [`KopiaErrorClass::Unknown`].
    pub fn from_label(s: &str) -> KopiaErrorClass {
        match s {
            "RepositoryUnavailable" => KopiaErrorClass::RepositoryUnavailable,
            "AuthFailure" => KopiaErrorClass::AuthFailure,
            "AccessDenied" => KopiaErrorClass::AccessDenied,
            "PermissionDenied" => KopiaErrorClass::PermissionDenied,
            "NotFound" => KopiaErrorClass::NotFound,
            "Locked" => KopiaErrorClass::Locked,
            "SourceError" => KopiaErrorClass::SourceError,
            _ => KopiaErrorClass::Unknown,
        }
    }

    /// A **stable**, volatile-free one-line summary of what this class means and
    /// how to fix it, suitable for a status *condition message*.
    ///
    /// Unlike `KopiaError::to_string` (which embeds the kopia stderr tail — and
    /// thus a per-attempt-random temp filename like `.shards.tmp.<hex>`), this is
    /// byte-identical across repeated failures of the same class. The controller
    /// uses it for the persisted condition so that re-writing an unchanged Failed
    /// status is a true no-op (no resourceVersion bump → no self-triggered
    /// reconcile). The full, volatile detail still goes to the Warning Event.
    pub fn summary(&self) -> &'static str {
        match self {
            KopiaErrorClass::RepositoryUnavailable => {
                "repository backend is unreachable; check the endpoint/network and retry"
            }
            KopiaErrorClass::AuthFailure => {
                "repository password was rejected; check the encryption password Secret \
                 (the KOPIA_PASSWORD key)"
            }
            KopiaErrorClass::AccessDenied => {
                "the storage backend denied access; check the credentials Secret and that the \
                 bucket/container/path exists and is reachable"
            }
            KopiaErrorClass::PermissionDenied => {
                "repository path is not writable by the operator's UID; fix ownership/mode on the \
                 backing PVC/NFS export"
            }
            KopiaErrorClass::NotFound => {
                "the requested repository path, snapshot, or target was not found"
            }
            KopiaErrorClass::Locked => {
                "a repository lock is held by another writer; it usually clears on retry"
            }
            KopiaErrorClass::SourceError => "a source filesystem error occurred during upload",
            KopiaErrorClass::Unknown => "an unclassified repository backend error occurred",
        }
    }

    /// Whether re-running the same operation later might succeed without any
    /// configuration change. This is the operator's default retry hint; the
    /// caller may override it with policy.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            KopiaErrorClass::RepositoryUnavailable
                | KopiaErrorClass::Locked
                | KopiaErrorClass::SourceError
        )
    }

    /// Best-effort classification from captured stderr text. Matches against
    /// substrings kopia is observed to emit (kopia 0.23). Order matters: more
    /// specific checks come first.
    pub fn classify(stderr: &str) -> KopiaErrorClass {
        let s = stderr.to_ascii_lowercase();
        if s.contains("invalid repository password")
            || s.contains("incorrect password")
            || s.contains("unable to derive")
        {
            KopiaErrorClass::AuthFailure
        } else if s.contains("access denied")
            || s.contains("accessdenied")
            || s.contains("forbidden")
            || s.contains("not authorized")
        {
            // Backend authorization (e.g. S3 "Access Denied"). Checked before the
            // generic permission/not-found arms because the backend phrasing is
            // specific and the fix (creds/bucket) is distinct.
            KopiaErrorClass::AccessDenied
        } else if s.contains("permission denied")
            || s.contains("operation not permitted")
            || s.contains("eacces")
        {
            // Local repo path not writable by our UID. Checked before SourceError
            // (which used to absorb "permission denied" and wrongly mark it
            // retryable) and before NotFound.
            KopiaErrorClass::PermissionDenied
        } else if s.contains("repository is locked")
            || s.contains("another process")
            || s.contains("lock")
        {
            KopiaErrorClass::Locked
        } else if s.contains("no such file or directory")
            || s.contains("not found")
            || s.contains("does not exist")
            || s.contains("unable to find snapshot")
            // kopia's `repo.ErrRepositoryNotInitialized` ("repository not initialized
            // in the provided storage") — an *empty* backend with no kopia repo at the
            // prefix. The CLI wraps it as `error connecting to repository: repository
            // not initialized ...`, so it MUST be matched here, ahead of the
            // RepositoryUnavailable arm, or an uninitialized repo is misread as an
            // unreachable backend. Classifying it `NotFound` is what lets the mover
            // surface the actionable `RepositoryNotInitialized` outcome (set
            // `spec.create.enabled: true`) instead of a misleading "backend
            // unreachable, retry".
            || s.contains("not initialized")
        {
            KopiaErrorClass::NotFound
        } else if s.contains("error connecting to repository")
            || s.contains("unable to open repository")
            || s.contains("connection refused")
            || s.contains("dial tcp")
            || s.contains("no route to host")
            || s.contains("timeout")
        {
            KopiaErrorClass::RepositoryUnavailable
        } else if s.contains("upload error") || s.contains("failed to prepare source") {
            KopiaErrorClass::SourceError
        } else {
            KopiaErrorClass::Unknown
        }
    }
}

impl fmt::Display for KopiaErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Errors produced while invoking kopia or parsing its `--json` output.
///
/// Each variant's `Display` is actionable — it names the operation, the failure,
/// and (for non-zero exits) the error class plus the stderr tail — so it can be
/// dropped straight into a `status.failure` block (ADR §4.10):
///
/// ```
/// use kopiur_kopia::{KopiaError, KopiaErrorClass};
///
/// let err = KopiaError::NonZeroExit {
///     args: "snapshot create".into(),
///     code: Some(1),
///     class: KopiaErrorClass::Locked,
///     stderr_tail: "repository is locked by another process".into(),
/// };
/// assert_eq!(
///     err.to_string(),
///     "kopia `snapshot create` exited with code Some(1) (class Locked): \
///      repository is locked by another process",
/// );
/// // The class drives the retry decision; the stderr tail is recoverable.
/// assert_eq!(err.class(), KopiaErrorClass::Locked);
/// assert!(err.class().is_retryable());
/// assert_eq!(err.stderr_tail(), Some("repository is locked by another process"));
///
/// // A timeout names the args and elapsed seconds, and maps to a retryable class.
/// let to = KopiaError::Timeout { args: "maintenance run --full".into(), seconds: 3600 };
/// assert_eq!(to.to_string(), "kopia `maintenance run --full` timed out after 3600s");
/// assert_eq!(to.class(), KopiaErrorClass::RepositoryUnavailable);
/// ```
#[derive(thiserror::Error, Debug)]
pub enum KopiaError {
    /// The kopia binary could not be spawned at all (missing binary, not
    /// executable, fork failure). Carries the OS error.
    #[error("failed to spawn kopia binary `{binary}`: {source}")]
    Spawn {
        /// Path we attempted to execute.
        binary: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// kopia ran but exited with a non-zero status. Carries everything needed
    /// to build a `status.failure` block.
    #[error("kopia `{args}` exited with code {code:?} (class {class}): {stderr_tail}")]
    NonZeroExit {
        /// The subcommand + args that were run (for diagnostics; secrets are
        /// passed via env, never argv).
        args: String,
        /// Process exit code, if one was reported (None if killed by signal).
        code: Option<i32>,
        /// Best-effort error classification from stderr.
        class: KopiaErrorClass,
        /// The last [`STDERR_TAIL_LINES`] lines of stderr, joined by newlines.
        stderr_tail: String,
    },

    /// kopia exited 0 (or produced output) but the JSON could not be parsed
    /// into the expected type — usually a kopia version skew.
    #[error("failed to parse kopia JSON output for `{context}`: {source}")]
    Json {
        /// What we were trying to parse (e.g. "snapshot create result").
        context: String,
        /// The serde error.
        #[source]
        source: serde_json::Error,
    },

    /// We expected a JSON object/array on stdout but found none (kopia printed
    /// only progress / nothing).
    #[error("no JSON output found on stdout for `{context}`")]
    EmptyOutput {
        /// What we were trying to parse.
        context: String,
    },

    /// The operation exceeded its configured timeout and was killed.
    #[error("kopia `{args}` timed out after {seconds}s")]
    Timeout {
        /// The subcommand + args that were run.
        args: String,
        /// The timeout that elapsed, in seconds.
        seconds: u64,
    },
}

impl KopiaError {
    /// The error class for this error, for retry decisions and metrics. Spawn,
    /// JSON-parse, empty-output, and timeout errors map to a fixed class;
    /// non-zero exits carry their own classification.
    pub fn class(&self) -> KopiaErrorClass {
        match self {
            KopiaError::NonZeroExit { class, .. } => *class,
            // A spawn failure is environmental (bad image / missing binary) —
            // retrying the same pod won't help, treat as Unknown/non-retryable.
            KopiaError::Spawn { .. } => KopiaErrorClass::Unknown,
            KopiaError::Json { .. } | KopiaError::EmptyOutput { .. } => KopiaErrorClass::Unknown,
            // Timeouts are usually a slow backend → worth a retry.
            KopiaError::Timeout { .. } => KopiaErrorClass::RepositoryUnavailable,
        }
    }

    /// The trailing stderr lines, if this error captured any.
    pub fn stderr_tail(&self) -> Option<&str> {
        match self {
            KopiaError::NonZeroExit { stderr_tail, .. } => Some(stderr_tail.as_str()),
            _ => None,
        }
    }
}

/// Keep only the last `STDERR_TAIL_LINES` non-empty-trimmed lines of a stderr
/// blob, joined by newlines. Used when building a [`KopiaError::NonZeroExit`].
pub(crate) fn tail_lines(stderr: &str) -> String {
    let lines: Vec<&str> = stderr.lines().filter(|l| !l.trim().is_empty()).collect();
    let start = lines.len().saturating_sub(STDERR_TAIL_LINES);
    lines[start..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_known_patterns() {
        assert_eq!(
            KopiaErrorClass::classify("ERROR error connecting to repository: dial tcp ..."),
            KopiaErrorClass::RepositoryUnavailable
        );
        assert_eq!(
            KopiaErrorClass::classify("invalid repository password"),
            KopiaErrorClass::AuthFailure
        );
        assert_eq!(
            KopiaErrorClass::classify("lstat /nope: no such file or directory"),
            KopiaErrorClass::NotFound
        );
        assert_eq!(
            KopiaErrorClass::classify("repository is locked by another process"),
            KopiaErrorClass::Locked
        );
        assert_eq!(
            KopiaErrorClass::classify("upload error: unsupported source"),
            KopiaErrorClass::SourceError
        );
        assert_eq!(
            KopiaErrorClass::classify("something totally unexpected"),
            KopiaErrorClass::Unknown
        );
    }

    #[test]
    fn classify_uninitialized_repository_as_not_found() {
        // Regression: connecting to an empty backend (no kopia repo at the prefix)
        // makes kopia emit `repo.ErrRepositoryNotInitialized`, which the CLI wraps
        // with its generic connect prefix. That prefix matches the
        // RepositoryUnavailable arm, so without an explicit "not initialized" check
        // the empty-bucket case was misclassified as a transient unreachable backend
        // — and the mover's `not_initialized()` path (keyed on NotFound) never fired,
        // so the operator saw "backend unreachable; retry" instead of the actionable
        // "set spec.create.enabled: true". It must classify as NotFound.
        assert_eq!(
            KopiaErrorClass::classify(
                "ERROR error connecting to repository: repository not initialized in the \
                 provided storage"
            ),
            KopiaErrorClass::NotFound
        );
        // Bare form (no connect prefix) classifies the same way.
        assert_eq!(
            KopiaErrorClass::classify("repository not initialized in the provided storage"),
            KopiaErrorClass::NotFound
        );
        // NotFound is non-retryable: the fix is a spec change, not a blind retry.
        assert!(!KopiaErrorClass::NotFound.is_retryable());
    }

    #[test]
    fn classify_access_denied_and_permission_denied() {
        // The exact RustFS/S3 message we observed live (bucket missing, masked as
        // Access Denied) must classify as AccessDenied, not Unknown.
        assert_eq!(
            KopiaErrorClass::classify(
                "can't connect to storage: error retrieving storage config from bucket \
                 \"kopiur\": Access Denied"
            ),
            KopiaErrorClass::AccessDenied
        );
        assert_eq!(
            KopiaErrorClass::classify("403 Forbidden"),
            KopiaErrorClass::AccessDenied
        );
        // Filesystem repo path not writable by our UID → PermissionDenied, NOT
        // the old SourceError (which marked it retryable).
        assert_eq!(
            KopiaErrorClass::classify("unable to create directory /repo: permission denied"),
            KopiaErrorClass::PermissionDenied
        );
        assert_eq!(
            KopiaErrorClass::classify("open /repo/kopia.repository: operation not permitted"),
            KopiaErrorClass::PermissionDenied
        );
    }

    #[test]
    fn from_label_roundtrips_every_variant() {
        for c in [
            KopiaErrorClass::RepositoryUnavailable,
            KopiaErrorClass::AuthFailure,
            KopiaErrorClass::AccessDenied,
            KopiaErrorClass::PermissionDenied,
            KopiaErrorClass::NotFound,
            KopiaErrorClass::Locked,
            KopiaErrorClass::SourceError,
            KopiaErrorClass::Unknown,
        ] {
            assert_eq!(KopiaErrorClass::from_label(c.as_str()), c);
        }
        assert_eq!(
            KopiaErrorClass::from_label("not-a-real-class"),
            KopiaErrorClass::Unknown
        );
    }

    #[test]
    fn summary_is_stable_and_volatile_free() {
        // Every class yields a non-empty, stable summary with no per-attempt
        // volatile content (the temp-filename suffix kopia emits in stderr must
        // never leak into the condition message — that volatility is what caused
        // the reconcile hot-loop).
        for c in [
            KopiaErrorClass::RepositoryUnavailable,
            KopiaErrorClass::AuthFailure,
            KopiaErrorClass::AccessDenied,
            KopiaErrorClass::PermissionDenied,
            KopiaErrorClass::NotFound,
            KopiaErrorClass::Locked,
            KopiaErrorClass::SourceError,
            KopiaErrorClass::Unknown,
        ] {
            let s = c.summary();
            assert!(!s.is_empty());
            assert!(
                !s.contains(".shards"),
                "summary leaks a volatile temp path: {s}"
            );
            assert!(
                !s.contains(".tmp"),
                "summary leaks a volatile temp path: {s}"
            );
            // Stable across calls (it returns a 'static str, but assert intent).
            assert_eq!(s, c.summary());
        }
        // The PermissionDenied summary is the actionable one for the reported bug.
        assert!(
            KopiaErrorClass::PermissionDenied
                .summary()
                .contains("not writable")
        );
    }

    #[test]
    fn retryable_classification() {
        assert!(KopiaErrorClass::RepositoryUnavailable.is_retryable());
        assert!(KopiaErrorClass::Locked.is_retryable());
        assert!(!KopiaErrorClass::AuthFailure.is_retryable());
        assert!(!KopiaErrorClass::AccessDenied.is_retryable());
        assert!(!KopiaErrorClass::PermissionDenied.is_retryable());
        assert!(!KopiaErrorClass::NotFound.is_retryable());
        assert!(!KopiaErrorClass::Unknown.is_retryable());
    }

    #[test]
    fn tail_keeps_last_lines() {
        let blob: String = (0..50)
            .map(|i| format!("line {i}\n"))
            .collect::<Vec<_>>()
            .join("");
        let tail = tail_lines(&blob);
        let kept: Vec<&str> = tail.lines().collect();
        assert_eq!(kept.len(), STDERR_TAIL_LINES);
        assert_eq!(*kept.last().unwrap(), "line 49");
    }

    #[test]
    fn error_class_propagation() {
        let e = KopiaError::NonZeroExit {
            args: "snapshot create".into(),
            code: Some(1),
            class: KopiaErrorClass::Locked,
            stderr_tail: "repository is locked".into(),
        };
        assert_eq!(e.class(), KopiaErrorClass::Locked);
        assert_eq!(e.stderr_tail(), Some("repository is locked"));
    }
}
