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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KopiaErrorClass {
    /// Repository could not be reached / opened (network, backend down,
    /// bad endpoint). Typically transient → retry.
    RepositoryUnavailable,
    /// Authentication / password / credential failure. Not retryable without
    /// a config change.
    AuthFailure,
    /// The requested source path / snapshot / target was not found.
    NotFound,
    /// A repository lock is held by another writer. Often transient → retry.
    Locked,
    /// Source filesystem error during upload (permission, I/O).
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
            KopiaErrorClass::NotFound => "NotFound",
            KopiaErrorClass::Locked => "Locked",
            KopiaErrorClass::SourceError => "SourceError",
            KopiaErrorClass::Unknown => "Unknown",
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
        } else if s.contains("repository is locked")
            || s.contains("another process")
            || s.contains("lock")
        {
            KopiaErrorClass::Locked
        } else if s.contains("no such file or directory")
            || s.contains("not found")
            || s.contains("does not exist")
            || s.contains("unable to find snapshot")
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
        } else if s.contains("upload error")
            || s.contains("failed to prepare source")
            || s.contains("permission denied")
        {
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
    fn retryable_classification() {
        assert!(KopiaErrorClass::RepositoryUnavailable.is_retryable());
        assert!(KopiaErrorClass::Locked.is_retryable());
        assert!(!KopiaErrorClass::AuthFailure.is_retryable());
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
