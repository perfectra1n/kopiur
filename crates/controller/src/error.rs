//! Controller error type and the kind-aware `error_policy` (ADR §5.2).
//!
//! Errors are classified [`ErrorClass::Transient`] (kopia subprocess, API
//! server, webhook outage — short retry) or [`ErrorClass::Structural`] (a bug
//! in the CRD/our logic — long retry, clamped at 5 min). The generic
//! [`error_policy`] logs, increments `controller_reconcile_errors_total`, and
//! requeues accordingly. Each per-CRD controller wires it via a tiny closure
//! that supplies the `kind` label.

use std::time::Duration;

use kube::runtime::controller::Action;

use crate::context::Context;

/// Backoff for transient errors (kopia / API server / webhook). ADR §5.2.
pub const TRANSIENT_BACKOFF: Duration = Duration::from_secs(30);
/// Maximum backoff for structural errors, clamped per ADR §5.2.
pub const STRUCTURAL_BACKOFF: Duration = Duration::from_secs(300);

/// All errors a reconciler can surface. The exhaustive `class()` mapping is
/// what drives requeue timing — a new variant must be classified to compile.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A `kube::Client` API call failed (GET/PATCH/CREATE/DELETE). Transient.
    #[error("kube api error: {0}")]
    Kube(#[from] kube::Error),

    /// A kopia subprocess invocation failed. Transient (repo may be offline).
    #[error("kopia error: {0}")]
    Kopia(#[from] kopiur_kopia::KopiaError),

    /// Defensive re-validation (via `api::validate`) found a spec problem.
    /// Structural: the object will not reconcile until the user fixes it.
    #[error("validation error: {0}")]
    Validation(String),

    /// A required referenced object (Repository, BackupConfig, …) was not
    /// found. Transient: it may appear shortly (GitOps apply ordering).
    #[error("missing dependency: {0}")]
    MissingDependency(String),

    /// JSON (de)serialization of a spec/status/work-spec failed. Structural.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// A cron expression failed to parse at scheduling time. Structural.
    #[error("invalid schedule: {0}")]
    InvalidSchedule(String),

    /// An object lacked a field the reconciler requires (e.g. `.metadata.name`).
    /// Structural.
    #[error("invariant violated: {0}")]
    Invariant(String),
}

/// Transient vs structural — the classification that picks the requeue delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Short retry (kopia/API/webhook outage, missing dependency).
    Transient,
    /// Long retry, clamped (a spec/logic bug needing human intervention).
    Structural,
}

impl ErrorClass {
    /// The metric label string.
    pub fn label(self) -> &'static str {
        match self {
            ErrorClass::Transient => "transient",
            ErrorClass::Structural => "structural",
        }
    }

    /// The requeue backoff for this class (already clamped at 5 min). ADR §5.2.
    pub fn backoff(self) -> Duration {
        match self {
            ErrorClass::Transient => TRANSIENT_BACKOFF,
            ErrorClass::Structural => STRUCTURAL_BACKOFF,
        }
    }
}

impl Error {
    /// Classify this error. Exhaustive `match` — a new variant cannot compile
    /// until it is given a class (the type-safety thesis, ADR §5.5).
    pub fn class(&self) -> ErrorClass {
        match self {
            Error::Kube(_) | Error::Kopia(_) | Error::MissingDependency(_) => ErrorClass::Transient,
            Error::Validation(_)
            | Error::Serialization(_)
            | Error::InvalidSchedule(_)
            | Error::Invariant(_) => ErrorClass::Structural,
        }
    }
}

/// Result alias for reconcile functions.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Shared `error_policy` body: log, record the metric, and requeue by class.
///
/// Each controller passes its CRD `kind` label so the metric is correctly
/// attributed. This keeps one classification/backoff policy across all seven
/// reconcilers (ADR §5.2).
pub fn error_policy_for(kind: &str, err: &Error, ctx: &Context) -> Action {
    let class = err.class();
    ctx.metrics.record_error(kind, class.label());
    tracing::warn!(
        kind = kind,
        class = class.label(),
        error = %err,
        "reconcile error; requeueing"
    );
    Action::requeue(class.backoff())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kube_and_kopia_and_missing_are_transient() {
        assert_eq!(
            Error::MissingDependency("repo".into()).class(),
            ErrorClass::Transient
        );
        // KopiaError construction without a process: use the EmptyOutput variant.
        let kerr = kopiur_kopia::KopiaError::EmptyOutput {
            context: "x".into(),
        };
        assert_eq!(Error::Kopia(kerr).class(), ErrorClass::Transient);
    }

    #[test]
    fn validation_and_schedule_are_structural() {
        assert_eq!(
            Error::Validation("bad".into()).class(),
            ErrorClass::Structural
        );
        assert_eq!(
            Error::InvalidSchedule("nope".into()).class(),
            ErrorClass::Structural
        );
        assert_eq!(
            Error::Invariant("no name".into()).class(),
            ErrorClass::Structural
        );
    }

    #[test]
    fn backoff_is_30s_transient_and_5m_structural_clamp() {
        assert_eq!(ErrorClass::Transient.backoff(), Duration::from_secs(30));
        assert_eq!(ErrorClass::Structural.backoff(), Duration::from_secs(300));
        // Structural backoff is the documented 5-minute clamp.
        assert!(ErrorClass::Structural.backoff() <= Duration::from_secs(300));
    }
}
