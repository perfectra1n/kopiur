//! Controller error type and the kind-aware `error_policy` (ADR §5.2).
//!
//! Errors are classified [`ErrorClass::Transient`] (kopia subprocess, API
//! server, webhook outage — short retry) or [`ErrorClass::Structural`] (a bug
//! in the CRD/our logic — long retry, clamped at 5 min). The generic
//! [`error_policy_for`] logs, increments `controller_reconcile_errors_total`, and
//! requeues accordingly. Each per-CRD controller wires it via a tiny closure
//! that supplies the `kind` label.

use std::time::Duration;

use kube::runtime::controller::Action;

use crate::context::Context;

/// Backoff for transient errors (kopia / API server / webhook). ADR §5.2.
pub const TRANSIENT_BACKOFF: Duration = Duration::from_secs(30);
/// Maximum backoff for structural errors, clamped per ADR §5.2.
pub const STRUCTURAL_BACKOFF: Duration = Duration::from_secs(300);
/// Heartbeat interval for *terminal* errors — a permanent failure (e.g. filesystem
/// `PermissionDenied`, wrong repository password) that will not succeed on retry
/// without a spec change. We do NOT use a pure [`Action::await_change`] because the
/// kube maintainers warn it can miss updates on a watch desync; instead we requeue
/// on a long, deliberately quiet interval. The reconciler's terminal gate makes the
/// wake a no-op (it re-checks `observedGeneration` and returns without backend IO or
/// an error log) until the spec actually changes.
pub const TERMINAL_HEARTBEAT: Duration = Duration::from_secs(1800);

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

    /// A required referenced object (Repository, SnapshotPolicy, …) was not
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

    /// Self-managed webhook TLS setup failed on the **cluster IO** side: reading/
    /// writing the serving Secret or injecting `caBundle` into a webhook
    /// configuration ([`crate::webhook_tls`]). Transient — the webhook config or
    /// namespace may not exist yet at boot, and the periodic reconcile retries.
    /// Never fatal to the controller (degrade-not-crash): admission simply stays
    /// untrusted until it succeeds.
    ///
    /// Deliberately a `String`: each call site wraps a `kube::Error` with its own
    /// what/why context (which Secret, which configuration), and that composed
    /// message is the useful payload — a `{context, source}` struct would add
    /// ceremony without adding information. Failures from the pure cert-minting
    /// layer are the typed [`Error::WebhookCert`] instead.
    #[error("webhook TLS setup failed: {0}")]
    WebhookSetup(String),

    /// The pure cert-minting layer failed to produce the CA or serving leaf
    /// ([`crate::webhook_tls::CertError`]). Transient like [`Error::WebhookSetup`]
    /// (the periodic reconcile retries; admission stays untrusted, the controller
    /// never crashes), but typed so the `rcgen` source chain stays inspectable.
    #[error("webhook TLS setup failed: could not mint or resolve the CA/serving certificate: {0}")]
    WebhookCert(#[from] crate::webhook_tls::CertError),
}

/// How a reconcile error should be re-driven — the classification that picks the
/// requeue behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Short retry (kopia/API/webhook outage, missing dependency, a *retryable*
    /// kopia class).
    Transient,
    /// Long retry, clamped (a spec/logic bug needing human intervention).
    Structural,
    /// A permanent failure that will not succeed on retry without a spec change
    /// (a *non-retryable* kopia class: `PermissionDenied`, `AuthFailure`,
    /// `AccessDenied`, `NotFound`, `Unknown`). Re-driven only on a long quiet
    /// heartbeat so we stop hammering the backend and flooding the logs.
    Terminal,
}

impl ErrorClass {
    /// The metric label string.
    pub fn label(self) -> &'static str {
        match self {
            ErrorClass::Transient => "transient",
            ErrorClass::Structural => "structural",
            ErrorClass::Terminal => "terminal",
        }
    }

    /// The requeue [`Action`] for this class. kube applies the returned delay
    /// verbatim (no implicit backoff), so this is the single source of cadence.
    pub fn action(self) -> Action {
        match self {
            ErrorClass::Transient => Action::requeue(TRANSIENT_BACKOFF),
            ErrorClass::Structural => Action::requeue(STRUCTURAL_BACKOFF),
            ErrorClass::Terminal => Action::requeue(TERMINAL_HEARTBEAT),
        }
    }
}

impl Error {
    /// Classify this error. Exhaustive `match` — a new variant cannot compile
    /// until it is given a class (the type-safety thesis, ADR §5.5).
    ///
    /// A kopia error is split on its own retry hint
    /// ([`kopiur_kopia::KopiaErrorClass::is_retryable`]): a transient backend blip
    /// (unreachable, locked) is [`Transient`](ErrorClass::Transient) and worth a
    /// 30 s retry, but a permanent failure (permission denied, wrong password) is
    /// [`Terminal`](ErrorClass::Terminal) — retrying it on a tight loop only spams.
    pub fn class(&self) -> ErrorClass {
        match self {
            Error::Kube(_)
            | Error::MissingDependency(_)
            | Error::WebhookSetup(_)
            | Error::WebhookCert(_) => ErrorClass::Transient,
            Error::Kopia(e) => {
                if e.class().is_retryable() {
                    ErrorClass::Transient
                } else {
                    ErrorClass::Terminal
                }
            }
            Error::Validation(_)
            | Error::Serialization(_)
            | Error::InvalidSchedule(_)
            | Error::Invariant(_) => ErrorClass::Structural,
        }
    }
}

/// Result alias for reconcile functions.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Shared `error_policy` body: log, record the metric, surface the failure as
/// a Warning Event on the failing object, and requeue by class.
///
/// Each controller passes its CRD `kind` label so the metric is correctly
/// attributed, and the object so the Event has a `regarding` reference. This
/// keeps one classification/backoff/visibility policy across all reconcilers
/// (ADR §5.2): every kind's failures are visible in `kubectl get events`, not
/// only the ones with bespoke in-reconcile publishes.
///
/// The Event publish is fire-and-forget on the runtime (`error_policy` is
/// sync) and best-effort; repeats of the same failure aggregate into a single
/// Event object via the Recorder's dedup (see [`crate::io::event_ref`] for why
/// the reference must not carry a `resourceVersion`). Outside a tokio runtime
/// (pure unit tests) the publish is skipped — degrade, never panic.
pub fn error_policy_for<K>(kind: &str, obj: &K, err: &Error, ctx: &Context) -> Action
where
    K: kube::Resource<DynamicType = ()>,
{
    let class = err.class();
    ctx.metrics.record_error(kind, class.label());
    tracing::warn!(
        kind = kind,
        class = class.label(),
        error = %err,
        "reconcile error; requeueing"
    );
    let event = crate::io::reconcile_failure_event(err, crate::io::operator_uid());
    let regarding = crate::io::event_ref(obj);
    let recorder = ctx.recorder.clone();
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(crate::io::publish_failure(recorder, regarding, event));
    }
    class.action()
}

#[cfg(test)]
mod tests {
    use super::*;

    use kopiur_kopia::{KopiaError, KopiaErrorClass};

    #[test]
    fn kube_and_missing_are_transient() {
        assert_eq!(
            Error::MissingDependency("repo".into()).class(),
            ErrorClass::Transient
        );
    }

    #[test]
    fn webhook_setup_is_transient() {
        // Webhook-TLS setup retries (the config/namespace may not exist yet at
        // boot); it must never hard-stop the controller.
        assert_eq!(
            Error::WebhookSetup("no such config".into()).class(),
            ErrorClass::Transient
        );
    }

    #[test]
    fn webhook_cert_is_transient_and_keeps_the_source_chain() {
        // The typed cert-minting failure classifies exactly like WebhookSetup
        // (retry, degrade-not-crash) but preserves the rcgen source error.
        let err = Error::WebhookCert(crate::webhook_tls::CertError::Generate(
            rcgen::Error::CouldNotParseCertificate,
        ));
        assert_eq!(err.class(), ErrorClass::Transient);
        assert!(err.to_string().contains("could not mint"));
        assert!(
            std::error::Error::source(&err).is_some(),
            "the CertError source chain must stay inspectable"
        );
    }

    #[test]
    fn kopia_class_follows_retryability() {
        // A retryable kopia class (backend unreachable / locked) is Transient —
        // worth a 30 s retry.
        let retryable = KopiaError::NonZeroExit {
            args: "repository connect".into(),
            code: Some(1),
            class: KopiaErrorClass::RepositoryUnavailable,
            stderr_tail: "dial tcp: connection refused".into(),
        };
        assert_eq!(Error::Kopia(retryable).class(), ErrorClass::Transient);

        // A non-retryable kopia class (filesystem permission denied — the reported
        // bug) is Terminal: hard-stop, do NOT requeue on a tight loop.
        let terminal = KopiaError::NonZeroExit {
            args: "repository connect".into(),
            code: Some(1),
            class: KopiaErrorClass::PermissionDenied,
            stderr_tail: "open /repo/.shards.tmp.deadbeef: permission denied".into(),
        };
        assert_eq!(Error::Kopia(terminal).class(), ErrorClass::Terminal);

        // EmptyOutput maps to Unknown, which is non-retryable → Terminal.
        let unknown = KopiaError::EmptyOutput {
            context: "x".into(),
        };
        assert_eq!(Error::Kopia(unknown).class(), ErrorClass::Terminal);
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
    fn action_cadence_per_class() {
        assert_eq!(
            ErrorClass::Transient.action(),
            Action::requeue(Duration::from_secs(30))
        );
        assert_eq!(
            ErrorClass::Structural.action(),
            Action::requeue(Duration::from_secs(300))
        );
        // The whole point of the fix: a terminal error does NOT requeue at the
        // transient 30 s cadence — it goes quiet on the 30 min heartbeat.
        let terminal = ErrorClass::Terminal.action();
        assert_eq!(terminal, Action::requeue(Duration::from_secs(1800)));
        assert_ne!(terminal, Action::requeue(Duration::from_secs(30)));
    }
}
