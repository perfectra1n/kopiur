use k8s_openapi::api::core::v1::ObjectReference;
use kube::Resource;
use kube::runtime::events::{Event, EventType, Recorder};

use kopiur_kopia::KopiaErrorClass;

use crate::consts::{
    BOOTSTRAP_JOB_FAILED_REASON, CHECK_API_SERVER_ACTION, CHECK_BACKEND_ACTION,
    CHECK_CREDENTIALS_ACTION, CHECK_PERMISSIONS_ACTION, CHECK_REFERENCES_ACTION,
    CHECK_WEBHOOK_CONFIGURATION_ACTION, FIX_SCHEDULE_ACTION, FIX_SPEC_ACTION,
    INVALID_SCHEDULE_REASON, INVALID_SPEC_REASON, INVARIANT_VIOLATED_REASON, KUBE_API_ERROR_REASON,
    MISSING_CREDENTIALS_REASON, MISSING_DEPENDENCY_REASON, REPORT_ISSUE_ACTION,
    SERIALIZATION_FAILED_REASON, WEBHOOK_SETUP_FAILED_REASON,
};
use crate::context::Context;
use crate::error::Error;

/// `obj.object_ref(&())` with `resource_version` stripped — the reference every
/// Event publish must use as `regarding`.
///
/// The kube-runtime Recorder aggregates repeats of the same Event (within its
/// dedup TTL) into one object with a climbing `series.count`, keyed in part on
/// the `regarding` reference. The cache key's `Hash` ignores
/// `resource_version`, but its derived `PartialEq` compares the full
/// `ObjectReference` — so a reference carrying the object's current (churning)
/// `resourceVersion` mints a brand-new Event object on every repeat instead of
/// aggregating the series. Stripping it here keeps repeated identical failures
/// at exactly one Event object.
pub(crate) fn event_ref<K>(obj: &K) -> ObjectReference
where
    K: Resource<DynamicType = ()>,
{
    ObjectReference {
        resource_version: None,
        ..obj.object_ref(&())
    }
}

/// Emit a `Warning` Event on `obj` so an actionable message is visible via
/// `kubectl describe`/`get events`, not only in the controller log. Best-effort: a
/// publish failure is logged, never fatal.
pub async fn publish_warning_event<K>(
    ctx: &Context,
    obj: &K,
    reason: &str,
    action: &str,
    message: &str,
) where
    K: Resource<DynamicType = ()>,
{
    let regarding = event_ref(obj);
    if let Err(e) = ctx
        .recorder
        .publish(
            &Event {
                type_: EventType::Warning,
                reason: reason.into(),
                note: Some(message.to_string()),
                action: action.into(),
                secondary: None,
            },
            &regarding,
        )
        .await
    {
        tracing::warn!(error = %e, reason, "failed to publish Warning event");
    }
}

/// Emit a `Warning` Event for a missing credentials Secret (see
/// [`publish_warning_event`]).
pub async fn publish_missing_creds_event<K>(ctx: &Context, obj: &K, message: &str)
where
    K: Resource<DynamicType = ()>,
{
    publish_warning_event(
        ctx,
        obj,
        MISSING_CREDENTIALS_REASON,
        CHECK_CREDENTIALS_ACTION,
        message,
    )
    .await;
}

/// Kubernetes caps an Event's `note` at 1024 bytes (the apiserver validates with
/// Go's `len`, i.e. bytes). A longer note is rejected with a 422, so the
/// *actionable* warning never reaches `kubectl describe`. We clamp every
/// composed note to this.
pub(crate) const EVENT_NOTE_MAX_BYTES: usize = 1024;

/// Budget for the kopia error message embedded *inside* a note. Capping the
/// message before composing keeps the surrounding remediation text — the part a
/// user actually acts on — from being eaten by a huge kopia stderr tail when the
/// whole note is finally clamped to [`EVENT_NOTE_MAX_BYTES`].
const EVENT_MESSAGE_BUDGET_BYTES: usize = 512;

/// Appended to a string that was truncated, signalling the cut to readers.
pub(crate) const TRUNCATION_MARKER: &str = "…";

/// Truncate `s` to at most `max` bytes on a UTF-8 char boundary, appending
/// [`TRUNCATION_MARKER`] when anything was dropped. The result is always
/// `<= max` bytes (assuming `max >= TRUNCATION_MARKER.len()`).
pub(crate) fn truncate_for_note(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max.saturating_sub(TRUNCATION_MARKER.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{TRUNCATION_MARKER}", &s[..end])
}

/// Map a kopia failure class to the `(action, note)` of the Warning Event we
/// surface on the repository CR. The Event `reason` is the class label itself
/// (`class.as_str()`, set by the caller) so it matches the `Bootstrapped=False`
/// condition reason and is machine-readable; only the remediation hint and the
/// human note vary here. Exhaustive on `KopiaErrorClass` so a new class forces an
/// explicit decision (ADR §5.5).
///
/// The kopia `message` (which carries the stderr tail, up to ~4 KB) is truncated
/// to [`EVENT_MESSAGE_BUDGET_BYTES`] before composing and the whole note is
/// clamped to [`EVENT_NOTE_MAX_BYTES`], so the Event always publishes and the
/// remediation hint always survives.
///
/// `uid` is the operator's effective UID (from [`operator_uid`]) — reported
/// verbatim in the `PermissionDenied` hint so the `chown` advice names the real
/// UID the operator runs as, not a hardcoded guess (it varies with the chart's
/// `podSecurityContext.runAsUser`).
pub(crate) fn backend_failure_event(
    class: KopiaErrorClass,
    message: &str,
    uid: u32,
) -> (&'static str, String) {
    let message = truncate_for_note(message, EVENT_MESSAGE_BUDGET_BYTES);
    let (action, note) = match class {
        KopiaErrorClass::AccessDenied => (
            CHECK_CREDENTIALS_ACTION,
            format!(
                "the storage backend denied access: {message}. The credentials Secret may lack \
                 permission, or the configured bucket/container/path does not exist (some backends \
                 report a missing bucket as \"Access Denied\"). Verify the credentials Secret and \
                 that the bucket/path exists and is reachable."
            ),
        ),
        KopiaErrorClass::PermissionDenied => (
            CHECK_PERMISSIONS_ACTION,
            format!(
                "the repository path is not writable by the operator: {message}. The filesystem \
                 export or PVC must be writable by the operator's UID ({uid}) — fix its \
                 ownership/mode (e.g. `chown -R {uid} <path>`) and reconcile again."
            ),
        ),
        KopiaErrorClass::AuthFailure => (
            CHECK_CREDENTIALS_ACTION,
            format!(
                "the repository password was rejected: {message}. Check the encryption password \
                 Secret (the `KOPIA_PASSWORD` key) referenced by this repository."
            ),
        ),
        KopiaErrorClass::RepositoryUnavailable
        | KopiaErrorClass::NotFound
        | KopiaErrorClass::Locked
        | KopiaErrorClass::SourceError
        | KopiaErrorClass::Unknown => (
            CHECK_BACKEND_ACTION,
            format!("repository backend error ({}): {message}", class.as_str()),
        ),
    };
    (action, truncate_for_note(&note, EVENT_NOTE_MAX_BYTES))
}

/// The operator's effective UID — the identity that writes a filesystem repo in
/// the controller's in-process kopia ops, and (by default) the mover pods' UID.
/// Surfaced in the `PermissionDenied` remediation hint so it names the real UID
/// rather than a hardcoded constant.
pub(crate) fn operator_uid() -> u32 {
    // SAFETY: geteuid() is always-succeeds and thread-safe; it has no
    // preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

/// A pre-composed Warning Event for a failed reconcile: the `(reason, action,
/// note)` triple [`error_policy_for`](crate::error::error_policy_for) publishes
/// on the failing object. The note is already clamped to
/// [`EVENT_NOTE_MAX_BYTES`].
pub(crate) struct FailureEvent {
    /// Machine-readable cause (a kopia class label or a `*_REASON` const).
    pub reason: &'static str,
    /// The remediation hint (a `*_ACTION` const).
    pub action: &'static str,
    /// The human note: what failed, why, and how to fix it.
    pub note: String,
}

/// Map a reconcile [`Error`] to the Warning Event surfaced on the failing
/// object. Exhaustive over `Error` — **no `_ =>` arm** — so a new error variant
/// cannot compile until it is given an Event decision (ADR §5.5), exactly like
/// `Error::class()` forces a requeue decision.
///
/// A kopia failure reuses the per-class remediation machinery
/// ([`backend_failure_event`]) so its Event is identical in shape to the
/// bootstrap-failure ones (reason = the kopia class). Every other variant pairs
/// the error's own message with a short, stable remediation hint. `uid` is the
/// operator's effective UID (see [`operator_uid`]), forwarded to the
/// `PermissionDenied` hint.
pub(crate) fn reconcile_failure_event(err: &Error, uid: u32) -> FailureEvent {
    let (reason, action, note): (&'static str, &'static str, String) = match err {
        Error::Kopia(e) => {
            let class = e.class();
            let (action, note) = backend_failure_event(class, &e.to_string(), uid);
            (class.as_str(), action, note)
        }
        Error::Kube(_) => (
            KUBE_API_ERROR_REASON,
            CHECK_API_SERVER_ACTION,
            format!(
                "{err}. This is usually a transient API-server problem and the reconcile retries \
                 automatically; if it persists, check the API server's health and the operator's \
                 RBAC."
            ),
        ),
        Error::Validation(_) => (
            INVALID_SPEC_REASON,
            FIX_SPEC_ACTION,
            format!(
                "{err}. The object will not reconcile until the spec is corrected — fix the \
                 field(s) named above and re-apply."
            ),
        ),
        Error::MissingDependency(_) => (
            MISSING_DEPENDENCY_REASON,
            CHECK_REFERENCES_ACTION,
            format!(
                "{err} — create it, or fix the reference in this object's spec; the reconcile \
                 retries automatically."
            ),
        ),
        Error::Serialization(_) => (
            SERIALIZATION_FAILED_REASON,
            REPORT_ISSUE_ACTION,
            format!(
                "{err}. This is likely a bug in kopiur — please report it together with this \
                 object's YAML."
            ),
        ),
        Error::InvalidSchedule(_) => (
            INVALID_SCHEDULE_REASON,
            FIX_SCHEDULE_ACTION,
            format!(
                "{err}. Fix the cron expression in this object's spec (five fields, e.g. \
                 `0 3 * * *`)."
            ),
        ),
        Error::Invariant(_) => (
            INVARIANT_VIOLATED_REASON,
            REPORT_ISSUE_ACTION,
            format!(
                "{err}. This is likely a bug in kopiur — please report it together with this \
                 object's YAML."
            ),
        ),
        Error::WebhookSetup(_) | Error::WebhookCert(_) => (
            WEBHOOK_SETUP_FAILED_REASON,
            CHECK_WEBHOOK_CONFIGURATION_ACTION,
            format!(
                "{err}. Admission stays untrusted until TLS setup succeeds (it is retried); check \
                 the webhook configuration/namespace and the operator's RBAC on Secrets and \
                 webhook configurations."
            ),
        ),
    };
    FailureEvent {
        reason,
        action,
        note: truncate_for_note(&note, EVENT_NOTE_MAX_BYTES),
    }
}

/// Publish a [`FailureEvent`] as a Warning on `regarding`. Owned arguments so
/// the sync `error_policy` can fire-and-forget it on the runtime (`tokio::spawn`).
/// Best-effort: a failed publish is logged, never fatal — a dropped Event must
/// not change requeue behavior.
pub(crate) async fn publish_failure(
    recorder: Recorder,
    regarding: ObjectReference,
    event: FailureEvent,
) {
    if let Err(e) = recorder
        .publish(
            &Event {
                type_: EventType::Warning,
                reason: event.reason.to_string(),
                note: Some(event.note),
                action: event.action.to_string(),
                secondary: None,
            },
            &regarding,
        )
        .await
    {
        tracing::warn!(
            error = %e,
            reason = event.reason,
            "failed to publish reconcile-failure Warning event"
        );
    }
}

/// Publish a Warning Event on `regarding` with a pre-composed `(reason, action,
/// note)`. The single low-level publish primitive every backend/bootstrap warning
/// funnels through. Best-effort: a failed publish is logged, never fatal (a
/// dropped Event must not fail a reconcile).
async fn publish_warning(
    ctx: &Context,
    regarding: &ObjectReference,
    name: &str,
    reason: &str,
    action: &str,
    note: String,
) {
    if let Err(e) = ctx
        .recorder
        .publish(
            &Event {
                type_: EventType::Warning,
                reason: reason.to_string(),
                note: Some(note),
                action: action.to_string(),
                secondary: None,
            },
            regarding,
        )
        .await
    {
        tracing::warn!(error = %e, repo = %name, reason, "failed to publish Warning event");
    }
}

/// Surface a repository connect/create failure as a Warning Event on the CR, so
/// *what* the backend rejected (e.g. S3 "Access Denied") is visible from
/// `kubectl get events` / `describe` and not buried in a status condition. The
/// Event `reason` is the kopia class (matching the `Bootstrapped=False`
/// condition). Best-effort: a failed publish is logged, never fatal.
pub async fn publish_backend_failure(
    ctx: &Context,
    regarding: &ObjectReference,
    name: &str,
    class: KopiaErrorClass,
    message: &str,
) {
    let (action, note) = backend_failure_event(class, message, operator_uid());
    publish_warning(ctx, regarding, name, class.as_str(), action, note).await;
}

/// Why an object-store repository bootstrap Job did not yield a healthy
/// repository. Each variant maps **exhaustively** (ADR §5.5) to a
/// `Bootstrapped=False` condition reason/message and a Warning Event, so a new
/// terminal failure mode forces an explicit decision instead of silently
/// degrading. Shared by the `Repository` and `ClusterRepository` finalizers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapFailure {
    /// The mover ran and wrote a structured failure result: kopia could not
    /// connect to / create the repository. Carries the kopia error class and the
    /// human message (its stderr tail), exactly as the mover classified them.
    Backend {
        /// The kopia error class the mover recorded (drives the Event `reason`).
        class: KopiaErrorClass,
        /// The mover's persisted failure message (stable across re-reads).
        message: String,
    },
    /// The bootstrap Job reached a terminal/failed state but wrote **no** result:
    /// the mover pod crashed, was OOM-killed/evicted, exceeded its
    /// `activeDeadlineSeconds`, or never scheduled (e.g. a missing mover
    /// ServiceAccount). The cause lives in the Job's pod logs.
    JobFailedWithoutResult {
        /// The bootstrap Job's name, so the message can point an operator at it.
        job_name: String,
    },
}

impl BootstrapFailure {
    /// The machine-readable `reason` shared by the `Bootstrapped=False` condition
    /// and the Warning Event — they must match (ADR §5.5). A backend rejection
    /// reuses the kopia class label; a result-less Job failure is its own reason
    /// so the two are never conflated.
    pub fn reason(&self) -> &'static str {
        match self {
            BootstrapFailure::Backend { class, .. } => class.as_str(),
            BootstrapFailure::JobFailedWithoutResult { .. } => BOOTSTRAP_JOB_FAILED_REASON,
        }
    }

    /// The stable, actionable condition message (what failed / why / how to find
    /// the cause). Volatile-free so the guarded status write stays a no-op across
    /// repeated identical failures (no hot-loop — see [`crate::io::patch_status_if_changed`]).
    pub fn condition_message(&self) -> String {
        match self {
            BootstrapFailure::Backend { message, .. } => message.clone(),
            BootstrapFailure::JobFailedWithoutResult { job_name } => {
                bootstrap_job_failed_message(job_name)
            }
        }
    }

    /// Publish this failure as a Warning Event on `regarding`. The `Backend`
    /// variant reuses the kopia-class remediation machinery
    /// (`backend_failure_event`); the result-less variant carries its own
    /// actionable note. Both are clamped to `EVENT_NOTE_MAX_BYTES`.
    pub async fn publish(&self, ctx: &Context, regarding: &ObjectReference, name: &str) {
        match self {
            BootstrapFailure::Backend { class, message } => {
                publish_backend_failure(ctx, regarding, name, *class, message).await;
            }
            BootstrapFailure::JobFailedWithoutResult { job_name } => {
                let note = truncate_for_note(
                    &bootstrap_job_failed_message(job_name),
                    EVENT_NOTE_MAX_BYTES,
                );
                publish_warning(
                    ctx,
                    regarding,
                    name,
                    BOOTSTRAP_JOB_FAILED_REASON,
                    CHECK_BACKEND_ACTION,
                    note,
                )
                .await;
            }
        }
    }
}

/// The terminal outcome of a repository bootstrap Job, derived purely from the
/// `(result, job state)` pair. Exhaustive (ADR §5.5): the success arm **owns**
/// the [`BootstrapResult`], so the reconciler binds it by `match` instead of
/// asserting an invariant with `.expect()` — an unreadable-success state is
/// unrepresentable. Shared by the `Repository` and `ClusterRepository`
/// finalizers.
pub enum BootstrapOutcome {
    /// The Job completed but the result ConfigMap is not readable yet (a
    /// write/propagation race) — requeue briefly rather than guessing. A truly
    /// result-less Job stays terminal-`Failed` on the next pass.
    ResultPending,
    /// A terminal, typed failure (kopia rejection or a result-less failed Job).
    Failed(BootstrapFailure),
    /// The bootstrap succeeded; carries the mover's result.
    Succeeded(Box<kopiur_mover::bootstrap::BootstrapResult>),
}

/// Classify a bootstrap Job's `(result, job_succeeded)` into a
/// [`BootstrapOutcome`]. Pure, so the four-way mapping is unit-tested.
pub fn bootstrap_outcome(
    result: Option<kopiur_mover::bootstrap::BootstrapResult>,
    job_succeeded: bool,
    job_name: &str,
) -> BootstrapOutcome {
    match result {
        None if job_succeeded => BootstrapOutcome::ResultPending,
        None => BootstrapOutcome::Failed(BootstrapFailure::JobFailedWithoutResult {
            job_name: job_name.to_string(),
        }),
        Some(r) if !r.success => BootstrapOutcome::Failed(BootstrapFailure::Backend {
            class: r
                .failure
                .as_ref()
                .map(|f| KopiaErrorClass::from_label(&f.kopia_error_class))
                .unwrap_or(KopiaErrorClass::Unknown),
            message: r
                .failure
                .as_ref()
                .map(|f| f.message.clone())
                .unwrap_or_else(|| "repository bootstrap failed".to_string()),
        }),
        Some(r) => BootstrapOutcome::Succeeded(Box::new(r)),
    }
}

/// The actionable message for a bootstrap Job that failed without writing a
/// result. Pure so the exact text is unit-asserted. Names the Job, explains the
/// likely causes, and gives the concrete commands to find the real error.
pub fn bootstrap_job_failed_message(job_name: &str) -> String {
    format!(
        "the repository bootstrap Job `{job_name}` failed without writing a result — the mover \
         pod crashed, was evicted, exceeded its deadline, or never scheduled (e.g. a missing mover \
         ServiceAccount in this namespace). Inspect it with `kubectl describe job/{job_name}` and \
         `kubectl logs job/{job_name}` to find the underlying error."
    )
}
