//! The typed admission-denial surface (ADR §5.5, [[actionable-error-messages]]).
//!
//! Every way the webhook can deny a request is a variant of [`AdmissionError`];
//! handlers return `Result<AdmissionResponse, AdmissionError>` and
//! [`dispatch`](crate::handlers::dispatch) is the **single** `deny()` choke
//! point — a handler cannot invent an ad-hoc denial string. The `Display` of
//! each variant is the exact message the API server relays to `kubectl apply`,
//! and [`AdmissionError::reason`] gives a stable machine-readable label for
//! logs/metrics.

use kopiur_api::error::ValidationError;

use crate::tenancy::TenancyDenial;

/// Every reason the webhook denies an admission request. Exhaustive — a new
/// denial mode must be added here (and given a [`reason`](Self::reason) label)
/// before any handler can produce it.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    /// The admission request carried no object at all.
    #[error("admission request carried no object to validate")]
    MissingObject,

    /// `object.data["spec"]` did not decode into the typed spec for `kind`.
    #[error("failed to decode {kind} spec: {source}")]
    SpecDecode {
        /// The CRD kind being admitted.
        kind: &'static str,
        /// The serde failure (names the offending field/value).
        #[source]
        source: serde_json::Error,
    },

    /// One or more shared-validator rejections (`kopiur_api::validate`).
    /// Display joins every message with `"; "` so the user sees all problems
    /// in one apply. Invariant: non-empty (handlers only construct it from a
    /// non-empty error vec).
    #[error("{}", join(.0))]
    Invalid(Vec<ValidationError>),

    /// A `ClusterRepository` tenancy rejection (the fail-closed family).
    #[error(transparent)]
    Tenancy(#[from] TenancyDenial),

    /// Building the mutating JSON patch failed (an internal bug, fail closed).
    #[error("internal error building admission patch: {source}")]
    InternalPatch {
        /// The patch serialization failure.
        #[source]
        source: kube::core::admission::SerializePatchError,
    },
}

impl AdmissionError {
    /// Stable machine-readable label per denial mode, for the structured
    /// denial log (and any future per-reason metric). Exhaustive `match`, no
    /// `_ =>` (ADR §5.5).
    pub fn reason(&self) -> &'static str {
        match self {
            AdmissionError::MissingObject => "missing_object",
            AdmissionError::SpecDecode { .. } => "spec_decode",
            AdmissionError::Invalid(_) => "invalid_spec",
            AdmissionError::Tenancy(_) => "tenancy_denied",
            AdmissionError::InternalPatch { .. } => "internal_patch",
        }
    }
}

/// Join a validation-error vec into the single user-facing rejection message
/// (every problem in one apply).
fn join(errs: &[ValidationError]) -> String {
    errs.iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

/// Result alias for admission handlers: an allowed (possibly patched) response,
/// or the typed denial.
pub type AdmissionResult<T = kube::core::admission::AdmissionResponse> =
    std::result::Result<T, AdmissionError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_decode_renders_the_historical_deny_text() {
        // A spec that is structurally not an object always fails to decode.
        let source = serde_json::from_str::<kopiur_api::restore::RestoreSpec>("123").unwrap_err();
        let err = AdmissionError::SpecDecode {
            kind: "Restore",
            source,
        };
        let msg = err.to_string();
        assert!(msg.starts_with("failed to decode Restore spec: "), "{msg}");
    }

    #[test]
    fn invalid_joins_all_errors_with_semicolons() {
        let errs = vec![
            ValidationError::InvalidFieldValue {
                field: "spec.retention.daily".into(),
                reason: "must be >= 1".into(),
            },
            ValidationError::InvalidCron {
                expr: "* *".into(),
                reason: "expected five fields".into(),
            },
        ];
        let joined = AdmissionError::Invalid(errs.clone()).to_string();
        assert!(joined.contains("; "), "{joined}");
        assert!(joined.contains(&errs[0].to_string()));
        assert!(joined.contains(&errs[1].to_string()));

        // A single-element vec renders exactly as the bare ValidationError —
        // the identity-collision / self-mirror denials must not gain a prefix.
        let one = ValidationError::InvalidCron {
            expr: "* *".into(),
            reason: "expected five fields".into(),
        };
        assert_eq!(
            AdmissionError::Invalid(vec![one.clone()]).to_string(),
            one.to_string()
        );
    }

    #[test]
    fn tenancy_is_transparent_over_the_denial_text() {
        let denial = TenancyDenial::NotAllowed {
            consumer_namespace: "evil".into(),
            repo_name: "shared".into(),
        };
        let expected = denial.to_string();
        assert_eq!(AdmissionError::Tenancy(denial).to_string(), expected);
        assert!(expected.contains("allowedNamespaces"));
    }

    #[test]
    fn missing_object_text_is_stable() {
        assert_eq!(
            AdmissionError::MissingObject.to_string(),
            "admission request carried no object to validate"
        );
    }

    #[test]
    fn reason_labels_cover_every_variant() {
        // One row per constructible variant — the exhaustive match in reason()
        // plus this table means a new denial mode can't ship unlabeled.
        // (InternalPatch is absent only because SerializePatchError has no
        // public constructor; its arm is still compile-checked by reason().)
        let denial = TenancyDenial::NoConsumerNamespace;
        let cases: Vec<(AdmissionError, &str)> = vec![
            (AdmissionError::MissingObject, "missing_object"),
            (
                AdmissionError::SpecDecode {
                    kind: "Snapshot",
                    source: serde_json::from_str::<serde_json::Value>("{").unwrap_err(),
                },
                "spec_decode",
            ),
            (
                AdmissionError::Invalid(vec![ValidationError::InvalidFieldValue {
                    field: "f".into(),
                    reason: "r".into(),
                }]),
                "invalid_spec",
            ),
            (AdmissionError::Tenancy(denial), "tenancy_denied"),
        ];
        for (err, expected) in cases {
            assert_eq!(err.reason(), expected, "{err}");
        }
    }
}
