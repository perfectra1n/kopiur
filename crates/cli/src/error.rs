//! The CLI's typed error surface. One exhaustive enum; every message states
//! what failed, why, and how to fix it (the message text is unit-tested).

/// Everything `kubectl kopiur` can fail with. Exhaustive — a new failure mode
/// is a new variant, never a stringly-typed catch-all.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// The kubeconfig could not be loaded or the requested context resolved.
    #[error(
        "could not load a Kubernetes client configuration: {source}. \
         kubectl-kopiur reads the same configuration kubectl does \
         ($KUBECONFIG, ~/.kube/config, or in-cluster). \
         Fix: check --kubeconfig/--context, or verify your setup with \
         `kubectl config current-context`"
    )]
    KubeConfig {
        /// The underlying kube config/client construction error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The API server refused the request with 403.
    #[error(
        "forbidden: cannot {verb} {resource}{scope}: {source}. \
         Your kubeconfig user lacks RBAC for this. \
         Fix: ask a cluster admin to grant `{verb}` on `{resource}` \
         (kopiur.home-operations.com) to your user, or run with a more \
         privileged kubeconfig/--context"
    )]
    Forbidden {
        /// The verb that was refused (`get`, `list`, `patch`, …).
        verb: &'static str,
        /// The (plural) resource the verb targeted.
        resource: &'static str,
        /// Human-readable scope suffix (`" in namespace x"` or `""`).
        scope: String,
        /// The API server's error.
        #[source]
        source: Box<kube::Error>,
    },

    /// The resource *type* is unknown to the API server — the kopiur CRDs are
    /// not installed (or not this version).
    #[error(
        "the API server does not know the {kind} resource type: {source}. \
         The kopiur CRDs are missing or outdated on this cluster. \
         Fix: install kopiur (`helm install kopiur oci://ghcr.io/home-operations/charts/kopiur`) \
         or apply the CRDs from deploy/crds/, then retry"
    )]
    KindNotInstalled {
        /// The kopiur kind that is missing.
        kind: &'static str,
        /// The API server's error.
        #[source]
        source: Box<kube::Error>,
    },

    /// A named object was not found.
    #[error(
        "{kind} {name:?} not found{scope}. \
         Fix: list what exists with `kubectl get {plural}{scope_flag}` and check \
         the name (and --namespace/--context)"
    )]
    NotFound {
        /// The kopiur kind looked up.
        kind: &'static str,
        /// Its plural, for the remediation command.
        plural: &'static str,
        /// The missing object's name.
        name: String,
        /// Human-readable scope suffix (`" in namespace x"` or `""`).
        scope: String,
        /// The matching `kubectl` scope flag (`" -n x"`, `" -A"`, or `""`).
        scope_flag: String,
    },

    /// Any other Kubernetes API failure.
    #[error(
        "Kubernetes API request failed: cannot {verb} {resource}{scope}: {source}. \
         Fix: check cluster/API-server health (`kubectl version`) and connectivity, \
         then retry"
    )]
    Api {
        /// The verb attempted.
        verb: &'static str,
        /// The (plural) resource targeted.
        resource: &'static str,
        /// Human-readable scope suffix.
        scope: String,
        /// The underlying kube client error.
        #[source]
        source: Box<kube::Error>,
    },

    /// An admission webhook (kopiur's, or a cluster policy engine) rejected
    /// the object.
    #[error(
        "an admission webhook rejected this object: {message}. \
         Fix: correct the flags/spec per the message above and retry \
         (the message names the webhook that denied it)"
    )]
    AdmissionDenied {
        /// The webhook's denial message (already actionable by project norm).
        message: String,
    },

    /// A `--wait` deadline expired before the object reached a terminal state.
    #[error(
        "timed out after {after} waiting for {what}. \
         The operation is still running in the cluster — waiting stopped, the work did not. \
         Fix: {hint}"
    )]
    WaitTimeout {
        /// What was being waited on.
        what: String,
        /// The timeout that expired, humanized.
        after: String,
        /// How to keep observing or adjust the deadline.
        hint: String,
    },

    /// A log stream broke mid-flight (network blip, apiserver restart).
    #[error(
        "the log stream was interrupted: {source}. \
         The connection to the API server dropped mid-stream; the run itself is unaffected. \
         Fix: re-run the same `kubectl kopiur logs` command to resume following"
    )]
    LogStreamInterrupted {
        /// The underlying stream error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The object being waited on was deleted mid-wait.
    #[error(
        "{what} was deleted while waiting for it to finish. \
         Something (a user, GitOps prune, or retention) removed the object. \
         Fix: check `kubectl get events` for who deleted it, then re-run"
    )]
    GoneWhileWaiting {
        /// What was being waited on.
        what: String,
    },

    /// A by-reference lookup matched more than one object.
    #[error(
        "{what}: {candidates}. \
         Fix: name the one you mean explicitly (pass it as the positional NAME argument)"
    )]
    AmbiguousTarget {
        /// What was looked up and how many matched.
        what: String,
        /// The matching object names.
        candidates: String,
    },

    /// `-A` was passed to a command that targets exactly one object.
    #[error(
        "{command} targets a single object in one namespace, so -A/--all-namespaces \
         does not apply. Fix: drop -A and pass -n <namespace> instead"
    )]
    AllNamespacesNotApplicable {
        /// The command that rejected `-A`.
        command: &'static str,
    },

    /// (De)serializing an object for output failed — a kopiur bug, not a user error.
    #[error(
        "failed to serialize {what} for output: {source}. \
         This is a kubectl-kopiur bug — please report it at \
         https://github.com/home-operations/kopiur/issues"
    )]
    Serialization {
        /// What was being serialized.
        what: &'static str,
        /// The serde error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Human-readable scope suffix for error messages: `" in namespace x"` for a
/// namespaced call, `""` for a cluster-scoped one.
pub fn scope_suffix(namespace: Option<&str>) -> String {
    match namespace {
        Some(ns) => format!(" in namespace {ns}"),
        None => String::new(),
    }
}

/// The API server's NotFoundHandler message when the URL's resource *type* is
/// unknown (the CRD is absent). An object-level 404 instead names the object
/// (`snapshots.kopiur… "x" not found`). kube's `Status` carries no structured
/// discriminator between the two, so this message match is the only signal —
/// single definition here, exercised by the tests below.
const KIND_NOT_FOUND_NEEDLE: &str = "could not find the requested resource";

/// Classify a `kube::Error` from a `{verb} {resource}` call into the matching
/// [`CliError`] variant, so every command surfaces the same actionable
/// messages without forking the mapping logic. Pass `name` for object-level
/// calls (get/patch/delete) so their 404 maps to [`CliError::NotFound`]; a 404
/// whose message says the *resource type* is unknown maps to
/// [`CliError::KindNotInstalled`] either way.
pub fn classify_kube(
    verb: &'static str,
    kind: &'static str,
    resource: &'static str,
    namespace: Option<&str>,
    name: Option<&str>,
    source: kube::Error,
) -> CliError {
    let scope = scope_suffix(namespace);
    match &source {
        // An admission-webhook denial (apiserver relays it as 400/403 with the
        // webhook's message). Checked before the RBAC arm — a denial can be 403.
        kube::Error::Api(ae) if ae.message.contains("denied the request") => {
            CliError::AdmissionDenied {
                message: ae.message.clone(),
            }
        }
        kube::Error::Api(ae) if ae.code == 403 => CliError::Forbidden {
            verb,
            resource,
            scope,
            source: Box::new(source),
        },
        kube::Error::Api(ae) if ae.code == 404 && ae.message.contains(KIND_NOT_FOUND_NEEDLE) => {
            CliError::KindNotInstalled {
                kind,
                source: Box::new(source),
            }
        }
        kube::Error::Api(ae) if ae.code == 404 => match name {
            Some(n) => CliError::NotFound {
                kind,
                plural: resource,
                name: n.to_string(),
                scope,
                scope_flag: namespace.map(|ns| format!(" -n {ns}")).unwrap_or_default(),
            },
            // A collection-level 404 that doesn't carry the unknown-type
            // message: don't guess, surface it as a plain API failure.
            None => CliError::Api {
                verb,
                resource,
                scope,
                source: Box::new(source),
            },
        },
        _ => CliError::Api {
            verb,
            resource,
            scope,
            source: Box::new(source),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api_err(code: u16) -> kube::Error {
        kube::Error::Api(
            kube::core::Status::failure("denied", "Forbidden")
                .with_code(code)
                .boxed(),
        )
    }

    #[test]
    fn forbidden_message_names_verb_resource_and_fix() {
        let err = classify_kube(
            "patch",
            "SnapshotPolicy",
            "snapshotpolicies",
            Some("media"),
            Some("nightly"),
            api_err(403),
        );
        let msg = err.to_string();
        assert!(
            msg.contains("cannot patch snapshotpolicies in namespace media"),
            "{msg}"
        );
        assert!(msg.contains("grant `patch` on `snapshotpolicies`"), "{msg}");
        assert!(msg.contains("RBAC"), "{msg}");
    }

    #[test]
    fn collection_404_maps_to_missing_crds_with_install_fix() {
        let err = classify_kube(
            "list",
            "Snapshot",
            "snapshots",
            None,
            None,
            kube::Error::Api(
                kube::core::Status::failure(
                    "the server could not find the requested resource",
                    "NotFound",
                )
                .with_code(404)
                .boxed(),
            ),
        );
        let msg = err.to_string();
        assert!(
            msg.contains("does not know the Snapshot resource type"),
            "{msg}"
        );
        assert!(msg.contains("install kopiur"), "{msg}");
        assert!(msg.contains("deploy/crds"), "{msg}");
    }

    #[test]
    fn not_found_message_offers_a_listing_command() {
        let err = CliError::NotFound {
            kind: "SnapshotSchedule",
            plural: "snapshotschedules",
            name: "nightly".into(),
            scope: scope_suffix(Some("media")),
            scope_flag: " -n media".into(),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("SnapshotSchedule \"nightly\" not found in namespace media"),
            "{msg}"
        );
        assert!(
            msg.contains("kubectl get snapshotschedules -n media"),
            "{msg}"
        );
    }

    #[test]
    fn object_level_404_maps_to_not_found_even_from_patch() {
        // The object vanished between GET and PATCH: the message names the
        // object, so this must be NotFound — never "CRDs not installed".
        let err = classify_kube(
            "patch",
            "SnapshotPolicy",
            "snapshotpolicies",
            Some("media"),
            Some("nightly"),
            kube::Error::Api(
                kube::core::Status::failure(
                    "snapshotpolicies.kopiur.home-operations.com \"nightly\" not found",
                    "NotFound",
                )
                .with_code(404)
                .boxed(),
            ),
        );
        let msg = err.to_string();
        assert!(
            msg.contains("SnapshotPolicy \"nightly\" not found in namespace media"),
            "{msg}"
        );
        assert!(!msg.contains("does not know"), "{msg}");
    }

    #[test]
    fn unknown_type_404_wins_even_for_object_calls() {
        // A get/patch against an uninstalled CRD also 404s — with the
        // NotFoundHandler message — and must report the missing CRDs.
        let err = classify_kube(
            "get",
            "SnapshotPolicy",
            "snapshotpolicies",
            Some("media"),
            Some("nightly"),
            kube::Error::Api(
                kube::core::Status::failure(
                    "the server could not find the requested resource",
                    "NotFound",
                )
                .with_code(404)
                .boxed(),
            ),
        );
        assert!(
            err.to_string()
                .contains("does not know the SnapshotPolicy resource type")
        );
    }

    #[test]
    fn admission_denial_beats_the_403_rbac_arm_and_quotes_the_webhook() {
        // A denial often arrives as 403; it must NOT be misread as missing RBAC,
        // and the message must carry the webhook's own (actionable) text.
        let err = classify_kube(
            "create",
            "Snapshot",
            "snapshots",
            Some("media"),
            Some("s"),
            kube::Error::Api(
                kube::core::Status::failure(
                    "admission webhook \"vkopiur.kopiur.home-operations.com\" denied the request: deletionPolicy Retain is forced for discovered snapshots",
                    "Forbidden",
                )
                .with_code(403)
                .boxed(),
            ),
        );
        let msg = err.to_string();
        assert!(
            msg.contains("an admission webhook rejected this object"),
            "{msg}"
        );
        assert!(msg.contains("deletionPolicy Retain is forced"), "{msg}");
        assert!(!msg.contains("RBAC"), "{msg}");
    }

    #[test]
    fn log_stream_interruption_is_not_reported_as_a_bug() {
        let err = CliError::LogStreamInterrupted {
            source: std::io::Error::other("connection reset").into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("log stream was interrupted"), "{msg}");
        assert!(
            msg.contains("re-run the same `kubectl kopiur logs`"),
            "{msg}"
        );
        assert!(!msg.contains("bug"), "{msg}");
    }

    #[test]
    fn all_namespaces_rejection_says_what_to_do_instead() {
        let msg = CliError::AllNamespacesNotApplicable { command: "suspend" }.to_string();
        assert!(msg.contains("suspend targets a single object"), "{msg}");
        assert!(msg.contains("drop -A and pass -n"), "{msg}");
    }

    #[test]
    fn generic_api_error_points_at_cluster_health() {
        let err = classify_kube(
            "list",
            "Snapshot",
            "snapshots",
            Some("x"),
            None,
            api_err(500),
        );
        let msg = err.to_string();
        assert!(
            msg.contains("cannot list snapshots in namespace x"),
            "{msg}"
        );
        assert!(msg.contains("kubectl version"), "{msg}");
    }
}
