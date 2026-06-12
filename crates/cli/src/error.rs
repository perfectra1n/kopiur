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

    /// A migration input (VolSync object / restic Secret) can't be translated.
    #[error("{what}. Fix: {fix}")]
    MigrationInput {
        /// What is wrong with the input.
        what: String,
        /// What to do about it.
        fix: String,
    },

    /// A snapshot's repository lives in a different namespace than the
    /// session pod would.
    #[error(
        "the snapshot's repository ({repo}) lives in namespace {repo_namespace}, but the \
         browse session pod runs in the snapshot's namespace ({session_namespace}) and is \
         owned by the repository — Kubernetes forbids cross-namespace owners, so the session \
         would be garbage-collected mid-read. \
         Fix: run the command against a snapshot in the repository's namespace, or use --local"
    )]
    RepoOutsideSessionNamespace {
        /// `kind/name` of the repository.
        repo: String,
        /// Where the repository lives.
        repo_namespace: String,
        /// Where the session pod would run.
        session_namespace: String,
    },

    /// The session mover image could not be resolved safely.
    #[error("cannot resolve the mover image for the browse session: {why}. Fix: {fix}")]
    MoverImageUnresolvable {
        /// What went wrong with the lookup.
        why: String,
        /// What to do about it.
        fix: String,
    },

    /// A path component that must be a directory is something else.
    #[error(
        "{path:?} is not a directory (kopia entry type {entry_type:?}); \
         `ls` lists directories — use `cat`/`download` to read a file"
    )]
    NotADirectory {
        /// The offending path.
        path: String,
        /// The kopia entry type encountered (`f`, `s`, …).
        entry_type: String,
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

    // --- browse data-plane (ls / cat / download / browse / session end) ---
    /// The Snapshot has no kopia snapshot id pinned in status, so there is
    /// nothing to read.
    #[error(
        "Snapshot {name:?} cannot be browsed: {reason}. \
         Browsing reads the kopia snapshot recorded in status.snapshot.kopiaSnapshotID, \
         which only exists once a snapshot succeeded (or was discovered). \
         Fix: wait for it to reach Succeeded, or pick another with \
         `kubectl kopiur snapshots list`"
    )]
    SnapshotNotBrowsable {
        /// The Snapshot name.
        name: String,
        /// Why it cannot be browsed (current phase / missing status).
        reason: String,
    },

    /// The Snapshot's repository cannot be derived (no pinned resolved ref, no
    /// owning repository).
    #[error(
        "cannot determine which repository Snapshot {snapshot:?} lives in: it has \
         neither a pinned status.resolved.repository nor a Repository/ClusterRepository \
         ownerReference. \
         Fix: this usually means the snapshot never ran — create a fresh one with \
         `kubectl kopiur snapshot now`, or browse a discovered snapshot"
    )]
    RepositoryUnderivable {
        /// The Snapshot name.
        snapshot: String,
    },

    /// The repository's credential Secret lives outside the namespace the
    /// session pod would run in (a pod cannot `envFrom` across namespaces).
    #[error(
        "the repository credential Secret {secret:?} lives in namespace \
         {secret_namespace}, but the browse session pod runs in namespace \
         {session_namespace}, and a pod cannot load a Secret from another namespace. \
         Fix: browse a snapshot in namespace {secret_namespace}, copy the Secret \
         into {session_namespace}, or read locally with --local"
    )]
    CredsOutsideSessionNamespace {
        /// The credential Secret name.
        secret: String,
        /// Where the Secret actually lives.
        secret_namespace: String,
        /// Where the session pod runs (the Snapshot's namespace).
        session_namespace: String,
    },

    /// A ClusterRepository credential reference pins no namespace, so the
    /// Secret cannot be located.
    #[error(
        "ClusterRepository {repository:?} references credential Secret {secret:?} \
         without a namespace, so it cannot be located from a browse session. \
         Cluster-scoped repositories must pin secretRef.namespace explicitly. \
         Fix: set the namespace on the ClusterRepository's secret references"
    )]
    ClusterRepoSecretNamespaceMissing {
        /// The credential Secret name.
        secret: String,
        /// The ClusterRepository name.
        repository: String,
    },

    /// The session pod failed before becoming ready.
    #[error(
        "the browse session pod (Job {job} in namespace {namespace}) failed before \
         becoming ready: {detail}. \
         The session connects to the repository read-only; the pod logs above name \
         the cause. Fix: check credentials and backend reachability \
         (`kubectl kopiur doctor`), then retry"
    )]
    SessionPodFailed {
        /// The session Job name.
        job: String,
        /// The Job's namespace.
        namespace: String,
        /// Failure detail (pod state + log tail when available).
        detail: String,
    },

    /// Waiting for the session pod to become ready timed out.
    #[error(
        "timed out after {after} waiting for the browse session pod (Job {job}) to \
         become ready. It may still be pulling its image or connecting to a slow \
         backend. Fix: inspect it with \
         `kubectl get pods -l batch.kubernetes.io/job-name={job}` (and its logs), \
         then retry — a warm session answers instantly"
    )]
    SessionNotReady {
        /// The session Job name.
        job: String,
        /// How long we waited, humanized.
        after: String,
    },

    /// An exec'd in-session kopia read failed.
    #[error(
        "the in-session kopia read failed ({what}): {stderr}. \
         The session is connected read-only, so this is a read/availability problem, \
         never a mutation. Fix: retry; if it persists, end the session \
         (`kubectl kopiur session end`) and start fresh"
    )]
    SessionExec {
        /// Which read failed.
        what: String,
        /// kopia's stderr (tail).
        stderr: String,
    },

    /// `--local` was passed but no kopia binary is available.
    #[error(
        "--local needs a kopia binary on this machine, but {bin:?} was not found. \
         Fix: install kopia (https://kopia.io/docs/installation/) or pass \
         --kopia-bin PATH — or drop --local to use the in-cluster session, \
         which needs no local kopia"
    )]
    LocalKopiaMissing {
        /// The binary that was looked for.
        bin: String,
    },

    /// A `--local` kopia invocation failed.
    #[error(
        "--local kopia operation failed ({what}): {source}. \
         --local talks to the backend FROM THIS MACHINE with the repository's \
         credentials. Fix: verify the endpoint is reachable from here (in-cluster-only \
         endpoints need a port-forward) and the credentials Secret is valid — or drop \
         --local to read through the in-cluster session"
    )]
    LocalKopia {
        /// Which operation failed.
        what: String,
        /// The kopia client error.
        #[source]
        source: Box<kopiur_kopia::KopiaError>,
    },

    /// `--local` cannot mount a cluster-volume filesystem repository.
    #[error(
        "--local cannot read repository {repository:?}: its filesystem backend lives \
         on a cluster volume (PVC/inline NFS) this machine cannot mount. \
         Fix: drop --local and use the in-cluster session, which mounts the \
         repository volume read-only"
    )]
    LocalRepoVolume {
        /// The repository name.
        repository: String,
    },

    /// `--local` cannot authenticate a workload-identity repository.
    #[error(
        "--local cannot read repository {repository:?}: its backend authenticates via \
         workload identity (ServiceAccount {service_account:?}), whose federated \
         credentials only exist inside a pod running as that ServiceAccount — there \
         is no credential Secret to copy onto this machine. \
         Fix: drop --local and use the in-cluster session, which runs as the \
         federated ServiceAccount"
    )]
    LocalWorkloadIdentity {
        /// The repository name.
        repository: String,
        /// The federated ServiceAccount the backend names.
        service_account: String,
    },

    /// Reading the credential Secret for `--local` was refused.
    #[error(
        "forbidden: cannot get Secret {secret:?} in namespace {namespace}: {source}. \
         --local copies the repository credentials onto this machine, which needs \
         `get` on `secrets` — RBAC the in-cluster session path deliberately does NOT \
         need. Fix: ask a cluster admin for `get secrets` in {namespace}, or drop --local"
    )]
    SecretsForbidden {
        /// The Secret name.
        secret: String,
        /// Its namespace.
        namespace: String,
        /// The API server's error.
        #[source]
        source: Box<kube::Error>,
    },

    /// A user-supplied snapshot path is malformed or escapes the snapshot root.
    #[error(
        "invalid snapshot path {path:?}: {reason}. \
         Paths are relative to the snapshot root (e.g. `sub/file.txt`); `..` and \
         absolute paths are not allowed. \
         Fix: pass a relative path — list the root first with `kubectl kopiur ls <snapshot>`"
    )]
    InvalidPath {
        /// The offending path.
        path: String,
        /// What is wrong with it.
        reason: String,
    },

    /// A snapshot path does not exist.
    #[error(
        "path {path:?} does not exist in this snapshot. \
         Fix: list the directory with `kubectl kopiur ls <snapshot> [dir]` and check \
         the spelling (names are case-sensitive)"
    )]
    PathNotFound {
        /// The missing path.
        path: String,
    },

    /// `cat`/`download` was pointed at a directory.
    #[error(
        "{path:?} is a directory; cat/download read files. \
         Fix: list it with `kubectl kopiur ls <snapshot> {path}`, then name a file inside it"
    )]
    IsADirectory {
        /// The directory path.
        path: String,
    },

    /// `cat`/`download` was pointed at a non-regular-file entry (symlink, …).
    #[error(
        "{path:?} is not a regular file (kopia entry type {entry_type:?}), so its \
         bytes cannot be streamed. Fix: only regular files can be read; list the \
         directory with `kubectl kopiur ls` to see entry types"
    )]
    NotAFile {
        /// The entry path.
        path: String,
        /// The kopia entry type (`d`, `s`, …).
        entry_type: String,
    },

    /// The kopia snapshot id pinned in status is gone from the repository.
    #[error(
        "kopia snapshot {id} is not in the repository catalog (it may have been \
         expired by retention or deleted out-of-band). \
         Fix: pick a current snapshot with `kubectl kopiur snapshots list`"
    )]
    SnapshotMissingInRepo {
        /// The kopia snapshot manifest id.
        id: String,
    },

    /// A download wrote fewer/more bytes than the snapshot manifest records.
    #[error(
        "download of {path:?} is incomplete: expected {expected} bytes, wrote {actual}. \
         The partial file at {dest} was removed so a truncated restore can't be \
         mistaken for the real one. Fix: retry; if it persists, verify the snapshot \
         (kopiur's verification, or `kopia snapshot verify`)"
    )]
    DownloadIncomplete {
        /// The snapshot path downloaded.
        path: String,
        /// Bytes the manifest records.
        expected: i64,
        /// Bytes actually written.
        actual: u64,
        /// Destination whose partial content was removed.
        dest: String,
    },

    /// kopia produced output the CLI could not interpret.
    #[error(
        "unexpected kopia output while reading {what}: {detail}. \
         Fix: retry; if it persists this is likely a kopiur/kopia version mismatch — \
         report it at https://github.com/home-operations/kopiur/issues"
    )]
    UnexpectedKopiaOutput {
        /// What was being read.
        what: String,
        /// Why the output couldn't be interpreted.
        detail: String,
    },

    /// A local filesystem operation (download dest, --local staging dir) failed.
    #[error(
        "local file operation failed ({what}): {source}. \
         Fix: check the path exists, is writable, and has free space, then retry"
    )]
    LocalIo {
        /// What was being done.
        what: String,
        /// The underlying IO error.
        #[source]
        source: std::io::Error,
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
    fn not_a_directory_points_at_cat_download() {
        let msg = CliError::NotADirectory {
            path: "a.txt".into(),
            entry_type: "f".into(),
        }
        .to_string();
        assert!(msg.contains("is not a directory"), "{msg}");
        assert!(msg.contains("cat`/`download"), "{msg}");
    }

    #[test]
    fn mover_image_resolution_failures_carry_the_fix() {
        let msg = CliError::MoverImageUnresolvable {
            why: "2 Deployments match".into(),
            fix: "remove the impostor".into(),
        }
        .to_string();
        assert!(msg.contains("cannot resolve the mover image"), "{msg}");
        assert!(msg.contains("remove the impostor"), "{msg}");
    }

    #[test]
    fn cross_namespace_repo_session_names_the_gc_hazard_and_local_escape() {
        let msg = CliError::RepoOutsideSessionNamespace {
            repo: "Repository/nas".into(),
            repo_namespace: "backups".into(),
            session_namespace: "media".into(),
        }
        .to_string();
        assert!(msg.contains("cross-namespace owners"), "{msg}");
        assert!(msg.contains("--local"), "{msg}");
    }

    #[test]
    fn all_namespaces_rejection_says_what_to_do_instead() {
        let msg = CliError::AllNamespacesNotApplicable { command: "suspend" }.to_string();
        assert!(msg.contains("suspend targets a single object"), "{msg}");
        assert!(msg.contains("drop -A and pass -n"), "{msg}");
    }

    // --- browse data-plane: every variant says what failed, why, and the fix ---

    #[test]
    fn snapshot_not_browsable_names_the_status_field_and_fix() {
        let msg = CliError::SnapshotNotBrowsable {
            name: "db-1".into(),
            reason: "phase is Running".into(),
        }
        .to_string();
        assert!(msg.contains("Snapshot \"db-1\" cannot be browsed"), "{msg}");
        assert!(msg.contains("phase is Running"), "{msg}");
        assert!(msg.contains("status.snapshot.kopiaSnapshotID"), "{msg}");
        assert!(msg.contains("kubectl kopiur snapshots list"), "{msg}");
    }

    #[test]
    fn repository_underivable_explains_both_derivation_sources() {
        let msg = CliError::RepositoryUnderivable {
            snapshot: "db-1".into(),
        }
        .to_string();
        assert!(msg.contains("status.resolved.repository"), "{msg}");
        assert!(msg.contains("ownerReference"), "{msg}");
        assert!(msg.contains("kubectl kopiur snapshot now"), "{msg}");
    }

    #[test]
    fn cross_namespace_creds_offer_three_outs() {
        let msg = CliError::CredsOutsideSessionNamespace {
            secret: "s3-creds".into(),
            secret_namespace: "backups".into(),
            session_namespace: "media".into(),
        }
        .to_string();
        assert!(msg.contains("\"s3-creds\""), "{msg}");
        assert!(msg.contains("namespace backups"), "{msg}");
        assert!(
            msg.contains("cannot load a Secret from another namespace"),
            "{msg}"
        );
        assert!(msg.contains("--local"), "{msg}");
    }

    #[test]
    fn cluster_repo_secret_without_namespace_names_the_field() {
        let msg = CliError::ClusterRepoSecretNamespaceMissing {
            secret: "creds".into(),
            repository: "nas".into(),
        }
        .to_string();
        assert!(msg.contains("ClusterRepository \"nas\""), "{msg}");
        assert!(msg.contains("secretRef.namespace"), "{msg}");
    }

    #[test]
    fn session_pod_failure_and_timeout_point_at_pods_and_doctor() {
        let failed = CliError::SessionPodFailed {
            job: "kopiur-browse-nas-abc123".into(),
            namespace: "media".into(),
            detail: "repository not initialized".into(),
        }
        .to_string();
        assert!(failed.contains("kopiur-browse-nas-abc123"), "{failed}");
        assert!(failed.contains("repository not initialized"), "{failed}");
        assert!(failed.contains("kubectl kopiur doctor"), "{failed}");

        let timeout = CliError::SessionNotReady {
            job: "kopiur-browse-nas-abc123".into(),
            after: "2m".into(),
        }
        .to_string();
        assert!(timeout.contains("timed out after 2m"), "{timeout}");
        assert!(
            timeout.contains("batch.kubernetes.io/job-name=kopiur-browse-nas-abc123"),
            "{timeout}"
        );
    }

    #[test]
    fn session_exec_failure_quotes_stderr_and_reassures_read_only() {
        let msg = CliError::SessionExec {
            what: "show kdeadbeef".into(),
            stderr: "object not found".into(),
        }
        .to_string();
        assert!(msg.contains("object not found"), "{msg}");
        assert!(msg.contains("read-only"), "{msg}");
        assert!(msg.contains("session end"), "{msg}");
    }

    #[test]
    fn local_errors_explain_the_local_contract() {
        let missing = CliError::LocalKopiaMissing {
            bin: "kopia".into(),
        }
        .to_string();
        assert!(missing.contains("install kopia"), "{missing}");
        assert!(missing.contains("drop --local"), "{missing}");

        let volume = CliError::LocalRepoVolume {
            repository: "fs-repo".into(),
        }
        .to_string();
        assert!(volume.contains("cluster volume"), "{volume}");
        assert!(volume.contains("in-cluster session"), "{volume}");

        let forbidden = CliError::SecretsForbidden {
            secret: "s3-creds".into(),
            namespace: "media".into(),
            source: Box::new(api_err(403)),
        }
        .to_string();
        assert!(forbidden.contains("`get` on `secrets`"), "{forbidden}");
        assert!(forbidden.contains("or drop --local"), "{forbidden}");
    }

    #[test]
    fn path_errors_teach_the_path_grammar() {
        let invalid = CliError::InvalidPath {
            path: "../etc/passwd".into(),
            reason: "`..` components are not allowed".into(),
        }
        .to_string();
        assert!(
            invalid.contains("relative to the snapshot root"),
            "{invalid}"
        );

        let missing = CliError::PathNotFound {
            path: "sub/missing.txt".into(),
        }
        .to_string();
        assert!(
            missing.contains("does not exist in this snapshot"),
            "{missing}"
        );
        assert!(missing.contains("kubectl kopiur ls"), "{missing}");

        let dir = CliError::IsADirectory { path: "sub".into() }.to_string();
        assert!(dir.contains("is a directory"), "{dir}");
        assert!(dir.contains("ls <snapshot> sub"), "{dir}");

        let link = CliError::NotAFile {
            path: "link".into(),
            entry_type: "s".into(),
        }
        .to_string();
        assert!(link.contains("not a regular file"), "{link}");
    }

    #[test]
    fn missing_catalog_entry_and_incomplete_download_are_actionable() {
        let gone = CliError::SnapshotMissingInRepo { id: "kdead".into() }.to_string();
        assert!(gone.contains("kdead"), "{gone}");
        assert!(gone.contains("expired by retention"), "{gone}");

        let short = CliError::DownloadIncomplete {
            path: "sub/b.txt".into(),
            expected: 12,
            actual: 7,
            dest: "/tmp/b.txt".into(),
        }
        .to_string();
        assert!(short.contains("expected 12 bytes, wrote 7"), "{short}");
        assert!(
            short.contains("partial file at /tmp/b.txt was removed"),
            "{short}"
        );
    }

    #[test]
    fn unexpected_kopia_output_is_reported_as_a_compat_bug() {
        let msg = CliError::UnexpectedKopiaOutput {
            what: "directory manifest kabc".into(),
            detail: "stream marker was \"kopia:other\"".into(),
        }
        .to_string();
        assert!(msg.contains("directory manifest kabc"), "{msg}");
        assert!(msg.contains("version mismatch"), "{msg}");
        assert!(
            msg.contains("github.com/home-operations/kopiur/issues"),
            "{msg}"
        );
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
