//! `kubectl kopiur restore` — the one-liner over the `Restore` CRD: exactly
//! one source (snapshot / policy / raw identity) into exactly one target
//! (created PVC / existing PVC / populator), with optional wait + log stream.

use chrono::{DateTime, Utc};
use kopiur_api::common::{FailurePolicy, ObjectRef, RepositoryRef};
use kopiur_api::restore::{
    FromPolicy, IdentitySource, PvcTemplate, RestoreOptions, RestorePolicy, RestoreSpec,
};
use kopiur_api::{PopulatorTarget, Restore, RestorePhase, RestoreSource, RestoreTarget};
use kube::api::{Api, PostParams};

use crate::CmdOutput;
use crate::cli::RestoreArgs;
use crate::context::KubeCtx;
use crate::error::{CliError, classify_kube};
use crate::output::{OutputFormat, human_bytes};
use crate::wait::{DEFAULT_WAIT_TIMEOUT, wait_for};

/// The short source token used in the default Restore name.
fn source_token(args: &RestoreArgs) -> &str {
    if let Some(s) = &args.from_snapshot {
        s
    } else if let Some(p) = &args.from_policy {
        p
    } else if let Some(id) = &args.identity {
        &id.username
    } else {
        unreachable!("clap requires exactly one source")
    }
}

/// Build the `Restore` CR from the parsed flags. Pure — `now` is injected so
/// names are deterministic under test. The exactly-one-of invariants are
/// enforced by clap groups; this maps each flag set 1:1 onto the
/// externally-tagged enums (no field may be dropped — the restore-options bug
/// class is regression-tested below).
pub fn build_restore(args: &RestoreArgs, namespace: &str, now: DateTime<Utc>) -> Restore {
    let source = match (&args.from_snapshot, &args.from_policy, &args.identity) {
        (Some(snapshot), None, None) => RestoreSource::SnapshotRef(ObjectRef {
            name: snapshot.clone(),
            namespace: args.snapshot_namespace.clone(),
        }),
        (None, Some(policy), None) => RestoreSource::FromPolicy(FromPolicy {
            name: policy.clone(),
            namespace: args.policy_namespace.clone(),
            as_of: args.as_of.clone(),
            offset: args.offset.unwrap_or(0),
        }),
        (None, None, Some(identity)) => RestoreSource::Identity(IdentitySource {
            username: identity.username.clone(),
            hostname: identity.hostname.clone(),
            source_path: identity.source_path.clone(),
            snapshot_id: args.snapshot_id.clone(),
            as_of: args.as_of.clone(),
            offset: args.offset,
        }),
        _ => unreachable!("clap group enforces exactly one source"),
    };

    let target = match (&args.to_pvc, &args.create_pvc, args.populator) {
        (Some(existing), None, false) => RestoreTarget::PvcRef(ObjectRef {
            name: existing.clone(),
            namespace: None,
        }),
        (None, Some(create), false) => RestoreTarget::Pvc(PvcTemplate {
            name: create.clone(),
            storage_class_name: args.storage_class.clone(),
            capacity: args.size.clone(),
            access_modes: args.access_modes.clone(),
        }),
        (None, None, true) => RestoreTarget::Populator(PopulatorTarget {}),
        _ => unreachable!("clap group enforces exactly one target"),
    };

    let repository = args.repository.as_ref().map(|name| RepositoryRef {
        kind: args.repository_kind.into(),
        name: name.clone(),
        namespace: args.repository_namespace.clone(),
    });

    let options = if args.enable_file_deletion
        || args.ignore_permission_errors.is_some()
        || args.write_files_atomically.is_some()
    {
        Some(RestoreOptions {
            enable_file_deletion: args.enable_file_deletion,
            ignore_permission_errors: args.ignore_permission_errors,
            write_files_atomically: args.write_files_atomically,
        })
    } else {
        None
    };

    let policy = if args.on_missing_snapshot.is_some() || args.wait_timeout.is_some() {
        Some(RestorePolicy {
            on_missing_snapshot: args.on_missing_snapshot.map(Into::into),
            wait_timeout: args.wait_timeout.clone(),
        })
    } else {
        None
    };

    let failure_policy = if args.backoff_limit.is_some() || args.active_deadline_seconds.is_some() {
        Some(FailurePolicy {
            backoff_limit: args.backoff_limit,
            active_deadline_seconds: args.active_deadline_seconds,
        })
    } else {
        None
    };

    let name = args.name.clone().unwrap_or_else(|| {
        format!(
            "restore-{}-{}",
            source_token(args),
            now.format("%Y%m%d%H%M%S")
        )
    });
    let mut restore = Restore::new(
        &name,
        RestoreSpec {
            repository,
            source,
            target,
            options,
            policy,
            credential_projection: None,
            mover: None,
            failure_policy,
        },
    );
    restore.metadata.namespace = Some(namespace.to_string());
    restore
}

/// Terminal-phase classification. Exhaustive over [`RestorePhase`].
pub fn terminal(restore: &Restore) -> Option<Result<Box<Restore>, Box<Restore>>> {
    match restore.status.as_ref().and_then(|s| s.phase)? {
        RestorePhase::Pending | RestorePhase::Resolving | RestorePhase::Restoring => None,
        RestorePhase::Completed => Some(Ok(Box::new(restore.clone()))),
        RestorePhase::Failed => Some(Err(Box::new(restore.clone()))),
    }
}

/// One-line success summary from the terminal object's status.
pub fn success_summary(restore: &Restore) -> String {
    let status = restore.status.as_ref();
    let name = restore.metadata.name.as_deref().unwrap_or("?");
    let id = status
        .and_then(|s| s.resolved.as_ref())
        .and_then(|r| r.kopia_snapshot_id.as_deref())
        .unwrap_or("?");
    let bytes = status
        .and_then(|s| s.progress.as_ref())
        .and_then(|p| p.bytes_restored)
        .map(human_bytes)
        .unwrap_or_else(|| "?".into());
    let files = status
        .and_then(|s| s.progress.as_ref())
        .and_then(|p| p.files_restored)
        .map(|f| f.to_string())
        .unwrap_or_else(|| "?".into());
    let target = status
        .and_then(|s| s.target.as_ref())
        .and_then(|t| t.pvc_ref.as_ref())
        .map(|p| format!("pvc/{}", p.name))
        .unwrap_or_else(|| "target".into());
    format!("restore {name} completed: kopia id {id}, {bytes} / {files} files into {target}\n")
}

/// Failure detail from the terminal object's status, for stderr.
pub fn failure_detail(restore: &Restore) -> String {
    let name = restore.metadata.name.as_deref().unwrap_or("?");
    let mut out = format!("restore {name} failed");
    if let Some(f) = restore.status.as_ref().and_then(|s| s.failure.as_ref()) {
        out.push_str(&format!(" ({}): {}", f.kopia_error_class, f.message));
        if let Some(stderr) = &f.stderr_tail {
            out.push_str(&format!("\n--- kopia stderr tail ---\n{stderr}"));
        }
    }
    if let Some(tail) = restore.status.as_ref().and_then(|s| s.log_tail.as_ref()) {
        out.push_str(&format!("\n--- log tail ---\n{tail}"));
    }
    out.push('\n');
    out
}

/// Run `restore`.
pub async fn run(
    ctx: &KubeCtx,
    args: &RestoreArgs,
    output: OutputFormat,
    now: DateTime<Utc>,
) -> Result<CmdOutput, CliError> {
    if matches!(ctx.scope, crate::context::Scope::All) {
        return Err(CliError::AllNamespacesNotApplicable { command: "restore" });
    }
    let ns = ctx.namespace.as_str();
    let restores: Api<Restore> = Api::namespaced(ctx.client.clone(), ns);
    let restore = build_restore(args, ns, now);
    let name = restore.metadata.name.clone().expect("name set by builder");
    let created = restores
        .create(&PostParams::default(), &restore)
        .await
        .map_err(|e| classify_kube("create", "Restore", "restores", Some(ns), Some(&name), e))?;

    let wait = args.wait || args.logs;
    let created_line = format!("restore.{}/{} created\n", kopiur_api::GROUP, name);
    if !wait {
        let text = match output {
            OutputFormat::Table | OutputFormat::Wide => created_line,
            OutputFormat::Yaml => {
                // Through a JSON Value first: serde_yaml would render the
                // externally-tagged enums (source/target) as `!snapshotRef`
                // YAML tags — not the cluster's encoding (convention #5).
                let value =
                    serde_json::to_value(&created).map_err(|e| CliError::Serialization {
                        what: "created Restore",
                        source: e.into(),
                    })?;
                serde_yaml::to_string(&value).map_err(|e| CliError::Serialization {
                    what: "created Restore",
                    source: e.into(),
                })?
            }
            OutputFormat::Json => {
                let mut s = serde_json::to_string_pretty(&created).map_err(|e| {
                    CliError::Serialization {
                        what: "created Restore",
                        source: e.into(),
                    }
                })?;
                s.push('\n');
                s
            }
            OutputFormat::Name => format!("restore.{}/{}\n", kopiur_api::GROUP, name),
        };
        return Ok(CmdOutput { text, exit: 0 });
    }

    eprint!("{created_line}");
    let log_task = if args.logs {
        let ctx_clone = ctx.clone();
        let restore_name = name.clone();
        Some(tokio::spawn(async move {
            crate::cmd::logs::stream_target_logs_when_ready(
                &ctx_clone,
                crate::cmd::logs::LogsTarget::Restore,
                &restore_name,
            )
            .await
        }))
    } else {
        None
    };

    let timeout = args.timeout.unwrap_or(DEFAULT_WAIT_TIMEOUT);
    let verdict = wait_for(
        &restores,
        &name,
        format!("restore {name}"),
        format!(
            "follow it with `kubectl kopiur logs restore {name} -n {ns} -f`, or raise --timeout"
        ),
        timeout,
        terminal,
    )
    .await;

    if let Some(mut task) = log_task
        && tokio::time::timeout(std::time::Duration::from_secs(5), &mut task)
            .await
            .is_err()
    {
        task.abort();
    }

    match verdict? {
        Ok(completed) => Ok(CmdOutput {
            text: success_summary(&completed),
            exit: 0,
        }),
        Err(failed) => {
            eprint!("{}", failure_detail(&failed));
            Ok(CmdOutput {
                text: String::new(),
                exit: 1,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command, OnMissingSnapshotArg, RepositoryKindArg};
    use chrono::TimeZone;
    use clap::Parser;

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 11, 3, 0, 12).unwrap()
    }

    /// Parse a full command line and extract the RestoreArgs.
    fn parse(args: &[&str]) -> RestoreArgs {
        let cli = Cli::try_parse_from(
            ["kubectl-kopiur", "restore"]
                .into_iter()
                .chain(args.iter().copied()),
        )
        .unwrap_or_else(|e| panic!("args {args:?} should parse: {e}"));
        match cli.command {
            Command::Restore(a) => *a,
            other => panic!("expected restore, got {other:?}"),
        }
    }

    fn parse_err(args: &[&str]) -> clap::Error {
        Cli::try_parse_from(
            ["kubectl-kopiur", "restore"]
                .into_iter()
                .chain(args.iter().copied()),
        )
        .expect_err("should not parse")
    }

    #[test]
    fn every_source_target_combination_builds_the_right_enums() {
        let sources: [(&[&str], &str); 3] = [
            (&["--from-snapshot", "snap1"], "SnapshotRef"),
            (&["--from-policy", "pol1"], "FromPolicy"),
            (
                &["--identity", "u@h:/data", "--repository", "repo1"],
                "Identity",
            ),
        ];
        let targets: [(&[&str], &str); 3] = [
            (&["--to-pvc", "existing"], "PvcRef"),
            (&["--create-pvc", "fresh", "--size", "1Gi"], "Pvc"),
            (&["--populator"], "Populator"),
        ];
        for (source_flags, source_kind) in sources {
            for (target_flags, target_kind) in targets {
                let mut flags: Vec<&str> = source_flags.to_vec();
                flags.extend_from_slice(target_flags);
                let args = parse(&flags);
                let restore = build_restore(&args, "media", at());
                assert_eq!(restore.spec.source.kind_str(), source_kind, "{flags:?}");
                assert_eq!(restore.spec.target.kind_str(), target_kind, "{flags:?}");
                // Round-trip through JSON (the cluster's encoding).
                let wire = serde_json::to_value(&restore).unwrap();
                let reparsed: Restore = serde_json::from_value(wire).unwrap();
                assert_eq!(reparsed.spec, restore.spec);
            }
        }
    }

    #[test]
    fn missing_source_or_target_fails_at_parse_time() {
        let err = parse_err(&["--to-pvc", "x"]);
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        let err = parse_err(&["--from-snapshot", "s"]);
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        let err = parse_err(&[
            "--from-snapshot",
            "s",
            "--from-policy",
            "p",
            "--to-pvc",
            "x",
        ]);
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
        let err = parse_err(&["--from-snapshot", "s", "--to-pvc", "x", "--populator"]);
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn create_pvc_requires_an_explicit_size() {
        // The webhook refuses a created PVC without capacity; fail at parse
        // time with flag-level wording instead.
        let err = parse_err(&["--from-policy", "p", "--create-pvc", "x"]);
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn snapshot_id_excludes_point_in_time_selectors() {
        let err = parse_err(&[
            "--identity",
            "u@h",
            "--repository",
            "r",
            "--snapshot-id",
            "abc",
            "--as-of",
            "2026-01-01T00:00:00Z",
            "--to-pvc",
            "x",
        ]);
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn identity_requires_repository_and_as_of_conflicts_with_snapshot_ref() {
        let err = parse_err(&["--identity", "u@h", "--to-pvc", "x"]);
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
        let err = parse_err(&[
            "--from-snapshot",
            "s",
            "--as-of",
            "2026-01-01T00:00:00Z",
            "--to-pvc",
            "x",
        ]);
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn identity_value_parses_user_host_and_optional_path() {
        let args = parse(&[
            "--identity",
            "pg@media:/pvc/data",
            "--repository",
            "nas",
            "--populator",
        ]);
        let id = args.identity.unwrap();
        assert_eq!(id.username, "pg");
        assert_eq!(id.hostname, "media");
        assert_eq!(id.source_path.as_deref(), Some("/pvc/data"));

        let args = parse(&[
            "--identity",
            "pg@media",
            "--repository",
            "nas",
            "--populator",
        ]);
        assert_eq!(args.identity.unwrap().source_path, None);

        for bad in ["nohost", "@h", "u@", "u@h:"] {
            let err = parse_err(&["--identity", bad, "--repository", "nas", "--populator"]);
            assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation, "{bad}");
        }
    }

    #[test]
    fn every_option_flag_lands_in_the_spec() {
        // The restore-options-dropped bug class: EVERY flag must round-trip.
        let args = parse(&[
            "--from-policy",
            "pol1",
            "--policy-namespace",
            "other",
            "--as-of",
            "2026-06-01T00:00:00Z",
            "--offset",
            "2",
            "--create-pvc",
            "fresh",
            "--size",
            "10Gi",
            "--storage-class",
            "fast",
            "--access-mode",
            "ReadWriteOnce",
            "--access-mode",
            "ReadOnlyMany",
            "--enable-file-deletion",
            "--ignore-permission-errors",
            "false",
            "--write-files-atomically",
            "true",
            "--on-missing-snapshot",
            "continue",
            "--wait-timeout",
            "5m",
            "--backoff-limit",
            "1",
            "--active-deadline-seconds",
            "600",
            "--repository",
            "nas",
            "--repository-kind",
            "cluster-repository",
            "--name",
            "my-restore",
        ]);
        assert_eq!(args.repository_kind, RepositoryKindArg::ClusterRepository);
        assert_eq!(
            args.on_missing_snapshot,
            Some(OnMissingSnapshotArg::Continue)
        );
        let wire = serde_json::to_value(build_restore(&args, "media", at())).unwrap();
        assert_eq!(wire["metadata"]["name"], "my-restore");
        let spec = &wire["spec"];
        assert_eq!(spec["source"]["fromPolicy"]["name"], "pol1");
        assert_eq!(spec["source"]["fromPolicy"]["namespace"], "other");
        assert_eq!(spec["source"]["fromPolicy"]["asOf"], "2026-06-01T00:00:00Z");
        assert_eq!(spec["source"]["fromPolicy"]["offset"], 2);
        assert_eq!(spec["target"]["pvc"]["name"], "fresh");
        assert_eq!(spec["target"]["pvc"]["capacity"], "10Gi");
        assert_eq!(spec["target"]["pvc"]["storageClassName"], "fast");
        assert_eq!(
            spec["target"]["pvc"]["accessModes"],
            serde_json::json!(["ReadWriteOnce", "ReadOnlyMany"])
        );
        assert_eq!(spec["options"]["enableFileDeletion"], true);
        assert_eq!(spec["options"]["ignorePermissionErrors"], false);
        assert_eq!(spec["options"]["writeFilesAtomically"], true);
        assert_eq!(spec["policy"]["onMissingSnapshot"], "Continue");
        assert_eq!(spec["policy"]["waitTimeout"], "5m");
        assert_eq!(spec["failurePolicy"]["backoffLimit"], 1);
        assert_eq!(spec["failurePolicy"]["activeDeadlineSeconds"], 600);
        assert_eq!(spec["repository"]["kind"], "ClusterRepository");
        assert_eq!(spec["repository"]["name"], "nas");
    }

    #[test]
    fn minimal_restore_has_no_optional_noise_on_the_wire() {
        let args = parse(&["--from-snapshot", "snap1", "--to-pvc", "data"]);
        let wire = serde_json::to_value(build_restore(&args, "media", at())).unwrap();
        assert_eq!(wire["metadata"]["name"], "restore-snap1-20260611030012");
        let spec = &wire["spec"];
        assert_eq!(spec["source"]["snapshotRef"]["name"], "snap1");
        assert_eq!(spec["target"]["pvcRef"]["name"], "data");
        for key in ["options", "policy", "failurePolicy", "repository", "mover"] {
            assert!(spec.get(key).is_none(), "{key} should be absent");
        }
    }

    #[test]
    fn identity_snapshot_id_lands_with_the_adr_capitalization() {
        let args = parse(&[
            "--identity",
            "u@h:/p",
            "--snapshot-id",
            "abc123",
            "--repository",
            "nas",
            "--to-pvc",
            "x",
        ]);
        let wire = serde_json::to_value(build_restore(&args, "media", at())).unwrap();
        assert_eq!(wire["spec"]["source"]["identity"]["snapshotID"], "abc123");
        assert_eq!(wire["spec"]["source"]["identity"]["username"], "u");
        assert_eq!(wire["spec"]["source"]["identity"]["sourcePath"], "/p");
    }

    fn with_phase(phase: &str) -> Restore {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Restore",
            "metadata": { "name": "r", "namespace": "media" },
            "spec": {
                "source": { "snapshotRef": { "name": "s" } },
                "target": { "pvcRef": { "name": "d" } }
            },
            "status": { "phase": phase }
        }))
        .unwrap()
    }

    #[test]
    fn yaml_output_uses_the_cluster_encoding_not_serde_yaml_tags() {
        // serde_yaml renders newtype enum variants as `!snapshotRef` tags when
        // serialized directly; the CLI must emit the cluster's plain-mapping
        // encoding (via a JSON Value) so the output is kubectl-applyable.
        let args = parse(&["--from-snapshot", "snap1", "--to-pvc", "data"]);
        let restore = build_restore(&args, "media", at());
        let value = serde_json::to_value(&restore).unwrap();
        let yaml = serde_yaml::to_string(&value).unwrap();
        assert!(yaml.contains("snapshotRef:"), "{yaml}");
        assert!(yaml.contains("pvcRef:"), "{yaml}");
        assert!(
            !yaml.contains('!'),
            "no serde_yaml enum tags allowed: {yaml}"
        );
        // The direct serialization WOULD tag — this guards the reason the
        // Value route exists. If serde_yaml ever changes, revisit.
        let direct = serde_yaml::to_string(&restore).unwrap();
        assert!(
            direct.contains('!'),
            "serde_yaml behavior changed: {direct}"
        );
    }

    #[test]
    fn terminal_classification_is_exhaustive_and_correct() {
        for pending in ["Pending", "Resolving", "Restoring"] {
            assert!(terminal(&with_phase(pending)).is_none(), "{pending}");
        }
        assert!(matches!(terminal(&with_phase("Completed")), Some(Ok(_))));
        assert!(matches!(terminal(&with_phase("Failed")), Some(Err(_))));
    }

    #[test]
    fn success_summary_reports_id_bytes_files_and_target() {
        let restore: Restore = serde_json::from_value(serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Restore",
            "metadata": { "name": "r" },
            "spec": {
                "source": { "snapshotRef": { "name": "s" } },
                "target": { "pvcRef": { "name": "d" } }
            },
            "status": {
                "phase": "Completed",
                "resolved": { "kopiaSnapshotID": "abc123" },
                "progress": { "bytesRestored": 1536, "filesRestored": 12 },
                "target": { "pvcRef": { "name": "d" } }
            }
        }))
        .unwrap();
        assert_eq!(
            success_summary(&restore),
            "restore r completed: kopia id abc123, 1.5 KiB / 12 files into pvc/d\n"
        );
    }
}
