//! Fake-kopia shim tests. We write a tiny shell script that mimics kopia's
//! stdout-JSON / stderr-progress split and a chosen exit code, point a
//! `KopiaClient` at it, and assert the client parses success and surfaces
//! errors correctly. This exercises the full subprocess path with NO real
//! kopia.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use kopiur_kopia::{
    ConnectSpec, KopiaClient, KopiaError, KopiaErrorClass, PolicyArgs, RestoreOptions,
    VerifyOptions,
};

/// Write an executable shell script to a tempdir and return its path. The
/// tempdir is leaked into the returned guard so the file outlives the test
/// body.
struct Shim {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

fn shim(script: &str) -> Shim {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kopia-shim.sh");
    std::fs::write(&path, script).unwrap();
    let mut perms = std::fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&path, perms).unwrap();
    Shim { _dir: dir, path }
}

fn client_for(shim: &Shim) -> KopiaClient {
    KopiaClient::builder().binary(shim.path.clone()).build()
}

#[tokio::test]
async fn snapshot_create_parses_stdout_json_ignoring_stderr_progress() {
    // Emulate kopia: progress to stderr, JSON result to stdout, exit 0.
    let s = shim(
        r#"#!/bin/sh
echo "Snapshotting root@host:/data ..." 1>&2
echo "  \\ 0 hashing, 2 hashed (12 B), 0 cached" 1>&2
echo '{"id":"deadbeef","source":{"host":"h","userName":"u","path":"/data"},"description":"","startTime":"2026-06-02T03:13:59Z","endTime":"2026-06-02T03:14:00Z","rootEntry":{"name":"data","type":"d","obj":"k123","summ":{"size":12,"files":2,"symlinks":0,"dirs":1,"numFailed":0}}}'
exit 0
"#,
    );
    let client = client_for(&s);
    let res = client
        .snapshot_create("/data", &BTreeMap::new(), None)
        .await
        .expect("should parse success");
    assert_eq!(res.id, "deadbeef");
    assert_eq!(res.source.identity(), "u@h:/data");
    assert_eq!(res.total_bytes(), 12);
    assert_eq!(res.file_count(), 2);
}

#[tokio::test]
async fn snapshot_create_passes_tags_as_args() {
    // The shim echoes its own args to stderr; we assert success and that the
    // JSON still parses. (Tag wiring itself is covered by the unit test on
    // arg construction; here we prove tags don't break invocation.)
    let s = shim(
        r#"#!/bin/sh
echo "args: $@" 1>&2
echo '{"id":"t1","source":{"host":"h","userName":"u","path":"/d"},"startTime":"2026-06-02T03:13:59Z","endTime":"2026-06-02T03:14:00Z"}'
exit 0
"#,
    );
    let client = client_for(&s);
    let mut tags = BTreeMap::new();
    tags.insert("app".to_string(), "db".to_string());
    let res = client.snapshot_create("/d", &tags, None).await.unwrap();
    assert_eq!(res.id, "t1");
}

#[tokio::test]
async fn snapshot_create_passes_override_source() {
    // The shim exits non-zero UNLESS it is invoked with the resolved identity as
    // `--override-source u@h:/d`, proving Kopiur records snapshots under the
    // operator identity, not the pod's (ADR §4.2).
    let s = shim(
        r#"#!/bin/sh
case "$*" in
  *"--override-source u@h:/d"*)
    echo '{"id":"ok","source":{"host":"h","userName":"u","path":"/d"},"startTime":"2026-06-02T03:13:59Z","endTime":"2026-06-02T03:14:00Z"}'
    exit 0 ;;
  *) echo "missing --override-source: $*" 1>&2; exit 7 ;;
esac
"#,
    );
    let client = client_for(&s);
    let res = client
        .snapshot_create("/d", &BTreeMap::new(), Some("u@h:/d"))
        .await
        .expect("override-source must be passed through");
    assert_eq!(res.id, "ok");
}

#[tokio::test]
async fn nonzero_exit_surfaces_code_and_stderr_and_class() {
    let s = shim(
        r#"#!/bin/sh
echo "Snapshotting root@host:/nonexistent ..." 1>&2
echo "encountered 2 errors:" 1>&2
echo "failed to prepare source: lstat /nonexistent: no such file or directory" 1>&2
exit 1
"#,
    );
    let client = client_for(&s);
    let err = client
        .snapshot_create("/nonexistent", &BTreeMap::new(), None)
        .await
        .expect_err("should fail");
    match err {
        KopiaError::NonZeroExit {
            code,
            class,
            stderr_tail,
            ..
        } => {
            assert_eq!(code, Some(1));
            // "no such file or directory" classifies as NotFound.
            assert_eq!(class, KopiaErrorClass::NotFound);
            assert!(stderr_tail.contains("no such file or directory"));
            // Progress line retained in tail too (we keep last N lines).
            assert!(stderr_tail.contains("failed to prepare source"));
        }
        other => panic!("expected NonZeroExit, got {other:?}"),
    }
}

#[tokio::test]
async fn stderr_streamed_lines_still_fully_captured_in_tail() {
    // Regression for the stderr line-streaming refactor: kopia's stderr is read
    // line-by-line (and echoed to logs at debug, target `kopia`) instead of
    // slurped whole. Prove every line — including a final line with NO trailing
    // newline — still reaches the failure tail.
    let s = shim(
        r#"#!/bin/sh
echo "first progress line" 1>&2
echo "second progress line" 1>&2
printf "final line without newline" 1>&2
exit 3
"#,
    );
    let client = client_for(&s);
    let err = client
        .repository_status()
        .await
        .expect_err("nonzero exit should surface");
    match err {
        KopiaError::NonZeroExit {
            code, stderr_tail, ..
        } => {
            assert_eq!(code, Some(3));
            assert!(stderr_tail.contains("first progress line"), "{stderr_tail}");
            assert!(
                stderr_tail.contains("second progress line"),
                "{stderr_tail}"
            );
            assert!(
                stderr_tail.contains("final line without newline"),
                "{stderr_tail}"
            );
        }
        other => panic!("expected NonZeroExit, got {other:?}"),
    }
}

#[tokio::test]
async fn auth_failure_classified() {
    let s = shim(
        r#"#!/bin/sh
echo "ERROR error connecting to repository: invalid repository password" 1>&2
exit 1
"#,
    );
    let client = client_for(&s);
    let err = client
        .repository_connect(
            &ConnectSpec::Filesystem {
                path: PathBuf::from("/repo"),
            },
            Default::default(),
        )
        .await
        .expect_err("should fail");
    assert_eq!(err.class(), KopiaErrorClass::AuthFailure);
    assert!(!err.class().is_retryable());
}

#[tokio::test]
async fn empty_stdout_is_empty_output_error() {
    // Exit 0 but no JSON on stdout (only progress on stderr) → EmptyOutput.
    let s = shim(
        r#"#!/bin/sh
echo "Finished quick maintenance." 1>&2
exit 0
"#,
    );
    let client = client_for(&s);
    let err = client.repository_status().await.expect_err("no json");
    assert!(matches!(err, KopiaError::EmptyOutput { .. }));
}

#[tokio::test]
async fn malformed_json_is_parse_error() {
    let s = shim(
        r#"#!/bin/sh
echo '{not valid json'
exit 0
"#,
    );
    let client = client_for(&s);
    let err = client.repository_status().await.expect_err("bad json");
    assert!(matches!(err, KopiaError::Json { .. }));
}

#[tokio::test]
async fn spawn_failure_when_binary_missing() {
    let client = KopiaClient::builder()
        .binary("/definitely/not/a/real/kopia/binary")
        .build();
    let err = client.repository_status().await.expect_err("spawn fails");
    assert!(matches!(err, KopiaError::Spawn { .. }));
    assert_eq!(err.class(), KopiaErrorClass::Unknown);
}

#[tokio::test]
async fn snapshot_delete_and_restore_use_exit_code_only() {
    // These subcommands emit no JSON; success is exit 0.
    let s = shim(
        r#"#!/bin/sh
echo "Restored 2 files, 2 directories and 0 symbolic links (12 B)." 1>&2
exit 0
"#,
    );
    let client = client_for(&s);
    client.snapshot_restore("abc", "/target").await.unwrap();
    client.snapshot_delete("abc").await.unwrap();
}

// --- New verb / backend coverage. The shims gate exit 0 on the expected argv,
// so these double as wiring assertions against the real kopia 0.23 flag names. ---

/// A shim that exits 0 iff its argv contains `$NEEDLE`, else 9 with a diagnostic.
fn argv_gate_shim(needle: &str) -> Shim {
    shim(&format!(
        r#"#!/bin/sh
case "$*" in
  *"{needle}"*) exit 0 ;;
  *) echo "argv did not contain [{needle}]: $*" 1>&2; exit 9 ;;
esac
"#
    ))
}

#[tokio::test]
async fn connect_azure_backend_argv() {
    let s = argv_gate_shim("repository connect azure --container backups --prefix k/");
    let client = client_for(&s);
    client
        .repository_connect(
            &ConnectSpec::Azure {
                container: "backups".into(),
                storage_account: None,
                prefix: Some("k/".into()),
            },
            Default::default(),
        )
        .await
        .expect("azure connect argv must match");
}

#[tokio::test]
async fn connect_sftp_backend_argv() {
    let s =
        argv_gate_shim("repository connect sftp --host h --path /repo --port 2222 --username u");
    let client = client_for(&s);
    client
        .repository_connect(
            &ConnectSpec::Sftp {
                host: "h".into(),
                path: "/repo".into(),
                port: Some(2222),
                username: Some("u".into()),
                keyfile: None,
                known_hosts: None,
            },
            Default::default(),
        )
        .await
        .expect("sftp connect argv must match");
}

#[tokio::test]
async fn restore_with_options_passes_flags() {
    let s = argv_gate_shim(
        "snapshot restore snap1 /target --no-ignore-permission-errors --write-files-atomically",
    );
    let client = client_for(&s);
    client
        .snapshot_restore_with(
            "snap1",
            "/target",
            &RestoreOptions {
                ignore_permission_errors: Some(false),
                write_files_atomically: Some(true),
                ..Default::default()
            },
        )
        .await
        .expect("restore option flags must pass through");
}

#[tokio::test]
async fn snapshot_verify_passes_flags() {
    let s = argv_gate_shim("snapshot verify --verify-files-percent 5 --max-errors 0");
    let client = client_for(&s);
    client
        .snapshot_verify(&VerifyOptions {
            verify_files_percent: Some(5),
            max_errors: Some(0),
            parallel: None,
        })
        .await
        .expect("verify flags must pass through");
}

#[tokio::test]
async fn snapshot_pin_unpin_expire_estimate_validate() {
    // Each gates on its own argv; run them against fresh shims.
    let pin = argv_gate_shim("snapshot pin s1 --add protected");
    client_for(&pin)
        .snapshot_pin("s1", "protected")
        .await
        .unwrap();

    let unpin = argv_gate_shim("snapshot pin s1 --remove protected");
    client_for(&unpin)
        .snapshot_unpin("s1", "protected")
        .await
        .unwrap();

    let expire = argv_gate_shim("snapshot expire --all --delete");
    client_for(&expire).snapshot_expire(true).await.unwrap();

    let estimate = argv_gate_shim("snapshot estimate /data");
    client_for(&estimate)
        .snapshot_estimate("/data")
        .await
        .unwrap();

    let validate = argv_gate_shim("repository validate-provider");
    client_for(&validate)
        .repository_validate_provider()
        .await
        .unwrap();
}

#[tokio::test]
async fn policy_set_passes_flags() {
    let s = argv_gate_shim("policy set user@host:/d --compression zstd --add-ignore *.tmp");
    let client = client_for(&s);
    client
        .policy_set(
            "user@host:/d",
            &PolicyArgs {
                compression: Some("zstd".into()),
                ignore: vec!["*.tmp".into()],
                ..Default::default()
            },
        )
        .await
        .expect("policy set flags must pass through");
}

#[tokio::test]
async fn policy_show_parses_json() {
    let s = shim(
        r#"#!/bin/sh
echo '{"compression":{"compressorName":"zstd"},"splitter":{"algorithm":"DYNAMIC-4M-BUZHASH"}}'
exit 0
"#,
    );
    let client = client_for(&s);
    let v = client.policy_show("user@host:/d").await.unwrap();
    assert_eq!(v["compression"]["compressorName"], "zstd");
}
