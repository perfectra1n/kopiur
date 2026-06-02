//! Fake-kopia shim tests. We write a tiny shell script that mimics kopia's
//! stdout-JSON / stderr-progress split and a chosen exit code, point a
//! `KopiaClient` at it, and assert the client parses success and surfaces
//! errors correctly. This exercises the full subprocess path with NO real
//! kopia.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use kopiur_kopia::{ConnectSpec, KopiaClient, KopiaError, KopiaErrorClass};

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
        .snapshot_create("/data", &BTreeMap::new())
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
    let res = client.snapshot_create("/d", &tags).await.unwrap();
    assert_eq!(res.id, "t1");
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
        .snapshot_create("/nonexistent", &BTreeMap::new())
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
async fn auth_failure_classified() {
    let s = shim(
        r#"#!/bin/sh
echo "ERROR error connecting to repository: invalid repository password" 1>&2
exit 1
"#,
    );
    let client = client_for(&s);
    let err = client
        .repository_connect(&ConnectSpec::Filesystem {
            path: PathBuf::from("/repo"),
        })
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
