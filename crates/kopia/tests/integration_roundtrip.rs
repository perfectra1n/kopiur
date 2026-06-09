//! Real kopia filesystem round-trip integration test.
//!
//! Gated behind the `integration` feature and `#[ignore]` by default so the
//! hermetic `cargo test` never invokes the real binary. Run with:
//!
//! ```text
//! cargo test -p kopiur-kopia --features integration -- --ignored
//! ```
//!
//! It creates a filesystem repo in a tempdir, snapshots a tempdir with known
//! content, lists (asserts the snapshot appears), restores to another tempdir,
//! and asserts byte-identical content.

#![cfg(unix)]

use std::collections::BTreeMap;

use kopiur_kopia::{
    ConnectSpec, KopiaClient, MaintenanceMode, PolicyArgs, RestoreOptions, VerifyOptions,
};

/// Build a client whose env isolates kopia state inside `config_dir` so the
/// test never touches the user's real `~/.config/kopia`.
fn isolated_client(config_dir: &std::path::Path) -> KopiaClient {
    KopiaClient::builder()
        .binary("kopia")
        .env("KOPIA_PASSWORD", "test1234")
        .env(
            "KOPIA_CONFIG_PATH",
            config_dir.join("repository.config").display().to_string(),
        )
        .env(
            "KOPIA_CACHE_DIRECTORY",
            config_dir.join("cache").display().to_string(),
        )
        .env(
            "KOPIA_LOG_DIR",
            config_dir.join("logs").display().to_string(),
        )
        // Suppress the GitHub update check via env (it's a per-subcommand flag,
        // not a global one, so it can't go in common_args).
        .env("KOPIA_CHECK_FOR_UPDATES", "false")
        .build()
}

#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn filesystem_roundtrip() {
    let repo_dir = tempfile::tempdir().unwrap();
    let config_dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let restore_dir = tempfile::tempdir().unwrap();

    // Known content.
    std::fs::write(source_dir.path().join("a.txt"), b"hello kopiur\n").unwrap();
    std::fs::create_dir(source_dir.path().join("sub")).unwrap();
    std::fs::write(source_dir.path().join("sub/b.bin"), [0u8, 1, 2, 3, 255]).unwrap();

    let client = isolated_client(config_dir.path());

    // Create the repository.
    client
        .repository_create(
            &ConnectSpec::Filesystem {
                path: repo_dir.path().to_path_buf(),
            },
            Default::default(),
            &Default::default(),
        )
        .await
        .expect("repository create");

    // Snapshot with a tag.
    let mut tags = BTreeMap::new();
    tags.insert("test".to_string(), "roundtrip".to_string());
    let created = client
        .snapshot_create(
            source_dir.path().to_str().unwrap(),
            &tags,
            Some("testuser@testhost:/data"),
        )
        .await
        .expect("snapshot create");
    assert!(!created.id.is_empty());
    assert_eq!(
        created.source.user_name, "testuser",
        "snapshot recorded under the override identity, not the ambient user"
    );
    assert_eq!(created.source.host, "testhost");
    assert_eq!(created.file_count(), 2, "two files snapshotted");
    assert_eq!(created.total_bytes(), 13 + 5);

    // Repository status round-trips.
    let status = client.repository_status().await.expect("repo status");
    assert!(!status.unique_id_hex.is_empty());
    assert_eq!(status.storage.storage_type, "filesystem");

    // List shows the snapshot.
    let list = client.snapshot_list(None).await.expect("snapshot list");
    assert!(
        list.iter().any(|e| e.id == created.id),
        "created snapshot must appear in list"
    );
    let entry = list.iter().find(|e| e.id == created.id).unwrap();
    assert_eq!(entry.stats.file_count, 2);

    // Filtered list by the created snapshot's source identity also finds it.
    let filtered = client
        .snapshot_list(Some(&created.source))
        .await
        .expect("filtered list");
    assert!(filtered.iter().any(|e| e.id == created.id));

    // Maintenance info parses against the real repo.
    let info = client.maintenance_info().await.expect("maintenance info");
    assert!(!info.owner.is_empty());

    // Restore to a fresh dir.
    client
        .snapshot_restore(&created.id, restore_dir.path().to_str().unwrap())
        .await
        .expect("snapshot restore");

    // Byte-identical assertions.
    let a = std::fs::read(restore_dir.path().join("a.txt")).expect("a.txt restored");
    assert_eq!(a, b"hello kopiur\n");
    let b = std::fs::read(restore_dir.path().join("sub/b.bin")).expect("b.bin restored");
    assert_eq!(b, &[0u8, 1, 2, 3, 255]);

    // A quick maintenance pass succeeds.
    client
        .maintenance_run(MaintenanceMode::Quick)
        .await
        .expect("quick maintenance");

    // Delete the snapshot, then confirm it's gone from the list.
    client
        .snapshot_delete(&created.id)
        .await
        .expect("snapshot delete");
    let after = client.snapshot_list(None).await.expect("list after delete");
    assert!(
        !after.iter().any(|e| e.id == created.id),
        "deleted snapshot must not appear"
    );
}

/// Exercises the broadened verb surface (policy, verify, estimate, pin, restore
/// options, validate-provider) against a real filesystem repo. Proves the args
/// we build are accepted by kopia 0.23, not just shaped correctly.
#[tokio::test]
#[cfg_attr(not(feature = "integration"), ignore)]
async fn verbs_roundtrip() {
    let repo_dir = tempfile::tempdir().unwrap();
    let config_dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let restore_dir = tempfile::tempdir().unwrap();

    std::fs::write(source_dir.path().join("keep.txt"), b"keep me\n").unwrap();
    std::fs::write(source_dir.path().join("skip.tmp"), b"scratch\n").unwrap();

    let client = isolated_client(config_dir.path());
    client
        .repository_create(
            &ConnectSpec::Filesystem {
                path: repo_dir.path().to_path_buf(),
            },
            Default::default(),
            &Default::default(),
        )
        .await
        .expect("repository create");

    // validate-provider preflight succeeds against a freshly created repo.
    client
        .repository_validate_provider()
        .await
        .expect("validate-provider");

    let identity = "verbuser@verbhost:/data";

    // Apply a policy (compression + ignore glob) before snapshotting.
    client
        .policy_set(
            identity,
            &PolicyArgs {
                compression: Some("zstd".into()),
                ignore: vec!["*.tmp".into()],
                ..Default::default()
            },
        )
        .await
        .expect("policy set");

    // policy show reflects the compression we set.
    let shown = client.policy_show(identity).await.expect("policy show");
    assert!(
        shown.to_string().contains("zstd"),
        "policy show should reflect zstd compression, got {shown}"
    );

    // Estimate runs cleanly.
    client
        .snapshot_estimate(source_dir.path().to_str().unwrap())
        .await
        .expect("snapshot estimate");

    // Snapshot; the ignore policy should drop skip.tmp (1 file, not 2).
    let created = client
        .snapshot_create(
            source_dir.path().to_str().unwrap(),
            &BTreeMap::new(),
            Some(identity),
        )
        .await
        .expect("snapshot create");
    assert_eq!(
        created.file_count(),
        1,
        "ignore policy should exclude *.tmp"
    );

    // Verify integrity (read 100% of files).
    client
        .snapshot_verify(&VerifyOptions {
            verify_files_percent: Some(100),
            ..Default::default()
        })
        .await
        .expect("verify");

    // Restore honoring options (atomic writes, ignore permission errors).
    client
        .snapshot_restore_with(
            &created.id,
            restore_dir.path().to_str().unwrap(),
            &RestoreOptions {
                ignore_permission_errors: Some(true),
                write_files_atomically: Some(true),
                ..Default::default()
            },
        )
        .await
        .expect("restore with options");

    // Pin then unpin the snapshot (protects it from expiry). Pinning rewrites the
    // manifest, so the manifest id changes — re-list to get the current id before
    // unpinning, mirroring what a reconciler must do.
    client
        .snapshot_pin(&created.id, "protected")
        .await
        .expect("pin");
    let pinned = client
        .snapshot_list(Some(&created.source))
        .await
        .expect("list after pin");
    let current_id = pinned
        .first()
        .map(|e| e.id.clone())
        .expect("snapshot still present after pin");
    client
        .snapshot_unpin(&current_id, "protected")
        .await
        .expect("unpin");
    let kept = std::fs::read(restore_dir.path().join("keep.txt")).expect("keep.txt restored");
    assert_eq!(kept, b"keep me\n");
    assert!(
        !restore_dir.path().join("skip.tmp").exists(),
        "ignored file should not be in the snapshot/restore"
    );
}
