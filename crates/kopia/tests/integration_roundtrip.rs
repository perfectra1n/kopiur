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

use kopiur_kopia::{ConnectSpec, KopiaClient, MaintenanceMode};

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
        .repository_create(&ConnectSpec::Filesystem {
            path: repo_dir.path().to_path_buf(),
        })
        .await
        .expect("repository create");

    // Snapshot with a tag.
    let mut tags = BTreeMap::new();
    tags.insert("test".to_string(), "roundtrip".to_string());
    let created = client
        .snapshot_create(source_dir.path().to_str().unwrap(), &tags)
        .await
        .expect("snapshot create");
    assert!(!created.id.is_empty());
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
