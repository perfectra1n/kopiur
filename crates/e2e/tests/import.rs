//! e2e: **importing an existing kopia repository** — repositories and snapshots
//! created by RAW kopia (never by kopiur), then adopted via a `Repository` /
//! `ClusterRepository` with `create.enabled: false`.
//!
//! These scenarios are the heart of "kopia-native": a repository written by a
//! previous cluster, a workstation, or plain `kopia` CLI must surface its
//! snapshots as `origin: discovered` `Snapshot` CRs with the FOREIGN identity
//! intact, restore byte-for-byte, never lose data when a discovered CR is
//! deleted (forced `Retain`), honor `catalog.retain` per identity, pick up
//! out-of-band snapshots on the `catalog.refreshInterval` cadence, and place
//! `ClusterRepository` discoveries in the namespace the identity hostname names.
//!
//! The foreign writer is `builders::foreign_kopia_pod` — raw `kopia` (the mover
//! image's binary) run as sequential initContainers against the e2e MinIO.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; driven by
//! `mise run //crates/e2e:test`. Skips gracefully without a cluster.

#![cfg(all(unix, feature = "e2e"))]

use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, ListParams, Patch, PatchParams, PostParams};
use kube::{Api, Client, ResourceExt};
use serde::de::DeserializeOwned;

use kopiur_api::{ClusterRepository, Repository, Restore, Snapshot};
use kopiur_e2e::builders::{self, SeedStep};
use kopiur_e2e::{
    E2E_NAMESPACE, Need, World, consts, default_timeout, poll_interval, wait, wait_until,
};

/// Deserialize a CR from a JSON literal into its typed kube object.
fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// An S3 `Repository` for the import scenarios: adopt-only by default
/// (`create.enabled` per `create`), managed maintenance OFF (the repo belongs to
/// a foreign writer — the import tests assert catalog behavior, not lease
/// takeover), optional `catalog` bounds.
fn import_repository_json(
    name: &str,
    bucket: &str,
    create: bool,
    catalog: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut spec = serde_json::json!({
        "backend": { "s3": {
            "bucket": bucket,
            "endpoint": consts::MINIO_ENDPOINT,
            "region": "us-east-1",
            "tls": { "disableTls": true },
            "auth": { "secretRef": { "name": consts::SECRET_S3_CREDS, "namespace": E2E_NAMESPACE } }
        }},
        "encryption": {
            "passwordSecretRef": { "name": consts::SECRET_S3_CREDS, "key": "KOPIA_PASSWORD" }
        },
        "create": { "enabled": create },
        "maintenance": { "enabled": false }
    });
    if let Some(c) = catalog {
        spec["catalog"] = c;
    }
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": spec
    })
}

/// Poll a CR until `status.phase == want_phase`.
async fn wait_phase<K>(api: &Api<K>, name: &str, want_phase: &str) -> anyhow::Result<()>
where
    K: kube::Resource + Clone + DeserializeOwned + serde::Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    wait_until(
        &format!("{name} phase={want_phase}"),
        default_timeout(),
        poll_interval(),
        || async {
            match api.get_opt(name).await? {
                Some(obj) => {
                    let v = serde_json::to_value(&obj).unwrap_or_default();
                    let phase = v
                        .get("status")
                        .and_then(|s| s.get("phase"))
                        .and_then(|p| p.as_str())
                        .unwrap_or("");
                    Ok((phase == want_phase).then_some(()))
                }
                None => Ok(None),
            }
        },
    )
    .await
}

/// Read a CR's `status` as JSON (or `null` if absent).
async fn status_json<K>(api: &Api<K>, name: &str) -> serde_json::Value
where
    K: kube::Resource + Clone + DeserializeOwned + serde::Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    match api.get_opt(name).await.ok().flatten() {
        Some(obj) => serde_json::to_value(&obj)
            .ok()
            .and_then(|v| v.get("status").cloned())
            .unwrap_or(serde_json::Value::Null),
        None => serde_json::Value::Null,
    }
}

/// Run a foreign seeder pod to completion (the shared
/// [`kopiur_e2e::apply::run_foreign_seeder`], expect-wrapped for test flow).
async fn run_seeder(client: &Client, name: &str, steps: &[SeedStep<'_>]) {
    kopiur_e2e::apply::run_foreign_seeder(client, E2E_NAMESPACE, name, steps)
        .await
        .expect("foreign kopia seeder");
}

/// This repository CR's discovered rows, via the dedup labels the catalog scan
/// stamps (`origin=discovered` + the repository UID).
async fn discovered_rows(client: &Client, ns: &str, repo_uid: &str) -> Vec<Snapshot> {
    let api: Api<Snapshot> = Api::namespaced(client.clone(), ns);
    let selector = format!(
        "kopiur.home-operations.com/origin=discovered,\
         kopiur.home-operations.com/repository-uid={repo_uid}"
    );
    api.list(&ListParams::default().labels(&selector))
        .await
        .expect("list discovered Snapshots")
        .items
}

/// Wait until this repository's `status.catalog.discoveredBackupCount` reaches
/// `want` exactly (counts can pass through intermediate values mid-scan).
async fn wait_discovered_count<K>(api: &Api<K>, name: &str, want: i64) -> anyhow::Result<()>
where
    K: kube::Resource + Clone + DeserializeOwned + serde::Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    wait_until(
        &format!("{name} discoveredBackupCount={want}"),
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(api, name).await;
            let n = s
                .get("catalog")
                .and_then(|c| c.get("discoveredBackupCount"))
                .and_then(|v| v.as_i64())
                .unwrap_or(-1);
            Ok((n == want).then_some(()))
        },
    )
    .await
}

/// The `username@hostname:path` identity recorded on a discovered Snapshot.
fn row_identity(s: &Snapshot) -> String {
    let v = serde_json::to_value(s).unwrap_or_default();
    let id = v
        .pointer("/status/snapshot/identity")
        .cloned()
        .unwrap_or_default();
    format!(
        "{}@{}:{}",
        id.get("username").and_then(|x| x.as_str()).unwrap_or(""),
        id.get("hostname").and_then(|x| x.as_str()).unwrap_or(""),
        id.get("sourcePath").and_then(|x| x.as_str()).unwrap_or(""),
    )
}

/// A discovered row's `status.timing.endTime` (RFC3339), for newest/oldest picks.
fn row_end_time(s: &Snapshot) -> String {
    serde_json::to_value(s)
        .unwrap_or_default()
        .pointer("/status/timing/endTime")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

/// The headline import scenario, end-to-end:
/// 1. RAW kopia seeds a repository in MinIO under TWO foreign identities —
///    `legacy@import-host:/data/app` (two snapshots: v1 then v2 content) and
///    `other@other-app:/data/cfg` (one snapshot). kopiur never saw any of it.
/// 2. A `Repository` with `create.enabled: false` adopts it → `Ready` +
///    "connected to the existing repository", and materializes EXACTLY 3
///    `origin: discovered` Snapshot CRs carrying the foreign identities, real
///    kopia ids, snapshot timing, and a FORCED `deletionPolicy: Retain`.
/// 3. A `Restore` from the OLDER discovered snapshot (snapshotRef → an
///    operator-created PVC) completes and the restored bytes are the v1
///    content — snapshot-id-precise, not just "some snapshot".
/// 4. Deleting a discovered Snapshot CR deletes ONLY the CR: a second adopting
///    `Repository` still discovers all 3 snapshots (the kopia data survived).
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn s3_foreign_import_adopt_discover_restore_and_retain_on_delete() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Minio]).await.expect("provision MinIO");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // 1. Foreign writer: repo + 3 snapshots under 2 identities, NO kopiur involved.
    run_seeder(
        &client,
        "e2e-import-seed",
        &[
            SeedStep::WipeBucket {
                bucket: "kopiur-import",
            },
            SeedStep::WriteFile {
                dir: "app",
                file: "hello.txt",
                content: "foreign-data-v1",
            },
            SeedStep::CreateRepo {
                bucket: "kopiur-import",
                username: "legacy",
                hostname: "import-host",
            },
            SeedStep::Snapshot { dir: "app" },
            SeedStep::WriteFile {
                dir: "app",
                file: "hello.txt",
                content: "foreign-data-v2",
            },
            SeedStep::Snapshot { dir: "app" },
            SeedStep::ConnectRepo {
                bucket: "kopiur-import",
                username: "other",
                hostname: "other-app",
            },
            SeedStep::WriteFile {
                dir: "cfg",
                file: "app.conf",
                content: "key equals value",
            },
            SeedStep::Snapshot { dir: "cfg" },
        ],
    )
    .await;

    // 2. Adopt (create: false) → Ready + the full discovered catalog.
    repos
        .create(
            &PostParams::default(),
            &cr(import_repository_json(
                "e2e-import",
                "kopiur-import",
                false,
                None,
            )),
        )
        .await
        .expect("create adopting Repository");
    wait_phase(&repos, "e2e-import", "Ready")
        .await
        .expect("adopting Repository should reach Ready (connect, no create)");
    wait_discovered_count(&repos, "e2e-import", 3)
        .await
        .expect("all 3 foreign snapshots should materialize as discovered Snapshots");

    let repo_uid = repos
        .get("e2e-import")
        .await
        .expect("get Repository")
        .uid()
        .expect("Repository has a uid");
    let rows = discovered_rows(&client, E2E_NAMESPACE, &repo_uid).await;
    assert_eq!(rows.len(), 3, "exactly one row per foreign snapshot");
    for row in &rows {
        let v = serde_json::to_value(row).unwrap();
        assert!(
            row.name_any().starts_with("e2e-import-disc-"),
            "row name is repo-scoped: {}",
            row.name_any()
        );
        assert_eq!(
            v.pointer("/spec/deletionPolicy").and_then(|x| x.as_str()),
            Some("Retain"),
            "discovered rows are FORCED Retain (the operator never deletes a \
             discovered kopia snapshot): {v}"
        );
        assert_eq!(
            v.pointer("/status/phase").and_then(|x| x.as_str()),
            Some("Discovered"),
            "{v}"
        );
        assert_eq!(
            v.pointer("/status/origin").and_then(|x| x.as_str()),
            Some("discovered"),
            "{v}"
        );
        assert!(
            v.pointer("/status/snapshot/kopiaSnapshotID")
                .and_then(|x| x.as_str())
                .is_some_and(|s| !s.is_empty()),
            "a discovered row carries the real kopia snapshot id: {v}"
        );
        assert!(
            !row_end_time(row).is_empty(),
            "a discovered row records the snapshot's timing: {v}"
        );
        // The repo CR owns its rows (GC on repo delete).
        assert!(
            v.pointer("/metadata/ownerReferences/0/kind")
                .and_then(|x| x.as_str())
                == Some("Repository"),
            "{v}"
        );
    }
    let mut identities: Vec<String> = rows.iter().map(row_identity).collect();
    identities.sort();
    assert_eq!(
        identities,
        vec![
            "legacy@import-host:/data/app".to_string(),
            "legacy@import-host:/data/app".to_string(),
            "other@other-app:/data/cfg".to_string(),
        ],
        "the FOREIGN identities are preserved verbatim"
    );

    // 3. Restore the OLDER legacy snapshot — must yield the v1 bytes.
    let mut legacy: Vec<&Snapshot> = rows
        .iter()
        .filter(|r| row_identity(r) == "legacy@import-host:/data/app")
        .collect();
    legacy.sort_by_key(|r| row_end_time(r));
    let older = legacy[0].name_any();
    restores
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Restore",
                "metadata": { "name": "e2e-import-restore", "namespace": E2E_NAMESPACE },
                "spec": {
                    "repository": { "kind": "Repository", "name": "e2e-import" },
                    "source": { "snapshotRef": { "name": older } },
                    "target": { "pvc": { "name": "e2e-import-restored", "capacity": "100Mi" } }
                }
            })),
        )
        .await
        .expect("create Restore from the discovered snapshot");
    wait_phase(&restores, "e2e-import-restore", "Completed")
        .await
        .expect("Restore from a discovered (foreign) snapshot should complete");

    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let reader = builders::one_shot_pod(
        E2E_NAMESPACE,
        "e2e-import-reader",
        &[
            "sh",
            "-c",
            // v1 exactly: restoring the OLDER snapshot must not yield v2.
            "test \"$(cat /restore/hello.txt)\" = foreign-data-v1",
        ],
        &[("e2e-import-restored", "/restore")],
    );
    pods.create(&PostParams::default(), &reader)
        .await
        .expect("create reader pod");
    wait::pod_succeeded(&client, E2E_NAMESPACE, "e2e-import-reader")
        .await
        .expect("restored PVC must hold the foreign v1 bytes, byte-exact");

    // 4. Deleting a discovered CR deletes ONLY the CR (forced Retain): a fresh
    //    adopting Repository still sees all 3 snapshots in the store.
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    backups
        .delete(&older, &DeleteParams::default())
        .await
        .expect("delete a discovered Snapshot CR");
    wait_until(
        "the discovered CR is fully deleted (finalizer released without touching kopia)",
        default_timeout(),
        poll_interval(),
        || async { Ok(backups.get_opt(&older).await?.is_none().then_some(())) },
    )
    .await
    .expect("discovered CR should delete cleanly");

    repos
        .create(
            &PostParams::default(),
            &cr(import_repository_json(
                "e2e-import-readopt",
                "kopiur-import",
                false,
                None,
            )),
        )
        .await
        .expect("create re-adopting Repository");
    wait_phase(&repos, "e2e-import-readopt", "Ready")
        .await
        .expect("re-adopting Repository should reach Ready");
    wait_discovered_count(&repos, "e2e-import-readopt", 3)
        .await
        .expect("all 3 kopia snapshots must still exist after the CR delete (Retain)");
}

/// `catalog.retain.perIdentity` bounds rows PER identity (not globally), and a
/// retain tightening expires over-cap rows without touching kopia:
/// 1. Foreign repo: identity A has 3 snapshots, identity B has 1.
/// 2. Adopt with `retain.perIdentity: 2` → 3 rows (A's 2 newest + B's 1) — a
///    global cap would have starved B (the old, pre-per-identity behavior).
/// 3. Tighten to `perIdentity: 1` (a spec change → the bootstrap recycles for a
///    fresh scan) → 2 rows, and A's surviving row is its NEWEST snapshot.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn catalog_retain_per_identity_bounds_and_expires_on_spec_change() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Minio]).await.expect("provision MinIO");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    run_seeder(
        &client,
        "e2e-import-retain-seed",
        &[
            SeedStep::WipeBucket {
                bucket: "kopiur-import-retain",
            },
            SeedStep::WriteFile {
                dir: "a",
                file: "f.txt",
                content: "a1",
            },
            SeedStep::CreateRepo {
                bucket: "kopiur-import-retain",
                username: "a",
                hostname: "host-a",
            },
            SeedStep::Snapshot { dir: "a" },
            SeedStep::WriteFile {
                dir: "a",
                file: "f.txt",
                content: "a2",
            },
            SeedStep::Snapshot { dir: "a" },
            SeedStep::WriteFile {
                dir: "a",
                file: "f.txt",
                content: "a3",
            },
            SeedStep::Snapshot { dir: "a" },
            SeedStep::ConnectRepo {
                bucket: "kopiur-import-retain",
                username: "b",
                hostname: "host-b",
            },
            SeedStep::WriteFile {
                dir: "b",
                file: "g.txt",
                content: "b1",
            },
            SeedStep::Snapshot { dir: "b" },
        ],
    )
    .await;

    repos
        .create(
            &PostParams::default(),
            &cr(import_repository_json(
                "e2e-import-retain",
                "kopiur-import-retain",
                false,
                Some(serde_json::json!({ "retain": { "perIdentity": 2 } })),
            )),
        )
        .await
        .expect("create adopting Repository with retain bounds");
    wait_phase(&repos, "e2e-import-retain", "Ready")
        .await
        .expect("Repository should adopt to Ready");
    // PER identity: 2 of A's 3, plus B's single one — 3 rows, not 2.
    wait_discovered_count(&repos, "e2e-import-retain", 3)
        .await
        .expect("perIdentity=2 keeps 2 of identity A + 1 of identity B (per-identity, not global)");

    let repo_uid = repos
        .get("e2e-import-retain")
        .await
        .expect("get Repository")
        .uid()
        .expect("uid");
    let rows = discovered_rows(&client, E2E_NAMESPACE, &repo_uid).await;
    let a_rows = rows
        .iter()
        .filter(|r| row_identity(r) == "a@host-a:/data/a")
        .count();
    let b_rows = rows
        .iter()
        .filter(|r| row_identity(r) == "b@host-b:/data/b")
        .count();
    assert_eq!((a_rows, b_rows), (2, 1), "2 of A + 1 of B");
    // A's newest two survived the cap; remember the newest for the tighten step.
    let newest_a_end = rows
        .iter()
        .filter(|r| row_identity(r) == "a@host-a:/data/a")
        .map(row_end_time)
        .max()
        .expect("A has rows");

    // Tighten to perIdentity: 1 — the spec change recycles the finished
    // bootstrap Job, the rescan expires the over-cap rows (CRs only).
    repos
        .patch(
            "e2e-import-retain",
            &PatchParams::default(),
            &Patch::Merge(serde_json::json!({
                "spec": { "catalog": { "retain": { "perIdentity": 1 } } }
            })),
        )
        .await
        .expect("tighten retain.perIdentity");
    wait_discovered_count(&repos, "e2e-import-retain", 2)
        .await
        .expect("tightening perIdentity to 1 should expire the over-cap rows");
    let rows = discovered_rows(&client, E2E_NAMESPACE, &repo_uid).await;
    let surviving_a: Vec<&Snapshot> = rows
        .iter()
        .filter(|r| row_identity(r) == "a@host-a:/data/a")
        .collect();
    assert_eq!(surviving_a.len(), 1, "one A row survives");
    assert_eq!(
        row_end_time(surviving_a[0]),
        newest_a_end,
        "the survivor is identity A's NEWEST snapshot"
    );
}

/// `catalog.refreshInterval`: a Ready repository keeps watching its repository —
/// and never duplicates its own produced snapshots as discovered rows:
/// 1. kopiur creates a fresh repo (`create: true`, `refreshInterval: 30s`) and
///    runs a normal scheduled-style backup through a SnapshotPolicy → the
///    repository's only snapshot is kopiur-produced.
/// 2. After a refresh cycle passes (the finished bootstrap Job is recycled for a
///    fresh listing), the produced snapshot must NOT appear as a discovered row
///    (`discoveredBackupCount == 0`) — the duplicate-on-rescan regression guard.
/// 3. A foreign writer then connects OUT-OF-BAND and snapshots under a new
///    identity → within the refresh cadence a discovered row appears, foreign
///    identity intact, without re-creating or disturbing the repository.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn catalog_refresh_discovers_out_of_band_snapshots_and_never_duplicates_produced() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::Filesystem])
        .await
        .expect("provision MinIO + source PVC");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<kopiur_api::SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // Make the bucket pristine so counts are exact on reused clusters.
    run_seeder(
        &client,
        "e2e-import-refresh-wipe",
        &[SeedStep::WipeBucket {
            bucket: "kopiur-import-refresh",
        }],
    )
    .await;

    repos
        .create(
            &PostParams::default(),
            &cr(import_repository_json(
                "e2e-import-refresh",
                "kopiur-import-refresh",
                true,
                Some(serde_json::json!({ "refreshInterval": "30s" })),
            )),
        )
        .await
        .expect("create Repository with a fast refresh");
    wait_phase(&repos, "e2e-import-refresh", "Ready")
        .await
        .expect("Repository should bootstrap to Ready");

    // 1. A kopiur-produced snapshot.
    policies
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "SnapshotPolicy",
                "metadata": { "name": "e2e-import-refresh-pol", "namespace": E2E_NAMESPACE },
                "spec": {
                    "repository": { "kind": "Repository", "name": "e2e-import-refresh" },
                    "sources": [ { "pvc": { "name": consts::PVC_SRC } } ],
                    "retention": { "keepLatest": 5 }
                }
            })),
        )
        .await
        .expect("create SnapshotPolicy");
    backups
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Snapshot",
                "metadata": { "name": "e2e-import-refresh-snap", "namespace": E2E_NAMESPACE },
                "spec": { "policyRef": { "name": "e2e-import-refresh-pol" } }
            })),
        )
        .await
        .expect("create Snapshot");
    wait_phase(&backups, "e2e-import-refresh-snap", "Succeeded")
        .await
        .expect("produced Snapshot should succeed");
    let produced_at = chrono::Utc::now();

    // 2. Wait for a refresh cycle that SAW the produced snapshot (lastRefreshAt
    //    after its completion), then assert it was not duplicated as discovered.
    //    On the pre-refresh code this wait times out (object-store repos never
    //    re-scanned); on the pre-dedup code the count comes back 1.
    wait_until(
        "a catalog refresh ran after the produced snapshot",
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(&repos, "e2e-import-refresh").await;
            let refreshed = s
                .pointer("/catalog/lastRefreshAt")
                .and_then(|v| v.as_str())
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .is_some_and(|t| t.with_timezone(&chrono::Utc) > produced_at);
            Ok(refreshed.then_some(()))
        },
    )
    .await
    .expect("catalog.refreshInterval=30s must re-scan after the backup");
    let s = status_json(&repos, "e2e-import-refresh").await;
    assert_eq!(
        s.pointer("/catalog/discoveredBackupCount")
            .and_then(|v| v.as_i64()),
        Some(0),
        "a kopiur-produced snapshot must NEVER come back as a discovered row: {s}"
    );

    // 3. Foreign out-of-band writer (connect, not create) under a new identity.
    run_seeder(
        &client,
        "e2e-import-refresh-seed",
        &[
            SeedStep::WriteFile {
                dir: "oob",
                file: "late.txt",
                content: "out-of-band",
            },
            SeedStep::ConnectRepo {
                bucket: "kopiur-import-refresh",
                username: "drifter",
                hostname: "elsewhere",
            },
            SeedStep::Snapshot { dir: "oob" },
        ],
    )
    .await;
    wait_discovered_count(&repos, "e2e-import-refresh", 1)
        .await
        .expect("the out-of-band snapshot should be discovered on the next refresh");
    let repo_uid = repos
        .get("e2e-import-refresh")
        .await
        .expect("get Repository")
        .uid()
        .expect("uid");
    let rows = discovered_rows(&client, E2E_NAMESPACE, &repo_uid).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        row_identity(&rows[0]),
        "drifter@elsewhere:/data/oob",
        "the out-of-band identity is preserved"
    );
    // The repository itself was refreshed, never re-created.
    let s = status_json(&repos, "e2e-import-refresh").await;
    assert_eq!(
        s.get("phase").and_then(|v| v.as_str()),
        Some("Ready"),
        "refresh cycles must not disturb readiness: {s}"
    );
}

/// `ClusterRepository` import: discovered snapshots are placed in the namespace
/// their identity hostname names (when it exists and passes `allowedNamespaces`),
/// else in `catalog.fallbackNamespace` (ADR §2.3):
/// - identity `app@<workload-ns>:/data/app` → the row lands IN the workload ns;
/// - identity `app@no-such-ns:/data/stray` → the row lands in the fallback ns.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn cluster_repository_places_discovered_snapshots_by_identity_hostname() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::WorkloadNs])
        .await
        .expect("provision MinIO + workload namespace");
    let client = world.client().clone();
    let crepos: Api<ClusterRepository> = Api::all(client.clone());

    run_seeder(
        &client,
        "e2e-import-crepo-seed",
        &[
            SeedStep::WipeBucket {
                bucket: "kopiur-import-crepo",
            },
            SeedStep::WriteFile {
                dir: "app",
                file: "f.txt",
                content: "in-workload-ns",
            },
            SeedStep::CreateRepo {
                bucket: "kopiur-import-crepo",
                username: "app",
                hostname: consts::WORKLOAD_NS,
            },
            SeedStep::Snapshot { dir: "app" },
            SeedStep::ConnectRepo {
                bucket: "kopiur-import-crepo",
                username: "app",
                hostname: "no-such-ns",
            },
            SeedStep::WriteFile {
                dir: "stray",
                file: "g.txt",
                content: "fallback-bound",
            },
            SeedStep::Snapshot { dir: "stray" },
        ],
    )
    .await;

    let name = "e2e-import-crepo";
    crepos
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "ClusterRepository",
                "metadata": { "name": name },
                "spec": {
                    "backend": { "s3": {
                        "bucket": "kopiur-import-crepo",
                        "endpoint": consts::MINIO_ENDPOINT,
                        "region": "us-east-1",
                        "tls": { "disableTls": true },
                        "auth": { "secretRef": {
                            "name": consts::SECRET_S3_CREDS, "namespace": E2E_NAMESPACE
                        } }
                    }},
                    "encryption": { "passwordSecretRef": {
                        "name": consts::SECRET_S3_CREDS,
                        "namespace": E2E_NAMESPACE,
                        "key": "KOPIA_PASSWORD"
                    }},
                    "create": { "enabled": false },
                    "maintenance": { "enabled": false },
                    "allowedNamespaces": { "list": [consts::WORKLOAD_NS] },
                    "catalog": { "fallbackNamespace": E2E_NAMESPACE }
                }
            })),
        )
        .await
        .expect("create adopting ClusterRepository");
    wait_phase(&crepos, name, "Ready")
        .await
        .expect("adopting ClusterRepository should reach Ready");
    wait_until(
        "both discovered rows are placed",
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(&crepos, name).await;
            let n = s
                .pointer("/catalog/discoveredBackupCount")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            Ok((n == 2).then_some(()))
        },
    )
    .await
    .expect("both foreign snapshots should be placed (identity ns + fallback)");

    let crepo_uid = crepos
        .get(name)
        .await
        .expect("get ClusterRepository")
        .uid()
        .expect("uid");
    // The allowed-namespace identity landed IN that namespace…
    let in_workload = discovered_rows(&client, consts::WORKLOAD_NS, &crepo_uid).await;
    assert_eq!(
        in_workload.len(),
        1,
        "the row for identity hostname={} lands in that namespace",
        consts::WORKLOAD_NS
    );
    assert_eq!(
        row_identity(&in_workload[0]),
        format!("app@{}:/data/app", consts::WORKLOAD_NS)
    );
    // …with the cluster-scoped owner (legal: namespaced dependent, cluster owner).
    let v = serde_json::to_value(&in_workload[0]).unwrap();
    assert_eq!(
        v.pointer("/metadata/ownerReferences/0/kind")
            .and_then(|x| x.as_str()),
        Some("ClusterRepository"),
        "{v}"
    );
    // The unknown-hostname identity fell back to catalog.fallbackNamespace.
    let in_fallback = discovered_rows(&client, E2E_NAMESPACE, &crepo_uid).await;
    assert_eq!(
        in_fallback.len(),
        1,
        "the row for an unknown identity hostname lands in catalog.fallbackNamespace"
    );
    assert_eq!(row_identity(&in_fallback[0]), "app@no-such-ns:/data/stray");

    // Best-effort cleanup (cluster-scoped objects persist across reused clusters).
    let _ = crepos.delete(name, &DeleteParams::default()).await;
}
