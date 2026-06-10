//! End-to-end guards for two production bugs that the prior harness masked.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`, skipping gracefully without
//! a cluster (`mise run //crates/e2e:test`). Run with `mise run //crates/e2e:test`.
//!
//! 1. **Writable kopia cache (the `/nonexistent` bug).** The controller runs
//!    short kopia ops in-process. kopia defaults its cache/logs/config under
//!    `$HOME`, which is `/nonexistent` on distroless:nonroot with a read-only
//!    rootfs, so every invocation used to spew `mkdir /nonexistent: read-only
//!    file system` and could never persist its repository config — filesystem
//!    repos never bootstrapped in production. The fix mounts a writable
//!    `kopia-cache` emptyDir from the *chart* and points kopia's `KOPIA_*` env at
//!    it from the *binary*. The harness no longer sets any manual cache
//!    workaround, so a filesystem `Repository` reaching `Ready` with **no**
//!    cache errors in the controller log proves the chart fix is effective.
//!
//! 2. **Event note ≤ 1024 bytes (the 422 bug).** A backend-failure Warning Event
//!    embeds kopia's stderr; an oversized note was rejected by the apiserver with
//!    `can have at most 1024 characters`, so the actionable warning never landed.
//!    A failing `Repository` must now actually publish a Warning Event whose note
//!    is within the limit — its mere presence proves the apiserver accepted it.

#![cfg(all(unix, feature = "e2e"))]

use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::events::v1::Event;
use kube::Api;
use kube::api::{ListParams, LogParams, PostParams};
use serde::de::DeserializeOwned;

use kopiur_api::{Maintenance, Repository};
use kopiur_e2e::{E2E_NAMESPACE, Need, World, default_timeout, poll_interval, wait_until};

/// The Kubernetes Event `note` byte limit the apiserver enforces.
const EVENT_NOTE_MAX_BYTES: usize = 1024;

/// Substrings that appear ONLY when kopia can't write its cache/logs/config —
/// the signature of the `/nonexistent` read-only-rootfs bug.
const CACHE_ERROR_SIGNATURES: &[&str] = &[
    "/nonexistent",
    "read-only file system",
    "unable to open log file",
    "Unable to create logs directory",
    "Unable to create cache marker",
];

/// Deserialize a CR from a JSON literal into its typed kube object.
fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// A filesystem `Repository` (create-on-first-use) backed by the harness's
/// hostPath repo PVC, which the controller mounts at `/repo` for in-process ops.
fn fs_repository_json(name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "filesystem": { "path": "/repo", "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
            "encryption": {
                "passwordSecretRef": { "name": "kopia-creds", "key": "KOPIA_PASSWORD" }
            },
            "create": { "enabled": true }
        }
    })
}

/// A connect-only S3 `Repository` (`create: false`) pointing at an *empty,
/// uninitialized* bucket. The bootstrap connect must fail ("repository not
/// initialized"), driving the backend-failure Event path.
fn s3_connect_only_repository_json(name: &str, bucket: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "s3": {
                "bucket": bucket,
                "endpoint": "minio.kopiur-e2e.svc.cluster.local:9000",
                "region": "us-east-1",
                "tls": { "disableTls": true },
                "auth": { "secretRef": { "name": "kopia-s3-creds", "namespace": E2E_NAMESPACE } }
            }},
            "encryption": {
                "passwordSecretRef": { "name": "kopia-s3-creds", "key": "KOPIA_PASSWORD" }
            },
            "create": { "enabled": false }
        }
    })
}

/// Poll a namespaced CR until `status.phase == want_phase`.
async fn wait_phase(api: &Api<Repository>, name: &str, want_phase: &str) -> anyhow::Result<()> {
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

/// Read the logs of the first non-terminating pod matching `selector`.
async fn pod_logs_for(
    client: &kube::Client,
    selector: &str,
) -> Result<Option<String>, kube::Error> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let list = pods.list(&ListParams::default().labels(selector)).await?;
    let Some(name) = list
        .items
        .into_iter()
        .filter(|p| p.metadata.deletion_timestamp.is_none())
        .find_map(|p| p.metadata.name)
    else {
        return Ok(None);
    };
    Ok(Some(pods.logs(&name, &LogParams::default()).await?))
}

/// Regression guard for the `/nonexistent` cache bug: with NO manual cache
/// workaround in the harness, a filesystem `Repository` must still bootstrap to
/// `Ready` (the controller's in-process kopia connect/create needs a writable
/// cache + config), and the controller log must be free of every cache-error
/// signature. Before the fix the connect failed and the log was full of
/// `mkdir /nonexistent: read-only file system`.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn controller_kopia_has_writable_cache_and_no_nonexistent_errors() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("provision filesystem fixtures");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // The in-process filesystem connect+create only succeeds if kopia has a
    // writable cache/log/config — provided by the chart's kopia-cache emptyDir.
    repos
        .create(
            &PostParams::default(),
            &cr(fs_repository_json("e2e-cache-repo")),
        )
        .await
        .expect("create filesystem Repository");
    wait_phase(&repos, "e2e-cache-repo", "Ready")
        .await
        .expect("filesystem Repository should reach Ready (proves kopia has a writable cache)");

    // The controller log (cumulative across its lifetime) must carry none of the
    // cache-error signatures — for ANY in-process kopia op, not just this repo.
    let logs = pod_logs_for(&client, "app.kubernetes.io/component=controller")
        .await
        .expect("read controller logs")
        .expect("a controller pod should exist");
    for sig in CACHE_ERROR_SIGNATURES {
        assert!(
            !logs.contains(sig),
            "controller log contains the kopia cache-error signature {sig:?} — the writable \
             cache fix regressed. Offending log:\n{logs}"
        );
    }
}

/// Regression guard for the 1024-byte Event 422: a failing `Repository` must
/// publish a Warning Event whose note is within the limit. The Event existing in
/// the API at all proves the apiserver accepted it (an oversized note would have
/// been rejected, exactly the original bug), and we additionally assert the
/// note's length and that it carries a machine-readable reason.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn backend_failure_publishes_a_bounded_warning_event() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio])
        .await
        .expect("provision MinIO + buckets");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let events: Api<Event> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // Connect-only against the empty `kopiur-guard` bucket → "not initialized".
    let repo = "e2e-evt-fail";
    repos
        .create(
            &PostParams::default(),
            &cr(s3_connect_only_repository_json(repo, "kopiur-guard")),
        )
        .await
        .expect("create connect-only S3 Repository");

    // A Warning Event regarding this Repository must appear (the publish path).
    let ev = wait_until(
        "a Warning Event is published for the failing Repository",
        default_timeout(),
        poll_interval(),
        || async {
            let list = events.list(&ListParams::default()).await?;
            let found = list.items.into_iter().find(|e| {
                e.type_.as_deref() == Some("Warning")
                    && e.regarding.as_ref().is_some_and(|r| {
                        r.kind.as_deref() == Some("Repository") && r.name.as_deref() == Some(repo)
                    })
            });
            Ok(found)
        },
    )
    .await
    .expect(
        "the backend-failure Warning Event must be published (regression: 422 on a >1024B note)",
    );

    let note = ev.note.unwrap_or_default();
    assert!(
        !note.is_empty(),
        "the published Event must carry a human-readable note"
    );
    assert!(
        note.len() <= EVENT_NOTE_MAX_BYTES,
        "Event note is {} bytes, exceeds the {EVENT_NOTE_MAX_BYTES}-byte apiserver limit: {note}",
        note.len()
    );
    // The connect found no repository and `create.enabled` is off, so the failure
    // is the actionable `RepositoryNotInitialized` (a kopiur create-policy outcome),
    // NOT a bare kopia `NotFound`. The reason is machine-readable and the note tells
    // the operator exactly which field to flip.
    assert_eq!(
        ev.reason.as_deref(),
        Some("RepositoryNotInitialized"),
        "an uninitialized repo with create disabled must surface RepositoryNotInitialized, got {:?}",
        ev.reason
    );
    assert!(
        note.contains("spec.create.enabled: true"),
        "the note must tell the operator how to initialize the repo, got: {note}"
    );
}

/// Every reconcile failure — for EVERY kind — must surface as a Warning Event on
/// the failing object (`error_policy_for` → `reconcile_failure_event`), and
/// repeats of the same failure must aggregate into ONE Event object via the
/// Recorder series instead of flooding `kubectl get events`.
///
/// Regression guard: `Maintenance` (like SnapshotSchedule/SnapshotPolicy/
/// RepositoryReplication) used to emit NO Events at all — a Maintenance pointing
/// at a missing Repository failed silently into the controller log, invisible to
/// `kubectl get events`. Pre-fix this test times out waiting for the Event.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn reconcile_failure_publishes_an_aggregated_missing_dependency_event() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("provision base fixtures");
    let client = world.client().clone();
    let maints: Api<Maintenance> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let events: Api<Event> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // A Maintenance whose Repository does not exist → reconcile fails with
    // MissingDependency on the transient (30 s) cadence.
    let name = "e2e-evt-missing-dep";
    maints
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Maintenance",
                "metadata": { "name": name, "namespace": E2E_NAMESPACE },
                "spec": {
                    "repository": { "kind": "Repository", "name": "does-not-exist" },
                    "schedule": { "quick": { "cron": "*/5 * * * *" }, "full": { "cron": "0 3 * * 0" } },
                    "ownership": { "owner": "kopiur-e2e-evt", "takeoverPolicy": "Force" }
                }
            })),
        )
        .await
        .expect("create Maintenance referencing a nonexistent Repository");

    let matching = |list: Vec<Event>| -> Vec<Event> {
        list.into_iter()
            .filter(|e| {
                e.type_.as_deref() == Some("Warning")
                    && e.reason.as_deref() == Some("MissingDependency")
                    && e.regarding.as_ref().is_some_and(|r| {
                        r.kind.as_deref() == Some("Maintenance") && r.name.as_deref() == Some(name)
                    })
            })
            .collect()
    };

    // 1) The Event appears at all (pre-fix: never — Maintenance emitted nothing).
    let ev = wait_until(
        "a MissingDependency Warning Event regarding the Maintenance",
        default_timeout(),
        poll_interval(),
        || async {
            let list = events.list(&ListParams::default()).await?;
            Ok(matching(list.items).into_iter().next())
        },
    )
    .await
    .expect("a failing Maintenance reconcile must publish a MissingDependency Warning Event");

    // The note is the actionable what/why/fix message, within the apiserver cap.
    let note = ev.note.unwrap_or_default();
    assert!(
        note.contains("does-not-exist") && note.contains("create it, or fix the reference"),
        "the note must name the missing dependency and the fix: {note}"
    );
    assert!(note.len() <= EVENT_NOTE_MAX_BYTES);

    // 2) Repeats AGGREGATE: the 30 s transient requeue re-fails; within the
    // Recorder's dedup window that must become `series.count >= 2` on the SAME
    // single Event object — not a new object per retry (the resourceVersion-in-
    // the-dedup-key footgun event_ref guards against).
    wait_until(
        "the MissingDependency Event aggregates as a series (count >= 2)",
        default_timeout(),
        poll_interval(),
        || async {
            let list = events.list(&ListParams::default()).await?;
            let found = matching(list.items)
                .iter()
                .any(|e| e.series.as_ref().is_some_and(|s| s.count >= 2));
            Ok(found.then_some(()))
        },
    )
    .await
    .expect("repeated MissingDependency failures must aggregate into an Event series");

    let all = matching(
        events
            .list(&ListParams::default())
            .await
            .expect("list events")
            .items,
    );
    assert_eq!(
        all.len(),
        1,
        "repeated identical reconcile failures must aggregate into exactly ONE \
         Event object, got {}: a churning dedup key would flood kubectl get events",
        all.len()
    );
}
