//! e2e regression guards for the **TTL-reap re-run loop**: mover Jobs self-reap
//! via `ttlSecondsAfterFinished`, and a reconciler that keys "the work is done"
//! on the Job's *existence* (ephemeral) instead of the CR's *status* (durable)
//! re-creates the Job — and re-executes the work — after every reap, forever.
//!
//! Found live: every Snapshot in a real cluster re-ran its full backup every
//! ~61 minutes (TTL 3600 + reconcile latency); a foreign-owned repository's
//! Maintenance respawned a yield Job on the same year-old cron slot each hour;
//! and a Job-TTL shorter than `catalog.refreshInterval` would silently pin a
//! repository's re-scan cadence to the TTL.
//!
//! Each scenario drives the real pipeline with a tiny `ttlSecondsAfterFinished`
//! (5s) so the kube TTL controller reaps the finished Job in-test, then asserts
//! the Job **stays gone** and the status stays byte-identical. On the buggy
//! code every one of these fails: the Job reappears within seconds of the reap
//! (Snapshot owns its Job, so the deletion event itself re-triggered it) or of
//! the explicit reconcile pokes (Repository/Maintenance ride requeues).
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; driven by
//! `mise run //crates/e2e:test`. Skips gracefully without a cluster.

#![cfg(all(unix, feature = "e2e"))]

use std::time::Duration;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::{Api, Client, ResourceExt};
use serde::de::DeserializeOwned;

use kopiur_api::{Maintenance, Repository, Snapshot, SnapshotPolicy};
use kopiur_e2e::builders::{self, SeedStep};
use kopiur_e2e::{
    E2E_NAMESPACE, Need, World, consts, default_timeout, poll_interval, wait, wait_until,
};

/// Deserialize a CR from a JSON literal into its typed kube object.
fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// The repository password Secret the chart-installed operator reads.
const CREDS_SECRET: &str = "kopia-creds";

/// How long the mover Jobs in these scenarios linger after finishing. Small so
/// the kube TTL controller reaps them in-test; large enough that the controller
/// reliably observes the terminal state first.
const JOB_TTL_SECONDS: i64 = 5;

/// How long a reaped Job must STAY gone. On the buggy code the re-create
/// happened within seconds of the deletion event / reconcile poke, so this
/// window is generous.
const QUIET_WINDOW: Duration = Duration::from_secs(45);

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

/// Wait until the Job named `name` is GONE (the kube TTL controller reaped it).
async fn wait_job_reaped(jobs: &Api<Job>, name: &str) {
    wait_until(
        &format!("Job {name} TTL-reaped"),
        default_timeout(),
        poll_interval(),
        || async { Ok(jobs.get_opt(name).await?.is_none().then_some(())) },
    )
    .await
    .expect("finished Job should be reaped by its ttlSecondsAfterFinished");
}

/// Assert the Job named exactly `name` does not exist at any point during
/// [`QUIET_WINDOW`]. THE regression assertion: on the buggy code the reaped Job
/// reappears here (a re-run always reuses the deterministic Job name).
async fn assert_job_stays_gone(jobs: &Api<Job>, name: &str, while_doing: &str) {
    let deadline = tokio::time::Instant::now() + QUIET_WINDOW;
    while tokio::time::Instant::now() < deadline {
        if let Some(j) = jobs.get_opt(name).await.expect("get Job") {
            panic!(
                "TTL-rerun regression while {while_doing}: Job {} was re-created at {:?} \
                 after its predecessor was TTL-reaped",
                j.name_any(),
                j.metadata.creation_timestamp
            );
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Assert no Job whose name starts with `prefix` exists at any point during
/// [`QUIET_WINDOW`] — for per-slot Job families whose exact slot suffix isn't
/// known up front (Maintenance). Pick a prefix no other Job in the namespace
/// shares (a repository's `<name>-bootstrap` is the classic collision).
async fn assert_no_job_with_prefix(jobs: &Api<Job>, prefix: &str, while_doing: &str) {
    let deadline = tokio::time::Instant::now() + QUIET_WINDOW;
    while tokio::time::Instant::now() < deadline {
        let list = jobs
            .list(&Default::default())
            .await
            .expect("list Jobs in the e2e namespace");
        if let Some(j) = list.items.iter().find(|j| j.name_any().starts_with(prefix)) {
            panic!(
                "TTL-rerun regression while {while_doing}: Job {} was re-created at {:?} \
                 after its predecessor was TTL-reaped",
                j.name_any(),
                j.metadata.creation_timestamp
            );
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Force a reconcile of a CR by bumping a metadata annotation (no `generation`
/// change). Repository/Maintenance do not `.owns()` their Jobs, so the TTL reap
/// does not wake them — on the buggy code it is the NEXT reconcile that
/// re-creates the Job, and this poke makes that deterministic and immediate.
async fn poke<K>(api: &Api<K>, name: &str, nonce: &str)
where
    K: kube::Resource + Clone + DeserializeOwned + serde::Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    let patch = serde_json::json!({
        "metadata": { "annotations": { "kopiur-e2e/reconcile-poke": nonce } }
    });
    api.patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .expect("poke CR with an annotation to force a reconcile");
}

/// Delete any leftover CR of the same name and wait it out, then create fresh —
/// a reused cluster must not 409 (and a leftover already-terminal CR would make
/// the scenario vacuous). Mirrors `run_seeder`'s leftover handling.
async fn recreate<K>(api: &Api<K>, obj: &K)
where
    K: kube::Resource + Clone + DeserializeOwned + serde::Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    let name = obj.meta().name.clone().expect("CR has a name");
    if api
        .get_opt(&name)
        .await
        .expect("query leftover CR")
        .is_some()
    {
        let _ = api.delete(&name, &DeleteParams::default()).await;
        wait_until(
            &format!("leftover {name} is gone"),
            default_timeout(),
            poll_interval(),
            || async { Ok(api.get_opt(&name).await?.is_none().then_some(())) },
        )
        .await
        .expect("leftover CR should delete (finalizers included)");
    }
    api.create(&PostParams::default(), obj)
        .await
        .expect("create CR");
}

// ---------------------------------------------------------------------------
// 1. Snapshot: a Succeeded backup must never re-run.
// ---------------------------------------------------------------------------

/// The live-cluster bug verbatim: a `Succeeded` Snapshot's mover Job is reaped
/// by its TTL, the deletion event re-triggers the reconciler (Snapshot `.owns()`
/// its Job), and the buggy reconciler — keying on Job existence, not phase —
/// minted a fresh Job and re-ran the whole backup. Every TTL period. Forever.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn succeeded_snapshot_is_not_rerun_after_its_job_is_ttl_reaped() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("provision filesystem fixtures");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": "e2e-ttl-repo", "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "filesystem": { "path": "/repo", "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
            "encryption": { "passwordSecretRef": { "name": CREDS_SECRET, "key": "KOPIA_PASSWORD" } },
            "create": { "enabled": true }
        }
    });
    let _ = repos.create(&PostParams::default(), &cr(repo)).await;
    wait_phase(&repos, "e2e-ttl-repo", "Ready")
        .await
        .expect("repository should reach Ready");

    let cfg = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "SnapshotPolicy",
        "metadata": { "name": "e2e-ttl-cfg", "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": "e2e-ttl-repo" },
            "sources": [ { "pvc": { "name": "e2e-src" } } ],
            "retention": { "keepLatest": 5 },
            // Tiny TTL: the finished mover Job is reaped in-test.
            "mover": { "ttlSecondsAfterFinished": JOB_TTL_SECONDS }
        }
    });
    let _ = configs.create(&PostParams::default(), &cr(cfg)).await;

    let backup = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Snapshot",
        "metadata": { "name": "e2e-ttl-backup", "namespace": E2E_NAMESPACE },
        "spec": { "policyRef": { "name": "e2e-ttl-cfg" }, "deletionPolicy": "Retain" }
    });
    recreate(&backups, &cr(backup)).await;
    wait_phase(&backups, "e2e-ttl-backup", "Succeeded")
        .await
        .expect("Snapshot should reach Succeeded");

    // Pin the result the re-run would overwrite.
    let before = status_json(&backups, "e2e-ttl-backup").await;
    let snapshot_id = before
        .pointer("/snapshot/kopiaSnapshotID")
        .and_then(|v| v.as_str())
        .expect("Succeeded Snapshot must carry a kopia snapshot id")
        .to_string();
    let end_time = before
        .pointer("/timing/endTime")
        .and_then(|v| v.as_str())
        .expect("Succeeded Snapshot must carry timing.endTime")
        .to_string();

    // The TTL reaps the finished Job; the deletion event hits the reconciler.
    wait_job_reaped(&jobs, "e2e-ttl-backup").await;

    // THE assertion: the backup must NOT run again.
    assert_job_stays_gone(
        &jobs,
        "e2e-ttl-backup",
        "waiting after the Snapshot Job reap",
    )
    .await;

    let after = status_json(&backups, "e2e-ttl-backup").await;
    assert_eq!(
        after.get("phase").and_then(|p| p.as_str()),
        Some("Succeeded"),
        "phase must stay Succeeded after the Job reap"
    );
    assert_eq!(
        after
            .pointer("/snapshot/kopiaSnapshotID")
            .and_then(|v| v.as_str()),
        Some(snapshot_id.as_str()),
        "kopiaSnapshotID must not be overwritten by a re-run"
    );
    assert_eq!(
        after.pointer("/timing/endTime").and_then(|v| v.as_str()),
        Some(end_time.as_str()),
        "timing.endTime must not move (a re-run rewrites it)"
    );
}

// ---------------------------------------------------------------------------
// 2. Snapshot: a Failed backup is terminal — no TTL-driven retry loop.
// ---------------------------------------------------------------------------

/// `Failed` is terminal until the spec changes (ADR: Failed → kstatus Stalled).
/// On the buggy code the reaped Job was re-created and the doomed backup
/// re-ran — and re-failed — every TTL period.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn failed_snapshot_is_not_retried_after_its_job_is_ttl_reaped() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("provision filesystem fixtures");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": "e2e-ttl-fail-repo", "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "filesystem": { "path": "/repo", "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
            "encryption": { "passwordSecretRef": { "name": CREDS_SECRET, "key": "KOPIA_PASSWORD" } },
            "create": { "enabled": true }
        }
    });
    let _ = repos.create(&PostParams::default(), &cr(repo)).await;
    wait_phase(&repos, "e2e-ttl-fail-repo", "Ready")
        .await
        .expect("repository should reach Ready");

    // Deterministic mover failure: an unknown kopia flag (terminal_failure.rs's
    // injection), one attempt only.
    let cfg = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "SnapshotPolicy",
        "metadata": { "name": "e2e-ttl-fail-cfg", "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": "e2e-ttl-fail-repo" },
            "sources": [ { "pvc": { "name": "e2e-src" } } ],
            "extraArgs": ["--kopiur-e2e-bogus-flag"],
            "mover": { "ttlSecondsAfterFinished": JOB_TTL_SECONDS }
        }
    });
    let _ = configs.create(&PostParams::default(), &cr(cfg)).await;

    let backup = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Snapshot",
        "metadata": { "name": "e2e-ttl-fail", "namespace": E2E_NAMESPACE },
        "spec": {
            "policyRef": { "name": "e2e-ttl-fail-cfg" },
            "deletionPolicy": "Retain",
            "failurePolicy": { "backoffLimit": 0 }
        }
    });
    recreate(&backups, &cr(backup)).await;
    wait_phase(&backups, "e2e-ttl-fail", "Failed")
        .await
        .expect("poisoned Snapshot should end Failed");

    wait_job_reaped(&jobs, "e2e-ttl-fail").await;
    assert_job_stays_gone(
        &jobs,
        "e2e-ttl-fail",
        "waiting after the failed-Snapshot Job reap",
    )
    .await;

    let after = status_json(&backups, "e2e-ttl-fail").await;
    assert_eq!(
        after.get("phase").and_then(|p| p.as_str()),
        Some("Failed"),
        "phase must stay Failed — a terminal failure is not a retry loop"
    );
}

// ---------------------------------------------------------------------------
// 3. Repository: the Job TTL must not override catalog.refreshInterval.
// ---------------------------------------------------------------------------

/// A Ready, mover-bootstrapped Repository re-scans its catalog by *recycling*
/// the finished bootstrap Job on the `catalog.refreshInterval` cadence — but
/// when the kube TTL reaps that Job first, the buggy no-Job path re-created it
/// unconditionally, pinning the re-scan cadence to the TTL instead of the
/// configured interval. A spec change must still re-bootstrap immediately.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn ready_repository_is_not_rebootstrapped_after_its_job_is_ttl_reaped() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Filesystem])
        .await
        .expect("provision filesystem fixtures");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": "e2e-ttl-boot", "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "filesystem": { "path": "/repo", "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
            "encryption": { "passwordSecretRef": { "name": CREDS_SECRET, "key": "KOPIA_PASSWORD" } },
            "create": { "enabled": true },
            // Tiny Job TTL vs the default 1h refreshInterval: the reap fires
            // long before a re-scan is due.
            "moverDefaults": { "ttlSecondsAfterFinished": JOB_TTL_SECONDS }
        }
    });
    recreate(&repos, &cr(repo)).await;
    wait_phase(&repos, "e2e-ttl-boot", "Ready")
        .await
        .expect("repository should reach Ready");

    wait_job_reaped(&jobs, "e2e-ttl-boot-bootstrap").await;

    // The Repository does not own its bootstrap Job, so the reap does not wake
    // it — force reconciles the way any watch event would. On the buggy code
    // each poke re-created the bootstrap Job within seconds.
    poke(&repos, "e2e-ttl-boot", "1").await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    poke(&repos, "e2e-ttl-boot", "2").await;
    assert_job_stays_gone(
        &jobs,
        "e2e-ttl-boot-bootstrap",
        "poking a Ready Repository whose refresh is not due",
    )
    .await;

    let after = status_json(&repos, "e2e-ttl-boot").await;
    assert_eq!(
        after.get("phase").and_then(|p| p.as_str()),
        Some("Ready"),
        "phase must stay Ready with no Job present"
    );

    // Counter-assert: a SPEC change (generation bump) must still re-bootstrap —
    // the gate holds only while nothing changed and no refresh is due.
    let patch = serde_json::json!({ "spec": { "catalog": { "refreshInterval": "45m" } } });
    repos
        .patch(
            "e2e-ttl-boot",
            &PatchParams::default(),
            &Patch::Merge(&patch),
        )
        .await
        .expect("patch repository spec.catalog");
    wait_until(
        "spec change re-creates the bootstrap Job",
        default_timeout(),
        poll_interval(),
        || async {
            Ok(jobs
                .get_opt("e2e-ttl-boot-bootstrap")
                .await?
                .is_some()
                .then_some(()))
        },
    )
    .await
    .expect("a generation bump must re-run the bootstrap");
}

// ---------------------------------------------------------------------------
// 4. Maintenance: a yielded slot is handled — it must not respawn every TTL.
// ---------------------------------------------------------------------------

/// A repository created by a FOREIGN kopia writer is maintenance-owned by that
/// foreign identity; with `takeoverPolicy: Never` every kopiur maintenance run
/// *yields* (correct) — but a yield never advances `lastRunAt`, so on the buggy
/// code the same (year-old, first-ever) cron slot stayed due forever and a
/// fresh yield Job spawned after every TTL reap. The durable
/// `status.<mode>.lastHandledSlot` must keep the slot from re-firing, and the
/// CR must surface the yield-forever state as Ready=False (MaintenanceYielding)
/// instead of a misleading Ready=True.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn yielded_maintenance_slot_is_not_respawned_after_its_job_is_ttl_reaped() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio])
        .await
        .expect("provision MinIO fixtures");
    let client = world.client().clone();
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let maints: Api<Maintenance> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // 1. Foreign writer creates the repo → kopia's maintenance owner is the
    //    foreign identity, exactly the live-cluster shape.
    run_seeder(
        &client,
        "e2e-ttl-maint-seed",
        &[
            SeedStep::WipeBucket {
                bucket: "kopiur-ttl-maint",
            },
            SeedStep::WriteFile {
                dir: "app",
                file: "data.txt",
                content: "foreign-owner-data",
            },
            SeedStep::CreateRepo {
                bucket: "kopiur-ttl-maint",
                username: "foreign",
                hostname: "elsewhere",
            },
            SeedStep::Snapshot { dir: "app" },
        ],
    )
    .await;

    // 2. Adopt it (create disabled, managed maintenance off — we drive an
    //    explicit Maintenance with takeoverPolicy=Never).
    let repo = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": "e2e-ttl-srepo", "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "s3": {
                "bucket": "kopiur-ttl-maint",
                "endpoint": consts::MINIO_ENDPOINT,
                "region": "us-east-1",
                "tls": { "disableTls": true },
                "auth": { "secretRef": { "name": consts::SECRET_S3_CREDS, "namespace": E2E_NAMESPACE } }
            }},
            "encryption": {
                "passwordSecretRef": { "name": consts::SECRET_S3_CREDS, "key": "KOPIA_PASSWORD" }
            },
            "create": { "enabled": false },
            "maintenance": { "enabled": false }
        }
    });
    recreate(&repos, &cr(repo)).await;
    wait_phase(&repos, "e2e-ttl-srepo", "Ready")
        .await
        .expect("adopted repository should reach Ready");

    // 3. Maintenance that must YIELD: foreign owner + takeoverPolicy=Never.
    //    Daily crons pinned ~12h away from NOW: the first-ever slots are due
    //    immediately (year-long lookback) and, once handled, the next slot is
    //    half a day out — no legitimately-due slot can fire during the quiet
    //    window no matter when this test runs (a fixed cron would flake when CI
    //    brackets the hard-coded time).
    let half_day_away = chrono::Utc::now() + chrono::Duration::hours(12);
    use chrono::Timelike;
    let quick_cron = format!("{} {} * * *", half_day_away.minute(), half_day_away.hour());
    let full_cron = format!(
        "{} {} * * *",
        (half_day_away.minute() + 30) % 60,
        half_day_away.hour()
    );
    let maint = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Maintenance",
        "metadata": { "name": "e2e-ttl-yield", "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": "e2e-ttl-srepo" },
            "schedule": {
                "quick": { "cron": quick_cron },
                "full": { "cron": full_cron }
            },
            "ownership": { "owner": "kopiur-e2e-ttl", "takeoverPolicy": "Never" },
            "mover": { "ttlSecondsAfterFinished": JOB_TTL_SECONDS }
        }
    });
    recreate(&maints, &cr(maint)).await;

    // 4. Both first-ever slots run (full first, then quick), each yields, and
    //    the controller records the durable handled marker for each.
    wait_until(
        "both modes record lastHandledAt",
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(&maints, "e2e-ttl-yield").await;
            let full = s.pointer("/full/lastHandledAt").is_some();
            let quick = s.pointer("/quick/lastHandledAt").is_some();
            Ok((full && quick).then_some(()))
        },
    )
    .await
    .expect("yielded slots must durably record lastHandledAt");

    // The mover recorded the yield, and the controller surfaces it as a real
    // health signal: maintenance is NOT running (GC/compaction is not
    // happening) — Ready=False / MaintenanceYielding with the remediation.
    let s = status_json(&maints, "e2e-ttl-yield").await;
    let lease = s
        .get("conditions")
        .and_then(|c| c.as_array())
        .and_then(|a| {
            a.iter()
                .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("LeaseOwned"))
        })
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    assert_eq!(
        lease.get("status").and_then(|v| v.as_str()),
        Some("False"),
        "the yield must be recorded on LeaseOwned"
    );

    // 5. Wait for every per-slot Job to be TTL-reaped, then poke: on the buggy
    //    code each poke respawned the same year-old slot's yield Job.
    wait_until(
        "all maintenance Jobs TTL-reaped",
        default_timeout(),
        poll_interval(),
        || async {
            let list = jobs.list(&Default::default()).await?;
            Ok(list
                .items
                .iter()
                .all(|j| !j.name_any().starts_with("e2e-ttl-yield-"))
                .then_some(()))
        },
    )
    .await
    .expect("finished maintenance Jobs should be reaped by their TTL");

    poke(&maints, "e2e-ttl-yield", "1").await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    poke(&maints, "e2e-ttl-yield", "2").await;
    assert_no_job_with_prefix(
        &jobs,
        "e2e-ttl-yield-",
        "poking a Maintenance whose only due slots were already handled (yielded)",
    )
    .await;

    // 6. The yield-forever state stays visible: Ready=False, MaintenanceYielding.
    let s = status_json(&maints, "e2e-ttl-yield").await;
    let ready = s
        .get("conditions")
        .and_then(|c| c.as_array())
        .and_then(|a| {
            a.iter()
                .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("Ready"))
        })
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    assert_eq!(
        ready.get("status").and_then(|v| v.as_str()),
        Some("False"),
        "a Maintenance that only ever yields must not report Ready=True"
    );
    assert_eq!(
        ready.get("reason").and_then(|v| v.as_str()),
        Some("MaintenanceYielding"),
        "the Ready degradation must name the yield-forever state"
    );
    assert!(
        ready
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .contains("takeoverPolicy=Force"),
        "the message must carry the remediation"
    );

    // Cleanup (the repo CR; the bucket is wiped by the next run's seeder).
    let _ = maints
        .delete("e2e-ttl-yield", &DeleteParams::default())
        .await;
    let _ = repos
        .delete("e2e-ttl-srepo", &DeleteParams::default())
        .await;
}

/// Run a foreign seeder pod to completion (deleting any leftover of the same
/// name first, so a reused cluster can't 409 or replay a finished pod).
async fn run_seeder(client: &Client, name: &str, steps: &[SeedStep<'_>]) {
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    if pods
        .get_opt(name)
        .await
        .expect("query seeder pod")
        .is_some()
    {
        let _ = pods.delete(name, &DeleteParams::default()).await;
        wait_until(
            &format!("leftover seeder pod {name} is gone"),
            default_timeout(),
            poll_interval(),
            || async { Ok(pods.get_opt(name).await?.is_none().then_some(())) },
        )
        .await
        .expect("leftover seeder pod should delete");
    }
    pods.create(
        &PostParams::default(),
        &builders::foreign_kopia_pod(E2E_NAMESPACE, name, steps),
    )
    .await
    .expect("create foreign kopia seeder pod");
    wait::pod_succeeded(client, E2E_NAMESPACE, name)
        .await
        .expect("foreign kopia seeder pod should succeed");
}
