//! End-to-end scenarios for the `kubectl kopiur` plugin (crates/cli), exercised
//! exactly as a user runs it: the compiled `kubectl-kopiur` binary as a
//! subprocess against the e2e cluster's kubeconfig.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`, skipping gracefully without
//! a cluster. Driven by `mise run //crates/e2e:test` (which also builds the
//! plugin binary). Covers M1+M2 of the krew-plugin plan:
//!
//! - `suspend`/`resume schedule` actually control SnapshotSchedule firing
//!   (resume → a scheduled Snapshot appears; suspend → firing stops), with
//!   idempotent no-op output on an already-suspended object;
//! - `snapshots list` shows a real produced snapshot with policy/origin/size,
//!   and `--policy`/`--origin`/`--repository`/`-o name|json` filters/formats work;
//! - `snapshot now --wait` runs a policy to Succeeded with real kopia stats,
//!   exits 1 with the failure block for a deterministically-poisoned policy,
//!   and `logs snapshot` returns real mover output (or the honest GC fallback);
//! - a missing object yields a non-zero exit and an actionable what/why/fix error.

#![cfg(all(unix, feature = "e2e"))]

use std::time::Duration;

use kube::Api;
use kube::api::{DeleteParams, ListParams, PostParams};
use serde::de::DeserializeOwned;

use kopiur_api::{Repository, Snapshot, SnapshotPolicy, SnapshotSchedule};
use kopiur_e2e::cli::run_cli;
use kopiur_e2e::{E2E_NAMESPACE, Need, World, default_timeout, poll_interval, wait_until};

/// Deserialize a CR from a JSON literal into its typed kube object.
fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

const REPO: &str = "e2e-cli";
const BUCKET: &str = "kopiur-cli";
const POLICY: &str = "e2e-cli-pol";
const SCHEDULE: &str = "e2e-cli-sched";
const S3_CREDS: &str = "kopia-s3-creds";

fn repository_json() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": REPO, "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "s3": {
                "bucket": BUCKET,
                "endpoint": "minio.kopiur-e2e.svc.cluster.local:9000",
                "region": "us-east-1",
                "tls": { "disableTls": true },
                "auth": { "secretRef": { "name": S3_CREDS, "namespace": E2E_NAMESPACE } }
            }},
            "encryption": {
                "passwordSecretRef": { "name": S3_CREDS, "key": "KOPIA_PASSWORD" }
            },
            "create": { "enabled": true }
        }
    })
}

fn policy_json() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "SnapshotPolicy",
        "metadata": { "name": POLICY, "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": REPO },
            "sources": [ { "pvc": { "name": "e2e-src" } } ],
            "retention": { "keepLatest": 5 }
        }
    })
}

/// Created SUSPENDED so the first cron slot can't race the test: the
/// resume→fire and suspend→stop transitions are then both driven by the CLI.
fn schedule_json() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "SnapshotSchedule",
        "metadata": { "name": SCHEDULE, "namespace": E2E_NAMESPACE },
        "spec": {
            "policyRef": { "name": POLICY },
            "schedule": { "cron": "* * * * *", "runOnCreate": false, "suspend": true }
        }
    })
}

/// Poll a namespaced CR until `status.phase == want_phase`.
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

/// SCHEDULED snapshots produced from our policy. Origin-filtered so the
/// `snapshot now` test's manual snapshots (same config label) never leak into
/// the schedule-firing assertions.
async fn policy_snapshots(api: &Api<Snapshot>) -> Vec<Snapshot> {
    api.list(&ListParams::default().labels(&format!(
        "{}={POLICY},{}=scheduled",
        kopiur_api::consts::CONFIG_LABEL,
        kopiur_api::consts::ORIGIN_LABEL
    )))
    .await
    .expect("list snapshots")
    .items
}

/// The schedule's suspend value as the cluster sees it.
async fn schedule_suspended(api: &Api<SnapshotSchedule>) -> bool {
    api.get(SCHEDULE)
        .await
        .expect("get SnapshotSchedule")
        .spec
        .schedule
        .suspend
}

/// Delete an object if it exists and wait until it is fully gone (finalizers
/// included), so a rerun against a dirty/reused cluster starts clean.
async fn delete_and_wait_gone<K>(api: &Api<K>, name: &str)
where
    K: kube::Resource + Clone + DeserializeOwned + serde::Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    let _ = api.delete(name, &DeleteParams::default()).await;
    wait_until(
        &format!("{name} deleted"),
        default_timeout(),
        poll_interval(),
        || async { Ok(api.get_opt(name).await?.is_none().then_some(())) },
    )
    .await
    .unwrap_or_else(|e| panic!("leftover {name} should delete: {e}"));
}

/// Idempotently provision the shared CLI fixtures (S3 repository → Ready, and
/// the policy over `e2e-src`), so the two CLI tests are order-independent.
async fn ensure_cli_fixtures(client: &kube::Client) {
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = repos
        .create(&PostParams::default(), &cr(repository_json()))
        .await;
    wait_phase(&repos, REPO, "Ready")
        .await
        .expect("repository Ready");
    let _ = policies
        .create(&PostParams::default(), &cr(policy_json()))
        .await;
}

#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn cli_suspend_resume_and_snapshots_list() {
    let Some(world) = World::connect().await else {
        return;
    };
    // Minio for the S3 repository; Filesystem for the `e2e-src` source PVC the
    // policy snapshots (it is NOT part of the Minio fixtures).
    world
        .ensure(&[Need::Minio, Need::Filesystem])
        .await
        .expect("provision MinIO + buckets + source PVC");
    let client = world.client().clone();
    let schedules: Api<SnapshotSchedule> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let snapshots: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // --- Fixture: S3 repository (Ready), policy, and a SUSPENDED every-minute schedule.
    // Recreate-from-scratch so a rerun on a reused cluster (killed harness)
    // starts clean: no stale schedule, no leftover scheduled Snapshots.
    ensure_cli_fixtures(&client).await;
    delete_and_wait_gone(&schedules, SCHEDULE).await;
    for snap in policy_snapshots(&snapshots).await {
        delete_and_wait_gone(&snapshots, &snap.metadata.name.clone().unwrap_or_default()).await;
    }
    schedules
        .create(&PostParams::default(), &cr(schedule_json()))
        .await
        .expect("create SnapshotSchedule");

    // --- suspend on an already-suspended schedule is an idempotent no-op.
    let out = run_cli(&["-n", E2E_NAMESPACE, "suspend", "schedule", SCHEDULE]);
    assert!(out.success, "suspend failed: stderr={}", out.stderr);
    assert!(
        out.stdout.contains("unchanged (already suspended)"),
        "expected idempotent no-op message, got: {}",
        out.stdout
    );

    // --- While suspended, the every-minute cron must NOT fire.
    tokio::time::sleep(Duration::from_secs(75)).await;
    let fired = policy_snapshots(&snapshots).await;
    assert!(
        fired.is_empty(),
        "suspended schedule must not create Snapshots, found: {:?}",
        fired
            .iter()
            .map(|s| s.metadata.name.clone())
            .collect::<Vec<_>>()
    );

    // --- CLI resume (true→false transition) makes the schedule fire.
    let out = run_cli(&["-n", E2E_NAMESPACE, "resume", "schedule", SCHEDULE]);
    assert!(out.success, "resume failed: stderr={}", out.stderr);
    assert!(
        out.stdout.contains(&format!(
            "snapshotschedule.kopiur.home-operations.com/{SCHEDULE} resumed"
        )),
        "unexpected resume output: {}",
        out.stdout
    );
    assert!(
        !schedule_suspended(&schedules).await,
        "spec must be unsuspended"
    );

    let snap_name = wait_until(
        "resumed schedule creates a scheduled Snapshot",
        default_timeout(),
        poll_interval(),
        || async {
            let items = policy_snapshots(&snapshots).await;
            Ok(items.first().and_then(|s| s.metadata.name.clone()))
        },
    )
    .await
    .expect("schedule should fire after resume");
    wait_phase(&snapshots, &snap_name, "Succeeded")
        .await
        .expect("scheduled Snapshot should succeed");

    // --- snapshots list: table content for the real, succeeded snapshot.
    let out = run_cli(&["-n", E2E_NAMESPACE, "snapshots", "list"]);
    assert!(out.success, "snapshots list failed: stderr={}", out.stderr);
    let table = &out.stdout;
    assert!(
        table.lines().next().unwrap_or("").starts_with("NAME"),
        "missing header: {table}"
    );
    let row = table
        .lines()
        .find(|l| l.starts_with(&snap_name))
        .unwrap_or_else(|| panic!("snapshot {snap_name} not in table:\n{table}"));
    assert!(row.contains(POLICY), "POLICY column missing: {row}");
    assert!(row.contains("scheduled"), "ORIGIN column missing: {row}");
    assert!(row.contains("Succeeded"), "PHASE column missing: {row}");
    assert!(
        !row.contains(" -  - "),
        "SIZE/FILES should be populated from real kopia stats: {row}"
    );

    // --- filters: --policy and --origin narrow server-side via labels.
    let out = run_cli(&["-n", E2E_NAMESPACE, "snapshots", "list", "--policy", POLICY]);
    assert!(
        out.stdout.contains(&snap_name),
        "--policy should match: {}",
        out.stdout
    );
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "snapshots",
        "list",
        "--policy",
        "no-such-policy",
    ]);
    assert!(
        out.stdout.contains("No snapshots found"),
        "non-matching --policy should list nothing: {}",
        out.stdout
    );
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "snapshots",
        "list",
        "--origin",
        "discovered",
    ]);
    assert!(
        !out.stdout.contains(&snap_name),
        "--origin discovered must exclude a scheduled snapshot: {}",
        out.stdout
    );

    // --- --repository matches a PRODUCED snapshot through status.resolved.repository.
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "snapshots",
        "list",
        "--repository",
        REPO,
    ]);
    assert!(
        out.stdout.contains(&snap_name),
        "--repository should match via resolved ref: {}",
        out.stdout
    );

    // --- output formats: -o name and -o json (machine-readable, verbatim CRs).
    let out = run_cli(&["-n", E2E_NAMESPACE, "snapshots", "list", "-o", "name"]);
    assert!(
        out.stdout
            .contains(&format!("snapshot.kopiur.home-operations.com/{snap_name}")),
        "-o name format: {}",
        out.stdout
    );
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "snapshots",
        "list",
        "-o",
        "json",
        "--policy",
        POLICY,
    ]);
    let parsed: serde_json::Value =
        serde_json::from_str(&out.stdout).expect("-o json emits valid JSON");
    assert_eq!(parsed["kind"], "List");
    let id = parsed["items"][0]["status"]["snapshot"]["kopiaSnapshotID"]
        .as_str()
        .unwrap_or_default();
    assert!(
        !id.is_empty(),
        "items carry the real kopia snapshot id: {parsed}"
    );

    // --- CLI suspend (false→true transition) stops further firing.
    let out = run_cli(&["-n", E2E_NAMESPACE, "suspend", "schedule", SCHEDULE]);
    assert!(out.success, "suspend failed: stderr={}", out.stderr);
    assert!(
        out.stdout.contains(&format!(
            "snapshotschedule.kopiur.home-operations.com/{SCHEDULE} suspended"
        )),
        "unexpected suspend output: {}",
        out.stdout
    );
    assert!(
        schedule_suspended(&schedules).await,
        "spec must be suspended"
    );
    // Settle, then baseline: nothing new may fire for the next ~75s.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let baseline = policy_snapshots(&snapshots).await.len();
    tokio::time::sleep(Duration::from_secs(75)).await;
    let after = policy_snapshots(&snapshots).await.len();
    assert_eq!(
        baseline, after,
        "suspended schedule must not create new Snapshots"
    );

    // --- Error path: -A makes no sense for a single-object command.
    let out = run_cli(&["-A", "suspend", "schedule", SCHEDULE]);
    assert!(!out.success, "-A must be rejected for suspend");
    assert!(
        out.stderr.contains("drop -A and pass -n"),
        "stderr should explain the -A rejection: {}",
        out.stderr
    );

    // --- Error path: a missing schedule exits non-zero with what/why/fix.
    let out = run_cli(&["-n", E2E_NAMESPACE, "suspend", "schedule", "does-not-exist"]);
    assert!(!out.success, "missing object must fail");
    assert_eq!(out.code, Some(1));
    assert!(
        out.stderr.contains("\"does-not-exist\" not found"),
        "stderr should say what failed: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("kubectl get snapshotschedules"),
        "stderr should say how to fix: {}",
        out.stderr
    );
}

/// A recipe poisoned with an unknown kopia flag: the mover's policy-set step
/// fails deterministically ("unknown long flag"), giving the CLI's failure
/// path a real Failed snapshot to report. Same injection as
/// `terminal_failure::failed_mover_writes_log_tail_and_failure_block`.
fn poisoned_policy_json() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "SnapshotPolicy",
        "metadata": { "name": "e2e-cli-badpol", "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": REPO },
            "sources": [ { "pvc": { "name": "e2e-src" } } ],
            "extraArgs": ["--kopiur-e2e-bogus-flag"]
        }
    })
}

#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn cli_snapshot_now_wait_logs_and_failure() {
    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::Filesystem])
        .await
        .expect("provision MinIO + buckets + source PVC");
    let client = world.client().clone();
    ensure_cli_fixtures(&client).await;
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let snapshots: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    // Rerun-on-dirty-cluster hygiene: the fixed names must not collide.
    delete_and_wait_gone(&snapshots, "e2e-cli-manual").await;
    delete_and_wait_gone(&snapshots, "e2e-cli-fail").await;

    // --- Happy path: snapshot now --logs (implies --wait) runs the policy,
    // STREAMS the mover's logs (regression guard: streaming must survive the
    // pod's ContainerCreating window), and reports stats.
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "snapshot",
        "now",
        "--policy",
        POLICY,
        "--name",
        "e2e-cli-manual",
        "--tag",
        "reason=e2e",
        "--logs",
        "--timeout",
        "5m",
    ]);
    assert!(
        out.success,
        "snapshot now --wait failed: stdout={} stderr={}",
        out.stdout, out.stderr
    );
    assert!(
        out.stderr
            .contains("snapshot.kopiur.home-operations.com/e2e-cli-manual created"),
        "creation line goes to stderr while waiting: {}",
        out.stderr
    );
    assert!(
        out.stdout
            .contains("snapshot e2e-cli-manual succeeded: kopia id "),
        "success summary with real kopia id: {}",
        out.stdout
    );

    // The CR is a real manual snapshot: origin label + tag + policyRef landed.
    let created = snapshots
        .get("e2e-cli-manual")
        .await
        .expect("created Snapshot exists");
    let v = serde_json::to_value(&created).unwrap();
    assert_eq!(
        v["metadata"]["labels"]["kopiur.home-operations.com/origin"],
        "manual"
    );
    assert_eq!(v["spec"]["tags"]["reason"], "e2e");
    assert_eq!(v["status"]["origin"], "manual");

    // …and `snapshots list --origin manual` shows it.
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "snapshots",
        "list",
        "--origin",
        "manual",
    ]);
    assert!(
        out.stdout.contains("e2e-cli-manual"),
        "manual snapshot listed: {}",
        out.stdout
    );

    // --- logs: real mover output for the finished snapshot.
    let out = run_cli(&["-n", E2E_NAMESPACE, "logs", "snapshot", "e2e-cli-manual"]);
    assert!(out.success, "logs failed: stderr={}", out.stderr);
    assert!(
        out.stdout.contains("kopiur_mover") || out.stdout.contains("no longer exist"),
        "logs show real mover output (or the honest GC fallback): {}",
        out.stdout
    );

    // --- Failure path: a poisoned policy ends Failed; the CLI exits 1 with detail.
    let _ = policies
        .create(&PostParams::default(), &cr(poisoned_policy_json()))
        .await;
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "snapshot",
        "now",
        "--policy",
        "e2e-cli-badpol",
        "--name",
        "e2e-cli-fail",
        "--backoff-limit",
        "0",
        "--wait",
        "--timeout",
        "5m",
    ]);
    assert!(!out.success, "a Failed snapshot must exit non-zero");
    assert_eq!(out.code, Some(1), "stderr={}", out.stderr);
    assert!(
        out.stderr.contains("snapshot e2e-cli-fail failed"),
        "failure summary on stderr: {}",
        out.stderr
    );

    // --- Preflight: a missing policy is an actionable error, no CR created.
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "snapshot",
        "now",
        "--policy",
        "no-such-policy",
    ]);
    assert!(!out.success);
    assert!(
        out.stderr.contains("\"no-such-policy\" not found"),
        "stderr: {}",
        out.stderr
    );
    assert!(
        out.stderr.contains("kubectl get snapshotpolicies"),
        "stderr: {}",
        out.stderr
    );
}

/// M3: the restore one-liner end-to-end, against what each backend supports:
/// - S3 repo + `--from-snapshot --create-pvc --wait`: real data round-trips
///   (reader pod proves the seeded bytes);
/// - filesystem repo + `--from-policy`: identity-based resolution (the
///   operator only implements in-process snapshot listing for filesystem
///   backends — pairing fromPolicy with S3 leaves the Restore Pending with an
///   InvalidSpec warning, which is how this test's first draft failed);
/// - a missing `--from-snapshot` fails closed → exit 1.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn cli_restore_from_policy_into_created_pvc() {
    use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
    use kopiur_api::Restore;
    use kopiur_e2e::builders;

    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::Filesystem])
        .await
        .expect("provision MinIO + buckets + source PVC");
    let client = world.client().clone();
    ensure_cli_fixtures(&client).await;
    let snapshots: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // Rerun hygiene: fixed names from a previous (possibly killed) run.
    delete_and_wait_gone(&snapshots, "e2e-cli-restore-src").await;
    delete_and_wait_gone(&snapshots, "e2e-cli-fs-src").await;
    delete_and_wait_gone(&restores, "e2e-cli-restore").await;
    delete_and_wait_gone(&restores, "e2e-cli-restore-fs").await;
    delete_and_wait_gone(&restores, "e2e-cli-restore-miss").await;
    delete_and_wait_gone(&pods, "e2e-cli-restored-reader").await;
    delete_and_wait_gone(&pvcs, "e2e-cli-restored").await;
    delete_and_wait_gone(&pvcs, "e2e-cli-restored-fs").await;

    // A fresh snapshot of e2e-src (seeded with known bytes by node-seed).
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "snapshot",
        "now",
        "--policy",
        POLICY,
        "--name",
        "e2e-cli-restore-src",
        "--wait",
        "--timeout",
        "5m",
    ]);
    assert!(out.success, "snapshot for restore failed: {}", out.stderr);

    // The one-liner under test: snapshot source × created-PVC target (S3).
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "restore",
        "--from-snapshot",
        "e2e-cli-restore-src",
        "--create-pvc",
        "e2e-cli-restored",
        "--size",
        "1Gi",
        "--name",
        "e2e-cli-restore",
        "--wait",
        "--timeout",
        "5m",
    ]);
    assert!(
        out.success,
        "restore --wait failed: stdout={} stderr={}",
        out.stdout, out.stderr
    );
    assert!(
        out.stderr
            .contains("restore.kopiur.home-operations.com/e2e-cli-restore created"),
        "creation line on stderr: {}",
        out.stderr
    );
    assert!(
        out.stdout
            .contains("restore e2e-cli-restore completed: kopia id "),
        "completion summary with the pinned kopia id: {}",
        out.stdout
    );

    // The data round-tripped: a reader pod greps the seeded bytes out of the
    // operator-created PVC.
    let reader = builders::one_shot_pod(
        E2E_NAMESPACE,
        "e2e-cli-restored-reader",
        &[
            "sh",
            "-c",
            "grep -q 'hello kopiur e2e' /restore/a.txt && grep -q 'nested data' /restore/sub/b.txt",
        ],
        &[("e2e-cli-restored", "/restore")],
    );
    pods.create(&PostParams::default(), &reader)
        .await
        .expect("create reader pod");
    kopiur_e2e::wait::pod_succeeded(&client, E2E_NAMESPACE, "e2e-cli-restored-reader")
        .await
        .expect("restored PVC must contain the seeded bytes");

    // --- fromPolicy (identity-based) resolution: filesystem repo only — the
    // operator's in-process snapshot listing doesn't support object stores.
    let fs_repo = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": "e2e-cli-fs", "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "filesystem": { "path": "/repo", "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
            "encryption": { "passwordSecretRef": { "name": "kopia-creds", "key": "KOPIA_PASSWORD" } },
            "create": { "enabled": true }
        }
    });
    let fs_policy = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "SnapshotPolicy",
        "metadata": { "name": "e2e-cli-fs-pol", "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": "e2e-cli-fs" },
            "sources": [ { "pvc": { "name": "e2e-src" } } ]
        }
    });
    let repos: Api<kopiur_api::Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = repos.create(&PostParams::default(), &cr(fs_repo)).await;
    wait_phase(&repos, "e2e-cli-fs", "Ready")
        .await
        .expect("filesystem repository Ready");
    let _ = policies
        .create(&PostParams::default(), &cr(fs_policy))
        .await;

    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "snapshot",
        "now",
        "--policy",
        "e2e-cli-fs-pol",
        "--name",
        "e2e-cli-fs-src",
        "--wait",
        "--timeout",
        "5m",
    ]);
    assert!(out.success, "fs snapshot failed: {}", out.stderr);

    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "restore",
        "--from-policy",
        "e2e-cli-fs-pol",
        "--create-pvc",
        "e2e-cli-restored-fs",
        "--size",
        "1Gi",
        "--name",
        "e2e-cli-restore-fs",
        "--wait",
        "--timeout",
        "5m",
    ]);
    assert!(
        out.success,
        "restore --from-policy (filesystem) failed: stdout={} stderr={}",
        out.stdout, out.stderr
    );
    assert!(
        out.stdout
            .contains("restore e2e-cli-restore-fs completed: kopia id "),
        "fromPolicy completion summary: {}",
        out.stdout
    );

    // Restore error path: a snapshotRef that doesn't exist fails closed
    // (onMissingSnapshot defaults to Fail for explicit sources) → exit 1.
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "restore",
        "--from-snapshot",
        "no-such-snapshot",
        "--to-pvc",
        "e2e-cli-restored",
        "--name",
        "e2e-cli-restore-miss",
        "--wait-timeout",
        "10s",
        "--wait",
        "--timeout",
        "4m",
    ]);
    assert!(!out.success, "missing snapshot must fail the restore");
    assert_eq!(out.code, Some(1), "stderr={}", out.stderr);
    assert!(
        out.stderr.contains("restore e2e-cli-restore-miss failed"),
        "failure summary on stderr: {}",
        out.stderr
    );
    let _ = restores
        .delete("e2e-cli-restore-miss", &DeleteParams::default())
        .await;
}

/// M4: `status` aggregates real cluster state, and `doctor` passes on a
/// healthy install, then FAILS actionably (exit 1, what/why/fix naming the
/// Secret) once a repository's credential Secret is deleted.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn cli_doctor_and_status() {
    use k8s_openapi::api::core::v1::Secret;

    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::Filesystem])
        .await
        .expect("provision MinIO + buckets + source PVC");
    let client = world.client().clone();
    ensure_cli_fixtures(&client).await;

    // --- status: the real repository/policy state on one screen.
    let out = run_cli(&["-n", E2E_NAMESPACE, "status"]);
    assert!(out.success, "status failed: {}", out.stderr);
    assert!(out.stdout.contains("REPOSITORIES"), "{}", out.stdout);
    let repo_line = out
        .stdout
        .lines()
        .find(|l| l.contains(REPO) && l.starts_with("Repository"))
        .unwrap_or_else(|| panic!("repository row missing:\n{}", out.stdout));
    assert!(repo_line.contains("Ready"), "{repo_line}");
    assert!(repo_line.contains("S3"), "{repo_line}");
    assert!(out.stdout.contains(POLICY), "{}", out.stdout);
    assert!(out.stdout.contains("IN FLIGHT"), "{}", out.stdout);

    let out = run_cli(&["-n", E2E_NAMESPACE, "status", "-o", "json"]);
    let parsed: serde_json::Value = serde_json::from_str(&out.stdout).expect("status -o json");
    assert!(
        parsed["repositories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["name"] == REPO && r["phase"] == "Ready"),
        "{parsed}"
    );

    // --- doctor on a healthy install. Assert the check lines this test
    // CONTROLS (other e2e binaries deliberately leave Failed repositories
    // behind in a full-suite run, so an overall exit-0 assert is shard-only
    // and deliberately omitted).
    let out = run_cli(&["-n", E2E_NAMESPACE, "doctor"]);
    assert!(
        out.stdout.contains("ok    CRDs installed"),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout.contains("ok    controller running"),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout.contains("ok    credential secrets present"),
        "{}",
        out.stdout
    );
    // The e2e chart installs the webhook: both the Deployment check and the
    // LIVE dry-run admission probe must pass against the real webhook.
    assert!(
        out.stdout.contains("ok    webhook running"),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout
            .contains("ok    webhook admission (live dry-run probe)"),
        "the dry-run probe must be denied by the real kopiur webhook: {}",
        out.stdout
    );

    // --- Break it: delete the repo credentials Secret; doctor must FAIL with
    // the Secret named, then recover once the Secret is restored.
    let secrets: Api<Secret> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let saved = secrets.get("kopia-s3-creds").await.expect("creds secret");
    delete_and_wait_gone(&secrets, "kopia-s3-creds").await;

    let out = run_cli(&["-n", E2E_NAMESPACE, "doctor"]);
    assert!(!out.success, "doctor must fail with the Secret gone");
    assert_eq!(out.code, Some(1), "{}", out.stdout);
    assert!(
        out.stdout.contains("FAIL  credential secrets present"),
        "{}",
        out.stdout
    );
    assert!(
        out.stdout.contains("kopia-s3-creds"),
        "the missing Secret must be named: {}",
        out.stdout
    );
    assert!(out.stdout.contains("fix:"), "{}", out.stdout);

    // Restore the Secret (metadata stripped to a creatable object) so the
    // remaining CLI tests keep a working repository.
    let mut restored = saved.clone();
    restored.metadata.resource_version = None;
    restored.metadata.uid = None;
    restored.metadata.creation_timestamp = None;
    restored.metadata.managed_fields = None;
    secrets
        .create(&PostParams::default(), &restored)
        .await
        .expect("re-create creds secret");
    // The Secret deletion flipped the Repository non-Ready (referent watch);
    // wait for it to recover before asserting the credentials check again.
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    wait_phase(&repos, REPO, "Ready")
        .await
        .expect("repository recovers after the Secret returns");
    let out = run_cli(&["-n", E2E_NAMESPACE, "doctor"]);
    assert!(
        out.stdout.contains("ok    credential secrets present"),
        "doctor must report the credentials present again: {}",
        out.stdout
    );
}

/// M5: `maintenance run` — the annotation trigger drives a REAL out-of-band
/// mover run through the operator (same lease/single-flight path as cron),
/// answered in `status.manualRun`; `--repository` resolution and `--full` both
/// work end-to-end.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + MinIO + built images + helm install"]
async fn cli_maintenance_run() {
    use kopiur_api::Maintenance;

    let Some(world) = World::connect().await else {
        return;
    };
    world
        .ensure(&[Need::Minio, Need::Filesystem])
        .await
        .expect("provision MinIO + buckets + source PVC");
    let client = world.client().clone();
    ensure_cli_fixtures(&client).await;

    // The operator default-manages a Maintenance per repository (ADR §3.7);
    // wait for the one covering our repo to be projected.
    let maints: Api<Maintenance> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    wait_until(
        "default-managed Maintenance exists",
        default_timeout(),
        poll_interval(),
        || async { Ok(maints.get_opt(REPO).await?.map(|_| ())) },
    )
    .await
    .expect("operator projects the managed Maintenance");

    // --- quick run, addressed by NAME.
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "maintenance",
        "run",
        REPO,
        "--wait",
        "--timeout",
        "5m",
    ]);
    assert!(
        out.success,
        "maintenance run --wait failed: stdout={} stderr={}",
        out.stdout, out.stderr
    );
    assert!(out.stdout.contains("quick run completed"), "{}", out.stdout);
    let m = maints.get(REPO).await.expect("maintenance");
    let manual = m
        .status
        .as_ref()
        .and_then(|s| s.manual_run.as_ref())
        .expect("manualRun status written");
    assert_eq!(
        manual.phase,
        Some(kopiur_api::ManualRunPhase::Succeeded),
        "{manual:?}"
    );
    assert_eq!(manual.mode, Some(kopiur_api::ManualRunMode::Quick));
    let first_request = manual.requested_at.clone().expect("requestedAt pinned");
    // REAL-run proof: the mover only writes lastRunAt when maintenance actually
    // ran — a lease YIELD also reports Succeeded but writes nothing. This is
    // the regression guard for the held_by_other identity bug (kopia's owner
    // vs. the logical lease string), which made every run on a
    // mover-bootstrapped repository yield forever.
    assert!(
        m.status
            .as_ref()
            .and_then(|s| s.quick.as_ref())
            .and_then(|q| q.last_run_at.as_ref())
            .is_some(),
        "quick maintenance must have ACTUALLY run (lastRunAt set): {:?}",
        m.status
    );

    // --- FULL run, addressed via --repository (resolution path), and a NEW
    // requestedAt (the trigger re-arms on a fresh timestamp).
    let out = run_cli(&[
        "-n",
        E2E_NAMESPACE,
        "maintenance",
        "run",
        "--repository",
        REPO,
        "--full",
        "--wait",
        "--timeout",
        "5m",
    ]);
    assert!(
        out.success,
        "maintenance run --repository --full failed: stdout={} stderr={}",
        out.stdout, out.stderr
    );
    assert!(out.stdout.contains("full run completed"), "{}", out.stdout);
    let m = maints.get(REPO).await.expect("maintenance");
    let manual = m
        .status
        .as_ref()
        .and_then(|s| s.manual_run.as_ref())
        .expect("manualRun status written");
    assert_eq!(manual.mode, Some(kopiur_api::ManualRunMode::Full));
    assert_ne!(
        manual.requested_at.as_ref(),
        Some(&first_request),
        "a new request must carry a new requestedAt"
    );
    assert!(
        m.status
            .as_ref()
            .and_then(|s| s.full.as_ref())
            .and_then(|f| f.last_run_at.as_ref())
            .is_some(),
        "full maintenance must have ACTUALLY run (lastRunAt set): {:?}",
        m.status
    );

    // --- A missing Maintenance is an actionable error.
    let out = run_cli(&["-n", E2E_NAMESPACE, "maintenance", "run", "does-not-exist"]);
    assert!(!out.success);
    assert!(
        out.stderr.contains("\"does-not-exist\" not found"),
        "{}",
        out.stderr
    );
}
