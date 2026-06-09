//! Restore-focused end-to-end scenarios — its own e2e shard (`bins: "restore"`).
//!
//! These exercise the `Restore` mover surface that reached parity with a backup's:
//! `spec.mover.securityContext` (UID/GID), `inheritSecurityContextFrom`,
//! `spec.mover.cache` (ephemeral + persistent volumes), `spec.failurePolicy`, and
//! the privileged-mover namespace gate. Each asserts on the **restore mover `Job`'s
//! pod template** (and provisioned PVCs) the controller produces, so they prove the
//! settings reach the run rather than being silently dropped (the original bug:
//! "the restore mover had no UID control").
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; skip gracefully with no cluster.
//! Driven by `mise run //crates/e2e:test` (Filesystem fixtures only — no object
//! store). All restores reuse a single seed Snapshot in the operator namespace.

#![cfg(all(unix, feature = "e2e"))]

use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};
use serde::Serialize;
use serde::de::DeserializeOwned;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};

use kopiur_api::{ClusterRepository, Repository, Restore, Snapshot, SnapshotPolicy};
use kopiur_e2e::{
    E2E_NAMESPACE, Need, World, annotate_namespace, default_timeout, ensure_namespace,
    poll_interval, wait_until,
};

const PRIVILEGED_ANNOTATION: &str = "kopiur.home-operations.com/privileged-movers";

/// Deserialize a CR from a JSON literal into its typed kube object.
fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// Poll a namespaced CR until `status.phase == want_phase`.
async fn wait_phase<K>(api: &Api<K>, name: &str, want_phase: &str) -> anyhow::Result<()>
where
    K: kube::Resource + Clone + DeserializeOwned + Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    wait_until(
        &format!("{name} phase={want_phase}"),
        default_timeout(),
        poll_interval(),
        || async {
            let Some(obj) = api.get_opt(name).await? else {
                return Ok(None);
            };
            let v = serde_json::to_value(&obj).unwrap_or_default();
            let phase = v
                .get("status")
                .and_then(|s| s.get("phase"))
                .and_then(|p| p.as_str())
                .unwrap_or("");
            Ok((phase == want_phase).then_some(()))
        },
    )
    .await
}

/// Read a CR's `status` as JSON (or `null`).
async fn status_json<K>(api: &Api<K>, name: &str) -> serde_json::Value
where
    K: kube::Resource + Clone + DeserializeOwned + Serialize + std::fmt::Debug,
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

/// Poll a CR until its `status.conditions[type=type_].status` equals `want`.
async fn wait_condition<K>(api: &Api<K>, name: &str, type_: &str, want: &str) -> anyhow::Result<()>
where
    K: kube::Resource + Clone + DeserializeOwned + Serialize + std::fmt::Debug,
    <K as kube::Resource>::DynamicType: Default,
{
    wait_until(
        &format!("{name} {type_}={want}"),
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(api, name).await;
            let got = s
                .get("conditions")
                .and_then(|c| c.as_array())
                .and_then(|a| {
                    a.iter()
                        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some(type_))
                })
                .and_then(|c| c.get("status").and_then(|v| v.as_str()));
            Ok((got == Some(want)).then_some(()))
        },
    )
    .await
}

/// Wait until the restore mover `Job` named `name` exists, returning it. The Job is
/// created with its full pod template before the mover runs, so assertions on the
/// template don't need the restore to complete (its target PVC may never bind).
async fn wait_for_job(jobs: &Api<Job>, name: &str) -> Job {
    wait_until(
        &format!("mover Job {name} created"),
        default_timeout(),
        poll_interval(),
        || async { jobs.get_opt(name).await },
    )
    .await
    .unwrap_or_else(|_| panic!("mover Job {name} should be created"))
}

/// The mover container's `runAsUser` from a Job's pod template (`None` if unset).
fn job_run_as_user(job: &Job) -> Option<i64> {
    job.spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.containers.first())
        .and_then(|c| c.security_context.as_ref())
        .and_then(|sc| sc.run_as_user)
}

fn repository_json(name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "filesystem": { "path": "/repo", "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
            "encryption": { "passwordSecretRef": { "name": "kopia-creds", "key": "KOPIA_PASSWORD" } },
            "create": { "enabled": true }
        }
    })
}

const SEED_REPO: &str = "e2e-r-repo";
const SEED_CFG: &str = "e2e-r-cfg";
const SEED_BACKUP: &str = "e2e-r-seed";

/// Ensure a single Repository + SnapshotPolicy + Snapshot exist and the Snapshot has
/// `Succeeded` (a real snapshot to restore from). Idempotent so every restore test
/// can call it; the restores reference `SEED_BACKUP` cross-resource.
async fn ensure_seed_backup(client: &Client) {
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    if repos.get_opt(SEED_REPO).await.ok().flatten().is_none() {
        let _ = repos
            .create(&PostParams::default(), &cr(repository_json(SEED_REPO)))
            .await;
    }
    wait_phase(&repos, SEED_REPO, "Ready")
        .await
        .expect("seed Repository should reach Ready");

    if configs.get_opt(SEED_CFG).await.ok().flatten().is_none() {
        let cfg = serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "SnapshotPolicy",
            "metadata": { "name": SEED_CFG, "namespace": E2E_NAMESPACE },
            "spec": {
                "repository": { "kind": "Repository", "name": SEED_REPO },
                "sources": [ { "pvc": { "name": "e2e-src" } } ],
                "retention": { "keepLatest": 5 }
            }
        });
        let _ = configs.create(&PostParams::default(), &cr(cfg)).await;
    }
    if backups.get_opt(SEED_BACKUP).await.ok().flatten().is_none() {
        let backup = serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Snapshot",
            "metadata": { "name": SEED_BACKUP, "namespace": E2E_NAMESPACE },
            "spec": { "policyRef": { "name": SEED_CFG }, "deletionPolicy": "Retain" }
        });
        let _ = backups.create(&PostParams::default(), &cr(backup)).await;
    }
    wait_phase(&backups, SEED_BACKUP, "Succeeded")
        .await
        .expect("seed Snapshot should reach Succeeded with a snapshot");
}

/// A Restore referencing the seed backup, writing into a fresh target PVC, with the
/// given extra `spec` fields merged in (mover/cache/failurePolicy).
fn restore_json(name: &str, extra_spec: serde_json::Value) -> serde_json::Value {
    let mut spec = serde_json::json!({
        "repository": { "kind": "Repository", "name": SEED_REPO },
        "source": { "snapshotRef": { "name": SEED_BACKUP } },
        "target": { "pvc": { "name": format!("{name}-dst"), "capacity": "1Gi", "accessModes": ["ReadWriteOnce"] } }
    });
    let (serde_json::Value::Object(base), serde_json::Value::Object(more)) =
        (&mut spec, extra_spec)
    else {
        panic!("specs must be objects");
    };
    base.extend(more);
    serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Restore",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": spec
    })
}

async fn cleanup_restore(restores: &Api<Restore>, name: &str) {
    let _ = restores.delete(name, &DeleteParams::default()).await;
}

/// The headline fix: `Restore.spec.mover.securityContext.runAsUser` reaches the
/// restore mover pod (before this, restore hardcoded UID 65532).
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn restore_mover_runs_as_configured_uid() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_seed_backup(&client).await;

    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = "e2e-r-uid";
    restores
        .create(
            &PostParams::default(),
            &cr(restore_json(
                name,
                serde_json::json!({
                    "mover": { "securityContext": {
                        "runAsUser": 2000, "runAsGroup": 2000, "runAsNonRoot": true
                    } }
                }),
            )),
        )
        .await
        .expect("create Restore with mover.securityContext");

    let job = wait_for_job(&jobs, name).await;
    assert_eq!(
        job_run_as_user(&job),
        Some(2000),
        "restore mover must run as the configured runAsUser (2000)"
    );
    cleanup_restore(&restores, name).await;
}

/// `Restore.spec.mover.podSecurityContext.fsGroup` reaches the restore mover **pod**
/// — the pod-level knob that lets an unprivileged mover populate a freshly-provisioned
/// volume (the gap container-level `securityContext` alone can't close).
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn restore_mover_pod_security_context_fsgroup_is_applied() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_seed_backup(&client).await;

    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = "e2e-r-fsgroup";
    restores
        .create(
            &PostParams::default(),
            &cr(restore_json(
                name,
                serde_json::json!({
                    "mover": {
                        "securityContext": { "runAsUser": 2000, "runAsNonRoot": true },
                        "podSecurityContext": { "fsGroup": 2000, "fsGroupChangePolicy": "OnRootMismatch" }
                    }
                }),
            )),
        )
        .await
        .expect("create Restore with mover.podSecurityContext");

    let job = wait_for_job(&jobs, name).await;
    let fs_group = job
        .spec
        .and_then(|s| s.template.spec)
        .and_then(|p| p.security_context)
        .and_then(|sc| sc.fs_group);
    assert_eq!(
        fs_group,
        Some(2000),
        "restore mover pod must carry the configured fsGroup (2000)"
    );
    cleanup_restore(&restores, name).await;
}

/// `Restore.spec.failurePolicy` drives the restore Job's backoff/deadline (parity
/// with `Snapshot.spec.failurePolicy`).
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn restore_failure_policy_sets_job_backoff_and_deadline() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_seed_backup(&client).await;

    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = "e2e-r-failpol";
    restores
        .create(
            &PostParams::default(),
            &cr(restore_json(
                name,
                serde_json::json!({
                    "failurePolicy": { "backoffLimit": 4, "activeDeadlineSeconds": 1234 }
                }),
            )),
        )
        .await
        .expect("create Restore with failurePolicy");

    let job = wait_for_job(&jobs, name).await;
    let spec = job.spec.as_ref().expect("job spec");
    assert_eq!(spec.backoff_limit, Some(4), "failurePolicy.backoffLimit");
    assert_eq!(
        spec.active_deadline_seconds,
        Some(1234),
        "failurePolicy.activeDeadlineSeconds"
    );
    cleanup_restore(&restores, name).await;
}

/// `Restore.spec.mover.cache` with `mode: Ephemeral` + a capacity produces a sized
/// generic-ephemeral cache volume on the restore mover pod.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn restore_mover_cache_ephemeral_is_sized() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_seed_backup(&client).await;

    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = "e2e-r-cache-eph";
    restores
        .create(
            &PostParams::default(),
            &cr(restore_json(
                name,
                serde_json::json!({
                    "mover": { "cache": { "capacity": "2Gi", "mode": "Ephemeral" } }
                }),
            )),
        )
        .await
        .expect("create Restore with ephemeral cache");

    let job = wait_for_job(&jobs, name).await;
    let vols = job
        .spec
        .and_then(|s| s.template.spec)
        .and_then(|p| p.volumes)
        .unwrap_or_default();
    let cache = vols
        .iter()
        .find(|v| v.name == "kopia-cache")
        .expect("kopia-cache volume");
    let tmpl = cache
        .ephemeral
        .as_ref()
        .and_then(|e| e.volume_claim_template.as_ref())
        .expect("ephemeral cache should use a volumeClaimTemplate");
    let storage = tmpl
        .spec
        .resources
        .as_ref()
        .and_then(|r| r.requests.as_ref())
        .and_then(|r| r.get("storage"))
        .map(|q| q.0.clone());
    assert_eq!(
        storage.as_deref(),
        Some("2Gi"),
        "cache PVC sized to capacity"
    );
    cleanup_restore(&restores, name).await;
}

/// `Restore.spec.mover.cache` with `mode: Persistent` provisions a controller-owned
/// cache PVC the restore mover mounts (warm cache reused across runs).
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn restore_mover_cache_persistent_provisions_pvc() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_seed_backup(&client).await;

    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = "e2e-r-cache-persist";
    restores
        .create(
            &PostParams::default(),
            &cr(restore_json(
                name,
                serde_json::json!({
                    "mover": { "cache": { "capacity": "2Gi", "mode": "Persistent" } }
                }),
            )),
        )
        .await
        .expect("create Restore with persistent cache");

    let job = wait_for_job(&jobs, name).await;
    let claim = format!("kopiur-cache-{name}");
    // The controller provisioned the cache PVC...
    wait_until(
        &format!("cache PVC {claim} provisioned"),
        default_timeout(),
        poll_interval(),
        || async { Ok(pvcs.get_opt(&claim).await?.map(|_| ())) },
    )
    .await
    .expect("persistent cache PVC should be provisioned");
    // ...and the restore mover mounts it as kopia-cache.
    let mounted = job
        .spec
        .and_then(|s| s.template.spec)
        .and_then(|p| p.volumes)
        .unwrap_or_default()
        .into_iter()
        .find(|v| v.name == "kopia-cache")
        .and_then(|v| v.persistent_volume_claim)
        .map(|p| p.claim_name);
    assert_eq!(
        mounted.as_deref(),
        Some(claim.as_str()),
        "restore mover should mount the persistent cache PVC"
    );
    cleanup_restore(&restores, name).await;
}

/// `Restore.spec.mover.inheritSecurityContextFrom` copies a live workload pod's
/// securityContext onto the restore mover (UID/GID match without hard-coding).
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn restore_inherits_security_context_from_workload_pod() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_seed_backup(&client).await;

    // A labeled workload pod running as a specific UID.
    let pods: Api<Pod> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let pod = serde_json::json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": { "name": "e2e-r-inherit-pod", "namespace": E2E_NAMESPACE,
            "labels": { "app": "e2e-r-inherit" } },
        "spec": {
            "securityContext": { "fsGroup": 2500 }, // pod-level — must be inherited too
            "containers": [{
                "name": "app", "image": "registry.k8s.io/pause:3.9",
                "securityContext": { "runAsUser": 2500, "runAsGroup": 2500, "runAsNonRoot": true }
            }]
        }
    });
    let _ = pods.create(&PostParams::default(), &cr(pod)).await;
    wait_until(
        "inherit pod Running",
        default_timeout(),
        poll_interval(),
        || async {
            Ok(pods.get_opt("e2e-r-inherit-pod").await?.filter(|p| {
                p.status
                    .as_ref()
                    .and_then(|s| s.phase.as_deref())
                    .map(|ph| ph == "Running")
                    .unwrap_or(false)
            }))
        },
    )
    .await
    .expect("workload pod Running");

    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = "e2e-r-inherit";
    restores
        .create(
            &PostParams::default(),
            &cr(restore_json(
                name,
                serde_json::json!({
                    "mover": { "inheritSecurityContextFrom": {
                        "podSelector": { "matchLabels": { "app": "e2e-r-inherit" } }
                    } }
                }),
            )),
        )
        .await
        .expect("create Restore with inheritSecurityContextFrom");

    let job = wait_for_job(&jobs, name).await;
    // CONTAINER-level UID inherited...
    assert_eq!(
        job_run_as_user(&job),
        Some(2500),
        "restore mover must inherit the workload pod's container runAsUser (2500)"
    );
    // ...and the POD-level fsGroup inherited too (so a fresh restore volume is writable).
    let fs_group = job
        .spec
        .and_then(|s| s.template.spec)
        .and_then(|p| p.security_context)
        .and_then(|sc| sc.fs_group);
    assert_eq!(
        fs_group,
        Some(2500),
        "restore mover must inherit the workload pod's fsGroup (2500)"
    );
    cleanup_restore(&restores, name).await;
    let _ = pods
        .delete("e2e-r-inherit-pod", &DeleteParams::default())
        .await;
}

/// Create a restore (referencing the seed backup) in `E2E_NAMESPACE` with the given
/// mover spec and assert it is refused with `MoverPermitted=False`. The op-in
/// annotation is NOT set, so this asserts refusal only and leaves no namespace state.
async fn assert_restore_mover_gated(client: &Client, name: &str, mover: serde_json::Value) {
    ensure_seed_backup(client).await;
    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    restores
        .create(
            &PostParams::default(),
            &cr(restore_json(name, serde_json::json!({ "mover": mover }))),
        )
        .await
        .unwrap_or_else(|e| panic!("create gated Restore {name}: {e}"));
    wait_condition(&restores, name, "MoverPermitted", "False")
        .await
        .unwrap_or_else(|_| panic!("restore {name} must be refused with MoverPermitted=False"));
    cleanup_restore(&restores, name).await;
}

/// `privilegedMode: true` alone (no securityContext) trips the restore gate.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn privileged_mode_flag_alone_gates_restore() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    assert_restore_mover_gated(
        &world.client().clone(),
        "e2e-r-privmode",
        serde_json::json!({ "privilegedMode": true }),
    )
    .await;
}

/// A POD-level `runAsUser: 0` (with a benign container) trips the restore gate — the
/// pod-level privilege check, not just the container one.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn pod_level_root_gates_restore() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    assert_restore_mover_gated(
        &world.client().clone(),
        "e2e-r-podroot",
        serde_json::json!({
            "securityContext": { "runAsUser": 1000, "runAsNonRoot": true },
            "podSecurityContext": { "runAsUser": 0 }
        }),
    )
    .await;
}

/// The privileged-mover gate guards restores too: a root restore mover is refused
/// with `MoverPermitted=False` until the restore's namespace opts in, then clears.
/// Self-contained in its own namespace (via a ClusterRepository + cross-namespace
/// snapshotRef) so the opt-in annotation doesn't leak into the other restore tests.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test)"]
async fn privileged_restore_mover_requires_namespace_optin() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_seed_backup(&client).await;
    const RESTORE_NS: &str = "kopiur-e2e-privr";

    // A ClusterRepository over the same repo so a cross-namespace restore resolves it.
    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    if crepos
        .get_opt("e2e-privr-crepo")
        .await
        .ok()
        .flatten()
        .is_none()
    {
        let crepo = serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "ClusterRepository",
            "metadata": { "name": "e2e-privr-crepo" },
            "spec": {
                "backend": { "filesystem": { "path": "/repo", "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
                "encryption": { "passwordSecretRef": { "name": "kopia-creds", "namespace": E2E_NAMESPACE, "key": "KOPIA_PASSWORD" } },
                "create": { "enabled": true },
                "allowedNamespaces": { "all": true }
            }
        });
        let _ = crepos.create(&PostParams::default(), &cr(crepo)).await;
    }
    wait_phase(&crepos, "e2e-privr-crepo", "Ready")
        .await
        .expect("ClusterRepository Ready");

    ensure_namespace(&client, RESTORE_NS)
        .await
        .expect("restore namespace");
    let restores: Api<Restore> = Api::namespaced(client.clone(), RESTORE_NS);
    let restore = serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Restore",
        "metadata": { "name": "e2e-privr-restore", "namespace": RESTORE_NS },
        "spec": {
            "repository": { "kind": "ClusterRepository", "name": "e2e-privr-crepo" },
            "source": { "snapshotRef": { "name": SEED_BACKUP, "namespace": E2E_NAMESPACE } },
            "target": { "pvc": { "name": "e2e-privr-dst", "capacity": "1Gi", "accessModes": ["ReadWriteOnce"] } },
            "mover": { "securityContext": { "runAsUser": 0, "runAsGroup": 0 } }
        }
    });
    restores
        .create(&PostParams::default(), &cr(restore))
        .await
        .expect("create privileged Restore");

    wait_condition(&restores, "e2e-privr-restore", "MoverPermitted", "False")
        .await
        .expect("privileged restore mover refused until the namespace opts in");
    let s = status_json(&restores, "e2e-privr-restore").await;
    let msg = s
        .get("conditions")
        .and_then(|c| c.as_array())
        .and_then(|a| {
            a.iter()
                .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("MoverPermitted"))
        })
        .and_then(|c| c.get("message").and_then(|m| m.as_str()))
        .unwrap_or("");
    assert!(
        msg.contains(PRIVILEGED_ANNOTATION) && msg.contains("Restore `e2e-privr-restore`"),
        "message must name the Restore and the annotation; got: {msg}"
    );

    annotate_namespace(&client, RESTORE_NS, PRIVILEGED_ANNOTATION, "true")
        .await
        .expect("annotate restore namespace");
    wait_condition(&restores, "e2e-privr-restore", "MoverPermitted", "True")
        .await
        .expect("MoverPermitted clears once the namespace opts in");

    let _ = crepos
        .delete("e2e-privr-crepo", &DeleteParams::default())
        .await;
    let nss: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(client.clone());
    let _ = nss.delete(RESTORE_NS, &DeleteParams::default()).await;
}
