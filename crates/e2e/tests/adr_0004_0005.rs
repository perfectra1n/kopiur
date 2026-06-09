//! End-to-end regression scenarios for the ADR-0004 / ADR-0005 snapshot reshape.
//!
//! These cover the highest-value NEW pipeline behaviors the reshape introduced —
//! exactly the "pipeline bug" class the e2e harness exists for, so each is written
//! to FAIL on the buggy code and pass on the fix:
//!
//! - **`moverDefaults` inheritance + the bootstrap-gap fix** (the headline): a
//!   `Repository.spec.moverDefaults` security/pod context flows into the BOOTSTRAP
//!   Job's pod (the gap fix) AND a backup mover's pod, a per-recipe `mover`
//!   override merges field-wise OVER `moverDefaults`, and the hardened
//!   `capabilities.drop:[ALL]` + `seccompProfile: RuntimeDefault` SURVIVE the merge
//!   (the de-hardening regression).
//! - **`onNamespaceDelete` Orphan vs Delete** (the data-loss-prevention fix):
//!   deleting a consuming namespace orphans the kopia snapshot by default, but
//!   cascades the delete when the owning repository opts in with `Delete`.
//! - **Snapshot `spec.pin` survives GFS prune** (ADR-0005 §13(c)).
//! - **Restore `target.populator: {}`** form, **`mode: ReadOnly`** repo refusing
//!   backups while serving restores, **`credentialProjection.allowed` fail-closed**
//!   gate, and **kstatus `Ready`** so `kubectl wait --for=condition=Ready` works.
//! - **Verification** (`successExpr`) and **`RepositoryReplication`** between two
//!   filesystem repos.
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; skip gracefully with no cluster.
//! Driven by `mise run //crates/e2e:test`. Each scenario uses its OWN repo
//! subdirectory under the shared `/repo` hostPath so snapshot counts stay isolated
//! across the parallel scenarios.

#![cfg(all(unix, feature = "e2e"))]

use std::time::Duration;

use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};
use serde::Serialize;
use serde::de::DeserializeOwned;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Namespace;

use kopiur_api::{
    ClusterRepository, Repository, RepositoryReplication, Restore, Snapshot, SnapshotPolicy,
};
use kopiur_e2e::{
    E2E_NAMESPACE, Need, World, default_timeout, ensure_namespace, poll_interval, wait_until,
};

/// The repository password Secret the chart-installed operator reads.
const CREDS_SECRET: &str = "kopia-creds";

/// Deserialize a CR from a JSON literal into its typed kube object.
fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// A namespaced filesystem `Repository` over a dedicated subdirectory of the shared
/// `/repo` hostPath, with the given extra `spec` fields merged in (moverDefaults,
/// mode, onNamespaceDelete, …).
fn repository_json(name: &str, subpath: &str, extra_spec: serde_json::Value) -> serde_json::Value {
    merge_spec(
        serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Repository",
            "metadata": { "name": name, "namespace": E2E_NAMESPACE },
            "spec": {
                "backend": { "filesystem": { "path": format!("/repo/{subpath}"), "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
                "encryption": { "passwordSecretRef": { "name": CREDS_SECRET, "key": "KOPIA_PASSWORD" } },
                "create": { "enabled": true }
            }
        }),
        extra_spec,
    )
}

/// A cluster-scoped filesystem `ClusterRepository` over a dedicated `/repo`
/// subdirectory, opened to all namespaces, with extra `spec` fields merged in.
fn cluster_repository_json(
    name: &str,
    subpath: &str,
    extra_spec: serde_json::Value,
) -> serde_json::Value {
    merge_spec(
        serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "ClusterRepository",
            "metadata": { "name": name },
            "spec": {
                "backend": { "filesystem": { "path": format!("/repo/{subpath}"), "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
                "encryption": { "passwordSecretRef": { "name": CREDS_SECRET, "namespace": E2E_NAMESPACE, "key": "KOPIA_PASSWORD" } },
                "create": { "enabled": true },
                "allowedNamespaces": { "all": true }
            }
        }),
        extra_spec,
    )
}

/// Merge `extra` (an object) into `base["spec"]`.
fn merge_spec(mut base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    if let (Some(spec), serde_json::Value::Object(more)) = (base.get_mut("spec"), extra) {
        let serde_json::Value::Object(s) = spec else {
            panic!("spec must be an object");
        };
        s.extend(more);
    }
    base
}

fn snapshot_policy_json(
    ns: &str,
    name: &str,
    repo_kind: &str,
    repo: &str,
    extra_spec: serde_json::Value,
) -> serde_json::Value {
    merge_spec(
        serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "SnapshotPolicy",
            "metadata": { "name": name, "namespace": ns },
            "spec": {
                "repository": { "kind": repo_kind, "name": repo },
                "sources": [ { "pvc": { "name": "e2e-src" } } ],
                "retention": { "keepLatest": 5 }
            }
        }),
        extra_spec,
    )
}

fn snapshot_json(
    ns: &str,
    name: &str,
    policy: &str,
    extra_spec: serde_json::Value,
) -> serde_json::Value {
    merge_spec(
        serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Snapshot",
            "metadata": { "name": name, "namespace": ns },
            "spec": { "policyRef": { "name": policy }, "deletionPolicy": "Retain" }
        }),
        extra_spec,
    )
}

/// Poll a CR until `status.phase == want_phase`.
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

/// Poll a CR until its `status.conditions[type=type_].status` equals `want`,
/// returning the matching condition object.
async fn wait_condition<K>(
    api: &Api<K>,
    name: &str,
    type_: &str,
    want: &str,
) -> anyhow::Result<serde_json::Value>
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
            let cond = s
                .get("conditions")
                .and_then(|c| c.as_array())
                .and_then(|a| {
                    a.iter()
                        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some(type_))
                })
                .cloned();
            Ok(cond.filter(|c| c.get("status").and_then(|s| s.as_str()) == Some(want)))
        },
    )
    .await
}

/// Wait until the mover `Job` named `name` (in `ns`) exists, returning it.
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

/// The mover container's container-level `securityContext` from a Job's pod template.
fn job_container_sc(job: &Job) -> Option<k8s_openapi::api::core::v1::SecurityContext> {
    job.spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.containers.first())
        .and_then(|c| c.security_context.clone())
}

/// The mover pod-level `securityContext` from a Job's pod template.
fn job_pod_sc(job: &Job) -> Option<k8s_openapi::api::core::v1::PodSecurityContext> {
    job.spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.security_context.clone())
}

/// Assert a rendered mover container securityContext STILL carries the hardened
/// `capabilities.drop:[ALL]` + `seccompProfile: RuntimeDefault` — the de-hardening
/// regression guard (ADR-0004 §2): a partial `moverDefaults`/`mover` override must
/// never wipe the hardened base.
fn assert_hardening_survives(sc: &k8s_openapi::api::core::v1::SecurityContext, ctx: &str) {
    let drops = sc
        .capabilities
        .as_ref()
        .and_then(|c| c.drop.clone())
        .unwrap_or_default();
    assert!(
        drops.iter().any(|d| d == "ALL"),
        "{ctx}: hardened capabilities.drop:[ALL] must survive the merge, got {drops:?}"
    );
    assert_eq!(
        sc.seccomp_profile.as_ref().map(|s| s.type_.as_str()),
        Some("RuntimeDefault"),
        "{ctx}: hardened seccompProfile: RuntimeDefault must survive the merge"
    );
}

// ---------------------------------------------------------------------------
// (a) moverDefaults inheritance + bootstrap-gap fix + de-hardening regression
// ---------------------------------------------------------------------------

/// THE HEADLINE. A `Repository.spec.moverDefaults` security/pod context must reach
/// (1) the BOOTSTRAP Job's pod (the bootstrap-gap fix — before ADR-0004 the
/// connect/create Job ignored moverDefaults, so a filesystem repo on a
/// non-65532-owned dir was un-bootstrappable), and (2) the backup mover's pod. A
/// per-recipe `mover.securityContext.runAsUser` then merges OVER moverDefaults, and
/// in every rendered container the hardened `drop:[ALL]`/seccomp SURVIVES.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn mover_defaults_inherited_by_bootstrap_and_backup_with_recipe_override() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();

    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = "e2e-md-repo";
    // moverDefaults inherited by EVERY mover (bootstrap + backup): pod fsGroup +
    // container runAsUser/runAsGroup. The mover image runs as 65532, so keep the repo
    // dir accessible — these values just prove inheritance, not a UID the kopia run
    // must match for a hostPath repo (kopia creates the subdir as the running UID).
    repos
        .create(
            &PostParams::default(),
            &cr(repository_json(
                repo,
                "moverdefaults",
                serde_json::json!({
                    "moverDefaults": {
                        "podSecurityContext": { "fsGroup": 65532 },
                        "securityContext": { "runAsUser": 65532, "runAsGroup": 65532, "runAsNonRoot": true }
                    }
                }),
            )),
        )
        .await
        .expect("create Repository with moverDefaults");

    // (1) BOOTSTRAP GAP FIX: the connect/create Job (`<repo>-bootstrap`) inherits the
    //     repository's moverDefaults. Before ADR-0004 the bootstrap path used a bare
    //     hardened context and ignored moverDefaults entirely.
    let boot = wait_for_job(&jobs, &format!("{repo}-bootstrap")).await;
    assert_eq!(
        job_pod_sc(&boot).and_then(|sc| sc.fs_group),
        Some(65532),
        "bootstrap Job pod must inherit moverDefaults.podSecurityContext.fsGroup (the gap fix)"
    );
    let boot_sc = job_container_sc(&boot).expect("bootstrap container securityContext");
    assert_eq!(
        boot_sc.run_as_user,
        Some(65532),
        "bootstrap Job container must inherit moverDefaults.securityContext.runAsUser"
    );
    assert_hardening_survives(&boot_sc, "bootstrap");

    wait_phase(&repos, repo, "Ready").await.expect(
        "Repository should bootstrap to Ready (proving moverDefaults didn't break bootstrap)",
    );

    // (2) A backup mover whose recipe `mover.securityContext.runAsUser` OVERRIDES
    //     moverDefaults (3000 wins over 65532), while the pod fsGroup still inherits
    //     from moverDefaults (the recipe set no podSecurityContext) and the hardened
    //     drop:[ALL]/seccomp survive the field-wise merge.
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                E2E_NAMESPACE,
                "e2e-md-policy",
                "Repository",
                repo,
                serde_json::json!({
                    "mover": { "securityContext": { "runAsUser": 3000, "runAsNonRoot": true } }
                }),
            )),
        )
        .await
        .expect("create SnapshotPolicy with a recipe mover override");
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-md-backup",
                "e2e-md-policy",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create Snapshot");

    let bjob = wait_for_job(&jobs, "e2e-md-backup").await;
    let bsc = job_container_sc(&bjob).expect("backup container securityContext");
    assert_eq!(
        bsc.run_as_user,
        Some(3000),
        "recipe mover.securityContext.runAsUser (3000) must win over moverDefaults (65532)"
    );
    assert_eq!(
        job_pod_sc(&bjob).and_then(|sc| sc.fs_group),
        Some(65532),
        "backup mover pod must inherit moverDefaults.podSecurityContext.fsGroup (recipe set none)"
    );
    assert_hardening_survives(&bsc, "backup (recipe override)");

    // Cleanup.
    let _ = backups
        .delete("e2e-md-backup", &DeleteParams::default())
        .await;
    let _ = policies
        .delete("e2e-md-policy", &DeleteParams::default())
        .await;
    let _ = repos.delete(repo, &DeleteParams::default()).await;
}

// ---------------------------------------------------------------------------
// (b) onNamespaceDelete Orphan (default) vs Delete — the data-loss-prevention fix
// ---------------------------------------------------------------------------

/// Bootstrap a fresh verifier `Repository` over `subpath` and return its observed
/// `status.storageStats.snapshotCount` (the catalog-scan count). Used after a
/// namespace deletion to prove whether the kopia snapshot survived in the repo.
async fn observed_snapshot_count(client: &Client, verifier: &str, subpath: &str) -> i64 {
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = repos
        .create(
            &PostParams::default(),
            // ReadOnly verifier so it never writes/maintains; create disabled so it
            // only ever CONNECTS to the existing repo and scans its catalog.
            &cr(repository_json(
                verifier,
                subpath,
                serde_json::json!({ "mode": "ReadOnly", "create": { "enabled": false } }),
            )),
        )
        .await;
    wait_phase(&repos, verifier, "Ready")
        .await
        .expect("verifier Repository should connect+scan to Ready");
    // The catalog scan stamps storageStats.snapshotCount; poll until present.
    let count = wait_until(
        &format!("verifier {verifier} reports a snapshotCount"),
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(&repos, verifier).await;
            Ok(s.pointer("/storageStats/snapshotCount")
                .and_then(|v| v.as_i64()))
        },
    )
    .await
    .unwrap_or_else(|_| panic!("verifier {verifier} should report storageStats.snapshotCount"));
    let _ = repos.delete(verifier, &DeleteParams::default()).await;
    count
}

/// Drive a ClusterRepository (with the given `onNamespaceDelete` policy) + a
/// SnapshotPolicy + Snapshot in a fresh workload namespace to Succeeded, delete the
/// namespace, and return the snapshot count observed in the repo afterward.
async fn namespace_delete_scenario(
    client: &Client,
    label: &str,
    subpath: &str,
    on_namespace_delete: &str,
) -> i64 {
    let app_ns = format!("kopiur-e2e-nsdel-{label}");
    let crepo = format!("e2e-nsdel-{label}-crepo");

    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    crepos
        .create(
            &PostParams::default(),
            &cr(cluster_repository_json(
                &crepo,
                subpath,
                serde_json::json!({ "onNamespaceDelete": on_namespace_delete }),
            )),
        )
        .await
        .expect("create ClusterRepository");
    wait_phase(&crepos, &crepo, "Ready")
        .await
        .expect("ClusterRepository should reach Ready");

    ensure_namespace(client, &app_ns)
        .await
        .expect("create workload namespace");
    // The workload namespace needs the repo password Secret (a mover loads it via
    // envFrom; a ClusterRepository's Secret lives in the operator namespace).
    kopiur_e2e::apply_secret(
        client,
        &app_ns,
        CREDS_SECRET,
        &[("KOPIA_PASSWORD", "e2e-test-password-123")],
    )
    .await
    .expect("place creds Secret in workload namespace");
    // And its own source PVC over the shared source hostPath dir.
    ensure_workload_source(client, &app_ns, label).await;

    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), &app_ns);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), &app_ns);
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                &app_ns,
                "nsdel-policy",
                "ClusterRepository",
                &crepo,
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create SnapshotPolicy");
    // deletionPolicy: Delete so the per-Snapshot plan would delete — the namespace
    // cascade policy is what decides whether that plan actually runs.
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                &app_ns,
                "nsdel-backup",
                "nsdel-policy",
                serde_json::json!({ "deletionPolicy": "Delete" }),
            )),
        )
        .await
        .expect("create Snapshot");
    wait_phase(&backups, "nsdel-backup", "Succeeded")
        .await
        .expect("Snapshot should reach Succeeded");

    // Delete the consuming namespace and wait until it is fully gone (the Snapshot's
    // finalizer must run the namespace-delete cascade before the ns is reaped).
    let nss: Api<Namespace> = Api::all(client.clone());
    nss.delete(&app_ns, &DeleteParams::default())
        .await
        .expect("delete workload namespace");
    wait_until(
        &format!("namespace {app_ns} fully deleted"),
        Duration::from_secs(180),
        poll_interval(),
        || async {
            match nss.get_opt(&app_ns).await? {
                Some(_) => Ok(None),
                None => Ok(Some(())),
            }
        },
    )
    .await
    .expect("workload namespace should be fully deleted (finalizers cleared)");

    let count =
        observed_snapshot_count(client, &format!("e2e-nsdel-{label}-verify"), subpath).await;
    let _ = crepos.delete(&crepo, &DeleteParams::default()).await;
    count
}

/// Create a workload-namespace source PVC bound to a fresh hostPath PV over the
/// shared `/kopiur-e2e/src` dir (a hostPath PV binds 1:1 to a PVC).
async fn ensure_workload_source(client: &Client, ns: &str, label: &str) {
    use kopiur_e2e::apply::{Fixture, apply_all};
    use kopiur_e2e::{builders, consts};
    let pv = format!("kopiur-e2e-src-nsdel-{label}");
    let fixtures: Vec<Fixture> = vec![
        builders::hostpath_pv(&pv, consts::HOSTPATH_SRC, "1Gi").into(),
        builders::static_pvc(ns, consts::PVC_SRC, &pv, "1Gi").into(),
    ];
    apply_all(client, &fixtures)
        .await
        .expect("provision workload-namespace source PV/PVC");
}

/// DEFAULT (`Orphan`): deleting the consuming namespace must NOT destroy the kopia
/// snapshot — `kubectl delete ns` no longer loses off-site backup history (the
/// breaking default change, ADR-0005 §5). The snapshot survives in the repo.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn on_namespace_delete_orphan_keeps_snapshot() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    let count = namespace_delete_scenario(&client, "orphan", "nsdel-orphan", "Orphan").await;
    assert!(
        count >= 1,
        "with onNamespaceDelete: Orphan the kopia snapshot must survive a namespace delete, \
         but the repo reports {count} snapshots"
    );
}

/// Opt-in (`Delete`): the cascade honors each `Snapshot`'s own `deletionPolicy`, so a
/// `deletionPolicy: Delete` snapshot IS removed from the repo when the namespace is
/// deleted. Proves the opt-in path actually reclaims storage.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn on_namespace_delete_delete_cascades_snapshot() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    let count = namespace_delete_scenario(&client, "delete", "nsdel-delete", "Delete").await;
    assert_eq!(
        count, 0,
        "with onNamespaceDelete: Delete the snapshot's own deletionPolicy:Delete must cascade, \
         leaving the repo empty, but it reports {count} snapshots"
    );
}

// ---------------------------------------------------------------------------
// (c) Snapshot pin survives GFS prune
// ---------------------------------------------------------------------------

/// A pinned `Snapshot` (`spec.pin: true`, ADR-0005 §13(c)) is EXEMPT from GFS
/// retention: with `keepLatest: 1`, a second (unpinned) snapshot would normally
/// prune the first — but a pinned first snapshot survives while an unpinned older
/// snapshot does not.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn pinned_snapshot_survives_gfs_prune() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();

    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    let repo = "e2e-pin-repo";
    let policy = "e2e-pin-policy";
    repos
        .create(
            &PostParams::default(),
            &cr(repository_json(repo, "pin", serde_json::json!({}))),
        )
        .await
        .expect("create Repository");
    wait_phase(&repos, repo, "Ready").await.expect("repo Ready");
    // keepLatest: 1 so any second snapshot prunes everything but the newest UNLESS pinned.
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                E2E_NAMESPACE,
                policy,
                "Repository",
                repo,
                serde_json::json!({ "retention": { "keepLatest": 1 } }),
            )),
        )
        .await
        .expect("create SnapshotPolicy keepLatest=1");

    // A PINNED first snapshot, and an UNPINNED second snapshot of the same policy.
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-pin-keep",
                policy,
                serde_json::json!({ "pin": true, "deletionPolicy": "Delete" }),
            )),
        )
        .await
        .expect("create pinned Snapshot");
    wait_phase(&backups, "e2e-pin-keep", "Succeeded")
        .await
        .expect("pinned Snapshot Succeeded");
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-pin-prune",
                policy,
                serde_json::json!({ "deletionPolicy": "Delete" }),
            )),
        )
        .await
        .expect("create unpinned Snapshot");
    wait_phase(&backups, "e2e-pin-prune", "Succeeded")
        .await
        .expect("unpinned Snapshot Succeeded");

    // The unpinned older snapshot must be pruned by retention (keepLatest=1)...
    wait_until(
        "unpinned older Snapshot pruned by GFS retention",
        Duration::from_secs(150),
        poll_interval(),
        || async {
            match backups.get_opt("e2e-pin-prune").await? {
                Some(_) => Ok(None),
                None => Ok(Some(())),
            }
        },
    )
    .await
    .expect("the unpinned Snapshot should be pruned by keepLatest=1 retention");

    // ...while the PINNED snapshot survives the same prune.
    let pinned = backups
        .get_opt("e2e-pin-keep")
        .await
        .expect("get pinned Snapshot");
    assert!(
        pinned.is_some(),
        "the pinned Snapshot must survive GFS retention that pruned the unpinned one"
    );

    // Cleanup.
    let _ = backups
        .delete("e2e-pin-keep", &DeleteParams::default())
        .await;
    let _ = policies.delete(policy, &DeleteParams::default()).await;
    let _ = repos.delete(repo, &DeleteParams::default()).await;
}

// ---------------------------------------------------------------------------
// (d) Restore populator target + ReadOnly repo + credentialProjection.allowed +
//     kstatus Ready
// ---------------------------------------------------------------------------

/// Seed a Repository + SnapshotPolicy + Snapshot over `subpath`, returning once the
/// Snapshot has Succeeded (a real snapshot to restore/operate on).
async fn ensure_seed(client: &Client, repo: &str, policy: &str, backup: &str, subpath: &str) {
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    if repos.get_opt(repo).await.ok().flatten().is_none() {
        let _ = repos
            .create(
                &PostParams::default(),
                &cr(repository_json(repo, subpath, serde_json::json!({}))),
            )
            .await;
    }
    wait_phase(&repos, repo, "Ready")
        .await
        .expect("seed repo Ready");
    if policies.get_opt(policy).await.ok().flatten().is_none() {
        let _ = policies
            .create(
                &PostParams::default(),
                &cr(snapshot_policy_json(
                    E2E_NAMESPACE,
                    policy,
                    "Repository",
                    repo,
                    serde_json::json!({}),
                )),
            )
            .await;
    }
    if backups.get_opt(backup).await.ok().flatten().is_none() {
        let _ = backups
            .create(
                &PostParams::default(),
                &cr(snapshot_json(
                    E2E_NAMESPACE,
                    backup,
                    policy,
                    serde_json::json!({}),
                )),
            )
            .await;
    }
    wait_phase(&backups, backup, "Succeeded")
        .await
        .expect("seed Snapshot Succeeded");
}

/// `Restore.spec.target.populator: {}` (ADR-0005 §9): the explicit passive-populator
/// target form is accepted and threads through to a restore mover Job. (The empty
/// `target` form was removed; this proves the replacement form is wired.)
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn restore_populator_target_form_is_accepted() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_seed(
        &client,
        "e2e-pop-repo",
        "e2e-pop-policy",
        "e2e-pop-seed",
        "populator",
    )
    .await;

    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let jobs: Api<Job> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = "e2e-pop-restore";
    restores
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Restore",
                "metadata": { "name": name, "namespace": E2E_NAMESPACE },
                "spec": {
                    "repository": { "kind": "Repository", "name": "e2e-pop-repo" },
                    "source": { "snapshotRef": { "name": "e2e-pop-seed" } },
                    "target": { "populator": {} }
                }
            })),
        )
        .await
        .expect("create Restore with target.populator:{} (the explicit populator form)");

    // The controller accepts the populator target and builds a restore mover Job for it.
    let _ = wait_for_job(&jobs, name).await;
    let _ = restores.delete(name, &DeleteParams::default()).await;
}

/// `mode: ReadOnly` (ADR-0005 §11): a ReadOnly repository serves restores but the
/// controller REFUSES backups against it. A Snapshot whose policy points at a
/// ReadOnly repo must not produce a snapshot; a Restore against it works.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn readonly_repo_refuses_backup_but_allows_restore() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();

    // First seed a snapshot via a READWRITE repo over the subdir, so there is data to
    // restore once we flip a repo to ReadOnly over the same subdir.
    ensure_seed(
        &client,
        "e2e-ro-rw-repo",
        "e2e-ro-rw-policy",
        "e2e-ro-seed",
        "readonly",
    )
    .await;

    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let restores: Api<Restore> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // A ReadOnly repo over the same subdir (create disabled — it already exists).
    let ro_repo = "e2e-ro-repo";
    repos
        .create(
            &PostParams::default(),
            &cr(repository_json(
                ro_repo,
                "readonly",
                serde_json::json!({ "mode": "ReadOnly", "create": { "enabled": false } }),
            )),
        )
        .await
        .expect("create ReadOnly Repository");
    wait_phase(&repos, ro_repo, "Ready")
        .await
        .expect("ReadOnly repo should connect to Ready");

    // A backup against the ReadOnly repo must be refused: it never reaches Succeeded
    // and surfaces a not-Ready/blocked condition rather than writing to the repo.
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                E2E_NAMESPACE,
                "e2e-ro-policy",
                "Repository",
                ro_repo,
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create SnapshotPolicy against ReadOnly repo");
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                E2E_NAMESPACE,
                "e2e-ro-backup",
                "e2e-ro-policy",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create Snapshot against ReadOnly repo");

    // The backup must be refused: phase Failed + RepositoryWritable=False
    // (reason RepositoryReadOnly), and it must never reach Succeeded.
    let cond = wait_condition(&backups, "e2e-ro-backup", "RepositoryWritable", "False")
        .await
        .expect("a Snapshot against a ReadOnly repository must surface RepositoryWritable=False");
    assert_eq!(
        cond.get("reason").and_then(|r| r.as_str()),
        Some("RepositoryReadOnly"),
        "the refusal reason must be RepositoryReadOnly"
    );
    assert_eq!(
        status_json(&backups, "e2e-ro-backup")
            .await
            .get("phase")
            .and_then(|p| p.as_str()),
        Some("Failed"),
        "a refused backup against a ReadOnly repository must be phase Failed"
    );

    // A Restore against the ReadOnly repo WORKS (serves reads): Completed.
    restores
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "Restore",
                "metadata": { "name": "e2e-ro-restore", "namespace": E2E_NAMESPACE },
                "spec": {
                    "repository": { "kind": "Repository", "name": ro_repo },
                    "source": { "snapshotRef": { "name": "e2e-ro-seed" } },
                    "target": { "pvc": { "name": "e2e-ro-dst", "capacity": "1Gi", "accessModes": ["ReadWriteOnce"] } }
                }
            })),
        )
        .await
        .expect("create Restore against ReadOnly repo");
    wait_phase(&restores, "e2e-ro-restore", "Completed")
        .await
        .expect("a Restore against a ReadOnly repository must Complete (it serves reads)");

    // Cleanup.
    let _ = restores
        .delete("e2e-ro-restore", &DeleteParams::default())
        .await;
    let _ = backups
        .delete("e2e-ro-backup", &DeleteParams::default())
        .await;
    let _ = policies
        .delete("e2e-ro-policy", &DeleteParams::default())
        .await;
    let _ = repos.delete(ro_repo, &DeleteParams::default()).await;
}

/// `credentialProjection.allowed` fail-closed gate (ADR-0005 §8): a consumer that
/// opts in (`credentialProjection.enabled: true`) but whose `ClusterRepository`
/// owner has NOT set `credentialProjection.allowed: true` must be refused — the
/// Snapshot blocks on `CredentialsAvailable=False` naming the unmet owner gate, and
/// never launches a mover. (The projection-ON happy path is covered in
/// credential_projection.rs, where the owner allows it.)
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn credential_projection_fails_closed_when_owner_disallows() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();

    const APP_NS: &str = "kopiur-e2e-projgate";
    let crepo = "e2e-projgate-crepo";

    // A ClusterRepository that does NOT allow projection (allowed defaults to false).
    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    crepos
        .create(
            &PostParams::default(),
            &cr(cluster_repository_json(
                crepo,
                "projgate",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create ClusterRepository (credentialProjection.allowed defaults false)");
    wait_phase(&crepos, crepo, "Ready")
        .await
        .expect("ClusterRepository Ready");

    // A workload namespace WITHOUT the creds Secret, and its own source PVC.
    ensure_namespace(&client, APP_NS)
        .await
        .expect("create workload namespace");
    ensure_workload_source(&client, APP_NS, "projgate").await;

    // A SnapshotPolicy that OPTS IN to projection (enabled: true) — but the owner
    // disallows it, so it must fail closed.
    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), APP_NS);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), APP_NS);
    policies
        .create(
            &PostParams::default(),
            &cr(snapshot_policy_json(
                APP_NS,
                "e2e-projgate-policy",
                "ClusterRepository",
                crepo,
                serde_json::json!({ "credentialProjection": { "enabled": true } }),
            )),
        )
        .await
        .expect("create SnapshotPolicy opting into projection");
    backups
        .create(
            &PostParams::default(),
            &cr(snapshot_json(
                APP_NS,
                "e2e-projgate-backup",
                "e2e-projgate-policy",
                serde_json::json!({}),
            )),
        )
        .await
        .expect("create Snapshot");

    // Fail closed: CredentialsAvailable=False, message names the owner-allow gate.
    let cond = wait_condition(
        &backups,
        "e2e-projgate-backup",
        "CredentialsAvailable",
        "False",
    )
    .await
    .expect("projection must fail closed when the ClusterRepository owner disallows it");
    let msg = cond.get("message").and_then(|m| m.as_str()).unwrap_or("");
    assert!(
        msg.contains("credentialProjection.allowed"),
        "the fail-closed message must point the user at the owner-allow gate; got: {msg}"
    );
    // It must NOT have launched a mover / Succeeded.
    let phase = status_json(&backups, "e2e-projgate-backup")
        .await
        .get("phase")
        .and_then(|p| p.as_str())
        .unwrap_or("")
        .to_string();
    assert_ne!(
        phase, "Succeeded",
        "a fail-closed Snapshot must not have succeeded"
    );

    // Cleanup.
    let _ = crepos.delete(crepo, &DeleteParams::default()).await;
    let nss: Api<Namespace> = Api::all(client.clone());
    let _ = nss.delete(APP_NS, &DeleteParams::default()).await;
}

/// kstatus `Ready` (ADR-0005 §2): once a Repository is Ready and a SnapshotPolicy is
/// reconciled, the SnapshotPolicy carries a `Ready=True` condition AND a Succeeded
/// Snapshot does too — so `kubectl wait --for=condition=Ready` (and Flux/Argo
/// health) work. We assert the condition the way `kubectl wait` reads it.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn kstatus_ready_condition_present_for_wait() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    ensure_seed(
        &client,
        "e2e-ready-repo",
        "e2e-ready-policy",
        "e2e-ready-seed",
        "kstatus",
    )
    .await;

    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let backups: Api<Snapshot> = Api::namespaced(client.clone(), E2E_NAMESPACE);

    // The SnapshotPolicy reaches Ready=True (its Repository is Ready, retention enforced).
    wait_condition(&policies, "e2e-ready-policy", "Ready", "True")
        .await
        .expect(
            "SnapshotPolicy must carry Ready=True so `kubectl wait --for=condition=Ready` works",
        );
    // A Succeeded Snapshot also carries Ready=True (kstatus on the data resource).
    wait_condition(&backups, "e2e-ready-seed", "Ready", "True")
        .await
        .expect("a Succeeded Snapshot must carry Ready=True");

    // Cleanup leaves the seed for reuse (E2E_NAMESPACE persists); nothing to delete.
}

// ---------------------------------------------------------------------------
// (e) verification (successExpr) + RepositoryReplication
// ---------------------------------------------------------------------------

/// Verification (ADR-0005 §4): a `SnapshotPolicy.spec.verification.quick` with an
/// every-minute cron and a `successExpr` over the result drives a `kopia snapshot
/// verify` mover Job; on success the controller stamps `status.lastVerified`.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn verification_quick_with_success_expr_stamps_last_verified() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    // Seed a real snapshot so quick-verify has something to verify.
    ensure_seed(
        &client,
        "e2e-verify-repo",
        "e2e-verify-policy",
        "e2e-verify-seed",
        "verify",
    )
    .await;

    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    // Patch verification onto the existing policy: every-minute quick verify, gated by
    // a successExpr asserting the verify reported zero errors.
    let patch = serde_json::json!({
        "spec": { "verification": {
            "quick": { "cron": "* * * * *" },
            "successExpr": "stats.errors == 0"
        } }
    });
    policies
        .patch(
            "e2e-verify-policy",
            &kube::api::PatchParams::default(),
            &kube::api::Patch::Merge(&patch),
        )
        .await
        .expect("patch verification onto the SnapshotPolicy");

    // Within a couple of minutes a quick-verify Job runs and stamps lastVerified.
    wait_until(
        "SnapshotPolicy.status.lastVerified is stamped by a passing quick verify",
        Duration::from_secs(240),
        poll_interval(),
        || async {
            let s = status_json(&policies, "e2e-verify-policy").await;
            Ok(s.get("lastVerified")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|_| ()))
        },
    )
    .await
    .expect(
        "a passing quick verify (successExpr stats.errors == 0) must stamp status.lastVerified",
    );
}

/// `RepositoryReplication` (ADR-0005 §13(d)): mirror a source filesystem repo to a
/// SECOND filesystem repo on a schedule (`kopia repository sync-to`). An
/// every-minute schedule drives a replication mover Job; on success the controller
/// records `status.lastReplicated`. We then verify the destination actually received
/// the snapshot by connecting a verifier Repository to it.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn repository_replication_mirrors_to_second_filesystem_repo() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    // A source repo with a real snapshot to mirror.
    ensure_seed(
        &client,
        "e2e-repl-src",
        "e2e-repl-policy",
        "e2e-repl-seed",
        "repl-src",
    )
    .await;

    let repls: Api<RepositoryReplication> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = "e2e-repl";
    // Mirror to a second filesystem path (the destination reuses the source password,
    // so destinationEncryption is omitted — the common true-mirror case).
    repls
        .create(
            &PostParams::default(),
            &cr(serde_json::json!({
                "apiVersion": "kopiur.home-operations.com/v1alpha1",
                "kind": "RepositoryReplication",
                "metadata": { "name": name, "namespace": E2E_NAMESPACE },
                "spec": {
                    "sourceRef": { "kind": "Repository", "name": "e2e-repl-src" },
                    "destination": { "filesystem": { "path": "/repo/repl-dst", "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
                    "schedule": { "cron": "* * * * *" }
                }
            })),
        )
        .await
        .expect("create RepositoryReplication to a second filesystem repo");

    // Within a couple of minutes a replication runs and records lastReplicated.
    wait_until(
        "RepositoryReplication records a successful run (status.lastReplicated)",
        Duration::from_secs(240),
        poll_interval(),
        || async {
            let s = status_json(&repls, name).await;
            Ok(s.get("lastReplicated")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|_| ()))
        },
    )
    .await
    .expect("the replication should run and stamp status.lastReplicated");

    // The destination repo must now hold the mirrored snapshot: connect a verifier.
    let count = observed_snapshot_count(&client, "e2e-repl-verify", "repl-dst").await;
    assert!(
        count >= 1,
        "the destination repository must hold the mirrored snapshot, got {count}"
    );

    let _ = repls.delete(name, &DeleteParams::default()).await;
}

/// Compile-time guard that `Client` is reachable even when the `e2e` feature gates
/// the bodies above.
#[allow(dead_code)]
fn _type_anchor(c: Client) {
    let _ = c.default_namespace();
}
