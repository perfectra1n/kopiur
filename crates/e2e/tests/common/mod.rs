//! Shared helpers for the ADR-0004 / ADR-0005 reshape e2e scenarios.
//!
//! Split across several purpose-named test files (`mover_config`, `data_integrity`,
//! `repository_lifecycle`, `tenancy`, `verification`, `replication`); each includes
//! this module with `mod common;`. As a subdirectory module (`tests/common/mod.rs`)
//! cargo does NOT compile it as its own test binary.
//!
//! ## Repo isolation (load-bearing)
//!
//! The operator mounts a filesystem repo's PVC at `backend.path` AND runs
//! `kopia --path=backend.path`, so the PVC *root* IS the kopia repo. A `path` subdir
//! under one shared PVC therefore does NOT isolate repos — every scenario would
//! collide on the same kopia repo at the PVC root. So each scenario binds its OWN
//! per-`subpath` PV/PVC (over `/kopiur-e2e/repos/<subpath>`, seeded 0777 by the mise
//! `e2e-node-seed` task) via [`ensure_repo`], using the fixed in-pod mount path
//! [`consts::ISOLATED_REPO_PATH`]. Keep `REPO_SUBPATHS` (consts) and the node-seed
//! list in lockstep.

#![allow(dead_code)]

use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};
use serde::Serialize;
use serde::de::DeserializeOwned;

use k8s_openapi::api::batch::v1::Job;

use kopiur_api::{Repository, Snapshot, SnapshotPolicy};
use kopiur_e2e::{E2E_NAMESPACE, consts, default_timeout, poll_interval, wait_until};

/// The repository password Secret the chart-installed operator reads.
pub const CREDS_SECRET: &str = "kopia-creds";

/// Deserialize a CR from a JSON literal into its typed kube object.
pub fn cr<T: DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("CR JSON deserializes into typed object")
}

/// Provision the isolated per-`subpath` repo PV/PVC (idempotent), so a filesystem
/// `Repository`/`ClusterRepository` over `subpath` gets its OWN kopia repo (the PVC
/// root). See the module docs for why a shared PVC + `path` subdir does not isolate.
pub async fn ensure_repo(client: &Client, subpath: &str) {
    use kopiur_e2e::apply::{Fixture, apply_all};
    use kopiur_e2e::builders;
    let pv = consts::isolated_repo_pv(subpath);
    let pvc = consts::isolated_repo_pvc(subpath);
    let host = consts::isolated_repo_hostpath(subpath);
    let fixtures: Vec<Fixture> = vec![
        builders::hostpath_pv(&pv, &host, "1Gi").into(),
        builders::static_pvc(E2E_NAMESPACE, &pvc, &pv, "1Gi").into(),
    ];
    apply_all(client, &fixtures)
        .await
        .unwrap_or_else(|e| panic!("provision isolated repo PV/PVC for subpath {subpath}: {e}"));
}

/// A namespaced filesystem `Repository` over its OWN isolated repo hostPath (keyed by
/// `subpath`), with the given extra `spec` fields merged in. Callers must
/// [`ensure_repo`]`(subpath)` first.
pub fn repository_json(
    name: &str,
    subpath: &str,
    extra_spec: serde_json::Value,
) -> serde_json::Value {
    merge_spec(
        serde_json::json!({
            "apiVersion": "kopiur.home-operations.com/v1alpha1",
            "kind": "Repository",
            "metadata": { "name": name, "namespace": E2E_NAMESPACE },
            "spec": {
                "backend": { "filesystem": { "path": consts::ISOLATED_REPO_PATH, "volume": { "pvc": { "name": consts::isolated_repo_pvc(subpath) } } } },
                "encryption": { "passwordSecretRef": { "name": CREDS_SECRET, "key": "KOPIA_PASSWORD" } },
                "create": { "enabled": true }
            }
        }),
        extra_spec,
    )
}

/// A cluster-scoped filesystem `ClusterRepository` over its OWN isolated repo hostPath
/// (keyed by `subpath`), opened to all namespaces, with extra `spec` fields merged in.
/// Callers must [`ensure_repo`]`(subpath)` first.
pub fn cluster_repository_json(
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
                "backend": { "filesystem": { "path": consts::ISOLATED_REPO_PATH, "volume": { "pvc": { "name": consts::isolated_repo_pvc(subpath) } } } },
                "encryption": { "passwordSecretRef": { "name": CREDS_SECRET, "namespace": E2E_NAMESPACE, "key": "KOPIA_PASSWORD" } },
                "create": { "enabled": true },
                "allowedNamespaces": { "all": true }
            }
        }),
        extra_spec,
    )
}

/// Merge `extra` (an object) into `base["spec"]`.
pub fn merge_spec(mut base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    if let (Some(spec), serde_json::Value::Object(more)) = (base.get_mut("spec"), extra) {
        let serde_json::Value::Object(s) = spec else {
            panic!("spec must be an object");
        };
        s.extend(more);
    }
    base
}

/// A `SnapshotPolicy` over the shared `e2e-src` source PVC, referencing `repo`.
pub fn snapshot_policy_json(
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

/// A `Snapshot` referencing `policy` (default `deletionPolicy: Retain`).
pub fn snapshot_json(
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
pub async fn wait_phase<K>(api: &Api<K>, name: &str, want_phase: &str) -> anyhow::Result<()>
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
pub async fn status_json<K>(api: &Api<K>, name: &str) -> serde_json::Value
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
pub async fn wait_condition<K>(
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
pub async fn wait_for_job(jobs: &Api<Job>, name: &str) -> Job {
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
pub fn job_container_sc(job: &Job) -> Option<k8s_openapi::api::core::v1::SecurityContext> {
    job.spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.containers.first())
        .and_then(|c| c.security_context.clone())
}

/// The mover pod-level `securityContext` from a Job's pod template.
pub fn job_pod_sc(job: &Job) -> Option<k8s_openapi::api::core::v1::PodSecurityContext> {
    job.spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|p| p.security_context.clone())
}

/// Assert a rendered mover container securityContext STILL carries the hardened
/// `capabilities.drop:[ALL]` + `seccompProfile: RuntimeDefault` — the de-hardening
/// regression guard (ADR-0004 §2): a partial `moverDefaults`/`mover` override must
/// never wipe the hardened base.
pub fn assert_hardening_survives(sc: &k8s_openapi::api::core::v1::SecurityContext, ctx: &str) {
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

/// Bootstrap a fresh ReadOnly verifier `Repository` over `subpath` and return its
/// observed `status.storageStats.snapshotCount` (the catalog-scan count). Used to
/// prove whether a kopia snapshot exists in the repo.
pub async fn observed_snapshot_count(client: &Client, verifier: &str, subpath: &str) -> i64 {
    // The verifier connects to the SAME isolated repo dir as the writer (same subpath
    // ⇒ same PVC); ensure it exists (idempotent) in case the verifier runs first.
    ensure_repo(client, subpath).await;
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let _ = repos
        .create(
            &PostParams::default(),
            // ReadOnly + create disabled: only ever CONNECTS to the existing repo and
            // scans its catalog (never writes/maintains).
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

/// Create a workload-namespace source PVC bound to a fresh hostPath PV over the shared
/// `/kopiur-e2e/src` dir (a hostPath PV binds 1:1 to a PVC).
pub async fn ensure_workload_source(client: &Client, ns: &str, label: &str) {
    use kopiur_e2e::apply::{Fixture, apply_all};
    use kopiur_e2e::builders;
    let pv = format!("kopiur-e2e-src-{label}");
    let fixtures: Vec<Fixture> = vec![
        builders::hostpath_pv(&pv, consts::HOSTPATH_SRC, "1Gi").into(),
        builders::static_pvc(ns, consts::PVC_SRC, &pv, "1Gi").into(),
    ];
    apply_all(client, &fixtures)
        .await
        .expect("provision workload-namespace source PV/PVC");
}

/// Seed a `Repository` + `SnapshotPolicy` + `Snapshot` over `subpath`, returning once
/// the Snapshot has Succeeded (a real snapshot to restore/operate on).
pub async fn ensure_seed(client: &Client, repo: &str, policy: &str, backup: &str, subpath: &str) {
    ensure_repo(client, subpath).await;
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
