//! RBAC manifest generation (ADR-0003 §4.12).
//!
//! The operator's permissions are expressed as a set of `PolicyRule`s, each
//! block documented inline. We emit two install flavours.
//!
//! Cluster-scoped (`deploy/rbac/operator-clusterrole.yaml`): a `ClusterRole` +
//! `ServiceAccount` + `ClusterRoleBinding` for the default cluster-wide install.
//! Includes the cluster-scoped `clusterrepositories` CRD and the
//! `serviceaccounts` create/get used to mint per-namespace mover SAs.
//!
//! Namespaced (`deploy/rbac/operator-role.yaml`): a `Role` + `ServiceAccount` +
//! `RoleBinding` for the namespaced-install mode (§4.11/§4.12), with the same
//! rules minus the cluster-scoped bits (`clusterrepositories`, `serviceaccounts`
//! minting) that don't apply when the operator is confined to a single
//! namespace.
//!
//! k8s-openapi structs don't carry `apiVersion`/`kind` as fields, so we
//! serialize to a `serde_json::Value` and splice those in from the `Resource`
//! trait constants before rendering YAML — exactly the shape `kubectl apply`
//! expects.

use anyhow::{Context, Result};
use k8s_openapi::Resource;
use k8s_openapi::api::core::v1::ServiceAccount;
use k8s_openapi::api::rbac::v1::{
    ClusterRole, ClusterRoleBinding, PolicyRule, Role, RoleBinding, RoleRef, Subject,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::Serialize;

use crate::artifact::{Artifact, RBAC_HEADER};

/// Operator identity used across the generated manifests.
const SA_NAME: &str = "kopiur-controller";
const CLUSTERROLE_NAME: &str = "kopiur-controller";
const ROLE_NAME: &str = "kopiur-controller";
const DEFAULT_NAMESPACE: &str = "kopiur-system";

const KOPIA_GROUP: &str = "kopiur.home-operations.com";

/// All 7 CRD plurals in `kopiur.home-operations.com`. `clusterrepositories` is cluster-scoped.
const NAMESPACED_CRDS: &[&str] = &[
    "repositories",
    "backupconfigs",
    "backups",
    "backupschedules",
    "restores",
    "maintenances",
];
const CLUSTER_CRDS: &[&str] = &["clusterrepositories"];

fn verbs(vs: &[&str]) -> Vec<String> {
    vs.iter().map(|s| s.to_string()).collect()
}

fn rule(api_groups: &[&str], resources: &[String], vs: &[&str]) -> PolicyRule {
    PolicyRule {
        api_groups: Some(api_groups.iter().map(|s| s.to_string()).collect()),
        resources: Some(resources.to_vec()),
        verbs: verbs(vs),
        ..Default::default()
    }
}

const FULL_VERBS: &[&str] = &[
    "get", "list", "watch", "create", "update", "patch", "delete",
];
const SUBRESOURCE_VERBS: &[&str] = &["get", "update", "patch"];
const READ_VERBS: &[&str] = &["get", "list", "watch"];

/// Build the `kopiur.home-operations.com` CRD rules shared by both flavours.
///
/// `include_cluster_crds` adds the cluster-scoped `clusterrepositories` CRD,
/// which only belongs in the cluster-scoped `ClusterRole`.
fn kopia_crd_rules(include_cluster_crds: bool) -> Vec<PolicyRule> {
    let mut crds: Vec<&str> = NAMESPACED_CRDS.to_vec();
    if include_cluster_crds {
        crds.extend_from_slice(CLUSTER_CRDS);
    }

    // Primary resources: full CRUD.
    let primary: Vec<String> = crds.iter().map(|s| s.to_string()).collect();
    // Subresources: status + finalizers, get/update/patch only.
    let mut sub: Vec<String> = Vec::with_capacity(crds.len() * 2);
    for c in &crds {
        sub.push(format!("{c}/status"));
        sub.push(format!("{c}/finalizers"));
    }

    vec![
        rule(&[KOPIA_GROUP], &primary, FULL_VERBS),
        rule(&[KOPIA_GROUP], &sub, SUBRESOURCE_VERBS),
    ]
}

/// Core / batch / snapshot rules the controller needs to drive movers, hooks,
/// jobs, and CSI snapshots (§4.12). `include_sa_minting` controls whether the
/// `serviceaccounts` create/get block (per-namespace mover SA minting) is
/// included — only meaningful cluster-wide.
fn workload_rules(include_sa_minting: bool) -> Vec<PolicyRule> {
    let mut rules = vec![
        // Mover pods + exec for pre/post hooks; PVCs for snapshot/restore I/O;
        // events for surfacing reconcile outcomes; configmaps carry the mover
        // work spec AND (for repository bootstrap) receive the mover's result
        // patch — the mover runs as this SA, so its result write reuses these
        // configmaps verbs (no separate rule needed).
        rule(
            &[""],
            &[
                "pods".into(),
                "persistentvolumeclaims".into(),
                "configmaps".into(),
            ],
            FULL_VERBS,
        ),
        // Hook execution into running workload pods.
        rule(&[""], &["pods/exec".into()], &["create", "get"]),
        // Surface reconcile outcomes as Kubernetes Events. kube's `Recorder`
        // writes the modern `events.k8s.io/v1` Event (not the legacy core Event),
        // so BOTH api groups are required — the core `""` group alone yields a 403
        // on create and the Event is silently dropped.
        rule(&[""], &["events".into()], &["create", "patch"]),
        rule(&["events.k8s.io"], &["events".into()], &["create", "patch"]),
        // Secrets hold repository credentials — read-only, never written.
        rule(&[""], &["secrets".into()], READ_VERBS),
        // Mover Jobs.
        rule(&["batch"], &["jobs".into()], FULL_VERBS),
        // CSI volume snapshots used as a consistent source for snapshotting.
        rule(
            &["snapshot.storage.k8s.io"],
            &["volumesnapshots".into()],
            &["get", "list", "watch", "create", "delete"],
        ),
        rule(
            &["groupsnapshot.storage.k8s.io"],
            &["volumegroupsnapshots".into()],
            &["get", "list", "watch", "create", "delete"],
        ),
    ];

    if include_sa_minting {
        // Per-namespace mover ServiceAccount minted by the controller (§4.12).
        rules.push(rule(&[""], &["serviceaccounts".into()], &["create", "get"]));
    }

    rules
}

/// Splice `apiVersion`/`kind` into a serialized k8s-openapi object and render
/// it as a YAML document body (no leading header).
fn render<T: Serialize + Resource>(obj: &T) -> Result<String> {
    let mut value = serde_json::to_value(obj).context("serializing RBAC object")?;
    let map = value
        .as_object_mut()
        .context("RBAC object did not serialize to a JSON object")?;
    map.insert("apiVersion".into(), T::API_VERSION.into());
    map.insert("kind".into(), T::KIND.into());
    let yaml = serde_yaml::to_string(&value).context("rendering RBAC object to YAML")?;
    Ok(yaml)
}

/// Join several rendered documents with `---` under a single header.
fn document(parts: &[String]) -> String {
    let body = parts
        .iter()
        .map(|p| p.trim_end())
        .collect::<Vec<_>>()
        .join("\n---\n");
    format!("{RBAC_HEADER}{body}\n")
}

fn metadata(name: &str, namespace: Option<&str>) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: namespace.map(str::to_string),
        labels: Some(std::collections::BTreeMap::from([(
            "app.kubernetes.io/name".to_string(),
            "kopiur".to_string(),
        )])),
        ..Default::default()
    }
}

/// Generate the cluster-scoped RBAC artifact.
fn cluster_artifact() -> Result<Artifact> {
    let mut rules = kopia_crd_rules(true);
    rules.extend(workload_rules(true));

    let clusterrole = ClusterRole {
        metadata: metadata(CLUSTERROLE_NAME, None),
        rules: Some(rules),
        ..Default::default()
    };

    let sa = ServiceAccount {
        metadata: metadata(SA_NAME, Some(DEFAULT_NAMESPACE)),
        ..Default::default()
    };

    let binding = ClusterRoleBinding {
        metadata: metadata(CLUSTERROLE_NAME, None),
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "ClusterRole".to_string(),
            name: CLUSTERROLE_NAME.to_string(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: SA_NAME.to_string(),
            namespace: Some(DEFAULT_NAMESPACE.to_string()),
            api_group: None,
        }]),
    };

    let content = document(&[render(&sa)?, render(&clusterrole)?, render(&binding)?]);
    Ok(Artifact::new(
        "rbac/operator-clusterrole.yaml".to_string(),
        content,
    ))
}

/// Generate the namespaced RBAC artifact (§4.11/§4.12 namespaced-install mode).
fn namespaced_artifact() -> Result<Artifact> {
    // No cluster-scoped clusterrepositories; no cluster-wide SA minting.
    let mut rules = kopia_crd_rules(false);
    rules.extend(workload_rules(false));

    let role = Role {
        metadata: metadata(ROLE_NAME, Some(DEFAULT_NAMESPACE)),
        rules: Some(rules),
    };

    let sa = ServiceAccount {
        metadata: metadata(SA_NAME, Some(DEFAULT_NAMESPACE)),
        ..Default::default()
    };

    let binding = RoleBinding {
        metadata: metadata(ROLE_NAME, Some(DEFAULT_NAMESPACE)),
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "Role".to_string(),
            name: ROLE_NAME.to_string(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: SA_NAME.to_string(),
            namespace: Some(DEFAULT_NAMESPACE.to_string()),
            api_group: None,
        }]),
    };

    let content = document(&[render(&sa)?, render(&role)?, render(&binding)?]);
    Ok(Artifact::new(
        "rbac/operator-role.yaml".to_string(),
        content,
    ))
}

/// All RBAC artifacts.
pub fn artifacts() -> Result<Vec<Artifact>> {
    Ok(vec![cluster_artifact()?, namespaced_artifact()?])
}
