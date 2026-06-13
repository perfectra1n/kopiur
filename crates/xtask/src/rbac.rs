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

/// The dedicated, least-privilege mover role (§4.12). Bound to a per-namespace
/// `kopiur-mover` ServiceAccount by a controller-minted RoleBinding; the SA and
/// binding are created at runtime, so only the role itself is shipped here.
const MOVER_CLUSTERROLE_NAME: &str = "kopiur-mover";
const MOVER_ROLE_NAME: &str = "kopiur-mover";

/// CRD plurals whose `.status` the mover PATCHes (the dynamic `targetRef` kinds).
/// Deliberately excludes `snapshotschedules`, which the mover never touches.
const MOVER_STATUS_CRDS: &[&str] = &[
    "snapshots",
    "restores",
    "repositories",
    "clusterrepositories",
    "maintenances",
    // The replication mover PATCHes RepositoryReplication/status (ADR-0005 §13(d)),
    // and the verify mover PATCHes snapshotpolicies/status (ADR-0005 §4) — both
    // covered here.
    "snapshotpolicies",
    "repositoryreplications",
];

const KOPIA_GROUP: &str = "kopiur.home-operations.com";

/// Default names of the webhook configurations and serving Secret for the
/// self-managed-TLS RBAC (`webhook.tls.mode: self`). These match the chart's
/// helpers for the default release: `{fullname}-validating`/`-mutating` and the
/// `webhook.tls.secretName` default. The Helm templates re-derive them from the
/// release; the shipped static artifacts use the default-release names.
const WEBHOOK_VALIDATING_CONFIG: &str = "kopiur-validating";
const WEBHOOK_MUTATING_CONFIG: &str = "kopiur-mutating";
const WEBHOOK_TLS_SECRET: &str = "kopiur-webhook-tls";

/// All 8 CRD plurals in `kopiur.home-operations.com`. `clusterrepositories` is cluster-scoped.
const NAMESPACED_CRDS: &[&str] = &[
    "repositories",
    "snapshotpolicies",
    "snapshots",
    "snapshotschedules",
    "restores",
    "maintenances",
    "repositoryreplications",
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

/// Like [`rule`] but scoped to specific `resourceNames` (least privilege). Use
/// only with verbs that honor names (`get`/`update`/`patch`/`delete`) — `create`,
/// `list`, and `watch` are NOT name-scopable and would be denied.
fn rule_named(
    api_groups: &[&str],
    resources: &[String],
    vs: &[&str],
    names: &[String],
) -> PolicyRule {
    PolicyRule {
        api_groups: Some(api_groups.iter().map(|s| s.to_string()).collect()),
        resources: Some(resources.to_vec()),
        verbs: verbs(vs),
        resource_names: Some(names.to_vec()),
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
/// jobs, and CSI snapshots (§4.12).
///
/// Includes the `serviceaccounts` + `rolebindings` minting rules: before every
/// mover Job the controller ensures a least-privilege `kopiur-mover`
/// ServiceAccount and a RoleBinding (to the `kopiur-mover` ClusterRole/Role)
/// exist in the Job's namespace — the workload namespace, which differs from the
/// operator's. Without this the mover Job's SA does not exist in that namespace
/// and the Job never schedules a pod (`FailedCreate: serviceaccount ... not
/// found`). The controller already holds a superset of the mover role's perms, so
/// creating the binding is not a privilege escalation (no `bind` verb needed).
fn workload_rules() -> Vec<PolicyRule> {
    vec![
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
        // Secrets hold repository credentials. Read is always needed; create+patch
        // back the opt-in credential projection (`spec.credentialProjection`), where
        // the controller copies a repository's Secret(s) into each mover Job's
        // namespace via server-side apply (a PATCH). `create` cannot be
        // resourceName-scoped (the authorizer can't match a name at create time) and
        // the projected name is per-Job, so this is necessarily unscoped. No
        // `delete`: projected copies carry an ownerRef and are reaped by GC. The
        // Helm chart gates create/patch behind `secretProjection.enabled`.
        // Secrets: read repository credentials, create+patch the opt-in credential
        // projection AND the operator-owned kopia web-UI auth Secret (`spec.server`
        // Generate mode) + the cross-namespace credentials mirror for
        // ClusterRepository servers, and `delete` them on server teardown / namespace
        // migration (owner-ref GC can't reach a cluster-scoped owner's children).
        // FULL_VERBS is a deliberate escalation over the previous read+create+patch
        // grant (§ server addendum).
        rule(&[""], &["secrets".into()], FULL_VERBS),
        // Services exposing the kopia web-UI server (`spec.server`).
        rule(&[""], &["services".into()], FULL_VERBS),
        // Mover Jobs and the kopia web-UI server Deployment (`spec.server`).
        rule(&["batch"], &["jobs".into()], FULL_VERBS),
        rule(&["apps"], &["deployments".into()], FULL_VERBS),
        // CSI volume snapshots used as a consistent source for snapshotting
        // (`copyMethod: Snapshot`/`Clone`, ADR §3.3) — namespaced, so they live here.
        // `patch` backs the server-side apply the controller uses to (idempotently)
        // create the staged VolumeSnapshot. The cluster-scoped VolumeSnapshotClasses /
        // VolumeSnapshotContents / StorageClasses are granted in the ClusterRole only.
        rule(
            &["snapshot.storage.k8s.io"],
            &["volumesnapshots".into()],
            &["get", "list", "watch", "create", "patch", "delete"],
        ),
        rule(
            &["groupsnapshot.storage.k8s.io"],
            &["volumegroupsnapshots".into()],
            &["get", "list", "watch", "create", "delete"],
        ),
        // Per-namespace mover RBAC minted by the controller (§4.12): a
        // `kopiur-mover` ServiceAccount plus a RoleBinding to the `kopiur-mover`
        // role, created in each mover Job's namespace. `io::ensure_mover_rbac`
        // mints via server-side apply (a PATCH), so `patch` (and `update`) are
        // REQUIRED alongside `create`/`get` — without `patch` the apply is 403'd
        // and the SA is never minted, so the mover Job FailedCreates.
        // `list`/`watch` added for workload identity: the Repository /
        // ClusterRepository controllers watch ServiceAccounts so creating the
        // `auth.workloadIdentity` SA un-sticks a blocked repo immediately.
        rule(
            &[""],
            &["serviceaccounts".into()],
            &["get", "list", "watch", "create", "update", "patch"],
        ),
        rule(
            &["rbac.authorization.k8s.io"],
            &["rolebindings".into()],
            &["get", "create", "update", "patch"],
        ),
    ]
}

/// RBAC for self-managed webhook TLS (`webhook.tls.mode: self`): the controller
/// mints its own serving certificate and injects the `caBundle` into its webhook
/// configurations, removing the cert-manager dependency. Carried by both install
/// flavours (admission configs are cluster-scoped; the Secret is namespace-local);
/// the chart only grants these in `self` mode.
///
/// The **cluster-scoped** half: `get`+`patch` on the two webhook configurations,
/// scoped by `resourceName`. The controller GETs each by name and patches its
/// `caBundle` (no `list`/`watch` needed — and those can't be name-scoped anyway).
/// Webhook configurations are cluster-scoped, so this only belongs in a
/// `ClusterRole`; a namespaced install pairs its Role with a small ClusterRole
/// the chart renders for `self` mode.
fn webhook_cert_cluster_rules() -> Vec<PolicyRule> {
    vec![rule_named(
        &["admissionregistration.k8s.io"],
        &[
            "validatingwebhookconfigurations".into(),
            "mutatingwebhookconfigurations".into(),
        ],
        &["get", "patch"],
        &[
            WEBHOOK_VALIDATING_CONFIG.into(),
            WEBHOOK_MUTATING_CONFIG.into(),
        ],
    )]
}

/// The **namespace-local** half: writing the serving Secret. `create` is unscoped
/// because the authorizer cannot match a `resourceName` at create time; the
/// rotation re-apply (`update`/`patch`) is scoped to the serving Secret by name.
/// Reads come from the existing `secrets` `get`/`list`/`watch` rule. Valid in
/// both a Role and a ClusterRole.
fn webhook_cert_secret_rules() -> Vec<PolicyRule> {
    vec![
        rule(&[""], &["secrets".into()], &["create"]),
        rule_named(
            &[""],
            &["secrets".into()],
            &["update", "patch"],
            &[WEBHOOK_TLS_SECRET.into()],
        ),
    ]
}

/// The minimal RBAC a mover Job actually uses, verified against `crates/mover/src/`:
/// it PATCHes the owning CR's `.status` subresource (the target kind is dynamic —
/// `backups`, `restores`, `repositories`, `clusterrepositories`, `maintenances`)
/// and PATCHes the work-spec `ConfigMap` to write the repository-bootstrap result.
/// Nothing else — credentials are env-mounted (no `secrets` access) and the work
/// spec is a mounted volume (no `configmaps` get for it). This is deliberately a
/// tiny subset of [`workload_rules`]; the mover runs as its own least-privilege SA.
fn mover_rules() -> Vec<PolicyRule> {
    let statuses: Vec<String> = MOVER_STATUS_CRDS
        .iter()
        .map(|c| format!("{c}/status"))
        .collect();
    vec![
        rule(&[KOPIA_GROUP], &statuses, &["get", "patch"]),
        rule(&[""], &["configmaps".into()], &["get", "patch"]),
    ]
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
    rules.extend(workload_rules());
    // Read Namespaces to check the privileged-movers opt-in annotation (ADR
    // §4.11/§G16). Cluster-scoped resource → cluster install only; a namespaced
    // install can't grant it and the controller fails the check open there.
    rules.push(rule(&[""], &["namespaces".into()], READ_VERBS));
    // RWO Multi-Attach avoidance: discover the node a ReadWriteOnce source/
    // destination PVC is attached to so the mover can be pinned there. The bound
    // PV's `nodeAffinity` (topology-pinned volumes) and the CSI `VolumeAttachment`
    // (ground-truth attached node) are read-only fallbacks when no consuming pod is
    // found. `patch` lets staging flip a staged PVC's bound PV from a `Retain` reclaim
    // policy to `Delete` before deleting it, so a `Retain` StorageClass doesn't leak the
    // PV + backend volume (ADR §3.3). All cluster-scoped → cluster install only (a
    // namespaced install can't grant them; co-location then relies on the
    // consuming-pod lookup, which is namespace-local and still works).
    rules.push(rule(
        &[""],
        &["persistentvolumes".into()],
        &["get", "list", "watch", "patch"],
    ));
    rules.push(rule(
        &["storage.k8s.io"],
        &["volumeattachments".into()],
        READ_VERBS,
    ));
    // CSI snapshot staging (`copyMethod: Snapshot`/`Clone`, ADR §3.3): read
    // StorageClasses to resolve the source PVC's provisioner (driver); read
    // VolumeSnapshotClasses to pick the driver's class + detect whether the snapshot
    // stack is installed; delete VolumeSnapshotContents on cleanup. All cluster-scoped →
    // cluster install only (a namespaced install can't stage CSI snapshots).
    rules.push(rule(
        &["storage.k8s.io"],
        &["storageclasses".into()],
        READ_VERBS,
    ));
    rules.push(rule(
        &["snapshot.storage.k8s.io"],
        &["volumesnapshotclasses".into()],
        READ_VERBS,
    ));
    rules.push(rule(
        &["snapshot.storage.k8s.io"],
        &["volumesnapshotcontents".into()],
        &["get", "list", "watch", "delete"],
    ));
    // Self-managed webhook TLS (`webhook.tls.mode: self`): both halves fit a
    // ClusterRole.
    rules.extend(webhook_cert_cluster_rules());
    rules.extend(webhook_cert_secret_rules());

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
    // No cluster-scoped clusterrepositories. The controller still mints the mover
    // SA + RoleBinding in the (single) workload namespace, so the SA/rolebinding
    // minting rules are retained.
    let mut rules = kopia_crd_rules(false);
    rules.extend(workload_rules());
    // Self-managed webhook TLS (`webhook.tls.mode: self`): only the Secret write
    // is namespace-local and fits a Role. The cluster-scoped webhook-config patch
    // cannot live in a Role — a self-mode namespaced install pairs this Role with
    // the small ClusterRole the chart renders.
    rules.extend(webhook_cert_secret_rules());

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

/// Generate the cluster-scoped mover `ClusterRole`. The per-namespace
/// `kopiur-mover` ServiceAccount and its RoleBinding are minted by the controller
/// at runtime (in each mover Job's namespace), so only the role is shipped.
fn mover_cluster_artifact() -> Result<Artifact> {
    let clusterrole = ClusterRole {
        metadata: metadata(MOVER_CLUSTERROLE_NAME, None),
        rules: Some(mover_rules()),
        ..Default::default()
    };
    let content = document(&[render(&clusterrole)?]);
    Ok(Artifact::new(
        "rbac/mover-clusterrole.yaml".to_string(),
        content,
    ))
}

/// Generate the namespaced mover `Role` (namespaced-install mode). Same minimal
/// rules as the ClusterRole; the controller mints the SA + RoleBinding in the
/// workload namespace at runtime.
fn mover_namespaced_artifact() -> Result<Artifact> {
    let role = Role {
        metadata: metadata(MOVER_ROLE_NAME, Some(DEFAULT_NAMESPACE)),
        rules: Some(mover_rules()),
    };
    let content = document(&[render(&role)?]);
    Ok(Artifact::new("rbac/mover-role.yaml".to_string(), content))
}

/// All RBAC artifacts.
pub fn artifacts() -> Result<Vec<Artifact>> {
    Ok(vec![
        cluster_artifact()?,
        namespaced_artifact()?,
        mover_cluster_artifact()?,
        mover_namespaced_artifact()?,
    ])
}
