use super::*;

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{
    ConfigMap, PersistentVolumeClaim, PersistentVolumeClaimSpec, Pod, SecurityContext,
    ServiceAccount, VolumeResourceRequirements,
};
use k8s_openapi::api::rbac::v1::{RoleBinding, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use kube::api::{ListParams, PostParams};
use kube::core::ObjectMeta;
use kube::{Api, ResourceExt};

use kopiur_api::common::{MoverSpec, PodSelector};

use crate::consts::PRIVILEGED_MOVERS_ANNOTATION;
use crate::error::{Error, Result};

/// Apply both the work-spec `ConfigMap` and the mover `Job` (server-side).
/// Both carry the owner reference so GC reaps them with the CR (§4.10).
pub async fn apply_mover_objects(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    config_map: &ConfigMap,
    job: &Job,
) -> Result<()> {
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    apply(&cm_api, name, config_map).await?;
    let job_api: Api<Job> = Api::namespaced(client.clone(), namespace);
    apply(&job_api, name, job).await?;
    Ok(())
}

/// Labels marking the per-namespace mover RBAC objects as kopiur-managed.
fn mover_managed_labels() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            "kopiur".to_string(),
        ),
        (
            "app.kubernetes.io/component".to_string(),
            "mover".to_string(),
        ),
    ])
}

/// Build the least-privilege mover `ServiceAccount` for namespace `ns`. Pure (no
/// IO) so the shape is unit-testable. The mover Job runs as this SA; it is minted
/// per workload namespace because a mover Job runs there, not in the operator's
/// namespace where the operator SA lives.
pub fn build_mover_service_account(ns: &str, name: &str) -> ServiceAccount {
    ServiceAccount {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            labels: Some(mover_managed_labels()),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Build the `RoleBinding` that grants the mover SA the mover role within `ns`.
/// `role_kind` is `ClusterRole` (cluster install: one shared role bound per
/// namespace) or `Role` (namespaced install: a role in the operator namespace).
/// Pure (no IO) so the subject/roleRef wiring is unit-testable.
pub fn build_mover_rolebinding(
    ns: &str,
    sa_name: &str,
    role_kind: &str,
    role_name: &str,
) -> RoleBinding {
    RoleBinding {
        metadata: ObjectMeta {
            name: Some(sa_name.to_string()),
            namespace: Some(ns.to_string()),
            labels: Some(mover_managed_labels()),
            ..Default::default()
        },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: role_kind.to_string(),
            name: role_name.to_string(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: sa_name.to_string(),
            namespace: Some(ns.to_string()),
            api_group: None,
        }]),
    }
}

/// Ensure the mover `ServiceAccount` + its `RoleBinding` exist in `ns` (the mover
/// Job's namespace). Idempotent server-side apply — reconcilers call this before
/// every mover Job so the SA is present in the workload namespace (else the Job
/// `FailedCreate`s with `serviceaccount ... not found` and never schedules a pod).
/// The objects are kopiur-managed and shared across all mover Jobs in the
/// namespace (no owner reference, so deleting one Backup does not revoke them).
pub async fn ensure_mover_rbac(
    client: &kube::Client,
    ns: &str,
    sa_name: &str,
    role_kind: &str,
    role_name: &str,
) -> Result<()> {
    let sa = build_mover_service_account(ns, sa_name);
    let sa_api: Api<ServiceAccount> = Api::namespaced(client.clone(), ns);
    apply(&sa_api, sa_name, &sa).await?;

    let rb = build_mover_rolebinding(ns, sa_name, role_kind, role_name);
    let rb_api: Api<RoleBinding> = Api::namespaced(client.clone(), ns);
    apply(&rb_api, sa_name, &rb).await?;
    Ok(())
}

/// Whether namespace `ns` has opted in to elevated (root/privileged) movers via the
/// [`PRIVILEGED_MOVERS_ANNOTATION`]. If the namespace cannot be read because the
/// operator lacks `namespaces get` (a namespaced-scope install, where the operator
/// is already confined to admin-chosen namespaces), the check fails **open** with a
/// warning rather than blocking every privileged mover.
pub async fn namespace_allows_privileged_movers(client: &kube::Client, ns: &str) -> Result<bool> {
    use k8s_openapi::api::core::v1::Namespace;
    let api: Api<Namespace> = Api::all(client.clone());
    match api.get(ns).await {
        Ok(namespace) => Ok(namespace
            .annotations()
            .get(PRIVILEGED_MOVERS_ANNOTATION)
            .is_some_and(|v| v == "true")),
        // Forbidden (no cluster-scoped namespaces:get, e.g. a namespaced install):
        // can't determine the opt-in, so don't block — the operator is already
        // scoped to admin-selected namespaces in that mode.
        Err(kube::Error::Api(e)) if e.code == 403 => {
            tracing::warn!(
                namespace = ns,
                "cannot read namespace to check the privileged-movers opt-in (operator lacks \
                 namespaces:get); allowing the privileged mover"
            );
            Ok(true)
        }
        Err(e) => Err(Error::Kube(e)),
    }
}

/// The actionable message for a privileged mover refused in a namespace that has
/// not opted in (what / why / how-to-fix). Pure so the exact text is unit-asserted.
/// `kind` is the owning resource's kind (e.g. `BackupConfig`, `Restore`) and `name`
/// its name, so the message names the right object to fix.
pub fn privileged_mover_message(kind: &str, name: &str, ns: &str, mover_sa: &str) -> String {
    format!(
        "{kind} `{name}` requests a privileged mover (e.g. `runAsUser: 0`, \
         `privileged: true`, added capabilities, or `privilegedMode`), but namespace `{ns}` has \
         not opted in. A tenant with access to `{ns}` could reuse the minted `{mover_sa}` \
         ServiceAccount to run pods at that privilege, so an elevated mover requires an explicit \
         per-namespace opt-in. Fix: a cluster admin annotates the namespace — `kubectl annotate \
         namespace {ns} {PRIVILEGED_MOVERS_ANNOTATION}=true` — or remove the elevated \
         securityContext/privilegedMode from the {kind} `spec.mover`."
    )
}

/// Render a k8s [`LabelSelector`] as a kube list-query string
/// (`k1=v1,k2=v2,key in (a,b),!key`). kube 3.1 has no built-in `LabelSelector` →
/// query conversion, so this fills the gap for [`resolve_inherited_security_context`].
/// Pure + unit-tested. An empty selector renders to `""` (matches everything — the
/// caller treats a `matchNothing` selector as a config error before calling).
pub fn label_selector_to_string(sel: &LabelSelector) -> String {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;
    let mut terms: Vec<String> = Vec::new();
    if let Some(labels) = &sel.match_labels {
        for (k, v) in labels {
            terms.push(format!("{k}={v}"));
        }
    }
    if let Some(exprs) = &sel.match_expressions {
        for LabelSelectorRequirement {
            key,
            operator,
            values,
        } in exprs
        {
            let vals = values.clone().unwrap_or_default().join(",");
            match operator.as_str() {
                "In" => terms.push(format!("{key} in ({vals})")),
                "NotIn" => terms.push(format!("{key} notin ({vals})")),
                "Exists" => terms.push(key.clone()),
                "DoesNotExist" => terms.push(format!("!{key}")),
                // Unknown operator: skip (the webhook/schema constrain the set).
                _ => {}
            }
        }
    }
    terms.join(",")
}

/// Resolve `inheritSecurityContextFrom` to a concrete container [`SecurityContext`]:
/// find a workload pod in `ns` matching the selector, pick the named container (or the
/// pod's first), and return its `securityContext`. A `Running` pod is preferred so a
/// terminating/pending replica isn't read. Returns `Err(MissingDependency)` — a
/// transient, requeue-on-the-fast-cadence condition — when no pod matches or the chosen
/// container has no `securityContext` to inherit, with an actionable message.
pub async fn resolve_inherited_security_context(
    client: &kube::Client,
    ns: &str,
    selector: &PodSelector,
) -> Result<SecurityContext> {
    let query = label_selector_to_string(&selector.pod_selector);
    if query.is_empty() {
        return Err(Error::MissingDependency(format!(
            "mover.inheritSecurityContextFrom.podSelector is empty in namespace `{ns}` — set \
             matchLabels/matchExpressions identifying the workload pod whose securityContext the \
             mover should inherit (UID/GID match)"
        )));
    }
    let api: Api<Pod> = Api::namespaced(client.clone(), ns);
    let pods = api.list(&ListParams::default().labels(&query)).await?.items;
    if pods.is_empty() {
        return Err(Error::MissingDependency(format!(
            "no pod matches mover.inheritSecurityContextFrom (`{query}`) in namespace `{ns}` — the \
             workload whose securityContext the mover inherits must be running so its UID/GID can \
             be read; scale it up or fix the selector"
        )));
    }
    // Prefer a Running pod; otherwise take the first match.
    let pod = pods
        .iter()
        .find(|p| {
            p.status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .map(|ph| ph == "Running")
                .unwrap_or(false)
        })
        .unwrap_or(&pods[0]);
    let containers = pod
        .spec
        .as_ref()
        .map(|s| s.containers.as_slice())
        .unwrap_or(&[]);
    let container = match &selector.container {
        Some(name) => containers.iter().find(|c| &c.name == name).ok_or_else(|| {
            Error::MissingDependency(format!(
                "pod `{}` (matched by mover.inheritSecurityContextFrom in `{ns}`) has no container \
                 `{name}` — fix `inheritSecurityContextFrom.container`",
                pod.name_any()
            ))
        })?,
        None => containers.first().ok_or_else(|| {
            Error::MissingDependency(format!(
                "pod `{}` (matched by mover.inheritSecurityContextFrom in `{ns}`) has no containers \
                 to inherit a securityContext from",
                pod.name_any()
            ))
        })?,
    };
    container.security_context.clone().ok_or_else(|| {
        Error::MissingDependency(format!(
            "container `{}` of pod `{}` (mover.inheritSecurityContextFrom, `{ns}`) sets no \
             securityContext to inherit — set one on the workload, or use an explicit \
             mover.securityContext instead",
            container.name,
            pod.name_any()
        ))
    })
}

/// Ensure a controller-owned **persistent** kopia cache PVC named `name` exists in
/// `ns` (a warm cache reused across this owner's runs, ADR §3.1). Idempotent:
/// returns the claim name if it already exists (the spec is immutable, so we never
/// re-apply over it), otherwise creates it `ReadWriteOnce` at `capacity` with the
/// optional `storage_class`, owner-referenced so it is GC'd with `owner`. Because it
/// is `ReadWriteOnce`, persistent cache assumes non-overlapping runs for the owner.
pub async fn ensure_cache_pvc(
    client: &kube::Client,
    ns: &str,
    name: &str,
    owner: OwnerReference,
    capacity: &str,
    storage_class: Option<&str>,
) -> Result<String> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), ns);
    if api.get_opt(name).await?.is_some() {
        return Ok(name.to_string());
    }
    let pvc = PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(ns.to_string()),
            owner_references: Some(vec![owner]),
            labels: Some(child_labels(&[(
                "kopiur.home-operations.com/component",
                "mover-cache",
            )])),
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            resources: Some(VolumeResourceRequirements {
                requests: Some(std::collections::BTreeMap::from([(
                    "storage".to_string(),
                    Quantity(capacity.to_string()),
                )])),
                limits: None,
            }),
            storage_class_name: storage_class.map(String::from),
            ..Default::default()
        }),
        ..Default::default()
    };
    match api.create(&PostParams::default(), &pvc).await {
        Ok(_) => Ok(name.to_string()),
        // A concurrent reconcile won the create race: the PVC is there, reuse it.
        Err(kube::Error::Api(e)) if e.code == 409 => Ok(name.to_string()),
        Err(e) => Err(Error::Kube(e)),
    }
}

/// The mover's **effective** container security context: the one resolved from
/// `inheritSecurityContextFrom` when set, else the explicit `securityContext`
/// (the two are mutually exclusive — webhook/`validate_mover`-enforced). `None`
/// when the mover sets neither (the Job builder then applies the hardened default).
/// The result feeds BOTH the privileged-mover gate and the mover `Job`, so an
/// inherited root context is gated exactly like an explicit one.
pub async fn resolve_mover_security_context(
    client: &kube::Client,
    ns: &str,
    mover: Option<&MoverSpec>,
) -> Result<Option<SecurityContext>> {
    match mover {
        Some(m) => match &m.inherit_security_context_from {
            Some(sel) => Ok(Some(
                resolve_inherited_security_context(client, ns, sel).await?,
            )),
            None => Ok(m.security_context.clone()),
        },
        None => Ok(None),
    }
}
