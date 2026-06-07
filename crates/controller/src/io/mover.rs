use super::*;

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{ConfigMap, ServiceAccount};
use k8s_openapi::api::rbac::v1::{RoleBinding, RoleRef, Subject};
use kube::core::ObjectMeta;
use kube::{Api, ResourceExt};

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
pub fn privileged_mover_message(config_name: &str, ns: &str, mover_sa: &str) -> String {
    format!(
        "BackupConfig `{config_name}` requests a privileged mover (e.g. `runAsUser: 0`, \
         `privileged: true`, added capabilities, or `privilegedMode`), but namespace `{ns}` has \
         not opted in. A tenant with access to `{ns}` could reuse the minted `{mover_sa}` \
         ServiceAccount to run pods at that privilege, so an elevated mover requires an explicit \
         per-namespace opt-in. Fix: a cluster admin annotates the namespace — `kubectl annotate \
         namespace {ns} {PRIVILEGED_MOVERS_ANNOTATION}=true` — or remove the elevated \
         securityContext/privilegedMode from the BackupConfig `spec.mover`."
    )
}
