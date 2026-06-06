//! Pure builders for the cluster fixtures the e2e scenarios depend on.
//!
//! Each function returns a typed `k8s-openapi` object built the cluster's way
//! (`serde_json::json!` → `serde_json::from_value`, matching the existing
//! `apply_secret`/`ensure_namespace` style and sidestepping serde_yaml's
//! externally-tagged-enum quirk). They are deliberately *pure* — no cluster, no
//! IO — so they unit-test hermetically (mirrors `crates/controller/src/jobs.rs`).
//! The imperative side (apply, wait, the one-shot bucket Pod) lives in
//! [`crate::apply`] / [`crate::wait`] / [`crate::world`].

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{
    Namespace, PersistentVolume, PersistentVolumeClaim, Pod, Secret, Service,
};
use serde_json::json;

use crate::consts;

fn from_json<T: serde::de::DeserializeOwned>(v: serde_json::Value) -> T {
    serde_json::from_value(v).expect("fixture JSON deserializes into typed object")
}

/// A `Namespace`.
pub fn namespace(name: &str) -> Namespace {
    from_json(json!({
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": { "name": name },
    }))
}

/// A statically-bound hostPath `PersistentVolume` (`storageClassName: ""` so no
/// dynamic provisioner claims it). `type: Directory` requires the dir to already
/// exist on the node — seeded by the mise `e2e-node-seed` task.
pub fn hostpath_pv(name: &str, host_path: &str, size: &str) -> PersistentVolume {
    from_json(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": { "name": name },
        "spec": {
            "capacity": { "storage": size },
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": "",
            "hostPath": { "path": host_path, "type": "Directory" },
        },
    }))
}

/// A `PersistentVolumeClaim` that binds a specific (static) PV by name.
pub fn static_pvc(ns: &str, name: &str, volume_name: &str, size: &str) -> PersistentVolumeClaim {
    from_json(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "storageClassName": "",
            "volumeName": volume_name,
            "resources": { "requests": { "storage": size } },
        },
    }))
}

/// A dynamically-provisioned `PersistentVolumeClaim` (default storage class) —
/// used for the restore destination.
pub fn dynamic_pvc(ns: &str, name: &str, size: &str) -> PersistentVolumeClaim {
    from_json(json!({
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": { "requests": { "storage": size } },
        },
    }))
}

/// An `Opaque` `Secret` carrying `stringData` (server applies it base64-encoded).
pub fn opaque_secret(ns: &str, name: &str, string_data: &[(&str, &str)]) -> Secret {
    let data: serde_json::Map<String, serde_json::Value> = string_data
        .iter()
        .map(|(k, v)| ((*k).to_string(), json!(v)))
        .collect();
    from_json(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": name, "namespace": ns },
        "type": "Opaque",
        "stringData": data,
    }))
}

/// A single-pod, HTTP-only MinIO `Deployment`. `IfNotPresent` because the image
/// is preloaded into the node (a slow docker.io pull otherwise times out the
/// rollout). A readiness probe gates the `wait::deployment_ready` the harness does.
pub fn minio_deployment(ns: &str) -> Deployment {
    from_json(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "minio", "namespace": ns },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app": "minio" } },
            "template": {
                "metadata": { "labels": { "app": "minio" } },
                "spec": {
                    "containers": [{
                        "name": "minio",
                        "image": consts::MINIO_IMAGE,
                        "imagePullPolicy": "IfNotPresent",
                        "args": ["server", "/data", "--console-address", ":9001"],
                        "env": [
                            { "name": "MINIO_ROOT_USER", "value": consts::MINIO_USER },
                            { "name": "MINIO_ROOT_PASSWORD", "value": consts::MINIO_PASS },
                        ],
                        "ports": [{ "containerPort": 9000 }],
                        "readinessProbe": {
                            "httpGet": { "path": "/minio/health/ready", "port": 9000 },
                            "periodSeconds": 3,
                        },
                    }],
                },
            },
        },
    }))
}

/// The `Service` fronting MinIO on port 9000 (the in-cluster S3 endpoint).
pub fn minio_service(ns: &str) -> Service {
    from_json(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": "minio", "namespace": ns },
        "spec": {
            "selector": { "app": "minio" },
            "ports": [{ "name": "s3", "port": 9000, "targetPort": 9000 }],
        },
    }))
}

/// A one-shot `Pod` that creates all [`consts::BUCKETS`] via `mc` (idempotent
/// `mb --ignore-existing`), retrying `mc alias set` until MinIO answers. The
/// harness runs it to completion, then deletes it.
pub fn mc_bucket_pod(ns: &str, name: &str) -> Pod {
    let mut script = format!(
        "set -e\nuntil mc alias set local http://minio:9000 {user} {pass} >/dev/null 2>&1; \
         do sleep 2; done\n",
        user = consts::MINIO_USER,
        pass = consts::MINIO_PASS,
    );
    for bucket in consts::BUCKETS {
        script.push_str(&format!("mc mb --ignore-existing local/{bucket}\n"));
    }
    from_json(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "restartPolicy": "Never",
            "containers": [{
                "name": "mc",
                "image": consts::MC_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "command": ["/bin/sh", "-c", script],
            }],
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    /// Re-serialize a typed object to JSON so assertions read the API field names
    /// (the way the cluster sees them), independent of k8s-openapi's Rust idents.
    fn val<T: serde::Serialize>(o: &T) -> Value {
        serde_json::to_value(o).unwrap()
    }

    #[test]
    fn hostpath_pv_has_path_rwo_and_empty_storage_class() {
        let pv = hostpath_pv(consts::PV_REPO, consts::HOSTPATH_REPO, "1Gi");
        let v = val(&pv);
        assert_eq!(v.pointer("/metadata/name").unwrap(), consts::PV_REPO);
        assert_eq!(
            v.pointer("/spec/hostPath/path").unwrap(),
            consts::HOSTPATH_REPO
        );
        assert_eq!(v.pointer("/spec/hostPath/type").unwrap(), "Directory");
        assert_eq!(v.pointer("/spec/accessModes/0").unwrap(), "ReadWriteOnce");
        // "" keeps a dynamic provisioner from hijacking the static binding.
        assert_eq!(v.pointer("/spec/storageClassName").unwrap(), "");
        assert_eq!(v.pointer("/spec/capacity/storage").unwrap(), "1Gi");
    }

    #[test]
    fn static_pvc_binds_named_volume() {
        let pvc = static_pvc(
            consts::OPERATOR_NS,
            consts::PVC_REPO,
            consts::PV_REPO,
            "1Gi",
        );
        let v = val(&pvc);
        assert_eq!(
            v.pointer("/metadata/namespace").unwrap(),
            consts::OPERATOR_NS
        );
        assert_eq!(v.pointer("/spec/volumeName").unwrap(), consts::PV_REPO);
        assert_eq!(v.pointer("/spec/storageClassName").unwrap(), "");
        assert_eq!(
            v.pointer("/spec/resources/requests/storage").unwrap(),
            "1Gi"
        );
    }

    #[test]
    fn dynamic_pvc_omits_volume_and_storage_class() {
        let pvc = dynamic_pvc(consts::OPERATOR_NS, consts::PVC_DST, "1Gi");
        let v = val(&pvc);
        assert!(v.pointer("/spec/volumeName").is_none());
        assert!(v.pointer("/spec/storageClassName").is_none());
        assert_eq!(
            v.pointer("/spec/resources/requests/storage").unwrap(),
            "1Gi"
        );
    }

    #[test]
    fn opaque_secret_carries_string_data() {
        let sec = opaque_secret(
            consts::OPERATOR_NS,
            consts::SECRET_S3_CREDS,
            &[
                (consts::KEY_KOPIA_PASSWORD, consts::KOPIA_PASSWORD),
                (consts::KEY_AWS_ACCESS_KEY_ID, consts::MINIO_USER),
            ],
        );
        let v = val(&sec);
        assert_eq!(v.pointer("/type").unwrap(), "Opaque");
        assert_eq!(
            v.pointer("/stringData/KOPIA_PASSWORD").unwrap(),
            consts::KOPIA_PASSWORD
        );
        assert_eq!(
            v.pointer("/stringData/AWS_ACCESS_KEY_ID").unwrap(),
            consts::MINIO_USER
        );
    }

    #[test]
    fn minio_deployment_pulls_ifnotpresent_and_exposes_9000() {
        let d = minio_deployment(consts::OPERATOR_NS);
        let v = val(&d);
        let c = v
            .pointer("/spec/template/spec/containers/0")
            .expect("container");
        assert_eq!(c.pointer("/image").unwrap(), consts::MINIO_IMAGE);
        assert_eq!(c.pointer("/imagePullPolicy").unwrap(), "IfNotPresent");
        assert_eq!(c.pointer("/ports/0/containerPort").unwrap(), 9000);
        assert_eq!(
            c.pointer("/readinessProbe/httpGet/path").unwrap(),
            "/minio/health/ready"
        );
    }

    #[test]
    fn minio_service_targets_9000() {
        let s = minio_service(consts::OPERATOR_NS);
        let v = val(&s);
        assert_eq!(v.pointer("/spec/ports/0/port").unwrap(), 9000);
        assert_eq!(v.pointer("/spec/selector/app").unwrap(), "minio");
    }

    #[test]
    fn mc_bucket_pod_creates_every_bucket_and_never_restarts() {
        let p = mc_bucket_pod(consts::OPERATOR_NS, "mc-mkbucket");
        let v = val(&p);
        assert_eq!(v.pointer("/spec/restartPolicy").unwrap(), "Never");
        let script = v
            .pointer("/spec/containers/0/command/2")
            .unwrap()
            .as_str()
            .unwrap();
        for bucket in consts::BUCKETS {
            assert!(
                script.contains(&format!("local/{bucket}")),
                "script missing bucket {bucket}"
            );
        }
        assert!(script.contains("--ignore-existing"));
    }
}
