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

/// An in-cluster SFTP server (atmoz/sftp) for the SFTP-backend e2e. The user +
/// repo subdirectory come from the container args (atmoz `user:pass:::dir`
/// format); the client's authorized public key and the server's fixed host key
/// are mounted from [`consts::SECRET_SFTP_SERVER`] so host-key verification is
/// deterministic. Readiness is a TCP probe (SFTP has no HTTP health endpoint).
pub fn sftp_deployment(ns: &str) -> Deployment {
    let repo_dir = consts::SFTP_PATH.trim_start_matches('/');
    let user_spec = format!(
        "{}:{}:::{}",
        consts::SFTP_USER,
        consts::SFTP_PASSWORD,
        repo_dir
    );
    from_json(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "sftp", "namespace": ns },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app": "sftp" } },
            "template": {
                "metadata": { "labels": { "app": "sftp" } },
                "spec": {
                    "containers": [{
                        "name": "sftp",
                        "image": consts::SFTP_IMAGE,
                        "imagePullPolicy": "IfNotPresent",
                        "args": [user_spec],
                        "ports": [{ "containerPort": 22 }],
                        "readinessProbe": {
                            "tcpSocket": { "port": 22 },
                            "periodSeconds": 3,
                        },
                        "volumeMounts": [
                            // atmoz installs every *.pub under .ssh/keys into the
                            // user's authorized_keys at startup.
                            {
                                "name": "client-key",
                                "mountPath": format!("/home/{}/.ssh/keys", consts::SFTP_USER),
                                "readOnly": true,
                            },
                            // A fixed host key so `known_hosts` can be pinned.
                            {
                                "name": "host-key",
                                "mountPath": "/etc/ssh/ssh_host_ed25519_key",
                                "subPath": "ssh_host_ed25519_key",
                                "readOnly": true,
                            },
                            // Startup script that drops the random RSA/ECDSA host
                            // keys so only the pinned ed25519 key is offered.
                            {
                                "name": "sftpd",
                                "mountPath": "/etc/sftp.d",
                                "readOnly": true,
                            },
                        ],
                    }],
                    "volumes": [
                        {
                            "name": "client-key",
                            "secret": {
                                "secretName": consts::SECRET_SFTP_SERVER,
                                "items": [{ "key": consts::KEY_SFTP_AUTHORIZED, "path": "client.pub" }],
                            },
                        },
                        {
                            "name": "host-key",
                            "secret": {
                                "secretName": consts::SECRET_SFTP_SERVER,
                                // 0600 — sshd refuses a group/world-readable host key.
                                "defaultMode": 384,
                                "items": [{
                                    "key": consts::KEY_SFTP_HOST_KEY,
                                    "path": "ssh_host_ed25519_key",
                                    "mode": 384,
                                }],
                            },
                        },
                        {
                            "name": "sftpd",
                            "secret": {
                                "secretName": consts::SECRET_SFTP_SERVER,
                                // 0755 — atmoz only runs /etc/sftp.d/* files that are executable.
                                "items": [{
                                    "key": consts::KEY_SFTP_ONLY_ED25519,
                                    "path": "00-only-ed25519.sh",
                                    "mode": 493,
                                }],
                            },
                        },
                    ],
                },
            },
        },
    }))
}

/// The `Service` fronting the SFTP server on port 22.
pub fn sftp_service(ns: &str) -> Service {
    from_json(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": "sftp", "namespace": ns },
        "spec": {
            "selector": { "app": "sftp" },
            "ports": [{ "name": "sftp", "port": 22, "targetPort": 22 }],
        },
    }))
}

/// An in-cluster WebDAV server (Apache + mod_dav) with HTTP basic auth from env.
/// Readiness is a TCP probe — an unauthenticated HTTP GET returns 401, which a
/// `httpGet` probe would treat as unready.
pub fn webdav_deployment(ns: &str) -> Deployment {
    from_json(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "webdav", "namespace": ns },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app": "webdav" } },
            "template": {
                "metadata": { "labels": { "app": "webdav" } },
                "spec": {
                    "containers": [{
                        "name": "webdav",
                        "image": consts::WEBDAV_IMAGE,
                        "imagePullPolicy": "IfNotPresent",
                        "env": [
                            { "name": "AUTH_TYPE", "value": "Basic" },
                            { "name": "USERNAME", "value": consts::WEBDAV_USER },
                            { "name": "PASSWORD", "value": consts::WEBDAV_PASSWORD },
                        ],
                        "ports": [{ "containerPort": 80 }],
                        "readinessProbe": {
                            "tcpSocket": { "port": 80 },
                            "periodSeconds": 3,
                        },
                    }],
                },
            },
        },
    }))
}

/// The `Service` fronting the WebDAV server on port 80.
pub fn webdav_service(ns: &str) -> Service {
    from_json(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": "webdav", "namespace": ns },
        "spec": {
            "selector": { "app": "webdav" },
            "ports": [{ "name": "http", "port": 80, "targetPort": 80 }],
        },
    }))
}

/// The in-cluster NFS server `Deployment` (`obeone/docker-nfs-server`, kernel
/// `nfsd`). Runs **privileged** — it loads `nfsd` and mounts the nfsd/rpc_pipefs
/// filesystems inside the container — and exports [`consts::NFS_EXPORT_PATH`]
/// read-write with `fsid=0` (the NFSv4 root). We serve **NFSv4 only**
/// (`NFS_DISABLE_VERSION_3=1`), so a client mounts the export at `/`
/// ([`consts::NFS_MOUNT_PATH`]) and only port 2049 is needed.
///
/// The export is backed by a **memory-medium** `emptyDir` (tmpfs) so each fresh
/// server starts clean (a reused server keeps prior repo/snapshot state, which
/// the tests tolerate). tmpfs is load-bearing, not an optimization: Ganesha's
/// VFS FSAL resolves the export root with `name_to_handle_at(2)`, which a default
/// (disk-backed) `emptyDir` cannot service when the node's backing store is
/// overlayfs — Ganesha then logs `init_export_root ... FSAL_ERROR=(Operation not
/// supported,95)`, never binds 2049, and every mover NFS mount fails with
/// `exit status 32`. tmpfs implements the exportfs file-handle ops, so the
/// export root resolves and the server listens. An init container `chmod 0777`s
/// it so the non-root mover (uid [`consts::MOVER_UID`]) can write the repo —
/// `no_root_squash` alone is not enough because the mover does not run as root.
/// The mover mounts this export inline (no PVC) as the filesystem repo volume or
/// as an NFS backup source.
pub fn nfs_deployment(ns: &str) -> Deployment {
    from_json(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": { "name": "nfs", "namespace": ns },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app": "nfs" } },
            "template": {
                "metadata": { "labels": { "app": "nfs" } },
                "spec": {
                    // Make the exported emptyDir world-writable before Ganesha
                    // starts so the non-root mover can create the kopia repo under it.
                    "initContainers": [{
                        "name": "chmod-export",
                        "image": consts::NFS_IMAGE,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["sh", "-c", format!("chmod 0777 {}", consts::NFS_EXPORT_PATH)],
                        "volumeMounts": [{ "name": "export", "mountPath": consts::NFS_EXPORT_PATH }],
                    }],
                    "containers": [{
                        "name": "nfs",
                        "image": consts::NFS_IMAGE,
                        "imagePullPolicy": "IfNotPresent",
                        // Userspace NFS-Ganesha: no kernel nfsd module, so no
                        // `privileged` — just the two capabilities the image needs to
                        // bind/mount in its own namespace (per its docs).
                        "securityContext": {
                            "capabilities": { "add": ["SYS_ADMIN", "DAC_READ_SEARCH"] },
                        },
                        "env": [
                            // Export the emptyDir at the NFSv4 pseudo-root `/` (so
                            // clients mount NFS_MOUNT_PATH = "/"), NFSv4 only.
                            // No_Root_Squash + the init chmod let the non-root mover
                            // write under the export.
                            { "name": "EXPORT_PATH", "value": consts::NFS_EXPORT_PATH },
                            { "name": "PSEUDO_PATH", "value": "/" },
                            { "name": "PROTOCOLS", "value": "4" },
                            { "name": "SQUASH_MODE", "value": "No_Root_Squash" },
                        ],
                        "ports": [{ "name": "nfs", "containerPort": 2049 }],
                        "volumeMounts": [{ "name": "export", "mountPath": consts::NFS_EXPORT_PATH }],
                        "readinessProbe": {
                            "tcpSocket": { "port": 2049 },
                            "periodSeconds": 3,
                        },
                    }],
                    // tmpfs (memory medium): the VFS FSAL needs file-handle
                    // support (`name_to_handle_at`) on the export root, which an
                    // overlayfs-backed emptyDir does not provide — see the doc
                    // comment above. sizeLimit bounds the throwaway repo.
                    "volumes": [{ "name": "export", "emptyDir": { "medium": "Memory", "sizeLimit": "512Mi" } }],
                },
            },
        },
    }))
}

/// The `Service` fronting the NFSv4 server (nfsd on 2049 only — v3 is disabled,
/// so there is no rpcbind/mountd to expose).
pub fn nfs_service(ns: &str) -> Service {
    from_json(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": { "name": "nfs", "namespace": ns },
        "spec": {
            "selector": { "app": "nfs" },
            "ports": [{ "name": "nfs", "port": 2049, "targetPort": 2049 }],
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

/// A one-shot helper Pod (`restartPolicy: Never`) running `command` under
/// [`consts::BUSYBOX_IMAGE`], with each `(pvc, mount_path)` in `pvc_mounts`
/// mounted read-write. Drive it with `wait::pod_succeeded` — a non-zero exit
/// leaves the pod `Failed` and the wait reports it. Used by hook/sentinel
/// scenarios (write a marker into a source PVC; read one out of a restore).
pub fn one_shot_pod(ns: &str, name: &str, command: &[&str], pvc_mounts: &[(&str, &str)]) -> Pod {
    let mounts: Vec<serde_json::Value> = pvc_mounts
        .iter()
        .enumerate()
        .map(|(i, (_, path))| json!({ "name": format!("vol{i}"), "mountPath": path }))
        .collect();
    let volumes: Vec<serde_json::Value> = pvc_mounts
        .iter()
        .enumerate()
        .map(|(i, (pvc, _))| {
            json!({ "name": format!("vol{i}"), "persistentVolumeClaim": { "claimName": pvc } })
        })
        .collect();
    from_json(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "restartPolicy": "Never",
            "containers": [{
                "name": "task",
                "image": consts::BUSYBOX_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "command": command,
                "volumeMounts": mounts,
            }],
            "volumes": volumes,
        },
    }))
}

/// One step of a foreign-repo seeder pod ([`foreign_kopia_pod`]): each step runs
/// as one sequential initContainer, so a multi-step seed (wipe → write → create →
/// snapshot → connect-as-someone-else → snapshot) needs no shell in the
/// distroless mover image.
pub enum SeedStep<'a> {
    /// Empty the MinIO bucket (mc, `--force`) so a reused cluster can't leak a
    /// previous run's repository into a `kopia repository create`.
    WipeBucket {
        /// Bucket to empty (must already exist — `World::ensure(Minio)`).
        bucket: &'a str,
    },
    /// Write `content` to `/data/<dir>/<file>` (busybox), creating the dir.
    WriteFile {
        /// Data dir under `/data` (becomes the kopia source path `/data/<dir>`).
        dir: &'a str,
        /// File name inside the dir.
        file: &'a str,
        /// File content (keep it shell-safe: alnum/dash/space).
        content: &'a str,
    },
    /// `kopia repository create s3` against MinIO under the given identity —
    /// a repository kopiur did NOT create.
    CreateRepo {
        /// Target bucket.
        bucket: &'a str,
        /// kopia `--override-username` (the foreign identity's user).
        username: &'a str,
        /// kopia `--override-hostname` (the foreign identity's host — for a
        /// ClusterRepository this is the namespace the discovered Snapshot
        /// should land in).
        hostname: &'a str,
    },
    /// `kopia repository connect s3` — same flags as [`SeedStep::CreateRepo`]
    /// but joins an EXISTING repository (out-of-band writers, new identities).
    ConnectRepo {
        /// Target bucket.
        bucket: &'a str,
        /// kopia `--override-username`.
        username: &'a str,
        /// kopia `--override-hostname`.
        hostname: &'a str,
    },
    /// `kopia snapshot create /data/<dir>` under the identity of the most recent
    /// create/connect step.
    Snapshot {
        /// Data dir under `/data` to snapshot.
        dir: &'a str,
    },
}

/// A one-shot Pod (`restartPolicy: Never`) that drives RAW kopia against the
/// in-cluster MinIO to seed a *foreign* repository — snapshots kopiur didn't
/// produce, under identities kopiur never resolved. The import e2e adopts the
/// result. Drive it with `wait::pod_succeeded`.
///
/// Credentials are the e2e MinIO root keys + the shared e2e repo password
/// (matching `kopia-s3-creds`, so a kopiur `Repository` can adopt the result);
/// kopia's config/cache/logs live on a pod-local emptyDir.
pub fn foreign_kopia_pod(ns: &str, name: &str, steps: &[SeedStep<'_>]) -> Pod {
    let kopia_env = json!([
        { "name": "KOPIA_PASSWORD", "value": consts::KOPIA_PASSWORD },
        { "name": "KOPIA_CONFIG_PATH", "value": "/kopia/repository.config" },
        { "name": "KOPIA_CACHE_DIRECTORY", "value": "/kopia/cache" },
        { "name": "KOPIA_LOG_DIR", "value": "/kopia/logs" },
        { "name": "KOPIA_CHECK_FOR_UPDATES", "value": "false" },
    ]);
    let kopia_mounts = json!([
        { "name": "data", "mountPath": "/data" },
        { "name": "kopia", "mountPath": "/kopia" },
    ]);
    let repo_args = |verb: &str, bucket: &str, username: &str, hostname: &str| {
        json!([
            consts::KOPIA_BIN,
            "repository",
            verb,
            "s3",
            "--bucket",
            bucket,
            "--endpoint",
            consts::MINIO_ENDPOINT,
            "--disable-tls",
            "--access-key",
            consts::MINIO_USER,
            "--secret-access-key",
            consts::MINIO_PASS,
            "--override-username",
            username,
            "--override-hostname",
            hostname,
        ])
    };
    let init: Vec<serde_json::Value> = steps
        .iter()
        .enumerate()
        .map(|(i, step)| match step {
            SeedStep::WipeBucket { bucket } => json!({
                "name": format!("step-{i}-wipe"),
                "image": consts::MC_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "command": ["/bin/sh", "-c", format!(
                    "mc alias set local http://{endpoint} {user} {pass} && \
                     (mc rm -r --force local/{bucket} || true)",
                    endpoint = consts::MINIO_ENDPOINT,
                    user = consts::MINIO_USER,
                    pass = consts::MINIO_PASS,
                )],
            }),
            SeedStep::WriteFile { dir, file, content } => json!({
                "name": format!("step-{i}-write"),
                "image": consts::BUSYBOX_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "command": ["sh", "-c", format!(
                    // 0777/0666 so the 65532 kopia containers can read what the
                    // root busybox wrote into the shared emptyDir.
                    "mkdir -p /data/{dir} && chmod 0777 /data/{dir} && \
                     printf '%s' '{content}' > /data/{dir}/{file} && \
                     chmod 0666 /data/{dir}/{file}"
                )],
                "volumeMounts": [ { "name": "data", "mountPath": "/data" } ],
            }),
            SeedStep::CreateRepo {
                bucket,
                username,
                hostname,
            } => json!({
                "name": format!("step-{i}-create"),
                "image": consts::MOVER_IMAGE,
                "imagePullPolicy": "Never",
                "command": repo_args("create", bucket, username, hostname),
                "env": kopia_env.clone(),
                "volumeMounts": kopia_mounts.clone(),
            }),
            SeedStep::ConnectRepo {
                bucket,
                username,
                hostname,
            } => json!({
                "name": format!("step-{i}-connect"),
                "image": consts::MOVER_IMAGE,
                "imagePullPolicy": "Never",
                "command": repo_args("connect", bucket, username, hostname),
                "env": kopia_env.clone(),
                "volumeMounts": kopia_mounts.clone(),
            }),
            SeedStep::Snapshot { dir } => json!({
                "name": format!("step-{i}-snapshot"),
                "image": consts::MOVER_IMAGE,
                "imagePullPolicy": "Never",
                "command": [consts::KOPIA_BIN, "snapshot", "create", &format!("/data/{dir}")],
                "env": kopia_env.clone(),
                "volumeMounts": kopia_mounts.clone(),
            }),
        })
        .collect();
    from_json(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "restartPolicy": "Never",
            "initContainers": init,
            "containers": [{
                "name": "done",
                "image": consts::BUSYBOX_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "command": ["true"],
            }],
            "volumes": [
                { "name": "data", "emptyDir": {} },
                { "name": "kopia", "emptyDir": {} },
            ],
        },
    }))
}

/// A long-running labeled workload Pod (busybox `sleep`) mounting `pvc` at
/// `mount_path` — the exec target for `workloadExec` hook scenarios.
pub fn sleeper_pod(
    ns: &str,
    name: &str,
    labels: &[(&str, &str)],
    pvc: &str,
    mount_path: &str,
) -> Pod {
    let labels: serde_json::Map<String, serde_json::Value> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), json!(v)))
        .collect();
    from_json(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": name, "namespace": ns, "labels": labels },
        "spec": {
            "restartPolicy": "Never",
            "containers": [{
                "name": "app",
                "image": consts::BUSYBOX_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "command": ["sleep", "3600"],
                "volumeMounts": [{ "name": "data", "mountPath": mount_path }],
            }],
            "volumes": [{ "name": "data", "persistentVolumeClaim": { "claimName": pvc } }],
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
    fn sftp_deployment_pins_host_key_0600_and_passes_user_spec() {
        let d = sftp_deployment(consts::OPERATOR_NS);
        let v = val(&d);
        let c = v
            .pointer("/spec/template/spec/containers/0")
            .expect("container");
        assert_eq!(c.pointer("/image").unwrap(), consts::SFTP_IMAGE);
        // atmoz user spec: user:pass:::dir (dir = repo path without leading slash).
        assert_eq!(
            c.pointer("/args/0").unwrap(),
            &serde_json::json!(format!(
                "{}:{}:::kopia",
                consts::SFTP_USER,
                consts::SFTP_PASSWORD
            ))
        );
        assert_eq!(c.pointer("/readinessProbe/tcpSocket/port").unwrap(), 22);
        // The host key must be mounted 0600 (decimal 384) or sshd ignores it.
        let host_vol = v
            .pointer("/spec/template/spec/volumes/1")
            .expect("host-key volume");
        assert_eq!(host_vol.pointer("/secret/items/0/mode").unwrap(), 384);
        assert_eq!(
            host_vol.pointer("/secret/secretName").unwrap(),
            consts::SECRET_SFTP_SERVER
        );
    }

    #[test]
    fn webdav_deployment_sets_basic_auth_and_tcp_probe() {
        let d = webdav_deployment(consts::OPERATOR_NS);
        let v = val(&d);
        let c = v
            .pointer("/spec/template/spec/containers/0")
            .expect("container");
        assert_eq!(c.pointer("/image").unwrap(), consts::WEBDAV_IMAGE);
        assert_eq!(c.pointer("/readinessProbe/tcpSocket/port").unwrap(), 80);
        let env = c.pointer("/env").unwrap().as_array().unwrap();
        // USERNAME/PASSWORD/AUTH_TYPE are present for basic auth.
        let names: Vec<&str> = env
            .iter()
            .filter_map(|e| e.get("name").and_then(|n| n.as_str()))
            .collect();
        assert!(names.contains(&"USERNAME") && names.contains(&"PASSWORD"));
    }

    #[test]
    fn nfs_deployment_is_userspace_ganesha_exporting_v4_with_probe() {
        let d = nfs_deployment(consts::OPERATOR_NS);
        let v = val(&d);
        let c = v
            .pointer("/spec/template/spec/containers/0")
            .expect("container");
        assert_eq!(c.pointer("/image").unwrap(), consts::NFS_IMAGE);
        // Userspace Ganesha: NOT privileged — just SYS_ADMIN + DAC_READ_SEARCH.
        assert!(
            c.pointer("/securityContext/privileged").is_none(),
            "userspace Ganesha must not need privileged"
        );
        let caps: Vec<String> = c
            .pointer("/securityContext/capabilities/add")
            .and_then(|a| a.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            caps.contains(&"SYS_ADMIN".to_string())
                && caps.contains(&"DAC_READ_SEARCH".to_string())
        );
        // Readiness gates on 2049, so the server is up before a mover mounts.
        assert_eq!(c.pointer("/readinessProbe/tcpSocket/port").unwrap(), 2049);
        // The export dir is mounted (backed by an emptyDir on the pod).
        assert_eq!(
            c.pointer("/volumeMounts/0/mountPath").unwrap(),
            consts::NFS_EXPORT_PATH
        );
        // The export MUST be a memory-medium emptyDir (tmpfs): the VFS FSAL
        // resolves the export root with name_to_handle_at, which an overlayfs
        // emptyDir cannot service (Ganesha then never binds 2049 and every NFS
        // mount fails with `exit status 32`). Regression guard for that bug.
        assert_eq!(
            v.pointer("/spec/template/spec/volumes/0/emptyDir/medium")
                .and_then(|m| m.as_str()),
            Some("Memory"),
            "NFS export must be tmpfs-backed so Ganesha's VFS FSAL can resolve the export root"
        );
        // Ganesha export config via env: export the dir at the NFSv4 pseudo-root.
        let env: Vec<(String, String)> = c
            .pointer("/env")
            .and_then(|e| e.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|e| {
                        Some((
                            e.get("name")?.as_str()?.into(),
                            e.get("value")?.as_str()?.into(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let get = |k: &str| {
            env.iter()
                .find(|(n, _)| n == k)
                .map(|(_, val)| val.as_str())
        };
        assert_eq!(get("EXPORT_PATH"), Some(consts::NFS_EXPORT_PATH));
        assert_eq!(get("PSEUDO_PATH"), Some("/"));
        assert_eq!(get("PROTOCOLS"), Some("4"));
        // An init container makes the export world-writable for the non-root mover.
        assert!(
            v.pointer("/spec/template/spec/initContainers/0/command")
                .is_some()
        );
    }

    #[test]
    fn nfs_service_exposes_only_nfsd_2049() {
        let s = nfs_service(consts::OPERATOR_NS);
        let v = val(&s);
        let ports: Vec<i64> = v
            .pointer("/spec/ports")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|p| p.get("port").and_then(|n| n.as_i64()))
            .collect();
        // NFSv4-only: just 2049, no mountd/rpcbind.
        assert_eq!(ports, vec![2049]);
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
