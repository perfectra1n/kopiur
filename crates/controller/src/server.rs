//! Pure builders + cleanup planning for the kopia **web-UI server** (`spec.server`).
//!
//! When a `Repository`/`ClusterRepository` requests a server, the controller runs
//! `kopia server start` in a long-lived `Deployment` and exposes it via a `Service`
//! (ClusterIP by default — networking is the user's job). This module is the
//! **pure builder** (mirrors [`crate::jobs`]): given resolved inputs it produces the
//! `ConfigMap` + `Deployment` + `Service` (+ optional generated `Secret`) with the
//! `replicas: 1` / `Recreate` / hardened-securityContext defaults the feature needs.
//! No `kube::Client`, no IO — unit-tested directly.
//!
//! See [`crate::server::plan_server`] for the desired-vs-observed cleanup decision
//! that owner-ref GC cannot make (toggle-off / namespace migration).

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, EmptyDirVolumeSource, EnvFromSource, EnvVar,
    EnvVarSource, NFSVolumeSource, PersistentVolumeClaimVolumeSource, PodSpec, PodTemplateSpec,
    Probe, ResourceRequirements, Secret, SecretEnvSource, SecretKeySelector, SecurityContext,
    Service, ServicePort, ServiceSpec, TCPSocketAction, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kopiur_mover::serve::{ServerAuthSpec, ServerWorkSpec};
use kopiur_mover::workspec::RepositoryConnect;

use kopiur_api::common::hardened_security_context;

use crate::consts::{
    MANAGED_BY_LABEL, MANAGED_BY_VALUE, SERVER_COMPONENT_LABEL, SERVER_COMPONENT_VALUE,
    SERVER_INSTANCE_LABEL, SERVER_NAME_LABEL, SERVER_NAME_VALUE,
};

/// The repo PVC mount for a filesystem-backend server (the long-lived server holds
/// the repository volume RW; it MUST be ReadWriteMany so it co-mounts with movers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PvcMount {
    /// The PVC `claim_name`.
    pub claim_name: String,
    /// Where the repo volume is mounted in the server container.
    pub mount_path: String,
    /// Whether the mount is read-only (always `false` for the server's repo PVC).
    pub read_only: bool,
}

/// How a filesystem-backend server mounts its repository volume. Externally mirrors
/// [`kopiur_api::backend::RepoVolume`]; matched exhaustively so a new volume kind
/// cannot compile until the server builder handles it (ADR §5.5). Object-store
/// backends have no repo volume (`None`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerRepoVolume {
    /// A `PersistentVolumeClaim` (must be ReadWriteMany — verified at reconcile).
    Pvc(PvcMount),
    /// An inline NFS export mounted directly (multi-writer by nature, so no RWX
    /// access-mode check applies).
    Nfs {
        /// NFS server hostname or IP.
        server: String,
        /// Exported path on the NFS server.
        path: String,
        /// Where the export is mounted in the server container (the repo `path`).
        mount_path: String,
    },
}

/// Mount path of the server work-spec ConfigMap.
pub const SERVER_SPEC_MOUNT: &str = "/etc/kopiur";
/// File name of the server work spec within the mount.
pub const SERVER_SPEC_FILE: &str = "server-spec.json";
/// Env var the mover `serve` entrypoint reads for the server-spec path. Single
/// source of truth lives in [`kopiur_mover::env`] so the controller↔mover env
/// contract can't drift.
pub const SERVER_SPEC_ENV: &str = kopiur_mover::env::SERVER_SPEC_PATH;
/// Writable kopia config dir (emptyDir) — distroless default `~/.config` is not writable.
pub const SERVER_CONFIG_DIR: &str = "/config";
/// kopia config file path inside [`SERVER_CONFIG_DIR`].
pub const SERVER_CONFIG_FILE: &str = "/config/repository.config";
/// Writable kopia cache dir (emptyDir).
pub const SERVER_CACHE_DIR: &str = "/cache";

/// The `Deployment`/`Service`/`ConfigMap` name for a repository's server.
pub fn server_object_name(instance: &str) -> String {
    format!("{instance}-kopia-ui")
}

/// The operator-owned `Secret` name for `Generate` auth.
pub fn generated_secret_name(instance: &str) -> String {
    format!("{instance}-kopia-ui-auth")
}

/// The selector labels shared by the Deployment, its pods, and the Service. These
/// must be identical across all three for the Service to route to the pods.
pub fn selector_labels(instance: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (SERVER_NAME_LABEL.to_string(), SERVER_NAME_VALUE.to_string()),
        (SERVER_INSTANCE_LABEL.to_string(), instance.to_string()),
        (
            SERVER_COMPONENT_LABEL.to_string(),
            SERVER_COMPONENT_VALUE.to_string(),
        ),
    ])
}

/// Full metadata labels: the selector labels, the `managed-by` label (so the
/// controller's label-scoped owned/child watches see these objects without
/// listing every Deployment/Service in the cluster), plus any back-reference
/// labels (used by `ClusterRepository` children, which have no ownerReference).
///
/// `managed-by` is added to the object metadata only — NOT to [`selector_labels`],
/// which the `Service` selector and `Deployment` matchLabels use for routing and
/// must stay stable.
fn object_labels(instance: &str, extra: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut labels = selector_labels(instance);
    labels.insert(MANAGED_BY_LABEL.to_string(), MANAGED_BY_VALUE.to_string());
    for (k, v) in extra {
        labels.insert(k.clone(), v.clone());
    }
    labels
}

/// Resolved UI authentication: either a username + the Secret key holding the
/// password (env-injected, never on the controller-issued argv), or no auth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedAuth {
    /// Password auth: `username` goes to the work spec (→ `--server-username`); the
    /// password is read from `password_secret[password_key]` as an env var.
    Password {
        /// HTTP basic-auth username for the UI (`--server-username`).
        username: String,
        /// Secret holding the UI password (env-injected, never on argv).
        password_secret: String,
        /// Key within `password_secret` holding the password.
        password_key: String,
    },
    /// No UI auth (`--without-password`). Only reachable via the acknowledged
    /// insecure mode.
    None,
}

/// All inputs needed to build a repository's server objects.
pub struct ServerBuildInputs<'a> {
    /// Owning repository name (drives object names + the instance label).
    pub instance: &'a str,
    /// Target namespace for all objects.
    pub namespace: &'a str,
    /// Owner reference (the namespaced `Repository` case). `None` for
    /// `ClusterRepository` (cluster-scoped owners can't own namespaced objects).
    pub owner: Option<OwnerReference>,
    /// Back-reference labels (cluster-repository name/UID) for the watch+cleanup
    /// path; empty for the namespaced `Repository` case.
    pub extra_labels: BTreeMap<String, String>,
    /// Mover image (carries the kopia binary + embedded UI).
    pub image: &'a str,
    /// Image pull policy (e.g. `IfNotPresent` for a kind-loaded image).
    pub image_pull_policy: Option<&'a str>,
    /// ServiceAccount for the server pod (it never PATCHes CR status, so a minimal
    /// SA suffices; `None` uses the namespace default).
    pub service_account: Option<&'a str>,
    /// How the server connects to the repository.
    pub repository: RepositoryConnect,
    /// Listen/Service port.
    pub port: u16,
    /// Service type string (`ClusterIP`/`NodePort`/`LoadBalancer`).
    pub service_type: &'a str,
    /// Service annotations (the seam for the user's ingress/LB controller).
    pub service_annotations: BTreeMap<String, String>,
    /// Resolved auth.
    pub auth: ResolvedAuth,
    /// Repository credentials Secret (KOPIA_PASSWORD + backend creds), env-injected
    /// via `envFrom` exactly like the mover Job.
    pub creds_secret: &'a str,
    /// The repo volume for the filesystem backend (PVC must be ReadWriteMany, or an
    /// inline NFS export), mounted RW. `None` for object-store backends.
    pub repo_volume: Option<ServerRepoVolume>,
    /// Optional resource requests/limits.
    pub resources: Option<ResourceRequirements>,
    /// Optional security-context override (defaults to the hardened context).
    pub security_context: Option<SecurityContext>,
}

impl ServerBuildInputs<'_> {
    fn meta(&self, name: &str) -> ObjectMeta {
        ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(self.namespace.to_string()),
            labels: Some(object_labels(self.instance, &self.extra_labels)),
            owner_references: self.owner.clone().map(|o| vec![o]),
            ..Default::default()
        }
    }
}

/// Build the server work spec the mover `serve` entrypoint consumes.
pub fn build_server_work_spec(inputs: &ServerBuildInputs<'_>) -> ServerWorkSpec {
    ServerWorkSpec {
        version: 1,
        repository: inputs.repository.clone(),
        listen_port: inputs.port,
        auth: match &inputs.auth {
            ResolvedAuth::Password { username, .. } => ServerAuthSpec::Password {
                username: username.clone(),
            },
            ResolvedAuth::None => ServerAuthSpec::None {},
        },
        ui: true,
    }
}

/// Build the `ConfigMap` carrying the serialized server work spec.
pub fn build_server_config_map(
    inputs: &ServerBuildInputs<'_>,
) -> Result<ConfigMap, serde_json::Error> {
    let json = serde_json::to_string_pretty(&build_server_work_spec(inputs))?;
    Ok(ConfigMap {
        metadata: inputs.meta(&server_object_name(inputs.instance)),
        data: Some(BTreeMap::from([(SERVER_SPEC_FILE.to_string(), json)])),
        ..Default::default()
    })
}

/// Build the operator-owned `Secret` for `Generate` auth. The data is set ONCE on
/// create; the reconciler never re-applies it (which would rotate the password).
pub fn build_generated_secret(
    inputs: &ServerBuildInputs<'_>,
    username: &str,
    password: &str,
) -> Secret {
    Secret {
        metadata: inputs.meta(&generated_secret_name(inputs.instance)),
        string_data: Some(BTreeMap::from([
            ("username".to_string(), username.to_string()),
            ("password".to_string(), password.to_string()),
        ])),
        type_: Some("Opaque".to_string()),
        ..Default::default()
    }
}

/// Build the server `Deployment`: `replicas: 1`, `strategy: Recreate`, the mover
/// image run as `serve`, hardened securityContext, emptyDir config+cache, and a TCP
/// readiness/liveness probe on the server port.
pub fn build_server_deployment(inputs: &ServerBuildInputs<'_>) -> Deployment {
    let name = server_object_name(inputs.instance);
    let sec_ctx = inputs
        .security_context
        .clone()
        .unwrap_or_else(hardened_security_context);

    // Volumes: work-spec ConfigMap (ro), writable config + cache (emptyDir), and the
    // repo PVC (rw) for filesystem backends.
    let mut volumes = vec![
        Volume {
            name: "server-spec".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: name.clone(),
                ..Default::default()
            }),
            ..Default::default()
        },
        Volume {
            name: "config".to_string(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        },
        Volume {
            name: "cache".to_string(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        },
    ];
    let mut volume_mounts = vec![
        VolumeMount {
            name: "server-spec".to_string(),
            mount_path: SERVER_SPEC_MOUNT.to_string(),
            read_only: Some(true),
            ..Default::default()
        },
        VolumeMount {
            name: "config".to_string(),
            mount_path: SERVER_CONFIG_DIR.to_string(),
            ..Default::default()
        },
        VolumeMount {
            name: "cache".to_string(),
            mount_path: SERVER_CACHE_DIR.to_string(),
            ..Default::default()
        },
    ];
    // The repo volume for a filesystem-backend server. Exhaustive over
    // [`ServerRepoVolume`] (ADR §5.5): a PVC (must be RWX, checked at reconcile) or
    // an inline NFS export (multi-writer by nature). Object stores have no volume.
    match &inputs.repo_volume {
        Some(ServerRepoVolume::Pvc(pvc)) => {
            volumes.push(Volume {
                name: "repo".to_string(),
                persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                    claim_name: pvc.claim_name.clone(),
                    read_only: Some(pvc.read_only),
                }),
                ..Default::default()
            });
            volume_mounts.push(VolumeMount {
                name: "repo".to_string(),
                mount_path: pvc.mount_path.clone(),
                ..Default::default()
            });
        }
        Some(ServerRepoVolume::Nfs {
            server,
            path,
            mount_path,
        }) => {
            volumes.push(Volume {
                name: "repo".to_string(),
                nfs: Some(NFSVolumeSource {
                    server: server.clone(),
                    path: path.clone(),
                    read_only: Some(false),
                }),
                ..Default::default()
            });
            volume_mounts.push(VolumeMount {
                name: "repo".to_string(),
                mount_path: mount_path.clone(),
                ..Default::default()
            });
        }
        None => {}
    }

    // Non-secret env. Credentials arrive via envFrom (repo creds) + a secretKeyRef
    // (UI password); usernames/ports are non-secret.
    let mut env = vec![
        EnvVar {
            name: SERVER_SPEC_ENV.to_string(),
            value: Some(format!("{SERVER_SPEC_MOUNT}/{SERVER_SPEC_FILE}")),
            value_from: None,
        },
        EnvVar {
            name: "KOPIA_CONFIG_PATH".to_string(),
            value: Some(SERVER_CONFIG_FILE.to_string()),
            value_from: None,
        },
        EnvVar {
            name: "KOPIA_CACHE_DIRECTORY".to_string(),
            value: Some(SERVER_CACHE_DIR.to_string()),
            value_from: None,
        },
        EnvVar {
            name: "KOPIA_CHECK_FOR_UPDATES".to_string(),
            value: Some("false".to_string()),
            value_from: None,
        },
    ];
    if let ResolvedAuth::Password {
        password_secret,
        password_key,
        ..
    } = &inputs.auth
    {
        env.push(EnvVar {
            name: "KOPIA_SERVER_PASSWORD".to_string(),
            value: None,
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: password_secret.clone(),
                    key: password_key.clone(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
        });
    }

    // Repo creds (KOPIA_PASSWORD + backend creds) via envFrom — same as the mover.
    let env_from = vec![EnvFromSource {
        secret_ref: Some(SecretEnvSource {
            name: inputs.creds_secret.to_string(),
            optional: Some(false),
        }),
        ..Default::default()
    }];

    let probe = Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::Int(inputs.port as i32),
            host: None,
        }),
        initial_delay_seconds: Some(5),
        period_seconds: Some(10),
        ..Default::default()
    };

    let container = Container {
        name: "kopia-server".to_string(),
        image: Some(inputs.image.to_string()),
        image_pull_policy: inputs.image_pull_policy.map(str::to_string),
        // The mover image's entrypoint is the mover binary; `serve` selects the
        // long-lived server path (connect-then-exec kopia server start).
        args: Some(vec!["serve".to_string()]),
        env: Some(env),
        env_from: Some(env_from),
        ports: Some(vec![k8s_openapi::api::core::v1::ContainerPort {
            name: Some("http".to_string()),
            container_port: inputs.port as i32,
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }]),
        volume_mounts: Some(volume_mounts),
        resources: inputs.resources.clone(),
        security_context: Some(sec_ctx),
        readiness_probe: Some(probe.clone()),
        liveness_probe: Some(probe),
        ..Default::default()
    };

    let pod_spec = PodSpec {
        containers: vec![container],
        volumes: Some(volumes),
        service_account_name: inputs.service_account.map(str::to_string),
        ..Default::default()
    };

    Deployment {
        metadata: inputs.meta(&name),
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            // Recreate: a RollingUpdate would double-bind the server port (and
            // double-mount the repo PVC) during a rollout.
            strategy: Some(DeploymentStrategy {
                type_: Some("Recreate".to_string()),
                rolling_update: None,
            }),
            selector: LabelSelector {
                match_labels: Some(selector_labels(inputs.instance)),
                match_expressions: None,
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(object_labels(inputs.instance, &inputs.extra_labels)),
                    ..Default::default()
                }),
                spec: Some(pod_spec),
            },
            ..Default::default()
        }),
        status: None,
    }
}

/// Build the `Service` exposing the server. ClusterIP by default; only a `Service`
/// is ever created — Ingress/HTTPRoute is the user's responsibility.
pub fn build_server_service(inputs: &ServerBuildInputs<'_>) -> Service {
    let mut meta = inputs.meta(&server_object_name(inputs.instance));
    if !inputs.service_annotations.is_empty() {
        meta.annotations = Some(inputs.service_annotations.clone());
    }
    Service {
        metadata: meta,
        spec: Some(ServiceSpec {
            type_: Some(inputs.service_type.to_string()),
            selector: Some(selector_labels(inputs.instance)),
            ports: Some(vec![ServicePort {
                name: Some("http".to_string()),
                port: inputs.port as i32,
                target_port: Some(IntOrString::Int(inputs.port as i32)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    }
}

/// The desired-vs-observed server reconcile decision. Owner-ref GC only fires on CR
/// deletion, so toggling the server off or moving its namespace needs an explicit
/// teardown — this pure function decides which, and the reconciler executes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerAction {
    /// Ensure the server exists in `namespace`.
    Ensure {
        /// The desired server namespace.
        namespace: String,
    },
    /// The desired namespace changed: create in `to`, delete the stale objects in `from`.
    Migrate {
        /// The stale (observed) namespace to tear down.
        from: String,
        /// The new (desired) namespace to ensure.
        to: String,
    },
    /// The server was removed/disabled: delete the objects in `namespace`.
    Teardown {
        /// The observed namespace whose objects to delete.
        namespace: String,
    },
    /// Nothing to do (no server desired, none observed).
    Noop,
}

/// Decide the server action from the desired namespace (`Some` when a server is
/// configured) and the last-applied namespace pinned in `status.server.namespace`.
pub fn plan_server(desired_ns: Option<&str>, observed_ns: Option<&str>) -> ServerAction {
    match (desired_ns, observed_ns) {
        (Some(d), None) => ServerAction::Ensure {
            namespace: d.to_string(),
        },
        (Some(d), Some(o)) if d == o => ServerAction::Ensure {
            namespace: d.to_string(),
        },
        (Some(d), Some(o)) => ServerAction::Migrate {
            from: o.to_string(),
            to: d.to_string(),
        },
        (None, Some(o)) => ServerAction::Teardown {
            namespace: o.to_string(),
        },
        (None, None) => ServerAction::Noop,
    }
}

/// The operator-owned mirror of a `ClusterRepository`'s credentials Secret, placed
/// in the server namespace (envFrom needs a same-namespace Secret).
pub fn mirrored_creds_secret_name(instance: &str) -> String {
    format!("{instance}-kopia-ui-repo-creds")
}

/// The status block the reconciler pins after a successful ensure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerStatusPin {
    /// In-cluster endpoint (`<service>.<namespace>.svc:<port>`).
    pub endpoint: String,
    /// Namespace the server objects were applied to (pinned for migration detection).
    pub namespace: String,
    /// Resolved auth-mode discriminant (`Generate`/`SecretRef`/`Insecure`).
    pub auth_mode: String,
    /// For `Generate` mode: the operator-owned credentials Secret name.
    pub generated_secret_ref: Option<String>,
}

/// Outcome of [`reconcile_server`]: pin status, clear it, or nothing to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerOutcome {
    /// A server was applied/migrated; pin the given status.
    Active(ServerStatusPin),
    /// The server was torn down; clear `status.server`.
    Cleared,
    /// Nothing to do (no server desired, none observed).
    Noop,
}

/// Everything the orchestration needs, computed by each reconciler (which differ on
/// ownership/cleanup but share the build+apply core).
pub struct ServerReconcileCtx<'a> {
    /// The kube client for the apply/delete IO.
    pub client: &'a kube::Client,
    /// Owning repository name (drives object names + the instance label).
    pub instance: &'a str,
    /// The repository's storage backend (filesystem → repo volume mount).
    pub backend: &'a kopiur_api::backend::Backend,
    /// The repository's encryption config (names the credentials Secret).
    pub encryption: &'a kopiur_api::common::Encryption,
    /// The (namespace-agnostic) server spec; `None` when disabled.
    pub server: Option<&'a kopiur_api::server::ServerSpec>,
    /// Target namespace when enabled.
    pub target_namespace: Option<String>,
    /// `status.server.namespace` (the last-applied namespace).
    pub observed_namespace: Option<String>,
    /// Owner reference for children (namespaced `Repository` only).
    pub owner: Option<OwnerReference>,
    /// Back-reference labels (cluster-repository name/UID) for `ClusterRepository`.
    pub extra_labels: BTreeMap<String, String>,
    /// Namespace the repository credentials Secret lives in.
    pub creds_src_namespace: String,
    /// Whether the owner is cluster-scoped (drives creds mirroring + no owner refs).
    pub is_cluster: bool,
    /// Mover image (carries the kopia binary + embedded UI) for the server pod.
    pub image: &'a str,
    /// Image pull policy (e.g. `IfNotPresent` for a kind-loaded image).
    pub image_pull_policy: Option<&'a str>,
    /// ServiceAccount for the server pod (`None` → namespace default).
    pub service_account: Option<&'a str>,
}

/// The `status.server` merge body for an outcome, or `None` when there is nothing
/// to pin (`Noop`). `Cleared` emits an explicit `null` so the block is removed.
pub fn server_status_json(outcome: &ServerOutcome) -> Option<serde_json::Value> {
    match outcome {
        ServerOutcome::Active(p) => Some(serde_json::json!({
            "server": {
                "endpoint": p.endpoint,
                "namespace": p.namespace,
                "authMode": p.auth_mode,
                "generatedSecretRef": p.generated_secret_ref.as_ref().map(|n| serde_json::json!({ "name": n })),
            }
        })),
        ServerOutcome::Cleared => Some(serde_json::json!({ "server": null })),
        ServerOutcome::Noop => None,
    }
}

/// Reconcile the kopia server for one repository: apply / migrate / teardown the
/// Deployment+Service+ConfigMap (+ generated Secret, + mirrored creds for cluster
/// repos) per [`plan_server`]. The pure builders above do the object construction;
/// this is the thin IO that the unit tests don't exercise.
pub async fn reconcile_server(rc: &ServerReconcileCtx<'_>) -> crate::error::Result<ServerOutcome> {
    let name = server_object_name(rc.instance);
    let gen_secret = generated_secret_name(rc.instance);

    match plan_server(
        rc.target_namespace.as_deref(),
        rc.observed_namespace.as_deref(),
    ) {
        ServerAction::Noop => Ok(ServerOutcome::Noop),
        ServerAction::Teardown { namespace } => {
            teardown_in(rc, &namespace, &name, &gen_secret).await?;
            Ok(ServerOutcome::Cleared)
        }
        ServerAction::Migrate { from, to } => {
            teardown_in(rc, &from, &name, &gen_secret).await?;
            let pin = ensure_in(rc, &to).await?;
            Ok(ServerOutcome::Active(pin))
        }
        ServerAction::Ensure { namespace } => {
            let pin = ensure_in(rc, &namespace).await?;
            Ok(ServerOutcome::Active(pin))
        }
    }
}

async fn teardown_in(
    rc: &ServerReconcileCtx<'_>,
    namespace: &str,
    name: &str,
    gen_secret: &str,
) -> crate::error::Result<()> {
    use crate::io;
    // Deployment + Service + ConfigMap + the generated-auth Secret.
    io::delete_server_objects(rc.client, namespace, name, Some(gen_secret)).await?;
    // The mirrored creds Secret (cluster-repo cross-namespace case) is operator-owned.
    if rc.is_cluster {
        io::delete_secret_if_present(
            rc.client,
            namespace,
            &mirrored_creds_secret_name(rc.instance),
        )
        .await?;
    }
    Ok(())
}

async fn ensure_in(
    rc: &ServerReconcileCtx<'_>,
    namespace: &str,
) -> crate::error::Result<ServerStatusPin> {
    use crate::error::Error;
    use crate::io;
    use kopiur_api::backend::{Backend, RepoVolume};

    let server = rc
        .server
        .ok_or_else(|| Error::Invariant("ensure_in called with no server spec".into()))?;
    let port = server
        .service
        .as_ref()
        .map(|s| s.resolved_port())
        .unwrap_or(kopiur_api::server::DEFAULT_SERVER_PORT);
    let service_type = server
        .service
        .as_ref()
        .map(|s| s.r#type.as_str())
        .unwrap_or("ClusterIP");
    let service_annotations = server
        .service
        .as_ref()
        .map(|s| s.annotations.clone())
        .unwrap_or_default();

    // Filesystem backend → mount the repo volume. A PVC MUST be ReadWriteMany so a
    // long-lived server can co-mount it with backup/restore movers; an inline NFS
    // export is multi-writer by nature. A bare path (no volume) is node-local and
    // unreachable by the server pod, so it is rejected. Object stores connect over
    // the network and need no volume. Exhaustive over `RepoVolume` (ADR §5.5).
    let repo_volume = match rc.backend {
        Backend::Filesystem(fs) => match &fs.volume {
            Some(RepoVolume::Pvc(pvc)) => {
                let modes = io::pvc_access_modes(rc.client, namespace, &pvc.name).await?;
                if !modes.iter().any(|m| m == "ReadWriteMany") {
                    return Err(Error::Validation(format!(
                        "spec.server on a filesystem Repository requires PVC {namespace}/{} \
                         to be ReadWriteMany (a long-lived server holding an RWO repo PVC would \
                         block backup/restore movers); got accessModes {modes:?}",
                        pvc.name
                    )));
                }
                Some(ServerRepoVolume::Pvc(PvcMount {
                    claim_name: pvc.name.clone(),
                    mount_path: fs.path.clone(),
                    read_only: false,
                }))
            }
            Some(RepoVolume::Nfs(nfs)) => Some(ServerRepoVolume::Nfs {
                server: nfs.server.clone(),
                path: nfs.path.clone(),
                mount_path: fs.path.clone(),
            }),
            None => {
                return Err(Error::Validation(
                    "spec.server on a filesystem Repository requires backend.filesystem.volume \
                     (a pvc or nfs export) — a node-local/baked-in path is not reachable by the \
                     server pod"
                        .into(),
                ));
            }
        },
        _ => None,
    };

    // Credentials Secret the Deployment env-injects (KOPIA_PASSWORD + backend creds).
    // For a ClusterRepository the source Secret may live in another namespace, which
    // envFrom can't reach — mirror it into the server namespace.
    let creds_secret = io::repo_credentials(rc.encryption).secret_name;
    let creds_secret_name = if rc.is_cluster && rc.creds_src_namespace != namespace {
        let mirror_name = mirrored_creds_secret_name(rc.instance);
        let mut labels = selector_labels(rc.instance);
        labels.extend(rc.extra_labels.clone());
        let dst = k8s_openapi::api::core::v1::Secret {
            metadata: ObjectMeta {
                name: Some(mirror_name.clone()),
                namespace: Some(namespace.to_string()),
                labels: Some(labels),
                owner_references: rc.owner.clone().map(|o| vec![o]),
                ..Default::default()
            },
            ..Default::default()
        };
        io::mirror_secret(rc.client, &rc.creds_src_namespace, &creds_secret, dst).await?;
        mirror_name
    } else {
        creds_secret
    };

    // Resolve auth → builder form + (for Generate) the credentials to mint once.
    let (auth, generated_secret_ref) = resolve_auth(rc, namespace, server).await?;

    let repository = crate::snapshot::backend_to_repository_connect(rc.backend);
    let inputs = ServerBuildInputs {
        instance: rc.instance,
        namespace,
        owner: rc.owner.clone(),
        extra_labels: rc.extra_labels.clone(),
        image: rc.image,
        image_pull_policy: rc.image_pull_policy,
        service_account: rc.service_account,
        repository,
        port,
        service_type,
        service_annotations,
        auth: auth.clone(),
        creds_secret: &creds_secret_name,
        repo_volume,
        resources: server.resources.clone(),
        security_context: server.security_context.clone(),
    };

    // Generate auth: create the Secret ONCE (never re-apply → never rotate).
    if let ResolvedAuth::Password {
        password_secret, ..
    } = &auth
        && generated_secret_ref.is_some()
    {
        let pw = io::random_password();
        let username = match server.auth.as_ref() {
            Some(kopiur_api::server::ServerAuth::Generate(g)) => {
                g.username.clone().unwrap_or_else(|| "kopia".to_string())
            }
            _ => "kopia".to_string(),
        };
        let secret = build_generated_secret(&inputs, &username, &pw);
        debug_assert_eq!(
            secret.metadata.name.as_deref(),
            Some(password_secret.as_str())
        );
        io::ensure_secret_once(rc.client, namespace, &secret).await?;
    }

    let cm = build_server_config_map(&inputs)?;
    let dep = build_server_deployment(&inputs);
    let svc = build_server_service(&inputs);
    io::apply_server_objects(
        rc.client,
        namespace,
        &server_object_name(rc.instance),
        &cm,
        &dep,
        &svc,
    )
    .await?;

    Ok(ServerStatusPin {
        endpoint: format!(
            "{}.{}.svc:{}",
            server_object_name(rc.instance),
            namespace,
            port
        ),
        namespace: namespace.to_string(),
        auth_mode: server
            .auth
            .as_ref()
            .map(|a| a.kind_str().to_string())
            .unwrap_or_else(|| "Generate".to_string()),
        generated_secret_ref,
    })
}

/// Resolve the CR's `auth` into the builder's [`ResolvedAuth`] plus, for `Generate`,
/// the generated Secret name to pin in status. Reads the user Secret's username for
/// `SecretRef` (it goes to argv; non-secret).
async fn resolve_auth(
    rc: &ServerReconcileCtx<'_>,
    namespace: &str,
    server: &kopiur_api::server::ServerSpec,
) -> crate::error::Result<(ResolvedAuth, Option<String>)> {
    use crate::io;
    use kopiur_api::server::ServerAuth;

    match server.auth.as_ref() {
        // Omitted ⇒ Generate (the safe default).
        None | Some(ServerAuth::Generate(_)) => {
            let secret = generated_secret_name(rc.instance);
            let username = match server.auth.as_ref() {
                Some(ServerAuth::Generate(g)) => {
                    g.username.clone().unwrap_or_else(|| "kopia".to_string())
                }
                _ => "kopia".to_string(),
            };
            Ok((
                ResolvedAuth::Password {
                    username,
                    password_secret: secret.clone(),
                    password_key: "password".to_string(),
                },
                Some(secret),
            ))
        }
        Some(ServerAuth::SecretRef(s)) => {
            let username =
                io::read_secret_value(rc.client, namespace, &s.name, &s.username_key).await?;
            Ok((
                ResolvedAuth::Password {
                    username,
                    password_secret: s.name.clone(),
                    password_key: s.password_key.clone(),
                },
                None,
            ))
        }
        Some(ServerAuth::Insecure(_)) => Ok((ResolvedAuth::None, None)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs<'a>(ns: &'a str, auth: ResolvedAuth) -> ServerBuildInputs<'a> {
        ServerBuildInputs {
            instance: "nas",
            namespace: ns,
            owner: None,
            extra_labels: BTreeMap::new(),
            image: "ghcr.io/home-operations/kopiur-mover:test",
            image_pull_policy: Some("IfNotPresent"),
            service_account: Some("kopiur-controller"),
            repository: RepositoryConnect::S3 {
                bucket: "b".into(),
                endpoint: Some("https://minio".into()),
                prefix: None,
                region: None,
                disable_tls: false,
                disable_tls_verification: false,
                ambient_credentials: false,
            },
            port: 51515,
            service_type: "ClusterIP",
            service_annotations: BTreeMap::new(),
            auth,
            creds_secret: "nas-creds",
            repo_volume: None,
            resources: None,
            security_context: None,
        }
    }

    fn gen_auth() -> ResolvedAuth {
        ResolvedAuth::Password {
            username: "kopia".into(),
            password_secret: "nas-kopia-ui-auth".into(),
            password_key: "password".into(),
        }
    }

    #[test]
    fn object_names_are_derived_from_instance() {
        assert_eq!(server_object_name("nas"), "nas-kopia-ui");
        assert_eq!(generated_secret_name("nas"), "nas-kopia-ui-auth");
    }

    #[test]
    fn work_spec_maps_password_auth_and_port() {
        let ws = build_server_work_spec(&inputs("ns", gen_auth()));
        assert_eq!(ws.listen_port, 51515);
        assert_eq!(
            ws.auth,
            ServerAuthSpec::Password {
                username: "kopia".into()
            }
        );
        assert_eq!(ws.to_start_spec().address, "0.0.0.0:51515");
    }

    #[test]
    fn work_spec_maps_no_auth() {
        let ws = build_server_work_spec(&inputs("ns", ResolvedAuth::None));
        assert_eq!(ws.auth, ServerAuthSpec::None {});
    }

    #[test]
    fn deployment_is_single_replica_recreate_with_probe() {
        let dep = build_server_deployment(&inputs("ns", gen_auth()));
        let spec = dep.spec.unwrap();
        assert_eq!(spec.replicas, Some(1));
        assert_eq!(spec.strategy.unwrap().type_.as_deref(), Some("Recreate"));
        // The selector must be a SUBSET of the pod template labels (or the Service
        // can't route / the Deployment is rejected). The template additionally
        // carries the `managed-by` label so the controller's scoped watches see it.
        let selector = spec.selector.match_labels.clone().unwrap();
        let template_labels = spec
            .template
            .metadata
            .as_ref()
            .unwrap()
            .labels
            .clone()
            .unwrap();
        for (k, v) in &selector {
            assert_eq!(
                template_labels.get(k),
                Some(v),
                "selector label {k} missing/mismatched in template"
            );
        }
        assert!(
            template_labels.contains_key("app.kubernetes.io/managed-by"),
            "pod template should carry managed-by for scoped watches"
        );
        let c = &spec.template.spec.as_ref().unwrap().containers[0];
        assert_eq!(c.args.as_ref().unwrap(), &vec!["serve".to_string()]);
        let probe = c.readiness_probe.as_ref().unwrap();
        assert_eq!(
            probe.tcp_socket.as_ref().unwrap().port,
            IntOrString::Int(51515)
        );
    }

    #[test]
    fn deployment_password_auth_injects_server_password_env_from_secret() {
        let dep = build_server_deployment(&inputs("ns", gen_auth()));
        let c = &dep.spec.unwrap().template.spec.unwrap().containers[0];
        let env = c.env.as_ref().unwrap();
        let pw = env
            .iter()
            .find(|e| e.name == "KOPIA_SERVER_PASSWORD")
            .expect("KOPIA_SERVER_PASSWORD env");
        // The password is a secretKeyRef, never an inline value.
        assert!(pw.value.is_none());
        let sk = pw
            .value_from
            .as_ref()
            .unwrap()
            .secret_key_ref
            .as_ref()
            .unwrap();
        assert_eq!(sk.name, "nas-kopia-ui-auth");
        assert_eq!(sk.key, "password");
        // Repo creds via envFrom (KOPIA_PASSWORD + backend creds).
        assert_eq!(
            c.env_from.as_ref().unwrap()[0]
                .secret_ref
                .as_ref()
                .unwrap()
                .name,
            "nas-creds"
        );
    }

    #[test]
    fn deployment_no_auth_omits_server_password_env() {
        let dep = build_server_deployment(&inputs("ns", ResolvedAuth::None));
        let c = &dep.spec.unwrap().template.spec.unwrap().containers[0];
        assert!(
            !c.env
                .as_ref()
                .unwrap()
                .iter()
                .any(|e| e.name == "KOPIA_SERVER_PASSWORD")
        );
    }

    #[test]
    fn deployment_mounts_repo_pvc_for_filesystem() {
        let mut i = inputs("ns", gen_auth());
        i.repo_volume = Some(ServerRepoVolume::Pvc(PvcMount {
            claim_name: "repo-rwx".into(),
            mount_path: "/repo".into(),
            read_only: false,
        }));
        let dep = build_server_deployment(&i);
        let pod = dep.spec.unwrap().template.spec.unwrap();
        let repo = pod
            .volumes
            .unwrap()
            .into_iter()
            .find(|v| v.name == "repo")
            .unwrap();
        assert_eq!(repo.persistent_volume_claim.unwrap().claim_name, "repo-rwx");
    }

    #[test]
    fn deployment_mounts_nfs_export_for_filesystem() {
        let mut i = inputs("ns", gen_auth());
        i.repo_volume = Some(ServerRepoVolume::Nfs {
            server: "nas.lan".into(),
            path: "/export/kopia".into(),
            mount_path: "/repo".into(),
        });
        let dep = build_server_deployment(&i);
        let pod = dep.spec.unwrap().template.spec.unwrap();
        let repo = pod
            .volumes
            .unwrap()
            .into_iter()
            .find(|v| v.name == "repo")
            .unwrap();
        let nfs = repo.nfs.unwrap();
        assert_eq!(nfs.server, "nas.lan");
        assert_eq!(nfs.path, "/export/kopia");
    }

    #[test]
    fn service_selector_matches_deployment_and_carries_type_and_annotations() {
        let mut i = inputs("ns", gen_auth());
        i.service_type = "LoadBalancer";
        i.service_annotations =
            BTreeMap::from([("io.cilium/lb-ipam-ips".to_string(), "10.0.0.5".to_string())]);
        let svc = build_server_service(&i);
        let spec = svc.spec.unwrap();
        assert_eq!(spec.type_.as_deref(), Some("LoadBalancer"));
        assert_eq!(spec.selector, Some(selector_labels("nas")));
        assert_eq!(spec.ports.unwrap()[0].port, 51515);
        assert_eq!(
            svc.metadata.annotations.unwrap()["io.cilium/lb-ipam-ips"],
            "10.0.0.5"
        );
    }

    #[test]
    fn generated_secret_holds_credentials_once() {
        let s = build_generated_secret(&inputs("ns", gen_auth()), "kopia", "s3cret");
        let data = s.string_data.unwrap();
        assert_eq!(data["username"], "kopia");
        assert_eq!(data["password"], "s3cret");
        assert_eq!(s.metadata.name.as_deref(), Some("nas-kopia-ui-auth"));
    }

    #[test]
    fn config_map_round_trips_the_work_spec() {
        let cm = build_server_config_map(&inputs("ns", gen_auth())).unwrap();
        let body = &cm.data.unwrap()[SERVER_SPEC_FILE];
        let parsed: ServerWorkSpec = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.listen_port, 51515);
    }

    // --- plan_server ---

    #[test]
    fn plan_ensure_when_desired_and_none_observed() {
        assert_eq!(
            plan_server(Some("ns"), None),
            ServerAction::Ensure {
                namespace: "ns".into()
            }
        );
    }

    #[test]
    fn plan_ensure_when_namespace_unchanged() {
        assert_eq!(
            plan_server(Some("ns"), Some("ns")),
            ServerAction::Ensure {
                namespace: "ns".into()
            }
        );
    }

    #[test]
    fn plan_migrate_when_namespace_changed() {
        assert_eq!(
            plan_server(Some("new"), Some("old")),
            ServerAction::Migrate {
                from: "old".into(),
                to: "new".into()
            }
        );
    }

    #[test]
    fn plan_teardown_when_disabled_but_observed() {
        assert_eq!(
            plan_server(None, Some("old")),
            ServerAction::Teardown {
                namespace: "old".into()
            }
        );
    }

    #[test]
    fn plan_noop_when_nothing_desired_or_observed() {
        assert_eq!(plan_server(None, None), ServerAction::Noop);
    }
}
