//! Mover `Job` + `ConfigMap` construction (ADR §4.10 / §4.11).
//!
//! The controller delegates every long-running kopia operation to a mover
//! `Job`: it writes a `ConfigMap` holding the serialized [`MoverWorkSpec`] and
//! creates a `Job` that mounts it and runs the kopiur-mover image. This module
//! is the **pure builder** — given resolved inputs it produces the two objects
//! with the security-context, `backoffLimit`, and `activeDeadlineSeconds`
//! defaults the ADR mandates (§4.10/§4.11/G16). No `kube::Client`, no IO, so it
//! is unit-tested directly.

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, EmptyDirVolumeSource, EnvFromSource,
    PersistentVolumeClaimVolumeSource, PodSpec, PodTemplateSpec, ResourceRequirements,
    SeccompProfile, SecretEnvSource, SecurityContext, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kopiur_mover::workspec::MoverWorkSpec;

use crate::consts::API_VERSION;

/// Default mover image; overridable per deployment via the controller config.
/// `:latest` is deliberately avoided (G15) — callers should pin a digest/tag.
pub const DEFAULT_MOVER_IMAGE: &str = "ghcr.io/home-operations/kopiur-mover:v0.1.0";

/// Path inside the mover pod where the work-spec ConfigMap is mounted.
pub const WORK_SPEC_MOUNT: &str = "/etc/kopiur";
/// File name of the work spec within the mount.
pub const WORK_SPEC_FILE: &str = "work-spec.json";
/// Env var the mover reads for the work-spec path. Sourced from the mover
/// crate's single definition so the controller↔mover contract can't drift.
pub const WORK_SPEC_ENV: &str = kopiur_mover::env::WORK_SPEC_PATH;
/// Env var naming the ConfigMap the mover writes a bootstrap result into (set
/// only for `BootstrapRepository` Jobs). Single definition shared with the mover.
pub const RESULT_CONFIGMAP_ENV: &str = kopiur_mover::env::RESULT_CONFIGMAP;

/// Defaults for the mover `Job`, sourced from `FailurePolicy` (ADR §4.10, G6).
#[derive(Debug, Clone, Copy)]
pub struct JobLimits {
    /// `Job.spec.backoffLimit`. ADR default of 2 retries when unset.
    pub backoff_limit: i32,
    /// `Job.spec.activeDeadlineSeconds`. None = no deadline.
    pub active_deadline_seconds: Option<i64>,
}

impl Default for JobLimits {
    fn default() -> Self {
        JobLimits {
            backoff_limit: 2,
            active_deadline_seconds: None,
        }
    }
}

/// A PVC mounted into the mover pod at a path.
#[derive(Debug, Clone)]
pub struct PvcMount {
    /// The `PersistentVolumeClaim` name in the mover's namespace.
    pub claim_name: String,
    /// Absolute mount path inside the mover container.
    pub mount_path: String,
    /// Whether the mount is read-only (Backup sources are mounted read-only).
    pub read_only: bool,
}

/// All inputs needed to build a mover run's `ConfigMap` + `Job`.
pub struct MoverJobInputs<'a> {
    /// Base name for both objects (e.g. the `Backup` CR name).
    pub name: &'a str,
    /// Namespace both objects live in.
    pub namespace: &'a str,
    /// The owning CR's `OwnerReference` (so GC reaps both with the CR, §4.10).
    pub owner: OwnerReference,
    /// Resolved work spec (identity already pinned, repo connect concrete).
    pub work_spec: &'a MoverWorkSpec,
    /// Container image for the mover.
    pub image: &'a str,
    /// Image pull policy (e.g. `IfNotPresent` for a locally-loaded e2e image).
    /// `None` lets Kubernetes default it.
    pub image_pull_policy: Option<&'a str>,
    /// Job retry/deadline limits.
    pub limits: JobLimits,
    /// Optional resource requests/limits for the mover container.
    pub resources: Option<ResourceRequirements>,
    /// Optional per-recipe security-context override; merged over the
    /// hardened defaults.
    pub security_context: Option<SecurityContext>,
    /// Extra labels applied to both objects (origin/config/snapshot keys).
    pub labels: BTreeMap<String, String>,
    /// The source PVC to back up, mounted read-only at the snapshot source path
    /// (Backup ops). `None` for non-PVC sources / restore / delete ops.
    pub source_pvc: Option<PvcMount>,
    /// The repo PVC for the filesystem backend, mounted read-write at the repo
    /// path so kopia can write the repository. `None` for object-store backends.
    pub repo_pvc: Option<PvcMount>,
    /// Names of `Secret`s whose keys are exposed as env vars to the mover
    /// (`KOPIA_PASSWORD` from the encryption secret, plus backend credentials
    /// like `AWS_*` from the backend `auth.secretRef`). Each distinct secret
    /// becomes one `envFrom` entry; callers dedupe identical names (the common
    /// single-secret case collapses to one). Credentials NEVER come from the
    /// work-spec ConfigMap (§4.10/§4.11). Empty only in tests / filesystem repos.
    pub creds_secrets: Vec<String>,
    /// Name of the ConfigMap the mover writes its bootstrap result into (set only
    /// for `BootstrapRepository` runs; `None` for backup/restore/delete).
    pub result_configmap: Option<&'a str>,
    /// ServiceAccount the mover pod runs as. The mover PATCHes the owning
    /// Backup/Restore `.status`, so it needs an SA bound to the operator's
    /// status-patch rules. `None` falls back to the namespace `default` SA
    /// (which generally cannot patch `*/status`), so the controller should
    /// always supply one in a real deployment.
    pub service_account: Option<&'a str>,
    /// Extra environment passed through to the mover container: the
    /// `OTEL_EXPORTER_OTLP_*` config (when a collector is set) plus the logging
    /// vars (`RUST_LOG`, `KOPIUR_LOG_FORMAT`) so the mover inherits the
    /// controller's level/format. `(name, value)` pairs; may be empty.
    pub passthrough_env: Vec<(String, String)>,
}

/// The restricted-PSA-compatible default security context (§4.11/G16):
/// non-root, no privilege escalation, drop ALL caps, seccomp RuntimeDefault.
/// A per-recipe override (e.g. `privilegedMode` for `lost+found`) replaces it.
pub fn default_security_context() -> SecurityContext {
    SecurityContext {
        run_as_non_root: Some(true),
        allow_privilege_escalation: Some(false),
        read_only_root_filesystem: Some(false),
        capabilities: Some(k8s_openapi::api::core::v1::Capabilities {
            drop: Some(vec!["ALL".to_string()]),
            add: None,
        }),
        seccomp_profile: Some(SeccompProfile {
            type_: "RuntimeDefault".to_string(),
            localhost_profile: None,
        }),
        ..Default::default()
    }
}

/// Build the `ConfigMap` carrying the serialized work spec. Returns a
/// serialization error only if the work spec can't be JSON-encoded (never, for
/// the closed types — but propagated rather than panicked).
pub fn build_config_map(inputs: &MoverJobInputs<'_>) -> Result<ConfigMap, serde_json::Error> {
    let json = serde_json::to_string_pretty(inputs.work_spec)?;
    let mut data = BTreeMap::new();
    data.insert(WORK_SPEC_FILE.to_string(), json);
    Ok(ConfigMap {
        metadata: ObjectMeta {
            name: Some(inputs.name.to_string()),
            namespace: Some(inputs.namespace.to_string()),
            labels: Some(inputs.labels.clone()),
            owner_references: Some(vec![inputs.owner.clone()]),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    })
}

/// Build the mover `Job` that mounts the work-spec ConfigMap and runs the
/// kopiur-mover image. `restartPolicy: Never`; backoff/deadline from limits.
pub fn build_job(inputs: &MoverJobInputs<'_>) -> Job {
    let sec_ctx = inputs
        .security_context
        .clone()
        .unwrap_or_else(default_security_context);

    // Volumes + mounts: always the work-spec ConfigMap; plus the source PVC
    // (read-only, for Backup) and the repo PVC (read-write, filesystem backend).
    let mut volumes = vec![Volume {
        name: "work-spec".to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: inputs.name.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }];
    let mut volume_mounts = vec![VolumeMount {
        name: "work-spec".to_string(),
        mount_path: WORK_SPEC_MOUNT.to_string(),
        read_only: Some(true),
        ..Default::default()
    }];

    // Writable cache/logs/config for kopia. kopia defaults these under $HOME,
    // which is /nonexistent on distroless:nonroot; without this emptyDir (and the
    // KOPIA_* env below) every mover kopia call fails to create its cache. Mount
    // path is the shared default kopiur_kopia::env::DEFAULT_CACHE_DIR.
    volumes.push(Volume {
        name: "kopia-cache".to_string(),
        empty_dir: Some(EmptyDirVolumeSource::default()),
        ..Default::default()
    });
    volume_mounts.push(VolumeMount {
        name: "kopia-cache".to_string(),
        mount_path: kopiur_kopia::env::DEFAULT_CACHE_DIR.to_string(),
        ..Default::default()
    });

    if let Some(src) = &inputs.source_pvc {
        volumes.push(Volume {
            name: "source".to_string(),
            persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                claim_name: src.claim_name.clone(),
                read_only: Some(src.read_only),
            }),
            ..Default::default()
        });
        volume_mounts.push(VolumeMount {
            name: "source".to_string(),
            mount_path: src.mount_path.clone(),
            read_only: Some(src.read_only),
            ..Default::default()
        });
    }
    if let Some(repo) = &inputs.repo_pvc {
        volumes.push(Volume {
            name: "repo".to_string(),
            persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                claim_name: repo.claim_name.clone(),
                read_only: Some(repo.read_only),
            }),
            ..Default::default()
        });
        volume_mounts.push(VolumeMount {
            name: "repo".to_string(),
            mount_path: repo.mount_path.clone(),
            read_only: Some(repo.read_only),
            ..Default::default()
        });
    }

    // Credentials (KOPIA_PASSWORD + backend creds) come from Secret(s) as env,
    // never from the ConfigMap. One `envFrom` per distinct secret so an
    // object-store repo whose password and backend keys live in separate Secrets
    // both reach the mover.
    let env_from: Option<Vec<EnvFromSource>> = if inputs.creds_secrets.is_empty() {
        None
    } else {
        Some(
            inputs
                .creds_secrets
                .iter()
                .map(|secret| EnvFromSource {
                    secret_ref: Some(SecretEnvSource {
                        name: secret.clone(),
                        optional: Some(false),
                    }),
                    ..Default::default()
                })
                .collect(),
        )
    };

    // Work-spec path env, plus any passthrough (OTLP + RUST_LOG/KOPIUR_LOG_FORMAT)
    // so the mover exports to the same collector and logs at the same level/format
    // as the controller.
    let base = kopiur_kopia::env::DEFAULT_CACHE_DIR;
    let mut env = vec![
        k8s_openapi::api::core::v1::EnvVar {
            name: WORK_SPEC_ENV.to_string(),
            value: Some(format!("{WORK_SPEC_MOUNT}/{WORK_SPEC_FILE}")),
            value_from: None,
        },
        // Redirect kopia's cache/logs/config onto the writable emptyDir mounted
        // above (one mover pod = one op, so a fixed config path is safe).
        k8s_openapi::api::core::v1::EnvVar {
            name: kopiur_kopia::env::CACHE_DIRECTORY_ENV.to_string(),
            value: Some(format!("{base}/cache")),
            value_from: None,
        },
        k8s_openapi::api::core::v1::EnvVar {
            name: kopiur_kopia::env::LOG_DIR_ENV.to_string(),
            value: Some(format!("{base}/logs")),
            value_from: None,
        },
        k8s_openapi::api::core::v1::EnvVar {
            name: kopiur_kopia::env::CONFIG_PATH_ENV.to_string(),
            value: Some(format!("{base}/repository.config")),
            value_from: None,
        },
    ];
    if let Some(cm) = inputs.result_configmap {
        env.push(k8s_openapi::api::core::v1::EnvVar {
            name: RESULT_CONFIGMAP_ENV.to_string(),
            value: Some(cm.to_string()),
            value_from: None,
        });
    }
    env.extend(
        inputs
            .passthrough_env
            .iter()
            .map(|(k, v)| k8s_openapi::api::core::v1::EnvVar {
                name: k.clone(),
                value: Some(v.clone()),
                value_from: None,
            }),
    );

    let container = Container {
        name: "mover".to_string(),
        image: Some(inputs.image.to_string()),
        image_pull_policy: inputs.image_pull_policy.map(str::to_string),
        env: Some(env),
        env_from,
        volume_mounts: Some(volume_mounts),
        resources: inputs.resources.clone(),
        security_context: Some(sec_ctx),
        ..Default::default()
    };

    let pod_spec = PodSpec {
        restart_policy: Some("Never".to_string()),
        containers: vec![container],
        volumes: Some(volumes),
        service_account_name: inputs.service_account.map(str::to_string),
        ..Default::default()
    };

    Job {
        metadata: ObjectMeta {
            name: Some(inputs.name.to_string()),
            namespace: Some(inputs.namespace.to_string()),
            labels: Some(inputs.labels.clone()),
            owner_references: Some(vec![inputs.owner.clone()]),
            ..Default::default()
        },
        spec: Some(JobSpec {
            backoff_limit: Some(inputs.limits.backoff_limit),
            active_deadline_seconds: inputs.limits.active_deadline_seconds,
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(inputs.labels.clone()),
                    ..Default::default()
                }),
                spec: Some(pod_spec),
            },
            ..Default::default()
        }),
        status: None,
    }
}

/// Build an [`OwnerReference`] to a CR so child Job/ConfigMap are garbage
/// collected with it (controller owner reference, blocking-owner-deletion off).
pub fn owner_ref(kind: &str, name: &str, uid: &str) -> OwnerReference {
    OwnerReference {
        api_version: API_VERSION.to_string(),
        kind: kind.to_string(),
        name: name.to_string(),
        uid: uid.to_string(),
        controller: Some(true),
        block_owner_deletion: Some(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kopiur_mover::workspec::{
        BackupOp, MoverOptions, Operation, RepositoryConnect, ResolvedIdentity, TargetRef,
    };

    fn sample_work_spec() -> MoverWorkSpec {
        MoverWorkSpec {
            version: 1,
            operation: Operation::Backup(BackupOp {
                source_path: "/data".into(),
                tags: BTreeMap::new(),
            }),
            identity: ResolvedIdentity {
                username: "db".into(),
                hostname: "prod".into(),
                source_path: "/pvc/db".into(),
            },
            repository: RepositoryConnect::Filesystem {
                path: "/repo".into(),
            },
            target_ref: TargetRef {
                api_version: API_VERSION.into(),
                kind: "Backup".into(),
                name: "db-1".into(),
                namespace: "prod".into(),
            },
            hook_plan: Default::default(),
            options: MoverOptions::default(),
        }
    }

    fn inputs(ws: &MoverWorkSpec, limits: JobLimits) -> MoverJobInputs<'_> {
        let mut labels = BTreeMap::new();
        labels.insert(
            "kopiur.home-operations.com/origin".to_string(),
            "scheduled".to_string(),
        );
        MoverJobInputs {
            name: "db-1",
            namespace: "prod",
            owner: owner_ref("Backup", "db-1", "uid-123"),
            work_spec: ws,
            image: DEFAULT_MOVER_IMAGE,
            image_pull_policy: None,
            limits,
            resources: None,
            security_context: None,
            labels,
            source_pvc: None,
            repo_pvc: None,
            creds_secrets: Vec::new(),
            result_configmap: None,
            service_account: Some("kopiur-operator"),
            passthrough_env: Vec::new(),
        }
    }

    #[test]
    fn job_runs_under_the_supplied_service_account() {
        // The mover PATCHes the owning CR's status, so the pod must run as the
        // operator SA, not the namespace `default` SA.
        let ws = sample_work_spec();
        let job = build_job(&inputs(&ws, JobLimits::default()));
        let sa = job
            .spec
            .unwrap()
            .template
            .spec
            .unwrap()
            .service_account_name;
        assert_eq!(sa.as_deref(), Some("kopiur-operator"));
    }

    #[test]
    fn config_map_carries_serialized_work_spec() {
        let ws = sample_work_spec();
        let cm = build_config_map(&inputs(&ws, JobLimits::default())).unwrap();
        assert_eq!(cm.metadata.name.as_deref(), Some("db-1"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("prod"));
        let body = &cm.data.as_ref().unwrap()[WORK_SPEC_FILE];
        // The serialized spec round-trips back to the same MoverWorkSpec.
        let parsed: MoverWorkSpec = serde_json::from_str(body).unwrap();
        assert_eq!(parsed, ws);
        // Owner reference present so GC reaps it with the CR.
        assert_eq!(
            cm.metadata.owner_references.as_ref().unwrap()[0].uid,
            "uid-123"
        );
    }

    #[test]
    fn job_applies_backoff_and_deadline_from_limits() {
        let ws = sample_work_spec();
        let limits = JobLimits {
            backoff_limit: 5,
            active_deadline_seconds: Some(7200),
        };
        let job = build_job(&inputs(&ws, limits));
        let spec = job.spec.as_ref().unwrap();
        assert_eq!(spec.backoff_limit, Some(5));
        assert_eq!(spec.active_deadline_seconds, Some(7200));
        let pod = spec.template.spec.as_ref().unwrap();
        assert_eq!(pod.restart_policy.as_deref(), Some("Never"));
        assert_eq!(pod.containers[0].name, "mover");
    }

    #[test]
    fn job_uses_hardened_security_context_by_default() {
        let ws = sample_work_spec();
        let job = build_job(&inputs(&ws, JobLimits::default()));
        let sc = job.spec.unwrap().template.spec.unwrap().containers[0]
            .security_context
            .clone()
            .unwrap();
        assert_eq!(sc.run_as_non_root, Some(true));
        assert_eq!(sc.allow_privilege_escalation, Some(false));
        assert_eq!(
            sc.capabilities.unwrap().drop.unwrap(),
            vec!["ALL".to_string()]
        );
        assert_eq!(sc.seccomp_profile.unwrap().type_, "RuntimeDefault");
    }

    #[test]
    fn default_backoff_limit_is_two() {
        assert_eq!(JobLimits::default().backoff_limit, 2);
        assert_eq!(JobLimits::default().active_deadline_seconds, None);
    }

    #[test]
    fn job_mounts_source_and_repo_pvcs_and_secret_env() {
        let ws = sample_work_spec();
        let mut i = inputs(&ws, JobLimits::default());
        i.source_pvc = Some(PvcMount {
            claim_name: "data-pvc".into(),
            mount_path: "/data".into(),
            read_only: true,
        });
        i.repo_pvc = Some(PvcMount {
            claim_name: "repo-pvc".into(),
            mount_path: "/repo".into(),
            read_only: false,
        });
        i.creds_secrets = vec!["kopia-creds".into()];
        i.image_pull_policy = Some("IfNotPresent");

        let job = build_job(&i);
        let pod = job.spec.unwrap().template.spec.unwrap();
        let vols = pod.volumes.as_ref().unwrap();

        // Source PVC: read-only at /data.
        let src = vols
            .iter()
            .find(|v| v.name == "source")
            .expect("source vol");
        let src_claim = src.persistent_volume_claim.as_ref().unwrap();
        assert_eq!(src_claim.claim_name, "data-pvc");
        assert_eq!(src_claim.read_only, Some(true));

        // Repo PVC: read-write at /repo.
        let repo = vols.iter().find(|v| v.name == "repo").expect("repo vol");
        let repo_claim = repo.persistent_volume_claim.as_ref().unwrap();
        assert_eq!(repo_claim.claim_name, "repo-pvc");
        assert_eq!(repo_claim.read_only, Some(false));

        let container = &pod.containers[0];
        let mounts = container.volume_mounts.as_ref().unwrap();
        let src_mount = mounts.iter().find(|m| m.name == "source").unwrap();
        assert_eq!(src_mount.mount_path, "/data");
        assert_eq!(src_mount.read_only, Some(true));
        let repo_mount = mounts.iter().find(|m| m.name == "repo").unwrap();
        assert_eq!(repo_mount.mount_path, "/repo");
        assert_eq!(repo_mount.read_only, Some(false));

        // Credentials come from the Secret via envFrom (not the ConfigMap).
        let env_from = container.env_from.as_ref().expect("envFrom present");
        let secret_ref = env_from[0].secret_ref.as_ref().unwrap();
        assert_eq!(secret_ref.name, "kopia-creds");
        assert_eq!(secret_ref.optional, Some(false));

        // Image pull policy applied.
        assert_eq!(container.image_pull_policy.as_deref(), Some("IfNotPresent"));
    }

    #[test]
    fn job_mounts_each_distinct_creds_secret_and_result_configmap_env() {
        // Object-store bootstrap: password secret + backend auth secret both reach
        // the mover (one envFrom each), and the result ConfigMap name is exported.
        let ws = sample_work_spec();
        let mut i = inputs(&ws, JobLimits::default());
        i.creds_secrets = vec!["kopia-password".into(), "s3-creds".into()];
        i.result_configmap = Some("repo-bootstrap");

        let job = build_job(&i);
        let container = &job.spec.unwrap().template.spec.unwrap().containers[0];

        let env_from = container.env_from.as_ref().expect("envFrom present");
        let names: Vec<&str> = env_from
            .iter()
            .filter_map(|e| e.secret_ref.as_ref().map(|s| s.name.as_str()))
            .collect();
        assert_eq!(names, vec!["kopia-password", "s3-creds"]);

        let env = container.env.as_ref().unwrap();
        let result_env = env
            .iter()
            .find(|e| e.name == RESULT_CONFIGMAP_ENV)
            .expect("result configmap env present");
        assert_eq!(result_env.value.as_deref(), Some("repo-bootstrap"));
    }

    #[test]
    fn job_without_pvcs_or_secret_has_only_work_spec_and_cache_volumes() {
        let ws = sample_work_spec();
        let job = build_job(&inputs(&ws, JobLimits::default()));
        let pod = job.spec.unwrap().template.spec.unwrap();
        let vols = pod.volumes.as_ref().unwrap();
        // work-spec ConfigMap + the always-present writable kopia cache emptyDir.
        let names: Vec<&str> = vols.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names, vec!["work-spec", "kopia-cache"]);
        assert!(pod.containers[0].env_from.is_none());
    }

    // --- regression: the mover used to inherit no writable kopia cache, so on a
    // distroless:nonroot pod ($HOME=/nonexistent) every kopia call failed with
    // `mkdir /nonexistent: read-only file system`. The Job must mount a writable
    // emptyDir and point kopia's cache/log/config env at it. ---
    #[test]
    fn job_mounts_writable_kopia_cache_volume_and_env() {
        let ws = sample_work_spec();
        let job = build_job(&inputs(&ws, JobLimits::default()));
        let pod = job.spec.unwrap().template.spec.unwrap();

        // emptyDir volume present.
        let vol = pod
            .volumes
            .as_ref()
            .unwrap()
            .iter()
            .find(|v| v.name == "kopia-cache")
            .expect("kopia-cache volume missing");
        assert!(vol.empty_dir.is_some(), "kopia-cache must be an emptyDir");

        // Mounted at the shared default base.
        let container = &pod.containers[0];
        let mount = container
            .volume_mounts
            .as_ref()
            .unwrap()
            .iter()
            .find(|m| m.name == "kopia-cache")
            .expect("kopia-cache mount missing");
        assert_eq!(mount.mount_path, kopiur_kopia::env::DEFAULT_CACHE_DIR);

        // kopia env redirected under that base.
        let env = container.env.as_ref().unwrap();
        let get = |name: &str| {
            env.iter()
                .find(|e| e.name == name)
                .and_then(|e| e.value.clone())
                .unwrap_or_else(|| panic!("env {name} missing"))
        };
        let base = kopiur_kopia::env::DEFAULT_CACHE_DIR;
        assert_eq!(
            get(kopiur_kopia::env::CACHE_DIRECTORY_ENV),
            format!("{base}/cache")
        );
        assert_eq!(get(kopiur_kopia::env::LOG_DIR_ENV), format!("{base}/logs"));
        assert_eq!(
            get(kopiur_kopia::env::CONFIG_PATH_ENV),
            format!("{base}/repository.config")
        );
    }

    #[test]
    fn job_mounts_work_spec_configmap_with_matching_env() {
        let ws = sample_work_spec();
        let job = build_job(&inputs(&ws, JobLimits::default()));
        let pod = job.spec.unwrap().template.spec.unwrap();
        let vol = &pod.volumes.as_ref().unwrap()[0];
        assert_eq!(vol.config_map.as_ref().unwrap().name, "db-1");
        let env = &pod.containers[0].env.as_ref().unwrap()[0];
        assert_eq!(env.name, WORK_SPEC_ENV);
        assert_eq!(
            env.value.as_deref(),
            Some(format!("{WORK_SPEC_MOUNT}/{WORK_SPEC_FILE}").as_str())
        );
    }
}
