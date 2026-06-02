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
    ConfigMap, ConfigMapVolumeSource, Container, PodSpec, PodTemplateSpec, ResourceRequirements,
    SeccompProfile, SecurityContext, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use kopiur_mover::workspec::MoverWorkSpec;

use crate::consts::API_VERSION;

/// Default mover image; overridable per deployment via the controller config.
/// `:latest` is deliberately avoided (G15) — callers should pin a digest/tag.
pub const DEFAULT_MOVER_IMAGE: &str = "ghcr.io/perfectra1n/kopiur-mover:v0.1.0";

/// Path inside the mover pod where the work-spec ConfigMap is mounted.
pub const WORK_SPEC_MOUNT: &str = "/etc/kopiur";
/// File name of the work spec within the mount.
pub const WORK_SPEC_FILE: &str = "work-spec.json";
/// Env var the mover reads for the work-spec path (matches the mover binary).
pub const WORK_SPEC_ENV: &str = "KOPIUR_WORK_SPEC_PATH";

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
    /// Job retry/deadline limits.
    pub limits: JobLimits,
    /// Optional resource requests/limits for the mover container.
    pub resources: Option<ResourceRequirements>,
    /// Optional per-recipe security-context override; merged over the
    /// hardened defaults.
    pub security_context: Option<SecurityContext>,
    /// Extra labels applied to both objects (origin/config/snapshot keys).
    pub labels: BTreeMap<String, String>,
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

    let container = Container {
        name: "mover".to_string(),
        image: Some(inputs.image.to_string()),
        env: Some(vec![k8s_openapi::api::core::v1::EnvVar {
            name: WORK_SPEC_ENV.to_string(),
            value: Some(format!("{WORK_SPEC_MOUNT}/{WORK_SPEC_FILE}")),
            value_from: None,
        }]),
        volume_mounts: Some(vec![VolumeMount {
            name: "work-spec".to_string(),
            mount_path: WORK_SPEC_MOUNT.to_string(),
            read_only: Some(true),
            ..Default::default()
        }]),
        resources: inputs.resources.clone(),
        security_context: Some(sec_ctx),
        ..Default::default()
    };

    let pod_spec = PodSpec {
        restart_policy: Some("Never".to_string()),
        containers: vec![container],
        volumes: Some(vec![Volume {
            name: "work-spec".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: inputs.name.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }]),
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
        labels.insert("kopia.io/origin".to_string(), "scheduled".to_string());
        MoverJobInputs {
            name: "db-1",
            namespace: "prod",
            owner: owner_ref("Backup", "db-1", "uid-123"),
            work_spec: ws,
            image: DEFAULT_MOVER_IMAGE,
            limits,
            resources: None,
            security_context: None,
            labels,
        }
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
