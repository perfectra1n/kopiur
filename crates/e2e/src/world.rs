//! `World`: the declarative provisioning entry point for the e2e scenarios.
//!
//! A test states *what cluster state it requires* as data — `world.ensure(&[Need::Minio])`
//! — and the harness builds it idempotently. [`Need`] is an exhaustive enum, so a
//! new requirement forces a new `match` arm. Each branch is guarded by a
//! per-process `OnceCell`, so the (idempotent) work runs at most once per test
//! binary; re-runs across the six binaries are cheap (SSA no-ops + waits that
//! return immediately when already satisfied).
//!
//! The host-level mirror — the kind cluster, the loaded images, the node-side
//! hostPath dirs ([`consts::HOSTPATH_REPO`]/`SRC`/`RO_REPO`), and the helm
//! install — is owned by the mise `e2e-*` tasks, NOT this module.

use anyhow::{Context, Result};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};
use tokio::sync::OnceCell;

use crate::apply::{Fixture, apply_all};
use crate::{builders, consts, default_timeout, ensure_namespace, poll_interval, try_client, wait};

/// A cluster prerequisite a scenario declares. New requirement ⇒ new variant ⇒
/// every `match` over `Need` must account for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// hostPath repo/src PVs+PVCs, a restore-destination PVC, and the
    /// filesystem-backend credentials Secret, in the operator namespace.
    Filesystem,
    /// A running MinIO (Deployment+Service) with the S3 buckets created and the
    /// good/bad S3 credential Secrets, in the operator namespace.
    Minio,
    /// The workload namespace with its own source PV/PVC and S3 credentials
    /// (implies [`Need::Minio`], since those creds target MinIO).
    WorkloadNs,
}

/// Handle to a reachable cluster plus per-`Need` idempotency latches.
pub struct World {
    client: Client,
    fs: OnceCell<()>,
    minio: OnceCell<()>,
    workload_ns: OnceCell<()>,
}

impl World {
    /// Connect to the cluster, or `None` (printing a skip notice) when none is
    /// reachable — so an `--features e2e` run is a graceful no-op off-cluster.
    pub async fn connect() -> Option<World> {
        let client = try_client().await?;
        Some(World {
            client,
            fs: OnceCell::new(),
            minio: OnceCell::new(),
            workload_ns: OnceCell::new(),
        })
    }

    /// The underlying kube client (for the scenario's own CR operations).
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Ensure every declared prerequisite exists (idempotent, deduped).
    pub async fn ensure(&self, needs: &[Need]) -> Result<()> {
        for need in needs {
            match need {
                Need::Filesystem => {
                    self.fs.get_or_try_init(|| self.ensure_filesystem()).await?;
                }
                Need::Minio => {
                    self.minio.get_or_try_init(|| self.ensure_minio()).await?;
                }
                Need::WorkloadNs => {
                    self.workload_ns
                        .get_or_try_init(|| self.ensure_workload_ns())
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn ensure_filesystem(&self) -> Result<()> {
        let fixtures: Vec<Fixture> = vec![
            builders::hostpath_pv(consts::PV_REPO, consts::HOSTPATH_REPO, "1Gi").into(),
            builders::hostpath_pv(consts::PV_SRC, consts::HOSTPATH_SRC, "1Gi").into(),
            builders::static_pvc(
                consts::OPERATOR_NS,
                consts::PVC_REPO,
                consts::PV_REPO,
                "1Gi",
            )
            .into(),
            builders::static_pvc(consts::OPERATOR_NS, consts::PVC_SRC, consts::PV_SRC, "1Gi")
                .into(),
            builders::dynamic_pvc(consts::OPERATOR_NS, consts::PVC_DST, "1Gi").into(),
            builders::opaque_secret(
                consts::OPERATOR_NS,
                consts::SECRET_FS_CREDS,
                &[(consts::KEY_KOPIA_PASSWORD, consts::KOPIA_PASSWORD)],
            )
            .into(),
        ];
        apply_all(&self.client, &fixtures).await
    }

    async fn ensure_minio(&self) -> Result<()> {
        let fixtures: Vec<Fixture> = vec![
            builders::minio_deployment(consts::OPERATOR_NS).into(),
            builders::minio_service(consts::OPERATOR_NS).into(),
            builders::opaque_secret(consts::OPERATOR_NS, consts::SECRET_S3_CREDS, &s3_creds())
                .into(),
            builders::opaque_secret(consts::OPERATOR_NS, consts::SECRET_S3_BADPW, &s3_badpw())
                .into(),
        ];
        apply_all(&self.client, &fixtures).await?;
        wait::deployment_ready(&self.client, consts::OPERATOR_NS, "minio").await?;
        self.ensure_buckets().await
    }

    /// Run the one-shot `mc` Pod to create the buckets, then clean it up. Deletes
    /// any leftover from a reused cluster first so the create can't 409.
    async fn ensure_buckets(&self) -> Result<()> {
        const POD: &str = "mc-mkbucket";
        let api: Api<Pod> = Api::namespaced(self.client.clone(), consts::OPERATOR_NS);

        let _ = api.delete(POD, &DeleteParams::default()).await;
        crate::wait_until(
            "leftover mc-mkbucket cleared",
            default_timeout(),
            poll_interval(),
            || async {
                match api.get_opt(POD).await? {
                    Some(_) => Ok(None),
                    None => Ok(Some(())),
                }
            },
        )
        .await?;

        let pod = builders::mc_bucket_pod(consts::OPERATOR_NS, POD);
        api.create(&PostParams::default(), &pod)
            .await
            .context("create mc-mkbucket pod")?;
        wait::pod_succeeded(&self.client, consts::OPERATOR_NS, POD).await?;
        // Best-effort cleanup; the buckets persist on MinIO.
        let _ = api.delete(POD, &DeleteParams::default()).await;
        Ok(())
    }

    async fn ensure_workload_ns(&self) -> Result<()> {
        // The workload-namespace S3 creds target MinIO, so it must exist first.
        // Drive the MinIO cell directly (not via `ensure`) to avoid a recursive
        // `async fn` cycle while still deduping through the same latch.
        self.minio.get_or_try_init(|| self.ensure_minio()).await?;
        ensure_namespace(&self.client, consts::WORKLOAD_NS).await?;
        let fixtures: Vec<Fixture> = vec![
            builders::hostpath_pv(consts::PV_SRC_XNS, consts::HOSTPATH_SRC, "1Gi").into(),
            builders::static_pvc(
                consts::WORKLOAD_NS,
                consts::PVC_SRC,
                consts::PV_SRC_XNS,
                "1Gi",
            )
            .into(),
            builders::opaque_secret(consts::WORKLOAD_NS, consts::SECRET_S3_CREDS, &s3_creds())
                .into(),
        ];
        apply_all(&self.client, &fixtures).await
    }
}

/// Repo password + valid S3 keys (the single-secret homelab layout).
fn s3_creds() -> [(&'static str, &'static str); 3] {
    [
        (consts::KEY_KOPIA_PASSWORD, consts::KOPIA_PASSWORD),
        (consts::KEY_AWS_ACCESS_KEY_ID, consts::MINIO_USER),
        (consts::KEY_AWS_SECRET_ACCESS_KEY, consts::MINIO_PASS),
    ]
}

/// Valid S3 keys but a WRONG repo password (safe-create guard scenario).
fn s3_badpw() -> [(&'static str, &'static str); 3] {
    [
        (consts::KEY_KOPIA_PASSWORD, consts::KOPIA_BADPW),
        (consts::KEY_AWS_ACCESS_KEY_ID, consts::MINIO_USER),
        (consts::KEY_AWS_SECRET_ACCESS_KEY, consts::MINIO_PASS),
    ]
}
