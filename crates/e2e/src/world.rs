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
    /// A running SFTP server (atmoz/sftp) with a fixed host key + the client's
    /// authorized key, and the SFTP credentials Secret (private key + known_hosts)
    /// the mover reads. Implies [`Need::Filesystem`] for a backup source PVC.
    Sftp,
    /// A running WebDAV server (basic auth) and the WebDAV credentials Secret.
    /// Implies [`Need::Filesystem`] for a backup source PVC.
    WebDav,
    /// The rclone credentials Secret (an `rclone.conf` whose `s3` remote points at
    /// the in-cluster MinIO). Implies [`Need::Minio`].
    Rclone,
}

/// Handle to a reachable cluster plus per-`Need` idempotency latches.
pub struct World {
    client: Client,
    fs: OnceCell<()>,
    minio: OnceCell<()>,
    workload_ns: OnceCell<()>,
    sftp: OnceCell<()>,
    webdav: OnceCell<()>,
    rclone: OnceCell<()>,
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
            sftp: OnceCell::new(),
            webdav: OnceCell::new(),
            rclone: OnceCell::new(),
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
                Need::Sftp => {
                    self.sftp.get_or_try_init(|| self.ensure_sftp()).await?;
                }
                Need::WebDav => {
                    self.webdav.get_or_try_init(|| self.ensure_webdav()).await?;
                }
                Need::Rclone => {
                    self.rclone.get_or_try_init(|| self.ensure_rclone()).await?;
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

    /// Stand up the in-cluster SFTP server (fixed host key + the client's
    /// authorized public key) and seed the SFTP credentials Secret the mover
    /// reads (private key + known_hosts + repo password). Needs a backup source,
    /// so it depends on the filesystem fixtures.
    async fn ensure_sftp(&self) -> Result<()> {
        self.fs.get_or_try_init(|| self.ensure_filesystem()).await?;
        let fixtures: Vec<Fixture> = vec![
            // Server-side material mounted into the sftp Deployment.
            builders::opaque_secret(
                consts::OPERATOR_NS,
                consts::SECRET_SFTP_SERVER,
                &[
                    (consts::KEY_SFTP_AUTHORIZED, consts::SFTP_CLIENT_PUBLIC_KEY),
                    (consts::KEY_SFTP_HOST_KEY, consts::SFTP_HOST_PRIVATE_KEY),
                ],
            )
            .into(),
            // Client-side credentials the mover materializes into files.
            builders::opaque_secret(
                consts::OPERATOR_NS,
                consts::SECRET_SFTP_CREDS,
                &[
                    (consts::KEY_KOPIA_PASSWORD, consts::KOPIA_PASSWORD),
                    (consts::KEY_SFTP_KEY_DATA, consts::SFTP_CLIENT_PRIVATE_KEY),
                    (consts::KEY_SFTP_KNOWN_HOSTS, consts::SFTP_KNOWN_HOSTS),
                ],
            )
            .into(),
            builders::sftp_deployment(consts::OPERATOR_NS).into(),
            builders::sftp_service(consts::OPERATOR_NS).into(),
        ];
        apply_all(&self.client, &fixtures).await?;
        wait::deployment_ready(&self.client, consts::OPERATOR_NS, "sftp").await
    }

    /// Stand up the in-cluster WebDAV server (basic auth) and seed the WebDAV
    /// credentials Secret the mover reads. Needs a backup source.
    async fn ensure_webdav(&self) -> Result<()> {
        self.fs.get_or_try_init(|| self.ensure_filesystem()).await?;
        let fixtures: Vec<Fixture> = vec![
            builders::opaque_secret(
                consts::OPERATOR_NS,
                consts::SECRET_WEBDAV_CREDS,
                &[
                    (consts::KEY_KOPIA_PASSWORD, consts::KOPIA_PASSWORD),
                    (consts::KEY_WEBDAV_USERNAME, consts::WEBDAV_USER),
                    (consts::KEY_WEBDAV_PASSWORD, consts::WEBDAV_PASSWORD),
                ],
            )
            .into(),
            builders::webdav_deployment(consts::OPERATOR_NS).into(),
            builders::webdav_service(consts::OPERATOR_NS).into(),
        ];
        apply_all(&self.client, &fixtures).await?;
        wait::deployment_ready(&self.client, consts::OPERATOR_NS, "webdav").await
    }

    /// Seed the rclone credentials Secret (an `rclone.conf` whose `s3` remote
    /// targets the in-cluster MinIO). Implies MinIO + its `kopiur-rclone` bucket.
    async fn ensure_rclone(&self) -> Result<()> {
        self.minio.get_or_try_init(|| self.ensure_minio()).await?;
        let conf = rclone_config();
        let fixtures: Vec<Fixture> = vec![
            builders::opaque_secret(
                consts::OPERATOR_NS,
                consts::SECRET_RCLONE_CREDS,
                &[
                    (consts::KEY_KOPIA_PASSWORD, consts::KOPIA_PASSWORD),
                    (consts::KEY_RCLONE_CONFIG, &conf),
                ],
            )
            .into(),
        ];
        apply_all(&self.client, &fixtures).await
    }
}

/// An `rclone.conf` defining the `miniors3` remote (an rclone `s3` provider
/// pointing at the in-cluster MinIO over plain HTTP). The mover materializes
/// this from the credentials Secret and forwards it to rclone via `--config`.
fn rclone_config() -> String {
    format!(
        "[miniors3]\n\
         type = s3\n\
         provider = Minio\n\
         access_key_id = {user}\n\
         secret_access_key = {pass}\n\
         endpoint = http://{endpoint}\n\
         region = us-east-1\n\
         force_path_style = true\n",
        user = consts::MINIO_USER,
        pass = consts::MINIO_PASS,
        endpoint = consts::MINIO_ENDPOINT,
    )
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
