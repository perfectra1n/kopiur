//! Storage backends for a kopia repository.
//!
//! ADR-0003 §3.1: `Backend` is a `#[serde(tag = "kind")]` enum. This is the
//! load-bearing example of the ADR's type-safety thesis — a deserialized
//! `Backend` is *always exactly one* variant, so the "exactly one backend block"
//! rule that predecessor drafts enforced with a JSON-schema `oneOf` + webhook
//! check becomes a compile-time invariant. The webhook still validates *content*
//! (bucket names, credential reachability) but cannot receive a multi-variant value.

use crate::common::{SecretRef, TlsConfig};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Credentials for an object-store backend. Always a Secret reference. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackendAuth {
    /// Secret holding the backend's access credentials. The operator reads
    /// well-known keys (e.g. `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` for S3).
    /// ADR §3.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretRef>,
    /// Advanced auth: workload identity (IRSA/WIF). Structurally present, deprioritized
    /// for the homelab default (ADR §4.11).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_identity: Option<WorkloadIdentity>,
}

/// Cloud workload-identity binding (IRSA / GKE Workload Identity / Azure WIF):
/// the mover authenticates as a Kubernetes `ServiceAccount` instead of a static
/// Secret. Deprioritized for the homelab default. ADR §4.11.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadIdentity {
    /// Name of the `ServiceAccount` the mover pod runs as, federated to the
    /// cloud IAM role/identity that grants backend access.
    pub service_account_name: String,
}

/// The discriminated backend union. Exactly one variant by construction.
///
/// ## Representation choice
///
/// ADR-0003 §3.1 *sketches* this as `#[serde(tag = "kind")]` (a `kind: S3` inline
/// discriminant). In practice an internally-tagged enum cannot produce a valid
/// Kubernetes *structural* schema: kube's schema rewriter hoists `oneOf` branch
/// properties to the root and requires the shared `kind` property to be identical
/// across branches, but each variant needs a distinct `kind` const — a hard
/// conflict. We therefore use serde's **externally-tagged** representation
/// (`backend: { s3: {...} }`), which:
///   * is exactly the YAML shape ADR-0001 §3.1 actually used (`backend.s3.bucket`);
///   * generates a valid structural schema (a `oneOf` of distinct optional
///     properties — kubectl enforces "exactly one backend");
///   * preserves the ADR's type-safety thesis verbatim — this is still a Rust
///     `enum`, a value is still exactly one variant, and reconcilers still
///     `match` it exhaustively.
///
/// The webhook (`api::validate`) validates *content* (bucket-name format,
/// credential-secret reachability) that the schema can't express.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Backend {
    /// Amazon S3 or any S3-compatible object store (MinIO, RustFS, Ceph RGW, …).
    S3(S3Backend),
    /// Azure Blob Storage.
    Azure(AzureBackend),
    /// Google Cloud Storage.
    Gcs(GcsBackend),
    /// Backblaze B2.
    B2(B2Backend),
    /// A local filesystem path, backed by a PVC the operator mounts into the mover.
    Filesystem(FilesystemBackend),
    /// SFTP server.
    Sftp(SftpBackend),
    /// WebDAV endpoint.
    WebDav(WebDavBackend),
    /// Any rclone remote (kopia shells out to `rclone`), broadening reach to
    /// providers without a native kopia backend.
    Rclone(RcloneBackend),
}

impl Backend {
    /// Stable discriminant string for status/metrics/printcolumns.
    ///
    /// Returns the variant's PascalCase name, independent of the camelCase wire
    /// key (`backend: { s3: ... }` deserializes to [`Backend::S3`], whose
    /// `kind_str()` is `"S3"`).
    ///
    /// ```
    /// use kopiur_api::backend::{Backend, FilesystemBackend};
    ///
    /// let b = Backend::Filesystem(FilesystemBackend {
    ///     path: "/repo".into(),
    ///     volume: None,
    /// });
    /// assert_eq!(b.kind_str(), "Filesystem");
    ///
    /// // The wire key is camelCase, but the discriminant stays PascalCase.
    /// let s3: Backend = serde_json::from_value(serde_json::json!({
    ///     "s3": { "bucket": "my-backups" }
    /// }))
    /// .unwrap();
    /// assert_eq!(s3.kind_str(), "S3");
    /// ```
    pub fn kind_str(&self) -> &'static str {
        match self {
            Backend::S3(_) => "S3",
            Backend::Azure(_) => "Azure",
            Backend::Gcs(_) => "Gcs",
            Backend::B2(_) => "B2",
            Backend::Filesystem(_) => "Filesystem",
            Backend::Sftp(_) => "Sftp",
            Backend::WebDav(_) => "WebDav",
            Backend::Rclone(_) => "Rclone",
        }
    }
}

/// S3 / S3-compatible object-store backend. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3Backend {
    /// Bucket holding the kopia repository.
    pub bucket: String,
    /// Key prefix under the bucket, letting several repositories share one bucket
    /// (e.g. `clusters/prod/`). Empty/absent means the bucket root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// S3 endpoint host. Omit for AWS; set it for MinIO/RustFS/other
    /// S3-compatible stores.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// S3 region. Required by AWS and some compatible providers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Access credentials (Secret ref / workload identity). ADR §3.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
    /// TLS overrides for self-signed CAs or HTTP-only endpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,
}

/// Azure Blob Storage backend. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AzureBackend {
    /// Blob container holding the kopia repository.
    pub container: String,
    /// Blob-name prefix within the container; empty/absent means the container root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Storage-account name (when not inferred from credentials).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_account: Option<String>,
    /// Access credentials (Secret ref / workload identity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
}

/// Google Cloud Storage backend. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GcsBackend {
    /// GCS bucket holding the kopia repository.
    pub bucket: String,
    /// Object-name prefix within the bucket; empty/absent means the bucket root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Access credentials (service-account key Secret / workload identity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
}

/// Backblaze B2 backend. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct B2Backend {
    /// B2 bucket holding the kopia repository.
    pub bucket: String,
    /// Object-name prefix within the bucket; empty/absent means the bucket root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Access credentials (application key ID/key Secret).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
}

/// Local-filesystem backend: kopia writes the repository to a path inside the
/// mover pod. The path is populated by a [`RepoVolume`] the operator mounts — a
/// `PersistentVolumeClaim` or an inline NFS export. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FilesystemBackend {
    /// Mount path inside the mover pod where kopia writes the repository (e.g. `/repo`).
    pub path: String,
    /// What backs `path`. A PVC (`{ pvc: { name } }`) or an inline NFS export
    /// (`{ nfs: { server, path } }`). Absent for a path already present on the
    /// node/image (a `hostPath`/baked-in mount; mainly the e2e harness).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume: Option<RepoVolume>,
}

/// What backs a filesystem repository's mount path. Externally-tagged; exactly
/// one variant. ADR §3.1.
///
/// Wire shape: `volume: { pvc: { name: "repo-pvc" } }` or
/// `volume: { nfs: { server: "nas.lan", path: "/export/kopia" } }`.
///
/// kopia has no NFS backend — NFS is reached through the *filesystem* backend by
/// mounting the export at `path`. Modeling it as a volume source (not a `Backend`
/// variant) keeps that truth: the kopia connect spec is `Filesystem { path }`
/// regardless, and the mover is transparent to how the path is mounted.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RepoVolume {
    /// A `PersistentVolumeClaim` mounted read-write at the repo path.
    Pvc(PvcVolume),
    /// An inline NFS export mounted directly (no PVC).
    Nfs(NfsVolume),
}

impl RepoVolume {
    /// Stable discriminant string for status/metrics.
    pub fn kind_str(&self) -> &'static str {
        match self {
            RepoVolume::Pvc(_) => "Pvc",
            RepoVolume::Nfs(_) => "Nfs",
        }
    }
}

/// A `PersistentVolumeClaim` mounted into the mover pod. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PvcVolume {
    /// Name of the `PersistentVolumeClaim` to mount (in the mover's namespace).
    pub name: String,
}

/// An inline NFS export mounted directly into the mover pod — no PVC, no
/// StorageClass. Used both as a filesystem-repo volume and as a backup source.
/// ADR §3.1/§3.3.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NfsVolume {
    /// NFS server hostname or IP (e.g. `nas.lan` or `expanse.internal`).
    pub server: String,
    /// Exported path on the NFS server (e.g. `/export/kopia` or `/mnt/eros/Media`).
    pub path: String,
}

/// SFTP backend. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SftpBackend {
    /// SFTP server hostname or IP.
    pub host: String,
    /// Remote path on the server that holds the kopia repository.
    pub path: String,
    /// TCP port; defaults to 22 when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// SSH username to connect as.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Credentials (e.g. SSH private key / known-hosts) sourced from a Secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
}

/// WebDAV backend. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebDavBackend {
    /// WebDAV collection URL holding the kopia repository.
    pub url: String,
    /// HTTP basic-auth credentials sourced from a Secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
}

/// rclone-remote backend; kopia shells out to `rclone` so any rclone-supported
/// provider is reachable. ADR §3.1.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RcloneBackend {
    /// rclone path in `remote:path` form (the remote name must exist in the
    /// supplied rclone config).
    pub remote_path: String,
    /// Secret holding the `rclone.conf` that defines the remote referenced by
    /// `remote_path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_secret_ref: Option<SecretRef>,
}
