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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<SecretRef>,
    /// Advanced auth: workload identity (IRSA/WIF). Structurally present, deprioritized
    /// for the homelab default (ADR §4.11).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_identity: Option<WorkloadIdentity>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadIdentity {
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
    S3(S3Backend),
    Azure(AzureBackend),
    Gcs(GcsBackend),
    B2(B2Backend),
    Filesystem(FilesystemBackend),
    Sftp(SftpBackend),
    WebDav(WebDavBackend),
    Rclone(RcloneBackend),
}

impl Backend {
    /// Stable discriminant string for status/metrics/printcolumns.
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3Backend {
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsConfig>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AzureBackend {
    pub container: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_account: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GcsBackend {
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct B2Backend {
    pub bucket: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct FilesystemBackend {
    /// Mount path inside the mover pod. Backed by a PVC the operator mounts.
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc_name: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SftpBackend {
    pub host: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebDavBackend {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<BackendAuth>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RcloneBackend {
    pub remote_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_secret_ref: Option<SecretRef>,
}
