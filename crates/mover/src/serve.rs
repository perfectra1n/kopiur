//! The **serve** work spec: the JSON contract for running the kopia web UI.
//!
//! Distinct from [`crate::workspec::MoverWorkSpec`] (the run-once
//! backup/restore/delete contract). A server run is long-lived and has no terminal
//! status, so it is modeled as its own strongly-typed spec and driven by the
//! mover's `serve` entrypoint, which connects the repository then `exec`s
//! `kopia server start`.
//!
//! Like the run-once spec this is **pure data** plus serde — no kube, no kopia
//! subprocess. Credentials are NOT here: the repository password
//! (`KOPIA_PASSWORD`) and the UI password (`KOPIA_SERVER_PASSWORD`) arrive as env
//! vars from a mounted Secret, so they never land in a ConfigMap.

use serde::{Deserialize, Serialize};

use crate::workspec::RepositoryConnect;

/// UI authentication for the served repository. Externally tagged: exactly one
/// variant. Mirrors the api crate's `ServerAuth`, minus the secret material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ServerAuthSpec {
    /// Require a UI login. `username` is non-secret; the password arrives via the
    /// `KOPIA_SERVER_PASSWORD` env var (mounted Secret).
    Password {
        /// HTTP basic-auth username for the UI.
        username: String,
    },
    /// No UI authentication (`--without-password`). Only reachable when the CR
    /// explicitly acknowledged the insecure mode (webhook-enforced upstream).
    None {},
}

impl ServerAuthSpec {
    /// Convert to the kopia client's auth mode (still secret-free).
    pub fn to_auth_mode(&self) -> kopiur_kopia::ServerAuthMode {
        match self {
            ServerAuthSpec::Password { username } => kopiur_kopia::ServerAuthMode::Password {
                username: username.clone(),
            },
            ServerAuthSpec::None {} => kopiur_kopia::ServerAuthMode::None,
        }
    }

    /// Stable discriminant string for logging.
    pub fn kind_str(&self) -> &'static str {
        match self {
            ServerAuthSpec::Password { .. } => "Password",
            ServerAuthSpec::None {} => "None",
        }
    }
}

/// The full work spec the controller writes for a kopia server (UI) run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerWorkSpec {
    /// Schema version for forward compatibility.
    #[serde(default = "default_spec_version")]
    pub version: u32,
    /// How to connect to the repository (reuses the run-once backend contract).
    pub repository: RepositoryConnect,
    /// Port to listen on; the server always binds `0.0.0.0` so a Service can reach it.
    pub listen_port: u16,
    /// UI authentication mode.
    pub auth: ServerAuthSpec,
    /// Serve the embedded HTML UI. Defaults to enabled.
    #[serde(default = "default_true")]
    pub ui: bool,
}

impl ServerWorkSpec {
    /// Build the kopia client's [`kopiur_kopia::ServerStartSpec`] — binds
    /// `0.0.0.0:<listenPort>` so the Service can reach it.
    pub fn to_start_spec(&self) -> kopiur_kopia::ServerStartSpec {
        kopiur_kopia::ServerStartSpec {
            address: format!("0.0.0.0:{}", self.listen_port),
            auth: self.auth.to_auth_mode(),
            ui: self.ui,
        }
    }
}

fn default_spec_version() -> u32 {
    1
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(spec: &ServerWorkSpec) -> ServerWorkSpec {
        let json = serde_json::to_string_pretty(spec).unwrap();
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn password_auth_roundtrip_and_start_spec() {
        let spec = ServerWorkSpec {
            version: 1,
            repository: RepositoryConnect::S3 {
                bucket: "backups".into(),
                endpoint: Some("https://minio.local".into()),
                prefix: None,
                region: None,
                disable_tls: false,
                disable_tls_verification: false,
                ambient_credentials: false,
            },
            listen_port: 51515,
            auth: ServerAuthSpec::Password {
                username: "kopia".into(),
            },
            ui: true,
        };
        assert_eq!(roundtrip(&spec), spec);
        let start = spec.to_start_spec();
        assert_eq!(start.address, "0.0.0.0:51515");
        assert_eq!(
            start.auth,
            kopiur_kopia::ServerAuthMode::Password {
                username: "kopia".into()
            }
        );
        assert!(start.ui);
    }

    #[test]
    fn none_auth_externally_tagged_and_converts() {
        let spec: ServerWorkSpec = serde_json::from_str(
            r#"{"repository":{"filesystem":{"path":"/repo"}},"listenPort":8080,"auth":{"none":{}}}"#,
        )
        .unwrap();
        assert_eq!(spec.version, 1, "version defaults");
        assert!(spec.ui, "ui defaults true");
        assert_eq!(spec.auth.kind_str(), "None");
        assert_eq!(
            spec.to_start_spec().auth,
            kopiur_kopia::ServerAuthMode::None
        );
        // Wire shape is externally tagged.
        let v = serde_json::to_value(&spec).unwrap();
        assert!(v["auth"]["none"].is_object());
    }

    #[test]
    fn unknown_auth_variant_rejected() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"repository":{"filesystem":{"path":"/r"}},"listenPort":1,"auth":{"oauth":{}}}"#,
        )
        .unwrap();
        assert!(serde_json::from_value::<ServerWorkSpec>(v).is_err());
    }
}
