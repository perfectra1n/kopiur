//! Optional kopia **web-UI server** surface for `Repository` / `ClusterRepository`.
//!
//! When `spec.server` is present, the operator runs `kopia server start` in a
//! Deployment and exposes it via a Service so users can browse the repository
//! through kopia's built-in HTML UI. Networking (Ingress/HTTPRoute) is left to the
//! user — only the `Service` is created.
//!
//! ## Security reality (read before changing defaults)
//!
//! kopia's server UI has **no read-only mode**: it is full read-write-**delete**, and
//! the server process holds the repository decryption key. Exposing it therefore
//! exposes full mutation of all backups. Consequences encoded here:
//!   * [`ServerAuth`] defaults (when `auth` is omitted) to [`ServerAuth::Generate`] —
//!     never to no-auth.
//!   * the no-auth variant ([`ServerAuth::Insecure`]) carries a mandatory
//!     `acknowledgeInsecure: true`, webhook-rejected otherwise (see `api::validate`).
//!
//! ## Encoding
//!
//! [`ServerAuth`] is an **externally-tagged enum** (the same representation as
//! [`crate::backend::Backend`]), so a value is always exactly one variant and
//! reconcilers `match` it exhaustively. Every variant wraps a struct (never a unit
//! variant — which would serialize as a bare string — and never a bare `bool`).

use crate::common::SecretRef;
use k8s_openapi::api::core::v1::{ResourceRequirements, SecurityContext};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// kopia's default server listen port; the resolved value is pinned to status.
pub const DEFAULT_SERVER_PORT: u16 = 51515;

/// Namespace-agnostic kopia web-UI server configuration. Embedded directly by
/// `RepositorySpec.server`; wrapped (with a required `namespace`) by
/// [`ClusterServerSpec`] for the cluster-scoped CRD.
///
/// Presence of `spec.server` means **enabled** — there is no `enabled` bool (it
/// would create two ways to express "off"). Not `Eq`: embeds k8s-openapi
/// `ResourceRequirements`/`SecurityContext` (`PartialEq` only).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServerSpec {
    /// UI authentication mode. Omitted ⇒ [`ServerAuth::Generate`] (resolved at
    /// admission, pinned to `status.server.authMode`). **Never** defaults to no-auth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<ServerAuth>,
    /// How the server is exposed as a Kubernetes `Service`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<ServerService>,
    /// Resource requests/limits for the server pod.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
    /// Override the hardened default container security context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_context: Option<SecurityContext>,
}

/// `ClusterRepository`-only server config: a cluster-scoped server has no implicit
/// namespace, so the target `namespace` is **required** (a cluster-scoped owner also
/// cannot own the namespaced Deployment/Service via ownerReferences — the controller
/// cleans them up via a finalizer + label selector instead).
///
/// The shared [`ServerSpec`] is flattened in, so the wire shape stays flat:
/// `server: { namespace, auth, service, ... }`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterServerSpec {
    /// Target namespace for the server Deployment/Service (and generated Secret).
    pub namespace: String,
    /// The shared, namespace-agnostic server configuration (auth/service/resources).
    #[serde(flatten)]
    pub server: ServerSpec,
}

/// UI authentication mode. Externally-tagged; exactly one variant by construction.
///
/// `Eq` is fine here — none of the variants embed a k8s-openapi type.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ServerAuth {
    /// The operator mints random UI credentials into an owned `Secret` and pins the
    /// reference to `status.server.generatedSecretRef`. The safe default.
    Generate(GenerateAuth),
    /// The user supplies UI credentials via a `Secret` (`name` + the two keys).
    SecretRef(ServerSecretRef),
    /// No UI authentication. Requires an explicit `acknowledgeInsecure: true`
    /// (webhook-rejected otherwise) because it exposes full read/write/delete of the
    /// repository with no login.
    Insecure(InsecureAuth),
}

impl ServerAuth {
    /// Stable discriminant string for status/metrics/logs.
    pub fn kind_str(&self) -> &'static str {
        match self {
            ServerAuth::Generate(_) => "Generate",
            ServerAuth::SecretRef(_) => "SecretRef",
            ServerAuth::Insecure(_) => "Insecure",
        }
    }
}

/// Marker payload for [`ServerAuth::Generate`]. Empty today; a sub-object so future
/// knobs (username, rotation) slot in without an API break (ADR §4.11).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GenerateAuth {
    /// UI username to provision (default `kopia`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

/// User-supplied UI credentials. All fields required so "keys are present" is a
/// structural guarantee, not a runtime validator.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServerSecretRef {
    /// Name of the `Secret` holding the UI credentials.
    pub name: String,
    /// Key within the secret holding the username.
    pub username_key: String,
    /// Key within the secret holding the password.
    pub password_key: String,
}

/// Payload for [`ServerAuth::Insecure`]; the acknowledgement lives *inside* the
/// variant so it is unreachable unless no-auth is explicitly chosen.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InsecureAuth {
    /// Must be `true`; the webhook rejects insecure mode otherwise.
    #[serde(default)]
    pub acknowledge_insecure: bool,
}

/// How the kopia server is exposed as a Kubernetes `Service`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServerService {
    /// Service type. Defaults to `ClusterIP` (routing is the user's responsibility).
    #[serde(default)]
    pub r#type: ServiceType,
    /// Listen/Service port. Defaults to [`DEFAULT_SERVER_PORT`] when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// Annotations applied to the `Service` — the seam users wire their own
    /// Ingress/LoadBalancer controller onto.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

impl ServerService {
    /// The effective Service/listen port (`port` or [`DEFAULT_SERVER_PORT`]).
    pub fn resolved_port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_SERVER_PORT)
    }
}

/// Kubernetes `Service.spec.type`. A closed enum; variants serialize as Kubernetes'
/// **exact** casing (so **no** `#[serde(rename_all)]` here — `clusterIp` would be wrong).
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default, JsonSchema)]
pub enum ServiceType {
    /// In-cluster only (default); routing to the outside is the user's job.
    #[default]
    ClusterIP,
    /// Exposed on each node's IP at a static port.
    NodePort,
    /// Provisioned through the cloud/load-balancer controller.
    LoadBalancer,
}

impl ServiceType {
    /// The Kubernetes `Service.spec.type` string.
    pub fn as_str(&self) -> &'static str {
        match self {
            ServiceType::ClusterIP => "ClusterIP",
            ServiceType::NodePort => "NodePort",
            ServiceType::LoadBalancer => "LoadBalancer",
        }
    }
}

/// Status block for the kopia server, pinned by the reconciler. Never carries a
/// password — only the resolved mode and (for `Generate`) the secret reference.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServerStatus {
    /// In-cluster endpoint, `<service>.<namespace>.svc:<port>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Namespace the server objects were last applied to. **Load-bearing**: lets the
    /// reconciler detect a `server.namespace` change and delete the stale objects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Resolved auth mode discriminant (`Generate`/`SecretRef`/`Insecure`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_mode: Option<String>,
    /// For `Generate` mode: the operator-owned Secret holding the UI credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_secret_ref: Option<SecretRef>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::from_yaml;

    #[test]
    fn generate_auth_is_externally_tagged_empty_object() {
        // The empty-struct variant is a path the rest of the API never exercises;
        // prove it encodes as `{ generate: {} }`, not a bare string.
        let auth: ServerAuth = from_yaml("generate: {}\n");
        assert_eq!(auth.kind_str(), "Generate");
        let v = serde_json::to_value(&auth).unwrap();
        assert!(v.get("generate").is_some(), "wire shape: {v}");
        assert!(v["generate"].is_object());
    }

    #[test]
    fn secret_ref_auth_roundtrips() {
        let auth: ServerAuth =
            from_yaml("secretRef:\n  name: ui\n  usernameKey: username\n  passwordKey: password\n");
        match &auth {
            ServerAuth::SecretRef(s) => {
                assert_eq!(s.name, "ui");
                assert_eq!(s.username_key, "username");
                assert_eq!(s.password_key, "password");
            }
            other => panic!("expected SecretRef, got {}", other.kind_str()),
        }
        let reparsed: ServerAuth =
            serde_json::from_value(serde_json::to_value(&auth).unwrap()).unwrap();
        assert_eq!(auth, reparsed);
    }

    #[test]
    fn insecure_auth_carries_acknowledgement() {
        let auth: ServerAuth = from_yaml("insecure:\n  acknowledgeInsecure: true\n");
        match &auth {
            ServerAuth::Insecure(i) => assert!(i.acknowledge_insecure),
            other => panic!("expected Insecure, got {}", other.kind_str()),
        }
        // Default (no ack) is the un-acknowledged state.
        let auth2: ServerAuth = from_yaml("insecure: {}\n");
        assert!(matches!(auth2, ServerAuth::Insecure(i) if !i.acknowledge_insecure));
    }

    #[test]
    fn unknown_auth_variant_is_rejected() {
        let value: serde_json::Value = serde_yaml::from_str("oauth: {}\n").unwrap();
        assert!(serde_json::from_value::<ServerAuth>(value).is_err());
    }

    #[test]
    fn service_type_serializes_with_exact_k8s_casing() {
        // The regression guard: `rename_all = camelCase` here would emit `clusterIp`.
        assert_eq!(
            serde_json::to_value(ServiceType::ClusterIP).unwrap(),
            serde_json::json!("ClusterIP")
        );
        assert_eq!(
            serde_json::to_value(ServiceType::NodePort).unwrap(),
            serde_json::json!("NodePort")
        );
        assert_eq!(
            serde_json::to_value(ServiceType::LoadBalancer).unwrap(),
            serde_json::json!("LoadBalancer")
        );
        let t: ServiceType = serde_json::from_value(serde_json::json!("LoadBalancer")).unwrap();
        assert_eq!(t, ServiceType::LoadBalancer);
    }

    #[test]
    fn service_defaults_type_clusterip_and_resolves_default_port() {
        let svc = ServerService::default();
        assert_eq!(svc.r#type, ServiceType::ClusterIP);
        assert_eq!(svc.resolved_port(), DEFAULT_SERVER_PORT);
        assert_eq!(
            ServerService {
                port: Some(8080),
                ..Default::default()
            }
            .resolved_port(),
            8080
        );
    }

    #[test]
    fn cluster_server_spec_flattens_namespace_with_base() {
        let cs: ClusterServerSpec = from_yaml(
            "namespace: kopiur-system\nauth:\n  generate: {}\nservice:\n  type: NodePort\n  port: 30515\n",
        );
        assert_eq!(cs.namespace, "kopiur-system");
        assert_eq!(cs.server.auth.as_ref().unwrap().kind_str(), "Generate");
        let svc = cs.server.service.as_ref().unwrap();
        assert_eq!(svc.r#type, ServiceType::NodePort);
        assert_eq!(svc.resolved_port(), 30515);
        // Flattened round-trip is structurally stable.
        let reparsed: ClusterServerSpec =
            serde_json::from_value(serde_json::to_value(&cs).unwrap()).unwrap();
        assert_eq!(cs, reparsed);
    }
}
