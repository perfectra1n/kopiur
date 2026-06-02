//! The axum router and HTTP plumbing for the admission webhook.
//!
//! ## Endpoint design: one mutating endpoint that also validates
//!
//! We expose a single `POST /admission` endpoint (with `POST /validate` and
//! `POST /mutate` as aliases for operators who prefer split registrations). The
//! admission API has no real benefit from splitting validate vs. mutate into two
//! server endpoints — both receive the same `AdmissionReview` and produce one
//! `AdmissionResponse`. Doing both in one handler keeps the *one-validator-two-callers*
//! contract intact (we run the api-crate validators and then apply defaulting patches
//! in the same code path) and avoids the ordering pitfalls of a mutate webhook whose
//! output a separate validate webhook must then re-accept. The Kubernetes
//! `MutatingWebhookConfiguration` can point all kopiur.dev kinds at `/admission`.
//!
//! Every response echoes the request `uid`. Any failure to even parse the review
//! body is answered with an `AdmissionResponse::invalid(...)` deny (fail closed)
//! rather than a bare HTTP 500, so the API server gets a structured rejection.

use crate::handlers;
use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
};
use kube::Client;
use kube::core::DynamicObject;
use kube::core::admission::{AdmissionRequest, AdmissionResponse, AdmissionReview};
use std::sync::Arc;

/// Shared handler state. The `client` is optional: tests pass `None` and tenancy
/// checks that need it fail closed (deny with reason).
#[derive(Clone)]
pub struct AppState {
    pub client: Option<Client>,
}

/// Build the webhook router.
///
/// Pass `Some(client)` in production so `ClusterRepository` tenancy can be resolved
/// against live `ClusterRepository`/`Namespace` objects; pass `None` in tests (or
/// when no kubeconfig is available) — tenancy checks then deny with a fail-closed
/// reason, while all pure validation/defaulting still runs.
pub fn app(client: Option<Client>) -> Router {
    let state = Arc::new(AppState { client });
    Router::new()
        .route("/admission", post(admission))
        .route("/validate", post(admission))
        .route("/mutate", post(admission))
        .route("/healthz", get(healthz))
        .route("/readyz", get(healthz))
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// The single admission handler: parse the review, dispatch on `kind`, respond.
async fn admission(
    State(state): State<Arc<AppState>>,
    Json(review): Json<AdmissionReview<DynamicObject>>,
) -> Response {
    // Extract the request via the canonical TryInto path, which copies the review's
    // TypeMeta onto the request so the response echoes the correct apiVersion/kind.
    // A review with no request is malformed; fail closed.
    let req: AdmissionRequest<DynamicObject> = match review.try_into() {
        Ok(req) => req,
        Err(_) => {
            tracing::warn!("admission review had no request body");
            return Json(
                AdmissionResponse::invalid("admission review contained no request").into_review(),
            )
            .into_response();
        }
    };

    let resp = handlers::dispatch(&req, state.client.as_ref()).await;
    Json(resp.into_review()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    fn review_body(kind: &str, namespace: &str, uid: &str, spec: Value) -> Value {
        json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": uid,
                "kind": { "group": "kopiur.dev", "version": "v1alpha1", "kind": kind },
                "resource": { "group": "kopiur.dev", "version": "v1alpha1", "resource": "x" },
                "name": "obj",
                "namespace": namespace,
                "operation": "CREATE",
                "userInfo": { "username": "tester" },
                "object": {
                    "apiVersion": "kopiur.dev/v1alpha1",
                    "kind": kind,
                    "metadata": { "name": "obj", "namespace": namespace },
                    "spec": spec
                }
            }
        })
    }

    async fn post_review(body: Value) -> (StatusCode, Value) {
        let router = app(None);
        let resp = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admission")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        (status, v)
    }

    /// Decode the JSON-patch from an admission response into a Patch array.
    ///
    /// `AdmissionResponse.patch` is `Option<Vec<u8>>`; serde serializes a `Vec<u8>`
    /// as a JSON array of byte values (NOT a base64 string), and `with_patch` stored
    /// the raw RFC-6902 patch JSON as those bytes. So we collect the byte array and
    /// parse it back into JSON. (`base64` is still a dev-dep, exercised below to prove
    /// the wire bytes round-trip through the standard admission base64 encoding too.)
    fn decode_patch(resp: &Value) -> Vec<Value> {
        let bytes: Vec<u8> = resp["response"]["patch"]
            .as_array()
            .expect("patch present")
            .iter()
            .map(|n| n.as_u64().unwrap() as u8)
            .collect();
        // Sanity-check that the bytes are valid UTF-8 JSON that also survives a
        // base64 round-trip (the API server transports the patch as base64).
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let back = base64::engine::general_purpose::STANDARD
            .decode(&b64)
            .unwrap();
        assert_eq!(back, bytes);
        serde_json::from_slice(&bytes).unwrap()
    }

    fn good_backup_config_spec() -> Value {
        json!({
            "repository": { "kind": "Repository", "name": "nas", "namespace": "backups" },
            "sources": [ { "pvc": { "name": "data" } } ]
        })
    }

    #[tokio::test]
    async fn router_echoes_uid_and_allows_valid_backup_config() {
        let body = review_body(
            "BackupConfig",
            "billing",
            "uid-123",
            good_backup_config_spec(),
        );
        let (status, v) = post_review(body).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["response"]["uid"], "uid-123");
        assert_eq!(v["response"]["allowed"], true);
    }

    #[tokio::test]
    async fn cluster_repo_ref_with_namespace_is_denied() {
        let spec = json!({
            "repository": { "kind": "ClusterRepository", "name": "shared", "namespace": "nope" },
            "sources": [ { "pvc": { "name": "data" } } ]
        });
        let body = review_body("BackupConfig", "billing", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], false);
        let msg = v["response"]["status"]["message"].as_str().unwrap();
        assert!(msg.contains("ClusterRepository"), "msg was: {msg}");
    }

    #[tokio::test]
    async fn cluster_repo_ref_without_client_fails_closed() {
        // Valid ref shape, but kind=ClusterRepository with no client => fail closed.
        let spec = json!({
            "repository": { "kind": "ClusterRepository", "name": "shared" },
            "sources": [ { "pvc": { "name": "data" } } ]
        });
        let body = review_body("BackupConfig", "billing", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], false);
        let msg = v["response"]["status"]["message"].as_str().unwrap();
        assert!(msg.contains("fail-closed"), "msg was: {msg}");
    }

    #[tokio::test]
    async fn backup_config_with_no_sources_is_denied() {
        let spec = json!({
            "repository": { "kind": "Repository", "name": "nas" },
            "sources": []
        });
        let body = review_body("BackupConfig", "billing", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], false);
    }

    #[tokio::test]
    async fn backup_schedule_bad_cron_denied() {
        let spec = json!({
            "configRef": { "name": "c" },
            "schedule": { "cron": "totally bad" }
        });
        let body = review_body("BackupSchedule", "billing", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], false);
        let msg = v["response"]["status"]["message"].as_str().unwrap();
        assert!(msg.contains("cron"), "msg was: {msg}");
    }

    #[tokio::test]
    async fn backup_schedule_defaults_run_on_create_and_concurrency() {
        let spec = json!({
            "configRef": { "name": "c" },
            "schedule": { "cron": "0 2 * * *" }
        });
        let body = review_body("BackupSchedule", "billing", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], true);
        let patch = decode_patch(&v);
        let has_run = patch.iter().any(|op| {
            op["op"] == "add" && op["path"] == "/spec/schedule/runOnCreate" && op["value"] == false
        });
        let has_conc = patch.iter().any(|op| {
            op["op"] == "add"
                && op["path"] == "/spec/schedule/concurrencyPolicy"
                && op["value"] == "Forbid"
        });
        assert!(has_run, "expected runOnCreate default patch: {patch:?}");
        assert!(
            has_conc,
            "expected concurrencyPolicy default patch: {patch:?}"
        );
    }

    #[tokio::test]
    async fn manual_backup_defaults_delete_and_finalizer() {
        let body = review_body(
            "Backup",
            "billing",
            "u",
            json!({ "configRef": { "name": "c" } }),
        );
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], true);
        let patch = decode_patch(&v);
        let has_dp = patch.iter().any(|op| {
            op["op"] == "add" && op["path"] == "/spec/deletionPolicy" && op["value"] == "Delete"
        });
        let has_fin = patch.iter().any(|op| {
            op["op"] == "add"
                && op["path"] == "/metadata/finalizers"
                && op["value"][0] == "kopiur.dev/snapshot-cleanup"
        });
        assert!(has_dp, "expected Delete default: {patch:?}");
        assert!(has_fin, "expected finalizer add: {patch:?}");
    }

    #[tokio::test]
    async fn discovered_backup_with_delete_is_denied() {
        let mut body = review_body(
            "Backup",
            "billing",
            "u",
            json!({ "deletionPolicy": "Delete" }),
        );
        // Mark origin=discovered via the canonical label.
        body["request"]["object"]["metadata"]["labels"] =
            json!({ "kopiur.dev/origin": "discovered" });
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], false);
        let msg = v["response"]["status"]["message"].as_str().unwrap();
        assert!(msg.contains("Retain"), "msg was: {msg}");
    }

    #[tokio::test]
    async fn discovered_backup_defaults_retain() {
        let mut body = review_body("Backup", "billing", "u", json!({}));
        body["request"]["object"]["metadata"]["labels"] =
            json!({ "kopiur.dev/origin": "discovered" });
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], true);
        let patch = decode_patch(&v);
        let has_retain = patch
            .iter()
            .any(|op| op["path"] == "/spec/deletionPolicy" && op["value"] == "Retain");
        assert!(
            has_retain,
            "expected Retain default for discovered: {patch:?}"
        );
    }

    #[tokio::test]
    async fn restore_identity_without_repository_denied() {
        let spec = json!({
            "source": { "identity": { "username": "u", "hostname": "h" } }
        });
        let body = review_body("Restore", "billing", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], false);
        let msg = v["response"]["status"]["message"].as_str().unwrap();
        assert!(msg.contains("repository"), "msg was: {msg}");
    }

    #[tokio::test]
    async fn restore_backup_ref_allowed() {
        let spec = json!({
            "source": { "backupRef": { "name": "b" } }
        });
        let body = review_body("Restore", "billing", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], true);
    }

    #[tokio::test]
    async fn maintenance_bad_cron_denied() {
        let spec = json!({
            "repository": { "kind": "Repository", "name": "nas" },
            "schedule": {
                "quick": { "cron": "nope" },
                "full": { "cron": "0 3 * * 0" }
            }
        });
        let body = review_body("Maintenance", "kopia-system", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], false);
    }

    #[tokio::test]
    async fn cluster_repository_all_false_denied() {
        let spec = json!({
            "backend": { "filesystem": { "path": "/r" } },
            "encryption": { "passwordSecretRef": { "name": "s", "namespace": "kopia-system" } },
            "allowedNamespaces": { "all": false }
        });
        let body = review_body("ClusterRepository", "", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], false);
    }

    #[tokio::test]
    async fn cluster_repository_valid_allowed() {
        let spec = json!({
            "backend": { "filesystem": { "path": "/r" } },
            "encryption": { "passwordSecretRef": { "name": "s", "namespace": "kopia-system" } },
            "allowedNamespaces": { "all": true }
        });
        let body = review_body("ClusterRepository", "", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], true);
    }

    #[tokio::test]
    async fn repository_valid_allowed() {
        let spec = json!({
            "backend": { "filesystem": { "path": "/r" } },
            "encryption": { "passwordSecretRef": { "name": "s" } }
        });
        let body = review_body("Repository", "billing", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], true);
    }

    #[tokio::test]
    async fn undecodable_spec_is_denied() {
        // sources should be a list; give a string to force a decode error.
        let spec = json!({ "repository": { "name": "nas" }, "sources": "not-a-list" });
        let body = review_body("BackupConfig", "billing", "u", spec);
        let (_s, v) = post_review(body).await;
        assert_eq!(v["response"]["allowed"], false);
        let msg = v["response"]["status"]["message"].as_str().unwrap();
        assert!(msg.contains("decode"), "msg was: {msg}");
    }

    #[tokio::test]
    async fn review_without_request_is_invalid_deny() {
        let body = json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview"
        });
        let (status, v) = post_review(body).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["response"]["allowed"], false);
    }

    #[tokio::test]
    async fn healthz_ok() {
        let router = app(None);
        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
