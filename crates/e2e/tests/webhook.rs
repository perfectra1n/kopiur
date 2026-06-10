//! End-to-end: self-managed webhook TLS (`webhook.tls.mode: self`).
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`, skipping gracefully without
//! a cluster. The chart installs the webhook with self-managed TLS (no
//! cert-manager): the controller mints its own CA + serving cert into the
//! `kopiur-webhook-tls` Secret and injects the CA into both webhook
//! configurations' `caBundle`. This scenario asserts that whole bootstrap chain
//! against a real API server:
//!
//! 1. the serving Secret is minted (carries `tls.crt`/`tls.key`/`ca.crt`),
//! 2. `caBundle` is populated on the Validating + Mutating configs,
//! 3. a VALID CR is admitted — which, under `failurePolicy: Fail`, can only
//!    happen if the API server reached the webhook over TLS and trusted its cert
//!    (i.e. mint → Secret → pod TLS → caBundle → trust all worked), and
//! 4. an INVALID CR is rejected by the webhook (admission actually runs).
//!
//! Run: `mise run //crates/e2e:test`.

#![cfg(all(unix, feature = "e2e"))]

use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};

use k8s_openapi::api::admissionregistration::v1::{
    MutatingWebhookConfiguration, ValidatingWebhookConfiguration,
};
use k8s_openapi::api::core::v1::Secret;

use kopiur_api::{Repository, SnapshotPolicy};
use kopiur_e2e::{E2E_NAMESPACE, World, default_timeout, poll_interval, wait_until};

/// Names the chart renders for release "kopiur" (the e2e release).
const WEBHOOK_SECRET: &str = "kopiur-webhook-tls";
const VALIDATING_CONFIG: &str = "kopiur-validating";
const MUTATING_CONFIG: &str = "kopiur-mutating";

/// A spec-valid Repository (filesystem backend) — admission validates the spec
/// shape only, so the referenced Secret/PVC need not exist for it to be admitted.
fn valid_repository(name: &str) -> Repository {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "Repository",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "backend": { "filesystem": { "path": "/repo", "volume": { "pvc": { "name": "kopiur-e2e-repo" } } } },
            "encryption": { "passwordSecretRef": { "name": "kopia-creds", "key": "KOPIA_PASSWORD" } },
            "create": { "enabled": true }
        }
    }))
    .expect("valid Repository JSON deserializes")
}

/// A SnapshotPolicy whose single source names NEITHER a pvc NOR a selector. This
/// passes the CRD structural schema (both are optional) but the shared
/// `api::validate` validator the webhook runs rejects it — so it exercises the
/// admission *logic*, not just the cert plumbing.
fn invalid_backup_config(name: &str) -> SnapshotPolicy {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "SnapshotPolicy",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": "any" },
            "sources": [ {} ],
            "retention": { "keepLatest": 5 }
        }
    }))
    .expect("SnapshotPolicy JSON deserializes")
}

/// A SnapshotPolicy whose mover sets BOTH `securityContext` and
/// `inheritSecurityContextFrom` — structurally valid, but the shared `validate_mover`
/// rejects it (they're mutually exclusive). Exercises the mover-validation path
/// through admission.
fn mover_mutually_exclusive_backup_config(name: &str) -> SnapshotPolicy {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "kopiur.home-operations.com/v1alpha1",
        "kind": "SnapshotPolicy",
        "metadata": { "name": name, "namespace": E2E_NAMESPACE },
        "spec": {
            "repository": { "kind": "Repository", "name": "any" },
            "sources": [ { "pvc": { "name": "data" } } ],
            "retention": { "keepLatest": 5 },
            "mover": {
                "securityContext": { "runAsUser": 1000 },
                "inheritSecurityContextFrom": { "podSelector": { "matchLabels": { "app": "x" } } }
            }
        }
    }))
    .expect("SnapshotPolicy JSON deserializes")
}

#[tokio::test]
#[ignore = "requires a kind cluster with the operator installed (mise //crates/e2e:test)"]
async fn self_managed_webhook_tls_bootstraps_and_gates_admission() {
    let Some(world) = World::connect().await else {
        return; // no cluster: graceful no-op
    };
    let client = world.client().clone();

    // 1. The controller mints the serving Secret (tls + ca material).
    let secrets: Api<Secret> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    wait_until(
        "webhook serving Secret minted with tls + ca material",
        default_timeout(),
        poll_interval(),
        || async {
            let Some(s) = secrets.get_opt(WEBHOOK_SECRET).await? else {
                return Ok(None);
            };
            let data = s.data.unwrap_or_default();
            let has = |k: &str| data.get(k).is_some_and(|b| !b.0.is_empty());
            Ok((has("tls.crt") && has("tls.key") && has("ca.crt")).then_some(()))
        },
    )
    .await
    .expect("webhook TLS Secret should be minted by the controller");

    // 2. The caBundle is injected into BOTH webhook configurations.
    wait_until(
        "caBundle injected into the validating + mutating webhook configs",
        default_timeout(),
        poll_interval(),
        || async {
            let ok_v = validating_has_ca_bundle(&client).await?;
            let ok_m = mutating_has_ca_bundle(&client).await?;
            Ok((ok_v && ok_m).then_some(()))
        },
    )
    .await
    .expect("caBundle should be injected into both webhook configurations");

    // 3. A valid CR is admitted. Under failurePolicy=Fail this proves the API
    //    server reached the webhook over TLS and trusted its cert.
    let repos: Api<Repository> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let name = "webhook-admit-ok";
    let _ = repos.delete(name, &DeleteParams::default()).await; // clean any leftover
    repos
        .create(&PostParams::default(), &valid_repository(name))
        .await
        .expect("a valid Repository must be ADMITTED — failure here means the API server could not reach/trust the self-managed webhook");
    // Don't let it linger reconciling against absent infra.
    let _ = repos.delete(name, &DeleteParams::default()).await;

    // 4. An invalid CR is rejected BY THE WEBHOOK (admission logic runs).
    let configs: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    let bad = "webhook-deny";
    let _ = configs.delete(bad, &DeleteParams::default()).await;
    let err = configs
        .create(&PostParams::default(), &invalid_backup_config(bad))
        .await
        .expect_err("a source with neither pvc nor selector must be DENIED by the webhook");
    let msg = err.to_string();
    assert!(
        msg.contains("denied the request") || msg.to_lowercase().contains("admission"),
        "rejection should come from the admission webhook, got: {msg}"
    );
    let _ = configs.delete(bad, &DeleteParams::default()).await;

    // 5. A mover that sets BOTH securityContext and inheritSecurityContextFrom is
    //    rejected by the shared mover validator (mutually exclusive).
    let bad_mover = "webhook-deny-mover";
    let _ = configs.delete(bad_mover, &DeleteParams::default()).await;
    let err = configs
        .create(
            &PostParams::default(),
            &mover_mutually_exclusive_backup_config(bad_mover),
        )
        .await
        .expect_err("securityContext + inheritSecurityContextFrom must be DENIED by the webhook");
    let msg = err.to_string();
    assert!(
        msg.contains("denied the request") || msg.to_lowercase().contains("admission"),
        "mover mutual-exclusivity rejection should come from the webhook, got: {msg}"
    );
    let _ = configs.delete(bad_mover, &DeleteParams::default()).await;
}

/// True when every webhook in the ValidatingWebhookConfiguration carries a
/// non-empty caBundle. Returns `kube::Error` so it composes with the
/// `wait_until` polling closures (whose error type is `kube::Error`).
async fn validating_has_ca_bundle(client: &Client) -> Result<bool, kube::Error> {
    let api: Api<ValidatingWebhookConfiguration> = Api::all(client.clone());
    let Some(cfg) = api.get_opt(VALIDATING_CONFIG).await? else {
        return Ok(false);
    };
    let webhooks = cfg.webhooks.unwrap_or_default();
    Ok(!webhooks.is_empty()
        && webhooks.iter().all(|w| {
            w.client_config
                .ca_bundle
                .as_ref()
                .is_some_and(|b| !b.0.is_empty())
        }))
}

/// True when every webhook in the MutatingWebhookConfiguration carries a
/// non-empty caBundle. Returns `kube::Error` (see [`validating_has_ca_bundle`]).
async fn mutating_has_ca_bundle(client: &Client) -> Result<bool, kube::Error> {
    let api: Api<MutatingWebhookConfiguration> = Api::all(client.clone());
    let Some(cfg) = api.get_opt(MUTATING_CONFIG).await? else {
        return Ok(false);
    };
    let webhooks = cfg.webhooks.unwrap_or_default();
    Ok(!webhooks.is_empty()
        && webhooks.iter().all(|w| {
            w.client_config
                .ca_bundle
                .as_ref()
                .is_some_and(|b| !b.0.is_empty())
        }))
}
