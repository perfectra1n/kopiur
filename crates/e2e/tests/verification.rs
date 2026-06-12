//! e2e: first-class backup verification — a `SnapshotPolicy.spec.verification.quick`
//! with a `successExpr` drives a `kopia snapshot verify` mover Job and stamps
//! `status.lastVerified` (ADR-0005 §4).
//!
//! Gated by `#[cfg(feature = "e2e")]` + `#[ignore]`; driven by
//! `mise run //crates/e2e:test`. Skips gracefully without a cluster.

#![cfg(all(unix, feature = "e2e"))]

mod common;
use common::*;

use kube::Api;
use kube::api::{Patch, PatchParams};

use kopiur_api::SnapshotPolicy;
use kopiur_e2e::{E2E_NAMESPACE, Need, World, default_timeout, poll_interval, wait_until};

/// Verification (ADR-0005 §4): a `SnapshotPolicy.spec.verification.quick` with an
/// every-minute cron and a `successExpr` over the result drives a `kopia snapshot
/// verify` mover Job; on success the controller stamps `status.lastVerified`.
#[tokio::test]
#[ignore = "requires the e2e harness (mise run //crates/e2e:test): kind + built images + helm install"]
async fn verification_quick_with_success_expr_stamps_last_verified() {
    let Some(world) = World::connect().await else {
        return;
    };
    world.ensure(&[Need::Filesystem]).await.expect("fixtures");
    let client = world.client().clone();
    // Seed a real snapshot so quick-verify has something to verify.
    ensure_seed(
        &client,
        "e2e-verify-repo",
        "e2e-verify-policy",
        "e2e-verify-seed",
        "verify",
    )
    .await;

    let policies: Api<SnapshotPolicy> = Api::namespaced(client.clone(), E2E_NAMESPACE);
    // Patch verification onto the existing policy: every-minute quick verify, gated by
    // a successExpr asserting the verify reported zero errors.
    let patch = serde_json::json!({
        "spec": { "verification": {
            "quick": { "cron": "* * * * *" },
            "successExpr": "stats.errors == 0"
        } }
    });
    policies
        .patch(
            "e2e-verify-policy",
            &PatchParams::default(),
            &Patch::Merge(&patch),
        )
        .await
        .expect("patch verification onto the SnapshotPolicy");

    // Within a couple of minutes a quick-verify Job runs and stamps lastVerified.
    wait_until(
        "SnapshotPolicy.status.lastVerified is stamped by a passing quick verify",
        default_timeout(),
        poll_interval(),
        || async {
            let s = status_json(&policies, "e2e-verify-policy").await;
            Ok(s.get("lastVerified")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|_| ()))
        },
    )
    .await
    .expect(
        "a passing quick verify (successExpr stats.errors == 0) must stamp status.lastVerified",
    );
}
