//! Cluster-dependent integration test (feature `integration`, `#[ignore]`).
//!
//! Applies the generated CRDs to a Kubernetes API server via `kubectl apply
//! --server-side -f deploy/crds/`. The real validation here is that the API
//! server accepts the structural schemas — a mis-encoded enum or invalid schema
//! is rejected at apply time.
//!
//! Server-side apply is REQUIRED, not a stylistic choice: the `SnapshotPolicy` CRD
//! is large (its `runJob` hook embeds a full Kubernetes `JobSpec`), and a
//! client-side `kubectl apply` would store the entire object in the
//! `last-applied-configuration` annotation, blowing past the 256KB annotation
//! limit. This was caught running against a real kind cluster. Operators must
//! install these CRDs server-side (documented in docs/install.md).
//!
//! We do NOT create a cluster here: `scripts/with-kind.sh` provides
//! an ephemeral kind cluster and exports `KUBECONFIG`. If `kubectl` or
//! `KUBECONFIG` is absent, we skip gracefully (the test passes without asserting
//! against a cluster) so the hermetic suite is unaffected.
//!
//! Run with:
//!   scripts/with-kind.sh cargo test -p xtask --features integration -- --include-ignored

use std::process::Command;

use xtask::paths::deploy_dir;

#[test]
#[cfg_attr(not(feature = "integration"), ignore)]
fn kubectl_apply_generated_crds() {
    // Skip gracefully without a configured cluster.
    if std::env::var_os("KUBECONFIG").is_none() {
        eprintln!("KUBECONFIG not set; skipping cluster apply test");
        return;
    }
    if which_kubectl().is_none() {
        eprintln!("kubectl not found on PATH; skipping cluster apply test");
        return;
    }

    let crds_dir = deploy_dir().join("crds");
    assert!(
        crds_dir.is_dir(),
        "{} should exist (run `cargo xtask gen-all`)",
        crds_dir.display()
    );

    // Apply the per-CRD files (not all-crds.yaml, to avoid applying each twice).
    // `--server-side` avoids the last-applied-configuration annotation, which the
    // large SnapshotPolicy schema would otherwise overflow.
    let status = Command::new("kubectl")
        .args(["apply", "--server-side", "--force-conflicts", "-f"])
        .arg(crds_dir.join("all-crds.yaml"))
        .status()
        .expect("failed to spawn kubectl");

    assert!(
        status.success(),
        "kubectl apply --server-side -f {} exited with {status}",
        crds_dir.join("all-crds.yaml").display()
    );
}

fn which_kubectl() -> Option<()> {
    Command::new("kubectl")
        .arg("version")
        .arg("--client")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|_| ())
}
