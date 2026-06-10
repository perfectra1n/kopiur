//! insta snapshot tests on generated CRD YAML.
//!
//! These pin the exact generated schema for `Repository` and `Snapshot` so any
//! accidental schema drift (a field rename, a removed validation, a changed
//! default) shows up as a reviewable snapshot diff rather than silently shipping.

use xtask::artifact::{Artifact, GEN_HEADER};

fn body(artifacts: &[Artifact], plural: &str) -> String {
    let want = format!("crds/{plural}.yaml");
    let a = artifacts
        .iter()
        .find(|a| a.rel_path == want)
        .unwrap_or_else(|| panic!("missing {want}"));
    a.content
        .strip_prefix(GEN_HEADER)
        .unwrap_or(&a.content)
        .to_string()
}

#[test]
fn snapshot_repository_crd() {
    let artifacts = xtask::crds::artifacts().expect("generate CRDs");
    insta::assert_snapshot!("repository_crd", body(&artifacts, "repositories"));
}

#[test]
fn snapshot_snapshot_crd() {
    let artifacts = xtask::crds::artifacts().expect("generate CRDs");
    insta::assert_snapshot!("snapshot_crd", body(&artifacts, "snapshots"));
}

#[test]
fn snapshot_repository_replication_crd() {
    // ADR-0005 §13(d): pin the generated schema for the net-new CRD so any
    // accidental drift in its fields shows up as a reviewable snapshot diff.
    let artifacts = xtask::crds::artifacts().expect("generate CRDs");
    insta::assert_snapshot!(
        "repository_replication_crd",
        body(&artifacts, "repositoryreplications")
    );
}
