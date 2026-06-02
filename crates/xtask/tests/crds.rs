//! Tests for generated CRD YAML.
//!
//! Each generated CRD must round-trip back into a `CustomResourceDefinition`
//! (proving the YAML the cluster would receive is well-formed), expose the
//! `kopiur.dev` group + `v1alpha1` version, and carry the correct scope.

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use xtask::artifact::{Artifact, GEN_HEADER};

/// Fetch the body (header stripped) of a generated per-CRD artifact by plural.
fn crd_yaml(artifacts: &[Artifact], plural: &str) -> String {
    let want = format!("crds/{plural}.yaml");
    let a = artifacts
        .iter()
        .find(|a| a.rel_path == want)
        .unwrap_or_else(|| panic!("missing generated artifact {want}"));
    assert!(
        a.content.starts_with(GEN_HEADER),
        "{want} is missing the generated-file header"
    );
    a.content.strip_prefix(GEN_HEADER).unwrap().to_string()
}

fn parse(yaml: &str) -> CustomResourceDefinition {
    serde_yaml::from_str(yaml).expect("generated CRD YAML must parse as a CustomResourceDefinition")
}

#[test]
fn every_crd_roundtrips_with_expected_group_version_and_scope() {
    let artifacts = xtask::crds::artifacts().expect("generate CRD artifacts");

    // (plural, expected scope)
    let expected = [
        ("repositories", "Namespaced"),
        ("clusterrepositories", "Cluster"),
        ("backupconfigs", "Namespaced"),
        ("backups", "Namespaced"),
        ("backupschedules", "Namespaced"),
        ("restores", "Namespaced"),
        ("maintenances", "Namespaced"),
    ];

    for (plural, scope) in expected {
        let crd = parse(&crd_yaml(&artifacts, plural));

        assert_eq!(crd.spec.group, "kopiur.dev", "{plural} group");
        assert_eq!(
            crd.spec.names.plural, plural,
            "{plural} metadata plural mismatch"
        );
        assert_eq!(crd.spec.scope, scope, "{plural} scope");

        let versions: Vec<&str> = crd.spec.versions.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(
            versions,
            vec!["v1alpha1"],
            "{plural} should expose exactly v1alpha1"
        );
    }
}

#[test]
fn bundle_contains_all_seven_crds() {
    let artifacts = xtask::crds::artifacts().expect("generate CRD artifacts");
    let bundle = artifacts
        .iter()
        .find(|a| a.rel_path == "crds/all-crds.yaml")
        .expect("missing all-crds.yaml bundle");

    let docs: Vec<&str> = bundle.content.split("\n---\n").collect();
    assert_eq!(docs.len(), 7, "bundle should hold 7 CRD documents");

    // Every document parses as a CRD.
    for (i, doc) in docs.iter().enumerate() {
        let cleaned = doc.strip_prefix(GEN_HEADER).unwrap_or(doc);
        let crd: CustomResourceDefinition =
            serde_yaml::from_str(cleaned).unwrap_or_else(|e| panic!("bundle doc {i} parse: {e}"));
        assert_eq!(crd.spec.group, "kopiur.dev");
    }
}
