//! Tests for generated RBAC YAML.

use k8s_openapi::api::rbac::v1::{ClusterRole, PolicyRule, Role};
use xtask::artifact::{Artifact, RBAC_HEADER};

fn artifact<'a>(artifacts: &'a [Artifact], rel: &str) -> &'a Artifact {
    artifacts
        .iter()
        .find(|a| a.rel_path == rel)
        .unwrap_or_else(|| panic!("missing generated artifact {rel}"))
}

/// Split a multi-doc RBAC file into its individual YAML documents (header off).
fn docs(content: &str) -> Vec<String> {
    let body = content.strip_prefix(RBAC_HEADER).unwrap_or(content);
    body.split("\n---\n").map(str::to_string).collect()
}

fn rule_grants(rules: &[PolicyRule], group: &str, resource: &str) -> bool {
    rules.iter().any(|r| {
        r.api_groups
            .as_ref()
            .is_some_and(|g| g.iter().any(|x| x == group))
            && r.resources
                .as_ref()
                .is_some_and(|res| res.iter().any(|x| x == resource))
    })
}

#[test]
fn clusterrole_parses_and_grants_expected_rules() {
    let artifacts = xtask::rbac::artifacts().expect("generate RBAC artifacts");
    let a = artifact(&artifacts, "rbac/operator-clusterrole.yaml");
    assert!(a.content.starts_with(RBAC_HEADER));

    // The ClusterRole is one of the documents in the file.
    let clusterrole = docs(&a.content)
        .into_iter()
        .find_map(|d| {
            let v: serde_yaml::Value = serde_yaml::from_str(&d).ok()?;
            if v.get("kind").and_then(|k| k.as_str()) == Some("ClusterRole") {
                serde_yaml::from_str::<ClusterRole>(&d).ok()
            } else {
                None
            }
        })
        .expect("ClusterRole document must parse");

    let rules = clusterrole.rules.expect("ClusterRole must have rules");

    assert!(
        rule_grants(&rules, "kopia.io", "backups"),
        "must grant backups under kopia.io"
    );
    assert!(
        rule_grants(&rules, "kopia.io", "clusterrepositories"),
        "cluster role must include cluster-scoped clusterrepositories"
    );
    assert!(
        rule_grants(&rules, "batch", "jobs"),
        "must grant jobs under batch"
    );
    assert!(
        rule_grants(&rules, "", "serviceaccounts"),
        "cluster role must allow minting per-namespace serviceaccounts"
    );
}

#[test]
fn namespaced_role_parses_and_omits_cluster_scoped_bits() {
    let artifacts = xtask::rbac::artifacts().expect("generate RBAC artifacts");
    let a = artifact(&artifacts, "rbac/operator-role.yaml");

    let role = docs(&a.content)
        .into_iter()
        .find_map(|d| {
            let v: serde_yaml::Value = serde_yaml::from_str(&d).ok()?;
            if v.get("kind").and_then(|k| k.as_str()) == Some("Role") {
                serde_yaml::from_str::<Role>(&d).ok()
            } else {
                None
            }
        })
        .expect("Role document must parse");

    let rules = role.rules.expect("Role must have rules");

    // Same core grants...
    assert!(rule_grants(&rules, "kopia.io", "backups"));
    assert!(rule_grants(&rules, "batch", "jobs"));
    // ...but cluster-scoped bits are dropped in namespaced mode.
    assert!(
        !rule_grants(&rules, "kopia.io", "clusterrepositories"),
        "namespaced role must NOT include cluster-scoped clusterrepositories"
    );
    assert!(
        !rule_grants(&rules, "", "serviceaccounts"),
        "namespaced role must NOT mint serviceaccounts cluster-wide"
    );
}
