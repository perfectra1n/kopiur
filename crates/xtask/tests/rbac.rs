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
        rule_grants(&rules, "kopiur.home-operations.com", "backups"),
        "must grant backups under kopiur.home-operations.com"
    );
    assert!(
        rule_grants(&rules, "kopiur.home-operations.com", "clusterrepositories"),
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
    // kube's Recorder writes events.k8s.io/v1 Events — without this group the
    // create is 403'd and reconcile-outcome Events (e.g. MaintenanceNotConfigured,
    // SnapshotOrphaned) are silently dropped.
    assert!(
        rule_grants(&rules, "events.k8s.io", "events"),
        "must grant events under events.k8s.io (kube Recorder target)"
    );
    assert!(
        rule_grants(&rules, "", "events"),
        "must also grant legacy core events"
    );
}

#[test]
fn namespaced_role_omits_cluster_crds_but_keeps_mover_minting() {
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
    assert!(rule_grants(&rules, "kopiur.home-operations.com", "backups"));
    assert!(rule_grants(&rules, "batch", "jobs"));
    // Events surfacing works in namespaced mode too (Recorder → events.k8s.io/v1).
    assert!(
        rule_grants(&rules, "events.k8s.io", "events"),
        "namespaced role must grant events under events.k8s.io"
    );
    // ...the cluster-scoped CRD is dropped in namespaced mode.
    assert!(
        !rule_grants(&rules, "kopiur.home-operations.com", "clusterrepositories"),
        "namespaced role must NOT include cluster-scoped clusterrepositories"
    );
    // ...but the mover SA + RoleBinding minting rules ARE retained: even in
    // namespaced mode the controller mints the least-privilege mover RBAC in the
    // (in-scope) workload namespace before each mover Job (ADR §4.12).
    assert!(
        rule_grants(&rules, "", "serviceaccounts"),
        "namespaced role must mint the mover ServiceAccount in its own namespace"
    );
    assert!(
        rule_grants(&rules, "rbac.authorization.k8s.io", "rolebindings"),
        "namespaced role must mint the mover RoleBinding in its own namespace"
    );
}

/// The dedicated, least-privilege mover role is generated for both install modes
/// and grants ONLY what the mover uses (status patch on the owning CRDs + the
/// bootstrap-result configmap patch) — never the operator's broad rule set.
#[test]
fn mover_role_is_least_privilege() {
    let artifacts = xtask::rbac::artifacts().expect("generate RBAC artifacts");
    for rel in ["rbac/mover-clusterrole.yaml", "rbac/mover-role.yaml"] {
        let a = artifact(&artifacts, rel);
        assert!(a.content.starts_with(RBAC_HEADER), "{rel} missing header");
        let role_rules = docs(&a.content)
            .into_iter()
            .find_map(|d| {
                let v: serde_yaml::Value = serde_yaml::from_str(&d).ok()?;
                match v.get("kind").and_then(|k| k.as_str()) {
                    Some("ClusterRole") => serde_yaml::from_str::<ClusterRole>(&d).ok()?.rules,
                    Some("Role") => serde_yaml::from_str::<Role>(&d).ok()?.rules,
                    _ => None,
                }
            })
            .unwrap_or_else(|| panic!("{rel} must contain a (Cluster)Role with rules"));

        // Grants the mover's actual API surface.
        assert!(
            rule_grants(&role_rules, "kopiur.home-operations.com", "backups/status"),
            "{rel} must grant backups/status"
        );
        assert!(
            rule_grants(&role_rules, "", "configmaps"),
            "{rel} must grant configmaps (bootstrap result write)"
        );
        // Least privilege: NONE of the operator's broad grants leak into the mover.
        for (g, r) in [
            ("batch", "jobs"),
            ("", "secrets"),
            ("", "pods"),
            ("kopiur.home-operations.com", "backups"),
        ] {
            assert!(
                !rule_grants(&role_rules, g, r),
                "{rel} must NOT grant {g}/{r} (mover is least-privilege)"
            );
        }
    }
}
