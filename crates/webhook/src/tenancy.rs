//! Cross-namespace `ClusterRepository` tenancy enforcement (ADR §3.2).
//!
//! The validating webhook is the single enforcement point for `allowedNamespaces`
//! on a `ClusterRepository` — the controller never trusts that the API server
//! pre-filtered cross-tenant references (ADR §3.2). Consumer CRs (`BackupConfig`,
//! manual `Backup`, `Restore`, `Maintenance`) that reference a `ClusterRepository`
//! must have their namespace gated here.
//!
//! ## Pure core + thin IO (mirrors the api-crate pattern)
//!
//! The api-crate validator [`api::validate::validate_consumer_against_cluster_repo`]
//! is **fail-open** for `Selector` gates whose `matchExpressions` it cannot
//! evaluate, and returns [`api::error::ValidationError::SelectorLabelsUnavailable`]
//! when namespace labels are absent. That is correct for the api crate (it cannot
//! fetch a `Namespace`), but a webhook MUST fail **closed**.
//!
//! So the tenancy check is split:
//!
//! - [`evaluate_tenancy`] — a **pure function** taking the resolved
//!   `ClusterRepository` spec + the consumer namespace's labels as inputs. It is
//!   exhaustively unit-tested (no cluster). It hardens the api-crate behavior:
//!   * `matchExpressions` present → we do NOT treat it as "no constraint"; if we
//!     cannot prove a match we deny (fail closed).
//!   * labels unresolvable → deny (fail closed).
//! - [`resolve_tenancy_inputs`] — the **thin IO caller** that fetches the
//!   `ClusterRepository` and the consumer `Namespace`'s labels via a
//!   [`kube::Client`], then calls [`evaluate_tenancy`]. Any fetch failure (client
//!   absent, repo not found, namespace not found, API error) becomes a deny.

use kopiur_api as api;

use api::cluster_repository::{AllowedNamespaces, ClusterRepository};
use k8s_openapi::api::core::v1::Namespace;
use kube::{Api, Client};
use std::collections::BTreeMap;

/// The outcome of a tenancy evaluation. `Deny` carries a user-facing reason that is
/// surfaced verbatim in the `kubectl apply` rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenancyDecision {
    Allow,
    Deny(String),
}

impl TenancyDecision {
    pub fn is_allow(&self) -> bool {
        matches!(self, TenancyDecision::Allow)
    }
}

/// Pure tenancy decision. **Fails closed.**
///
/// `consumer_namespace` is the namespace of the consumer CR being admitted;
/// `repo_name` is the referenced `ClusterRepository`; `allowed` is its
/// `allowedNamespaces` gate; `labels` are the consumer namespace's labels (the
/// caller resolved them from a `Namespace` get — `None` means "could not resolve").
///
/// Hardening over [`api::validate::validate_consumer_against_cluster_repo`]:
/// - `Selector` with **no resolvable labels** → deny (the api crate also denies
///   here via `SelectorLabelsUnavailable`, but we phrase it as a hard deny).
/// - `Selector` carrying `matchExpressions` is **not** treated as "no constraint":
///   if every `matchExpressions` term is satisfied by the labels we allow,
///   otherwise we deny. The api crate documents `matchExpressions` as fail-open;
///   here we evaluate it and fail closed on any unsatisfied/unknown term.
pub fn evaluate_tenancy(
    consumer_namespace: &str,
    repo_name: &str,
    allowed: &AllowedNamespaces,
    labels: Option<&BTreeMap<String, String>>,
) -> TenancyDecision {
    match allowed {
        AllowedNamespaces::All(true) => TenancyDecision::Allow,
        AllowedNamespaces::All(false) => deny_not_allowed(consumer_namespace, repo_name),
        AllowedNamespaces::List(names) => {
            if names.iter().any(|n| n == consumer_namespace) {
                TenancyDecision::Allow
            } else {
                deny_not_allowed(consumer_namespace, repo_name)
            }
        }
        AllowedNamespaces::Selector(sel) => {
            let Some(labels) = labels else {
                return TenancyDecision::Deny(format!(
                    "ClusterRepository {repo_name:?} gates namespace {consumer_namespace:?} by a \
                     label selector, but the namespace's labels could not be resolved; denying \
                     (fail-closed)"
                ));
            };
            // matchLabels: every (k, v) must be present and equal.
            let match_labels = sel.match_labels.clone().unwrap_or_default();
            let labels_ok = match_labels
                .iter()
                .all(|(k, v)| labels.get(k).map(|got| got == v).unwrap_or(false));
            if !labels_ok {
                return deny_not_allowed(consumer_namespace, repo_name);
            }
            // matchExpressions: evaluate each term and fail closed on any miss or
            // unknown operator. The api crate treats these as "no constraint"; we do
            // NOT — the webhook is the enforcement point and must not fail open.
            for expr in sel.match_expressions.clone().unwrap_or_default() {
                if !expression_satisfied(&expr, labels) {
                    return deny_not_allowed(consumer_namespace, repo_name);
                }
            }
            TenancyDecision::Allow
        }
    }
}

fn deny_not_allowed(consumer_namespace: &str, repo_name: &str) -> TenancyDecision {
    TenancyDecision::Deny(format!(
        "namespace {consumer_namespace:?} is not in the allowedNamespaces of ClusterRepository \
         {repo_name:?}"
    ))
}

/// Evaluate a single `matchExpressions` term against a label set, failing closed on
/// any unknown operator.
fn expression_satisfied(
    expr: &k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement,
    labels: &BTreeMap<String, String>,
) -> bool {
    let values = expr.values.clone().unwrap_or_default();
    match expr.operator.as_str() {
        "In" => labels
            .get(&expr.key)
            .map(|got| values.iter().any(|v| v == got))
            .unwrap_or(false),
        "NotIn" => labels
            .get(&expr.key)
            .map(|got| !values.iter().any(|v| v == got))
            .unwrap_or(true),
        "Exists" => labels.contains_key(&expr.key),
        "DoesNotExist" => !labels.contains_key(&expr.key),
        // Unknown operator: fail closed.
        _ => false,
    }
}

/// Thin IO caller: fetch the referenced `ClusterRepository` and the consumer
/// `Namespace`'s labels, then delegate to [`evaluate_tenancy`]. **Fails closed** on
/// any error (no client, repo missing, namespace missing, API error).
pub async fn resolve_tenancy_inputs(
    client: Option<&Client>,
    consumer_namespace: &str,
    repo_name: &str,
) -> TenancyDecision {
    let Some(client) = client else {
        return TenancyDecision::Deny(format!(
            "cannot verify ClusterRepository {repo_name:?} tenancy for namespace \
             {consumer_namespace:?}: the webhook has no Kubernetes client; denying (fail-closed)"
        ));
    };

    let crepos: Api<ClusterRepository> = Api::all(client.clone());
    let crepo = match crepos.get(repo_name).await {
        Ok(c) => c,
        Err(e) => {
            return TenancyDecision::Deny(format!(
                "cannot resolve ClusterRepository {repo_name:?} referenced from namespace \
                 {consumer_namespace:?}: {e}; denying (fail-closed)"
            ));
        }
    };

    // Only fetch namespace labels when the gate actually needs them. For List/All the
    // labels are irrelevant, so a Namespace-get failure must not block an otherwise
    // valid reference.
    let labels = match &crepo.spec.allowed_namespaces {
        AllowedNamespaces::Selector(_) => {
            let nss: Api<Namespace> = Api::all(client.clone());
            match nss.get(consumer_namespace).await {
                Ok(ns) => Some(ns.metadata.labels.clone().unwrap_or_default()),
                Err(e) => {
                    return TenancyDecision::Deny(format!(
                        "cannot resolve labels of namespace {consumer_namespace:?} to evaluate \
                         ClusterRepository {repo_name:?} selector: {e}; denying (fail-closed)"
                    ));
                }
            }
        }
        _ => None,
    };

    evaluate_tenancy(
        consumer_namespace,
        repo_name,
        &crepo.spec.allowed_namespaces,
        labels.as_ref(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, LabelSelectorRequirement};

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn list_membership_allows() {
        let allowed = AllowedNamespaces::List(vec!["billing".into(), "staging".into()]);
        assert!(evaluate_tenancy("billing", "repo", &allowed, None).is_allow());
    }

    #[test]
    fn list_non_membership_denies() {
        let allowed = AllowedNamespaces::List(vec!["billing".into()]);
        assert!(!evaluate_tenancy("evil", "repo", &allowed, None).is_allow());
    }

    #[test]
    fn all_true_allows_all_false_denies() {
        assert!(evaluate_tenancy("any", "repo", &AllowedNamespaces::All(true), None).is_allow());
        assert!(!evaluate_tenancy("any", "repo", &AllowedNamespaces::All(false), None).is_allow());
    }

    #[test]
    fn selector_match_labels_match_allows() {
        let sel = LabelSelector {
            match_labels: Some(labels(&[("kopiur.dev/tier", "enterprise")])),
            ..Default::default()
        };
        let allowed = AllowedNamespaces::Selector(sel);
        let ns_labels = labels(&[("kopiur.dev/tier", "enterprise")]);
        assert!(evaluate_tenancy("ns", "repo", &allowed, Some(&ns_labels)).is_allow());
    }

    #[test]
    fn selector_match_labels_mismatch_denies() {
        let sel = LabelSelector {
            match_labels: Some(labels(&[("kopiur.dev/tier", "enterprise")])),
            ..Default::default()
        };
        let allowed = AllowedNamespaces::Selector(sel);
        let ns_labels = labels(&[("kopiur.dev/tier", "free")]);
        assert!(!evaluate_tenancy("ns", "repo", &allowed, Some(&ns_labels)).is_allow());
    }

    #[test]
    fn selector_without_labels_fails_closed() {
        let allowed = AllowedNamespaces::Selector(LabelSelector::default());
        let d = evaluate_tenancy("ns", "repo", &allowed, None);
        assert!(!d.is_allow());
        match d {
            TenancyDecision::Deny(msg) => assert!(msg.contains("fail-closed")),
            TenancyDecision::Allow => unreachable!(),
        }
    }

    #[test]
    fn selector_match_expressions_in_allows_and_denies() {
        let sel = LabelSelector {
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "kopiur.dev/tier".into(),
                operator: "In".into(),
                values: Some(vec!["gold".into(), "platinum".into()]),
            }]),
            ..Default::default()
        };
        let allowed = AllowedNamespaces::Selector(sel);
        assert!(evaluate_tenancy(
            "ns",
            "repo",
            &allowed,
            Some(&labels(&[("kopiur.dev/tier", "gold")]))
        )
        .is_allow());
        assert!(!evaluate_tenancy(
            "ns",
            "repo",
            &allowed,
            Some(&labels(&[("kopiur.dev/tier", "bronze")]))
        )
        .is_allow());
    }

    #[test]
    fn selector_match_expressions_exists_evaluated_not_fail_open() {
        // The api crate would treat matchExpressions as "no constraint" (fail open).
        // We evaluate it: Exists on a missing key must DENY (fail closed).
        let sel = LabelSelector {
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "kopiur.dev/team".into(),
                operator: "Exists".into(),
                values: None,
            }]),
            ..Default::default()
        };
        let allowed = AllowedNamespaces::Selector(sel);
        assert!(evaluate_tenancy(
            "ns",
            "repo",
            &allowed,
            Some(&labels(&[("kopiur.dev/team", "x")]))
        )
        .is_allow());
        assert!(
            !evaluate_tenancy("ns", "repo", &allowed, Some(&labels(&[("other", "y")]))).is_allow()
        );
    }

    #[test]
    fn selector_unknown_operator_fails_closed() {
        let sel = LabelSelector {
            match_expressions: Some(vec![LabelSelectorRequirement {
                key: "k".into(),
                operator: "Bogus".into(),
                values: None,
            }]),
            ..Default::default()
        };
        let allowed = AllowedNamespaces::Selector(sel);
        assert!(!evaluate_tenancy("ns", "repo", &allowed, Some(&labels(&[("k", "v")]))).is_allow());
    }

    #[tokio::test]
    async fn resolve_without_client_fails_closed() {
        let d = resolve_tenancy_inputs(None, "ns", "repo").await;
        assert!(!d.is_allow());
    }
}
