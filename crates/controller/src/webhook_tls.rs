//! Self-managed admission-webhook TLS (the `tls.mode: self` chart path).
//!
//! When the chart provisions the webhook with `webhook.tls.mode: self`, the
//! operator — not cert-manager — owns the webhook's serving certificate. The
//! controller mints a **long-lived self-signed CA** and a short-lived **leaf**
//! signed by it, writes both into the serving `Secret`, and SSA-patches the CA
//! into the `caBundle` of the Validating/Mutating webhook configurations so the
//! API server trusts the webhook (see [`crate::webhook_tls::ensure`]).
//!
//! ## Why a stable CA + rotating leaf
//!
//! The API server's trust anchor is the **CA**, pinned in each webhook config's
//! `caBundle`. Keeping the CA long-lived (10y) means `caBundle` is written once
//! and effectively never changes, so we can rotate the served **leaf** freely
//! underneath it without the dual-CA overlap dance that generic rotators
//! (OPA `cert-controller`, controller-runtime's `rotator`) implement. The leaf
//! is renewed well before expiry; the webhook hot-reloads it from its mounted
//! files, so rotation is zero-downtime.
//!
//! This module splits the **pure** crypto (CA/leaf minting, the renewal
//! predicate — all unit-tested without a cluster) from the thin Kubernetes IO in
//! [`ensure`].

use time::{Duration, OffsetDateTime};

/// CA validity. Long enough that the `caBundle` is effectively write-once; the
/// leaf rotates underneath it (see module docs).
pub const CA_VALIDITY_DAYS: i64 = 3650; // 10 years
/// Leaf serving-cert validity.
pub const LEAF_VALIDITY_DAYS: i64 = 365; // 1 year
/// Renew the leaf once it is within this window of expiry. With a 1-year leaf
/// this rotates roughly every ~9 months, leaving a wide overlap so a webhook
/// that has not yet hot-reloaded keeps serving a still-valid cert.
pub const LEAF_RENEW_BEFORE_DAYS: i64 = 90;

/// Common Name stamped on the self-signed CA certificate.
const CA_COMMON_NAME: &str = "kopiur-webhook-ca";

/// A self-signed CA: its certificate and signing key, both PEM-encoded.
#[derive(Debug, Clone)]
pub struct Ca {
    /// PEM-encoded CA certificate (this is what lands in `caBundle`).
    pub cert_pem: String,
    /// PEM-encoded CA private key. Persisted so the controller can mint new
    /// leaves after a restart without rotating the CA (and thus the `caBundle`).
    pub key_pem: String,
}

/// A serving leaf certificate signed by the [`Ca`].
#[derive(Debug, Clone)]
pub struct Leaf {
    /// PEM-encoded leaf certificate (the webhook serves this as `tls.crt`).
    pub cert_pem: String,
    /// PEM-encoded leaf private key (`tls.key`).
    pub key_pem: String,
    /// Leaf `notAfter` as a Unix timestamp (seconds). Persisted as a Secret
    /// annotation and fed to [`needs_renewal`] so rotation needs no X.509 parser.
    pub not_after_unix: i64,
}

/// Errors from the pure cert-minting layer.
#[derive(Debug, thiserror::Error)]
pub enum CertError {
    /// `rcgen` failed to generate a key pair or certificate.
    #[error("certificate generation failed: {0}")]
    Generate(#[from] rcgen::Error),
}

/// Mint a fresh self-signed CA valid for [`CA_VALIDITY_DAYS`].
pub fn mint_ca() -> Result<Ca, CertError> {
    mint_ca_at(OffsetDateTime::now_utc())
}

/// [`mint_ca`] with an injectable "now" for deterministic tests.
fn mint_ca_at(now: OffsetDateTime) -> Result<Ca, CertError> {
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose};

    let key = KeyPair::generate()?;
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params
        .distinguished_name
        .push(DnType::CommonName, CA_COMMON_NAME);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params.not_before = now;
    params.not_after = now + Duration::days(CA_VALIDITY_DAYS);

    let cert = params.self_signed(&key)?;
    Ok(Ca {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
    })
}

/// Mint a serving leaf for `dns_names`, signed by `ca`, valid for
/// [`LEAF_VALIDITY_DAYS`]. `dns_names` becomes the cert's DNS SANs (the API
/// server matches the webhook `Service` DNS name against these).
pub fn mint_leaf(ca: &Ca, dns_names: &[String]) -> Result<Leaf, CertError> {
    mint_leaf_at(ca, dns_names, OffsetDateTime::now_utc())
}

/// [`mint_leaf`] with an injectable "now" for deterministic tests.
fn mint_leaf_at(ca: &Ca, dns_names: &[String], now: OffsetDateTime) -> Result<Leaf, CertError> {
    use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, Issuer, KeyPair, KeyUsagePurpose};

    // Reload the persisted CA as an issuer so leaves minted across controller
    // restarts all chain to the same `caBundle`.
    let ca_key = KeyPair::from_pem(&ca.key_pem)?;
    let issuer = Issuer::from_ca_cert_pem(&ca.cert_pem, ca_key)?;

    let leaf_key = KeyPair::generate()?;
    // `new` populates `subject_alt_names` with a DnsName SAN per entry.
    let mut params = CertificateParams::new(dns_names.to_vec())?;
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.not_before = now;
    let not_after = now + Duration::days(LEAF_VALIDITY_DAYS);
    params.not_after = not_after;

    let cert = params.signed_by(&leaf_key, &issuer)?;
    Ok(Leaf {
        cert_pem: cert.pem(),
        key_pem: leaf_key.serialize_pem(),
        not_after_unix: not_after.unix_timestamp(),
    })
}

/// Whether a leaf expiring at `not_after_unix` should be renewed as of
/// `now_unix`. True once inside the [`LEAF_RENEW_BEFORE_DAYS`] window (or already
/// expired). Pure integer math so rotation needs no certificate parsing.
pub fn needs_renewal(not_after_unix: i64, now_unix: i64) -> bool {
    not_after_unix - now_unix < LEAF_RENEW_BEFORE_DAYS * 86_400
}

/// The DNS names a webhook serving cert must carry: `<svc>.<ns>.svc` and its
/// fully-qualified `.svc.cluster.local` form. The API server reaches the webhook
/// via the `Service` DNS name, so the cert must be valid for it.
pub fn service_dns_names(service: &str, namespace: &str) -> Vec<String> {
    vec![
        format!("{service}.{namespace}.svc"),
        format!("{service}.{namespace}.svc.cluster.local"),
    ]
}

// === Cluster IO =============================================================
//
// The thin Kubernetes layer over the pure cert logic above. Kept in this module
// (not the already-large `io.rs`) so all self-managed webhook-TLS code lives in
// one place.

use std::collections::BTreeMap;

use k8s_openapi::ByteString;
use k8s_openapi::api::admissionregistration::v1::{
    MutatingWebhookConfiguration, ValidatingWebhookConfiguration,
};
use k8s_openapi::api::core::v1::Secret;
use kube::api::{Patch, PatchParams};
use kube::core::ObjectMeta;
use kube::{Api, Client, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::consts::WEBHOOK_CERT_NOT_AFTER_ANNOTATION;
use crate::error::{Error, Result};
use crate::io::FIELD_MANAGER;

/// Where the self-managed webhook cert lives and which configs trust it.
/// Assembled from env at boot (see [`crate::config`]); only present in
/// `webhook.tls.mode: self`.
#[derive(Debug, Clone)]
pub struct WebhookTlsConfig {
    /// Operator namespace (where the serving `Secret` lives, and the webhook runs).
    pub namespace: String,
    /// Name of the `kubernetes.io/tls` serving Secret the webhook pod mounts.
    pub secret_name: String,
    /// Webhook `Service` name — the leaf cert's SAN.
    pub service_name: String,
    /// `ValidatingWebhookConfiguration` to inject `caBundle` into.
    pub validating_config: String,
    /// `MutatingWebhookConfiguration` to inject `caBundle` into.
    pub mutating_config: String,
}

/// The certificate material a reconcile resolved to, and whether the serving
/// Secret must be (re)written.
struct Material {
    ca: Ca,
    leaf: Leaf,
    /// True when the Secret needs an apply (fresh CA, or the leaf was rotated).
    write: bool,
}

/// Ensure the webhook has a valid serving cert and the API server trusts it:
/// mint/rotate the CA+leaf into the serving Secret as needed, then inject the CA
/// into each webhook configuration's `caBundle`. Idempotent — safe to call at
/// boot and on the periodic reconcile.
pub async fn ensure(client: &Client, cfg: &WebhookTlsConfig) -> Result<()> {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), &cfg.namespace);
    let existing = secrets.get_opt(&cfg.secret_name).await.map_err(|e| {
        Error::WebhookSetup(format!(
            "reading webhook TLS Secret {}/{}: {e}",
            cfg.namespace, cfg.secret_name
        ))
    })?;

    let now = chrono::Utc::now().timestamp();
    let dns = service_dns_names(&cfg.service_name, &cfg.namespace);
    let material = resolve_material(existing.as_ref(), &dns, now)
        .map_err(|e| Error::WebhookSetup(e.to_string()))?;

    if material.write {
        let secret = build_tls_secret(
            &cfg.secret_name,
            &cfg.namespace,
            &material.ca,
            &material.leaf,
        );
        crate::io::apply(&secrets, &cfg.secret_name, &secret)
            .await
            .map_err(|e| {
                Error::WebhookSetup(format!(
                    "writing webhook TLS Secret {}/{}: {e}",
                    cfg.namespace, cfg.secret_name
                ))
            })?;
        tracing::info!(
            secret = %cfg.secret_name,
            namespace = %cfg.namespace,
            not_after = material.leaf.not_after_unix,
            "minted/rotated self-managed webhook serving certificate"
        );
    }

    // Inject the CA into both webhook configs. Idempotent: re-applying the same
    // bundle is a server-side no-op.
    inject_validating(client, &cfg.validating_config, &material.ca.cert_pem).await?;
    inject_mutating(client, &cfg.mutating_config, &material.ca.cert_pem).await?;
    Ok(())
}

/// Decide the CA + leaf to use, reusing a persisted CA across restarts and only
/// rotating the leaf when it is near expiry. Pure (no IO) so it is unit-tested.
fn resolve_material(
    existing: Option<&Secret>,
    dns: &[String],
    now: i64,
) -> Result<Material, CertError> {
    if let Some(secret) = existing
        && let Some(ca) = read_ca(secret)
    {
        // A persisted CA exists — keep it (so the caBundle never changes) and
        // reuse the leaf unless it is missing or due for renewal.
        let leaf = read_leaf(secret);
        if let Some(leaf) = leaf
            && !needs_renewal(leaf.not_after_unix, now)
        {
            return Ok(Material {
                ca,
                leaf,
                write: false,
            });
        }
        let leaf = mint_leaf(&ca, dns)?;
        return Ok(Material {
            ca,
            leaf,
            write: true,
        });
    }
    // No usable CA: mint a fresh CA + leaf.
    let ca = mint_ca()?;
    let leaf = mint_leaf(&ca, dns)?;
    Ok(Material {
        ca,
        leaf,
        write: true,
    })
}

/// Build the `kubernetes.io/tls` serving Secret. Carries the leaf (`tls.crt`/
/// `tls.key`) the webhook serves plus the CA (`ca.crt`/`ca.key`) so the
/// controller can re-sign rotated leaves after a restart. The leaf `notAfter` is
/// stamped as an annotation so rotation needs no cert parsing.
fn build_tls_secret(name: &str, namespace: &str, ca: &Ca, leaf: &Leaf) -> Secret {
    let string_data = BTreeMap::from([
        ("tls.crt".to_string(), leaf.cert_pem.clone()),
        ("tls.key".to_string(), leaf.key_pem.clone()),
        ("ca.crt".to_string(), ca.cert_pem.clone()),
        ("ca.key".to_string(), ca.key_pem.clone()),
    ]);
    let annotations = BTreeMap::from([(
        WEBHOOK_CERT_NOT_AFTER_ANNOTATION.to_string(),
        leaf.not_after_unix.to_string(),
    )]);
    let labels = BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            "kopiur".to_string(),
        ),
        (
            "app.kubernetes.io/component".to_string(),
            "webhook".to_string(),
        ),
    ]);
    Secret {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            annotations: Some(annotations),
            labels: Some(labels),
            ..Default::default()
        },
        type_: Some("kubernetes.io/tls".to_string()),
        string_data: Some(string_data),
        ..Default::default()
    }
}

/// Read a value from a Secret's (already-decoded) `data` as UTF-8.
fn secret_str(secret: &Secret, key: &str) -> Option<String> {
    secret
        .data
        .as_ref()?
        .get(key)
        .and_then(|b| String::from_utf8(b.0.clone()).ok())
}

/// Recover the persisted CA (cert + key) from a serving Secret, if both present.
fn read_ca(secret: &Secret) -> Option<Ca> {
    Some(Ca {
        cert_pem: secret_str(secret, "ca.crt")?,
        key_pem: secret_str(secret, "ca.key")?,
    })
}

/// Recover the serving leaf (cert + key + recorded `notAfter`) from a Secret.
fn read_leaf(secret: &Secret) -> Option<Leaf> {
    let not_after_unix = secret
        .metadata
        .annotations
        .as_ref()?
        .get(WEBHOOK_CERT_NOT_AFTER_ANNOTATION)?
        .parse::<i64>()
        .ok()?;
    Some(Leaf {
        cert_pem: secret_str(secret, "tls.crt")?,
        key_pem: secret_str(secret, "tls.key")?,
        not_after_unix,
    })
}

/// Set `caBundle` on every webhook in a `ValidatingWebhookConfiguration`.
async fn inject_validating(client: &Client, name: &str, ca_pem: &str) -> Result<()> {
    let api: Api<ValidatingWebhookConfiguration> = Api::all(client.clone());
    let mut cfg = get_config(&api, name).await?;
    let bundle = ByteString(ca_pem.as_bytes().to_vec());
    if let Some(webhooks) = cfg.webhooks.as_mut() {
        for w in webhooks {
            w.client_config.ca_bundle = Some(bundle.clone());
        }
    }
    merge_webhooks(&api, name, &cfg.webhooks).await
}

/// Set `caBundle` on every webhook in a `MutatingWebhookConfiguration`.
async fn inject_mutating(client: &Client, name: &str, ca_pem: &str) -> Result<()> {
    let api: Api<MutatingWebhookConfiguration> = Api::all(client.clone());
    let mut cfg = get_config(&api, name).await?;
    let bundle = ByteString(ca_pem.as_bytes().to_vec());
    if let Some(webhooks) = cfg.webhooks.as_mut() {
        for w in webhooks {
            w.client_config.ca_bundle = Some(bundle.clone());
        }
    }
    merge_webhooks(&api, name, &cfg.webhooks).await
}

/// GET a webhook configuration, mapping a not-found/transport failure to an
/// actionable [`Error::WebhookSetup`].
async fn get_config<K>(api: &Api<K>, name: &str) -> Result<K>
where
    K: Resource + DeserializeOwned + Clone + std::fmt::Debug,
{
    api.get(name).await.map_err(|e| {
        Error::WebhookSetup(format!(
            "reading webhook configuration {name} (does it exist yet?): {e}"
        ))
    })
}

/// Merge-patch only the `webhooks` array back, leaving every other field the
/// chart owns untouched. A JSON merge patch replaces the array wholesale, which
/// is why we GET-modify-write the full (CA-injected) array.
async fn merge_webhooks<K, W>(api: &Api<K>, name: &str, webhooks: &W) -> Result<()>
where
    K: Resource + DeserializeOwned + Clone + std::fmt::Debug,
    W: Serialize,
{
    let patch = serde_json::json!({ "webhooks": webhooks });
    let pp = PatchParams {
        field_manager: Some(FIELD_MANAGER.to_string()),
        ..Default::default()
    };
    api.patch(name, &pp, &Patch::Merge(&patch))
        .await
        .map_err(|e| Error::WebhookSetup(format!("injecting caBundle into {name}: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use k8s_openapi::ByteString;
    use k8s_openapi::api::core::v1::Secret;
    use kube::core::ObjectMeta;
    use rcgen::KeyPair;
    use x509_parser::prelude::*;

    /// Build a Secret shaped like one read back from the API (decoded `data` +
    /// the notAfter annotation), for the `resolve_material` tests.
    fn read_secret_from(ca: &Ca, leaf: &Leaf) -> Secret {
        let data = BTreeMap::from([
            (
                "tls.crt".to_string(),
                ByteString(leaf.cert_pem.clone().into_bytes()),
            ),
            (
                "tls.key".to_string(),
                ByteString(leaf.key_pem.clone().into_bytes()),
            ),
            (
                "ca.crt".to_string(),
                ByteString(ca.cert_pem.clone().into_bytes()),
            ),
            (
                "ca.key".to_string(),
                ByteString(ca.key_pem.clone().into_bytes()),
            ),
        ]);
        Secret {
            metadata: ObjectMeta {
                annotations: Some(BTreeMap::from([(
                    WEBHOOK_CERT_NOT_AFTER_ANNOTATION.to_string(),
                    leaf.not_after_unix.to_string(),
                )])),
                ..Default::default()
            },
            data: Some(data),
            ..Default::default()
        }
    }

    /// Collect the DNS-name SANs from a PEM-encoded certificate.
    fn dns_sans(cert_pem: &str) -> Vec<String> {
        let pem = parse_x509_pem(cert_pem.as_bytes()).unwrap().1;
        let cert = pem.parse_x509().unwrap();
        let san = cert
            .subject_alternative_name()
            .unwrap()
            .expect("leaf must carry a SAN extension");
        san.value
            .general_names
            .iter()
            .filter_map(|n| match n {
                GeneralName::DNSName(d) => Some((*d).to_string()),
                _ => None,
            })
            .collect()
    }

    /// The issuer CommonName of a PEM-encoded certificate.
    fn issuer_cn(cert_pem: &str) -> String {
        let pem = parse_x509_pem(cert_pem.as_bytes()).unwrap().1;
        let cert = pem.parse_x509().unwrap();
        cert.issuer()
            .iter_common_name()
            .next()
            .and_then(|cn| cn.as_str().ok())
            .unwrap()
            .to_string()
    }

    #[test]
    fn ca_is_a_ca_with_cert_sign() {
        let ca = mint_ca().unwrap();
        assert!(ca.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(ca.key_pem.contains("BEGIN PRIVATE KEY"));
        // Re-parse the CA as an issuer: proves it is a usable signing CA.
        let key = KeyPair::from_pem(&ca.key_pem).unwrap();
        rcgen::Issuer::from_ca_cert_pem(&ca.cert_pem, key)
            .expect("a minted CA must be loadable as an issuer");
        // The CA must assert the cA basic constraint.
        let pem = parse_x509_pem(ca.cert_pem.as_bytes()).unwrap().1;
        let cert = pem.parse_x509().unwrap();
        let bc = cert
            .basic_constraints()
            .unwrap()
            .expect("CA needs basicConstraints");
        assert!(bc.value.ca, "minted CA cert must have cA=true");
    }

    #[test]
    fn leaf_carries_service_sans_and_chains_to_ca() {
        let ca = mint_ca().unwrap();
        let dns = service_dns_names("kopiur-webhook", "kopiur-system");
        let leaf = mint_leaf(&ca, &dns).unwrap();
        assert!(leaf.cert_pem.contains("BEGIN CERTIFICATE"));

        let sans = dns_sans(&leaf.cert_pem);
        assert!(
            sans.iter().any(|s| s == "kopiur-webhook.kopiur-system.svc"),
            "leaf SANs missing service DNS name: {sans:?}"
        );
        assert!(
            sans.iter()
                .any(|s| s == "kopiur-webhook.kopiur-system.svc.cluster.local"),
            "leaf SANs missing cluster-local DNS name: {sans:?}"
        );
        // The leaf must be issued by our CA (chains to the caBundle).
        assert_eq!(issuer_cn(&leaf.cert_pem), CA_COMMON_NAME);
    }

    #[test]
    fn leaf_not_after_is_about_a_year_out() {
        let now = OffsetDateTime::now_utc();
        let ca = mint_ca_at(now).unwrap();
        let leaf = mint_leaf_at(&ca, &["svc.ns.svc".to_string()], now).unwrap();
        let expected = (now + Duration::days(LEAF_VALIDITY_DAYS)).unix_timestamp();
        assert_eq!(leaf.not_after_unix, expected);
    }

    #[test]
    fn resolve_material_mints_when_secret_absent() {
        let dns = service_dns_names("kopiur-webhook", "ns");
        let m = resolve_material(None, &dns, 1_000_000_000).unwrap();
        assert!(m.write, "a fresh install must write the Secret");
        assert_eq!(issuer_cn(&m.leaf.cert_pem), CA_COMMON_NAME);
    }

    #[test]
    fn resolve_material_reuses_a_fresh_cert() {
        let ca = mint_ca().unwrap();
        let dns = service_dns_names("kopiur-webhook", "ns");
        let leaf = mint_leaf(&ca, &dns).unwrap();
        let secret = read_secret_from(&ca, &leaf);
        // `now` well before expiry → no renewal, no write.
        let m = resolve_material(Some(&secret), &dns, leaf.not_after_unix - 365 * 86_400).unwrap();
        assert!(!m.write, "a fresh cert must not be rewritten");
        assert_eq!(m.ca.cert_pem, ca.cert_pem);
        assert_eq!(m.leaf.cert_pem, leaf.cert_pem);
    }

    #[test]
    fn resolve_material_rotates_leaf_but_keeps_ca() {
        let ca = mint_ca().unwrap();
        let dns = service_dns_names("kopiur-webhook", "ns");
        let leaf = mint_leaf(&ca, &dns).unwrap();
        let secret = read_secret_from(&ca, &leaf);
        // `now` inside the renewal window → rotate the leaf, keep the CA so the
        // caBundle (and thus API-server trust) is undisturbed.
        let m = resolve_material(Some(&secret), &dns, leaf.not_after_unix - 10).unwrap();
        assert!(m.write, "an expiring leaf must be rotated");
        assert_eq!(
            m.ca.cert_pem, ca.cert_pem,
            "CA must be preserved across rotation"
        );
        assert_ne!(m.leaf.cert_pem, leaf.cert_pem, "a new leaf must be minted");
        assert_eq!(issuer_cn(&m.leaf.cert_pem), CA_COMMON_NAME);
    }

    #[test]
    fn build_tls_secret_is_a_well_formed_tls_secret() {
        let ca = mint_ca().unwrap();
        let leaf = mint_leaf(&ca, &service_dns_names("kopiur-webhook", "ns")).unwrap();
        let s = build_tls_secret("kopiur-webhook-tls", "kopiur-system", &ca, &leaf);
        assert_eq!(s.type_.as_deref(), Some("kubernetes.io/tls"));
        assert_eq!(s.metadata.namespace.as_deref(), Some("kopiur-system"));
        let sd = s.string_data.expect("string_data");
        for k in ["tls.crt", "tls.key", "ca.crt", "ca.key"] {
            assert!(sd.contains_key(k), "TLS secret missing key {k}");
        }
        let ann = s.metadata.annotations.expect("annotations");
        assert_eq!(
            ann.get(WEBHOOK_CERT_NOT_AFTER_ANNOTATION)
                .map(String::as_str),
            Some(leaf.not_after_unix.to_string().as_str())
        );
    }

    #[test]
    fn renewal_predicate_windows() {
        let now = 1_000_000_000;
        let day = 86_400;
        // Fresh leaf (~1 year out): no renewal.
        assert!(!needs_renewal(now + 365 * day, now));
        // Just outside the window: still no renewal.
        assert!(!needs_renewal(
            now + (LEAF_RENEW_BEFORE_DAYS + 1) * day,
            now
        ));
        // Inside the window: renew.
        assert!(needs_renewal(now + (LEAF_RENEW_BEFORE_DAYS - 1) * day, now));
        // Already expired: renew.
        assert!(needs_renewal(now - day, now));
    }
}
