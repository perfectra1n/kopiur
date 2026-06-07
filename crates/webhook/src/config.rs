//! The single place that names every environment variable the webhook reads,
//! plus its fixed defaults.

/// Address the webhook server binds to.
pub const WEBHOOK_ADDR_ENV: &str = "KOPIUR_WEBHOOK_ADDR";
/// PEM cert chain path; presence (with the key) enables TLS.
pub const WEBHOOK_TLS_CERT_ENV: &str = "KOPIUR_WEBHOOK_TLS_CERT";
/// PEM private key path.
pub const WEBHOOK_TLS_KEY_ENV: &str = "KOPIUR_WEBHOOK_TLS_KEY";

/// Default bind address when [`WEBHOOK_ADDR_ENV`] is unset (k8s requires HTTPS
/// for admission; the chart maps Service 443 → this container port).
pub const DEFAULT_ADDR: &str = "0.0.0.0:8443";

/// How often the TLS server re-reads its cert/key files so an operator-rotated
/// serving leaf (the `webhook.tls.mode: self` path — the controller rewrites the
/// mounted Secret) is picked up without a pod restart. Rotation is rare and the
/// reload is a cheap PEM read, so a calm cadence is plenty.
pub const TLS_RELOAD_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);
