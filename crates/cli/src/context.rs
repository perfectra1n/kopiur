//! Kubernetes client construction honoring the same configuration sources
//! kubectl uses: `--kubeconfig`/`--context` flags, `$KUBECONFIG`, the default
//! kubeconfig file, or in-cluster service-account credentials.

use kube::Client;
use kube::config::{KubeConfigOptions, Kubeconfig};

use crate::cli::GlobalArgs;
use crate::error::CliError;

/// Where a list-style command looks. Exhaustive — every list call site
/// `match`es this, so the `-A`/`-n` decision can never be half-applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// One namespace (from `-n` or the kubeconfig context default).
    Namespace(String),
    /// Every namespace (`-A`).
    All,
}

impl Scope {
    /// The namespace to use for namespaced *single-object* operations, and the
    /// `kubectl` flag suffix matching this scope (for error remediation text).
    pub fn flag_suffix(&self) -> String {
        match self {
            Scope::Namespace(ns) => format!(" -n {ns}"),
            Scope::All => " -A".to_string(),
        }
    }
}

/// A connected client plus the resolved namespace scope.
#[derive(Clone)]
pub struct KubeCtx {
    /// The kube client, ready for `Api` construction.
    pub client: Client,
    /// The single namespace single-object commands operate in.
    pub namespace: String,
    /// The list scope (`-A` vs the resolved namespace).
    pub scope: Scope,
}

/// Build the client from the global flags. Resolution order matches kubectl:
/// an explicit `--kubeconfig`/`--context` wins, then `Config::infer()` covers
/// `$KUBECONFIG` → `~/.kube/config` → in-cluster.
pub async fn connect(global: &GlobalArgs) -> Result<KubeCtx, CliError> {
    // The webhook/mover do the same: ring may already be installed; ignore the error.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let config = match (&global.kubeconfig, &global.context) {
        (None, None) => kube::Config::infer()
            .await
            .map_err(|e| CliError::KubeConfig { source: e.into() })?,
        (path, context) => {
            let kubeconfig = match path {
                Some(p) => Kubeconfig::read_from(p)
                    .map_err(|e| CliError::KubeConfig { source: e.into() })?,
                None => {
                    Kubeconfig::read().map_err(|e| CliError::KubeConfig { source: e.into() })?
                }
            };
            let options = KubeConfigOptions {
                context: context.clone(),
                cluster: None,
                user: None,
            };
            kube::Config::from_custom_kubeconfig(kubeconfig, &options)
                .await
                .map_err(|e| CliError::KubeConfig { source: e.into() })?
        }
    };

    let default_namespace = config.default_namespace.clone();
    let client = Client::try_from(config).map_err(|e| CliError::KubeConfig { source: e.into() })?;

    let namespace = global.namespace.clone().unwrap_or(default_namespace);
    let scope = if global.all_namespaces {
        Scope::All
    } else {
        Scope::Namespace(namespace.clone())
    };

    Ok(KubeCtx {
        client,
        namespace,
        scope,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_flag_suffix_matches_kubectl_flags() {
        assert_eq!(Scope::Namespace("media".into()).flag_suffix(), " -n media");
        assert_eq!(Scope::All.flag_suffix(), " -A");
    }
}
