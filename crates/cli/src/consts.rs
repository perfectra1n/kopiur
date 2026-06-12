//! CLI-wide constants — every env var name and shared config value lives here
//! so call sites use the constant, never a string literal.

/// The field manager recorded on every PATCH the plugin issues, so
/// `managedFields` attributes changes to `kubectl kopiur` rather than an
/// anonymous client.
pub const FIELD_MANAGER: &str = "kubectl-kopiur";

/// Env var the user can set to control diagnostic verbosity with a full
/// `tracing_subscriber::EnvFilter` expression. `-v`/`-vv` flags take precedence.
pub const LOG_ENV: &str = "KOPIUR_LOG";

/// The plugin version: the release tag stamped at build time by CI
/// (`KOPIUR_VERSION`), falling back to the workspace crate version for local
/// builds. Mirrors how the operator images stamp `VERSION`.
pub const VERSION: &str = match option_env!("KOPIUR_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};
