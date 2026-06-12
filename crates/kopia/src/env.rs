//! The environment variables kopia itself reads, named once so every caller
//! (the controller's in-process [`crate::KopiaClient`] and the mover `Job` the
//! controller stamps) points at the same writable cache/log/config location.
//!
//! kopia defaults its cache, logs, and config under `$HOME` (`~/.cache/kopia`,
//! `~/.config/kopia/repository.config`). On a distroless `nonroot` image `$HOME`
//! is `/nonexistent`, and the operator runs with a read-only root filesystem, so
//! those defaults are unwritable and every invocation errors with
//! `mkdir /nonexistent: read-only file system`. We redirect all three onto a
//! writable `emptyDir` mounted at [`DEFAULT_CACHE_DIR`].
//!
//! ```
//! use kopiur_kopia::env;
//!
//! assert_eq!(env::CACHE_DIRECTORY_ENV, "KOPIA_CACHE_DIRECTORY");
//! assert_eq!(env::LOG_DIR_ENV, "KOPIA_LOG_DIR");
//! assert_eq!(env::CONFIG_PATH_ENV, "KOPIA_CONFIG_PATH");
//! assert_eq!(env::DEFAULT_CACHE_DIR, "/var/cache/kopia");
//! ```

/// kopia's content/metadata cache directory. Content-addressed, so it is safe to
/// share across concurrent invocations.
pub const CACHE_DIRECTORY_ENV: &str = "KOPIA_CACHE_DIRECTORY";

/// Directory kopia writes its CLI/content log files into.
pub const LOG_DIR_ENV: &str = "KOPIA_LOG_DIR";

/// Path to kopia's connection config file (the persisted repository binding).
/// Unlike the cache, this is per-connection state: concurrent connects to
/// *different* repositories must not share one file, or they clobber each other.
pub const CONFIG_PATH_ENV: &str = "KOPIA_CONFIG_PATH";

/// The writable base directory both the controller `Deployment` and the mover
/// `Job` mount an `emptyDir` at, and that the binaries default their kopia
/// cache/log/config under. Keep this in sync with the `kopia-cache` volume mount
/// in `deploy/helm/kopiur/templates/deployment.yaml` and in
/// `kopiur_mover::jobs::build_job`.
pub const DEFAULT_CACHE_DIR: &str = "/var/cache/kopia";
