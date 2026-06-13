//! The single place that names every environment variable the mover reads
//! (plus the well-known in-pod paths shared with the Job builder).
//!
//! Exposed from the library (not just `main.rs`) because the controller stamps
//! these onto the mover `Job` it creates — both sides reference the same
//! constant so the controller↔mover env contract can't drift.
//!
//! ```
//! use kopiur_mover::env;
//!
//! assert_eq!(env::WORK_SPEC_PATH, "KOPIUR_WORK_SPEC_PATH");
//! assert_eq!(env::KOPIA_BINARY, "KOPIUR_KOPIA_BINARY");
//! assert_eq!(env::RESULT_CONFIGMAP, "KOPIUR_RESULT_CONFIGMAP");
//! // The browse-session readiness marker lives under the writable kopia-cache
//! // mount (the only writable volume on a read-only-root mover pod).
//! assert!(env::READY_MARKER.starts_with(kopiur_kopia::env::DEFAULT_CACHE_DIR));
//! ```

/// Path to the mounted work-spec JSON (the controller↔mover contract). The
/// controller sets this on the Job; the mover reads it (falling back to `argv[1]`).
pub const WORK_SPEC_PATH: &str = "KOPIUR_WORK_SPEC_PATH";

/// Optional override for the `kopia` binary path (defaults to `kopia` on PATH).
pub const KOPIA_BINARY: &str = "KOPIUR_KOPIA_BINARY";

/// Path to the mounted server work-spec JSON consumed by the `serve` entrypoint
/// (the controller↔mover contract for `spec.server`). The controller sets this on
/// the server `Deployment`; the mover reads it (falling back to `argv[2]`).
pub const SERVER_SPEC_PATH: &str = "KOPIUR_SERVER_SPEC_PATH";

/// UI password env for the `serve` entrypoint's `ServerAuthMode::Password` mode.
/// The controller injects it via a `secretKeyRef` (never argv/ConfigMap); the mover
/// appends it to `kopia server start` at exec time inside the server pod.
pub const SERVER_PASSWORD: &str = "KOPIA_SERVER_PASSWORD";

/// Name of the `ConfigMap` (in the work spec's `targetRef.namespace`) the mover
/// writes its bootstrap result into. Set by the controller only for
/// `BootstrapRepository` runs; absent for backup/restore/delete. The controller
/// reads the result back to drive the `Repository` status + catalog.
pub const RESULT_CONFIGMAP: &str = "KOPIUR_RESULT_CONFIGMAP";

/// Marker file a `BrowseSession` mover writes once its read-only repository
/// connect succeeded. The session pod's readinessProbe execs `kopiur-mover
/// ready`, which exits 0 iff this file exists — the distroless image has no
/// shell, so the probe re-invokes the mover binary itself. The path sits under
/// the writable kopia-cache volume mount
/// ([`kopiur_kopia::env::DEFAULT_CACHE_DIR`], `/var/cache/kopia`) because that
/// emptyDir is the only writable mount on a read-only-root mover pod.
pub const READY_MARKER: &str = "/var/cache/kopia/.kopiur-session-ready";
