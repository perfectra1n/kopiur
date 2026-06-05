//! The single place that names every environment variable the mover reads.
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
//! ```

/// Path to the mounted work-spec JSON (the controller↔mover contract). The
/// controller sets this on the Job; the mover reads it (falling back to `argv[1]`).
pub const WORK_SPEC_PATH: &str = "KOPIUR_WORK_SPEC_PATH";

/// Optional override for the `kopia` binary path (defaults to `kopia` on PATH).
pub const KOPIA_BINARY: &str = "KOPIUR_KOPIA_BINARY";

/// Name of the `ConfigMap` (in the work spec's `targetRef.namespace`) the mover
/// writes its bootstrap result into. Set by the controller only for
/// `BootstrapRepository` runs; absent for backup/restore/delete. The controller
/// reads the result back to drive the `Repository` status + catalog.
pub const RESULT_CONFIGMAP: &str = "KOPIUR_RESULT_CONFIGMAP";
