//! Mover `Job` + `ConfigMap` construction (ADR §4.10 / §4.11) — re-exported.
//!
//! The pure builder (and its unit tests) moved to [`kopiur_mover::jobs`] so
//! non-controller callers — the `kubectl kopiur` plugin's browse-session
//! spawner, external tooling — can build byte-identical mover Jobs without a
//! controller dependency. It sits next to `kopiur_mover::workspec`, the other
//! half of the controller↔mover contract, and stays free of `kube::Client`/IO.
//! This module re-exports everything so controller call sites keep their
//! existing `crate::jobs::*` import paths unchanged.

pub use kopiur_mover::jobs::*;
