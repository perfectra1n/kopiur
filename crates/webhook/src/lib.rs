//! # kopiur-webhook
//!
//! An `axum`-based Kubernetes admission webhook for the kopiur CRDs (ADR-0003 §5.3).
//!
//! The webhook is the enforcement point for the cross-field rules the type system
//! can't express (ADR §2.2 principle 8): mutually-exclusive fields, malformed cron,
//! `ClusterRepository` tenancy, and origin-aware `deletionPolicy`. It calls the
//! **same** `kopiur_api::validate` validators the controller calls defensively, so
//! validation behavior is identical across the two call sites (SKILL hard-rule 4 —
//! one validator, two callers). No validation logic is forked here.
//!
//! It is both a **validating** and a **mutating** webhook served from a single
//! endpoint: it denies invalid objects and applies safe defaulting patches (origin-
//! aware `deletionPolicy`, the `kopiur.dev/snapshot-cleanup` finalizer, GitOps-safe
//! schedule defaults) in one pass. See [`routes`] for the endpoint design rationale.

pub mod handlers;
pub mod routes;
pub mod tenancy;

pub use routes::{AppState, app};
