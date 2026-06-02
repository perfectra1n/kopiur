//! # kopiur-kopia
//!
//! Typed models for kopia's `--json` output plus a `tokio::process`-based
//! client (ADR §5.4). This crate is **controller-agnostic**: it has no
//! `kube`/`k8s-openapi` dependency, so both the controller (short, idempotent
//! ops) and the mover binary (long-running snapshot/restore) can use it.
//!
//! - [`model`] — structs modeled against real kopia 0.23 JSON: snapshot create
//!   result, snapshot list entry, repository status, maintenance info. They use
//!   `camelCase` and tolerate unknown fields (kopia adds fields across
//!   releases).
//! - [`error`] — [`KopiaError`] with structured variants for spawn failure,
//!   non-zero exit (with exit code + stderr tail + best-effort error class),
//!   JSON parse error, empty output, and timeout — enough to build a
//!   `status.failure` block (ADR §4.10).
//! - [`client`] — [`KopiaClient`] and its builder, with async methods for
//!   connect/create, snapshot create/list/delete/restore, repository status,
//!   and maintenance.
//!
//! ## stdout vs stderr
//!
//! kopia prints progress (`Snapshotting ...`, `Restored N files`) to **stderr**
//! and the machine-readable `--json` result to **stdout**. The client parses
//! stdout and retains stderr for diagnostics on failure.

pub mod client;
pub mod error;
pub mod model;

pub use client::{ConnectSpec, KopiaClient, KopiaClientBuilder, MaintenanceMode};
pub use error::{KopiaError, KopiaErrorClass};
pub use model::{
    ClientOptions, ContentFormat, DirSummary, MaintenanceCadence, MaintenanceInfo,
    MaintenanceSchedule, RepositoryStatus, RootEntry, SnapshotCreateResult, SnapshotListEntry,
    SnapshotSource, SnapshotStats, StorageInfo,
};
