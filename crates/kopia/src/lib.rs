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
//! - [`client`] — [`KopiaClient`] and its builder, covering the operator's full
//!   declarative backup/restore/maintain surface: connect/create against every
//!   kopia 0.23 backend (filesystem, s3, azure, gcs, b2, sftp, webdav, rclone,
//!   gdrive, from-config, server), snapshot create/list/delete/restore (with
//!   [`RestoreOptions`]), verify/estimate/pin/expire, policy set/show
//!   ([`PolicyArgs`]), repository status + validate-provider, and maintenance.
//!
//! ## Deliberately out of scope
//!
//! Commands that have no place inside a declarative Kubernetes operator are not
//! wrapped: running a kopia API server (`server start`), FUSE `mount`,
//! `notification` profiles, `repository change-password`, `benchmark`, and the
//! low-level `blob`/`content`/`index`/`manifest`/`acl`/`users` plumbing.
//!
//! ## stdout vs stderr
//!
//! kopia prints progress (`Snapshotting ...`, `Restored N files`) to **stderr**
//! and the machine-readable `--json` result to **stdout**. The client parses
//! stdout and retains stderr for diagnostics on failure.

pub mod client;
pub mod error;
pub mod model;

pub use client::{
    ConnectSpec, KopiaClient, KopiaClientBuilder, MaintenanceMode, PolicyArgs, RestoreOptions,
    VerifyOptions,
};
pub use error::{KopiaError, KopiaErrorClass};
pub use model::{
    ClientOptions, ContentFormat, DirSummary, MaintenanceCadence, MaintenanceInfo,
    MaintenanceSchedule, RepositoryStatus, RootEntry, SnapshotCreateResult, SnapshotListEntry,
    SnapshotSource, SnapshotStats, StorageInfo,
};
