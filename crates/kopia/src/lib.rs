#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

pub mod client;
pub mod env;
pub mod error;
pub mod model;
pub mod session;

pub use client::{
    CacheTuning, ConnectSpec, CreateOptions, KopiaClient, KopiaClientBuilder, MaintenanceMode,
    PolicyArgs, RestoreOptions, ThrottleArgs, VerifyOptions, split_policy_scopes,
};
pub use error::{KopiaError, KopiaErrorClass};
pub use model::{
    ClientOptions, ContentFormat, DirEntry, DirManifest, DirSummary, DirSummaryLite,
    MaintenanceCadence, MaintenanceInfo, MaintenanceSchedule, RepositoryStatus, RootEntry,
    SnapshotCreateResult, SnapshotListEntry, SnapshotSource, SnapshotStats, StorageInfo,
};
pub use session::SessionCmd;
