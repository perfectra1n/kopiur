#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

pub mod client;
pub mod env;
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
