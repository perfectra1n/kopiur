//! Shared cluster-IO helpers for the reconcilers (the "thin IO calling tested
//! pure fns" layer, ADR §5.2/§5.4).
//!
//! These wrap the repetitive `kube::Api` mechanics — server-side apply with a
//! stable field manager, finalizer add/remove, status subresource patches, and
//! resolving the credentials Secret for a repository — so each reconciler stays
//! focused on its decision logic. The decision logic itself lives in the
//! per-reconciler pure functions (which remain unit-tested without a cluster).

mod apply;
mod colocation;
mod creds;
mod events;
mod finalizer;
mod maintenance;
mod mover;
mod repo;
mod server;
mod staging;

pub use apply::*;
pub use colocation::*;
pub use creds::*;
pub use events::*;
pub use finalizer::*;
pub use maintenance::*;
pub use mover::*;
pub use repo::*;
pub use server::*;
pub use staging::*;

#[cfg(test)]
mod tests;
