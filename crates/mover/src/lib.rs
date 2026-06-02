//! # kopiur-mover (library)
//!
//! The mover is primarily a binary (see `main.rs`), but its **pure data**
//! modules are exposed as a library so the controller can construct a
//! [`workspec::MoverWorkSpec`] (the controller↔mover JSON contract, ADR §4.10)
//! and unit-test that construction without a cluster.
//!
//! Only the cluster-free layers are public here: [`workspec`] (the work-spec
//! contract) and [`status`] (the pure kopia-result → status mapping). The kube
//! PATCH path lives in `main.rs` and is not part of the library surface.

pub mod status;
pub mod workspec;
