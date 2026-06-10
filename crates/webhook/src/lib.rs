#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

pub mod config;
pub mod error;
pub mod handlers;
pub mod identity_collision;
pub mod metrics;
pub mod routes;
pub mod tenancy;

pub use routes::{AppState, app};
