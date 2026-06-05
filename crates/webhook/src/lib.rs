#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

pub mod config;
pub mod handlers;
pub mod metrics;
pub mod routes;
pub mod tenancy;

pub use routes::{AppState, app};
