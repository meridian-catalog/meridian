//! Shared foundations for Meridian: typed identifiers, the error model,
//! configuration loading, and telemetry initialization.
//!
//! This crate must stay dependency-light and free of business logic — it is
//! depended on by every other crate in the workspace.

pub mod config;
pub mod error;
pub mod id;
pub mod principal;
pub mod telemetry;

pub use config::AppConfig;
pub use error::{MeridianError, Result};
