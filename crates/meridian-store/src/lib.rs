//! Postgres access layer for Meridian.
//!
//! Owns the connection pool, embedded migrations, the transactional outbox,
//! and the hash-chained audit log. All SQL uses runtime-checked queries for
//! now (no `sqlx::query!` macros) so the workspace compiles without a live
//! database; compile-time checking may be revisited once the schema settles.

pub mod audit;
pub mod commit;
pub mod consumer;
pub mod contracts;
pub mod federation;
pub mod foreign;
pub mod health;
pub mod incidents;
pub mod maintenance;
pub mod monitors;
pub mod namespace;
pub mod outbox;
pub mod planning;
pub mod policy;
pub mod pool;
pub mod principal;
pub mod quality_score;
pub mod rbac;
pub mod search;
pub mod table;
pub mod tags;
pub mod tenancy;
pub mod view;
pub mod warehouse;
pub mod webhook;

pub use pool::{connect, health_check};

/// Embedded migrations from `crates/meridian-store/migrations`.
///
/// Applied via [`MIGRATOR`]`.run(&pool)` — the CLI does this on startup, and
/// tests do it against their target database. sqlx serializes concurrent
/// migration runs with a Postgres advisory lock, so parallel test binaries
/// are safe.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// Maps a sqlx error onto the Meridian error model.
///
/// Connectivity-shaped failures become `Unavailable`; everything else is
/// `Internal`. Constraint-violation mapping to `Conflict` is intentionally
/// not done here: callers that expect conflicts must inspect the database
/// error themselves so that unexpected constraint violations still surface
/// as internal errors.
#[must_use]
pub fn map_sqlx_error(context: &str, error: sqlx::Error) -> meridian_common::MeridianError {
    match &error {
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Io(_) => {
            meridian_common::MeridianError::Unavailable(format!("{context}: database unavailable"))
        }
        _ => meridian_common::MeridianError::internal(context.to_owned(), error),
    }
}
