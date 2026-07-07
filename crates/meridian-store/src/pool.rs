//! Connection pool construction and health checking.

use std::time::Duration;

use meridian_common::config::DatabaseConfig;
use meridian_common::{MeridianError, Result};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// Builds a [`PgPool`] from configuration.
///
/// Fails fast: attempts one connection eagerly so a bad URL or unreachable
/// database is reported at startup, not on first request.
pub async fn connect(config: &DatabaseConfig) -> Result<PgPool> {
    if config.url.is_empty() {
        return Err(MeridianError::Validation(
            "database.url is not configured; set DATABASE_URL or MERIDIAN__DATABASE__URL"
                .to_owned(),
        ));
    }

    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
        .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
        .connect(&config.url)
        .await
        .map_err(|e| match e {
            sqlx::Error::Configuration(_) => {
                MeridianError::Validation(format!("invalid database configuration: {e}"))
            }
            other => MeridianError::internal("failed to connect to Postgres", other),
        })
}

/// Cheap liveness probe: `SELECT 1` over a pooled connection.
pub async fn health_check(pool: &PgPool) -> Result<()> {
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(pool)
        .await
        .map_err(|e| MeridianError::Unavailable(format!("database health check failed: {e}")))?;
    Ok(())
}
