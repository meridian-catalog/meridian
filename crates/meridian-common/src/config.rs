//! Layered application configuration.
//!
//! Precedence (lowest to highest):
//!
//! 1. Built-in defaults
//! 2. Optional TOML file (`meridian.toml` by default, or `--config <path>`)
//! 3. `DATABASE_URL` (conventional shortcut for the database URL)
//! 4. `MERIDIAN__*` environment variables (e.g. `MERIDIAN__SERVER__PORT=8181`,
//!    nesting separated by `__`)

use std::path::Path;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};

use crate::error::MeridianError;

/// Default file name looked up in the working directory when no explicit
/// config path is given.
pub const DEFAULT_CONFIG_FILE: &str = "meridian.toml";

/// Environment variable prefix for configuration overrides.
pub const ENV_PREFIX: &str = "MERIDIAN__";

/// Top-level application configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AppConfig {
    /// HTTP server settings.
    pub server: ServerConfig,
    /// Database settings.
    pub database: DatabaseConfig,
    /// Logging/tracing settings.
    pub telemetry: TelemetryConfig,
}

/// HTTP server settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Address to bind, e.g. `0.0.0.0`.
    pub host: String,
    /// TCP port to listen on.
    pub port: u16,
    /// Per-request timeout in seconds.
    pub request_timeout_secs: u64,
    /// Maximum accepted request body size in bytes.
    pub max_body_bytes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_owned(),
            port: 8181,
            request_timeout_secs: 30,
            max_body_bytes: 16 * 1024 * 1024,
        }
    }
}

/// Database settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DatabaseConfig {
    /// Postgres connection URL, e.g.
    /// `postgres://meridian:meridian@localhost:5433/meridian`.
    ///
    /// Also settable via the conventional `DATABASE_URL` environment
    /// variable.
    pub url: String,
    /// Maximum number of pooled connections.
    pub max_connections: u32,
    /// Timeout in seconds when acquiring a connection from the pool.
    pub acquire_timeout_secs: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_connections: 10,
            acquire_timeout_secs: 5,
        }
    }
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    /// Human-readable multi-line output for local development.
    Pretty,
    /// Structured JSON lines for production log pipelines.
    Json,
}

/// Logging/tracing settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TelemetryConfig {
    /// Output format.
    pub format: LogFormat,
    /// Default tracing filter directive, overridable at runtime with
    /// `RUST_LOG`.
    pub filter: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            format: LogFormat::Pretty,
            filter: "info,meridian=debug".to_owned(),
        }
    }
}

impl AppConfig {
    /// Loads configuration with full layering.
    ///
    /// `config_file`: explicit TOML file path. When `None`, `meridian.toml`
    /// in the working directory is used if it exists. An explicitly given
    /// path that does not exist is an error; the implicit default file is
    /// optional.
    pub fn load(config_file: Option<&Path>) -> Result<Self, MeridianError> {
        let mut figment = Figment::from(Serialized::defaults(Self::default()));

        match config_file {
            Some(path) => {
                if !path.exists() {
                    return Err(MeridianError::Validation(format!(
                        "config file not found: {}",
                        path.display()
                    )));
                }
                figment = figment.merge(Toml::file(path));
            }
            None => {
                // Optional by design: absent file simply contributes nothing.
                figment = figment.merge(Toml::file(DEFAULT_CONFIG_FILE));
            }
        }

        figment = figment
            .merge(
                Env::raw()
                    .only(&["DATABASE_URL"])
                    .map(|_| "database.url".into()),
            )
            .merge(Env::prefixed(ENV_PREFIX).split("__"));

        figment
            .extract()
            .map_err(|e| MeridianError::Validation(format!("invalid configuration: {e}")))
    }

    /// The socket address string the server should bind.
    #[must_use]
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }
}

#[cfg(test)]
// figment::Jail's closure signature returns figment's (large) error type.
#[allow(clippy::result_large_err)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sensible() {
        let cfg = AppConfig::default();
        assert_eq!(cfg.server.port, 8181);
        assert_eq!(cfg.bind_addr(), "0.0.0.0:8181");
        assert_eq!(cfg.telemetry.format, LogFormat::Pretty);
        assert!(cfg.database.url.is_empty());
    }

    #[test]
    fn env_overrides_apply() {
        // figment::Jail isolates environment mutation to this test.
        figment::Jail::expect_with(|jail| {
            jail.set_env("MERIDIAN__SERVER__PORT", "9999");
            jail.set_env("DATABASE_URL", "postgres://u:p@localhost:5433/db");
            let cfg = AppConfig::load(None).expect("load config");
            assert_eq!(cfg.server.port, 9999);
            assert_eq!(cfg.database.url, "postgres://u:p@localhost:5433/db");
            Ok(())
        });
    }

    #[test]
    fn meridian_env_beats_database_url() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("DATABASE_URL", "postgres://generic/db");
            jail.set_env("MERIDIAN__DATABASE__URL", "postgres://specific/db");
            let cfg = AppConfig::load(None).expect("load config");
            assert_eq!(cfg.database.url, "postgres://specific/db");
            Ok(())
        });
    }

    #[test]
    fn toml_file_layering() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "meridian.toml",
                r#"
                    [server]
                    port = 8282

                    [telemetry]
                    format = "json"
                "#,
            )?;
            let cfg = AppConfig::load(None).expect("load config");
            assert_eq!(cfg.server.port, 8282);
            assert_eq!(cfg.telemetry.format, LogFormat::Json);
            // Untouched values keep defaults.
            assert_eq!(cfg.server.host, "0.0.0.0");
            Ok(())
        });
    }

    #[test]
    fn missing_explicit_config_file_is_an_error() {
        let err = AppConfig::load(Some(Path::new("/nonexistent/meridian.toml"))).unwrap_err();
        assert!(matches!(err, MeridianError::Validation(_)));
    }
}
