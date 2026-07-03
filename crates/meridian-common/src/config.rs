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
    /// Authentication settings.
    pub auth: AuthConfig,
    /// Event delivery settings (outbox relay, webhooks).
    pub events: EventsConfig,
    /// Server-side scan planning settings.
    pub planning: PlanningConfig,
}

/// Server-side scan-planning settings (`[planning]`): the IRC
/// `planTableScan` / `fetchPlanningResult` / `fetchScanTasks` surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct PlanningConfig {
    /// Master switch. When `false` the planning endpoints return 406
    /// `UnsupportedOperationException` and are not advertised in
    /// `GET /v1/config`.
    pub enabled: bool,
    /// Tables whose snapshot tracks at most this many live data files
    /// (counted from the manifest list) are planned synchronously, with
    /// the full result inline in the planTableScan response. Larger
    /// tables get the asynchronous submitted/poll/fetch flow.
    pub sync_max_data_files: i64,
    /// File-scan tasks per result page on the asynchronous path.
    pub page_size_files: usize,
    /// Maximum concurrently running asynchronous plans per pod; further
    /// submissions are rejected with 503 until capacity frees up.
    pub max_concurrent_plans: usize,
    /// Plan (and result page) time-to-live in seconds. Expired plan-ids
    /// answer 404, matching the spec's "plan-id is invalid" semantics.
    pub plan_ttl_secs: u64,
    /// Interval of the background sweep that deletes expired plans and
    /// enforces the Postgres manifest-cache budget, in seconds.
    pub sweep_interval_secs: u64,
    /// In-process manifest LRU budget in bytes. The accounting unit is
    /// the *estimated parsed size* of each manifest (see
    /// `meridian-server`'s planning cache), not its raw file size.
    pub cache_max_bytes: u64,
    /// Total budget in bytes for the cross-pod manifest byte cache in
    /// Postgres (raw file bytes). `0` disables that cache tier entirely.
    pub pg_cache_max_bytes: u64,
}

impl Default for PlanningConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sync_max_data_files: 2_000,
            page_size_files: 500,
            max_concurrent_plans: 4,
            plan_ttl_secs: 3_600,
            sweep_interval_secs: 60,
            cache_max_bytes: 256 * 1024 * 1024,
            pg_cache_max_bytes: 1024 * 1024 * 1024,
        }
    }
}

/// Event delivery settings (`[events]`): the outbox relay and the webhook
/// dispatcher, both background tasks inside `meridian serve`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EventsConfig {
    /// Maximum outbox rows published per relay iteration. Bounded batches
    /// keep a large first-boot backlog from turning into one giant
    /// transaction; the relay loops without sleeping while full batches
    /// keep coming.
    pub relay_batch_size: i64,
    /// Relay poll interval in milliseconds once the backlog is drained.
    pub relay_poll_ms: u64,
    /// Webhook dispatcher poll interval in milliseconds.
    pub webhook_poll_ms: u64,
    /// Per-request timeout for webhook deliveries, in seconds.
    pub webhook_timeout_secs: u64,
    /// Delivery attempts before a webhook delivery is dead-lettered.
    pub webhook_max_attempts: i32,
    /// Base delay for webhook retry backoff, in seconds. Attempt `n`
    /// retries after `base * 2^(n-1)`, capped at 15 minutes.
    pub webhook_retry_base_secs: u64,
}

impl Default for EventsConfig {
    fn default() -> Self {
        Self {
            relay_batch_size: 500,
            relay_poll_ms: 1_000,
            webhook_poll_ms: 1_000,
            webhook_timeout_secs: 10,
            webhook_max_attempts: 10,
            webhook_retry_base_secs: 10,
        }
    }
}

/// Authentication mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    /// No authentication: every request runs as the anonymous principal.
    /// The server logs a loud warning at startup — never run this exposed
    /// to an untrusted network.
    #[default]
    Disabled,
    /// OIDC bearer tokens from configured external identity providers.
    /// Meridian validates tokens; it never issues its own.
    Oidc,
}

/// Authentication settings (`[auth]`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    /// Authentication mode: `"disabled"` (default) or `"oidc"`.
    pub mode: AuthMode,
    /// OIDC settings; only consulted when `mode = "oidc"`.
    pub oidc: OidcConfig,
    /// Identity granted the built-in `admin` role at startup (idempotent).
    /// This is how the first administrator gets access in `oidc` mode,
    /// where authorization is deny-by-default.
    pub bootstrap_admin: Option<BootstrapAdminConfig>,
}

/// The startup bootstrap identity (`[auth.bootstrap_admin]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapAdminConfig {
    /// Token issuer URL of the bootstrap identity (matched against the
    /// `iss` claim exactly, like `[[auth.oidc.issuers]].issuer_url`).
    pub issuer: String,
    /// OIDC `sub` claim of the bootstrap identity.
    pub subject: String,
}

/// OIDC validation settings (`[auth.oidc]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct OidcConfig {
    /// Trusted token issuers. At least one is required when `mode = "oidc"`.
    pub issuers: Vec<OidcIssuerConfig>,
    /// Accepted clock skew, in seconds, when validating `exp`/`nbf`.
    pub clock_skew_secs: u64,
    /// Require `https://` issuer URLs. May be disabled for tests against a
    /// local issuer; doing so logs a warning at startup.
    pub require_https_issuers: bool,
    /// Optional claim name that marks a token as a workload/service
    /// credential (in addition to the built-in heuristics: a
    /// `gty = "client-credentials"` claim, or the absence of both `email`
    /// and `preferred_username`).
    pub service_claim: Option<String>,
}

impl Default for OidcConfig {
    fn default() -> Self {
        Self {
            issuers: Vec::new(),
            clock_skew_secs: 60,
            require_https_issuers: true,
            service_claim: None,
        }
    }
}

/// One trusted OIDC issuer (`[[auth.oidc.issuers]]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OidcIssuerConfig {
    /// Issuer URL, matched exactly against the token's `iss` claim.
    pub issuer_url: String,
    /// Audience the token's `aud` claim must contain.
    pub audience: String,
    /// JWKS endpoint. When absent, it is discovered from
    /// `<issuer_url>/.well-known/openid-configuration`.
    #[serde(default)]
    pub jwks_uri: Option<String>,
}

impl AuthConfig {
    /// Cross-field validation, run as part of [`AppConfig::load`].
    pub fn validate(&self) -> Result<(), MeridianError> {
        if let Some(bootstrap) = &self.bootstrap_admin
            && (bootstrap.issuer.is_empty() || bootstrap.subject.is_empty())
        {
            return Err(MeridianError::Validation(
                "auth.bootstrap_admin must set both issuer and subject".to_owned(),
            ));
        }
        if self.mode == AuthMode::Disabled {
            return Ok(());
        }
        if self.oidc.issuers.is_empty() {
            return Err(MeridianError::Validation(
                "auth.mode is \"oidc\" but auth.oidc.issuers is empty; configure at least one \
                 issuer or set auth.mode = \"disabled\""
                    .to_owned(),
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for issuer in &self.oidc.issuers {
            if issuer.issuer_url.is_empty() {
                return Err(MeridianError::Validation(
                    "auth.oidc.issuers entries must set issuer_url".to_owned(),
                ));
            }
            if issuer.audience.is_empty() {
                return Err(MeridianError::Validation(format!(
                    "auth.oidc issuer {:?} must set a non-empty audience",
                    issuer.issuer_url
                )));
            }
            if self.oidc.require_https_issuers && !issuer.issuer_url.starts_with("https://") {
                return Err(MeridianError::Validation(format!(
                    "auth.oidc issuer {:?} is not https; use https or (for tests only) set \
                     auth.oidc.require_https_issuers = false",
                    issuer.issuer_url
                )));
            }
            if !seen.insert(issuer.issuer_url.as_str()) {
                return Err(MeridianError::Validation(format!(
                    "auth.oidc issuer {:?} is configured more than once",
                    issuer.issuer_url
                )));
            }
        }
        Ok(())
    }
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

        let config: Self = figment
            .extract()
            .map_err(|e| MeridianError::Validation(format!("invalid configuration: {e}")))?;
        config.auth.validate()?;
        Ok(config)
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

    #[test]
    fn auth_defaults_to_disabled() {
        let cfg = AppConfig::default();
        assert_eq!(cfg.auth.mode, AuthMode::Disabled);
        assert_eq!(cfg.auth.oidc.clock_skew_secs, 60);
        assert!(cfg.auth.oidc.require_https_issuers);
        assert!(cfg.auth.oidc.issuers.is_empty());
        assert!(cfg.auth.validate().is_ok());
    }

    #[test]
    fn oidc_config_loads_from_toml() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "meridian.toml",
                r#"
                    [auth]
                    mode = "oidc"

                    [auth.oidc]
                    clock_skew_secs = 30
                    service_claim = "meridian_service"

                    [[auth.oidc.issuers]]
                    issuer_url = "https://idp.example.com"
                    audience = "meridian"

                    [[auth.oidc.issuers]]
                    issuer_url = "https://other.example.com"
                    audience = "meridian"
                    jwks_uri = "https://other.example.com/keys"
                "#,
            )?;
            let cfg = AppConfig::load(None).expect("load config");
            assert_eq!(cfg.auth.mode, AuthMode::Oidc);
            assert_eq!(cfg.auth.oidc.clock_skew_secs, 30);
            assert_eq!(
                cfg.auth.oidc.service_claim.as_deref(),
                Some("meridian_service")
            );
            assert_eq!(cfg.auth.oidc.issuers.len(), 2);
            assert_eq!(cfg.auth.oidc.issuers[0].jwks_uri, None);
            assert_eq!(
                cfg.auth.oidc.issuers[1].jwks_uri.as_deref(),
                Some("https://other.example.com/keys")
            );
            Ok(())
        });
    }

    #[test]
    fn oidc_mode_without_issuers_is_rejected() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("meridian.toml", "[auth]\nmode = \"oidc\"\n")?;
            let err = AppConfig::load(None).expect_err("must reject issuer-less oidc");
            assert!(err.to_string().contains("issuers is empty"), "{err}");
            Ok(())
        });
    }

    #[test]
    fn non_https_issuer_is_rejected_unless_opted_out() {
        let mut cfg = AppConfig::default();
        cfg.auth.mode = AuthMode::Oidc;
        cfg.auth.oidc.issuers.push(OidcIssuerConfig {
            issuer_url: "http://idp.local".to_owned(),
            audience: "meridian".to_owned(),
            jwks_uri: None,
        });
        let err = cfg
            .auth
            .validate()
            .expect_err("http issuer must be rejected");
        assert!(err.to_string().contains("not https"), "{err}");

        cfg.auth.oidc.require_https_issuers = false;
        assert!(cfg.auth.validate().is_ok());
    }

    #[test]
    fn duplicate_issuers_are_rejected() {
        let mut cfg = AppConfig::default();
        cfg.auth.mode = AuthMode::Oidc;
        for _ in 0..2 {
            cfg.auth.oidc.issuers.push(OidcIssuerConfig {
                issuer_url: "https://idp.example.com".to_owned(),
                audience: "meridian".to_owned(),
                jwks_uri: None,
            });
        }
        let err = cfg
            .auth
            .validate()
            .expect_err("duplicate issuer must be rejected");
        assert!(err.to_string().contains("more than once"), "{err}");
    }
}
