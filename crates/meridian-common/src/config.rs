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
    /// Autonomous table-maintenance settings (Pillar C worker).
    pub maintenance: MaintenanceConfig,
    /// Catalog-federation settings (Pillar B inbound-mirror sync worker).
    pub federation: FederationConfig,
    /// Lineage settings (Pillar F: the post-commit lineage worker and the
    /// OpenLineage emitter).
    pub lineage: LineageConfig,
    /// Data-quality settings (Pillar E: the zero-scan monitor evaluation
    /// worker that opens incidents from the commit stream).
    pub quality: QualityConfig,
    /// Transpilation-sidecar settings (Pillar G: universal-view transpilation
    /// and metric compilation via the `SQLGlot` sidecar, §8.5).
    pub transpilation: TranspilationConfig,
}

/// Transpilation-sidecar settings (`[transpilation]`): how the Rust server
/// reaches the `SQLGlot` sidecar (§8.5) for universal-view translation (G-F1)
/// and metric compilation (G-F2).
///
/// The sidecar is a separate, stateless, localhost-scoped process (see
/// `sidecar/`). The server calls it over HTTP; when it is unreachable, the
/// universal-view path degrades gracefully (serves the canonical representation
/// with a status note) rather than failing a `LoadView`. The deterministic
/// `SQLGlot` path is the sidecar's only transpilation engine; the optional
/// BYO-key LLM-assist fallback is configured *on the sidecar*, never here.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TranspilationConfig {
    /// Base URL of the transpilation sidecar. Localhost by default (the sidecar
    /// binds `127.0.0.1:8200` by design). The server health-checks and calls
    /// `<base>/v1/transpile`, `<base>/v1/compile_metric`, and `<base>/healthz`.
    pub sidecar_url: String,
    /// Per-request timeout (seconds) for sidecar calls. Transpilation is a
    /// bounded CPU operation; this caps a pathological request so a slow sidecar
    /// cannot stall a `LoadView` or a metric compile.
    pub request_timeout_secs: u64,
}

impl Default for TranspilationConfig {
    fn default() -> Self {
        Self {
            sidecar_url: "http://127.0.0.1:8200".to_owned(),
            request_timeout_secs: 15,
        }
    }
}

/// Data-quality settings (`[quality]`): the post-commit monitor evaluation
/// worker (E-F1/E-F5). Monitors, incidents, contracts, and the quality score
/// are always manageable through the API/CLI and readable in the console; this
/// only governs the background *evaluation* worker that computes monitor
/// results from the commit stream and opens incidents.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct QualityConfig {
    /// Master switch for the monitor evaluation worker. When `false` the worker
    /// is not spawned: monitors/incidents can still be managed and queried, but
    /// nothing is evaluated on the commit stream and no incidents are opened
    /// automatically.
    pub enabled: bool,
    /// How often (seconds) the worker polls the committed-event stream once
    /// caught up.
    pub poll_interval_secs: u64,
    /// How many prior commits to summarize as the baseline history window for
    /// the anomaly scorers. A larger window is more stable but slower to react
    /// to a genuine regime change; the default balances the two.
    pub history_window: i64,
    /// Commit-failure storm threshold: this many failed/retried commit attempts
    /// on a table inside the window opens a commit-failure incident.
    pub commit_failure_threshold: i64,
}

impl Default for QualityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 5,
            history_window: 20,
            commit_failure_threshold: 5,
        }
    }
}

/// Lineage settings (`[lineage]`): the post-commit lineage worker (F-F1) and
/// the OpenLineage emitter for Meridian-initiated jobs (F-F2). The OpenLineage
/// *sink* (`POST /api/v2/lineage/openlineage`) is always available and needs
/// no configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LineageConfig {
    /// Master switch for the post-commit lineage worker. When `false` the
    /// worker is not spawned: the `OpenLineage` sink and the graph/impact reads
    /// still work, but commit-native edges are not derived on a schedule.
    pub enabled: bool,
    /// How often (seconds) the post-commit worker polls the committed-event
    /// stream once caught up.
    pub poll_interval_secs: u64,
    /// Optional `OpenLineage` collector base URL (e.g. a Marquez instance).
    /// When set, Meridian-initiated jobs emit a `RunEvent` to
    /// `<url>/api/v1/lineage`; when unset, nothing is emitted (events can
    /// still be pulled from Meridian's own lineage graph).
    pub openlineage_url: Option<String>,
}

impl Default for LineageConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 5,
            openlineage_url: None,
        }
    }
}

/// Catalog-federation settings (`[federation]`): the background sync worker
/// that pulls inbound mirrors (spec Pillar B, B-F1). Runs inside
/// `meridian serve` alongside the maintenance and events workers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FederationConfig {
    /// Master switch. When `false` the sync worker is not spawned: mirrors can
    /// still be registered and their config managed, and a manual `sync now`
    /// still works, but nothing syncs on a schedule.
    pub enabled: bool,
    /// How often (seconds) the worker polls for a mirror that is due to sync
    /// (never-synced, past its interval, or flagged by `sync now`).
    pub poll_interval_secs: u64,
    /// Per-HTTP-request timeout (seconds) when talking to a source catalog:
    /// bounds each `GET /v1/config` / list / `loadTable` call so an
    /// unresponsive source cannot stall a sync run indefinitely.
    pub request_timeout_secs: u64,
    /// Lease deadline in seconds for a mirror claimed for sync (its status is
    /// `running`). A worker that crashes mid-sync leaves the mirror `running`;
    /// this reclaims it only after `updated_at` is older than the lease, so a
    /// second worker (another replica, or the scheduler racing a manual
    /// `sync now`) does not double-sync a mirror that is actively being synced.
    /// Set above the longest legitimate sync run; a sync is idempotent
    /// (mirror rows are upserted), so a rare double-run is wasteful, not
    /// corrupting.
    pub sync_lease_secs: u64,
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 60,
            request_timeout_secs: 30,
            sync_lease_secs: 900,
        }
    }
}

/// Autonomous table-maintenance settings (`[maintenance]`): the background
/// worker that runs the built-in executors and the desired-state
/// reconciliation loop (spec Pillar C). Both run inside `meridian serve`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MaintenanceConfig {
    /// Master switch. When `false` neither the job worker nor the
    /// reconciliation loop is spawned: maintenance jobs stay `queued` and no
    /// policy-driven jobs are enqueued. The API and CLI still work (manual
    /// triggers enqueue; they are just not drained).
    pub enabled: bool,
    /// Job-worker poll interval in milliseconds once the queue is drained.
    /// The worker claims one job per iteration (`FOR UPDATE SKIP LOCKED`) and
    /// keeps going while jobs are available, sleeping only when the queue is
    /// empty.
    pub worker_poll_ms: u64,
    /// Attempts a single maintenance job gets before it is marked `failed`.
    /// A commit-conflict re-plan (yield to a writer) does not consume an
    /// attempt on its own; this bounds genuine execution failures.
    pub max_job_attempts: i32,
    /// How many times one job execution re-plans and retries after losing the
    /// optimistic-commit race to a concurrent writer commit (spec C-F4:
    /// maintenance commits always yield to writer commits). Exhausting this
    /// re-queues the job (it is not a failure — the table is simply busy).
    pub commit_retry_limit: u32,
    /// Whether the desired-state reconciliation loop runs (spec C-F3). When
    /// `false` the worker only drains explicitly-enqueued jobs; no jobs are
    /// created from policy target violations.
    pub reconcile_enabled: bool,
    /// Reconciliation-loop interval in seconds: how often enabled policies are
    /// evaluated against table health to enqueue violating tables.
    pub reconcile_interval_secs: u64,
    /// Per-table debounce in seconds: the reconciliation loop will not enqueue
    /// a second job for a table until this long after the previous enqueue,
    /// so one unhealthy table cannot flood the queue (spec C-F3 debounce).
    pub reconcile_debounce_secs: u64,
    /// Commit-storm coalescing window in seconds (spec C-F3 streaming-aware
    /// mode): a table whose newest snapshot advanced within this many seconds
    /// of the reconciliation pass is treated as actively committing and is
    /// skipped — compacting it would only lose the commit race to the writer.
    pub reconcile_commit_quiet_secs: i64,
    /// Small-file ratio at or above which the reconciliation loop enqueues a
    /// compaction (in `[0,1]`). The health model's small-file signal.
    pub reconcile_small_file_ratio: f64,
    /// Snapshot count above which the reconciliation loop enqueues a snapshot
    /// expiry, when the effective policy's retention is exceeded.
    pub reconcile_snapshot_slack: i32,
    /// Extra retained snapshots kept beyond the policy's `retention_count`
    /// even when age would allow expiry — a fixed safety window so expiry
    /// never trims a table down to exactly the current snapshot (spec C-F2
    /// "safety window"). Expiry keeps `max(retention_count, this)` snapshots.
    pub expiry_min_snapshots_kept: i32,
    /// Whether the worker may run snapshot-expiry jobs. Expiry is
    /// metadata-only (drops old snapshots via `remove-snapshots`), but it is
    /// destructive of history, so it has its own switch, default on.
    pub expiry_enabled: bool,
    /// Backoff in seconds applied when a maintenance job is re-queued (it
    /// yielded to a concurrent writer commit, or errored with retries left).
    /// The job's `run_after` is set this far ahead so it is not re-claimed
    /// immediately: without it, a perpetually-busy table spins the worker
    /// (claim, yield, re-queue, claim, …) and starves other jobs. Should be
    /// short enough to retry promptly once the table quiets, long enough to
    /// break the tight loop.
    pub requeue_backoff_secs: i64,
    /// Lease deadline in seconds for a claimed (`running`) maintenance job. A
    /// worker that crashes or is `SIGKILL`ed mid-job leaves it `running` with
    /// no one to finish it, which permanently blocks that table's future
    /// maintenance (the reconciler debounces on in-flight jobs). The reconciler
    /// reclaims a `running` job whose `updated_at` is older than this — back to
    /// `queued` for a retry, or to `failed` once the attempt budget is spent.
    /// Set comfortably above the longest legitimate job runtime; the
    /// maintenance commit is optimistic-CAS safe, so a reclaim racing a
    /// still-alive original loses the commit rather than corrupting anything.
    pub job_lease_secs: i64,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            worker_poll_ms: 2_000,
            max_job_attempts: 3,
            commit_retry_limit: 5,
            reconcile_enabled: true,
            reconcile_interval_secs: 300,
            reconcile_debounce_secs: 3_600,
            reconcile_commit_quiet_secs: 120,
            reconcile_small_file_ratio: 0.30,
            reconcile_snapshot_slack: 0,
            expiry_min_snapshots_kept: 1,
            expiry_enabled: true,
            requeue_backoff_secs: 60,
            job_lease_secs: 1_800,
        }
    }
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
    /// Browser origins permitted by CORS. Engines are not browsers and are
    /// unaffected by this; it exists so the web console (a separate-origin
    /// client) can call the API. The default allows the console on its usual
    /// localhost dev ports. Set to `["*"]` to allow any origin (credentials
    /// are then disallowed per the CORS spec), or `[]` to disable CORS
    /// entirely.
    pub cors_allowed_origins: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_owned(),
            port: 8181,
            request_timeout_secs: 30,
            max_body_bytes: 16 * 1024 * 1024,
            cors_allowed_origins: vec![
                "http://localhost:3000".to_owned(),
                "http://localhost:3100".to_owned(),
            ],
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
    /// Maximum number of pooled connections. This pool is shared by every API
    /// request handler **and** the background workers (outbox relay, webhook
    /// dispatcher, maintenance worker + reconciler, federation and lineage
    /// loops, plan sweeper), several of which hold a connection continuously.
    /// Size it above `(peak concurrent requests) + (worker count)`; the default
    /// leaves headroom for the workers plus a modest request concurrency. Stays
    /// well under Postgres's own `max_connections` (default 100).
    pub max_connections: u32,
    /// Minimum warm connections kept open, so a burst after idle does not pay
    /// connection-establishment latency on the first requests.
    pub min_connections: u32,
    /// Timeout in seconds when acquiring a connection from the pool. On
    /// exhaustion a handler fails fast with this bound rather than hanging.
    pub acquire_timeout_secs: u64,
    /// Recycle a connection after it has been idle this long (seconds), so the
    /// pool releases capacity back to Postgres during quiet periods.
    pub idle_timeout_secs: u64,
    /// Hard cap on a connection's lifetime (seconds) regardless of use. Bounds
    /// staleness and lets the pool recover cleanly after a Postgres failover or
    /// a rolling restart of the database (old connections are retired rather
    /// than erroring mid-query indefinitely).
    pub max_lifetime_secs: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_connections: 20,
            min_connections: 2,
            acquire_timeout_secs: 5,
            idle_timeout_secs: 600,
            max_lifetime_secs: 1_800,
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
