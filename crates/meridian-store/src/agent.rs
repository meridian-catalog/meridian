//! Agent-gateway persistence (Pillar H, H-F1/H-F4 — the agent firewall).
//!
//! Owns the three tables migration 0020 introduces:
//!
//! - `agent_principals`: the per-agent governance envelope (owner, purpose,
//!   environment, lifecycle dates, the kill switch), 1:1 with a `principals`
//!   row of kind `agent`.
//! - `agent_budgets`: per-agent caps (queries/hour, scanned-bytes/day,
//!   dollar-estimate/day) plus rolling-window counters.
//! - `agent_activity`: the append-only per-tool-call ledger — the evidence
//!   half of the audit chain (the tamper-evident half is [`crate::audit`]).
//!
//! # What this module is (and is not)
//!
//! It is pure persistence + the budget-window arithmetic. It does **not** make
//! MCP protocol decisions, resolve governed context, or run queries — those are
//! the `meridian-agents` crate and the server's `/mcp` route. The type boundary
//! mirrors the rest of the store: rows in, records out, every mutation carrying
//! its audit row and outbox event on the *same* transaction (the invariant the
//! whole codebase holds: no mutation without its audit row).
//!
//! # Budget semantics (the graceful-refusal path)
//!
//! [`check_and_consume_budget`] is the one call the gateway makes before a
//! query tool runs. It takes the budget row `FOR UPDATE`, rolls any window that
//! has elapsed (per-hour queries; per-day scanned-bytes + dollar-estimate),
//! then decides: if applying the increment would exceed a cap, it refuses
//! *without* consuming (returning which dimension and the numbers, for the
//! agent-relayable message); otherwise it consumes and allows. A `NULL` cap is
//! uncapped. Reads (context tools) do not consume the query budget — only the
//! governed query tools do, and only on an allowed decision.

use chrono::{DateTime, Duration, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::json;
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// Length of the per-hour queries budget window.
const QUERIES_WINDOW: Duration = Duration::hours(1);

/// Length of the per-day scanned-bytes + dollar-estimate budget window.
const DAY_WINDOW: Duration = Duration::days(1);

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// The persisted agent governance envelope.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AgentPrincipal {
    /// The agent's principal id (`principals.id`, kind `agent`).
    pub principal_id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Audit string of the accountable owner (e.g. `user:alice@example.com`).
    pub owner: String,
    /// The agent's declared purpose statement.
    pub purpose: String,
    /// Deployment environment (`dev` | `prod`).
    pub environment: String,
    /// Hard expiry: after this instant every tool is refused. `None` = no
    /// expiry.
    pub expires_at: Option<DateTime<Utc>>,
    /// Advisory recertification date. `None` = none set.
    pub review_at: Option<DateTime<Utc>>,
    /// The kill switch: `false` refuses every tool call. Default `true`.
    pub enabled: bool,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl AgentPrincipal {
    /// Whether the agent is expired as of `now`.
    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_some_and(|exp| now >= exp)
    }
}

/// The persisted per-agent budget row (caps + rolling-window counters).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AgentBudget {
    /// The agent's principal id.
    pub agent_id: String,
    /// Cap on queries per rolling hour; `None` = uncapped.
    pub queries_per_hour: Option<i64>,
    /// Cap on scanned bytes per rolling day; `None` = uncapped.
    pub scanned_bytes_per_day: Option<i64>,
    /// Cap on the dollar estimate per rolling day, in micro-dollars; `None` =
    /// uncapped.
    pub dollar_cap_micros: Option<i64>,
    /// Start of the current per-hour queries window.
    pub queries_window_start: DateTime<Utc>,
    /// Queries consumed in the current per-hour window.
    pub queries_in_window: i64,
    /// Start of the current per-day window (scanned-bytes + dollar-estimate).
    pub day_window_start: DateTime<Utc>,
    /// Scanned bytes consumed in the current per-day window.
    pub scanned_bytes_in_day: i64,
    /// Dollar estimate consumed in the current per-day window (micro-dollars).
    pub cost_micros_in_day: i64,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// The caps to set on an agent's budget. `None` on a field means "uncapped".
#[derive(Debug, Clone, Copy, Default)]
pub struct BudgetLimits {
    /// Cap on queries per rolling hour.
    pub queries_per_hour: Option<i64>,
    /// Cap on scanned bytes per rolling day.
    pub scanned_bytes_per_day: Option<i64>,
    /// Cap on the dollar estimate per rolling day, in micro-dollars.
    pub dollar_cap_micros: Option<i64>,
}

/// Everything needed to register an agent: its identity plus its envelope.
#[derive(Debug, Clone)]
pub struct NewAgent<'a> {
    /// The agent's principal id (already provisioned, kind `agent`).
    pub principal_id: &'a str,
    /// Accountable owner (audit string).
    pub owner: &'a str,
    /// Purpose statement.
    pub purpose: &'a str,
    /// Environment (`dev` | `prod`).
    pub environment: &'a str,
    /// Hard expiry, if any.
    pub expires_at: Option<DateTime<Utc>>,
    /// Advisory review date, if any.
    pub review_at: Option<DateTime<Utc>>,
    /// Initial budget caps.
    pub limits: BudgetLimits,
}

const AGENT_COLUMNS: &str = "principal_id, workspace_id, owner, purpose, environment, \
     expires_at, review_at, enabled, created_at, updated_at";

const BUDGET_COLUMNS: &str = "agent_id, queries_per_hour, scanned_bytes_per_day, \
     dollar_cap_micros, queries_window_start, queries_in_window, day_window_start, \
     scanned_bytes_in_day, cost_micros_in_day, updated_at";

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// True when the error is a Postgres foreign-key violation.
fn is_fk_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Registers an agent: inserts its governance envelope and its budget row, with
/// the audit row and outbox event, all on one transaction.
///
/// The principal row (kind `agent`) must already exist — the caller provisions
/// it via [`crate::principal::ensure`] first (an unknown principal id is a
/// validation error). Registering an already-registered agent is a conflict.
pub async fn register(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    new: &NewAgent<'_>,
    actor: &str,
) -> Result<AgentPrincipal> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin agent registration", e))?;

    let record: AgentPrincipal = sqlx::query_as(&format!(
        "INSERT INTO agent_principals
             (principal_id, workspace_id, owner, purpose, environment, expires_at, review_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING {AGENT_COLUMNS}"
    ))
    .bind(new.principal_id)
    .bind(workspace_id.to_string())
    .bind(new.owner)
    .bind(new.purpose)
    .bind(new.environment)
    .bind(new.expires_at)
    .bind(new.review_at)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!(
                "agent {:?} is already registered",
                new.principal_id
            ))
        } else if is_fk_violation(&e) {
            MeridianError::Validation(format!(
                "unknown principal id {:?} (provision the agent principal first)",
                new.principal_id
            ))
        } else {
            map_sqlx_error("failed to insert agent principal", e)
        }
    })?;

    sqlx::query(
        "INSERT INTO agent_budgets
             (agent_id, queries_per_hour, scanned_bytes_per_day, dollar_cap_micros)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(new.principal_id)
    .bind(new.limits.queries_per_hour)
    .bind(new.limits.scanned_bytes_per_day)
    .bind(new.limits.dollar_cap_micros)
    .execute(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to insert agent budget", e))?;

    let details = json!({
        "principal_id": new.principal_id,
        "owner": new.owner,
        "purpose": new.purpose,
        "environment": new.environment,
        "expires_at": new.expires_at,
        "review_at": new.review_at,
        "queries_per_hour": new.limits.queries_per_hour,
        "scanned_bytes_per_day": new.limits.scanned_bytes_per_day,
        "dollar_cap_micros": new.limits.dollar_cap_micros,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("agent:{}", new.principal_id),
            event_type: "agent.registered".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: actor.to_owned(),
            action: "agent.register".to_owned(),
            resource: format!("agent:{}", new.principal_id),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit agent registration", e))?;
    Ok(record)
}

// ---------------------------------------------------------------------------
// Lookups
// ---------------------------------------------------------------------------

/// Loads an agent envelope by principal id.
pub async fn get_agent(pool: &PgPool, principal_id: &str) -> Result<Option<AgentPrincipal>> {
    sqlx::query_as(&format!(
        "SELECT {AGENT_COLUMNS} FROM agent_principals WHERE principal_id = $1"
    ))
    .bind(principal_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load agent", e))
}

/// Lists every registered agent in a workspace, oldest first.
pub async fn list_agents(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<AgentPrincipal>> {
    sqlx::query_as(&format!(
        "SELECT {AGENT_COLUMNS} FROM agent_principals WHERE workspace_id = $1 ORDER BY created_at"
    ))
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list agents", e))
}

/// Loads an agent's budget row by agent id.
pub async fn get_budget(pool: &PgPool, agent_id: &str) -> Result<Option<AgentBudget>> {
    sqlx::query_as(&format!(
        "SELECT {BUDGET_COLUMNS} FROM agent_budgets WHERE agent_id = $1"
    ))
    .bind(agent_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load agent budget", e))
}

// ---------------------------------------------------------------------------
// Kill switch / lifecycle mutations
// ---------------------------------------------------------------------------

/// Flips the kill switch (`enabled`) for an agent, with its audit row + outbox
/// event. `false` suspends the agent (every tool refused); `true` re-enables.
/// A no-op change (already in the requested state) still records the intent —
/// an operator's decision to (re)assert a state is itself audit-worthy.
pub async fn set_enabled(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    agent_id: &str,
    enabled: bool,
    actor: &str,
    reason: &str,
) -> Result<AgentPrincipal> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin agent kill-switch update", e))?;

    let record: Option<AgentPrincipal> = sqlx::query_as(&format!(
        "UPDATE agent_principals SET enabled = $2, updated_at = now()
         WHERE principal_id = $1
         RETURNING {AGENT_COLUMNS}"
    ))
    .bind(agent_id)
    .bind(enabled)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to update agent kill switch", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "agent {agent_id:?} is not registered"
        )));
    };

    let action = if enabled {
        "agent.enable"
    } else {
        "agent.suspend"
    };
    let details = json!({ "agent_id": agent_id, "enabled": enabled, "reason": reason });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("agent:{agent_id}"),
            event_type: format!("agent.{}", if enabled { "enabled" } else { "suspended" }),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: actor.to_owned(),
            action: action.to_owned(),
            resource: format!("agent:{agent_id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit agent kill-switch update", e))?;
    Ok(record)
}

// ---------------------------------------------------------------------------
// Budget check + consume (the graceful-refusal path)
// ---------------------------------------------------------------------------

/// The dimension a budget refusal is attributed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetDimension {
    /// The per-hour queries cap.
    QueriesPerHour,
    /// The per-day scanned-bytes cap.
    ScannedBytesPerDay,
    /// The per-day dollar-estimate cap.
    DollarPerDay,
}

impl BudgetDimension {
    /// A short, stable label for messages and audit detail.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QueriesPerHour => "queries_per_hour",
            Self::ScannedBytesPerDay => "scanned_bytes_per_day",
            Self::DollarPerDay => "dollar_cap_per_day",
        }
    }
}

/// The outcome of a budget check.
#[derive(Debug, Clone)]
pub enum BudgetOutcome {
    /// Within budget; the increment was consumed. Carries the post-consume
    /// window usage for the caller's activity/analytics record.
    Allowed {
        /// Queries used in the current hour after consuming.
        queries_used: i64,
        /// Scanned bytes used in the current day after consuming.
        bytes_used: i64,
        /// Dollar estimate used in the current day after consuming (micros).
        cost_used_micros: i64,
    },
    /// A cap would be exceeded; nothing was consumed. Carries the offending
    /// dimension, the cap, and what the usage would have become — everything a
    /// graceful, agent-relayable refusal message needs.
    Refused {
        /// Which cap was hit.
        dimension: BudgetDimension,
        /// The cap value.
        limit: i64,
        /// Current usage in the window (before this call).
        used: i64,
        /// What this call would have added.
        requested: i64,
    },
}

impl BudgetOutcome {
    /// Whether the call is allowed.
    #[must_use]
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed { .. })
    }
}

/// The cost of one query call to charge against the budget.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryCost {
    /// Estimated bytes the call will scan.
    pub bytes: i64,
    /// Estimated dollar cost of the call, in micro-dollars.
    pub cost_micros: i64,
}

/// Atomically rolls elapsed windows, checks the caps, and — if within
/// budget — consumes one query plus `cost` against the agent's budget.
///
/// Runs in its own transaction, taking the budget row `FOR UPDATE` so
/// concurrent tool calls from the same agent cannot race past a cap. On
/// [`BudgetOutcome::Refused`] nothing is consumed and the row is left rolled
/// (an elapsed window still resets even on a refusal — the refusal reflects the
/// *current* window). An agent with no budget row is treated as fully uncapped
/// (returns `Allowed` with zero usage) — registration always creates one, so
/// this only covers a manually-inserted legacy agent.
pub async fn check_and_consume_budget(
    pool: &PgPool,
    agent_id: &str,
    cost: QueryCost,
) -> Result<BudgetOutcome> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin budget check", e))?;

    let budget: Option<AgentBudget> = sqlx::query_as(&format!(
        "SELECT {BUDGET_COLUMNS} FROM agent_budgets WHERE agent_id = $1 FOR UPDATE"
    ))
    .bind(agent_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load budget for update", e))?;

    let Some(budget) = budget else {
        // No budget row: uncapped (registration always creates one).
        tx.commit()
            .await
            .map_err(|e| map_sqlx_error("failed to commit budget check", e))?;
        return Ok(BudgetOutcome::Allowed {
            queries_used: 0,
            bytes_used: 0,
            cost_used_micros: 0,
        });
    };

    let now = Utc::now();

    // Roll windows that have fully elapsed.
    let (queries_window_start, mut queries_used) = roll(
        budget.queries_window_start,
        budget.queries_in_window,
        now,
        QUERIES_WINDOW,
    );
    let (day_window_start, mut bytes_used, mut cost_used) = roll_day(
        budget.day_window_start,
        budget.scanned_bytes_in_day,
        budget.cost_micros_in_day,
        now,
    );

    // Check each cap against (current usage + this call's increment). Use
    // saturating arithmetic: `cost.bytes`/`cost.cost_micros` are clamped to
    // i64::MAX upstream (an oversized or corrupt manifest estimate), so a plain
    // `+` could overflow — in release that wraps negative and silently passes
    // the `> limit` check, failing OPEN. Saturating to i64::MAX makes an
    // oversized estimate refuse, which is the safe direction.
    if let Some(limit) = budget.queries_per_hour
        && queries_used.saturating_add(1) > limit
    {
        return refuse(tx, BudgetDimension::QueriesPerHour, limit, queries_used, 1).await;
    }
    if let Some(limit) = budget.scanned_bytes_per_day
        && bytes_used.saturating_add(cost.bytes) > limit
    {
        return refuse(
            tx,
            BudgetDimension::ScannedBytesPerDay,
            limit,
            bytes_used,
            cost.bytes,
        )
        .await;
    }
    if let Some(limit) = budget.dollar_cap_micros
        && cost_used.saturating_add(cost.cost_micros) > limit
    {
        return refuse(
            tx,
            BudgetDimension::DollarPerDay,
            limit,
            cost_used,
            cost.cost_micros,
        )
        .await;
    }

    // Within budget: consume (saturating, matching the checks above).
    queries_used = queries_used.saturating_add(1);
    bytes_used = bytes_used.saturating_add(cost.bytes);
    cost_used = cost_used.saturating_add(cost.cost_micros);

    sqlx::query(
        "UPDATE agent_budgets SET
             queries_window_start = $2, queries_in_window = $3,
             day_window_start = $4, scanned_bytes_in_day = $5, cost_micros_in_day = $6,
             updated_at = now()
         WHERE agent_id = $1",
    )
    .bind(agent_id)
    .bind(queries_window_start)
    .bind(queries_used)
    .bind(day_window_start)
    .bind(bytes_used)
    .bind(cost_used)
    .execute(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to consume budget", e))?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit budget consume", e))?;

    Ok(BudgetOutcome::Allowed {
        queries_used,
        bytes_used,
        cost_used_micros: cost_used,
    })
}

/// Commits the rolled (but not consumed) budget window and returns a refusal.
/// The window reset is persisted so a later call sees the fresh window even
/// though this one was refused; the counters themselves are unchanged from
/// their rolled values (no increment).
async fn refuse(
    tx: sqlx::Transaction<'_, sqlx::Postgres>,
    dimension: BudgetDimension,
    limit: i64,
    used: i64,
    requested: i64,
) -> Result<BudgetOutcome> {
    // We do not persist the roll here to keep refusal a pure read: rolling on
    // refusal is a nicety, not a correctness need, and a pure read avoids a
    // write under the FOR UPDATE lock on the (common) refusal path. The next
    // allowed call rolls and persists.
    tx.rollback()
        .await
        .map_err(|e| map_sqlx_error("failed to roll back budget check", e))?;
    Ok(BudgetOutcome::Refused {
        dimension,
        limit,
        used,
        requested,
    })
}

/// Rolls a single-counter window: if `now` is past `start + window`, the window
/// restarts at `now` with a zero counter; otherwise it is unchanged.
fn roll(
    start: DateTime<Utc>,
    counter: i64,
    now: DateTime<Utc>,
    window: Duration,
) -> (DateTime<Utc>, i64) {
    if now >= start + window {
        (now, 0)
    } else {
        (start, counter)
    }
}

/// Rolls the per-day window (two counters share it).
fn roll_day(
    start: DateTime<Utc>,
    bytes: i64,
    cost: i64,
    now: DateTime<Utc>,
) -> (DateTime<Utc>, i64, i64) {
    if now >= start + DAY_WINDOW {
        (now, 0, 0)
    } else {
        (start, bytes, cost)
    }
}

// ---------------------------------------------------------------------------
// Activity ledger
// ---------------------------------------------------------------------------

/// A tool-call decision as recorded in the activity ledger. Mirrors the
/// `agent_activity.decision` CHECK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityDecision {
    /// The call was authorized and ran (or would have run — for a stubbed
    /// executor, `allowed` still means governance passed).
    Allowed,
    /// A policy denied the call (RBAC or ABAC).
    Denied,
    /// A budget cap refused the call.
    RefusedBudget,
    /// The kill switch (disabled agent) refused the call.
    RefusedKilled,
    /// The agent's lifecycle expiry refused the call.
    RefusedExpired,
    /// The call errored (a tool-internal failure).
    Error,
}

impl ActivityDecision {
    /// The database/wire rendering (matches the 0020 CHECK).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Denied => "denied",
            Self::RefusedBudget => "refused_budget",
            Self::RefusedKilled => "refused_killed",
            Self::RefusedExpired => "refused_expired",
            Self::Error => "error",
        }
    }
}

/// A new activity-ledger row to append.
#[derive(Debug, Clone)]
pub struct NewActivity<'a> {
    /// The acting agent's principal id.
    pub agent_id: &'a str,
    /// The agent's audit string (kept even if the agent row is later deleted).
    pub agent_audit: &'a str,
    /// The MCP tool invoked.
    pub tool: &'a str,
    /// A sha256 hex digest of the redacted call arguments.
    pub args_digest: &'a str,
    /// The governance decision.
    pub decision: ActivityDecision,
    /// The resolved purpose for this call, if any.
    pub purpose: Option<&'a str>,
    /// Rows touched (context tools pass `None`).
    pub rows_touched: Option<i64>,
    /// Bytes scanned (0 for a refusal or a metadata read).
    pub bytes_scanned: i64,
    /// Dollar estimate in micro-dollars (0 for a refusal or a metadata read).
    pub cost_micros: i64,
    /// The cross-referenced `audit_log` seq (the tamper-evident chain entry).
    pub audit_seq: Option<i64>,
}

/// A persisted activity-ledger row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AgentActivity {
    /// ULID of the ledger row.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Acting agent's principal id (`None` if the agent was later deleted).
    pub agent_id: Option<String>,
    /// The agent's audit string at the time of the call.
    pub agent_audit: String,
    /// The MCP tool invoked.
    pub tool: String,
    /// sha256 hex digest of the redacted arguments.
    pub args_digest: String,
    /// The governance decision (wire form).
    pub decision: String,
    /// Resolved purpose, if any.
    pub purpose: Option<String>,
    /// Rows touched, if applicable.
    pub rows_touched: Option<i64>,
    /// Bytes scanned.
    pub bytes_scanned: i64,
    /// Dollar estimate (micro-dollars).
    pub cost_micros: i64,
    /// Cross-referenced `audit_log` seq.
    pub audit_seq: Option<i64>,
    /// When the call occurred.
    pub occurred_at: DateTime<Utc>,
}

const ACTIVITY_COLUMNS: &str = "id, workspace_id, agent_id, agent_audit, tool, args_digest, \
     decision, purpose, rows_touched, bytes_scanned, cost_micros, audit_seq, occurred_at";

/// Appends an activity-ledger row on the caller's transaction.
///
/// Kept as an `_in_tx` variant so the gateway writes the ledger row and the
/// tamper-evident [`crate::audit`] chain entry on the *same* transaction — the
/// audit-chain-is-the-product invariant (H-F4). Returns the persisted row.
pub async fn record_activity_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace_id: WorkspaceId,
    activity: &NewActivity<'_>,
) -> Result<AgentActivity> {
    let id = Ulid::new().to_string();
    let record: AgentActivity = sqlx::query_as(&format!(
        "INSERT INTO agent_activity
             (id, workspace_id, agent_id, agent_audit, tool, args_digest, decision, purpose,
              rows_touched, bytes_scanned, cost_micros, audit_seq)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         RETURNING {ACTIVITY_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(activity.agent_id)
    .bind(activity.agent_audit)
    .bind(activity.tool)
    .bind(activity.args_digest)
    .bind(activity.decision.as_str())
    .bind(activity.purpose)
    .bind(activity.rows_touched)
    .bind(activity.bytes_scanned)
    .bind(activity.cost_micros)
    .bind(activity.audit_seq)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| map_sqlx_error("failed to append agent activity", e))?;
    Ok(record)
}

/// Filters for querying the activity ledger. Conjunctive; unset fields do not
/// constrain.
#[derive(Debug, Clone, Default)]
pub struct ActivityQuery<'a> {
    /// Restrict to one agent.
    pub agent_id: Option<&'a str>,
    /// Restrict to one tool.
    pub tool: Option<&'a str>,
    /// Restrict to one decision (wire form).
    pub decision: Option<&'a str>,
    /// Keyset cursor: only rows strictly older than this id.
    pub before_id: Option<&'a str>,
    /// Page size (the caller clamps).
    pub limit: i64,
}

/// Queries the activity ledger for a workspace, newest first (`id` descending,
/// which is time-ordered for ULIDs), with keyset pagination.
pub async fn query_activity(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    query: &ActivityQuery<'_>,
) -> Result<Vec<AgentActivity>> {
    let mut builder = sqlx::QueryBuilder::new(&format!(
        "SELECT {ACTIVITY_COLUMNS} FROM agent_activity WHERE workspace_id = "
    ));
    builder.push_bind(workspace_id.to_string());
    if let Some(agent_id) = query.agent_id {
        builder.push(" AND agent_id = ").push_bind(agent_id);
    }
    if let Some(tool) = query.tool {
        builder.push(" AND tool = ").push_bind(tool);
    }
    if let Some(decision) = query.decision {
        builder.push(" AND decision = ").push_bind(decision);
    }
    if let Some(before_id) = query.before_id {
        builder.push(" AND id < ").push_bind(before_id);
    }
    builder
        .push(" ORDER BY id DESC LIMIT ")
        .push_bind(query.limit);
    builder
        .build_query_as()
        .fetch_all(pool)
        .await
        .map_err(|e| map_sqlx_error("failed to query agent activity", e))
}

/// Counts the distinct tables an agent has touched (from the activity ledger
/// tool arguments is out of scope here; anomaly hooks live in the gateway).
/// This helper returns how many activity rows an agent has in a recent window,
/// used by the off-hours / volume anomaly signal.
pub async fn recent_activity_count(
    pool: &PgPool,
    agent_id: &str,
    since: DateTime<Utc>,
) -> Result<i64> {
    sqlx::query_scalar(
        "SELECT count(*) FROM agent_activity WHERE agent_id = $1 AND occurred_at >= $2",
    )
    .bind(agent_id)
    .bind(since)
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to count recent agent activity", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_rolls_only_after_elapsed() {
        let start = DateTime::parse_from_rfc3339("2026-07-04T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        // Within the window: unchanged.
        let inside = start + Duration::minutes(30);
        assert_eq!(roll(start, 5, inside, QUERIES_WINDOW), (start, 5));
        // Past the window: reset to `now` with a zero counter.
        let outside = start + Duration::hours(1) + Duration::seconds(1);
        assert_eq!(roll(start, 5, outside, QUERIES_WINDOW), (outside, 0));
        // Exactly at the boundary rolls (>=).
        let boundary = start + Duration::hours(1);
        assert_eq!(roll(start, 5, boundary, QUERIES_WINDOW), (boundary, 0));
    }

    #[test]
    fn day_window_rolls_both_counters() {
        let start = DateTime::parse_from_rfc3339("2026-07-04T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let inside = start + Duration::hours(5);
        assert_eq!(roll_day(start, 100, 200, inside), (start, 100, 200));
        let outside = start + Duration::days(1) + Duration::seconds(1);
        assert_eq!(roll_day(start, 100, 200, outside), (outside, 0, 0));
    }

    #[test]
    fn decision_and_dimension_strings_are_stable() {
        assert_eq!(ActivityDecision::Allowed.as_str(), "allowed");
        assert_eq!(ActivityDecision::RefusedBudget.as_str(), "refused_budget");
        assert_eq!(ActivityDecision::RefusedKilled.as_str(), "refused_killed");
        assert_eq!(ActivityDecision::RefusedExpired.as_str(), "refused_expired");
        assert_eq!(BudgetDimension::QueriesPerHour.as_str(), "queries_per_hour");
        assert_eq!(
            BudgetDimension::ScannedBytesPerDay.as_str(),
            "scanned_bytes_per_day"
        );
    }

    #[test]
    fn expiry_is_inclusive_of_the_deadline() {
        let now = DateTime::parse_from_rfc3339("2026-07-04T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let agent = |exp: Option<DateTime<Utc>>| AgentPrincipal {
            principal_id: "a".into(),
            workspace_id: "w".into(),
            owner: "user:o".into(),
            purpose: "p".into(),
            environment: "dev".into(),
            expires_at: exp,
            review_at: None,
            enabled: true,
            created_at: now,
            updated_at: now,
        };
        assert!(!agent(None).is_expired(now));
        assert!(!agent(Some(now + Duration::seconds(1))).is_expired(now));
        assert!(agent(Some(now)).is_expired(now));
        assert!(agent(Some(now - Duration::seconds(1))).is_expired(now));
    }
}
