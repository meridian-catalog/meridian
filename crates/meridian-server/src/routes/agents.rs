//! Agent management API (Pillar H, H-F1/H-F4), mounted under `/api/v2/agents`.
//! The control plane for the agent firewall the MCP endpoint enforces:
//!
//! - **Register** (`POST /agents`): provision an agent principal (kind
//!   `agent`) from its OIDC identity and attach its governance envelope —
//!   owner, purpose, environment, lifecycle dates, and initial budget caps.
//! - **List / get** (`GET /agents`, `GET /agents/{id}`): the agent registry.
//! - **Kill switch** (`POST /agents/{id}/suspend`, `.../enable`, H-F4): flip
//!   an agent live/suspended. A suspended agent has every MCP tool refused.
//! - **Activity** (`GET /agents/activity`, H-F5): the per-tool-call ledger —
//!   the CISO's "which agent read what, for which purpose, and what the policy
//!   decided" evidence.
//!
//! # Authorization
//!
//! Every route is **management-gated** (`admin` role or any `MANAGE_WAREHOUSE`
//! grant) — the same gate governance and maintenance mutations use. Registering
//! an agent, setting its budget, and pulling the kill switch are privileged,
//! cross-resource security actions; a dedicated management check is the honest
//! fit (matching `routes::governance`). Grants *to* an agent are made through
//! the ordinary RBAC API (`/api/v2/grants` with the agent's principal id) — an
//! agent is a first-class principal, so its scoped access reuses the existing
//! grant machinery rather than a parallel one.

use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use meridian_common::principal::{Principal, PrincipalKind};
use meridian_store::agent::{self, ActivityQuery, AgentPrincipal, BudgetLimits, NewAgent};
use meridian_store::{principal, tenancy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::require_management;

/// Request body to register an agent.
#[derive(Debug, Deserialize)]
pub struct RegisterAgentRequest {
    /// The agent's OIDC issuer URL (matched against the `iss` claim of the
    /// token the agent will present, exactly like a user/service principal).
    pub issuer: String,
    /// The agent's OIDC subject (`sub`).
    pub subject: String,
    /// A display name for the agent, if any.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Audit string of the accountable owner (e.g. `user:alice@example.com`).
    pub owner: String,
    /// The agent's declared purpose statement.
    pub purpose: String,
    /// Deployment environment (`dev` | `prod`). Defaults to `dev`.
    #[serde(default = "default_environment")]
    pub environment: String,
    /// Hard expiry (after this the agent is refused). Optional.
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    /// Advisory recertification date. Optional.
    #[serde(default)]
    pub review_at: Option<DateTime<Utc>>,
    /// Cap on queries per rolling hour. Optional (uncapped if absent).
    #[serde(default)]
    pub queries_per_hour: Option<i64>,
    /// Cap on scanned bytes per rolling day. Optional.
    #[serde(default)]
    pub scanned_bytes_per_day: Option<i64>,
    /// Cap on the dollar estimate per rolling day, in micro-dollars. Optional.
    #[serde(default)]
    pub dollar_cap_micros: Option<i64>,
}

fn default_environment() -> String {
    "dev".to_owned()
}

/// An agent as rendered by the API (the envelope; budgets are a sub-resource
/// read via the get endpoint).
#[derive(Debug, Serialize)]
pub struct AgentResponse {
    /// The agent's principal id.
    pub id: String,
    /// Accountable owner.
    pub owner: String,
    /// Purpose statement.
    pub purpose: String,
    /// Environment.
    pub environment: String,
    /// Hard expiry, if set.
    pub expires_at: Option<DateTime<Utc>>,
    /// Advisory review date, if set.
    pub review_at: Option<DateTime<Utc>>,
    /// Whether the agent is live (kill switch on = `true`).
    pub enabled: bool,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

impl From<AgentPrincipal> for AgentResponse {
    fn from(a: AgentPrincipal) -> Self {
        Self {
            id: a.principal_id,
            owner: a.owner,
            purpose: a.purpose,
            environment: a.environment,
            expires_at: a.expires_at,
            review_at: a.review_at,
            enabled: a.enabled,
            created_at: a.created_at,
        }
    }
}

/// `POST /api/v2/agents` — register an agent (provision its principal + attach
/// its envelope and budget).
pub async fn register_agent(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Json(req): Json<RegisterAgentRequest>,
) -> Result<(axum::http::StatusCode, Json<AgentResponse>), ApiError> {
    require_management(&state.pool, &caller).await?;
    let workspace_id = tenancy::default_workspace_id();

    if req.environment != "dev" && req.environment != "prod" {
        return Err(ApiError::bad_request(
            "environment must be \"dev\" or \"prod\"",
        ));
    }
    if req.issuer.is_empty() || req.subject.is_empty() {
        return Err(ApiError::bad_request("issuer and subject are required"));
    }

    // Provision the agent's principal row (kind = agent) from its OIDC identity,
    // so the token it later presents resolves to this same identity.
    let identity = Principal {
        kind: PrincipalKind::Agent,
        subject: req.subject.clone(),
        issuer: Some(req.issuer.clone()),
        display_name: req.display_name.clone(),
    };
    let principal_record = principal::ensure(&state.pool, workspace_id, &identity).await?;
    // Guard: the identity might already exist as a non-agent principal
    // (issuer+subject is globally unique). Registering it as an agent would be
    // a category error — refuse rather than governing a human as an agent.
    if principal_record.kind != "agent" {
        return Err(ApiError::new(
            axum::http::StatusCode::CONFLICT,
            "AlreadyExistsException",
            format!(
                "identity {}::{} already exists as a {} principal and cannot be registered as \
                 an agent",
                req.issuer, req.subject, principal_record.kind
            ),
        ));
    }

    let agent = agent::register(
        &state.pool,
        workspace_id,
        &NewAgent {
            principal_id: &principal_record.id,
            owner: &req.owner,
            purpose: &req.purpose,
            environment: &req.environment,
            expires_at: req.expires_at,
            review_at: req.review_at,
            limits: BudgetLimits {
                queries_per_hour: req.queries_per_hour,
                scanned_bytes_per_day: req.scanned_bytes_per_day,
                dollar_cap_micros: req.dollar_cap_micros,
            },
        },
        &caller.audit_string(),
    )
    .await?;

    Ok((
        axum::http::StatusCode::CREATED,
        Json(AgentResponse::from(agent)),
    ))
}

/// `GET /api/v2/agents` — list all registered agents.
pub async fn list_agents(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let agents = agent::list_agents(&state.pool, tenancy::default_workspace_id()).await?;
    let out: Vec<AgentResponse> = agents.into_iter().map(AgentResponse::from).collect();
    Ok(Json(json!({ "agents": out })))
}

/// `GET /api/v2/agents/{id}` — one agent's envelope plus its budget (caps +
/// current window usage).
pub async fn get_agent(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let agent = agent::get_agent(&state.pool, &id).await?.ok_or_else(|| {
        ApiError::new(
            axum::http::StatusCode::NOT_FOUND,
            "NotFoundException",
            format!("agent {id:?} is not registered"),
        )
    })?;
    let budget = agent::get_budget(&state.pool, &id).await?;
    let budget_json = budget.map(|b| {
        json!({
            "queries_per_hour": b.queries_per_hour,
            "scanned_bytes_per_day": b.scanned_bytes_per_day,
            "dollar_cap_micros": b.dollar_cap_micros,
            "queries_in_window": b.queries_in_window,
            "scanned_bytes_in_day": b.scanned_bytes_in_day,
            "cost_micros_in_day": b.cost_micros_in_day,
        })
    });
    Ok(Json(json!({
        "agent": AgentResponse::from(agent),
        "budget": budget_json,
    })))
}

/// Request body for a kill-switch flip (a reason for the audit trail).
#[derive(Debug, Deserialize)]
pub struct KillSwitchRequest {
    /// Why the agent is being suspended/re-enabled (recorded in the audit log).
    #[serde(default)]
    pub reason: Option<String>,
}

/// `POST /api/v2/agents/{id}/suspend` — engage the kill switch (H-F4). Every
/// MCP tool call for this agent is refused until it is re-enabled.
pub async fn suspend_agent(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
    body: Option<Json<KillSwitchRequest>>,
) -> Result<Json<AgentResponse>, ApiError> {
    set_enabled(state, caller, id, false, body).await
}

/// `POST /api/v2/agents/{id}/enable` — release the kill switch.
pub async fn enable_agent(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Path(id): Path<String>,
    body: Option<Json<KillSwitchRequest>>,
) -> Result<Json<AgentResponse>, ApiError> {
    set_enabled(state, caller, id, true, body).await
}

async fn set_enabled(
    state: AppState,
    caller: Principal,
    id: String,
    enabled: bool,
    body: Option<Json<KillSwitchRequest>>,
) -> Result<Json<AgentResponse>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let reason = body
        .and_then(|b| b.0.reason)
        .unwrap_or_else(|| "no reason given".to_owned());
    let agent = agent::set_enabled(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        enabled,
        &caller.audit_string(),
        &reason,
    )
    .await?;
    Ok(Json(AgentResponse::from(agent)))
}

/// Query parameters for the activity ledger.
#[derive(Debug, Deserialize)]
pub struct ActivityParams {
    /// Restrict to one agent.
    #[serde(default)]
    pub agent_id: Option<String>,
    /// Restrict to one tool.
    #[serde(default)]
    pub tool: Option<String>,
    /// Restrict to one decision (wire form, e.g. `refused_budget`).
    #[serde(default)]
    pub decision: Option<String>,
    /// Keyset cursor: only rows strictly older than this id.
    #[serde(default)]
    pub before: Option<String>,
    /// Page size (clamped to 1..=200, default 50).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /api/v2/agents/activity` — the per-tool-call ledger (H-F5). The CISO
/// evidence view: which agent called which tool, the decision, and what it
/// touched.
pub async fn list_activity(
    State(state): State<AppState>,
    Extension(caller): Extension<Principal>,
    Query(params): Query<ActivityParams>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &caller).await?;
    let limit = params.limit.unwrap_or(50).clamp(1, 200);
    let rows = agent::query_activity(
        &state.pool,
        tenancy::default_workspace_id(),
        &ActivityQuery {
            agent_id: params.agent_id.as_deref(),
            tool: params.tool.as_deref(),
            decision: params.decision.as_deref(),
            before_id: params.before.as_deref(),
            limit,
        },
    )
    .await?;

    let next = rows.last().map(|r| r.id.clone());
    let items: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            json!({
                "id": r.id,
                "agent_id": r.agent_id,
                "agent": r.agent_audit,
                "tool": r.tool,
                "args_digest": r.args_digest,
                "decision": r.decision,
                "purpose": r.purpose,
                "rows_touched": r.rows_touched,
                "bytes_scanned": r.bytes_scanned,
                "cost_micros": r.cost_micros,
                "audit_seq": r.audit_seq,
                "occurred_at": r.occurred_at,
            })
        })
        .collect();

    // The next-page cursor is only meaningful when the page was full.
    let next_cursor = if i64::try_from(items.len()).unwrap_or(i64::MAX) == limit {
        next
    } else {
        None
    };
    Ok(Json(json!({ "activity": items, "next": next_cursor })))
}
