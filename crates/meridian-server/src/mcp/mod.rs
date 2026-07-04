//! The MCP agent-gateway internals (Pillar H, H-F1/H-F2/H-F4 — the agent
//! firewall). The `/mcp` HTTP route (`crate::routes::mcp`) is thin; the
//! governance lives here.
//!
//! # What a tool call goes through
//!
//! Every `tools/call` from an agent runs the same chain, and every step is
//! recorded:
//!
//! 1. **Identity.** The caller is an authenticated [`Principal`]. The gateway
//!    requires it to be a *registered agent* (kind `agent` with an
//!    `agent_principals` envelope). A non-agent principal reaching `/mcp` is a
//!    protocol error (the gateway is the agent door).
//! 2. **Kill switch + lifecycle.** A disabled agent (kill switch) or an
//!    expired one is refused *before* any tool logic — a tool-error result the
//!    agent can relay, plus an audited `refused_killed` / `refused_expired`
//!    activity row.
//! 3. **Per-tool governance.** Context tools (H-F2) resolve RBAC visibility +
//!    ABAC (masked columns ABSENT from returned schema). Query tools (H-F3)
//!    additionally check the budget and then run (stubbed until wave 2).
//! 4. **Audit.** Whatever the outcome — allowed, denied, refused, error — the
//!    call writes an `agent_activity` ledger row AND a tamper-evident
//!    `audit_log` chain entry on the **same transaction** (the chain is the
//!    product, H-F4). The two cross-reference by `audit_seq`.
//!
//! # The one place the decision is made
//!
//! [`dispatch`] is the single entry point the route calls per tool. It owns the
//! chain above; the per-tool handlers ([`context`], [`query`]) are pure
//! producers of a [`ToolResponse`] (a governed answer + what it touched + the
//! decision). `dispatch` turns that into the wire [`CallToolResult`] and writes
//! the audit rows. No handler writes audit or checks the kill switch itself —
//! so a new tool cannot forget to.

pub mod context;
pub mod engine;
pub mod executor;
pub mod query;

use std::sync::Arc;

use meridian_agents::catalog::{self, ToolClass};
use meridian_agents::decision::{RefusalReason, args_digest};
use meridian_agents::executor::QueryExecutor;
use meridian_agents::protocol::CallToolResult;
use meridian_common::principal::{Principal, PrincipalKind};
use meridian_store::agent::{self, ActivityDecision, AgentPrincipal, NewActivity};
use meridian_store::audit::{self, NewAuditEntry};
use meridian_store::tenancy;
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::AppState;

/// Everything a tool handler needs, resolved once by [`dispatch`]: the app
/// state, the executor seam, the calling agent's envelope, and the resolved
/// purpose for this call.
#[derive(Debug)]
pub struct GatewayCall<'a> {
    /// Shared app state (pool + config).
    pub state: &'a AppState,
    /// The wired query executor (the `QueryExecutor` seam). Its label is
    /// recorded in the audit trail; governed multi-table execution is resolved
    /// per-principal in [`crate::mcp::engine`], which the query handlers call
    /// directly (only there is the principal available to govern the query).
    pub executor: &'a Arc<dyn QueryExecutor>,
    /// The calling agent's envelope (already checked live + unexpired).
    pub agent: &'a AgentPrincipal,
    /// The caller principal (for RBAC/ABAC resolution).
    pub principal: &'a Principal,
    /// The purpose in force for this call: the per-call override if the agent
    /// supplied one, else the agent's registered purpose.
    pub purpose: String,
}

/// What a tool handler returns: the governed answer plus what it touched and
/// the governance decision, for the audit trail. `dispatch` renders this into
/// the wire result and writes the ledger + chain.
#[derive(Debug)]
pub struct ToolResponse {
    /// The wire result to return to the client (may be a tool-error).
    pub result: CallToolResult,
    /// The decision to record in the activity ledger.
    pub decision: ActivityDecision,
    /// Rows touched (context tools pass `None`).
    pub rows: Option<i64>,
    /// Bytes scanned (0 for a context read or a refusal).
    pub bytes: i64,
    /// Dollar estimate in micro-dollars (0 for a context read or a refusal).
    pub cost_micros: i64,
    /// Structured detail folded into the audit-chain entry (what was enforced:
    /// removed columns, applied policies, provenance, …).
    pub audit_detail: Value,
}

impl ToolResponse {
    /// An allowed context response: a governed answer that touched no rows.
    #[must_use]
    pub fn context(result: CallToolResult, audit_detail: Value) -> Self {
        Self {
            result,
            decision: ActivityDecision::Allowed,
            rows: None,
            bytes: 0,
            cost_micros: 0,
            audit_detail,
        }
    }

    /// A refusal/denial response built from a [`RefusalReason`]. Renders the
    /// relayable message as a tool-error result and maps the reason to its
    /// ledger decision.
    #[must_use]
    pub fn refused(reason: &RefusalReason) -> Self {
        let decision = match reason.activity_label() {
            "refused_budget" => ActivityDecision::RefusedBudget,
            "refused_killed" => ActivityDecision::RefusedKilled,
            "refused_expired" => ActivityDecision::RefusedExpired,
            _ => ActivityDecision::Denied,
        };
        Self {
            result: CallToolResult::error(reason.message()),
            decision,
            rows: None,
            bytes: 0,
            cost_micros: 0,
            audit_detail: json!({ "refusal": reason.message() }),
        }
    }
}

/// Resolves the registered-agent envelope for a caller, or an [`AgentGateError`]
/// explaining why the caller cannot use the gateway.
///
/// This is the identity gate: the gateway serves *registered agents* only.
///
/// # Registration, not the token, decides
///
/// An agent authenticates with an ordinary OIDC token — often a
/// client-credentials token, which the edge classifies as a *service*
/// principal. So the token-derived [`PrincipalKind`] is **not** the authority
/// on whether an identity is an agent; the stored `principals` row (kind
/// `agent`, created at registration) is. This function therefore resolves the
/// caller's `(issuer, subject)` to its stored principal record and checks
/// *that* kind — an identity registered as an agent (with an
/// `agent_principals` envelope) is admitted regardless of how its token was
/// classified. An identity that is not registered as an agent is turned away
/// with a distinct error so the route can answer correctly.
pub async fn resolve_agent(
    pool: &PgPool,
    principal: &Principal,
) -> Result<AgentPrincipal, AgentGateError> {
    // Anonymous (auth-disabled dev mode): there is no agent identity to govern,
    // so the gateway is unavailable. Agents are always authenticated.
    if principal.is_anonymous() {
        return Err(AgentGateError::NotAnAgent(
            "the MCP gateway requires an authenticated agent principal; it is unavailable when \
             authentication is disabled"
                .to_owned(),
        ));
    }
    let Some(issuer) = principal.issuer.as_deref() else {
        return Err(AgentGateError::NotAnAgent(
            "the caller is missing an issuer".to_owned(),
        ));
    };
    let record = meridian_store::principal::get_by_identity(pool, issuer, &principal.subject)
        .await
        .map_err(AgentGateError::Store)?;
    let Some(record) = record else {
        return Err(AgentGateError::NotRegistered);
    };
    // The stored kind is the authority: only an identity registered as an agent
    // may use the gateway (its token's edge-classification is irrelevant).
    if record.kind != meridian_store::principal::kind_str(PrincipalKind::Agent) {
        return Err(AgentGateError::NotAnAgent(format!(
            "the MCP gateway serves registered agents only; identity {} is a {} principal",
            principal.audit_string(),
            record.kind
        )));
    }
    let agent = agent::get_agent(pool, &record.id)
        .await
        .map_err(AgentGateError::Store)?;
    agent.ok_or(AgentGateError::NotRegistered)
}

/// Why the gateway turned a caller away.
#[derive(Debug)]
pub enum AgentGateError {
    /// The caller is not an agent principal at all.
    NotAnAgent(String),
    /// The caller is an agent principal but has no `agent_principals` envelope
    /// (it was never registered with the gateway).
    NotRegistered,
    /// A store failure while resolving the agent.
    Store(meridian_common::MeridianError),
}

/// Dispatches one `tools/call` through the full governance chain and returns the
/// wire result.
///
/// `arguments` is the tool's `arguments` object; `per_call_purpose` is an
/// optional purpose override the client supplied. The calling agent envelope
/// has already been resolved by the route via [`resolve_agent`]. Every path
/// through this function writes exactly one activity-ledger row and one
/// audit-chain entry (same transaction).
pub async fn dispatch(
    state: &AppState,
    executor: &Arc<dyn QueryExecutor>,
    agent: &AgentPrincipal,
    principal: &Principal,
    tool_name: &str,
    arguments: &Value,
    per_call_purpose: Option<&str>,
) -> CallToolResult {
    let digest = args_digest(arguments);
    let purpose = per_call_purpose.map_or_else(|| agent.purpose.clone(), str::to_owned);

    // (1) Kill switch: a suspended agent is refused before any tool logic.
    if !agent.enabled {
        let refusal = RefusalReason::Killed;
        return finalize(
            state,
            agent,
            tool_name,
            &digest,
            &purpose,
            ToolResponse::refused(&refusal),
        )
        .await;
    }

    // (2) Lifecycle: an expired agent is refused.
    if agent.is_expired(chrono::Utc::now()) {
        let refusal = RefusalReason::Expired;
        return finalize(
            state,
            agent,
            tool_name,
            &digest,
            &purpose,
            ToolResponse::refused(&refusal),
        )
        .await;
    }

    // (3) Resolve the tool. An unknown tool never reaches here (the route maps
    // it to a protocol error), but be defensive: treat it as a tool-error.
    let Some(tool) = catalog::find(tool_name) else {
        let resp = ToolResponse {
            result: CallToolResult::error(format!("unknown tool {tool_name:?}")),
            decision: ActivityDecision::Error,
            rows: None,
            bytes: 0,
            cost_micros: 0,
            audit_detail: json!({ "error": "unknown tool" }),
        };
        return finalize(state, agent, tool_name, &digest, &purpose, resp).await;
    };

    let call = GatewayCall {
        state,
        executor,
        agent,
        principal,
        purpose: purpose.clone(),
    };

    // (4) Per-tool governance + execution.
    let response = match tool.class {
        ToolClass::Context => context::handle(&call, tool.name, arguments).await,
        ToolClass::Query => query::handle(&call, tool.name, arguments).await,
    };

    finalize(state, agent, tool_name, &digest, &purpose, response).await
}

/// Writes the activity-ledger row and the tamper-evident audit-chain entry for
/// a completed tool call on one transaction, then returns the wire result.
///
/// This is the audit-chain-is-the-product seam (H-F4): the ledger row (queried
/// for the CISO view) and the hash-chain entry (tamper-evident evidence) are
/// atomic — a tool call can never appear in one and not the other. If the audit
/// write itself fails, the call becomes an internal tool-error (we never return
/// a governed answer that we could not record).
async fn finalize(
    state: &AppState,
    agent: &AgentPrincipal,
    tool: &str,
    digest: &str,
    purpose: &str,
    response: ToolResponse,
) -> CallToolResult {
    let workspace_id = tenancy::default_workspace_id();
    let agent_audit = format!("agent:{}", agent_subject(state, agent).await);

    let write = async {
        let mut tx = state
            .pool
            .begin()
            .await
            .map_err(|e| meridian_store::map_sqlx_error("failed to begin agent activity", e))?;
        // The tamper-evident chain entry first, so we can cross-reference its
        // seq from the ledger row.
        let audit_record = audit::append_in_tx(
            &mut tx,
            NewAuditEntry {
                workspace_id: Some(workspace_id),
                principal: agent_audit.clone(),
                action: format!("agent.tool.{}", decision_action(response.decision)),
                resource: format!("agent:{}", agent.principal_id),
                details: json!({
                    "tool": tool,
                    "args_digest": digest,
                    "decision": response.decision.as_str(),
                    "purpose": purpose,
                    "rows": response.rows,
                    "bytes_scanned": response.bytes,
                    "cost_micros": response.cost_micros,
                    "detail": response.audit_detail,
                }),
            },
        )
        .await?;

        agent::record_activity_in_tx(
            &mut tx,
            workspace_id,
            &NewActivity {
                agent_id: &agent.principal_id,
                agent_audit: &agent_audit,
                tool,
                args_digest: digest,
                decision: response.decision,
                purpose: Some(purpose),
                rows_touched: response.rows,
                bytes_scanned: response.bytes,
                cost_micros: response.cost_micros,
                audit_seq: Some(audit_record.seq),
            },
        )
        .await?;

        tx.commit()
            .await
            .map_err(|e| meridian_store::map_sqlx_error("failed to commit agent activity", e))?;
        Ok::<_, meridian_common::MeridianError>(())
    };

    match write.await {
        Ok(()) => response.result,
        Err(error) => {
            // We could not record the call; refuse to return the answer.
            tracing::error!(%error, tool, "failed to record agent tool call; refusing result");
            CallToolResult::error(
                "internal error: the tool call could not be recorded in the audit trail and was \
                 therefore not completed",
            )
        }
    }
}

/// Resolves the agent's subject (for the audit string) from its principal row.
/// Best-effort: falls back to the principal id if the row is unreadable (the
/// id is always correct even if the display subject is not resolvable).
async fn agent_subject(state: &AppState, agent: &AgentPrincipal) -> String {
    let subject: Option<String> =
        sqlx::query_scalar("SELECT subject FROM principals WHERE id = $1")
            .bind(&agent.principal_id)
            .fetch_optional(&state.pool)
            .await
            .ok()
            .flatten();
    subject.unwrap_or_else(|| agent.principal_id.clone())
}

/// The audit-action verb for a decision (`agent.tool.<verb>`).
fn decision_action(decision: ActivityDecision) -> &'static str {
    match decision {
        ActivityDecision::Allowed => "call",
        ActivityDecision::Denied => "denied",
        ActivityDecision::RefusedBudget => "refused_budget",
        ActivityDecision::RefusedKilled => "refused_killed",
        ActivityDecision::RefusedExpired => "refused_expired",
        ActivityDecision::Error => "error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_response_refused_maps_reasons() {
        let killed = ToolResponse::refused(&RefusalReason::Killed);
        assert_eq!(killed.decision, ActivityDecision::RefusedKilled);
        assert!(killed.result.is_error);

        let budget = ToolResponse::refused(&RefusalReason::Budget {
            dimension: "queries_per_hour".into(),
            limit: 1,
            used: 1,
            requested: 1,
        });
        assert_eq!(budget.decision, ActivityDecision::RefusedBudget);

        let denied = ToolResponse::refused(&RefusalReason::PolicyDenied {
            reason: "no".into(),
        });
        assert_eq!(denied.decision, ActivityDecision::Denied);
    }

    #[test]
    fn decision_actions_are_stable() {
        assert_eq!(decision_action(ActivityDecision::Allowed), "call");
        assert_eq!(
            decision_action(ActivityDecision::RefusedKilled),
            "refused_killed"
        );
    }
}
