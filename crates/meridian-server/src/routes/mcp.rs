//! The `/mcp` endpoint (Pillar H, H-F1): a Streamable-HTTP MCP server.
//!
//! MCP over JSON-RPC 2.0, spec revision `2025-06-18`. The transport is the
//! Streamable HTTP profile in its simplest spec-compliant form: the client
//! POSTs one JSON-RPC message and the server answers with one
//! `application/json` JSON-RPC message. (The spec permits answering a request
//! with a single JSON object instead of opening an SSE stream; the gateway does
//! not push server-initiated messages, so it never needs SSE. A GET for a
//! server-to-client stream is answered `405`, which the spec explicitly allows
//! for a server that offers no such stream.)
//!
//! # What this handler owns (and what it delegates)
//!
//! This module is the protocol boundary: method routing, the JSON-RPC envelope,
//! `initialize`/`tools/list` bookkeeping, `Origin` validation (the spec's DNS-
//! rebinding defense), session-id issuance, and mapping protocol-vs-tool errors.
//! The governance — agent identity, kill switch, policy, budget, audit — lives
//! in [`crate::mcp`]; `tools/call` delegates each call to `mcp::dispatch`.
//!
//! # Auth
//!
//! `/mcp` sits behind the same OIDC middleware as every route, so the caller
//! arrives as a [`Principal`] in request extensions. The gateway requires that
//! principal to be a registered agent (kind `agent` with an `agent_principals`
//! envelope); anything else is turned away. Agent tokens are ordinary OIDC
//! tokens — an agent is a first-class principal, distinct from a user/service.

use std::sync::Arc;

use axum::Extension;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use meridian_agents::catalog;
use meridian_agents::executor::QueryExecutor;
use meridian_agents::protocol::{
    self, CallToolParams, Implementation, InitializeResult, JsonRpcError, JsonRpcRequest,
    JsonRpcResponse, ListToolsResult, PROTOCOL_VERSION, ServerCapabilities, ToolsCapability,
    error_codes,
};
use meridian_common::principal::Principal;
use serde_json::{Value, json};
use ulid::Ulid;

use crate::AppState;
use crate::mcp::{self, AgentGateError};

/// The MCP purpose-declaration header: an agent may narrow the purpose for a
/// single tool call (defaults to the agent's registered purpose otherwise).
/// Namespaced as a Meridian extension, matching the scan-plan purpose header.
const PURPOSE_HEADER: &str = "x-meridian-purpose";

/// The session-id header the spec defines for Streamable HTTP.
const SESSION_HEADER: &str = "mcp-session-id";

/// `GET /mcp`: the client asks to open a server-to-client SSE stream. The
/// gateway pushes no server-initiated messages, so — per the transport spec —
/// it answers `405`, telling the client no stream is offered here.
pub async fn mcp_get() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        "the Meridian MCP endpoint does not offer a server-initiated event stream; POST \
         JSON-RPC requests instead",
    )
        .into_response()
}

/// `DELETE /mcp`: the client asks to terminate its session. The gateway keeps
/// no server-side session state (the agent's identity is its token, revalidated
/// each request), so there is nothing to tear down — answer `405`, which the
/// spec permits for a server that does not allow client session termination.
pub async fn mcp_delete() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        "sessions are stateless; nothing to terminate",
    )
        .into_response()
}

/// `POST /mcp`: the single JSON-RPC entry point.
pub async fn mcp_post(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Extension(executor): Extension<Arc<dyn QueryExecutor>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // (Security) Validate Origin when present — the transport spec's DNS-
    // rebinding defense. Browsers set Origin; MCP clients and engines do not,
    // so a missing Origin is fine. A present-but-unallowed Origin is rejected.
    if let Some(rejection) = origin_rejection(&state, &headers) {
        return rejection;
    }

    // Parse the JSON-RPC message. A parse failure is a protocol error with a
    // null id (we could not read the id).
    let request: JsonRpcRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(error) => {
            return json_rpc_error(
                Value::Null,
                error_codes::PARSE_ERROR,
                format!("invalid JSON-RPC request: {error}"),
            );
        }
    };

    if request.jsonrpc != protocol::JSONRPC_VERSION {
        return json_rpc_error(
            request.id.clone().unwrap_or(Value::Null),
            error_codes::INVALID_REQUEST,
            "jsonrpc must be \"2.0\"",
        );
    }

    // A notification (no id) is acknowledged with 202 and no body — e.g.
    // `notifications/initialized`.
    if request.is_notification() {
        return StatusCode::ACCEPTED.into_response();
    }
    let id = request.id.clone().unwrap_or(Value::Null);

    match request.method.as_str() {
        "initialize" => handle_initialize(&state, id, request.params.as_ref()),
        "ping" => json_rpc_ok(id, json!({})),
        "tools/list" => json_rpc_ok(id, tools_list_result()),
        "tools/call" => {
            handle_tools_call(&state, &executor, &principal, &headers, id, request.params).await
        }
        other => json_rpc_error(
            id,
            error_codes::METHOD_NOT_FOUND,
            format!("method {other:?} is not supported"),
        ),
    }
}

/// `initialize`: negotiate the protocol version, advertise the `tools`
/// capability, and mint a session id (returned in the `Mcp-Session-Id` header).
fn handle_initialize(state: &AppState, id: Value, _params: Option<&Value>) -> Response {
    // We speak exactly one protocol version. The spec's negotiation rule says:
    // if we support the client's requested version, echo it; otherwise answer
    // with a version we support. Either way that is our single version, so a
    // client asking for a different one is told what we speak and decides
    // whether to proceed.
    let negotiated = PROTOCOL_VERSION;

    let result = InitializeResult {
        protocol_version: negotiated.to_owned(),
        capabilities: ServerCapabilities {
            tools: ToolsCapability {
                list_changed: false,
            },
        },
        server_info: Implementation {
            name: "meridian".to_owned(),
            title: "Meridian Agent Gateway".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
        },
        instructions: Some(
            "Meridian is a governed data catalog. Use the context tools to discover and \
             understand governed assets, and the query tools to read them within your grants \
             and budget. Restricted columns are absent from schemas by design."
                .to_owned(),
        ),
    };

    let session_id = Ulid::new().to_string();
    let body = JsonRpcResponse::new(id, serde_json::to_value(result).unwrap_or(Value::Null));
    let _ = state; // state reserved for future per-workspace negotiation.
    let mut response = axum::Json(body).into_response();
    if let Ok(value) = axum::http::HeaderValue::from_str(&session_id) {
        response.headers_mut().insert(SESSION_HEADER, value);
    }
    response
}

/// `tools/list`: the governed tool catalog.
fn tools_list_result() -> Value {
    let tools = catalog::wire_tools();
    serde_json::to_value(ListToolsResult { tools }).unwrap_or(Value::Null)
}

/// `tools/call`: resolve the agent, then run the call through the governance
/// chain. A caller that is not a registered agent is a protocol error (the
/// gateway is the agent door); an unknown *tool* is a protocol error too. A
/// governed refusal (kill switch, budget, policy) is a *tool* result with
/// `isError: true`, not a protocol error.
async fn handle_tools_call(
    state: &AppState,
    executor: &Arc<dyn QueryExecutor>,
    principal: &Principal,
    headers: &HeaderMap,
    id: Value,
    params: Option<Value>,
) -> Response {
    let Some(params) = params else {
        return json_rpc_error(
            id,
            error_codes::INVALID_PARAMS,
            "tools/call requires params",
        );
    };
    let params: CallToolParams = match serde_json::from_value(params) {
        Ok(params) => params,
        Err(error) => {
            return json_rpc_error(
                id,
                error_codes::INVALID_PARAMS,
                format!("invalid tools/call params: {error}"),
            );
        }
    };

    // Unknown tool -> protocol error (the spec's example for an unknown tool).
    if catalog::find(&params.name).is_none() {
        return json_rpc_error(
            id,
            error_codes::METHOD_NOT_FOUND,
            format!("unknown tool: {}", params.name),
        );
    }

    // Resolve the calling agent. Not-an-agent / not-registered are protocol
    // errors: the caller cannot use the gateway at all.
    let agent = match mcp::resolve_agent(&state.pool, principal).await {
        Ok(agent) => agent,
        Err(AgentGateError::NotAnAgent(message)) => {
            return json_rpc_error(id, error_codes::INVALID_REQUEST, message);
        }
        Err(AgentGateError::NotRegistered) => {
            return json_rpc_error(
                id,
                error_codes::INVALID_REQUEST,
                "this agent is not registered with the gateway; register it (POST \
                 /api/v2/agents) before calling tools",
            );
        }
        Err(AgentGateError::Store(error)) => {
            return json_rpc_error(
                id,
                error_codes::INTERNAL_ERROR,
                format!("failed to resolve agent: {}", error.public_message()),
            );
        }
    };

    let purpose = header_str(headers, PURPOSE_HEADER);
    let result = mcp::dispatch(
        state,
        executor,
        &agent,
        principal,
        &params.name,
        &params.arguments,
        purpose.as_deref(),
    )
    .await;

    json_rpc_ok(id, serde_json::to_value(result).unwrap_or(Value::Null))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Validates the `Origin` header against the configured CORS allow-list. A
/// missing Origin is permitted (non-browser clients); a present Origin must be
/// allowed (`*` or on the list). This is the transport spec's DNS-rebinding
/// defense, reusing the existing CORS origin configuration so operators
/// configure one allow-list, not two.
fn origin_rejection(state: &AppState, headers: &HeaderMap) -> Option<Response> {
    let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok())?;
    let allowed = &state.config.server.cors_allowed_origins;
    if allowed.iter().any(|o| o == "*" || o == origin) {
        None
    } else {
        Some(
            (
                StatusCode::FORBIDDEN,
                "origin not allowed for the MCP endpoint",
            )
                .into_response(),
        )
    }
}

/// Reads a request header as an owned, non-empty string.
fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// A JSON-RPC success response as `application/json`.
fn json_rpc_ok(id: Value, result: Value) -> Response {
    axum::Json(JsonRpcResponse::new(id, result)).into_response()
}

/// A JSON-RPC error response. The HTTP status stays `200 OK` — a JSON-RPC error
/// is carried in the body, not the HTTP status (an HTTP error status is for
/// transport failures, per the transport spec). The one exception the spec
/// calls out (a bad Origin, a session problem) is handled before this point.
fn json_rpc_error(id: Value, code: i64, message: impl Into<String>) -> Response {
    axum::Json(JsonRpcError::new(id, code, message)).into_response()
}
