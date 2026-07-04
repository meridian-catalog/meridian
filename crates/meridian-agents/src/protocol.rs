//! The Model Context Protocol wire types (spec revision `2025-06-18`).
//!
//! JSON-RPC 2.0 over HTTP: an MCP client POSTs one JSON-RPC message to the
//! `/mcp` endpoint and the server answers with one JSON-RPC message. This
//! module models exactly the message shapes Meridian's gateway produces and
//! consumes — `initialize`, `tools/list`, `tools/call`, and the
//! `notifications/initialized` acknowledgement — plus the two error channels
//! MCP defines (a protocol-level JSON-RPC `error`, and a tool-level result with
//! `isError: true`).
//!
//! # Why a hand-rolled model (not an SDK)
//!
//! The correctness-critical path stays in-house (the workspace principle): the
//! gateway is a governance boundary, so the exact bytes on the wire are ours to
//! own and test. The surface is small and stable, and modeling it directly
//! keeps the dependency footprint at `serde` — matching how the IRC surface is
//! built.
//!
//! # Protocol-error vs tool-error (the distinction the spec draws)
//!
//! - A malformed request, an unknown method, or an unknown tool is a
//!   **protocol error**: a JSON-RPC response carrying an `error` object with a
//!   standard code (see [`error_codes`]).
//! - A tool that ran but refused or failed (a budget refusal, a policy denial,
//!   a kill-switched agent, an executor that is not wired yet) is a **tool
//!   error**: a *successful* JSON-RPC response whose `result` is a
//!   [`CallToolResult`] with `is_error = true`. The agent can read and relay
//!   the text — which is exactly what the graceful-refusal requirement (H-F4)
//!   needs.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The MCP protocol revision this gateway implements.
pub const PROTOCOL_VERSION: &str = "2025-06-18";

/// The JSON-RPC version string (always `"2.0"`).
pub const JSONRPC_VERSION: &str = "2.0";

/// Standard JSON-RPC error codes used by the gateway.
pub mod error_codes {
    /// Invalid JSON was received (parse error).
    pub const PARSE_ERROR: i64 = -32700;
    /// The JSON sent is not a valid Request object.
    pub const INVALID_REQUEST: i64 = -32600;
    /// The method does not exist / is not available.
    pub const METHOD_NOT_FOUND: i64 = -32601;
    /// Invalid method parameters.
    pub const INVALID_PARAMS: i64 = -32602;
    /// Internal JSON-RPC error.
    pub const INTERNAL_ERROR: i64 = -32603;
}

/// A JSON-RPC request or notification as received from an MCP client.
///
/// A *notification* is a request with no `id` (e.g.
/// `notifications/initialized`); the server acknowledges it without a result
/// body. `params` is left as an opaque [`Value`] and decoded per-method.
#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    /// Must be `"2.0"`.
    pub jsonrpc: String,
    /// Request id — a string or a number. Absent for notifications. Preserved
    /// verbatim so the response id matches the request id exactly.
    #[serde(default)]
    pub id: Option<Value>,
    /// The method, e.g. `initialize`, `tools/list`, `tools/call`.
    pub method: String,
    /// Method parameters (method-specific shape).
    #[serde(default)]
    pub params: Option<Value>,
}

impl JsonRpcRequest {
    /// Whether this is a notification (no `id`) — acknowledged without a
    /// result.
    #[must_use]
    pub fn is_notification(&self) -> bool {
        self.id.is_none()
    }
}

/// A JSON-RPC success response.
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcResponse {
    /// Always `"2.0"`.
    pub jsonrpc: &'static str,
    /// Echoes the request id.
    pub id: Value,
    /// The method result.
    pub result: Value,
}

impl JsonRpcResponse {
    /// Builds a success response echoing `id`.
    #[must_use]
    pub fn new(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION,
            id,
            result,
        }
    }
}

/// A JSON-RPC error response (a protocol-level error, never a tool error).
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcError {
    /// Always `"2.0"`.
    pub jsonrpc: &'static str,
    /// Echoes the request id, or `null` when the id could not be determined.
    pub id: Value,
    /// The error object.
    pub error: ErrorObject,
}

/// The `error` member of a JSON-RPC error response.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorObject {
    /// A standard JSON-RPC error code (see [`error_codes`]).
    pub code: i64,
    /// A short, client-safe description.
    pub message: String,
    /// Optional structured detail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcError {
    /// Builds an error response echoing `id` (use `Value::Null` when unknown).
    #[must_use]
    pub fn new(id: Value, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION,
            id,
            error: ErrorObject {
                code,
                message: message.into(),
                data: None,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// initialize
// ---------------------------------------------------------------------------

/// Implementation identity returned to the client in `serverInfo`.
#[derive(Debug, Clone, Serialize)]
pub struct Implementation {
    /// Machine name of the server.
    pub name: String,
    /// Human-readable display name.
    pub title: String,
    /// Server version string.
    pub version: String,
}

/// The `initialize` result: negotiated protocol version, advertised
/// capabilities, and server identity.
#[derive(Debug, Clone, Serialize)]
pub struct InitializeResult {
    /// The protocol version the server will speak (echoes the client's when
    /// supported, otherwise the server's latest).
    #[serde(rename = "protocolVersion")]
    pub protocol_version: String,
    /// Server capabilities. The gateway advertises `tools` only.
    pub capabilities: ServerCapabilities,
    /// Server identity.
    #[serde(rename = "serverInfo")]
    pub server_info: Implementation,
    /// Optional free-text guidance for the client/model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

/// The capabilities the gateway advertises. Only `tools` is offered — no
/// prompts, resources, sampling, or logging.
#[derive(Debug, Clone, Serialize)]
pub struct ServerCapabilities {
    /// The tools capability (present ⇒ `tools/list` and `tools/call` work).
    pub tools: ToolsCapability,
}

/// The `tools` capability object.
#[derive(Debug, Clone, Serialize)]
pub struct ToolsCapability {
    /// Whether the server emits `notifications/tools/list_changed`. The
    /// gateway's tool list is static, so this is `false`.
    #[serde(rename = "listChanged")]
    pub list_changed: bool,
}

// ---------------------------------------------------------------------------
// tools/list
// ---------------------------------------------------------------------------

/// A tool definition as advertised by `tools/list`.
#[derive(Debug, Clone, Serialize)]
pub struct Tool {
    /// Unique tool identifier (e.g. `get_table_context`).
    pub name: String,
    /// Human-readable display name.
    pub title: String,
    /// What the tool does.
    pub description: String,
    /// JSON Schema for the tool's arguments.
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

/// The `tools/list` result.
#[derive(Debug, Clone, Serialize)]
pub struct ListToolsResult {
    /// The advertised tools.
    pub tools: Vec<Tool>,
}

/// The parameters of a `tools/call` request.
#[derive(Debug, Clone, Deserialize)]
pub struct CallToolParams {
    /// The tool to invoke.
    pub name: String,
    /// The tool's arguments (defaults to an empty object).
    #[serde(default)]
    pub arguments: Value,
}

// ---------------------------------------------------------------------------
// tools/call result
// ---------------------------------------------------------------------------

/// One content item in a [`CallToolResult`]. The gateway only ever emits text
/// content (structured payloads travel additionally in `structuredContent`).
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum Content {
    /// A text content block.
    #[serde(rename = "text")]
    Text {
        /// The text.
        text: String,
    },
}

impl Content {
    /// A text content block.
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }
}

/// The result of `tools/call`: content plus the tool-error flag.
///
/// `is_error = true` marks a tool that ran but refused/failed (the agent can
/// relay the text). It is NOT a protocol error — the JSON-RPC envelope is still
/// a success `result`. `structured_content` carries the machine-readable form
/// of the same answer for tools that return structured data.
#[derive(Debug, Clone, Serialize)]
pub struct CallToolResult {
    /// Unstructured content (always at least one text block).
    pub content: Vec<Content>,
    /// The structured form of the result, when the tool produces one.
    #[serde(rename = "structuredContent", skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<Value>,
    /// Whether the tool call is an error the agent should treat as a failure
    /// (a refusal, denial, or tool-internal error).
    #[serde(rename = "isError")]
    pub is_error: bool,
}

impl CallToolResult {
    /// A successful tool result: a text summary plus the structured payload.
    ///
    /// Per the spec, a tool returning structured content SHOULD also serialize
    /// it into a text block for backwards compatibility — so the structured
    /// value is rendered to `content` when no explicit summary is given.
    #[must_use]
    pub fn structured(summary: impl Into<String>, structured: Value) -> Self {
        Self {
            content: vec![Content::text(summary)],
            structured_content: Some(structured),
            is_error: false,
        }
    }

    /// A successful text-only tool result.
    #[must_use]
    pub fn ok_text(text: impl Into<String>) -> Self {
        Self {
            content: vec![Content::text(text)],
            structured_content: None,
            is_error: false,
        }
    }

    /// A tool-error result (`is_error = true`) carrying a relayable message.
    /// This is the graceful-refusal shape: a budget refusal, a policy denial,
    /// a kill-switched agent, or an unwired executor.
    #[must_use]
    pub fn error(text: impl Into<String>) -> Self {
        Self {
            content: vec![Content::text(text)],
            structured_content: None,
            is_error: true,
        }
    }

    /// A tool-error result that also carries a structured refusal payload (so a
    /// client can act on the reason programmatically, not just relay text).
    #[must_use]
    pub fn error_structured(text: impl Into<String>, structured: Value) -> Self {
        Self {
            content: vec![Content::text(text)],
            structured_content: Some(structured),
            is_error: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_without_id_is_a_notification() {
        let req: JsonRpcRequest = serde_json::from_value(
            json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
        )
        .unwrap();
        assert!(req.is_notification());
        assert_eq!(req.method, "notifications/initialized");
    }

    #[test]
    fn request_with_id_is_not_a_notification() {
        let req: JsonRpcRequest =
            serde_json::from_value(json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
                .unwrap();
        assert!(!req.is_notification());
    }

    #[test]
    fn call_tool_result_serializes_is_error_and_content() {
        let ok = CallToolResult::ok_text("hi");
        let v = serde_json::to_value(&ok).unwrap();
        assert_eq!(v["isError"], json!(false));
        assert_eq!(v["content"][0]["type"], json!("text"));
        assert_eq!(v["content"][0]["text"], json!("hi"));
        // No structured content field when absent.
        assert!(v.get("structuredContent").is_none());

        let err = CallToolResult::error("budget exceeded");
        let v = serde_json::to_value(&err).unwrap();
        assert_eq!(v["isError"], json!(true));
    }

    #[test]
    fn structured_result_includes_both_channels() {
        let r = CallToolResult::structured("summary", json!({ "k": "v" }));
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["isError"], json!(false));
        assert_eq!(v["structuredContent"], json!({ "k": "v" }));
        assert_eq!(v["content"][0]["text"], json!("summary"));
    }

    #[test]
    fn error_response_shape() {
        let e = JsonRpcError::new(json!(7), error_codes::METHOD_NOT_FOUND, "no such method");
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["jsonrpc"], json!("2.0"));
        assert_eq!(v["id"], json!(7));
        assert_eq!(v["error"]["code"], json!(-32601));
        assert_eq!(v["error"]["message"], json!("no such method"));
        assert!(v["error"].get("data").is_none());
    }

    #[test]
    fn initialize_result_uses_spec_field_names() {
        let r = InitializeResult {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            capabilities: ServerCapabilities {
                tools: ToolsCapability {
                    list_changed: false,
                },
            },
            server_info: Implementation {
                name: "meridian".into(),
                title: "Meridian".into(),
                version: "0.1.0".into(),
            },
            instructions: None,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["protocolVersion"], json!("2025-06-18"));
        assert_eq!(v["capabilities"]["tools"]["listChanged"], json!(false));
        assert_eq!(v["serverInfo"]["name"], json!("meridian"));
        assert!(v.get("instructions").is_none());
    }
}
