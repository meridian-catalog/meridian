//! Meridian MCP agent gateway core (Pillar H — the agent firewall).
//!
//! This crate is the **pure, database-free** heart of the agent gateway: the
//! Model Context Protocol wire types, the governed tool catalog, the
//! [`QueryExecutor`] seam the query engine implements, and the graceful-refusal
//! decision model. The HTTP endpoint, the OIDC auth, the governed-context
//! resolution, the budget enforcement, and the audit-chain writes live in
//! `meridian-server` (which needs the store); this crate owns the vocabulary
//! and the wire contract so both sides — and the wave-2 executor — agree on the
//! shapes without depending on each other.
//!
//! # The agent firewall, in one paragraph
//!
//! Agents are first-class principals (kind `agent`), distinct from users and
//! services, each with an owner, a purpose, an environment, a lifecycle, a set
//! of scoped grants, a budget, and a kill switch. Every MCP tool call is
//! authenticated, governed (RBAC + ABAC — masked columns are *absent* from
//! returned context, not nulled), budget-checked, and written to both the
//! append-only activity ledger and the tamper-evident hash-chained audit log.
//! That chain — *which agent called which tool, what the policy decided, and
//! what data it touched* — is the product (H-F4).
//!
//! # Module map
//!
//! - [`protocol`] — JSON-RPC 2.0 + MCP `initialize`/`tools/list`/`tools/call`
//!   wire types (spec `2025-06-18`), including the protocol-error vs
//!   tool-error (`isError`) distinction.
//! - [`catalog`] — the governed tool catalog: the context tools (H-F2) and
//!   query tools (H-F3), each with its argument schema and its read-vs-query
//!   governance class.
//! - [`executor`] — the [`QueryExecutor`] trait (implemented by the
//!   `DataFusion` executor in wave 2) and the [`executor::NotWiredExecutor`]
//!   wave-1 stub.
//! - [`decision`] — the stable argument digest and the graceful-refusal
//!   messages.

pub mod catalog;
pub mod decision;
pub mod executor;
pub mod protocol;

pub use catalog::{CATALOG, CatalogTool, ToolClass};
pub use decision::{RefusalReason, args_digest};
pub use executor::{
    ExecutorError, NotWiredExecutor, Provenance, QueryExecutor, QueryOutcome, QueryRequest,
};
pub use protocol::{
    CallToolParams, CallToolResult, Content, InitializeResult, JsonRpcError, JsonRpcRequest,
    JsonRpcResponse, ListToolsResult, PROTOCOL_VERSION, Tool,
};
