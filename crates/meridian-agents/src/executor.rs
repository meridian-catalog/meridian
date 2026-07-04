//! The [`QueryExecutor`] trait — the seam between the agent gateway's
//! governance (this crate + the server) and the query engine that actually
//! runs SQL (the sibling `DataFusion` executor, wave 2).
//!
//! # The contract
//!
//! The gateway owns *governance*: it authenticates the agent, resolves the
//! policy (row filters, column masks), checks the budget, and writes the audit
//! chain. It does **not** run SQL. When a query tool (`run_sql`,
//! `query_metrics`, `preview_table`) passes governance, the gateway hands a
//! fully-resolved [`QueryRequest`] to a `QueryExecutor` and records the
//! [`QueryOutcome`] it returns (rows + provenance + the bytes/cost actually
//! touched, which reconcile the pre-execution estimate the budget charged).
//!
//! The request the gateway passes is **already governed**: the SQL has been
//! validated and the applicable row-filter predicate and the set of
//! columns-to-drop are attached, so a correct executor needs only to *apply*
//! them, never to resolve policy itself. This keeps the policy decision in one
//! place (the gateway) and makes the executor a pure compute component.
//!
//! # Why a trait (and why `dyn`)
//!
//! Wave 2 plugs the real executor in without the gateway depending on the
//! executor crate — the gateway holds a `dyn QueryExecutor`. Until then the
//! [`NotWiredExecutor`] stub answers every query with a clean, agent-relayable
//! "executor not wired" tool-error, so the governance around queries (budget,
//! policy, audit) is exercised and correct *now*, and swapping in real
//! execution changes nothing about the governance path.

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

/// A governed, ready-to-run query handed to the executor.
///
/// Everything here has already passed governance: `sql` is validated, and the
/// row filter + dropped columns describe exactly the policy the executor must
/// apply. The executor does not re-derive policy.
#[derive(Debug, Clone)]
pub struct QueryRequest {
    /// The validated SQL to execute.
    pub sql: String,
    /// The warehouse the query targets (routing + credential scope).
    pub warehouse: Option<String>,
    /// A serialized row-filter predicate to AND into the query, if the policy
    /// resolved one. `None` = no row filter. (Serialized so this crate does not
    /// depend on the iceberg expression type; the executor re-parses it.)
    pub row_filter: Option<Value>,
    /// Columns the policy requires be absent from the result (masked/denied).
    /// The executor must not return these.
    pub dropped_columns: Vec<String>,
    /// A hard cap on rows returned (results are size-capped for agents).
    pub row_limit: i64,
    /// The purpose declared for this query (for the executor's own audit hooks,
    /// e.g. session tags on vended credentials).
    pub purpose: Option<String>,
}

/// Provenance for a query result: the exact data read, so an agent can cite.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Provenance {
    /// The table ids (Meridian internal ids) the query read.
    pub table_ids: Vec<String>,
    /// The snapshot ids read, paired by index with `table_ids` where known.
    pub snapshot_ids: Vec<i64>,
}

impl Provenance {
    /// Empty provenance (nothing read).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            table_ids: Vec::new(),
            snapshot_ids: Vec::new(),
        }
    }
}

/// The result of executing a governed query.
#[derive(Debug, Clone)]
pub struct QueryOutcome {
    /// The column names, in result order (already policy-filtered).
    pub columns: Vec<String>,
    /// The rows, each a JSON array aligned to `columns`.
    pub rows: Vec<Value>,
    /// Where the data came from (tables + snapshots), for citation.
    pub provenance: Provenance,
    /// Bytes actually scanned (reconciles the budget's pre-estimate).
    pub bytes_scanned: i64,
    /// Dollar cost actually incurred, in micro-dollars.
    pub cost_micros: i64,
}

/// The error an executor returns. Kept small and stringly-typed at this seam so
/// the executor crate is free to use its own richer error internally; the
/// gateway renders this into a tool-error result the agent can relay.
#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    /// The executor is not wired into this build (the wave-1 default).
    #[error("query execution is not available: {0}")]
    NotWired(String),
    /// The SQL was rejected by the executor (parse/plan failure, unsupported
    /// construct).
    #[error("query rejected: {0}")]
    Rejected(String),
    /// Execution failed at runtime (storage error, engine failure).
    #[error("query execution failed: {0}")]
    Failed(String),
}

/// The seam a query engine implements to run governed agent queries.
///
/// Implemented by the sibling `DataFusion` executor in wave 2; the gateway
/// depends only on this trait. `async_trait` keeps it object-safe so the
/// gateway can hold a `dyn QueryExecutor`. The `Debug` bound lets the server's
/// per-call context structs derive `Debug` even while holding a
/// `dyn QueryExecutor` (the codebase warns on missing `Debug`).
#[async_trait]
pub trait QueryExecutor: Send + Sync + std::fmt::Debug {
    /// Executes a governed query and returns rows + provenance.
    ///
    /// The request is already policy-resolved; the implementation applies the
    /// attached row filter and column drops and enforces `row_limit`. It must
    /// never widen access beyond what the request specifies.
    async fn run_sql(&self, request: &QueryRequest) -> Result<QueryOutcome, ExecutorError>;

    /// A short label naming the executor, for diagnostics and audit detail
    /// (e.g. `"datafusion"`, `"not-wired"`).
    fn label(&self) -> &'static str;
}

/// The wave-1 default executor: refuses every query with a clean "not wired"
/// message. Lets the full governance chain (auth, policy, budget, audit) run
/// and be tested now, before real execution lands.
#[derive(Debug, Clone, Copy, Default)]
pub struct NotWiredExecutor;

#[async_trait]
impl QueryExecutor for NotWiredExecutor {
    async fn run_sql(&self, _request: &QueryRequest) -> Result<QueryOutcome, ExecutorError> {
        Err(ExecutorError::NotWired(
            "the query executor is not wired into this build yet (wave 2); the query was \
             authorized and would have run"
                .to_owned(),
        ))
    }

    fn label(&self) -> &'static str {
        "not-wired"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn not_wired_executor_refuses_cleanly() {
        let exec = NotWiredExecutor;
        assert_eq!(exec.label(), "not-wired");
        let req = QueryRequest {
            sql: "SELECT 1".into(),
            warehouse: None,
            row_filter: None,
            dropped_columns: vec![],
            row_limit: 100,
            purpose: None,
        };
        let err = exec.run_sql(&req).await.unwrap_err();
        assert!(matches!(err, ExecutorError::NotWired(_)));
        assert!(err.to_string().contains("not wired"));
    }

    #[test]
    fn executor_trait_is_object_safe() {
        // Compiles only if QueryExecutor is dyn-compatible.
        let _boxed: Box<dyn QueryExecutor> = Box::new(NotWiredExecutor);
    }

    #[test]
    fn provenance_serializes() {
        let p = Provenance {
            table_ids: vec!["t1".into()],
            snapshot_ids: vec![42],
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["table_ids"][0], "t1");
        assert_eq!(v["snapshot_ids"][0], 42);
    }
}
