//! The wired `DataFusion` query executor for the agent gateway seam
//! (`meridian_agents::executor::QueryExecutor`).
//!
//! # Why this is thin
//!
//! The gateway's [`QueryExecutor`] trait models one call as a single
//! `QueryRequest` carrying *one* table's resolved policy (a row filter + a drop
//! list). That shape predates the multi-table reality of `run_sql`: a real query
//! can join several tables, each with its own row/column policy, and the policy
//! for a table can only be resolved once the table it refers to is known — which
//! requires parsing the SQL and hitting the store (the principal, the tags, the
//! policies). That resolution lives in [`crate::mcp::engine`], which has the
//! `AppState` and the `Principal`; the query handlers call it directly.
//!
//! So this type's job is narrow and honest: it is the **wired marker** for the
//! seam — its [`label`](QueryExecutor::label) reports `"datafusion"` so the audit
//! trail records the real engine, and its presence in place of
//! `NotWiredExecutor` is what makes the gateway's query path "wired". Its
//! `run_sql` handles only the degenerate, table-free request (`SELECT 1`,
//! `SELECT now()`), which needs no catalog resolution or policy; any request that
//! references catalog tables is routed through [`crate::mcp::engine`] by the
//! handler, not here, because only there is the principal available to govern it.

use async_trait::async_trait;
use meridian_agents::executor::{
    ExecutorError, Provenance, QueryExecutor, QueryOutcome, QueryRequest,
};
use meridian_query::{Caps, GovernedTable};

/// The wired `DataFusion` executor (the seam marker). Governed, multi-table
/// execution goes through [`crate::mcp::engine`]; this covers only table-free
/// requests and reports the engine label for the audit trail.
#[derive(Debug, Clone, Copy, Default)]
pub struct DataFusionExecutor;

#[async_trait]
impl QueryExecutor for DataFusionExecutor {
    async fn run_sql(&self, request: &QueryRequest) -> Result<QueryOutcome, ExecutorError> {
        // The gateway's single-table `QueryRequest` cannot express per-table
        // policy for a multi-table query, and this seam has no principal to
        // resolve policy with. A table-free query (no catalog tables) needs
        // neither, so run it directly; anything referencing a table must go
        // through the principal-aware engine path (the handler does that).
        let refs = meridian_query::referenced_tables(&request.sql)
            .map_err(|e| ExecutorError::Rejected(e.to_string()))?;
        if !refs.is_empty() {
            return Err(ExecutorError::Failed(
                "this query references catalog tables and must be run through the governed \
                 engine path (which resolves per-table policy for the calling principal); it \
                 was not routed there"
                    .to_owned(),
            ));
        }

        let caps = Caps {
            max_result_rows: usize::try_from(request.row_limit.max(0)).unwrap_or(usize::MAX),
            ..Caps::default()
        };
        let no_tables: [GovernedTable<'_>; 0] = [];
        let output = meridian_query::run(&request.sql, &no_tables, caps)
            .await
            .map_err(|e| {
                if e.is_caller_refusal() {
                    ExecutorError::Rejected(e.to_string())
                } else {
                    ExecutorError::Failed(e.to_string())
                }
            })?;

        Ok(QueryOutcome {
            columns: output.columns.iter().map(|c| c.name.clone()).collect(),
            rows: output.rows_as_arrays(),
            provenance: Provenance::empty(),
            bytes_scanned: i64::try_from(output.bytes_scanned).unwrap_or(i64::MAX),
            cost_micros: 0,
        })
    }

    fn label(&self) -> &'static str {
        "datafusion"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn table_free_query_runs() {
        let exec = DataFusionExecutor;
        assert_eq!(exec.label(), "datafusion");
        let req = QueryRequest {
            sql: "SELECT 1 AS one".into(),
            warehouse: None,
            row_filter: None,
            dropped_columns: vec![],
            row_limit: 100,
            purpose: None,
        };
        let out = exec.run_sql(&req).await.expect("table-free query runs");
        assert_eq!(out.columns, vec!["one".to_owned()]);
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.provenance, Provenance::empty());
    }

    #[tokio::test]
    async fn table_referencing_query_is_routed_elsewhere() {
        let exec = DataFusionExecutor;
        let req = QueryRequest {
            sql: "SELECT * FROM sales".into(),
            warehouse: None,
            row_filter: None,
            dropped_columns: vec![],
            row_limit: 100,
            purpose: None,
        };
        let err = exec
            .run_sql(&req)
            .await
            .expect_err("must not run un-governed");
        assert!(matches!(err, ExecutorError::Failed(_)));
    }
}
