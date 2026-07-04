//! Query tool handlers (Pillar H, H-F3): governed *execution*. Each tool runs
//! the same governance the context reads do, then adds the budget gate (H-F4)
//! and runs the query on the built-in small-scan executor via
//! [`crate::mcp::engine`] — the shared engine behind `run_sql` and the workbench.
//!
//! # The governed-execution path (validate → plan → price → budget → run)
//!
//! 1. **Plan.** [`engine::plan`] enumerates the tables the SQL references (with
//!    the executor's own parser, so the set matches what it binds), resolves each
//!    to its metadata + storage + the RBAC-checked, ABAC-resolved policy for the
//!    calling agent (masks folded to **drops** — H-F2, a restricted column is
//!    absent), and prices the scan from manifest stats **without reading data**.
//!    An ABAC deny short-circuits here as a relayable refusal, before any budget
//!    is spent.
//! 2. **Budget (fail before you charge).** The estimated scanned bytes + a
//!    dollar estimate are checked against the agent's budget *before* execution
//!    (H-F3/H-F4). Over budget → a graceful, relayable refusal, audited
//!    `refused_budget`, nothing consumed and nothing run. Within budget → the
//!    estimate is consumed atomically with one query.
//! 3. **Run.** The executor runs the governed query (small-scan only; an
//!    oversized scan was already priced and would have been refused). Every
//!    result carries **provenance** — the tables + snapshot ids read and the
//!    policies applied — mapped to Meridian internal table ids so the agent can
//!    cite (H-F3) and the CISO audit can answer "which agent read which columns
//!    under which policy" (H-F4).
//!
//! `query_metrics` is the deterministic compiled-SQL path (the high-accuracy
//! path for covered questions); until the semantic layer is populated it returns
//! an honest "not yet populated" answer, governed and audited like the others.

use std::collections::BTreeMap;

use meridian_agents::protocol::CallToolResult;
use meridian_query::{Caps, QueryError, QueryOutput};
use meridian_store::agent::{self, ActivityDecision, BudgetOutcome, QueryCost};
use serde_json::{Value, json};

use super::engine::{self, MaskMode, PlanError, PlanOutcome, QueryScope};
use super::{GatewayCall, ToolResponse};
use crate::error::ApiError;
use crate::routes::namespaces::decode_namespace_param;

/// Micro-dollars charged per gibibyte scanned, for the pre-execution dollar
/// estimate that the budget's dollar cap is checked against. A coarse,
/// documented rate — the estimate exists to make the `$-estimate/day` cap bite
/// (H-F4), not to bill; the exact number is a placeholder pending the cost model
/// (Pillar C savings ledger). At ~$5/TiB this is `5_000_000 / 1024` per GiB.
const MICROS_PER_GIB_SCANNED: i64 = 4883;

/// A hard cap on result rows returned to an agent, independent of the scan cap:
/// an agent's context window should not be flooded even by a small scan.
const AGENT_RESULT_ROW_CAP: usize = 1_000;

/// Routes a query tool call to its handler.
pub async fn handle(call: &GatewayCall<'_>, tool: &str, args: &Value) -> ToolResponse {
    let outcome = match tool {
        "run_sql" => run_sql(call, args).await,
        "query_metrics" => query_metrics(call, args).await,
        "preview_table" => preview_table(call, args).await,
        other => Err(ApiError::bad_request(format!(
            "unknown query tool {other:?}"
        ))),
    };
    match outcome {
        Ok(response) => response,
        Err(api_error) => {
            let denied = api_error.status == axum::http::StatusCode::FORBIDDEN;
            ToolResponse {
                result: CallToolResult::error(api_error.message.clone()),
                decision: if denied {
                    ActivityDecision::Denied
                } else {
                    ActivityDecision::Error
                },
                rows: None,
                bytes: 0,
                cost_micros: 0,
                audit_detail: json!({ "error": api_error.message, "denied": denied }),
            }
        }
    }
}

/// The shared governed-query path: plan → budget → run.
///
/// `warehouse` and `default_namespace` scope table resolution; `sql` is the
/// query to run. Returns the [`ToolResponse`] with the right decision, touched-
/// resource counts, provenance, and audit detail for every branch (policy deny,
/// bad/oversized SQL, budget refusal, success).
async fn run_governed_query(
    call: &GatewayCall<'_>,
    tool: &str,
    sql: &str,
    warehouse: &str,
    default_namespace: Option<&[String]>,
) -> Result<ToolResponse, ApiError> {
    let scope = QueryScope {
        warehouse,
        default_namespace,
    };

    // (1) Plan + govern + price (no execution, no budget spend yet).
    let planned = match engine::plan(
        call.state,
        call.principal,
        sql,
        &scope,
        Some(&call.purpose),
        MaskMode::Drop,
    )
    .await
    {
        Ok(PlanOutcome::Planned(planned)) => planned,
        Ok(PlanOutcome::Denied {
            table,
            reason,
            applied_policies,
        }) => {
            // ABAC denied a referenced table: a relayable policy denial, audited
            // as `denied`, no budget spent.
            let refusal = meridian_agents::decision::RefusalReason::PolicyDenied {
                reason: format!("access to {table} is denied: {reason}"),
            };
            let mut response = ToolResponse::refused(&refusal);
            response.audit_detail = json!({
                "tool": tool,
                "denied_table": table,
                "reason": reason,
                "applied_policies": applied_policies,
            });
            return Ok(response);
        }
        // The executor rejected the SQL (bad syntax, oversized) — a relayable
        // tool-error the agent can fix or re-ask against a big engine.
        Err(PlanError::Executor(err)) => return Ok(query_error_response(tool, &err)),
        // A resolution/RBAC fault: surface via the handler's ApiError mapping (a
        // 403 becomes a `denied` decision; NOT_FOUND/others an `error`).
        Err(PlanError::Resolve(api_error)) => return Err(*api_error),
    };

    // (2) Budget gate (fail before you charge): price the estimate and check it
    // against the agent's budget. A refusal is graceful, audited, unconsumed.
    let cost = QueryCost {
        bytes: i64::try_from(planned.estimate.bytes).unwrap_or(i64::MAX),
        cost_micros: estimate_cost_micros(planned.estimate.bytes),
    };
    let outcome =
        agent::check_and_consume_budget(&call.state.pool, &call.agent.principal_id, cost).await?;
    if let BudgetOutcome::Refused {
        dimension,
        limit,
        used,
        requested,
    } = outcome
    {
        let reason = meridian_agents::decision::RefusalReason::Budget {
            dimension: dimension.as_str().to_owned(),
            limit,
            used,
            requested,
        };
        return Ok(ToolResponse::refused(&reason));
    }

    // (3) Execute. The scan cap is set at least as large as the (already
    // budget-checked) estimate so a well-priced query is not double-refused;
    // the result-row cap protects the agent's context window.
    let caps = Caps {
        max_scan_bytes: planned
            .estimate
            .bytes
            .max(meridian_query::DEFAULT_MAX_SCAN_BYTES),
        max_scan_rows: planned
            .estimate
            .rows
            .max(meridian_query::DEFAULT_MAX_SCAN_ROWS),
        max_result_rows: AGENT_RESULT_ROW_CAP,
    };

    match planned.execute(caps).await {
        Ok((output, table_ids)) => Ok(success_response(
            tool,
            output,
            &table_ids,
            cost.cost_micros,
            call.executor.label(),
        )),
        Err(err) => Ok(query_error_response(tool, &err)),
    }
}

/// Builds the success [`ToolResponse`]: the structured rows + provenance, the
/// touched-resource counts, and the audit detail (H-F3/H-F4). `executor_label`
/// is the wired executor's label (from the `QueryExecutor` seam), recorded so
/// the audit names the engine that ran the query.
// Takes `output` by value: its `rows`/`columns` are moved into the structured
// result rather than cloned (a large result should not be duplicated).
#[allow(clippy::needless_pass_by_value)]
fn success_response(
    tool: &str,
    output: QueryOutput,
    table_ids: &BTreeMap<String, String>,
    cost_micros: i64,
    executor_label: &str,
) -> ToolResponse {
    let provenance = engine::provenance_json(&output, table_ids);
    let row_count = output.rows.len();
    let structured = json!({
        "columns": output.columns,
        "rows": output.rows,
        "row_count": row_count,
        "truncated": output.truncated,
        "provenance": provenance,
    });
    let bytes = i64::try_from(output.bytes_scanned).unwrap_or(i64::MAX);
    ToolResponse {
        result: CallToolResult::structured(
            format!(
                "{row_count} row(s) returned{}",
                if output.truncated { " (truncated)" } else { "" }
            ),
            structured,
        ),
        decision: ActivityDecision::Allowed,
        rows: Some(i64::try_from(row_count).unwrap_or(i64::MAX)),
        bytes,
        cost_micros,
        audit_detail: json!({
            "tool": tool,
            "executor": executor_label,
            "provenance": provenance,
            "bytes_scanned": bytes,
            "truncated": output.truncated,
        }),
    }
}

/// Maps a [`QueryError`] to a tool-error [`ToolResponse`]. A caller-facing
/// refusal (bad/oversized SQL, unknown table) is relayed verbatim so the agent
/// can fix or re-ask against a big engine; an operational fault surfaces as a
/// generic error. Either way the decision is `Error` (not a policy denial).
fn query_error_response(tool: &str, err: &QueryError) -> ToolResponse {
    ToolResponse {
        result: CallToolResult::error(err.to_string()),
        decision: ActivityDecision::Error,
        rows: None,
        bytes: 0,
        cost_micros: 0,
        audit_detail: json!({
            "tool": tool,
            "error": err.to_string(),
            "caller_refusal": err.is_caller_refusal(),
        }),
    }
}

/// The dollar estimate (micro-dollars) for a scan of `bytes`, for the budget's
/// dollar cap. Ceil-divides so any non-zero scan costs at least a micro-dollar.
fn estimate_cost_micros(bytes: u64) -> i64 {
    const GIB: u128 = 1024 * 1024 * 1024;
    let micros =
        (u128::from(bytes) * u128::try_from(MICROS_PER_GIB_SCANNED).unwrap_or(0)).div_ceil(GIB);
    i64::try_from(micros).unwrap_or(i64::MAX)
}

// ---------------------------------------------------------------------------
// run_sql
// ---------------------------------------------------------------------------

async fn run_sql(call: &GatewayCall<'_>, args: &Value) -> Result<ToolResponse, ApiError> {
    let sql = str_arg(args, "sql")?;
    // The warehouse scopes table resolution. It is optional so a table-free
    // query (`SELECT 1`, `SELECT now()`) works with no warehouse; when the SQL
    // *does* reference a table, resolution needs the warehouse and a query
    // without one is refused there (an unknown/unqualified table error the agent
    // can act on). A bare table name has no default namespace here — an agent
    // qualifies its tables (`namespace.table`).
    let warehouse = optional_str_arg(args, "warehouse").unwrap_or_default();
    run_governed_query(call, "run_sql", &sql, &warehouse, None).await
}

// ---------------------------------------------------------------------------
// query_metrics
// ---------------------------------------------------------------------------

// `async` for the uniform dispatch signature; it becomes genuinely async when
// the semantic layer (Pillar G) is wired and metric queries compile to SQL.
#[allow(clippy::unused_async)]
async fn query_metrics(call: &GatewayCall<'_>, args: &Value) -> Result<ToolResponse, ApiError> {
    let metric = str_arg(args, "metric")?;
    let _ = call;
    // The deterministic compiled-SQL path (high-accuracy) needs the semantic
    // layer's metric definitions (Pillar G, another wave). Until those exist,
    // answer honestly — governed and audited (as an allowed context-shaped
    // answer), without inventing a metric or charging a scan.
    Ok(ToolResponse {
        result: CallToolResult::error(format!(
            "metric {metric:?} cannot be queried yet: the semantic layer (metric definitions) is \
             not populated in this deployment. Use run_sql against governed tables instead."
        )),
        decision: ActivityDecision::Error,
        rows: None,
        bytes: 0,
        cost_micros: 0,
        audit_detail: json!({
            "tool": "query_metrics",
            "metric": metric,
            "status": "semantic_layer_not_populated",
        }),
    })
}

// ---------------------------------------------------------------------------
// preview_table
// ---------------------------------------------------------------------------

async fn preview_table(call: &GatewayCall<'_>, args: &Value) -> Result<ToolResponse, ApiError> {
    let warehouse = str_arg(args, "warehouse")?;
    let namespace = str_arg(args, "namespace")?;
    let table_name = str_arg(args, "table")?;
    let limit = int_arg(args, "limit").unwrap_or(20).clamp(1, 100);
    // Resolve the namespace levels the same way the IRC/context routes do, then
    // build a fully-qualified reference so the executor binds exactly this
    // table (a bare name could be shadowed by the default-namespace rule).
    let levels = decode_namespace_param(&namespace)?;
    let qualified = format!(
        "{}.{}",
        levels
            .iter()
            .map(|l| quote_ident(l))
            .collect::<Vec<_>>()
            .join("."),
        quote_ident(&table_name)
    );
    let sql = format!("SELECT * FROM {qualified} LIMIT {limit}");
    run_governed_query(call, "preview_table", &sql, &warehouse, Some(&levels)).await
}

/// Quotes a SQL identifier (double-quoted, embedded quotes doubled) so a
/// namespace/table name with reserved words or odd characters binds safely.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

// --- argument extraction (shared shapes with context.rs, kept local to avoid
// a cross-module pub surface for tiny helpers) ---

fn str_arg(args: &Value, key: &str) -> Result<String, ApiError> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| ApiError::bad_request(format!("missing or empty string argument {key:?}")))
}

fn optional_str_arg(args: &Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn int_arg(args: &Value, key: &str) -> Option<i64> {
    args.get(key).and_then(Value::as_i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_estimate_is_ceiled_and_monotonic() {
        assert_eq!(estimate_cost_micros(0), 0);
        // Any non-zero scan costs at least one micro-dollar.
        assert_eq!(estimate_cost_micros(1), 1);
        // One GiB is the per-GiB rate.
        assert_eq!(
            estimate_cost_micros(1024 * 1024 * 1024),
            MICROS_PER_GIB_SCANNED
        );
        // Monotonic.
        assert!(estimate_cost_micros(2 * 1024 * 1024 * 1024) > estimate_cost_micros(1024 * 1024));
    }

    #[test]
    fn quote_ident_escapes_quotes() {
        assert_eq!(quote_ident("sales"), "\"sales\"");
        assert_eq!(quote_ident("od\"d"), "\"od\"\"d\"");
    }
}
