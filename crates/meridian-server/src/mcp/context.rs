//! Context tool handlers (Pillar H, H-F2): the governed *reads* an agent uses
//! to understand the lakehouse before (or instead of) querying it.
//!
//! Every handler here is governed. The headline is [`get_table_context`]: it
//! resolves the same RBAC + ABAC decision the scan planner uses
//! (`crate::governance::resolve_scan_policy`) and **removes masked/denied
//! columns from the returned schema** — they are *absent*, not nulled, so a
//! restricted column's very existence never leaks into an agent's prompt
//! (H-F2). A table the agent cannot read at all is refused (RBAC deny). The
//! rest of the context — owners, quality/trust score, freshness, contract
//! status — is assembled from the write-through index (no scans).
//!
//! The semantic-layer tools (`list_metrics` / `get_metric_definition` /
//! `list_data_products` / `get_glossary_term`) read the real semantics store
//! (Pillar G): certified metrics, data products, and glossary terms an agent can
//! reference. They are *context* reads (no query budget consumed); executing a
//! metric is separately governed via `query_metrics`.

use std::collections::BTreeSet;

use meridian_agents::protocol::CallToolResult;
use meridian_store::rbac::{Privilege, SecurableScope};
use meridian_store::{search, semantics, table, tenancy};
use serde_json::{Value, json};

use super::{GatewayCall, ToolResponse};
use crate::error::ApiError;
use crate::governance::{self, TableContext};
use crate::routes::grants::{namespace_scope_chain, require};
use crate::routes::namespaces::{decode_namespace_param, resolve_warehouse};

/// Routes a context tool call to its handler. An unknown name is impossible
/// (the catalog gated it) but handled defensively as a tool-error.
pub async fn handle(call: &GatewayCall<'_>, tool: &str, args: &Value) -> ToolResponse {
    let outcome = match tool {
        "search_assets" => search_assets(call, args).await,
        "get_table_context" => get_table_context(call, args).await,
        "get_lineage" => get_lineage(call, args).await,
        "list_metrics" => list_metrics(call, args).await,
        "get_metric_definition" => get_metric_definition(call, args).await,
        "list_data_products" => list_data_products(call, args).await,
        "get_glossary_term" => get_glossary_term(call, args).await,
        other => Err(ApiError::bad_request(format!(
            "unknown context tool {other:?}"
        ))),
    };
    match outcome {
        Ok(response) => response,
        // A governance denial (RBAC/ABAC) is a relayable tool-error, not a
        // protocol error: the agent asked for something it may not have.
        Err(api_error) => tool_error_from(&api_error),
    }
}

/// Maps an `ApiError` raised inside a context handler onto a tool-error
/// [`ToolResponse`]. A 403 becomes a `denied` ledger decision; anything else is
/// an `error`. The message is the client-safe one already on the `ApiError`.
fn tool_error_from(error: &ApiError) -> ToolResponse {
    use meridian_store::agent::ActivityDecision;
    let denied = error.status == axum::http::StatusCode::FORBIDDEN;
    ToolResponse {
        result: CallToolResult::error(error.message.clone()),
        decision: if denied {
            ActivityDecision::Denied
        } else {
            ActivityDecision::Error
        },
        rows: None,
        bytes: 0,
        cost_micros: 0,
        audit_detail: json!({ "error": error.message, "denied": denied }),
    }
}

// ---------------------------------------------------------------------------
// search_assets
// ---------------------------------------------------------------------------

/// `search_assets`: governed full-text search. Reuses the same visibility
/// resolution the management search API uses, so an agent sees only assets its
/// grants permit — nothing about restricted assets is returned.
async fn search_assets(call: &GatewayCall<'_>, args: &Value) -> Result<ToolResponse, ApiError> {
    let query = str_arg(args, "query")?;
    let limit = int_arg(args, "limit").unwrap_or(20).clamp(1, 100);

    let visibility = search::visibility_for(&call.state.pool, call.principal).await?;
    let page = search::search(
        &call.state.pool,
        tenancy::default_workspace_id(),
        &search::SearchRequest {
            text: &query,
            warehouse_id: None,
            namespace: None,
            kinds: None,
            limit,
            page_token: None,
        },
        &visibility,
    )
    .await?;

    let hits: Vec<Value> = page
        .hits
        .iter()
        .map(|h| {
            json!({
                "kind": h.kind.as_str(),
                "name": h.name,
                "namespace": h.namespace,
                "warehouse": h.warehouse,
                "score": h.rank,
            })
        })
        .collect();
    let count = hits.len();
    let structured = json!({ "results": hits });
    Ok(ToolResponse::context(
        CallToolResult::structured(format!("{count} asset(s) matched {query:?}"), structured),
        json!({ "query_digest": count, "returned": count }),
    ))
}

// ---------------------------------------------------------------------------
// get_table_context (the masked-column-absent guarantee)
// ---------------------------------------------------------------------------

/// `get_table_context`: the governed table briefing. RBAC READ gates access to
/// the table at all; ABAC then decides which columns the agent may see, and the
/// masked/denied ones are **removed from the returned schema** (H-F2). Owners,
/// trust score, freshness, and contract status come from the index.
// One coherent flow (resolve → RBAC → schema → ABAC → assemble); splitting it
// would scatter the single governance decision this tool exists to make.
#[allow(clippy::too_many_lines)]
async fn get_table_context(call: &GatewayCall<'_>, args: &Value) -> Result<ToolResponse, ApiError> {
    let warehouse = str_arg(args, "warehouse")?;
    let namespace = str_arg(args, "namespace")?;
    let table_name = str_arg(args, "table")?;
    let workspace_id = tenancy::default_workspace_id();

    // Resolve the table + its namespace chain.
    let wh = resolve_warehouse(&call.state.pool, &warehouse).await?;
    let levels = decode_namespace_param(&namespace)?;
    let record = table::get_by_name(&call.state.pool, &wh.id, &levels, &table_name)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                axum::http::StatusCode::NOT_FOUND,
                "NoSuchTableException",
                format!("table {warehouse}/{namespace}/{table_name} does not exist"),
            )
        })?;
    let chain = namespace_scope_chain(&call.state.pool, &wh.id, &levels).await?;

    // (RBAC) The agent must be able to READ the table, or the whole context is
    // refused — a denied caller learns nothing (not even the schema shape).
    require(
        &call.state.pool,
        call.principal,
        Privilege::Read,
        &SecurableScope::table(&wh.id, chain.clone(), Some(&record.id)),
    )
    .await?;

    // Load the current schema (the column universe).
    let schema = load_current_schema(&wh, &record).await?;

    // (ABAC) Resolve the row/column policy. A full deny refuses the context;
    // otherwise `removed_columns` names the columns to hide.
    let policy = governance::resolve_scan_policy(
        &call.state.pool,
        call.principal,
        &TableContext {
            table_id: &record.id,
            namespace_ids: &chain,
            schema: &schema,
            owner: None,
        },
        Some(&call.purpose),
    )
    .await?;

    if policy.denied {
        return Err(forbidden(format!(
            "access to {warehouse}/{namespace}/{table_name} is denied by policy: {}",
            policy.reason
        )));
    }

    // Build the governed column list: every top-level field EXCEPT the masked/
    // denied ones, which are ABSENT (not nulled).
    let removed: BTreeSet<&str> = policy.removed_columns.iter().map(String::as_str).collect();
    let columns: Vec<Value> = schema
        .fields
        .iter()
        .filter(|f| !removed.contains(f.name.as_str()))
        .map(|f| {
            json!({
                "name": f.name,
                "type": serde_json::to_value(&f.field_type).unwrap_or(Value::Null),
                "required": f.required,
                "doc": f.doc,
            })
        })
        .collect();

    // Trust score (E-F6) and contract status (E-F3) from the index.
    let trust = meridian_store::quality_score::score_table(
        &call.state.pool,
        workspace_id,
        &record.id,
        &chain,
    )
    .await?;
    let contracts = meridian_store::contracts::resolve_for_table(
        &call.state.pool,
        workspace_id,
        &record.id,
        &chain,
    )
    .await?;
    let freshness = latest_commit_time(&call.state.pool, &record.id).await?;

    let structured = json!({
        "identity": {
            "warehouse": warehouse,
            "namespace": levels,
            "table": table_name,
            "format_version": record.format_version,
        },
        "schema": { "columns": columns },
        "owners": table_owner_labels(&record),
        "documentation": record.properties.0.get("comment"),
        "trust": trust.to_json(),
        "freshness": {
            "latest_commit": freshness,
        },
        "contract": {
            "present": !contracts.is_empty(),
            "count": contracts.len(),
        },
        // Sample values are policy-gated and only offered where a sample policy
        // permits; none are included by default (H-F2: samples only if allowed).
        "sample_values": Value::Null,
    });

    let summary = format!(
        "{warehouse}/{namespace}/{table_name}: {} column(s) visible{}",
        columns.len(),
        if removed.is_empty() {
            String::new()
        } else {
            format!(" ({} hidden by policy)", removed.len())
        }
    );

    Ok(ToolResponse::context(
        CallToolResult::structured(summary, structured),
        json!({
            "table_id": record.id,
            "columns_returned": columns.len(),
            "columns_removed": policy.removed_columns,
            "row_filter_applied": policy.row_filter.is_some(),
            "applied_policies": policy.applied_policies,
            "reason": policy.reason,
        }),
    ))
}

// ---------------------------------------------------------------------------
// get_lineage
// ---------------------------------------------------------------------------

/// `get_lineage`: the up/downstream graph around a table the agent can read.
async fn get_lineage(call: &GatewayCall<'_>, args: &Value) -> Result<ToolResponse, ApiError> {
    let warehouse = str_arg(args, "warehouse")?;
    let namespace = str_arg(args, "namespace")?;
    let table_name = str_arg(args, "table")?;
    let direction = optional_str_arg(args, "direction");
    let depth = u32::try_from(int_arg(args, "depth").unwrap_or(2).clamp(1, 5)).unwrap_or(2);

    let wh = resolve_warehouse(&call.state.pool, &warehouse).await?;
    let levels = decode_namespace_param(&namespace)?;
    let record = table::get_by_name(&call.state.pool, &wh.id, &levels, &table_name)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                axum::http::StatusCode::NOT_FOUND,
                "NoSuchTableException",
                format!("table {warehouse}/{namespace}/{table_name} does not exist"),
            )
        })?;
    let chain = namespace_scope_chain(&call.state.pool, &wh.id, &levels).await?;
    require(
        &call.state.pool,
        call.principal,
        Privilege::Read,
        &SecurableScope::table(&wh.id, chain, Some(&record.id)),
    )
    .await?;

    let graph = meridian_lineage::impact::lineage_graph(
        &call.state.pool,
        tenancy::default_workspace_id(),
        &record.id,
        meridian_lineage::impact::Direction::parse(direction.as_deref()),
        depth,
    )
    .await?;

    let structured = serde_json::to_value(&graph).unwrap_or(Value::Null);
    let node_count = graph.nodes.len();
    Ok(ToolResponse::context(
        CallToolResult::structured(
            format!("lineage for {table_name}: {node_count} node(s)"),
            structured,
        ),
        json!({ "table_id": record.id, "nodes": node_count }),
    ))
}

// ---------------------------------------------------------------------------
// Semantic-layer tools (Pillar G served to agents, H-F2)
// ---------------------------------------------------------------------------
//
// These read the real semantics store (metrics, glossary, data products). They
// are *context* reads, so they consume no query budget. Certification status is
// surfaced verbatim so an agent can prefer certified definitions — the semantic
// layer is precisely what makes agent answers accurate (§2.5). To run a metric
// (not just read it), an agent uses the governed `query_metrics` tool, which
// compiles the definition and applies policy.

/// `list_metrics`: the certified/known metrics an agent can reference. Read-only
/// context; the definitions are workspace metadata, not row data, so no ABAC
/// filtering applies here (running a metric is separately governed).
async fn list_metrics(call: &GatewayCall<'_>, args: &Value) -> Result<ToolResponse, ApiError> {
    let limit = int_arg(args, "limit").unwrap_or(100).clamp(1, 500);
    let records = semantics::list_metrics(
        &call.state.pool,
        tenancy::default_workspace_id(),
        None,
        Some(limit),
    )
    .await?;
    let items: Vec<Value> = records
        .iter()
        .map(|m| {
            json!({
                "name": m.name,
                "display_name": m.display_name,
                "source": m.source,
                "grain": m.grain,
                "description": m.description,
                "certification": m.certification,
            })
        })
        .collect();
    let count = items.len();
    Ok(ToolResponse::context(
        CallToolResult::structured(
            format!("{count} metric(s) defined"),
            json!({ "metrics": items }),
        ),
        json!({ "returned": count }),
    ))
}

/// `get_metric_definition`: the full definition of one metric by name — measure,
/// dimensions, filters, grain, owner, certification. This is the ~100%-accuracy
/// path's source of truth: an agent reads the definition here, then calls
/// `query_metrics` to execute it under policy.
async fn get_metric_definition(
    call: &GatewayCall<'_>,
    args: &Value,
) -> Result<ToolResponse, ApiError> {
    let name = str_arg(args, "name")?;
    let Some(metric) =
        semantics::get_metric_by_name(&call.state.pool, tenancy::default_workspace_id(), &name)
            .await?
    else {
        return Ok(ToolResponse::context(
            CallToolResult::structured(
                format!("metric {name:?} is not defined"),
                json!({ "metric": name, "status": "not_found" }),
            ),
            json!({ "metric": name, "status": "not_found" }),
        ));
    };
    let structured = json!({
        "name": metric.name,
        "display_name": metric.display_name,
        "source": metric.source,
        "expression": metric.expression,
        "dialect": metric.dialect,
        "dimensions": metric.dimensions.0,
        "filters": metric.filters.0,
        "grain": metric.grain,
        "description": metric.description,
        "owner": metric.owner,
        "certification": metric.certification,
    });
    Ok(ToolResponse::context(
        CallToolResult::structured(
            format!(
                "metric {:?} ({}): {}",
                metric.name, metric.certification, metric.expression
            ),
            structured,
        ),
        json!({ "metric": metric.name, "certification": metric.certification }),
    ))
}

/// `list_data_products`: the certified data products (named bundles) an agent
/// can consume. Products are the unit of consumption for agents (G-F4).
async fn list_data_products(
    call: &GatewayCall<'_>,
    args: &Value,
) -> Result<ToolResponse, ApiError> {
    let limit = int_arg(args, "limit").unwrap_or(100).clamp(1, 500);
    let records = semantics::list_products(
        &call.state.pool,
        tenancy::default_workspace_id(),
        None,
        Some(limit),
    )
    .await?;
    let items: Vec<Value> = records
        .iter()
        .map(|p| {
            json!({
                "name": p.name,
                "display_name": p.display_name,
                "description": p.description,
                "owner": p.owner,
                "sla": p.sla,
                "certification": p.certification,
            })
        })
        .collect();
    let count = items.len();
    Ok(ToolResponse::context(
        CallToolResult::structured(
            format!("{count} data product(s) defined"),
            json!({ "data_products": items }),
        ),
        json!({ "returned": count }),
    ))
}

/// `get_glossary_term`: the definition of one business term by name, plus its
/// steward and certification. Lets an agent resolve business vocabulary to a
/// precise, stewarded meaning (G-F3).
async fn get_glossary_term(call: &GatewayCall<'_>, args: &Value) -> Result<ToolResponse, ApiError> {
    let term = str_arg(args, "term")?;
    let Some(record) =
        semantics::get_term_by_name(&call.state.pool, tenancy::default_workspace_id(), &term)
            .await?
    else {
        return Ok(ToolResponse::context(
            CallToolResult::structured(
                format!("glossary term {term:?} is not defined"),
                json!({ "term": term, "status": "not_found" }),
            ),
            json!({ "term": term, "status": "not_found" }),
        ));
    };
    let structured = json!({
        "name": record.name,
        "definition": record.definition,
        "steward": record.steward,
        "certification": record.certification,
    });
    Ok(ToolResponse::context(
        CallToolResult::structured(
            format!("{}: {}", record.name, record.definition),
            structured,
        ),
        json!({ "term": record.name, "certification": record.certification }),
    ))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Loads a table's current schema from storage (the column universe for the
/// governance decision). Mirrors the governance route's `load_table_schema`.
async fn load_current_schema(
    wh: &meridian_store::warehouse::WarehouseRecord,
    record: &table::TableRecord,
) -> Result<meridian_iceberg::spec::Schema, ApiError> {
    let Some(metadata_location) = record.metadata_location.clone() else {
        return Err(ApiError::new(
            axum::http::StatusCode::NOT_FOUND,
            "NoSuchTableException",
            "table has no metadata".to_owned(),
        ));
    };
    let storage = crate::routes::tables::connect_storage(wh)?;
    let metadata = meridian_storage::read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .map_err(|e| {
            ApiError::new(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("table metadata is unreadable: {e}"),
            )
        })?;
    metadata
        .schemas
        .iter()
        .find(|s| s.schema_id == Some(metadata.current_schema_id))
        .cloned()
        .ok_or_else(|| {
            ApiError::new(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                "current schema missing from table metadata".to_owned(),
            )
        })
}

/// The timestamp of the table's current snapshot (freshness), from the
/// write-through snapshot index. `None` for a table with no snapshots.
async fn latest_commit_time(
    pool: &sqlx::PgPool,
    table_id: &str,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, ApiError> {
    let ms: Option<i64> = sqlx::query_scalar(
        "SELECT timestamp_ms FROM table_snapshots
         WHERE table_id = $1
         ORDER BY is_current DESC, timestamp_ms DESC
         LIMIT 1",
    )
    .bind(table_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        ApiError::from(meridian_store::map_sqlx_error(
            "failed to read freshness",
            e,
        ))
    })?;
    Ok(ms.and_then(chrono::DateTime::from_timestamp_millis))
}

/// The owner labels for a table from its properties (`owner`), if present.
fn table_owner_labels(record: &table::TableRecord) -> Value {
    match record.properties.0.get("owner") {
        Some(owner) => json!([owner]),
        None => json!([]),
    }
}

// --- argument extraction ---

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

fn forbidden(message: impl Into<String>) -> ApiError {
    ApiError::new(
        axum::http::StatusCode::FORBIDDEN,
        "NotAuthorizedException",
        message,
    )
}
