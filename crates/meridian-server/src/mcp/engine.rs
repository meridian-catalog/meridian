//! The governed small-scan query engine seam (Pillar H `run_sql` H-F3, Pillar L
//! workbench L-F1): the server-side glue that turns a SQL string + a principal
//! into a governed execution against `meridian_query::run`.
//!
//! `meridian_query::run` is a pure function of *(metadata, bytes, policy, SQL,
//! caps)*: it resolves no names, loads no metadata, and reads no policy (ADR
//! 010). This module is the caller the ADR names. It splits the work into two
//! phases so the caller can price a query against a budget *before* it executes
//! (H-F3: cost-estimated before execution).
//!
//! - [`plan`] **enumerates** the catalog tables the SQL references (using the
//!   executor's own parser so the set matches exactly what it will bind),
//!   **resolves** each to its warehouse / namespace chain / `TableMetadata` /
//!   `Storage` handle, checks **RBAC READ** on each, **resolves policy** for
//!   each `(principal, table)` via `crate::governance::resolve_query_enforcement`
//!   (the same Pillar-D decision the scan planner uses — folding masks to drops
//!   for agents, H-F2), and computes the **metadata-only cost estimate**. It
//!   returns a [`PlannedQuery`] the caller budget-checks.
//! - [`PlannedQuery::execute`] runs the governed query and maps provenance
//!   (registered names + snapshot ids) back to Meridian internal table ids so the
//!   caller can audit and the agent can cite (H-F3).
//!
//! An ABAC **deny** on any referenced table surfaces from [`plan`] as
//! [`PlanOutcome::Denied`], which the caller renders as a graceful, relayable
//! refusal — before any budget is spent.
//!
//! Both surfaces share this one path so `run_sql` and the workbench cannot
//! enforce policy differently. The caller (the MCP query handler / the workbench
//! route) owns the budget gate and the audit write; this module owns resolution,
//! estimation, and governed execution.

use std::collections::BTreeMap;
use std::sync::Arc;

use meridian_authz::{ColumnMask, Enforcement, MaskKind};
use meridian_common::principal::Principal;
use meridian_iceberg::spec::{Schema, TableMetadata};
use meridian_query::{
    Caps, CatalogTable, GovernedTable, QueryError, QueryOutput, ScanEstimate, TableRef,
};
use meridian_storage::Storage;

use crate::AppState;
use crate::error::ApiError;
use crate::governance::{self, TableContext};
use crate::routes::grants::{namespace_scope_chain, require};
use crate::routes::namespaces::resolve_warehouse;
use crate::routes::tables::connect_storage;
use meridian_store::rbac::{Privilege, SecurableScope};
use meridian_store::{table, warehouse};

/// How column masks are applied by the query, per surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaskMode {
    /// Agent path (H-F2): every masked column is **dropped** — absent from
    /// results and from the result schema — so a restricted column cannot leak
    /// into an agent's prompt, matching the scan-plan fold.
    Drop,
    /// Workbench path (L-F1): value-preserving masks (hash, partial, null) are
    /// rendered in SQL as themselves; only a `Drop`/`Custom` mask removes the
    /// column. A human at the workbench sees masked *values*, not absence.
    Preserve,
}

/// Where a **bare** (unqualified) table name in the SQL resolves.
#[derive(Debug, Clone)]
pub struct QueryScope<'a> {
    /// The warehouse every referenced table must live in.
    pub warehouse: &'a str,
    /// The namespace a bare table name resolves in (a qualified `ns.table` uses
    /// `ns`). `None` means bare names are unresolvable and are refused.
    pub default_namespace: Option<&'a [String]>,
}

/// A resolved, governed, priced query ready to execute (or to refuse on budget).
#[derive(Debug)]
pub struct PlannedQuery {
    /// The validated SQL to run.
    sql: String,
    /// The resolved tables (owning their metadata + storage + enforcement).
    tables: Vec<ResolvedTable>,
    /// The metadata-only scan estimate (bytes/rows/files), for the budget gate.
    pub estimate: ScanEstimate,
    /// Ids of every policy that contributed a filter/mask, sorted & deduped.
    pub applied_policies: Vec<String>,
}

impl PlannedQuery {
    /// Executes the planned query with the given caps and returns the output
    /// plus the registered-name → Meridian internal table id map (for
    /// provenance/audit).
    ///
    /// `caps` should carry the caller's result-row cap and a scan cap at least
    /// as large as [`Self::estimate`]'s bytes (the caller already budget-checked
    /// the estimate); the executor still enforces the cap defensively.
    pub async fn execute(
        &self,
        caps: Caps,
    ) -> Result<(QueryOutput, BTreeMap<String, String>), QueryError> {
        let governed: Vec<GovernedTable<'_>> = self
            .tables
            .iter()
            .map(|rt| GovernedTable {
                table: CatalogTable {
                    name: rt.query_name.clone(),
                    metadata: &rt.metadata,
                    storage: rt.storage.as_ref(),
                },
                enforcement: rt.enforcement.clone(),
            })
            .collect();

        let output = meridian_query::run(&self.sql, &governed, caps).await?;

        let table_ids: BTreeMap<String, String> = self
            .tables
            .iter()
            .map(|rt| (rt.query_name.clone(), rt.internal_id.clone()))
            .collect();
        Ok((output, table_ids))
    }
}

/// The result of planning a governed query.
#[derive(Debug)]
pub enum PlanOutcome {
    /// Resolved and permitted; ready to budget-check and execute.
    Planned(PlannedQuery),
    /// ABAC denied one of the referenced tables. The whole query is refused;
    /// the caller renders a graceful, relayable denial.
    Denied {
        /// The table (as referenced) that was denied.
        table: String,
        /// The policy engine's reason.
        reason: String,
        /// The policies that produced the deny, for the audit record.
        applied_policies: Vec<String>,
    },
}

/// Why planning failed (distinct from an ABAC deny, which is a [`PlanOutcome`]).
///
/// The two arms have different audiences and renderings: an [`Executor`] error
/// is the executor's own verdict on the SQL (a syntax error, an oversized scan)
/// — relayable to an agent verbatim; a [`Resolve`] error is a server-side or
/// RBAC fault resolving a referenced table (unknown table, unreadable metadata,
/// a missing READ grant).
///
/// [`Executor`]: PlanError::Executor
/// [`Resolve`]: PlanError::Resolve
#[derive(Debug)]
pub enum PlanError {
    /// The executor rejected or could not run the SQL (parse failure, oversized
    /// scan, unknown column). Relayable to an agent.
    Executor(QueryError),
    /// Resolving a referenced table failed (unknown table, unreadable metadata,
    /// RBAC denial). Carries the server error with its status + message (boxed —
    /// [`ApiError`] is much larger than [`QueryError`]).
    Resolve(Box<ApiError>),
}

impl From<QueryError> for PlanError {
    fn from(e: QueryError) -> Self {
        Self::Executor(e)
    }
}

impl From<ApiError> for PlanError {
    fn from(e: ApiError) -> Self {
        Self::Resolve(Box::new(e))
    }
}

/// The provenance payload a caller returns to an agent (to cite, H-F3) or a
/// workbench user, and that a CISO audit reads (H-F4): every table + snapshot
/// read (with its Meridian internal id) and the row-filter/column-mask policies
/// applied. `table_ids` maps the executor's registered (query) names to Meridian
/// internal table ids.
#[must_use]
pub fn provenance_json(
    output: &QueryOutput,
    table_ids: &BTreeMap<String, String>,
) -> serde_json::Value {
    use serde_json::json;
    let tables: Vec<serde_json::Value> = output
        .provenance
        .tables
        .iter()
        .map(|t| {
            json!({
                "name": t.table,
                "table_id": table_ids.get(&t.table),
                "table_uuid": t.table_uuid,
                "snapshot_id": t.snapshot_id,
            })
        })
        .collect();
    json!({
        "tables": tables,
        "row_filter_policies": output.provenance.row_filter_policies,
        "column_mask_policies": output.provenance.column_mask_policies,
        "masked_columns": output.provenance.masked_columns,
    })
}

/// A resolved table ready to hand to the executor, owning its metadata and
/// storage so the borrows `meridian_query::run` needs outlive the call.
#[derive(Debug)]
struct ResolvedTable {
    /// The name the SQL references this table by (bare or qualified).
    query_name: String,
    /// Meridian internal table id (for provenance/audit).
    internal_id: String,
    /// The table's metadata (its current snapshot is what the executor reads).
    metadata: TableMetadata,
    /// The warehouse storage handle.
    storage: Arc<dyn Storage>,
    /// The row filters + column masks resolved for the principal.
    enforcement: Enforcement,
}

/// Plans a governed small-scan query for `principal`: resolves + governs the
/// referenced tables and prices the scan, **without executing**.
///
/// Returns [`PlanOutcome::Denied`] if ABAC denies any referenced table, or a
/// [`PlanError`] for a caller-facing executor refusal (bad SQL, oversized scan)
/// or a resolution/RBAC fault — the caller maps each to the right tool-error /
/// HTTP shape. It does **not** enforce the scan cap (execution does); the caller
/// checks [`PlannedQuery::estimate`] against the budget first.
pub async fn plan(
    state: &AppState,
    principal: &Principal,
    sql: &str,
    scope: &QueryScope<'_>,
    purpose: Option<&str>,
    mode: MaskMode,
) -> Result<PlanOutcome, PlanError> {
    // (1) Enumerate the tables the SQL references, using the executor's own
    // parser so the set matches exactly what it will bind.
    let refs = meridian_query::referenced_tables(sql)?;

    // (2/3) Resolve + govern each referenced table.
    let mut resolved: Vec<ResolvedTable> = Vec::with_capacity(refs.len());
    let mut applied_policies: Vec<String> = Vec::new();
    for table_ref in &refs {
        match resolve_one(state, principal, scope, purpose, table_ref, mode).await? {
            ResolveOutcome::Table {
                table: rt,
                policies,
            } => {
                applied_policies.extend(policies);
                resolved.push(*rt);
            }
            ResolveOutcome::Denied { reason, policies } => {
                return Ok(PlanOutcome::Denied {
                    table: table_ref.qualified_name(),
                    reason,
                    applied_policies: dedup_sorted(policies),
                });
            }
        }
    }

    // Price the scan from manifest stats (no data read).
    let catalog_tables: Vec<CatalogTable<'_>> = resolved
        .iter()
        .map(|rt| CatalogTable {
            name: rt.query_name.clone(),
            metadata: &rt.metadata,
            storage: rt.storage.as_ref(),
        })
        .collect();
    let estimate = meridian_query::estimate(&catalog_tables).await?;
    drop(catalog_tables);

    Ok(PlanOutcome::Planned(PlannedQuery {
        sql: sql.to_owned(),
        tables: resolved,
        estimate,
        applied_policies: dedup_sorted(applied_policies),
    }))
}

/// The result of resolving one referenced table.
enum ResolveOutcome {
    /// Resolved and permitted (RBAC + ABAC allow). The resolved table is boxed —
    /// it carries the (large) `TableMetadata`, dwarfing the `Denied` arm.
    Table {
        table: Box<ResolvedTable>,
        policies: Vec<String>,
    },
    /// ABAC denied this table.
    Denied {
        reason: String,
        policies: Vec<String>,
    },
}

/// Resolves one referenced table to its metadata + storage + enforcement,
/// checking RBAC READ and resolving the ABAC decision.
async fn resolve_one(
    state: &AppState,
    principal: &Principal,
    scope: &QueryScope<'_>,
    purpose: Option<&str>,
    table_ref: &TableRef,
    mode: MaskMode,
) -> Result<ResolveOutcome, ApiError> {
    // The namespace: a qualified reference carries it; a bare name uses the
    // caller's default namespace. No default + a bare name is unresolvable.
    let levels: Vec<String> = if table_ref.namespace.is_empty() {
        match scope.default_namespace {
            Some(ns) if !ns.is_empty() => ns.to_vec(),
            _ => {
                return Err(ApiError::bad_request(format!(
                    "table {:?} is unqualified and no default namespace is set; qualify it as \
                     `namespace.{}`",
                    table_ref.table, table_ref.table
                )));
            }
        }
    } else {
        table_ref.namespace.clone()
    };

    let wh = resolve_warehouse(&state.pool, scope.warehouse).await?;
    let record = table::get_by_name(&state.pool, &wh.id, &levels, &table_ref.table)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                axum::http::StatusCode::NOT_FOUND,
                "NoSuchTableException",
                format!(
                    "table {}/{}/{} does not exist",
                    scope.warehouse,
                    levels.join("."),
                    table_ref.table
                ),
            )
        })?;
    let chain = namespace_scope_chain(&state.pool, &wh.id, &levels).await?;

    // (RBAC) The principal must be able to READ the table, or the query fails.
    require(
        &state.pool,
        principal,
        Privilege::Read,
        &SecurableScope::table(&wh.id, chain.clone(), Some(&record.id)),
    )
    .await?;

    // Load the current metadata (for the executor) and its current schema (for
    // the ABAC column universe). One metadata read serves both.
    let metadata = load_metadata(&wh, &record).await?;
    let schema = current_schema(&metadata)?;

    // (ABAC) Resolve the raw enforcement. A deny short-circuits the whole query.
    let resolved = governance::resolve_query_enforcement(
        &state.pool,
        principal,
        &TableContext {
            table_id: &record.id,
            namespace_ids: &chain,
            schema: &schema,
            owner: None,
        },
        purpose,
    )
    .await?;

    if resolved.denied {
        return Ok(ResolveOutcome::Denied {
            reason: resolved.reason,
            policies: resolved.applied_policies,
        });
    }

    // Apply the surface's mask mode: agents drop every masked column (H-F2),
    // the workbench keeps value-preserving masks.
    let enforcement = match mode {
        MaskMode::Drop => fold_masks_to_drop(resolved.enforcement),
        MaskMode::Preserve => resolved.enforcement,
    };

    Ok(ResolveOutcome::Table {
        table: Box::new(ResolvedTable {
            query_name: table_ref.qualified_name(),
            internal_id: record.id,
            metadata,
            storage: connect_storage(&wh)?,
            enforcement,
        }),
        policies: resolved.applied_policies,
    })
}

/// Rewrites every column mask to a `Drop` (H-F2): on the agent path a masked
/// column is absent, never a rewritten value. Row filters are untouched.
fn fold_masks_to_drop(mut enforcement: Enforcement) -> Enforcement {
    enforcement.column_masks = enforcement
        .column_masks
        .into_iter()
        .map(|m| ColumnMask::new(m.column, MaskKind::Drop, m.source_policy))
        .collect();
    enforcement
}

/// Reads a table's full metadata from storage (the executor needs the whole
/// `TableMetadata`, not just the schema).
async fn load_metadata(
    wh: &warehouse::WarehouseRecord,
    record: &table::TableRecord,
) -> Result<TableMetadata, ApiError> {
    let Some(metadata_location) = record.metadata_location.clone() else {
        return Err(ApiError::new(
            axum::http::StatusCode::NOT_FOUND,
            "NoSuchTableException",
            "table has no metadata".to_owned(),
        ));
    };
    let storage = connect_storage(wh)?;
    meridian_storage::read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .map_err(|e| {
            ApiError::new(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("table metadata is unreadable: {e}"),
            )
        })
}

/// The table's current schema, cloned, or a clear error.
fn current_schema(metadata: &TableMetadata) -> Result<Schema, ApiError> {
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

fn dedup_sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v.dedup();
    v
}
