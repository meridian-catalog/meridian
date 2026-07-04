//! The small-scan query executor: the top-level [`run`] entry point that ties
//! catalog resolution, the up-front cost cap, policy compilation, Parquet
//! reading, `DataFusion` execution, and result assembly together.
//!
//! The order is deliberate and is the spec's `run_sql` contract (H-F3):
//! **validate -> estimate -> refuse-or-execute**. The scanned-bytes/row estimate
//! comes from manifest stats and is checked *before* any data file is read, so
//! an oversized query costs a metadata read, not a scan. Only then are data
//! files read, tables registered under private names, governed views built over
//! them, and the (validated, read-only) user SQL run against those views.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use datafusion::catalog::MemorySchemaProvider;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use meridian_authz::Enforcement;

use crate::catalog::{CatalogTable, ScanPlan, StorageBytes, TableScan, resolve_scan};
use crate::error::{QueryError, QueryResult};
use crate::policy::build_governed_view;
use crate::reader::build_table;
use crate::result::batches_to_rows;
use crate::types::{Caps, Provenance, QueryOutput, TableSnapshot};

/// A table the query may read, paired with the enforcement resolved for the
/// querying principal against that table.
///
/// The caller builds one of these per table the SQL may touch: it loads the
/// table's `TableMetadata`, opens the warehouse `Storage`, and calls
/// `meridian_authz::resolve_filters_and_masks(principal, table, ...)` to get the
/// [`Enforcement`]. The executor never resolves policy itself — it applies what
/// it is given, so the same Pillar-D decision drives `run_sql` and scan
/// planning.
pub struct GovernedTable<'a> {
    /// The table and how to read it.
    pub table: CatalogTable<'a>,
    /// Row filters + column masks resolved for the querying principal. Use
    /// [`Enforcement::none`] for an unrestricted principal.
    pub enforcement: Enforcement,
}

impl std::fmt::Debug for GovernedTable<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GovernedTable")
            .field("table", &self.table)
            .field("enforcement", &self.enforcement)
            .finish()
    }
}

/// Prefix for the private name a table's raw data is registered under, before
/// the governed view is layered on top. Chosen to be unlikely to collide with a
/// user's own table names.
const RAW_PREFIX: &str = "__meridian_raw__";

/// Runs a governed, small-scan SELECT over the given tables.
///
/// - `sql` is the caller's query. It must be a single read-only statement
///   (`SELECT` or a read-only CTE); anything else is refused
///   ([`QueryError::NotReadOnly`]).
/// - `tables` are the tables the query may reference, each with its resolved
///   enforcement. A query may reference a subset; unreferenced tables are still
///   registered (cheap, since their data is only read lazily by `DataFusion`) but
///   contribute to the pre-execution estimate only if actually scanned — see the
///   note below on estimation.
/// - `caps` bound the scan (bytes/rows) and the result size.
///
/// Returns a [`QueryOutput`] with JSON rows, the result schema, provenance
/// (tables + snapshot ids + policies applied), and the byte/row estimate.
///
/// # Cost estimation
///
/// The estimate sums the on-disk size and record counts of every provided
/// table's current snapshot. This is a conservative upper bound: it assumes the
/// query may touch every provided table. Callers should pass only the tables the
/// query can actually reference (they know the parsed table list) so the
/// estimate is tight; passing unrelated large tables would over-estimate and
/// refuse a query that is in fact small.
// One linear orchestration (validate → estimate → govern each table →
// execute → serialize); splitting it would scatter the shared provenance and
// cap state without making it clearer.
#[allow(clippy::too_many_lines)]
pub async fn run(sql: &str, tables: &[GovernedTable<'_>], caps: Caps) -> QueryResult<QueryOutput> {
    // 1. Validate: exactly one read-only statement.
    ensure_read_only(sql)?;

    // 2. Resolve each table's scan plan and sum the pre-execution estimate.
    let mut scans: Vec<(&GovernedTable<'_>, ScanPlan)> = Vec::with_capacity(tables.len());
    let mut total_bytes: u64 = 0;
    let mut total_rows: u64 = 0;
    for gt in tables {
        let plan = resolve_scan(&gt.table).await?;
        total_bytes = total_bytes.saturating_add(plan.bytes);
        total_rows = total_rows.saturating_add(plan.rows);
        scans.push((gt, plan));
    }

    // 3. Refuse up front if over cap — before reading any data file.
    let file_count: usize = scans.iter().map(|(_, p)| p.data_files.len()).sum();
    if total_bytes > caps.max_scan_bytes {
        return Err(QueryError::ScanTooLarge {
            requested_bytes: total_bytes,
            limit_bytes: caps.max_scan_bytes,
            file_count,
        });
    }
    if total_rows > caps.max_scan_rows {
        return Err(QueryError::TooManyRows {
            requested_rows: total_rows,
            limit_rows: caps.max_scan_rows,
        });
    }

    // 4. Read each table, apply governance in an ISOLATED context, and
    //    register only the resulting governed rows in the user context.
    //    The user's SQL therefore runs against a context that contains no
    //    raw table and no view over one — masking and row-filtering are
    //    safe by construction, independent of what the caller pre-validated
    //    or what the SQL references. Provenance accumulates as we go.
    let ctx = SessionContext::new();
    let mut provenance = Provenance::default();
    let mut seen_tables: BTreeSet<String> = BTreeSet::new();
    let mut row_policies: BTreeSet<String> = BTreeSet::new();
    let mut mask_policies: BTreeSet<String> = BTreeSet::new();
    let mut masked_cols: BTreeSet<String> = BTreeSet::new();

    for (index, (gt, plan)) in scans.iter().enumerate() {
        // Two tables under the same query name would make the applicable
        // enforcement ambiguous; reject rather than silently shadow one.
        if !seen_tables.insert(gt.table.name.clone()) {
            return Err(QueryError::UnqueryableTable {
                table: gt.table.name.clone(),
                reason: "the same table name was provided more than once".to_owned(),
            });
        }

        let schema = crate::catalog::current_schema(&gt.table)?;
        let table_uuid = gt.table.metadata.table_uuid.to_string();
        let scan = TableScan {
            name: &gt.table.name,
            schema,
            plan,
            bytes: StorageBytes::new(gt.table.storage),
        };
        let (mem, _arrow) = build_table(&scan).await?;

        // Apply governance in a throwaway private context: register the raw
        // MemTable there, run the masked/filtered SELECT, and collect the
        // governed batches. The private context is dropped at the end of the
        // iteration — the raw table never exists in the user context.
        let raw_name = format!("{RAW_PREFIX}{index}");
        let private = SessionContext::new();
        private
            .register_table(TableReference::bare(raw_name.clone()), mem)
            .map_err(|e| QueryError::engine("register table", e))?;

        let view = build_governed_view(&gt.table.name, &raw_name, schema, &gt.enforcement)?;
        let governed_df = private
            .sql(&view.select_sql)
            .await
            .map_err(|e| map_engine_view_error(&gt.table.name, &e))?;
        // The DataFrame's Arrow schema is defined even when zero rows come
        // back, so capture it before collecting.
        let governed_schema: ArrowSchema = governed_df.schema().as_arrow().clone();
        let governed_batches = governed_df
            .collect()
            .await
            .map_err(|e| map_engine_view_error(&gt.table.name, &e))?;

        // The query refers to this table by `gt.table.name`, which may be
        // qualified (`namespace.table`). Register the already-governed rows
        // under exactly that reference — creating the implied schema first —
        // so the user's `FROM namespace.table` resolves to governed data and
        // nothing else is reachable.
        let view_ref = TableReference::parse_str(&gt.table.name);
        ensure_schema(&ctx, &view_ref)?;
        let governed = MemTable::try_new(Arc::new(governed_schema), vec![governed_batches])
            .map_err(|e| QueryError::engine("register governed table", e))?;
        ctx.register_table(view_ref.clone(), Arc::new(governed))
            .map_err(|e| QueryError::engine("register governed table", e))?;

        // Provenance for this table (names are unique — guarded above).
        provenance.tables.push(TableSnapshot {
            table: gt.table.name.clone(),
            table_uuid: table_uuid.clone(),
            snapshot_id: plan.snapshot_id,
        });
        for f in &gt.enforcement.row_filters {
            row_policies.insert(f.source_policy.clone());
        }
        for m in &gt.enforcement.column_masks {
            mask_policies.insert(m.source_policy.clone());
        }
        for c in view.masked_columns {
            masked_cols.insert(c);
        }
    }

    provenance.row_filter_policies = row_policies.into_iter().collect();
    provenance.column_mask_policies = mask_policies.into_iter().collect();
    provenance.masked_columns = masked_cols.into_iter().collect();

    // 5. Plan and execute the user SQL against the governed views, capping the
    //    result at max_result_rows + 1 so we can flag truncation.
    let limit = caps.max_result_rows;
    let df = ctx
        .sql(sql)
        .await
        .map_err(|e| map_user_sql_error(&e))?
        .limit(0, Some(limit.saturating_add(1)))
        .map_err(|e| map_user_sql_error(&e))?;

    let batches = df.collect().await.map_err(|e| map_user_sql_error(&e))?;

    // 6. Serialize to JSON rows + columns, applying the truncation cut.
    let (columns, mut rows) = batches_to_rows(&batches)?;
    let truncated = rows.len() > limit;
    if truncated {
        rows.truncate(limit);
    }

    Ok(QueryOutput {
        columns,
        rows,
        provenance,
        bytes_scanned: total_bytes,
        rows_scanned: total_rows,
        truncated,
    })
}

/// Ensures the schema (and catalog) a qualified [`TableReference`] names exists
/// in `ctx`, so a view can be registered under that reference and the user's
/// `FROM schema.table` resolves to it. A bare reference (default catalog/schema)
/// needs nothing — those always exist. Registering a schema that already exists
/// is a no-op we ignore.
fn ensure_schema(ctx: &SessionContext, reference: &TableReference) -> QueryResult<()> {
    let (catalog_name, schema_name) = match reference {
        TableReference::Bare { .. } => return Ok(()),
        TableReference::Partial { schema, .. } => (
            ctx.state().config_options().catalog.default_catalog.clone(),
            schema.to_string(),
        ),
        TableReference::Full {
            catalog, schema, ..
        } => (catalog.to_string(), schema.to_string()),
    };

    let catalog = ctx.catalog(&catalog_name).ok_or_else(|| {
        QueryError::engine(
            "register schema",
            format!("default catalog {catalog_name:?} is not present"),
        )
    })?;
    if catalog.schema(&schema_name).is_none() {
        // Ignore an AlreadyExists race — the goal is only that it exists.
        let _ = catalog.register_schema(&schema_name, Arc::new(MemorySchemaProvider::new()));
    }
    Ok(())
}

/// Estimates the scan cost of a set of tables **without reading any data**:
/// resolves each table's current snapshot from manifests and sums the on-disk
/// bytes, record counts, and live data-file count.
///
/// This is the same metadata-only estimate [`run`] computes before it reads
/// anything, exposed so a caller can price a query against a budget *before*
/// executing (H-F3: cost-estimated before execution). Since the executor reads
/// every live file fully, this is also the bytes a run will scan — the estimate
/// and [`QueryOutput::bytes_scanned`] agree. Policy does not affect scan size,
/// so this takes plain [`CatalogTable`]s, not [`GovernedTable`]s.
///
/// [`QueryOutput::bytes_scanned`]: crate::types::QueryOutput::bytes_scanned
pub async fn estimate(tables: &[CatalogTable<'_>]) -> QueryResult<crate::types::ScanEstimate> {
    let mut bytes: u64 = 0;
    let mut rows: u64 = 0;
    let mut files: usize = 0;
    for table in tables {
        let plan = resolve_scan(table).await?;
        bytes = bytes.saturating_add(plan.bytes);
        rows = rows.saturating_add(plan.rows);
        files += plan.data_files.len();
    }
    Ok(crate::types::ScanEstimate { bytes, rows, files })
}

/// Ensures the SQL is exactly one read-only statement. Uses `DataFusion`'s SQL
/// parser to classify, so this is not a fragile string match: DML (`INSERT`,
/// `UPDATE`, `DELETE`), DDL (`CREATE`, `DROP`, `ALTER`), `COPY`, `EXPLAIN
/// ANALYZE`-that-runs, and multi-statement input are all refused. Only a single
/// `Query` (a `SELECT`, or a CTE resolving to a select) passes.
fn ensure_read_only(sql: &str) -> QueryResult<()> {
    let statements = DFParser::parse_sql(sql).map_err(|e| QueryError::InvalidSql(e.to_string()))?;
    if statements.len() != 1 {
        return Err(QueryError::NotReadOnly {
            reason: format!("expected exactly one statement, found {}", statements.len()),
        });
    }
    // `front()` exists (len == 1). Classify it.
    let stmt = statements.front().expect("one statement");
    // The wildcard is intentional and load-bearing: this is a fail-closed
    // read-only gate, so any DataFusion statement kind we do not name (today
    // `Reset`; tomorrow, whatever upstream adds) must be refused, not compiled.
    #[allow(clippy::match_wildcard_for_single_variants)]
    match stmt {
        DFStatement::Statement(inner) => match inner.as_ref() {
            SqlStatement::Query(_) => Ok(()),
            other => Err(QueryError::NotReadOnly {
                reason: statement_label(other),
            }),
        },
        // Non-ANSI DataFusion statements (COPY, CREATE EXTERNAL TABLE, EXPLAIN,
        // SET/RESET, ...) are not plain reads.
        DFStatement::CopyTo(_) => Err(QueryError::NotReadOnly {
            reason: "COPY".to_owned(),
        }),
        DFStatement::Explain(_) => Err(QueryError::NotReadOnly {
            reason: "EXPLAIN".to_owned(),
        }),
        DFStatement::CreateExternalTable(_) => Err(QueryError::NotReadOnly {
            reason: "CREATE EXTERNAL TABLE".to_owned(),
        }),
        _ => Err(QueryError::NotReadOnly {
            reason: "non-query statement".to_owned(),
        }),
    }
}

/// A short label for a rejected statement kind (for the refusal message).
fn statement_label(stmt: &SqlStatement) -> String {
    // The Debug of the enum variant is verbose; take the leading keyword-ish
    // token so the message reads cleanly.
    let dbg = format!("{stmt:?}");
    let head = dbg.split(['(', ' ', '{']).next().unwrap_or("statement");
    head.to_owned()
}

/// Maps a `DataFusion` error from planning/executing the *user* SQL to a
/// caller-facing [`QueryError::InvalidSql`]. User SQL errors are the caller's to
/// fix (unknown column, type mismatch, unknown table), so they surface as
/// invalid-SQL, never as an engine fault or a stack trace.
fn map_user_sql_error(err: &datafusion::error::DataFusionError) -> QueryError {
    QueryError::InvalidSql(clean_df_message(err))
}

/// Maps a `DataFusion` error from creating/collecting a governed *view* (our own
/// generated SQL). A failure here is an engine/policy-compilation fault, not the
/// caller's SQL — surface it as an engine error tagged with the table.
fn map_engine_view_error(table: &str, err: &datafusion::error::DataFusionError) -> QueryError {
    QueryError::engine(
        "governed view",
        format!("table {table}: {}", clean_df_message(err)),
    )
}

/// Strips `DataFusion`'s backtrace suffix so the caller sees the message, not a
/// stack trace. The error's `Display` is already reasonable; we only trim a
/// trailing backtrace marker if one is present.
fn clean_df_message(err: &datafusion::error::DataFusionError) -> String {
    let s = err.to_string();
    match s.split_once("\n\nbacktrace") {
        Some((head, _)) => head.trim().to_owned(),
        None => s.trim().to_owned(),
    }
}
