//! Meridian's built-in **small-scan query executor** (spec §8.1, C-F4 tier 1,
//! H-F3 `run_sql`, L-F1 workbench): a `DataFusion`-backed engine that runs
//! governed `SELECT`s over the Iceberg tables the catalog owns, for small scans
//! only.
//!
//! It is the shared execution engine behind two surfaces:
//!
//! - the **agent gateway** (`run_sql`, Pillar H): an agent asks a question, the
//!   query is validated, policy-rewritten, and routed — small scans run here,
//!   big ones go to a registered customer engine;
//! - the **workbench** (Pillar L): a human runs SQL over any governed asset;
//!   small queries run here for a zero-setup first taste (the < 5-minute
//!   time-to-first-query goal).
//!
//! ## What it does
//!
//! Given a set of tables (each an Iceberg [`TableMetadata`] plus a warehouse
//! [`Storage`] handle) and the row-filter/column-mask [`Enforcement`] resolved
//! for the querying principal, [`run`] does, in this order (the `run_sql`
//! contract, H-F3):
//!
//! 1. **Validate** — the SQL must be a single read-only statement (`SELECT` or a
//!    read-only CTE). DML/DDL/`COPY`/multi-statement input is refused, so a
//!    governed query can never mutate.
//! 2. **Estimate + cap** — sum the on-disk bytes and record counts of the
//!    tables' current snapshots *from manifest stats* and refuse **before**
//!    reading any data if the scan exceeds the [`Caps`] (the small-scan
//!    boundary; the refusal message is written to be relayed to an agent so it
//!    can re-ask against a big engine).
//! 3. **Read** — resolve each table's current snapshot, read its live Parquet
//!    data files to Arrow (mapping columns by Iceberg **field id**, synthesizing
//!    nulls for schema-evolved files, and materializing merge-on-read deletes),
//!    and register each table as an in-memory `DataFusion` table under a private
//!    name.
//! 4. **Govern** — build a per-table SQL **view** over the private table that
//!    injects the row filter as a `WHERE` and masks or drops columns, so the
//!    user's SQL is enforced by `DataFusion`'s own planner — the same Pillar-D
//!    machinery scan planning uses. Dropped columns are *absent*, not nulled
//!    (H-F2), so restricted schema cannot leak.
//! 5. **Execute + return** — run the validated SQL against the governed views,
//!    cap the result rows, and return [`QueryOutput`]: JSON rows, the result
//!    schema, a **provenance** record (tables + snapshot ids read, policies
//!    applied — so agents can cite, H-F3), and the byte/row estimate (for
//!    budget accounting, H-F4).
//!
//! ## What it does not do
//!
//! It does not resolve names or load metadata (the caller hands it
//! `TableMetadata`), does not resolve policy (the caller hands it a resolved
//! [`Enforcement`] from `meridian_authz::resolve_filters_and_masks`), does not
//! touch server routes or the audit chain (the calling route audits the agent
//! action, H-F4), and does not run big scans (those route to customer engines).
//! It is deliberately a pure function of *(metadata + bytes + policy + SQL +
//! caps)*, which is what makes it testable against in-memory Iceberg fixtures.
//!
//! ## Relationship to the agent gateway's `QueryExecutor` trait
//!
//! The MCP crate (`meridian-agents`, Pillar H) owns an async, `dyn`-dispatched
//! `QueryExecutor` trait (`meridian_agents::executor`) so it can hold the
//! executor behind a trait object and be plugged in by a later wave without
//! depending on `DataFusion`. This crate exposes the clean async function
//! [`run`] that a thin **adapter** (the server wiring, wave 2) calls to
//! implement that trait. The adapter — not this crate — depends on
//! `meridian-agents`, so `meridian-query` takes no dependency on the gateway and
//! the gateway takes none on `DataFusion`: exactly the decoupling the trait
//! exists for.
//!
//! The mapping the adapter performs, against the trait as it stands
//! (`QueryExecutor::run_sql(&QueryRequest) -> Result<QueryOutcome,
//! ExecutorError>`):
//!
//! | `QueryRequest` (gateway) | → `meridian_query::run` input |
//! |--------------------------|-------------------------------|
//! | `sql` | `sql` |
//! | `row_filter: Option<Value>` (a *serialized* predicate) | deserialize into a `meridian_authz::RowPredicate`, wrap in a `RowFilter`, put on the table's `Enforcement::row_filters` |
//! | `dropped_columns: Vec<String>` | one `meridian_authz::ColumnMask` of kind `Drop` per name, on `Enforcement::column_masks` |
//! | `row_limit: i64` | [`Caps::max_result_rows`] (plus the byte/row scan caps from the agent's budget) |
//! | `warehouse` / `purpose` | routing/audit hints the adapter uses to open the right `Storage` and tag the audit; not needed by `run` |
//!
//! | `meridian_query::run` output ([`QueryOutput`]) | → `QueryOutcome` (gateway) |
//! |-----------------------------------------------|----------------------------|
//! | `columns: Vec<Column>` | `columns: Vec<String>` — take each `Column::name` |
//! | `rows: Vec<Value>` (JSON **objects** keyed by column) | `rows: Vec<Value>` (JSON **arrays** aligned to `columns`) — use [`QueryOutput::rows_as_arrays`] |
//! | `provenance.tables` (name + uuid + snapshot id) | `Provenance { table_ids, snapshot_ids }` — the adapter maps registered names to the Meridian internal table ids it already holds |
//! | `bytes_scanned: u64` | `bytes_scanned: i64` (cast); `cost_micros` from the caller's pricing, not computed here |
//!
//! | [`QueryError`] | → `ExecutorError` (gateway) |
//! |----------------|----------------------------|
//! | any where [`QueryError::is_caller_refusal`] holds (`InvalidSql`, `NotReadOnly`, `ScanTooLarge`, `TooManyRows`) | `Rejected` |
//! | operational faults (`Storage`, `Manifest`, `Engine`, `UnqueryableTable`, `UnmappableField`, `DeletionVectorUnsupported`) | `Failed` |
//!
//! The gateway's `QueryRequest` models column policy only as *dropped columns*;
//! this crate additionally supports value-preserving masks (hash, partial, null)
//! for the workbench path, which pass richer `Enforcement`s directly to [`run`]
//! without going through the drop-only trait shape.
//!
//! [`TableMetadata`]: meridian_iceberg::spec::TableMetadata
//! [`Storage`]: meridian_storage::Storage
//! [`Enforcement`]: meridian_authz::Enforcement

mod catalog;
mod error;
mod executor;
mod policy;
mod reader;
mod refs;
mod result;
mod types;

pub use catalog::CatalogTable;
pub use error::{QueryError, QueryResult};
pub use executor::{GovernedTable, estimate, run};
pub use refs::{TableRef, referenced_tables};
pub use types::{
    Caps, Column, DEFAULT_MAX_RESULT_ROWS, DEFAULT_MAX_SCAN_BYTES, DEFAULT_MAX_SCAN_ROWS,
    Provenance, QueryOutput, ScanEstimate, TableSnapshot,
};
