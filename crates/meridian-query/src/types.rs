//! Public API types: the query result, its provenance record, and the resource
//! caps that bound a small scan.
//!
//! These are the shapes the calling route (Pillar H `run_sql`, Pillar L
//! workbench) hands in and gets back. They are deliberately arrow-free — rows
//! are plain JSON, columns are names + type labels — so the executor's internal
//! Arrow/`DataFusion` types never leak across the crate boundary and callers do
//! not take an Arrow dependency to consume a result.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Default small-scan byte cap: 128 MiB of on-disk data. The embedded executor
/// is for small scans only (spec §8.1); anything larger routes to a customer
/// engine. Callers override this per request from an agent's budget (H-F4).
pub const DEFAULT_MAX_SCAN_BYTES: u64 = 128 * 1024 * 1024;

/// Default small-scan row cap: 5 million input rows. Bounds wide-but-small
/// tables that slip under the byte cap.
pub const DEFAULT_MAX_SCAN_ROWS: u64 = 5_000_000;

/// Default cap on the number of result rows returned to the caller. Protects
/// the caller (and an agent's context window) from an unbounded result even
/// when the *scanned* size is within budget. Applied as a hard `LIMIT` wrapper.
pub const DEFAULT_MAX_RESULT_ROWS: usize = 10_000;

/// Resource caps that bound one small-scan query. Enforced **before** execution
/// where possible (byte/row estimates come from manifest stats), so an oversized
/// query is refused up front rather than after burning I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Caps {
    /// Maximum on-disk bytes the scan may read across all referenced tables'
    /// current snapshots. Estimated from manifest file sizes before execution.
    pub max_scan_bytes: u64,
    /// Maximum input rows the scan may read (summed record counts). Estimated
    /// from manifest record counts before execution.
    pub max_scan_rows: u64,
    /// Maximum rows returned to the caller. Enforced as a `LIMIT` so the engine
    /// stops producing rows past it; the result is marked
    /// [`QueryOutput::truncated`] when it bites.
    pub max_result_rows: usize,
}

impl Default for Caps {
    fn default() -> Self {
        Self {
            max_scan_bytes: DEFAULT_MAX_SCAN_BYTES,
            max_scan_rows: DEFAULT_MAX_SCAN_ROWS,
            max_result_rows: DEFAULT_MAX_RESULT_ROWS,
        }
    }
}

impl Caps {
    /// Caps with the given byte limit, other limits left at their defaults.
    #[must_use]
    pub fn with_max_scan_bytes(bytes: u64) -> Self {
        Self {
            max_scan_bytes: bytes,
            ..Self::default()
        }
    }
}

/// The pre-execution cost estimate for a set of tables: the summed on-disk
/// bytes and record counts of their current snapshots, plus the live data-file
/// count, all from **manifest stats only** (no data file is read).
///
/// This is exactly what [`run`](crate::run) sums in its estimate step, exposed
/// so a caller can price a query against a budget *before* executing it (H-F3:
/// "results size-capped + cost-estimated before execution"). Because the
/// small-scan executor reads every live file fully, this estimate is also the
/// bytes actually scanned — there is no reconciliation gap between the estimate
/// and the [`QueryOutput::bytes_scanned`] a run reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanEstimate {
    /// Summed on-disk bytes of the live data files across all tables.
    pub bytes: u64,
    /// Summed record counts of the live data files across all tables.
    pub rows: u64,
    /// Total live data-file count across all tables.
    pub files: usize,
}

/// One output column: its name and a human-readable Arrow/SQL type label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    /// Column name as it appears in the result rows' JSON objects.
    pub name: String,
    /// The column's logical type, as an Arrow `DataType` display string
    /// (e.g. `Int64`, `Utf8`, `Timestamp(Microsecond, None)`). A label for
    /// display/debugging, not a parsed type.
    pub data_type: String,
}

/// One table + snapshot the query actually read, so results can be cited
/// (H-F3: "every result carries provenance — tables + snapshot ids used").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSnapshot {
    /// The table's name as the query referenced it (the registered name).
    pub table: String,
    /// The table's UUID from its metadata, stable across renames.
    pub table_uuid: String,
    /// The snapshot id read (the table's current snapshot at query time), or
    /// `None` for a table with no snapshots yet (empty table — zero rows).
    pub snapshot_id: Option<i64>,
}

/// The provenance of a query result: every table + snapshot it read, and the
/// row/column policies that were applied. An agent cites `tables`; a CISO audit
/// reads `policies_applied` to answer "which agent read which columns under
/// which policy" (H-F4).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    /// Tables + snapshot ids read, in the order they were registered. Deduped:
    /// a table referenced twice appears once.
    pub tables: Vec<TableSnapshot>,
    /// Ids of the row-filter policies that were applied (from the resolved
    /// [`meridian_authz::Enforcement`]), deduped and sorted. Empty when no row
    /// filter applied.
    pub row_filter_policies: Vec<String>,
    /// Ids of the column-mask policies that were applied, deduped and sorted.
    /// Empty when no mask applied.
    pub column_mask_policies: Vec<String>,
    /// Names of columns that were masked or dropped for this principal, deduped
    /// and sorted. A masked column may still appear in results (transformed); a
    /// dropped column is absent. Recorded so the audit shows exactly what the
    /// principal could and could not see.
    pub masked_columns: Vec<String>,
}

/// The result of a small-scan query.
///
/// Rows are JSON objects keyed by column name (the shape `DataFusion`'s Arrow ->
/// JSON writer produces): each value is a `serde_json::Value`. A caller streams
/// these to an agent or renders them in the workbench without touching Arrow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryOutput {
    /// The result schema: one entry per output column, in column order.
    pub columns: Vec<Column>,
    /// The result rows. Each is a JSON object mapping column name to value;
    /// SQL `NULL` is JSON `null` (present, so column order is preserved).
    pub rows: Vec<Value>,
    /// Provenance: tables + snapshot ids read and policies applied, for citation
    /// and audit.
    pub provenance: Provenance,
    /// On-disk bytes the scan was estimated to read (summed from manifest file
    /// sizes for the referenced snapshots). The pre-execution cost estimate that
    /// the cap is checked against; recorded so budget accounting (scanned-bytes/
    /// day, H-F4) is exact.
    pub bytes_scanned: u64,
    /// Input rows the scan was estimated to read (summed record counts).
    pub rows_scanned: u64,
    /// Whether the result was truncated by [`Caps::max_result_rows`]. When
    /// `true`, more rows matched than were returned.
    pub truncated: bool,
}

impl QueryOutput {
    /// The number of rows returned to the caller (after any `LIMIT`/truncation).
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Re-shapes [`Self::rows`] (JSON objects keyed by column name) into JSON
    /// **arrays** aligned to [`Self::columns`], the shape the agent gateway's
    /// `QueryOutcome` expects. A column absent from a row object (which should
    /// not happen — the serializer writes explicit nulls) fills with JSON
    /// `null`, so every array has one entry per column, in column order.
    ///
    /// The objects form stays the public default (self-describing, order-
    /// independent, and makes the "dropped column is absent" guarantee visible);
    /// this is the one-call conversion for the array-shaped seam.
    #[must_use]
    pub fn rows_as_arrays(&self) -> Vec<Value> {
        self.rows
            .iter()
            .map(|row| {
                let cells = self
                    .columns
                    .iter()
                    .map(|c| row.get(&c.name).cloned().unwrap_or(Value::Null))
                    .collect();
                Value::Array(cells)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Column, Provenance, QueryOutput};

    #[test]
    fn rows_as_arrays_aligns_to_column_order() {
        let out = QueryOutput {
            columns: vec![
                Column {
                    name: "id".to_owned(),
                    data_type: "Int64".to_owned(),
                },
                Column {
                    name: "amount".to_owned(),
                    data_type: "Int64".to_owned(),
                },
            ],
            // Note: second row's keys are in a different order, and a null.
            rows: vec![
                json!({"id": 1, "amount": 100}),
                json!({"amount": null, "id": 2}),
            ],
            provenance: Provenance::default(),
            bytes_scanned: 0,
            rows_scanned: 0,
            truncated: false,
        };
        assert_eq!(
            out.rows_as_arrays(),
            vec![json!([1, 100]), json!([2, null])]
        );
    }
}
