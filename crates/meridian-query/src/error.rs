//! Errors surfaced by the small-scan query executor.
//!
//! The executor runs *governed* SQL on behalf of a principal (a human in the
//! Pillar L workbench, or an agent via Pillar H `run_sql`). Its failures split
//! into two families with different audiences:
//!
//! - **Caller-facing refusals** — [`QueryError::ScanTooLarge`] and
//!   [`QueryError::InvalidSql`] are answers, not incidents. The size cap is the
//!   spec's small-scan boundary (§8.1: the embedded executor stays small; big
//!   queries route to customer engines); tripping it is expected and the
//!   message is written to be relayed verbatim to an agent, which can then re-ask
//!   against a registered engine. Malformed SQL is likewise the caller's to fix.
//! - **Operational faults** — storage, manifest, and Parquet-decode errors mean
//!   the catalog could not read data it owns; these surface loudly.
//!
//! Every variant carries enough context to audit (which table, which cap, which
//! file) because an agent's governed query and the reason it failed both belong
//! in the audit chain (H-F4).

use meridian_iceberg::manifest::ManifestError;
use meridian_storage::StorageError;

/// A query execution failure.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// The estimated scan exceeds the configured small-scan cap, so the
    /// executor refuses **before** reading any data. This is the deliberate
    /// boundary between the built-in executor and customer engines; the message
    /// is phrased to be relayed to an agent so it can retry against a registered
    /// engine. Not an incident.
    #[error(
        "query refused: it would scan {requested_bytes} bytes across {file_count} file(s), \
         over the {limit_bytes}-byte small-scan limit — route this query to a registered \
         engine (Trino/Snowflake/Spark/ClickHouse) for large scans"
    )]
    ScanTooLarge {
        /// Bytes the scan would read, summed from manifest file sizes for the
        /// referenced tables' current snapshots.
        requested_bytes: u64,
        /// The configured cap that was exceeded.
        limit_bytes: u64,
        /// How many data files the referenced tables' snapshots hold.
        file_count: usize,
    },

    /// The estimated row count exceeds the configured cap, refused before
    /// execution. Complements the byte cap for wide-but-small-on-disk tables.
    #[error(
        "query refused: it would scan {requested_rows} rows, over the {limit_rows}-row \
         small-scan limit — route this query to a registered engine for large scans"
    )]
    TooManyRows {
        /// Rows the scan would read (summed record counts).
        requested_rows: u64,
        /// The configured cap that was exceeded.
        limit_rows: u64,
    },

    /// The SQL failed to parse, plan, or execute for a caller reason (unknown
    /// column, type mismatch, unsupported syntax, a reference to a table the
    /// query context does not expose). Carries `DataFusion`'s message so the
    /// caller can fix the query. This is the "malformed SQL -> clean error"
    /// path: it never leaks internals or a stack trace.
    #[error("query is invalid: {0}")]
    InvalidSql(String),

    /// The SQL statement is not a read. The small-scan executor only runs
    /// `SELECT` (and read-only CTEs); DDL/DML/`COPY`/multi-statement input is
    /// refused so a governed query can never mutate.
    #[error("only read-only SELECT statements are allowed here; refused: {reason}")]
    NotReadOnly {
        /// What kind of statement was rejected.
        reason: String,
    },

    /// A referenced table is missing something the executor requires to read it
    /// (no current snapshot, an unresolvable current schema, a manifest list
    /// with no location). A structural precondition, not a row-level fault.
    #[error("table {table:?} cannot be queried: {reason}")]
    UnqueryableTable {
        /// The table name as the caller referenced it.
        table: String,
        /// Why it cannot be read.
        reason: String,
    },

    /// A data file references an Iceberg field id absent from the table's
    /// current schema, so its columns cannot be mapped by id. Refused rather
    /// than guessed by name — silently misaligning columns would return wrong
    /// data, the one outcome worse than an error.
    #[error(
        "data file {location:?} carries field id {field_id}, absent from the current schema of \
         table {table:?}; cannot map columns by field id"
    )]
    UnmappableField {
        /// The table the file belongs to.
        table: String,
        /// The offending file.
        location: String,
        /// The field id that could not be resolved.
        field_id: i32,
    },

    /// A data file carries a v3 deletion vector (Puffin blob), which the plain
    /// Parquet reader cannot materialize. Refused with a clear reason rather
    /// than silently returning deleted rows. (Position/equality *delete files*
    /// — the v2 merge-on-read shape — are applied.)
    #[error(
        "table {table:?} has a data file {location:?} with an attached deletion vector, which \
         the small-scan executor cannot yet apply; route this query to a registered engine"
    )]
    DeletionVectorUnsupported {
        /// The table involved.
        table: String,
        /// The data file carrying the deletion vector.
        location: String,
    },

    /// Object storage could not be read.
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// A manifest list or manifest could not be read or parsed.
    #[error(transparent)]
    Manifest(#[from] ManifestError),

    /// A data file could not be decoded as Parquet, or an internal Arrow/
    /// `DataFusion` invariant failed while assembling or serializing results.
    /// These are engine-internal faults, distinct from a caller's invalid SQL.
    #[error("query engine failed at {stage}: {reason}")]
    Engine {
        /// Where in the pipeline the failure occurred (e.g. `read parquet`,
        /// `register table`, `serialize rows`).
        stage: &'static str,
        /// The underlying cause.
        reason: String,
    },
}

/// A query result.
pub type QueryResult<T> = Result<T, QueryError>;

impl QueryError {
    /// Builds an [`QueryError::Engine`] from a stage label and any displayable
    /// cause.
    pub(crate) fn engine(stage: &'static str, reason: impl std::fmt::Display) -> Self {
        Self::Engine {
            stage,
            reason: reason.to_string(),
        }
    }

    /// Whether this error is a caller-facing refusal/answer (safe and expected
    /// to surface to the requester) rather than an operational fault. Useful to
    /// the calling route when deciding audit severity and HTTP status.
    #[must_use]
    pub fn is_caller_refusal(&self) -> bool {
        matches!(
            self,
            Self::ScanTooLarge { .. }
                | Self::TooManyRows { .. }
                | Self::InvalidSql(_)
                | Self::NotReadOnly { .. }
        )
    }
}
