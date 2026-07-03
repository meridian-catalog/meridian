//! Errors surfaced by the compaction engine.
//!
//! Compaction is a *catalog-side* operation running against the customer's
//! own data and metadata: every failure here is an operator/data-integrity
//! problem, surfaced loudly, never masked. In particular the correctness
//! assertions (row count in must equal row count out; field ids must resolve)
//! are hard errors — a violated invariant aborts the plan rather than
//! producing a commit that would silently lose or duplicate rows.

use meridian_iceberg::manifest::ManifestError;
use meridian_storage::StorageError;

/// A compaction failure.
#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    /// Object storage could not be read or written.
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// A manifest list or manifest could not be read or written.
    #[error(transparent)]
    Manifest(#[from] ManifestError),

    /// A data or delete file could not be read, decoded, or written as
    /// Parquet.
    #[error("parquet at {location:?} is unreadable: {reason}")]
    Parquet {
        /// The file location.
        location: String,
        /// What went wrong.
        reason: String,
    },

    /// The table's metadata is missing something compaction requires (no
    /// current snapshot, an unresolvable schema/spec, a manifest list with no
    /// location). These are structural preconditions, not row-level problems.
    #[error("table is not in a compactable state: {0}")]
    Unsupported(String),

    /// A file references an Iceberg field id that is not in the target
    /// schema, so its columns cannot be mapped by id. Refused rather than
    /// guessed by name — silently misaligning columns is the one outcome
    /// worse than not compacting.
    #[error(
        "data file {location:?} carries field id {field_id} which is absent from the current schema; \
         cannot map columns by field id"
    )]
    UnmappableField {
        /// The offending file.
        location: String,
        /// The field id that could not be resolved.
        field_id: i32,
    },

    /// The output row count did not equal the input row count after applying
    /// pending deletes. This is the central correctness assertion of
    /// compaction; tripping it is a bug in the engine and must abort the plan.
    #[error(
        "row-count mismatch rewriting {group}: expected {expected} rows out, produced {produced} \
         (compaction must be row-count preserving after deletes)"
    )]
    RowCountMismatch {
        /// Human-readable identity of the bin-pack group (partition + inputs).
        group: String,
        /// Rows expected after delete application.
        expected: i64,
        /// Rows actually written.
        produced: i64,
    },

    /// A precondition on the requested job is not met (target size of zero,
    /// contradictory options).
    #[error("invalid compaction request: {0}")]
    InvalidRequest(String),
}

/// A compaction result.
pub type CompactionResult<T> = Result<T, CompactionError>;

impl CompactionError {
    /// Builds a [`CompactionError::Parquet`] from a location and any
    /// displayable cause.
    pub(crate) fn parquet(location: impl Into<String>, reason: impl std::fmt::Display) -> Self {
        Self::Parquet {
            location: location.into(),
            reason: reason.to_string(),
        }
    }
}
