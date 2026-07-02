//! The commit protocol contract: version-guarded pointer compare-and-set.
//!
//! This module is the code form of `docs/design/commit-protocol.md`. It
//! defines the *protocol model* — the [`CommitBackend`] trait every pointer
//! store must implement, the pointer/receipt/error types, and the
//! single-table commit driver ([`commit_single_table`]) implementing the
//! optimistic loop: recall idempotency key → load pointer → check
//! requirements → stage metadata → compare-and-set → bounded rebase-retry.
//!
//! The in-memory model backend lives in
//! `tests/commit_properties.rs` together with the property suite; the
//! Postgres-backed implementation (M1) must implement this same trait and
//! pass that same suite.
//!
//! TODO(M1): metadata-level requirement evaluation (`assert-ref-snapshot-id`
//! and friends) against [`crate::spec::TableMetadata`]; [`PointerRequirement`]
//! is the pointer-level projection of it.
//! TODO(M1): multi-table commit driver — [`CommitBackend::commit_atomic`] is
//! already multi-table; the orchestration loop mirrors
//! [`commit_single_table`] with deterministic lock ordering in the backend.
//! TODO(M1): the Postgres `CommitBackend` implementation (guarded `UPDATE`
//! inside one transaction with index write-through, audit row, outbox event).

use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;

/// A version-stamped pointer to a table's current `metadata.json`.
///
/// The `version` is internal to Meridian: it increases by exactly 1 per
/// successful commit, which lets the pointer swap be expressed as a
/// compare-and-set and makes commit history provably gapless (invariant I3
/// in the design doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePointer {
    /// Monotonic pointer version; +1 per committed swap.
    pub version: u64,
    /// Object-storage location of the current `metadata.json`.
    pub metadata_location: String,
}

/// One table's compare-and-set operation within a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointerCas<T> {
    /// The table whose pointer is being swapped.
    pub table: T,
    /// The pointer version this commit was built against. The swap succeeds
    /// only if the table is still at exactly this version.
    pub expected_version: u64,
    /// Location of the staged candidate `metadata.json`.
    pub new_metadata_location: String,
}

/// Per-table result of a successful commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTable<T> {
    /// The committed table.
    pub table: T,
    /// The table's pointer version after the swap.
    pub version: u64,
    /// The now-current metadata location.
    pub metadata_location: String,
}

/// Receipt for a commit: what was applied, and whether this response is a
/// replay of a previously recorded outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitReceipt<T> {
    /// Per-table outcomes, in the order the operations were submitted.
    pub tables: Vec<CommittedTable<T>>,
    /// `true` when this receipt was replayed from an idempotency record
    /// rather than produced by applying state changes. Backends record
    /// receipts with `replayed: false`; every replay path sets it to `true`.
    pub replayed: bool,
}

/// A pointer-level commit requirement.
///
/// This is the protocol-model projection of Iceberg's optimistic
/// `requirements` list: an assertion about the state a commit was built
/// against, checked by the arbiter before the swap. TODO(M1): metadata-level
/// requirements evaluated against the loaded [`crate::spec::TableMetadata`];
/// the conflict and retry semantics are identical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PointerRequirement {
    /// The pointer must currently be at exactly this version.
    VersionIs(u64),
    /// The current metadata location must be exactly this.
    MetadataLocationIs(String),
}

impl PointerRequirement {
    /// Checks the requirement against a freshly loaded pointer.
    pub fn check(&self, pointer: &TablePointer) -> Result<(), RequirementViolation> {
        let holds = match self {
            Self::VersionIs(version) => pointer.version == *version,
            Self::MetadataLocationIs(location) => pointer.metadata_location == *location,
        };
        if holds {
            Ok(())
        } else {
            Err(RequirementViolation {
                requirement: self.clone(),
                actual: pointer.clone(),
            })
        }
    }
}

impl fmt::Display for PointerRequirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VersionIs(version) => write!(f, "pointer version must be {version}"),
            Self::MetadataLocationIs(location) => {
                write!(f, "metadata location must be {location}")
            }
        }
    }
}

/// A commit requirement that does not hold against the current table state.
///
/// Maps to `409 CommitFailedException` at the API boundary: the client must
/// refresh the table and rebuild its commit.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "commit requirement not met: {requirement}; table is at version {} ({})",
    actual.version,
    actual.metadata_location
)]
pub struct RequirementViolation {
    /// The requirement that failed.
    pub requirement: PointerRequirement,
    /// The pointer state it was evaluated against.
    pub actual: TablePointer,
}

/// Errors surfaced by a [`CommitBackend`].
///
/// Table identities are rendered to strings here so the error type stays
/// independent of the backend's [`CommitBackend::TableId`] type.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommitBackendError {
    /// The table has no pointer record.
    #[error("table {table} does not exist")]
    TableNotFound {
        /// The missing table.
        table: String,
    },

    /// The version guard failed: another commit moved the pointer after
    /// `expected` was observed. Retryable after refresh (state F6 in the
    /// design doc).
    #[error("version conflict on table {table}: expected {expected}, found {actual}")]
    VersionConflict {
        /// The conflicted table.
        table: String,
        /// The version the commit was built against.
        expected: u64,
        /// The version actually found.
        actual: u64,
    },

    /// The idempotency key is already recorded for a different request
    /// (failure F9 in the design doc). A client bug — never silently
    /// ignored, never applied.
    #[error("idempotency key {key:?} was already used for a different commit")]
    IdempotencyKeyReuse {
        /// The reused key.
        key: String,
    },

    /// The same table appears more than once in one atomic commit; there is
    /// no defined merge order for two operations on one table.
    #[error("duplicate table in multi-table commit: {table}")]
    DuplicateTable {
        /// The duplicated table.
        table: String,
    },

    /// A commit must contain at least one table operation.
    #[error("empty commit: a commit must contain at least one table operation")]
    EmptyCommit,

    /// The backend is temporarily unable to serve (e.g. Postgres down).
    /// Retryable; nothing was applied.
    #[error("commit backend unavailable: {message}")]
    Unavailable {
        /// Operator-facing description.
        message: String,
    },

    /// The backend failed at the point of no return (the transaction commit
    /// itself errored, e.g. a connection drop mid-`COMMIT`), so whether the
    /// commit applied cannot be determined from this attempt (failure F3 in
    /// the design doc). Maps to `5xx CommitStateUnknownException` at the API
    /// boundary; a client retry with the same idempotency key resolves the
    /// ambiguity (replay if it applied, fresh attempt if it did not).
    #[error("commit state unknown: {message}")]
    StateUnknown {
        /// Operator-facing description.
        message: String,
    },
}

/// The pointer store the commit protocol runs against.
///
/// Implementations: the in-memory model in `tests/commit_properties.rs`
/// (M0), and the Postgres store (M1) where `commit_atomic` is one
/// transaction carrying the guarded pointer `UPDATE`s, index write-through,
/// audit row, and outbox event.
///
/// # Contract
///
/// Every implementation MUST uphold the following; the property suite in
/// `tests/commit_properties.rs` is the executable form of this contract and
/// must pass against every implementation.
///
/// 1. **Atomicity.** `commit_atomic` applies *all* operations or *none*,
///    observable at every instant including across crashes.
/// 2. **Version guard.** An operation applies only if the table's current
///    version equals `expected_version`; on success the version becomes
///    exactly `expected_version + 1` and the location becomes
///    `new_metadata_location`. Any guard failure fails the whole commit with
///    [`CommitBackendError::VersionConflict`].
/// 3. **Deterministic lock order.** Validation/locking proceeds in ascending
///    [`CommitBackend::TableId`] order regardless of the order operations
///    were submitted (deadlock freedom for concurrent multi-table commits).
/// 4. **Idempotency recording.** When a key is supplied, the receipt is
///    recorded atomically with the state change. If the key is already
///    recorded: an identical request replays the recorded receipt (with
///    [`CommitReceipt::replayed`] set) and changes nothing; a different
///    request fails with [`CommitBackendError::IdempotencyKeyReuse`]. This
///    check happens *before* version validation so a duplicate of a
///    successful commit replays rather than conflicting.
/// 5. **Validation.** Empty commits and duplicate tables are rejected
///    without state change.
pub trait CommitBackend {
    /// Stable table identity. Production uses ULID-based ids; the `Ord` on
    /// this type defines the deterministic lock order for multi-table
    /// commits (contract item 3).
    type TableId: Clone + fmt::Debug + fmt::Display + Ord + Send + Sync;

    /// Loads the current pointer for a table.
    fn load_pointer(
        &self,
        table: &Self::TableId,
    ) -> impl Future<Output = Result<TablePointer, CommitBackendError>> + Send;

    /// Looks up the recorded receipt for an idempotency key, if any.
    ///
    /// A `None` here is not authoritative under concurrency — the backend
    /// re-checks the key inside `commit_atomic` (contract item 4).
    fn recall_idempotency_key(
        &self,
        key: &str,
    ) -> impl Future<Output = Result<Option<CommitReceipt<Self::TableId>>, CommitBackendError>> + Send;

    /// Atomically applies all pointer swaps, or none (contract items 1–5).
    fn commit_atomic(
        &self,
        ops: &[PointerCas<Self::TableId>],
        idempotency_key: Option<&str>,
    ) -> impl Future<Output = Result<CommitReceipt<Self::TableId>, CommitBackendError>> + Send;
}

/// Errors from the commit driver.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommitError {
    /// A commit requirement does not hold against the current state
    /// (`409 CommitFailedException`; the client must refresh and rebuild).
    #[error(transparent)]
    RequirementFailed(#[from] RequirementViolation),

    /// Every attempt lost the compare-and-set race within the retry budget.
    /// Maps to `409 CommitFailedException` (retryable by the client).
    #[error("commit lost the compare-and-set race {attempts} time(s); giving up")]
    RetriesExhausted {
        /// How many attempts were made.
        attempts: u32,
    },

    /// A backend failure (missing table, unavailability, key reuse, …).
    #[error(transparent)]
    Backend(#[from] CommitBackendError),
}

/// Commits a single table: the optimistic loop from the design doc's state
/// model (`docs/design/commit-protocol.md` §6).
///
/// Per attempt: load the pointer, check `requirements` against it, stage the
/// candidate metadata via `stage_metadata` (in production this writes a
/// uniquely named `metadata.json`; every retry re-stages against the
/// refreshed base), then compare-and-set. On a lost race the requirements
/// are re-checked against the new state and the commit is retried, up to
/// `max_attempts`; the previously staged file becomes an orphan handled by
/// the cleanup strategy in the design doc (§7.1).
///
/// If `idempotency_key` is supplied and already recorded, the recorded
/// receipt is replayed without touching any state — even if `requirements`
/// would fail against the current state (the original commit already moved
/// it; that is what makes the retry safe).
pub async fn commit_single_table<B, S>(
    backend: &B,
    table: B::TableId,
    requirements: &[PointerRequirement],
    mut stage_metadata: S,
    idempotency_key: Option<&str>,
    max_attempts: NonZeroU32,
) -> Result<CommitReceipt<B::TableId>, CommitError>
where
    B: CommitBackend,
    S: FnMut(&TablePointer) -> String + Send,
{
    if let Some(key) = idempotency_key
        && let Some(mut receipt) = backend.recall_idempotency_key(key).await?
    {
        receipt.replayed = true;
        return Ok(receipt);
    }

    let attempts = max_attempts.get();
    for _ in 1..=attempts {
        let base = backend.load_pointer(&table).await?;
        for requirement in requirements {
            requirement.check(&base)?;
        }

        let op = PointerCas {
            table: table.clone(),
            expected_version: base.version,
            new_metadata_location: stage_metadata(&base),
        };
        match backend
            .commit_atomic(std::slice::from_ref(&op), idempotency_key)
            .await
        {
            Ok(receipt) => return Ok(receipt),
            Err(CommitBackendError::VersionConflict { .. }) => {
                // Lost the race. The staged file is now an orphan (design
                // doc §7.1). Refresh, re-check requirements, retry.
            }
            Err(other) => return Err(other.into()),
        }
    }
    Err(CommitError::RetriesExhausted { attempts })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pointer(version: u64, location: &str) -> TablePointer {
        TablePointer {
            version,
            metadata_location: location.to_owned(),
        }
    }

    #[test]
    fn version_requirement_checks_exact_version() {
        let current = pointer(4, "s3://b/metadata/00004-x.metadata.json");
        assert!(PointerRequirement::VersionIs(4).check(&current).is_ok());

        let violation = PointerRequirement::VersionIs(3)
            .check(&current)
            .expect_err("stale version must be rejected");
        assert_eq!(violation.requirement, PointerRequirement::VersionIs(3));
        assert_eq!(violation.actual, current);
    }

    #[test]
    fn location_requirement_checks_exact_location() {
        let current = pointer(9, "s3://b/metadata/00009-y.metadata.json");
        assert!(
            PointerRequirement::MetadataLocationIs(current.metadata_location.clone())
                .check(&current)
                .is_ok()
        );
        assert!(
            PointerRequirement::MetadataLocationIs("s3://b/metadata/stale.json".to_owned())
                .check(&current)
                .is_err()
        );
    }

    #[test]
    fn violation_message_names_requirement_and_actual_state() {
        let violation = PointerRequirement::VersionIs(1)
            .check(&pointer(2, "s3://b/m.json"))
            .expect_err("must fail");
        let message = violation.to_string();
        assert!(message.contains("pointer version must be 1"), "{message}");
        assert!(message.contains("version 2"), "{message}");
    }
}
