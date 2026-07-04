//! The Postgres-backed commit backend: the production implementation of the
//! commit protocol's pointer store.
//!
//! This is the M1 implementation promised by `docs/design/commit-protocol.md`
//! §9 and by [`meridian_iceberg::commit::CommitBackend`]. One call to
//! [`PostgresCommitBackend::commit_tables`] is one Postgres transaction
//! carrying, in order (§3 steps 5–11):
//!
//! 1. `SELECT … FROM tables WHERE id = ANY($ids) ORDER BY id FOR UPDATE` —
//!    row locks in ascending-id order (deterministic lock order, §4);
//! 2. the in-transaction idempotency-receipt check (§8: the recall outside
//!    the transaction is not authoritative under concurrency);
//! 3. version-guard validation for every operation (all-or-nothing);
//! 4. the guarded pointer `UPDATE`s (the CAS — retained even under the row
//!    lock, defense in depth per §3);
//! 5. index write-through (`tables` row columns + `table_snapshots`);
//! 6. outbox events + hash-chained audit rows sharing one `commit_id`;
//! 7. the idempotency receipt, when a key was supplied;
//! 8. `COMMIT` — the point of no return. An error from the commit statement
//!    itself is surfaced as [`CommitBackendError::StateUnknown`] (failure F3:
//!    the transaction may or may not have applied).
//!
//! Staging the candidate `metadata.json` happens *before* this call, outside
//! the transaction (the optimistic-staging variant that §3 "Locking versus
//! CAS" explicitly allows): the guard, not the lock, carries invariant I1.
//! A lost guard means the staged file is an orphan — cleanup per §7.1.

use std::collections::{BTreeMap, BTreeSet};

use meridian_common::id::WorkspaceId;
use meridian_iceberg::commit::{
    CommitBackend, CommitBackendError, CommitReceipt, CommittedTable, PointerCas, TablePointer,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::types::Json;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry, canonical_json, compute_hash};
use crate::outbox::{self, NewOutboxEvent};

/// How long a recorded idempotency receipt is replayable (design doc §8).
pub const RECEIPT_TTL_HOURS: i32 = 24;

/// One snapshot row for the write-through index, extracted from the new
/// metadata by the caller (this crate does not depend on the metadata
/// model's internals beyond the commit contract).
#[derive(Debug, Clone)]
pub struct SnapshotIndexRow {
    /// Snapshot id.
    pub snapshot_id: i64,
    /// Parent snapshot id, if any.
    pub parent_snapshot_id: Option<i64>,
    /// Commit sequence number (v2+).
    pub sequence_number: Option<i64>,
    /// Commit timestamp (epoch millis).
    pub timestamp_ms: i64,
    /// Manifest-list location.
    pub manifest_list: Option<String>,
    /// Summary `operation`, when present.
    pub operation: Option<String>,
    /// Full snapshot summary.
    pub summary: Value,
    /// Whether this is the table's current snapshot.
    pub is_current: bool,
}

/// Everything derived from the new metadata that is write-through-indexed
/// in the commit transaction (ADR 003).
#[derive(Debug, Clone)]
pub struct DerivedTableState {
    /// Iceberg format version of the new metadata.
    pub format_version: i16,
    /// Table properties of the new metadata.
    pub properties: BTreeMap<String, String>,
    /// The complete retained snapshot set of the new metadata; replaces the
    /// previous index rows (snapshot expiry removes rows).
    pub snapshots: Vec<SnapshotIndexRow>,
    /// Flattened column names and docs of the new metadata's current schema
    /// (see [`crate::search::schema_search_text`]), indexed for full-text
    /// search by the trigger from migration 0010. `None` clears the index
    /// text (a metadata without a resolvable current schema).
    pub schema_text: Option<String>,
    /// Extra detail merged into the audit/outbox payload (snapshot ids,
    /// operation, …).
    pub event_details: Value,
}

/// One table operation within [`PostgresCommitBackend::commit_tables`]: the
/// protocol-level compare-and-set plus the optional write-through state.
///
/// The property harness drives the pure-CAS form (`derived: None`) through
/// the [`CommitBackend`] trait; the API layer always supplies the derived
/// state.
#[derive(Debug, Clone)]
pub struct CommitTableOp {
    /// The pointer compare-and-set.
    pub cas: PointerCas<String>,
    /// Write-through index state derived from the new metadata.
    pub derived: Option<DerivedTableState>,
    /// A contract violation to record **atomically with this pointer swap**
    /// (Pillar E, E-F4). Set by the commit-path pre-commit hook in warn and
    /// quarantine modes — the violation row(s) + event join this transaction,
    /// so they are durable iff the commit is (commit-protocol I4). `None` for
    /// an ordinary commit and for the pure-CAS trait path. Block-mode
    /// violations are *not* carried here: a blocked commit never opens this
    /// transaction, so its record is written separately.
    pub contract_violation: Option<crate::contracts::OwnedViolationRecord>,
}

/// An idempotency receipt ready to record in a commit transaction.
#[derive(Debug, Clone)]
pub struct ReceiptToRecord {
    /// The client-supplied key.
    pub key: String,
    /// Fingerprint of the request the key was used for.
    pub fingerprint: String,
    /// HTTP status of the recorded response.
    pub response_status: i16,
    /// The recorded receipt body.
    pub response_body: Value,
}

impl ReceiptToRecord {
    /// Builds the stored form of a successful commit receipt, in the same
    /// shape [`PostgresCommitBackend::recall_receipt`] parses back.
    #[must_use]
    pub fn new(key: &str, fingerprint: &str, receipt: &CommitReceipt<String>) -> Self {
        Self {
            key: key.to_owned(),
            fingerprint: fingerprint.to_owned(),
            response_status: 200,
            response_body: receipt_to_json(receipt),
        }
    }
}

/// A recorded idempotency receipt, as recalled from the store.
#[derive(Debug, Clone)]
pub struct RecalledReceipt {
    /// Fingerprint of the original request.
    pub fingerprint: String,
    /// The per-table outcomes of the original commit.
    pub receipt: CommitReceipt<String>,
}

/// The Postgres-backed pointer store.
///
/// Carries the workspace scope and acting principal so every commit
/// transaction can write its audit rows (invariant I6) without threading
/// request context through the protocol trait.
#[derive(Debug, Clone)]
pub struct PostgresCommitBackend {
    pool: PgPool,
    workspace_id: WorkspaceId,
    principal: String,
}

impl PostgresCommitBackend {
    /// Builds a backend scoped to a workspace and principal.
    #[must_use]
    pub fn new(pool: PgPool, workspace_id: WorkspaceId, principal: impl Into<String>) -> Self {
        Self {
            pool,
            workspace_id,
            principal: principal.into(),
        }
    }

    /// Recalls a receipt together with its fingerprint (the API layer's
    /// recall — unlike the trait method, callers can detect key reuse with a
    /// different request, failure F9).
    pub async fn recall_receipt(
        &self,
        key: &str,
    ) -> Result<Option<RecalledReceipt>, CommitBackendError> {
        let row: Option<(String, Option<Json<Value>>)> = sqlx::query_as(
            "SELECT request_hash, response_body FROM idempotency_keys
             WHERE workspace_id = $1 AND idempotency_key = $2 AND expires_at > now()",
        )
        .bind(self.workspace_id.to_string())
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;

        match row {
            None => Ok(None),
            Some((fingerprint, body)) => {
                let receipt = receipt_from_json(body.as_ref().map_or(&Value::Null, |b| &b.0))?;
                Ok(Some(RecalledReceipt {
                    fingerprint,
                    receipt,
                }))
            }
        }
    }

    /// Atomically applies all pointer swaps with write-through, or none.
    ///
    /// `idempotency`: `(key, request fingerprint)`. The fingerprint is
    /// compared on replay; a recorded key with a different fingerprint is
    /// [`CommitBackendError::IdempotencyKeyReuse`] (F9).
    ///
    /// This is the one code path in Meridian that moves a table pointer.
    // One transaction, one function: splitting the commit sequence across
    // helpers would hide the ordering the design doc makes normative.
    #[allow(clippy::too_many_lines)]
    pub async fn commit_tables(
        &self,
        ops: &[CommitTableOp],
        idempotency: Option<(&str, &str)>,
    ) -> Result<CommitReceipt<String>, CommitBackendError> {
        // Structural validation before any I/O (§3 step 3).
        if ops.is_empty() {
            return Err(CommitBackendError::EmptyCommit);
        }
        let mut seen = BTreeSet::new();
        for op in ops {
            if !seen.insert(op.cas.table.as_str()) {
                return Err(CommitBackendError::DuplicateTable {
                    table: op.cas.table.clone(),
                });
            }
        }

        let mut tx = self.pool.begin().await.map_err(unavailable)?;

        // Deterministic lock order: ascending table id, locks taken in the
        // order rows are produced (§4). The sorted op list also drives guard
        // validation order, matching the protocol model.
        let mut ordered_ops: Vec<&CommitTableOp> = ops.iter().collect();
        ordered_ops.sort_unstable_by(|a, b| a.cas.table.cmp(&b.cas.table));
        let ordered_ids: Vec<&str> = ordered_ops.iter().map(|op| op.cas.table.as_str()).collect();
        let locked: Vec<(String, i64, Option<String>)> = sqlx::query_as(
            "SELECT id, pointer_version, metadata_location
             FROM tables
             WHERE id = ANY($1)
             ORDER BY id
             FOR UPDATE",
        )
        .bind(&ordered_ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
        let current: BTreeMap<&str, (i64, Option<&str>)> = locked
            .iter()
            .map(|(id, version, location)| (id.as_str(), (*version, location.as_deref())))
            .collect();

        // In-transaction idempotency check, before version validation
        // (contract item 4): a duplicate of a successful commit replays
        // rather than conflicting, even when the recall raced.
        if let Some((key, fingerprint)) = idempotency {
            let recorded: Option<(String, Option<Json<Value>>)> = sqlx::query_as(
                "SELECT request_hash, response_body FROM idempotency_keys
                 WHERE workspace_id = $1 AND idempotency_key = $2 AND expires_at > now()",
            )
            .bind(self.workspace_id.to_string())
            .bind(key)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
            if let Some((recorded_fingerprint, body)) = recorded {
                if recorded_fingerprint == fingerprint {
                    let mut receipt =
                        receipt_from_json(body.as_ref().map_or(&Value::Null, |b| &b.0))?;
                    receipt.replayed = true;
                    return Ok(receipt);
                }
                return Err(CommitBackendError::IdempotencyKeyReuse {
                    key: key.to_owned(),
                });
            }
        }

        // Validate every guard before touching anything (all-or-nothing),
        // in ascending table order.
        for op in &ordered_ops {
            let id = op.cas.table.as_str();
            let Some((version, _)) = current.get(id) else {
                return Err(CommitBackendError::TableNotFound {
                    table: id.to_owned(),
                });
            };
            let actual = u64::try_from(*version).map_err(|_| CommitBackendError::Unavailable {
                message: format!("table {id} has a negative pointer version"),
            })?;
            if actual != op.cas.expected_version {
                return Err(CommitBackendError::VersionConflict {
                    table: id.to_owned(),
                    expected: op.cas.expected_version,
                    actual,
                });
            }
        }

        // Apply. The guarded UPDATE is the CAS from the design doc §2; the
        // guard is retained even though the row lock makes losing it
        // impossible here (defense in depth, §3).
        let commit_id = Ulid::new().to_string();
        let mut committed: Vec<CommittedTable<String>> = Vec::with_capacity(ops.len());
        for op in ops {
            let expected = i64::try_from(op.cas.expected_version).map_err(|_| {
                CommitBackendError::Unavailable {
                    message: format!(
                        "expected version {} does not fit a bigint",
                        op.cas.expected_version
                    ),
                }
            })?;
            let previous_location = current
                .get(op.cas.table.as_str())
                .and_then(|(_, location)| *location);

            let updated = if let Some(derived) = &op.derived {
                sqlx::query(
                    "UPDATE tables
                     SET pointer_version = pointer_version + 1,
                         metadata_location = $1,
                         previous_metadata_location = $2,
                         format_version = $3,
                         properties = $4,
                         schema_text = $5,
                         updated_at = now()
                     WHERE id = $6 AND pointer_version = $7",
                )
                .bind(&op.cas.new_metadata_location)
                .bind(previous_location)
                .bind(derived.format_version)
                .bind(Json(&derived.properties))
                .bind(derived.schema_text.as_deref())
                .bind(&op.cas.table)
                .bind(expected)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?
            } else {
                sqlx::query(
                    "UPDATE tables
                     SET pointer_version = pointer_version + 1,
                         metadata_location = $1,
                         previous_metadata_location = $2,
                         updated_at = now()
                     WHERE id = $3 AND pointer_version = $4",
                )
                .bind(&op.cas.new_metadata_location)
                .bind(previous_location)
                .bind(&op.cas.table)
                .bind(expected)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?
            };
            if updated.rows_affected() != 1 {
                // Unreachable under the row lock; the guard is the actual
                // correctness mechanism and must never be trusted less than
                // the lock.
                return Err(CommitBackendError::VersionConflict {
                    table: op.cas.table.clone(),
                    expected: op.cas.expected_version,
                    actual: op.cas.expected_version, // observed via rows_affected only
                });
            }

            if let Some(derived) = &op.derived {
                write_snapshot_index(&mut tx, &op.cas.table, &derived.snapshots).await?;
            }

            committed.push(CommittedTable {
                table: op.cas.table.clone(),
                version: op.cas.expected_version + 1,
                metadata_location: op.cas.new_metadata_location.clone(),
            });
        }

        // Outbox events and audit rows, one per table, sharing the commit id
        // so multi-table transactions are reconstructable from the log (§4).
        for (op, entry) in ops.iter().zip(&committed) {
            let mut details = json!({
                "commit_id": commit_id,
                "pointer_version": entry.version,
                "metadata_location": entry.metadata_location,
                "previous_metadata_location":
                    current.get(op.cas.table.as_str()).and_then(|(_, l)| *l),
            });
            if let Some(derived) = &op.derived
                && let (Value::Object(target), Value::Object(extra)) =
                    (&mut details, &derived.event_details)
            {
                target.extend(extra.clone());
            }

            outbox::enqueue(
                &mut *tx,
                &NewOutboxEvent {
                    workspace_id: Some(self.workspace_id),
                    aggregate: format!("table:{}", op.cas.table),
                    event_type: "table.committed".to_owned(),
                    payload: details.clone(),
                },
            )
            .await
            .map_err(meridian_to_backend)?;

            audit::append_in_tx(
                &mut tx,
                NewAuditEntry {
                    workspace_id: Some(self.workspace_id),
                    principal: self.principal.clone(),
                    action: "table.commit".to_owned(),
                    resource: format!("table:{}", op.cas.table),
                    details,
                },
            )
            .await
            .map_err(meridian_to_backend)?;

            // Contract violation (warn / quarantine), recorded atomically with
            // this pointer swap (Pillar E, E-F4). Durable iff the commit is —
            // the same guarantee the audit row above carries (I4).
            if let Some(violation) = &op.contract_violation {
                crate::contracts::record_violation_in_tx(
                    &mut tx,
                    self.workspace_id,
                    &violation.as_ref(),
                )
                .await
                .map_err(meridian_to_backend)?;
            }
        }

        let receipt = CommitReceipt {
            tables: committed,
            replayed: false,
        };

        // Idempotency receipt, in this same transaction (§3 step 10). A
        // unique violation means a same-key commit on a *disjoint* table set
        // won the race (same-table racers serialize on the row lock and were
        // caught by the in-transaction check above): roll back and resolve
        // against the winner's recorded receipt.
        if let Some((key, fingerprint)) = idempotency {
            let insert = sqlx::query(
                "INSERT INTO idempotency_keys
                     (workspace_id, idempotency_key, request_hash, response_status,
                      response_body, expires_at)
                 VALUES ($1, $2, $3, 200, $4, now() + make_interval(hours => $5))",
            )
            .bind(self.workspace_id.to_string())
            .bind(key)
            .bind(fingerprint)
            .bind(Json(receipt_to_json(&receipt)))
            .bind(RECEIPT_TTL_HOURS)
            .execute(&mut *tx)
            .await;
            if let Err(error) = insert {
                let unique = error
                    .as_database_error()
                    .is_some_and(sqlx::error::DatabaseError::is_unique_violation);
                // Roll back: nothing from this attempt applies. The failed
                // statement already poisoned the transaction, so a rollback
                // error only means the connection is going away with it.
                if let Err(rollback_error) = tx.rollback().await {
                    tracing::warn!(%rollback_error, "rollback after receipt conflict failed");
                }
                if unique {
                    return self.resolve_key_race(key, fingerprint).await;
                }
                return Err(unavailable(error));
            }
        }

        // The point of no return. Failure *of the commit statement itself*
        // is the one place the outcome is genuinely unknown (F3).
        tx.commit()
            .await
            .map_err(|error| CommitBackendError::StateUnknown {
                message: format!("transaction commit failed: {error}"),
            })?;

        Ok(receipt)
    }

    /// Resolves a lost same-key insert race: replay the winner's receipt if
    /// it recorded the same request, otherwise surface key reuse (§8).
    async fn resolve_key_race(
        &self,
        key: &str,
        fingerprint: &str,
    ) -> Result<CommitReceipt<String>, CommitBackendError> {
        match self.recall_receipt(key).await? {
            Some(recalled) if recalled.fingerprint == fingerprint => {
                let mut receipt = recalled.receipt;
                receipt.replayed = true;
                Ok(receipt)
            }
            Some(_) => Err(CommitBackendError::IdempotencyKeyReuse {
                key: key.to_owned(),
            }),
            // The winner's receipt expired or vanished between the violation
            // and this read; nothing from our attempt applied, so retryable.
            None => Err(CommitBackendError::Unavailable {
                message: format!("idempotency key {key:?} raced and cannot be recalled"),
            }),
        }
    }

    // -----------------------------------------------------------------------
    // Branch-scoped commits (Pillar K, K-F1/K-F2). A branch commit moves a
    // *branch* pointer (branch_table_pointers), never main's tables pointer,
    // via the SAME lock → guard → CAS → audit → outbox sequence as
    // `commit_tables`. There must be exactly one function per pointer kind that
    // moves it; this is that function for branch pointers. See
    // `docs/design/branching.md` §3 for the invariant-preservation argument.
    // -----------------------------------------------------------------------

    /// Loads the pointer a branch commit is built against: the branch's
    /// diverged pointer if the table has diverged, else a fall-through to
    /// main's live pointer. The returned `version` is the branch-local
    /// `pointer_version` when diverged, or main's `pointer_version` when not
    /// (the value the branch CAS will guard against as `expected_base`).
    ///
    /// `TableNotFound` when the table has neither a branch pointer nor a main
    /// pointer (an uncommitted stage-create is not addressable on a branch).
    pub async fn load_branch_pointer(
        &self,
        branch_id: &str,
        table_id: &str,
    ) -> Result<(TablePointer, bool), CommitBackendError> {
        let branch: Option<(i64, String)> = sqlx::query_as(
            "SELECT pointer_version, metadata_location
             FROM branch_table_pointers WHERE branch_id = $1 AND table_id = $2",
        )
        .bind(branch_id)
        .bind(table_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        if let Some((version, location)) = branch {
            let version = u64::try_from(version).map_err(|_| CommitBackendError::Unavailable {
                message: format!("branch pointer for {table_id} has a negative version"),
            })?;
            return Ok((
                TablePointer {
                    version,
                    metadata_location: location,
                },
                true,
            ));
        }
        // Fall through to main.
        let pointer = self.load_pointer(&table_id.to_owned()).await?;
        Ok((pointer, false))
    }

    /// Commits one table to a branch: the branch-pointer compare-and-set.
    ///
    /// - `expected_version`: for a **diverged** table, the branch pointer's
    ///   current `pointer_version`; for a table diverging for the **first
    ///   time**, main's current `pointer_version` (the base it is forking).
    /// - `already_diverged`: which of the two the caller loaded (from
    ///   [`Self::load_branch_pointer`]).
    ///
    /// The sequence mirrors [`Self::commit_tables`] exactly (design doc §3):
    /// lock the base `tables` row `FOR UPDATE` (serializes first-divergence and
    /// drop races) → in-transaction idempotency check → guard → CAS on
    /// `branch_table_pointers` → audit row + outbox event (shared commit id) →
    /// receipt → COMMIT. main's pointer is never in the write set.
    #[allow(clippy::too_many_lines)]
    pub async fn commit_branch_table(
        &self,
        branch_id: &str,
        table_id: &str,
        expected_version: u64,
        already_diverged: bool,
        new_metadata_location: &str,
        idempotency: Option<(&str, &str)>,
    ) -> Result<CommitReceipt<String>, CommitBackendError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;

        // Lock the base table row: this serializes a first-divergence seed
        // read, a concurrent main commit to the same table, and a drop.
        let base_row: Option<(i64,)> =
            sqlx::query_as("SELECT pointer_version FROM tables WHERE id = $1 FOR UPDATE")
                .bind(table_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(unavailable)?;
        let Some((main_version,)) = base_row else {
            return Err(CommitBackendError::TableNotFound {
                table: table_id.to_owned(),
            });
        };

        // In-transaction idempotency check (§3 step 2), before guard.
        if let Some((key, fingerprint)) = idempotency {
            let recorded: Option<(String, Option<Json<Value>>)> = sqlx::query_as(
                "SELECT request_hash, response_body FROM idempotency_keys
                 WHERE workspace_id = $1 AND idempotency_key = $2 AND expires_at > now()",
            )
            .bind(self.workspace_id.to_string())
            .bind(key)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
            if let Some((recorded_fingerprint, body)) = recorded {
                if recorded_fingerprint == fingerprint {
                    let mut receipt =
                        receipt_from_json(body.as_ref().map_or(&Value::Null, |b| &b.0))?;
                    receipt.replayed = true;
                    return Ok(receipt);
                }
                return Err(CommitBackendError::IdempotencyKeyReuse {
                    key: key.to_owned(),
                });
            }
        }

        // Read the current branch pointer under the base-row lock.
        let current_branch: Option<(i64, Option<String>)> = sqlx::query_as(
            "SELECT pointer_version, metadata_location
             FROM branch_table_pointers WHERE branch_id = $1 AND table_id = $2",
        )
        .bind(branch_id)
        .bind(table_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;

        // Guard + CAS. Two shapes: advance an existing branch pointer, or seed
        // the first divergence (INSERT). In both cases the guard is
        // `expected_version`; a mismatch is a VersionConflict → rebase-retry.
        let (new_version, previous_location) = if already_diverged {
            let Some((actual, previous)) = current_branch else {
                // The caller loaded a diverged pointer but the row vanished
                // (a concurrent delete/merge). Retryable as a conflict.
                return Err(CommitBackendError::VersionConflict {
                    table: table_id.to_owned(),
                    expected: expected_version,
                    actual: 0,
                });
            };
            let actual_u = u64::try_from(actual).map_err(|_| CommitBackendError::Unavailable {
                message: format!("branch pointer for {table_id} has a negative version"),
            })?;
            if actual_u != expected_version {
                return Err(CommitBackendError::VersionConflict {
                    table: table_id.to_owned(),
                    expected: expected_version,
                    actual: actual_u,
                });
            }
            let updated = sqlx::query(
                "UPDATE branch_table_pointers
                 SET pointer_version = pointer_version + 1,
                     metadata_location = $1,
                     previous_metadata_location = $2,
                     updated_at = now()
                 WHERE branch_id = $3 AND table_id = $4 AND pointer_version = $5",
            )
            .bind(new_metadata_location)
            .bind(previous.as_deref())
            .bind(branch_id)
            .bind(table_id)
            .bind(actual)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            if updated.rows_affected() != 1 {
                return Err(CommitBackendError::VersionConflict {
                    table: table_id.to_owned(),
                    expected: expected_version,
                    actual: expected_version,
                });
            }
            (expected_version + 1, previous)
        } else {
            // First divergence: main must still be at the base the caller
            // forked (a concurrent main commit that moved past it is a
            // conflict → rebase-retry rebuilds against the new main base).
            let main_u =
                u64::try_from(main_version).map_err(|_| CommitBackendError::Unavailable {
                    message: format!("table {table_id} has a negative pointer version"),
                })?;
            if main_u != expected_version {
                return Err(CommitBackendError::VersionConflict {
                    table: table_id.to_owned(),
                    expected: expected_version,
                    actual: main_u,
                });
            }
            // If a branch row somehow exists (racing first-divergence winner),
            // this INSERT conflicts on the PK → surfaced as a conflict.
            let inserted = sqlx::query(
                "INSERT INTO branch_table_pointers
                     (branch_id, table_id, metadata_location, pointer_version,
                      base_pointer_version, previous_metadata_location)
                 VALUES ($1, $2, $3, 0, $4, NULL)
                 ON CONFLICT (branch_id, table_id) DO NOTHING",
            )
            .bind(branch_id)
            .bind(table_id)
            .bind(new_metadata_location)
            .bind(main_version)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            if inserted.rows_affected() != 1 {
                return Err(CommitBackendError::VersionConflict {
                    table: table_id.to_owned(),
                    expected: expected_version,
                    actual: expected_version,
                });
            }
            (0, None)
        };

        let commit_id = Ulid::new().to_string();
        let details = json!({
            "commit_id": commit_id,
            "branch_id": branch_id,
            "pointer_version": new_version,
            "metadata_location": new_metadata_location,
            "previous_metadata_location": previous_location,
        });
        outbox::enqueue(
            &mut *tx,
            &NewOutboxEvent {
                workspace_id: Some(self.workspace_id),
                aggregate: format!("branch:{branch_id}/table:{table_id}"),
                event_type: "table.branch_committed".to_owned(),
                payload: details.clone(),
            },
        )
        .await
        .map_err(meridian_to_backend)?;
        audit::append_in_tx(
            &mut tx,
            NewAuditEntry {
                workspace_id: Some(self.workspace_id),
                principal: self.principal.clone(),
                action: "table.branch_commit".to_owned(),
                resource: format!("branch:{branch_id}/table:{table_id}"),
                details,
            },
        )
        .await
        .map_err(meridian_to_backend)?;

        let receipt = CommitReceipt {
            tables: vec![CommittedTable {
                table: table_id.to_owned(),
                version: new_version,
                metadata_location: new_metadata_location.to_owned(),
            }],
            replayed: false,
        };

        if let Some((key, fingerprint)) = idempotency {
            let insert = sqlx::query(
                "INSERT INTO idempotency_keys
                     (workspace_id, idempotency_key, request_hash, response_status,
                      response_body, expires_at)
                 VALUES ($1, $2, $3, 200, $4, now() + make_interval(hours => $5))",
            )
            .bind(self.workspace_id.to_string())
            .bind(key)
            .bind(fingerprint)
            .bind(Json(receipt_to_json(&receipt)))
            .bind(RECEIPT_TTL_HOURS)
            .execute(&mut *tx)
            .await;
            if let Err(error) = insert {
                let unique = error
                    .as_database_error()
                    .is_some_and(sqlx::error::DatabaseError::is_unique_violation);
                if let Err(rollback_error) = tx.rollback().await {
                    tracing::warn!(%rollback_error, "rollback after branch receipt conflict failed");
                }
                if unique {
                    return self.resolve_key_race(key, fingerprint).await;
                }
                return Err(unavailable(error));
            }
        }

        tx.commit()
            .await
            .map_err(|error| CommitBackendError::StateUnknown {
                message: format!("branch transaction commit failed: {error}"),
            })?;

        Ok(receipt)
    }

    /// Fast-forwards main from a branch head for one table, **through main's
    /// own CAS** (the merge apply step, branching.md §6). This is an ordinary
    /// main commit whose new metadata is the branch's head file; it carries
    /// every main invariant. `expected_main_version` is main's current pointer
    /// version (the merge-base check already confirmed main has not advanced
    /// past the divergence base; a concurrent main commit that lands here makes
    /// the guard fail → the caller reports a newly-conflicting table rather
    /// than clobbering).
    pub async fn fast_forward_main(
        &self,
        table_id: &str,
        expected_main_version: u64,
        branch_metadata_location: &str,
        derived: DerivedTableState,
    ) -> Result<CommitReceipt<String>, CommitBackendError> {
        let op = CommitTableOp {
            cas: PointerCas {
                table: table_id.to_owned(),
                expected_version: expected_main_version,
                new_metadata_location: branch_metadata_location.to_owned(),
            },
            derived: Some(derived),
            contract_violation: None,
        };
        self.commit_tables(std::slice::from_ref(&op), None).await
    }
}

impl CommitBackend for PostgresCommitBackend {
    type TableId = String;

    async fn load_pointer(&self, table: &String) -> Result<TablePointer, CommitBackendError> {
        let row: Option<(i64, Option<String>)> =
            sqlx::query_as("SELECT pointer_version, metadata_location FROM tables WHERE id = $1")
                .bind(table)
                .fetch_optional(&self.pool)
                .await
                .map_err(unavailable)?;
        let Some((version, location)) = row else {
            return Err(CommitBackendError::TableNotFound {
                table: table.clone(),
            });
        };
        let version = u64::try_from(version).map_err(|_| CommitBackendError::Unavailable {
            message: format!("table {table} has a negative pointer version"),
        })?;
        // A committed table always has a location (creation writes the file
        // before the row); a NULL here is store corruption, not absence.
        let metadata_location = location.ok_or_else(|| CommitBackendError::Unavailable {
            message: format!("table {table} has no metadata location"),
        })?;
        Ok(TablePointer {
            version,
            metadata_location,
        })
    }

    async fn recall_idempotency_key(
        &self,
        key: &str,
    ) -> Result<Option<CommitReceipt<String>>, CommitBackendError> {
        Ok(self.recall_receipt(key).await?.map(|r| r.receipt))
    }

    async fn commit_atomic(
        &self,
        ops: &[PointerCas<String>],
        idempotency_key: Option<&str>,
    ) -> Result<CommitReceipt<String>, CommitBackendError> {
        let ops: Vec<CommitTableOp> = ops
            .iter()
            .map(|cas| CommitTableOp {
                cas: cas.clone(),
                derived: None,
                contract_violation: None,
            })
            .collect();
        // The trait carries no request identity, so the fingerprint is the
        // operation list itself — the same semantics the protocol model
        // (`MockCatalog`) implements.
        let fingerprint = idempotency_key.map(|_| ops_fingerprint(&ops));
        let idempotency = match (idempotency_key, &fingerprint) {
            (Some(key), Some(fp)) => Some((key, fp.as_str())),
            _ => None,
        };
        self.commit_tables(&ops, idempotency).await
    }
}

/// Replaces the snapshot index rows for a table from the new metadata.
pub(crate) async fn write_snapshot_index(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table_id: &str,
    snapshots: &[SnapshotIndexRow],
) -> Result<(), CommitBackendError> {
    sqlx::query("DELETE FROM table_snapshots WHERE table_id = $1")
        .bind(table_id)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    for snapshot in snapshots {
        sqlx::query(
            "INSERT INTO table_snapshots
                 (table_id, snapshot_id, parent_snapshot_id, sequence_number, timestamp_ms,
                  manifest_list, operation, summary, is_current)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(table_id)
        .bind(snapshot.snapshot_id)
        .bind(snapshot.parent_snapshot_id)
        .bind(snapshot.sequence_number)
        .bind(snapshot.timestamp_ms)
        .bind(&snapshot.manifest_list)
        .bind(&snapshot.operation)
        .bind(&snapshot.summary)
        .bind(snapshot.is_current)
        .execute(&mut **tx)
        .await
        .map_err(unavailable)?;
    }
    Ok(())
}

/// The stored JSON form of a commit receipt.
fn receipt_to_json(receipt: &CommitReceipt<String>) -> Value {
    json!({
        "tables": receipt
            .tables
            .iter()
            .map(|t| {
                json!({
                    "table": t.table,
                    "version": t.version,
                    "metadata_location": t.metadata_location,
                })
            })
            .collect::<Vec<_>>(),
    })
}

/// Parses a stored receipt body back into a [`CommitReceipt`].
fn receipt_from_json(value: &Value) -> Result<CommitReceipt<String>, CommitBackendError> {
    let malformed = || CommitBackendError::Unavailable {
        message: "recorded idempotency receipt is malformed".to_owned(),
    };
    let tables = value
        .get("tables")
        .and_then(Value::as_array)
        .ok_or_else(malformed)?
        .iter()
        .map(|entry| {
            Ok(CommittedTable {
                table: entry
                    .get("table")
                    .and_then(Value::as_str)
                    .ok_or_else(malformed)?
                    .to_owned(),
                version: entry
                    .get("version")
                    .and_then(Value::as_u64)
                    .ok_or_else(malformed)?,
                metadata_location: entry
                    .get("metadata_location")
                    .and_then(Value::as_str)
                    .ok_or_else(malformed)?
                    .to_owned(),
            })
        })
        .collect::<Result<Vec<_>, CommitBackendError>>()?;
    Ok(CommitReceipt {
        tables,
        replayed: false,
    })
}

/// Fingerprint of an operation list: sha-256 over its canonical JSON.
fn ops_fingerprint(ops: &[CommitTableOp]) -> String {
    let value = json!(
        ops.iter()
            .map(|op| {
                json!({
                    "table": op.cas.table,
                    "expected_version": op.cas.expected_version,
                    "new_metadata_location": op.cas.new_metadata_location,
                })
            })
            .collect::<Vec<_>>()
    );
    compute_hash(None, &canonical_json(&value))
}

/// Maps a sqlx failure inside the commit path onto the protocol error model.
///
/// Everything before the final `COMMIT` rolls back cleanly, so these are all
/// "nothing was applied, retry later" — never `StateUnknown`.
#[allow(clippy::needless_pass_by_value)] // by-value so `map_err(unavailable)` reads cleanly
fn unavailable(error: sqlx::Error) -> CommitBackendError {
    CommitBackendError::Unavailable {
        message: format!("commit transaction failed: {error}"),
    }
}

/// Maps store-layer errors (audit/outbox helpers) onto the protocol error
/// model; same pre-commit "nothing applied" semantics as [`unavailable`].
#[allow(clippy::needless_pass_by_value)] // by-value so `map_err(meridian_to_backend)` reads cleanly
fn meridian_to_backend(error: meridian_common::MeridianError) -> CommitBackendError {
    CommitBackendError::Unavailable {
        message: format!("commit transaction failed: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_round_trips_through_json() {
        let receipt = CommitReceipt {
            tables: vec![
                CommittedTable {
                    table: "01A".to_owned(),
                    version: 7,
                    metadata_location: "s3://b/t/metadata/00007-x.metadata.json".to_owned(),
                },
                CommittedTable {
                    table: "01B".to_owned(),
                    version: 1,
                    metadata_location: "s3://b/u/metadata/00001-y.metadata.json".to_owned(),
                },
            ],
            replayed: false,
        };
        let parsed = receipt_from_json(&receipt_to_json(&receipt)).expect("round trip");
        assert_eq!(parsed, receipt);
    }

    #[test]
    fn malformed_receipt_is_rejected() {
        assert!(receipt_from_json(&json!({})).is_err());
        assert!(receipt_from_json(&json!({"tables": [{"table": 1}]})).is_err());
    }

    #[test]
    fn ops_fingerprint_is_stable_and_order_sensitive() {
        let op = |table: &str, version: u64| CommitTableOp {
            cas: PointerCas {
                table: table.to_owned(),
                expected_version: version,
                new_metadata_location: format!("s3://b/{table}/m.json"),
            },
            derived: None,
            contract_violation: None,
        };
        let a = ops_fingerprint(&[op("t1", 1), op("t2", 2)]);
        assert_eq!(a, ops_fingerprint(&[op("t1", 1), op("t2", 2)]));
        assert_ne!(a, ops_fingerprint(&[op("t2", 2), op("t1", 1)]));
        assert_ne!(a, ops_fingerprint(&[op("t1", 2), op("t2", 2)]));
    }
}
