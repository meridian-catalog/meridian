//! Property-based tests for the commit protocol model.
//!
//! This file is the executable form of the contract in
//! `docs/design/commit-protocol.md` §9. It runs the protocol against
//! [`MockCatalog`], a minimal in-memory [`CommitBackend`]: a map of
//! version-stamped pointers plus an idempotency-receipt map, with
//! `commit_atomic` applied under one mutex (the model of "one Postgres
//! transaction").
//!
//! Concurrency is modelled with *generated schedules*: proptest generates
//! arbitrary interleavings of the individual backend interactions (load,
//! compare-and-set) of many committers, so the lost-update window between a
//! committer's load and its swap is exercised deterministically and
//! shrinkably — strictly more adversarially than the real server, which
//! additionally holds a row lock across that window.
//!
//! Properties (numbering from the design doc):
//! - (a) no lost updates under arbitrary interleavings (invariants I1, I3);
//! - (b) a stale requirement always fails and changes nothing (F7);
//! - (c) idempotent replay: same key, same receipt, no double-apply (I5, F4);
//! - (d) multi-table commits are all-or-nothing (I2, F10).
//!
//! **Contract for M1:** the Postgres-backed `CommitBackend` must pass this
//! same suite through this same trait. What the model deliberately does not
//! cover — requirement evaluation against real `TableMetadata`, index
//! write-through, audit/outbox rows, real storage I/O, crash injection —
//! lands with the store-backed tests and the chaos suite.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use meridian_iceberg::commit::{
    CommitBackend, CommitBackendError, CommitError, CommitReceipt, CommittedTable, PointerCas,
    PointerRequirement, TablePointer, commit_single_table,
};
use proptest::prelude::*;

/// Completes a future that never actually waits (every mock future is ready
/// on first poll, and the driver only awaits mock futures).
///
/// # Panics
///
/// Panics if the future returns `Poll::Pending` — that would mean the model
/// grew a real await point and the tests need a real executor.
fn block_on_ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("mock commit futures must complete without waiting"),
    }
}

// ---------------------------------------------------------------------------
// MockCatalog: the in-memory protocol model
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct MockState {
    tables: BTreeMap<u32, TablePointer>,
    /// key → (request fingerprint, recorded receipt).
    idempotency: HashMap<String, (Vec<PointerCas<u32>>, CommitReceipt<u32>)>,
    /// Append-only log of every applied pointer swap, for invariant checks.
    log: Vec<CommittedTable<u32>>,
}

/// In-memory model of the pointer store: versioned pointers, atomic
/// multi-table compare-and-set, idempotency receipts.
#[derive(Debug, Default)]
struct MockCatalog {
    state: std::sync::Mutex<MockState>,
}

impl MockCatalog {
    fn with_tables<I>(tables: I) -> Self
    where
        I: IntoIterator<Item = (u32, TablePointer)>,
    {
        let catalog = Self::default();
        catalog
            .state
            .lock()
            .expect("mock state lock poisoned")
            .tables
            .extend(tables);
        catalog
    }

    fn with_table(table: u32, version: u64, location: &str) -> Self {
        Self::with_tables([(
            table,
            TablePointer {
                version,
                metadata_location: location.to_owned(),
            },
        )])
    }

    /// Current pointer of `table`; panics if the table does not exist.
    fn pointer(&self, table: u32) -> TablePointer {
        self.state
            .lock()
            .expect("mock state lock poisoned")
            .tables
            .get(&table)
            .cloned()
            .expect("table must exist in mock catalog")
    }

    /// Snapshot of the append-only commit log.
    fn log(&self) -> Vec<CommittedTable<u32>> {
        self.state
            .lock()
            .expect("mock state lock poisoned")
            .log
            .clone()
    }
}

impl CommitBackend for MockCatalog {
    type TableId = u32;

    async fn load_pointer(&self, table: &u32) -> Result<TablePointer, CommitBackendError> {
        let state = self.state.lock().expect("mock state lock poisoned");
        state
            .tables
            .get(table)
            .cloned()
            .ok_or(CommitBackendError::TableNotFound {
                table: table.to_string(),
            })
    }

    async fn recall_idempotency_key(
        &self,
        key: &str,
    ) -> Result<Option<CommitReceipt<u32>>, CommitBackendError> {
        let state = self.state.lock().expect("mock state lock poisoned");
        Ok(state
            .idempotency
            .get(key)
            .map(|(_, receipt)| receipt.clone()))
    }

    async fn commit_atomic(
        &self,
        ops: &[PointerCas<u32>],
        idempotency_key: Option<&str>,
    ) -> Result<CommitReceipt<u32>, CommitBackendError> {
        // One mutex acquisition == the model of one Postgres transaction.
        let mut state = self.state.lock().expect("mock state lock poisoned");

        if ops.is_empty() {
            return Err(CommitBackendError::EmptyCommit);
        }
        let mut seen = BTreeSet::new();
        for op in ops {
            if !seen.insert(op.table) {
                return Err(CommitBackendError::DuplicateTable {
                    table: op.table.to_string(),
                });
            }
        }

        // Idempotency check strictly before version validation, so a
        // duplicate of a successful commit replays instead of conflicting
        // (contract item 4).
        if let Some(key) = idempotency_key
            && let Some((fingerprint, receipt)) = state.idempotency.get(key)
        {
            if fingerprint == ops {
                let mut replay = receipt.clone();
                replay.replayed = true;
                return Ok(replay);
            }
            return Err(CommitBackendError::IdempotencyKeyReuse {
                key: key.to_owned(),
            });
        }

        // Validate every guard before touching anything (all-or-nothing),
        // in ascending table order — the model of
        // `SELECT ... ORDER BY id FOR UPDATE` (contract items 1–3).
        let mut ordered: Vec<&PointerCas<u32>> = ops.iter().collect();
        ordered.sort_by_key(|op| op.table);
        for op in &ordered {
            let pointer = state
                .tables
                .get(&op.table)
                .ok_or(CommitBackendError::TableNotFound {
                    table: op.table.to_string(),
                })?;
            if pointer.version != op.expected_version {
                return Err(CommitBackendError::VersionConflict {
                    table: op.table.to_string(),
                    expected: op.expected_version,
                    actual: pointer.version,
                });
            }
        }

        // Apply. Receipt entries follow submission order (client-facing).
        let mut tables = Vec::with_capacity(ops.len());
        for op in ops {
            let pointer = state
                .tables
                .get_mut(&op.table)
                .expect("validated above: table exists");
            pointer.version += 1;
            pointer
                .metadata_location
                .clone_from(&op.new_metadata_location);
            let committed = CommittedTable {
                table: op.table,
                version: pointer.version,
                metadata_location: op.new_metadata_location.clone(),
            };
            state.log.push(committed.clone());
            tables.push(committed);
        }

        let receipt = CommitReceipt {
            tables,
            replayed: false,
        };
        if let Some(key) = idempotency_key {
            state
                .idempotency
                .insert(key.to_owned(), (ops.to_vec(), receipt.clone()));
        }
        Ok(receipt)
    }
}

// ---------------------------------------------------------------------------
// Scheduled committers: deterministic interleaving of protocol steps
// ---------------------------------------------------------------------------

const TABLE: u32 = 7;
const INITIAL_LOCATION: &str = "s3://bucket/metadata/00000-initial.metadata.json";

#[derive(Debug)]
enum Phase {
    NeedLoad,
    ReadyToSwap { base: TablePointer },
    Done,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Committed(CommittedTable<u32>),
    RequirementFailed,
    Exhausted,
}

/// One committer running the protocol loop step by step, where a "step" is
/// exactly one backend interaction — the unit the generated schedule
/// interleaves. The steps mirror `commit_single_table`; splitting them makes
/// the load→swap race window explicit and schedulable.
#[derive(Debug)]
struct ScriptedCommitter {
    label: usize,
    requirements: Vec<PointerRequirement>,
    max_attempts: u32,
    attempts: u32,
    phase: Phase,
    outcome: Option<Outcome>,
}

impl ScriptedCommitter {
    fn new(label: usize, requirements: Vec<PointerRequirement>, max_attempts: u32) -> Self {
        Self {
            label,
            requirements,
            max_attempts,
            attempts: 0,
            phase: Phase::NeedLoad,
            outcome: None,
        }
    }

    fn done(&self) -> bool {
        matches!(self.phase, Phase::Done)
    }

    /// Advances by exactly one backend interaction.
    fn step(&mut self, catalog: &MockCatalog) {
        match std::mem::replace(&mut self.phase, Phase::Done) {
            Phase::NeedLoad => {
                let base = block_on_ready(catalog.load_pointer(&TABLE)).expect("table must exist");
                if self.requirements.iter().any(|r| r.check(&base).is_err()) {
                    self.outcome = Some(Outcome::RequirementFailed);
                    return; // phase stays Done
                }
                self.attempts += 1;
                self.phase = Phase::ReadyToSwap { base };
            }
            Phase::ReadyToSwap { base } => {
                let op = PointerCas {
                    table: TABLE,
                    expected_version: base.version,
                    new_metadata_location: format!(
                        "s3://bucket/metadata/{:05}-committer{}-attempt{}.metadata.json",
                        base.version + 1,
                        self.label,
                        self.attempts,
                    ),
                };
                match block_on_ready(catalog.commit_atomic(std::slice::from_ref(&op), None)) {
                    Ok(receipt) => {
                        let committed = receipt
                            .tables
                            .into_iter()
                            .next()
                            .expect("single-table receipt has one entry");
                        self.outcome = Some(Outcome::Committed(committed));
                    }
                    Err(CommitBackendError::VersionConflict { .. }) => {
                        if self.attempts >= self.max_attempts {
                            self.outcome = Some(Outcome::Exhausted);
                        } else {
                            self.phase = Phase::NeedLoad;
                        }
                    }
                    Err(other) => panic!("unexpected backend error in model: {other}"),
                }
            }
            Phase::Done => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

proptest! {
    /// Property (a): under arbitrary interleavings of many committers'
    /// load/swap steps, updates are never lost — the final version delta
    /// equals the number of successful commits, the commit log is gapless,
    /// and successful receipts match the log one-to-one (invariants I1, I3).
    #[test]
    fn interleaved_commits_never_lose_updates(
        initial_version in 0u64..1_000,
        // (has_stale_requirement, retry budget) per committer.
        specs in prop::collection::vec((any::<bool>(), 1u32..4), 2..6),
        schedule in prop::collection::vec(any::<prop::sample::Index>(), 0..120),
    ) {
        let catalog = MockCatalog::with_table(TABLE, initial_version, INITIAL_LOCATION);
        let mut committers: Vec<ScriptedCommitter> = specs
            .iter()
            .enumerate()
            .map(|(label, &(stale, max_attempts))| {
                // "Stale" committers pin the initial version: legitimate for
                // the first commit, guaranteed stale after it.
                let requirements = if stale {
                    vec![PointerRequirement::VersionIs(initial_version)]
                } else {
                    Vec::new()
                };
                ScriptedCommitter::new(label, requirements, max_attempts)
            })
            .collect();

        // Interleave per the generated schedule...
        for index in &schedule {
            let position = index.index(committers.len());
            let committer = &mut committers[position];
            if !committer.done() {
                committer.step(&catalog);
            }
        }
        // ...then drain round-robin so every committer reaches an outcome
        // (termination: attempts are bounded).
        loop {
            let mut progressed = false;
            for committer in &mut committers {
                if !committer.done() {
                    committer.step(&catalog);
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }

        let successes: Vec<&CommittedTable<u32>> = committers
            .iter()
            .filter_map(|c| match &c.outcome {
                Some(Outcome::Committed(t)) => Some(t),
                _ => None,
            })
            .collect();
        let success_count =
            u64::try_from(successes.len()).expect("committer count fits in u64");
        let log = catalog.log();
        let final_pointer = catalog.pointer(TABLE);

        // I1: accounting closes — no lost, no phantom updates.
        prop_assert_eq!(final_pointer.version, initial_version + success_count);
        prop_assert_eq!(log.len(), successes.len());

        // I3: the log is gapless and strictly monotonic.
        for (i, entry) in log.iter().enumerate() {
            let offset = u64::try_from(i).expect("log index fits in u64");
            prop_assert_eq!(entry.version, initial_version + offset + 1);
        }

        // Every successful receipt appears in the log exactly once
        // (staged locations are unique by construction).
        let logged: BTreeSet<(u64, &str)> = log
            .iter()
            .map(|e| (e.version, e.metadata_location.as_str()))
            .collect();
        prop_assert_eq!(logged.len(), log.len());
        for receipt in &successes {
            prop_assert!(
                logged.contains(&(receipt.version, receipt.metadata_location.as_str())),
                "receipt {receipt:?} missing from commit log"
            );
        }

        // The final pointer is the last committed swap (or untouched).
        if let Some(last) = log.last() {
            prop_assert_eq!(&final_pointer.metadata_location, &last.metadata_location);
        } else {
            prop_assert_eq!(final_pointer.metadata_location.as_str(), INITIAL_LOCATION);
        }

        // A committer pinning the initial version can only ever have won
        // the very first swap.
        for (committer, &(stale, _)) in committers.iter().zip(&specs) {
            if let (true, Some(Outcome::Committed(t))) = (stale, &committer.outcome) {
                prop_assert_eq!(t.version, initial_version + 1);
            }
        }
    }

    /// Property (b): a commit built against stale state always fails with a
    /// requirement violation and changes nothing (F7).
    #[test]
    fn stale_requirement_always_fails(
        initial_version in 0u64..1_000,
        advances in 1u32..8,
        pin_location in any::<bool>(),
    ) {
        let catalog = MockCatalog::with_table(TABLE, initial_version, INITIAL_LOCATION);

        // Advance the table past the state the requirement will pin.
        for i in 0..advances {
            let current = catalog.pointer(TABLE);
            let op = PointerCas {
                table: TABLE,
                expected_version: current.version,
                new_metadata_location: format!(
                    "s3://bucket/metadata/{:05}-advance{}.metadata.json",
                    current.version + 1,
                    i,
                ),
            };
            block_on_ready(catalog.commit_atomic(std::slice::from_ref(&op), None))
                .expect("advancing commit must succeed");
        }

        let before_pointer = catalog.pointer(TABLE);
        let before_log = catalog.log();
        let requirement = if pin_location {
            PointerRequirement::MetadataLocationIs(INITIAL_LOCATION.to_owned())
        } else {
            PointerRequirement::VersionIs(initial_version)
        };

        let result = block_on_ready(commit_single_table(
            &catalog,
            TABLE,
            std::slice::from_ref(&requirement),
            |_base| "s3://bucket/metadata/99999-stale-attempt.metadata.json".to_owned(),
            None,
            NonZeroU32::new(3).expect("nonzero"),
        ));

        prop_assert!(
            matches!(result, Err(CommitError::RequirementFailed(_))),
            "stale requirement must be rejected, got {result:?}"
        );
        prop_assert_eq!(catalog.pointer(TABLE), before_pointer);
        prop_assert_eq!(catalog.log(), before_log);
    }

    /// Property (c): retrying with the same idempotency key replays the
    /// recorded receipt and applies nothing — even though the retry's
    /// requirements are stale by then, because its own first attempt moved
    /// the table (I5, F4).
    #[test]
    fn idempotent_retry_replays_without_double_apply(
        initial_version in 0u64..1_000,
        key in "[a-z0-9]{8,16}",
    ) {
        let catalog = MockCatalog::with_table(TABLE, initial_version, INITIAL_LOCATION);
        let requirements = [PointerRequirement::VersionIs(initial_version)];
        let one_attempt = NonZeroU32::new(1).expect("nonzero");

        let first = block_on_ready(commit_single_table(
            &catalog,
            TABLE,
            &requirements,
            |base| format!(
                "s3://bucket/metadata/{:05}-first.metadata.json",
                base.version + 1,
            ),
            Some(&key),
            one_attempt,
        ))
        .expect("first commit succeeds");
        prop_assert!(!first.replayed);

        let after_first_pointer = catalog.pointer(TABLE);
        let after_first_log = catalog.log();
        prop_assert_eq!(after_first_pointer.version, initial_version + 1);

        // The retry would stage a different file and its requirement is now
        // stale — replay must short-circuit before both.
        let second = block_on_ready(commit_single_table(
            &catalog,
            TABLE,
            &requirements,
            |_base| "s3://bucket/metadata/would-be-a-double-apply.metadata.json".to_owned(),
            Some(&key),
            one_attempt,
        ))
        .expect("retry with the same key replays");

        prop_assert!(second.replayed);
        prop_assert_eq!(&second.tables, &first.tables);
        prop_assert_eq!(catalog.pointer(TABLE), after_first_pointer);
        prop_assert_eq!(catalog.log(), after_first_log);
    }

    /// Property (d): a multi-table commit applies every pointer swap or none
    /// (I2, F10), regardless of submission order.
    #[test]
    fn multi_table_commit_is_all_or_nothing(
        // (initial version, is_stale) per table.
        tables in prop::collection::vec((0u64..1_000, any::<bool>()), 2..6),
    ) {
        let ids: Vec<u32> =
            (0..u32::try_from(tables.len()).expect("few tables")).collect();
        let catalog = MockCatalog::with_tables(ids.iter().zip(&tables).map(
            |(&id, &(version, _))| {
                (
                    id,
                    TablePointer {
                        version,
                        metadata_location: format!(
                            "s3://bucket/t{id}/metadata/{version:05}-initial.metadata.json"
                        ),
                    },
                )
            },
        ));
        let before: Vec<TablePointer> = ids.iter().map(|&id| catalog.pointer(id)).collect();

        // Submit in reverse id order: the backend must order internally.
        let ops: Vec<PointerCas<u32>> = ids
            .iter()
            .zip(&tables)
            .rev()
            .map(|(&id, &(version, stale))| PointerCas {
                table: id,
                expected_version: if stale { version + 1 } else { version },
                new_metadata_location: format!(
                    "s3://bucket/t{id}/metadata/{:05}-txn.metadata.json",
                    version + 1,
                ),
            })
            .collect();
        let any_stale = tables.iter().any(|&(_, stale)| stale);

        let result = block_on_ready(catalog.commit_atomic(&ops, None));

        if any_stale {
            prop_assert!(
                matches!(result, Err(CommitBackendError::VersionConflict { .. })),
                "stale guard must fail the whole transaction, got {result:?}"
            );
            // Nothing changed — for any table.
            for (&id, expected) in ids.iter().zip(&before) {
                prop_assert_eq!(&catalog.pointer(id), expected);
            }
            prop_assert!(catalog.log().is_empty());
        } else {
            let receipt = result.expect("all guards hold: transaction commits");
            prop_assert!(!receipt.replayed);
            // Receipt follows submission order.
            for (entry, op) in receipt.tables.iter().zip(&ops) {
                prop_assert_eq!(entry.table, op.table);
                prop_assert_eq!(entry.version, op.expected_version + 1);
                prop_assert_eq!(&entry.metadata_location, &op.new_metadata_location);
            }
            // Every pointer advanced by exactly one.
            for (&id, previous) in ids.iter().zip(&before) {
                let now = catalog.pointer(id);
                prop_assert_eq!(now.version, previous.version + 1);
                prop_assert_ne!(&now.metadata_location, &previous.metadata_location);
            }
            prop_assert_eq!(catalog.log().len(), ids.len());
        }
    }
}

// ---------------------------------------------------------------------------
// Deterministic edge cases
// ---------------------------------------------------------------------------

#[test]
fn empty_commit_is_rejected() {
    let catalog = MockCatalog::with_table(TABLE, 0, INITIAL_LOCATION);
    let result = block_on_ready(catalog.commit_atomic(&[], None));
    assert_eq!(result, Err(CommitBackendError::EmptyCommit));
    assert!(catalog.log().is_empty());
}

#[test]
fn duplicate_table_in_one_commit_is_rejected() {
    let catalog = MockCatalog::with_table(TABLE, 0, INITIAL_LOCATION);
    let op = |location: &str| PointerCas {
        table: TABLE,
        expected_version: 0,
        new_metadata_location: location.to_owned(),
    };
    let result = block_on_ready(catalog.commit_atomic(&[op("s3://a"), op("s3://b")], None));
    assert_eq!(
        result,
        Err(CommitBackendError::DuplicateTable {
            table: TABLE.to_string()
        })
    );
    assert_eq!(catalog.pointer(TABLE).version, 0);
}

#[test]
fn unknown_table_is_reported() {
    let catalog = MockCatalog::with_table(TABLE, 0, INITIAL_LOCATION);
    let missing = TABLE + 1;

    let load = block_on_ready(catalog.load_pointer(&missing));
    assert_eq!(
        load,
        Err(CommitBackendError::TableNotFound {
            table: missing.to_string()
        })
    );

    let op = PointerCas {
        table: missing,
        expected_version: 0,
        new_metadata_location: "s3://bucket/metadata/00001-x.metadata.json".to_owned(),
    };
    let commit = block_on_ready(catalog.commit_atomic(std::slice::from_ref(&op), None));
    assert_eq!(
        commit,
        Err(CommitBackendError::TableNotFound {
            table: missing.to_string()
        })
    );
}

#[test]
fn reusing_a_key_for_a_different_commit_is_rejected() {
    let catalog = MockCatalog::with_table(TABLE, 0, INITIAL_LOCATION);
    let first = PointerCas {
        table: TABLE,
        expected_version: 0,
        new_metadata_location: "s3://bucket/metadata/00001-a.metadata.json".to_owned(),
    };
    block_on_ready(catalog.commit_atomic(std::slice::from_ref(&first), Some("key-1")))
        .expect("first commit succeeds");

    // Same key, different request: must fail loudly, not replay or apply.
    let second = PointerCas {
        table: TABLE,
        expected_version: 1,
        new_metadata_location: "s3://bucket/metadata/00002-b.metadata.json".to_owned(),
    };
    let result =
        block_on_ready(catalog.commit_atomic(std::slice::from_ref(&second), Some("key-1")));
    assert_eq!(
        result,
        Err(CommitBackendError::IdempotencyKeyReuse {
            key: "key-1".to_owned()
        })
    );
    assert_eq!(catalog.pointer(TABLE).version, 1);
    assert_eq!(catalog.log().len(), 1);
}

#[test]
fn retries_are_bounded_under_perpetual_contention() {
    let catalog = MockCatalog::with_table(TABLE, 0, INITIAL_LOCATION);
    let mut interlopers = 0u32;

    // The staging callback sneaks a competing commit in between the driver's
    // load and its swap — a deterministic always-lose race.
    let result = block_on_ready(commit_single_table(
        &catalog,
        TABLE,
        &[],
        |base| {
            interlopers += 1;
            let op = PointerCas {
                table: TABLE,
                expected_version: base.version,
                new_metadata_location: format!(
                    "s3://bucket/metadata/{:05}-interloper{}.metadata.json",
                    base.version + 1,
                    interlopers,
                ),
            };
            block_on_ready(catalog.commit_atomic(std::slice::from_ref(&op), None))
                .expect("interloper commit succeeds");
            format!("s3://bucket/metadata/victim-attempt{interlopers}.metadata.json")
        },
        None,
        NonZeroU32::new(3).expect("nonzero"),
    ));

    assert_eq!(result, Err(CommitError::RetriesExhausted { attempts: 3 }));
    // Exactly the three interloper commits landed; none from the victim.
    let final_pointer = catalog.pointer(TABLE);
    assert_eq!(final_pointer.version, 3);
    assert!(final_pointer.metadata_location.contains("interloper"));
    assert!(
        catalog
            .log()
            .iter()
            .all(|e| !e.metadata_location.contains("victim"))
    );
}
