//! The shared, backend-generic commit-protocol property harness.
//!
//! This module is the executable form of the contract in
//! `docs/design/commit-protocol.md` §9, written once and instantiated
//! against **every** [`CommitBackend`] implementation:
//!
//! - `meridian-iceberg/tests/commit_properties.rs` runs it against
//!   [`MockCatalog`], the minimal in-memory protocol model (M0);
//! - `meridian-store/tests/commit_properties_pg.rs` runs the identical
//!   properties against the production `PostgresCommitBackend` (M1), as the
//!   design doc requires ("the M1 Postgres-backed `CommitBackend` must pass
//!   this identical suite through the same trait").
//!
//! It is plain test source shared by `mod`/`#[path]` inclusion — not a
//! library module — so proptest stays a dev-dependency everywhere.
//!
//! Concurrency is modelled with *generated schedules*: proptest generates
//! arbitrary interleavings of the individual backend interactions (load,
//! compare-and-set) of many committers, so the lost-update window between a
//! committer's load and its swap is exercised deterministically and
//! shrinkably — strictly more adversarially than the real server, which
//! additionally holds a row lock across that window.
//!
//! Properties (lettering from the design doc §9):
//! - (a) no lost updates under arbitrary interleavings (invariants I1, I3);
//! - (b) a stale requirement always fails and changes nothing (F7);
//! - (c) idempotent replay: same key, same receipt, no double-apply (I5, F4);
//! - (d) multi-table commits are all-or-nothing (I2, F10).

// The harness is compiled into more than one test binary; each instantiation
// uses the subset it needs (the Postgres binary has no use for MockCatalog's
// constructors, the mock binary none for some accessors).
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Context, Poll, Waker};

use meridian_iceberg::commit::{
    CommitBackend, CommitBackendError, CommitError, CommitReceipt, CommittedTable, PointerCas,
    PointerRequirement, TablePointer, commit_single_table,
};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

/// Initial metadata location used for harness-provisioned tables.
pub(crate) const INITIAL_LOCATION: &str = "s3://bucket/metadata/00000-initial.metadata.json";

/// Completes a future that never actually waits (every mock future is ready
/// on first poll, and the mock instantiation only awaits mock futures).
///
/// # Panics
///
/// Panics if the future returns `Poll::Pending` — that would mean the model
/// grew a real await point and the tests need a real executor.
pub(crate) fn block_on_ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("mock commit futures must complete without waiting"),
    }
}

// ---------------------------------------------------------------------------
// The backend-under-test contract
// ---------------------------------------------------------------------------

/// What the harness needs from a backend beyond the protocol trait itself:
/// provisioning fresh tables and observing state for assertions.
///
/// `provision_table` must mint an identity that is unique per call —
/// property cases run against shared, persistent stores (Postgres), so
/// nothing may collide across cases or across concurrently running suites.
pub(crate) trait HarnessBackend: CommitBackend {
    /// Creates a table at (`version`, `location`) and returns its id.
    fn provision_table(
        &self,
        version: u64,
        location: &str,
    ) -> impl Future<Output = Self::TableId> + Send;

    /// The current pointer of an existing table (panics if missing).
    fn pointer(&self, table: &Self::TableId) -> impl Future<Output = TablePointer> + Send;

    /// The append-only log of every applied swap on `tables`, in commit
    /// order. For the production backend this is derived from the audit
    /// trail — which simultaneously checks invariant I6 (no pointer moves
    /// without an audit row).
    fn commit_log(
        &self,
        tables: &[Self::TableId],
    ) -> impl Future<Output = Vec<CommittedTable<Self::TableId>>> + Send;
}

// ---------------------------------------------------------------------------
// MockCatalog: the in-memory protocol model (M0)
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
pub(crate) struct MockCatalog {
    state: std::sync::Mutex<MockState>,
    next_id: AtomicU32,
}

impl MockCatalog {
    pub(crate) fn with_tables<I>(tables: I) -> Self
    where
        I: IntoIterator<Item = (u32, TablePointer)>,
    {
        let catalog = Self::default();
        {
            let mut state = catalog.state.lock().expect("mock state lock poisoned");
            state.tables.extend(tables);
        }
        let max_id = catalog
            .state
            .lock()
            .expect("mock state lock poisoned")
            .tables
            .keys()
            .max()
            .copied()
            .unwrap_or(0);
        catalog.next_id.store(max_id + 1, Ordering::Relaxed);
        catalog
    }

    pub(crate) fn with_table(table: u32, version: u64, location: &str) -> Self {
        Self::with_tables([(
            table,
            TablePointer {
                version,
                metadata_location: location.to_owned(),
            },
        )])
    }

    /// Current pointer of `table`; panics if the table does not exist.
    pub(crate) fn pointer_sync(&self, table: u32) -> TablePointer {
        self.state
            .lock()
            .expect("mock state lock poisoned")
            .tables
            .get(&table)
            .cloned()
            .expect("table must exist in mock catalog")
    }

    /// Snapshot of the append-only commit log.
    pub(crate) fn log_sync(&self) -> Vec<CommittedTable<u32>> {
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

impl HarnessBackend for MockCatalog {
    async fn provision_table(&self, version: u64, location: &str) -> u32 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.state
            .lock()
            .expect("mock state lock poisoned")
            .tables
            .insert(
                id,
                TablePointer {
                    version,
                    metadata_location: location.to_owned(),
                },
            );
        id
    }

    async fn pointer(&self, table: &u32) -> TablePointer {
        self.pointer_sync(*table)
    }

    async fn commit_log(&self, tables: &[u32]) -> Vec<CommittedTable<u32>> {
        let wanted: BTreeSet<u32> = tables.iter().copied().collect();
        self.log_sync()
            .into_iter()
            .filter(|entry| wanted.contains(&entry.table))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Scheduled committers: deterministic interleaving of protocol steps
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum Phase {
    NeedLoad,
    ReadyToSwap { base: TablePointer },
    Done,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome<T> {
    Committed(CommittedTable<T>),
    RequirementFailed,
    Exhausted,
}

/// One committer running the protocol loop step by step, where a "step" is
/// exactly one backend interaction — the unit the generated schedule
/// interleaves. The steps mirror `commit_single_table`; splitting them makes
/// the load→swap race window explicit and schedulable.
#[derive(Debug)]
struct ScriptedCommitter<T> {
    table: T,
    label: usize,
    requirements: Vec<PointerRequirement>,
    max_attempts: u32,
    attempts: u32,
    phase: Phase,
    outcome: Option<Outcome<T>>,
}

impl<T: Clone> ScriptedCommitter<T> {
    fn new(
        table: T,
        label: usize,
        requirements: Vec<PointerRequirement>,
        max_attempts: u32,
    ) -> Self {
        Self {
            table,
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
    async fn step<B>(&mut self, backend: &B)
    where
        B: CommitBackend<TableId = T>,
    {
        match std::mem::replace(&mut self.phase, Phase::Done) {
            Phase::NeedLoad => {
                let base = backend
                    .load_pointer(&self.table)
                    .await
                    .expect("table must exist");
                if self.requirements.iter().any(|r| r.check(&base).is_err()) {
                    self.outcome = Some(Outcome::RequirementFailed);
                    return; // phase stays Done
                }
                self.attempts += 1;
                self.phase = Phase::ReadyToSwap { base };
            }
            Phase::ReadyToSwap { base } => {
                let op = PointerCas {
                    table: self.table.clone(),
                    expected_version: base.version,
                    new_metadata_location: format!(
                        "s3://bucket/metadata/{:05}-committer{}-attempt{}.metadata.json",
                        base.version + 1,
                        self.label,
                        self.attempts,
                    ),
                };
                match backend.commit_atomic(std::slice::from_ref(&op), None).await {
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
                    Err(other) => panic!("unexpected backend error in harness: {other}"),
                }
            }
            Phase::Done => {}
        }
    }
}

// ---------------------------------------------------------------------------
// The properties (generic bodies; instantiated per backend)
// ---------------------------------------------------------------------------

/// Property (a): under arbitrary interleavings of many committers'
/// load/swap steps, updates are never lost — the final version delta
/// equals the number of successful commits, the commit log is gapless,
/// and successful receipts match the log one-to-one (invariants I1, I3).
pub(crate) async fn interleaved_commits_never_lose_updates<B>(
    backend: &B,
    initial_version: u64,
    specs: &[(bool, u32)],
    schedule: &[prop::sample::Index],
) -> Result<(), TestCaseError>
where
    B: HarnessBackend,
{
    let table = backend
        .provision_table(initial_version, INITIAL_LOCATION)
        .await;
    let mut committers: Vec<ScriptedCommitter<B::TableId>> = specs
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
            ScriptedCommitter::new(table.clone(), label, requirements, max_attempts)
        })
        .collect();

    // Interleave per the generated schedule...
    for index in schedule {
        let position = index.index(committers.len());
        let committer = &mut committers[position];
        if !committer.done() {
            committer.step(backend).await;
        }
    }
    // ...then drain round-robin so every committer reaches an outcome
    // (termination: attempts are bounded).
    loop {
        let mut progressed = false;
        for committer in &mut committers {
            if !committer.done() {
                committer.step(backend).await;
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }

    let successes: Vec<&CommittedTable<B::TableId>> = committers
        .iter()
        .filter_map(|c| match &c.outcome {
            Some(Outcome::Committed(t)) => Some(t),
            _ => None,
        })
        .collect();
    let success_count = u64::try_from(successes.len()).expect("committer count fits in u64");
    let log = backend.commit_log(std::slice::from_ref(&table)).await;
    let final_pointer = backend.pointer(&table).await;

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
    for (committer, &(stale, _)) in committers.iter().zip(specs) {
        if let (true, Some(Outcome::Committed(t))) = (stale, &committer.outcome) {
            prop_assert_eq!(t.version, initial_version + 1);
        }
    }
    Ok(())
}

/// Property (b): a commit built against stale state always fails with a
/// requirement violation and changes nothing (F7).
pub(crate) async fn stale_requirement_always_fails<B>(
    backend: &B,
    initial_version: u64,
    advances: u32,
    pin_location: bool,
) -> Result<(), TestCaseError>
where
    B: HarnessBackend,
{
    let table = backend
        .provision_table(initial_version, INITIAL_LOCATION)
        .await;

    // Advance the table past the state the requirement will pin.
    for i in 0..advances {
        let current = backend.pointer(&table).await;
        let op = PointerCas {
            table: table.clone(),
            expected_version: current.version,
            new_metadata_location: format!(
                "s3://bucket/metadata/{:05}-advance{}.metadata.json",
                current.version + 1,
                i,
            ),
        };
        backend
            .commit_atomic(std::slice::from_ref(&op), None)
            .await
            .expect("advancing commit must succeed");
    }

    let before_pointer = backend.pointer(&table).await;
    let before_log = backend.commit_log(std::slice::from_ref(&table)).await;
    let requirement = if pin_location {
        PointerRequirement::MetadataLocationIs(INITIAL_LOCATION.to_owned())
    } else {
        PointerRequirement::VersionIs(initial_version)
    };

    let result = commit_single_table(
        backend,
        table.clone(),
        std::slice::from_ref(&requirement),
        |_base| "s3://bucket/metadata/99999-stale-attempt.metadata.json".to_owned(),
        None,
        NonZeroU32::new(3).expect("nonzero"),
    )
    .await;

    prop_assert!(
        matches!(result, Err(CommitError::RequirementFailed(_))),
        "stale requirement must be rejected, got {result:?}"
    );
    prop_assert_eq!(backend.pointer(&table).await, before_pointer);
    prop_assert_eq!(
        backend.commit_log(std::slice::from_ref(&table)).await,
        before_log
    );
    Ok(())
}

/// Property (c): retrying with the same idempotency key replays the
/// recorded receipt and applies nothing — even though the retry's
/// requirements are stale by then, because its own first attempt moved
/// the table (I5, F4).
pub(crate) async fn idempotent_retry_replays_without_double_apply<B>(
    backend: &B,
    initial_version: u64,
    key_seed: &str,
) -> Result<(), TestCaseError>
where
    B: HarnessBackend,
{
    let table = backend
        .provision_table(initial_version, INITIAL_LOCATION)
        .await;
    // Keys are scoped per provisioned table: the production store is
    // persistent and shared, so a bare generated key could collide with an
    // earlier case's recorded receipt.
    let key = format!("{key_seed}-{table}");
    let requirements = [PointerRequirement::VersionIs(initial_version)];
    let one_attempt = NonZeroU32::new(1).expect("nonzero");

    let first = commit_single_table(
        backend,
        table.clone(),
        &requirements,
        |base| {
            format!(
                "s3://bucket/metadata/{:05}-first.metadata.json",
                base.version + 1,
            )
        },
        Some(&key),
        one_attempt,
    )
    .await
    .expect("first commit succeeds");
    prop_assert!(!first.replayed);

    let after_first_pointer = backend.pointer(&table).await;
    let after_first_log = backend.commit_log(std::slice::from_ref(&table)).await;
    prop_assert_eq!(after_first_pointer.version, initial_version + 1);

    // The retry would stage a different file and its requirement is now
    // stale — replay must short-circuit before both.
    let second = commit_single_table(
        backend,
        table.clone(),
        &requirements,
        |_base| "s3://bucket/metadata/would-be-a-double-apply.metadata.json".to_owned(),
        Some(&key),
        one_attempt,
    )
    .await
    .expect("retry with the same key replays");

    prop_assert!(second.replayed);
    prop_assert_eq!(&second.tables, &first.tables);
    prop_assert_eq!(backend.pointer(&table).await, after_first_pointer);
    prop_assert_eq!(
        backend.commit_log(std::slice::from_ref(&table)).await,
        after_first_log
    );
    Ok(())
}

/// Property (d): a multi-table commit applies every pointer swap or none
/// (I2, F10), regardless of submission order.
pub(crate) async fn multi_table_commit_is_all_or_nothing<B>(
    backend: &B,
    tables: &[(u64, bool)],
) -> Result<(), TestCaseError>
where
    B: HarnessBackend,
{
    let mut ids: Vec<B::TableId> = Vec::with_capacity(tables.len());
    for (i, &(version, _)) in tables.iter().enumerate() {
        let id = backend
            .provision_table(
                version,
                &format!("s3://bucket/t{i}/metadata/{version:05}-initial.metadata.json"),
            )
            .await;
        ids.push(id);
    }
    // Pair each table with its spec, then order pairs by ascending id so the
    // "submit in reverse id order" below is meaningful for any id scheme.
    let mut paired: Vec<(B::TableId, u64, bool)> = ids
        .iter()
        .cloned()
        .zip(tables)
        .map(|(id, &(version, stale))| (id, version, stale))
        .collect();
    paired.sort_by(|a, b| a.0.cmp(&b.0));

    let mut before: Vec<TablePointer> = Vec::with_capacity(paired.len());
    for (id, _, _) in &paired {
        before.push(backend.pointer(id).await);
    }

    // Submit in reverse id order: the backend must order internally.
    let ops: Vec<PointerCas<B::TableId>> = paired
        .iter()
        .rev()
        .map(|(id, version, stale)| PointerCas {
            table: id.clone(),
            expected_version: if *stale { version + 1 } else { *version },
            new_metadata_location: format!(
                "s3://bucket/{id}/metadata/{:05}-txn.metadata.json",
                version + 1,
            ),
        })
        .collect();
    let any_stale = paired.iter().any(|(_, _, stale)| *stale);

    let result = backend.commit_atomic(&ops, None).await;

    if any_stale {
        prop_assert!(
            matches!(result, Err(CommitBackendError::VersionConflict { .. })),
            "stale guard must fail the whole transaction, got {result:?}"
        );
        // Nothing changed — for any table.
        for ((id, _, _), expected) in paired.iter().zip(&before) {
            prop_assert_eq!(&backend.pointer(id).await, expected);
        }
        prop_assert!(backend.commit_log(&ids).await.is_empty());
    } else {
        let receipt = result.expect("all guards hold: transaction commits");
        prop_assert!(!receipt.replayed);
        // Receipt follows submission order.
        for (entry, op) in receipt.tables.iter().zip(&ops) {
            prop_assert_eq!(&entry.table, &op.table);
            prop_assert_eq!(entry.version, op.expected_version + 1);
            prop_assert_eq!(&entry.metadata_location, &op.new_metadata_location);
        }
        // Every pointer advanced by exactly one.
        for ((id, _, _), previous) in paired.iter().zip(&before) {
            let now = backend.pointer(id).await;
            prop_assert_eq!(now.version, previous.version + 1);
            prop_assert_ne!(&now.metadata_location, &previous.metadata_location);
        }
        prop_assert_eq!(backend.commit_log(&ids).await.len(), ids.len());
    }
    Ok(())
}
