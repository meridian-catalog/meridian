//! Property-based tests for the commit protocol model.
//!
//! The property bodies live in `commit_harness/mod.rs`, shared verbatim with
//! the Postgres instantiation in `meridian-store` — the design doc's
//! requirement that the production backend pass the identical suite through
//! the same trait. This file instantiates them against [`MockCatalog`], the
//! in-memory protocol model, and keeps the model-only deterministic edge
//! cases (contract violations that the generic properties do not reach).

mod commit_harness;

use std::num::NonZeroU32;

use commit_harness::{INITIAL_LOCATION, MockCatalog, block_on_ready};
use meridian_iceberg::commit::{
    CommitBackend, CommitBackendError, CommitError, PointerCas, commit_single_table,
};
use proptest::prelude::*;

const TABLE: u32 = 7;

proptest! {
    /// Property (a) — invariants I1, I3 (see the harness for the full story).
    #[test]
    fn interleaved_commits_never_lose_updates(
        initial_version in 0u64..1_000,
        // (has_stale_requirement, retry budget) per committer.
        specs in prop::collection::vec((any::<bool>(), 1u32..4), 2..6),
        schedule in prop::collection::vec(any::<prop::sample::Index>(), 0..120),
    ) {
        let catalog = MockCatalog::default();
        block_on_ready(commit_harness::interleaved_commits_never_lose_updates(
            &catalog,
            initial_version,
            &specs,
            &schedule,
        ))?;
    }

    /// Property (b) — stale requirements always fail (F7).
    #[test]
    fn stale_requirement_always_fails(
        initial_version in 0u64..1_000,
        advances in 1u32..8,
        pin_location in any::<bool>(),
    ) {
        let catalog = MockCatalog::default();
        block_on_ready(commit_harness::stale_requirement_always_fails(
            &catalog,
            initial_version,
            advances,
            pin_location,
        ))?;
    }

    /// Property (c) — idempotent replay, no double-apply (I5, F4).
    #[test]
    fn idempotent_retry_replays_without_double_apply(
        initial_version in 0u64..1_000,
        key in "[a-z0-9]{8,16}",
    ) {
        let catalog = MockCatalog::default();
        block_on_ready(commit_harness::idempotent_retry_replays_without_double_apply(
            &catalog,
            initial_version,
            &key,
        ))?;
    }

    /// Property (d) — multi-table commits are all-or-nothing (I2, F10).
    #[test]
    fn multi_table_commit_is_all_or_nothing(
        // (initial version, is_stale) per table.
        tables in prop::collection::vec((0u64..1_000, any::<bool>()), 2..6),
    ) {
        let catalog = MockCatalog::default();
        block_on_ready(commit_harness::multi_table_commit_is_all_or_nothing(
            &catalog,
            &tables,
        ))?;
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
    assert!(catalog.log_sync().is_empty());
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
    assert_eq!(catalog.pointer_sync(TABLE).version, 0);
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
    assert_eq!(catalog.pointer_sync(TABLE).version, 1);
    assert_eq!(catalog.log_sync().len(), 1);
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
    let final_pointer = catalog.pointer_sync(TABLE);
    assert_eq!(final_pointer.version, 3);
    assert!(final_pointer.metadata_location.contains("interloper"));
    assert!(
        catalog
            .log_sync()
            .iter()
            .all(|e| !e.metadata_location.contains("victim"))
    );
}
