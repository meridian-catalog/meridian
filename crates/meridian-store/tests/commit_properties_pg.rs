//! The commit-protocol property suite, run against the production
//! [`PostgresCommitBackend`] — the design doc's requirement (§9) that the
//! M1 backend pass the *identical* suite through the same trait as the M0
//! protocol model.
//!
//! The property bodies are shared source with
//! `meridian-iceberg/tests/commit_properties.rs` (see `commit_harness`).
//! Requires a running Postgres and `DATABASE_URL`; without it every test
//! skips with a note on stderr. Each proptest case provisions its own table
//! rows under fresh ULIDs, so the suite is safe to run in parallel with
//! itself and with the rest of the workspace against one database.

#[path = "../../meridian-iceberg/tests/commit_harness/mod.rs"]
mod commit_harness;

use std::sync::OnceLock;

use commit_harness::HarnessBackend;
use meridian_common::config::DatabaseConfig;
use meridian_iceberg::commit::{
    CommitBackend, CommitBackendError, CommitReceipt, CommittedTable, PointerCas, TablePointer,
};
use meridian_store::commit::PostgresCommitBackend;
use meridian_store::tenancy;
use proptest::prelude::*;
use serde_json::Value;
use sqlx::PgPool;
use tokio::runtime::Runtime;
use ulid::Ulid;

/// Fewer cases than the in-memory model: every case is real Postgres I/O.
/// The model suite keeps the default budget; this one checks the identical
/// properties against the production backend.
const PG_CASES: u32 = 16;

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        // Multi-threaded so concurrent `block_on` calls from parallel test
        // threads are driven by the workers instead of serializing.
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build tokio runtime")
    })
}

/// The production backend plus the observation/provisioning hooks the
/// harness needs.
struct PgHarness {
    backend: PostgresCommitBackend,
    pool: PgPool,
    namespace_id: String,
}

impl CommitBackend for PgHarness {
    type TableId = String;

    async fn load_pointer(&self, table: &String) -> Result<TablePointer, CommitBackendError> {
        self.backend.load_pointer(table).await
    }

    async fn recall_idempotency_key(
        &self,
        key: &str,
    ) -> Result<Option<CommitReceipt<String>>, CommitBackendError> {
        self.backend.recall_idempotency_key(key).await
    }

    async fn commit_atomic(
        &self,
        ops: &[PointerCas<String>],
        idempotency_key: Option<&str>,
    ) -> Result<CommitReceipt<String>, CommitBackendError> {
        self.backend.commit_atomic(ops, idempotency_key).await
    }
}

impl HarnessBackend for PgHarness {
    async fn provision_table(&self, version: u64, location: &str) -> String {
        let id = Ulid::new().to_string();
        // table_uuid only needs to be unique here; harness rows never pass
        // through the metadata model.
        sqlx::query(
            "INSERT INTO tables
                 (id, workspace_id, namespace_id, name, table_uuid, metadata_location,
                  pointer_version, format_version, properties)
             VALUES ($1, $2, $3, $4, $5, $6, $7, 2, '{}'::jsonb)",
        )
        .bind(&id)
        .bind(tenancy::DEFAULT_WORKSPACE_ID)
        .bind(&self.namespace_id)
        .bind(format!("prop-{id}"))
        .bind(format!("uuid-{id}"))
        .bind(location)
        .bind(i64::try_from(version).expect("harness versions fit in i64"))
        .execute(&self.pool)
        .await
        .expect("provision harness table");
        id
    }

    async fn pointer(&self, table: &String) -> TablePointer {
        self.backend
            .load_pointer(table)
            .await
            .expect("harness table must exist")
    }

    /// Derives the commit log from the audit trail — every pointer swap must
    /// have written its audit row in the same transaction (invariant I6), so
    /// a missing or malformed entry fails the property.
    async fn commit_log(&self, tables: &[String]) -> Vec<CommittedTable<String>> {
        let resources: Vec<String> = tables.iter().map(|id| format!("table:{id}")).collect();
        let rows: Vec<(String, Value)> = sqlx::query_as(
            "SELECT resource, details FROM audit_log
             WHERE action = 'table.commit' AND resource = ANY($1)
             ORDER BY seq",
        )
        .bind(&resources)
        .fetch_all(&self.pool)
        .await
        .expect("read audit-derived commit log");

        rows.into_iter()
            .map(|(resource, details)| {
                let table = resource
                    .strip_prefix("table:")
                    .expect("commit audit resource is table-scoped")
                    .to_owned();
                let version = details
                    .get("pointer_version")
                    .and_then(Value::as_u64)
                    .expect("commit audit row records pointer_version");
                let metadata_location = details
                    .get("metadata_location")
                    .and_then(Value::as_str)
                    .expect("commit audit row records metadata_location")
                    .to_owned();
                CommittedTable {
                    table,
                    version,
                    metadata_location,
                }
            })
            .collect()
    }
}

/// Connects, migrates, and provisions the harness namespace once per
/// process. `None` (with a skip note) when `DATABASE_URL` is unset.
fn harness() -> Option<&'static PgHarness> {
    static HARNESS: OnceLock<Option<PgHarness>> = OnceLock::new();
    HARNESS
        .get_or_init(|| {
            let Ok(url) = std::env::var("DATABASE_URL") else {
                eprintln!("skipping Postgres commit property tests: DATABASE_URL is not set");
                return None;
            };
            let config = DatabaseConfig {
                url,
                ..DatabaseConfig::default()
            };
            runtime().block_on(async {
                let pool = meridian_store::connect(&config)
                    .await
                    .expect("connect to test database");
                meridian_store::MIGRATOR
                    .run(&pool)
                    .await
                    .expect("run migrations");

                let workspace = tenancy::default_workspace_id();
                let run = Ulid::new().to_string().to_lowercase();
                let warehouse = meridian_store::warehouse::create(
                    &pool,
                    workspace,
                    &format!("prop-wh-{run}"),
                    "s3://prop-bucket/root",
                    std::collections::BTreeMap::new(),
                    "test:commit-properties",
                )
                .await
                .expect("create harness warehouse");
                let namespace = meridian_store::namespace::create(
                    &pool,
                    workspace,
                    &warehouse.id,
                    &[format!("prop_ns_{run}")],
                    std::collections::BTreeMap::new(),
                    "test:commit-properties",
                )
                .await
                .expect("create harness namespace");

                Some(PgHarness {
                    backend: PostgresCommitBackend::new(
                        pool.clone(),
                        workspace,
                        "test:commit-properties",
                    ),
                    pool,
                    namespace_id: namespace.id,
                })
            })
        })
        .as_ref()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(PG_CASES))]

    /// Property (a) — invariants I1, I3 — against Postgres.
    #[test]
    fn pg_interleaved_commits_never_lose_updates(
        initial_version in 0u64..1_000,
        specs in prop::collection::vec((any::<bool>(), 1u32..4), 2..6),
        schedule in prop::collection::vec(any::<prop::sample::Index>(), 0..60),
    ) {
        let Some(harness) = harness() else { return Ok(()); };
        runtime().block_on(commit_harness::interleaved_commits_never_lose_updates(
            harness,
            initial_version,
            &specs,
            &schedule,
        ))?;
    }

    /// Property (b) — stale requirements always fail (F7) — against Postgres.
    #[test]
    fn pg_stale_requirement_always_fails(
        initial_version in 0u64..1_000,
        advances in 1u32..8,
        pin_location in any::<bool>(),
    ) {
        let Some(harness) = harness() else { return Ok(()); };
        runtime().block_on(commit_harness::stale_requirement_always_fails(
            harness,
            initial_version,
            advances,
            pin_location,
        ))?;
    }

    /// Property (c) — idempotent replay (I5, F4) — against Postgres.
    #[test]
    fn pg_idempotent_retry_replays_without_double_apply(
        initial_version in 0u64..1_000,
        key in "[a-z0-9]{8,16}",
    ) {
        let Some(harness) = harness() else { return Ok(()); };
        runtime().block_on(commit_harness::idempotent_retry_replays_without_double_apply(
            harness,
            initial_version,
            &key,
        ))?;
    }

    /// Property (d) — multi-table all-or-nothing (I2, F10) — against Postgres.
    #[test]
    fn pg_multi_table_commit_is_all_or_nothing(
        tables in prop::collection::vec((0u64..1_000, any::<bool>()), 2..6),
    ) {
        let Some(harness) = harness() else { return Ok(()); };
        runtime().block_on(commit_harness::multi_table_commit_is_all_or_nothing(
            harness,
            &tables,
        ))?;
    }
}

// ---------------------------------------------------------------------------
// Deterministic edge cases, against the production backend
// ---------------------------------------------------------------------------

// All tests share one current-thread runtime (the pool's home), so nothing
// crosses runtimes.

#[test]
fn pg_empty_and_duplicate_commits_are_rejected() {
    let Some(harness) = harness() else { return };
    runtime().block_on(async {
        let result = harness.backend.commit_atomic(&[], None).await;
        assert_eq!(result, Err(CommitBackendError::EmptyCommit));

        let table = harness
            .provision_table(0, commit_harness::INITIAL_LOCATION)
            .await;
        let op = |location: &str| PointerCas {
            table: table.clone(),
            expected_version: 0,
            new_metadata_location: location.to_owned(),
        };
        let result = harness
            .backend
            .commit_atomic(&[op("s3://a"), op("s3://b")], None)
            .await;
        assert_eq!(
            result,
            Err(CommitBackendError::DuplicateTable {
                table: table.clone()
            })
        );
        assert_eq!(harness.pointer(&table).await.version, 0);
    });
}

#[test]
fn pg_unknown_table_is_reported() {
    let Some(harness) = harness() else { return };
    runtime().block_on(async {
        let missing = Ulid::new().to_string();

        let load = harness.backend.load_pointer(&missing).await;
        assert_eq!(
            load,
            Err(CommitBackendError::TableNotFound {
                table: missing.clone()
            })
        );

        let op = PointerCas {
            table: missing.clone(),
            expected_version: 0,
            new_metadata_location: "s3://bucket/metadata/00001-x.metadata.json".to_owned(),
        };
        let commit = harness
            .backend
            .commit_atomic(std::slice::from_ref(&op), None)
            .await;
        assert_eq!(
            commit,
            Err(CommitBackendError::TableNotFound { table: missing })
        );
    });
}

#[test]
fn pg_reusing_a_key_for_a_different_commit_is_rejected() {
    let Some(harness) = harness() else { return };
    runtime().block_on(async {
        let table = harness
            .provision_table(0, commit_harness::INITIAL_LOCATION)
            .await;
        let key = format!("reuse-{table}");

        let first = PointerCas {
            table: table.clone(),
            expected_version: 0,
            new_metadata_location: "s3://bucket/metadata/00001-a.metadata.json".to_owned(),
        };
        harness
            .backend
            .commit_atomic(std::slice::from_ref(&first), Some(&key))
            .await
            .expect("first commit succeeds");

        // Same key, different request: must fail loudly, not replay or apply.
        let second = PointerCas {
            table: table.clone(),
            expected_version: 1,
            new_metadata_location: "s3://bucket/metadata/00002-b.metadata.json".to_owned(),
        };
        let result = harness
            .backend
            .commit_atomic(std::slice::from_ref(&second), Some(&key))
            .await;
        assert_eq!(result, Err(CommitBackendError::IdempotencyKeyReuse { key }));
        assert_eq!(harness.pointer(&table).await.version, 1);
    });
}
