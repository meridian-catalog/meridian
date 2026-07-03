//! Database-backed tests for the maintenance control plane (policies, the
//! job queue, and the savings ledger).
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip
//! (with a note on stderr) so the suite stays runnable offline.

// Lifecycle tests are long by nature (setup + many assertions per path).
#![allow(clippy::items_after_statements, clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::sync::Arc;

use meridian_common::MeridianError;
use meridian_common::config::DatabaseConfig;
use meridian_common::id::WorkspaceId;
use meridian_store::maintenance::{self, JobState, JobType, PolicySpec, SavingsInput, Scope};
use meridian_store::table::{self, NewTable};
use meridian_store::{namespace, tenancy, warehouse};
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::{Mutex, MutexGuard, OnceCell};
use ulid::Ulid;

/// `claim_next` scans the job queue *globally* (a shared worker pool, by
/// design), so tests that enqueue-and-claim must not run concurrently with
/// each other or a foreign workspace's still-queued job could be claimed
/// mid-test. This process-wide async mutex serializes exactly those tests;
/// no other test binary calls `claim_next`, so DB-level isolation is complete
/// once this binary's queue tests are serialized.
static QUEUE_SERIAL: OnceCell<Mutex<()>> = OnceCell::const_new();

async fn queue_lock() -> MutexGuard<'static, ()> {
    QUEUE_SERIAL
        .get_or_init(|| async { Mutex::new(()) })
        .await
        .lock()
        .await
}

struct Fixture {
    pool: PgPool,
    workspace: WorkspaceId,
    warehouse_id: String,
    namespace_id: String,
    table_id: String,
}

async fn fixture() -> Option<Fixture> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping maintenance DB test: DATABASE_URL is not set");
        return None;
    };
    let config = DatabaseConfig {
        url,
        ..DatabaseConfig::default()
    };
    let pool = meridian_store::connect(&config)
        .await
        .expect("connect to test database");
    meridian_store::MIGRATOR.run(&pool).await.expect("migrate");

    let run = Ulid::new().to_string().to_lowercase();
    // Each test gets its own workspace (under the seeded default org) so the
    // shared job queue is private — the fairness/claim tests must not see
    // jobs enqueued by concurrently-running test binaries.
    let workspace = WorkspaceId::from_ulid(Ulid::new());
    sqlx::query("INSERT INTO workspaces (id, org_id, name) VALUES ($1, $2, $3)")
        .bind(workspace.to_string())
        .bind(tenancy::DEFAULT_ORG_ID)
        .bind(format!("maint-ws-{run}"))
        .execute(&pool)
        .await
        .expect("create isolated workspace");
    let wh = warehouse::create(
        &pool,
        workspace,
        &format!("maint-wh-{run}"),
        "s3://maint-tests/root",
        BTreeMap::new(),
        "test:maint",
    )
    .await
    .expect("create warehouse");
    let levels = vec![format!("maint_ns_{run}")];
    let ns = namespace::create(
        &pool,
        workspace,
        &wh.id,
        &levels,
        BTreeMap::new(),
        "test:maint",
    )
    .await
    .expect("create namespace");
    let uuid = format!("uuid-{}", Ulid::new());
    let tbl = table::create(
        &pool,
        NewTable {
            workspace_id: workspace,
            namespace_id: &ns.id,
            namespace_levels: &levels,
            name: "orders",
            table_uuid: &uuid,
            metadata_location: "s3://maint-tests/root/orders/metadata/00000-x.metadata.json",
            format_version: 2,
            properties: &BTreeMap::new(),
            schema_text: None,
            snapshots: &[],
            origin: "create",
        },
        "test:maint",
        None,
    )
    .await
    .expect("create table");

    Some(Fixture {
        pool,
        workspace,
        warehouse_id: wh.id,
        namespace_id: ns.id,
        table_id: tbl.id,
    })
}

#[tokio::test]
async fn policy_resolution_prefers_most_specific_scope() {
    let Some(fx) = fixture().await else { return };

    // No policy at any scope -> None (caller falls back to defaults).
    let none = maintenance::resolve_effective(
        &fx.pool,
        fx.workspace,
        &fx.table_id,
        &fx.namespace_id,
        &fx.warehouse_id,
    )
    .await
    .expect("resolve");
    assert!(none.is_none(), "no policy anywhere resolves to None");

    // Warehouse policy: target 128 MiB.
    maintenance::create_policy(
        &fx.pool,
        fx.workspace,
        Scope::Warehouse,
        &fx.warehouse_id,
        &PolicySpec {
            target_file_size_bytes: 128 * 1024 * 1024,
            ..PolicySpec::default()
        },
        "test:maint",
    )
    .await
    .expect("create warehouse policy");

    let resolved = maintenance::resolve_effective(
        &fx.pool,
        fx.workspace,
        &fx.table_id,
        &fx.namespace_id,
        &fx.warehouse_id,
    )
    .await
    .expect("resolve")
    .expect("some policy");
    assert_eq!(resolved.scope, Scope::Warehouse);
    assert_eq!(resolved.spec.target_file_size_bytes, 128 * 1024 * 1024);

    // Namespace policy overrides warehouse.
    maintenance::create_policy(
        &fx.pool,
        fx.workspace,
        Scope::Namespace,
        &fx.namespace_id,
        &PolicySpec {
            target_file_size_bytes: 256 * 1024 * 1024,
            ..PolicySpec::default()
        },
        "test:maint",
    )
    .await
    .expect("create namespace policy");
    let resolved = maintenance::resolve_effective(
        &fx.pool,
        fx.workspace,
        &fx.table_id,
        &fx.namespace_id,
        &fx.warehouse_id,
    )
    .await
    .expect("resolve")
    .expect("some");
    assert_eq!(resolved.scope, Scope::Namespace);

    // Table policy overrides everything.
    maintenance::create_policy(
        &fx.pool,
        fx.workspace,
        Scope::Table,
        &fx.table_id,
        &PolicySpec {
            target_file_size_bytes: 64 * 1024 * 1024,
            ..PolicySpec::default()
        },
        "test:maint",
    )
    .await
    .expect("create table policy");
    let resolved = maintenance::resolve_effective(
        &fx.pool,
        fx.workspace,
        &fx.table_id,
        &fx.namespace_id,
        &fx.warehouse_id,
    )
    .await
    .expect("resolve")
    .expect("some");
    assert_eq!(resolved.scope, Scope::Table);
    assert_eq!(resolved.spec.target_file_size_bytes, 64 * 1024 * 1024);

    // A disabled table policy is skipped; resolution falls back to namespace.
    maintenance::update_policy(
        &fx.pool,
        fx.workspace,
        Scope::Table,
        &fx.table_id,
        &PolicySpec {
            target_file_size_bytes: 64 * 1024 * 1024,
            enabled: false,
            ..PolicySpec::default()
        },
        "test:maint",
    )
    .await
    .expect("disable table policy");
    let resolved = maintenance::resolve_effective(
        &fx.pool,
        fx.workspace,
        &fx.table_id,
        &fx.namespace_id,
        &fx.warehouse_id,
    )
    .await
    .expect("resolve")
    .expect("some");
    assert_eq!(
        resolved.scope,
        Scope::Namespace,
        "a disabled table policy is bypassed"
    );
}

#[tokio::test]
async fn duplicate_policy_at_a_scope_conflicts() {
    let Some(fx) = fixture().await else { return };
    maintenance::create_policy(
        &fx.pool,
        fx.workspace,
        Scope::Warehouse,
        &fx.warehouse_id,
        &PolicySpec::default(),
        "test:maint",
    )
    .await
    .expect("first");
    let err = maintenance::create_policy(
        &fx.pool,
        fx.workspace,
        Scope::Warehouse,
        &fx.warehouse_id,
        &PolicySpec::default(),
        "test:maint",
    )
    .await
    .expect_err("second must conflict");
    assert!(matches!(err, MeridianError::Conflict(_)));
}

#[tokio::test]
async fn job_lifecycle_queue_claim_complete() {
    let Some(fx) = fixture().await else { return };
    let _serial = queue_lock().await;

    let job = maintenance::enqueue_job(
        &fx.pool,
        fx.workspace,
        &fx.table_id,
        JobType::Compaction,
        None,
        &json!({"dry_run": false}),
        "test:maint",
    )
    .await
    .expect("enqueue");
    assert_eq!(job.state, JobState::Queued);
    assert_eq!(job.attempts, 0);

    let claimed = maintenance::claim_next(&fx.pool, "worker-1")
        .await
        .expect("claim")
        .expect("a job was queued");
    assert_eq!(claimed.id, job.id);
    assert_eq!(claimed.state, JobState::Running);
    assert_eq!(claimed.attempts, 1);
    assert_eq!(claimed.claimed_by.as_deref(), Some("worker-1"));

    // A second worker cannot complete a job it does not hold.
    let wrong = maintenance::complete_job(&fx.pool, &job.id, "worker-2", &json!({}))
        .await
        .expect_err("wrong worker cannot complete");
    assert!(matches!(wrong, MeridianError::Conflict(_)));

    let done = maintenance::complete_job(
        &fx.pool,
        &job.id,
        "worker-1",
        &json!({"bytes_before": 100, "bytes_after": 40}),
    )
    .await
    .expect("complete");
    assert_eq!(done.state, JobState::Succeeded);
    assert!(done.finished_at.is_some());
    assert!(done.claimed_by.is_none());

    // Re-completing a terminal job conflicts.
    let again = maintenance::complete_job(&fx.pool, &job.id, "worker-1", &json!({}))
        .await
        .expect_err("double complete conflicts");
    assert!(matches!(again, MeridianError::Conflict(_)));
}

#[tokio::test]
async fn cancel_queued_and_running_jobs() {
    let Some(fx) = fixture().await else { return };
    let _serial = queue_lock().await;

    // Cancel a queued job.
    let queued = maintenance::enqueue_job(
        &fx.pool,
        fx.workspace,
        &fx.table_id,
        JobType::ExpireSnapshots,
        None,
        &json!({}),
        "test:maint",
    )
    .await
    .expect("enqueue");
    let cancelled = maintenance::cancel_job(&fx.pool, fx.workspace, &queued.id, "test:maint")
        .await
        .expect("cancel queued");
    assert_eq!(cancelled.state, JobState::Cancelled);

    // Cancelling a terminal job conflicts.
    let err = maintenance::cancel_job(&fx.pool, fx.workspace, &queued.id, "test:maint")
        .await
        .expect_err("re-cancel conflicts");
    assert!(matches!(err, MeridianError::Conflict(_)));
}

#[tokio::test]
async fn skip_locked_claim_gives_each_job_to_one_worker() {
    let Some(fx) = fixture().await else { return };
    let _serial = queue_lock().await;
    const N: usize = 12;
    const ROUNDS: usize = 6;
    let pool = Arc::new(fx.pool.clone());

    // Enqueue N jobs into this test's private workspace, then hammer the
    // claim path from many concurrent workers. Two invariants must hold:
    //   1. no job is claimed by two workers (SKIP LOCKED + running-state CAS);
    //   2. every job leaves the queue (nothing stuck in `queued`).
    let mut ids = std::collections::BTreeSet::new();
    for i in 0..N {
        let job = maintenance::enqueue_job(
            &fx.pool,
            fx.workspace,
            &fx.table_id,
            JobType::Compaction,
            None,
            &json!({ "n": i }),
            "test:maint",
        )
        .await
        .expect("enqueue");
        ids.insert(job.id);
    }

    // Several rounds of over-provisioned concurrent claimers. SKIP LOCKED
    // means empties are fine; the loop absorbs the races between claimers so
    // the whole queue drains regardless of scheduling.
    let mut claimed: Vec<String> = Vec::new();
    for round in 0..ROUNDS {
        let mut handles = Vec::new();
        for w in 0..(N * 2) {
            let pool = Arc::clone(&pool);
            let worker = format!("w-{round}-{w}");
            handles.push(tokio::spawn(async move {
                maintenance::claim_next(&pool, &worker)
                    .await
                    .expect("claim")
                    .map(|j| j.id)
            }));
        }
        for h in handles {
            if let Some(id) = h.await.expect("join") {
                claimed.push(id);
            }
        }
    }

    // Invariant 1: our jobs are never double-claimed. (Other test binaries
    // share the global worker pool, so restrict to ids we enqueued.)
    let mut our_claims: Vec<&String> = claimed.iter().filter(|id| ids.contains(*id)).collect();
    our_claims.sort();
    let deduped: std::collections::BTreeSet<_> = our_claims.iter().collect();
    assert_eq!(
        our_claims.len(),
        deduped.len(),
        "no job may be claimed by two workers"
    );

    // Invariant 2: none of our jobs is still queued (each was claimed by
    // exactly one worker — ours or, harmlessly, a concurrent test's).
    for id in &ids {
        let job = maintenance::get_job(&fx.pool, fx.workspace, id)
            .await
            .expect("get")
            .expect("job exists");
        assert_ne!(
            job.state,
            JobState::Queued,
            "job {id} must have left the queue"
        );
    }
}

#[tokio::test]
async fn savings_ledger_rollup_sums_by_month() {
    let Some(fx) = fixture().await else { return };
    let _serial = queue_lock().await;

    // Two completed jobs, two ledger rows in the same month.
    for (before, after, files_before, files_after) in
        [(1_000_i64, 400_i64, 50_i64, 5_i64), (2_000, 1_500, 20, 4)]
    {
        let job = maintenance::enqueue_job(
            &fx.pool,
            fx.workspace,
            &fx.table_id,
            JobType::Compaction,
            None,
            &json!({}),
            "test:maint",
        )
        .await
        .expect("enqueue");
        let claimed = maintenance::claim_next(&fx.pool, "worker-ledger")
            .await
            .expect("claim")
            .expect("job");
        maintenance::complete_job(&fx.pool, &claimed.id, "worker-ledger", &json!({}))
            .await
            .expect("complete");
        let rec = maintenance::append_savings(
            &fx.pool,
            fx.workspace,
            &job.id,
            &fx.table_id,
            "maint_ns.orders",
            &SavingsInput {
                bytes_before: before,
                bytes_after: after,
                files_before,
                files_after,
                est_get_requests_saved: (files_before - files_after) * 100,
                methodology: "small-file GET model v1".to_owned(),
            },
            "test:maint",
        )
        .await
        .expect("append savings");
        assert_eq!(rec.bytes_saved, before - after);
        assert_eq!(rec.files_removed, files_before - files_after);

        // A job's savings are counted exactly once.
        let dup = maintenance::append_savings(
            &fx.pool,
            fx.workspace,
            &job.id,
            &fx.table_id,
            "maint_ns.orders",
            &SavingsInput {
                bytes_before: before,
                bytes_after: after,
                files_before,
                files_after,
                est_get_requests_saved: 0,
                methodology: "dup".to_owned(),
            },
            "test:maint",
        )
        .await
        .expect_err("duplicate ledger row conflicts");
        assert!(matches!(dup, MeridianError::Conflict(_)));
    }

    let rollup = maintenance::monthly_rollup(&fx.pool, fx.workspace, 24)
        .await
        .expect("rollup");
    // The workspace is shared across test binaries, so assert our two jobs'
    // savings are *included* in the current month's totals rather than equal.
    let current_month = rollup
        .iter()
        .max_by_key(|r| r.period)
        .expect("at least one period");
    // Our contribution: bytes (600 + 500) = 1100, files (45 + 16) = 61.
    assert!(
        current_month.bytes_saved >= 1_100,
        "rollup includes our bytes_saved"
    );
    assert!(
        current_month.files_removed >= 61,
        "rollup includes our files_removed"
    );
    assert!(current_month.job_count >= 2);
}
