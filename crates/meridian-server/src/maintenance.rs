//! The autonomous table-maintenance worker (spec Pillar C: C-F3 policy
//! engine + reconciliation, C-F4 execution layer, C-F5 savings ledger).
//!
//! Two background tasks run inside `meridian serve` (spawned by
//! [`crate::serve`], exactly like the events relay):
//!
//! - [`run_worker`] — the **job worker**. Claims queued `maintenance_jobs`
//!   with `FOR UPDATE SKIP LOCKED` (via [`meridian_store::maintenance::claim_next`],
//!   per-tenant fair), runs the built-in executor to produce a plan, and
//!   **commits that plan through [`PostgresCommitBackend`]** as an ordinary,
//!   audited, snapshot-rollback-revertible Iceberg commit — the same one code
//!   path a normal writer uses (spec §6 Pillar C enterprise notes: "every
//!   mutation is a normal Iceberg commit"). Then it writes the
//!   `savings_ledger` row and transitions the job to `succeeded`. The commit
//!   is **conflict-safe**: on an optimistic-CAS loss to a concurrent writer
//!   commit it re-plans against fresh state and retries a bounded number of
//!   times, and if the table stays busy it re-queues the job rather than
//!   failing it — maintenance always yields to writer commits (spec C-F4).
//!
//! - [`run_reconciler`] — the **desired-state loop** (C-F3). Periodically
//!   evaluates enabled policies against each table's computed health and
//!   enqueues jobs for tables that violate their targets (small-file ratio
//!   over threshold, snapshot count over retention). It debounces per table
//!   and is streaming-aware: a table that is actively committing (its newest
//!   snapshot advanced within the coalescing window) is skipped, because
//!   compacting it would only lose the commit race to the writer.
//!
//! # What runs here vs. the store
//!
//! The *control plane* (the job queue, policies, the ledger, and their
//! audit/outbox rows) lives in [`meridian_store::maintenance`]; this module
//! owns the *execution*: reading table state, driving [`meridian_executor`],
//! building the metadata commit, and the two loops. The reconcile debounce
//! state (migration 0013) is read and written here with direct queries.
//!
//! # Expiry (metadata-only)
//!
//! Snapshot expiry is implemented alongside compaction because it is
//! metadata-only: it drops snapshots older than the effective retention (via
//! a `remove-snapshots` [`TableUpdate`]) while **never** removing the current
//! snapshot, any branch/tag-referenced snapshot, or the safety-window tail —
//! the same optimistic-commit discipline, so a racing writer makes it fail
//! cleanly and retry.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use meridian_common::config::MaintenanceConfig;
use meridian_executor::{CompactionOptions, CompactionStats};
use meridian_iceberg::commit::{CommitBackend, CommitBackendError, PointerCas};
use meridian_iceberg::spec::{TableMetadata, TableRequirement, TableUpdate};
use meridian_storage::{Storage, new_metadata_location, read_table_metadata};
use meridian_store::commit::{
    CommitTableOp, DerivedTableState, PostgresCommitBackend, SnapshotIndexRow,
};
use meridian_store::maintenance::{self, JobRecord, JobType, PolicySpec, SavingsInput};
use meridian_store::warehouse::WarehouseRecord;
use meridian_store::{table, tenancy};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

/// Longest pause the loops take after repeated infrastructure errors.
const MAX_ERROR_DELAY: Duration = Duration::from_secs(30);

/// The worker identity recorded on claimed jobs. A ULID suffix disambiguates
/// pods; the prefix keeps it human-legible in the queue's `claimed_by`.
fn worker_id() -> String {
    format!("worker-{}", ulid::Ulid::new())
}

// ---------------------------------------------------------------------------
// Job worker loop
// ---------------------------------------------------------------------------

/// The maintenance job-worker loop: claim one job, run it, repeat; sleep only
/// when the queue is empty. Never returns; run it under `tokio::spawn`.
///
/// Claiming and every state transition go through the store's audited queue
/// API. A full drain (a claim that finds work) loops immediately so a backlog
/// clears without a thundering herd; an empty claim sleeps `worker_poll_ms`.
/// Infrastructure errors back off exponentially (1s → 30s).
pub async fn run_worker(pool: PgPool, config: MaintenanceConfig) {
    let worker = worker_id();
    let idle_sleep = Duration::from_millis(config.worker_poll_ms);
    let mut error_delay = Duration::from_secs(1);
    tracing::info!(%worker, "maintenance job worker started");
    loop {
        match claim_and_run(&pool, &config, &worker).await {
            Ok(true) => {
                // Ran a job; more may be waiting — keep going without sleeping.
                error_delay = Duration::from_secs(1);
            }
            Ok(false) => {
                error_delay = Duration::from_secs(1);
                tokio::time::sleep(idle_sleep).await;
            }
            Err(error) => {
                tracing::warn!(%error, "maintenance worker iteration failed; backing off");
                tokio::time::sleep(error_delay).await;
                error_delay = (error_delay * 2).min(MAX_ERROR_DELAY);
            }
        }
    }
}

/// Claims the next job and runs it once, to completion. Returns whether a job
/// was claimed (so the loop knows whether to sleep). A job-level failure is
/// recorded on the job (via `fail_job`) and is *not* propagated as an iteration
/// error — only claim-path infrastructure failures are.
///
/// Exposed (crate-public) so integration tests can drive one worker step
/// deterministically without racing the spawned loop.
pub async fn claim_and_run(
    pool: &PgPool,
    config: &MaintenanceConfig,
    worker: &str,
) -> Result<bool, meridian_common::MeridianError> {
    let Some(job) = maintenance::claim_next(pool, worker).await? else {
        return Ok(false);
    };
    tracing::info!(job_id = %job.id, job_type = job.job_type.as_str(), table_id = %job.table_id, "claimed maintenance job");

    match execute_job(pool, config, worker, &job).await {
        Ok(JobOutcome::Committed {
            stats,
            table_ident,
            new_snapshot_id,
        }) => {
            record_success(pool, worker, &job, &stats, &table_ident, new_snapshot_id).await?;
        }
        Ok(JobOutcome::Noop { reason }) => {
            // Nothing to do (already compact / nothing expirable). A no-op is
            // a legitimate success — the desired state already holds.
            let result = json!({ "outcome": "noop", "reason": reason });
            if let Err(error) = maintenance::complete_job(pool, &job.id, worker, &result).await {
                tracing::warn!(job_id = %job.id, %error, "failed to mark no-op job succeeded");
            }
        }
        Ok(JobOutcome::Requeued { reason }) => {
            // The table is busy: yield to the writer (spec C-F4). Reset the job
            // to `queued` so a later pass retries; do not count it as a failure.
            requeue_job(pool, &job.id, &reason).await?;
        }
        Err(error) => {
            let payload = json!({ "error": error.to_string() });
            tracing::warn!(job_id = %job.id, %error, "maintenance job failed");
            // If the job has retries left, re-queue for a fresh attempt;
            // otherwise mark it failed. `attempts` was bumped at claim time.
            if job.attempts < config.max_job_attempts {
                requeue_job(pool, &job.id, &format!("retry after error: {error}")).await?;
            } else if let Err(fail_error) =
                maintenance::fail_job(pool, &job.id, worker, &payload).await
            {
                tracing::warn!(job_id = %job.id, %fail_error, "failed to mark job failed");
            }
        }
    }
    Ok(true)
}

/// The result of executing one job.
enum JobOutcome {
    /// A commit landed; carry the ledger stats and the new snapshot id.
    Committed {
        stats: CompactionStats,
        table_ident: String,
        new_snapshot_id: Option<i64>,
    },
    /// Nothing needed doing (already in the desired state).
    Noop { reason: String },
    /// The table is busy; the job was yielded and should be retried later.
    Requeued { reason: String },
}

/// Resets a running job back to `queued`, clearing its claim, so a later pass
/// re-runs it. Used for both conflict-yield and retryable errors.
async fn requeue_job(
    pool: &PgPool,
    job_id: &str,
    reason: &str,
) -> Result<(), meridian_common::MeridianError> {
    // A direct update (not an audited transition): re-queue is an internal
    // scheduling decision, not a terminal outcome. The claim/complete/fail
    // transitions remain the audited lifecycle events.
    let updated = sqlx::query(
        "UPDATE maintenance_jobs
         SET state = 'queued', claimed_by = NULL, started_at = NULL, updated_at = now()
         WHERE id = $1 AND state = 'running'",
    )
    .bind(job_id)
    .execute(pool)
    .await
    .map_err(|e| {
        meridian_common::MeridianError::internal("failed to re-queue maintenance job", e)
    })?;
    if updated.rows_affected() == 1 {
        tracing::info!(job_id = %job_id, reason, "re-queued maintenance job");
    }
    Ok(())
}

/// Records a committed job: append the savings-ledger row, then mark the job
/// `succeeded` with its before/after result. The ledger append is idempotent
/// on `job_id` (a `UNIQUE` guard), so a crash between ledger and completion
/// cannot double-count.
async fn record_success(
    pool: &PgPool,
    worker: &str,
    job: &JobRecord,
    stats: &CompactionStats,
    table_ident: &str,
    new_snapshot_id: Option<i64>,
) -> Result<(), meridian_common::MeridianError> {
    let workspace = job.workspace_id.parse().map_err(|_| {
        meridian_common::MeridianError::internal_msg("job has an invalid workspace id")
    })?;
    let input = SavingsInput {
        bytes_before: i64::try_from(stats.bytes_before).unwrap_or(i64::MAX),
        bytes_after: i64::try_from(stats.bytes_after).unwrap_or(i64::MAX),
        files_before: i64::try_from(stats.files_before).unwrap_or(i64::MAX),
        files_after: i64::try_from(stats.files_after).unwrap_or(i64::MAX),
        est_get_requests_saved: estimate_get_requests_saved(stats),
        methodology: savings_methodology(job.job_type),
    };
    // A ledger row only makes sense when the job actually removed something.
    // A metadata-only expiry removes no data bytes/files, so it does not
    // ledger storage savings; it is still an audited commit and a success.
    if stats.files_before > 0 || stats.bytes_before > 0 {
        let appended = maintenance::append_savings(
            pool,
            workspace,
            &job.id,
            &job.table_id,
            table_ident,
            &input,
            worker,
        )
        .await;
        // A duplicate (already ledgered on a prior crash-retry) is fine.
        if let Err(error) = appended
            && !matches!(error, meridian_common::MeridianError::Conflict(_))
        {
            return Err(error);
        }
    }

    let result = json!({
        "outcome": "committed",
        "new_snapshot_id": new_snapshot_id,
        "files_before": stats.files_before,
        "files_after": stats.files_after,
        "bytes_before": stats.bytes_before,
        "bytes_after": stats.bytes_after,
        "bytes_saved": stats.bytes_saved(),
        "delete_files_removed": stats.delete_files_removed,
    });
    if let Err(error) = maintenance::complete_job(pool, &job.id, worker, &result).await {
        tracing::warn!(job_id = %job.id, %error, "failed to mark job succeeded after commit");
    }
    Ok(())
}

/// The savings methodology string stored on the ledger row (shown in the
/// CFO-legible export so the claim is auditable, spec C-F5).
fn savings_methodology(job_type: JobType) -> String {
    match job_type {
        JobType::Compaction => "bin-pack compaction: bytes/files before vs after the rewrite \
             snapshot; GET-requests-saved = data files removed (one GET per file avoided per scan)"
            .to_owned(),
        JobType::ExpireSnapshots => {
            "snapshot expiry: metadata-only; no data bytes removed".to_owned()
        }
        JobType::RemoveOrphans => "orphan-file cleanup".to_owned(),
        JobType::RewriteManifests => "manifest rewrite".to_owned(),
    }
}

/// A simple, honest GET-request savings estimate: one object-store GET per
/// data file removed is avoided on each future scan (the small-file tax). We
/// count the files removed once; the ledger notes it is per-scan in the
/// methodology so the number is never overstated.
fn estimate_get_requests_saved(stats: &CompactionStats) -> i64 {
    let removed = stats.files_before.saturating_sub(stats.files_after);
    i64::try_from(removed).unwrap_or(i64::MAX)
}

// ---------------------------------------------------------------------------
// Job execution
// ---------------------------------------------------------------------------

/// Runs one job to a [`JobOutcome`]: resolve the table + effective policy,
/// dispatch on the job type, and (for mutating jobs) commit through the
/// backend with conflict-yield retry.
async fn execute_job(
    pool: &PgPool,
    config: &MaintenanceConfig,
    worker: &str,
    job: &JobRecord,
) -> Result<JobOutcome, MaintenanceError> {
    let ctx = resolve_table_context(pool, &job.table_id).await?;
    let policy = effective_policy(pool, &ctx).await?;
    let storage = connect_storage(&ctx.warehouse)?;

    match job.job_type {
        JobType::Compaction => {
            run_compaction(pool, config, worker, job, &ctx, &policy, &storage).await
        }
        JobType::ExpireSnapshots => {
            if !config.expiry_enabled {
                return Ok(JobOutcome::Noop {
                    reason: "snapshot expiry is disabled by configuration".to_owned(),
                });
            }
            run_expiry(pool, config, worker, job, &ctx, &policy, &storage).await
        }
        JobType::RemoveOrphans | JobType::RewriteManifests => {
            Err(MaintenanceError::Unsupported(format!(
                "{} jobs are not implemented by the built-in worker yet",
                job.job_type.as_str()
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

/// Runs a compaction job: plan with the executor, then commit the plan with
/// conflict-yield retry.
async fn run_compaction(
    pool: &PgPool,
    config: &MaintenanceConfig,
    worker: &str,
    job: &JobRecord,
    ctx: &TableContext,
    policy: &PolicySpec,
    storage: &Arc<dyn Storage>,
) -> Result<JobOutcome, MaintenanceError> {
    let target = u64::try_from(policy.target_file_size_bytes)
        .unwrap_or(u64::MAX)
        .max(1);
    let min_files = usize::try_from(policy.min_input_files).unwrap_or(1).max(1);
    let dry_run = job_bool(&job.spec, "dry_run");
    let options = CompactionOptions {
        target_file_size_bytes: target,
        min_input_files: min_files,
        dry_run,
    };
    let backend = commit_backend(pool, ctx, worker);

    for _attempt in 0..=config.commit_retry_limit {
        // Re-read the current metadata each attempt so a re-plan runs against
        // fresh state after a conflict (spec C-F4: re-plan and retry).
        let (pointer_version, metadata_location, metadata) =
            current_metadata(&backend, storage, &ctx.table.id).await?;

        let plan = meridian_executor::compact_table(
            storage.as_ref(),
            &metadata,
            &options,
            &new_snapshot_id,
        )
        .await
        .map_err(MaintenanceError::Compaction)?;

        if plan.is_noop() {
            return Ok(JobOutcome::Noop {
                reason: if dry_run {
                    "dry-run: nothing staged".to_owned()
                } else {
                    "table already compact (no partition met the threshold)".to_owned()
                },
            });
        }

        // Commit the plan's updates/requirements as a normal Iceberg commit.
        match commit_plan(
            &backend,
            storage,
            &ctx.table.id,
            pointer_version,
            &metadata_location,
            &metadata,
            &plan.updates,
            &plan.requirements,
        )
        .await
        {
            Ok(()) => {
                return Ok(JobOutcome::Committed {
                    stats: plan.stats,
                    table_ident: ctx.ident.clone(),
                    new_snapshot_id: plan.new_snapshot_id,
                });
            }
            Err(CommitOutcome::Conflict) => {
                // Lost the race to a writer; the executor already wrote its
                // output/manifests (unreferenced orphans the sweep collects),
                // so re-plan and retry on the next loop iteration.
                tracing::info!(job_id = %job.id, table_id = %ctx.table.id, "compaction lost commit race; re-planning");
            }
            Err(CommitOutcome::Fatal(error)) => return Err(error),
        }
    }
    // Exhausted the conflict budget: the table is committing faster than we
    // can compact it. Yield — re-queue and let a later pass try.
    Ok(JobOutcome::Requeued {
        reason: format!(
            "yielded to concurrent writer commits after {} attempts",
            config.commit_retry_limit + 1
        ),
    })
}

/// A deterministic-per-call snapshot id source for the executor. Uses a fresh
/// UUID's low 63 bits, so the id is always non-negative (Iceberg snapshot ids
/// are non-negative) and collision-free in practice.
fn new_snapshot_id() -> i64 {
    // Mask to the low 63 bits, so the value is always in `0..=i64::MAX` and
    // the conversions are provably lossless (masked to well under each type's
    // range). A fresh v4 UUID makes the low bits effectively random.
    let masked = Uuid::new_v4().as_u128() & u128::from(u64::MAX >> 1);
    i64::try_from(masked).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Snapshot expiry (metadata-only)
// ---------------------------------------------------------------------------

/// Runs a snapshot-expiry job: compute the removable snapshot set (respecting
/// retention count, retention age, the safety window, and never touching the
/// current or any referenced snapshot), then commit a `remove-snapshots`
/// update with conflict-yield retry. Metadata-only: no data files are deleted.
async fn run_expiry(
    pool: &PgPool,
    config: &MaintenanceConfig,
    worker: &str,
    job: &JobRecord,
    ctx: &TableContext,
    policy: &PolicySpec,
    storage: &Arc<dyn Storage>,
) -> Result<JobOutcome, MaintenanceError> {
    let backend = commit_backend(pool, ctx, worker);
    let now_ms = Utc::now().timestamp_millis();

    for _attempt in 0..=config.commit_retry_limit {
        let (pointer_version, metadata_location, metadata) =
            current_metadata(&backend, storage, &ctx.table.id).await?;

        let removable = expirable_snapshots(&metadata, policy, config, now_ms);
        if removable.is_empty() {
            return Ok(JobOutcome::Noop {
                reason: "no snapshots are past retention (age + count + safety window)".to_owned(),
            });
        }

        let snapshots_before = metadata.snapshots.as_ref().map_or(0, Vec::len);
        let updates = vec![TableUpdate::RemoveSnapshots {
            snapshot_ids: removable.clone(),
        }];
        // Assert the table's main branch has not moved since we planned, so a
        // concurrent writer commit makes this fail cleanly and we re-plan.
        let requirements = expiry_requirements(&metadata);

        match commit_plan(
            &backend,
            storage,
            &ctx.table.id,
            pointer_version,
            &metadata_location,
            &metadata,
            &updates,
            &requirements,
        )
        .await
        {
            Ok(()) => {
                // Expiry removes no data files/bytes; the "savings" it records
                // are snapshot-history reduction, surfaced via the job result.
                let stats = CompactionStats {
                    files_before: 0,
                    files_after: 0,
                    bytes_before: 0,
                    bytes_after: 0,
                    records_before: 0,
                    records_after: 0,
                    delete_files_removed: 0,
                };
                tracing::info!(
                    job_id = %job.id,
                    table_id = %ctx.table.id,
                    removed = removable.len(),
                    snapshots_before,
                    "expired snapshots"
                );
                return Ok(JobOutcome::Committed {
                    stats,
                    table_ident: ctx.ident.clone(),
                    new_snapshot_id: None,
                });
            }
            Err(CommitOutcome::Conflict) => {
                tracing::info!(job_id = %job.id, table_id = %ctx.table.id, "expiry lost commit race; re-planning");
            }
            Err(CommitOutcome::Fatal(error)) => return Err(error),
        }
    }
    Ok(JobOutcome::Requeued {
        reason: format!(
            "expiry yielded to concurrent writer commits after {} attempts",
            config.commit_retry_limit + 1
        ),
    })
}

/// Computes the set of snapshot ids that may be expired: those failing BOTH
/// the retention-count keep and the retention-age keep, and which are neither
/// the current snapshot nor referenced by any branch/tag. A fixed safety
/// window (`expiry_min_snapshots_kept`) is always retained on top of the
/// policy count so expiry never trims to the bone (spec C-F2 safety window).
///
/// Pure: takes metadata + policy + now, returns ids. The metadata builder
/// enforces the same never-current / never-referenced invariants as a second
/// safety net at apply time.
fn expirable_snapshots(
    metadata: &TableMetadata,
    policy: &PolicySpec,
    config: &MaintenanceConfig,
    now_ms: i64,
) -> Vec<i64> {
    let Some(snapshots) = &metadata.snapshots else {
        return Vec::new();
    };
    if snapshots.is_empty() {
        return Vec::new();
    }

    // Protected: the current snapshot and every ref target — never expirable.
    let mut protected: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    if let Some(current) = metadata.current_snapshot_id.filter(|id| *id >= 0) {
        protected.insert(current);
    }
    if let Some(refs) = &metadata.refs {
        for reference in refs.values() {
            protected.insert(reference.snapshot_id);
        }
    }

    // Keep the newest N by timestamp, where N is the larger of the policy's
    // retention count and the configured safety-window minimum.
    let keep_count = policy
        .snapshot_retention_count
        .max(config.expiry_min_snapshots_kept)
        .max(1);
    let keep_count = usize::try_from(keep_count).unwrap_or(usize::MAX);

    let mut by_recency: Vec<&meridian_iceberg::spec::Snapshot> = snapshots.iter().collect();
    by_recency.sort_by_key(|s| std::cmp::Reverse(s.timestamp_ms));
    let kept_by_count: std::collections::BTreeSet<i64> = by_recency
        .iter()
        .take(keep_count)
        .map(|s| s.snapshot_id)
        .collect();

    let age_cutoff = now_ms.saturating_sub(policy.snapshot_retention_age_ms);

    snapshots
        .iter()
        .filter(|s| {
            let id = s.snapshot_id;
            // Never expire protected or count-retained snapshots.
            if protected.contains(&id) || kept_by_count.contains(&id) {
                return false;
            }
            // Only expire snapshots strictly older than the age cutoff.
            s.timestamp_ms < age_cutoff
        })
        .map(|s| s.snapshot_id)
        .collect()
}

/// The optimistic requirement for an expiry commit: the `main` branch still
/// points where it did when we planned (so a writer commit in between makes
/// the expiry fail cleanly rather than racing history removal).
fn expiry_requirements(metadata: &TableMetadata) -> Vec<TableRequirement> {
    let mut requirements = vec![TableRequirement::AssertTableUuid {
        uuid: metadata.table_uuid,
    }];
    let main_snapshot = metadata
        .refs
        .as_ref()
        .and_then(|refs| refs.get("main"))
        .map(|r| r.snapshot_id)
        .or(metadata.current_snapshot_id);
    if let Some(snapshot_id) = main_snapshot {
        requirements.push(TableRequirement::AssertRefSnapshotId {
            r#ref: "main".to_owned(),
            snapshot_id: Some(snapshot_id),
        });
    }
    requirements
}

// ---------------------------------------------------------------------------
// The shared commit path
// ---------------------------------------------------------------------------

/// The result of trying to commit a plan.
enum CommitOutcome {
    /// Lost the optimistic CAS to a concurrent writer commit (retryable).
    Conflict,
    /// A non-retryable failure (staging I/O, backend error, build error).
    Fatal(MaintenanceError),
}

/// Applies `updates`/`requirements` to `base`, stages the candidate
/// `metadata.json`, and commits the pointer swap through the backend — the
/// same optimistic-staging + guarded-CAS sequence the REST commit endpoint
/// uses (`docs/design/commit-protocol.md` §3). On a version conflict the
/// staged file is discarded and [`CommitOutcome::Conflict`] is returned so the
/// caller re-plans.
#[allow(clippy::too_many_arguments)]
async fn commit_plan(
    backend: &PostgresCommitBackend,
    storage: &Arc<dyn Storage>,
    table_id: &str,
    pointer_version: u64,
    base_location: &str,
    base: &TableMetadata,
    updates: &[TableUpdate],
    requirements: &[TableRequirement],
) -> Result<(), CommitOutcome> {
    // Requirements are checked against the base we loaded; a violation here is
    // a lost race (the table moved) — treat as conflict so we re-plan.
    for requirement in requirements {
        if requirement.check(Some(base)).is_err() {
            return Err(CommitOutcome::Conflict);
        }
    }

    let mut builder = base.builder_from();
    builder
        .apply_all(updates.iter().cloned())
        .map_err(|e| CommitOutcome::Fatal(MaintenanceError::Build(e.to_string())))?;
    let candidate = builder
        .build(Utc::now().timestamp_millis(), Some(base_location))
        .map_err(|e| CommitOutcome::Fatal(MaintenanceError::Build(e.to_string())))?;

    // Stage under the next pointer version with a fresh uuid: unique per
    // attempt, so no attempt can overwrite a published file.
    let staged_location =
        new_metadata_location(&candidate.location, pointer_version + 1, Uuid::new_v4());
    meridian_storage::write_table_metadata(storage.as_ref(), &staged_location, &candidate)
        .await
        .map_err(|e| CommitOutcome::Fatal(MaintenanceError::Storage(e.to_string())))?;

    let op = CommitTableOp {
        cas: PointerCas {
            table: table_id.to_owned(),
            expected_version: pointer_version,
            new_metadata_location: staged_location.clone(),
        },
        derived: Some(derived_state(&candidate)),
    };

    match backend.commit_tables(std::slice::from_ref(&op), None).await {
        Ok(_) => Ok(()),
        Err(CommitBackendError::VersionConflict { .. }) => {
            discard_staged(storage, &staged_location).await;
            Err(CommitOutcome::Conflict)
        }
        Err(CommitBackendError::StateUnknown { message }) => {
            // Point of no return: the staged file must NOT be deleted (the
            // commit may have applied it). Surface as fatal; the job re-queues
            // or fails, and a re-run is a no-op if it did apply.
            Err(CommitOutcome::Fatal(MaintenanceError::CommitUnknown(
                message,
            )))
        }
        Err(other) => {
            discard_staged(storage, &staged_location).await;
            Err(CommitOutcome::Fatal(MaintenanceError::Backend(
                other.to_string(),
            )))
        }
    }
}

/// Loads the current pointer + base metadata for a table.
async fn current_metadata(
    backend: &PostgresCommitBackend,
    storage: &Arc<dyn Storage>,
    table_id: &str,
) -> Result<(u64, String, TableMetadata), MaintenanceError> {
    let pointer = backend
        .load_pointer(&table_id.to_owned())
        .await
        .map_err(|e| MaintenanceError::Backend(e.to_string()))?;
    let metadata = read_table_metadata(storage.as_ref(), &pointer.metadata_location)
        .await
        .map_err(|e| MaintenanceError::Storage(e.to_string()))?;
    Ok((pointer.version, pointer.metadata_location, metadata))
}

/// Extracts the write-through index state from new metadata (mirrors the
/// table route's `derived_state`; snapshot-expiry commits shrink the indexed
/// snapshot set, which the commit backend replaces wholesale).
fn derived_state(metadata: &TableMetadata) -> DerivedTableState {
    let current = metadata.current_snapshot_id.filter(|id| *id >= 0);
    let snapshots: Vec<SnapshotIndexRow> = metadata
        .snapshots
        .iter()
        .flatten()
        .map(|snapshot| SnapshotIndexRow {
            snapshot_id: snapshot.snapshot_id,
            parent_snapshot_id: snapshot.parent_snapshot_id,
            sequence_number: snapshot.sequence_number,
            timestamp_ms: snapshot.timestamp_ms,
            manifest_list: snapshot.manifest_list.clone(),
            operation: snapshot
                .summary
                .as_ref()
                .and_then(|summary| summary.get("operation").cloned()),
            summary: json!(snapshot.summary.clone().unwrap_or_default()),
            is_current: current == Some(snapshot.snapshot_id),
        })
        .collect();
    DerivedTableState {
        format_version: i16::from(metadata.format_version),
        properties: metadata.properties.clone().unwrap_or_default(),
        event_details: json!({
            "snapshot_count": snapshots.len(),
            "current_snapshot_id": current,
            "maintenance": true,
        }),
        snapshots,
        schema_text: metadata
            .current_schema()
            .map(meridian_store::search::schema_search_text),
    }
}

/// Best-effort delete of a staged file that will never be published.
async fn discard_staged(storage: &Arc<dyn Storage>, location: &str) {
    if let Err(error) = storage.delete(location).await {
        tracing::warn!(%location, %error, "failed to delete orphaned staged maintenance metadata");
    }
}

// ---------------------------------------------------------------------------
// Table + policy resolution
// ---------------------------------------------------------------------------

/// The resolved context for running a job on a table: the record, its display
/// identity, and its containing namespace + warehouse (for policy scope
/// resolution and storage).
struct TableContext {
    table: table::TableRecord,
    ident: String,
    namespace_id: String,
    warehouse: WarehouseRecord,
}

/// Resolves a table id to its full context (table row → namespace → warehouse
/// → display ident). Fails if the table or its containers are gone.
async fn resolve_table_context(
    pool: &PgPool,
    table_id: &str,
) -> Result<TableContext, MaintenanceError> {
    // One join fetches the table, its namespace levels, and its warehouse id.
    let row: Option<(String, String, Vec<String>)> = sqlx::query_as(
        "SELECT t.namespace_id, n.warehouse_id, n.levels
         FROM tables t JOIN namespaces n ON n.id = t.namespace_id
         WHERE t.id = $1",
    )
    .bind(table_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| MaintenanceError::Store(e.to_string()))?;
    let Some((namespace_id, warehouse_id, levels)) = row else {
        return Err(MaintenanceError::TableGone(table_id.to_owned()));
    };

    let table = table_by_id(pool, table_id)
        .await?
        .ok_or_else(|| MaintenanceError::TableGone(table_id.to_owned()))?;
    let warehouse = warehouse_by_id(pool, &warehouse_id).await?.ok_or_else(|| {
        MaintenanceError::TableGone(format!("warehouse {warehouse_id} of {table_id}"))
    })?;

    let ident = if levels.is_empty() {
        table.name.clone()
    } else {
        format!("{}.{}", levels.join("."), table.name)
    };
    Ok(TableContext {
        table,
        ident,
        namespace_id,
        warehouse,
    })
}

/// Resolves the effective [`PolicySpec`] for a table: the most-specific
/// matching policy scope (table > namespace > warehouse), or the C-F3
/// defaults when none is set.
async fn effective_policy(
    pool: &PgPool,
    ctx: &TableContext,
) -> Result<PolicySpec, MaintenanceError> {
    let resolved = maintenance::resolve_effective(
        pool,
        tenancy::default_workspace_id(),
        &ctx.table.id,
        &ctx.namespace_id,
        &ctx.warehouse.id,
    )
    .await
    .map_err(|e| MaintenanceError::Store(e.to_string()))?;
    Ok(resolved.map_or_else(PolicySpec::default, |record| record.spec))
}

/// Loads a table by id (the store's typed accessors are namespace+name; this
/// worker holds a table id from the job, so it queries by id directly).
async fn table_by_id(
    pool: &PgPool,
    table_id: &str,
) -> Result<Option<table::TableRecord>, MaintenanceError> {
    sqlx::query_as::<_, table::TableRecord>(
        "SELECT id, workspace_id, namespace_id, name, table_uuid, metadata_location, \
                previous_metadata_location, pointer_version, format_version, properties, \
                created_at, updated_at
         FROM tables WHERE id = $1",
    )
    .bind(table_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| MaintenanceError::Store(e.to_string()))
}

/// Loads a warehouse by id.
async fn warehouse_by_id(
    pool: &PgPool,
    warehouse_id: &str,
) -> Result<Option<WarehouseRecord>, MaintenanceError> {
    sqlx::query_as::<_, WarehouseRecord>(
        "SELECT id, workspace_id, name, storage_root, storage_config, created_at, updated_at
         FROM warehouses WHERE id = $1",
    )
    .bind(warehouse_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| MaintenanceError::Store(e.to_string()))
}

/// Connects the warehouse's storage profile.
fn connect_storage(warehouse: &WarehouseRecord) -> Result<Arc<dyn Storage>, MaintenanceError> {
    let profile =
        meridian_storage::StorageProfile::parse(&warehouse.storage_root, &warehouse.storage_config)
            .map_err(|e| MaintenanceError::Storage(e.to_string()))?;
    profile
        .connect()
        .map_err(|e| MaintenanceError::Storage(e.to_string()))
}

/// Builds a commit backend scoped to the workspace and a maintenance
/// principal, so every maintenance commit's audit rows attribute to the
/// worker (`worker:<id>`) and are distinguishable from user commits.
fn commit_backend(pool: &PgPool, ctx: &TableContext, worker: &str) -> PostgresCommitBackend {
    let workspace = ctx
        .table
        .workspace_id
        .parse()
        .unwrap_or_else(|_| tenancy::default_workspace_id());
    PostgresCommitBackend::new(pool.clone(), workspace, format!("maintenance:{worker}"))
}

/// Reads a boolean field from a job spec (defaults to `false`).
fn job_bool(spec: &Value, key: &str) -> bool {
    spec.get(key).and_then(Value::as_bool).unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Reconciliation loop (desired-state)
// ---------------------------------------------------------------------------

/// The desired-state reconciliation loop (spec C-F3): periodically evaluate
/// enabled policies against table health and enqueue jobs for violating
/// tables. Never returns; run it under `tokio::spawn`.
pub async fn run_reconciler(pool: PgPool, config: MaintenanceConfig) {
    if !config.reconcile_enabled {
        tracing::info!("maintenance reconciliation loop disabled by configuration");
        return;
    }
    let interval = Duration::from_secs(config.reconcile_interval_secs.max(1));
    let mut error_delay = Duration::from_secs(1);
    tracing::info!(
        interval_secs = config.reconcile_interval_secs,
        "maintenance reconciler started"
    );
    loop {
        match reconcile_once(&pool, &config).await {
            Ok(enqueued) => {
                error_delay = Duration::from_secs(1);
                if enqueued > 0 {
                    tracing::info!(enqueued, "reconciler enqueued maintenance jobs");
                }
                tokio::time::sleep(interval).await;
            }
            Err(error) => {
                tracing::warn!(%error, "reconciler iteration failed; backing off");
                tokio::time::sleep(error_delay).await;
                error_delay = (error_delay * 2).min(MAX_ERROR_DELAY);
            }
        }
    }
}

/// One reconciliation pass: for every table under an enabled policy, evaluate
/// the newest health snapshot against the effective policy targets and
/// enqueue a compaction/expiry job when a target is violated and the debounce
/// + commit-quiet gates allow. Returns the number of jobs enqueued.
///
/// Health is read from the newest persisted `health_snapshots` row (the
/// health model writes these zero-scan); the reconciler does not itself
/// recompute health — that keeps the pass cheap and lets health computation be
/// scheduled independently.
pub async fn reconcile_once(
    pool: &PgPool,
    config: &MaintenanceConfig,
) -> Result<usize, meridian_common::MeridianError> {
    let candidates = reconcile_candidates(pool).await?;
    let now = Utc::now();
    let mut enqueued = 0usize;

    for cand in candidates {
        match evaluate_candidate(pool, config, &cand, now).await {
            Ok(true) => enqueued += 1,
            Ok(false) => {}
            Err(error) => {
                // One table's failure must not stall the pass.
                tracing::warn!(table_id = %cand.table_id, %error, "reconcile evaluation failed for table");
            }
        }
    }
    Ok(enqueued)
}

/// A reconciliation candidate: a table that has at least one enabled policy in
/// its scope chain and a computed health snapshot to evaluate.
struct ReconcileCandidate {
    table_id: String,
    workspace_id: String,
    namespace_id: String,
    warehouse_id: String,
    ident: String,
    score: i16,
    small_file_ratio: f64,
    snapshot_count: i32,
    newest_snapshot_ms: Option<i64>,
    last_enqueued_at: Option<DateTime<Utc>>,
    last_snapshot_ms: Option<i64>,
}

/// Finds tables to evaluate: those whose warehouse, namespace, or self has an
/// *enabled* maintenance policy, joined to their newest health snapshot and
/// their reconcile debounce state. A table with no health snapshot yet is not
/// a candidate (nothing to evaluate against); a table with no policy in scope
/// is not a candidate (reconciliation is opt-in via policy — spec C-F3
/// desired-state is declared, not implicit).
async fn reconcile_candidates(
    pool: &PgPool,
) -> Result<Vec<ReconcileCandidate>, meridian_common::MeridianError> {
    // Latest health per table via a lateral-free correlated pick: the
    // health_snapshots index is (table_id, computed_at DESC), so DISTINCT ON
    // is cheap. Then require an enabled policy somewhere in the table's scope
    // chain (table id, namespace id, or warehouse id).
    let rows: Vec<ReconcileRow> = sqlx::query_as(
        "WITH latest_health AS (
             SELECT DISTINCT ON (h.table_id)
                    h.table_id, h.score, h.small_file_ratio, h.snapshot_count
             FROM health_snapshots h
             ORDER BY h.table_id, h.computed_at DESC
         )
         SELECT t.id AS table_id, t.workspace_id, t.namespace_id, n.warehouse_id, n.levels, t.name,
                lh.score, lh.small_file_ratio, lh.snapshot_count,
                (SELECT MAX(ts.timestamp_ms) FROM table_snapshots ts WHERE ts.table_id = t.id)
                    AS newest_snapshot_ms,
                rs.last_enqueued_at, rs.last_snapshot_ms
         FROM tables t
         JOIN namespaces n ON n.id = t.namespace_id
         JOIN latest_health lh ON lh.table_id = t.id
         LEFT JOIN maintenance_reconcile_state rs ON rs.table_id = t.id
         WHERE EXISTS (
             SELECT 1 FROM maintenance_policies p
             WHERE p.enabled = TRUE AND (
                 (p.scope = 'table' AND p.scope_id = t.id)
                 OR (p.scope = 'namespace' AND p.scope_id = t.namespace_id)
                 OR (p.scope = 'warehouse' AND p.scope_id = n.warehouse_id)
             )
         )",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        meridian_common::MeridianError::internal("failed to load reconcile candidates", e)
    })?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let ident = if r.levels.is_empty() {
                r.name.clone()
            } else {
                format!("{}.{}", r.levels.join("."), r.name)
            };
            ReconcileCandidate {
                table_id: r.table_id,
                workspace_id: r.workspace_id,
                namespace_id: r.namespace_id,
                warehouse_id: r.warehouse_id,
                ident,
                score: r.score,
                small_file_ratio: r.small_file_ratio,
                snapshot_count: r.snapshot_count,
                newest_snapshot_ms: r.newest_snapshot_ms,
                last_enqueued_at: r.last_enqueued_at,
                last_snapshot_ms: r.last_snapshot_ms,
            }
        })
        .collect())
}

#[derive(sqlx::FromRow)]
struct ReconcileRow {
    table_id: String,
    workspace_id: String,
    namespace_id: String,
    warehouse_id: String,
    levels: Vec<String>,
    name: String,
    score: i16,
    small_file_ratio: f64,
    snapshot_count: i32,
    newest_snapshot_ms: Option<i64>,
    last_enqueued_at: Option<DateTime<Utc>>,
    last_snapshot_ms: Option<i64>,
}

/// Evaluates one candidate and enqueues at most one job (compaction takes
/// precedence over expiry — small files hurt scans more than snapshot bloat).
/// Returns whether a job was enqueued. Always records the evaluation in the
/// reconcile state (advancing the observed newest-snapshot watermark).
async fn evaluate_candidate(
    pool: &PgPool,
    config: &MaintenanceConfig,
    cand: &ReconcileCandidate,
    now: DateTime<Utc>,
) -> Result<bool, meridian_common::MeridianError> {
    let workspace = cand.workspace_id.parse().map_err(|_| {
        meridian_common::MeridianError::internal_msg(
            "reconcile candidate has an invalid workspace id",
        )
    })?;

    // Streaming-aware coalescing: if the table's newest snapshot advanced
    // within the commit-quiet window, it is actively committing — skip it
    // (spec C-F3). We compare the current newest-snapshot timestamp against
    // wall-clock now; a snapshot younger than the window means a very recent
    // commit.
    if let Some(newest) = cand.newest_snapshot_ms {
        let age_ms = now.timestamp_millis().saturating_sub(newest);
        if age_ms < config.reconcile_commit_quiet_secs.saturating_mul(1000) {
            record_reconcile_eval(pool, workspace, cand, now, None).await?;
            return Ok(false);
        }
    }

    // Debounce: do not enqueue again within the debounce window of the last
    // enqueue for this table.
    if let Some(last) = cand.last_enqueued_at {
        let since = now.signed_duration_since(last);
        if since
            < chrono::Duration::seconds(
                i64::try_from(config.reconcile_debounce_secs).unwrap_or(i64::MAX),
            )
        {
            record_reconcile_eval(pool, workspace, cand, now, None).await?;
            return Ok(false);
        }
    }

    // If a job for this table is already queued or running, do not pile on.
    if table_has_active_job(pool, &cand.table_id).await? {
        record_reconcile_eval(pool, workspace, cand, now, None).await?;
        return Ok(false);
    }

    // Resolve the effective policy so the thresholds respect per-scope config
    // (retention count for expiry; the compaction threshold is the configured
    // small-file ratio floor).
    let policy = policy_for_reconcile(pool, cand).await?;

    // Compaction first: small-file ratio over the configured floor.
    let job_type = if cand.small_file_ratio >= config.reconcile_small_file_ratio {
        Some(JobType::Compaction)
    } else if cand.snapshot_count
        > policy
            .snapshot_retention_count
            .saturating_add(config.reconcile_snapshot_slack)
    {
        Some(JobType::ExpireSnapshots)
    } else {
        None
    };

    let Some(job_type) = job_type else {
        record_reconcile_eval(pool, workspace, cand, now, None).await?;
        return Ok(false);
    };
    if matches!(job_type, JobType::ExpireSnapshots) && !config.expiry_enabled {
        record_reconcile_eval(pool, workspace, cand, now, None).await?;
        return Ok(false);
    }

    let spec = json!({
        "reason": "reconcile",
        "trigger": match job_type {
            JobType::Compaction => "small_file_ratio",
            _ => "snapshot_count",
        },
        "small_file_ratio": cand.small_file_ratio,
        "snapshot_count": cand.snapshot_count,
        "score": cand.score,
    });
    let job = maintenance::enqueue_job(
        pool,
        workspace,
        &cand.table_id,
        job_type,
        None,
        &spec,
        "reconciler",
    )
    .await?;
    tracing::info!(
        table_id = %cand.table_id,
        ident = %cand.ident,
        job_type = job_type.as_str(),
        job_id = %job.id,
        "reconciler enqueued maintenance job"
    );
    record_reconcile_eval(pool, workspace, cand, now, Some(&job.id)).await?;
    Ok(true)
}

/// Resolves the effective policy for a reconcile candidate (defaults when
/// none is set at any scope).
async fn policy_for_reconcile(
    pool: &PgPool,
    cand: &ReconcileCandidate,
) -> Result<PolicySpec, meridian_common::MeridianError> {
    let resolved = maintenance::resolve_effective(
        pool,
        tenancy::default_workspace_id(),
        &cand.table_id,
        &cand.namespace_id,
        &cand.warehouse_id,
    )
    .await?;
    Ok(resolved.map_or_else(PolicySpec::default, |record| record.spec))
}

/// Whether the table already has a `queued` or `running` maintenance job.
async fn table_has_active_job(
    pool: &PgPool,
    table_id: &str,
) -> Result<bool, meridian_common::MeridianError> {
    let exists: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM maintenance_jobs
         WHERE table_id = $1 AND state IN ('queued', 'running') LIMIT 1",
    )
    .bind(table_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| meridian_common::MeridianError::internal("failed to check active jobs", e))?;
    Ok(exists.is_some())
}

/// Upserts the reconcile debounce state for a table after an evaluation:
/// advances `last_evaluated_at` and the observed newest-snapshot watermark,
/// and sets `last_enqueued_at`/`last_job_id` when a job was enqueued.
async fn record_reconcile_eval(
    pool: &PgPool,
    workspace: meridian_common::id::WorkspaceId,
    cand: &ReconcileCandidate,
    now: DateTime<Utc>,
    enqueued_job: Option<&str>,
) -> Result<(), meridian_common::MeridianError> {
    sqlx::query(
        "INSERT INTO maintenance_reconcile_state
             (table_id, workspace_id, last_evaluated_at, last_enqueued_at, last_snapshot_ms,
              last_snapshot_seen_at, last_job_id, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, now())
         ON CONFLICT (table_id) DO UPDATE SET
             last_evaluated_at = EXCLUDED.last_evaluated_at,
             last_enqueued_at = COALESCE(EXCLUDED.last_enqueued_at, maintenance_reconcile_state.last_enqueued_at),
             last_snapshot_ms = EXCLUDED.last_snapshot_ms,
             last_snapshot_seen_at = EXCLUDED.last_snapshot_seen_at,
             last_job_id = COALESCE(EXCLUDED.last_job_id, maintenance_reconcile_state.last_job_id),
             updated_at = now()",
    )
    .bind(&cand.table_id)
    .bind(workspace.to_string())
    .bind(now)
    .bind(enqueued_job.map(|_| now))
    .bind(cand.newest_snapshot_ms)
    .bind(cand.newest_snapshot_ms.map(|_| now))
    .bind(enqueued_job)
    .execute(pool)
    .await
    .map_err(|e| meridian_common::MeridianError::internal("failed to record reconcile state", e))?;
    // Suppress unused-field warnings for state we persist but don't branch on.
    let _ = &cand.last_snapshot_ms;
    Ok(())
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// A failure while executing a maintenance job. These are recorded on the job
/// (or trigger a re-queue) — never panics; the worker keeps running.
#[derive(Debug, thiserror::Error)]
enum MaintenanceError {
    /// The table (or a container) was dropped between enqueue and execution.
    #[error("table {0} no longer exists")]
    TableGone(String),
    /// The compaction engine failed.
    #[error("compaction failed: {0}")]
    Compaction(#[from] meridian_executor::CompactionError),
    /// Metadata could not be built from the plan's updates.
    #[error("metadata build failed: {0}")]
    Build(String),
    /// Object storage read/write failed.
    #[error("storage error: {0}")]
    Storage(String),
    /// A store-layer query failed.
    #[error("store error: {0}")]
    Store(String),
    /// The commit backend rejected the commit (non-conflict).
    #[error("commit backend error: {0}")]
    Backend(String),
    /// The commit outcome is genuinely unknown (point-of-no-return failure).
    #[error("commit state unknown: {0}")]
    CommitUnknown(String),
    /// The job type is not implemented by the built-in worker.
    #[error("{0}")]
    Unsupported(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use meridian_iceberg::spec::{RefType, Snapshot, SnapshotRef};
    use std::collections::BTreeMap;

    fn snapshot(id: i64, ts: i64) -> Snapshot {
        Snapshot {
            snapshot_id: id,
            parent_snapshot_id: None,
            sequence_number: Some(id),
            timestamp_ms: ts,
            manifest_list: Some(format!("s3://b/t/metadata/snap-{id}.avro")),
            summary: None,
            schema_id: Some(0),
            first_row_id: None,
            added_rows: None,
            extra: serde_json::Map::new(),
        }
    }

    fn metadata_with(snapshots: Vec<Snapshot>, current: i64) -> TableMetadata {
        let mut refs = BTreeMap::new();
        refs.insert(
            "main".to_owned(),
            SnapshotRef {
                snapshot_id: current,
                ref_type: RefType::Branch,
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
                extra: serde_json::Map::new(),
            },
        );
        TableMetadata {
            format_version: 2,
            table_uuid: Uuid::from_u128(1),
            location: "s3://b/t".to_owned(),
            last_sequence_number: Some(i64::try_from(snapshots.len()).unwrap_or(i64::MAX)),
            next_row_id: None,
            last_updated_ms: 0,
            last_column_id: 3,
            schemas: vec![meridian_iceberg::spec::Schema::new(vec![]).with_schema_id(0)],
            current_schema_id: 0,
            partition_specs: vec![meridian_iceberg::spec::PartitionSpec::unpartitioned(0)],
            default_spec_id: 0,
            last_partition_id: 999,
            sort_orders: vec![meridian_iceberg::spec::SortOrder::unsorted()],
            default_sort_order_id: 0,
            properties: None,
            current_snapshot_id: Some(current),
            snapshots: Some(snapshots),
            snapshot_log: None,
            metadata_log: None,
            refs: Some(refs),
            statistics: None,
            partition_statistics: None,
            encryption_keys: None,
            extra: serde_json::Map::new(),
        }
    }

    fn policy(retention_count: i32, retention_age_ms: i64) -> PolicySpec {
        PolicySpec {
            snapshot_retention_count: retention_count,
            snapshot_retention_age_ms: retention_age_ms,
            ..PolicySpec::default()
        }
    }

    #[test]
    fn expiry_never_removes_current_or_referenced() {
        // Five old snapshots, current is the oldest by timestamp on purpose.
        let snaps = vec![
            snapshot(1, 1_000),
            snapshot(2, 2_000),
            snapshot(3, 3_000),
            snapshot(4, 4_000),
            snapshot(5, 5_000),
        ];
        let mut meta = metadata_with(snaps, 1); // current = snapshot 1 (oldest)
        // Add a tag referencing snapshot 2.
        if let Some(refs) = &mut meta.refs {
            refs.insert(
                "audit".to_owned(),
                SnapshotRef {
                    snapshot_id: 2,
                    ref_type: RefType::Tag,
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                    extra: serde_json::Map::new(),
                },
            );
        }
        let cfg = MaintenanceConfig::default();
        // Retain 1 by count, age cutoff at now=10_000 with a 0 age so all are
        // "old"; current (1) and referenced (2) must survive regardless.
        let removable = expirable_snapshots(&meta, &policy(1, 0), &cfg, 10_000);
        assert!(
            !removable.contains(&1),
            "current snapshot must never expire"
        );
        assert!(
            !removable.contains(&2),
            "tag-referenced snapshot must never expire"
        );
        // The newest (5) is kept by count=1; 3 and 4 are expirable.
        assert!(removable.contains(&3));
        assert!(removable.contains(&4));
        assert!(!removable.contains(&5), "newest kept by retention count");
    }

    #[test]
    fn expiry_respects_age_window() {
        let snaps = vec![snapshot(1, 1_000), snapshot(2, 2_000), snapshot(3, 9_500)];
        let meta = metadata_with(snaps, 3);
        let cfg = MaintenanceConfig::default();
        // now=10_000, retention age=1000 -> cutoff 9_000. Keep count 1 keeps
        // the newest (3). Snapshot 2 (ts 2000) is older than cutoff -> expire.
        // Snapshot 1 is current? No, current is 3. So 1 and 2 both older than
        // cutoff and not count-kept -> both expirable.
        let removable = expirable_snapshots(&meta, &policy(1, 1_000), &cfg, 10_000);
        assert!(removable.contains(&1));
        assert!(removable.contains(&2));
        assert!(!removable.contains(&3));
    }

    #[test]
    fn expiry_age_window_protects_recent_snapshots() {
        let snaps = vec![snapshot(1, 8_000), snapshot(2, 9_000), snapshot(3, 9_800)];
        let meta = metadata_with(snaps, 3);
        let cfg = MaintenanceConfig::default();
        // now=10_000, age=5000 -> cutoff 5000; every snapshot is younger than
        // the cutoff, so none expire even though count=1 would allow it: age
        // AND count must both permit removal.
        let removable = expirable_snapshots(&meta, &policy(1, 5_000), &cfg, 10_000);
        assert!(
            removable.is_empty(),
            "all snapshots within the age window are retained"
        );
    }

    #[test]
    fn expiry_safety_window_floor_applies() {
        let snaps = vec![snapshot(1, 1_000), snapshot(2, 2_000), snapshot(3, 3_000)];
        let meta = metadata_with(snaps, 3);
        // Policy retention count 1, but the config safety floor keeps 3.
        let cfg = MaintenanceConfig {
            expiry_min_snapshots_kept: 3,
            ..MaintenanceConfig::default()
        };
        let removable = expirable_snapshots(&meta, &policy(1, 0), &cfg, 10_000);
        assert!(
            removable.is_empty(),
            "safety-window floor retains all three"
        );
    }

    #[test]
    fn get_request_estimate_counts_removed_files() {
        let stats = CompactionStats {
            files_before: 20,
            files_after: 2,
            bytes_before: 100,
            bytes_after: 80,
            records_before: 5,
            records_after: 5,
            delete_files_removed: 0,
        };
        assert_eq!(estimate_get_requests_saved(&stats), 18);
    }
}
