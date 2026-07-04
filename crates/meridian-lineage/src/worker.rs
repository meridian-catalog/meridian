//! The post-commit lineage hook (F-F1), as a background worker.
//!
//! Lineage is derived **after** the commit, never inside the commit
//! transaction: the sacred commit path (spec §8.3, §12.1) must not take on
//! lineage work synchronously. The commit already enqueues a durable
//! `table.committed` outbox event in its own transaction; this worker is a
//! crash-safe consumer of that event stream:
//!
//! 1. read the next batch of published `table.committed` events after our
//!    durable cursor (`[list_published]`, gap-free and totally ordered);
//! 2. for each, load the committed table's **current** snapshot summary from
//!    the `table_snapshots` write-through index (authoritative — the outbox
//!    payload deliberately carries only ids, not the whole summary);
//! 3. derive commit-native lineage from that summary
//!    ([`crate::commit_hook::record_commit_lineage`]);
//! 4. advance the cursor only after the batch is processed.
//!
//! Processing is at-least-once (a crash between step 3 and step 4 reprocesses
//! the batch), and edge upserts are idempotent, so reprocessing is safe. The
//! worker never blocks or fails a commit; a derivation error on one event is
//! logged and the cursor still advances past it (a poisoned event must not
//! wedge the whole stream), because the edge it would have produced is
//! recoverable from OpenLineage or a later commit.
//!
//! `meridian serve` spawns [`run_worker`] alongside the events/maintenance/
//! federation workers; aborting it at shutdown is fine (durable cursor).

use std::time::Duration;

use meridian_common::Result;
use meridian_common::id::WorkspaceId;
use meridian_store::outbox;
use serde_json::Value;
use sqlx::PgPool;

use crate::commit_hook::record_commit_lineage;

/// The durable consumer name this worker owns in `event_consumers`. The
/// `system:` prefix marks it as internal bookkeeping (cursor advances are not
/// audited — an offset is consumption state, not a catalog mutation; see the
/// consumer module docs).
pub const CONSUMER_NAME: &str = "system:lineage";

/// The one outbox event type the worker consumes.
const COMMITTED: &str = "table.committed";

/// How many events to process per batch.
const BATCH_SIZE: i64 = 200;

/// The background loop. Never returns; run under `tokio::spawn`. Drains the
/// backlog in batches, then polls every `poll_interval` once caught up.
pub async fn run_worker(pool: PgPool, workspace_id: WorkspaceId, poll_interval: Duration) {
    let mut error_delay = Duration::from_secs(1);
    let max_error_delay = Duration::from_secs(30);
    tracing::info!("lineage post-commit worker started");
    loop {
        match process_batch(&pool, workspace_id).await {
            Ok(processed) => {
                error_delay = Duration::from_secs(1);
                if processed > 0 {
                    tracing::debug!(processed, "lineage worker processed committed events");
                }
                // A full batch means more is waiting — keep going; otherwise
                // the stream is caught up, so sleep.
                if processed < BATCH_SIZE {
                    tokio::time::sleep(poll_interval).await;
                }
            }
            Err(error) => {
                tracing::warn!(%error, "lineage worker batch failed; backing off");
                tokio::time::sleep(error_delay).await;
                error_delay = (error_delay * 2).min(max_error_delay);
            }
        }
    }
}

/// Reads and processes one batch, advancing the cursor. Returns the number of
/// events consumed (so the caller knows whether to keep draining).
pub async fn process_batch(pool: &PgPool, workspace_id: WorkspaceId) -> Result<i64> {
    let cursor = read_cursor(pool, workspace_id).await?;
    let types = [COMMITTED.to_owned()];
    let events = outbox::list_published(pool, &cursor, Some(&types), BATCH_SIZE).await?;
    if events.is_empty() {
        return Ok(0);
    }

    let mut last_id = cursor;
    for event in &events {
        // A per-event derivation failure is logged and skipped: it must not
        // wedge the cursor for every following event.
        if let Err(error) = handle_committed(pool, workspace_id, &event.aggregate).await {
            tracing::warn!(
                %error,
                event_id = %event.id,
                aggregate = %event.aggregate,
                "lineage derivation failed for committed table; skipping event",
            );
        }
        last_id = event.id.clone();
    }

    write_cursor(pool, workspace_id, &last_id).await?;
    Ok(i64::try_from(events.len()).unwrap_or(i64::MAX))
}

/// Derives commit-native lineage for one committed table, addressed by the
/// event aggregate `table:<id>`.
async fn handle_committed(pool: &PgPool, workspace_id: WorkspaceId, aggregate: &str) -> Result<()> {
    let Some(table_id) = aggregate.strip_prefix("table:") else {
        return Ok(()); // not a table aggregate; nothing to do
    };
    let Some(summary) = current_snapshot_summary(pool, table_id).await? else {
        return Ok(()); // no current snapshot (e.g. metadata-only commit)
    };
    let recorded = record_commit_lineage(pool, workspace_id, table_id, &summary).await?;
    if recorded > 0 {
        tracing::debug!(table_id, recorded, "recorded commit-native lineage edges");
    }
    Ok(())
}

/// Reads the current snapshot's summary from the write-through index. Returns
/// `None` when the table has no current snapshot row.
async fn current_snapshot_summary(pool: &PgPool, table_id: &str) -> Result<Option<Value>> {
    let summary: Option<Value> = sqlx::query_scalar(
        "SELECT summary FROM table_snapshots
         WHERE table_id = $1 AND is_current = TRUE
         LIMIT 1",
    )
    .bind(table_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| meridian_store::map_sqlx_error("failed to read current snapshot summary", e))?;
    Ok(summary)
}

/// Reads the worker's durable cursor, returning the empty string (start of
/// feed) when it has never committed one.
async fn read_cursor(pool: &PgPool, workspace_id: WorkspaceId) -> Result<String> {
    let cursor: Option<Option<String>> = sqlx::query_scalar(
        "SELECT cursor FROM event_consumers WHERE workspace_id = $1 AND name = $2",
    )
    .bind(workspace_id.to_string())
    .bind(CONSUMER_NAME)
    .fetch_optional(pool)
    .await
    .map_err(|e| meridian_store::map_sqlx_error("failed to read lineage cursor", e))?;
    Ok(cursor.flatten().unwrap_or_default())
}

/// Upserts the worker's durable cursor. This is internal consumption
/// bookkeeping — deliberately not audited and emitting no outbox event, per
/// the consumer module's rationale (an offset advance is not a mutation, and
/// an audited cursor would feed a subscribed-to-everything consumer forever).
/// The `cursor <= EXCLUDED.cursor` guard keeps advances monotonic even under
/// a reprocessed batch.
async fn write_cursor(pool: &PgPool, workspace_id: WorkspaceId, cursor: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO event_consumers (workspace_id, name, cursor)
         VALUES ($1, $2, $3)
         ON CONFLICT (workspace_id, name)
         DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()
         WHERE event_consumers.cursor IS NULL
            OR event_consumers.cursor <= EXCLUDED.cursor",
    )
    .bind(workspace_id.to_string())
    .bind(CONSUMER_NAME)
    .bind(cursor)
    .execute(pool)
    .await
    .map_err(|e| meridian_store::map_sqlx_error("failed to write lineage cursor", e))?;
    Ok(())
}
