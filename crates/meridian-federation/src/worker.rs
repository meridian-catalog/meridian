//! The background **sync worker** for inbound mirrors (spec Pillar B, B-F1),
//! and the manual "sync now" entry point.
//!
//! [`run_worker`] is spawned by `meridian serve` exactly like the maintenance
//! and events workers: a claim-run-repeat loop that finds mirrors whose
//! `sync_interval` has elapsed (or whose prior `running` claim went stale past
//! the lease, i.e. a crashed worker) and syncs them one at a time, recording
//! each run's outcome. It never returns and
//! never panics — a sync failure is recorded on the mirror and the loop
//! continues.
//!
//! [`sync_mirror_now`] runs one mirror to completion synchronously and records
//! the outcome; the server's `POST /api/v2/mirrors/{name}/sync` handler calls
//! it so a manual trigger does real work (rather than only marking intent).

use std::time::Duration;

use meridian_common::config::FederationConfig;
use meridian_common::id::WorkspaceId;
use meridian_store::federation::{self, MirrorRecord, SyncOutcome};
use meridian_store::{foreign, tenancy};
use sqlx::PgPool;

use crate::sync::{SyncStats, sync_mirror};

/// Longest pause the loop takes after repeated infrastructure errors.
const MAX_ERROR_DELAY: Duration = Duration::from_secs(60);

/// The federation sync-worker loop: find one due mirror, sync it, repeat;
/// sleep only when nothing is due. Never returns; run it under `tokio::spawn`.
///
/// A due mirror is enabled and either has never synced, is past its
/// `sync_interval`, or was flagged `running` by a `sync_now` trigger. Each run
/// is recorded via [`federation::record_sync_result`], so the mirror's
/// freshness/status is always current. Infrastructure errors (claim-path
/// failures) back off exponentially; a *sync* failure is recorded on the mirror
/// and is not treated as a loop error.
pub async fn run_worker(pool: PgPool, config: FederationConfig) {
    if !config.enabled {
        tracing::info!("federation sync worker disabled by configuration");
        return;
    }
    let idle_sleep = Duration::from_secs(config.poll_interval_secs.max(1));
    let request_timeout = Duration::from_secs(config.request_timeout_secs.max(1));
    let workspace = tenancy::default_workspace_id();
    let mut error_delay = Duration::from_secs(1);
    tracing::info!(
        poll_interval_secs = config.poll_interval_secs,
        "federation sync worker started"
    );
    let sync_lease_secs = i64::try_from(config.sync_lease_secs).unwrap_or(i64::MAX);
    loop {
        match sync_next_due(&pool, workspace, request_timeout, sync_lease_secs).await {
            Ok(true) => {
                // Synced one; more may be due — keep going without sleeping.
                error_delay = Duration::from_secs(1);
            }
            Ok(false) => {
                error_delay = Duration::from_secs(1);
                tokio::time::sleep(idle_sleep).await;
            }
            Err(error) => {
                tracing::warn!(%error, "federation worker iteration failed; backing off");
                tokio::time::sleep(error_delay).await;
                error_delay = (error_delay * 2).min(MAX_ERROR_DELAY);
            }
        }
    }
}

/// Finds the next due mirror and syncs it once. Returns whether one was found
/// (so the loop knows whether to sleep). A *sync* failure is recorded on the
/// mirror and returns `Ok(true)` (work was done); only a claim-path failure is
/// an `Err`.
async fn sync_next_due(
    pool: &PgPool,
    workspace: WorkspaceId,
    request_timeout: Duration,
    sync_lease_secs: i64,
) -> Result<bool, meridian_common::MeridianError> {
    let Some(mirror) = claim_due_mirror(pool, workspace, sync_lease_secs).await? else {
        return Ok(false);
    };
    run_and_record(pool, workspace, &mirror, request_timeout).await;
    Ok(true)
}

/// Claims the next due, enabled mirror by marking it `running` with a guarded
/// update (so two workers cannot claim the same mirror), and returns it.
///
/// "Due" = enabled AND (never synced, OR older than its `sync_interval`, OR a
/// **stale** `running` claim past the lease — a crashed prior run we reclaim).
/// Oldest first. A fresh `running` claim (a worker actively syncing) is not due,
/// so a second worker cannot double-sync it. (Manual `sync_now` does not use
/// this path; it syncs the named mirror directly.)
async fn claim_due_mirror(
    pool: &PgPool,
    workspace: WorkspaceId,
    sync_lease_secs: i64,
) -> Result<Option<MirrorRecord>, meridian_common::MeridianError> {
    // One statement claims and returns the mirror: the `UPDATE ... WHERE id IN
    // (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)` pattern is the same
    // single-claim discipline the maintenance queue uses, so concurrent workers
    // never claim the same row *at the same instant*. But the claim commits and
    // releases the row lock before the (potentially long) sync runs, so a
    // `running` mirror must NOT be treated as immediately due — otherwise a
    // second worker (another replica, or the scheduler racing a manual
    // `sync now`) claims and double-syncs it. A `running` mirror is reclaimable
    // only once its `updated_at` is older than the lease (crash recovery),
    // which the claim itself refreshes.
    let record: Option<MirrorRecord> = sqlx::query_as(
        "UPDATE catalog_mirrors m
         SET last_sync_status = 'running', updated_at = now()
         WHERE m.id = (
             SELECT id FROM catalog_mirrors
             WHERE workspace_id = $1
               AND enabled = TRUE
               AND (
                   last_synced_at IS NULL
                   OR (last_sync_status = 'running'
                       AND updated_at < now() - make_interval(secs => $2::double precision))
                   OR last_synced_at < now() - make_interval(secs => sync_interval_s)
               )
             ORDER BY last_synced_at ASC NULLS FIRST
             FOR UPDATE SKIP LOCKED
             LIMIT 1
         )
         RETURNING id, workspace_id, name, kind, endpoint, remote_catalog, config,
                   enabled, sync_interval_s, last_synced_at, last_sync_status,
                   last_sync_detail, asset_count, created_at, updated_at",
    )
    .bind(workspace.to_string())
    .bind(sync_lease_secs)
    .fetch_optional(pool)
    .await
    .map_err(|e| meridian_common::MeridianError::internal("failed to claim due mirror", e))?;
    Ok(record)
}

/// Runs one mirror to completion and records the outcome (success or failure)
/// via [`federation::record_sync_result`]. Never propagates: a sync failure is
/// a recorded run, not a crash.
async fn run_and_record(
    pool: &PgPool,
    workspace: WorkspaceId,
    mirror: &MirrorRecord,
    request_timeout: Duration,
) {
    let outcome = match sync_mirror(pool, workspace, mirror, request_timeout).await {
        Ok(stats) => success_outcome(pool, mirror, &stats).await,
        Err(error) => {
            tracing::warn!(mirror = %mirror.name, %error, "mirror sync failed");
            SyncOutcome {
                status: "error".to_owned(),
                assets_seen: 0,
                detail: Some(truncate(&error.to_string())),
            }
        }
    };
    if let Err(error) = federation::record_sync_result(pool, workspace, &mirror.id, &outcome).await
    {
        // The sync itself may have succeeded; failing to *record* it just means
        // the freshness stamp lags. Log and move on — the next run corrects it.
        tracing::warn!(mirror = %mirror.name, %error, "failed to record mirror sync result");
    }
}

/// Builds the success outcome, using the authoritative indexed count (foreign
/// tables now present for the mirror) rather than only this run's delta, so the
/// mirror's `asset_count` reflects the full mirrored set.
async fn success_outcome(pool: &PgPool, mirror: &MirrorRecord, stats: &SyncStats) -> SyncOutcome {
    let assets_seen = match foreign::count_foreign_assets(pool, &mirror.id).await {
        Ok((_, tables)) => tables,
        // Fall back to the run's observed table count if the count query fails.
        Err(_) => i64::try_from(stats.tables_seen).unwrap_or(i64::MAX),
    };
    SyncOutcome {
        status: "ok".to_owned(),
        assets_seen,
        detail: Some(stats.summary()),
    }
}

/// Syncs a single named mirror to completion and records the outcome — the
/// manual "sync now" path. Returns the [`SyncStats`] on success so the caller
/// (the API handler) can report what happened.
///
/// Returns [`MeridianError::NotFound`] when the mirror does not exist and
/// [`MeridianError::Conflict`] when it is disabled (a disabled mirror is not
/// synced, matching the `sync_now` handler's contract).
pub async fn sync_mirror_now(
    pool: &PgPool,
    mirror_name: &str,
    config: &FederationConfig,
) -> Result<SyncStats, meridian_common::MeridianError> {
    let workspace = tenancy::default_workspace_id();
    let mirror = federation::get_by_name(pool, workspace, mirror_name)
        .await?
        .ok_or_else(|| {
            meridian_common::MeridianError::NotFound(format!(
                "mirror {mirror_name:?} does not exist"
            ))
        })?;
    if !mirror.enabled {
        return Err(meridian_common::MeridianError::Conflict(format!(
            "mirror {mirror_name:?} is disabled; enable it before syncing"
        )));
    }
    let request_timeout = Duration::from_secs(config.request_timeout_secs.max(1));
    let stats = match sync_mirror(pool, workspace, &mirror, request_timeout).await {
        Ok(stats) => stats,
        Err(error) => {
            // Record the failed run, then surface the error to the caller.
            let outcome = SyncOutcome {
                status: "error".to_owned(),
                assets_seen: 0,
                detail: Some(truncate(&error.to_string())),
            };
            let _ = federation::record_sync_result(pool, workspace, &mirror.id, &outcome).await;
            return Err(meridian_common::MeridianError::internal_msg(format!(
                "mirror sync failed: {error}"
            )));
        }
    };
    let outcome = success_outcome(pool, &mirror, &stats).await;
    federation::record_sync_result(pool, workspace, &mirror.id, &outcome).await?;
    Ok(stats)
}

/// Truncates a detail message so a runaway error string cannot bloat the run
/// record.
fn truncate(message: &str) -> String {
    const MAX: usize = 500;
    if message.len() <= MAX {
        return message.to_owned();
    }
    let mut end = MAX;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &message[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_bounds_length_on_char_boundary() {
        let short = "ok";
        assert_eq!(truncate(short), short);
        let long = "é".repeat(400); // 800 bytes
        let out = truncate(&long);
        assert!(out.len() <= 503, "truncated to the cap plus the ellipsis");
        assert!(out.ends_with('…'));
    }
}
