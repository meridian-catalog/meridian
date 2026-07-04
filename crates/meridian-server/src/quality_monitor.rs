//! The post-commit monitor evaluation worker (Pillar E, E-F1 / E-F5).
//!
//! Data-quality monitors are evaluated **after** the commit, never inside the
//! sacred commit transaction (spec §8.3, §12.1): the commit path must not take
//! on monitor work synchronously. The commit already enqueues a durable
//! `table.committed` outbox event; this worker is a crash-safe consumer of that
//! stream, exactly like the lineage worker:
//!
//! 1. read the next batch of published `table.committed` events after our
//!    durable cursor (`outbox::list_published`, gap-free + totally ordered);
//! 2. for each, resolve the committed table and build a zero-scan
//!    [`CommitObservation`] + baseline [`History`] from the `table_snapshots`
//!    write-through index — no data-file access;
//! 3. evaluate every enabled monitor bound to the table (directly or via its
//!    namespace chain), record a `monitor_results` row per monitor, and on a
//!    breach open (or re-touch) an incident — computing the downstream blast
//!    radius via the lineage impact function and capturing the owner;
//! 4. advance the cursor only after the batch is processed.
//!
//! Processing is at-least-once (a crash between step 3 and step 4 reprocesses
//! the batch). Re-evaluation is idempotent at the incident level: the incident
//! de-duplication (`incidents_live_dedup_idx`) means a reprocessed breach
//! re-touches the same live incident rather than opening a duplicate. Duplicate
//! `monitor_results` rows on a reprocess are harmless (they are an append-only
//! series; a repeated row is at worst a redundant data point). The worker never
//! blocks or fails a commit; a per-event error is logged and the cursor still
//! advances past it (a poisoned event must not wedge the stream).
//!
//! # The schema-change monitor
//!
//! Detecting a schema change and classifying it as breaking needs the *base*
//! and *staged* schemas, which the snapshot index does not carry. The worker
//! reads the two small `metadata.json` documents (current + its predecessor via
//! `previous_metadata_location`) — metadata only, never data — and reuses the
//! contract schema-diff (`contracts::classify_schema_evolution`) to classify.
//! This is the one monitor that touches storage, and only the JSON metadata
//! layer.
//!
//! Byte/file counts from the snapshot summary are converted to `f64` for the
//! average-file-size baseline; precision loss beyond 2^52 bytes is irrelevant to
//! an anomaly heuristic, so the cast lint is allowed at module scope (matching
//! the store's scoring modules).
#![allow(clippy::cast_precision_loss)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use meridian_common::config::QualityConfig;
use meridian_common::id::WorkspaceId;
use meridian_iceberg::spec::{Schema, TableMetadata};
use meridian_lineage::impact::{self, Change};
use meridian_storage::{Storage, read_table_metadata};
use meridian_store::contracts::{self, AllowedEvolution};
use meridian_store::incidents::{self, NewIncident, Source};
use meridian_store::monitors::{
    self, CommitObservation, Evaluation, History, Monitor, MonitorKind, NewResult, ResultStatus,
    Severity,
};
use meridian_store::warehouse::WarehouseRecord;
use serde_json::Value;
use sqlx::PgPool;

/// The durable consumer name this worker owns in `event_consumers`. The
/// `system:` prefix marks it internal (cursor advances are not audited).
pub const CONSUMER_NAME: &str = "system:quality-monitor";

/// The outbox event types the worker consumes: the committed-table stream (for
/// monitor evaluation) and the contract-violation stream (to open a
/// contract-sourced incident so the ledger is one pane of glass, E-F5). All are
/// derived off the sacred commit path.
const COMMITTED: &str = "table.committed";
const CONTRACT_VIOLATED: &str = "quality.contract.violated";
const CONTRACT_QUARANTINED: &str = "quality.contract.quarantined";
const CONTRACT_BLOCKED: &str = "quality.contract.blocked";

/// How many events to process per batch.
const BATCH_SIZE: i64 = 200;

/// The maximum downstream depth the blast-radius query walks when opening an
/// incident (bounded so one incident cannot walk an unbounded chain).
const BLAST_DEPTH: u32 = 5;

/// The background loop. Never returns; run under `tokio::spawn`. Drains the
/// backlog in batches, then polls once caught up.
pub async fn run_worker(pool: PgPool, workspace_id: WorkspaceId, config: QualityConfig) {
    let poll_interval = Duration::from_secs(config.poll_interval_secs.max(1));
    let mut error_delay = Duration::from_secs(1);
    let max_error_delay = Duration::from_secs(30);
    tracing::info!("quality monitor evaluation worker started");
    loop {
        match process_batch(&pool, workspace_id, &config).await {
            Ok(processed) => {
                error_delay = Duration::from_secs(1);
                if processed > 0 {
                    tracing::debug!(
                        processed,
                        "quality monitor worker evaluated committed events"
                    );
                }
                if processed < BATCH_SIZE {
                    tokio::time::sleep(poll_interval).await;
                }
            }
            Err(error) => {
                tracing::warn!(%error, "quality monitor worker batch failed; backing off");
                tokio::time::sleep(error_delay).await;
                error_delay = (error_delay * 2).min(max_error_delay);
            }
        }
    }
}

/// Reads and processes one batch, advancing the cursor. Returns the number of
/// events consumed. Public so integration tests can drive one deterministic
/// pass (no reliance on the poll loop timing).
pub async fn process_batch(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    config: &QualityConfig,
) -> Result<i64, meridian_common::MeridianError> {
    let cursor = read_cursor(pool, workspace_id).await?;
    let types = [
        COMMITTED.to_owned(),
        CONTRACT_VIOLATED.to_owned(),
        CONTRACT_QUARANTINED.to_owned(),
        CONTRACT_BLOCKED.to_owned(),
    ];
    let events =
        meridian_store::outbox::list_published(pool, &cursor, Some(&types), BATCH_SIZE).await?;
    if events.is_empty() {
        return Ok(0);
    }

    let mut last_id = cursor;
    for event in &events {
        let result = if event.event_type == COMMITTED {
            evaluate_committed(pool, workspace_id, config, event).await
        } else {
            open_contract_incident(pool, workspace_id, event).await
        };
        if let Err(error) = result {
            tracing::warn!(
                %error,
                event_id = %event.id,
                event_type = %event.event_type,
                aggregate = %event.aggregate,
                "quality worker failed to process event; skipping",
            );
        }
        last_id = event.id.clone();
    }

    write_cursor(pool, workspace_id, &last_id).await?;
    Ok(i64::try_from(events.len()).unwrap_or(i64::MAX))
}

/// Opens (or re-touches) a contract-sourced incident from a contract-violation
/// outbox event, so a circuit-breaker violation shows up in the same ledger as
/// a monitor breach (E-F5). The event payload carries the contract identity, the
/// table, the mode, and the violations; severity maps from the mode (block =
/// high, quarantine = medium, warn = low). Idempotent via incident
/// de-duplication, so reprocessing a batch does not duplicate the incident.
async fn open_contract_incident(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    event: &meridian_store::outbox::OutboxRecord,
) -> Result<(), meridian_common::MeridianError> {
    let payload = &event.payload;
    let Some(table_id) = payload.get("table_id").and_then(Value::as_str) else {
        return Ok(()); // malformed payload; nothing to open
    };
    let contract_name = payload
        .get("contract_name")
        .and_then(Value::as_str)
        .unwrap_or("contract");
    let mode = payload
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("warn");
    let severity = match mode {
        "block" => Severity::High,
        "quarantine" => Severity::Medium,
        _ => Severity::Low,
    };

    // The first violation's detail is the incident detail; its kind is the
    // incident kind so distinct violation kinds de-duplicate separately.
    let first = payload
        .get("violations")
        .and_then(Value::as_array)
        .and_then(|v| v.first());
    let violation_kind = first
        .and_then(|v| v.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or("contract-violation")
        .to_owned();
    let detail = first
        .and_then(|v| v.get("detail"))
        .and_then(Value::as_str)
        .unwrap_or("a data contract was violated")
        .to_owned();

    // Resolve the table's ident + owner + blast radius (best-effort — the
    // incident is the point, the enrichment is a bonus).
    let ident = resolve_table_context(pool, table_id)
        .await?
        .map_or_else(|| table_id.to_owned(), |ctx| ctx.ident);
    let (owner, radius) = blast_radius(pool, workspace_id, table_id).await;

    let title = format!("{ident} violated the {contract_name} contract ({mode})");
    incidents::open_or_touch_standalone(
        pool,
        workspace_id,
        "system:quality-monitor",
        &NewIncident {
            table_id,
            table_ident: &ident,
            source: Source::Contract,
            kind: &violation_kind,
            severity,
            title: &title,
            detail: &detail,
            owner: owner.as_deref(),
            blast_radius: radius,
            monitor_id: None,
        },
    )
    .await?;
    Ok(())
}

/// Table context resolved for one committed event.
struct TableContext {
    table: meridian_store::table::TableRecord,
    ident: String,
    namespace_chain: Vec<String>,
}

/// Evaluates every monitor bound to one committed table, addressed by the event
/// aggregate `table:<id>`.
async fn evaluate_committed(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    config: &QualityConfig,
    event: &meridian_store::outbox::OutboxRecord,
) -> Result<(), meridian_common::MeridianError> {
    let Some(table_id) = event.aggregate.strip_prefix("table:") else {
        return Ok(()); // not a table aggregate
    };

    let Some(ctx) = resolve_table_context(pool, table_id).await? else {
        return Ok(()); // the table (or its containers) is gone; nothing to evaluate
    };

    let monitors =
        monitors::resolve_for_table(pool, workspace_id, table_id, &ctx.namespace_chain).await?;
    if monitors.is_empty() {
        return Ok(()); // nothing watches this table
    }

    // Build the zero-scan observation + history once for all monitors.
    let Some(obs) = current_observation(pool, table_id).await? else {
        return Ok(()); // no current snapshot (metadata-only commit)
    };
    let history = load_history(pool, table_id, obs.snapshot_id, config.history_window).await?;

    for monitor in &monitors {
        if let Err(error) =
            evaluate_one(pool, workspace_id, config, &ctx, monitor, &obs, &history).await
        {
            tracing::warn!(
                %error,
                monitor_id = %monitor.id,
                kind = %monitor.kind,
                "quality monitor evaluation failed for one monitor; continuing",
            );
        }
    }
    Ok(())
}

/// Evaluates one monitor: computes the result, records it, and on a breach opens
/// an incident. The result-row write and (on breach) the incident open share one
/// transaction, so a breach and its evidence commit atomically.
async fn evaluate_one(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    config: &QualityConfig,
    ctx: &TableContext,
    monitor: &Monitor,
    obs: &CommitObservation,
    history: &History,
) -> Result<(), meridian_common::MeridianError> {
    // The two kinds the pure engine cannot score from a successful-commit
    // observation are scored here with the extra inputs.
    let eval = match monitor.kind {
        MonitorKind::SchemaChange => evaluate_schema_change(pool, ctx, &monitor.config).await?,
        MonitorKind::CommitFailure => {
            let failures =
                count_recent_commit_failures(pool, workspace_id, &ctx.table.id, config).await?;
            monitors::score_commit_failure(failures, config.commit_failure_threshold)
        }
        other => other.evaluate(obs, history, &monitor.config),
    };

    // Compute the blast radius + owner only when we are about to open an
    // incident (a breach) — no point walking lineage for an ok/warn result.
    let (owner, blast_radius) = if eval.status == ResultStatus::Breach {
        blast_radius(pool, workspace_id, &ctx.table.id).await
    } else {
        (None, serde_json::json!([]))
    };

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| meridian_store::map_sqlx_error("failed to begin monitor evaluation tx", e))?;

    monitors::record_result_in_tx(
        &mut tx,
        workspace_id,
        &NewResult {
            monitor_id: &monitor.id,
            table_id: &ctx.table.id,
            kind: monitor.kind,
            eval: &eval,
            snapshot_id: Some(obs.snapshot_id),
        },
    )
    .await?;

    if eval.status == ResultStatus::Breach {
        let title = incident_title(monitor.kind, &ctx.ident);
        incidents::open_or_touch(
            &mut tx,
            workspace_id,
            "system:quality-monitor",
            &NewIncident {
                table_id: &ctx.table.id,
                table_ident: &ctx.ident,
                source: Source::Monitor,
                kind: monitor.kind.as_str(),
                severity: monitor.severity,
                title: &title,
                detail: &eval.detail,
                owner: owner.as_deref(),
                blast_radius,
                monitor_id: Some(&monitor.id),
            },
        )
        .await?;
    }

    tx.commit()
        .await
        .map_err(|e| meridian_store::map_sqlx_error("failed to commit monitor evaluation tx", e))?;
    Ok(())
}

/// A one-line incident title for a monitor kind + table.
fn incident_title(kind: MonitorKind, ident: &str) -> String {
    let what = match kind {
        MonitorKind::Freshness => "is stale",
        MonitorKind::Volume => "had an anomalous write volume",
        MonitorKind::SchemaChange => "had a breaking schema change",
        MonitorKind::FileSize => "had a small-file regression",
        MonitorKind::SnapshotDebt => "has snapshot / delete-file debt",
        MonitorKind::CommitFailure => "is failing commits",
    };
    format!("{ident} {what}")
}

/// Computes the downstream blast radius + owner for a table via the lineage
/// impact function (treating the change as a whole-table impact — the broadest
/// blast). A lineage failure degrades gracefully to no blast + no owner rather
/// than blocking the incident; the incident itself is the important signal.
async fn blast_radius(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
) -> (Option<String>, Value) {
    match impact::impact_of(
        pool,
        workspace_id,
        table_id,
        &Change::DropTable,
        BLAST_DEPTH,
    )
    .await
    {
        Ok(report) => {
            // The impacted table's *own* owner is captured on the incident; the
            // blast radius lists the downstream assets + their owners so the
            // notification can route to everyone affected.
            let assets: Vec<Value> = report
                .affected
                .iter()
                .map(|a| {
                    serde_json::json!({
                        "table_id": a.table_id,
                        "ident": a.ident,
                        "owner": a.owner,
                        "depth": a.depth,
                    })
                })
                .collect();
            // The changed table's own owner comes from the impact of a self —
            // read it directly from the table's properties for correctness (the
            // impact report owners are the *downstream* owners).
            let owner = table_owner(pool, table_id).await;
            (owner, Value::Array(assets))
        }
        Err(error) => {
            tracing::debug!(%error, table_id, "blast-radius lineage query failed; incident opens without it");
            (table_owner(pool, table_id).await, serde_json::json!([]))
        }
    }
}

/// Reads a table's `owner` property, or `None` when unset/absent. Never
/// fabricated.
async fn table_owner(pool: &PgPool, table_id: &str) -> Option<String> {
    let row: Option<sqlx::types::Json<BTreeMap<String, String>>> =
        sqlx::query_scalar("SELECT properties FROM tables WHERE id = $1")
            .bind(table_id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    row.and_then(|props| {
        props
            .0
            .get("owner")
            .filter(|v| !v.trim().is_empty())
            .cloned()
    })
}

// ---------------------------------------------------------------------------
// Zero-scan observation + history from the snapshot index
// ---------------------------------------------------------------------------

/// A snapshot index row's fields the scorers read.
struct SnapshotRow {
    snapshot_id: i64,
    timestamp_ms: i64,
    summary: Value,
    is_current: bool,
}

/// Reads all snapshot index rows for a table (small — bounded by the retained
/// snapshot set). Ordered newest-first by timestamp.
async fn read_snapshot_rows(
    pool: &PgPool,
    table_id: &str,
) -> Result<Vec<SnapshotRow>, meridian_common::MeridianError> {
    let rows: Vec<(i64, i64, Value, bool)> = sqlx::query_as(
        "SELECT snapshot_id, timestamp_ms, summary, is_current
         FROM table_snapshots WHERE table_id = $1
         ORDER BY timestamp_ms DESC, snapshot_id DESC",
    )
    .bind(table_id)
    .fetch_all(pool)
    .await
    .map_err(|e| meridian_store::map_sqlx_error("failed to read snapshot index for monitor", e))?;
    Ok(rows
        .into_iter()
        .map(
            |(snapshot_id, timestamp_ms, summary, is_current)| SnapshotRow {
                snapshot_id,
                timestamp_ms,
                summary,
                is_current,
            },
        )
        .collect())
}

/// Reads a string-valued summary key and parses it as `i64`.
fn summary_i64(summary: &Value, key: &str) -> Option<i64> {
    summary.get(key).and_then(|v| match v {
        Value::String(s) => s.parse::<i64>().ok(),
        Value::Number(n) => n.as_i64(),
        _ => None,
    })
}

/// Builds the [`CommitObservation`] for the table's current snapshot from the
/// index. Returns `None` when there is no current snapshot row.
async fn current_observation(
    pool: &PgPool,
    table_id: &str,
) -> Result<Option<CommitObservation>, meridian_common::MeridianError> {
    let rows = read_snapshot_rows(pool, table_id).await?;
    let snapshot_count = i64::try_from(rows.len()).unwrap_or(i64::MAX);
    let Some(current) = rows.iter().find(|r| r.is_current) else {
        return Ok(None);
    };
    Ok(Some(observation_from_row(current, snapshot_count)))
}

/// Builds a [`CommitObservation`] from one snapshot row + the retained count.
fn observation_from_row(row: &SnapshotRow, snapshot_count: i64) -> CommitObservation {
    let s = &row.summary;
    CommitObservation {
        snapshot_id: row.snapshot_id,
        timestamp_ms: row.timestamp_ms,
        added_records: summary_i64(s, "added-records"),
        total_records: summary_i64(s, "total-records"),
        added_data_files: summary_i64(s, "added-data-files"),
        added_files_size: summary_i64(s, "added-files-size"),
        snapshot_count,
        total_delete_files: summary_i64(s, "total-delete-files"),
        operation: s
            .get("operation")
            .and_then(Value::as_str)
            .map(str::to_owned),
    }
}

/// Builds the baseline [`History`] from the prior snapshots (all except the one
/// under evaluation), bounded to the most-recent `window`. Every field is
/// derived from the index summaries — no data-file access.
async fn load_history(
    pool: &PgPool,
    table_id: &str,
    current_snapshot_id: i64,
    window: i64,
) -> Result<History, meridian_common::MeridianError> {
    let rows = read_snapshot_rows(pool, table_id).await?;
    let total = rows.len();
    let mut hist = History::default();
    let window = usize::try_from(window.max(1)).unwrap_or(usize::MAX);

    for (idx, row) in rows.iter().enumerate() {
        if row.snapshot_id == current_snapshot_id {
            continue; // exclude the commit under evaluation
        }
        if hist.timestamps_ms.len() >= window {
            break; // newest `window` prior commits only (rows are newest-first)
        }
        hist.timestamps_ms.push(row.timestamp_ms);
        if let Some(added) = summary_i64(&row.summary, "added-records") {
            hist.added_records.push(added);
        }
        if let (Some(bytes), Some(files)) = (
            summary_i64(&row.summary, "added-files-size"),
            summary_i64(&row.summary, "added-data-files"),
        ) && files > 0
        {
            hist.avg_file_bytes.push(bytes as f64 / files as f64);
        }
        if let Some(deletes) = summary_i64(&row.summary, "total-delete-files") {
            hist.delete_files.push(deletes);
        }
        // The retained snapshot count as-of each prior commit is not stored per
        // row; approximate the count-at-time as (total - index) so a growing
        // chain shows a rising baseline. This is monotonic and index-only.
        let count_at = i64::try_from(total.saturating_sub(idx)).unwrap_or(i64::MAX);
        hist.snapshot_counts.push(count_at);
    }
    Ok(hist)
}

// ---------------------------------------------------------------------------
// Schema-change monitor (reads the two metadata JSONs, reuses contract diff)
// ---------------------------------------------------------------------------

/// Evaluates the schema-change monitor by comparing the current schema against
/// its predecessor and reusing the contract schema-diff to classify breaking-
/// ness. Reads only the `metadata.json` layer (current + previous), never data.
/// Degrades to an ok "not measurable" result when either metadata is missing.
async fn evaluate_schema_change(
    pool: &PgPool,
    ctx: &TableContext,
    config: &monitors::MonitorConfig,
) -> Result<Evaluation, meridian_common::MeridianError> {
    let (Some(current_loc), Some(previous_loc)) = (
        ctx.table.metadata_location.as_deref(),
        ctx.table.previous_metadata_location.as_deref(),
    ) else {
        // First commit (no predecessor) or no location: nothing to diff.
        return Ok(monitors::score_schema_change(false, false, config));
    };

    let storage = match connect_storage(pool, ctx).await {
        Ok(storage) => storage,
        Err(error) => {
            tracing::debug!(%error, "schema-change monitor could not connect storage; skipping");
            return Ok(monitors::score_schema_change(false, false, config));
        }
    };

    let current = read_metadata(storage.as_ref(), current_loc).await;
    let previous = read_metadata(storage.as_ref(), previous_loc).await;
    let (Some(current), Some(previous)) = (current, previous) else {
        return Ok(monitors::score_schema_change(false, false, config));
    };

    let (Some(base_schema), Some(staged_schema)) =
        (previous.current_schema(), current.current_schema())
    else {
        return Ok(monitors::score_schema_change(false, false, config));
    };

    let (changed, breaking) = classify_change(base_schema, staged_schema);
    Ok(monitors::score_schema_change(changed, breaking, config))
}

/// Classifies the change from `base` to `staged`: `(changed, breaking)`.
/// `changed` is any structural difference; `breaking` is whether the contract
/// schema-diff (under the strictest `no_narrowing` rule) reports a
/// drop/narrow/tighten. Additive-only changes are `changed && !breaking`.
fn classify_change(base: &Schema, staged: &Schema) -> (bool, bool) {
    let changed = base.fields != staged.fields;
    if !changed {
        return (false, false);
    }
    // Reuse the contract classifier: any violation under `no_narrowing` is a
    // breaking change (a narrowing, a drop, or a nullability tighten). An
    // additive change produces no violations under `no_narrowing`.
    let mut violations = Vec::new();
    contracts::classify_schema_evolution(
        base,
        staged,
        AllowedEvolution::NoNarrowing,
        &mut violations,
    );
    (true, !violations.is_empty())
}

/// Reads a metadata document, returning `None` on any error (the caller treats
/// an unreadable metadata as "not measurable" rather than failing).
async fn read_metadata(storage: &dyn Storage, location: &str) -> Option<TableMetadata> {
    read_table_metadata(storage, location).await.ok()
}

// ---------------------------------------------------------------------------
// Commit-failure monitor (reads the audit trail)
// ---------------------------------------------------------------------------

/// Counts recent failed/retried commit attempts on a table from the events feed.
/// A commit *retry storm* shows up as `quality.contract.blocked` events (block-
/// mode rejections) plus any recorded commit-conflict retries. We count the
/// blocked-commit events inside a recent window as the honest, index-only
/// signal (a genuinely failed write leaves this trail); a broader failure
/// taxonomy is a tracked refinement.
async fn count_recent_commit_failures(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
    config: &QualityConfig,
) -> Result<i64, meridian_common::MeridianError> {
    // Window: the last `history_window × poll` seconds is a reasonable recent
    // window; use a fixed 1-hour lookback bounded by the config for simplicity.
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events_outbox
         WHERE workspace_id = $1
           AND aggregate = $2
           AND event_type = 'quality.contract.blocked'
           AND created_at > now() - interval '1 hour'",
    )
    .bind(workspace_id.to_string())
    .bind(format!("table:{table_id}"))
    .fetch_one(pool)
    .await
    .map_err(|e| meridian_store::map_sqlx_error("failed to count recent commit failures", e))?;
    let _ = config; // window is fixed for now; config bounds the threshold
    Ok(count)
}

// ---------------------------------------------------------------------------
// Table context + storage
// ---------------------------------------------------------------------------

/// Resolves a table id to its record + display ident + namespace scope chain
/// (self and ancestors, for resolving namespace-bound monitors). Returns `None`
/// when the table or its containers are gone.
async fn resolve_table_context(
    pool: &PgPool,
    table_id: &str,
) -> Result<Option<TableContext>, meridian_common::MeridianError> {
    let table: Option<meridian_store::table::TableRecord> = sqlx::query_as(
        "SELECT id, workspace_id, namespace_id, name, table_uuid, metadata_location, \
                previous_metadata_location, pointer_version, format_version, properties, \
                mirror_id, created_at, updated_at
         FROM tables WHERE id = $1",
    )
    .bind(table_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| meridian_store::map_sqlx_error("failed to load table for monitor", e))?;
    let Some(table) = table else {
        return Ok(None);
    };

    // The namespace levels + warehouse, for the ident and the scope chain.
    let ns_row: Option<(String, Vec<String>)> =
        sqlx::query_as("SELECT n.warehouse_id, n.levels FROM namespaces n WHERE n.id = $1")
            .bind(&table.namespace_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                meridian_store::map_sqlx_error("failed to load namespace for monitor", e)
            })?;
    let Some((warehouse_id, levels)) = ns_row else {
        return Ok(None);
    };
    let warehouse_name: Option<String> =
        sqlx::query_scalar("SELECT name FROM warehouses WHERE id = $1")
            .bind(&warehouse_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                meridian_store::map_sqlx_error("failed to load warehouse for monitor", e)
            })?;
    let Some(warehouse_name) = warehouse_name else {
        return Ok(None);
    };

    let ident = {
        let mut ident = warehouse_name;
        for level in &levels {
            ident.push('.');
            ident.push_str(level);
        }
        ident.push('.');
        ident.push_str(&table.name);
        ident
    };

    let namespace_chain = namespace_scope_chain(pool, &warehouse_id, &levels).await?;

    Ok(Some(TableContext {
        table,
        ident,
        namespace_chain,
    }))
}

/// Resolves the namespace ids in a table's self-and-ancestors chain (for
/// namespace-bound monitor resolution). Walks the level prefixes: `[a, b, c]`
/// resolves the ids of `a`, `a.b`, and `a.b.c` that exist. Mirrors the RBAC
/// scope-chain builder but is inlined here to avoid a route dependency in the
/// worker.
async fn namespace_scope_chain(
    pool: &PgPool,
    warehouse_id: &str,
    levels: &[String],
) -> Result<Vec<String>, meridian_common::MeridianError> {
    let mut chain = Vec::new();
    for depth in 1..=levels.len() {
        let prefix = &levels[..depth];
        let id: Option<String> =
            sqlx::query_scalar("SELECT id FROM namespaces WHERE warehouse_id = $1 AND levels = $2")
                .bind(warehouse_id)
                .bind(prefix)
                .fetch_optional(pool)
                .await
                .map_err(|e| {
                    meridian_store::map_sqlx_error(
                        "failed to resolve namespace chain for monitor",
                        e,
                    )
                })?;
        if let Some(id) = id {
            chain.push(id);
        }
    }
    Ok(chain)
}

/// Connects the storage profile of the warehouse a table lives in.
async fn connect_storage(
    pool: &PgPool,
    ctx: &TableContext,
) -> Result<Arc<dyn Storage>, meridian_common::MeridianError> {
    let warehouse: Option<WarehouseRecord> = sqlx::query_as(
        "SELECT w.id, w.workspace_id, w.name, w.storage_root, w.storage_config, w.created_at, \
                w.updated_at
         FROM warehouses w
         JOIN namespaces n ON n.warehouse_id = w.id
         WHERE n.id = $1",
    )
    .bind(&ctx.table.namespace_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| meridian_store::map_sqlx_error("failed to load warehouse for storage", e))?;
    let Some(warehouse) = warehouse else {
        return Err(meridian_common::MeridianError::internal_msg(
            "warehouse for table is gone",
        ));
    };
    let profile =
        meridian_storage::StorageProfile::parse(&warehouse.storage_root, &warehouse.storage_config)
            .map_err(|e| meridian_common::MeridianError::internal("bad storage profile", e))?;
    profile
        .connect()
        .map_err(|e| meridian_common::MeridianError::internal("failed to connect storage", e))
}

// ---------------------------------------------------------------------------
// Durable cursor (identical discipline to the lineage worker)
// ---------------------------------------------------------------------------

async fn read_cursor(
    pool: &PgPool,
    workspace_id: WorkspaceId,
) -> Result<String, meridian_common::MeridianError> {
    let cursor: Option<Option<String>> = sqlx::query_scalar(
        "SELECT cursor FROM event_consumers WHERE workspace_id = $1 AND name = $2",
    )
    .bind(workspace_id.to_string())
    .bind(CONSUMER_NAME)
    .fetch_optional(pool)
    .await
    .map_err(|e| meridian_store::map_sqlx_error("failed to read monitor cursor", e))?;
    Ok(cursor.flatten().unwrap_or_default())
}

async fn write_cursor(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    cursor: &str,
) -> Result<(), meridian_common::MeridianError> {
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
    .map_err(|e| meridian_store::map_sqlx_error("failed to write monitor cursor", e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn summary_i64_parses_string_and_number() {
        let s = json!({ "total-records": "42", "added-data-files": 7 });
        assert_eq!(summary_i64(&s, "total-records"), Some(42));
        assert_eq!(summary_i64(&s, "added-data-files"), Some(7));
        assert_eq!(summary_i64(&s, "missing"), None);
    }

    #[test]
    fn observation_reads_iceberg_summary() {
        let row = SnapshotRow {
            snapshot_id: 100,
            timestamp_ms: 1_700_000_000_000,
            summary: json!({
                "operation": "append",
                "added-records": "500",
                "total-records": "1500",
                "added-data-files": "3",
                "added-files-size": "300000",
                "total-delete-files": "2",
            }),
            is_current: true,
        };
        let obs = observation_from_row(&row, 5);
        assert_eq!(obs.added_records, Some(500));
        assert_eq!(obs.total_records, Some(1500));
        assert_eq!(obs.added_data_files, Some(3));
        assert_eq!(obs.added_files_size, Some(300_000));
        assert_eq!(obs.total_delete_files, Some(2));
        assert_eq!(obs.snapshot_count, 5);
        assert_eq!(obs.avg_added_file_bytes(), Some(100_000.0));
    }

    #[test]
    fn classify_change_detects_additive_vs_breaking() {
        use meridian_iceberg::spec::{PrimitiveType, StructField, Type};
        let field = |id, name: &str, req, ty| {
            if req {
                StructField::required(id, name, Type::Primitive(ty))
            } else {
                StructField::optional(id, name, Type::Primitive(ty))
            }
        };
        let base = Schema::new(vec![
            field(1, "id", true, PrimitiveType::Long),
            field(2, "email", false, PrimitiveType::String),
        ])
        .with_schema_id(0);

        // Additive: add a column.
        let mut added = base.fields.clone();
        added.push(field(3, "region", false, PrimitiveType::String));
        let additive = Schema::new(added).with_schema_id(0);
        assert_eq!(classify_change(&base, &additive), (true, false));

        // Breaking: drop a column.
        let dropped =
            Schema::new(vec![field(1, "id", true, PrimitiveType::Long)]).with_schema_id(0);
        assert_eq!(classify_change(&base, &dropped), (true, true));

        // Unchanged.
        assert_eq!(classify_change(&base, &base), (false, false));
    }

    #[test]
    fn incident_titles_are_human() {
        assert!(incident_title(MonitorKind::Volume, "wh.ns.orders").contains("orders"));
        assert!(incident_title(MonitorKind::Freshness, "wh.ns.orders").contains("stale"));
    }
}
