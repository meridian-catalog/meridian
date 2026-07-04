//! Catalog-federation persistence: mirror configs, mirrored assets, and the
//! sprawl summary that rolls up across every catalog Meridian knows about
//! (its own warehouses plus registered mirrors).
//!
//! A *mirror* is a registered pointer to an external catalog (another IRC
//! endpoint or an AWS Glue Data Catalog) that Meridian tracks for sprawl
//! visibility and zero-copy register, without owning the underlying storage.
//! See `migrations/0014_federation_mirrors.sql` for the schema and the
//! division of labor with the federation sync worker.
//!
//! Every mutation here writes its audit row and outbox event on the same
//! transaction as the state change (commit protocol §I4), exactly like
//! [`crate::warehouse`].
//!
//! INTEGRATION NOTE (federation crate): the sync worker owns
//! [`record_sync_result`] and the population of `mirror_assets`. The CRUD and
//! read paths here are what the management API, CLI, and console consume. If
//! the worker is not yet wired, mirrors created here report as never-synced
//! and the sprawl summary counts zero assets for them.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::json;
use sqlx::PgPool;
use sqlx::types::Json;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// Accepted mirror source kinds. Kept in sync with the DB CHECK constraint.
pub const MIRROR_KINDS: &[&str] = &["iceberg-rest", "glue"];

/// A persisted mirror row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MirrorRecord {
    /// ULID of the mirror.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Operator-facing handle, unique per workspace.
    pub name: String,
    /// Source kind (`iceberg-rest` | `glue`).
    pub kind: String,
    /// Connection endpoint (IRC base URI, or AWS region for Glue).
    pub endpoint: String,
    /// Remote catalog identifier within the endpoint, when applicable.
    pub remote_catalog: Option<String>,
    /// Non-secret connection options.
    pub config: Json<BTreeMap<String, String>>,
    /// Whether the sync worker should pull this mirror.
    pub enabled: bool,
    /// Desired sync cadence in seconds (advisory).
    pub sync_interval_s: i32,
    /// Last successful/attempted sync time; `None` = never synced.
    pub last_synced_at: Option<DateTime<Utc>>,
    /// Outcome of the most recent run (`ok` | `error` | `running`).
    pub last_sync_status: Option<String>,
    /// Human-readable detail for the most recent run.
    pub last_sync_detail: Option<String>,
    /// Assets discovered on the most recent successful sync.
    pub asset_count: i64,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// Fields for creating a mirror. Validation (kind membership, non-empty
/// endpoint) is the API's responsibility; this layer trusts its inputs and
/// relies on the DB CHECK constraints as a backstop.
#[derive(Debug, Clone)]
pub struct NewMirror {
    /// Operator-facing handle.
    pub name: String,
    /// Source kind.
    pub kind: String,
    /// Connection endpoint.
    pub endpoint: String,
    /// Remote catalog identifier, when applicable.
    pub remote_catalog: Option<String>,
    /// Non-secret connection options.
    pub config: BTreeMap<String, String>,
    /// Whether the mirror is enabled for syncing.
    pub enabled: bool,
    /// Desired sync cadence in seconds.
    pub sync_interval_s: i32,
}

/// A recorded sync run (history entry).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SyncRunRecord {
    /// ULID of the run.
    pub id: String,
    /// The mirror this run belongs to.
    pub mirror_id: String,
    /// Run outcome (`ok` | `error` | `running`).
    pub status: String,
    /// Assets discovered on this run.
    pub assets_seen: i64,
    /// Error message or short summary.
    pub detail: Option<String>,
    /// When the run started.
    pub started_at: DateTime<Utc>,
    /// When the run finished; `None` while running.
    pub finished_at: Option<DateTime<Utc>>,
}

const MIRROR_COLUMNS: &str = "id, workspace_id, name, kind, endpoint, remote_catalog, config, \
     enabled, sync_interval_s, last_synced_at, last_sync_status, last_sync_detail, \
     asset_count, created_at, updated_at";

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// Creates a mirror, with its audit row and outbox event, atomically.
///
/// Returns [`MeridianError::Conflict`] when a mirror of the same name already
/// exists in the workspace.
pub async fn create(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    new: NewMirror,
    principal: &str,
) -> Result<MirrorRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin mirror create", e))?;

    let id = Ulid::new().to_string();
    let record: MirrorRecord = sqlx::query_as(&format!(
        "INSERT INTO catalog_mirrors
             (id, workspace_id, name, kind, endpoint, remote_catalog, config,
              enabled, sync_interval_s)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         RETURNING {MIRROR_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(&new.name)
    .bind(&new.kind)
    .bind(&new.endpoint)
    .bind(&new.remote_catalog)
    .bind(Json(&new.config))
    .bind(new.enabled)
    .bind(new.sync_interval_s)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!("mirror {:?} already exists", new.name))
        } else {
            map_sqlx_error("failed to insert mirror", e)
        }
    })?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("mirror:{id}"),
            event_type: "mirror.created".to_owned(),
            payload: json!({ "name": new.name, "kind": new.kind, "endpoint": new.endpoint }),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "mirror.create".to_owned(),
            resource: format!("mirror:{id}"),
            details: json!({ "name": new.name, "kind": new.kind, "endpoint": new.endpoint }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit mirror create", e))?;

    Ok(record)
}

/// Lists all mirrors of a workspace, ordered by name.
pub async fn list(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<MirrorRecord>> {
    sqlx::query_as(&format!(
        "SELECT {MIRROR_COLUMNS} FROM catalog_mirrors
         WHERE workspace_id = $1 ORDER BY name"
    ))
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list mirrors", e))
}

/// Looks a mirror up by name within a workspace.
pub async fn get_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
) -> Result<Option<MirrorRecord>> {
    sqlx::query_as(&format!(
        "SELECT {MIRROR_COLUMNS} FROM catalog_mirrors
         WHERE workspace_id = $1 AND name = $2"
    ))
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load mirror", e))
}

/// Fields a mirror update may change. `None` leaves a field untouched.
#[derive(Debug, Default, Clone)]
// `remote_catalog` is intentionally `Option<Option<String>>`: outer `None` =
// unchanged, `Some(None)` = clear, `Some(Some)` = set.
#[allow(clippy::option_option)]
pub struct MirrorUpdate {
    /// New endpoint, if changing.
    pub endpoint: Option<String>,
    /// New remote catalog id. `Some(None)` clears it; `None` leaves it.
    pub remote_catalog: Option<Option<String>>,
    /// New config (replaces wholesale), if changing.
    pub config: Option<BTreeMap<String, String>>,
    /// New enabled flag, if changing.
    pub enabled: Option<bool>,
    /// New sync interval, if changing.
    pub sync_interval_s: Option<i32>,
}

impl MirrorUpdate {
    /// True when no field is set (nothing to update).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.endpoint.is_none()
            && self.remote_catalog.is_none()
            && self.config.is_none()
            && self.enabled.is_none()
            && self.sync_interval_s.is_none()
    }
}

/// Applies a partial update to a mirror by name, audited.
///
/// Returns [`MeridianError::NotFound`] when the mirror does not exist.
pub async fn update_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    update: MirrorUpdate,
    principal: &str,
) -> Result<MirrorRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin mirror update", e))?;

    let existing: Option<MirrorRecord> = sqlx::query_as(&format!(
        "SELECT {MIRROR_COLUMNS} FROM catalog_mirrors
         WHERE workspace_id = $1 AND name = $2 FOR UPDATE"
    ))
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load mirror for update", e))?;

    let Some(existing) = existing else {
        return Err(MeridianError::NotFound(format!(
            "mirror {name:?} does not exist"
        )));
    };

    // COALESCE-style application: bind either the new value or the current one.
    let endpoint = update.endpoint.unwrap_or(existing.endpoint);
    let remote_catalog = match update.remote_catalog {
        Some(v) => v,
        None => existing.remote_catalog,
    };
    let config = update.config.map_or(existing.config, Json);
    let enabled = update.enabled.unwrap_or(existing.enabled);
    let sync_interval_s = update.sync_interval_s.unwrap_or(existing.sync_interval_s);

    let record: MirrorRecord = sqlx::query_as(&format!(
        "UPDATE catalog_mirrors
         SET endpoint = $3, remote_catalog = $4, config = $5, enabled = $6,
             sync_interval_s = $7, updated_at = now()
         WHERE workspace_id = $1 AND name = $2
         RETURNING {MIRROR_COLUMNS}"
    ))
    .bind(workspace_id.to_string())
    .bind(name)
    .bind(&endpoint)
    .bind(&remote_catalog)
    .bind(&config)
    .bind(enabled)
    .bind(sync_interval_s)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to update mirror", e))?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("mirror:{}", record.id),
            event_type: "mirror.updated".to_owned(),
            payload: json!({ "name": name }),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "mirror.update".to_owned(),
            resource: format!("mirror:{}", record.id),
            details: json!({ "name": name, "enabled": enabled }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit mirror update", e))?;

    Ok(record)
}

/// Deletes a mirror by name (cascading its assets and run history), audited.
///
/// Returns [`MeridianError::NotFound`] when the mirror does not exist.
pub async fn delete_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin mirror delete", e))?;

    let row: Option<(String,)> = sqlx::query_as(
        "SELECT id FROM catalog_mirrors WHERE workspace_id = $1 AND name = $2 FOR UPDATE",
    )
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load mirror for delete", e))?;

    let Some((id,)) = row else {
        return Err(MeridianError::NotFound(format!(
            "mirror {name:?} does not exist"
        )));
    };

    sqlx::query("DELETE FROM catalog_mirrors WHERE id = $1")
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to delete mirror", e))?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("mirror:{id}"),
            event_type: "mirror.deleted".to_owned(),
            payload: json!({ "name": name }),
        },
    )
    .await?;

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "mirror.delete".to_owned(),
            resource: format!("mirror:{id}"),
            details: json!({ "name": name }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit mirror delete", e))?;

    Ok(())
}

/// Returns the recent sync-run history for a mirror, newest first.
pub async fn sync_history(
    pool: &PgPool,
    mirror_id: &str,
    limit: i64,
) -> Result<Vec<SyncRunRecord>> {
    sqlx::query_as(
        "SELECT id, mirror_id, status, assets_seen, detail, started_at, finished_at
         FROM mirror_sync_runs
         WHERE mirror_id = $1
         ORDER BY started_at DESC
         LIMIT $2",
    )
    .bind(mirror_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load mirror sync history", e))
}

/// The outcome of a completed sync run, recorded by the federation worker.
#[derive(Debug, Clone)]
pub struct SyncOutcome {
    /// Run status (`ok` | `error` | `running`).
    pub status: String,
    /// Assets discovered on this run.
    pub assets_seen: i64,
    /// Error message or short summary.
    pub detail: Option<String>,
}

/// Records a sync run and stamps the mirror's `last_*` fields, atomically.
///
/// INTEGRATION NOTE: this is the entry point the federation sync worker calls
/// after fetching a remote catalog and upserting `mirror_assets`. It is
/// deliberately not audited (it is machine-driven bookkeeping, not an
/// operator mutation) but does emit an outbox event so downstream consumers
/// see sync activity. The worker should populate `mirror_assets` in the same
/// logical operation; this function only records the run summary.
pub async fn record_sync_result(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    mirror_id: &str,
    outcome: &SyncOutcome,
) -> Result<SyncRunRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin sync record", e))?;

    let run_id = Ulid::new().to_string();
    let run: SyncRunRecord = sqlx::query_as(
        "INSERT INTO mirror_sync_runs
             (id, mirror_id, workspace_id, status, assets_seen, detail, finished_at)
         VALUES ($1, $2, $3, $4, $5, $6, now())
         RETURNING id, mirror_id, status, assets_seen, detail, started_at, finished_at",
    )
    .bind(&run_id)
    .bind(mirror_id)
    .bind(workspace_id.to_string())
    .bind(&outcome.status)
    .bind(outcome.assets_seen)
    .bind(&outcome.detail)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to insert sync run", e))?;

    // On a successful run, bump asset_count; otherwise leave the prior count.
    sqlx::query(
        "UPDATE catalog_mirrors
         SET last_synced_at = now(), last_sync_status = $2, last_sync_detail = $3,
             asset_count = CASE WHEN $2 = 'ok' THEN $4 ELSE asset_count END,
             updated_at = now()
         WHERE id = $1",
    )
    .bind(mirror_id)
    .bind(&outcome.status)
    .bind(&outcome.detail)
    .bind(outcome.assets_seen)
    .execute(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to stamp mirror sync status", e))?;

    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("mirror:{mirror_id}"),
            event_type: "mirror.synced".to_owned(),
            payload: json!({ "status": outcome.status, "assets_seen": outcome.assets_seen }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit sync record", e))?;

    Ok(run)
}

// ===========================================================================
// Supplementary sprawl index (`mirror_assets`)
// ===========================================================================
//
// INTEGRATION SEAM (federation crate, ADR 008): a sync run materializes each
// mirrored table as a native foreign `tables` row (via `crate::foreign`) AND
// upserts a flat `mirror_assets` row here. The two indexes serve two
// consumers: native rows drive the read-side features (search/health); these
// rows drive the sprawl summary's location/ownership roll-ups. The sync engine
// builds a `MirrorAssetInput` per discovered table and calls
// [`replace_mirror_assets`] once per run to refresh the mirror's index
// wholesale.

/// One asset row the sync engine records in the supplementary sprawl index.
#[derive(Debug, Clone)]
pub struct MirrorAssetInput {
    /// Fully-qualified remote identity (e.g. `db.schema.table`).
    pub remote_ident: String,
    /// `table` | `view`.
    pub asset_type: String,
    /// The asset's storage location — the join key for duplicate detection.
    pub storage_location: Option<String>,
    /// Remote-reported owner, when available (drives ownership-gap metrics).
    pub owner: Option<String>,
    /// Free-form remote-reported detail.
    pub properties: serde_json::Value,
}

/// Replaces a mirror's supplementary sprawl-index rows wholesale, in one
/// transaction: the index always reflects the mirror's last successful sync.
///
/// This does not audit (machine-driven bookkeeping written alongside the
/// audited foreign-table upserts of the same sync run) and does not emit an
/// outbox event (the `mirror.synced` event from [`record_sync_result`] covers
/// the run). Called by the federation sync engine.
pub async fn replace_mirror_assets(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    mirror_id: &str,
    assets: &[MirrorAssetInput],
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin mirror-asset refresh", e))?;

    sqlx::query("DELETE FROM mirror_assets WHERE mirror_id = $1")
        .bind(mirror_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to clear mirror assets", e))?;

    for asset in assets {
        sqlx::query(
            "INSERT INTO mirror_assets
                 (id, mirror_id, workspace_id, remote_ident, asset_type,
                  storage_location, owner, properties)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (mirror_id, remote_ident) DO UPDATE
               SET asset_type = EXCLUDED.asset_type,
                   storage_location = EXCLUDED.storage_location,
                   owner = EXCLUDED.owner,
                   properties = EXCLUDED.properties,
                   observed_at = now()",
        )
        .bind(Ulid::new().to_string())
        .bind(mirror_id)
        .bind(workspace_id.to_string())
        .bind(&asset.remote_ident)
        .bind(&asset.asset_type)
        .bind(&asset.storage_location)
        .bind(&asset.owner)
        .bind(Json(&asset.properties))
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to upsert mirror asset", e))?;
    }

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit mirror-asset refresh", e))?;
    Ok(())
}

// ===========================================================================
// Sprawl summary
// ===========================================================================

/// Per-source asset counts: one entry per catalog Meridian knows about,
/// whether a native warehouse or a mirror.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct SourceCount {
    /// `warehouse` | `mirror`.
    pub source_type: String,
    /// The source's stable id (warehouse id or mirror id).
    pub source_id: String,
    /// The source's operator-facing name.
    pub name: String,
    /// For mirrors, the kind (`iceberg-rest` | `glue`); for warehouses,
    /// `native`.
    pub kind: String,
    /// Number of assets attributed to this source.
    pub asset_count: i64,
    /// For mirrors: last sync time (RFC3339) or `null` if never; for
    /// warehouses this is always `null` (they are live, not synced).
    pub last_synced_at: Option<DateTime<Utc>>,
}

/// Most duplicate locations returned in a sprawl summary. The true total is
/// reported separately (`SprawlSummary::duplicate_count`) so a large estate is
/// never silently under-reported.
const MAX_DUPLICATES: i64 = 500;

/// A storage location registered in more than one place (a zero-copy
/// duplicate): the same physical dataset pointed at by multiple catalogs.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct DuplicateLocation {
    /// The shared storage location.
    pub storage_location: String,
    /// How many distinct sources register it.
    pub source_count: i64,
    /// The source names that register it (warehouse and/or mirror names).
    pub sources: Vec<String>,
}

/// A mirror whose last successful sync is older than the staleness threshold
/// (or which has never synced).
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct StaleMirror {
    /// Mirror id.
    pub mirror_id: String,
    /// Mirror name.
    pub name: String,
    /// Last sync time, or `null` if never synced.
    pub last_synced_at: Option<DateTime<Utc>>,
    /// Age of the last sync in seconds, or `null` if never synced.
    pub age_seconds: Option<i64>,
    /// The mirror's configured sync interval in seconds.
    pub sync_interval_s: i32,
}

/// The fully computed sprawl summary.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SprawlSummary {
    /// Total distinct sources Meridian tracks (warehouses + mirrors).
    pub source_count: i64,
    /// Warehouses (native sources).
    pub warehouse_count: i64,
    /// Mirrors (external sources).
    pub mirror_count: i64,
    /// Total assets across all sources.
    pub total_assets: i64,
    /// Per-source asset counts.
    pub sources: Vec<SourceCount>,
    /// Storage locations registered in more than one source (capped at
    /// `MAX_DUPLICATES` entries; see `duplicate_count` for the true total).
    pub duplicates: Vec<DuplicateLocation>,
    /// Total number of duplicated locations, whether or not they all fit in
    /// `duplicates`. Lets callers report "showing N of M".
    pub duplicate_count: i64,
    /// Whether `duplicates` was truncated (more exist than are listed).
    pub duplicates_truncated: bool,
    /// Mirrors past their staleness threshold (or never synced).
    pub stale_mirrors: Vec<StaleMirror>,
    /// Count of mirror assets with no known owner (ownership gaps).
    pub ownership_gaps: i64,
    /// Count of mirror assets with a known owner.
    pub owned_mirror_assets: i64,
    /// Health roll-up over the native (warehouse) assets that are indexed.
    pub health: SprawlHealth,
}

/// The health roll-up reused from the maintenance health model, restricted to
/// native assets that have a computed health snapshot.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct SprawlHealth {
    /// Native tables with a computed health snapshot.
    pub tables_scored: i64,
    /// Mean health score across those tables (0-100).
    pub avg_score: f64,
    /// Tables scoring < 50.
    pub unhealthy_count: i64,
    /// Tables scoring 50-79.
    pub degraded_count: i64,
    /// Tables scoring >= 80.
    pub healthy_count: i64,
    /// Total bytes across the scored native tables.
    pub total_bytes: i64,
}

/// Computes the sprawl summary across native warehouses and mirrors.
///
/// The staleness threshold is expressed in seconds; a mirror is stale if its
/// last successful sync is older than that (never-synced mirrors are always
/// stale).
///
/// INTEGRATION NOTE (federation crate, ADR 008): the sync engine materializes
/// each mirrored table as an *ordinary* row in the native `tables` table under
/// a dedicated foreign warehouse (`mirror__<name>`, tagged `meridian:foreign`
/// in its storage config), carrying `tables.mirror_id`. To avoid
/// double-counting, this summary excludes those foreign warehouses from the
/// native source list and attributes foreign tables to their *mirror* instead.
/// Duplicate detection reads the authoritative `tables.metadata_location` for
/// both native and foreign rows, so it works whether or not the supplementary
/// `mirror_assets` index (0014) is populated.
pub async fn sprawl_summary(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    stale_threshold_s: i64,
) -> Result<SprawlSummary> {
    let ws = workspace_id.to_string();

    let sources = sprawl_sources(pool, &ws).await?;
    let warehouse_count = sources
        .iter()
        .filter(|s| s.source_type == "warehouse")
        .count();
    let mirror_count = sources.iter().filter(|s| s.source_type == "mirror").count();
    let total_assets: i64 = sources.iter().map(|s| s.asset_count).sum();

    let duplicates = sprawl_duplicates(pool, &ws).await?;
    let duplicate_count = sprawl_duplicate_count(pool, &ws).await?;
    let duplicates_truncated =
        duplicate_count > i64::try_from(duplicates.len()).unwrap_or(i64::MAX);
    let stale_mirrors = sprawl_stale_mirrors(pool, &ws, stale_threshold_s).await?;
    let (ownership_gaps, owned_mirror_assets) = sprawl_ownership(pool, &ws).await?;
    let health = sprawl_health(pool, &ws).await?;

    Ok(SprawlSummary {
        source_count: i64::try_from(warehouse_count + mirror_count).unwrap_or(i64::MAX),
        warehouse_count: i64::try_from(warehouse_count).unwrap_or(i64::MAX),
        mirror_count: i64::try_from(mirror_count).unwrap_or(i64::MAX),
        total_assets,
        sources,
        duplicates,
        duplicate_count,
        duplicates_truncated,
        stale_mirrors,
        ownership_gaps,
        owned_mirror_assets,
        health,
    })
}

/// Per-source asset counts. Native warehouses (excluding a mirror's private
/// foreign warehouse) count their non-foreign tables; each mirror counts its
/// foreign tables via the denormalized `asset_count`.
async fn sprawl_sources(pool: &PgPool, ws: &str) -> Result<Vec<SourceCount>> {
    sqlx::query_as(
        "SELECT 'warehouse' AS source_type, w.id AS source_id, w.name,
                'native' AS kind,
                (SELECT count(*) FROM tables t
                   JOIN namespaces n ON n.id = t.namespace_id
                  WHERE n.warehouse_id = w.id AND t.mirror_id IS NULL)::bigint AS asset_count,
                NULL::timestamptz AS last_synced_at
           FROM warehouses w
          WHERE w.workspace_id = $1
            AND COALESCE(w.storage_config ->> 'meridian:foreign', 'false') <> 'true'
         UNION ALL
         SELECT 'mirror' AS source_type, m.id AS source_id, m.name,
                m.kind, m.asset_count, m.last_synced_at
           FROM catalog_mirrors m
          WHERE m.workspace_id = $1
          ORDER BY source_type, name",
    )
    .bind(ws)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to compute sprawl source counts", e))
}

/// Storage locations registered in more than one source: a zero-copy overlap.
/// Attributes native tables to their warehouse and foreign tables to their
/// mirror, using the authoritative `tables.metadata_location`.
async fn sprawl_duplicates(pool: &PgPool, ws: &str) -> Result<Vec<DuplicateLocation>> {
    sqlx::query_as(
        "WITH located AS (
             -- Native tables → their (non-foreign) warehouse.
             SELECT t.metadata_location AS location, w.name AS source_name
               FROM tables t
               JOIN namespaces n ON n.id = t.namespace_id
               JOIN warehouses w ON w.id = n.warehouse_id
              WHERE t.workspace_id = $1 AND t.mirror_id IS NULL
                AND t.metadata_location IS NOT NULL
             UNION ALL
             -- Foreign tables → the mirror that materialized them.
             SELECT t.metadata_location AS location, m.name AS source_name
               FROM tables t
               JOIN catalog_mirrors m ON m.id = t.mirror_id
              WHERE t.workspace_id = $1 AND t.mirror_id IS NOT NULL
                AND t.metadata_location IS NOT NULL
             UNION ALL
             -- Supplementary sprawl index (0014), when the worker populates it.
             SELECT ma.storage_location AS location, m.name AS source_name
               FROM mirror_assets ma
               JOIN catalog_mirrors m ON m.id = ma.mirror_id
              WHERE m.workspace_id = $1 AND ma.storage_location IS NOT NULL
         ),
         per_location AS (
             SELECT location,
                    count(DISTINCT source_name) AS source_count,
                    array_agg(DISTINCT source_name ORDER BY source_name) AS sources
               FROM located
              GROUP BY location
         )
         SELECT location AS storage_location, source_count, sources
           FROM per_location
          WHERE source_count > 1
          ORDER BY source_count DESC, location
          LIMIT $2",
    )
    .bind(ws)
    .bind(MAX_DUPLICATES)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to compute sprawl duplicates", e))
}

/// The true number of duplicated locations (`source_count > 1`), uncapped —
/// so callers can report "showing N of M" when [`sprawl_duplicates`] is
/// truncated at [`MAX_DUPLICATES`].
async fn sprawl_duplicate_count(pool: &PgPool, ws: &str) -> Result<i64> {
    let count: i64 = sqlx::query_scalar(
        "WITH located AS (
             SELECT t.metadata_location AS location, w.name AS source_name
               FROM tables t
               JOIN namespaces n ON n.id = t.namespace_id
               JOIN warehouses w ON w.id = n.warehouse_id
              WHERE t.workspace_id = $1 AND t.mirror_id IS NULL
                AND t.metadata_location IS NOT NULL
             UNION ALL
             SELECT t.metadata_location AS location, m.name AS source_name
               FROM tables t
               JOIN catalog_mirrors m ON m.id = t.mirror_id
              WHERE t.workspace_id = $1 AND t.mirror_id IS NOT NULL
                AND t.metadata_location IS NOT NULL
             UNION ALL
             SELECT ma.storage_location AS location, m.name AS source_name
               FROM mirror_assets ma
               JOIN catalog_mirrors m ON m.id = ma.mirror_id
              WHERE m.workspace_id = $1 AND ma.storage_location IS NOT NULL
         )
         SELECT count(*) FROM (
             SELECT location FROM located
              GROUP BY location HAVING count(DISTINCT source_name) > 1
         ) d",
    )
    .bind(ws)
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to count sprawl duplicates", e))?;
    Ok(count)
}

/// Mirrors whose last sync is older than the threshold, or never synced.
async fn sprawl_stale_mirrors(
    pool: &PgPool,
    ws: &str,
    stale_threshold_s: i64,
) -> Result<Vec<StaleMirror>> {
    sqlx::query_as(
        "SELECT id AS mirror_id, name, last_synced_at,
                CASE WHEN last_synced_at IS NULL THEN NULL
                     ELSE EXTRACT(EPOCH FROM (now() - last_synced_at))::bigint
                END AS age_seconds,
                sync_interval_s
           FROM catalog_mirrors
          WHERE workspace_id = $1
            AND (last_synced_at IS NULL
                 OR last_synced_at < now() - make_interval(secs => $2::double precision))
          ORDER BY last_synced_at ASC NULLS FIRST",
    )
    .bind(ws)
    .bind(stale_threshold_s)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to compute sprawl staleness", e))
}

/// Ownership gaps over the supplementary mirror-asset index: assets with no
/// known owner vs. those with one. `(gaps, owned)`.
async fn sprawl_ownership(pool: &PgPool, ws: &str) -> Result<(i64, i64)> {
    sqlx::query_as(
        "SELECT
             count(*) FILTER (WHERE owner IS NULL OR owner = '')::bigint AS gaps,
             count(*) FILTER (WHERE owner IS NOT NULL AND owner <> '')::bigint AS owned
           FROM mirror_assets ma
           JOIN catalog_mirrors m ON m.id = ma.mirror_id
          WHERE m.workspace_id = $1",
    )
    .bind(ws)
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to compute sprawl ownership gaps", e))
}

/// Health roll-up over native (non-foreign) tables with a computed snapshot,
/// reusing the maintenance health model's persisted scores.
async fn sprawl_health(pool: &PgPool, ws: &str) -> Result<SprawlHealth> {
    sqlx::query_as(
        "WITH latest AS (
             SELECT DISTINCT ON (h.table_id) h.score, h.total_bytes
               FROM health_snapshots h
               JOIN tables t ON t.id = h.table_id
              WHERE t.workspace_id = $1 AND t.mirror_id IS NULL
              ORDER BY h.table_id, h.computed_at DESC
         )
         SELECT
             count(*)::bigint AS tables_scored,
             COALESCE(AVG(score), 0)::double precision AS avg_score,
             count(*) FILTER (WHERE score < 50)::bigint AS unhealthy_count,
             count(*) FILTER (WHERE score >= 50 AND score < 80)::bigint AS degraded_count,
             count(*) FILTER (WHERE score >= 80)::bigint AS healthy_count,
             COALESCE(SUM(total_bytes), 0)::bigint AS total_bytes
           FROM latest",
    )
    .bind(ws)
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to compute sprawl health rollup", e))
}
