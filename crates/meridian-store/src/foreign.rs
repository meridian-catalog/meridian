//! Foreign-asset materialization for inbound catalog mirrors (Pillar B,
//! B-F1).
//!
//! A *foreign asset* is a namespace or table synced from an external catalog
//! (via a mirror in `catalog_mirrors`; see [`crate::federation`]) and stored
//! as an **ordinary row in the native `namespaces` / `tables` tables**, tagged
//! with `mirror_id`. That single tag is the whole native-vs-foreign
//! distinction: a row with `mirror_id IS NULL` is native and writable; a row
//! with `mirror_id` set is foreign and **read-only** (writes are rejected at
//! the commit boundary — see the server's `commit_table` / `commit_transaction`
//! guards). Reusing the native tables is what makes search (the 0010 triggers),
//! the health model, and the write-through snapshot index work on foreign
//! assets with no read-path changes (ADR 008).
//!
//! This module is the write side the federation **sync engine** calls: it
//! creates the mirror's dedicated warehouse, upserts foreign namespaces and
//! tables, lists what a mirror currently holds (for diffing against the
//! source), and removes assets that vanished from the source. Every mutation
//! writes its audit row and outbox event on the same transaction as the state
//! change, the same discipline as [`crate::table`] and [`crate::warehouse`].

use std::collections::BTreeMap;

use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::json;
use sqlx::PgPool;
use sqlx::types::Json;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::commit::SnapshotIndexRow;
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// Storage-config key marking a warehouse as holding a mirror's foreign
/// (read-only) assets. Its presence is the cheap signal the create/register
/// guards use to reject native writes under a foreign warehouse.
pub const FOREIGN_WAREHOUSE_MARKER: &str = "meridian:foreign";

/// Storage-config key recording which mirror a foreign warehouse belongs to.
pub const FOREIGN_WAREHOUSE_MIRROR_KEY: &str = "meridian:mirror_id";

/// The warehouse name for a mirror's foreign assets, derived from the mirror
/// name so it is stable and human-legible in listings.
#[must_use]
pub fn foreign_warehouse_name(mirror_name: &str) -> String {
    format!("mirror__{mirror_name}")
}

/// True when a warehouse (identified by its storage config) holds a mirror's
/// foreign assets. Used by the write-path guards to reject native
/// create/register under a foreign warehouse.
#[must_use]
pub fn storage_config_is_foreign(storage_config: &BTreeMap<String, String>) -> bool {
    storage_config
        .get(FOREIGN_WAREHOUSE_MARKER)
        .is_some_and(|v| v == "true")
}

/// Ensures the mirror's dedicated warehouse exists and returns its id.
///
/// The warehouse is created on first sync (idempotently), named
/// `mirror__<mirror-name>`, rooted at a synthetic `mirror://<mirror-name>`
/// location (foreign assets are never written to, so the root is a label, not
/// a real storage target), and marked foreign via its storage config. A
/// concurrent creation loses the unique race and is resolved by re-reading.
///
/// Not itself audited as a distinct event: it is an implementation detail of a
/// sync run, and the `warehouse.created` outbox/audit rows from the underlying
/// insert already record it.
pub async fn ensure_foreign_warehouse(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    mirror_id: &str,
    mirror_name: &str,
    principal: &str,
) -> Result<String> {
    let name = foreign_warehouse_name(mirror_name);
    if let Some(id) = warehouse_id_by_name(pool, workspace_id, &name).await? {
        return Ok(id);
    }
    let mut config = BTreeMap::new();
    config.insert(FOREIGN_WAREHOUSE_MARKER.to_owned(), "true".to_owned());
    config.insert(
        FOREIGN_WAREHOUSE_MIRROR_KEY.to_owned(),
        mirror_id.to_owned(),
    );
    let root = format!("mirror://{mirror_name}");
    match crate::warehouse::create(pool, workspace_id, &name, &root, config, principal).await {
        Ok(record) => Ok(record.id),
        // Lost the create race to a concurrent sync of the same mirror: the
        // warehouse now exists, so re-read it.
        Err(MeridianError::Conflict(_)) => warehouse_id_by_name(pool, workspace_id, &name)
            .await?
            .ok_or_else(|| {
                MeridianError::internal_msg("foreign warehouse vanished after a create conflict")
            }),
        Err(other) => Err(other),
    }
}

/// Looks up a warehouse id by name within a workspace.
async fn warehouse_id_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT id FROM warehouses WHERE workspace_id = $1 AND name = $2")
            .bind(workspace_id.to_string())
            .bind(name)
            .fetch_optional(pool)
            .await
            .map_err(|e| map_sqlx_error("failed to look up foreign warehouse", e))?;
    Ok(row.map(|(id,)| id))
}

/// Upserts a foreign namespace (idempotent on `(warehouse_id, levels)`) and
/// returns its id. Tagged with `mirror_id`; created rows carry the mirror's
/// properties. A pre-existing native namespace at the same path is a
/// conflict — a mirror must own its warehouse exclusively, so this should
/// never collide in practice (the warehouse is mirror-private).
pub async fn upsert_foreign_namespace(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    warehouse_id: &str,
    mirror_id: &str,
    levels: &[String],
    principal: &str,
) -> Result<String> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin foreign namespace upsert", e))?;

    // Fast path: already present for this mirror.
    let existing: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT id, mirror_id FROM namespaces WHERE warehouse_id = $1 AND levels = $2 FOR UPDATE",
    )
    .bind(warehouse_id)
    .bind(levels)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load foreign namespace", e))?;

    if let Some((id, existing_mirror)) = existing {
        if existing_mirror.as_deref() != Some(mirror_id) {
            return Err(MeridianError::Conflict(format!(
                "namespace {:?} already exists and is not owned by this mirror",
                levels.join(".")
            )));
        }
        tx.commit()
            .await
            .map_err(|e| map_sqlx_error("failed to commit foreign namespace upsert", e))?;
        return Ok(id);
    }

    let id = Ulid::new().to_string();
    sqlx::query(
        "INSERT INTO namespaces (id, workspace_id, warehouse_id, levels, properties, mirror_id)
         VALUES ($1, $2, $3, $4, '{}'::jsonb, $5)",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(warehouse_id)
    .bind(levels)
    .bind(mirror_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to insert foreign namespace", e))?;

    let payload = json!({ "namespace": levels, "mirror_id": mirror_id, "foreign": true });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("namespace:{id}"),
            event_type: "namespace.created".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "federation.namespace.sync".to_owned(),
            resource: format!("namespace:{id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit foreign namespace upsert", e))?;
    Ok(id)
}

/// The metadata to write through when upserting a foreign table.
#[derive(Debug, Clone)]
pub struct ForeignTableInput<'a> {
    /// Owning (foreign) namespace id.
    pub namespace_id: &'a str,
    /// Namespace levels (for event/audit payloads).
    pub namespace_levels: &'a [String],
    /// Table name.
    pub name: &'a str,
    /// Iceberg table UUID (canonical hyphenated form) from the source.
    pub table_uuid: &'a str,
    /// The source's current `metadata.json` location. This is both the pointer
    /// and the incremental-sync key: an unchanged location means unchanged
    /// metadata, so the table is not re-indexed.
    pub metadata_location: &'a str,
    /// Iceberg format version.
    pub format_version: i16,
    /// Table properties from the source metadata (write-through-indexed).
    pub properties: &'a BTreeMap<String, String>,
    /// Flattened schema text for full-text search (see
    /// [`crate::search::schema_search_text`]).
    pub schema_text: Option<&'a str>,
    /// The source's snapshot set, write-through-indexed for health/observability.
    pub snapshots: &'a [SnapshotIndexRow],
}

/// The outcome of a foreign-table upsert, so the sync engine can count what
/// changed and skip unchanged tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertOutcome {
    /// A new foreign table row was inserted.
    Inserted,
    /// An existing foreign table's metadata changed and was re-indexed.
    Updated,
    /// The table's metadata location was unchanged; nothing was rewritten.
    Unchanged,
}

/// Upserts a foreign table and its snapshot index, audited, atomically.
///
/// Incremental: if a foreign row already exists for `(namespace_id, name)` with
/// the same `metadata_location`, this is a no-op returning
/// [`UpsertOutcome::Unchanged`] (the common case on a re-sync). Otherwise the
/// row and its snapshot index are (re)written to reflect the source.
///
/// A pre-existing **native** table at the same identity is a conflict: the
/// mirror's warehouse is mirror-private, so this indicates misuse rather than a
/// legitimate race.
// The insert/update/unchanged arms each carry their own bind block, which
// pushes this just over the pedantic line budget; splitting them would hurt
// readability more than the length costs. (Added by the sprawl-half agent to
// keep the shared workspace clippy gate green; owner of foreign.rs may fold
// this into a refactor.)
#[allow(clippy::too_many_lines)]
pub async fn upsert_foreign_table(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    mirror_id: &str,
    input: &ForeignTableInput<'_>,
    principal: &str,
) -> Result<UpsertOutcome> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin foreign table upsert", e))?;

    let existing: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT id, mirror_id, metadata_location FROM tables
         WHERE namespace_id = $1 AND name = $2 FOR UPDATE",
    )
    .bind(input.namespace_id)
    .bind(input.name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load foreign table", e))?;

    let (table_id, outcome) = match existing {
        Some((_, existing_mirror, _)) if existing_mirror.as_deref() != Some(mirror_id) => {
            return Err(MeridianError::Conflict(format!(
                "table {:?} already exists and is not owned by this mirror",
                display_ident(input.namespace_levels, input.name)
            )));
        }
        Some((_, _, current_location))
            if current_location.as_deref() == Some(input.metadata_location) =>
        {
            // Unchanged since the last sync: nothing to rewrite.
            tx.commit()
                .await
                .map_err(|e| map_sqlx_error("failed to commit unchanged foreign table", e))?;
            return Ok(UpsertOutcome::Unchanged);
        }
        Some((id, _, _)) => {
            // Metadata changed: update the pointer + write-through index.
            sqlx::query(
                "UPDATE tables
                 SET metadata_location = $2, format_version = $3, properties = $4,
                     schema_text = $5, table_uuid = $6, updated_at = now()
                 WHERE id = $1",
            )
            .bind(&id)
            .bind(input.metadata_location)
            .bind(input.format_version)
            .bind(Json(input.properties))
            .bind(input.schema_text)
            .bind(input.table_uuid)
            .execute(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error("failed to update foreign table", e))?;
            // Replace the snapshot index wholesale (a foreign table's history is
            // whatever the source reports now).
            sqlx::query("DELETE FROM table_snapshots WHERE table_id = $1")
                .bind(&id)
                .execute(&mut *tx)
                .await
                .map_err(|e| map_sqlx_error("failed to clear foreign snapshot index", e))?;
            (id, UpsertOutcome::Updated)
        }
        None => {
            let id = Ulid::new().to_string();
            sqlx::query(
                "INSERT INTO tables
                     (id, workspace_id, namespace_id, name, table_uuid, metadata_location,
                      pointer_version, format_version, properties, schema_text, mirror_id)
                 VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9, $10)",
            )
            .bind(&id)
            .bind(workspace_id.to_string())
            .bind(input.namespace_id)
            .bind(input.name)
            .bind(input.table_uuid)
            .bind(input.metadata_location)
            .bind(input.format_version)
            .bind(Json(input.properties))
            .bind(input.schema_text)
            .bind(mirror_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error("failed to insert foreign table", e))?;
            (id, UpsertOutcome::Inserted)
        }
    };

    index_snapshots(&mut tx, &table_id, input.snapshots).await?;

    let payload = json!({
        "namespace": input.namespace_levels,
        "name": input.name,
        "table_uuid": input.table_uuid,
        "metadata_location": input.metadata_location,
        "mirror_id": mirror_id,
        "foreign": true,
        "outcome": match outcome { UpsertOutcome::Inserted => "inserted", _ => "updated" },
    });
    let event_type = if outcome == UpsertOutcome::Inserted {
        "table.created"
    } else {
        "table.updated"
    };
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("table:{table_id}"),
            event_type: event_type.to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "federation.table.sync".to_owned(),
            resource: format!("table:{table_id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit foreign table upsert", e))?;
    Ok(outcome)
}

/// Inserts the snapshot-index rows for a foreign table on the caller's
/// transaction (mirrors [`crate::table`]'s private helper).
async fn index_snapshots(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table_id: &str,
    snapshots: &[SnapshotIndexRow],
) -> Result<()> {
    for snapshot in snapshots {
        sqlx::query(
            "INSERT INTO table_snapshots
                 (table_id, snapshot_id, parent_snapshot_id, sequence_number, timestamp_ms,
                  manifest_list, operation, summary, is_current)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(table_id)
        .bind(snapshot.snapshot_id)
        .bind(snapshot.parent_snapshot_id)
        .bind(snapshot.sequence_number)
        .bind(snapshot.timestamp_ms)
        .bind(&snapshot.manifest_list)
        .bind(&snapshot.operation)
        .bind(&snapshot.summary)
        .bind(snapshot.is_current)
        .execute(&mut **tx)
        .await
        .map_err(|e| map_sqlx_error("failed to index foreign table snapshots", e))?;
    }
    Ok(())
}

/// A foreign table's identity as currently indexed for a mirror: its namespace
/// levels and name. Used by the sync engine to diff against the source and drop
/// tables that vanished.
#[derive(Debug, Clone)]
pub struct ForeignTableIdent {
    /// Namespace levels.
    pub namespace_levels: Vec<String>,
    /// Table name.
    pub name: String,
}

/// Lists every foreign table currently indexed for a mirror (namespace levels +
/// name), so the sync engine can compute which local tables the source no
/// longer reports.
pub async fn list_foreign_table_idents(
    pool: &PgPool,
    mirror_id: &str,
) -> Result<Vec<ForeignTableIdent>> {
    let rows: Vec<(Vec<String>, String)> = sqlx::query_as(
        "SELECT n.levels, t.name
         FROM tables t JOIN namespaces n ON n.id = t.namespace_id
         WHERE t.mirror_id = $1
         ORDER BY n.levels, t.name",
    )
    .bind(mirror_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list foreign tables", e))?;
    Ok(rows
        .into_iter()
        .map(|(namespace_levels, name)| ForeignTableIdent {
            namespace_levels,
            name,
        })
        .collect())
}

/// Removes a foreign table that vanished from the source, audited, atomically.
/// Only ever affects a row owned by `mirror_id` (a `mirror_id` guard in the
/// `DELETE` makes a mis-targeted call a no-op). The snapshot index rows go via
/// `ON DELETE CASCADE`.
pub async fn remove_foreign_table(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    mirror_id: &str,
    namespace_levels: &[String],
    name: &str,
    principal: &str,
) -> Result<bool> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin foreign table remove", e))?;

    let row: Option<(String,)> = sqlx::query_as(
        "SELECT t.id
         FROM tables t JOIN namespaces n ON n.id = t.namespace_id
         WHERE t.mirror_id = $1 AND n.levels = $2 AND t.name = $3 FOR UPDATE OF t",
    )
    .bind(mirror_id)
    .bind(namespace_levels)
    .bind(name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load foreign table for removal", e))?;

    let Some((table_id,)) = row else {
        tx.commit()
            .await
            .map_err(|e| map_sqlx_error("failed to commit no-op foreign removal", e))?;
        return Ok(false);
    };

    sqlx::query("DELETE FROM tables WHERE id = $1 AND mirror_id = $2")
        .bind(&table_id)
        .bind(mirror_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to remove foreign table", e))?;

    let payload = json!({
        "namespace": namespace_levels,
        "name": name,
        "mirror_id": mirror_id,
        "foreign": true,
        "reason": "absent from source on last sync",
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("table:{table_id}"),
            event_type: "table.dropped".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "federation.table.remove".to_owned(),
            resource: format!("table:{table_id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit foreign table remove", e))?;
    Ok(true)
}

/// Removes foreign namespaces of a mirror that have no remaining tables (after
/// table removals). Keeps the mirror's namespace set in step with the source
/// without leaving empty foreign namespaces behind. Returns how many were
/// removed. Not individually audited (housekeeping); the table removals that
/// emptied them are audited.
pub async fn prune_empty_foreign_namespaces(pool: &PgPool, mirror_id: &str) -> Result<u64> {
    let result = sqlx::query(
        "DELETE FROM namespaces n
         WHERE n.mirror_id = $1
           AND NOT EXISTS (SELECT 1 FROM tables t WHERE t.namespace_id = n.id)",
    )
    .bind(mirror_id)
    .execute(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to prune empty foreign namespaces", e))?;
    Ok(result.rows_affected())
}

/// Counts a mirror's currently-indexed foreign namespaces and tables (for the
/// sync-run summary and the mirror's denormalized counters).
pub async fn count_foreign_assets(pool: &PgPool, mirror_id: &str) -> Result<(i64, i64)> {
    let namespaces: i64 =
        sqlx::query_scalar("SELECT count(*) FROM namespaces WHERE mirror_id = $1")
            .bind(mirror_id)
            .fetch_one(pool)
            .await
            .map_err(|e| map_sqlx_error("failed to count foreign namespaces", e))?;
    let tables: i64 = sqlx::query_scalar("SELECT count(*) FROM tables WHERE mirror_id = $1")
        .bind(mirror_id)
        .fetch_one(pool)
        .await
        .map_err(|e| map_sqlx_error("failed to count foreign tables", e))?;
    Ok((namespaces, tables))
}

/// Renders a table identifier for human-readable messages.
fn display_ident(levels: &[String], name: &str) -> String {
    if levels.is_empty() {
        name.to_owned()
    } else {
        format!("{}.{name}", levels.join("."))
    }
}

// ---------------------------------------------------------------------------
// Sprawl index (mirror_assets)
// ---------------------------------------------------------------------------

/// One sprawl-index row for a mirrored asset. This is the flat per-asset index
/// the sprawl summary reads (`mirror_assets`, migration 0014) — separate from
/// the native foreign-asset rows above (which the read-side features query).
/// The sync engine produces one of these per synced table so cross-source
/// duplicate detection (same `storage_location` registered twice) and ownership
/// roll-ups see mirrored assets.
#[derive(Debug, Clone)]
pub struct MirrorAssetInput {
    /// Fully-qualified remote identity as the source reports it (`db.ns.table`).
    pub remote_ident: String,
    /// `table` | `view`.
    pub asset_type: String,
    /// The dataset's storage location (the duplicate-detection join key).
    pub storage_location: Option<String>,
    /// Remote-reported owner, when known (feeds the ownership-gap metric).
    pub owner: Option<String>,
    /// Free-form remote-reported detail.
    pub properties: serde_json::Value,
}

/// Replaces a mirror's sprawl-index rows wholesale (delete-then-insert in one
/// transaction), matching the `mirror_assets` migration's documented contract
/// ("a mirror's assets are replaced wholesale on each successful sync").
///
/// This is bookkeeping for the sprawl summary and is intentionally **not**
/// audited (the authoritative, audited record of what synced is the foreign
/// namespace/table upserts above); it emits no outbox event either. Passing an
/// empty slice clears the index (a mirror that now has no assets).
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
        let id = Ulid::new().to_string();
        sqlx::query(
            "INSERT INTO mirror_assets
                 (id, mirror_id, workspace_id, remote_ident, asset_type, storage_location,
                  owner, properties)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&id)
        .bind(mirror_id)
        .bind(workspace_id.to_string())
        .bind(&asset.remote_ident)
        .bind(&asset.asset_type)
        .bind(&asset.storage_location)
        .bind(&asset.owner)
        .bind(&asset.properties)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to insert mirror asset", e))?;
    }

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit mirror-asset refresh", e))?;
    Ok(())
}
