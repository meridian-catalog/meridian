//! AI-asset governance persistence (Pillar I, I-F1..I-F4): generic assets
//! (filesets, models, vector datasets), immutable training-run provenance, and
//! GDPR deletion-campaign evidence.
//!
//! This module owns the *definitions* and the *provenance records*; the vend
//! mechanics live in the `meridian_vending` crate (a fileset reuses `TableScope` on its
//! `storage_prefix`, exactly as a table does), the per-model lineage graph is
//! assembled by the server from [`crate::table`] + these records + the lineage
//! crate, and the physical snapshot expiry a deletion campaign tracks is the
//! existing maintenance `ExpireSnapshots` job ([`crate::maintenance`]) — this
//! module records which snapshots to expire and flips their status when expiry
//! is confirmed, it does not run expiry itself.
//!
//! Every mutation writes its audit row and outbox event on the same
//! transaction as the state change (invariant I6). Training runs are
//! **append-only**: there is no update or delete path for a run or its inputs —
//! [`create_training_run`] is the only writer, and the pinned snapshot ids are
//! recorded exactly as given so an Iceberg time-travel read reproduces the
//! training inputs (I-F2).

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::types::Json;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

// ===========================================================================
// Asset kinds
// ===========================================================================

/// The kind of a generic asset. Mirrors the `assets.kind` CHECK; new kinds
/// append to both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetKind {
    /// A directory-scoped asset on object storage: a storage prefix with
    /// grants and credential vending, exactly like a table (I-F1).
    Fileset,
    /// A model-registry entry: name, version, artifacts location, framework,
    /// owner, tags (I-F1).
    Model,
    /// A vector dataset (Lance today; Iceberg-native vectors when the format
    /// lands) — a generic asset kind (I-F1).
    VectorDataset,
}

impl AssetKind {
    /// The database/wire rendering.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fileset => "fileset",
            Self::Model => "model",
            Self::VectorDataset => "vector_dataset",
        }
    }

    /// Parses the wire rendering.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "fileset" => Some(Self::Fileset),
            "model" => Some(Self::Model),
            "vector_dataset" => Some(Self::VectorDataset),
            _ => None,
        }
    }
}

// ===========================================================================
// Asset row
// ===========================================================================

/// A persisted generic-asset row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AssetRecord {
    /// ULID of the asset.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Asset kind (`fileset` | `model` | `vector_dataset`).
    pub kind: String,
    /// Human name (unique per workspace + kind).
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Owning principal (audit string), free text.
    pub owner: Option<String>,
    /// Warehouse whose storage config drives the vend (filesets only).
    pub warehouse_id: Option<String>,
    /// A fileset's `s3://bucket/prefix` storage prefix; `None` for other kinds.
    pub storage_prefix: Option<String>,
    /// Kind-specific metadata owned by the caller.
    pub metadata: Json<Value>,
    /// Lightweight `key:value` labels (search + license/consent propagation).
    pub tags: Vec<String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

const ASSET_COLUMNS: &str = "id, workspace_id, kind, name, description, owner, \
     warehouse_id, storage_prefix, metadata, tags, created_at, updated_at";

/// A new generic asset to insert.
#[derive(Debug, Clone)]
pub struct NewAsset<'a> {
    /// Asset kind.
    pub kind: AssetKind,
    /// Human name.
    pub name: &'a str,
    /// Optional description.
    pub description: Option<&'a str>,
    /// Owning principal.
    pub owner: Option<&'a str>,
    /// Warehouse id (required for a fileset; ignored otherwise).
    pub warehouse_id: Option<&'a str>,
    /// Storage prefix (required for a fileset; must be `None` otherwise).
    pub storage_prefix: Option<&'a str>,
    /// Kind-specific metadata.
    pub metadata: Value,
    /// Lightweight tags.
    pub tags: Vec<String>,
}

/// Creates a generic asset with its audit row and outbox event, atomically.
///
/// # Errors
///
/// [`MeridianError::Validation`] when a fileset lacks a `storage_prefix` or
/// `warehouse_id` (or a non-fileset carries a `storage_prefix`);
/// [`MeridianError::Conflict`] when an asset of the same kind and name already
/// exists.
pub async fn create_asset(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    asset: NewAsset<'_>,
    principal: &str,
) -> Result<AssetRecord> {
    // Validate the fileset invariant in the Rust layer so the error is a clean
    // 400, not an opaque CHECK violation.
    match asset.kind {
        AssetKind::Fileset => {
            if asset.storage_prefix.is_none() || asset.warehouse_id.is_none() {
                return Err(MeridianError::Validation(
                    "a fileset requires both a warehouse and a storage_prefix".to_owned(),
                ));
            }
        }
        AssetKind::Model | AssetKind::VectorDataset => {
            if asset.storage_prefix.is_some() {
                return Err(MeridianError::Validation(format!(
                    "a {} asset must not carry a storage_prefix (its location lives in metadata)",
                    asset.kind.as_str()
                )));
            }
        }
    }

    let id = Ulid::new().to_string();
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin asset create", e))?;

    let record: AssetRecord = sqlx::query_as(&format!(
        "INSERT INTO assets
             (id, workspace_id, kind, name, description, owner, warehouse_id,
              storage_prefix, metadata, tags)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         RETURNING {ASSET_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(asset.kind.as_str())
    .bind(asset.name)
    .bind(asset.description)
    .bind(asset.owner)
    .bind(asset.warehouse_id)
    .bind(asset.storage_prefix)
    .bind(Json(&asset.metadata))
    .bind(&asset.tags)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if e.as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
        {
            MeridianError::Conflict(format!(
                "a {} named {:?} already exists",
                asset.kind.as_str(),
                asset.name
            ))
        } else if e
            .as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
        {
            MeridianError::NotFound("warehouse does not exist".to_owned())
        } else {
            map_sqlx_error("failed to insert asset", e)
        }
    })?;

    let payload = json!({
        "asset_id": record.id,
        "kind": record.kind,
        "name": record.name,
        "storage_prefix": record.storage_prefix,
        "tags": record.tags,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("asset:{id}"),
            event_type: "asset.created".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "asset.create".to_owned(),
            resource: format!("asset:{id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit asset create", e))?;
    Ok(record)
}

/// Loads an asset by id.
pub async fn get_asset(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
) -> Result<Option<AssetRecord>> {
    sqlx::query_as(&format!(
        "SELECT {ASSET_COLUMNS} FROM assets WHERE workspace_id = $1 AND id = $2"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load asset", e))
}

/// Loads an asset by (kind, name) — the human-addressable identity.
pub async fn get_asset_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    kind: AssetKind,
    name: &str,
) -> Result<Option<AssetRecord>> {
    sqlx::query_as(&format!(
        "SELECT {ASSET_COLUMNS} FROM assets
         WHERE workspace_id = $1 AND kind = $2 AND name = $3"
    ))
    .bind(workspace_id.to_string())
    .bind(kind.as_str())
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load asset by name", e))
}

/// Lists assets, optionally filtered to one kind, in stable id order (keyset).
pub async fn list_assets(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    kind: Option<AssetKind>,
    after_id: Option<&str>,
    limit: i64,
) -> Result<Vec<AssetRecord>> {
    sqlx::query_as(&format!(
        "SELECT {ASSET_COLUMNS} FROM assets
         WHERE workspace_id = $1
           AND ($2::text IS NULL OR kind = $2)
           AND ($3::text IS NULL OR id > $3)
         ORDER BY id
         LIMIT $4"
    ))
    .bind(workspace_id.to_string())
    .bind(kind.map(AssetKind::as_str))
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list assets", e))
}

/// Full-text search over generic assets (name/description/tags/owner),
/// optionally filtered to one kind. Returns matches in rank order.
///
/// Deliberately separate from the namespace-scoped table/view/namespace search
/// (migration 0010): generic assets are workspace-scoped with their own
/// grant-based visibility, so folding them into that UNION would entangle two
/// visibility models. Callers filter the returned ids by asset READ grants.
pub async fn search_assets(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    query: &str,
    kind: Option<AssetKind>,
    limit: i64,
) -> Result<Vec<AssetRecord>> {
    let tokens: Vec<String> = query
        .split_whitespace()
        .filter(|t| !t.is_empty())
        .map(|t| {
            format!(
                "{}:*",
                t.to_lowercase()
                    .replace([':', '&', '|', '!', '(', ')'], " ")
            )
        })
        .collect();
    if tokens.is_empty() {
        return Ok(Vec::new());
    }
    let tsquery = tokens.join(" & ");
    sqlx::query_as(&format!(
        "SELECT {ASSET_COLUMNS} FROM assets, to_tsquery('simple', $2) AS q
         WHERE workspace_id = $1
           AND ($3::text IS NULL OR kind = $3)
           AND search_tsv @@ q
         ORDER BY ts_rank(search_tsv, q) DESC, id
         LIMIT $4"
    ))
    .bind(workspace_id.to_string())
    .bind(&tsquery)
    .bind(kind.map(AssetKind::as_str))
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to search assets", e))
}

// ===========================================================================
// Training runs (I-F2): immutable model-version -> snapshot provenance
// ===========================================================================

/// One pinned training input: a table (or external dataset) at an exact
/// snapshot.
#[derive(Debug, Clone)]
pub struct TrainingInput {
    /// Native table id when the input is a Meridian table; `None` for external.
    pub table_id: Option<String>,
    /// Human-readable identifier (`warehouse.namespace.table` or a name).
    pub table_ref: String,
    /// The pinned Iceberg snapshot id, recorded exactly.
    pub snapshot_id: i64,
}

/// A request to record (pin) a training run.
#[derive(Debug, Clone)]
pub struct NewTrainingRun<'a> {
    /// Registered model asset id, when the model is a catalog asset.
    pub model_asset_id: Option<&'a str>,
    /// Model name (always recorded literally).
    pub model: &'a str,
    /// Model version (always recorded literally).
    pub model_version: &'a str,
    /// Free-form run metadata.
    pub metadata: Value,
    /// The pinned inputs. Must be non-empty.
    pub inputs: Vec<TrainingInput>,
}

/// A persisted training-run header.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TrainingRunRecord {
    /// ULID of the run.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Registered model asset, if any.
    pub model_asset_id: Option<String>,
    /// Model name.
    pub model: String,
    /// Model version.
    pub model_version: String,
    /// Free-form run metadata.
    pub metadata: Json<Value>,
    /// Who recorded the run.
    pub created_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

/// One persisted training-run input row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TrainingInputRecord {
    /// ULID of the input row.
    pub id: String,
    /// Owning run.
    pub training_run_id: String,
    /// Native table id, if any.
    pub table_id: Option<String>,
    /// Human-readable identifier.
    pub table_ref: String,
    /// The pinned snapshot id.
    pub snapshot_id: i64,
}

/// Records an immutable training run and its pinned inputs (I-F2).
///
/// The run and all its input rows are written in one transaction, together
/// with the audit row and outbox event. Once written, nothing mutates them —
/// this is the only writer. The pinned `snapshot_id`s are stored exactly as
/// given, so an Iceberg time-travel read reproduces the training inputs.
///
/// # Errors
///
/// [`MeridianError::Validation`] when `inputs` is empty.
pub async fn create_training_run(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    run: NewTrainingRun<'_>,
    principal: &str,
) -> Result<(TrainingRunRecord, Vec<TrainingInputRecord>)> {
    if run.inputs.is_empty() {
        return Err(MeridianError::Validation(
            "a training run must pin at least one input".to_owned(),
        ));
    }

    let id = Ulid::new().to_string();
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin training-run create", e))?;

    let header: TrainingRunRecord = sqlx::query_as(
        "INSERT INTO training_runs
             (id, workspace_id, model_asset_id, model, model_version, metadata, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING id, workspace_id, model_asset_id, model, model_version, metadata,
                   created_by, created_at",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(run.model_asset_id)
    .bind(run.model)
    .bind(run.model_version)
    .bind(Json(&run.metadata))
    .bind(principal)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if e.as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
        {
            MeridianError::NotFound("model asset does not exist".to_owned())
        } else {
            map_sqlx_error("failed to insert training run", e)
        }
    })?;

    let mut inputs = Vec::with_capacity(run.inputs.len());
    for input in &run.inputs {
        let input_id = Ulid::new().to_string();
        let row: TrainingInputRecord = sqlx::query_as(
            "INSERT INTO training_run_inputs
                 (id, training_run_id, table_id, table_ref, snapshot_id)
             VALUES ($1, $2, $3, $4, $5)
             RETURNING id, training_run_id, table_id, table_ref, snapshot_id",
        )
        .bind(&input_id)
        .bind(&id)
        .bind(input.table_id.as_deref())
        .bind(&input.table_ref)
        .bind(input.snapshot_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to insert training-run input", e))?;
        inputs.push(row);
    }

    let payload = json!({
        "training_run_id": id,
        "model": run.model,
        "model_version": run.model_version,
        "inputs": inputs.iter().map(|i| json!({
            "table_ref": i.table_ref,
            "table_id": i.table_id,
            "snapshot_id": i.snapshot_id,
        })).collect::<Vec<_>>(),
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("training-run:{id}"),
            event_type: "training_run.pinned".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "training_run.pin".to_owned(),
            resource: format!("training-run:{id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit training-run create", e))?;
    Ok((header, inputs))
}

/// Loads a training-run header by id.
pub async fn get_training_run(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
) -> Result<Option<TrainingRunRecord>> {
    sqlx::query_as(
        "SELECT id, workspace_id, model_asset_id, model, model_version, metadata,
                created_by, created_at
         FROM training_runs WHERE workspace_id = $1 AND id = $2",
    )
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load training run", e))
}

/// Loads a run's pinned inputs, in stable id order.
pub async fn training_run_inputs(
    pool: &PgPool,
    training_run_id: &str,
) -> Result<Vec<TrainingInputRecord>> {
    sqlx::query_as(
        "SELECT id, training_run_id, table_id, table_ref, snapshot_id
         FROM training_run_inputs WHERE training_run_id = $1 ORDER BY id",
    )
    .bind(training_run_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load training-run inputs", e))
}

/// Every training run for a (`model`, `model_version`), newest first. Powers the
/// per-model provenance report (I-F3).
pub async fn runs_for_model(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    model: &str,
    model_version: Option<&str>,
) -> Result<Vec<TrainingRunRecord>> {
    sqlx::query_as(
        "SELECT id, workspace_id, model_asset_id, model, model_version, metadata,
                created_by, created_at
         FROM training_runs
         WHERE workspace_id = $1 AND model = $2
           AND ($3::text IS NULL OR model_version = $3)
         ORDER BY id DESC",
    )
    .bind(workspace_id.to_string())
    .bind(model)
    .bind(model_version)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load runs for model", e))
}

// ===========================================================================
// Deletion campaigns (I-F4): GDPR "right to be forgotten" evidence
// ===========================================================================

/// A persisted deletion-campaign header.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DeletionCampaignRecord {
    /// ULID of the campaign.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Campaign name (unique per workspace).
    pub name: String,
    /// The erasure subject (data-subject id, DSAR ticket, ...).
    pub subject: String,
    /// Optional reason.
    pub reason: Option<String>,
    /// `open` | `evidence_ready` | `closed`.
    pub status: String,
    /// Who opened the campaign.
    pub created_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// One affected snapshot a campaign targets for expiry.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CampaignSnapshotRecord {
    /// ULID of the row.
    pub id: String,
    /// Owning campaign.
    pub campaign_id: String,
    /// Native table id, if known.
    pub table_id: Option<String>,
    /// Human-readable identifier.
    pub table_ref: String,
    /// The affected snapshot id.
    pub snapshot_id: i64,
    /// The Iceberg branch/ref the snapshot lives on (`None` = main).
    pub branch: Option<String>,
    /// `pending` | `expired`.
    pub expiry_status: String,
    /// When physical expiry was confirmed.
    pub expired_at: Option<DateTime<Utc>>,
}

/// One frozen model-exposure evidence row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ModelExposureRecord {
    /// ULID of the row.
    pub id: String,
    /// Owning campaign.
    pub campaign_id: String,
    /// The training run that pinned the affected snapshot.
    pub training_run_id: String,
    /// Model name.
    pub model: String,
    /// Model version.
    pub model_version: String,
    /// The affected input identifier.
    pub table_ref: String,
    /// The affected snapshot id.
    pub snapshot_id: i64,
}

/// One affected snapshot to add to a campaign.
#[derive(Debug, Clone)]
pub struct CampaignSnapshot {
    /// Native table id, if known.
    pub table_id: Option<String>,
    /// Human-readable identifier.
    pub table_ref: String,
    /// The affected snapshot id.
    pub snapshot_id: i64,
    /// The branch/ref (`None` = main).
    pub branch: Option<String>,
}

/// Opens a deletion campaign, audited + outboxed.
///
/// # Errors
///
/// [`MeridianError::Conflict`] when a campaign of that name already exists.
pub async fn create_campaign(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
    subject: &str,
    reason: Option<&str>,
    principal: &str,
) -> Result<DeletionCampaignRecord> {
    let id = Ulid::new().to_string();
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin campaign create", e))?;

    let record: DeletionCampaignRecord = sqlx::query_as(
        "INSERT INTO deletion_campaigns
             (id, workspace_id, name, subject, reason, created_by)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id, workspace_id, name, subject, reason, status, created_by,
                   created_at, updated_at",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(name)
    .bind(subject)
    .bind(reason)
    .bind(principal)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if e.as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
        {
            MeridianError::Conflict(format!("a deletion campaign named {name:?} already exists"))
        } else {
            map_sqlx_error("failed to insert deletion campaign", e)
        }
    })?;

    let payload = json!({ "campaign_id": id, "name": name, "subject": subject });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("deletion-campaign:{id}"),
            event_type: "deletion_campaign.opened".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "deletion_campaign.open".to_owned(),
            resource: format!("deletion-campaign:{id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit campaign create", e))?;
    Ok(record)
}

/// Loads a campaign by id.
pub async fn get_campaign(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
) -> Result<Option<DeletionCampaignRecord>> {
    sqlx::query_as(
        "SELECT id, workspace_id, name, subject, reason, status, created_by,
                created_at, updated_at
         FROM deletion_campaigns WHERE workspace_id = $1 AND id = $2",
    )
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load deletion campaign", e))
}

/// Lists campaigns in stable id order (keyset).
pub async fn list_campaigns(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    after_id: Option<&str>,
    limit: i64,
) -> Result<Vec<DeletionCampaignRecord>> {
    sqlx::query_as(
        "SELECT id, workspace_id, name, subject, reason, status, created_by,
                created_at, updated_at
         FROM deletion_campaigns
         WHERE workspace_id = $1 AND ($2::text IS NULL OR id > $2)
         ORDER BY id LIMIT $3",
    )
    .bind(workspace_id.to_string())
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list deletion campaigns", e))
}

/// Inserts one affected-snapshot row and freezes its model-exposure evidence
/// on the caller's transaction: every training run that pinned this exact
/// snapshot of this source (by native table id when present, else by ref for
/// external inputs) becomes a frozen exposure row. Returns how many exposure
/// rows it wrote.
async fn freeze_snapshot_exposure(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace_id: WorkspaceId,
    campaign_id: &str,
    snap: &CampaignSnapshot,
) -> Result<usize> {
    sqlx::query(
        "INSERT INTO deletion_campaign_snapshots
             (id, campaign_id, table_id, table_ref, snapshot_id, branch)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(Ulid::new().to_string())
    .bind(campaign_id)
    .bind(snap.table_id.as_deref())
    .bind(&snap.table_ref)
    .bind(snap.snapshot_id)
    .bind(snap.branch.as_deref())
    .execute(&mut **tx)
    .await
    .map_err(|e| map_sqlx_error("failed to insert campaign snapshot", e))?;

    let exposed: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT r.id, r.model, r.model_version, i.table_ref
         FROM training_run_inputs i
         JOIN training_runs r ON r.id = i.training_run_id
         WHERE r.workspace_id = $1
           AND i.snapshot_id = $2
           AND (
               ($3::text IS NOT NULL AND i.table_id = $3)
               OR ($3::text IS NULL AND i.table_id IS NULL AND i.table_ref = $4)
           )",
    )
    .bind(workspace_id.to_string())
    .bind(snap.snapshot_id)
    .bind(snap.table_id.as_deref())
    .bind(&snap.table_ref)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| map_sqlx_error("failed to resolve model exposure", e))?;

    let count = exposed.len();
    for (run_id, model, model_version, table_ref) in exposed {
        sqlx::query(
            "INSERT INTO deletion_campaign_model_exposure
                 (id, campaign_id, training_run_id, model, model_version, table_ref, snapshot_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(Ulid::new().to_string())
        .bind(campaign_id)
        .bind(&run_id)
        .bind(&model)
        .bind(&model_version)
        .bind(&table_ref)
        .bind(snap.snapshot_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| map_sqlx_error("failed to insert model exposure", e))?;
    }
    Ok(count)
}

/// Adds affected snapshots to a campaign and, in the same transaction, freezes
/// the model-exposure evidence: for each added snapshot, every training run
/// that pinned that exact `(table_id, snapshot_id)` (or `(table_ref,
/// snapshot_id)` for external inputs) is recorded as an exposure row. The
/// campaign advances to `evidence_ready`. Audited + outboxed.
///
/// The evidence is frozen (copied into `deletion_campaign_model_exposure`) so
/// it is a durable record, not a live re-query that could shift as runs are
/// added later. Returns the number of exposure rows produced.
///
/// # Errors
///
/// [`MeridianError::NotFound`] when the campaign does not exist.
pub async fn add_campaign_snapshots(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    campaign_id: &str,
    snapshots: &[CampaignSnapshot],
    principal: &str,
) -> Result<usize> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin add-campaign-snapshots", e))?;

    // Confirm the campaign exists in this workspace (and is not closed).
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status FROM deletion_campaigns WHERE workspace_id = $1 AND id = $2 FOR UPDATE",
    )
    .bind(workspace_id.to_string())
    .bind(campaign_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to lock campaign", e))?;
    let status = status.ok_or_else(|| MeridianError::NotFound("campaign not found".to_owned()))?;
    if status == "closed" {
        return Err(MeridianError::Validation(
            "campaign is closed; reopen is not supported".to_owned(),
        ));
    }

    let mut exposure_rows = 0usize;
    for snap in snapshots {
        exposure_rows += freeze_snapshot_exposure(&mut tx, workspace_id, campaign_id, snap).await?;
    }

    sqlx::query(
        "UPDATE deletion_campaigns SET status = 'evidence_ready', updated_at = now()
         WHERE id = $1 AND status = 'open'",
    )
    .bind(campaign_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to advance campaign status", e))?;

    let payload = json!({
        "campaign_id": campaign_id,
        "snapshots_added": snapshots.len(),
        "model_exposures": exposure_rows,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("deletion-campaign:{campaign_id}"),
            event_type: "deletion_campaign.evidence".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "deletion_campaign.add_snapshots".to_owned(),
            resource: format!("deletion-campaign:{campaign_id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit add-campaign-snapshots", e))?;
    Ok(exposure_rows)
}

/// The affected snapshots of a campaign.
pub async fn campaign_snapshots(
    pool: &PgPool,
    campaign_id: &str,
) -> Result<Vec<CampaignSnapshotRecord>> {
    sqlx::query_as(
        "SELECT id, campaign_id, table_id, table_ref, snapshot_id, branch,
                expiry_status, expired_at
         FROM deletion_campaign_snapshots WHERE campaign_id = $1 ORDER BY id",
    )
    .bind(campaign_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load campaign snapshots", e))
}

/// The frozen model-exposure evidence of a campaign.
pub async fn campaign_model_exposure(
    pool: &PgPool,
    campaign_id: &str,
) -> Result<Vec<ModelExposureRecord>> {
    sqlx::query_as(
        "SELECT id, campaign_id, training_run_id, model, model_version, table_ref, snapshot_id
         FROM deletion_campaign_model_exposure WHERE campaign_id = $1 ORDER BY id",
    )
    .bind(campaign_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load model exposure", e))
}

/// Marks a campaign's affected snapshot as physically expired — the tie-in the
/// maintenance `ExpireSnapshots` job ([`crate::maintenance`]) calls when it has
/// confirmed the snapshot is gone from object storage. Idempotent: expiring an
/// already-expired row is a no-op. When every snapshot of the campaign is
/// expired, the campaign advances to `closed`. Audited + outboxed.
///
/// This module records the evidence and the status; the physical deletion is
/// the maintenance job's responsibility (documented integration point, F-I4).
pub async fn mark_snapshot_expired(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    campaign_id: &str,
    snapshot_row_id: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin mark-expired", e))?;

    let updated = sqlx::query(
        "UPDATE deletion_campaign_snapshots
         SET expiry_status = 'expired', expired_at = now()
         WHERE id = $1 AND campaign_id = $2 AND expiry_status = 'pending'",
    )
    .bind(snapshot_row_id)
    .bind(campaign_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to mark snapshot expired", e))?;

    // If nothing changed the row may already be expired (idempotent no-op) or
    // may not exist; distinguish so a bad id is an honest 404.
    if updated.rows_affected() == 0 {
        let exists: Option<String> = sqlx::query_scalar(
            "SELECT id FROM deletion_campaign_snapshots WHERE id = $1 AND campaign_id = $2",
        )
        .bind(snapshot_row_id)
        .bind(campaign_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to check snapshot row", e))?;
        if exists.is_none() {
            return Err(MeridianError::NotFound(
                "campaign snapshot not found".to_owned(),
            ));
        }
        // Already expired — commit nothing new, still succeed.
        tx.commit()
            .await
            .map_err(|e| map_sqlx_error("failed to commit mark-expired", e))?;
        return Ok(());
    }

    // Close the campaign when nothing is pending anymore.
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM deletion_campaign_snapshots
         WHERE campaign_id = $1 AND expiry_status = 'pending'",
    )
    .bind(campaign_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to count pending snapshots", e))?;
    if pending == 0 {
        sqlx::query(
            "UPDATE deletion_campaigns SET status = 'closed', updated_at = now()
             WHERE id = $1 AND status <> 'closed'",
        )
        .bind(campaign_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to close campaign", e))?;
    }

    let payload = json!({
        "campaign_id": campaign_id,
        "snapshot_row_id": snapshot_row_id,
        "remaining_pending": pending,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("deletion-campaign:{campaign_id}"),
            event_type: "deletion_campaign.snapshot_expired".to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "deletion_campaign.snapshot_expired".to_owned(),
            resource: format!("deletion-campaign:{campaign_id}"),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit mark-expired", e))?;
    Ok(())
}
