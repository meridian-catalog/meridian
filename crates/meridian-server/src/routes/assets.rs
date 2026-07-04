//! AI Asset Governance management + provenance API (Pillar I, I-F1..I-F4),
//! mounted under `/api/v2`.
//!
//! - **Generic assets** (`/assets`, I-F1): CRUD for filesets, models, and
//!   vector datasets — one extensible asset model with a `kind` and
//!   kind-specific `metadata`. A **fileset** additionally vends scoped,
//!   short-lived storage credentials for its prefix
//!   (`POST /assets/{id}/credentials`), reusing the exact table-vend mechanics
//!   (`crate::routes::vending`) — the credentials are bound to the fileset
//!   prefix, nothing wider.
//! - **Training runs** (`/training-runs`, I-F2): `POST` pins a model version to
//!   the exact table snapshots that trained it — an immutable, append-only
//!   provenance record. Iceberg time-travel against the pinned snapshot ids
//!   makes the training inputs reproducible.
//! - **Provenance reporting** (`/models/{model}/provenance`, I-F3): the
//!   per-model lineage (data → run → model), the license/consent tags that
//!   propagated from the input sources, and an **EU AI Act GPAI
//!   training-content summary** generated from the pinned inputs + dataset
//!   docs/tags, plus auto-drafted dataset cards.
//! - **Deletion campaigns** (`/deletion-campaigns`, I-F4): GDPR "right to be
//!   forgotten" evidence — open a campaign, add the affected snapshots, and the
//!   server freezes which model versions saw that data. The physical snapshot
//!   expiry is the maintenance `ExpireSnapshots` job; this surface records the
//!   evidence and tracks expiry status.
//!
//! # Authorization
//!
//! Asset lifecycle (create/delete) and training-run/campaign mutations are
//! **management-gated** (`admin` role or any `MANAGE_WAREHOUSE` grant) — the
//! same gate governance uses — because these are privileged, cross-resource
//! governance operations. A **fileset credential vend** is authorized per-asset
//! by RBAC on the asset securable: a principal with `WRITE`/`COMMIT`/`DROP` on
//! the fileset gets read-write credentials, one with only `READ` gets
//! read-only (auth-disabled mode vends read-write to the anonymous principal,
//! matching the table path).
//!
//! # Honest scope
//!
//! The provenance report assembles the data → run → model chain from the
//! immutable training-run records and the input tables' downstream lineage. The
//! "model → agents using it" leg of the spec's chain is **not** modelled yet
//! (agents run governed SQL; there is no model→agent binding today) and is
//! reported as an explicit empty section, not fabricated. The AI Act summary is
//! generated deterministically from catalog facts (pinned inputs + tags +
//! docs); it is a first-class draft for a compliance officer, not a legal
//! opinion.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_store::assets::{
    self, AssetKind, AssetRecord, CampaignSnapshot, NewAsset, NewTrainingRun, TrainingInput,
};
use meridian_store::rbac::{self, Privilege, SecurableScope};
use meridian_store::{tags, tenancy, warehouse};
use meridian_vending::AccessMode;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::{require, require_management};
use crate::routes::vending::{storage_credential_json, vend_for_fileset};

// ===========================================================================
// Shared helpers
// ===========================================================================

/// A 404 in the management envelope.
fn not_found(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "NotFoundException", message)
}

/// Parses an `?type=` / `kind` string into an [`AssetKind`], or a 400.
fn parse_kind(raw: &str) -> Result<AssetKind, ApiError> {
    AssetKind::parse(raw).ok_or_else(|| {
        ApiError::bad_request(format!(
            "unknown asset kind {raw:?}: expected fileset, model, or vector_dataset"
        ))
    })
}

/// Renders an asset row as its JSON response body.
fn asset_json(a: &AssetRecord) -> Value {
    json!({
        "id": a.id,
        "kind": a.kind,
        "name": a.name,
        "description": a.description,
        "owner": a.owner,
        "warehouse_id": a.warehouse_id,
        "storage_prefix": a.storage_prefix,
        "metadata": a.metadata.0,
        "tags": a.tags,
        "created_at": a.created_at.to_rfc3339(),
        "updated_at": a.updated_at.to_rfc3339(),
    })
}

// ===========================================================================
// Generic assets (I-F1)
// ===========================================================================

/// `POST /api/v2/assets` — create a generic asset (management-gated).
#[derive(Debug, Deserialize)]
pub struct CreateAssetBody {
    kind: String,
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    owner: Option<String>,
    /// Warehouse *name* the fileset vends from (filesets only).
    #[serde(default)]
    warehouse: Option<String>,
    /// The fileset's `s3://bucket/prefix` (filesets only).
    #[serde(default)]
    storage_prefix: Option<String>,
    #[serde(default)]
    metadata: Value,
    #[serde(default)]
    tags: Vec<String>,
}

pub async fn create_asset(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(body): Json<CreateAssetBody>,
) -> Result<(axum::http::StatusCode, Json<Value>), ApiError> {
    require_management(&state.pool, &principal).await?;
    let workspace_id = tenancy::default_workspace_id();
    let kind = parse_kind(&body.kind)?;

    // Resolve the fileset's warehouse name to an id (the vend needs the id;
    // the store validates the fileset invariant too).
    let warehouse_id = match (kind, body.warehouse.as_deref()) {
        (AssetKind::Fileset, Some(name)) => {
            let wh = warehouse::get_by_name(&state.pool, workspace_id, name)
                .await?
                .ok_or_else(|| ApiError::no_such_warehouse(name))?;
            Some(wh.id)
        }
        (AssetKind::Fileset, None) => {
            return Err(ApiError::bad_request(
                "a fileset requires a warehouse (by name)",
            ));
        }
        _ => None,
    };

    let record = assets::create_asset(
        &state.pool,
        workspace_id,
        NewAsset {
            kind,
            name: &body.name,
            description: body.description.as_deref(),
            owner: body.owner.as_deref(),
            warehouse_id: warehouse_id.as_deref(),
            storage_prefix: body.storage_prefix.as_deref(),
            metadata: if body.metadata.is_null() {
                json!({})
            } else {
                body.metadata
            },
            tags: body.tags,
        },
        &principal.audit_string(),
    )
    .await
    .map_err(map_store_error)?;

    Ok((axum::http::StatusCode::CREATED, Json(asset_json(&record))))
}

/// `GET /api/v2/assets?type=&limit=&after=` — list assets.
#[derive(Debug, Deserialize)]
pub struct ListAssetsQuery {
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

pub async fn list_assets(
    State(state): State<AppState>,
    Extension(_principal): Extension<Principal>,
    Query(q): Query<ListAssetsQuery>,
) -> Result<Json<Value>, ApiError> {
    let workspace_id = tenancy::default_workspace_id();
    let kind = q.kind.as_deref().map(parse_kind).transpose()?;
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let rows = assets::list_assets(&state.pool, workspace_id, kind, q.after.as_deref(), limit)
        .await
        .map_err(map_store_error)?;
    Ok(Json(json!({
        "assets": rows.iter().map(asset_json).collect::<Vec<_>>(),
    })))
}

/// `GET /api/v2/assets/search?q=&type=` — full-text asset search.
#[derive(Debug, Deserialize)]
pub struct SearchAssetsQuery {
    q: String,
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

pub async fn search_assets(
    State(state): State<AppState>,
    Extension(_principal): Extension<Principal>,
    Query(q): Query<SearchAssetsQuery>,
) -> Result<Json<Value>, ApiError> {
    let workspace_id = tenancy::default_workspace_id();
    let kind = q.kind.as_deref().map(parse_kind).transpose()?;
    let limit = q.limit.unwrap_or(20).clamp(1, 100);
    let rows = assets::search_assets(&state.pool, workspace_id, &q.q, kind, limit)
        .await
        .map_err(map_store_error)?;
    Ok(Json(json!({
        "hits": rows.iter().map(asset_json).collect::<Vec<_>>(),
    })))
}

/// `GET /api/v2/assets/{id}` — load one asset.
pub async fn get_asset(
    State(state): State<AppState>,
    Extension(_principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let workspace_id = tenancy::default_workspace_id();
    let record = assets::get_asset(&state.pool, workspace_id, &id)
        .await
        .map_err(map_store_error)?
        .ok_or_else(|| not_found(format!("no asset {id}")))?;
    Ok(Json(asset_json(&record)))
}

/// `POST /api/v2/assets/{id}/credentials` — vend scoped credentials for a
/// **fileset**, bound to its storage prefix (I-F1). Access follows RBAC on the
/// asset securable, exactly like a table vend.
pub async fn vend_fileset_credentials(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let workspace_id = tenancy::default_workspace_id();
    let asset = assets::get_asset(&state.pool, workspace_id, &id)
        .await
        .map_err(map_store_error)?
        .ok_or_else(|| not_found(format!("no asset {id}")))?;

    let (Some(prefix), Some(warehouse_id)) = (
        asset.storage_prefix.as_deref(),
        asset.warehouse_id.as_deref(),
    ) else {
        return Err(ApiError::bad_request(
            "only filesets can vend credentials (this asset has no storage prefix)",
        ));
    };

    // RBAC on the asset securable decides read vs read-write, mirroring the
    // table vend: WRITE/COMMIT/DROP -> read-write, else READ -> read-only.
    let scope = SecurableScope::asset(Some(&id));
    let access = if rbac::authorize(&state.pool, &principal, Privilege::Write, &scope)
        .await
        .is_ok()
    {
        AccessMode::ReadWrite
    } else {
        require(&state.pool, &principal, Privilege::Read, &scope).await?;
        AccessMode::Read
    };

    let wh = warehouse::get_by_id(&state.pool, workspace_id, warehouse_id)
        .await?
        .ok_or_else(|| {
            MeridianError::internal(
                "fileset warehouse vanished",
                std::io::Error::other("warehouse not found"),
            )
        })?;

    let vended = vend_for_fileset(
        &state,
        &principal,
        &wh,
        &asset.id,
        &asset.name,
        prefix,
        access,
    )
    .await?
    .ok_or_else(|| {
        ApiError::bad_request(
            "credential vending is disabled on this fileset's warehouse (storage option \
             `vending` is `none`)",
        )
    })?;

    Ok(Json(json!({
        "storage-credentials": [storage_credential_json(&vended)],
        "access": access.as_str(),
    })))
}

// ===========================================================================
// Training runs (I-F2)
// ===========================================================================

/// `POST /api/v2/training-runs` — pin a model version to exact snapshots.
#[derive(Debug, Deserialize)]
pub struct CreateTrainingRunBody {
    model: String,
    model_version: String,
    /// Optional registered model asset id to link the run to.
    #[serde(default)]
    model_asset_id: Option<String>,
    #[serde(default)]
    metadata: Value,
    inputs: Vec<TrainingInputBody>,
}

#[derive(Debug, Deserialize)]
pub struct TrainingInputBody {
    /// Native table id, when the input is a Meridian table.
    #[serde(default)]
    table_id: Option<String>,
    /// Human-readable identifier (recorded verbatim; required).
    table_ref: String,
    /// The exact Iceberg snapshot id to pin (a signed 64-bit integer).
    snapshot_id: i64,
}

pub async fn create_training_run(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(body): Json<CreateTrainingRunBody>,
) -> Result<(axum::http::StatusCode, Json<Value>), ApiError> {
    require_management(&state.pool, &principal).await?;
    let workspace_id = tenancy::default_workspace_id();

    if body.inputs.is_empty() {
        return Err(ApiError::bad_request(
            "a training run must pin at least one input",
        ));
    }

    let inputs: Vec<TrainingInput> = body
        .inputs
        .into_iter()
        .map(|i| TrainingInput {
            table_id: i.table_id,
            table_ref: i.table_ref,
            snapshot_id: i.snapshot_id,
        })
        .collect();

    let (header, input_rows) = assets::create_training_run(
        &state.pool,
        workspace_id,
        NewTrainingRun {
            model_asset_id: body.model_asset_id.as_deref(),
            model: &body.model,
            model_version: &body.model_version,
            metadata: if body.metadata.is_null() {
                json!({})
            } else {
                body.metadata
            },
            inputs,
        },
        &principal.audit_string(),
    )
    .await
    .map_err(map_store_error)?;

    Ok((
        axum::http::StatusCode::CREATED,
        Json(training_run_json(&header, &input_rows)),
    ))
}

fn training_run_json(
    header: &assets::TrainingRunRecord,
    inputs: &[assets::TrainingInputRecord],
) -> Value {
    json!({
        "id": header.id,
        "model": header.model,
        "model_version": header.model_version,
        "model_asset_id": header.model_asset_id,
        "metadata": header.metadata.0,
        "created_by": header.created_by,
        "created_at": header.created_at.to_rfc3339(),
        "inputs": inputs.iter().map(|i| json!({
            "table_id": i.table_id,
            "table_ref": i.table_ref,
            "snapshot_id": i.snapshot_id,
        })).collect::<Vec<_>>(),
    })
}

/// `GET /api/v2/training-runs/{id}` — load an immutable training run.
pub async fn get_training_run(
    State(state): State<AppState>,
    Extension(_principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let workspace_id = tenancy::default_workspace_id();
    let header = assets::get_training_run(&state.pool, workspace_id, &id)
        .await
        .map_err(map_store_error)?
        .ok_or_else(|| not_found(format!("no training run {id}")))?;
    let inputs = assets::training_run_inputs(&state.pool, &id)
        .await
        .map_err(map_store_error)?;
    Ok(Json(training_run_json(&header, &inputs)))
}

// ===========================================================================
// Provenance reporting + EU AI Act summary (I-F3)
// ===========================================================================

#[derive(Debug, Deserialize)]
pub struct ModelProvenanceQuery {
    /// Optionally restrict to one model version.
    #[serde(default)]
    version: Option<String>,
}

/// `GET /api/v2/models/{model}/provenance?version=` — the per-model lineage
/// (data → run → model), the propagated license/consent tags, and the
/// auto-drafted dataset cards. Management-gated (a provenance report exposes
/// the full input inventory).
pub async fn model_provenance(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(model): Path<String>,
    Query(q): Query<ModelProvenanceQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let report = build_provenance(&state, &model, q.version.as_deref()).await?;
    Ok(Json(report.report_json))
}

/// `GET /api/v2/models/{model}/ai-act-summary?version=` — the EU AI Act GPAI
/// training-content summary, generated from the pinned inputs + dataset
/// docs/tags. Management-gated.
pub async fn model_ai_act_summary(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(model): Path<String>,
    Query(q): Query<ModelProvenanceQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let report = build_provenance(&state, &model, q.version.as_deref()).await?;
    Ok(Json(report.ai_act_json))
}

/// The assembled provenance material, shared by the two report endpoints.
struct Provenance {
    report_json: Value,
    ai_act_json: Value,
}

/// Assembles a model's provenance from its immutable training runs: the
/// data→run→model chain, the license/consent tags that propagate from each
/// input source, auto-drafted dataset cards, and the AI Act GPAI summary.
async fn build_provenance(
    state: &AppState,
    model: &str,
    version: Option<&str>,
) -> Result<Provenance, ApiError> {
    let workspace_id = tenancy::default_workspace_id();
    let runs = assets::runs_for_model(&state.pool, workspace_id, model, version)
        .await
        .map_err(map_store_error)?;
    if runs.is_empty() {
        return Err(not_found(format!(
            "no training runs recorded for model {model:?}{}",
            version.map_or_else(String::new, |v| format!(" version {v:?}"))
        )));
    }

    // Collect the runs (data → run → model chain) and, per distinct input
    // source, the propagated governance tags (license:*, consent:*, pii:*,
    // ...) resolved from the input table's own tag assignments (D-F3 model).
    let mut runs_json = Vec::new();
    let mut dataset_cards = Vec::new();
    // Distinct (table_ref) -> (table_id, snapshot_ids, tags).
    let mut sources: std::collections::BTreeMap<String, SourceFacts> =
        std::collections::BTreeMap::new();

    for run in &runs {
        let inputs = assets::training_run_inputs(&state.pool, &run.id)
            .await
            .map_err(map_store_error)?;
        runs_json.push(json!({
            "training_run_id": run.id,
            "model_version": run.model_version,
            "created_by": run.created_by,
            "created_at": run.created_at.to_rfc3339(),
            "inputs": inputs.iter().map(|i| json!({
                "table_ref": i.table_ref,
                "table_id": i.table_id,
                "snapshot_id": i.snapshot_id,
            })).collect::<Vec<_>>(),
        }));

        for input in &inputs {
            let facts = sources
                .entry(input.table_ref.clone())
                .or_insert_with(|| SourceFacts {
                    table_id: input.table_id.clone(),
                    snapshots: Vec::new(),
                    tags: Vec::new(),
                });
            if !facts.snapshots.contains(&input.snapshot_id) {
                facts.snapshots.push(input.snapshot_id);
            }
        }
    }

    // Resolve the propagated tags per source (only for native tables — an
    // external dataset's tags are whatever the caller documented in metadata).
    let mut all_propagated: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for facts in sources.values_mut() {
        if let Some(table_id) = &facts.table_id {
            // Resolve the input table's tags; namespace-inherited tags need the
            // chain, but the table's own + column tags are the propagating
            // classification. Pass an empty ancestor set — the direct table
            // classification is what "this model saw" honestly reflects.
            let resolved = tags::resolve_table_tags(&state.pool, workspace_id, table_id, &[])
                .await
                .map_err(map_store_error)?;
            for r in resolved {
                facts.tags.push(r.tag.clone());
                all_propagated.insert(r.tag);
            }
            facts.tags.sort();
            facts.tags.dedup();
        }
    }

    // Auto-draft a dataset card per source (I-F3): its identity, the pinned
    // snapshots this model saw, and its governance tags.
    for (table_ref, facts) in &sources {
        dataset_cards.push(json!({
            "source": table_ref,
            "table_id": facts.table_id,
            "pinned_snapshots": facts.snapshots,
            "tags": facts.tags,
        }));
    }

    let report_json = json!({
        "model": model,
        "version": version,
        "runs": runs_json,
        "propagated_tags": all_propagated.iter().collect::<Vec<_>>(),
        "dataset_cards": dataset_cards,
        // Honest boundary: no model→agent binding exists yet.
        "agents_using": Value::Array(vec![]),
    });

    // The EU AI Act GPAI training-content summary: a deterministic draft from
    // the catalog facts. Sections mirror the GPAI template — data sources, the
    // reproducibility pins, and the rights/consent posture derived from tags.
    let source_summaries: Vec<Value> = dataset_cards.clone();
    let license_tags: Vec<&String> = all_propagated
        .iter()
        .filter(|t| t.starts_with("license:") || t.starts_with("consent:"))
        .collect();
    let ai_act_json = json!({
        "model": model,
        "version": version,
        "generated_from": "pinned training-run inputs + source governance tags",
        "training_data_sources": source_summaries,
        "reproducibility": "each source is pinned to an exact Iceberg snapshot id; \
                            time-travel against that id reproduces the training inputs",
        "rights_and_consent": {
            "propagated_tags": license_tags.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            "note": "license/consent posture is derived from the governance tags on the \
                     input sources; absence of a tag is not evidence of clearance",
        },
        "disclaimer": "auto-generated draft from catalog metadata for a compliance officer \
                       to review; not a legal opinion",
    });

    Ok(Provenance {
        report_json,
        ai_act_json,
    })
}

struct SourceFacts {
    table_id: Option<String>,
    snapshots: Vec<i64>,
    tags: Vec<String>,
}

// ===========================================================================
// Deletion campaigns (I-F4)
// ===========================================================================

#[derive(Debug, Deserialize)]
pub struct CreateCampaignBody {
    name: String,
    subject: String,
    #[serde(default)]
    reason: Option<String>,
}

/// `POST /api/v2/deletion-campaigns` — open a GDPR erasure campaign.
pub async fn create_campaign(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(body): Json<CreateCampaignBody>,
) -> Result<(axum::http::StatusCode, Json<Value>), ApiError> {
    require_management(&state.pool, &principal).await?;
    let workspace_id = tenancy::default_workspace_id();
    let record = assets::create_campaign(
        &state.pool,
        workspace_id,
        &body.name,
        &body.subject,
        body.reason.as_deref(),
        &principal.audit_string(),
    )
    .await
    .map_err(map_store_error)?;
    Ok((
        axum::http::StatusCode::CREATED,
        Json(campaign_json(&record)),
    ))
}

fn campaign_json(c: &assets::DeletionCampaignRecord) -> Value {
    json!({
        "id": c.id,
        "name": c.name,
        "subject": c.subject,
        "reason": c.reason,
        "status": c.status,
        "created_by": c.created_by,
        "created_at": c.created_at.to_rfc3339(),
        "updated_at": c.updated_at.to_rfc3339(),
    })
}

/// `GET /api/v2/deletion-campaigns` — list campaigns.
pub async fn list_campaigns(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(q): Query<ListAssetsQuery>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let workspace_id = tenancy::default_workspace_id();
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let rows = assets::list_campaigns(&state.pool, workspace_id, q.after.as_deref(), limit)
        .await
        .map_err(map_store_error)?;
    Ok(Json(json!({
        "campaigns": rows.iter().map(campaign_json).collect::<Vec<_>>(),
    })))
}

#[derive(Debug, Deserialize)]
pub struct AddSnapshotsBody {
    snapshots: Vec<CampaignSnapshotBody>,
}

#[derive(Debug, Deserialize)]
pub struct CampaignSnapshotBody {
    #[serde(default)]
    table_id: Option<String>,
    table_ref: String,
    snapshot_id: i64,
    #[serde(default)]
    branch: Option<String>,
}

/// `POST /api/v2/deletion-campaigns/{id}/snapshots` — add affected snapshots
/// and freeze the model-exposure evidence.
pub async fn add_campaign_snapshots(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Json(body): Json<AddSnapshotsBody>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let workspace_id = tenancy::default_workspace_id();
    if body.snapshots.is_empty() {
        return Err(ApiError::bad_request("add at least one affected snapshot"));
    }
    let snapshots: Vec<CampaignSnapshot> = body
        .snapshots
        .into_iter()
        .map(|s| CampaignSnapshot {
            table_id: s.table_id,
            table_ref: s.table_ref,
            snapshot_id: s.snapshot_id,
            branch: s.branch,
        })
        .collect();
    let exposures = assets::add_campaign_snapshots(
        &state.pool,
        workspace_id,
        &id,
        &snapshots,
        &principal.audit_string(),
    )
    .await
    .map_err(map_store_error)?;
    Ok(Json(json!({
        "campaign_id": id,
        "model_exposures_recorded": exposures,
    })))
}

/// `GET /api/v2/deletion-campaigns/{id}/evidence` — the full GDPR evidence
/// record: affected snapshots + which model versions saw them + expiry status.
pub async fn campaign_evidence(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let workspace_id = tenancy::default_workspace_id();
    let campaign = assets::get_campaign(&state.pool, workspace_id, &id)
        .await
        .map_err(map_store_error)?
        .ok_or_else(|| not_found(format!("no deletion campaign {id}")))?;
    let snapshots = assets::campaign_snapshots(&state.pool, &id)
        .await
        .map_err(map_store_error)?;
    let exposure = assets::campaign_model_exposure(&state.pool, &id)
        .await
        .map_err(map_store_error)?;

    Ok(Json(json!({
        "campaign": campaign_json(&campaign),
        "affected_snapshots": snapshots.iter().map(|s| json!({
            "id": s.id,
            "table_ref": s.table_ref,
            "table_id": s.table_id,
            "snapshot_id": s.snapshot_id,
            "branch": s.branch,
            "expiry_status": s.expiry_status,
            "expired_at": s.expired_at.map(|t| t.to_rfc3339()),
        })).collect::<Vec<_>>(),
        "model_exposure": exposure.iter().map(|e| json!({
            "training_run_id": e.training_run_id,
            "model": e.model,
            "model_version": e.model_version,
            "table_ref": e.table_ref,
            "snapshot_id": e.snapshot_id,
        })).collect::<Vec<_>>(),
        "physical_expiry_note": "physical snapshot expiry is performed by the maintenance \
                                 ExpireSnapshots job; this record tracks expiry status per \
                                 affected snapshot",
    })))
}

#[derive(Debug, Deserialize)]
pub struct ExpireSnapshotBody {
    /// The affected-snapshot row id (from the evidence report) to mark expired.
    snapshot_row_id: String,
}

/// `POST /api/v2/deletion-campaigns/{id}/expire` — record that an affected
/// snapshot is physically expired. The **integration point** the maintenance
/// `ExpireSnapshots` job calls once it has deleted the snapshot; also usable
/// directly by an operator confirming manual expiry. Idempotent.
pub async fn mark_snapshot_expired(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Json(body): Json<ExpireSnapshotBody>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let workspace_id = tenancy::default_workspace_id();
    assets::mark_snapshot_expired(
        &state.pool,
        workspace_id,
        &id,
        &body.snapshot_row_id,
        &principal.audit_string(),
    )
    .await
    .map_err(map_store_error)?;
    // Return the fresh campaign status.
    let campaign = assets::get_campaign(&state.pool, workspace_id, &id)
        .await
        .map_err(map_store_error)?
        .ok_or_else(|| not_found(format!("no deletion campaign {id}")))?;
    Ok(Json(campaign_json(&campaign)))
}

// ===========================================================================
// Error mapping
// ===========================================================================

/// Maps store errors onto the management API's HTTP envelope.
fn map_store_error(error: MeridianError) -> ApiError {
    match error {
        MeridianError::NotFound(m) => not_found(m),
        MeridianError::Conflict(m) => ApiError::already_exists(m),
        MeridianError::Validation(m) => ApiError::bad_request(m),
        other => ApiError::from(other),
    }
}
