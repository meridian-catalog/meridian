//! Iceberg REST catalog table endpoints and the production commit driver.
//!
//! Mounted under both `/iceberg/v1/{prefix}` and `/v1/{prefix}`. This module
//! is the HTTP face of `docs/design/commit-protocol.md`: the commit
//! endpoints run the §6 state machine (recall → load → check requirements →
//! build → stage → guarded swap → bounded rebase-retry) against
//! [`PostgresCommitBackend`], with candidate `metadata.json` files staged to
//! the warehouse's object storage before the swap and orphaned files cleaned
//! per §7.1 (best-effort immediate delete; the periodic sweep is the
//! guarantee and lands with the maintenance worker).
//!
//! Conventions (documented decisions):
//!
//! - **Default table location** is
//!   `<warehouse-root>/<ns level>/<…>/<table-name>-<table-uuid>`. The UUID
//!   suffix guarantees a dropped-and-recreated table never reuses a path
//!   (stale files can never shadow live ones). Engines only ever see
//!   locations through table metadata, so the shape is free to choose; the
//!   uuid-suffixed layout is the convention hosted REST catalogs converged
//!   on.
//! - **Metadata file names** follow the Iceberg convention
//!   `metadata/<version, 5+ digits>-<random uuid>.metadata.json`, version 0
//!   for the initial file — matching what engines (Java, `PyIceberg`) write.
//! - **`stage-create=true`** initializes and returns table metadata without
//!   creating the pointer row *or writing any file* — exactly the reference
//!   implementation's behaviour. The create transaction later commits
//!   through the commit endpoint with `assert-create`, carrying the full
//!   update list, at which point the first metadata file is written and the
//!   row inserted atomically. Persisting a provisional staged file would
//!   only manufacture a guaranteed orphan.
//! - **Create-request field ids are provisional**: like the reference
//!   implementation (`AssignFreshIds`), `createTable` reassigns schema
//!   field ids server-side (1-based, nested types included) and remaps
//!   `identifier-field-ids` and partition-spec/sort-order source ids —
//!   Flink numbers provisional ids from 0, `PyIceberg` from 1. The
//!   requested partition spec becomes the table's only spec, id 0. Commit
//!   `add-schema` updates carry real ids and stay strictly validated. See
//!   [`meridian_iceberg::spec::fresh`].
//! - **`purgeRequested=true`** today: the pointer row is deleted and a
//!   `table.purge_requested` outbox event is enqueued in the same
//!   transaction, then the table's `metadata/` prefix is deleted
//!   best-effort. Data files are *not* deleted until the maintenance worker
//!   (outbox consumer) lands; the event carries everything it needs.
//! - **Table-uuid uniqueness**: one catalog, one live table per
//!   `table-uuid`, enforced by a unique index. Registering a metadata file
//!   whose UUID belongs to a still-registered table is a 409 naming the
//!   UUID conflict (the reference JDBC catalog permits such aliasing; we
//!   reject it because two pointers to one metadata lineage make ownership
//!   of maintenance, stats, and purge ambiguous). Adopt after dropping the
//!   owner, or into a different warehouse.
//! - **Idempotency** (design doc §8): commit endpoints (single- and
//!   multi-table) honor the `Idempotency-Key` header. The fingerprint is
//!   the sha-256 of the canonical request identity (endpoint + prefix +
//!   identifiers + body); same key + same fingerprint replays the recorded
//!   receipt, same key + different fingerprint is a 422 (F9). Receipts are
//!   retained for 24 h.
//! - **Authorization** (full mapping in the `crate::routes::grants` module
//!   docs): list `LIST_TABLES`, create/register `CREATE_TABLE`, load/exists
//!   `READ`, commit `COMMIT` (`CREATE_TABLE` for the assert-create
//!   finalization), drop `DROP`, metrics `WRITE`, rename `WRITE` on the
//!   source plus `CREATE_TABLE` on the destination namespace. Grants on a
//!   namespace or warehouse cover the tables they contain.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::Utc;
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_iceberg::commit::{CommitBackend, CommitBackendError, CommitReceipt, PointerCas};
use meridian_iceberg::spec::{
    MetadataBuildError, MetadataBuilder, PartitionSpec, Schema, SortOrder, TableMetadata,
    TableRequirement, TableUpdate,
};
use meridian_storage::{Storage, StorageError, new_metadata_location, read_table_metadata};
use meridian_store::commit::{
    CommitTableOp, DerivedTableState, PostgresCommitBackend, ReceiptToRecord, SnapshotIndexRow,
};
use meridian_store::rbac::{Privilege, SecurableScope};
use meridian_store::warehouse::WarehouseRecord;
use meridian_store::{audit, namespace, table, tenancy};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::{namespace_scope_chain, require};
use crate::routes::namespaces::{
    decode_namespace_param, next_page_token, resolve_pagination, resolve_warehouse,
};

/// Bounded rebase-retry budget for the compare-and-set loop (design doc §6).
/// Conflicts are only reachable through a concurrent commit between our
/// pointer load and our swap; exhaustion returns 409 and the client retries.
const MAX_COMMIT_ATTEMPTS: u32 = 3;

/// Upper bound on an accepted `Idempotency-Key` value.
const MAX_IDEMPOTENCY_KEY_LEN: usize = 255;

/// New tables built through the commit endpoint start at format version 1 so
/// the update list's `upgrade-format-version` lands wherever the client
/// asked (create transactions always carry one).
const CREATE_COMMIT_BASE_FORMAT_VERSION: u8 = 1;

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

/// A resolved `{prefix}/{namespace}` pair.
struct NamespaceContext {
    warehouse: WarehouseRecord,
    namespace: namespace::NamespaceRecord,
    levels: Vec<String>,
}

/// Resolves prefix + namespace, with exact IRC 404 types.
async fn resolve_namespace(
    state: &AppState,
    prefix: &str,
    raw_namespace: &str,
) -> Result<NamespaceContext, ApiError> {
    let warehouse = resolve_warehouse(&state.pool, prefix).await?;
    let levels = decode_namespace_param(raw_namespace)?;
    let namespace = namespace::get(&state.pool, &warehouse.id, &levels)
        .await?
        .ok_or_else(|| {
            ApiError::no_such_namespace(format!("namespace {:?} does not exist", levels.join(".")))
        })?;
    Ok(NamespaceContext {
        warehouse,
        namespace,
        levels,
    })
}

/// Validates a table name for use as a URL path segment.
fn validate_table_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(ApiError::bad_request("table name must not be empty"));
    }
    if name.contains('\u{1f}') {
        return Err(ApiError::bad_request(
            "table name must not contain the 0x1F unit separator",
        ));
    }
    Ok(())
}

/// Connects the warehouse's storage profile.
fn connect_storage(warehouse: &WarehouseRecord) -> Result<Arc<dyn Storage>, ApiError> {
    let profile =
        meridian_storage::StorageProfile::parse(&warehouse.storage_root, &warehouse.storage_config)
            .map_err(|e| storage_config_error(&warehouse.name, &e))?;
    profile
        .connect()
        .map_err(|e| storage_config_error(&warehouse.name, &e))
}

/// A warehouse whose stored configuration cannot be connected is a server
/// (operator) problem, not a client one — but the message must reach the
/// operator, so it is not masked.
fn storage_config_error(warehouse: &str, error: &StorageError) -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalServerError",
        format!("warehouse {warehouse:?} storage configuration is unusable: {error}"),
    )
}

/// Human-readable `ns.ns.table` identifier.
fn display_ident(levels: &[String], name: &str) -> String {
    if levels.is_empty() {
        name.to_owned()
    } else {
        format!("{}.{name}", levels.join("."))
    }
}

fn no_such_table(levels: &[String], name: &str) -> ApiError {
    ApiError::no_such_table(format!(
        "table {:?} does not exist",
        display_ident(levels, name)
    ))
}

/// The default location for a new table:
/// `<warehouse-root>/<ns levels>/<name>-<table uuid>` (see module docs).
fn default_table_location(
    storage_root: &str,
    levels: &[String],
    name: &str,
    table_uuid: Uuid,
) -> String {
    let root = storage_root.trim_end_matches('/');
    let path = levels.join("/");
    if path.is_empty() {
        format!("{root}/{name}-{table_uuid}")
    } else {
        format!("{root}/{path}/{name}-{table_uuid}")
    }
}

/// Renders metadata exactly as it is written to storage (including the v1
/// legacy fields), so the response body and the `metadata.json` file can
/// never disagree.
fn metadata_to_value(metadata: &TableMetadata) -> Result<Value, ApiError> {
    let text = metadata
        .to_json()
        .map_err(|e| MeridianError::internal("failed to serialize table metadata", e))?;
    serde_json::from_str(&text)
        .map_err(|e| MeridianError::internal("metadata JSON round-trip failed", e).into())
}

/// The `LoadTableResult` body. `config` carries the warehouse's non-secret
/// storage options mapped to Iceberg client property names (see
/// [`super::views::storage_client_config`] — shared with `LoadViewResult`;
/// credentials are never forwarded, that is the M2 vending milestone).
fn load_table_result(
    warehouse: &WarehouseRecord,
    metadata_location: Option<&str>,
    metadata: &TableMetadata,
) -> Result<Value, ApiError> {
    Ok(json!({
        "metadata-location": metadata_location,
        "metadata": metadata_to_value(metadata)?,
        "config": super::views::storage_client_config(warehouse),
    }))
}

// -- ETags -------------------------------------------------------------------

/// The strong `ETag` for a table representation. `pointer_version` uniquely
/// identifies the metadata version; the representation marker distinguishes
/// `snapshots=refs` from the full response (the spec requires distinct tags
/// for distinct representations of the same version).
fn table_etag(table_uuid: &str, pointer_version: i64, refs_only: bool) -> String {
    if refs_only {
        format!("\"{table_uuid}-g{pointer_version}-refs\"")
    } else {
        format!("\"{table_uuid}-g{pointer_version}\"")
    }
}

/// Whether an `If-None-Match` header matches `etag` (weak comparison: a
/// `W/` prefix on either side is ignored; `*` matches anything).
fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    let Some(raw) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let normalize = |tag: &str| {
        tag.trim()
            .trim_start_matches("W/")
            .trim_matches('"')
            .to_owned()
    };
    let target = normalize(etag);
    raw.split(',')
        .any(|candidate| candidate.trim() == "*" || normalize(candidate) == target)
}

/// Attaches an `ETag` header to a JSON response.
fn json_with_etag(status: StatusCode, body: Value, etag: &str) -> Response {
    let mut response = (status, Json(body)).into_response();
    if let Ok(value) = header::HeaderValue::from_str(etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
    response
}

// -- Idempotency --------------------------------------------------------------

/// Extracts and validates the optional `Idempotency-Key` header.
fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(value) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = value
        .to_str()
        .map_err(|_| ApiError::bad_request("Idempotency-Key must be visible ASCII"))?
        .trim();
    if key.is_empty() || key.len() > MAX_IDEMPOTENCY_KEY_LEN {
        return Err(ApiError::bad_request(format!(
            "Idempotency-Key must be 1–{MAX_IDEMPOTENCY_KEY_LEN} characters"
        )));
    }
    Ok(Some(key.to_owned()))
}

/// The request fingerprint recorded with an idempotency key: sha-256 over
/// the canonical JSON of the full request identity. Stable across retries
/// of the same logical request, different for any other request (F9).
fn request_fingerprint(endpoint: &str, prefix: &str, body: &Value) -> String {
    let identity = json!({
        "endpoint": endpoint,
        "prefix": prefix,
        "body": body,
    });
    audit::compute_hash(None, &audit::canonical_json(&identity))
}

/// 422 for idempotency-key reuse with a different request (F9: surfaced
/// loudly, never guessed, never applied).
fn key_reuse_error(key: &str) -> ApiError {
    ApiError::unprocessable(format!(
        "Idempotency-Key {key:?} was already used for a different request"
    ))
}

fn commit_backend(state: &AppState, principal: &Principal) -> PostgresCommitBackend {
    PostgresCommitBackend::new(
        state.pool.clone(),
        tenancy::default_workspace_id(),
        principal.audit_string(),
    )
}

/// Best-effort delete of a staged file that will never be published
/// (design doc §7.1 step 1). Failures are logged, never surfaced.
async fn discard_staged(storage: &Arc<dyn Storage>, location: &str) {
    if let Err(error) = storage.delete(location).await {
        tracing::warn!(%location, %error, "failed to delete orphaned staged metadata file");
    }
}

// ---------------------------------------------------------------------------
// Wire shapes
// ---------------------------------------------------------------------------

/// `TableIdentifier` from the IRC spec.
#[derive(Debug, Clone, Deserialize)]
pub struct TableIdentifier {
    /// Namespace levels.
    pub namespace: Vec<String>,
    /// Table name.
    pub name: String,
}

/// Query parameters for `GET .../tables`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesQuery {
    /// Opaque continuation token from a previous response.
    pub page_token: Option<String>,
    /// Upper bound on the number of results.
    pub page_size: Option<i64>,
}

/// `CreateTableRequest` from the IRC spec.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateTableRequest {
    /// Table name.
    pub name: String,
    /// Explicit table location; server-assigned when absent.
    pub location: Option<String>,
    /// The initial schema.
    pub schema: Schema,
    /// The initial partition spec (unpartitioned when absent).
    pub partition_spec: Option<PartitionSpec>,
    /// The initial sort order (unsorted when absent).
    pub write_order: Option<SortOrder>,
    /// When true, initialize and return metadata without creating the table.
    #[serde(default)]
    pub stage_create: bool,
    /// Initial table properties. `format-version` selects the format
    /// version (default 2) and is consumed, not stored.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// `RegisterTableRequest` from the IRC spec.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RegisterTableRequest {
    /// Table name.
    pub name: String,
    /// Existing metadata file to adopt.
    pub metadata_location: String,
    /// Overwrite an existing table of the same name (not supported yet).
    #[serde(default)]
    pub overwrite: bool,
}

/// `RenameTableRequest` from the IRC spec.
#[derive(Debug, Deserialize)]
pub struct RenameTableRequest {
    /// The existing table.
    pub source: TableIdentifier,
    /// The identifier to rename it to.
    pub destination: TableIdentifier,
}

/// Query parameters for `DELETE .../tables/{table}`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DropTableQuery {
    /// Whether to purge the table's data and metadata (see module docs for
    /// exactly what purge does today).
    #[serde(default, deserialize_with = "lenient_query_bool")]
    pub purge_requested: bool,
}

/// Query-string booleans as engines actually send them: Java writes
/// `true`/`false`, `PyIceberg` writes Python's `True`/`False`.
/// Case-insensitive by design; anything else is a 400.
fn lenient_query_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    match raw.to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" | "" => Ok(false),
        other => Err(serde::de::Error::custom(format!(
            "invalid boolean {other:?}: expected true or false"
        ))),
    }
}

/// Query parameters for `GET .../tables/{table}`.
#[derive(Debug, Deserialize)]
pub struct LoadTableQuery {
    /// `all` (default) or `refs`: which snapshots to include in the body.
    pub snapshots: Option<String>,
}

/// One table's parsed commit request (`CommitTableRequest`).
#[derive(Debug)]
struct ParsedCommit {
    identifier: Option<TableIdentifier>,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

/// Parses a `CommitTableRequest` from raw JSON. Unknown update/requirement
/// `action`/`type` strings are 400s per the spec ("server implementations
/// are required to fail with a 400 status code if any unknown updates or
/// requirements are received") — this is why the body arrives as `Value`.
fn parse_commit_request(value: &Value) -> Result<ParsedCommit, ApiError> {
    let object = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request("commit request must be a JSON object"))?;

    let identifier = match object.get("identifier") {
        None | Some(Value::Null) => None,
        Some(raw) => Some(
            serde_json::from_value::<TableIdentifier>(raw.clone())
                .map_err(|e| ApiError::bad_request(format!("invalid table identifier: {e}")))?,
        ),
    };

    let requirements_raw = object
        .get("requirements")
        .ok_or_else(|| ApiError::bad_request("commit request is missing 'requirements'"))?
        .as_array()
        .ok_or_else(|| ApiError::bad_request("'requirements' must be an array"))?;
    let requirements = requirements_raw
        .iter()
        .map(|raw| {
            serde_json::from_value::<TableRequirement>(raw.clone())
                .map_err(|e| ApiError::bad_request(format!("unknown or invalid requirement: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let updates_raw = object
        .get("updates")
        .ok_or_else(|| ApiError::bad_request("commit request is missing 'updates'"))?
        .as_array()
        .ok_or_else(|| ApiError::bad_request("'updates' must be an array"))?;
    let updates = updates_raw
        .iter()
        .map(|raw| {
            serde_json::from_value::<TableUpdate>(raw.clone())
                .map_err(|e| ApiError::bad_request(format!("unknown or invalid update: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ParsedCommit {
        identifier,
        requirements,
        updates,
    })
}

// ---------------------------------------------------------------------------
// GET /{prefix}/namespaces/{namespace}/tables — list
// ---------------------------------------------------------------------------

/// `GET /{prefix}/namespaces/{namespace}/tables` — list table identifiers.
pub async fn list_tables(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace)): Path<(String, String)>,
    Query(query): Query<ListTablesQuery>,
) -> Result<Json<Value>, ApiError> {
    let ctx = resolve_namespace(&state, &prefix, &raw_namespace).await?;
    let chain = namespace_scope_chain(&state.pool, &ctx.warehouse.id, &ctx.levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::ListTables,
        &SecurableScope::namespace(&ctx.warehouse.id, chain),
    )
    .await?;
    let pagination = resolve_pagination(query.page_token.as_deref(), query.page_size)?;

    let fetch_limit = pagination.limit.map(|l| l + 1);
    let mut rows = table::list(
        &state.pool,
        &ctx.namespace.id,
        pagination.after_id.as_deref(),
        fetch_limit,
    )
    .await?;
    let next = next_page_token(&mut rows, pagination.limit, |r| &r.id);

    let identifiers: Vec<Value> = rows
        .into_iter()
        .map(|row| json!({ "namespace": ctx.levels, "name": row.name }))
        .collect();
    Ok(Json(json!({
        "identifiers": identifiers,
        "next-page-token": next,
    })))
}

// ---------------------------------------------------------------------------
// POST /{prefix}/namespaces/{namespace}/tables — create (and stage-create)
// ---------------------------------------------------------------------------

/// Builds the initial metadata for a new table from a create request.
fn build_new_table_metadata(
    request: &CreateTableRequest,
    storage_root: &str,
    levels: &[String],
) -> Result<TableMetadata, ApiError> {
    // format-version arrives as a property (the ecosystem convention); it is
    // consumed here, never stored (it is a reserved property).
    let mut properties = request.properties.clone();
    let format_version = match properties.remove("format-version") {
        None => 2,
        Some(raw) => raw
            .parse::<u8>()
            .ok()
            .filter(|v| (1..=3).contains(v))
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "invalid format-version {raw:?}: expected 1, 2, or 3"
                ))
            })?,
    };

    let table_uuid = Uuid::new_v4();
    let location = match &request.location {
        Some(location) if !location.trim().is_empty() => location.trim_end_matches('/').to_owned(),
        _ => default_table_location(storage_root, levels, &request.name, table_uuid),
    };

    // Field ids in a create request are provisional (Flink numbers them
    // from 0, pyiceberg from 1): assign fresh server-side ids and remap the
    // requested partition-spec/sort-order source ids, as the reference
    // implementation does (`TypeUtil.assignFreshIds`). Commit-path
    // `add-schema` updates carry real ids and are validated strictly.
    let fresh = meridian_iceberg::spec::assign_fresh_ids(
        &request.schema,
        request.partition_spec.as_ref(),
        request.write_order.as_ref(),
    )
    .map_err(|e| map_build_error(&e))?;

    let mut builder =
        MetadataBuilder::new_table(format_version, location).map_err(|e| map_build_error(&e))?;
    let mut updates: Vec<TableUpdate> = vec![
        TableUpdate::AssignUuid { uuid: table_uuid },
        TableUpdate::AddSchema {
            schema: fresh.schema,
            last_column_id: None,
        },
        TableUpdate::SetCurrentSchema {
            schema_id: meridian_iceberg::spec::LAST_ADDED,
        },
    ];
    if let Some(spec) = fresh.partition_spec {
        updates.push(TableUpdate::AddSpec { spec });
        updates.push(TableUpdate::SetDefaultSpec {
            spec_id: meridian_iceberg::spec::LAST_ADDED,
        });
    }
    if let Some(sort_order) = fresh.sort_order {
        updates.push(TableUpdate::AddSortOrder { sort_order });
        updates.push(TableUpdate::SetDefaultSortOrder {
            sort_order_id: meridian_iceberg::spec::LAST_ADDED,
        });
    }
    if !properties.is_empty() {
        updates.push(TableUpdate::SetProperties {
            updates: properties,
        });
    }
    builder
        .apply_all(updates)
        .map_err(|e| map_build_error(&e))?;
    builder
        .build(Utc::now().timestamp_millis(), None)
        .map_err(|e| map_build_error(&e))
}

/// Maps builder rejections to the IRC error the operation requires:
/// conflicts with concurrent state are 409 `CommitFailedException`,
/// everything else is a 400 validation failure.
fn map_build_error(error: &MetadataBuildError) -> ApiError {
    match error {
        MetadataBuildError::SnapshotAlreadyExists { .. }
        | MetadataBuildError::NonMonotonicSequenceNumber { .. } => {
            ApiError::commit_failed(error.to_string())
        }
        other => ApiError::bad_request(other.to_string()),
    }
}

/// Extracts the write-through index state from new metadata (ADR 003).
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
        }),
        snapshots,
    }
}

/// Writes the initial metadata file and inserts the pointer row (used by
/// create, register, and the commit-endpoint create transaction). The file
/// write strictly precedes the row insert (invariant I4); a lost insert
/// race deletes the freshly staged file best-effort.
#[allow(clippy::too_many_arguments)] // one call site per origin; a struct would just rename the args
async fn materialize_new_table(
    state: &AppState,
    storage: &Arc<dyn Storage>,
    ctx: &NamespaceContext,
    name: &str,
    metadata: &TableMetadata,
    metadata_location: &str,
    origin: &str,
    principal: &str,
    receipt: Option<&ReceiptToRecord>,
) -> Result<table::TableRecord, MaterializeError> {
    meridian_storage::write_table_metadata(storage.as_ref(), metadata_location, metadata)
        .await
        .map_err(MaterializeError::Storage)?;

    let derived = derived_state(metadata);
    let record = table::create(
        &state.pool,
        table::NewTable {
            workspace_id: tenancy::default_workspace_id(),
            namespace_id: &ctx.namespace.id,
            namespace_levels: &ctx.levels,
            name,
            table_uuid: &metadata.table_uuid.to_string(),
            metadata_location,
            format_version: derived.format_version,
            properties: &derived.properties,
            origin,
        },
        principal,
        receipt,
    )
    .await;

    match record {
        Ok(record) => Ok(record),
        Err(error) => {
            // The row insert failed, so the file just written is an orphan.
            discard_staged(storage, metadata_location).await;
            Err(MaterializeError::Store(error))
        }
    }
}

/// Failure modes of [`materialize_new_table`], kept apart so callers can map
/// conflicts to the exception their endpoint requires (`AlreadyExists` for
/// create/register, `CommitFailed` for assert-create commits).
enum MaterializeError {
    Storage(StorageError),
    Store(MeridianError),
}

impl MaterializeError {
    fn into_api(self, conflict: impl Fn(String) -> ApiError) -> ApiError {
        match self {
            Self::Storage(error) => storage_to_api(&error),
            Self::Store(MeridianError::Conflict(message)) => conflict(message),
            Self::Store(MeridianError::NotFound(message)) => ApiError::no_such_namespace(message),
            Self::Store(other) => ApiError::from(other),
        }
    }
}

/// Maps storage failures on the table path onto client-facing errors.
fn storage_to_api(error: &StorageError) -> ApiError {
    match error {
        StorageError::InvalidLocation { location, root } => ApiError::bad_request(format!(
            "location {location:?} is outside the warehouse storage root {root:?}"
        )),
        StorageError::NotFound { location } => {
            ApiError::bad_request(format!("no object exists at {location:?}"))
        }
        StorageError::AlreadyExists { location } => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
            format!("staged metadata location collided at {location:?}"),
        ),
        StorageError::InvalidMetadata { .. } | StorageError::UnsupportedFormatVersion { .. } => {
            ApiError::bad_request(error.to_string())
        }
        StorageError::Transient { .. } => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailableException",
            "object storage is temporarily unavailable",
        ),
        StorageError::PermissionDenied { .. }
        | StorageError::Backend { .. }
        | StorageError::Config(_) => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
            format!("object storage operation failed: {error}"),
        ),
    }
}

/// `POST /{prefix}/namespaces/{namespace}/tables` — create a table, or
/// initialize a create transaction (`stage-create`).
pub async fn create_table(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace)): Path<(String, String)>,
    Json(request): Json<CreateTableRequest>,
) -> Result<Response, ApiError> {
    let ctx = resolve_namespace(&state, &prefix, &raw_namespace).await?;
    let chain = namespace_scope_chain(&state.pool, &ctx.warehouse.id, &ctx.levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::CreateTable,
        &SecurableScope::namespace(&ctx.warehouse.id, chain),
    )
    .await?;
    validate_table_name(&request.name)?;

    if table::get(&state.pool, &ctx.namespace.id, &request.name)
        .await?
        .is_some()
    {
        return Err(ApiError::already_exists(format!(
            "table {:?} already exists",
            display_ident(&ctx.levels, &request.name)
        )));
    }

    let metadata = build_new_table_metadata(&request, &ctx.warehouse.storage_root, &ctx.levels)?;

    if request.stage_create {
        // Nothing durable happens here (see module docs): the metadata is
        // returned for the client to build on, and the create transaction
        // commits through the commit endpoint with assert-create.
        let body = load_table_result(&ctx.warehouse, None, &metadata)?;
        return Ok((StatusCode::OK, Json(body)).into_response());
    }

    let storage = connect_storage(&ctx.warehouse)?;
    let location = new_metadata_location(&metadata.location, 0, Uuid::new_v4());
    let record = materialize_new_table(
        &state,
        &storage,
        &ctx,
        &request.name,
        &metadata,
        &location,
        "create",
        &principal.audit_string(),
        None,
    )
    .await
    .map_err(|e| e.into_api(ApiError::already_exists))?;

    let body = load_table_result(&ctx.warehouse, Some(&location), &metadata)?;
    Ok(json_with_etag(
        StatusCode::OK,
        body,
        &table_etag(&record.table_uuid, record.pointer_version, false),
    ))
}

// ---------------------------------------------------------------------------
// GET / HEAD / DELETE /{prefix}/namespaces/{namespace}/tables/{table}
// ---------------------------------------------------------------------------

/// Resolves a table for a table-scoped endpoint (missing namespace and
/// missing table are both `NoSuchTableException`, per the spec's loadTable
/// error shape).
async fn resolve_table(
    state: &AppState,
    prefix: &str,
    raw_namespace: &str,
    name: &str,
) -> Result<(WarehouseRecord, Vec<String>, table::TableRecord), ApiError> {
    let warehouse = resolve_warehouse(&state.pool, prefix).await?;
    let levels = decode_namespace_param(raw_namespace)?;
    let record = table::get_by_name(&state.pool, &warehouse.id, &levels, name)
        .await?
        .ok_or_else(|| no_such_table(&levels, name))?;
    Ok((warehouse, levels, record))
}

/// `GET /{prefix}/namespaces/{namespace}/tables/{table}` — load a table.
pub async fn load_table(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
    Query(query): Query<LoadTableQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let (warehouse, levels, record) = resolve_table(&state, &prefix, &raw_namespace, &name).await?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::Read,
        &SecurableScope::table(&warehouse.id, chain, Some(&record.id)),
    )
    .await?;
    let refs_only = match query.snapshots.as_deref() {
        None | Some("all") => false,
        Some("refs") => true,
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "invalid snapshots mode {other:?}: expected \"all\" or \"refs\""
            )));
        }
    };

    let etag = table_etag(&record.table_uuid, record.pointer_version, refs_only);
    if if_none_match_matches(&headers, &etag) {
        // A 304 carries no body, only the validator.
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        if let Ok(value) = header::HeaderValue::from_str(&etag) {
            response.headers_mut().insert(header::ETAG, value);
        }
        return Ok(response);
    }

    let Some(metadata_location) = record.metadata_location.clone() else {
        return Err(no_such_table(&levels, &name));
    };
    let storage = connect_storage(&warehouse)?;
    let mut metadata = read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .map_err(|e| current_metadata_unreadable(&metadata_location, &e))?;

    if refs_only {
        retain_referenced_snapshots(&mut metadata);
    }

    let body = load_table_result(&warehouse, Some(&metadata_location), &metadata)?;
    Ok(json_with_etag(StatusCode::OK, body, &etag))
}

/// `snapshots=refs`: keep only snapshots referenced by a branch/tag (the
/// current snapshot is always referenced by `main` when set).
fn retain_referenced_snapshots(metadata: &mut TableMetadata) {
    let mut referenced: std::collections::BTreeSet<i64> = metadata
        .refs
        .iter()
        .flatten()
        .map(|(_, reference)| reference.snapshot_id)
        .collect();
    if let Some(current) = metadata.current_snapshot_id.filter(|id| *id >= 0) {
        referenced.insert(current);
    }
    if let Some(snapshots) = &mut metadata.snapshots {
        snapshots.retain(|s| referenced.contains(&s.snapshot_id));
    }
}

/// The pointer references a file that cannot be read back — catalog-side
/// corruption, surfaced loudly (this is never a client mistake).
fn current_metadata_unreadable(location: &str, error: &StorageError) -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalServerError",
        format!("current metadata at {location:?} is unreadable: {error}"),
    )
}

/// `HEAD /{prefix}/namespaces/{namespace}/tables/{table}` — existence check.
pub async fn table_exists(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
) -> Result<StatusCode, ApiError> {
    let (warehouse, levels, record) = resolve_table(&state, &prefix, &raw_namespace, &name).await?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::Read,
        &SecurableScope::table(&warehouse.id, chain, Some(&record.id)),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /{prefix}/namespaces/{namespace}/tables/{table}` — drop a table.
pub async fn drop_table(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
    Query(query): Query<DropTableQuery>,
) -> Result<StatusCode, ApiError> {
    let warehouse = resolve_warehouse(&state.pool, &prefix).await?;
    let levels = decode_namespace_param(&raw_namespace)?;
    // The table id joins the scope when the table exists; a caller denied
    // here learns nothing about existence, and the store still 404s a
    // missing table for authorized callers.
    let table_id = table::get_by_name(&state.pool, &warehouse.id, &levels, &name)
        .await?
        .map(|r| r.id);
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::Drop,
        &SecurableScope::table(&warehouse.id, chain, table_id.as_deref()),
    )
    .await?;

    let record = table::drop_table(
        &state.pool,
        tenancy::default_workspace_id(),
        &warehouse.id,
        &levels,
        &name,
        query.purge_requested,
        &principal.audit_string(),
    )
    .await
    .map_err(|e| match e {
        MeridianError::NotFound(_) => no_such_table(&levels, &name),
        other => ApiError::from(other),
    })?;

    if query.purge_requested {
        // Best-effort immediate cleanup of the metadata prefix; the enqueued
        // purge event is the guarantee (see module docs for exactly what
        // purge does today).
        if let Some(metadata_location) = &record.metadata_location
            && let Some((base, _)) = metadata_location.rsplit_once("/metadata/")
            && let Ok(storage) = connect_storage(&warehouse)
        {
            let prefix = format!("{base}/metadata");
            if let Err(error) = storage.delete_prefix(&prefix).await {
                tracing::warn!(%prefix, %error, "best-effort metadata purge failed; sweep will collect");
            }
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// POST /{prefix}/tables/rename
// ---------------------------------------------------------------------------

/// `POST /{prefix}/tables/rename` — rename or move a table within the
/// warehouse.
pub async fn rename_table(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(prefix): Path<String>,
    Json(request): Json<RenameTableRequest>,
) -> Result<StatusCode, ApiError> {
    let warehouse = resolve_warehouse(&state.pool, &prefix).await?;
    validate_table_name(&request.destination.name)?;
    if request.source.namespace.is_empty() || request.destination.namespace.is_empty() {
        return Err(ApiError::bad_request(
            "source and destination identifiers must include a namespace",
        ));
    }

    // WRITE on the source table, CREATE_TABLE where it lands.
    let source_id = table::get_by_name(
        &state.pool,
        &warehouse.id,
        &request.source.namespace,
        &request.source.name,
    )
    .await?
    .map(|r| r.id);
    let source_chain =
        namespace_scope_chain(&state.pool, &warehouse.id, &request.source.namespace).await?;
    require(
        &state.pool,
        &principal,
        Privilege::Write,
        &SecurableScope::table(&warehouse.id, source_chain, source_id.as_deref()),
    )
    .await?;
    let dest_chain =
        namespace_scope_chain(&state.pool, &warehouse.id, &request.destination.namespace).await?;
    require(
        &state.pool,
        &principal,
        Privilege::CreateTable,
        &SecurableScope::namespace(&warehouse.id, dest_chain),
    )
    .await?;

    table::rename(
        &state.pool,
        tenancy::default_workspace_id(),
        &warehouse.id,
        &request.source.namespace,
        &request.source.name,
        &request.destination.namespace,
        &request.destination.name,
        &principal.audit_string(),
    )
    .await
    .map_err(|e| match e {
        MeridianError::NotFound(message) if message.starts_with("namespace") => {
            ApiError::no_such_namespace(message)
        }
        MeridianError::NotFound(message) => ApiError::no_such_table(message),
        MeridianError::Conflict(message) => ApiError::already_exists(message),
        other => ApiError::from(other),
    })?;

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// POST /{prefix}/namespaces/{namespace}/register
// ---------------------------------------------------------------------------

/// `POST /{prefix}/namespaces/{namespace}/register` — adopt an existing
/// metadata file as a catalog table.
pub async fn register_table(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace)): Path<(String, String)>,
    Json(request): Json<RegisterTableRequest>,
) -> Result<Response, ApiError> {
    let ctx = resolve_namespace(&state, &prefix, &raw_namespace).await?;
    let chain = namespace_scope_chain(&state.pool, &ctx.warehouse.id, &ctx.levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::CreateTable,
        &SecurableScope::namespace(&ctx.warehouse.id, chain),
    )
    .await?;
    validate_table_name(&request.name)?;
    if request.overwrite {
        // TODO(M1+): registerTable overwrite (pointer adoption over an
        // existing table). Needs its own audited pointer-swap path; rejected
        // rather than half-implemented.
        return Err(ApiError::bad_request(
            "register with overwrite=true is not supported yet",
        ));
    }

    if table::get(&state.pool, &ctx.namespace.id, &request.name)
        .await?
        .is_some()
    {
        return Err(ApiError::already_exists(format!(
            "table {:?} already exists",
            display_ident(&ctx.levels, &request.name)
        )));
    }

    let storage = connect_storage(&ctx.warehouse)?;
    // The metadata file must exist, parse, and live under the warehouse
    // root; it is adopted as-is (never rewritten).
    let metadata = read_table_metadata(storage.as_ref(), &request.metadata_location)
        .await
        .map_err(|e| storage_to_api(&e))?;

    let derived = derived_state(&metadata);
    let record = table::create(
        &state.pool,
        table::NewTable {
            workspace_id: tenancy::default_workspace_id(),
            namespace_id: &ctx.namespace.id,
            namespace_levels: &ctx.levels,
            name: &request.name,
            table_uuid: &metadata.table_uuid.to_string(),
            metadata_location: &request.metadata_location,
            format_version: derived.format_version,
            properties: &derived.properties,
            origin: "register",
        },
        &principal.audit_string(),
        None,
    )
    .await
    .map_err(|e| match e {
        MeridianError::Conflict(message) => ApiError::already_exists(message),
        MeridianError::NotFound(message) => ApiError::no_such_namespace(message),
        other => ApiError::from(other),
    })?;

    let body = load_table_result(&ctx.warehouse, Some(&request.metadata_location), &metadata)?;
    Ok(json_with_etag(
        StatusCode::OK,
        body,
        &table_etag(&record.table_uuid, record.pointer_version, false),
    ))
}

// ---------------------------------------------------------------------------
// POST /{prefix}/namespaces/{namespace}/tables/{table}/metrics
// ---------------------------------------------------------------------------

/// `POST .../tables/{table}/metrics` — accept a `ReportMetricsRequest`.
/// The raw payload is stored verbatim for the observability pillar.
pub async fn report_metrics(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
    Json(report): Json<Value>,
) -> Result<StatusCode, ApiError> {
    let (warehouse, levels, record) = resolve_table(&state, &prefix, &raw_namespace, &name).await?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::Write,
        &SecurableScope::table(&warehouse.id, chain, Some(&record.id)),
    )
    .await?;
    if !report.is_object() {
        return Err(ApiError::bad_request(
            "metrics report must be a JSON object",
        ));
    }
    let report_type = report.get("report-type").and_then(Value::as_str);

    table::record_metrics_report(
        &state.pool,
        tenancy::default_workspace_id(),
        &record.id,
        &display_ident(&levels, &name),
        report_type,
        &report,
    )
    .await?;

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// POST /{prefix}/namespaces/{namespace}/tables/{table} — THE commit endpoint
// ---------------------------------------------------------------------------

/// `POST /{prefix}/namespaces/{namespace}/tables/{table}` — commit updates
/// to one table (`CommitTableRequest` → `CommitTableResponse`).
pub async fn commit_table(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let warehouse = resolve_warehouse(&state.pool, &prefix).await?;
    let levels = decode_namespace_param(&raw_namespace)?;
    validate_table_name(&name)?;

    let parsed = parse_commit_request(&body)?;
    if let Some(identifier) = &parsed.identifier
        && (identifier.namespace != levels || identifier.name != name)
    {
        return Err(ApiError::bad_request(
            "body identifier does not match the request path",
        ));
    }

    let key = idempotency_key(&headers)?;
    let fingerprint = request_fingerprint(
        "commit-table",
        &prefix,
        &json!({ "namespace": levels, "name": name, "request": body }),
    );
    let backend = commit_backend(&state, &principal);
    let storage = connect_storage(&warehouse)?;

    // Authorize before the idempotency recall so an unauthorized caller
    // can never replay a recorded receipt: COMMIT on an existing table,
    // CREATE_TABLE on the namespace when the commit would create one.
    let record = table::get_by_name(&state.pool, &warehouse.id, &levels, &name).await?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    match &record {
        Some(existing) => {
            require(
                &state.pool,
                &principal,
                Privilege::Commit,
                &SecurableScope::table(&warehouse.id, chain, Some(&existing.id)),
            )
            .await?;
        }
        None => {
            require(
                &state.pool,
                &principal,
                Privilege::CreateTable,
                &SecurableScope::namespace(&warehouse.id, chain),
            )
            .await?;
        }
    }

    // Idempotency recall (§3 step 2): replay before touching any state.
    if let Some(key) = &key
        && let Some(response) = try_replay(&backend, &storage, key, &fingerprint).await?
    {
        return Ok(response);
    }

    let idem = key.as_deref().map(|k| (k, fingerprint.as_str()));

    match record {
        Some(record) => commit_existing_table(&backend, &storage, &record, &parsed, idem).await,
        None => {
            // A commit against a missing table is only meaningful as the
            // finalization of a create transaction (assert-create).
            if parsed
                .requirements
                .iter()
                .any(|r| matches!(r, TableRequirement::AssertCreate))
            {
                let ctx = resolve_namespace(&state, &prefix, &raw_namespace).await?;
                commit_create_table(
                    &state,
                    &storage,
                    &ctx,
                    &name,
                    &parsed,
                    key.as_deref(),
                    &fingerprint,
                    &principal.audit_string(),
                )
                .await
            } else {
                Err(no_such_table(&levels, &name))
            }
        }
    }
}

/// Replays a recorded receipt: 200 with the receipt's metadata, re-read from
/// its (immutable) metadata file. Fingerprint mismatch is F9.
async fn try_replay(
    backend: &PostgresCommitBackend,
    storage: &Arc<dyn Storage>,
    key: &str,
    fingerprint: &str,
) -> Result<Option<Response>, ApiError> {
    let Some(recalled) = backend.recall_receipt(key).await.map_err(backend_to_api)? else {
        return Ok(None);
    };
    if recalled.fingerprint != fingerprint {
        return Err(key_reuse_error(key));
    }
    Ok(Some(replay_response(storage, &recalled.receipt).await?))
}

/// Renders the stored receipt of a single-table commit as its original
/// `CommitTableResponse` (metadata files are immutable, so re-reading the
/// receipt's location reproduces the original body exactly).
async fn replay_response(
    storage: &Arc<dyn Storage>,
    receipt: &CommitReceipt<String>,
) -> Result<Response, ApiError> {
    let Some(entry) = receipt.tables.first() else {
        return Err(MeridianError::internal_msg("recorded receipt has no table entries").into());
    };
    let metadata = read_table_metadata(storage.as_ref(), &entry.metadata_location)
        .await
        .map_err(|e| current_metadata_unreadable(&entry.metadata_location, &e))?;
    let body = json!({
        "metadata-location": entry.metadata_location,
        "metadata": metadata_to_value(&metadata)?,
    });
    let version = i64::try_from(entry.version).unwrap_or(i64::MAX);
    Ok(json_with_etag(
        StatusCode::OK,
        body,
        &table_etag(&metadata.table_uuid.to_string(), version, false),
    ))
}

/// One prepared commit attempt for one table: requirements checked against
/// the loaded base, updates applied, candidate staged.
struct PreparedTable {
    op: CommitTableOp,
    metadata: TableMetadata,
}

/// Requirement violations are collected (not first-only) so multi-table
/// responses can name every failure (design doc §4).
fn check_requirements(
    requirements: &[TableRequirement],
    metadata: Option<&TableMetadata>,
    ident: &str,
    violations: &mut Vec<String>,
) {
    for requirement in requirements {
        if let Err(violation) = requirement.check(metadata) {
            violations.push(format!("{ident}: {violation}"));
        }
    }
}

/// Loads the base, checks requirements, applies updates, and stages the
/// candidate file for one table (design doc §3 steps 5–7, optimistic
/// staging variant). Returns the staged op; the caller owns cleanup of the
/// staged file on any non-committed outcome.
async fn prepare_table_commit(
    backend: &PostgresCommitBackend,
    storage: &Arc<dyn Storage>,
    record: &table::TableRecord,
    requirements: &[TableRequirement],
    updates: &[TableUpdate],
    ident: &str,
) -> Result<PreparedTable, ApiError> {
    let pointer = backend
        .load_pointer(&record.id)
        .await
        .map_err(backend_to_api)?;
    let base = read_table_metadata(storage.as_ref(), &pointer.metadata_location)
        .await
        .map_err(|e| current_metadata_unreadable(&pointer.metadata_location, &e))?;

    let mut violations = Vec::new();
    check_requirements(requirements, Some(&base), ident, &mut violations);
    if !violations.is_empty() {
        return Err(ApiError::commit_failed(violations.join("; ")));
    }

    let mut builder = base.builder_from();
    builder
        .apply_all(updates.iter().cloned())
        .map_err(|e| map_build_error(&e))?;
    let candidate = builder
        .build(
            Utc::now().timestamp_millis(),
            Some(&pointer.metadata_location),
        )
        .map_err(|e| map_build_error(&e))?;

    // Stage under the *next* pointer version with a fresh uuid: unique per
    // attempt, so no attempt can ever overwrite a published file.
    let staged_location =
        new_metadata_location(&candidate.location, pointer.version + 1, Uuid::new_v4());
    meridian_storage::write_table_metadata(storage.as_ref(), &staged_location, &candidate)
        .await
        .map_err(|e| storage_to_api(&e))?;

    Ok(PreparedTable {
        op: CommitTableOp {
            cas: PointerCas {
                table: record.id.clone(),
                expected_version: pointer.version,
                new_metadata_location: staged_location,
            },
            derived: Some(derived_state(&candidate)),
        },
        metadata: candidate,
    })
}

/// The single-table commit loop (design doc §6): bounded rebase-retry on a
/// lost compare-and-set, requirement re-check per attempt.
async fn commit_existing_table(
    backend: &PostgresCommitBackend,
    storage: &Arc<dyn Storage>,
    record: &table::TableRecord,
    parsed: &ParsedCommit,
    idempotency: Option<(&str, &str)>,
) -> Result<Response, ApiError> {
    for _attempt in 1..=MAX_COMMIT_ATTEMPTS {
        let prepared = prepare_table_commit(
            backend,
            storage,
            record,
            &parsed.requirements,
            &parsed.updates,
            &record.name,
        )
        .await?;
        let staged_location = prepared.op.cas.new_metadata_location.clone();

        match backend
            .commit_tables(std::slice::from_ref(&prepared.op), idempotency)
            .await
        {
            Ok(receipt) if receipt.replayed => {
                // A concurrent identical request won while we were staging;
                // our staged file is an orphan and the winner's receipt is
                // the response.
                discard_staged(storage, &staged_location).await;
                return replay_response(storage, &receipt).await;
            }
            Ok(_) => {
                let version =
                    i64::try_from(prepared.op.cas.expected_version + 1).unwrap_or(i64::MAX);
                let body = json!({
                    "metadata-location": staged_location,
                    "metadata": metadata_to_value(&prepared.metadata)?,
                });
                return Ok(json_with_etag(
                    StatusCode::OK,
                    body,
                    &table_etag(&record.table_uuid, version, false),
                ));
            }
            Err(CommitBackendError::VersionConflict { .. }) => {
                // Lost the race (F6): the staged file is an orphan; refresh
                // and retry with requirements re-checked against new state.
                discard_staged(storage, &staged_location).await;
            }
            Err(CommitBackendError::StateUnknown { message }) => {
                // Point-of-no-return failure (F3): the staged file must NOT
                // be deleted — the commit may have applied and published it.
                tracing::error!(%message, table = %record.id, "commit state unknown");
                return Err(ApiError::commit_state_unknown(
                    "the commit outcome could not be determined; retry with the same \
                     Idempotency-Key to resolve",
                ));
            }
            Err(other) => {
                discard_staged(storage, &staged_location).await;
                return Err(backend_to_api(other));
            }
        }
    }
    Err(ApiError::commit_failed(format!(
        "commit lost the compare-and-set race {MAX_COMMIT_ATTEMPTS} time(s); \
         refresh table state and retry"
    )))
}

/// Finalizes a create transaction: `assert-create` + the full update list
/// against an empty base. The row insert (with its audit row, outbox event,
/// and receipt) is the atomic publication point; a concurrent create makes
/// the insert conflict, which is exactly a failed `assert-create` (409).
#[allow(clippy::too_many_arguments)] // single call site; mirrors materialize_new_table
async fn commit_create_table(
    state: &AppState,
    storage: &Arc<dyn Storage>,
    ctx: &NamespaceContext,
    name: &str,
    parsed: &ParsedCommit,
    key: Option<&str>,
    fingerprint: &str,
    principal: &str,
) -> Result<Response, ApiError> {
    let mut violations = Vec::new();
    check_requirements(&parsed.requirements, None, name, &mut violations);
    if !violations.is_empty() {
        return Err(ApiError::commit_failed(violations.join("; ")));
    }

    // The default location needs the table uuid; honor an assign-uuid from
    // the update list (the create transaction always sends one), otherwise
    // mint one. A set-location update overrides the default either way.
    let table_uuid = parsed
        .updates
        .iter()
        .find_map(|update| match update {
            TableUpdate::AssignUuid { uuid } => Some(*uuid),
            _ => None,
        })
        .unwrap_or_else(Uuid::new_v4);
    let location =
        default_table_location(&ctx.warehouse.storage_root, &ctx.levels, name, table_uuid);

    let mut builder = MetadataBuilder::new_table(CREATE_COMMIT_BASE_FORMAT_VERSION, location)
        .map_err(|e| map_build_error(&e))?;
    builder
        .apply(TableUpdate::AssignUuid { uuid: table_uuid })
        .map_err(|e| map_build_error(&e))?;
    builder
        .apply_all(parsed.updates.iter().cloned())
        .map_err(|e| map_build_error(&e))?;
    let metadata = builder
        .build(Utc::now().timestamp_millis(), None)
        .map_err(|e| map_build_error(&e))?;

    let metadata_location = new_metadata_location(&metadata.location, 0, Uuid::new_v4());
    let receipt = key.map(|key| {
        ReceiptToRecord::new(
            key,
            fingerprint,
            &CommitReceipt {
                tables: vec![meridian_iceberg::commit::CommittedTable {
                    table: name.to_owned(),
                    version: 0,
                    metadata_location: metadata_location.clone(),
                }],
                replayed: false,
            },
        )
    });

    let record = materialize_new_table(
        state,
        storage,
        ctx,
        name,
        &metadata,
        &metadata_location,
        "commit-create",
        principal,
        receipt.as_ref(),
    )
    .await
    .map_err(|e| {
        // A conflicting insert means assert-create no longer holds.
        e.into_api(|message| ApiError::commit_failed(format!("assert-create failed: {message}")))
    })?;

    let body = json!({
        "metadata-location": metadata_location,
        "metadata": metadata_to_value(&metadata)?,
    });
    Ok(json_with_etag(
        StatusCode::OK,
        body,
        &table_etag(&record.table_uuid, record.pointer_version, false),
    ))
}

/// Maps commit-backend failures onto the IRC error surface.
fn backend_to_api(error: CommitBackendError) -> ApiError {
    match error {
        CommitBackendError::TableNotFound { table } => {
            ApiError::no_such_table(format!("table {table} does not exist"))
        }
        CommitBackendError::VersionConflict { .. } => ApiError::commit_failed(error.to_string()),
        CommitBackendError::IdempotencyKeyReuse { key } => key_reuse_error(&key),
        CommitBackendError::DuplicateTable { table } => ApiError::bad_request(format!(
            "table {table} appears more than once in the transaction"
        )),
        CommitBackendError::EmptyCommit => {
            ApiError::bad_request("a commit must contain at least one table change")
        }
        CommitBackendError::Unavailable { .. } => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailableException",
            "the catalog store is temporarily unavailable; nothing was applied",
        ),
        CommitBackendError::StateUnknown { .. } => ApiError::commit_state_unknown(
            "the commit outcome could not be determined; retry with the same Idempotency-Key",
        ),
    }
}

// ---------------------------------------------------------------------------
// POST /{prefix}/transactions/commit — multi-table transactions
// ---------------------------------------------------------------------------

/// `POST /{prefix}/transactions/commit` — commit changes to N tables
/// atomically (`CommitTransactionRequest`, 204 on success).
// The §4 sequence (validate → resolve → check all → stage all → one
// transaction) reads best as one function, mirroring the design doc.
#[allow(clippy::too_many_lines)]
pub async fn commit_transaction(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(prefix): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let warehouse = resolve_warehouse(&state.pool, &prefix).await?;

    let changes_raw = body
        .get("table-changes")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::bad_request("'table-changes' must be a non-empty array"))?;
    if changes_raw.is_empty() {
        return Err(ApiError::bad_request(
            "'table-changes' must be a non-empty array",
        ));
    }

    // Parse every change; identifiers are mandatory here.
    let mut changes: Vec<(TableIdentifier, ParsedCommit)> = Vec::with_capacity(changes_raw.len());
    for raw in changes_raw {
        let parsed = parse_commit_request(raw)?;
        let Some(identifier) = parsed.identifier.clone() else {
            return Err(ApiError::bad_request(
                "every table change in a transaction must carry an 'identifier'",
            ));
        };
        if parsed
            .requirements
            .iter()
            .any(|r| matches!(r, TableRequirement::AssertCreate))
        {
            // TODO(M1+): staged creates inside multi-table transactions.
            return Err(ApiError::bad_request(
                "assert-create is not supported in multi-table transactions",
            ));
        }
        changes.push((identifier, parsed));
    }

    // Duplicate tables have no defined merge order (design doc §4).
    let mut seen: std::collections::BTreeSet<(Vec<String>, String)> =
        std::collections::BTreeSet::new();
    for (identifier, _) in &changes {
        if !seen.insert((identifier.namespace.clone(), identifier.name.clone())) {
            return Err(ApiError::bad_request(format!(
                "table {:?} appears more than once in the transaction",
                display_ident(&identifier.namespace, &identifier.name)
            )));
        }
    }

    // Resolve every table up front (404 before any staging I/O).
    let mut records: Vec<table::TableRecord> = Vec::with_capacity(changes.len());
    for (identifier, _) in &changes {
        let record = table::get_by_name(
            &state.pool,
            &warehouse.id,
            &identifier.namespace,
            &identifier.name,
        )
        .await?
        .ok_or_else(|| no_such_table(&identifier.namespace, &identifier.name))?;
        records.push(record);
    }

    // COMMIT on every table, before the idempotency recall (an
    // unauthorized caller must not learn a receipt exists) and before any
    // staging I/O.
    for (identifier, record) in changes.iter().map(|(i, _)| i).zip(&records) {
        let chain =
            namespace_scope_chain(&state.pool, &warehouse.id, &identifier.namespace).await?;
        require(
            &state.pool,
            &principal,
            Privilege::Commit,
            &SecurableScope::table(&warehouse.id, chain, Some(&record.id)),
        )
        .await?;
    }

    let key = idempotency_key(&headers)?;
    let fingerprint = request_fingerprint("commit-transaction", &prefix, &body);
    let backend = commit_backend(&state, &principal);
    let storage = connect_storage(&warehouse)?;

    if let Some(key) = &key {
        match backend.recall_receipt(key).await.map_err(backend_to_api)? {
            Some(recalled) if recalled.fingerprint == fingerprint => {
                return Ok(StatusCode::NO_CONTENT.into_response());
            }
            Some(_) => return Err(key_reuse_error(key)),
            None => {}
        }
    }
    let idem = key.as_deref().map(|k| (k, fingerprint.as_str()));

    for _attempt in 1..=MAX_COMMIT_ATTEMPTS {
        // Evaluate ALL requirements before staging anything, collecting
        // every violation (design doc §4: the response names each one).
        let mut bases: Vec<(meridian_iceberg::commit::TablePointer, TableMetadata)> =
            Vec::with_capacity(records.len());
        let mut violations = Vec::new();
        for ((identifier, parsed), record) in changes.iter().zip(&records) {
            let pointer = backend
                .load_pointer(&record.id)
                .await
                .map_err(backend_to_api)?;
            let base = read_table_metadata(storage.as_ref(), &pointer.metadata_location)
                .await
                .map_err(|e| current_metadata_unreadable(&pointer.metadata_location, &e))?;
            check_requirements(
                &parsed.requirements,
                Some(&base),
                &display_ident(&identifier.namespace, &identifier.name),
                &mut violations,
            );
            bases.push((pointer, base));
        }
        if !violations.is_empty() {
            return Err(ApiError::commit_failed(violations.join("; ")));
        }

        // Build and stage every candidate (§4 step 3).
        let mut ops: Vec<CommitTableOp> = Vec::with_capacity(changes.len());
        let mut staged: Vec<String> = Vec::with_capacity(changes.len());
        let mut stage_failure: Option<ApiError> = None;
        for ((_, parsed), (record, (pointer, base))) in
            changes.iter().zip(records.iter().zip(bases))
        {
            let mut builder = base.builder_from();
            let build_result = builder
                .apply_all(parsed.updates.iter().cloned())
                .and_then(|()| {
                    builder.build(
                        Utc::now().timestamp_millis(),
                        Some(&pointer.metadata_location),
                    )
                });
            let candidate = match build_result {
                Ok(candidate) => candidate,
                Err(error) => {
                    stage_failure = Some(map_build_error(&error));
                    break;
                }
            };
            let staged_location =
                new_metadata_location(&candidate.location, pointer.version + 1, Uuid::new_v4());
            if let Err(error) = meridian_storage::write_table_metadata(
                storage.as_ref(),
                &staged_location,
                &candidate,
            )
            .await
            {
                stage_failure = Some(storage_to_api(&error));
                break;
            }
            staged.push(staged_location.clone());
            ops.push(CommitTableOp {
                cas: PointerCas {
                    table: record.id.clone(),
                    expected_version: pointer.version,
                    new_metadata_location: staged_location,
                },
                derived: Some(derived_state(&candidate)),
            });
        }
        if let Some(error) = stage_failure {
            for location in &staged {
                discard_staged(&storage, location).await;
            }
            return Err(error);
        }

        // One transaction: every pointer moves or none (§4 steps 4–5).
        match backend.commit_tables(&ops, idem).await {
            Ok(_) => return Ok(StatusCode::NO_CONTENT.into_response()),
            Err(CommitBackendError::VersionConflict { .. }) => {
                // F10: every staged file of this attempt is now an orphan.
                for location in &staged {
                    discard_staged(&storage, location).await;
                }
            }
            Err(CommitBackendError::StateUnknown { message }) => {
                tracing::error!(%message, "multi-table commit state unknown");
                return Err(ApiError::commit_state_unknown(
                    "the transaction outcome could not be determined; retry with the same \
                     Idempotency-Key to resolve",
                ));
            }
            Err(other) => {
                for location in &staged {
                    discard_staged(&storage, location).await;
                }
                return Err(backend_to_api(other));
            }
        }
    }

    Err(ApiError::commit_failed(format!(
        "transaction lost the compare-and-set race {MAX_COMMIT_ATTEMPTS} time(s); \
         refresh table state and retry"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_location_is_uuid_suffixed_under_namespace_path() {
        let uuid = Uuid::nil();
        assert_eq!(
            default_table_location(
                "s3://bucket/root/",
                &["a".to_owned(), "b".to_owned()],
                "t",
                uuid
            ),
            format!("s3://bucket/root/a/b/t-{uuid}")
        );
        assert_eq!(
            default_table_location("s3://bucket/root", &[], "t", uuid),
            format!("s3://bucket/root/t-{uuid}")
        );
    }

    #[test]
    fn etag_distinguishes_versions_and_representations() {
        let full = table_etag("u1", 3, false);
        assert_ne!(full, table_etag("u1", 4, false));
        assert_ne!(full, table_etag("u1", 3, true));
        assert_ne!(full, table_etag("u2", 3, false));
    }

    #[test]
    fn if_none_match_handles_lists_weak_tags_and_star() {
        let etag = "\"u1-g3\"";
        let with = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::IF_NONE_MATCH,
                header::HeaderValue::from_str(value).expect("header value"),
            );
            headers
        };
        assert!(if_none_match_matches(&with("\"u1-g3\""), etag));
        assert!(if_none_match_matches(&with("W/\"u1-g3\""), etag));
        assert!(if_none_match_matches(&with("\"other\", \"u1-g3\""), etag));
        assert!(if_none_match_matches(&with("*"), etag));
        assert!(!if_none_match_matches(&with("\"u1-g2\""), etag));
        assert!(!if_none_match_matches(&HeaderMap::new(), etag));
    }

    #[test]
    fn commit_request_parsing_rejects_unknown_vocabulary() {
        let bad_update = json!({
            "requirements": [],
            "updates": [{ "action": "definitely-not-an-action" }],
        });
        assert!(parse_commit_request(&bad_update).is_err());

        let bad_requirement = json!({
            "requirements": [{ "type": "assert-nonsense" }],
            "updates": [],
        });
        assert!(parse_commit_request(&bad_requirement).is_err());

        let ok = json!({
            "identifier": { "namespace": ["a"], "name": "t" },
            "requirements": [{ "type": "assert-create" }],
            "updates": [],
        });
        let parsed = parse_commit_request(&ok).expect("valid request parses");
        assert_eq!(parsed.requirements.len(), 1);
        assert!(parsed.updates.is_empty());
    }
}
