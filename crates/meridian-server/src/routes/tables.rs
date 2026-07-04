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
use meridian_store::contracts;
use meridian_store::rbac::{Privilege, SecurableScope};
use meridian_store::warehouse::WarehouseRecord;
use meridian_store::{audit, namespace, table, tenancy, view};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::{namespace_scope_chain, require};
use crate::routes::namespaces::{
    decode_namespace_param, next_page_token, resolve_pagination, resolve_warehouse,
};
use crate::routes::signing::remote_signing_config;
use crate::routes::vending::{
    RequestedDelegation, VendContext, requested_delegation, storage_credential_json,
    vend_access_mode, vend_for_table,
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
pub(crate) fn connect_storage(warehouse: &WarehouseRecord) -> Result<Arc<dyn Storage>, ApiError> {
    let profile =
        meridian_storage::StorageProfile::parse(&warehouse.storage_root, &warehouse.storage_config)
            .map_err(|e| storage_config_error(&warehouse.name, &e))?;
    profile
        .connect()
        .map_err(|e| storage_config_error(&warehouse.name, &e))
}

/// Connects storage for reading a **foreign** table's metadata (Pillar B).
///
/// A foreign asset's `metadata_location` points at the *source* catalog's
/// storage (e.g. `s3://…` / `file://…`), not the mirror's synthetic
/// `mirror://` warehouse root, so its metadata cannot be read through the
/// warehouse profile. This derives a read profile rooted at the metadata file's
/// table location (the path up to `/metadata/`), so `resolve` accepts the full
/// location. The mirror warehouse's non-secret storage config (which may carry
/// e.g. an S3 endpoint) is passed through; source credentials for non-public
/// object stores are a separate concern (metadata federation reads the
/// pointer/schema, not data files).
fn connect_foreign_storage(
    warehouse: &WarehouseRecord,
    metadata_location: &str,
) -> Result<Arc<dyn Storage>, ApiError> {
    // The table location is everything before the conventional `/metadata/`
    // segment; fall back to the whole location's parent if the convention does
    // not hold (still a valid prefix for `resolve`).
    let root = metadata_location
        .rsplit_once("/metadata/")
        .map(|(base, _)| base)
        .or_else(|| metadata_location.rsplit_once('/').map(|(base, _)| base))
        .unwrap_or(metadata_location);
    // The mirror warehouse's storage config holds only the foreign markers
    // (`meridian:foreign`, `meridian:mirror_id`), not storage options, so read
    // with an empty option set. Filesystem sources need none; non-public object
    // stores would need source credentials — out of scope for metadata
    // federation (which reads the pointer/schema, not data files).
    let options = std::collections::BTreeMap::new();
    let profile = meridian_storage::StorageProfile::parse(root, &options)
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

pub(crate) fn no_such_table(levels: &[String], name: &str) -> ApiError {
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
/// [`super::views::storage_client_config`] — shared with `LoadViewResult`).
/// Credentials only ever join the response through [`attach_vended`]: an
/// explicit client request (`X-Iceberg-Access-Delegation`) against a
/// warehouse that opted into vending (see [`super::vending`]).
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

/// Merges vended credentials into a `LoadTableResult`: the client
/// properties join `config` (what engines read today) and the spec's
/// `storage-credentials` field mirrors them (what newer clients prefer).
fn attach_vended(body: &mut Value, vended: &meridian_vending::VendedCredentials) {
    if let Some(config) = body.get_mut("config").and_then(Value::as_object_mut) {
        for (key, value) in &vended.config {
            config.insert(key.clone(), Value::String(value.clone()));
        }
    }
    body["storage-credentials"] = json!([storage_credential_json(vended)]);
}

/// Merges the remote-signing client properties (see
/// [`remote_signing_config`]) into a `LoadTableResult`'s `config`. No
/// credential material is involved — only the switch and the endpoint.
fn attach_signing(body: &mut Value, signing: &std::collections::BTreeMap<String, String>) {
    if let Some(config) = body.get_mut("config").and_then(Value::as_object_mut) {
        for (key, value) in signing {
            config.insert(key.clone(), Value::String(value.clone()));
        }
    }
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

/// Builds a commit backend from an audit string (the branch-merge path in
/// `crate::routes::branches` already holds the principal's audit string, not
/// the `Principal`).
pub(crate) fn commit_backend_for(state: &AppState, principal_audit: &str) -> PostgresCommitBackend {
    PostgresCommitBackend::new(
        state.pool.clone(),
        tenancy::default_workspace_id(),
        principal_audit.to_owned(),
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
pub(crate) fn derived_state(metadata: &TableMetadata) -> DerivedTableState {
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
        // Column names + docs of the current schema, indexed for full-text
        // search in the same write-through transaction (migration 0010).
        schema_text: metadata
            .current_schema()
            .map(meridian_store::search::schema_search_text),
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
            schema_text: derived.schema_text.as_deref(),
            snapshots: &derived.snapshots,
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
    headers: HeaderMap,
    Json(request): Json<CreateTableRequest>,
) -> Result<Response, ApiError> {
    let delegation = requested_delegation(&headers)?;
    let ctx = resolve_namespace(&state, &prefix, &raw_namespace).await?;
    let chain = namespace_scope_chain(&state.pool, &ctx.warehouse.id, &ctx.levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::CreateTable,
        &SecurableScope::namespace(&ctx.warehouse.id, chain),
    )
    .await?;
    // A mirror's foreign warehouse holds only read-only mirror-synced assets
    // (Pillar B, B-F1); native table creation there is rejected.
    reject_if_foreign_warehouse(&ctx.warehouse)?;
    validate_table_name(&request.name)?;

    // Fast-path collision checks for exact 409 messages; the store's create
    // transaction re-checks both under its insert (shared table/view name
    // space).
    if table::get(&state.pool, &ctx.namespace.id, &request.name)
        .await?
        .is_some()
    {
        return Err(ApiError::already_exists(format!(
            "table {:?} already exists",
            display_ident(&ctx.levels, &request.name)
        )));
    }
    if view::get(&state.pool, &ctx.namespace.id, &request.name)
        .await?
        .is_some()
    {
        return Err(ApiError::already_exists(format!(
            "the identifier {:?} already exists as a view \
             (tables and views share a namespace)",
            display_ident(&ctx.levels, &request.name)
        )));
    }

    let metadata = build_new_table_metadata(&request, &ctx.warehouse.storage_root, &ctx.levels)?;

    if request.stage_create {
        // Nothing durable happens here (see module docs): the metadata is
        // returned for the client to build on, and the create transaction
        // commits through the commit endpoint with assert-create. No table
        // row exists yet, so nothing is vended either (a staged create has
        // no audit resource); the client gets credentials when it loads
        // the committed table.
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

    let mut body = load_table_result(&ctx.warehouse, Some(&location), &metadata)?;
    if delegation == RequestedDelegation::VendedCredentials {
        // The caller just created the table and is about to write its
        // first data files: read-write, backed by the CREATE_TABLE grant
        // that got it this far.
        let vended = vend_for_table(
            &state,
            &principal,
            &VendContext {
                warehouse: &ctx.warehouse,
                table_id: &record.id,
                table_ident: &display_ident(&ctx.levels, &request.name),
                table_location: &metadata.location,
                access: meridian_vending::AccessMode::ReadWrite,
            },
        )
        .await?;
        if let Some(vended) = vended {
            attach_vended(&mut body, &vended);
        }
    } else if delegation == RequestedDelegation::RemoteSigning {
        // The creator is about to write the first data files through the
        // sign endpoint; advertisement only — nothing is vended, and
        // method policy is enforced per signing request.
        if let Some(signing) = remote_signing_config(
            &ctx.warehouse,
            &prefix,
            &ctx.levels,
            &request.name,
            &metadata.location,
        ) {
            attach_signing(&mut body, &signing);
        }
    }
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
pub(crate) async fn resolve_table(
    state: &AppState,
    prefix: &str,
    raw_namespace: &str,
    name: &str,
) -> Result<(WarehouseRecord, Vec<String>, table::TableRecord), ApiError> {
    // A `warehouse@branch` prefix (K-F2) addresses the same table row as its
    // base warehouse — only the pointer differs, resolved by the caller via the
    // catalog ref. `resolve_warehouse` resolves the base warehouse from either
    // prefix form, so branch-prefixed loads/commits find the table row.
    let warehouse = resolve_warehouse(&state.pool, prefix).await?;
    let levels = decode_namespace_param(raw_namespace)?;
    let record = table::get_by_name(&state.pool, &warehouse.id, &levels, name)
        .await?
        .ok_or_else(|| no_such_table(&levels, name))?;
    Ok((warehouse, levels, record))
}

/// Rejects a write against a **foreign** (mirrored, read-only) table with a
/// clear 409 that names the owning mirror and points the writer at the source
/// catalog (Pillar B, B-F1). A no-op for native tables.
pub(crate) fn reject_if_foreign(
    record: &table::TableRecord,
    levels: &[String],
    name: &str,
) -> Result<(), ApiError> {
    if let Some(mirror_id) = &record.mirror_id {
        return Err(ApiError::foreign_read_only(format!(
            "table {:?} is a foreign asset mirrored from an external catalog \
             (mirror {mirror_id}) and is read-only in Meridian; commit to the \
             source catalog instead",
            display_ident(levels, name)
        )));
    }
    Ok(())
}

/// Rejects a native create/register under a mirror's **foreign warehouse**
/// (which holds only mirror-synced, read-only assets). A no-op for native
/// warehouses.
pub(crate) fn reject_if_foreign_warehouse(warehouse: &WarehouseRecord) -> Result<(), ApiError> {
    if meridian_store::foreign::storage_config_is_foreign(&warehouse.storage_config.0) {
        return Err(ApiError::foreign_read_only(format!(
            "warehouse {:?} holds foreign (read-only) assets mirrored from an \
             external catalog; tables cannot be created or registered here",
            warehouse.name
        )));
    }
    Ok(())
}

/// `GET /{prefix}/namespaces/{namespace}/tables/{table}` — load a table.
pub async fn load_table(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
    Query(query): Query<LoadTableQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let delegation = requested_delegation(&headers)?;
    let (warehouse, levels, record) = resolve_table(&state, &prefix, &raw_namespace, &name).await?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::Read,
        &SecurableScope::table(&warehouse.id, chain.clone(), Some(&record.id)),
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

    // Branch-as-catalog (K-F2): on a `warehouse@branch` prefix, resolve the
    // table's pointer through the branch overlay — the branch's diverged
    // metadata.json if the table has diverged, else a fall-through to main.
    // The etag encodes the branch pointer version so branch and main loads
    // carry distinct validators (a branch commit does not invalidate a main
    // cached load and vice versa).
    let (_base, branch_suffix) = super::namespaces::split_prefix(&prefix);
    let (effective_location, effective_version) = if let Some(branch) = branch_suffix {
        // The base warehouse was resolved by resolve_table; look the branch/tag
        // up by name within the workspace (the prefix carried it).
        let record_branch = meridian_store::branches::get_by_name(
            &state.pool,
            meridian_store::tenancy::default_workspace_id(),
            branch,
        )
        .await?
        .filter(|b| b.state != "deleted")
        .ok_or_else(|| ApiError::no_such_warehouse(&prefix))?;
        // A tag reads its frozen pointer set (catalog_tags); a branch reads its
        // divergent overlay (branch_table_pointers). Both fall through to main
        // for a table they do not carry.
        let pointer = if record_branch.is_tag() {
            meridian_store::branches::resolve_tag_pointer(
                &state.pool,
                &record_branch.id,
                &record.id,
            )
            .await?
        } else {
            meridian_store::branches::resolve_pointer(&state.pool, &record_branch.id, &record.id)
                .await?
        }
        .ok_or_else(|| no_such_table(&levels, &name))?;
        (Some(pointer.metadata_location), pointer.pointer_version)
    } else {
        (record.metadata_location.clone(), record.pointer_version)
    };

    let etag = table_etag(&record.table_uuid, effective_version, refs_only);
    if if_none_match_matches(&headers, &etag) {
        // A 304 carries no body, only the validator.
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        if let Ok(value) = header::HeaderValue::from_str(&etag) {
            response.headers_mut().insert(header::ETAG, value);
        }
        return Ok(response);
    }

    let Some(metadata_location) = effective_location else {
        return Err(no_such_table(&levels, &name));
    };
    // A foreign (mirrored) table's metadata lives in the source catalog's
    // storage, addressed by its own location, not the mirror's synthetic
    // warehouse root (Pillar B, B-F1).
    let storage = if record.mirror_id.is_some() {
        connect_foreign_storage(&warehouse, &metadata_location)?
    } else {
        connect_storage(&warehouse)?
    };
    let mut metadata = read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .map_err(|e| current_metadata_unreadable(&metadata_location, &e))?;

    if refs_only {
        retain_referenced_snapshots(&mut metadata);
    }

    let mut body = load_table_result(&warehouse, Some(&metadata_location), &metadata)?;
    if delegation == RequestedDelegation::VendedCredentials {
        // Access follows the caller's grants: WRITE/COMMIT holders get
        // read-write credentials, READ-only holders read-only. A warehouse
        // without vending enabled ignores the header (pyiceberg sends it
        // by default), keeping plain config passthrough intact.
        let access = vend_access_mode(
            &state,
            &principal,
            &SecurableScope::table(&warehouse.id, chain, Some(&record.id)),
        )
        .await?;
        let vended = vend_for_table(
            &state,
            &principal,
            &VendContext {
                warehouse: &warehouse,
                table_id: &record.id,
                table_ident: &display_ident(&levels, &name),
                table_location: &metadata.location,
                access,
            },
        )
        .await?;
        if let Some(vended) = vended {
            attach_vended(&mut body, &vended);
        }
    } else if delegation == RequestedDelegation::RemoteSigning {
        // Advertisement only (silently skipped where signing cannot work,
        // mirroring the vended-credentials no-op on vending-disabled
        // warehouses): the per-request policy at the sign endpoint is
        // where access is enforced.
        if let Some(signing) =
            remote_signing_config(&warehouse, &prefix, &levels, &name, &metadata.location)
        {
            attach_signing(&mut body, &signing);
        }
    }
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
pub(crate) fn current_metadata_unreadable(location: &str, error: &StorageError) -> ApiError {
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
    // The table record joins the scope when the table exists; a caller denied
    // here learns nothing about existence, and the store still 404s a
    // missing table for authorized callers.
    let existing = table::get_by_name(&state.pool, &warehouse.id, &levels, &name).await?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::Drop,
        &SecurableScope::table(
            &warehouse.id,
            chain,
            existing.as_ref().map(|r| r.id.as_str()),
        ),
    )
    .await?;
    // Foreign (mirrored) tables are managed only by the sync engine — a user
    // drop is rejected (Pillar B, B-F1); removing the mirror removes them.
    if let Some(existing) = &existing {
        reject_if_foreign(existing, &levels, &name)?;
    }

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
    let source = table::get_by_name(
        &state.pool,
        &warehouse.id,
        &request.source.namespace,
        &request.source.name,
    )
    .await?;
    let source_chain =
        namespace_scope_chain(&state.pool, &warehouse.id, &request.source.namespace).await?;
    require(
        &state.pool,
        &principal,
        Privilege::Write,
        &SecurableScope::table(
            &warehouse.id,
            source_chain,
            source.as_ref().map(|r| r.id.as_str()),
        ),
    )
    .await?;
    // A foreign (mirrored) table cannot be renamed by a user — the source
    // catalog owns its identity (Pillar B, B-F1).
    if let Some(source) = &source {
        reject_if_foreign(source, &request.source.namespace, &request.source.name)?;
    }
    let dest_chain =
        namespace_scope_chain(&state.pool, &warehouse.id, &request.destination.namespace).await?;
    require(
        &state.pool,
        &principal,
        Privilege::CreateTable,
        &SecurableScope::namespace(&warehouse.id, dest_chain),
    )
    .await?;
    // Renaming *into* a mirror's foreign warehouse is also rejected (it holds
    // only mirror-synced assets).
    reject_if_foreign_warehouse(&warehouse)?;

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
    // Registering a table into a mirror's foreign warehouse is rejected — it
    // holds only read-only mirror-synced assets (Pillar B, B-F1).
    reject_if_foreign_warehouse(&ctx.warehouse)?;
    validate_table_name(&request.name)?;
    if request.overwrite {
        // TODO(M1+): registerTable overwrite (pointer adoption over an
        // existing table). Needs its own audited pointer-swap path; rejected
        // rather than half-implemented.
        return Err(ApiError::bad_request(
            "register with overwrite=true is not supported yet",
        ));
    }

    // Fast-path collision checks for exact 409 messages; the store's create
    // transaction re-checks both under its insert (shared table/view name
    // space).
    if table::get(&state.pool, &ctx.namespace.id, &request.name)
        .await?
        .is_some()
    {
        return Err(ApiError::already_exists(format!(
            "table {:?} already exists",
            display_ident(&ctx.levels, &request.name)
        )));
    }
    if view::get(&state.pool, &ctx.namespace.id, &request.name)
        .await?
        .is_some()
    {
        return Err(ApiError::already_exists(format!(
            "the identifier {:?} already exists as a view \
             (tables and views share a namespace)",
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
            schema_text: derived.schema_text.as_deref(),
            snapshots: &derived.snapshots,
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
// The commit endpoint's dispatch (authz + foreign guards + branch routing +
// replay + create-vs-update) reads as one linear sequence; splitting it would
// scatter the request-handling contract across helpers. The branch path is
// already extracted into `dispatch_branch_commit`.
#[allow(clippy::too_many_lines)]
pub async fn commit_table(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    // Branch-as-catalog (K-F2): resolve the base warehouse and the catalog ref
    // from the prefix. A branch commit goes to the branch pointer, not main.
    let (warehouse, catalog) = super::namespaces::resolve_catalog_ref(&state.pool, &prefix).await?;
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

    // Authorize before the idempotency recall so an unauthorized caller
    // can never replay a recorded receipt: COMMIT on an existing table,
    // CREATE_TABLE on the namespace when the commit would create one.
    let record = table::get_by_name(&state.pool, &warehouse.id, &levels, &name).await?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;

    // Commit to a branch (K-F1/K-F2) routes to the branch commit path, which
    // advances the branch pointer via the same CAS/audit/outbox discipline and
    // never touches main. Extracted to keep this function readable.
    if let Some(branch) = catalog.branch() {
        return dispatch_branch_commit(
            &state,
            &backend,
            &warehouse,
            branch,
            record,
            &chain,
            &levels,
            &name,
            &parsed,
            &principal,
            key.as_deref(),
            &fingerprint,
        )
        .await;
    }

    if let Some(existing) = &record {
        require(
            &state.pool,
            &principal,
            Privilege::Commit,
            &SecurableScope::table(&warehouse.id, chain.clone(), Some(&existing.id)),
        )
        .await?;
        // Foreign (mirrored) tables are read-only: the external catalog is the
        // write authority (Pillar B, B-F1). Reject after authorization so the
        // rejection does not leak table existence to callers without COMMIT.
        // This must precede connecting storage: a foreign warehouse's synthetic
        // `mirror://` root is not a real storage target.
        reject_if_foreign(existing, &levels, &name)?;
    } else {
        require(
            &state.pool,
            &principal,
            Privilege::CreateTable,
            &SecurableScope::namespace(&warehouse.id, chain.clone()),
        )
        .await?;
        // A commit that would create a table under a mirror's foreign warehouse
        // is likewise rejected — that warehouse holds only mirror-synced assets.
        reject_if_foreign_warehouse(&warehouse)?;
    }

    // Connect storage after the foreign guards (a foreign warehouse has no real
    // storage root to connect); the replay and commit both need it.
    let storage = connect_storage(&warehouse)?;

    // Idempotency recall (§3 step 2): replay before touching any state.
    if let Some(key) = &key
        && let Some(response) = try_replay(&backend, &storage, key, &fingerprint).await?
    {
        return Ok(response);
    }

    let idem = key.as_deref().map(|k| (k, fingerprint.as_str()));

    match record {
        Some(record) => {
            commit_existing_table(
                &state,
                &backend,
                &storage,
                &record,
                &chain,
                &principal.audit_string(),
                &parsed,
                idem,
            )
            .await
        }
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
    /// The base metadata the candidate was built on (the current-pointer
    /// metadata). Retained so the pre-commit contract hook can classify the
    /// staged schema against it without a second load.
    base: TableMetadata,
}

// ---------------------------------------------------------------------------
// The circuit breaker — pre-commit contract hook (Pillar E, E-F4)
//
// `commit-protocol.md` §3 step 6 names this exact insertion point. The hook
// runs after the candidate is built and staged, before the pointer CAS; its
// decision either lets the commit proceed (allow / warn / quarantine, the
// latter two carrying a violation record written atomically with the swap) or
// rejects it (block). Full semantics + the invariant-preservation argument:
// `docs/design/contracts-circuit-breaker.md`.
// ---------------------------------------------------------------------------

/// The outcome of evaluating all contracts bound to a table for one commit.
enum ContractDecision {
    /// No enabled contract was violated: proceed unchanged.
    Allow,
    /// Warn mode: the commit lands; the record is written with the swap.
    Warn(contracts::OwnedViolationRecord),
    /// Quarantine mode: the candidate is retargeted onto the named audit branch
    /// (`main` frozen); the record is written with the swap.
    Quarantine(contracts::OwnedViolationRecord, String),
    /// Block mode: reject. Carries the violated contract's identity + the
    /// violations for the machine-readable 409 body and the standalone record.
    Block(BlockedCommit),
}

/// A blocked commit's detail: everything the 409 body and the violation record
/// need.
struct BlockedCommit {
    contract_id: String,
    contract_name: String,
    mode: contracts::EnforcementMode,
    violations: Vec<contracts::Violation>,
}

/// Evaluates every enabled contract bound to `table_id` (directly or via its
/// namespace chain) against the staged candidate, and decides what the circuit
/// breaker does. Pure of side effects except the contract *resolution* read;
/// the classification itself is a pure CPU function (no data-file I/O).
///
/// Precedence when several contracts apply and are violated: **block wins over
/// quarantine wins over warn** — the strictest violated mode decides the
/// commit's fate, and its violations are the ones surfaced. (All violated
/// contracts still get a violation *record*; precedence only picks the
/// commit-fate + the machine body.)
///
/// `head_snapshot` is the candidate's current snapshot id (for the record) and
/// `summary` its summary (for predicates); both `None` for a schema-only
/// commit that adds no snapshot.
async fn decide_contracts(
    state: &AppState,
    table_id: &str,
    namespace_ids: &[String],
    base: &TableMetadata,
    candidate: &TableMetadata,
) -> Result<ContractDecision, ApiError> {
    let contracts_for_table = contracts::resolve_for_table(
        &state.pool,
        tenancy::default_workspace_id(),
        table_id,
        namespace_ids,
    )
    .await
    .map_err(ApiError::from)?;
    if contracts_for_table.is_empty() {
        return Ok(ContractDecision::Allow);
    }

    // The schemas to compare. A commit with no current schema (should not
    // happen for a live table) can't be classified — treat as allow (there is
    // nothing to evolve against).
    let (Some(base_schema), Some(staged_schema)) =
        (base.current_schema(), candidate.current_schema())
    else {
        return Ok(ContractDecision::Allow);
    };
    let head_snapshot = candidate.current_snapshot_id.filter(|id| *id >= 0);
    let summary = candidate
        .current_snapshot()
        .and_then(|s| s.summary.as_ref());

    // Evaluate every contract; keep the strictest violated mode as the fate.
    // The strictest violated mode decides the commit's fate; that contract's
    // own violations are what we surface and record (so a recorded violation is
    // always attributed to the contract it came from). When several contracts
    // of equal severity are violated on one commit — rare — the first in id
    // order wins the fate; recording each violated contract separately is a
    // tracked refinement, not needed for the correctness bar.
    let mut fate: Option<(
        contracts::EnforcementMode,
        contracts::Contract,
        Vec<contracts::Violation>,
    )> = None;

    for contract in &contracts_for_table {
        let violations = contract.spec.evaluate(base_schema, staged_schema, summary);
        if violations.is_empty() {
            continue;
        }
        let stricter = |a: contracts::EnforcementMode, b: contracts::EnforcementMode| {
            // block > quarantine > warn
            let rank = |m: contracts::EnforcementMode| match m {
                contracts::EnforcementMode::Warn => 0,
                contracts::EnforcementMode::Quarantine => 1,
                contracts::EnforcementMode::Block => 2,
            };
            if rank(a) >= rank(b) { a } else { b }
        };
        match &mut fate {
            None => fate = Some((contract.mode, contract.clone(), violations)),
            Some((mode, chosen, chosen_violations)) => {
                let winner = stricter(*mode, contract.mode);
                if winner != *mode {
                    *mode = winner;
                    *chosen = contract.clone();
                    *chosen_violations = violations;
                }
            }
        }
    }

    let Some((mode, contract, violations)) = fate else {
        return Ok(ContractDecision::Allow);
    };

    match mode {
        contracts::EnforcementMode::Block => Ok(ContractDecision::Block(BlockedCommit {
            contract_id: contract.id.clone(),
            contract_name: contract.name.clone(),
            mode,
            violations,
        })),
        contracts::EnforcementMode::Warn => {
            Ok(ContractDecision::Warn(contracts::OwnedViolationRecord {
                contract_id: contract.id.clone(),
                contract_name: contract.name.clone(),
                table_id: table_id.to_owned(),
                snapshot_id: head_snapshot,
                mode,
                outcome: contracts::ViolationOutcome::Warned,
                violations,
            }))
        }
        contracts::EnforcementMode::Quarantine => Ok(ContractDecision::Quarantine(
            contracts::OwnedViolationRecord {
                contract_id: contract.id.clone(),
                contract_name: contract.name.clone(),
                table_id: table_id.to_owned(),
                snapshot_id: head_snapshot,
                mode,
                outcome: contracts::ViolationOutcome::Quarantined,
                violations,
            },
            contract.quarantine_branch.clone(),
        )),
    }
}

/// Renders a blocked commit as a `409 CommitFailedException` whose envelope
/// carries a machine-readable `contract-violation` object (design doc §5). Built
/// locally so the shared `ErrorBody` stays a fixed 3-field shape; this is the
/// one response that needs the richer body.
fn blocked_commit_response(table_id: &str, blocked: &BlockedCommit) -> Response {
    let first = blocked
        .violations
        .first()
        .map_or("contract violated", |v| v.detail.as_str());
    let message = format!(
        "commit blocked by data contract {:?}: {first}",
        blocked.contract_name
    );
    let body = json!({
        "error": {
            "message": message,
            "type": "CommitFailedException",
            "code": 409,
            "contract-violation": {
                "contract-id": blocked.contract_id,
                "contract-name": blocked.contract_name,
                "mode": blocked.mode.as_str(),
                "table": table_id,
                "violations": blocked.violations,
            },
        }
    });
    (StatusCode::CONFLICT, Json(body)).into_response()
}

/// Records a block-mode violation in its own transaction (the commit was
/// rejected, so there is no commit transaction to join), then returns the
/// machine-readable 409. Recording failure is logged but never masks the block
/// — the contract still rejects the commit.
async fn reject_blocked_commit(
    state: &AppState,
    principal: &str,
    table_id: &str,
    snapshot_id: Option<i64>,
    blocked: BlockedCommit,
) -> Response {
    let record = contracts::ViolationRecord {
        contract_id: &blocked.contract_id,
        contract_name: &blocked.contract_name,
        table_id,
        snapshot_id,
        mode: blocked.mode,
        outcome: contracts::ViolationOutcome::Blocked,
        violations: &blocked.violations,
    };
    if let Err(error) = contracts::record_violation(
        &state.pool,
        tenancy::default_workspace_id(),
        principal,
        &record,
    )
    .await
    {
        tracing::error!(%error, contract = %blocked.contract_id, table = %table_id,
            "failed to record blocked-commit violation (the commit is still blocked)");
    }
    blocked_commit_response(table_id, &blocked)
}

/// Retargets a prepared commit onto its quarantine branch: rewrites the
/// candidate metadata so `main` is frozen (design doc §3.3), re-stages the
/// retargeted file, and discards the original staged file. Returns the
/// retargeted `PreparedTable` on success, or `None` when the candidate added no
/// snapshot (a schema-only violation cannot be quarantined — the caller
/// degrades to block).
async fn retarget_for_quarantine(
    storage: &Arc<dyn Storage>,
    mut prepared: PreparedTable,
    branch: &str,
) -> Result<Option<PreparedTable>, ApiError> {
    let base = prepared.base.clone();
    // The head snapshot id the retarget parks on the branch is already carried
    // by the violation record's snapshot_id; we only need to know whether a
    // retarget happened (a schema-only candidate returns None → degrade).
    if prepared
        .metadata
        .quarantine_retarget(&base, branch)
        .is_none()
    {
        return Ok(None);
    }

    // Re-stage the retargeted metadata under a fresh unique name; discard the
    // original staged candidate (it advanced main — it must never be the
    // pointer target).
    let old_staged = prepared.op.cas.new_metadata_location.clone();
    let restaged = new_metadata_location(
        &prepared.metadata.location,
        prepared.op.cas.expected_version + 1,
        Uuid::new_v4(),
    );
    if let Err(error) =
        meridian_storage::write_table_metadata(storage.as_ref(), &restaged, &prepared.metadata)
            .await
    {
        // The retargeted file failed to stage; the original staged candidate is
        // now an orphan (we are abandoning this attempt). Clean it best-effort,
        // exactly as the caller would on any other non-committed outcome (§7.1).
        discard_staged(storage, &old_staged).await;
        return Err(storage_to_api(&error));
    }
    discard_staged(storage, &old_staged).await;

    prepared.op.cas.new_metadata_location = restaged;
    prepared.op.derived = Some(derived_state(&prepared.metadata));
    Ok(Some(prepared))
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
            contract_violation: None,
        },
        metadata: candidate,
        base,
    })
}

/// The single-table commit loop (design doc §6): bounded rebase-retry on a
/// lost compare-and-set, requirement re-check per attempt. The pre-commit
/// contract hook (Pillar E, E-F4) runs each attempt after the candidate is
/// staged and before the CAS: it may block the commit (409), attach a warn
/// record, or retarget the candidate onto the quarantine branch.
// One call site; the args are the commit context (state, backend, storage,
// the table + its namespace chain, the acting principal, the parsed request,
// and the idempotency tuple) — a struct would only rename them.
#[allow(clippy::too_many_arguments)]
async fn commit_existing_table(
    state: &AppState,
    backend: &PostgresCommitBackend,
    storage: &Arc<dyn Storage>,
    record: &table::TableRecord,
    namespace_ids: &[String],
    principal: &str,
    parsed: &ParsedCommit,
    idempotency: Option<(&str, &str)>,
) -> Result<Response, ApiError> {
    for _attempt in 1..=MAX_COMMIT_ATTEMPTS {
        let mut prepared = prepare_table_commit(
            backend,
            storage,
            record,
            &parsed.requirements,
            &parsed.updates,
            &record.name,
        )
        .await?;

        // The circuit breaker: evaluate contracts against the staged candidate.
        match decide_contracts(
            state,
            &record.id,
            namespace_ids,
            &prepared.base,
            &prepared.metadata,
        )
        .await?
        {
            ContractDecision::Allow => {}
            ContractDecision::Warn(record_) => {
                prepared.op.contract_violation = Some(record_);
            }
            ContractDecision::Quarantine(record_, branch) => {
                let branch_snapshot = record_.snapshot_id;
                let Some(mut retargeted) =
                    retarget_for_quarantine(storage, prepared, &branch).await?
                else {
                    // Schema-only violation: quarantine has no snapshot to
                    // retarget, so it degrades to block (fail-closed).
                    let blocked = BlockedCommit {
                        contract_id: record_.contract_id,
                        contract_name: record_.contract_name,
                        mode: contracts::EnforcementMode::Block,
                        violations: record_.violations,
                    };
                    return Ok(reject_blocked_commit(
                        state,
                        principal,
                        &record.id,
                        branch_snapshot,
                        blocked,
                    )
                    .await);
                };
                retargeted.op.contract_violation = Some(record_);
                prepared = retargeted;
            }
            ContractDecision::Block(blocked) => {
                let snapshot = prepared.metadata.current_snapshot_id.filter(|id| *id >= 0);
                // The staged candidate never becomes durable — discard it.
                discard_staged(storage, &prepared.op.cas.new_metadata_location).await;
                return Ok(
                    reject_blocked_commit(state, principal, &record.id, snapshot, blocked).await,
                );
            }
        }

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

/// Dispatches a commit whose prefix carried a branch/tag (K-F1/K-F2): rejects a
/// tag (immutable), requires the table to already exist (creating a table only
/// on a branch is out of scope this milestone), authorizes, replays an
/// idempotency hit, and runs the branch commit loop.
// One call site; the args are the commit context (state, backend, warehouse,
// branch, the resolved record + its namespace chain, the parsed request, the
// principal, and the idempotency inputs) — a struct would only rename them.
#[allow(clippy::too_many_arguments)]
async fn dispatch_branch_commit(
    state: &AppState,
    backend: &PostgresCommitBackend,
    warehouse: &WarehouseRecord,
    branch: &meridian_store::branches::BranchRecord,
    record: Option<table::TableRecord>,
    chain: &[String],
    levels: &[String],
    name: &str,
    parsed: &ParsedCommit,
    principal: &Principal,
    key: Option<&str>,
    fingerprint: &str,
) -> Result<Response, ApiError> {
    if branch.is_tag() {
        return Err(ApiError::foreign_read_only(format!(
            "{:?} is a tag (an immutable ref) and cannot be committed to; \
             commit to a branch or to main instead",
            branch.name
        )));
    }
    let Some(existing) = record else {
        return Err(no_such_table(levels, name));
    };
    require(
        &state.pool,
        principal,
        Privilege::Commit,
        &SecurableScope::table(&warehouse.id, chain.to_vec(), Some(&existing.id)),
    )
    .await?;
    reject_if_foreign(&existing, levels, name)?;
    let storage = connect_storage(warehouse)?;
    if let Some(key) = key
        && let Some(response) = try_replay(backend, &storage, key, fingerprint).await?
    {
        return Ok(response);
    }
    let idem = key.map(|k| (k, fingerprint));
    commit_existing_table_branch(backend, &storage, branch, &existing, parsed, idem).await
}

/// The branch commit loop (K-F1/K-F2). Structurally identical to
/// [`commit_existing_table`]'s optimistic loop, but each attempt loads the
/// table's pointer *through the branch overlay* (the branch's diverged
/// metadata.json if present, else main's), stages a candidate, and CAS's the
/// **branch** pointer via [`PostgresCommitBackend::commit_branch_table`]. main
/// is never in the write set (branching.md §3), so a branch commit cannot move
/// main. On the first commit for a table the branch pointer is seeded from
/// main and the table diverges; thereafter it advances a branch-local version.
///
/// The circuit breaker (Pillar E) is intentionally not run on a branch commit:
/// contracts gate the *merge* to main (the branch merge gate, K-F3), letting a
/// branch hold in-progress/experimental state that would not yet pass main's
/// contracts — the whole point of a dev branch.
async fn commit_existing_table_branch(
    backend: &PostgresCommitBackend,
    storage: &Arc<dyn Storage>,
    branch: &meridian_store::branches::BranchRecord,
    record: &table::TableRecord,
    parsed: &ParsedCommit,
    idempotency: Option<(&str, &str)>,
) -> Result<Response, ApiError> {
    for _attempt in 1..=MAX_COMMIT_ATTEMPTS {
        // Load the base the commit builds on: the branch pointer if diverged,
        // else main (a fall-through seed for first divergence).
        let (pointer, diverged) = backend
            .load_branch_pointer(&branch.id, &record.id)
            .await
            .map_err(backend_to_api)?;
        let base = read_table_metadata(storage.as_ref(), &pointer.metadata_location)
            .await
            .map_err(|e| current_metadata_unreadable(&pointer.metadata_location, &e))?;

        let mut violations = Vec::new();
        check_requirements(
            &parsed.requirements,
            Some(&base),
            &record.name,
            &mut violations,
        );
        if !violations.is_empty() {
            return Err(ApiError::commit_failed(violations.join("; ")));
        }

        let mut builder = base.builder_from();
        builder
            .apply_all(parsed.updates.iter().cloned())
            .map_err(|e| map_build_error(&e))?;
        let candidate = builder
            .build(
                Utc::now().timestamp_millis(),
                Some(&pointer.metadata_location),
            )
            .map_err(|e| map_build_error(&e))?;

        // Stage under the next branch pointer version with a fresh uuid, so no
        // attempt can overwrite a published branch file.
        let staged_location =
            new_metadata_location(&candidate.location, pointer.version + 1, Uuid::new_v4());
        meridian_storage::write_table_metadata(storage.as_ref(), &staged_location, &candidate)
            .await
            .map_err(|e| storage_to_api(&e))?;

        match backend
            .commit_branch_table(
                &branch.id,
                &record.id,
                pointer.version,
                diverged,
                &staged_location,
                idempotency,
            )
            .await
        {
            Ok(receipt) if receipt.replayed => {
                discard_staged(storage, &staged_location).await;
                return replay_response(storage, &receipt).await;
            }
            Ok(receipt) => {
                let version = receipt
                    .tables
                    .first()
                    .map_or(0, |t| i64::try_from(t.version).unwrap_or(i64::MAX));
                let body = json!({
                    "metadata-location": staged_location,
                    "metadata": metadata_to_value(&candidate)?,
                });
                return Ok(json_with_etag(
                    StatusCode::OK,
                    body,
                    &table_etag(&record.table_uuid, version, false),
                ));
            }
            Err(CommitBackendError::VersionConflict { .. }) => {
                discard_staged(storage, &staged_location).await;
            }
            Err(CommitBackendError::StateUnknown { message }) => {
                tracing::error!(%message, table = %record.id, branch = %branch.id,
                    "branch commit state unknown");
                return Err(ApiError::commit_state_unknown(
                    "the branch commit outcome could not be determined; retry with the \
                     same Idempotency-Key to resolve",
                ));
            }
            Err(other) => {
                discard_staged(storage, &staged_location).await;
                return Err(backend_to_api(other));
            }
        }
    }
    Err(ApiError::commit_failed(format!(
        "branch commit lost the compare-and-set race {MAX_COMMIT_ATTEMPTS} time(s); \
         refresh and retry"
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
pub(crate) fn backend_to_api(error: CommitBackendError) -> ApiError {
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
    // staging I/O. The per-table namespace chains are retained for the
    // contract hook below (contracts bind to a table or its namespaces).
    let mut chains: Vec<Vec<String>> = Vec::with_capacity(records.len());
    for (identifier, record) in changes.iter().map(|(i, _)| i).zip(&records) {
        let chain =
            namespace_scope_chain(&state.pool, &warehouse.id, &identifier.namespace).await?;
        require(
            &state.pool,
            &principal,
            Privilege::Commit,
            &SecurableScope::table(&warehouse.id, chain.clone(), Some(&record.id)),
        )
        .await?;
        // A transaction touching any foreign (read-only) table is rejected as a
        // whole, before staging — the external catalog owns those tables
        // (Pillar B, B-F1).
        reject_if_foreign(record, &identifier.namespace, &identifier.name)?;
        chains.push(chain);
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
        // A block on any table rejects the whole transaction atomically (I2):
        // set on the first block, then unwind the staged files and 409.
        let mut blocked: Option<(String, Option<i64>, BlockedCommit)> = None;
        for (index, ((_, parsed), (record, (pointer, base)))) in
            changes.iter().zip(records.iter().zip(bases)).enumerate()
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

            // The circuit breaker, per table. In a multi-table transaction a
            // quarantine degrades to block (design doc §3.3: retargeting one
            // table of an atomic set would break the producer's atomicity), and
            // any block rejects the whole transaction (I2).
            let contract_violation =
                match decide_contracts(&state, &record.id, &chains[index], &base, &candidate).await
                {
                    Ok(ContractDecision::Allow) => None,
                    Ok(ContractDecision::Warn(rec)) => Some(rec),
                    Ok(ContractDecision::Quarantine(rec, _branch)) => {
                        // Degrade to block.
                        blocked = Some((
                            record.id.clone(),
                            rec.snapshot_id,
                            BlockedCommit {
                                contract_id: rec.contract_id,
                                contract_name: rec.contract_name,
                                mode: contracts::EnforcementMode::Block,
                                violations: rec.violations,
                            },
                        ));
                        break;
                    }
                    Ok(ContractDecision::Block(b)) => {
                        let snapshot = candidate.current_snapshot_id.filter(|id| *id >= 0);
                        blocked = Some((record.id.clone(), snapshot, b));
                        break;
                    }
                    Err(error) => {
                        stage_failure = Some(error);
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
                contract_violation,
            });
        }
        // A block unwinds every staged file of this attempt (nothing commits —
        // I2) and returns the machine-readable 409.
        if let Some((table_id, snapshot, blocked)) = blocked {
            for location in &staged {
                discard_staged(&storage, location).await;
            }
            return Ok(reject_blocked_commit(
                &state,
                &principal.audit_string(),
                &table_id,
                snapshot,
                blocked,
            )
            .await);
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
