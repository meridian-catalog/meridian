//! Iceberg REST catalog view endpoints.
//!
//! Mounted under both `/iceberg/v1/{prefix}` and `/v1/{prefix}`, mirroring
//! the table surface in [`super::tables`]. Views reuse the commit-protocol
//! pointer discipline (immutable metadata files, compare-and-set pointer
//! swap, audit + outbox in the swap transaction) but need none of the
//! multi-table machinery: the swap lives in [`meridian_store::view`].
//!
//! Conventions (documented decisions):
//!
//! - **Default view location** is
//!   `<warehouse-root>/<ns level>/<…>/<view-name>-<view-uuid>` — the same
//!   uuid-suffixed layout as tables, for the same reason (a dropped and
//!   recreated view never reuses a path).
//! - **Metadata file names** follow the table convention
//!   (`metadata/<version, 5+ digits>-<random uuid>.metadata.json`), which
//!   the view spec explicitly reuses.
//! - **Tables and views share one name space per namespace**: the spec's
//!   `createView`/`renameView` (and `createTable`/`renameTable`) 409 when
//!   "the identifier already exists as a table or view". Both directions are
//!   enforced: the views side here (and in [`meridian_store::view`]), the
//!   tables side in [`super::tables`] and [`meridian_store::table`].
//! - **No `ETag` on view responses**: the spec defines the `ETag` header and
//!   `If-None-Match` handling for table load responses only
//!   (`LoadViewResponse` carries no `etag` header), so views deliberately
//!   send none.
//! - **`dropView` never touches files**: the spec defines no
//!   `purgeRequested` for views; metadata files are left for the
//!   maintenance worker's sweep.
//! - **`Idempotency-Key` is not honored on view endpoints yet** (the spec
//!   draft attaches it to replace/drop/rename). A retried replace re-runs
//!   the commit; see `docs/api-status.md`.
//! - **Authorization** (full mapping in the `crate::routes::grants` module
//!   docs): list `LIST_TABLES` (namespace), create `CREATE_VIEW`
//!   (namespace), load/exists `READ` (view), replace `COMMIT` (view),
//!   drop `DROP` (view), rename `WRITE` on the source view plus
//!   `CREATE_VIEW` on the destination namespace. Grants on a namespace or
//!   warehouse cover the views they contain; every check runs before the
//!   handler mutates anything. Denials are 403 `ForbiddenException`.
//! - **`referenced-by`** on `loadView` (nested-view access chains) is
//!   accepted and ignored — the caller's `READ` on the view itself decides
//!   access; chain-based decisions are not implemented.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::Utc;
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_iceberg::spec::{
    LAST_ADDED, Schema, SqlViewRepresentation, ViewMetadata, ViewMetadataBuildError,
    ViewMetadataBuilder, ViewRepresentation, ViewRequirement, ViewUpdate, ViewVersion,
};
use meridian_storage::{Storage, StorageError, new_view_metadata_location, read_view_metadata};
use meridian_store::rbac::{Privilege, SecurableScope};
use meridian_store::view::{ViewCommitError, ViewPointerSwap};
use meridian_store::warehouse::WarehouseRecord;
use meridian_store::{namespace, semantics, table, tenancy, view};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::{namespace_scope_chain, require, require_management};
use crate::routes::namespaces::{
    decode_namespace_param, next_page_token, resolve_pagination, resolve_warehouse,
};
use crate::sidecar::{SidecarClient, TranspileStatus};

/// Bounded rebase-retry budget for the replace compare-and-set loop, same
/// as the table commit path (design doc §6).
const MAX_COMMIT_ATTEMPTS: u32 = 3;

// ---------------------------------------------------------------------------
// Storage config passthrough (shared with the table load responses)
// ---------------------------------------------------------------------------

/// Storage option keys that are credential material and must NEVER be
/// forwarded to clients in any response body. Credential delivery is the M2
/// vending milestone (`loadCredentials` / `X-Iceberg-Access-Delegation`),
/// never a side effect of config passthrough.
const STORAGE_CONFIG_DENYLIST: &[&str] = &["access-key-id", "secret-access-key", "session-token"];

/// Maps a warehouse's non-secret storage options onto the Iceberg client
/// property names for the `config` field of `LoadTableResult` /
/// `LoadViewResult`, so clients pointed at S3-compatible stores (`MinIO`,
/// R2, ...) resolve the endpoint and addressing style from the catalog
/// instead of local configuration.
///
/// Mapping (warehouse option → client property):
///
/// | option       | client properties                |
/// |--------------|----------------------------------|
/// | `endpoint`   | `s3.endpoint`                    |
/// | `region`     | `client.region`, `s3.region`     |
/// | `path-style` | `s3.path-style-access`           |
///
/// When `endpoint.external` is set on the warehouse it wins over `endpoint`
/// for `s3.endpoint`: the server keeps talking to storage via the internal
/// endpoint while every client-facing config advertises the external one
/// (containerized engines reach `MinIO` on a different address than the
/// server — the documented `host.docker.internal` situation).
///
/// Everything else is either credential material (denylisted above) or a
/// server-side concern (`retry.*`, `anonymous`) with no client property,
/// and is not forwarded. Filesystem-rooted warehouses carry none of these
/// options, so their `config` stays empty.
pub(crate) fn storage_client_config(warehouse: &WarehouseRecord) -> BTreeMap<String, String> {
    let mut config = BTreeMap::new();
    let external = warehouse.storage_config.0.get("endpoint.external");
    for (key, value) in &warehouse.storage_config.0 {
        if STORAGE_CONFIG_DENYLIST.contains(&key.as_str()) {
            continue;
        }
        match key.as_str() {
            "endpoint" | "endpoint.external" => {
                let advertised = external.unwrap_or(value);
                config.insert("s3.endpoint".to_owned(), advertised.clone());
            }
            "region" => {
                config.insert("client.region".to_owned(), value.clone());
                config.insert("s3.region".to_owned(), value.clone());
            }
            "path-style" => {
                config.insert("s3.path-style-access".to_owned(), value.clone());
            }
            _ => {}
        }
    }
    config
}

// ---------------------------------------------------------------------------
// Shared plumbing (kept local: the table module's equivalents are private
// and its handlers are owned separately)
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

/// Validates a view name for use as a URL path segment.
fn validate_view_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(ApiError::bad_request("view name must not be empty"));
    }
    if name.contains('\u{1f}') {
        return Err(ApiError::bad_request(
            "view name must not contain the 0x1F unit separator",
        ));
    }
    Ok(())
}

/// Connects the warehouse's storage profile.
fn connect_storage(warehouse: &WarehouseRecord) -> Result<Arc<dyn Storage>, ApiError> {
    let profile =
        meridian_storage::StorageProfile::parse(&warehouse.storage_root, &warehouse.storage_config)
            .map_err(|e| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalServerError",
                    format!(
                        "warehouse {:?} storage configuration is unusable: {e}",
                        warehouse.name
                    ),
                )
            })?;
    profile.connect().map_err(|e| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
            format!(
                "warehouse {:?} storage configuration is unusable: {e}",
                warehouse.name
            ),
        )
    })
}

/// Human-readable `ns.ns.view` identifier.
fn display_ident(levels: &[String], name: &str) -> String {
    if levels.is_empty() {
        name.to_owned()
    } else {
        format!("{}.{name}", levels.join("."))
    }
}

/// 404 `NoSuchViewException` (the spec's view-specific 404 type).
fn no_such_view(levels: &[String], name: &str) -> ApiError {
    ApiError::new(
        StatusCode::NOT_FOUND,
        "NoSuchViewException",
        format!("view {:?} does not exist", display_ident(levels, name)),
    )
}

/// The default location for a new view (see module docs).
fn default_view_location(
    storage_root: &str,
    levels: &[String],
    name: &str,
    view_uuid: Uuid,
) -> String {
    let root = storage_root.trim_end_matches('/');
    let path = levels.join("/");
    if path.is_empty() {
        format!("{root}/{name}-{view_uuid}")
    } else {
        format!("{root}/{path}/{name}-{view_uuid}")
    }
}

/// Renders view metadata exactly as written to storage, so the response
/// body and the `metadata.json` file can never disagree.
fn metadata_to_value(metadata: &ViewMetadata) -> Result<Value, ApiError> {
    let text = metadata
        .to_json()
        .map_err(|e| MeridianError::internal("failed to serialize view metadata", e))?;
    serde_json::from_str(&text)
        .map_err(|e| MeridianError::internal("view metadata JSON round-trip failed", e).into())
}

/// The `LoadViewResult` body. `config` carries the warehouse's non-secret
/// storage options mapped to Iceberg client property names.
fn load_view_result(
    warehouse: &WarehouseRecord,
    metadata_location: &str,
    metadata: &ViewMetadata,
) -> Result<Value, ApiError> {
    Ok(json!({
        "metadata-location": metadata_location,
        "metadata": metadata_to_value(metadata)?,
        "config": storage_client_config(warehouse),
    }))
}

// ---------------------------------------------------------------------------
// Universal views (Pillar G, G-F1): serve the representation matching the
// requesting engine's dialect, transpiling + caching when absent.
// ---------------------------------------------------------------------------

/// The representation-extra key that carries a translation's honest status
/// (`verified` | `best_effort` | `unsupported`) into the dialect-tagged Iceberg
/// SQL representation. A representation Meridian did not synthesize (the
/// author's own) carries no such key.
const TRANSPILE_STATUS_KEY: &str = "meridian.transpile-status";

/// The representation-extra key that marks a representation as synthesized by
/// Meridian's transpiler (vs. authored). Lets callers and the console
/// distinguish a translation from an original.
const TRANSPILE_ORIGIN_KEY: &str = "meridian.transpile-origin";

/// Query parameters accepted on `loadView`. `engine` is the explicit
/// requesting-engine dialect override (the console and migration tools set it);
/// when absent the dialect is inferred from the `User-Agent`.
#[derive(Debug, Default, Deserialize)]
pub struct LoadViewQuery {
    /// The requesting engine's SQL dialect (e.g. `trino`, `duckdb`). An explicit
    /// override; wins over `User-Agent` inference.
    pub engine: Option<String>,
}

/// Resolves the requesting engine's dialect for a `loadView`, in priority
/// order: the explicit `?engine=` override, then `User-Agent` inference, then
/// `None` (serve the view as authored, no transpilation).
///
/// Engine identification is deliberately conservative: only well-known clients
/// map by `User-Agent`, and an unrecognized agent yields `None` rather than a
/// guess (a wrong dialect would be worse than none). The `?engine=` override is
/// the escape hatch for anything the agent map does not cover.
fn resolve_requesting_dialect(query: &LoadViewQuery, headers: &HeaderMap) -> Option<String> {
    if let Some(engine) = query.engine.as_deref()
        && !engine.trim().is_empty()
    {
        return Some(normalize_dialect(engine));
    }
    let agent = headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())?
        .to_ascii_lowercase();
    dialect_from_user_agent(&agent)
}

/// Maps a lowercased `User-Agent` to a `SQLGlot` dialect, for the engines whose
/// clients identify themselves. Returns `None` for anything not recognized —
/// Meridian never fabricates a dialect binding.
fn dialect_from_user_agent(agent: &str) -> Option<String> {
    // Ordered by specificity; the first contained marker wins.
    const MARKERS: &[(&str, &str)] = &[
        ("trino", "trino"),
        ("presto", "presto"),
        ("snowflake", "snowflake"),
        ("duckdb", "duckdb"),
        ("clickhouse", "clickhouse"),
        ("starrocks", "starrocks"),
        ("bigquery", "bigquery"),
        ("spark", "spark"),
        ("pyspark", "spark"),
        ("dremio", "dremio"),
        ("postgres", "postgres"),
        // PyIceberg reads views but does not execute SQL; treat it as DuckDB,
        // its default local query engine, so a translation is still offered.
        ("pyiceberg", "duckdb"),
    ];
    MARKERS
        .iter()
        .find(|(marker, _)| agent.contains(marker))
        .map(|(_, dialect)| (*dialect).to_owned())
}

/// Lowercases and trims a dialect string (the cache and the `SQLGlot` dialect
/// namespace are case-insensitive).
fn normalize_dialect(dialect: &str) -> String {
    dialect.trim().to_ascii_lowercase()
}

/// The canonical SQL representation of a view's current version: the one the
/// author wrote (or the first SQL representation present). Translations are
/// derived from this. Returns `None` for a view whose current version carries
/// no SQL representation (nothing to translate).
fn canonical_representation(metadata: &ViewMetadata) -> Option<&SqlViewRepresentation> {
    metadata
        .current_version()?
        .representations
        .iter()
        .find_map(ViewRepresentation::as_sql)
}

/// sha256 (hex) of a canonical SQL string — the cache key component that ties a
/// cached translation to a specific view definition.
fn sql_hash(sql: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sql.as_bytes());
    hex::encode(hasher.finalize())
}

/// True when the current version already carries a SQL representation for
/// `dialect` (case-insensitively) — in which case no translation is needed.
fn has_representation_for(metadata: &ViewMetadata, dialect: &str) -> bool {
    metadata.current_version().is_some_and(|version| {
        version.representations.iter().any(|r| {
            r.as_sql()
                .is_some_and(|sql| normalize_dialect(&sql.dialect) == dialect)
        })
    })
}

/// The outcome of resolving a requesting dialect against a view: what to serve
/// and the honest status to report.
struct TranspileOutcome {
    /// A representation to fold into the served metadata's current version, if
    /// one was produced (present for `verified`/`best_effort`; absent for
    /// unsupported or when serving the canonical form on a sidecar outage).
    representation: Option<SqlViewRepresentation>,
    /// The honest status label to surface in the response's transpile note.
    status: &'static str,
    /// A human-readable note (diagnostics summary, or the degradation reason).
    note: String,
    /// The target dialect this outcome is about.
    dialect: String,
}

/// Resolves a translation of `metadata` into `target_dialect`: serves an
/// existing representation when present, else transpiles the canonical one via
/// the sidecar, caching the result. Never errors on a sidecar outage — it
/// degrades to serving the canonical form with an honest note.
///
/// The write to the durable cache is best-effort: a cache-write failure is
/// logged and never affects what is served.
async fn resolve_transpilation(
    state: &AppState,
    sidecar: Option<&SidecarClient>,
    view_id: &str,
    metadata: &ViewMetadata,
    target_dialect: &str,
) -> TranspileOutcome {
    // Already present in the metadata (authored or previously folded): serve it.
    if has_representation_for(metadata, target_dialect) {
        return TranspileOutcome {
            representation: None,
            status: "verified",
            note: format!("view already carries a {target_dialect} representation"),
            dialect: target_dialect.to_owned(),
        };
    }

    let Some(canonical) = canonical_representation(metadata) else {
        return TranspileOutcome {
            representation: None,
            status: "unsupported",
            note: "view has no SQL representation to translate".to_owned(),
            dialect: target_dialect.to_owned(),
        };
    };
    let source_dialect = normalize_dialect(&canonical.dialect);
    // A view authored in the requested dialect needs no translation (defensive:
    // has_representation_for already covered the exact-match case).
    if source_dialect == target_dialect {
        return TranspileOutcome {
            representation: None,
            status: "verified",
            note: format!("view is authored in {target_dialect}"),
            dialect: target_dialect.to_owned(),
        };
    }
    let hash = sql_hash(&canonical.sql);

    // Cache hit? Serve it (an unsupported entry short-circuits the sidecar).
    match semantics::get_cached_translation(&state.pool, view_id, target_dialect, &hash).await {
        Ok(Some(cached)) => return outcome_from_cache(&cached, target_dialect),
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(%error, view = %view_id, "view translation cache read failed; \
                transpiling fresh");
        }
    }

    // Cache miss: transpile via the sidecar. A sidecar outage degrades to the
    // canonical form with a note — never a 500.
    let Some(sidecar) = sidecar else {
        return TranspileOutcome {
            representation: None,
            status: "unsupported",
            note: "transpilation sidecar is not configured; serving the canonical representation"
                .to_owned(),
            dialect: target_dialect.to_owned(),
        };
    };

    match sidecar
        .transpile(&canonical.sql, &source_dialect, target_dialect)
        .await
    {
        Ok(response) => {
            let status = response.status.as_str();
            let note = summarize_diagnostics(&response.diagnostics)
                .unwrap_or_else(|| format!("translated {source_dialect} -> {target_dialect}"));
            let diagnostics_json: Vec<Value> = response
                .diagnostics
                .iter()
                .map(|d| json!({ "severity": d.severity, "code": d.code, "message": d.message }))
                .collect();

            // Persist to the durable cache (best-effort: never blocks serving).
            if let Err(error) = semantics::upsert_cached_translation(
                &state.pool,
                tenancy::default_workspace_id(),
                view_id,
                target_dialect,
                &source_dialect,
                &hash,
                response.sql.as_deref(),
                status,
                &diagnostics_json,
            )
            .await
            {
                tracing::warn!(%error, view = %view_id, "failed to persist view translation cache");
            }

            let representation = response
                .sql
                .filter(|_| response.status != TranspileStatus::Unsupported)
                .map(|sql| synthesized_representation(&sql, target_dialect, status));

            TranspileOutcome {
                representation,
                status,
                note,
                dialect: target_dialect.to_owned(),
            }
        }
        Err(error) => {
            // Transport failure: degrade gracefully.
            tracing::warn!(%error, view = %view_id, "sidecar transpile failed; \
                serving canonical representation");
            TranspileOutcome {
                representation: None,
                status: "unsupported",
                note: format!(
                    "transpilation unavailable ({error}); serving the canonical representation"
                ),
                dialect: target_dialect.to_owned(),
            }
        }
    }
}

/// Builds a [`TranspileOutcome`] from a cache row.
fn outcome_from_cache(
    cached: &semantics::ViewRepresentationCacheRecord,
    target_dialect: &str,
) -> TranspileOutcome {
    let status: &'static str = match cached.status.as_str() {
        "verified" => "verified",
        "best_effort" => "best_effort",
        _ => "unsupported",
    };
    let representation = cached
        .translated_sql
        .as_deref()
        .filter(|_| status != "unsupported")
        .map(|sql| synthesized_representation(sql, target_dialect, status));
    TranspileOutcome {
        representation,
        status,
        note: format!("served from translation cache ({status})"),
        dialect: target_dialect.to_owned(),
    }
}

/// Builds a dialect-tagged SQL representation carrying Meridian's transpile
/// status in its `extra` map (so the multi-representation form is honest and
/// self-describing, even to a non-Meridian reader).
fn synthesized_representation(sql: &str, dialect: &str, status: &str) -> SqlViewRepresentation {
    let mut representation = SqlViewRepresentation::new(sql.to_owned(), dialect.to_owned());
    representation.extra.insert(
        TRANSPILE_STATUS_KEY.to_owned(),
        Value::String(status.to_owned()),
    );
    representation.extra.insert(
        TRANSPILE_ORIGIN_KEY.to_owned(),
        Value::String("meridian-transpile".to_owned()),
    );
    representation
}

/// A one-line summary of the highest-severity diagnostic, if any.
fn summarize_diagnostics(diagnostics: &[crate::sidecar::Diagnostic]) -> Option<String> {
    diagnostics
        .iter()
        .find(|d| d.severity == "error")
        .or_else(|| diagnostics.iter().find(|d| d.severity == "warning"))
        .or_else(|| diagnostics.first())
        .map(|d| d.message.clone())
}

/// Folds a synthesized representation into a clone of `metadata`'s current
/// version so the served body carries the requesting dialect. The clone leaves
/// the stored metadata untouched (the durable cache is the persistence path);
/// this is purely the response projection.
fn with_folded_representation(
    metadata: &ViewMetadata,
    representation: &SqlViewRepresentation,
) -> ViewMetadata {
    let mut clone = metadata.clone();
    let current_id = clone.current_version_id;
    if let Some(version) = clone
        .versions
        .iter_mut()
        .find(|v| v.version_id == current_id)
    {
        version
            .representations
            .push(ViewRepresentation::Sql(representation.clone()));
    }
    clone
}

/// Maps view-builder rejections to the IRC error surface. Every variant is
/// a validation failure (identity conflicts are caught by the
/// `assert-view-uuid` requirement, checked before updates are applied).
fn map_view_build_error(error: &ViewMetadataBuildError) -> ApiError {
    ApiError::bad_request(error.to_string())
}

/// Maps storage failures on the view path onto client-facing errors
/// (the view-side twin of the table module's mapping).
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

/// The pointer references a file that cannot be read back — catalog-side
/// corruption, surfaced loudly (never a client mistake).
fn current_metadata_unreadable(location: &str, error: &StorageError) -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalServerError",
        format!("current view metadata at {location:?} is unreadable: {error}"),
    )
}

/// Best-effort delete of a staged file that will never be published.
/// Failures are logged, never surfaced.
async fn discard_staged(storage: &Arc<dyn Storage>, location: &str) {
    if let Err(error) = storage.delete(location).await {
        tracing::warn!(%location, %error, "failed to delete orphaned staged view metadata file");
    }
}

// ---------------------------------------------------------------------------
// Wire shapes
// ---------------------------------------------------------------------------

/// `TableIdentifier` from the IRC spec (views reuse it verbatim: the spec
/// has no separate `ViewIdentifier` schema).
#[derive(Debug, Clone, Deserialize)]
pub struct ViewIdentifier {
    /// Namespace levels.
    pub namespace: Vec<String>,
    /// View name.
    pub name: String,
}

/// Query parameters for `GET .../views`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListViewsQuery {
    /// Opaque continuation token from a previous response.
    pub page_token: Option<String>,
    /// Upper bound on the number of results.
    pub page_size: Option<i64>,
}

/// `CreateViewRequest` from the IRC spec.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateViewRequest {
    /// View name.
    pub name: String,
    /// Explicit view location; server-assigned when absent.
    pub location: Option<String>,
    /// The initial schema.
    pub schema: Schema,
    /// The initial view version. Its `schema-id` is replaced with the id
    /// assigned to `schema`, per the spec.
    pub view_version: ViewVersion,
    /// Initial view properties.
    #[serde(default)]
    pub properties: BTreeMap<String, String>,
}

/// `RenameTableRequest` from the IRC spec (`renameView` reuses the shape).
#[derive(Debug, Deserialize)]
pub struct RenameViewRequest {
    /// The existing view.
    pub source: ViewIdentifier,
    /// The identifier to rename it to.
    pub destination: ViewIdentifier,
}

/// One view's parsed commit request (`CommitViewRequest`).
struct ParsedViewCommit {
    identifier: Option<ViewIdentifier>,
    requirements: Vec<ViewRequirement>,
    updates: Vec<ViewUpdate>,
}

/// Parses a `CommitViewRequest` from raw JSON. Unknown update/requirement
/// `action`/`type` strings are 400s, exactly like table commits ("server
/// implementations are required to fail with a 400 status code if any
/// unknown updates or requirements are received"). `requirements` is
/// optional in the view schema (only `updates` is required).
fn parse_view_commit_request(value: &Value) -> Result<ParsedViewCommit, ApiError> {
    let object = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request("view commit request must be a JSON object"))?;

    let identifier = match object.get("identifier") {
        None | Some(Value::Null) => None,
        Some(raw) => Some(
            serde_json::from_value::<ViewIdentifier>(raw.clone())
                .map_err(|e| ApiError::bad_request(format!("invalid view identifier: {e}")))?,
        ),
    };

    let requirements = match object.get("requirements") {
        None | Some(Value::Null) => Vec::new(),
        Some(raw) => raw
            .as_array()
            .ok_or_else(|| ApiError::bad_request("'requirements' must be an array"))?
            .iter()
            .map(|raw| {
                serde_json::from_value::<ViewRequirement>(raw.clone()).map_err(|e| {
                    ApiError::bad_request(format!("unknown or invalid view requirement: {e}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    };

    let updates = object
        .get("updates")
        .ok_or_else(|| ApiError::bad_request("view commit request is missing 'updates'"))?
        .as_array()
        .ok_or_else(|| ApiError::bad_request("'updates' must be an array"))?
        .iter()
        .map(|raw| {
            serde_json::from_value::<ViewUpdate>(raw.clone())
                .map_err(|e| ApiError::bad_request(format!("unknown or invalid view update: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ParsedViewCommit {
        identifier,
        requirements,
        updates,
    })
}

/// Reassigns fresh, 1-based field ids to every `add-schema` update's schema,
/// treating replace-request field ids as provisional exactly as `createView`
/// does (see [`build_new_view_metadata`]).
///
/// Spark 3.5's `CREATE OR REPLACE VIEW` numbers the replacement view's output
/// schema from 0, identically to its `CREATE VIEW`; the strict view-metadata
/// builder would otherwise reject the commit with `field id 0 is not positive`.
/// View schemas carry no cross-version field-id continuity (no data files
/// reference them), so reassignment is safe and keeps create and replace
/// consistent — the same statement yields the same server-assigned ids whether
/// or not the view already existed. The deprecated `last-column-id` is
/// provisional too, and dropped. Genuinely broken schemas (duplicate sibling
/// names, unresolvable `identifier-field-ids`) still fail as 400s. The builder
/// keeps its strict validation for updates the server constructs itself.
fn assign_fresh_view_schema_ids(updates: &[ViewUpdate]) -> Result<Vec<ViewUpdate>, ApiError> {
    updates
        .iter()
        .map(|update| match update {
            ViewUpdate::AddSchema { schema, .. } => {
                let fresh = meridian_iceberg::spec::assign_fresh_ids(schema, None, None)
                    .map_err(|e| ApiError::bad_request(e.to_string()))?;
                Ok(ViewUpdate::AddSchema {
                    schema: fresh.schema,
                    last_column_id: None,
                })
            }
            other => Ok(other.clone()),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// GET /{prefix}/namespaces/{namespace}/views — list
// ---------------------------------------------------------------------------

/// `GET /{prefix}/namespaces/{namespace}/views` — list view identifiers
/// (the response reuses the `ListTablesResponse` shape, per the spec).
pub async fn list_views(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace)): Path<(String, String)>,
    Query(query): Query<ListViewsQuery>,
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
    let mut rows = view::list(
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
// POST /{prefix}/namespaces/{namespace}/views — create
// ---------------------------------------------------------------------------

/// Builds the initial metadata for a new view from a create request.
fn build_new_view_metadata(
    request: &CreateViewRequest,
    storage_root: &str,
    levels: &[String],
) -> Result<ViewMetadata, ApiError> {
    let view_uuid = Uuid::new_v4();
    let location = match &request.location {
        Some(location) if !location.trim().is_empty() => location.trim_end_matches('/').to_owned(),
        _ => default_view_location(storage_root, levels, &request.name, view_uuid),
    };

    // The request's view-version references the schema it travels with: its
    // schema-id is replaced with the id the builder assigns (per the spec's
    // CreateViewRequest description).
    let mut view_version = request.view_version.clone();
    view_version.schema_id = LAST_ADDED;

    // Field ids in a create request are provisional, exactly as on
    // `createTable`: Spark's `CREATE VIEW` numbers the output schema from 0,
    // pyiceberg from 1. Assign fresh server-side ids (view schemas have no
    // partition spec or sort order to remap).
    let fresh = meridian_iceberg::spec::assign_fresh_ids(&request.schema, None, None)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    let mut builder =
        ViewMetadataBuilder::new_view(location).map_err(|e| map_view_build_error(&e))?;
    let mut updates: Vec<ViewUpdate> = vec![
        ViewUpdate::AssignUuid { uuid: view_uuid },
        ViewUpdate::AddSchema {
            schema: fresh.schema,
            last_column_id: None,
        },
        ViewUpdate::AddViewVersion { view_version },
        ViewUpdate::SetCurrentViewVersion {
            view_version_id: LAST_ADDED,
        },
    ];
    if !request.properties.is_empty() {
        updates.push(ViewUpdate::SetProperties {
            updates: request.properties.clone(),
        });
    }
    builder
        .apply_all(updates)
        .map_err(|e| map_view_build_error(&e))?;
    builder
        .build(Utc::now().timestamp_millis())
        .map_err(|e| map_view_build_error(&e))
}

/// `POST /{prefix}/namespaces/{namespace}/views` — create a view.
pub async fn create_view(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace)): Path<(String, String)>,
    Json(request): Json<CreateViewRequest>,
) -> Result<Response, ApiError> {
    let ctx = resolve_namespace(&state, &prefix, &raw_namespace).await?;
    let chain = namespace_scope_chain(&state.pool, &ctx.warehouse.id, &ctx.levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::CreateView,
        &SecurableScope::namespace(&ctx.warehouse.id, chain),
    )
    .await?;
    validate_view_name(&request.name)?;

    // Fast-path collision checks for exact 409 messages; the store's create
    // transaction re-checks both under its insert.
    if view::get(&state.pool, &ctx.namespace.id, &request.name)
        .await?
        .is_some()
    {
        return Err(ApiError::already_exists(format!(
            "view {:?} already exists",
            display_ident(&ctx.levels, &request.name)
        )));
    }
    if table::get(&state.pool, &ctx.namespace.id, &request.name)
        .await?
        .is_some()
    {
        return Err(ApiError::already_exists(format!(
            "the identifier {:?} already exists as a table \
             (tables and views share a namespace)",
            display_ident(&ctx.levels, &request.name)
        )));
    }

    let metadata = build_new_view_metadata(&request, &ctx.warehouse.storage_root, &ctx.levels)?;

    // Write the initial file, then insert the pointer row (invariant I4:
    // the pointer never references a file that is not durably written).
    let storage = connect_storage(&ctx.warehouse)?;
    let metadata_location = new_view_metadata_location(&metadata.location, 0, Uuid::new_v4());
    meridian_storage::write_view_metadata(storage.as_ref(), &metadata_location, &metadata)
        .await
        .map_err(|e| storage_to_api(&e))?;

    let created = view::create(
        &state.pool,
        view::NewView {
            workspace_id: tenancy::default_workspace_id(),
            namespace_id: &ctx.namespace.id,
            namespace_levels: &ctx.levels,
            name: &request.name,
            view_uuid: &metadata.view_uuid.to_string(),
            metadata_location: &metadata_location,
            properties: &metadata.properties.clone().unwrap_or_default(),
        },
        &principal.audit_string(),
    )
    .await;

    if let Err(error) = created {
        // The row insert failed, so the file just written is an orphan.
        discard_staged(&storage, &metadata_location).await;
        return Err(match error {
            MeridianError::Conflict(message) => ApiError::already_exists(message),
            MeridianError::NotFound(message) => ApiError::no_such_namespace(message),
            other => ApiError::from(other),
        });
    }

    let body = load_view_result(&ctx.warehouse, &metadata_location, &metadata)?;
    Ok((StatusCode::OK, Json(body)).into_response())
}

// ---------------------------------------------------------------------------
// GET / HEAD / DELETE /{prefix}/namespaces/{namespace}/views/{view}
// ---------------------------------------------------------------------------

/// Resolves a view for a view-scoped endpoint (missing namespace and
/// missing view are both `NoSuchViewException`, mirroring the table
/// surface's loadTable semantics).
async fn resolve_view(
    state: &AppState,
    prefix: &str,
    raw_namespace: &str,
    name: &str,
) -> Result<(WarehouseRecord, Vec<String>, view::ViewRecord), ApiError> {
    let warehouse = resolve_warehouse(&state.pool, prefix).await?;
    let levels = decode_namespace_param(raw_namespace)?;
    let record = view::get_by_name(&state.pool, &warehouse.id, &levels, name)
        .await?
        .ok_or_else(|| no_such_view(&levels, name))?;
    Ok((warehouse, levels, record))
}

/// `GET /{prefix}/namespaces/{namespace}/views/{view}` — load a view.
///
/// **Universal views (G-F1):** when the requesting engine's dialect is
/// resolvable (via `?engine=` or the `User-Agent`) and the view does not already
/// carry a representation for it, Meridian transpiles the canonical
/// representation to that dialect via the sidecar, caches it, folds it into the
/// served `metadata` (dialect-tagged, carrying a `meridian.transpile-status`),
/// and reports the honest status under a `meridian-transpile` field of the
/// response. A sidecar outage degrades gracefully: the canonical representation
/// is served with a status note, never a 500. Requests that name no engine get
/// the view exactly as authored.
pub async fn load_view(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Extension(sidecar): Extension<Option<SidecarClient>>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
    Query(query): Query<LoadViewQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let (warehouse, levels, record) = resolve_view(&state, &prefix, &raw_namespace, &name).await?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::Read,
        &SecurableScope::view(&warehouse.id, chain, Some(&record.id)),
    )
    .await?;

    let Some(metadata_location) = record.metadata_location.clone() else {
        return Err(no_such_view(&levels, &name));
    };
    let storage = connect_storage(&warehouse)?;
    let metadata = read_view_metadata(storage.as_ref(), &metadata_location)
        .await
        .map_err(|e| current_metadata_unreadable(&metadata_location, &e))?;

    // Universal-view transpilation: serve the requesting engine's dialect.
    let transpile_note = match resolve_requesting_dialect(&query, &headers) {
        Some(target_dialect) => {
            let outcome = resolve_transpilation(
                &state,
                sidecar.as_ref(),
                &record.id,
                &metadata,
                &target_dialect,
            )
            .await;
            let served = match &outcome.representation {
                Some(representation) => with_folded_representation(&metadata, representation),
                None => metadata.clone(),
            };
            let note = json!({
                "requested_dialect": outcome.dialect,
                "status": outcome.status,
                "note": outcome.note,
            });
            (served, Some(note))
        }
        None => (metadata, None),
    };
    let (served_metadata, note) = transpile_note;

    let mut body = load_view_result(&warehouse, &metadata_location, &served_metadata)?;
    if let (Value::Object(map), Some(note)) = (&mut body, note) {
        map.insert("meridian-transpile".to_owned(), note);
    }
    Ok((StatusCode::OK, Json(body)).into_response())
}

/// `HEAD /{prefix}/namespaces/{namespace}/views/{view}` — existence check.
pub async fn view_exists(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
) -> Result<StatusCode, ApiError> {
    let (warehouse, levels, record) = resolve_view(&state, &prefix, &raw_namespace, &name).await?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    require(
        &state.pool,
        &principal,
        Privilege::Read,
        &SecurableScope::view(&warehouse.id, chain, Some(&record.id)),
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Requires `privilege` on a view that may not exist: the view id joins
/// the scope when it does, so a denied caller learns nothing about
/// existence (namespace/warehouse grants still decide), and the store
/// still 404s a missing view for authorized callers.
async fn require_on_view(
    state: &AppState,
    principal: &Principal,
    warehouse_id: &str,
    levels: &[String],
    name: &str,
    privilege: Privilege,
) -> Result<(), ApiError> {
    let view_id = view::get_by_name(&state.pool, warehouse_id, levels, name)
        .await?
        .map(|r| r.id);
    let chain = namespace_scope_chain(&state.pool, warehouse_id, levels).await?;
    require(
        &state.pool,
        principal,
        privilege,
        &SecurableScope::view(warehouse_id, chain, view_id.as_deref()),
    )
    .await
}

/// `DELETE /{prefix}/namespaces/{namespace}/views/{view}` — drop a view.
/// Metadata files are never deleted here (see module docs).
pub async fn drop_view(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
) -> Result<StatusCode, ApiError> {
    let warehouse = resolve_warehouse(&state.pool, &prefix).await?;
    let levels = decode_namespace_param(&raw_namespace)?;
    require_on_view(
        &state,
        &principal,
        &warehouse.id,
        &levels,
        &name,
        Privilege::Drop,
    )
    .await?;

    view::drop_view(
        &state.pool,
        tenancy::default_workspace_id(),
        &warehouse.id,
        &levels,
        &name,
        &principal.audit_string(),
    )
    .await
    .map_err(|e| match e {
        MeridianError::NotFound(_) => no_such_view(&levels, &name),
        other => ApiError::from(other),
    })?;

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// POST /{prefix}/namespaces/{namespace}/views/{view} — replace (commit)
// ---------------------------------------------------------------------------

/// `POST /{prefix}/namespaces/{namespace}/views/{view}` — commit updates to
/// a view (`CommitViewRequest` → `LoadViewResult`).
///
/// The §6 state machine, single-pointer edition: load base → check
/// requirements → build → stage → guarded swap, with a bounded
/// rebase-retry on a lost compare-and-set. Unlike tables there is no
/// `assert-create` path: a replace against a missing view is a plain 404.
#[allow(clippy::too_many_lines)] // the §6 loop reads better unsplit, like commit_table
pub async fn replace_view(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let warehouse = resolve_warehouse(&state.pool, &prefix).await?;
    let levels = decode_namespace_param(&raw_namespace)?;
    validate_view_name(&name)?;

    let mut parsed = parse_view_commit_request(&body)?;
    if let Some(identifier) = &parsed.identifier
        && (identifier.namespace != levels || identifier.name != name)
    {
        return Err(ApiError::bad_request(
            "body identifier does not match the request path",
        ));
    }

    // Authorize before any resource mutation (and before this loop stages
    // anything): COMMIT on the view. (View endpoints have no idempotency
    // replay yet; when it lands, this check must stay ahead of the recall,
    // like the table commit path.)
    require_on_view(
        &state,
        &principal,
        &warehouse.id,
        &levels,
        &name,
        Privilege::Commit,
    )
    .await?;

    // Request field ids are provisional on replace just as on create: Spark
    // 3.5's `CREATE OR REPLACE VIEW` numbers the output schema from 0. Assign
    // fresh server-side ids once (identical across rebase-retry attempts),
    // mirroring `createView`.
    parsed.updates = assign_fresh_view_schema_ids(&parsed.updates)?;

    let storage = connect_storage(&warehouse)?;

    for _attempt in 1..=MAX_COMMIT_ATTEMPTS {
        // Fresh base per attempt: a lost race retries with requirements
        // re-checked against the new state.
        let record = view::get_by_name(&state.pool, &warehouse.id, &levels, &name)
            .await?
            .ok_or_else(|| no_such_view(&levels, &name))?;
        let Some(base_location) = record.metadata_location.clone() else {
            return Err(no_such_view(&levels, &name));
        };
        let base = read_view_metadata(storage.as_ref(), &base_location)
            .await
            .map_err(|e| current_metadata_unreadable(&base_location, &e))?;

        let violations: Vec<String> = parsed
            .requirements
            .iter()
            .filter_map(|requirement| {
                requirement
                    .check(Some(&base))
                    .err()
                    .map(|violation| format!("{name}: {violation}"))
            })
            .collect();
        if !violations.is_empty() {
            return Err(ApiError::commit_failed(violations.join("; ")));
        }

        let mut builder = base.builder_from();
        builder
            .apply_all(parsed.updates.iter().cloned())
            .map_err(|e| map_view_build_error(&e))?;
        let candidate = builder
            .build(Utc::now().timestamp_millis())
            .map_err(|e| map_view_build_error(&e))?;

        // Stage under the *next* pointer version with a fresh uuid: unique
        // per attempt, so no attempt can ever overwrite a published file.
        let expected_version = record.pointer_version;
        let staged_location = new_view_metadata_location(
            &candidate.location,
            u64::try_from(expected_version + 1).unwrap_or(u64::MAX),
            Uuid::new_v4(),
        );
        meridian_storage::write_view_metadata(storage.as_ref(), &staged_location, &candidate)
            .await
            .map_err(|e| storage_to_api(&e))?;

        let properties = candidate.properties.clone().unwrap_or_default();
        let swap = ViewPointerSwap {
            view_id: &record.id,
            expected_version,
            new_metadata_location: &staged_location,
            properties: &properties,
            event_details: json!({
                "view_uuid": candidate.view_uuid,
                "current_version_id": candidate.current_version_id,
                "version_count": candidate.versions.len(),
            }),
        };
        match view::commit_replace(
            &state.pool,
            tenancy::default_workspace_id(),
            swap,
            &principal.audit_string(),
        )
        .await
        {
            Ok(_) => {
                let body = load_view_result(&warehouse, &staged_location, &candidate)?;
                return Ok((StatusCode::OK, Json(body)).into_response());
            }
            Err(ViewCommitError::VersionConflict { .. }) => {
                // Lost the race (F6): the staged file is an orphan; refresh
                // and retry.
                discard_staged(&storage, &staged_location).await;
            }
            Err(ViewCommitError::NotFound) => {
                discard_staged(&storage, &staged_location).await;
                return Err(no_such_view(&levels, &name));
            }
            Err(ViewCommitError::StateUnknown { message }) => {
                // Point-of-no-return failure (F3): the staged file must NOT
                // be deleted — the commit may have applied and published it.
                tracing::error!(%message, view = %record.id, "view replace state unknown");
                return Err(ApiError::commit_state_unknown(
                    "the view replace outcome could not be determined; refresh and retry",
                ));
            }
            Err(ViewCommitError::Store(other)) => {
                discard_staged(&storage, &staged_location).await;
                return Err(ApiError::from(other));
            }
        }
    }
    Err(ApiError::commit_failed(format!(
        "view replace lost the compare-and-set race {MAX_COMMIT_ATTEMPTS} time(s); \
         refresh view state and retry"
    )))
}

// ---------------------------------------------------------------------------
// POST /{prefix}/views/rename
// ---------------------------------------------------------------------------

/// `POST /{prefix}/views/rename` — rename or move a view within the
/// warehouse.
pub async fn rename_view(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(prefix): Path<String>,
    Json(request): Json<RenameViewRequest>,
) -> Result<StatusCode, ApiError> {
    let warehouse = resolve_warehouse(&state.pool, &prefix).await?;
    validate_view_name(&request.destination.name)?;
    if request.source.namespace.is_empty() || request.destination.namespace.is_empty() {
        return Err(ApiError::bad_request(
            "source and destination identifiers must include a namespace",
        ));
    }

    // WRITE on the source view, CREATE_VIEW where it lands.
    require_on_view(
        &state,
        &principal,
        &warehouse.id,
        &request.source.namespace,
        &request.source.name,
        Privilege::Write,
    )
    .await?;
    let dest_chain =
        namespace_scope_chain(&state.pool, &warehouse.id, &request.destination.namespace).await?;
    require(
        &state.pool,
        &principal,
        Privilege::CreateView,
        &SecurableScope::namespace(&warehouse.id, dest_chain),
    )
    .await?;

    view::rename(
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
        MeridianError::NotFound(_) => no_such_view(&request.source.namespace, &request.source.name),
        MeridianError::Conflict(message) => ApiError::already_exists(message),
        other => ApiError::from(other),
    })?;

    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// POST /api/v2/sql/transpile — standalone transpile passthrough (G-F1)
// ---------------------------------------------------------------------------

/// Request body for `POST /api/v2/sql/transpile`.
#[derive(Debug, Deserialize)]
pub struct TranspileApiRequest {
    /// The statement to translate.
    pub sql: String,
    /// Source SQL dialect.
    pub from_dialect: String,
    /// Target SQL dialect.
    pub to_dialect: String,
}

/// `POST /api/v2/sql/transpile` — a standalone dialect-translation endpoint
/// (G-F1). Useful for migrations and as a quiet demo magnet: paste SQL, name the
/// two dialects, get back the translation with its honest `verified` /
/// `best_effort` / `unsupported` status and diagnostics. Deterministic `SQLGlot`
/// first via the sidecar; the optional LLM-assist fallback (if configured on the
/// sidecar) is labelled and validated, never trusted blindly.
///
/// Management-gated: transpilation touches no catalog data, but it is an
/// operator tool, not an anonymous surface. A sidecar outage is a
/// `503 ServiceUnavailableException` (the caller can retry), never a 500.
pub async fn transpile_sql(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Extension(sidecar): Extension<Option<SidecarClient>>,
    Json(request): Json<TranspileApiRequest>,
) -> Result<Json<Value>, ApiError> {
    require_management(&state.pool, &principal).await?;

    if request.sql.trim().is_empty() {
        return Err(ApiError::bad_request("sql must not be empty"));
    }
    let from_dialect = normalize_dialect(&request.from_dialect);
    let to_dialect = normalize_dialect(&request.to_dialect);
    if from_dialect.is_empty() || to_dialect.is_empty() {
        return Err(ApiError::bad_request(
            "from_dialect and to_dialect must not be empty",
        ));
    }

    let Some(sidecar) = sidecar else {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailableException",
            "the transpilation sidecar is not configured",
        ));
    };

    match sidecar
        .transpile(&request.sql, &from_dialect, &to_dialect)
        .await
    {
        Ok(response) => {
            let diagnostics: Vec<Value> = response
                .diagnostics
                .iter()
                .map(|d| json!({ "severity": d.severity, "code": d.code, "message": d.message }))
                .collect();
            Ok(Json(json!({
                "sql": response.sql,
                "status": response.status.as_str(),
                "from_dialect": response.from_dialect,
                "to_dialect": response.to_dialect,
                "diagnostics": diagnostics,
            })))
        }
        Err(error) => Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailableException",
            format!("transpilation sidecar is unavailable: {error}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::types::Json as SqlxJson;

    fn warehouse_with_options(pairs: &[(&str, &str)]) -> WarehouseRecord {
        WarehouseRecord {
            id: "01TEST".to_owned(),
            workspace_id: "01WS".to_owned(),
            name: "wh".to_owned(),
            storage_root: "s3://bucket/prefix".to_owned(),
            storage_config: SqlxJson(
                pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
            ),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn storage_config_maps_non_secret_options_to_client_properties() {
        let warehouse = warehouse_with_options(&[
            ("endpoint", "http://localhost:9000"),
            ("region", "us-east-1"),
            ("path-style", "true"),
            ("retry.max-retries", "5"),
        ]);
        let config = storage_client_config(&warehouse);
        assert_eq!(
            config.get("s3.endpoint").map(String::as_str),
            Some("http://localhost:9000")
        );
        assert_eq!(
            config.get("client.region").map(String::as_str),
            Some("us-east-1")
        );
        assert_eq!(
            config.get("s3.region").map(String::as_str),
            Some("us-east-1")
        );
        assert_eq!(
            config.get("s3.path-style-access").map(String::as_str),
            Some("true")
        );
        // Server-side options have no client property.
        assert!(!config.keys().any(|k| k.contains("retry")));
    }

    #[test]
    fn storage_config_never_forwards_credentials() {
        let warehouse = warehouse_with_options(&[
            ("endpoint", "http://localhost:9000"),
            ("access-key-id", "AKIA_TEST_KEY_ID"),
            ("secret-access-key", "TEST_SECRET_VALUE"),
            ("session-token", "TEST_SESSION_TOKEN"),
        ]);
        let config = storage_client_config(&warehouse);
        let rendered = serde_json::to_string(&config).expect("serializable");
        for secret in [
            "AKIA_TEST_KEY_ID",
            "TEST_SECRET_VALUE",
            "TEST_SESSION_TOKEN",
        ] {
            assert!(
                !rendered.contains(secret),
                "credential value {secret:?} leaked into client config: {rendered}"
            );
        }
        assert_eq!(config.len(), 1, "only the endpoint maps: {rendered}");
    }

    #[test]
    fn default_view_location_is_uuid_suffixed_under_namespace_path() {
        let uuid = Uuid::nil();
        assert_eq!(
            default_view_location(
                "s3://bucket/root/",
                &["a".to_owned(), "b".to_owned()],
                "v",
                uuid
            ),
            format!("s3://bucket/root/a/b/v-{uuid}")
        );
        assert_eq!(
            default_view_location("s3://bucket/root", &[], "v", uuid),
            format!("s3://bucket/root/v-{uuid}")
        );
    }

    #[test]
    fn view_commit_request_parsing_rejects_unknown_vocabulary() {
        let bad_update = json!({
            "updates": [{ "action": "definitely-not-an-action" }],
        });
        assert!(parse_view_commit_request(&bad_update).is_err());

        let bad_requirement = json!({
            "requirements": [{ "type": "assert-nonsense" }],
            "updates": [],
        });
        assert!(parse_view_commit_request(&bad_requirement).is_err());

        // Requirements are optional in the view commit schema.
        let ok = json!({
            "identifier": { "namespace": ["a"], "name": "v" },
            "updates": [
                { "action": "set-properties", "updates": { "comment": "hi" } },
            ],
        });
        let parsed = parse_view_commit_request(&ok).expect("valid request parses");
        assert!(parsed.requirements.is_empty());
        assert_eq!(parsed.updates.len(), 1);
    }
}
