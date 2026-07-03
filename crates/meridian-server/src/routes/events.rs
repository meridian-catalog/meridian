//! Events management API: webhooks, the queryable event feed, and named
//! durable consumers. All endpoints live under `/api/v2`.
//!
//! # Authorization
//!
//! Every endpoint here requires **management access** (the `admin` role or
//! any `MANAGE_WAREHOUSE` grant), like the rest of the management API. The
//! feed spans events about *every* resource in the workspace, so a
//! resource-scoped privilege from the existing set cannot express
//! "may read events" without over- or under-granting; rather than mint a
//! new privilege prematurely, reading events is management-level for now.
//! Revisit if a finer-grained `READ_EVENTS` privilege earns its keep.
//!
//! # Cursors
//!
//! A feed cursor is an event id (a ULID): `after=<cursor>` returns events
//! strictly newer than it, in id order. `after=latest` starts at the
//! current end of the feed. Consumers persist the same kind of cursor via
//! `POST .../commit`; reads via `.../next` do not advance it, so consumer
//! processing is at-least-once.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_store::webhook::{DeliveryRecord, WebhookEndpointRecord};
use meridian_store::{consumer, outbox, tenancy, webhook};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::AppState;
use crate::error::ApiError;
use crate::events::{EVENT_TYPE_PREFIX, cloud_event};
use crate::routes::grants::require_management;

/// Longest accepted consumer name.
const MAX_CONSUMER_NAME_LEN: usize = 100;

/// Shortest accepted webhook secret. HMAC-SHA256 keys shorter than the
/// hash output weaken the MAC; 16 bytes is the pragmatic floor.
const MIN_SECRET_LEN: usize = 16;

/// Default and maximum page sizes for feed reads.
const DEFAULT_PAGE_SIZE: i64 = 100;
const MAX_PAGE_SIZE: i64 = 1000;

/// Maps store-layer errors onto the management API envelope.
fn store_error(error: MeridianError) -> ApiError {
    match error {
        MeridianError::NotFound(message) => {
            ApiError::new(StatusCode::NOT_FOUND, "NotFoundException", message)
        }
        MeridianError::Conflict(message) => ApiError::already_exists(message),
        MeridianError::Validation(message) => ApiError::bad_request(message),
        other => ApiError::from(other),
    }
}

// ---------------------------------------------------------------------------
// Webhooks
// ---------------------------------------------------------------------------

/// Request body for `POST /api/v2/webhooks`.
#[derive(Debug, Deserialize)]
pub struct CreateWebhookRequest {
    /// Destination URL (`http://` or `https://`).
    pub url: String,
    /// Full `CloudEvents` types to deliver (e.g.
    /// `com.meridian.table.committed`); empty or omitted = all events.
    #[serde(default)]
    pub event_types: Vec<String>,
    /// HMAC-SHA256 signing secret (min 16 characters). Write-only: never
    /// returned by any endpoint.
    pub secret: String,
}

/// A webhook endpoint as rendered by the API (no secret, ever).
#[derive(Debug, Serialize)]
pub struct WebhookResponse {
    /// ULID of the endpoint.
    pub id: String,
    /// Destination URL.
    pub url: String,
    /// `CloudEvents` type filter; empty = all events.
    pub event_types: Vec<String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

impl From<WebhookEndpointRecord> for WebhookResponse {
    fn from(record: WebhookEndpointRecord) -> Self {
        Self {
            id: record.id,
            url: record.url,
            event_types: record.event_types,
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

/// Validates a webhook event-type filter entry.
fn validate_event_type(event_type: &str) -> Result<(), ApiError> {
    if !event_type.starts_with(EVENT_TYPE_PREFIX) {
        return Err(ApiError::bad_request(format!(
            "unknown event type {event_type:?}: event types are CloudEvents type strings and \
             start with \"{EVENT_TYPE_PREFIX}\" (e.g. \"{EVENT_TYPE_PREFIX}table.committed\")"
        )));
    }
    Ok(())
}

/// `POST /api/v2/webhooks` — register a webhook endpoint.
pub async fn create_webhook(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateWebhookRequest>,
) -> Result<(StatusCode, Json<WebhookResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;

    if !(request.url.starts_with("https://") || request.url.starts_with("http://")) {
        return Err(ApiError::bad_request(
            "webhook url must start with https:// or http://",
        ));
    }
    if request.secret.len() < MIN_SECRET_LEN {
        return Err(ApiError::bad_request(format!(
            "webhook secret must be at least {MIN_SECRET_LEN} characters"
        )));
    }
    for event_type in &request.event_types {
        validate_event_type(event_type)?;
    }

    let record = webhook::create_endpoint(
        &state.pool,
        tenancy::default_workspace_id(),
        &request.url,
        &request.event_types,
        &request.secret,
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;

    Ok((StatusCode::CREATED, Json(record.into())))
}

/// Response body for `GET /api/v2/webhooks`.
#[derive(Debug, Serialize)]
pub struct ListWebhooksResponse {
    /// All webhook endpoints in the workspace.
    pub webhooks: Vec<WebhookResponse>,
}

/// `GET /api/v2/webhooks` — list webhook endpoints.
pub async fn list_webhooks(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<ListWebhooksResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let webhooks = webhook::list_endpoints(&state.pool, tenancy::default_workspace_id())
        .await?
        .into_iter()
        .map(WebhookResponse::from)
        .collect();
    Ok(Json(ListWebhooksResponse { webhooks }))
}

/// `GET /api/v2/webhooks/{id}` — load one webhook endpoint.
pub async fn get_webhook(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<WebhookResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let record = webhook::get_endpoint(&state.pool, tenancy::default_workspace_id(), &id)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("webhook {id:?} does not exist"),
            )
        })?;
    Ok(Json(record.into()))
}

/// `DELETE /api/v2/webhooks/{id}` — delete a webhook endpoint (its
/// delivery history goes with it).
pub async fn delete_webhook(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    webhook::delete_endpoint(
        &state.pool,
        tenancy::default_workspace_id(),
        &id,
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Query parameters for `GET /api/v2/webhooks/{id}/deliveries`.
#[derive(Debug, Deserialize)]
pub struct ListDeliveriesQuery {
    /// Filter by delivery status: `pending`, `delivered`, or `dead`.
    #[serde(default)]
    pub status: Option<String>,
    /// Page size (default 100, max 1000).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// One delivery in the API rendering.
#[derive(Debug, Serialize)]
pub struct DeliveryResponse {
    /// Event id (a feed cursor).
    pub event_id: String,
    /// Full `CloudEvents` event type.
    pub event_type: String,
    /// `pending`, `delivered`, or `dead`.
    pub status: String,
    /// Attempts made so far.
    pub attempts: i32,
    /// HTTP status of the last attempt, if a response was received.
    pub last_status: Option<i16>,
    /// Error detail of the last failed attempt.
    pub last_error: Option<String>,
    /// Next scheduled attempt (meaningful while `pending`).
    pub next_attempt_at: DateTime<Utc>,
    /// Last state change.
    pub updated_at: DateTime<Utc>,
}

impl From<DeliveryRecord> for DeliveryResponse {
    fn from(r: DeliveryRecord) -> Self {
        Self {
            event_id: r.event_id,
            event_type: r.event_type,
            status: r.status,
            attempts: r.attempts,
            last_status: r.last_status,
            last_error: r.last_error,
            next_attempt_at: r.next_attempt_at,
            updated_at: r.updated_at,
        }
    }
}

/// Response body for `GET /api/v2/webhooks/{id}/deliveries`.
#[derive(Debug, Serialize)]
pub struct ListDeliveriesResponse {
    /// Deliveries, newest event first.
    pub deliveries: Vec<DeliveryResponse>,
}

/// `GET /api/v2/webhooks/{id}/deliveries` — delivery history, including
/// dead-lettered deliveries (`?status=dead`).
pub async fn list_webhook_deliveries(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Query(query): Query<ListDeliveriesQuery>,
) -> Result<Json<ListDeliveriesResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    if let Some(status) = query.status.as_deref()
        && !matches!(status, "pending" | "delivered" | "dead")
    {
        return Err(ApiError::bad_request(
            "status must be one of pending, delivered, dead",
        ));
    }
    // 404 for an unknown endpoint (not just an empty list).
    if webhook::get_endpoint(&state.pool, tenancy::default_workspace_id(), &id)
        .await?
        .is_none()
    {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "NotFoundException",
            format!("webhook {id:?} does not exist"),
        ));
    }
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let deliveries = webhook::list_deliveries(&state.pool, &id, query.status.as_deref(), limit)
        .await?
        .into_iter()
        .map(DeliveryResponse::from)
        .collect();
    Ok(Json(ListDeliveriesResponse { deliveries }))
}

// ---------------------------------------------------------------------------
// The queryable feed
// ---------------------------------------------------------------------------

/// Query parameters for `GET /api/v2/events`.
#[derive(Debug, Deserialize)]
pub struct FeedQuery {
    /// Exclusive cursor: return events with id greater than this. Omitted
    /// or empty = from the beginning; the sentinel `latest` = only events
    /// published after this request.
    #[serde(default)]
    pub after: Option<String>,
    /// Comma-separated full `CloudEvents` types to include.
    #[serde(default)]
    pub types: Option<String>,
    /// Page size (default 100, max 1000).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Response body for feed reads (`GET /api/v2/events` and consumer
/// `.../next`).
#[derive(Debug, Serialize)]
pub struct FeedResponse {
    /// `CloudEvents` 1.0 JSON objects, in feed (id) order.
    pub events: Vec<Value>,
    /// Cursor for the next page: the id of the last event returned, or —
    /// when `events` is empty — the cursor that was passed in. Feed
    /// clients pass it as `after`; consumers pass it to `.../commit`.
    pub next_cursor: String,
}

/// Parses the `types` filter into stored (short) event types.
fn parse_types(types: Option<&str>) -> Result<Option<Vec<String>>, ApiError> {
    let Some(types) = types else { return Ok(None) };
    let mut short = Vec::new();
    for entry in types.split(',').map(str::trim).filter(|t| !t.is_empty()) {
        validate_event_type(entry)?;
        short.push(entry[EVENT_TYPE_PREFIX.len()..].to_owned());
    }
    Ok(if short.is_empty() { None } else { Some(short) })
}

/// Reads one feed page and renders it.
async fn read_feed(
    state: &AppState,
    after: &str,
    types: Option<&[String]>,
    limit: i64,
) -> Result<FeedResponse, ApiError> {
    let records = outbox::list_published(&state.pool, after, types, limit).await?;
    let next_cursor = records
        .last()
        .map_or_else(|| after.to_owned(), |r| r.id.clone());
    let events = records.iter().map(cloud_event).collect();
    Ok(FeedResponse {
        events,
        next_cursor,
    })
}

/// `GET /api/v2/events` — keyset-paginated feed of published events.
pub async fn list_events(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<FeedQuery>,
) -> Result<Json<FeedResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let after = match query.after.as_deref() {
        Some("latest") => outbox::latest_cursor(&state.pool).await?,
        Some(after) => after.to_owned(),
        None => String::new(),
    };
    let types = parse_types(query.types.as_deref())?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    Ok(Json(
        read_feed(&state, &after, types.as_deref(), limit).await?,
    ))
}

// ---------------------------------------------------------------------------
// Durable consumers
// ---------------------------------------------------------------------------

/// Request body for `POST /api/v2/events/consumers`.
#[derive(Debug, Deserialize)]
pub struct CreateConsumerRequest {
    /// Consumer name, unique per workspace, `[A-Za-z0-9._-]{1,100}`.
    pub name: String,
}

/// A consumer as rendered by the API.
#[derive(Debug, Serialize)]
pub struct ConsumerResponse {
    /// Consumer name.
    pub name: String,
    /// Last committed cursor; `null` until the first commit.
    pub cursor: Option<String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last cursor commit (or creation).
    pub updated_at: DateTime<Utc>,
}

impl From<consumer::ConsumerRecord> for ConsumerResponse {
    fn from(r: consumer::ConsumerRecord) -> Self {
        Self {
            name: r.name,
            cursor: r.cursor,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

/// Validates a consumer name.
fn validate_consumer_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() || name.len() > MAX_CONSUMER_NAME_LEN {
        return Err(ApiError::bad_request(format!(
            "consumer name must be 1–{MAX_CONSUMER_NAME_LEN} characters"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(ApiError::bad_request(
            "consumer name may only contain letters, digits, '.', '_' and '-'",
        ));
    }
    Ok(())
}

/// `POST /api/v2/events/consumers` — create a named durable consumer
/// starting at the beginning of the feed.
pub async fn create_consumer(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<CreateConsumerRequest>,
) -> Result<(StatusCode, Json<ConsumerResponse>), ApiError> {
    require_management(&state.pool, &principal).await?;
    validate_consumer_name(&request.name)?;
    let record = consumer::create(
        &state.pool,
        tenancy::default_workspace_id(),
        &request.name,
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;
    Ok((StatusCode::CREATED, Json(record.into())))
}

/// Response body for `GET /api/v2/events/consumers`.
#[derive(Debug, Serialize)]
pub struct ListConsumersResponse {
    /// All consumers in the workspace, by name.
    pub consumers: Vec<ConsumerResponse>,
}

/// `GET /api/v2/events/consumers` — list consumers.
pub async fn list_consumers(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<ListConsumersResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let consumers = consumer::list(&state.pool, tenancy::default_workspace_id())
        .await?
        .into_iter()
        .map(ConsumerResponse::from)
        .collect();
    Ok(Json(ListConsumersResponse { consumers }))
}

/// `DELETE /api/v2/events/consumers/{name}` — delete a consumer.
pub async fn delete_consumer(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_management(&state.pool, &principal).await?;
    consumer::delete(
        &state.pool,
        tenancy::default_workspace_id(),
        &name,
        &principal.audit_string(),
    )
    .await
    .map_err(store_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Query parameters for `GET /api/v2/events/consumers/{name}/next`.
#[derive(Debug, Deserialize)]
pub struct ConsumerNextQuery {
    /// Comma-separated full `CloudEvents` types to include.
    #[serde(default)]
    pub types: Option<String>,
    /// Page size (default 100, max 1000).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /api/v2/events/consumers/{name}/next` — the next batch after the
/// consumer's committed cursor. Does **not** advance the cursor: commit the
/// returned `next_cursor` once the batch is processed (at-least-once).
pub async fn consumer_next(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
    Query(query): Query<ConsumerNextQuery>,
) -> Result<Json<FeedResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    let record = consumer::get(&state.pool, tenancy::default_workspace_id(), &name)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("consumer {name:?} does not exist"),
            )
        })?;
    let types = parse_types(query.types.as_deref())?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let after = record.cursor.unwrap_or_default();
    Ok(Json(
        read_feed(&state, &after, types.as_deref(), limit).await?,
    ))
}

/// Request body for `POST /api/v2/events/consumers/{name}/commit`.
#[derive(Debug, Deserialize)]
pub struct CommitCursorRequest {
    /// The cursor to commit — typically `next_cursor` from `.../next`.
    pub cursor: String,
}

/// `POST /api/v2/events/consumers/{name}/commit` — persist the consumer's
/// cursor. Committing an already-committed cursor is a no-op; moving
/// backwards is a 409.
pub async fn consumer_commit(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
    Json(request): Json<CommitCursorRequest>,
) -> Result<Json<ConsumerResponse>, ApiError> {
    require_management(&state.pool, &principal).await?;
    if request.cursor.is_empty() {
        return Err(ApiError::bad_request("cursor must not be empty"));
    }
    let record = consumer::commit_cursor(
        &state.pool,
        tenancy::default_workspace_id(),
        &name,
        &request.cursor,
    )
    .await
    .map_err(|e| match e {
        MeridianError::Conflict(message) => {
            ApiError::new(StatusCode::CONFLICT, "CommitFailedException", message)
        }
        other => store_error(other),
    })?;
    Ok(Json(record.into()))
}
