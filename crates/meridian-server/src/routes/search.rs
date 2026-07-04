//! Asset search: `GET /api/v2/search` (search v1, Postgres FTS).
//!
//! One ranked query across tables, views, and namespaces; see
//! `meridian_store::search` for the query semantics, ranking, and
//! pagination model.
//!
//! # Authorization
//!
//! There is no single privilege gate on this endpoint. Instead, results are
//! **filtered to the caller's visibility**: tables and views require a
//! `READ` grant (on the asset, an ancestor namespace, or the warehouse),
//! namespaces require `LIST_NAMESPACES` on their warehouse, and the
//! built-in `admin`/`catalog_reader` roles see everything. The visibility
//! set is resolved once per request (two queries) and enforced inside the
//! search query itself — no per-result authorization round-trips. An
//! ungranted principal gets an empty result list, not a 403. The
//! `warehouse` filter parameter resolves before filtering and 404s on an
//! unknown name, consistent with the resolution-before-authorization
//! posture documented in `crate::routes::grants`.

use axum::extract::{Query, State};
use axum::{Extension, Json};
use meridian_common::principal::Principal;
use meridian_store::search::{self, SearchAssetKind, SearchRequest};
use meridian_store::{quality_score, tenancy, warehouse};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::ApiError;

/// Default page size.
const DEFAULT_LIMIT: i64 = 20;

/// Largest accepted page size.
const MAX_LIMIT: i64 = 100;

/// Query parameters of `GET /api/v2/search`.
#[derive(Debug, Deserialize)]
pub struct SearchParams {
    /// The query text (required, non-empty).
    pub q: String,
    /// Comma-separated asset kinds to include (`table`, `view`,
    /// `namespace`); all when absent.
    #[serde(rename = "type")]
    pub kinds: Option<String>,
    /// Restrict to one warehouse by name.
    pub warehouse: Option<String>,
    /// Restrict to assets at or under this namespace (dot-separated
    /// levels; levels containing literal dots are not addressable through
    /// this convenience parameter).
    pub namespace: Option<String>,
    /// Page size (1–100, default 20).
    pub limit: Option<i64>,
    /// Keyset token from the previous response.
    pub page_token: Option<String>,
}

/// One rendered search result.
#[derive(Debug, Serialize)]
pub struct SearchResult {
    /// Asset kind: `table`, `view`, or `namespace`.
    #[serde(rename = "type")]
    pub kind: &'static str,
    /// ULID of the asset.
    pub id: String,
    /// Asset name (last level, for namespaces).
    pub name: String,
    /// Namespace path (for namespaces: the full own path).
    pub namespace: Vec<String>,
    /// Containing warehouse name.
    pub warehouse: String,
    /// Relevance score; higher is better. Comparable only within one query.
    pub rank: f64,
    /// `ts_headline` snippet; matches are wrapped in `**`.
    pub snippet: String,
    /// The table's composite quality / trust score (E-F6), `0..=100`, when the
    /// hit is a table (a cheap single-row read per result). `None` for views and
    /// namespaces (the score is table-scoped). Agents and the console read this
    /// to prefer trustworthy tables among relevance-comparable hits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality_score: Option<u8>,
}

/// Response body of `GET /api/v2/search`.
#[derive(Debug, Serialize)]
pub struct SearchResponse {
    /// The results, best first.
    pub results: Vec<SearchResult>,
    /// Opaque token for the next page; absent on the last page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// Parses the comma-separated `type` parameter.
fn parse_kinds(raw: &str) -> Result<Vec<SearchAssetKind>, ApiError> {
    let mut kinds = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let kind = SearchAssetKind::parse(part).ok_or_else(|| {
            ApiError::bad_request(format!(
                "unknown asset type {part:?} (expected table, view, or namespace)"
            ))
        })?;
        if !kinds.contains(&kind) {
            kinds.push(kind);
        }
    }
    if kinds.is_empty() {
        return Err(ApiError::bad_request(
            "type must name at least one of table, view, namespace",
        ));
    }
    Ok(kinds)
}

/// `GET /api/v2/search` — ranked full-text search over catalog assets.
pub async fn search(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, ApiError> {
    if params.q.trim().is_empty() {
        return Err(ApiError::bad_request("q must not be empty"));
    }
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(ApiError::bad_request(format!(
            "limit must be between 1 and {MAX_LIMIT}"
        )));
    }
    let kinds = params.kinds.as_deref().map(parse_kinds).transpose()?;

    let warehouse_id = match &params.warehouse {
        Some(name) => Some(
            warehouse::get_by_name(&state.pool, tenancy::default_workspace_id(), name)
                .await?
                .ok_or_else(|| ApiError::no_such_warehouse(name))?
                .id,
        ),
        None => None,
    };
    let namespace: Option<Vec<String>> = params
        .namespace
        .as_deref()
        .map(|raw| raw.split('.').map(str::to_owned).collect::<Vec<_>>())
        .filter(|levels| levels.iter().all(|l| !l.is_empty()));
    if params.namespace.is_some() && namespace.is_none() {
        return Err(ApiError::bad_request(
            "namespace must be dot-separated non-empty levels",
        ));
    }

    let visibility = search::visibility_for(&state.pool, &principal).await?;
    let page = search::search(
        &state.pool,
        tenancy::default_workspace_id(),
        &SearchRequest {
            text: &params.q,
            warehouse_id: warehouse_id.as_deref(),
            namespace: namespace.as_deref(),
            kinds: kinds.as_deref(),
            limit,
            page_token: params.page_token.as_deref(),
        },
        &visibility,
    )
    .await?;

    // Fold the composite quality score onto table hits (E-F6). The page is
    // bounded (≤100), so this is at most 100 cheap single-row reads — the
    // "wire into search rank if cheap" the brief calls for, surfaced on the
    // result rather than reordering (reordering would break the FTS keyset
    // pagination contract). A scoring error on one hit degrades that hit's
    // score to absent, never failing the whole search.
    let mut results = Vec::with_capacity(page.hits.len());
    for hit in page.hits {
        let quality_score = if matches!(hit.kind, SearchAssetKind::Table) {
            quality_score::score_for_search(&state.pool, tenancy::default_workspace_id(), &hit.id)
                .await
                .ok()
        } else {
            None
        };
        results.push(SearchResult {
            kind: hit.kind.as_str(),
            id: hit.id,
            name: hit.name,
            namespace: hit.namespace,
            warehouse: hit.warehouse,
            rank: hit.rank,
            snippet: hit.snippet,
            quality_score,
        });
    }

    Ok(Json(SearchResponse {
        results,
        next_page_token: page.next_page_token,
    }))
}
