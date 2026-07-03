//! Router-level integration tests for the audit surface: the filterable
//! audit-log query (keyset pagination by chain position) and chain
//! verification (which must itself be audited).
//!
//! These require a running Postgres and `DATABASE_URL`; without it they
//! skip (with a note on stderr). The audit log is shared across all test
//! binaries, so every test isolates on a unique principal string.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use meridian_common::AppConfig;
use meridian_server::{AppState, build_router};
use meridian_store::audit::{self, NewAuditEntry};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use ulid::Ulid;

async fn test_app() -> Option<(Router, PgPool)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping audit API test: DATABASE_URL is not set");
        return None;
    };

    let mut config = AppConfig::default();
    config.database.url = url;

    let pool = meridian_store::connect(&config.database)
        .await
        .expect("connect to test database");
    meridian_store::MIGRATOR
        .run(&pool)
        .await
        .expect("run migrations");

    let router = build_router(AppState {
        pool: pool.clone(),
        config: Arc::new(config),
    });
    Some((router, pool))
}

/// Sends one GET through the full middleware stack and returns
/// (status, parsed JSON body).
async fn get(router: &Router, uri: &str) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("infallible router call");

    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    let value = serde_json::from_slice(&bytes).expect("response body is JSON");
    (status, value)
}

/// Seeds three audit entries under a fresh unique principal and returns
/// (principal, [seq of each entry in append order]).
async fn seed_entries(pool: &PgPool) -> (String, Vec<i64>) {
    let principal = format!("test:audit-api-{}", Ulid::new());
    let mut seqs = Vec::new();
    for (action, resource) in [
        ("alpha.one", "thing:1"),
        ("alpha.two", "thing:1"),
        ("beta.one", "thing:2"),
    ] {
        let record = audit::append(
            pool,
            NewAuditEntry {
                workspace_id: None,
                principal: principal.clone(),
                action: action.to_owned(),
                resource: resource.to_owned(),
                details: json!({ "test": true }),
            },
        )
        .await
        .expect("seed audit entry");
        seqs.push(record.seq);
    }
    (principal, seqs)
}

#[tokio::test]
async fn audit_query_filters_and_returns_full_entries() {
    let Some((router, pool)) = test_app().await else {
        return;
    };
    let (principal, seqs) = seed_entries(&pool).await;

    // Principal filter alone: exactly the three seeded entries, newest
    // first, rendered as full records.
    let (status, body) = get(&router, &format!("/api/v2/audit?principal={principal}")).await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let entries = body["entries"].as_array().expect("entries array");
    assert_eq!(entries.len(), 3);
    assert_eq!(
        entries
            .iter()
            .map(|e| e["seq"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![seqs[2], seqs[1], seqs[0]],
        "entries must come back newest first"
    );
    let newest = &entries[0];
    assert_eq!(newest["principal"], json!(principal));
    assert_eq!(newest["action"], json!("beta.one"));
    assert_eq!(newest["resource"], json!("thing:2"));
    assert_eq!(newest["details"], json!({ "test": true }));
    assert!(newest["id"].is_string(), "entry carries its ULID");
    assert!(newest["occurred_at"].is_string());
    assert!(newest["hash"].is_string());
    assert!(
        newest.get("prev_hash").is_some(),
        "chain linkage is part of the rendering"
    );
    assert!(
        body.get("next_cursor").is_none(),
        "short page must not advertise another one"
    );

    // Action prefix filter.
    let (status, body) = get(
        &router,
        &format!("/api/v2/audit?principal={principal}&action=alpha.*"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let actions: Vec<&str> = body["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    assert_eq!(actions, vec!["alpha.two", "alpha.one"]);

    // Exact action filter.
    let (status, body) = get(
        &router,
        &format!("/api/v2/audit?principal={principal}&action=alpha.one"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["entries"].as_array().unwrap().len(), 1);

    // Resource filter.
    let (status, body) = get(
        &router,
        &format!("/api/v2/audit?principal={principal}&resource=thing:1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["entries"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn audit_query_paginates_by_seq_keyset() {
    let Some((router, pool)) = test_app().await else {
        return;
    };
    let (principal, seqs) = seed_entries(&pool).await;

    // Page 1: two newest entries and a cursor.
    let (status, body) = get(
        &router,
        &format!("/api/v2/audit?principal={principal}&limit=2"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let page1: Vec<i64> = body["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(page1, vec![seqs[2], seqs[1]]);
    let cursor = body["next_cursor"].as_i64().expect("full page has cursor");
    assert_eq!(cursor, seqs[1]);

    // Page 2: the remaining oldest entry; no further cursor.
    let (status, body) = get(
        &router,
        &format!("/api/v2/audit?principal={principal}&limit=2&before={cursor}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let page2: Vec<i64> = body["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(page2, vec![seqs[0]]);
    assert!(body.get("next_cursor").is_none());
}

#[tokio::test]
async fn audit_query_filters_by_time_window() {
    let Some((router, pool)) = test_app().await else {
        return;
    };
    let (principal, seqs) = seed_entries(&pool).await;

    // Fetch the middle entry's timestamp and use it as both bounds:
    // inclusive from/to must keep it and drop the (strictly newer/older)
    // neighbors — the seeded appends are serialized, and occurred_at has
    // microsecond precision, so ties are effectively impossible.
    let (_, body) = get(&router, &format!("/api/v2/audit?principal={principal}")).await;
    let middle = body["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["seq"].as_i64() == Some(seqs[1]))
        .expect("middle entry present");
    let at = middle["occurred_at"].as_str().expect("timestamp string");

    let encoded = at.replace('+', "%2B");
    let (status, body) = get(
        &router,
        &format!("/api/v2/audit?principal={principal}&from={encoded}&to={encoded}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1, "window of one instant: {body}");
    assert_eq!(entries[0]["seq"].as_i64(), Some(seqs[1]));
}

#[tokio::test]
async fn audit_query_rejects_bad_parameters() {
    let Some((router, _pool)) = test_app().await else {
        return;
    };

    let (status, body) = get(&router, "/api/v2/audit?from=yesterday").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], json!("BadRequestException"));

    let (status, body) = get(
        &router,
        "/api/v2/audit?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], json!("BadRequestException"));
}

#[tokio::test]
async fn audit_verify_reports_valid_chain_and_audits_itself() {
    let Some((router, pool)) = test_app().await else {
        return;
    };
    // Ensure the chain is non-empty regardless of test ordering.
    let (_principal, _seqs) = seed_entries(&pool).await;

    let (status, first) = get(&router, "/api/v2/audit/verify").await;
    assert_eq!(status, StatusCode::OK, "body: {first}");
    assert_eq!(first["valid"], json!(true));
    let checked_1 = first["entries_checked"].as_u64().expect("count");
    assert!(checked_1 >= 3);
    assert!(first.get("broken_at").is_none());
    assert!(first.get("error").is_none());

    // The verification itself must land in the log: a second run checks a
    // strictly longer chain (appends are monotonic; concurrent tests only
    // grow it further).
    let (status, second) = get(&router, "/api/v2/audit/verify").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["valid"], json!(true));
    assert!(second["entries_checked"].as_u64().expect("count") > checked_1);

    // And the entry is queryable through the audit API.
    let (status, body) = get(&router, "/api/v2/audit?action=audit.verify&limit=1").await;
    assert_eq!(status, StatusCode::OK);
    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["resource"], json!("audit:chain"));
    assert_eq!(entries[0]["details"]["valid"], json!(true));
}
