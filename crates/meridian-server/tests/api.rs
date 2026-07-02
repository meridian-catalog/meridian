//! Router-level integration tests for the warehouse management API and the
//! Iceberg REST namespace surface.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip
//! (with a note on stderr). Each test provisions its own uniquely-named
//! warehouse so tests are isolated from each other and previous runs.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use meridian_common::AppConfig;
use meridian_server::{AppState, build_router};
use serde_json::{Value, json};
use tower::ServiceExt;
use ulid::Ulid;

async fn test_router() -> Option<Router> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping HTTP integration test: DATABASE_URL is not set");
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

    Some(build_router(AppState {
        pool,
        config: Arc::new(config),
    }))
}

/// Sends one request through the full middleware stack and returns
/// (status, parsed JSON body — `Value::Null` when the body is empty).
async fn send(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    let response = router
        .clone()
        .oneshot(builder.body(body).expect("build request"))
        .await
        .expect("infallible router call");

    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response body is JSON")
    };
    (status, value)
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}-{}", Ulid::new().to_string().to_lowercase())
}

/// Creates a warehouse through the management API and returns its name.
async fn make_warehouse(router: &Router) -> String {
    let name = unique_name("wh");
    let (status, body) = send(
        router,
        "POST",
        "/api/v2/warehouses",
        Some(json!({ "name": name, "storage_root": "s3://test-bucket/root" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "create warehouse: {body}");
    name
}

fn assert_error(body: &Value, code: u16, error_type: &str) {
    assert_eq!(body["error"]["code"], code, "envelope: {body}");
    assert_eq!(body["error"]["type"], error_type, "envelope: {body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| !m.is_empty()),
        "error must carry a message: {body}"
    );
}

// ---------------------------------------------------------------------------
// Management API: /api/v2/warehouses

#[tokio::test]
async fn warehouse_create_list_delete_roundtrip() {
    let Some(router) = test_router().await else {
        return;
    };

    let name = unique_name("wh");
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        Some(json!({
            "name": name,
            "storage_root": "s3://bucket/prefix",
            "storage_options": { "region": "eu-central-1" },
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["name"], name);
    assert_eq!(body["storage_root"], "s3://bucket/prefix");
    assert_eq!(body["storage_options"]["region"], "eu-central-1");
    assert!(body["id"].as_str().is_some_and(|id| id.len() == 26));

    let (status, body) = send(&router, "GET", "/api/v2/warehouses", None).await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = body["warehouses"]
        .as_array()
        .expect("warehouses array")
        .iter()
        .filter_map(|w| w["name"].as_str())
        .collect();
    assert!(names.contains(&name.as_str()));

    let (status, _) = send(
        &router,
        "DELETE",
        &format!("/api/v2/warehouses/{name}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = send(&router, "GET", "/api/v2/warehouses", None).await;
    let names: Vec<&str> = body["warehouses"]
        .as_array()
        .expect("warehouses array")
        .iter()
        .filter_map(|w| w["name"].as_str())
        .collect();
    assert!(!names.contains(&name.as_str()));
}

#[tokio::test]
async fn warehouse_create_rejects_duplicates_and_bad_input() {
    let Some(router) = test_router().await else {
        return;
    };
    let name = make_warehouse(&router).await;

    // Duplicate name.
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        Some(json!({ "name": name, "storage_root": "s3://other" })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_error(&body, 409, "AlreadyExistsException");

    // Name unusable as a URL prefix.
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        Some(json!({ "name": "bad/name", "storage_root": "s3://b" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");

    // Empty storage root.
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/warehouses",
        Some(json!({ "name": unique_name("wh"), "storage_root": "  " })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");
}

#[tokio::test]
async fn warehouse_delete_rejects_missing_and_nonempty() {
    let Some(router) = test_router().await else {
        return;
    };

    let (status, body) = send(&router, "DELETE", "/api/v2/warehouses/no-such-wh", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchWarehouseException");

    let name = make_warehouse(&router).await;
    let (status, _) = send(
        &router,
        "POST",
        &format!("/v1/{name}/namespaces"),
        Some(json!({ "namespace": ["occupied"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(
        &router,
        "DELETE",
        &format!("/api/v2/warehouses/{name}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_error(&body, 409, "WarehouseNotEmptyException");
}

// ---------------------------------------------------------------------------
// GET /v1/config

#[tokio::test]
async fn config_resolves_warehouse_to_prefix_on_both_mounts() {
    let Some(router) = test_router().await else {
        return;
    };
    let name = make_warehouse(&router).await;

    for base in ["/v1", "/iceberg/v1"] {
        let (status, body) = send(
            &router,
            "GET",
            &format!("{base}/config?warehouse={name}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["overrides"], json!({ "prefix": name }));
        assert_eq!(body["defaults"], json!({}));
        let endpoints = body["endpoints"].as_array().expect("endpoints array");
        assert!(
            endpoints.contains(&json!("GET /v1/{prefix}/namespaces")),
            "endpoints must list the namespace surface: {body}"
        );
        for table_endpoint in [
            "GET /v1/{prefix}/namespaces/{namespace}/tables",
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "POST /v1/{prefix}/tables/rename",
            "POST /v1/{prefix}/transactions/commit",
        ] {
            assert!(
                endpoints.contains(&json!(table_endpoint)),
                "endpoints must list the table surface ({table_endpoint}): {body}"
            );
        }
        // Only implemented endpoints may be advertised: the view surface
        // is implemented, register-view is not.
        for view_endpoint in [
            "GET /v1/{prefix}/namespaces/{namespace}/views",
            "POST /v1/{prefix}/namespaces/{namespace}/views/{view}",
            "POST /v1/{prefix}/views/rename",
        ] {
            assert!(
                endpoints.contains(&json!(view_endpoint)),
                "endpoints must list the view surface ({view_endpoint}): {body}"
            );
        }
        assert!(
            !endpoints
                .iter()
                .any(|e| e.as_str().is_some_and(|s| s.contains("register-view"))),
            "only implemented endpoints may be advertised: {body}"
        );
    }

    // Without a warehouse parameter the config carries no prefix override.
    let (status, body) = send(&router, "GET", "/v1/config", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["overrides"], json!({}));

    // Unknown warehouse: 404 NoSuchWarehouseException per the spec.
    let (status, body) = send(&router, "GET", "/v1/config?warehouse=no-such-wh", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchWarehouseException");
}

// ---------------------------------------------------------------------------
// Namespace surface

#[tokio::test]
async fn namespace_create_load_head_delete_roundtrip() {
    let Some(router) = test_router().await else {
        return;
    };
    let wh = make_warehouse(&router).await;

    let (status, body) = send(
        &router,
        "POST",
        &format!("/v1/{wh}/namespaces"),
        Some(json!({ "namespace": ["accounting"], "properties": { "owner": "test" } })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({ "namespace": ["accounting"], "properties": { "owner": "test" } })
    );

    // Load on both mounts.
    for base in ["/v1", "/iceberg/v1"] {
        let (status, body) = send(
            &router,
            "GET",
            &format!("{base}/{wh}/namespaces/accounting"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["namespace"], json!(["accounting"]));
        assert_eq!(body["properties"]["owner"], "test");
    }

    // HEAD: 204 when present, 404 when absent.
    let (status, _) = send(
        &router,
        "HEAD",
        &format!("/v1/{wh}/namespaces/accounting"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = send(&router, "HEAD", &format!("/v1/{wh}/namespaces/ghost"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // DELETE: 204, then the namespace is gone.
    let (status, _) = send(
        &router,
        "DELETE",
        &format!("/v1/{wh}/namespaces/accounting"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, body) = send(
        &router,
        "GET",
        &format!("/v1/{wh}/namespaces/accounting"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");
}

#[tokio::test]
async fn namespace_errors_use_exact_irc_exceptions() {
    let Some(router) = test_router().await else {
        return;
    };
    let wh = make_warehouse(&router).await;

    // Unknown prefix (warehouse) anywhere on the surface.
    let (status, body) = send(&router, "GET", "/v1/no-such-wh/namespaces", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchWarehouseException");

    // Missing namespace: GET and DELETE.
    let (status, body) = send(&router, "GET", &format!("/v1/{wh}/namespaces/ghost"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");
    let (status, body) = send(
        &router,
        "DELETE",
        &format!("/v1/{wh}/namespaces/ghost"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");

    // Duplicate create.
    let create = json!({ "namespace": ["dup"] });
    let (status, _) = send(
        &router,
        "POST",
        &format!("/v1/{wh}/namespaces"),
        Some(create.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send(
        &router,
        "POST",
        &format!("/v1/{wh}/namespaces"),
        Some(create),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_error(&body, 409, "AlreadyExistsException");

    // Multi-level create without a parent.
    let (status, body) = send(
        &router,
        "POST",
        &format!("/v1/{wh}/namespaces"),
        Some(json!({ "namespace": ["nope", "child"] })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");

    // Structurally invalid namespaces.
    let (status, body) = send(
        &router,
        "POST",
        &format!("/v1/{wh}/namespaces"),
        Some(json!({ "namespace": [] })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");
    let (status, body) = send(
        &router,
        "POST",
        &format!("/v1/{wh}/namespaces"),
        Some(json!({ "namespace": [""] })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");
}

#[tokio::test]
async fn nested_namespaces_use_unit_separator_encoding() {
    let Some(router) = test_router().await else {
        return;
    };
    let wh = make_warehouse(&router).await;

    for ns in [json!(["a"]), json!(["a", "b"]), json!(["a", "b", "c"])] {
        let (status, body) = send(
            &router,
            "POST",
            &format!("/v1/{wh}/namespaces"),
            Some(json!({ "namespace": ns })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "create {ns}: {body}");
    }

    // Address the nested namespace with the %1F unit separator.
    let (status, body) = send(&router, "GET", &format!("/v1/{wh}/namespaces/a%1Fb"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["namespace"], json!(["a", "b"]));
    let (status, _) = send(
        &router,
        "HEAD",
        &format!("/v1/{wh}/namespaces/a%1Fb%1Fc"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // List under a parent: only direct children come back.
    let (status, body) = send(
        &router,
        "GET",
        &format!("/v1/{wh}/namespaces?parent=a"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["namespaces"], json!([["a", "b"]]));
    let (status, body) = send(
        &router,
        "GET",
        &format!("/v1/{wh}/namespaces?parent=a%1Fb"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["namespaces"], json!([["a", "b", "c"]]));

    // Listing under a missing parent is a 404.
    let (status, body) = send(
        &router,
        "GET",
        &format!("/v1/{wh}/namespaces?parent=ghost"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");

    // Deleting the non-empty parent is rejected with the exact exception.
    let (status, body) = send(&router, "DELETE", &format!("/v1/{wh}/namespaces/a"), None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_error(&body, 409, "NamespaceNotEmptyException");

    // Bottom-up deletion works.
    for ns in ["a%1Fb%1Fc", "a%1Fb", "a"] {
        let (status, body) = send(
            &router,
            "DELETE",
            &format!("/v1/{wh}/namespaces/{ns}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT, "delete {ns}: {body}");
    }
}

#[tokio::test]
async fn namespace_listing_paginates_across_pages() {
    let Some(router) = test_router().await else {
        return;
    };
    let wh = make_warehouse(&router).await;

    let expected: Vec<String> = (0..5).map(|i| format!("ns{i}")).collect();
    for name in &expected {
        let (status, _) = send(
            &router,
            "POST",
            &format!("/v1/{wh}/namespaces"),
            Some(json!({ "namespace": [name] })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    // Unpaginated listing returns everything and a null next-page-token.
    let (status, body) = send(&router, "GET", &format!("/v1/{wh}/namespaces"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["namespaces"].as_array().map(Vec::len), Some(5));
    assert!(body["next-page-token"].is_null());

    // pageSize=2 walks three pages: 2 + 2 + 1.
    let mut seen: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    let mut pages = 0;
    loop {
        let uri = match &token {
            Some(t) => format!("/v1/{wh}/namespaces?pageSize=2&pageToken={t}"),
            None => format!("/v1/{wh}/namespaces?pageSize=2"),
        };
        let (status, body) = send(&router, "GET", &uri, None).await;
        assert_eq!(status, StatusCode::OK);
        pages += 1;
        assert!(pages <= 3, "must terminate in three pages");

        let page: Vec<String> = body["namespaces"]
            .as_array()
            .expect("namespaces array")
            .iter()
            .map(|ns| ns[0].as_str().expect("level string").to_owned())
            .collect();
        assert!(page.len() <= 2, "page respects pageSize");
        seen.extend(page);

        match body["next-page-token"].as_str() {
            Some(next) => token = Some(next.to_owned()),
            None => break,
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(seen, expected, "no row skipped or repeated across pages");

    // Bad pagination inputs.
    let (status, body) = send(
        &router,
        "GET",
        &format!("/v1/{wh}/namespaces?pageToken=notatoken"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");
    let (status, body) = send(
        &router,
        "GET",
        &format!("/v1/{wh}/namespaces?pageSize=0"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(&body, 400, "BadRequestException");
}

#[tokio::test]
async fn namespace_property_updates_and_conflicts() {
    let Some(router) = test_router().await else {
        return;
    };
    let wh = make_warehouse(&router).await;

    let (status, _) = send(
        &router,
        "POST",
        &format!("/v1/{wh}/namespaces"),
        Some(json!({ "namespace": ["props"], "properties": { "keep": "1", "drop": "2" } })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Updates + removals in one atomic call; absent removals are "missing".
    let (status, body) = send(
        &router,
        "POST",
        &format!("/v1/{wh}/namespaces/props/properties"),
        Some(json!({
            "updates": { "keep": "changed", "new": "3" },
            "removals": ["drop", "absent"],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["updated"], json!(["keep", "new"]));
    assert_eq!(body["removed"], json!(["drop"]));
    assert_eq!(body["missing"], json!(["absent"]));

    let (_, body) = send(&router, "GET", &format!("/v1/{wh}/namespaces/props"), None).await;
    assert_eq!(body["properties"], json!({ "keep": "changed", "new": "3" }));

    // A key in both updates and removals: 422 per the spec.
    let (status, body) = send(
        &router,
        "POST",
        &format!("/v1/{wh}/namespaces/props/properties"),
        Some(json!({ "updates": { "k": "v" }, "removals": ["k"] })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_error(&body, 422, "UnprocessableEntityException");

    // Property update on a missing namespace: 404.
    let (status, body) = send(
        &router,
        "POST",
        &format!("/v1/{wh}/namespaces/ghost/properties"),
        Some(json!({ "updates": { "k": "v" } })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error(&body, 404, "NoSuchNamespaceException");
}

#[tokio::test]
async fn wrong_method_on_namespace_routes_returns_envelope_405() {
    let Some(router) = test_router().await else {
        return;
    };
    let wh = make_warehouse(&router).await;

    let (status, body) = send(&router, "PUT", &format!("/v1/{wh}/namespaces"), None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_error(&body, 405, "MethodNotAllowedException");
}
