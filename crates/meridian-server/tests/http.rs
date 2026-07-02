//! HTTP integration tests.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip
//! (with a note on stderr). Shape assertions run the router in-process
//! (through the full middleware stack); one test binds a real TCP listener
//! to exercise the serve path end to end.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use meridian_common::AppConfig;
use meridian_server::{AppState, build_router};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;

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

async fn get_json(router: Router, uri: &str) -> (StatusCode, Value) {
    let response = router
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("infallible router call");

    let status = response.status();
    assert!(
        response.headers().contains_key("x-request-id"),
        "every response must carry a request id"
    );
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    let value = serde_json::from_slice(&bytes).expect("response body is JSON");
    (status, value)
}

#[tokio::test]
async fn healthz_reports_ok_with_reachable_database() {
    let Some(router) = test_router().await else {
        return;
    };
    let (status, body) = get_json(router, "/healthz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        serde_json::json!({"status": "ok", "checks": {"database": "ok"}})
    );
}

#[tokio::test]
async fn readyz_reports_ok_with_reachable_database() {
    let Some(router) = test_router().await else {
        return;
    };
    let (status, body) = get_json(router, "/readyz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn iceberg_config_returns_spec_shape_on_both_paths() {
    let Some(router) = test_router().await else {
        return;
    };

    let expected = serde_json::json!({"defaults": {}, "overrides": {}});

    let (status, body) = get_json(router.clone(), "/v1/config").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, expected);

    let (status, body) = get_json(router.clone(), "/iceberg/v1/config").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, expected);

    // The warehouse parameter is accepted (not rejected) even though it does
    // not affect the response yet.
    let (status, body) = get_json(router, "/v1/config?warehouse=s3://bucket/wh").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, expected);
}

#[tokio::test]
async fn unknown_route_returns_404_with_error_envelope() {
    let Some(router) = test_router().await else {
        return;
    };
    let (status, body) = get_json(router, "/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], 404);
    assert_eq!(body["error"]["type"], "NotFoundException");
    assert!(
        body["error"]["message"]
            .as_str()
            .is_some_and(|m| !m.is_empty()),
        "404 must carry a human-readable message in the IRC envelope"
    );
}

#[tokio::test]
async fn wrong_method_returns_405_with_error_envelope() {
    let Some(router) = test_router().await else {
        return;
    };
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/config")
                .body(Body::empty())
                .expect("build request"),
        )
        .await
        .expect("infallible router call");
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    let body: Value = serde_json::from_slice(&bytes).expect("405 body is JSON");
    assert_eq!(body["error"]["code"], 405);
    assert_eq!(body["error"]["type"], "MethodNotAllowedException");
}

#[tokio::test]
async fn served_over_real_tcp() {
    let Some(router) = test_router().await else {
        return;
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to server");
    stream
        .write_all(b"GET /v1/config HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf-8 response");

    assert!(
        text.starts_with("HTTP/1.1 200"),
        "unexpected status line in: {text}"
    );
    let body = text
        .split("\r\n\r\n")
        .nth(1)
        .expect("response has a body section");
    let value: Value = serde_json::from_str(body.trim()).expect("body is JSON");
    assert_eq!(value, serde_json::json!({"defaults": {}, "overrides": {}}));

    server.abort();
}
