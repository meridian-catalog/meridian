//! Router-level integration tests for the events surface: webhook CRUD and
//! delivery (against an in-process receiver, including HMAC verification
//! and dead-lettering), the queryable feed, and durable consumers.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they
//! skip (with a note on stderr). Tests that run the outbox relay serialize
//! on the same cross-binary Postgres advisory lock as the store's relay
//! tests, because relay claims are global.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use http_body_util::BodyExt;
use meridian_common::AppConfig;
use meridian_common::config::EventsConfig;
use meridian_server::{AppState, build_router, events};
use meridian_store::outbox::{self, NewOutboxEvent};
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, PgPool};
use tower::ServiceExt;
use ulid::Ulid;

/// Advisory lock key shared by all relay-claiming tests across test
/// binaries (ASCII "RELAYTST" packed into an i64; must match the store's
/// events tests).
const RELAY_TEST_LOCK_KEY: i64 = 0x5245_4C41_5954_5354;

async fn test_app() -> Option<(Router, PgPool, String)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping events API test: DATABASE_URL is not set");
        return None;
    };

    let mut config = AppConfig::default();
    config.database.url = url.clone();

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
    Some((router, pool, url))
}

/// Takes the cross-binary relay test lock on a dedicated (non-pooled)
/// connection; the lock releases when the returned connection drops.
async fn relay_test_lock(url: &str) -> PgConnection {
    let mut conn = PgConnection::connect(url)
        .await
        .expect("connect for advisory lock");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(RELAY_TEST_LOCK_KEY)
        .execute(&mut conn)
        .await
        .expect("take relay test lock");
    conn
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

/// Enqueues a committed outbox event with the given (short) type.
async fn enqueue(pool: &PgPool, short_type: &str) -> String {
    outbox::enqueue(
        pool,
        &NewOutboxEvent {
            workspace_id: None,
            aggregate: format!("test-events-api:{}", Ulid::new()),
            event_type: short_type.to_owned(),
            payload: json!({ "marker": short_type }),
        },
    )
    .await
    .expect("enqueue event")
}

/// Runs the relay until the given events are published (bounded).
///
/// The caller must already hold the feed serialization lock
/// ([`relay_test_lock`]) for the whole test: enqueues are not otherwise
/// serialized, so a concurrent test could insert an unpublished event older
/// than `ids` and stall the publication frontier below them.
async fn relay_until_published(pool: &PgPool, ids: &[String]) {
    for _ in 0..1_000 {
        let mut done = true;
        for id in ids {
            let published: Option<chrono::DateTime<chrono::Utc>> =
                sqlx::query_scalar("SELECT published_at FROM events_outbox WHERE id = $1")
                    .bind(id)
                    .fetch_one(pool)
                    .await
                    .expect("read published_at");
            if published.is_none() {
                done = false;
            }
        }
        if done {
            return;
        }
        outbox::relay_once(pool, 500).await.expect("relay_once");
    }
    panic!("events {ids:?} were not published within the iteration budget");
}

/// Resolves the current end-of-feed cursor through the API.
async fn latest_cursor(router: &Router) -> String {
    let (status, body) = send(router, "GET", "/api/v2/events?after=latest&limit=1", None).await;
    assert_eq!(status, StatusCode::OK, "latest-cursor read failed: {body}");
    body["next_cursor"]
        .as_str()
        .expect("next_cursor is a string")
        .to_owned()
}

/// A short unique suffix for event types and names.
fn unique(prefix: &str) -> String {
    format!("{prefix}.{}", Ulid::new().to_string().to_lowercase())
}

// ---------------------------------------------------------------------------
// Webhook CRUD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn webhook_crud_validates_and_never_returns_the_secret() {
    let Some((router, _pool, _url)) = test_app().await else {
        return;
    };

    let url = format!("https://example.invalid/hooks/{}", Ulid::new());

    // Validation: short secret, bad event type, bad URL.
    let (status, _) = send(
        &router,
        "POST",
        "/api/v2/webhooks",
        Some(json!({ "url": url, "secret": "short" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, body) = send(
        &router,
        "POST",
        "/api/v2/webhooks",
        Some(json!({
            "url": url,
            "secret": "0123456789abcdef",
            "event_types": ["table.committed"]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, _) = send(
        &router,
        "POST",
        "/api/v2/webhooks",
        Some(json!({ "url": "ftp://example.invalid/x", "secret": "0123456789abcdef" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Create.
    let (status, created) = send(
        &router,
        "POST",
        "/api/v2/webhooks",
        Some(json!({
            "url": url,
            "secret": "0123456789abcdef",
            "event_types": ["com.meridian.table.committed"]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert!(
        created.get("secret").is_none(),
        "secret must never be returned"
    );
    let id = created["id"].as_str().expect("id").to_owned();

    // Duplicate URL conflicts.
    let (status, _) = send(
        &router,
        "POST",
        "/api/v2/webhooks",
        Some(json!({ "url": url, "secret": "0123456789abcdef" })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    // List + get, still no secret anywhere.
    let (status, listed) = send(&router, "GET", "/api/v2/webhooks", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!listed.to_string().contains("0123456789abcdef"));
    assert!(
        listed["webhooks"]
            .as_array()
            .expect("webhooks array")
            .iter()
            .any(|w| w["id"] == id.as_str())
    );
    let (status, got) = send(&router, "GET", &format!("/api/v2/webhooks/{id}"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(got["url"], url.as_str());
    assert!(got.get("secret").is_none());

    // Delivery history: empty, and the status filter is validated.
    let (status, deliveries) = send(
        &router,
        "GET",
        &format!("/api/v2/webhooks/{id}/deliveries"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(deliveries["deliveries"], json!([]));
    let (status, _) = send(
        &router,
        "GET",
        &format!("/api/v2/webhooks/{id}/deliveries?status=bogus"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Delete is terminal.
    let (status, _) = send(&router, "DELETE", &format!("/api/v2/webhooks/{id}"), None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = send(&router, "GET", &format!("/api/v2/webhooks/{id}"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(&router, "DELETE", &format!("/api/v2/webhooks/{id}"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// The queryable feed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn feed_paginates_and_filters_published_events() {
    let Some((router, pool, url)) = test_app().await else {
        return;
    };
    // Held for the whole test: serializes every enqueue+relay+read against
    // other feed tests so the publication frontier stays predictable.
    let _serial = relay_test_lock(&url).await;

    let type_a = unique("test.feed-a");
    let type_b = unique("test.feed-b");
    let start = latest_cursor(&router).await;

    let e1 = enqueue(&pool, &type_a).await;
    let e2 = enqueue(&pool, &type_a).await;
    let e3 = enqueue(&pool, &type_b).await;
    relay_until_published(&pool, &[e1.clone(), e2.clone(), e3.clone()]).await;

    // Type filters must be full CloudEvents types.
    let (status, _) = send(
        &router,
        "GET",
        &format!("/api/v2/events?after={start}&types={type_a}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Page 1 (limit 2 of 3 matching events).
    let both = format!("com.meridian.{type_a},com.meridian.{type_b}");
    let (status, page1) = send(
        &router,
        "GET",
        &format!("/api/v2/events?after={start}&types={both}&limit=2"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page1}");
    let events1 = page1["events"].as_array().expect("events array");
    assert_eq!(events1.len(), 2);
    assert_eq!(events1[0]["id"], e1.as_str());
    assert_eq!(events1[1]["id"], e2.as_str());
    // CloudEvents 1.0 shape.
    assert_eq!(events1[0]["specversion"], "1.0");
    assert_eq!(events1[0]["type"], format!("com.meridian.{type_a}"));
    assert_eq!(events1[0]["source"], "meridian");
    assert_eq!(events1[0]["datacontenttype"], "application/json");
    assert_eq!(events1[0]["data"]["marker"], type_a.as_str());
    assert!(
        events1[0]["subject"]
            .as_str()
            .is_some_and(|s| s.starts_with("test-events-api:"))
    );
    assert_eq!(page1["next_cursor"], e2.as_str());

    // Page 2 via keyset cursor.
    let (status, page2) = send(
        &router,
        "GET",
        &format!("/api/v2/events?after={e2}&types={both}&limit=2"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let events2 = page2["events"].as_array().expect("events array");
    assert_eq!(events2.len(), 1);
    assert_eq!(events2[0]["id"], e3.as_str());

    // A single-type filter excludes the rest.
    let (status, only_b) = send(
        &router,
        "GET",
        &format!("/api/v2/events?after={start}&types=com.meridian.{type_b}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let events_b = only_b["events"].as_array().expect("events array");
    assert_eq!(events_b.len(), 1);
    assert_eq!(events_b[0]["id"], e3.as_str());

    // after=latest yields nothing new and echoes a usable cursor.
    let (status, tail) = send(
        &router,
        "GET",
        &format!("/api/v2/events?after=latest&types={both}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(tail["events"], json!([]));
    assert!(tail["next_cursor"].as_str().is_some_and(|c| !c.is_empty()));

    // order=desc returns the most recent matching events first (the UI
    // "recent activity" view); e3 is newest, e1 oldest.
    let (status, desc) = send(
        &router,
        "GET",
        &format!("/api/v2/events?order=desc&types={both}&limit=3"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{desc}");
    let desc_events = desc["events"].as_array().expect("events array");
    assert_eq!(desc_events.len(), 3);
    assert_eq!(desc_events[0]["id"], e3.as_str());
    assert_eq!(desc_events[1]["id"], e2.as_str());
    assert_eq!(desc_events[2]["id"], e1.as_str());
}

// ---------------------------------------------------------------------------
// Durable consumers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn consumers_read_commit_and_never_lose_uncommitted_batches() {
    let Some((router, pool, url)) = test_app().await else {
        return;
    };
    // Held for the whole test — see the feed test for why.
    let _serial = relay_test_lock(&url).await;

    let name = unique("consumer");
    let feed_type = unique("test.consumer");
    let full_type = format!("com.meridian.{feed_type}");

    // Name validation and create/conflict.
    let (status, _) = send(
        &router,
        "POST",
        "/api/v2/events/consumers",
        Some(json!({ "name": "bad name!" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, created) = send(
        &router,
        "POST",
        "/api/v2/events/consumers",
        Some(json!({ "name": name })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["cursor"], Value::Null);
    let (status, _) = send(
        &router,
        "POST",
        "/api/v2/events/consumers",
        Some(json!({ "name": name })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let e1 = enqueue(&pool, &feed_type).await;
    let e2 = enqueue(&pool, &feed_type).await;
    let e3 = enqueue(&pool, &feed_type).await;
    relay_until_published(&pool, &[e1.clone(), e2.clone(), e3.clone()]).await;

    // First batch; a re-read without commit returns the same batch
    // (at-least-once).
    let next_uri = format!("/api/v2/events/consumers/{name}/next?types={full_type}&limit=2");
    let (status, batch) = send(&router, "GET", &next_uri, None).await;
    assert_eq!(status, StatusCode::OK, "{batch}");
    assert_eq!(batch["events"].as_array().expect("events").len(), 2);
    assert_eq!(batch["events"][0]["id"], e1.as_str());
    assert_eq!(batch["next_cursor"], e2.as_str());
    let (_, again) = send(&router, "GET", &next_uri, None).await;
    assert_eq!(
        again["events"], batch["events"],
        "uncommitted batches must be re-served"
    );

    // Commit advances; the next read continues after the cursor.
    let commit_uri = format!("/api/v2/events/consumers/{name}/commit");
    let (status, committed) =
        send(&router, "POST", &commit_uri, Some(json!({ "cursor": e2 }))).await;
    assert_eq!(status, StatusCode::OK, "{committed}");
    assert_eq!(committed["cursor"], e2.as_str());
    let (_, rest) = send(&router, "GET", &next_uri, None).await;
    assert_eq!(rest["events"].as_array().expect("events").len(), 1);
    assert_eq!(rest["events"][0]["id"], e3.as_str());

    // Same-cursor commits are idempotent; regressions are conflicts.
    let (status, _) = send(&router, "POST", &commit_uri, Some(json!({ "cursor": e2 }))).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&router, "POST", &commit_uri, Some(json!({ "cursor": e1 }))).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Fully caught up: an empty batch echoes the committed cursor.
    let (status, _) = send(&router, "POST", &commit_uri, Some(json!({ "cursor": e3 }))).await;
    assert_eq!(status, StatusCode::OK);
    let (_, empty) = send(&router, "GET", &next_uri, None).await;
    assert_eq!(empty["events"], json!([]));
    assert_eq!(empty["next_cursor"], e3.as_str());

    // Listing shows the consumer; deletion is terminal.
    let (status, listed) = send(&router, "GET", "/api/v2/events/consumers", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        listed["consumers"]
            .as_array()
            .expect("consumers array")
            .iter()
            .any(|c| c["name"] == name.as_str())
    );
    let (status, _) = send(
        &router,
        "DELETE",
        &format!("/api/v2/events/consumers/{name}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = send(&router, "GET", &next_uri, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(&router, "POST", &commit_uri, Some(json!({ "cursor": e3 }))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Webhook delivery (in-process receiver)
// ---------------------------------------------------------------------------

/// One request as seen by the test receiver.
#[derive(Debug, Clone)]
struct ReceivedRequest {
    path: String,
    headers: HashMap<String, String>,
    body: String,
}

type Inbox = Arc<Mutex<Vec<ReceivedRequest>>>;

async fn receive(state: State<Inbox>, headers: HeaderMap, request: Request<Body>) -> StatusCode {
    let path = request.uri().path().to_owned();
    let body = request
        .into_body()
        .collect()
        .await
        .expect("read webhook body")
        .to_bytes();
    let headers: HashMap<String, String> = headers
        .iter()
        .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap_or("").to_owned()))
        .collect();
    let status = if path.ends_with("/fail") {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::OK
    };
    state.0.lock().expect("inbox lock").push(ReceivedRequest {
        path,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    });
    status
}

/// Binds an ephemeral-port receiver; returns its base URL and inbox.
async fn start_receiver() -> (String, Inbox) {
    let inbox: Inbox = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route("/ok", axum::routing::post(receive))
        .route("/fail", axum::routing::post(receive))
        .with_state(inbox.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind receiver");
    let addr = listener.local_addr().expect("receiver addr");
    tokio::spawn(async move {
        // The task dies with the test; failures here just end the server.
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), inbox)
}

/// Recomputes the expected signature independently of the server code
/// under test (same scheme: HMAC-SHA256 over `<timestamp>.<body>`).
fn expected_signature(secret: &str, timestamp: &str, body: &str) -> String {
    use hmac::Mac;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts any key length");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body.as_bytes());
    format!("v1={}", hex::encode(mac.finalize().into_bytes()))
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // one delivery story, told end to end
async fn webhooks_deliver_sign_retry_and_dead_letter() {
    let Some((router, pool, url)) = test_app().await else {
        return;
    };
    // Held for the whole test — see the feed test for why.
    let _serial = relay_test_lock(&url).await;
    let (receiver_url, inbox) = start_receiver().await;

    let feed_type = unique("test.delivery");
    let full_type = format!("com.meridian.{feed_type}");
    let secret_ok = "ok-secret-0123456789";
    let secret_fail = "fail-secret-0123456789";

    // One healthy endpoint, one that always fails.
    let (status, hook_ok) = send(
        &router,
        "POST",
        "/api/v2/webhooks",
        Some(json!({
            "url": format!("{receiver_url}/ok"),
            "secret": secret_ok,
            "event_types": [full_type]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{hook_ok}");
    let hook_ok_id = hook_ok["id"].as_str().expect("id").to_owned();
    let (status, hook_fail) = send(
        &router,
        "POST",
        "/api/v2/webhooks",
        Some(json!({
            "url": format!("{receiver_url}/fail"),
            "secret": secret_fail,
            "event_types": [full_type]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{hook_fail}");
    let hook_fail_id = hook_fail["id"].as_str().expect("id").to_owned();

    // Publish one matching event; the relay fans out two deliveries.
    let event_id = enqueue(&pool, &feed_type).await;
    relay_until_published(&pool, std::slice::from_ref(&event_id)).await;

    let config = EventsConfig {
        webhook_max_attempts: 2,
        webhook_retry_base_secs: 0, // retry immediately in tests
        webhook_timeout_secs: 5,
        ..EventsConfig::default()
    };
    let client = events::webhook_client(&config).expect("webhook client");

    // Drive the dispatcher until THIS test's healthy delivery is delivered.
    // dispatch_once is global, so counting attempts would race against any
    // other pending delivery in the shared table; poll our own endpoint for
    // the terminal state instead.
    let mut ok_delivery = Value::Null;
    for _ in 0..50 {
        events::dispatch_once(&pool, &client, &config)
            .await
            .expect("dispatch");
        let (status, ok_deliveries) = send(
            &router,
            "GET",
            &format!("/api/v2/webhooks/{hook_ok_id}/deliveries"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let first = ok_deliveries["deliveries"][0].clone();
        if first["status"] == "delivered" {
            ok_delivery = first;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        ok_delivery["status"], "delivered",
        "healthy webhook delivery must reach 'delivered'"
    );
    assert_eq!(ok_delivery["event_id"], event_id.as_str());
    assert_eq!(ok_delivery["last_status"], 200);
    assert_eq!(ok_delivery["event_type"], full_type.as_str());

    // Retry until dead-lettered (max_attempts = 2, zero backoff).
    for _ in 0..50 {
        let (_, fail_deliveries) = send(
            &router,
            "GET",
            &format!("/api/v2/webhooks/{hook_fail_id}/deliveries?status=dead"),
            None,
        )
        .await;
        if !fail_deliveries["deliveries"]
            .as_array()
            .expect("deliveries")
            .is_empty()
        {
            break;
        }
        events::dispatch_once(&pool, &client, &config)
            .await
            .expect("dispatch retry");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (status, dead) = send(
        &router,
        "GET",
        &format!("/api/v2/webhooks/{hook_fail_id}/deliveries?status=dead"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let dead_delivery = &dead["deliveries"][0];
    assert_eq!(dead_delivery["event_id"], event_id.as_str());
    assert_eq!(dead_delivery["status"], "dead");
    assert_eq!(dead_delivery["attempts"], 2);
    assert_eq!(dead_delivery["last_status"], 500);

    // The receiver saw one /ok request and two /fail requests, each a
    // signed CloudEvents document.
    let requests = inbox.lock().expect("inbox lock").clone();
    let ok_requests: Vec<&ReceivedRequest> = requests.iter().filter(|r| r.path == "/ok").collect();
    let fail_requests: Vec<&ReceivedRequest> =
        requests.iter().filter(|r| r.path == "/fail").collect();
    assert_eq!(ok_requests.len(), 1, "exactly one successful delivery");
    assert_eq!(fail_requests.len(), 2, "one attempt + one retry");

    let request = ok_requests[0];
    assert_eq!(
        request.headers.get("content-type").map(String::as_str),
        Some("application/cloudevents+json")
    );
    assert_eq!(
        request
            .headers
            .get("x-meridian-event-id")
            .map(String::as_str),
        Some(event_id.as_str())
    );
    assert_eq!(
        request
            .headers
            .get("x-meridian-event-type")
            .map(String::as_str),
        Some(full_type.as_str())
    );
    let timestamp = request
        .headers
        .get("x-meridian-timestamp")
        .expect("timestamp header");
    let signature = request
        .headers
        .get("x-meridian-signature")
        .expect("signature header");
    assert_eq!(
        signature,
        &expected_signature(secret_ok, timestamp, &request.body),
        "the documented verification recipe must reproduce the signature"
    );
    // The retried request was signed with the *other* endpoint's secret.
    let fail_ts = fail_requests[0]
        .headers
        .get("x-meridian-timestamp")
        .expect("timestamp header");
    assert_eq!(
        fail_requests[0]
            .headers
            .get("x-meridian-signature")
            .expect("signature"),
        &expected_signature(secret_fail, fail_ts, &fail_requests[0].body),
    );

    let event: Value = serde_json::from_str(&request.body).expect("CloudEvents JSON body");
    assert_eq!(event["specversion"], "1.0");
    assert_eq!(event["id"], event_id.as_str());
    assert_eq!(event["type"], full_type.as_str());
    assert_eq!(event["data"]["marker"], feed_type.as_str());

    // Cleanup: dead endpoints out of the way for other runs.
    for id in [&hook_ok_id, &hook_fail_id] {
        let (status, _) = send(&router, "DELETE", &format!("/api/v2/webhooks/{id}"), None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }
}
