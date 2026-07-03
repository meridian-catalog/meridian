//! Outbox relay integration tests: batch claiming under concurrency
//! (`FOR UPDATE SKIP LOCKED`), per-aggregate ordering, the publication
//! frontier, and the first-boot backlog drain.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they
//! skip (with a note on stderr).
//!
//! Relay claims are global (that is the point of the relay), so every test
//! that claims outbox rows — here and in the server's events tests —
//! serializes on a Postgres advisory lock ([`relay_test_lock`]) held on a
//! dedicated connection. Without it, two test binaries draining the same
//! database would race each other's ordering assertions.

use meridian_common::config::DatabaseConfig;
use meridian_store::outbox::{self, NewOutboxEvent};
use serde_json::json;
use sqlx::{Connection, PgConnection, PgPool};
use ulid::Ulid;

/// Advisory lock key shared by all relay-claiming tests across test
/// binaries (ASCII "RELAYTST" packed into an i64).
const RELAY_TEST_LOCK_KEY: i64 = 0x5245_4C41_5954_5354;

fn database_url() -> Option<String> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping relay test: DATABASE_URL is not set");
        return None;
    };
    Some(url)
}

async fn test_pool(url: &str) -> PgPool {
    let config = DatabaseConfig {
        url: url.to_owned(),
        ..DatabaseConfig::default()
    };
    let pool = meridian_store::connect(&config)
        .await
        .expect("connect to test database");
    meridian_store::MIGRATOR
        .run(&pool)
        .await
        .expect("run migrations");
    pool
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

fn test_event(aggregate: &str, event_type: &str) -> NewOutboxEvent {
    NewOutboxEvent {
        workspace_id: None,
        aggregate: aggregate.to_owned(),
        event_type: event_type.to_owned(),
        payload: json!({ "test": true }),
    }
}

async fn published_at(pool: &PgPool, id: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query_scalar("SELECT published_at FROM events_outbox WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read published_at")
}

/// Drains the current backlog: loops bounded batches until a claim comes
/// back empty. Returns the number of iterations.
async fn drain(pool: &PgPool) -> usize {
    let mut iterations = 0;
    loop {
        let published = outbox::relay_once(pool, 500).await.expect("relay_once");
        iterations += 1;
        assert!(published <= 500, "batches must stay bounded");
        if published == 0 {
            return iterations;
        }
        assert!(
            iterations < 1_000,
            "backlog drain did not terminate; something is re-enqueueing faster than we publish"
        );
    }
}

/// The first-boot story: thousands of pre-existing unpublished rows relay
/// cleanly in bounded batches, after which a marker event enqueued before
/// the drain is published and nothing older than it is left behind.
#[tokio::test]
async fn backlog_drains_in_bounded_batches() {
    let Some(url) = database_url() else {
        return;
    };
    let pool = test_pool(&url).await;
    let _lock = relay_test_lock(&url).await;

    let marker = outbox::enqueue(
        &pool,
        &test_event(&format!("test-drain:{}", Ulid::new()), "test.drain.marker"),
    )
    .await
    .expect("enqueue marker");

    drain(&pool).await;

    assert!(
        published_at(&pool, &marker).await.is_some(),
        "marker enqueued before the drain must be published"
    );
    let older_backlog: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events_outbox WHERE published_at IS NULL AND id < $1",
    )
    .bind(&marker)
    .fetch_one(&pool)
    .await
    .expect("count older backlog");
    assert_eq!(older_backlog, 0, "nothing older than the marker may remain");
}

/// Two concurrent claim transactions never claim the same row
/// (`FOR UPDATE SKIP LOCKED`), and together they cover the waiting events.
#[tokio::test]
async fn concurrent_claims_are_disjoint() {
    let Some(url) = database_url() else {
        return;
    };
    let pool = test_pool(&url).await;
    let _lock = relay_test_lock(&url).await;
    drain(&pool).await;

    // Six events on six distinct aggregates: the ordering guard never
    // filters them, so the claims split purely by SKIP LOCKED.
    let mut ours = Vec::new();
    for i in 0..6 {
        let id = outbox::enqueue(
            &pool,
            &test_event(&format!("test-claim:{}:{i}", Ulid::new()), "test.claim"),
        )
        .await
        .expect("enqueue");
        ours.push(id);
    }

    let mut tx1 = pool.begin().await.expect("begin tx1");
    let mut tx2 = pool.begin().await.expect("begin tx2");

    let batch1 = outbox::claim_batch(&mut tx1, 3).await.expect("claim tx1");
    let batch2 = outbox::claim_batch(&mut tx2, 500).await.expect("claim tx2");

    let ids1: Vec<&str> = batch1.iter().map(|e| e.id.as_str()).collect();
    let ids2: Vec<&str> = batch2.iter().map(|e| e.id.as_str()).collect();
    assert!(!batch1.is_empty(), "first claim must find the backlog");
    assert!(
        ids1.iter().all(|id| !ids2.contains(id)),
        "SKIP LOCKED claims must never overlap: {ids1:?} vs {ids2:?}"
    );
    for id in &ours {
        assert!(
            ids1.contains(&id.as_str()) || ids2.contains(&id.as_str()),
            "event {id} must be claimed by one of the two relays"
        );
    }

    // Publish both claims so later tests start from a drained state.
    let owned1: Vec<String> = ids1.iter().map(|s| (*s).to_owned()).collect();
    let owned2: Vec<String> = ids2.iter().map(|s| (*s).to_owned()).collect();
    outbox::mark_published(&mut tx1, &owned1)
        .await
        .expect("publish tx1");
    outbox::mark_published(&mut tx2, &owned2)
        .await
        .expect("publish tx2");
    tx1.commit().await.expect("commit tx1");
    tx2.commit().await.expect("commit tx2");
}

/// Per-aggregate ordering under concurrent relays: while an earlier event
/// of an aggregate is claimed by one relay (locked, unpublished), no other
/// relay may publish a later event of the same aggregate.
#[tokio::test]
async fn later_events_of_an_aggregate_wait_for_the_earlier_claim() {
    let Some(url) = database_url() else {
        return;
    };
    let pool = test_pool(&url).await;
    let _lock = relay_test_lock(&url).await;
    drain(&pool).await;

    let aggregate = format!("test-order:{}", Ulid::new());
    let e1 = outbox::enqueue(&pool, &test_event(&aggregate, "test.order.1"))
        .await
        .expect("enqueue e1");
    let e2 = outbox::enqueue(&pool, &test_event(&aggregate, "test.order.2"))
        .await
        .expect("enqueue e2");
    let e3 = outbox::enqueue(&pool, &test_event(&aggregate, "test.order.3"))
        .await
        .expect("enqueue e3");

    // Relay A claims e1 and stalls (open transaction, row locked).
    let mut relay_a = pool.begin().await.expect("begin relay A");
    let locked: Vec<String> =
        sqlx::query_scalar("SELECT id FROM events_outbox WHERE id = $1 FOR UPDATE")
            .bind(&e1)
            .fetch_all(&mut *relay_a)
            .await
            .expect("lock e1");
    assert_eq!(locked, vec![e1.clone()]);

    // Relay B claims everything it can: it must skip e2 and e3 (their
    // aggregate has an earlier unpublished event outside B's batch).
    let mut relay_b = pool.begin().await.expect("begin relay B");
    let batch_b = outbox::claim_batch(&mut relay_b, 500)
        .await
        .expect("claim B");
    let ids_b: Vec<&str> = batch_b.iter().map(|e| e.id.as_str()).collect();
    assert!(
        !ids_b.contains(&e1.as_str()),
        "e1 is locked by relay A and must be skipped"
    );
    assert!(
        !ids_b.contains(&e2.as_str()) && !ids_b.contains(&e3.as_str()),
        "e2/e3 must wait for e1: publishing them now would invert the aggregate's order"
    );
    relay_b.rollback().await.expect("rollback relay B");

    // Relay A crashes (rollback): e1 is unpublished again. The next relay
    // claims the aggregate's events in order.
    relay_a.rollback().await.expect("rollback relay A");

    let mut relay_c = pool.begin().await.expect("begin relay C");
    let batch_c = outbox::claim_batch(&mut relay_c, 500)
        .await
        .expect("claim C");
    let ids_c: Vec<&str> = batch_c.iter().map(|e| e.id.as_str()).collect();
    let pos = |id: &str| ids_c.iter().position(|x| *x == id);
    let (p1, p2, p3) = (pos(&e1), pos(&e2), pos(&e3));
    assert!(
        p1.is_some() && p2.is_some() && p3.is_some(),
        "all three events must be claimable once nothing is locked: {ids_c:?}"
    );
    assert!(p1 < p2 && p2 < p3, "claims must preserve aggregate order");
    let ids_c_owned: Vec<String> = ids_c.iter().map(|s| (*s).to_owned()).collect();
    outbox::mark_published(&mut relay_c, &ids_c_owned)
        .await
        .expect("publish");
    relay_c.commit().await.expect("commit relay C");
}

/// The feed's publication frontier: an event published *behind* a
/// still-unpublished earlier id stays invisible until the earlier id is
/// published, so keyset pagination never skips events.
#[tokio::test]
async fn feed_is_bounded_by_the_publication_frontier() {
    let Some(url) = database_url() else {
        return;
    };
    let pool = test_pool(&url).await;
    let _lock = relay_test_lock(&url).await;
    drain(&pool).await;

    let cursor_before = outbox::latest_cursor(&pool).await.expect("latest cursor");
    let feed_type = format!("test.frontier.{}", Ulid::new());
    let types = vec![feed_type.clone()];

    // Two events on *different* aggregates, so the ordering guard lets the
    // second one publish while the first is stuck.
    let g1 = outbox::enqueue(
        &pool,
        &test_event(&format!("test-f1:{}", Ulid::new()), &feed_type),
    )
    .await
    .expect("enqueue g1");
    let g2 = outbox::enqueue(
        &pool,
        &test_event(&format!("test-f2:{}", Ulid::new()), &feed_type),
    )
    .await
    .expect("enqueue g2");

    // A stuck relay holds g1; a healthy relay publishes g2.
    let mut stuck = pool.begin().await.expect("begin stuck relay");
    sqlx::query("SELECT id FROM events_outbox WHERE id = $1 FOR UPDATE")
        .bind(&g1)
        .execute(&mut *stuck)
        .await
        .expect("lock g1");
    drain(&pool).await;
    assert!(
        published_at(&pool, &g2).await.is_some(),
        "g2 (different aggregate) must publish while g1 is stuck"
    );

    // g2 is published but above the frontier (g1 < g2 is unpublished), so
    // the feed must not serve it yet.
    let page = outbox::list_published(&pool, &cursor_before, Some(&types), 10)
        .await
        .expect("list feed");
    assert!(
        page.is_empty(),
        "published-above-the-frontier events must stay invisible: {page:?}"
    );

    // Unstick g1; once published the feed serves both, in id order.
    stuck.rollback().await.expect("rollback stuck relay");
    drain(&pool).await;
    let page = outbox::list_published(&pool, &cursor_before, Some(&types), 10)
        .await
        .expect("list feed after unstick");
    let ids: Vec<&str> = page.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, vec![g1.as_str(), g2.as_str()]);

    // Keyset pagination: one at a time, cursor = last id of the page.
    let first = outbox::list_published(&pool, &cursor_before, Some(&types), 1)
        .await
        .expect("page 1");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].id, g1);
    let second = outbox::list_published(&pool, &first[0].id, Some(&types), 1)
        .await
        .expect("page 2");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].id, g2);
    let done = outbox::list_published(&pool, &second[0].id, Some(&types), 1)
        .await
        .expect("page 3");
    assert!(done.is_empty());
}
