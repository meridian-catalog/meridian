//! Database-backed integration tests.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip
//! (with a note on stderr) so the suite stays runnable offline.

use meridian_common::config::DatabaseConfig;
use meridian_store::audit::{self, NewAuditEntry};
use meridian_store::outbox::{self, NewOutboxEvent};
use serde_json::json;
use sqlx::PgPool;

async fn test_pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping DB test: DATABASE_URL is not set");
        return None;
    };
    let config = DatabaseConfig {
        url,
        ..DatabaseConfig::default()
    };
    let pool = meridian_store::connect(&config)
        .await
        .expect("connect to test database");
    meridian_store::MIGRATOR
        .run(&pool)
        .await
        .expect("run migrations");
    Some(pool)
}

#[tokio::test]
async fn health_check_succeeds() {
    let Some(pool) = test_pool().await else {
        return;
    };
    meridian_store::health_check(&pool)
        .await
        .expect("health check");
}

#[tokio::test]
async fn outbox_enqueue_inserts_unpublished_event() {
    let Some(pool) = test_pool().await else {
        return;
    };

    let event = NewOutboxEvent {
        workspace_id: None,
        aggregate: "test:outbox".to_owned(),
        event_type: "test.enqueued".to_owned(),
        payload: json!({"n": 1}),
    };
    // Enqueue and read back on one uncommitted transaction: a concurrent
    // relay (other test binaries drain the outbox) cannot see or publish
    // the row, so the freshly-enqueued state is observable race-free.
    let mut tx = pool.begin().await.expect("begin");
    let id = outbox::enqueue(&mut *tx, &event).await.expect("enqueue");

    let (event_type, published): (String, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT event_type, published_at FROM events_outbox WHERE id = $1")
            .bind(&id)
            .fetch_one(&mut *tx)
            .await
            .expect("read back event");
    assert_eq!(event_type, "test.enqueued");
    assert!(published.is_none(), "new events must be unpublished");
    tx.rollback().await.expect("rollback");
}

#[tokio::test]
async fn audit_append_builds_verifiable_chain() {
    let Some(pool) = test_pool().await else {
        return;
    };

    for i in 0..3 {
        let record = audit::append(
            &pool,
            NewAuditEntry {
                workspace_id: None,
                principal: "test:chain".to_owned(),
                action: format!("test.append.{i}"),
                resource: "test:audit".to_owned(),
                details: json!({ "i": i }),
            },
        )
        .await
        .expect("append audit entry");

        // Other tests in this binary append to the same audit_log
        // concurrently, so our own previous append is not necessarily the
        // predecessor. Link against the actual predecessor row: the greatest
        // committed seq below ours (identity sequences may have gaps from
        // aborted transactions, so `seq - 1` would be wrong too).
        let predecessor: Option<(String,)> =
            sqlx::query_as("SELECT hash FROM audit_log WHERE seq < $1 ORDER BY seq DESC LIMIT 1")
                .bind(record.seq)
                .fetch_optional(&pool)
                .await
                .expect("fetch predecessor row");
        match predecessor {
            Some((prev_hash,)) => assert_eq!(
                record.prev_hash.as_deref(),
                Some(prev_hash.as_str()),
                "entry must link to its actual predecessor"
            ),
            None => assert!(
                record.prev_hash.is_none(),
                "genesis entry must have no predecessor"
            ),
        }
        assert_eq!(record.hash.len(), 64);
    }

    // The full chain (including rows from previous test runs) must verify.
    let checked = audit::verify_chain(&pool).await.expect("verify chain");
    assert!(checked >= 3);
}

#[tokio::test]
async fn audit_log_rejects_updates_and_deletes() {
    let Some(pool) = test_pool().await else {
        return;
    };

    let record = audit::append(
        &pool,
        NewAuditEntry {
            workspace_id: None,
            principal: "test:immutability".to_owned(),
            action: "test.tamper".to_owned(),
            resource: "test:audit".to_owned(),
            details: json!({}),
        },
    )
    .await
    .expect("append audit entry");

    let update = sqlx::query("UPDATE audit_log SET principal = 'evil' WHERE seq = $1")
        .bind(record.seq)
        .execute(&pool)
        .await;
    assert!(update.is_err(), "UPDATE on audit_log must be rejected");

    let delete = sqlx::query("DELETE FROM audit_log WHERE seq = $1")
        .bind(record.seq)
        .execute(&pool)
        .await;
    assert!(delete.is_err(), "DELETE on audit_log must be rejected");
}
