//! Database-backed tests for the sharing store (Pillar J, J-F1): shares,
//! grants, terms acceptance, and revocation, plus their audit + outbox
//! invariant.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they skip.
//! Each test uses uniquely-named objects and scopes its assertions to its own
//! ids, so the suite is isolated from other tests and prior runs.

use meridian_common::config::DatabaseConfig;
use meridian_store::shares::{self, NewShare};
use meridian_store::tenancy;
use sqlx::PgPool;
use ulid::Ulid;

const PRINCIPAL: &str = "test:sharing";

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

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", Ulid::new().to_string().to_lowercase())
}

async fn audit_and_outbox_counts(pool: &PgPool, resource: &str, aggregate: &str) -> (i64, i64) {
    let audit: i64 = sqlx::query_scalar("SELECT count(*) FROM audit_log WHERE resource = $1")
        .bind(resource)
        .fetch_one(pool)
        .await
        .expect("count audit");
    let outbox: i64 = sqlx::query_scalar("SELECT count(*) FROM events_outbox WHERE aggregate = $1")
        .bind(aggregate)
        .fetch_one(pool)
        .await
        .expect("count outbox");
    (audit, outbox)
}

#[tokio::test]
async fn share_create_grant_revoke_writes_audit_and_outbox() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();

    let name = unique("share");
    let token = unique("tok");
    let share = shares::create_share(
        &pool,
        ws,
        NewShare {
            name: &name,
            recipient: "org:acme",
            token: &token,
            terms: None,
        },
        PRINCIPAL,
    )
    .await
    .expect("create share");
    assert_eq!(share.recipient, "org:acme");
    assert!(!share.revoked);
    assert!(share.is_servable(), "no terms => immediately servable");

    let resource = format!("share:{}", share.id);
    let (audit, outbox) = audit_and_outbox_counts(&pool, &resource, &resource).await;
    assert_eq!(audit, 1, "create writes exactly one audit row");
    assert_eq!(outbox, 1, "create writes exactly one outbox event");

    // The token is a bearer secret and must never appear in the audit payload.
    let leaked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_log WHERE resource = $1 AND details::text LIKE $2",
    )
    .bind(&resource)
    .bind(format!("%{token}%"))
    .fetch_one(&pool)
    .await
    .expect("scan audit for token");
    assert_eq!(
        leaked, 0,
        "the share token must not leak into the audit log"
    );

    // Add a grant with a row filter + column mask.
    let grant = shares::add_grant(
        &pool,
        ws,
        &share.id,
        "table",
        "table:demo",
        Some("region = 'EU'"),
        Some(&["ssn".to_owned(), "email".to_owned()]),
        PRINCIPAL,
    )
    .await
    .expect("add grant");
    assert_eq!(grant.row_filter.as_deref(), Some("region = 'EU'"));
    assert_eq!(
        grant.column_mask.as_ref().map(|m| m.0.clone()),
        Some(vec!["ssn".to_owned(), "email".to_owned()])
    );

    // Idempotent: re-adding the same securable returns the same row, no new audit.
    let again = shares::add_grant(
        &pool,
        ws,
        &share.id,
        "table",
        "table:demo",
        None,
        None,
        PRINCIPAL,
    )
    .await
    .expect("re-add grant");
    assert_eq!(again.id, grant.id, "re-grant is idempotent");

    let (audit_after_grant, _) = audit_and_outbox_counts(&pool, &resource, &resource).await;
    assert_eq!(
        audit_after_grant, 2,
        "create + one grant = 2 audit rows (idempotent re-grant writes nothing)"
    );

    let grants = shares::list_share_grants(&pool, &share.id)
        .await
        .expect("list grants");
    assert_eq!(grants.len(), 1, "the securable is granted once");

    // Revoke: idempotent, and flips is_servable.
    let revoked = shares::revoke_share(&pool, ws, &share.id, PRINCIPAL)
        .await
        .expect("revoke");
    assert!(revoked.revoked);
    assert!(revoked.revoked_at.is_some());
    assert!(!revoked.is_servable(), "revoked => not servable");

    let revoked_again = shares::revoke_share(&pool, ws, &share.id, PRINCIPAL)
        .await
        .expect("re-revoke is a no-op success");
    assert!(revoked_again.revoked);

    let (audit_after_revoke, _) = audit_and_outbox_counts(&pool, &resource, &resource).await;
    assert_eq!(
        audit_after_revoke, 3,
        "create + grant + one revoke = 3 (idempotent re-revoke writes nothing)"
    );
}

#[tokio::test]
async fn share_lookup_by_token_and_terms_gate() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();

    let token = unique("tok");
    let share = shares::create_share(
        &pool,
        ws,
        NewShare {
            name: &unique("share"),
            recipient: "org:partner",
            token: &token,
            terms: Some("Read-only. No redistribution."),
        },
        PRINCIPAL,
    )
    .await
    .expect("create share with terms");
    assert!(
        share.needs_terms_acceptance(),
        "a share with terms starts un-accepted"
    );
    assert!(!share.is_servable(), "un-accepted terms => not servable");

    // Token lookup resolves the share.
    let by_token = shares::get_share_by_token(&pool, &token)
        .await
        .expect("lookup by token")
        .expect("share found by token");
    assert_eq!(by_token.id, share.id);

    // Accept terms: idempotent, flips servability, writes a recipient-attributed
    // audit row.
    let accepted = shares::accept_terms(&pool, &share.id)
        .await
        .expect("accept terms");
    assert!(accepted.terms_accepted_at.is_some());
    assert!(accepted.is_servable(), "accepted terms => servable");

    let accepted_again = shares::accept_terms(&pool, &share.id)
        .await
        .expect("re-accept is a no-op success");
    assert_eq!(
        accepted_again.terms_accepted_at, accepted.terms_accepted_at,
        "the acceptance timestamp does not move on re-accept"
    );

    // A share with no terms cannot "accept".
    let no_terms = shares::create_share(
        &pool,
        ws,
        NewShare {
            name: &unique("share"),
            recipient: "org:x",
            token: &unique("tok"),
            terms: None,
        },
        PRINCIPAL,
    )
    .await
    .expect("create no-terms share");
    let err = shares::accept_terms(&pool, &no_terms.id).await;
    assert!(
        err.is_err(),
        "accepting terms on a no-terms share is an error"
    );

    // An unknown token resolves to None (the recipient endpoint 404s).
    let missing = shares::get_share_by_token(&pool, &unique("nope"))
        .await
        .expect("lookup missing token");
    assert!(missing.is_none());
}
