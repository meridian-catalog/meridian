//! OIDC authentication integration tests.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they
//! skip (with a note on stderr). Each test spins up an in-process "IdP":
//! an axum server on an ephemeral port serving an OIDC discovery document
//! and a JWKS whose keys can be rotated mid-test, plus helpers to mint
//! RS256 tokens with arbitrary claims.

mod idp;

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use idp::{AUDIENCE, KID1, KID2, TestIdp};
use meridian_common::AppConfig;
use meridian_common::config::{AuthMode, OidcIssuerConfig};
use meridian_server::{AppState, build_router};
use serde_json::{Value, json};
use sqlx::PgPool;
use tower::ServiceExt;
use ulid::Ulid;

/// Connects, migrates, and builds a router with the given auth config
/// applied. Returns `None` (skipping the test) without `DATABASE_URL`.
async fn test_router(configure: impl FnOnce(&mut AppConfig)) -> Option<(Router, PgPool)> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping auth integration test: DATABASE_URL is not set");
        return None;
    };

    let mut config = AppConfig::default();
    config.database.url = url;
    configure(&mut config);

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

/// An OIDC-mode router trusting the given test IdP.
async fn oidc_router(idp: &TestIdp) -> Option<(Router, PgPool)> {
    let issuer_url = idp.issuer.clone();
    test_router(move |config| {
        config.auth.mode = AuthMode::Oidc;
        // The in-process IdP is plain http; the config layer would reject
        // it without the (warned-about) test opt-out.
        config.auth.oidc.require_https_issuers = false;
        config.auth.oidc.issuers.push(OidcIssuerConfig {
            issuer_url,
            audience: AUDIENCE.to_owned(),
            // Deliberately absent: exercises discovery via
            // /.well-known/openid-configuration.
            jwks_uri: None,
        });
    })
    .await
}

/// Sends one request; returns status and parsed JSON body (Null when
/// empty).
async fn send(
    router: Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<&Value>,
) -> (StatusCode, Value, axum::http::HeaderMap) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let request = match body {
        Some(body) => builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())),
        None => builder.body(Body::empty()),
    }
    .expect("build request");

    let response = router.oneshot(request).await.expect("infallible router");
    let status = response.status();
    let headers = response.headers().clone();
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
    (status, value, headers)
}

/// Creates a warehouse (the simplest audited mutation) and returns its
/// unique name.
async fn create_warehouse(router: Router, token: Option<&str>) -> String {
    let name = format!("wh-auth-{}", Ulid::new()).to_lowercase();
    let body = json!({
        "name": name,
        "storage_root": format!("file:///tmp/meridian-auth-tests/{name}"),
    });
    let (status, response, _) =
        send(router, "POST", "/api/v2/warehouses", token, Some(&body)).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "unexpected response: {response}"
    );
    name
}

/// Provisions the token's principal (one authenticated request against the
/// authorization-exempt config endpoint, so JIT provisioning records the
/// token's own kind/display name) and then grants it the admin role.
/// Needed since RBAC landed: oidc mode denies mutations by default.
async fn provision_and_make_admin(
    router: &Router,
    pool: &PgPool,
    idp: &TestIdp,
    token: &str,
    sub: &str,
) {
    let (status, body, _) = send(router.clone(), "GET", "/v1/config", Some(token), None).await;
    assert_eq!(status, StatusCode::OK, "provisioning request: {body}");

    meridian_store::rbac::bootstrap_admin(
        pool,
        meridian_store::tenancy::default_workspace_id(),
        &idp.issuer,
        sub,
    )
    .await
    .expect("grant admin role");
}

/// The audit principal recorded for a warehouse create, found by name.
async fn audit_principal_for_warehouse(pool: &PgPool, name: &str) -> String {
    sqlx::query_scalar(
        "SELECT principal FROM audit_log
         WHERE action = 'warehouse.create' AND details->>'name' = $1",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("audit row for warehouse create")
}

#[tokio::test]
async fn valid_user_token_authenticates_and_audits_as_user() {
    let idp = TestIdp::start(&[KID1]).await;
    let Some((router, pool)) = oidc_router(&idp).await else {
        return;
    };

    let sub = format!("auth0|user-{}", Ulid::new());
    let token = idp::mint(
        KID1,
        &idp.claims(
            &sub,
            json!({ "email": "alice@example.com", "preferred_username": "alice" }),
        ),
    );

    provision_and_make_admin(&router, &pool, &idp, &token, &sub).await;
    let name = create_warehouse(router.clone(), Some(&token)).await;
    assert_eq!(
        audit_principal_for_warehouse(&pool, &name).await,
        format!("user:{sub}"),
        "audit rows must record the authenticated user principal"
    );

    // The JIT-provisioned row is visible on the management surface.
    let (status, body, _) = send(router, "GET", "/api/v2/principals", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    let entry = body["principals"]
        .as_array()
        .expect("principals array")
        .iter()
        .find(|p| p["subject"] == json!(sub))
        .cloned()
        .expect("JIT-provisioned principal row");
    assert_eq!(entry["kind"], json!("user"));
    assert_eq!(entry["issuer"], json!(idp.issuer));
    // preferred_username wins the display-name preference order.
    assert_eq!(entry["display_name"], json!("alice"));
}

#[tokio::test]
async fn client_credentials_token_authenticates_as_service() {
    let idp = TestIdp::start(&[KID1]).await;
    let Some((router, pool)) = oidc_router(&idp).await else {
        return;
    };

    // No email/preferred_username: client-credentials-style identity.
    let sub = format!("svc-{}", Ulid::new());
    let token = idp::mint(KID1, &idp.claims(&sub, json!({ "client_id": "spark-etl" })));

    provision_and_make_admin(&router, &pool, &idp, &token, &sub).await;
    let name = create_warehouse(router.clone(), Some(&token)).await;
    assert_eq!(
        audit_principal_for_warehouse(&pool, &name).await,
        format!("service:{sub}")
    );

    let (status, body, _) = send(router, "GET", "/api/v2/principals", Some(&token), None).await;
    assert_eq!(status, StatusCode::OK);
    let entry = body["principals"]
        .as_array()
        .expect("principals array")
        .iter()
        .find(|p| p["subject"] == json!(sub))
        .cloned()
        .expect("service principal row");
    assert_eq!(entry["kind"], json!("service"));
    assert_eq!(entry["display_name"], json!("spark-etl"));
}

#[tokio::test]
async fn bad_tokens_are_rejected_with_the_irc_envelope() {
    let idp = TestIdp::start(&[KID1]).await;
    let Some((router, _pool)) = oidc_router(&idp).await else {
        return;
    };

    let now = chrono::Utc::now().timestamp();
    let sub = "auth0|mallory";

    let expired = idp::mint(KID1, &{
        let mut claims = idp.claims(sub, json!({}));
        claims["exp"] = json!(now - 7200); // far past any clock skew
        claims
    });
    let wrong_audience = idp::mint(KID1, &{
        let mut claims = idp.claims(sub, json!({}));
        claims["aud"] = json!("some-other-api");
        claims
    });
    let wrong_issuer = idp::mint(KID1, &{
        let mut claims = idp.claims(sub, json!({}));
        claims["iss"] = json!("https://not-configured.example.com");
        claims
    });
    // Header names KID1 but the signature comes from key 2.
    let bad_signature = idp::mint_with_key(KID1, idp::KEY2_PEM, &idp.claims(sub, json!({})));

    let cases: Vec<(&str, Option<&str>)> = vec![
        ("missing token", None),
        ("garbage token", Some("not.a.jwt")),
        ("expired token", Some(&expired)),
        ("wrong audience", Some(&wrong_audience)),
        ("unknown issuer", Some(&wrong_issuer)),
        ("bad signature", Some(&bad_signature)),
    ];

    for (case, token) in cases {
        let (status, body, headers) = send(router.clone(), "GET", "/v1/config", token, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "case {case:?}: {body}");
        assert_eq!(
            body["error"]["type"],
            json!("NotAuthorizedException"),
            "case {case:?} must use the IRC envelope: {body}"
        );
        assert_eq!(body["error"]["code"], json!(401), "case {case:?}");
        assert!(
            headers.contains_key("www-authenticate"),
            "case {case:?} must carry the RFC 6750 challenge"
        );
    }
}

#[tokio::test]
async fn unknown_kid_triggers_jwks_refresh_and_backoff() {
    let idp = TestIdp::start(&[KID1]).await;
    let Some((router, _pool)) = oidc_router(&idp).await else {
        return;
    };

    // Let the boot prefetch land so the first request does not consume the
    // on-demand refresh budget.
    idp.wait_for_jwks_hits(1).await;
    let sub = format!("rotate-{}", Ulid::new());

    let token1 = idp::mint(KID1, &idp.claims(&sub, json!({})));
    let (status, body, _) = send(router.clone(), "GET", "/v1/config", Some(&token1), None).await;
    assert_eq!(status, StatusCode::OK, "pre-rotation token: {body}");
    let hits_before = idp.jwks_hits();

    // Rotate: the IdP now also signs with key 2. A token with the new kid
    // must trigger exactly one JWKS refresh and then validate.
    idp.set_keys(&[KID1, KID2]);
    let token2 = idp::mint(KID2, &idp.claims(&sub, json!({})));
    let (status, body, _) = send(router.clone(), "GET", "/v1/config", Some(&token2), None).await;
    assert_eq!(status, StatusCode::OK, "post-rotation token: {body}");
    assert_eq!(
        idp.jwks_hits(),
        hits_before + 1,
        "unknown kid must refresh the JWKS exactly once"
    );

    // A still-unknown kid within the refresh back-off window is rejected
    // without another fetch (rotation floods cannot hammer the IdP).
    let unknown = idp::mint_with_key("no-such-kid", idp::KEY2_PEM, &idp.claims(&sub, json!({})));
    let (status, body, _) = send(router, "GET", "/v1/config", Some(&unknown), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(
        idp.jwks_hits(),
        hits_before + 1,
        "refreshes within the back-off window must be skipped"
    );
}

#[tokio::test]
async fn disabled_mode_still_audits_as_anonymous() {
    let Some((router, pool)) = test_router(|_| {}).await else {
        return;
    };

    let name = create_warehouse(router, None).await;
    assert_eq!(
        audit_principal_for_warehouse(&pool, &name).await,
        "anonymous",
        "disabled-mode audit strings must stay byte-identical to the pre-auth behavior"
    );
}

#[tokio::test]
async fn health_probes_stay_open_in_oidc_mode() {
    let idp = TestIdp::start(&[KID1]).await;
    let Some((router, _pool)) = oidc_router(&idp).await else {
        return;
    };

    for path in ["/healthz", "/readyz"] {
        let (status, body, _) = send(router.clone(), "GET", path, None, None).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{path} must not require a token: {body}"
        );
    }

    // ... while everything else does.
    let (status, _, _) = send(router, "GET", "/v1/config", None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn jit_provisioning_creates_one_row_under_concurrent_first_requests() {
    let idp = TestIdp::start(&[KID1]).await;
    let Some((router, pool)) = oidc_router(&idp).await else {
        return;
    };

    let sub = format!("concurrent-{}", Ulid::new());
    let token = idp::mint(
        KID1,
        &idp.claims(&sub, json!({ "email": "race@example.com" })),
    );

    // /v1/config authenticates (which JIT-provisions) but is exempt from
    // authorization, so a grantless principal still gets a 200.
    let requests = (0..8).map(|_| {
        let router = router.clone();
        let token = token.clone();
        async move { send(router, "GET", "/v1/config", Some(&token), None).await }
    });
    for (status, body, _) in futures_join_all(requests).await {
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    let rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM principals WHERE subject = $1 AND issuer = $2")
            .bind(&sub)
            .bind(&idp.issuer)
            .fetch_one(&pool)
            .await
            .expect("count principals");
    assert_eq!(rows, 1, "exactly one principals row per identity");

    let provision_audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_log
         WHERE action = 'principal.provision' AND details->>'subject' = $1",
    )
    .bind(&sub)
    .fetch_one(&pool)
    .await
    .expect("count provision audits");
    assert_eq!(
        provision_audits, 1,
        "provisioning must be audited exactly once"
    );
}

/// Minimal `join_all` (avoids a futures-crate dev-dependency): polls the
/// futures sequentially-spawned as tasks, preserving order.
async fn futures_join_all<F, T>(futures: impl IntoIterator<Item = F>) -> Vec<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let handles: Vec<_> = futures.into_iter().map(tokio::spawn).collect();
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await.expect("request task panicked"));
    }
    results
}
