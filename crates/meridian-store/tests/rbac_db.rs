//! Database-backed tests for RBAC: authorization resolution (deny by
//! default, direct vs role grants, hierarchy inheritance, built-in
//! roles), grant/role lifecycle, bootstrap, and the audit trail.
//!
//! These require a running Postgres and `DATABASE_URL`; without it they
//! skip (with a note on stderr). Every test creates uniquely-named
//! resources so tests are isolated from each other and from previous runs.

use std::collections::BTreeMap;

use meridian_common::MeridianError;
use meridian_common::config::DatabaseConfig;
use meridian_common::principal::{Principal, PrincipalKind};
use meridian_store::rbac::{self, AuthzError, Grantee, Privilege, SecurableScope, SecurableType};
use meridian_store::{namespace, principal, table, tenancy, warehouse};
use sqlx::PgPool;
use ulid::Ulid;

const ACTOR: &str = "test:rbac";
const TEST_ISSUER: &str = "https://idp.rbac-tests.example.com";

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
    format!("{prefix}-{}", Ulid::new().to_string().to_lowercase())
}

fn levels(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|p| (*p).to_owned()).collect()
}

async fn make_warehouse(pool: &PgPool) -> warehouse::WarehouseRecord {
    warehouse::create(
        pool,
        tenancy::default_workspace_id(),
        &unique("wh-rbac"),
        "s3://test-bucket/rbac",
        BTreeMap::new(),
        ACTOR,
    )
    .await
    .expect("create warehouse")
}

async fn make_namespace(
    pool: &PgPool,
    warehouse_id: &str,
    parts: &[&str],
) -> namespace::NamespaceRecord {
    namespace::create(
        pool,
        tenancy::default_workspace_id(),
        warehouse_id,
        &levels(parts),
        BTreeMap::new(),
        ACTOR,
    )
    .await
    .expect("create namespace")
}

async fn make_table(
    pool: &PgPool,
    namespace_id: &str,
    ns_levels: &[String],
    name: &str,
) -> table::TableRecord {
    table::create(
        pool,
        table::NewTable {
            workspace_id: tenancy::default_workspace_id(),
            namespace_id,
            namespace_levels: ns_levels,
            name,
            table_uuid: &uuid_string(),
            metadata_location: "s3://test-bucket/rbac/fake/metadata/00000-x.metadata.json",
            format_version: 2,
            properties: &BTreeMap::new(),
            schema_text: None,
            origin: "create",
        },
        ACTOR,
        None,
    )
    .await
    .expect("create table")
}

fn uuid_string() -> String {
    // A random UUID built from two ULIDs' randomness would be overkill;
    // ULID-to-UUID via bytes keeps the dependency set unchanged.
    uuid_from_ulid(Ulid::new())
}

fn uuid_from_ulid(ulid: Ulid) -> String {
    use std::fmt::Write as _;
    let bytes = ulid.to_bytes();
    let mut s = String::new();
    for (i, b) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Provisions (or fetches) a fresh test principal; returns (identity, row
/// id).
async fn make_principal(pool: &PgPool) -> (Principal, String) {
    let identity = Principal {
        kind: PrincipalKind::User,
        subject: unique("sub"),
        issuer: Some(TEST_ISSUER.to_owned()),
        display_name: None,
    };
    let record = principal::ensure(pool, tenancy::default_workspace_id(), &identity)
        .await
        .expect("provision principal");
    (identity, record.id)
}

/// Asserts an authorization outcome is a denial (not a store error).
fn assert_forbidden(result: Result<(), AuthzError>) {
    match result {
        Err(AuthzError::Forbidden(_)) => {}
        Err(AuthzError::Store(error)) => panic!("expected Forbidden, got store error: {error}"),
        Ok(()) => panic!("expected Forbidden, got Ok"),
    }
}

#[tokio::test]
async fn deny_by_default_direct_grant_is_scoped() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;
    let ns = make_namespace(&pool, &wh.id, &["a"]).await;
    let t1 = make_table(&pool, &ns.id, &ns.levels, "t1").await;
    let t2 = make_table(&pool, &ns.id, &ns.levels, "t2").await;
    let (caller, caller_id) = make_principal(&pool).await;

    let chain = rbac::namespace_chain(&pool, &wh.id, &ns.levels)
        .await
        .expect("chain");
    let t1_scope = SecurableScope::table(&wh.id, chain.clone(), Some(&t1.id));
    let t2_scope = SecurableScope::table(&wh.id, chain.clone(), Some(&t2.id));

    // Deny by default.
    assert_forbidden(rbac::authorize(&pool, &caller, Privilege::Read, &t1_scope).await);

    // A direct READ grant on t1 allows READ on t1 — and nothing else.
    rbac::create_grant(
        &pool,
        ws,
        &Grantee::Principal(caller_id),
        SecurableType::Table,
        &t1.id,
        Privilege::Read,
        ACTOR,
    )
    .await
    .expect("create grant");

    rbac::authorize(&pool, &caller, Privilege::Read, &t1_scope)
        .await
        .expect("READ on t1 is granted");
    assert_forbidden(rbac::authorize(&pool, &caller, Privilege::Commit, &t1_scope).await);
    assert_forbidden(rbac::authorize(&pool, &caller, Privilege::Read, &t2_scope).await);

    // The anonymous principal bypasses authorization (disabled mode).
    rbac::authorize(&pool, &Principal::anonymous(), Privilege::Commit, &t2_scope)
        .await
        .expect("anonymous bypasses authz");
}

#[tokio::test]
async fn role_grants_apply_through_bindings_only() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;
    let (caller, caller_id) = make_principal(&pool).await;

    let role = rbac::create_role(&pool, ws, &unique("analysts"), Some("test role"), ACTOR)
        .await
        .expect("create role");
    rbac::create_grant(
        &pool,
        ws,
        &Grantee::Role(role.id.clone()),
        SecurableType::Warehouse,
        &wh.id,
        Privilege::ListNamespaces,
        ACTOR,
    )
    .await
    .expect("grant to role");

    let scope = SecurableScope::warehouse(&wh.id);
    assert_forbidden(rbac::authorize(&pool, &caller, Privilege::ListNamespaces, &scope).await);

    let created = rbac::bind_role(&pool, ws, &role.id, &caller_id, ACTOR)
        .await
        .expect("bind");
    assert!(created, "first binding is created");
    rbac::authorize(&pool, &caller, Privilege::ListNamespaces, &scope)
        .await
        .expect("role grant applies once bound");

    // Idempotent re-bind.
    let rebound = rbac::bind_role(&pool, ws, &role.id, &caller_id, ACTOR)
        .await
        .expect("re-bind");
    assert!(!rebound, "re-binding is a no-op");

    rbac::unbind_role(&pool, ws, &role.id, &caller_id, ACTOR)
        .await
        .expect("unbind");
    assert_forbidden(rbac::authorize(&pool, &caller, Privilege::ListNamespaces, &scope).await);
}

#[tokio::test]
async fn hierarchy_grants_inherit_downward() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;
    let parent = make_namespace(&pool, &wh.id, &["a"]).await;
    let child = make_namespace(&pool, &wh.id, &["a", "b"]).await;
    let t = make_table(&pool, &child.id, &child.levels, "t").await;
    let (caller, caller_id) = make_principal(&pool).await;

    let child_chain = rbac::namespace_chain(&pool, &wh.id, &child.levels)
        .await
        .expect("chain");
    assert!(
        child_chain.contains(&parent.id) && child_chain.contains(&child.id),
        "chain must contain self and ancestors: {child_chain:?}"
    );
    let table_scope = SecurableScope::table(&wh.id, child_chain.clone(), Some(&t.id));

    // COMMIT granted on the warehouse reaches a table two levels down.
    rbac::create_grant(
        &pool,
        ws,
        &Grantee::Principal(caller_id.clone()),
        SecurableType::Warehouse,
        &wh.id,
        Privilege::Commit,
        ACTOR,
    )
    .await
    .expect("warehouse-level grant");
    rbac::authorize(&pool, &caller, Privilege::Commit, &table_scope)
        .await
        .expect("warehouse grant covers contained tables");

    // MANAGE_NAMESPACE granted on the parent namespace covers the child.
    let (second, second_id) = make_principal(&pool).await;
    rbac::create_grant(
        &pool,
        ws,
        &Grantee::Principal(second_id),
        SecurableType::Namespace,
        &parent.id,
        Privilege::ManageNamespace,
        ACTOR,
    )
    .await
    .expect("namespace-level grant");
    rbac::authorize(
        &pool,
        &second,
        Privilege::ManageNamespace,
        &SecurableScope::namespace(&wh.id, child_chain),
    )
    .await
    .expect("parent-namespace grant covers the child namespace");

    // ... but not a sibling namespace tree.
    let sibling = make_namespace(&pool, &wh.id, &["x"]).await;
    let sibling_chain = rbac::namespace_chain(&pool, &wh.id, &sibling.levels)
        .await
        .expect("sibling chain");
    assert!(!sibling_chain.contains(&parent.id));
    assert_forbidden(
        rbac::authorize(
            &pool,
            &second,
            Privilege::ManageNamespace,
            &SecurableScope::namespace(&wh.id, sibling_chain),
        )
        .await,
    );
}

#[tokio::test]
async fn built_in_roles_have_code_defined_semantics() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;
    let scope = SecurableScope::warehouse(&wh.id);

    // catalog_reader: read-only privileges everywhere, nothing else.
    let (reader, reader_id) = make_principal(&pool).await;
    let reader_role = rbac::get_role_by_name(&pool, ws, rbac::CATALOG_READER_ROLE)
        .await
        .expect("query")
        .expect("seeded catalog_reader role");
    assert!(reader_role.built_in);
    rbac::bind_role(&pool, ws, &reader_role.id, &reader_id, ACTOR)
        .await
        .expect("bind reader");
    rbac::authorize(&pool, &reader, Privilege::ListNamespaces, &scope)
        .await
        .expect("reader can list");
    rbac::authorize(&pool, &reader, Privilege::Read, &scope)
        .await
        .expect("reader can read");
    assert_forbidden(rbac::authorize(&pool, &reader, Privilege::Commit, &scope).await);
    assert_forbidden(rbac::authorize(&pool, &reader, Privilege::CreateNamespace, &scope).await);
    assert_forbidden(rbac::authorize_management(&pool, &reader).await);

    // admin: everything, including the management surface.
    let (admin, admin_id) = make_principal(&pool).await;
    let admin_role = rbac::get_role_by_name(&pool, ws, rbac::ADMIN_ROLE)
        .await
        .expect("query")
        .expect("seeded admin role");
    assert!(admin_role.built_in);
    rbac::bind_role(&pool, ws, &admin_role.id, &admin_id, ACTOR)
        .await
        .expect("bind admin");
    for privilege in Privilege::ALL {
        rbac::authorize(&pool, &admin, privilege, &scope)
            .await
            .unwrap_or_else(|e| panic!("admin must hold {privilege}: {e}"));
    }
    rbac::authorize_management(&pool, &admin)
        .await
        .expect("admin can manage RBAC");

    // Built-in roles cannot be deleted.
    let err = rbac::delete_role(&pool, ws, rbac::ADMIN_ROLE, ACTOR)
        .await
        .expect_err("deleting a built-in role must fail");
    assert!(matches!(err, MeridianError::Validation(_)), "{err}");
}

#[tokio::test]
async fn bootstrap_admin_is_idempotent_and_audited_once() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let subject = unique("bootstrap");

    rbac::bootstrap_admin(&pool, ws, TEST_ISSUER, &subject)
        .await
        .expect("first bootstrap");
    rbac::bootstrap_admin(&pool, ws, TEST_ISSUER, &subject)
        .await
        .expect("second bootstrap is a no-op");

    let row = principal::get_by_identity(&pool, TEST_ISSUER, &subject)
        .await
        .expect("query")
        .expect("bootstrap provisions the principal row");

    let bindings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM role_bindings rb
         JOIN roles r ON r.id = rb.role_id
         WHERE rb.principal_id = $1 AND r.name = 'admin'",
    )
    .bind(&row.id)
    .fetch_one(&pool)
    .await
    .expect("count bindings");
    assert_eq!(bindings, 1, "exactly one admin binding");

    let bind_audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_log
         WHERE action = 'role.bind'
           AND principal = 'system:bootstrap'
           AND details->>'principal_id' = $1",
    )
    .bind(&row.id)
    .fetch_one(&pool)
    .await
    .expect("count bind audits");
    assert_eq!(bind_audits, 1, "the no-op re-bootstrap must not re-audit");

    // And the identity actually is an admin now.
    let identity = Principal {
        kind: PrincipalKind::User,
        subject,
        issuer: Some(TEST_ISSUER.to_owned()),
        display_name: None,
    };
    rbac::authorize_management(&pool, &identity)
        .await
        .expect("bootstrapped identity is admin");
}

#[tokio::test]
async fn grant_lifecycle_uniqueness_validation_and_audit_trail() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;
    let (caller, caller_id) = make_principal(&pool).await;
    let scope = SecurableScope::warehouse(&wh.id);

    let grant = rbac::create_grant(
        &pool,
        ws,
        &Grantee::Principal(caller_id.clone()),
        SecurableType::Warehouse,
        &wh.id,
        Privilege::CreateNamespace,
        "user:granter",
    )
    .await
    .expect("create grant");

    // Duplicate grants conflict.
    let dup = rbac::create_grant(
        &pool,
        ws,
        &Grantee::Principal(caller_id.clone()),
        SecurableType::Warehouse,
        &wh.id,
        Privilege::CreateNamespace,
        "user:granter",
    )
    .await
    .expect_err("duplicate grant must conflict");
    assert!(matches!(dup, MeridianError::Conflict(_)), "{dup}");

    // A privilege cannot be granted below its native securable type.
    let invalid = rbac::create_grant(
        &pool,
        ws,
        &Grantee::Principal(caller_id.clone()),
        SecurableType::Table,
        &wh.id,
        Privilege::ManageWarehouse,
        "user:granter",
    )
    .await
    .expect_err("MANAGE_WAREHOUSE on a table must be rejected");
    assert!(matches!(invalid, MeridianError::Validation(_)), "{invalid}");

    // The grant works, then delete revokes it — both leave audit rows
    // recording the real granting principal.
    rbac::authorize(&pool, &caller, Privilege::CreateNamespace, &scope)
        .await
        .expect("granted");
    rbac::delete_grant(&pool, ws, &grant.id, "user:granter")
        .await
        .expect("delete grant");
    assert_forbidden(rbac::authorize(&pool, &caller, Privilege::CreateNamespace, &scope).await);

    let audit_actions: Vec<String> = sqlx::query_scalar(
        "SELECT action FROM audit_log
         WHERE resource = $1 AND principal = 'user:granter'
         ORDER BY seq",
    )
    .bind(format!("grant:{}", grant.id))
    .fetch_all(&pool)
    .await
    .expect("audit rows");
    assert_eq!(audit_actions, vec!["grant.create", "grant.delete"]);

    // The same trail exists in the outbox (same-transaction guarantee).
    let outbox_events: Vec<String> =
        sqlx::query_scalar("SELECT event_type FROM events_outbox WHERE aggregate = $1 ORDER BY id")
            .bind(format!("grant:{}", grant.id))
            .fetch_all(&pool)
            .await
            .expect("outbox rows");
    assert_eq!(outbox_events, vec!["grant.created", "grant.deleted"]);
}

#[tokio::test]
async fn deleting_a_role_revokes_its_grants_and_bindings() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;
    let (caller, caller_id) = make_principal(&pool).await;
    let scope = SecurableScope::warehouse(&wh.id);

    let role_name = unique("ephemeral");
    let role = rbac::create_role(&pool, ws, &role_name, None, ACTOR)
        .await
        .expect("create role");
    rbac::create_grant(
        &pool,
        ws,
        &Grantee::Role(role.id.clone()),
        SecurableType::Warehouse,
        &wh.id,
        Privilege::Read,
        ACTOR,
    )
    .await
    .expect("grant");
    rbac::bind_role(&pool, ws, &role.id, &caller_id, ACTOR)
        .await
        .expect("bind");
    rbac::authorize(&pool, &caller, Privilege::Read, &scope)
        .await
        .expect("granted via role");

    rbac::delete_role(&pool, ws, &role_name, ACTOR)
        .await
        .expect("delete role");
    assert_forbidden(rbac::authorize(&pool, &caller, Privilege::Read, &scope).await);
}

#[tokio::test]
async fn effective_permissions_report_direct_and_role_sources() {
    let Some(pool) = test_pool().await else {
        return;
    };
    let ws = tenancy::default_workspace_id();
    let wh = make_warehouse(&pool).await;
    let (_, caller_id) = make_principal(&pool).await;

    let role_name = unique("perm-role");
    let role = rbac::create_role(&pool, ws, &role_name, None, ACTOR)
        .await
        .expect("create role");
    rbac::bind_role(&pool, ws, &role.id, &caller_id, ACTOR)
        .await
        .expect("bind");
    rbac::create_grant(
        &pool,
        ws,
        &Grantee::Role(role.id.clone()),
        SecurableType::Warehouse,
        &wh.id,
        Privilege::ListNamespaces,
        ACTOR,
    )
    .await
    .expect("role grant");
    rbac::create_grant(
        &pool,
        ws,
        &Grantee::Principal(caller_id.clone()),
        SecurableType::Warehouse,
        &wh.id,
        Privilege::CreateNamespace,
        ACTOR,
    )
    .await
    .expect("direct grant");

    let effective = rbac::effective_permissions(&pool, &caller_id)
        .await
        .expect("effective permissions");
    assert!(effective.roles.contains(&role_name));

    let direct = effective
        .permissions
        .iter()
        .find(|p| p.privilege == "CREATE_NAMESPACE")
        .expect("direct grant listed");
    assert_eq!(direct.via_role, None);
    assert_eq!(direct.securable_id, wh.id);

    let via_role = effective
        .permissions
        .iter()
        .find(|p| p.privilege == "LIST_NAMESPACES")
        .expect("role grant listed");
    assert_eq!(via_role.via_role.as_deref(), Some(role_name.as_str()));
}
