//! End-to-end tests for the small-scan query executor against real Iceberg
//! fixtures (Parquet data files + manifest Avro in an in-memory store).
//!
//! Each test builds its own table under a unique location and asserts only on
//! its own rows, so the suite is isolated and order-independent.

mod support;

use meridian_authz::{ColumnMask, Enforcement, MaskKind, RowFilter, RowPredicate};
use meridian_query::{Caps, CatalogTable, GovernedTable, QueryError, run};
use serde_json::{Value, json};
use support::{
    DataFileSpec, Layout, MemStorage, PositionDeleteSpec, Row, build_fixture,
    build_fixture_with_equality_delete, empty_metadata,
};

/// Builds a one-file orders fixture with four rows spanning two regions.
fn orders_fixture(store: &MemStorage, location: &str) -> meridian_iceberg::spec::TableMetadata {
    let rows = [
        Row::new(1, "alice@x.com", "EU", 100),
        Row::new(2, "bob@y.com", "US", 200),
        Row::new(3, "carol@z.com", "EU", 300),
        Row::new(4, "dave@w.com", "US", 400),
    ];
    // Two files, one per region, so partitioning is exercised.
    let eu = DataFileSpec::new(
        &format!("{location}/data/eu.parquet"),
        "EU",
        vec![rows[0].clone(), rows[2].clone()],
        1_024,
    );
    let us = DataFileSpec::new(
        &format!("{location}/data/us.parquet"),
        "US",
        vec![rows[1].clone(), rows[3].clone()],
        1_024,
    );
    build_fixture(store, location, 2, &[eu, us], &[], 1, 1)
}

/// A helper: run a query over a single governed table with the given
/// enforcement and caps.
async fn run_one(
    sql: &str,
    metadata: &meridian_iceberg::spec::TableMetadata,
    storage: &MemStorage,
    name: &str,
    enforcement: Enforcement,
    caps: Caps,
) -> Result<meridian_query::QueryOutput, QueryError> {
    let table = GovernedTable {
        table: CatalogTable {
            name: name.to_owned(),
            metadata,
            storage,
        },
        enforcement,
    };
    run(sql, &[table], caps).await
}

/// Sorts result rows by the `id` column so assertions are order-independent
/// (`DataFusion` does not guarantee scan order without an ORDER BY).
fn rows_by_id(mut rows: Vec<Value>) -> Vec<Value> {
    rows.sort_by_key(|r| r.get("id").and_then(Value::as_i64).unwrap_or(i64::MAX));
    rows
}

// ---------------------------------------------------------------------------
// 1. SELECT with projection / filter / aggregate -> correct rows + provenance.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn select_projection_and_filter_returns_correct_rows_and_provenance() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_pf");

    let out = run_one(
        "SELECT id, amount FROM orders WHERE region = 'EU' ORDER BY id",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        Caps::default(),
    )
    .await
    .expect("query runs");

    // Projection: only id + amount columns.
    let col_names: Vec<&str> = out.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(col_names, vec!["id", "amount"]);

    // Filter: only the two EU rows.
    assert_eq!(
        out.rows,
        vec![
            json!({"id": 1, "amount": 100}),
            json!({"id": 3, "amount": 300})
        ]
    );
    assert!(!out.truncated);

    // Provenance: the one table, its snapshot id, no policies.
    assert_eq!(out.provenance.tables.len(), 1);
    let t = &out.provenance.tables[0];
    assert_eq!(t.table, "orders");
    assert_eq!(t.snapshot_id, Some(1));
    assert!(out.provenance.row_filter_policies.is_empty());
    assert!(out.provenance.masked_columns.is_empty());

    // Cost estimate: two files at 1024 bytes each, four rows.
    assert_eq!(out.bytes_scanned, 2_048);
    assert_eq!(out.rows_scanned, 4);
}

/// A table referenced by a **qualified** name (`namespace.table`) is registered
/// under that reference, so the user's `FROM namespace.table` resolves to the
/// governed view — the shape the agent gateway / workbench passes (they refer to
/// tables by their catalog-qualified name).
#[tokio::test]
async fn qualified_table_name_resolves_and_is_governed() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_qualified");

    // Register as `sales.orders`; the SQL references it the same way. A drop
    // mask on `email` must still apply through the qualified view — `SELECT *`
    // sees every column EXCEPT the dropped one.
    let out = run_one(
        "SELECT * FROM sales.orders WHERE region = 'EU' ORDER BY id",
        &meta,
        &store,
        "sales.orders",
        Enforcement {
            row_filters: vec![],
            column_masks: vec![meridian_authz::ColumnMask::new(
                "email",
                meridian_authz::MaskKind::Drop,
                "p",
            )],
        },
        Caps::default(),
    )
    .await
    .expect("qualified query runs");

    // The masked `email` column is absent (dropped); the rest remain.
    let col_names: Vec<&str> = out.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(
        !col_names.contains(&"email"),
        "masked `email` absent from a qualified SELECT *: {col_names:?}"
    );
    assert!(col_names.contains(&"id") && col_names.contains(&"region"));
    // The row filter in the SQL still applied (only EU rows).
    assert!(
        out.rows.iter().all(|r| r["region"] == "EU"),
        "the query's WHERE is honored through the qualified view"
    );
    // Provenance carries the qualified name the query used + the mask.
    assert_eq!(out.provenance.tables[0].table, "sales.orders");
    assert_eq!(out.provenance.masked_columns, vec!["email".to_owned()]);
}

/// Defense-in-depth: the raw, ungoverned table is materialized only in a
/// private context and never registered where the user's SQL runs. A query
/// that tries to read the internal raw name must fail to resolve it — the
/// no-leak property holds by construction, not by caller pre-validation.
#[tokio::test]
async fn internal_raw_table_name_is_unreachable_from_user_sql() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_raw_probe");

    let result = run_one(
        "SELECT * FROM __meridian_raw__0",
        &meta,
        &store,
        "orders",
        Enforcement {
            row_filters: vec![],
            column_masks: vec![meridian_authz::ColumnMask::new(
                "email",
                meridian_authz::MaskKind::Drop,
                "p",
            )],
        },
        Caps::default(),
    )
    .await;

    // The internal name resolves to nothing in the user context; the query
    // errors rather than returning raw, unmasked rows.
    assert!(
        result.is_err(),
        "the internal raw table name must not be queryable: {result:?}"
    );
}

#[tokio::test]
async fn select_aggregate_groups_correctly() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_agg");

    let out = run_one(
        "SELECT region, SUM(amount) AS total, COUNT(*) AS n FROM orders GROUP BY region ORDER BY region",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        Caps::default(),
    )
    .await
    .expect("query runs");

    assert_eq!(
        out.rows,
        vec![
            json!({"region": "EU", "total": 400, "n": 2}),
            json!({"region": "US", "total": 600, "n": 2}),
        ]
    );
}

#[tokio::test]
async fn maps_columns_by_field_id_across_reversed_layout() {
    // One file has physically reversed columns; results must still be correct,
    // proving field-id (not positional) mapping.
    let store = MemStorage::default();
    let rows_eu = vec![
        Row::new(1, "a@x.com", "EU", 100),
        Row::new(3, "c@z.com", "EU", 300),
    ];
    let rows_us = vec![Row::new(2, "b@y.com", "US", 200)];
    let eu = DataFileSpec::new("s3://wh/orders_rev/data/eu.parquet", "EU", rows_eu, 1_024)
        .with_layout(Layout::Reversed);
    let us = DataFileSpec::new("s3://wh/orders_rev/data/us.parquet", "US", rows_us, 1_024);
    let meta = build_fixture(&store, "s3://wh/orders_rev", 2, &[eu, us], &[], 1, 1);

    let out = run_one(
        "SELECT id, email, region, amount FROM orders",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        Caps::default(),
    )
    .await
    .expect("query runs");

    let rows = rows_by_id(out.rows);
    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "email": "a@x.com", "region": "EU", "amount": 100}),
            json!({"id": 2, "email": "b@y.com", "region": "US", "amount": 200}),
            json!({"id": 3, "email": "c@z.com", "region": "EU", "amount": 300}),
        ]
    );
}

#[tokio::test]
async fn schema_evolution_synthesizes_null_for_missing_column() {
    // A file predating the `amount` column must read as null `amount`.
    let store = MemStorage::default();
    let rows = vec![
        Row::new(1, "a@x.com", "EU", 0),
        Row::new(2, "b@y.com", "EU", 0),
    ];
    let old = DataFileSpec::new("s3://wh/orders_evo/data/old.parquet", "EU", rows, 1_024)
        .with_layout(Layout::NoAmount);
    let meta = build_fixture(&store, "s3://wh/orders_evo", 2, &[old], &[], 1, 1);

    let out = run_one(
        "SELECT id, amount FROM orders ORDER BY id",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        Caps::default(),
    )
    .await
    .expect("query runs");

    assert_eq!(
        out.rows,
        vec![
            json!({"id": 1, "amount": null}),
            json!({"id": 2, "amount": null})
        ]
    );
}

// ---------------------------------------------------------------------------
// 2. Row-filter policy -> filtered rows.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn row_filter_policy_restricts_rows() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_rowpol");

    // Policy: this principal may see only EU rows.
    let enforcement = Enforcement {
        row_filters: vec![RowFilter::new(
            "policy-eu-only",
            RowPredicate::Eq {
                column: "region".to_owned(),
                value: json!("EU"),
            },
        )],
        column_masks: vec![],
    };

    let out = run_one(
        "SELECT id, region FROM orders",
        &meta,
        &store,
        "orders",
        enforcement,
        Caps::default(),
    )
    .await
    .expect("query runs");

    // Even a `SELECT *`-style read only sees EU rows — the US rows are invisible.
    let rows = rows_by_id(out.rows);
    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "region": "EU"}),
            json!({"id": 3, "region": "EU"})
        ]
    );
    // Provenance records the row-filter policy id.
    assert_eq!(out.provenance.row_filter_policies, vec!["policy-eu-only"]);
}

#[tokio::test]
async fn multiple_row_filters_are_conjoined() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_and");

    // Two policies: EU only AND amount >= 300. Only id=3 qualifies.
    let enforcement = Enforcement {
        row_filters: vec![
            RowFilter::new(
                "p-eu",
                RowPredicate::Eq {
                    column: "region".to_owned(),
                    value: json!("EU"),
                },
            ),
            RowFilter::new(
                "p-big",
                RowPredicate::GtEq {
                    column: "amount".to_owned(),
                    value: json!(300),
                },
            ),
        ],
        column_masks: vec![],
    };

    let out = run_one(
        "SELECT id FROM orders",
        &meta,
        &store,
        "orders",
        enforcement,
        Caps::default(),
    )
    .await
    .expect("query runs");

    assert_eq!(out.rows, vec![json!({"id": 3})]);
    let mut policies = out.provenance.row_filter_policies.clone();
    policies.sort();
    assert_eq!(policies, vec!["p-big", "p-eu"]);
}

// ---------------------------------------------------------------------------
// 3. Column mask -> masked / absent column.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn column_mask_partial_reveals_only_prefix() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_partial");

    // Partial-mask email: reveal first 1 char only.
    let enforcement = Enforcement {
        row_filters: vec![],
        column_masks: vec![ColumnMask::new(
            "email",
            MaskKind::Partial {
                show_first: 1,
                show_last: 0,
            },
            "policy-mask-email",
        )],
    };

    let out = run_one(
        "SELECT id, email FROM orders WHERE id = 1",
        &meta,
        &store,
        "orders",
        enforcement,
        Caps::default(),
    )
    .await
    .expect("query runs");

    // The raw email must not appear; only the masked form.
    assert_eq!(out.rows, vec![json!({"id": 1, "email": "a***"})]);
    assert_eq!(out.provenance.masked_columns, vec!["email"]);
    assert_eq!(
        out.provenance.column_mask_policies,
        vec!["policy-mask-email"]
    );
}

#[tokio::test]
async fn column_mask_hash_is_stable_and_hides_value() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_hash");

    let enforcement = Enforcement {
        row_filters: vec![],
        column_masks: vec![ColumnMask::new("email", MaskKind::Hash, "p-hash")],
    };

    let out = run_one(
        "SELECT email FROM orders WHERE id = 1",
        &meta,
        &store,
        "orders",
        enforcement,
        Caps::default(),
    )
    .await
    .expect("query runs");

    let email = out.rows[0].get("email").and_then(Value::as_str).unwrap();
    // Not the raw value, and a 64-hex-char sha256 digest.
    assert_ne!(email, "alice@x.com");
    assert_eq!(email.len(), 64);
    assert!(email.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
async fn column_mask_drop_makes_column_absent_not_null() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_drop");

    // Drop the email column entirely (H-F2: absent, not nulled).
    let enforcement = Enforcement {
        row_filters: vec![],
        column_masks: vec![ColumnMask::new("email", MaskKind::Drop, "p-drop")],
    };

    // `SELECT *` must not include email at all.
    let out = run_one(
        "SELECT * FROM orders WHERE id = 1",
        &meta,
        &store,
        "orders",
        enforcement.clone(),
        Caps::default(),
    )
    .await
    .expect("query runs");
    let col_names: Vec<&str> = out.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(
        !col_names.contains(&"email"),
        "dropped column leaked: {col_names:?}"
    );
    assert_eq!(out.provenance.masked_columns, vec!["email"]);

    // And referencing the dropped column by name must be a clean error — its
    // very existence is hidden, so schema of restricted data cannot leak.
    let err = run_one(
        "SELECT email FROM orders",
        &meta,
        &store,
        "orders",
        enforcement,
        Caps::default(),
    )
    .await
    .expect_err("referencing a dropped column errors");
    assert!(
        matches!(err, QueryError::InvalidSql(_)),
        "unexpected: {err:?}"
    );
    assert!(err.is_caller_refusal());
}

// ---------------------------------------------------------------------------
// 4. Oversized-scan cap -> polite refusal, before execution.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn oversized_scan_is_refused_up_front() {
    let store = MemStorage::default();
    // Two files at 1024 bytes each = 2048 bytes total.
    let meta = orders_fixture(&store, "s3://wh/orders_cap");

    // Cap below the estimate: refuse.
    let caps = Caps::with_max_scan_bytes(1_000);
    let err = run_one(
        "SELECT * FROM orders",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        caps,
    )
    .await
    .expect_err("oversized scan refused");

    match err {
        QueryError::ScanTooLarge {
            requested_bytes,
            limit_bytes,
            file_count,
        } => {
            assert_eq!(requested_bytes, 2_048);
            assert_eq!(limit_bytes, 1_000);
            assert_eq!(file_count, 2);
        }
        other => panic!("expected ScanTooLarge, got {other:?}"),
    }
    // The refusal is a caller-facing answer, and the message names the escape
    // hatch (a registered engine) so an agent can relay it.
    assert!(err.is_caller_refusal());
    assert!(err.to_string().contains("registered"));
}

#[tokio::test]
async fn row_cap_is_refused_up_front() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_rowcap");

    let caps = Caps {
        max_scan_rows: 3, // fixture has 4 rows
        ..Caps::default()
    };
    let err = run_one(
        "SELECT * FROM orders",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        caps,
    )
    .await
    .expect_err("too many rows refused");
    assert!(matches!(
        err,
        QueryError::TooManyRows {
            requested_rows: 4,
            limit_rows: 3
        }
    ));
}

#[tokio::test]
async fn scan_within_cap_succeeds() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_undercap");

    // Cap exactly at the estimate: allowed (cap is a `>` refusal).
    let caps = Caps::with_max_scan_bytes(2_048);
    let out = run_one(
        "SELECT COUNT(*) AS n FROM orders",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        caps,
    )
    .await
    .expect("at-cap query runs");
    assert_eq!(out.rows, vec![json!({"n": 4})]);
}

// ---------------------------------------------------------------------------
// 5. Malformed / non-read SQL -> clean errors.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn malformed_sql_returns_clean_invalid_error() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_bad");

    // Unknown column.
    let err = run_one(
        "SELECT nonesuch FROM orders",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        Caps::default(),
    )
    .await
    .expect_err("unknown column errors");
    assert!(matches!(err, QueryError::InvalidSql(_)), "got {err:?}");
    assert!(err.is_caller_refusal());

    // Syntactically broken SQL.
    let err = run_one(
        "SELECT FROM WHERE",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        Caps::default(),
    )
    .await
    .expect_err("broken sql errors");
    assert!(matches!(err, QueryError::InvalidSql(_)), "got {err:?}");
}

#[tokio::test]
async fn non_read_statements_are_refused() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_write");

    for sql in [
        "INSERT INTO orders VALUES (5, 'x', 'EU', 1)",
        "UPDATE orders SET amount = 0",
        "DELETE FROM orders",
        "DROP TABLE orders",
        "CREATE TABLE t (a INT)",
        "SELECT 1; SELECT 2",
    ] {
        let err = run_one(
            sql,
            &meta,
            &store,
            "orders",
            Enforcement::none(),
            Caps::default(),
        )
        .await
        .expect_err("non-read refused");
        assert!(
            matches!(
                err,
                QueryError::NotReadOnly { .. } | QueryError::InvalidSql(_)
            ),
            "sql {sql:?} gave {err:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// 6. Edge cases: empty table, merge-on-read deletes, result truncation.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn duplicate_table_name_is_rejected() {
    // Two governed tables under the same query name is ambiguous (which
    // enforcement applies?) — reject rather than silently shadow one.
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_dup");
    let a = GovernedTable {
        table: CatalogTable {
            name: "orders".to_owned(),
            metadata: &meta,
            storage: &store,
        },
        enforcement: Enforcement::none(),
    };
    let b = GovernedTable {
        table: CatalogTable {
            name: "orders".to_owned(),
            metadata: &meta,
            storage: &store,
        },
        enforcement: Enforcement::none(),
    };
    let err = run("SELECT * FROM orders", &[a, b], Caps::default())
        .await
        .expect_err("duplicate name rejected");
    assert!(
        matches!(err, QueryError::UnqueryableTable { .. }),
        "got {err:?}"
    );
}

#[tokio::test]
async fn empty_table_returns_no_rows() {
    let store = MemStorage::default();
    let meta = empty_metadata("s3://wh/orders_empty");

    let out = run_one(
        "SELECT * FROM orders",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        Caps::default(),
    )
    .await
    .expect("empty table queryable");
    assert!(out.rows.is_empty());
    assert_eq!(out.bytes_scanned, 0);
    // Provenance still lists the table, with a null snapshot id.
    assert_eq!(out.provenance.tables.len(), 1);
    assert_eq!(out.provenance.tables[0].snapshot_id, None);
}

#[tokio::test]
async fn position_deletes_are_materialized() {
    // A position-delete file removes id=3 (row index 1 of the EU file). The
    // query must not return it — a governed query must never surface a deleted
    // row.
    let store = MemStorage::default();
    let eu_path = "s3://wh/orders_del/data/eu.parquet".to_owned();
    let eu = DataFileSpec {
        path: eu_path.clone(),
        region: "EU".to_owned(),
        rows: vec![
            Row::new(1, "a@x.com", "EU", 100),
            Row::new(3, "c@z.com", "EU", 300),
        ],
        size_bytes: 1_024,
        sequence_number: 1,
        snapshot_id: 1,
        layout: Layout::Normal,
    };
    let del = PositionDeleteSpec {
        path: "s3://wh/orders_del/data/eu-delete.parquet".to_owned(),
        region: "EU".to_owned(),
        deletes: vec![(eu_path, 1)], // delete row at position 1 (id=3)
        sequence_number: 2,
        snapshot_id: 1,
    };
    let meta = build_fixture(&store, "s3://wh/orders_del", 2, &[eu], &[del], 1, 2);

    let out = run_one(
        "SELECT id FROM orders ORDER BY id",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        Caps::default(),
    )
    .await
    .expect("query runs");
    assert_eq!(out.rows, vec![json!({"id": 1})]);
}

#[tokio::test]
async fn equality_deletes_are_materialized() {
    // An equality-delete file keyed on id removes id=2. The query must not
    // return it.
    let store = MemStorage::default();
    let data = DataFileSpec::new(
        "s3://wh/orders_eqdel/data/d.parquet",
        "EU",
        vec![
            Row::new(1, "a@x.com", "EU", 100),
            Row::new(2, "b@x.com", "EU", 200),
            Row::new(3, "c@x.com", "EU", 300),
        ],
        1_024,
    );
    // Delete file at a strictly-later sequence number than the data (2 > 1).
    let meta = build_fixture_with_equality_delete(&store, "s3://wh/orders_eqdel", &data, &[2], 2);

    let out = run_one(
        "SELECT id FROM orders ORDER BY id",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        Caps::default(),
    )
    .await
    .expect("query runs");
    assert_eq!(out.rows, vec![json!({"id": 1}), json!({"id": 3})]);
}

#[tokio::test]
async fn result_truncation_is_flagged() {
    let store = MemStorage::default();
    let meta = orders_fixture(&store, "s3://wh/orders_trunc");

    // Cap the result at 2 rows though 4 match.
    let caps = Caps {
        max_result_rows: 2,
        ..Caps::default()
    };
    let out = run_one(
        "SELECT id FROM orders",
        &meta,
        &store,
        "orders",
        Enforcement::none(),
        caps,
    )
    .await
    .expect("query runs");
    assert_eq!(out.rows.len(), 2);
    assert!(out.truncated);
}

// ---------------------------------------------------------------------------
// 7. Multi-table: a join across two governed tables, each with its own policy.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn join_across_two_tables_applies_each_policy() {
    let store = MemStorage::default();
    let orders = orders_fixture(&store, "s3://wh/join_orders");
    // A second table under a different name, same schema, different data.
    let regions_rows = vec![
        Row::new(10, "eu-owner@x.com", "EU", 1),
        Row::new(20, "us-owner@x.com", "US", 1),
    ];
    let regions_file = DataFileSpec::new(
        "s3://wh/join_regions/data/r.parquet",
        "EU",
        regions_rows,
        512,
    );
    let regions = build_fixture(
        &store,
        "s3://wh/join_regions",
        2,
        &[regions_file],
        &[],
        2,
        1,
    );

    let orders_gt = GovernedTable {
        table: CatalogTable {
            name: "orders".to_owned(),
            metadata: &orders,
            storage: &store,
        },
        // Orders restricted to EU.
        enforcement: Enforcement {
            row_filters: vec![RowFilter::new(
                "orders-eu",
                RowPredicate::Eq {
                    column: "region".to_owned(),
                    value: json!("EU"),
                },
            )],
            column_masks: vec![],
        },
    };
    let regions_gt = GovernedTable {
        table: CatalogTable {
            name: "region_owners".to_owned(),
            metadata: &regions,
            storage: &store,
        },
        enforcement: Enforcement::none(),
    };

    let out = run(
        "SELECT o.id, r.email AS owner FROM orders o \
         JOIN region_owners r ON o.region = r.region ORDER BY o.id",
        &[orders_gt, regions_gt],
        Caps::default(),
    )
    .await
    .expect("join runs");

    // Only EU orders (1, 3) survive the orders policy, joined to the EU owner.
    assert_eq!(
        out.rows,
        vec![
            json!({"id": 1, "owner": "eu-owner@x.com"}),
            json!({"id": 3, "owner": "eu-owner@x.com"}),
        ]
    );
    // Provenance lists both tables.
    let names: Vec<&str> = out
        .provenance
        .tables
        .iter()
        .map(|t| t.table.as_str())
        .collect();
    assert!(names.contains(&"orders") && names.contains(&"region_owners"));
    assert_eq!(out.provenance.row_filter_policies, vec!["orders-eu"]);
}
