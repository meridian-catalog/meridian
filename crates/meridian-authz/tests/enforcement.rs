//! Row-filter / column-mask resolution and compilation tests (D-F2).
//!
//! These verify the wave-2 contract: resolution picks the right filters and
//! masks for a `(principal, table)`, layered policies compose (filters
//! AND-ed, strongest mask per column wins), and a [`RowFilter`] compiles to
//! the exact IRC [`Expression`](meridian_iceberg::expr::Expression) JSON the
//! scan-plan seam consumes.

use meridian_authz::enforcement::{Enforcement, MaskKind, RowFilter, RowPredicate};
use meridian_authz::{
    AbacRule, AuthzPrincipal, AuthzResource, PrincipalKind, ResolvedColumn, ResourceKind,
    resolve_filters_and_masks,
};
use serde_json::json;

fn principal(id: &str, groups: &[&str]) -> AuthzPrincipal {
    let mut p = AuthzPrincipal::new(id, PrincipalKind::User);
    for g in groups {
        p = p.with_group(*g);
    }
    p
}

fn table_with_tags(id: &str, tags: &[&str]) -> AuthzResource {
    let mut t = AuthzResource::new(id, ResourceKind::Table);
    for tag in tags {
        t = t.with_tag(*tag);
    }
    t
}

// ---------------------------------------------------------------------------
// RowFilter -> Expression compilation (exact IRC JSON shapes)
// ---------------------------------------------------------------------------

#[test]
fn row_predicate_compiles_to_exact_irc_json() {
    // region == "eu"
    let filter = RowFilter::new(
        "eu-only",
        RowPredicate::Eq {
            column: "region".to_owned(),
            value: json!("eu"),
        },
    );
    let expr = filter.to_expression();
    assert_eq!(
        serde_json::to_value(&expr).unwrap(),
        json!({"type": "eq", "term": "region", "value": "eu"})
    );
}

#[test]
fn compound_row_predicate_compiles() {
    // region == "eu" AND deleted != true
    let pred = RowPredicate::And {
        left: Box::new(RowPredicate::Eq {
            column: "region".to_owned(),
            value: json!("eu"),
        }),
        right: Box::new(RowPredicate::NotEq {
            column: "deleted".to_owned(),
            value: json!(true),
        }),
    };
    let expr = RowFilter::new("p", pred).to_expression();
    assert_eq!(
        serde_json::to_value(&expr).unwrap(),
        json!({
            "type": "and",
            "left": {"type": "eq", "term": "region", "value": "eu"},
            "right": {"type": "not-eq", "term": "deleted", "value": true}
        })
    );
}

#[test]
fn every_row_predicate_variant_compiles_to_the_right_type() {
    let cases: Vec<(RowPredicate, serde_json::Value)> = vec![
        (RowPredicate::True, json!({"type": "true"})),
        (RowPredicate::False, json!({"type": "false"})),
        (
            RowPredicate::Lt {
                column: "amount".into(),
                value: json!(100),
            },
            json!({"type": "lt", "term": "amount", "value": 100}),
        ),
        (
            RowPredicate::LtEq {
                column: "amount".into(),
                value: json!(100),
            },
            json!({"type": "lt-eq", "term": "amount", "value": 100}),
        ),
        (
            RowPredicate::Gt {
                column: "amount".into(),
                value: json!(0),
            },
            json!({"type": "gt", "term": "amount", "value": 0}),
        ),
        (
            RowPredicate::GtEq {
                column: "amount".into(),
                value: json!(0),
            },
            json!({"type": "gt-eq", "term": "amount", "value": 0}),
        ),
        (
            RowPredicate::In {
                column: "region".into(),
                values: vec![json!("eu"), json!("uk")],
            },
            json!({"type": "in", "term": "region", "values": ["eu", "uk"]}),
        ),
        (
            RowPredicate::NotIn {
                column: "region".into(),
                values: vec![json!("us")],
            },
            json!({"type": "not-in", "term": "region", "values": ["us"]}),
        ),
        (
            RowPredicate::IsNull {
                column: "deleted_at".into(),
            },
            json!({"type": "is-null", "term": "deleted_at"}),
        ),
        (
            RowPredicate::NotNull {
                column: "deleted_at".into(),
            },
            json!({"type": "not-null", "term": "deleted_at"}),
        ),
    ];
    for (pred, expected) in cases {
        let expr = pred.to_expression();
        assert_eq!(
            serde_json::to_value(&expr).unwrap(),
            expected,
            "predicate {pred:?} compiled to the wrong IRC JSON"
        );
    }
}

#[test]
fn compiled_expression_binds_against_a_real_schema() {
    // The whole point: the compiled Expression is a valid IRC filter that
    // the planner can bind. Round-trip through meridian_iceberg::expr::bind.
    use meridian_iceberg::expr::Expression;
    use meridian_iceberg::spec::{PrimitiveType, Schema, StructField, Type};

    let schema = Schema::new(vec![
        StructField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
        StructField::optional(2, "region", Type::Primitive(PrimitiveType::String)),
    ]);

    let pred = RowPredicate::Eq {
        column: "region".into(),
        value: json!("eu"),
    };
    let expr: Expression = pred.to_expression();
    // Bind must succeed and resolve the column/type.
    let bound = expr.bind(&schema, true);
    assert!(bound.is_ok(), "compiled filter binds against the schema");
}

// ---------------------------------------------------------------------------
// Enforcement folding
// ---------------------------------------------------------------------------

#[test]
fn no_filters_yields_no_row_predicate() {
    let e = Enforcement::none();
    assert!(e.is_empty());
    assert!(e.row_predicate().is_none());
}

#[test]
fn single_filter_is_its_own_predicate() {
    let mut e = Enforcement::none();
    e.row_filters.push(RowFilter::new(
        "p",
        RowPredicate::Eq {
            column: "region".into(),
            value: json!("eu"),
        },
    ));
    let expr = e.row_predicate().unwrap();
    assert_eq!(
        serde_json::to_value(&expr).unwrap(),
        json!({"type": "eq", "term": "region", "value": "eu"})
    );
}

#[test]
fn multiple_filters_are_anded_together() {
    let mut e = Enforcement::none();
    e.row_filters.push(RowFilter::new(
        "a",
        RowPredicate::Eq {
            column: "region".into(),
            value: json!("eu"),
        },
    ));
    e.row_filters.push(RowFilter::new(
        "b",
        RowPredicate::IsNull {
            column: "deleted_at".into(),
        },
    ));
    let expr = e.row_predicate().unwrap();
    assert_eq!(
        serde_json::to_value(&expr).unwrap(),
        json!({
            "type": "and",
            "left": {"type": "eq", "term": "region", "value": "eu"},
            "right": {"type": "is-null", "term": "deleted_at"}
        }),
        "layered row policies compose as a conjunction"
    );
}

// ---------------------------------------------------------------------------
// Mask strength / normalization
// ---------------------------------------------------------------------------

#[test]
fn strongest_mask_per_column_wins() {
    use meridian_authz::ColumnMask;
    let mut e = Enforcement::none();
    // Two policies mask `ssn`: one Partial, one Drop. Drop must win.
    e.column_masks.push(ColumnMask::new(
        "ssn",
        MaskKind::Partial {
            show_first: 0,
            show_last: 4,
        },
        "weak",
    ));
    e.column_masks
        .push(ColumnMask::new("ssn", MaskKind::Drop, "strong"));
    e.column_masks
        .push(ColumnMask::new("email", MaskKind::Hash, "email-hash"));
    e.normalize();

    // One mask per column now.
    assert_eq!(e.column_masks.len(), 2);
    let ssn = e.column_masks.iter().find(|m| m.column == "ssn").unwrap();
    assert_eq!(ssn.kind, MaskKind::Drop, "Drop outranks Partial");
    assert_eq!(ssn.source_policy, "strong");
    let email = e.column_masks.iter().find(|m| m.column == "email").unwrap();
    assert_eq!(email.kind, MaskKind::Hash);
}

#[test]
fn mask_strength_total_order() {
    // Drop > Hash > Null > Custom > Partial.
    assert!(MaskKind::Drop.strength() > MaskKind::Hash.strength());
    assert!(MaskKind::Hash.strength() > MaskKind::Null.strength());
    assert!(
        MaskKind::Null.strength()
            > MaskKind::Custom {
                expression: "x".into()
            }
            .strength()
    );
    assert!(
        MaskKind::Custom {
            expression: "x".into()
        }
        .strength()
            > MaskKind::Partial {
                show_first: 0,
                show_last: 4
            }
            .strength()
    );
}

// ---------------------------------------------------------------------------
// resolve_filters_and_masks: the full D-F2 resolution
// ---------------------------------------------------------------------------

#[test]
fn resolves_row_filter_when_table_carries_tag() {
    let rules = vec![AbacRule::TagRowFilter {
        id: Some("eu-residency".into()),
        description: Some("non-EU staff see only EU rows".into()),
        tag: "residency:eu".into(),
        exempt_groups: vec!["eu_staff".into()],
        predicate: RowPredicate::Eq {
            column: "region".into(),
            value: json!("eu"),
        },
    }];
    let table = table_with_tags("sales.orders", &["residency:eu"]);

    // A non-exempt principal gets the filter.
    let e = resolve_filters_and_masks(&principal("alice", &[]), &table, &[], &rules);
    assert_eq!(e.row_filters.len(), 1);
    assert_eq!(e.row_filters[0].source_policy, "eu-residency");
    let expr = e.row_predicate().unwrap();
    assert_eq!(
        serde_json::to_value(&expr).unwrap(),
        json!({"type": "eq", "term": "region", "value": "eu"})
    );

    // An exempt principal gets nothing.
    let e = resolve_filters_and_masks(&principal("bob", &["eu_staff"]), &table, &[], &rules);
    assert!(e.is_empty(), "exempt group sees everything");
}

#[test]
fn row_filter_skipped_when_table_lacks_tag() {
    let rules = vec![AbacRule::TagRowFilter {
        id: Some("eu-residency".into()),
        description: None,
        tag: "residency:eu".into(),
        exempt_groups: vec![],
        predicate: RowPredicate::Eq {
            column: "region".into(),
            value: json!("eu"),
        },
    }];
    let table = table_with_tags("sales.orders", &["finance"]); // different tag
    let e = resolve_filters_and_masks(&principal("alice", &[]), &table, &[], &rules);
    assert!(e.is_empty());
}

#[test]
fn resolves_column_mask_for_tagged_columns_only() {
    let rules = vec![AbacRule::TagColumnMask {
        id: Some("mask-email".into()),
        description: None,
        tag: "pii:email".into(),
        exempt_groups: vec!["pii_readers".into()],
        mask: MaskKind::Hash,
    }];
    let table = table_with_tags("users.profile", &[]);
    let columns = vec![
        ResolvedColumn::new("email", vec!["pii:email".to_owned()]),
        ResolvedColumn::new("name", vec![]),
        ResolvedColumn::new("backup_email", vec!["pii:email".to_owned()]),
    ];

    // Non-exempt principal: both pii:email columns masked, `name` untouched.
    let e = resolve_filters_and_masks(&principal("alice", &[]), &table, &columns, &rules);
    let masked = e.masked_columns();
    assert_eq!(masked.len(), 2);
    assert!(masked.contains(&"email".to_owned()));
    assert!(masked.contains(&"backup_email".to_owned()));
    assert!(!masked.contains(&"name".to_owned()));
    for m in &e.column_masks {
        assert_eq!(m.kind, MaskKind::Hash);
        assert_eq!(m.source_policy, "mask-email");
    }

    // Exempt principal: no masks.
    let e = resolve_filters_and_masks(
        &principal("carol", &["pii_readers"]),
        &table,
        &columns,
        &rules,
    );
    assert!(e.column_masks.is_empty());
}

#[test]
fn layered_policies_compose_filters_and_masks_together() {
    // A realistic layered set: EU residency row filter + email mask + a
    // stronger drop mask on ssn from a second policy.
    let rules = vec![
        AbacRule::TagRowFilter {
            id: Some("eu-residency".into()),
            description: None,
            tag: "residency:eu".into(),
            exempt_groups: vec![],
            predicate: RowPredicate::Eq {
                column: "region".into(),
                value: json!("eu"),
            },
        },
        AbacRule::TagRowFilter {
            id: Some("active-only".into()),
            description: None,
            tag: "soft_delete".into(),
            exempt_groups: vec![],
            predicate: RowPredicate::IsNull {
                column: "deleted_at".into(),
            },
        },
        AbacRule::TagColumnMask {
            id: Some("mask-email".into()),
            description: None,
            tag: "pii:email".into(),
            exempt_groups: vec![],
            mask: MaskKind::Hash,
        },
        AbacRule::TagColumnMask {
            id: Some("drop-ssn".into()),
            description: None,
            tag: "pii:high".into(),
            exempt_groups: vec![],
            mask: MaskKind::Drop,
        },
    ];
    let table = table_with_tags("sales.orders", &["residency:eu", "soft_delete"]);
    let columns = vec![
        ResolvedColumn::new("email", vec!["pii:email".to_owned()]),
        ResolvedColumn::new("ssn", vec!["pii:high".to_owned()]),
        ResolvedColumn::new("region", vec![]),
    ];

    let e = resolve_filters_and_masks(&principal("alice", &[]), &table, &columns, &rules);

    // Two row filters AND-ed.
    assert_eq!(e.row_filters.len(), 2);
    let expr = e.row_predicate().unwrap();
    assert_eq!(
        serde_json::to_value(&expr).unwrap(),
        json!({
            "type": "and",
            "left": {"type": "eq", "term": "region", "value": "eu"},
            "right": {"type": "is-null", "term": "deleted_at"}
        })
    );

    // Two masks, one per column.
    assert_eq!(e.column_masks.len(), 2);
    let email = e.column_masks.iter().find(|m| m.column == "email").unwrap();
    assert_eq!(email.kind, MaskKind::Hash);
    let ssn = e.column_masks.iter().find(|m| m.column == "ssn").unwrap();
    assert_eq!(ssn.kind, MaskKind::Drop);
}

#[test]
fn resolution_is_deterministic_and_order_stable() {
    // Masks come out sorted by column name regardless of input order.
    let rules = vec![AbacRule::TagColumnMask {
        id: Some("m-z".into()),
        description: None,
        tag: "pii".into(),
        exempt_groups: vec![],
        mask: MaskKind::Null,
    }];
    let table = table_with_tags("t", &[]);
    let columns = vec![
        ResolvedColumn::new("zebra", vec!["pii".to_owned()]),
        ResolvedColumn::new("apple", vec!["pii".to_owned()]),
        ResolvedColumn::new("mango", vec!["pii".to_owned()]),
    ];
    let e = resolve_filters_and_masks(&principal("p", &[]), &table, &columns, &rules);
    let cols: Vec<&str> = e.column_masks.iter().map(|m| m.column.as_str()).collect();
    assert_eq!(
        cols,
        vec!["apple", "mango", "zebra"],
        "masks sorted by column"
    );
}
