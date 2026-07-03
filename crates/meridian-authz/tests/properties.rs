//! Property-based tests (proptest).
//!
//! The invariants that must hold for *any* input, not just the hand-picked
//! cases: the tag→Cedar compiler never emits unparseable policy text; a
//! compiled row filter always round-trips through the IRC `Expression`
//! serde and never panics; mask normalization always yields at most one
//! (strongest) mask per column; and decisions are deterministic under
//! arbitrary principals/tags.

use meridian_authz::enforcement::{ColumnMask, Enforcement, MaskKind, RowPredicate};
use meridian_authz::{
    AbacRule, Action, AuthzPrincipal, AuthzResource, BaseEffect, PolicyEngine, PrincipalKind,
    RequestContext, ResourceKind, compile_ruleset, validate_syntax,
};
use meridian_iceberg::expr::Expression;
use proptest::prelude::*;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Arbitrary strings, deliberately including Cedar/JSON metacharacters so
/// escaping is exercised.
fn nasty_string() -> impl Strategy<Value = String> {
    prop_oneof![
        // Plausible tag/group/purpose names.
        "[a-z][a-z0-9_:.-]{0,20}",
        // Adversarial: quotes, backslashes, braces, cedar operators.
        Just(r#"a"b"#.to_owned()),
        Just(r"a\b".to_owned()),
        Just(r#"") || true || (""#.to_owned()),
        Just("x\ny".to_owned()),
        Just("тест".to_owned()),
        Just(String::new()),
    ]
}

fn scalar_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(|n| json!(n)),
        any::<bool>().prop_map(|b| json!(b)),
        nasty_string().prop_map(|s| json!(s)),
    ]
}

/// A bounded-depth arbitrary [`RowPredicate`].
fn row_predicate() -> impl Strategy<Value = RowPredicate> {
    let leaf = prop_oneof![
        Just(RowPredicate::True),
        Just(RowPredicate::False),
        ("[a-z_]{1,8}", scalar_value())
            .prop_map(|(column, value)| RowPredicate::Eq { column, value }),
        ("[a-z_]{1,8}", scalar_value())
            .prop_map(|(column, value)| RowPredicate::NotEq { column, value }),
        ("[a-z_]{1,8}", scalar_value())
            .prop_map(|(column, value)| RowPredicate::Lt { column, value }),
        ("[a-z_]{1,8}", scalar_value())
            .prop_map(|(column, value)| RowPredicate::GtEq { column, value }),
        ("[a-z_]{1,8}", prop::collection::vec(scalar_value(), 0..4))
            .prop_map(|(column, values)| RowPredicate::In { column, values }),
        "[a-z_]{1,8}".prop_map(|column| RowPredicate::IsNull { column }),
        "[a-z_]{1,8}".prop_map(|column| RowPredicate::NotNull { column }),
    ];
    leaf.prop_recursive(4, 16, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(l, r)| RowPredicate::And {
                left: Box::new(l),
                right: Box::new(r),
            }),
            (inner.clone(), inner.clone()).prop_map(|(l, r)| RowPredicate::Or {
                left: Box::new(l),
                right: Box::new(r),
            }),
            inner.prop_map(|c| RowPredicate::Not { child: Box::new(c) }),
        ]
    })
}

fn mask_kind() -> impl Strategy<Value = MaskKind> {
    prop_oneof![
        Just(MaskKind::Null),
        Just(MaskKind::Drop),
        Just(MaskKind::Hash),
        (0u32..8, 0u32..8).prop_map(|(show_first, show_last)| MaskKind::Partial {
            show_first,
            show_last
        }),
        nasty_string().prop_map(|expression| MaskKind::Custom { expression }),
    ]
}

/// A list of action verb strings (fresh strategy each call — proptest
/// strategies built from `nasty_string`-style closures are not `Clone`).
fn actions() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(
        prop_oneof![
            Just("read".to_owned()),
            Just("write".to_owned()),
            Just("commit".to_owned()),
            Just("manage".to_owned()),
        ],
        0..3,
    )
}

/// A list of group names (fresh strategy each call).
fn groups() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(nasty_string(), 0..3)
}

/// An arbitrary [`AbacRule`] across all variants.
fn abac_rule() -> impl Strategy<Value = AbacRule> {
    prop_oneof![
        (
            proptest::option::of(nasty_string()),
            nasty_string(),
            actions(),
            prop::collection::vec(nasty_string(), 0..3),
        )
            .prop_map(|(id, tag, actions, unless_purpose)| {
                AbacRule::TagDenyUnlessPurpose {
                    id,
                    description: None,
                    tag,
                    actions,
                    unless_purpose,
                }
            }),
        actions().prop_map(|actions| AbacRule::OwnerAllow {
            id: None,
            description: None,
            actions,
        }),
        (groups(), proptest::option::of(nasty_string()), actions()).prop_map(
            |(groups, tag, actions)| AbacRule::GroupAllow {
                id: None,
                description: None,
                groups,
                tag,
                actions,
            }
        ),
        (groups(), proptest::option::of(nasty_string()), actions()).prop_map(
            |(groups, tag, actions)| AbacRule::GroupDeny {
                id: None,
                description: None,
                groups,
                tag,
                actions,
            }
        ),
        (nasty_string(), groups(), row_predicate()).prop_map(|(tag, exempt_groups, predicate)| {
            AbacRule::TagRowFilter {
                id: None,
                description: None,
                tag,
                exempt_groups,
                predicate,
            }
        }),
        (nasty_string(), groups(), mask_kind()).prop_map(|(tag, exempt_groups, mask)| {
            AbacRule::TagColumnMask {
                id: None,
                description: None,
                tag,
                exempt_groups,
                mask,
            }
        }),
    ]
}

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

proptest! {
    /// A compiled row predicate always serializes to JSON and deserializes
    /// back to an equivalent `Expression` — i.e. it is a well-formed IRC
    /// filter. Never panics for any predicate.
    #[test]
    fn row_predicate_always_round_trips_as_expression(pred in row_predicate()) {
        let expr = pred.to_expression();
        let json = serde_json::to_value(&expr).expect("serialize");
        let back: Expression = serde_json::from_value(json).expect("deserialize");
        prop_assert_eq!(expr, back);
    }

    /// Any single compiled rule produces parseable Cedar text.
    #[test]
    fn any_rule_compiles_to_valid_cedar(rule in abac_rule()) {
        let text = rule.to_cedar();
        prop_assert!(
            validate_syntax(&text).is_ok(),
            "rule {:?} produced unparseable Cedar:\n{}",
            rule, text
        );
    }

    /// Any *set* of compiled rules produces parseable Cedar and builds an
    /// engine without error.
    #[test]
    fn any_ruleset_builds_an_engine(rules in prop::collection::vec(abac_rule(), 0..6)) {
        let text = compile_ruleset(&rules);
        prop_assert!(validate_syntax(&text).is_ok(), "ruleset unparseable:\n{}", text);
        let engine = PolicyEngine::new(&text, BaseEffect::AllowUnlessForbidden);
        prop_assert!(engine.is_ok(), "engine build failed:\n{}", text);
    }

    /// Mask normalization yields at most one mask per column, and that mask
    /// is the strongest among the inputs for its column.
    #[test]
    fn normalize_keeps_one_strongest_mask_per_column(
        masks in prop::collection::vec(
            ("[a-z]{1,4}", mask_kind()).prop_map(|(c, k)| ColumnMask::new(c, k, "p")),
            0..12,
        )
    ) {
        // Expected strongest per column, computed independently.
        use std::collections::BTreeMap;
        let mut expected: BTreeMap<String, u8> = BTreeMap::new();
        for m in &masks {
            let e = expected.entry(m.column.clone()).or_insert(0);
            *e = (*e).max(m.kind.strength());
        }

        let mut enforcement = Enforcement::none();
        enforcement.column_masks = masks;
        enforcement.normalize();

        // At most one per column, and columns are unique + sorted.
        let mut seen = std::collections::HashSet::new();
        let mut prev: Option<&str> = None;
        for m in &enforcement.column_masks {
            prop_assert!(seen.insert(m.column.clone()), "duplicate column {}", m.column);
            if let Some(p) = prev {
                prop_assert!(p < m.column.as_str(), "masks not sorted");
            }
            prev = Some(&m.column);
            // The kept mask is the strongest for its column.
            prop_assert_eq!(m.kind.strength(), expected[&m.column]);
        }
        // Every column that had a mask still has one.
        prop_assert_eq!(enforcement.column_masks.len(), expected.len());
    }

    /// Decisions are a pure function of their inputs: repeated evaluation
    /// with the same inputs yields the same decision, for arbitrary
    /// principals/tags/purposes.
    #[test]
    fn decisions_are_pure(
        groups in prop::collection::vec("[a-z]{1,5}", 0..3),
        tags in prop::collection::vec("[a-z:]{1,6}", 0..3),
        purpose in proptest::option::of("[a-z]{1,6}"),
    ) {
        let policy = r#"
            forbid(principal, action == Action::"read", resource)
              when { resource.tags.contains("pii:high") }
              unless { context has purpose && context.purpose == "ok" };
            permit(principal, action == Action::"read", resource)
              when { principal.groups.contains("readers") };
        "#;
        let engine = PolicyEngine::new(policy, BaseEffect::AllowUnlessForbidden).unwrap();

        let mut principal = AuthzPrincipal::new("p", PrincipalKind::User);
        for g in &groups { principal = principal.with_group(g.clone()); }
        let mut resource = AuthzResource::new("t", ResourceKind::Table);
        for t in &tags { resource = resource.with_tag(t.clone()); }
        let mut ctx = RequestContext::at(
            chrono::DateTime::from_timestamp(1_760_000_000, 0).unwrap()
        );
        if let Some(p) = &purpose { ctx = ctx.with_purpose(p.clone()); }

        let d1 = engine.authorize(&principal, Action::Read, &resource, &ctx).unwrap();
        let d2 = engine.authorize(&principal, Action::Read, &resource, &ctx).unwrap();
        prop_assert_eq!(&d1, &d2);

        // Sanity: a pii:high tag with no "ok" purpose is always denied.
        if tags.iter().any(|t| t == "pii:high") && purpose.as_deref() != Some("ok") {
            prop_assert!(d1.is_deny(), "pii:high without the purpose must deny");
        }
    }
}
