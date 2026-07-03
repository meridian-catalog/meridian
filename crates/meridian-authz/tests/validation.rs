//! Policy validation, malformed-policy rejection, and the tag→Cedar rule
//! compiler (D-F1: dry-run + validation; detect errors before save).

use meridian_authz::enforcement::{MaskKind, RowPredicate};
use meridian_authz::{
    AbacRule, Action, AuthzError, AuthzPrincipal, AuthzResource, BaseEffect, PolicyEngine,
    PrincipalKind, RequestContext, ResourceKind, compile_ruleset, dry_run, validate_against_schema,
    validate_syntax,
};

// ---------------------------------------------------------------------------
// Malformed-policy rejection
// ---------------------------------------------------------------------------

#[test]
fn malformed_policy_is_rejected_by_syntax_check() {
    // Missing semicolon / broken body.
    let err = validate_syntax("permit(principal, action, resource) when { .broken }").unwrap_err();
    assert!(matches!(err, AuthzError::PolicyParse { .. }), "got {err:?}");
}

#[test]
fn malformed_policy_is_rejected_by_engine_construction() {
    let err = PolicyEngine::new(
        "forbid(principal action resource);", // no commas
        BaseEffect::AllowUnlessForbidden,
    )
    .unwrap_err();
    assert!(matches!(err, AuthzError::PolicyParse { .. }));
    // The message carries Cedar's diagnostic so an author can fix it.
    let AuthzError::PolicyParse { message } = err else {
        unreachable!()
    };
    assert!(!message.is_empty());
}

#[test]
fn empty_policy_text_is_valid() {
    assert_eq!(validate_syntax("").unwrap(), 0);
    assert_eq!(validate_syntax("   \n  ").unwrap(), 0);
}

#[test]
fn valid_policy_reports_count() {
    let n = validate_syntax(
        r#"
        permit(principal, action, resource);
        forbid(principal, action == Action::"read", resource) when { resource.tags.contains("x") };
        "#,
    )
    .unwrap();
    assert_eq!(n, 2);
}

// ---------------------------------------------------------------------------
// Schema validation: catch attribute typos before save
// ---------------------------------------------------------------------------

#[test]
fn schema_validation_accepts_well_formed_policy() {
    let policy = r#"
        forbid(principal, action == Action::"read", resource)
          when { resource.tags.contains("pii:high") }
          unless { context has purpose && context.purpose == "audit" };
    "#;
    assert!(
        validate_against_schema(policy).is_ok(),
        "a policy using declared attributes validates"
    );
}

#[test]
fn schema_validation_rejects_misspelled_attribute() {
    // `resource.onwer` (typo) is not in the schema.
    let policy = r"
        permit(principal, action, resource)
          when { resource.onwer == principal.id };
    ";
    let err = validate_against_schema(policy).unwrap_err();
    assert!(
        matches!(err, AuthzError::Validation { .. }),
        "schema validation catches the typo: {err:?}"
    );
}

#[test]
fn schema_validation_rejects_unknown_action() {
    // `Action::"teleport"` is not a declared action.
    let policy = r#"permit(principal, action == Action::"teleport", resource);"#;
    let err = validate_against_schema(policy).unwrap_err();
    assert!(matches!(err, AuthzError::Validation { .. }), "got {err:?}");
}

#[test]
fn the_shipped_schema_itself_parses() {
    assert!(meridian_authz::meridian_schema().is_ok());
}

// ---------------------------------------------------------------------------
// Dry-run
// ---------------------------------------------------------------------------

#[test]
fn dry_run_previews_a_decision_without_saving() {
    let policy = r#"
        @id("deny-restricted")
        forbid(principal, action == Action::"read", resource)
          when { resource.classification == "restricted" };
    "#;
    let restricted = AuthzResource::new("t", ResourceKind::Table).with_classification("restricted");
    let d = dry_run(
        policy,
        BaseEffect::AllowUnlessForbidden,
        &AuthzPrincipal::new("alice", PrincipalKind::User),
        Action::Read,
        &restricted,
        &RequestContext::now(),
    )
    .unwrap();
    assert!(d.is_deny());
    assert_eq!(
        d.determining_policies[0].annotation_id.as_deref(),
        Some("deny-restricted")
    );
}

#[test]
fn dry_run_surfaces_a_bad_policy() {
    let err = dry_run(
        "this is not cedar",
        BaseEffect::AllowUnlessForbidden,
        &AuthzPrincipal::new("alice", PrincipalKind::User),
        Action::Read,
        &AuthzResource::new("t", ResourceKind::Table),
        &RequestContext::now(),
    )
    .unwrap_err();
    assert!(matches!(err, AuthzError::PolicyParse { .. }));
}

// ---------------------------------------------------------------------------
// Tag -> Cedar rule compiler: the generated policies parse AND behave
// ---------------------------------------------------------------------------

fn compile_and_engine(rules: &[AbacRule], base: BaseEffect) -> PolicyEngine {
    let text = compile_ruleset(rules);
    // Every generated ruleset must parse — the compiler never emits junk.
    assert!(
        validate_syntax(&text).is_ok(),
        "generated Cedar failed to parse:\n{text}"
    );
    PolicyEngine::new(&text, base).expect("generated policy builds an engine")
}

#[test]
fn compiled_tag_deny_unless_purpose_behaves() {
    let rules = vec![AbacRule::TagDenyUnlessPurpose {
        id: Some("pii-high".into()),
        description: Some("pii:high needs a purpose".into()),
        tag: "pii:high".into(),
        actions: vec!["read".into()],
        unless_purpose: vec!["fraud".into(), "audit".into()],
    }];
    let engine = compile_and_engine(&rules, BaseEffect::AllowUnlessForbidden);
    let pii = AuthzResource::new("t", ResourceKind::Table).with_tag("pii:high");
    let alice = AuthzPrincipal::new("alice", PrincipalKind::User);

    // No purpose => deny.
    let d = engine
        .authorize(&alice, Action::Read, &pii, &RequestContext::now())
        .unwrap();
    assert!(d.is_deny());
    assert_eq!(
        d.determining_policies[0].annotation_id.as_deref(),
        Some("pii-high")
    );

    // One of the granted purposes => allow.
    for purpose in ["fraud", "audit"] {
        let ctx = RequestContext::now().with_purpose(purpose);
        let d = engine.authorize(&alice, Action::Read, &pii, &ctx).unwrap();
        assert!(d.is_allow(), "purpose {purpose} lifts the deny");
    }

    // A different purpose => still deny.
    let ctx = RequestContext::now().with_purpose("marketing");
    let d = engine.authorize(&alice, Action::Read, &pii, &ctx).unwrap();
    assert!(d.is_deny());
}

#[test]
fn compiled_owner_allow_behaves() {
    let rules = vec![AbacRule::OwnerAllow {
        id: Some("owner".into()),
        description: None,
        actions: vec![],
    }];
    let engine = compile_and_engine(&rules, BaseEffect::DenyUnlessPermitted);
    let t = AuthzResource::new("t", ResourceKind::Table).with_owner("alice");

    let d = engine
        .authorize(
            &AuthzPrincipal::new("alice", PrincipalKind::User),
            Action::Read,
            &t,
            &RequestContext::now(),
        )
        .unwrap();
    assert!(d.is_allow(), "owner allowed");

    let d = engine
        .authorize(
            &AuthzPrincipal::new("bob", PrincipalKind::User),
            Action::Read,
            &t,
            &RequestContext::now(),
        )
        .unwrap();
    assert!(d.is_deny(), "non-owner denied");
}

#[test]
fn compiled_group_allow_with_tag_scope_behaves() {
    let rules = vec![AbacRule::GroupAllow {
        id: Some("finance-readers".into()),
        description: None,
        groups: vec!["finance".into()],
        tag: Some("finance".into()),
        actions: vec!["read".into()],
    }];
    let engine = compile_and_engine(&rules, BaseEffect::DenyUnlessPermitted);
    let finance_table = AuthzResource::new("t", ResourceKind::Table).with_tag("finance");
    let other_table = AuthzResource::new("u", ResourceKind::Table).with_tag("marketing");
    let member = AuthzPrincipal::new("alice", PrincipalKind::User).with_group("finance");

    // Member reading a finance-tagged table: allowed.
    assert!(
        engine
            .authorize(
                &member,
                Action::Read,
                &finance_table,
                &RequestContext::now()
            )
            .unwrap()
            .is_allow()
    );
    // Member reading a non-finance table: the tag scope fails => deny.
    assert!(
        engine
            .authorize(&member, Action::Read, &other_table, &RequestContext::now())
            .unwrap()
            .is_deny()
    );
    // Non-member: deny.
    let outsider = AuthzPrincipal::new("bob", PrincipalKind::User).with_group("interns");
    assert!(
        engine
            .authorize(
                &outsider,
                Action::Read,
                &finance_table,
                &RequestContext::now()
            )
            .unwrap()
            .is_deny()
    );
}

#[test]
fn compiled_group_deny_overrides_a_group_allow() {
    // Layer an allow and a deny; deny wins for the excluded group.
    let rules = vec![
        AbacRule::GroupAllow {
            id: Some("all-staff-read".into()),
            description: None,
            groups: vec!["staff".into()],
            tag: None,
            actions: vec!["read".into()],
        },
        AbacRule::GroupDeny {
            id: Some("contractors-blocked".into()),
            description: Some("contractors may not read pii".into()),
            groups: vec!["contractors".into()],
            tag: Some("pii:high".into()),
            actions: vec!["read".into()],
        },
    ];
    let engine = compile_and_engine(&rules, BaseEffect::DenyUnlessPermitted);
    let pii = AuthzResource::new("t", ResourceKind::Table).with_tag("pii:high");

    // A staff contractor: allow fires (staff) but deny fires (contractor +
    // pii) => deny wins.
    let staff_contractor = AuthzPrincipal::new("c", PrincipalKind::User)
        .with_group("staff")
        .with_group("contractors");
    let d = engine
        .authorize(
            &staff_contractor,
            Action::Read,
            &pii,
            &RequestContext::now(),
        )
        .unwrap();
    assert!(d.is_deny(), "group deny overrides group allow");
    assert_eq!(
        d.determining_policies[0].annotation_id.as_deref(),
        Some("contractors-blocked")
    );

    // A plain staff member: only the allow fires.
    let staff = AuthzPrincipal::new("s", PrincipalKind::User).with_group("staff");
    assert!(
        engine
            .authorize(&staff, Action::Read, &pii, &RequestContext::now())
            .unwrap()
            .is_allow()
    );
}

#[test]
fn compiled_group_rule_with_no_groups_matches_nobody() {
    // Guard against the footgun where an empty group list matches everyone.
    let rules = vec![AbacRule::GroupAllow {
        id: Some("empty".into()),
        description: None,
        groups: vec![],
        tag: None,
        actions: vec![],
    }];
    let engine = compile_and_engine(&rules, BaseEffect::DenyUnlessPermitted);
    let d = engine
        .authorize(
            &AuthzPrincipal::new("anyone", PrincipalKind::User),
            Action::Read,
            &AuthzResource::new("t", ResourceKind::Table),
            &RequestContext::now(),
        )
        .unwrap();
    assert!(d.is_deny(), "an empty-group allow must grant nobody");
}

#[test]
fn compiled_rules_survive_injection_attempts_in_tag_names() {
    // A tag containing Cedar metacharacters must not break out of the
    // string literal — the generated policy must still parse and mean what
    // it says (deny the literal weird tag, nothing else).
    let weird_tag = r#"pii"high") || true || ("#;
    let rules = vec![AbacRule::TagDenyUnlessPurpose {
        id: Some("weird".into()),
        description: None,
        tag: weird_tag.into(),
        actions: vec!["read".into()],
        unless_purpose: vec![],
    }];
    let text = compile_ruleset(&rules);
    assert!(
        validate_syntax(&text).is_ok(),
        "escaping keeps the generated Cedar valid:\n{text}"
    );
    let engine = PolicyEngine::new(&text, BaseEffect::AllowUnlessForbidden).unwrap();

    // A table WITHOUT the weird tag is unaffected (no accidental always-deny
    // from an injected `|| true`).
    let plain = AuthzResource::new("t", ResourceKind::Table).with_tag("pii:high");
    let d = engine
        .authorize(
            &AuthzPrincipal::new("alice", PrincipalKind::User),
            Action::Read,
            &plain,
            &RequestContext::now(),
        )
        .unwrap();
    assert!(
        d.is_allow(),
        "injection did not turn the rule into an always-deny"
    );

    // A table WITH the exact weird tag is denied (the rule still works).
    let tagged = AuthzResource::new("t", ResourceKind::Table).with_tag(weird_tag);
    let d = engine
        .authorize(
            &AuthzPrincipal::new("alice", PrincipalKind::User),
            Action::Read,
            &tagged,
            &RequestContext::now(),
        )
        .unwrap();
    assert!(d.is_deny(), "the literal weird tag is matched exactly");
}

#[test]
fn compiled_rules_are_schema_valid() {
    // The convenience compiler must emit schema-valid Cedar (strict mode)
    // for the shipped rule shapes — otherwise a "convenience" rule would be
    // rejected by the validation gate the API runs.
    let rules = vec![
        AbacRule::TagDenyUnlessPurpose {
            id: Some("a".into()),
            description: None,
            tag: "pii:high".into(),
            actions: vec!["read".into()],
            unless_purpose: vec!["audit".into()],
        },
        AbacRule::OwnerAllow {
            id: Some("b".into()),
            description: None,
            actions: vec![],
        },
        AbacRule::GroupAllow {
            id: Some("c".into()),
            description: None,
            groups: vec!["g".into()],
            tag: Some("t".into()),
            actions: vec!["read".into(), "write".into()],
        },
        AbacRule::TimeBoundAllow {
            id: Some("d".into()),
            description: None,
            not_before: Some("2026-01-01T00:00:00.000Z".into()),
            not_after: Some("2027-01-01T00:00:00.000Z".into()),
            actions: vec![],
        },
        AbacRule::TagRowFilter {
            id: Some("e".into()),
            description: None,
            tag: "residency:eu".into(),
            exempt_groups: vec!["eu".into()],
            predicate: RowPredicate::True,
        },
        AbacRule::TagColumnMask {
            id: Some("f".into()),
            description: None,
            tag: "pii:email".into(),
            exempt_groups: vec![],
            mask: MaskKind::Hash,
        },
    ];
    let text = compile_ruleset(&rules);
    let result = validate_against_schema(&text);
    assert!(
        result.is_ok(),
        "compiled convenience rules must be schema-valid:\n{text}\nerror: {result:?}"
    );
}
