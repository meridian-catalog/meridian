//! ABAC decision tests: the D-F1 rule shapes evaluated end-to-end through
//! [`PolicyEngine::authorize`], asserting effect, determining policies, and
//! the audit reason.

use chrono::{TimeZone, Utc};
use meridian_authz::{
    Action, AuthzPrincipal, AuthzResource, BaseEffect, Decision, Effect, PolicyEngine,
    PrincipalKind, RequestContext, ResourceKind,
};

fn table(id: &str) -> AuthzResource {
    AuthzResource::new(id, ResourceKind::Table)
}

fn user(id: &str) -> AuthzPrincipal {
    AuthzPrincipal::new(id, PrincipalKind::User)
}

fn decide(
    policy: &str,
    base: BaseEffect,
    principal: &AuthzPrincipal,
    action: Action,
    resource: &AuthzResource,
    ctx: &RequestContext,
) -> Decision {
    PolicyEngine::new(policy, base)
        .expect("policy parses")
        .authorize(principal, action, resource, ctx)
        .expect("authorize succeeds")
}

// ---------------------------------------------------------------------------
// pii:high deny-unless-purpose
// ---------------------------------------------------------------------------

const PII_HIGH_DENY: &str = r#"
    @id("pii-high-deny")
    @description("pii:high denies read unless a matching purpose is granted")
    forbid(principal, action == Action::"read", resource)
      when { resource.tags.contains("pii:high") }
      unless { context has purpose && context.purpose == "fraud_investigation" };
"#;

#[test]
fn pii_high_denies_read_without_purpose() {
    let orders = table("sales.orders").with_tag("pii:high");
    let d = decide(
        PII_HIGH_DENY,
        BaseEffect::AllowUnlessForbidden,
        &user("alice"),
        Action::Read,
        &orders,
        &RequestContext::now(),
    );
    assert!(d.is_deny(), "no purpose => denied");
    assert_eq!(d.determining_policies.len(), 1);
    let p = &d.determining_policies[0];
    assert_eq!(p.annotation_id.as_deref(), Some("pii-high-deny"));
    assert_eq!(p.effect, Effect::Deny);
    assert!(
        d.reason.contains("pii-high-deny"),
        "reason names the policy: {}",
        d.reason
    );
    assert!(
        d.reason.contains("pii:high denies read"),
        "reason carries the @description: {}",
        d.reason
    );
}

#[test]
fn pii_high_allows_read_with_granted_purpose() {
    let orders = table("sales.orders").with_tag("pii:high");
    let ctx = RequestContext::now().with_purpose("fraud_investigation");
    let d = decide(
        PII_HIGH_DENY,
        BaseEffect::AllowUnlessForbidden,
        &user("alice"),
        Action::Read,
        &orders,
        &ctx,
    );
    assert!(
        d.is_allow(),
        "granted purpose lifts the forbid; baseline allows"
    );
    // No forbid fired; the determining set is empty (baseline is filtered
    // out), so the reason is the baseline explanation.
    assert!(d.determining_policies.is_empty());
    assert!(d.reason.contains("baseline"), "reason: {}", d.reason);
}

#[test]
fn pii_high_deny_does_not_touch_untagged_tables() {
    // A table without the tag is unaffected by the pii:high forbid.
    let plain = table("sales.public_orders");
    let d = decide(
        PII_HIGH_DENY,
        BaseEffect::AllowUnlessForbidden,
        &user("alice"),
        Action::Read,
        &plain,
        &RequestContext::now(),
    );
    assert!(d.is_allow());
}

#[test]
fn pii_high_deny_wrong_purpose_still_denies() {
    let orders = table("sales.orders").with_tag("pii:high");
    let ctx = RequestContext::now().with_purpose("marketing"); // not the granted one
    let d = decide(
        PII_HIGH_DENY,
        BaseEffect::AllowUnlessForbidden,
        &user("alice"),
        Action::Read,
        &orders,
        &ctx,
    );
    assert!(
        d.is_deny(),
        "a non-matching purpose does not lift the forbid"
    );
}

#[test]
fn pii_high_deny_only_covers_read_not_write() {
    // The forbid is scoped to `read`; a write is not denied by it.
    let orders = table("sales.orders").with_tag("pii:high");
    let d = decide(
        PII_HIGH_DENY,
        BaseEffect::AllowUnlessForbidden,
        &user("alice"),
        Action::Write,
        &orders,
        &RequestContext::now(),
    );
    assert!(d.is_allow(), "write is out of the forbid's action scope");
}

// ---------------------------------------------------------------------------
// owner-allow
// ---------------------------------------------------------------------------

const OWNER_ALLOW: &str = r#"
    @id("owner-allow")
    permit(principal, action, resource)
      when { resource has owner && resource.owner == principal.id };
"#;

#[test]
fn owner_is_allowed_under_deny_by_default() {
    let orders = table("sales.orders").with_owner("alice");
    // Under DenyUnlessPermitted the owner permit is the ONLY thing granting.
    let d = decide(
        OWNER_ALLOW,
        BaseEffect::DenyUnlessPermitted,
        &user("alice"),
        Action::Read,
        &orders,
        &RequestContext::now(),
    );
    assert!(d.is_allow(), "owner matches => permit fires");
    assert_eq!(
        d.determining_policies[0].annotation_id.as_deref(),
        Some("owner-allow")
    );
    assert_eq!(d.determining_policies[0].effect, Effect::Allow);
    assert!(d.reason.starts_with("allowed by"), "reason: {}", d.reason);
}

#[test]
fn non_owner_is_denied_by_default() {
    let orders = table("sales.orders").with_owner("bob");
    let d = decide(
        OWNER_ALLOW,
        BaseEffect::DenyUnlessPermitted,
        &user("alice"),
        Action::Read,
        &orders,
        &RequestContext::now(),
    );
    assert!(d.is_deny(), "not the owner => no permit => default deny");
    assert!(d.determining_policies.is_empty());
    assert!(d.reason.contains("default deny"), "reason: {}", d.reason);
}

// ---------------------------------------------------------------------------
// group-based
// ---------------------------------------------------------------------------

const GROUP_ALLOW: &str = r#"
    @id("analysts-read")
    permit(principal, action == Action::"read", resource)
      when { principal.groups.contains("analysts") };
"#;

#[test]
fn group_member_is_allowed() {
    let d = decide(
        GROUP_ALLOW,
        BaseEffect::DenyUnlessPermitted,
        &user("alice").with_group("analysts"),
        Action::Read,
        &table("sales.orders"),
        &RequestContext::now(),
    );
    assert!(d.is_allow());
}

#[test]
fn non_group_member_is_denied() {
    let d = decide(
        GROUP_ALLOW,
        BaseEffect::DenyUnlessPermitted,
        &user("alice").with_group("interns"),
        Action::Read,
        &table("sales.orders"),
        &RequestContext::now(),
    );
    assert!(d.is_deny());
}

// ---------------------------------------------------------------------------
// deny-overrides-allow (the spec's required conflict resolution)
// ---------------------------------------------------------------------------

const ALLOW_THEN_DENY: &str = r#"
    @id("analysts-read")
    permit(principal, action == Action::"read", resource)
      when { principal.groups.contains("analysts") };

    @id("quarantine-deny")
    @description("tables tagged quarantine are never readable")
    forbid(principal, action == Action::"read", resource)
      when { resource.tags.contains("quarantine") };
"#;

#[test]
fn forbid_overrides_permit() {
    // alice is an analyst (permit fires) AND the table is quarantined
    // (forbid fires) => deny wins.
    let quarantined = table("sales.orders").with_tag("quarantine");
    let d = decide(
        ALLOW_THEN_DENY,
        BaseEffect::DenyUnlessPermitted,
        &user("alice").with_group("analysts"),
        Action::Read,
        &quarantined,
        &RequestContext::now(),
    );
    assert!(d.is_deny(), "forbid overrides the matching permit");
    // The determining policy is the forbid, not the permit.
    assert_eq!(d.determining_policies.len(), 1);
    assert_eq!(
        d.determining_policies[0].annotation_id.as_deref(),
        Some("quarantine-deny")
    );
    assert_eq!(d.determining_policies[0].effect, Effect::Deny);
}

#[test]
fn permit_alone_allows_when_no_forbid_matches() {
    // Same policy set, but the table is not quarantined.
    let d = decide(
        ALLOW_THEN_DENY,
        BaseEffect::DenyUnlessPermitted,
        &user("alice").with_group("analysts"),
        Action::Read,
        &table("sales.orders"),
        &RequestContext::now(),
    );
    assert!(d.is_allow());
    assert_eq!(
        d.determining_policies[0].annotation_id.as_deref(),
        Some("analysts-read")
    );
}

// ---------------------------------------------------------------------------
// time-bound
// ---------------------------------------------------------------------------

const TIME_BOUND: &str = r#"
    @id("q3-window")
    @description("access only during Q3 2026")
    permit(principal, action == Action::"read", resource)
      when {
        context.now >= datetime("2026-07-01T00:00:00.000Z") &&
        context.now < datetime("2026-10-01T00:00:00.000Z")
      };
"#;

#[test]
fn time_bound_allows_inside_window() {
    let inside = Utc.with_ymd_and_hms(2026, 8, 15, 12, 0, 0).unwrap();
    let d = decide(
        TIME_BOUND,
        BaseEffect::DenyUnlessPermitted,
        &user("alice"),
        Action::Read,
        &table("sales.orders"),
        &RequestContext::at(inside),
    );
    assert!(d.is_allow(), "inside the window => allowed");
}

#[test]
fn time_bound_denies_before_window() {
    let before = Utc.with_ymd_and_hms(2026, 6, 30, 23, 59, 0).unwrap();
    let d = decide(
        TIME_BOUND,
        BaseEffect::DenyUnlessPermitted,
        &user("alice"),
        Action::Read,
        &table("sales.orders"),
        &RequestContext::at(before),
    );
    assert!(d.is_deny(), "before the window => denied");
}

#[test]
fn time_bound_denies_after_window() {
    let after = Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap();
    let d = decide(
        TIME_BOUND,
        BaseEffect::DenyUnlessPermitted,
        &user("alice"),
        Action::Read,
        &table("sales.orders"),
        &RequestContext::at(after),
    );
    assert!(d.is_deny(), "at/after the exclusive upper bound => denied");
}

// ---------------------------------------------------------------------------
// principal-kind-scoped policy (agents governed distinctly)
// ---------------------------------------------------------------------------

#[test]
fn agents_can_be_forbidden_as_a_class() {
    // "No agent may read pii:high, ever."
    let policy = r#"
        @id("no-agent-pii")
        forbid(principal is Agent, action == Action::"read", resource)
          when { resource.tags.contains("pii:high") };
    "#;
    let pii = table("sales.orders").with_tag("pii:high");

    // An agent is denied.
    let agent = AuthzPrincipal::new("agent-007", PrincipalKind::Agent);
    let d = decide(
        policy,
        BaseEffect::AllowUnlessForbidden,
        &agent,
        Action::Read,
        &pii,
        &RequestContext::now(),
    );
    assert!(d.is_deny(), "agents are forbidden pii:high as a class");

    // A human is not affected by the agent-scoped forbid.
    let d = decide(
        policy,
        BaseEffect::AllowUnlessForbidden,
        &user("alice"),
        Action::Read,
        &pii,
        &RequestContext::now(),
    );
    assert!(d.is_allow(), "the forbid only scopes Agent principals");
}

// ---------------------------------------------------------------------------
// base-effect semantics
// ---------------------------------------------------------------------------

#[test]
fn empty_engine_allows_under_allow_baseline() {
    let engine = PolicyEngine::empty(BaseEffect::AllowUnlessForbidden).unwrap();
    let d = engine
        .authorize(
            &user("alice"),
            Action::Read,
            &table("t"),
            &RequestContext::now(),
        )
        .unwrap();
    assert!(
        d.is_allow(),
        "no policies + allow-baseline => allow (RBAC is the gate)"
    );
    assert_eq!(engine.policy_count(), 0);
}

#[test]
fn empty_engine_denies_under_deny_baseline() {
    let engine = PolicyEngine::empty(BaseEffect::DenyUnlessPermitted).unwrap();
    let d = engine
        .authorize(
            &user("alice"),
            Action::Read,
            &table("t"),
            &RequestContext::now(),
        )
        .unwrap();
    assert!(d.is_deny(), "no policies + deny-baseline => deny");
}

// ---------------------------------------------------------------------------
// determinism: the same inputs always yield the same decision
// ---------------------------------------------------------------------------

#[test]
fn decisions_are_deterministic() {
    let orders = table("sales.orders").with_tag("pii:high");
    let ctx = RequestContext::at(Utc.with_ymd_and_hms(2026, 7, 4, 0, 0, 0).unwrap());
    let engine = PolicyEngine::new(PII_HIGH_DENY, BaseEffect::AllowUnlessForbidden).unwrap();
    let first = engine
        .authorize(&user("alice"), Action::Read, &orders, &ctx)
        .unwrap();
    for _ in 0..25 {
        let again = engine
            .authorize(&user("alice"), Action::Read, &orders, &ctx)
            .unwrap();
        assert_eq!(first, again, "identical inputs => identical decision");
    }
}
