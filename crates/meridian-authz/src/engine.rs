//! The Cedar policy engine wrapper: parse a policy set, assemble an entity
//! store from Meridian principals/resources/tags, and evaluate a decision
//! with its determining policies and audit reason.
//!
//! # The Cedar model this crate fixes
//!
//! - **Principal entity types:** `User`, `Service`, `Agent` (from
//!   [`PrincipalKind`]). Attributes: `id` (String), `groups`
//!   (`Set<String>`), `roles` (`Set<String>`), optional `purpose`
//!   (String), optional `environment` (String), plus caller extras.
//! - **Resource entity types:** `Namespace`, `Table`, `View`, `Column`
//!   (from [`ResourceKind`]). Attributes: `tags` (`Set<String>`), optional
//!   `owner` (String), optional `classification` (String), plus extras. A
//!   `Column` entity is a Cedar *child* (`in`) of its parent table/view,
//!   so `resource in Table::"…"` policies work.
//! - **Actions:** `read`, `write`, `commit`, `create`, `drop`, `alter`,
//!   `manage` (from [`Action`]).
//! - **Context:** `now` (a `datetime`, from the Cedar `datetime`
//!   extension), optional `purpose` (String), plus caller session extras.
//!
//! # Deny model
//!
//! Cedar is deny-by-default with **forbid-overrides-permit** — exactly the
//! "deny-overrides-allow" the spec (D-F1) requires. Because Meridian's ABAC
//! layer runs *on top of* RBAC (which is the base allow layer, already
//! decided upstream), the engine is created with a [`BaseEffect`] that says
//! whether an unmatched request is allowed:
//!
//! - [`BaseEffect::AllowUnlessForbidden`] (the default, and what the
//!   store's tag-rule compiler targets): an implicit baseline permit is
//!   evaluated, so a tag rule set only ever *subtracts* access via
//!   `forbid`s — a table with no ABAC forbid is allowed, and RBAC remains
//!   the gate. Explicit `permit`s in the set still apply (they broaden
//!   nothing that a `forbid` then removes, since forbid wins).
//! - [`BaseEffect::DenyUnlessPermitted`]: pure deny-by-default — access
//!   requires a matching `permit`. Use when ABAC *is* the whole decision.

use std::fmt::Write as _;
use std::str::FromStr;

use cedar_policy::{
    Authorizer, Context, Decision as CedarDecision, Entities, Entity, EntityId, EntityTypeName,
    EntityUid, Policy, PolicyId, PolicySet, Request, RestrictedExpression,
};
use serde_json::Value;

use crate::context::RequestContext;
use crate::decision::{Decision, DeterminingPolicy, Effect};
use crate::error::AuthzError;
use crate::principal::{Action, AuthzPrincipal};
use crate::resource::AuthzResource;

/// How the engine treats a request that no `permit` matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BaseEffect {
    /// Allow unless a `forbid` fires. An implicit baseline `permit` is
    /// evaluated so that a policy set of tag-driven `forbid`s only
    /// subtracts access. This is the composition-with-RBAC default.
    #[default]
    AllowUnlessForbidden,
    /// Deny unless a `permit` fires (Cedar's native default). Use when the
    /// ABAC policy set is the complete access decision.
    DenyUnlessPermitted,
}

/// The id given to the engine's synthetic baseline permit (only present
/// under [`BaseEffect::AllowUnlessForbidden`]). Chosen to be recognizable
/// in an audit reason and unlikely to collide with a stored policy id.
const BASELINE_PERMIT_ID: &str = "__meridian_baseline_permit";

/// A parsed, ready-to-evaluate ABAC policy engine.
///
/// Construction validates the policy text; [`PolicyEngine::new`] returns a
/// [`AuthzError::PolicyParse`] with the location and message when the text
/// is malformed, so a bad policy is rejected *before* it can be saved (the
/// dry-run/validation path in [`crate::validate`] wraps this).
#[derive(Debug, Clone)]
pub struct PolicyEngine {
    policies: PolicySet,
    base: BaseEffect,
}

impl PolicyEngine {
    /// Parses a Cedar policy set from source text.
    ///
    /// # Errors
    ///
    /// [`AuthzError::PolicyParse`] if the text is not valid Cedar.
    pub fn new(policy_text: &str, base: BaseEffect) -> Result<Self, AuthzError> {
        let mut policies =
            PolicySet::from_str(policy_text).map_err(|e| AuthzError::PolicyParse {
                message: e.to_string(),
            })?;
        if base == BaseEffect::AllowUnlessForbidden {
            // A permit that matches anything; forbids still override it. Its
            // id is set explicitly (the `@id` *annotation* does not become
            // the policy id — Cedar assigns `policy0`, …), so it can never
            // collide with a stored policy's id and is filtered back out of
            // the audit reason by `BASELINE_PERMIT_ID`.
            let baseline_id =
                PolicyId::from_str(BASELINE_PERMIT_ID).map_err(|e| AuthzError::PolicyParse {
                    message: format!("internal baseline permit id is invalid: {e}"),
                })?;
            let baseline = Policy::from_str("permit(principal, action, resource);")
                .map_err(|e| AuthzError::PolicyParse {
                    message: format!("internal baseline permit failed to parse: {e}"),
                })?
                .new_id(baseline_id);
            policies
                .add(baseline)
                .map_err(|e| AuthzError::PolicyParse {
                    message: format!("internal baseline permit could not be added: {e}"),
                })?;
        }
        Ok(Self { policies, base })
    }

    /// An empty engine (no stored policies) with the given base effect.
    /// Under [`BaseEffect::AllowUnlessForbidden`] this allows everything
    /// (the "no ABAC policies configured" case); under
    /// [`BaseEffect::DenyUnlessPermitted`] it denies everything.
    ///
    /// # Errors
    ///
    /// Only if constructing the internal baseline permit fails, which does
    /// not happen for the constant text used here; the `Result` is kept so
    /// there is no panic path (a stored-policy variant would share it).
    pub fn empty(base: BaseEffect) -> Result<Self, AuthzError> {
        Self::new("", base)
    }

    /// The number of stored policies (excluding the synthetic baseline
    /// permit).
    #[must_use]
    pub fn policy_count(&self) -> usize {
        self.policies
            .policies()
            .filter(|p| p.id().to_string() != BASELINE_PERMIT_ID)
            .count()
    }

    /// Authorizes a `(principal, action, resource, context)` request,
    /// returning the [`Decision`] with its determining policies and audit
    /// reason.
    ///
    /// # Errors
    ///
    /// [`AuthzError::Entity`] if the principal/resource/context cannot be
    /// assembled into Cedar entities (e.g. a reserved attribute name), or
    /// [`AuthzError::Request`] if the request itself is rejected.
    pub fn authorize(
        &self,
        principal: &AuthzPrincipal,
        action: Action,
        resource: &AuthzResource,
        context: &RequestContext,
    ) -> Result<Decision, AuthzError> {
        let principal_uid = principal_uid(principal)?;
        let action_uid = action_uid(action)?;
        let resource_uid = resource_uid(resource)?;

        let entities = build_entities(principal, resource)?;
        let cedar_context = build_context(context)?;

        let request = Request::new(principal_uid, action_uid, resource_uid, cedar_context, None)
            .map_err(|e| AuthzError::Request {
                message: e.to_string(),
            })?;

        let response = Authorizer::new().is_authorized(&request, &self.policies, &entities);
        Ok(self.build_decision(&response))
    }

    /// Turns a Cedar [`Response`](cedar_policy::Response) into our
    /// audit-grade [`Decision`].
    fn build_decision(&self, response: &cedar_policy::Response) -> Decision {
        let effect = match response.decision() {
            CedarDecision::Allow => Effect::Allow,
            CedarDecision::Deny => Effect::Deny,
        };

        let determining: Vec<DeterminingPolicy> = response
            .diagnostics()
            .reason()
            .map(ToString::to_string)
            .filter(|pid| pid != BASELINE_PERMIT_ID)
            .map(|pid| self.describe_policy(&pid))
            .collect();

        let errors: Vec<String> = response
            .diagnostics()
            .errors()
            .map(ToString::to_string)
            .collect();

        let reason = self.build_reason(effect, &determining, &errors);

        Decision {
            effect,
            determining_policies: determining,
            reason,
            errors,
        }
    }

    /// Looks up a policy by id and reads its `@id`/`@description`
    /// annotations for the audit reason.
    fn describe_policy(&self, policy_id: &str) -> DeterminingPolicy {
        let policy = self
            .policies
            .policies()
            .find(|p| p.id().to_string() == policy_id);
        let (annotation_id, description, effect) = match policy {
            Some(p) => (
                p.annotation("id").map(str::to_owned),
                p.annotation("description").map(str::to_owned),
                match p.effect() {
                    cedar_policy::Effect::Permit => Effect::Allow,
                    cedar_policy::Effect::Forbid => Effect::Deny,
                },
            ),
            // Should not happen — a determining id is always in the set —
            // but stay total: report what we know.
            None => (None, None, Effect::Deny),
        };
        DeterminingPolicy {
            policy_id: policy_id.to_owned(),
            annotation_id,
            description,
            effect,
        }
    }

    /// Builds the human-readable audit reason.
    fn build_reason(
        &self,
        effect: Effect,
        determining: &[DeterminingPolicy],
        errors: &[String],
    ) -> String {
        let name = |p: &DeterminingPolicy| -> String {
            let id = p.annotation_id.as_deref().unwrap_or(&p.policy_id);
            match &p.description {
                Some(d) => format!("`{id}` ({d})"),
                None => format!("`{id}`"),
            }
        };
        let mut reason = match (effect, determining.is_empty()) {
            (Effect::Deny, true) => match self.base {
                BaseEffect::DenyUnlessPermitted => {
                    "denied: no policy permits this access (default deny)".to_owned()
                }
                BaseEffect::AllowUnlessForbidden => {
                    // Deny with no determining policy under allow-baseline
                    // is unusual, but describe it honestly.
                    "denied: no permit applied".to_owned()
                }
            },
            (Effect::Deny, false) => {
                let names: Vec<String> = determining.iter().map(name).collect();
                format!("denied by {}", names.join(", "))
            }
            (Effect::Allow, true) => "allowed: baseline (no forbid applied)".to_owned(),
            (Effect::Allow, false) => {
                let names: Vec<String> = determining.iter().map(name).collect();
                format!("allowed by {}", names.join(", "))
            }
        };
        if !errors.is_empty() {
            let _ = write!(
                reason,
                " [{} policy evaluation error(s): {}]",
                errors.len(),
                errors.join("; ")
            );
        }
        reason
    }
}

// ---------------------------------------------------------------------------
// Cedar entity / request assembly
// ---------------------------------------------------------------------------

/// Builds a `TYPE::"id"` UID from arbitrary bytes without manual escaping.
fn uid(entity_type: &str, id: &str) -> Result<EntityUid, AuthzError> {
    let type_name = EntityTypeName::from_str(entity_type).map_err(|e| AuthzError::Entity {
        message: format!("invalid entity type {entity_type:?}: {e}"),
    })?;
    // EntityId::from_str is infallible in practice (any string is a valid
    // id) but the signature is fallible; handle it total-functionally.
    let entity_id = EntityId::from_str(id).map_err(|e| AuthzError::Entity {
        message: format!("invalid entity id {id:?}: {e}"),
    })?;
    Ok(EntityUid::from_type_name_and_id(type_name, entity_id))
}

fn principal_uid(principal: &AuthzPrincipal) -> Result<EntityUid, AuthzError> {
    uid(principal.kind.cedar_type(), &principal.id)
}

fn resource_uid(resource: &AuthzResource) -> Result<EntityUid, AuthzError> {
    uid(resource.kind.cedar_type(), &resource.id)
}

fn action_uid(action: Action) -> Result<EntityUid, AuthzError> {
    // The action type is the fixed literal `Action` and its id is a known
    // ASCII verb, so this does not fail in practice; the error is
    // propagated rather than panicked to keep the call path total.
    uid("Action", action.cedar_id())
}

/// A `RestrictedExpression` for a JSON scalar/array, used for entity
/// attributes. Objects become records; arrays become sets; scalars map
/// directly. Cedar's `datetime`/`decimal` extension values are not produced
/// here (attributes stay plain JSON); only the context `now` uses the
/// datetime extension, built separately.
fn json_to_expr(value: &Value) -> Result<RestrictedExpression, AuthzError> {
    let expr = match value {
        Value::Null => {
            // Cedar has no null. Top-level null attributes are skipped
            // upstream (treated as absent); a null reaching here is nested
            // inside an array/record where an element cannot be omitted, so
            // it becomes an empty set — a policy reading it should guard.
            RestrictedExpression::new_set(std::iter::empty())
        }
        Value::Bool(b) => RestrictedExpression::new_bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                RestrictedExpression::new_long(i)
            } else {
                // Cedar longs are i64; a non-integer or out-of-range number
                // is stored as its string form so no information is lost and
                // no silent truncation happens.
                RestrictedExpression::new_string(n.to_string())
            }
        }
        Value::String(s) => RestrictedExpression::new_string(s.clone()),
        Value::Array(items) => {
            let exprs = items
                .iter()
                .map(json_to_expr)
                .collect::<Result<Vec<_>, _>>()?;
            RestrictedExpression::new_set(exprs)
        }
        Value::Object(map) => {
            let fields = map
                .iter()
                .map(|(k, v)| json_to_expr(v).map(|e| (k.clone(), e)))
                .collect::<Result<Vec<_>, _>>()?;
            RestrictedExpression::new_record(fields).map_err(|e| AuthzError::Entity {
                message: format!("duplicate record key in attribute: {e}"),
            })?
        }
    };
    Ok(expr)
}

fn set_of_strings<'a>(items: impl IntoIterator<Item = &'a String>) -> RestrictedExpression {
    RestrictedExpression::new_set(
        items
            .into_iter()
            .map(|s| RestrictedExpression::new_string(s.clone())),
    )
}

/// Pushes one caller-supplied extra attribute onto an entity's attribute
/// list, after rejecting reserved keys. A top-level JSON `null` is treated
/// as *attribute absent* (skipped) so `entity has key` reads `false` and a
/// policy can guard with `has` — the intuitive meaning. (A `null` nested
/// inside an array/record still gets a value via [`json_to_expr`], since an
/// element cannot be "absent".)
fn push_extra_attr(
    attrs: &mut Vec<(String, RestrictedExpression)>,
    key: &str,
    value: &Value,
    reserved: impl Fn(&str) -> bool,
    reserved_msg: &str,
) -> Result<(), AuthzError> {
    if reserved(key) {
        return Err(AuthzError::Entity {
            message: format!("attribute {key:?} is reserved ({reserved_msg})"),
        });
    }
    if value.is_null() {
        return Ok(());
    }
    attrs.push((key.to_owned(), json_to_expr(value)?));
    Ok(())
}

fn build_principal_entity(principal: &AuthzPrincipal) -> Result<Entity, AuthzError> {
    let uid = principal_uid(principal)?;
    let mut attrs: Vec<(String, RestrictedExpression)> = vec![
        (
            "id".to_owned(),
            RestrictedExpression::new_string(principal.id.clone()),
        ),
        ("groups".to_owned(), set_of_strings(&principal.groups)),
        ("roles".to_owned(), set_of_strings(&principal.roles)),
    ];
    if let Some(purpose) = &principal.purpose {
        attrs.push((
            "purpose".to_owned(),
            RestrictedExpression::new_string(purpose.clone()),
        ));
    }
    if let Some(env) = &principal.environment {
        attrs.push((
            "environment".to_owned(),
            RestrictedExpression::new_string(env.clone()),
        ));
    }
    for (k, v) in &principal.attributes {
        push_extra_attr(
            &mut attrs,
            k,
            v,
            is_reserved_principal_attr,
            "id/groups/roles/purpose/environment",
        )?;
    }
    Entity::new(
        uid,
        attrs.into_iter().collect(),
        std::collections::HashSet::new(),
    )
    .map_err(|e| AuthzError::Entity {
        message: format!("failed to build principal entity: {e}"),
    })
}

fn build_resource_entity(resource: &AuthzResource) -> Result<Entity, AuthzError> {
    let uid = resource_uid(resource)?;
    let mut attrs: Vec<(String, RestrictedExpression)> =
        vec![("tags".to_owned(), set_of_strings(&resource.tags))];
    if let Some(owner) = &resource.owner {
        attrs.push((
            "owner".to_owned(),
            RestrictedExpression::new_string(owner.clone()),
        ));
    }
    if let Some(classification) = &resource.classification {
        attrs.push((
            "classification".to_owned(),
            RestrictedExpression::new_string(classification.clone()),
        ));
    }
    for (k, v) in &resource.attributes {
        push_extra_attr(
            &mut attrs,
            k,
            v,
            is_reserved_resource_attr,
            "tags/owner/classification",
        )?;
    }
    // Parent link (column -> table/view) so `resource in Table::"…"` works.
    let mut parents = std::collections::HashSet::new();
    if let Some(parent_id) = &resource.parent {
        // A column's parent is a table by default; a view column's parent
        // is a view. We cannot know which from the id alone, so link to
        // both a Table and a View uid — Cedar `in` matches whichever the
        // policy names, and a nonexistent-in-store parent is simply never
        // matched. This keeps the crate free of a schema requirement.
        parents.insert(uid_table_or_view(parent_id, true)?);
        parents.insert(uid_table_or_view(parent_id, false)?);
    }
    Entity::new(uid, attrs.into_iter().collect(), parents).map_err(|e| AuthzError::Entity {
        message: format!("failed to build resource entity: {e}"),
    })
}

fn uid_table_or_view(id: &str, table: bool) -> Result<EntityUid, AuthzError> {
    uid(if table { "Table" } else { "View" }, id)
}

fn build_entities(
    principal: &AuthzPrincipal,
    resource: &AuthzResource,
) -> Result<Entities, AuthzError> {
    let principal_entity = build_principal_entity(principal)?;
    let resource_entity = build_resource_entity(resource)?;

    // If the resource is a column with a parent, add stub parent entities so
    // the `in` relation resolves even when the caller did not pass the
    // parent separately. Their attributes are empty; policies that read a
    // parent's attributes should pass the parent as the resource instead.
    let mut entity_vec = vec![principal_entity, resource_entity];
    if let Some(parent_id) = &resource.parent {
        for table in [true, false] {
            let puid = uid_table_or_view(parent_id, table)?;
            let stub = Entity::new(
                puid,
                std::collections::HashMap::new(),
                std::collections::HashSet::new(),
            )
            .map_err(|e| AuthzError::Entity {
                message: format!("failed to build parent stub entity: {e}"),
            })?;
            entity_vec.push(stub);
        }
    }

    Entities::from_entities(entity_vec, None).map_err(|e| AuthzError::Entity {
        message: format!("failed to assemble entity store: {e}"),
    })
}

fn build_context(context: &RequestContext) -> Result<Context, AuthzError> {
    let mut fields: Vec<(String, RestrictedExpression)> = Vec::new();
    // `now` as a Cedar `datetime` extension value (default feature), so
    // time-window policies can write `context.now < datetime("…")`. Built
    // from the RFC3339 form with millisecond precision, which the extension
    // parses. If for any reason the extension expression does not parse
    // (e.g. a build without the `datetime` feature), we simply omit `now`
    // and rely on `now_millis` below — a policy set that uses `now` would
    // then error visibly rather than silently mis-evaluate.
    let rfc3339 = context
        .now
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    if let Ok(now_expr) =
        RestrictedExpression::from_str(&format!("datetime({})", json_string(&rfc3339)))
    {
        fields.push(("now".to_owned(), now_expr));
    }
    // Also expose epoch millis as a plain long so policies can do simple
    // numeric window comparisons without the datetime extension if they
    // prefer.
    fields.push((
        "now_millis".to_owned(),
        RestrictedExpression::new_long(context.now.timestamp_millis()),
    ));
    if let Some(purpose) = &context.purpose {
        fields.push((
            "purpose".to_owned(),
            RestrictedExpression::new_string(purpose.clone()),
        ));
    }
    for (k, v) in &context.session {
        if k == "now" || k == "now_millis" || k == "purpose" {
            return Err(AuthzError::Request {
                message: format!("session attribute {k:?} is reserved"),
            });
        }
        // A top-level null session value means "absent" (see push_extra_attr).
        if v.is_null() {
            continue;
        }
        fields.push((k.clone(), json_to_expr(v)?));
    }
    Context::from_pairs(fields).map_err(|e| AuthzError::Request {
        message: format!("failed to build context: {e}"),
    })
}

/// Renders a string as a JSON/Cedar double-quoted string literal (Cedar
/// string-literal escaping matches JSON), for embedding in an extension
/// call like `datetime("…")`.
fn json_string(s: &str) -> String {
    Value::String(s.to_owned()).to_string()
}

fn is_reserved_principal_attr(key: &str) -> bool {
    matches!(key, "id" | "groups" | "roles" | "purpose" | "environment")
}

fn is_reserved_resource_attr(key: &str) -> bool {
    matches!(key, "tags" | "owner" | "classification")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::principal::PrincipalKind;
    use crate::resource::ResourceKind;
    use serde_json::json;

    fn user(id: &str) -> AuthzPrincipal {
        AuthzPrincipal::new(id, PrincipalKind::User)
    }

    fn table(id: &str) -> AuthzResource {
        AuthzResource::new(id, ResourceKind::Table)
    }

    #[test]
    fn extra_principal_attribute_is_readable_in_a_policy() {
        // A policy that reads a caller-supplied extra attribute
        // (`principal.clearance`) evaluates against it.
        let engine = PolicyEngine::new(
            r"permit(principal, action, resource) when { principal.clearance >= 3 };",
            BaseEffect::DenyUnlessPermitted,
        )
        .unwrap();
        let cleared = user("alice").with_attribute("clearance", json!(5));
        let d = engine
            .authorize(&cleared, Action::Read, &table("t"), &RequestContext::now())
            .unwrap();
        assert!(d.is_allow());

        let uncleared = user("bob").with_attribute("clearance", json!(1));
        let d = engine
            .authorize(
                &uncleared,
                Action::Read,
                &table("t"),
                &RequestContext::now(),
            )
            .unwrap();
        assert!(d.is_deny());
    }

    #[test]
    fn reserved_principal_attribute_is_rejected() {
        let engine = PolicyEngine::empty(BaseEffect::AllowUnlessForbidden).unwrap();
        // Trying to override `groups` via the extras bag is an error, not a
        // silent clobber.
        let p = user("alice").with_attribute("groups", json!(["x"]));
        let err = engine
            .authorize(&p, Action::Read, &table("t"), &RequestContext::now())
            .unwrap_err();
        assert!(matches!(err, AuthzError::Entity { .. }), "got {err:?}");
    }

    #[test]
    fn reserved_resource_attribute_is_rejected() {
        let engine = PolicyEngine::empty(BaseEffect::AllowUnlessForbidden).unwrap();
        let mut r = table("t");
        r.attributes.insert("tags".to_owned(), json!(["x"]));
        let err = engine
            .authorize(&user("alice"), Action::Read, &r, &RequestContext::now())
            .unwrap_err();
        assert!(matches!(err, AuthzError::Entity { .. }));
    }

    #[test]
    fn reserved_context_attribute_is_rejected() {
        let engine = PolicyEngine::empty(BaseEffect::AllowUnlessForbidden).unwrap();
        let ctx = RequestContext::now().with_session("purpose", json!("sneaky"));
        let err = engine
            .authorize(&user("alice"), Action::Read, &table("t"), &ctx)
            .unwrap_err();
        assert!(matches!(err, AuthzError::Request { .. }));
    }

    #[test]
    fn null_extra_attribute_is_treated_as_absent() {
        // A caller-supplied null attribute must read as *absent* so a
        // `has` guard works, not as an empty set that a bare read would
        // choke on.
        let engine = PolicyEngine::new(
            r"permit(principal, action, resource) when { !(principal has clearance) };",
            BaseEffect::DenyUnlessPermitted,
        )
        .unwrap();
        // clearance set to JSON null => absent => `!(principal has clearance)`
        // is true => permit fires.
        let p = user("alice").with_attribute("clearance", json!(null));
        let d = engine
            .authorize(&p, Action::Read, &table("t"), &RequestContext::now())
            .unwrap();
        assert!(d.is_allow(), "null attribute reads as absent");
        assert!(d.errors.is_empty(), "no evaluation error for a null attr");
    }

    #[test]
    fn session_attribute_is_readable_in_a_policy() {
        // Break-glass: a permit gated on a session attribute the caller set.
        let engine = PolicyEngine::new(
            r"permit(principal, action, resource) when { context.break_glass == true };",
            BaseEffect::DenyUnlessPermitted,
        )
        .unwrap();
        let ctx = RequestContext::now().with_session("break_glass", json!(true));
        let d = engine
            .authorize(&user("alice"), Action::Read, &table("t"), &ctx)
            .unwrap();
        assert!(d.is_allow());

        // Without the session flag, deny.
        let d = engine
            .authorize(
                &user("alice"),
                Action::Read,
                &table("t"),
                &RequestContext::now(),
            )
            .unwrap();
        assert!(d.is_deny());
    }

    #[test]
    fn column_resource_is_child_of_its_table() {
        // A forbid that matches columns of a specific table via `in`.
        let engine = PolicyEngine::new(
            r#"
            @id("no-ssn-column")
            forbid(principal, action == Action::"read", resource)
              when { resource in Table::"sales.orders" }
              when { resource.tags.contains("pii:high") };
            "#,
            BaseEffect::AllowUnlessForbidden,
        )
        .unwrap();
        let ssn = AuthzResource::new("sales.orders#ssn", ResourceKind::Column)
            .with_tag("pii:high")
            .with_parent("sales.orders");
        let d = engine
            .authorize(&user("alice"), Action::Read, &ssn, &RequestContext::now())
            .unwrap();
        assert!(d.is_deny(), "column is `in` its parent table");
        assert_eq!(
            d.determining_policies[0].annotation_id.as_deref(),
            Some("no-ssn-column")
        );
    }

    #[test]
    fn evaluation_error_is_surfaced_but_does_not_grant() {
        // A permit reading a missing attribute errors during evaluation;
        // the error is recorded and the permit does NOT fire (fail closed).
        let engine = PolicyEngine::new(
            r#"permit(principal, action, resource) when { resource.missing == "x" };"#,
            BaseEffect::DenyUnlessPermitted,
        )
        .unwrap();
        let d = engine
            .authorize(
                &user("alice"),
                Action::Read,
                &table("t"),
                &RequestContext::now(),
            )
            .unwrap();
        assert!(d.is_deny(), "an erroring permit does not grant access");
        assert!(!d.errors.is_empty(), "the evaluation error is captured");
        assert!(
            d.reason.contains("evaluation error"),
            "reason mentions the error: {}",
            d.reason
        );
    }

    #[test]
    fn all_action_verbs_map_to_distinct_cedar_ids() {
        let ids: std::collections::HashSet<&str> =
            Action::ALL.iter().map(|a| a.cedar_id()).collect();
        assert_eq!(ids.len(), Action::ALL.len(), "action ids are distinct");
    }

    #[test]
    fn service_and_agent_principals_get_distinct_entity_types() {
        // A policy scoped to `principal is Service` must not catch an Agent.
        let engine = PolicyEngine::new(
            r"forbid(principal is Service, action, resource);",
            BaseEffect::AllowUnlessForbidden,
        )
        .unwrap();
        let svc = AuthzPrincipal::new("etl", PrincipalKind::Service);
        let agent = AuthzPrincipal::new("bot", PrincipalKind::Agent);
        assert!(
            engine
                .authorize(&svc, Action::Read, &table("t"), &RequestContext::now())
                .unwrap()
                .is_deny()
        );
        assert!(
            engine
                .authorize(&agent, Action::Read, &table("t"), &RequestContext::now())
                .unwrap()
                .is_allow(),
            "agent is a different entity type than service"
        );
    }
}
