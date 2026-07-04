//! Governance enforcement bridge (Pillar D / D-F2): the layer that maps
//! persisted policy + tag rows (`meridian_store::{policy, tags}`) onto the
//! pure decision library (`meridian_authz`) and turns the result into the two
//! things the scan planner injects — a **row-filter expression** and a set of
//! **columns to remove** — while capturing an audit-grade record of every
//! decision.
//!
//! # Where this sits (Layer 1 of the enforcement matrix)
//!
//! This is the strongest layer: for engines that use server-side scan
//! planning (`DuckDB`, `PyIceberg`, `Daft`, any 1.11-planning client),
//! enforcement happens *inside the plan the engine executes*, so a
//! cooperating client cannot read a masked column or a filtered-out row — the
//! bytes are never offered. The honest limits (a client that ignores planning
//! and reads files directly with broad credentials is bounded only by the
//! storage floor, Layer 4) are stated in `docs/design/enforcement-matrix.md`;
//! nothing here should be read as a claim of universal enforcement.
//!
//! # The mask-on-scan-plan rule (fail closed)
//!
//! A scan plan returns *file bytes*; it cannot rewrite a value (`hash(email)`,
//! partial reveal). So on this path **every** column mask — Null, Hash,
//! Partial, Custom, Drop — is enforced by **removing the column** from the
//! returned scan (its stats are stripped and it is recorded absent). Removal
//! is strictly safe: the value is never exposed. A masked-but-not-dropped
//! value (e.g. show last 4 digits) that a customer wants *visible-but-masked*
//! is a compiled-secure-view concern (Layer 2, D-F2.2), not scan planning —
//! documented so the guarantee is never overstated. Column *removal* here is
//! exactly what the agent gateway (H-F2) needs: a restricted column's schema
//! is absent, not nulled.
//!
//! # The type boundary
//!
//! The store hands us opaque `serde_json::Value` policy definitions (it does
//! not depend on `meridian-authz`); we deserialize them into
//! [`meridian_authz::AbacRule`] here (this crate depends on both), so the ADR
//! 009 boundary holds: persistence vocabulary in the store, decision
//! vocabulary in authz, and the mapping between them lives exactly here.

use meridian_authz::engine::BaseEffect;
use meridian_authz::{
    Action, AuthzPrincipal, AuthzResource, Decision, Enforcement, PolicyEngine, PrincipalKind,
    RequestContext, ResolvedColumn, ResourceKind, compile_ruleset, resolve_filters_and_masks,
};
use meridian_common::principal::Principal;
use meridian_iceberg::expr::Expression;
use meridian_iceberg::spec::Schema;
use meridian_store::policy::{self, AppliedPolicy, AppliedVia};
use meridian_store::rbac;
use meridian_store::tags;
use meridian_store::tenancy;
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::error::ApiError;

/// The governance decision for one `(principal, table[, purpose])`, ready for
/// the planner to apply and for the audit trail to record.
#[derive(Debug, Clone)]
pub struct ScanPolicy {
    /// Whether the ABAC layer *denies* the read outright (a `pii:high`
    /// deny-unless-purpose with no matching purpose, an explicit group
    /// forbid, …). When true, the plan must be refused (403) — RBAC already
    /// said yes, ABAC overrides with a deny.
    pub denied: bool,
    /// The row-filter predicate to AND into every returned scan task's
    /// residual, or `None` when no row filter applies.
    pub row_filter: Option<Expression>,
    /// The columns to remove from the returned scan (masked or, transitively,
    /// dropped). Sorted, de-duplicated. The planner strips their stats and
    /// records them absent.
    pub removed_columns: Vec<String>,
    /// The ids of every policy that contributed to this decision (row
    /// filters, masks, and the deciding allow/deny policies), for the audit
    /// record. Sorted, de-duplicated.
    pub applied_policies: Vec<String>,
    /// The human-readable decision reason from the ABAC engine (captured for
    /// the audit trail — the audit trail is the product, D-F2).
    pub reason: String,
}

impl ScanPolicy {
    /// The permissive result: nothing to enforce (no policies apply).
    #[must_use]
    pub fn allow_all() -> Self {
        Self {
            denied: false,
            row_filter: None,
            removed_columns: Vec::new(),
            applied_policies: Vec::new(),
            reason: "no ABAC policies apply".to_owned(),
        }
    }

    /// Whether this policy changes what the caller sees (a filter, a removed
    /// column, or an outright deny). A no-op policy is not worth an audit row
    /// on every plan; a policy that changes the result always is.
    #[must_use]
    pub fn is_effective(&self) -> bool {
        self.denied || self.row_filter.is_some() || !self.removed_columns.is_empty()
    }

    /// The audit detail payload describing what was enforced.
    #[must_use]
    pub fn audit_details(&self, table_id: &str) -> Value {
        json!({
            "table_id": table_id,
            "denied": self.denied,
            "row_filter_applied": self.row_filter.is_some(),
            "removed_columns": self.removed_columns,
            "applied_policies": self.applied_policies,
            "reason": self.reason,
        })
    }
}

/// Maps a request [`Principal`] plus its RBAC roles onto an
/// [`AuthzPrincipal`]. RBAC roles are surfaced as both `roles` and `groups`
/// (Meridian's identity model exposes roles, not IdP groups, today — group-
/// based rules match role names; when SCIM/IdP groups land they are added
/// here without changing rule authoring). The declared `purpose` is set when
/// the caller supplied one.
fn authz_principal(
    principal: &Principal,
    roles: &[String],
    purpose: Option<&str>,
) -> AuthzPrincipal {
    let kind = match principal.kind {
        meridian_common::principal::PrincipalKind::Agent => PrincipalKind::Agent,
        meridian_common::principal::PrincipalKind::Service => PrincipalKind::Service,
        // Anonymous principals only reach here in auth-disabled dev mode,
        // where authorization is bypassed entirely before this point; treat
        // them as users for the (unreached) mapping.
        _ => PrincipalKind::User,
    };
    let mut p = AuthzPrincipal::new(principal.audit_string(), kind);
    for role in roles {
        p.roles.push(role.clone());
        p.groups.push(role.clone());
    }
    if let Some(purpose) = purpose {
        p.purpose = Some(purpose.to_owned());
    }
    p
}

/// Splits resolved table tags into the table-level tag set and the per-column
/// tag map, for the `AuthzResource` and `ResolvedColumn`s.
fn split_tags(
    resolved: &[tags::ResolvedTag],
) -> (Vec<String>, std::collections::BTreeMap<String, Vec<String>>) {
    let mut table_tags: Vec<String> = Vec::new();
    let mut column_tags: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    for rt in resolved {
        match &rt.column_name {
            None => table_tags.push(rt.tag.clone()),
            Some(col) => column_tags
                .entry(col.clone())
                .or_default()
                .push(rt.tag.clone()),
        }
    }
    table_tags.sort();
    table_tags.dedup();
    (table_tags, column_tags)
}

/// The raw governance decision for one `(principal, table[, purpose])`: the
/// allow/deny gate plus the *un-folded* [`Enforcement`] (row filters as their
/// closed predicate AST, column masks with their exact kinds).
///
/// This is the primitive both enforcement surfaces build on. It differs from
/// [`ScanPolicy`] in one deliberate way: it keeps the **full mask kind** per
/// column (Null/Hash/Partial/Custom/Drop) rather than folding every mask to a
/// removed column. The scan-plan path folds masks to removal (values can't be
/// rewritten in a scan task); the small-scan **query** path (workbench, L-F1)
/// can render a value-preserving mask in SQL and so wants the kind. The agent
/// `run_sql` path (H-F2) then re-folds to drops via [`ScanPolicy`].
#[derive(Debug, Clone)]
pub struct QueryEnforcement {
    /// Whether ABAC denies the read outright (RBAC already said yes).
    pub denied: bool,
    /// The resolved row filters + column masks for this principal (empty when
    /// no policies apply). Consumed directly by `meridian_query::run`.
    pub enforcement: Enforcement,
    /// The ids of every policy that contributed, for the audit record. Sorted,
    /// de-duplicated.
    pub applied_policies: Vec<String>,
    /// The human-readable decision reason from the ABAC engine.
    pub reason: String,
}

impl QueryEnforcement {
    /// The permissive result: nothing to enforce (no policies apply).
    #[must_use]
    pub fn allow_all() -> Self {
        Self {
            denied: false,
            enforcement: Enforcement::none(),
            applied_policies: Vec::new(),
            reason: "no ABAC policies apply".to_owned(),
        }
    }
}

/// Resolves the raw governance decision for a `(principal, table)` read: the
/// allow/deny gate and the un-folded [`Enforcement`] (row filters + column
/// masks with their kinds).
///
/// This is the shared primitive. It:
///   1. resolves the principal's RBAC roles and the table's (and columns')
///      approved tags,
///   2. resolves every enabled policy that applies (directly or via a tag),
///   3. compiles the ABAC rules to Cedar and evaluates the allow/deny gate,
///   4. resolves row filters + column masks (kinds preserved).
///
/// [`resolve_scan_policy`] wraps this and folds the result to the scan-plan
/// shape (masks → removed columns, filter → [`Expression`]); the small-scan
/// query executor consumes [`QueryEnforcement::enforcement`] directly.
///
/// RBAC is assumed already checked by the caller (READ passed); this is the
/// additive ABAC layer.
pub async fn resolve_query_enforcement(
    pool: &PgPool,
    principal: &Principal,
    table: &TableContext<'_>,
    purpose: Option<&str>,
) -> Result<QueryEnforcement, ApiError> {
    let workspace_id = tenancy::default_workspace_id();

    // (1) The principal's roles (best-effort: an unprovisioned principal has
    // no roles, which only ever *widens* what a deny-based policy set can
    // subtract — never grants access). Anonymous/dev-mode never reaches here.
    let roles = match principal.issuer.as_deref() {
        Some(issuer) => {
            match meridian_store::principal::get_by_identity(pool, issuer, &principal.subject)
                .await?
            {
                Some(record) => rbac::effective_permissions(pool, &record.id).await?.roles,
                None => Vec::new(),
            }
        }
        None => Vec::new(),
    };

    // (1b) The approved tags on the table and its columns (+ namespace tags).
    let resolved_tags =
        tags::resolve_table_tags(pool, workspace_id, table.table_id, table.namespace_ids).await?;
    let (table_tags, column_tags) = split_tags(&resolved_tags);

    // (2) Every enabled policy that applies. A tag binding applies when the
    // tag is anywhere on the table — table-level OR on any column (a
    // column-mask policy binds to a tag that lives only on a column). So the
    // binding-match set is the union of table and column rendered tags; the
    // per-column mask then applies only to the columns that carry the tag
    // (resolved by `resolve_filters_and_masks`).
    let mut all_tags: Vec<String> = table_tags.clone();
    for tags in column_tags.values() {
        all_tags.extend(tags.iter().cloned());
    }
    all_tags.sort();
    all_tags.dedup();

    let applied = policy::resolve_for_table(
        pool,
        workspace_id,
        table.table_id,
        table.namespace_ids,
        &all_tags,
    )
    .await?;
    if applied.is_empty() {
        return Ok(QueryEnforcement::allow_all());
    }

    // Deserialize each applied policy's definition into an AbacRule (the
    // store kept it opaque; the boundary crossing is exactly here).
    let mut abac_rules = Vec::with_capacity(applied.len());
    let mut policy_ids: Vec<String> = Vec::new();
    for ap in &applied {
        let rule = deserialize_rule(ap)?;
        policy_ids.push(ap.policy.id.clone());
        abac_rules.push(rule);
    }

    // (3) Build the authz principal + resource and run the allow/deny gate.
    let authz_principal = authz_principal(principal, &roles, purpose);
    let mut resource = AuthzResource::new(table.table_id, ResourceKind::Table);
    resource.tags.clone_from(&table_tags);
    if let Some(owner) = table.owner {
        resource.owner = Some(owner.to_owned());
    }

    let mut context = RequestContext::now();
    if let Some(purpose) = purpose {
        context = context.with_purpose(purpose);
    }

    let policy_text = compile_ruleset(&abac_rules);
    let decision: Decision = PolicyEngine::new(&policy_text, BaseEffect::AllowUnlessForbidden)
        .and_then(|engine| engine.authorize(&authz_principal, Action::Read, &resource, &context))
        .map_err(|e| {
            ApiError::new(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("policy evaluation failed: {e}"),
            )
        })?;

    if decision.is_deny() {
        return Ok(QueryEnforcement {
            denied: true,
            enforcement: Enforcement::none(),
            applied_policies: dedup_sorted(policy_ids),
            reason: decision.reason,
        });
    }

    // (4) Row filters + column masks. Only columns that exist in the scan
    // schema can be masked; a mask on a dropped/renamed column is inert.
    let columns: Vec<ResolvedColumn> = table
        .schema
        .fields
        .iter()
        .map(|f| {
            let tags = column_tags.get(&f.name).cloned().unwrap_or_default();
            ResolvedColumn::new(f.name.clone(), tags)
        })
        .collect();

    let enforcement: Enforcement =
        resolve_filters_and_masks(&authz_principal, &resource, &columns, &abac_rules);

    Ok(QueryEnforcement {
        denied: false,
        enforcement,
        applied_policies: dedup_sorted(policy_ids),
        reason: decision.reason,
    })
}

/// Resolves the governance decision for a `(principal, table)` read, given the
/// table's schema (for the column universe) and an optional declared purpose,
/// folded into the shape the **scan planner** injects.
///
/// This is the single entry point the scan planner (and the effective-policy
/// API) calls. It resolves the raw decision via [`resolve_query_enforcement`]
/// and then folds it to the scan-plan shape: masks become **removed columns**
/// (fail closed — a scan task returns bytes, not rewritten values, see the
/// module docs) and the row filters fold into a single [`Expression`].
///
/// RBAC is assumed already checked by the caller (READ passed); this is the
/// additive ABAC layer.
pub async fn resolve_scan_policy(
    pool: &PgPool,
    principal: &Principal,
    table: &TableContext<'_>,
    purpose: Option<&str>,
) -> Result<ScanPolicy, ApiError> {
    let resolved = resolve_query_enforcement(pool, principal, table, purpose).await?;

    if resolved.denied {
        return Ok(ScanPolicy {
            denied: true,
            row_filter: None,
            removed_columns: Vec::new(),
            applied_policies: resolved.applied_policies,
            reason: resolved.reason,
        });
    }

    let row_filter = resolved.enforcement.row_predicate();

    // Every mask becomes column removal on the scan-plan path (fail closed).
    let mut removed: Vec<String> = resolved.enforcement.masked_columns();
    removed.sort();
    removed.dedup();

    Ok(ScanPolicy {
        denied: false,
        row_filter,
        removed_columns: removed,
        applied_policies: resolved.applied_policies,
        reason: resolved.reason,
    })
}

/// Table identity + schema the enforcement resolver needs. Kept as a borrow
/// bundle so the caller (which already loaded these) does not re-fetch.
#[derive(Debug, Clone, Copy)]
pub struct TableContext<'a> {
    /// The table's internal id (the enforcement resource id).
    pub table_id: &'a str,
    /// The table's self-and-ancestor namespace ids (for namespace-bound
    /// policies and namespace tags; the caller resolves the chain).
    pub namespace_ids: &'a [String],
    /// The table's current (or scan) schema — the column universe masks and
    /// filters resolve against.
    pub schema: &'a Schema,
    /// The table owner's audit id, if known (for owner-allow rules).
    pub owner: Option<&'a str>,
}

/// Deserializes an applied policy's stored definition into an [`AbacRule`],
/// mapping a decode failure to a clear 500 (a stored policy that does not
/// deserialize is catalog-side corruption, not a client error). The
/// provenance annotation is folded into the reason via the rule's own id.
fn deserialize_rule(applied: &AppliedPolicy) -> Result<meridian_authz::AbacRule, ApiError> {
    serde_json::from_value(applied.policy.definition.clone()).map_err(|e| {
        let via = match &applied.via {
            AppliedVia::Table => "table binding".to_owned(),
            AppliedVia::Namespace { namespace_id } => format!("namespace {namespace_id}"),
            AppliedVia::Tag { tag } => format!("tag {tag}"),
        };
        ApiError::new(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
            format!(
                "policy {:?} (applied via {via}) has an undecodable definition: {e}",
                applied.policy.name
            ),
        )
    })
}

fn dedup_sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v.dedup();
    v
}

/// ANDs a governance row-filter expression into an existing residual (the
/// planner's per-file residual). Either side may be absent; the result is the
/// conjunction, or whichever single side exists, or `None`.
///
/// This is the exact fold the scan-plan seam performs: a policy predicate is
/// AND-ed onto the residual *after* partition-pruning folding, so pruning can
/// never drop it and every returned task carries it. Kept here (next to the
/// decision) so the seam in `planning::engine` stays a thin call.
#[must_use]
pub fn and_residual(
    residual: Option<Expression>,
    policy: Option<&Expression>,
) -> Option<Expression> {
    match (residual, policy) {
        (Some(r), Some(p)) => Some(Expression::And {
            left: Box::new(r),
            right: Box::new(p.clone()),
        }),
        (Some(r), None) => Some(r),
        (None, Some(p)) => Some(p.clone()),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use meridian_authz::{AbacRule, MaskKind, RowPredicate};

    use super::*;

    fn user(subject: &str) -> Principal {
        Principal {
            kind: meridian_common::principal::PrincipalKind::User,
            subject: subject.to_owned(),
            issuer: Some("https://idp.example.com".to_owned()),
            display_name: None,
        }
    }

    #[test]
    fn and_residual_folds_both_sides() {
        let r = Expression::Comparison {
            op: meridian_iceberg::expr::CompareOp::Eq,
            term: meridian_iceberg::expr::Term::Reference("a".into()),
            value: json!(1),
        };
        let p = Expression::Comparison {
            op: meridian_iceberg::expr::CompareOp::Eq,
            term: meridian_iceberg::expr::Term::Reference("b".into()),
            value: json!(2),
        };
        // both -> and
        assert!(matches!(
            and_residual(Some(r.clone()), Some(&p)),
            Some(Expression::And { .. })
        ));
        // one side only -> that side
        assert_eq!(and_residual(Some(r.clone()), None), Some(r.clone()));
        assert_eq!(and_residual(None, Some(&p)), Some(p.clone()));
        // neither -> none
        assert_eq!(and_residual(None, None), None);
    }

    #[test]
    fn authz_principal_surfaces_roles_as_groups_and_roles() {
        let p = user("alice");
        let mapped = authz_principal(&p, &["data_steward".to_owned()], Some("audit"));
        assert!(mapped.roles.contains(&"data_steward".to_owned()));
        assert!(mapped.groups.contains(&"data_steward".to_owned()));
        assert_eq!(mapped.purpose.as_deref(), Some("audit"));
        assert_eq!(mapped.id, "user:alice");
    }

    #[test]
    fn split_tags_separates_table_and_column() {
        let resolved = vec![
            tags::ResolvedTag {
                tag: "pii:high".into(),
                column_name: None,
            },
            tags::ResolvedTag {
                tag: "pii:email".into(),
                column_name: Some("email".into()),
            },
        ];
        let (table_tags, column_tags) = split_tags(&resolved);
        assert_eq!(table_tags, vec!["pii:high".to_owned()]);
        assert_eq!(
            column_tags.get("email").unwrap(),
            &vec!["pii:email".to_owned()]
        );
    }

    #[test]
    fn scan_policy_effectiveness_and_audit() {
        let mut sp = ScanPolicy::allow_all();
        assert!(!sp.is_effective());
        sp.removed_columns.push("ssn".into());
        assert!(sp.is_effective());
        let details = sp.audit_details("tbl1");
        assert_eq!(details["removed_columns"], json!(["ssn"]));
        assert_eq!(details["denied"], json!(false));
    }

    // A sanity check that a masking rule resolves to a removed column through
    // the same authz path the resolver uses (no DB needed): build the rule,
    // compile+resolve directly.
    #[test]
    fn mask_rule_resolves_to_masked_column() {
        let rule = AbacRule::TagColumnMask {
            id: Some("mask-email".into()),
            description: None,
            tag: "pii:email".into(),
            exempt_groups: vec!["data_steward".into()],
            mask: MaskKind::Hash,
        };
        let principal = AuthzPrincipal::new("user:bob", PrincipalKind::User);
        let mut resource = AuthzResource::new("tbl1", ResourceKind::Table);
        resource.tags = vec![];
        let columns = vec![ResolvedColumn::new("email", vec!["pii:email".into()])];
        let enforcement = resolve_filters_and_masks(&principal, &resource, &columns, &[rule]);
        assert_eq!(enforcement.masked_columns(), vec!["email".to_owned()]);
    }

    #[test]
    fn exempt_group_escapes_mask() {
        let rule = AbacRule::TagColumnMask {
            id: Some("mask-email".into()),
            description: None,
            tag: "pii:email".into(),
            exempt_groups: vec!["data_steward".into()],
            mask: MaskKind::Hash,
        };
        let mut principal = AuthzPrincipal::new("user:carol", PrincipalKind::User);
        principal.groups.push("data_steward".into());
        let resource = AuthzResource::new("tbl1", ResourceKind::Table);
        let columns = vec![ResolvedColumn::new("email", vec!["pii:email".into()])];
        let enforcement = resolve_filters_and_masks(&principal, &resource, &columns, &[rule]);
        assert!(enforcement.masked_columns().is_empty());
    }

    #[test]
    fn row_filter_rule_resolves_to_predicate() {
        let rule = AbacRule::TagRowFilter {
            id: Some("eu-only".into()),
            description: None,
            tag: "residency:eu".into(),
            exempt_groups: vec![],
            predicate: RowPredicate::Eq {
                column: "region".into(),
                value: json!("eu"),
            },
        };
        let principal = AuthzPrincipal::new("user:dan", PrincipalKind::User);
        let mut resource = AuthzResource::new("tbl1", ResourceKind::Table);
        resource.tags = vec!["residency:eu".into()];
        let enforcement = resolve_filters_and_masks(&principal, &resource, &[], &[rule]);
        assert!(enforcement.row_predicate().is_some());
    }
}
