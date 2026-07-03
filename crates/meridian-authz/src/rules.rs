//! The tag → policy convenience layer.
//!
//! Writing raw Cedar for every governance rule is powerful but verbose.
//! Most catalog policies are a handful of shapes the spec (D-F1) calls out
//! directly — "`pii:high` denies read unless a purpose is granted",
//! "owners may always read", "the `finance` group may read `finance`-tagged
//! tables", "this grant is valid only until a date". [`AbacRule`] captures
//! those shapes as data; [`AbacRule::to_cedar`] compiles one to Cedar
//! policy text, and [`compile_ruleset`] compiles many into one policy
//! document that [`crate::PolicyEngine`] evaluates.
//!
//! The **same rules** also drive row-filter/column-mask resolution
//! ([`crate::resolve_filters_and_masks`]): a [`AbacRule::TagRowFilter`]
//! carries the row predicate to enforce, and a [`AbacRule::TagColumnMask`]
//! carries the mask. Keeping both the Cedar (decision) and the enforcement
//! intent on one rule value is deliberate — a stored rule has one meaning,
//! evaluated two ways (allow/deny *and* filter/mask), and they can never
//! drift.
//!
//! Compilation is a pure string transform with careful escaping (every
//! literal goes through [`cedar_str`]); the output is then parsed by Cedar,
//! so a malformed rule is caught by the parser, never silently mis-applied.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::enforcement::{ColumnMask, MaskKind, RowFilter, RowPredicate};

/// A high-level ABAC rule that compiles to Cedar (and, for the filter/mask
/// variants, to enforcement actions).
///
/// Every rule names the [`Action`](crate::Action) verbs it applies to (as
/// strings, matching [`crate::Action::cedar_id`]); an empty `actions`
/// means "all actions".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AbacRule {
    /// A resource carrying `tag` is **denied** for the given actions,
    /// *unless* the request's purpose is one of `unless_purpose`. This is
    /// the canonical `pii:high` rule. With an empty `unless_purpose` it is
    /// an unconditional deny on the tag.
    TagDenyUnlessPurpose {
        /// Optional human-meaningful id (`@id`) and description
        /// (`@description`) for the audit reason.
        id: Option<String>,
        /// Human-readable reason.
        description: Option<String>,
        /// The tag that triggers the deny (e.g. `pii:high`).
        tag: String,
        /// Actions the deny covers (empty = all).
        actions: Vec<String>,
        /// Purposes that lift the deny.
        unless_purpose: Vec<String>,
    },

    /// The resource's owner is **allowed** the given actions
    /// (`resource.owner == principal.id`).
    OwnerAllow {
        /// Optional id.
        id: Option<String>,
        /// Optional description.
        description: Option<String>,
        /// Actions covered (empty = all).
        actions: Vec<String>,
    },

    /// A principal in any of `groups` is **allowed** the given actions on
    /// resources carrying `tag` (or on all resources when `tag` is
    /// `None`).
    GroupAllow {
        /// Optional id.
        id: Option<String>,
        /// Optional description.
        description: Option<String>,
        /// Groups that gain access.
        groups: Vec<String>,
        /// Restrict to resources with this tag, if set.
        tag: Option<String>,
        /// Actions covered (empty = all).
        actions: Vec<String>,
    },

    /// A principal in any of `groups` is **denied** the given actions
    /// (deny wins over any allow). Useful for explicit exclusions.
    GroupDeny {
        /// Optional id.
        id: Option<String>,
        /// Optional description.
        description: Option<String>,
        /// Groups that lose access.
        groups: Vec<String>,
        /// Restrict to resources with this tag, if set.
        tag: Option<String>,
        /// Actions covered (empty = all).
        actions: Vec<String>,
    },

    /// A **time-bound** allow: the given actions are permitted only while
    /// `context.now` is at/after `not_before` and before `not_after`
    /// (either bound optional). Bounds are RFC3339 timestamps.
    TimeBoundAllow {
        /// Optional id.
        id: Option<String>,
        /// Optional description.
        description: Option<String>,
        /// Lower bound (inclusive), RFC3339, if set.
        not_before: Option<String>,
        /// Upper bound (exclusive), RFC3339, if set.
        not_after: Option<String>,
        /// Actions covered (empty = all).
        actions: Vec<String>,
    },

    /// A resource carrying `tag` gets a **row filter** applied for
    /// principals **not** in `exempt_groups`. This drives both a Cedar
    /// record (informational — the decision remains allow, the *rows* are
    /// filtered) and the enforcement resolution.
    TagRowFilter {
        /// Optional id.
        id: Option<String>,
        /// Optional description.
        description: Option<String>,
        /// The tag that triggers the filter.
        tag: String,
        /// Groups exempt from the filter (see everything).
        exempt_groups: Vec<String>,
        /// The row predicate to enforce for non-exempt principals.
        predicate: RowPredicate,
    },

    /// A column carrying `tag` gets a **mask** applied for principals
    /// **not** in `exempt_groups`.
    TagColumnMask {
        /// Optional id.
        id: Option<String>,
        /// Optional description.
        description: Option<String>,
        /// The tag that triggers the mask (e.g. `pii:email`).
        tag: String,
        /// Groups exempt from the mask (see cleartext).
        exempt_groups: Vec<String>,
        /// How to mask.
        mask: MaskKind,
    },
}

impl AbacRule {
    /// Compiles this rule to Cedar policy text.
    ///
    /// The filter/mask variants compile to a `permit` (the decision is
    /// "allowed" — enforcement happens at the row/column layer, not by
    /// denying the whole request), annotated so the audit trail records
    /// that a filter/mask is in force.
    #[must_use]
    pub fn to_cedar(&self) -> String {
        match self {
            Self::TagDenyUnlessPurpose {
                id,
                description,
                tag,
                actions,
                unless_purpose,
            } => tag_deny_unless_purpose(
                id.as_deref(),
                description.as_deref(),
                tag,
                actions,
                unless_purpose,
            ),

            Self::OwnerAllow {
                id,
                description,
                actions,
            } => {
                let head = annotations(id.as_deref(), description.as_deref());
                let action_clause = action_scope(actions);
                format!(
                    "{head}permit(principal, {action_clause}, resource)\n  when {{ resource has owner && resource.owner == principal.id }};"
                )
            }

            Self::GroupAllow {
                id,
                description,
                groups,
                tag,
                actions,
            } => group_rule(
                true,
                id.as_deref(),
                description.as_deref(),
                groups,
                tag.as_deref(),
                actions,
            ),

            Self::GroupDeny {
                id,
                description,
                groups,
                tag,
                actions,
            } => group_rule(
                false,
                id.as_deref(),
                description.as_deref(),
                groups,
                tag.as_deref(),
                actions,
            ),

            Self::TimeBoundAllow {
                id,
                description,
                not_before,
                not_after,
                actions,
            } => time_bound_allow(
                id.as_deref(),
                description.as_deref(),
                not_before.as_deref(),
                not_after.as_deref(),
                actions,
            ),

            // A row filter and a column mask both compile to the same
            // decision: a tag-gated read *permit* (the request is allowed;
            // the row/column restriction is applied by the enforcement
            // layer). Emitting a policy keeps the rule visible in the set
            // and in the audit reason.
            Self::TagRowFilter {
                id,
                description,
                tag,
                exempt_groups,
                ..
            }
            | Self::TagColumnMask {
                id,
                description,
                tag,
                exempt_groups,
                ..
            } => tag_gated_read_permit(id.as_deref(), description.as_deref(), tag, exempt_groups),
        }
    }

    /// If this rule is a row filter, produces the [`RowFilter`] it enforces
    /// for a non-exempt principal (the caller checks exemption/tag match).
    #[must_use]
    pub fn as_row_filter(&self) -> Option<RowFilter> {
        match self {
            Self::TagRowFilter {
                id, tag, predicate, ..
            } => Some(RowFilter::new(
                id.clone().unwrap_or_else(|| format!("row_filter:{tag}")),
                predicate.clone(),
            )),
            _ => None,
        }
    }

    /// If this rule is a column mask, produces the [`ColumnMask`] for a
    /// given column (the caller supplies the column that carried the tag).
    #[must_use]
    pub fn as_column_mask(&self, column: &str) -> Option<ColumnMask> {
        match self {
            Self::TagColumnMask { id, tag, mask, .. } => Some(ColumnMask::new(
                column,
                mask.clone(),
                id.clone().unwrap_or_else(|| format!("column_mask:{tag}")),
            )),
            _ => None,
        }
    }

    /// The tag this rule keys on, if any (for filtering rules by a
    /// resource's tags during resolution).
    #[must_use]
    pub fn keyed_tag(&self) -> Option<&str> {
        match self {
            Self::TagDenyUnlessPurpose { tag, .. }
            | Self::TagRowFilter { tag, .. }
            | Self::TagColumnMask { tag, .. } => Some(tag),
            Self::GroupAllow { tag, .. } | Self::GroupDeny { tag, .. } => tag.as_deref(),
            _ => None,
        }
    }

    /// The groups exempt from this rule's filter/mask, if applicable.
    #[must_use]
    pub fn exempt_groups(&self) -> &[String] {
        match self {
            Self::TagRowFilter { exempt_groups, .. }
            | Self::TagColumnMask { exempt_groups, .. } => exempt_groups,
            _ => &[],
        }
    }
}

/// Compiles a set of rules into one Cedar policy document (one policy per
/// rule, separated by blank lines).
#[must_use]
pub fn compile_ruleset(rules: &[AbacRule]) -> String {
    rules
        .iter()
        .map(AbacRule::to_cedar)
        .collect::<Vec<_>>()
        .join("\n\n")
}

// ---------------------------------------------------------------------------
// Cedar text helpers (all escaping goes through here)
// ---------------------------------------------------------------------------

/// Compiles a `TagDenyUnlessPurpose` rule.
fn tag_deny_unless_purpose(
    id: Option<&str>,
    description: Option<&str>,
    tag: &str,
    actions: &[String],
    unless_purpose: &[String],
) -> String {
    let head = annotations(id, description);
    let action_clause = action_scope(actions);
    let mut body = format!(
        "forbid(principal, {action_clause}, resource)\n  when {{ resource.tags.contains({}) }}",
        cedar_str(tag)
    );
    if !unless_purpose.is_empty() {
        let disj = unless_purpose
            .iter()
            .map(|p| format!("context.purpose == {}", cedar_str(p)))
            .collect::<Vec<_>>()
            .join(" || ");
        // `context has purpose` guards the access so a request with no
        // purpose does not raise an evaluation error (which would drop the
        // forbid and *weaken* the policy — we want the opposite).
        let _ = write!(body, "\n  unless {{ context has purpose && ({disj}) }}");
    }
    format!("{head}{body};")
}

/// Compiles a `TimeBoundAllow` rule.
fn time_bound_allow(
    id: Option<&str>,
    description: Option<&str>,
    not_before: Option<&str>,
    not_after: Option<&str>,
    actions: &[String],
) -> String {
    let head = annotations(id, description);
    let action_clause = action_scope(actions);
    let mut conds: Vec<String> = Vec::new();
    if let Some(nb) = not_before {
        conds.push(format!("context.now >= datetime({})", cedar_str(nb)));
    }
    if let Some(na) = not_after {
        conds.push(format!("context.now < datetime({})", cedar_str(na)));
    }
    let when = if conds.is_empty() {
        String::new()
    } else {
        // Guard the optional `context.now` with `has` so the policy is
        // schema-safe (strict validation) and fails *closed*: if a request
        // ever arrives without `now`, the time window is not satisfied and
        // this permit does not fire, rather than raising an evaluation
        // error that silently drops the permit.
        format!("\n  when {{ context has now && {} }}", conds.join(" && "))
    };
    format!("{head}permit(principal, {action_clause}, resource){when};")
}

/// Compiles a tag-gated read permit (shared by `TagRowFilter` /
/// `TagColumnMask`): allow `read` on resources carrying `tag`, except for
/// principals in `exempt_groups`. The actual row filter / column mask is
/// applied by the enforcement layer ([`crate::resolve_filters_and_masks`]);
/// this policy exists so the rule is visible in the set and audit reason.
fn tag_gated_read_permit(
    id: Option<&str>,
    description: Option<&str>,
    tag: &str,
    exempt_groups: &[String],
) -> String {
    let head = annotations(id, description);
    let exempt = group_membership_disjunction(exempt_groups);
    let note = if exempt.is_empty() {
        format!("  when {{ resource.tags.contains({}) }}", cedar_str(tag))
    } else {
        format!(
            "  when {{ resource.tags.contains({}) }}\n  when {{ !({exempt}) }}",
            cedar_str(tag)
        )
    };
    format!("{head}permit(principal, action == Action::\"read\", resource)\n{note};")
}

/// Emits `@id(...)` / `@description(...)` annotation lines (each present
/// only when set), terminated by a newline so the policy body follows.
fn annotations(id: Option<&str>, description: Option<&str>) -> String {
    let mut out = String::new();
    if let Some(id) = id {
        let _ = writeln!(out, "@id({})", cedar_str(id));
    }
    if let Some(d) = description {
        let _ = writeln!(out, "@description({})", cedar_str(d));
    }
    out
}

/// The `action` scope clause. An empty action list means all actions
/// (`action`); otherwise `action in [Action::"a", Action::"b"]`, or
/// `action == Action::"a"` for a single one.
fn action_scope(actions: &[String]) -> String {
    match actions {
        [] => "action".to_owned(),
        [one] => format!("action == Action::{}", cedar_str(one)),
        many => {
            let list = many
                .iter()
                .map(|a| format!("Action::{}", cedar_str(a)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("action in [{list}]")
        }
    }
}

/// A `principal.groups.contains("a") || principal.groups.contains("b")`
/// disjunction, or empty when there are no groups.
fn group_membership_disjunction(groups: &[String]) -> String {
    groups
        .iter()
        .map(|g| format!("principal.groups.contains({})", cedar_str(g)))
        .collect::<Vec<_>>()
        .join(" || ")
}

fn group_rule(
    allow: bool,
    id: Option<&str>,
    description: Option<&str>,
    groups: &[String],
    tag: Option<&str>,
    actions: &[String],
) -> String {
    let head = annotations(id, description);
    let action_clause = action_scope(actions);
    let effect = if allow { "permit" } else { "forbid" };
    let mut conds: Vec<String> = Vec::new();
    let membership = group_membership_disjunction(groups);
    if membership.is_empty() {
        // No groups: a group rule with no groups matches nobody. Emit a
        // `when { false }` so it is inert rather than matching everyone.
        conds.push("false".to_owned());
    } else {
        conds.push(format!("({membership})"));
    }
    if let Some(tag) = tag {
        conds.push(format!("resource.tags.contains({})", cedar_str(tag)));
    }
    let when = format!("\n  when {{ {} }}", conds.join(" && "));
    format!("{head}{effect}(principal, {action_clause}, resource){when};")
}

/// Renders a Rust string as a Cedar string literal. Cedar string-literal
/// escaping is JSON-compatible for the characters that matter (`"` and
/// `\\`), so we route through `serde_json` for a correct, injection-safe
/// quoting — no rule input can break out of the literal.
pub(crate) fn cedar_str(s: &str) -> String {
    Value::String(s.to_owned()).to_string()
}
