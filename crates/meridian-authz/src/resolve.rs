//! Row-filter and column-mask **resolution** — the D-F2 question "which
//! filters and masks apply to this `(principal, table)`?" answered from the
//! ABAC rule set plus the table's (and its columns') tags.
//!
//! This is what wave-2 scan-plan enforcement calls to learn what to inject
//! into a scan plan. It is intentionally separate from
//! [`crate::PolicyEngine::authorize`]: `authorize` answers *may this
//! request proceed at all* (the allow/deny gate); resolution answers *given
//! that it may proceed, what does the principal get to see* (rows filtered,
//! columns masked). Both read the same rules, so they never disagree.
//!
//! ## Exemption semantics
//!
//! A [`TagRowFilter`](crate::AbacRule::TagRowFilter) /
//! [`TagColumnMask`](crate::AbacRule::TagColumnMask) applies to a principal
//! **unless** the principal is in one of the rule's `exempt_groups`. A
//! filter keyed on a tag applies only when the *table* carries the tag; a
//! mask keyed on a tag applies to each *column* that carries the tag.

use crate::enforcement::Enforcement;
use crate::principal::AuthzPrincipal;
use crate::resource::AuthzResource;
use crate::rules::AbacRule;

/// A column of the table being resolved, with the tags that column carries.
///
/// The store supplies these; the crate does not read schemas. A column's
/// tags are its *effective* tags (after any lineage propagation upstream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedColumn {
    /// The column name (as it appears in the table schema / scan
    /// projection).
    pub name: String,
    /// Tags on this column (e.g. `pii:email`).
    pub tags: Vec<String>,
}

impl ResolvedColumn {
    /// A column with a name and tags.
    #[must_use]
    pub fn new(name: impl Into<String>, tags: Vec<String>) -> Self {
        Self {
            name: name.into(),
            tags,
        }
    }

    fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }
}

/// Resolves the row filters and column masks that apply to `principal`
/// accessing `table` (whose masked columns are described by `columns`),
/// under the ABAC `rules`.
///
/// The returned [`Enforcement`] is normalized (one mask per column, the
/// strongest; row filters AND-ed by [`Enforcement::row_predicate`]). An
/// empty result means the principal sees the table unfiltered and
/// unmasked.
#[must_use]
pub fn resolve_filters_and_masks(
    principal: &AuthzPrincipal,
    table: &AuthzResource,
    columns: &[ResolvedColumn],
    rules: &[AbacRule],
) -> Enforcement {
    let mut enforcement = Enforcement::none();

    for rule in rules {
        // A principal in an exempt group escapes this rule's filter/mask.
        if principal_is_exempt(principal, rule.exempt_groups()) {
            continue;
        }

        match rule {
            AbacRule::TagRowFilter { tag, .. } => {
                // Applies when the *table* carries the tag.
                if table_has_tag(table, tag)
                    && let Some(filter) = rule.as_row_filter()
                {
                    enforcement.row_filters.push(filter);
                }
            }
            AbacRule::TagColumnMask { tag, .. } => {
                // Applies per column that carries the tag.
                for column in columns {
                    if column.has_tag(tag)
                        && let Some(mask) = rule.as_column_mask(&column.name)
                    {
                        enforcement.column_masks.push(mask);
                    }
                }
            }
            _ => {}
        }
    }

    enforcement.normalize();
    enforcement
}

fn principal_is_exempt(principal: &AuthzPrincipal, exempt_groups: &[String]) -> bool {
    exempt_groups.iter().any(|g| principal.groups.contains(g))
}

fn table_has_tag(table: &AuthzResource, tag: &str) -> bool {
    table.tags.iter().any(|t| t == tag)
}
