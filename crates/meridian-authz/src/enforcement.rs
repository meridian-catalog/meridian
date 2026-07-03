//! Row-filter and column-mask **enforcement-decision types** — the output
//! that the scan-plan enforcement seam (D-F2.1) consumes.
//!
//! This crate *owns* these types (the documented boundary: enforcement
//! decisions live here, persistence lives in the store). A
//! [`RowFilter`] compiles to [`meridian_iceberg::expr::Expression`] — the
//! exact IRC scan-filter tree that
//! `meridian_server::planning::apply_row_policy_seam` folds into every
//! returned `FileScanTask`'s residual — so there is no lossy re-encoding
//! between "what a policy says" and "what the planner enforces". A
//! [`ColumnMask`] names a column and how it must be transformed (or
//! dropped); the scan-plan route strips/rewrites masked columns from the
//! projection.
//!
//! ## Enforcement is a *conjunction*
//!
//! Multiple row filters that apply to the same `(principal, table)` are
//! **AND-ed**: every applicable policy must be satisfied for a row to be
//! visible (a layered "you may see EU rows" + "you may see non-deleted
//! rows" yields "EU AND non-deleted"). [`Enforcement::row_predicate`]
//! performs that fold and returns a single `Expression`, or `None` when no
//! row filter applies (the planner then injects nothing).

use std::collections::BTreeMap;

use meridian_iceberg::expr::{CompareOp, Expression, SetOp, Term, UnaryOp};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A predicate the engine resolved for row-level filtering.
///
/// This is a small, closed predicate AST — deliberately *not* free-form
/// SQL — so that every row filter provably compiles to an IRC
/// [`Expression`] and is enforceable inside scan planning. (Free-form SQL
/// row filters are a compiled-secure-view concern, D-F2.2, handled by the
/// `SQLGlot` subsystem, not here.) A filter references a **column** and,
/// crucially, may reference a **session attribute** by substituting the
/// principal's/context's value at resolution time — that substitution has
/// already happened by the time a `RowFilter` exists, so the literals here
/// are concrete.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowFilter {
    /// The id of the policy this filter came from (for audit + dedup).
    pub source_policy: String,
    /// The predicate to enforce.
    pub predicate: RowPredicate,
}

impl RowFilter {
    /// A row filter from a policy id and predicate.
    #[must_use]
    pub fn new(source_policy: impl Into<String>, predicate: RowPredicate) -> Self {
        Self {
            source_policy: source_policy.into(),
            predicate,
        }
    }

    /// Compiles this filter's predicate to an IRC [`Expression`].
    #[must_use]
    pub fn to_expression(&self) -> Expression {
        self.predicate.to_expression()
    }
}

/// A closed predicate AST for row filters. Mirrors the operators the IRC
/// [`Expression`] supports so compilation is total and lossless.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum RowPredicate {
    /// Always true (row always visible under this filter).
    True,
    /// Always false (row never visible under this filter).
    False,
    /// Logical AND of two predicates.
    And {
        /// Left operand.
        left: Box<RowPredicate>,
        /// Right operand.
        right: Box<RowPredicate>,
    },
    /// Logical OR of two predicates.
    Or {
        /// Left operand.
        left: Box<RowPredicate>,
        /// Right operand.
        right: Box<RowPredicate>,
    },
    /// Logical NOT of a predicate.
    Not {
        /// The negated predicate.
        child: Box<RowPredicate>,
    },
    /// `column = value`.
    Eq {
        /// Column name.
        column: String,
        /// Literal (raw JSON single-value form, typed at bind time by the
        /// planner against the table schema).
        value: Value,
    },
    /// `column != value`.
    NotEq {
        /// Column name.
        column: String,
        /// Literal.
        value: Value,
    },
    /// `column < value`.
    Lt {
        /// Column name.
        column: String,
        /// Literal.
        value: Value,
    },
    /// `column <= value`.
    LtEq {
        /// Column name.
        column: String,
        /// Literal.
        value: Value,
    },
    /// `column > value`.
    Gt {
        /// Column name.
        column: String,
        /// Literal.
        value: Value,
    },
    /// `column >= value`.
    GtEq {
        /// Column name.
        column: String,
        /// Literal.
        value: Value,
    },
    /// `column IN (values)`.
    In {
        /// Column name.
        column: String,
        /// Literals.
        values: Vec<Value>,
    },
    /// `column NOT IN (values)`.
    NotIn {
        /// Column name.
        column: String,
        /// Literals.
        values: Vec<Value>,
    },
    /// `column IS NULL`.
    IsNull {
        /// Column name.
        column: String,
    },
    /// `column IS NOT NULL`.
    NotNull {
        /// Column name.
        column: String,
    },
}

impl RowPredicate {
    /// Compiles to the IRC [`Expression`] tree (exact `OpenAPI` shapes).
    #[must_use]
    pub fn to_expression(&self) -> Expression {
        match self {
            Self::True => Expression::True,
            Self::False => Expression::False,
            Self::And { left, right } => Expression::And {
                left: Box::new(left.to_expression()),
                right: Box::new(right.to_expression()),
            },
            Self::Or { left, right } => Expression::Or {
                left: Box::new(left.to_expression()),
                right: Box::new(right.to_expression()),
            },
            Self::Not { child } => Expression::Not {
                child: Box::new(child.to_expression()),
            },
            Self::Eq { column, value } => comparison(CompareOp::Eq, column, value),
            Self::NotEq { column, value } => comparison(CompareOp::NotEq, column, value),
            Self::Lt { column, value } => comparison(CompareOp::Lt, column, value),
            Self::LtEq { column, value } => comparison(CompareOp::LtEq, column, value),
            Self::Gt { column, value } => comparison(CompareOp::Gt, column, value),
            Self::GtEq { column, value } => comparison(CompareOp::GtEq, column, value),
            Self::In { column, values } => Expression::Set {
                op: SetOp::In,
                term: Term::Reference(column.clone()),
                values: values.clone(),
            },
            Self::NotIn { column, values } => Expression::Set {
                op: SetOp::NotIn,
                term: Term::Reference(column.clone()),
                values: values.clone(),
            },
            Self::IsNull { column } => Expression::Unary {
                op: UnaryOp::IsNull,
                term: Term::Reference(column.clone()),
            },
            Self::NotNull { column } => Expression::Unary {
                op: UnaryOp::NotNull,
                term: Term::Reference(column.clone()),
            },
        }
    }
}

fn comparison(op: CompareOp, column: &str, value: &Value) -> Expression {
    Expression::Comparison {
        op,
        term: Term::Reference(column.to_owned()),
        value: value.clone(),
    }
}

/// How a column must be transformed before a principal sees it.
///
/// The scan-plan route applies masks by omitting or rewriting the column
/// in the returned projection; the compiled-secure-view path (D-F2.2)
/// applies the SQL forms. `Null`/`Drop`/`Hash`/`Partial`/`Custom` cover
/// the D-F1 mask kinds. The *strongest* mask wins when several apply (see
/// [`MaskKind::strength`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MaskKind {
    /// Replace every value with NULL.
    Null,
    /// Drop the column entirely (absent from results — the strongest, and
    /// what the agent gateway needs so schema of restricted columns cannot
    /// leak).
    Drop,
    /// Replace with a stable hash of the value.
    Hash,
    /// Show only part of the value (e.g. last 4 digits); `show_last` /
    /// `show_first` describe how much.
    Partial {
        /// Number of leading characters to reveal (0 = none).
        show_first: u32,
        /// Number of trailing characters to reveal (0 = none).
        show_last: u32,
    },
    /// A custom SQL expression (compiled-view path only; the scan-plan path
    /// treats an unresolvable custom mask as [`MaskKind::Drop`] — fail
    /// closed).
    Custom {
        /// The masking SQL expression, in the catalog's canonical dialect.
        expression: String,
    },
}

impl MaskKind {
    /// A total order on masks so the strongest wins when several apply to
    /// one column. `Drop` > `Hash` > `Null` > `Custom` > `Partial`.
    ///
    /// Rationale: `Drop` hides even the column's existence; `Hash` and
    /// `Null` fully hide values (Hash ranked above Null because it also
    /// resists a "this column is all-NULL" inference for a mostly-null
    /// column); `Custom` is treated conservatively (its strength is
    /// unknown to us, so it outranks a partial reveal); `Partial` reveals
    /// the most, so it is weakest.
    #[must_use]
    pub fn strength(&self) -> u8 {
        match self {
            Self::Drop => 4,
            Self::Hash => 3,
            Self::Null => 2,
            Self::Custom { .. } => 1,
            Self::Partial { .. } => 0,
        }
    }
}

/// A resolved mask on a specific column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMask {
    /// The column to mask.
    pub column: String,
    /// How to mask it.
    pub kind: MaskKind,
    /// The id of the policy this mask came from (for audit).
    pub source_policy: String,
}

impl ColumnMask {
    /// A mask from parts.
    #[must_use]
    pub fn new(
        column: impl Into<String>,
        kind: MaskKind,
        source_policy: impl Into<String>,
    ) -> Self {
        Self {
            column: column.into(),
            kind,
            source_policy: source_policy.into(),
        }
    }
}

/// The complete set of enforcement actions for one `(principal, resource)`
/// pair: the row filters to AND together and the column masks to apply.
///
/// This is what [`crate::resolve_filters_and_masks`] returns and what
/// wave-2 scan-plan enforcement consumes.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Enforcement {
    /// Row filters that all apply (AND-ed together). Empty means no
    /// row-level restriction.
    pub row_filters: Vec<RowFilter>,
    /// Column masks that apply, at most one per column (the strongest, per
    /// [`MaskKind::strength`], after [`Enforcement::normalize`]).
    pub column_masks: Vec<ColumnMask>,
}

impl Enforcement {
    /// An empty enforcement (nothing to apply).
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Whether there is nothing to enforce.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_filters.is_empty() && self.column_masks.is_empty()
    }

    /// Folds all row filters into a single IRC [`Expression`] by AND-ing
    /// them, or returns `None` when there are none. The result is exactly
    /// what the scan-plan seam ANDs into each task's residual.
    #[must_use]
    pub fn row_predicate(&self) -> Option<Expression> {
        let mut iter = self.row_filters.iter();
        let first = iter.next()?;
        let mut acc = first.to_expression();
        for filter in iter {
            acc = Expression::And {
                left: Box::new(acc),
                right: Box::new(filter.to_expression()),
            };
        }
        Some(acc)
    }

    /// The set of column names that are masked (any kind).
    #[must_use]
    pub fn masked_columns(&self) -> Vec<String> {
        self.column_masks.iter().map(|m| m.column.clone()).collect()
    }

    /// Collapses multiple masks on the same column to the strongest one,
    /// keeping a deterministic order (masked columns sorted by name). Row
    /// filters are left as-is (their conjunction is order-independent).
    /// Called by the resolver before returning.
    pub fn normalize(&mut self) {
        let mut strongest: BTreeMap<String, ColumnMask> = BTreeMap::new();
        for mask in self.column_masks.drain(..) {
            strongest
                .entry(mask.column.clone())
                .and_modify(|existing| {
                    if mask.kind.strength() > existing.kind.strength() {
                        *existing = mask.clone();
                    }
                })
                .or_insert(mask);
        }
        self.column_masks = strongest.into_values().collect();
    }
}
