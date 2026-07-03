//! The REST scan-filter expression model and its planning evaluators.
//!
//! [`Expression`] is the JSON tree the IRC `planTableScan` request carries
//! (`PlanTableScanRequest.filter`), (de)serialized with the exact
//! `OpenAPI` shapes: `and`/`or`/`not`, `lt`/`lt-eq`/`gt`/`gt-eq`/`eq`/
//! `not-eq`, `in`/`not-in`, `is-null`/`not-null`, `is-nan`/`not-nan`,
//! `starts-with`/`not-starts-with`, over a term that is either a column
//! [`Reference`](Term::Reference) or a
//! [`transform`](Term::Transform) of one.
//!
//! Evaluation is split in three, all *inclusive* (three-valued: a file is
//! kept unless it provably cannot contain a matching row):
//!
//! - [`bind`](Expression::bind) resolves names against a schema and types
//!   every literal, eliminating `not` by rewriting to negation normal
//!   form first (metrics evaluators are not safely negatable).
//! - [`project`] converts a bound predicate into a predicate on partition
//!   tuples via the spec's inclusive projection (see the transform
//!   support matrix on [`project`]); [`summaries_might_match`] applies it
//!   to manifest-list partition summaries and [`tuple_might_match`] to a
//!   file's partition tuple.
//! - [`file_might_match`] tests a bound predicate against a data file's
//!   column statistics (value/null/NaN counts, lower/upper bounds).

mod evaluate;
mod project;
pub(crate) mod transforms;

pub use evaluate::{file_might_match, summaries_might_match, tuple_might_match};
pub use project::{PartitionPredicate, project};
pub use transforms::apply as apply_transform;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value, json};

use crate::spec::{PrimitiveType, Schema, Transform, Type};
use crate::value::{Datum, ValueError};

/// A comparison operator (`LiteralExpression.type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    /// `lt`
    Lt,
    /// `lt-eq`
    LtEq,
    /// `gt`
    Gt,
    /// `gt-eq`
    GtEq,
    /// `eq`
    Eq,
    /// `not-eq`
    NotEq,
    /// `starts-with`
    StartsWith,
    /// `not-starts-with`
    NotStartsWith,
}

impl CompareOp {
    fn as_str(self) -> &'static str {
        match self {
            Self::Lt => "lt",
            Self::LtEq => "lt-eq",
            Self::Gt => "gt",
            Self::GtEq => "gt-eq",
            Self::Eq => "eq",
            Self::NotEq => "not-eq",
            Self::StartsWith => "starts-with",
            Self::NotStartsWith => "not-starts-with",
        }
    }

    /// The operator's negation (`NOT (a < b)` is `a >= b`).
    #[must_use]
    pub fn negated(self) -> Self {
        match self {
            Self::Lt => Self::GtEq,
            Self::LtEq => Self::Gt,
            Self::Gt => Self::LtEq,
            Self::GtEq => Self::Lt,
            Self::Eq => Self::NotEq,
            Self::NotEq => Self::Eq,
            Self::StartsWith => Self::NotStartsWith,
            Self::NotStartsWith => Self::StartsWith,
        }
    }
}

/// A null/NaN test operator (`UnaryExpression.type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// `is-null`
    IsNull,
    /// `not-null`
    NotNull,
    /// `is-nan`
    IsNan,
    /// `not-nan`
    NotNan,
}

impl UnaryOp {
    fn as_str(self) -> &'static str {
        match self {
            Self::IsNull => "is-null",
            Self::NotNull => "not-null",
            Self::IsNan => "is-nan",
            Self::NotNan => "not-nan",
        }
    }

    /// The operator's negation.
    #[must_use]
    pub fn negated(self) -> Self {
        match self {
            Self::IsNull => Self::NotNull,
            Self::NotNull => Self::IsNull,
            Self::IsNan => Self::NotNan,
            Self::NotNan => Self::IsNan,
        }
    }
}

/// A set membership operator (`SetExpression.type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    /// `in`
    In,
    /// `not-in`
    NotIn,
}

impl SetOp {
    fn as_str(self) -> &'static str {
        match self {
            Self::In => "in",
            Self::NotIn => "not-in",
        }
    }

    /// The operator's negation.
    #[must_use]
    pub fn negated(self) -> Self {
        match self {
            Self::In => Self::NotIn,
            Self::NotIn => Self::In,
        }
    }
}

/// A predicate term: a column reference or a transform of one (the REST
/// `Term` schema).
#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    /// A field name (`Reference`), possibly dotted for nested fields.
    Reference(String),
    /// A `TransformTerm`: a transform applied to a reference.
    Transform {
        /// The transform, e.g. `bucket[16]`, `day`.
        transform: Transform,
        /// The referenced column.
        reference: String,
    },
}

impl Term {
    fn to_json(&self) -> Value {
        match self {
            Self::Reference(name) => Value::String(name.clone()),
            Self::Transform {
                transform,
                reference,
            } => json!({
                "type": "transform",
                "transform": transform,
                "term": reference,
            }),
        }
    }

    fn from_json(value: &Value) -> Result<Self, String> {
        match value {
            Value::String(name) => Ok(Self::Reference(name.clone())),
            Value::Object(obj) => {
                if obj.get("type").and_then(Value::as_str) != Some("transform") {
                    return Err("term object must have type \"transform\"".to_owned());
                }
                let transform = obj
                    .get("transform")
                    .and_then(Value::as_str)
                    .ok_or("transform term needs a \"transform\" string")?
                    .parse::<Transform>()
                    .map_err(|e| e.to_string())?;
                let reference = match obj.get("term") {
                    Some(Value::String(name)) => name.clone(),
                    _ => return Err("transform term needs a string \"term\" reference".to_owned()),
                };
                Ok(Self::Transform {
                    transform,
                    reference,
                })
            }
            _ => Err("a term must be a reference string or a transform object".to_owned()),
        }
    }

    /// The referenced column name.
    #[must_use]
    pub fn reference(&self) -> &str {
        match self {
            Self::Reference(name) => name,
            Self::Transform { reference, .. } => reference,
        }
    }
}

/// The REST filter expression tree (`Expression` in the `OpenAPI` schema).
///
/// Literal values are kept as raw JSON here — the JSON single-value
/// encoding is ambiguous without the column type (`"2017-11-16"` is a
/// date or a string) — and are typed during [`Expression::bind`].
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// `true`
    True,
    /// `false`
    False,
    /// `and`
    And {
        /// Left operand.
        left: Box<Expression>,
        /// Right operand.
        right: Box<Expression>,
    },
    /// `or`
    Or {
        /// Left operand.
        left: Box<Expression>,
        /// Right operand.
        right: Box<Expression>,
    },
    /// `not`
    Not {
        /// Negated child.
        child: Box<Expression>,
    },
    /// `is-null` / `not-null` / `is-nan` / `not-nan`
    Unary {
        /// The operator.
        op: UnaryOp,
        /// The tested term.
        term: Term,
    },
    /// `lt` / `lt-eq` / `gt` / `gt-eq` / `eq` / `not-eq` / `starts-with` /
    /// `not-starts-with`
    Comparison {
        /// The operator.
        op: CompareOp,
        /// The compared term.
        term: Term,
        /// The literal, raw JSON single-value form.
        value: Value,
    },
    /// `in` / `not-in`
    Set {
        /// The operator.
        op: SetOp,
        /// The tested term.
        term: Term,
        /// The literals, raw JSON single-value forms.
        values: Vec<Value>,
    },
}

impl Expression {
    /// Rewrites to negation normal form: every `not` is pushed down and
    /// eliminated by flipping operators (De Morgan for `and`/`or`).
    #[must_use]
    pub fn rewrite_not(self) -> Self {
        match self {
            Self::And { left, right } => Self::And {
                left: Box::new(left.rewrite_not()),
                right: Box::new(right.rewrite_not()),
            },
            Self::Or { left, right } => Self::Or {
                left: Box::new(left.rewrite_not()),
                right: Box::new(right.rewrite_not()),
            },
            Self::Not { child } => child.negate(),
            leaf => leaf,
        }
    }

    fn negate(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::And { left, right } => Self::Or {
                left: Box::new(left.negate()),
                right: Box::new(right.negate()),
            },
            Self::Or { left, right } => Self::And {
                left: Box::new(left.negate()),
                right: Box::new(right.negate()),
            },
            Self::Not { child } => child.rewrite_not(),
            Self::Unary { op, term } => Self::Unary {
                op: op.negated(),
                term,
            },
            Self::Comparison { op, term, value } => Self::Comparison {
                op: op.negated(),
                term,
                value,
            },
            Self::Set { op, term, values } => Self::Set {
                op: op.negated(),
                term,
                values,
            },
        }
    }

    /// Binds this expression against a schema: resolves each term's
    /// column (dotted paths descend structs; `element`, `key`, `value`
    /// descend lists and maps), types every literal, eliminates `not`,
    /// and simplifies statically decidable cases (`is-null` on a required
    /// column, empty `in`).
    pub fn bind(self, schema: &Schema, case_sensitive: bool) -> Result<BoundPredicate, BindError> {
        let nnf = self.rewrite_not();
        nnf.bind_nnf(schema, case_sensitive)
    }

    fn bind_nnf(self, schema: &Schema, case_sensitive: bool) -> Result<BoundPredicate, BindError> {
        Ok(match self {
            Self::True => BoundPredicate::True,
            Self::False => BoundPredicate::False,
            Self::And { left, right } => BoundPredicate::And(
                Box::new(left.bind_nnf(schema, case_sensitive)?),
                Box::new(right.bind_nnf(schema, case_sensitive)?),
            ),
            Self::Or { left, right } => BoundPredicate::Or(
                Box::new(left.bind_nnf(schema, case_sensitive)?),
                Box::new(right.bind_nnf(schema, case_sensitive)?),
            ),
            // rewrite_not eliminates every `not`; handle a stray one
            // total-functionally anyway by negating its child.
            Self::Not { child } => child.negate().bind_nnf(schema, case_sensitive)?,
            Self::Unary { op, term } => {
                let bound = BoundTerm::bind(&term, schema, case_sensitive)?;
                match op {
                    UnaryOp::IsNull if bound.required && bound.transform.is_none() => {
                        BoundPredicate::False
                    }
                    UnaryOp::NotNull if bound.required && bound.transform.is_none() => {
                        BoundPredicate::True
                    }
                    UnaryOp::IsNan | UnaryOp::NotNan
                        if !matches!(
                            bound.value_type(),
                            PrimitiveType::Float | PrimitiveType::Double
                        ) =>
                    {
                        return Err(BindError::InvalidTerm {
                            term: term.reference().to_owned(),
                            reason: format!(
                                "{} requires a float or double term, not {}",
                                op.as_str(),
                                bound.value_type()
                            ),
                        });
                    }
                    _ => BoundPredicate::Unary { op, term: bound },
                }
            }
            Self::Comparison { op, term, value } => {
                let bound = BoundTerm::bind(&term, schema, case_sensitive)?;
                if matches!(op, CompareOp::StartsWith | CompareOp::NotStartsWith)
                    && *bound.value_type() != PrimitiveType::String
                {
                    return Err(BindError::InvalidTerm {
                        term: term.reference().to_owned(),
                        reason: format!(
                            "{} requires a string term, not {}",
                            op.as_str(),
                            bound.value_type()
                        ),
                    });
                }
                let literal =
                    Datum::from_rest_json(bound.value_type(), &value).map_err(|source| {
                        BindError::Literal {
                            term: term.reference().to_owned(),
                            source,
                        }
                    })?;
                BoundPredicate::Comparison {
                    op,
                    term: bound,
                    literal,
                }
            }
            Self::Set { op, term, values } => {
                let bound = BoundTerm::bind(&term, schema, case_sensitive)?;
                let literals = values
                    .iter()
                    .map(|v| Datum::from_rest_json(bound.value_type(), v))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|source| BindError::Literal {
                        term: term.reference().to_owned(),
                        source,
                    })?;
                match (op, literals.is_empty()) {
                    (SetOp::In, true) => BoundPredicate::False,
                    (SetOp::NotIn, true) => BoundPredicate::True,
                    _ => BoundPredicate::Set {
                        op,
                        term: bound,
                        literals,
                    },
                }
            }
        })
    }
}

impl Serialize for Expression {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_json().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Expression {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Self::from_json(&value).map_err(D::Error::custom)
    }
}

impl Expression {
    fn to_json(&self) -> Value {
        match self {
            Self::True => json!({"type": "true"}),
            Self::False => json!({"type": "false"}),
            Self::And { left, right } => json!({
                "type": "and", "left": left.to_json(), "right": right.to_json(),
            }),
            Self::Or { left, right } => json!({
                "type": "or", "left": left.to_json(), "right": right.to_json(),
            }),
            Self::Not { child } => json!({"type": "not", "child": child.to_json()}),
            Self::Unary { op, term } => json!({
                "type": op.as_str(), "term": term.to_json(),
            }),
            Self::Comparison { op, term, value } => json!({
                "type": op.as_str(), "term": term.to_json(), "value": value,
            }),
            Self::Set { op, term, values } => json!({
                "type": op.as_str(), "term": term.to_json(), "values": values,
            }),
        }
    }

    fn from_json(value: &Value) -> Result<Self, String> {
        let obj: &Map<String, Value> = value
            .as_object()
            .ok_or("an expression must be a JSON object")?;
        let ty = obj
            .get("type")
            .and_then(Value::as_str)
            .ok_or("expression is missing \"type\"")?;
        let child = |key: &str| -> Result<Box<Expression>, String> {
            let v = obj.get(key).ok_or_else(|| format!("{ty} needs {key:?}"))?;
            Ok(Box::new(Self::from_json(v)?))
        };
        let term = |obj: &Map<String, Value>| -> Result<Term, String> {
            Term::from_json(
                obj.get("term")
                    .ok_or_else(|| format!("{ty} needs a term"))?,
            )
        };
        match ty {
            "true" => Ok(Self::True),
            "false" => Ok(Self::False),
            "and" => Ok(Self::And {
                left: child("left")?,
                right: child("right")?,
            }),
            "or" => Ok(Self::Or {
                left: child("left")?,
                right: child("right")?,
            }),
            "not" => Ok(Self::Not {
                child: child("child")?,
            }),
            "is-null" | "not-null" | "is-nan" | "not-nan" => {
                let op = match ty {
                    "is-null" => UnaryOp::IsNull,
                    "not-null" => UnaryOp::NotNull,
                    "is-nan" => UnaryOp::IsNan,
                    _ => UnaryOp::NotNan,
                };
                Ok(Self::Unary {
                    op,
                    term: term(obj)?,
                })
            }
            "lt" | "lt-eq" | "gt" | "gt-eq" | "eq" | "not-eq" | "starts-with"
            | "not-starts-with" => {
                let op = match ty {
                    "lt" => CompareOp::Lt,
                    "lt-eq" => CompareOp::LtEq,
                    "gt" => CompareOp::Gt,
                    "gt-eq" => CompareOp::GtEq,
                    "eq" => CompareOp::Eq,
                    "not-eq" => CompareOp::NotEq,
                    "starts-with" => CompareOp::StartsWith,
                    _ => CompareOp::NotStartsWith,
                };
                let value = obj
                    .get("value")
                    .ok_or_else(|| format!("{ty} needs a value"))?
                    .clone();
                Ok(Self::Comparison {
                    op,
                    term: term(obj)?,
                    value,
                })
            }
            "in" | "not-in" => {
                let op = if ty == "in" { SetOp::In } else { SetOp::NotIn };
                let values = obj
                    .get("values")
                    .and_then(Value::as_array)
                    .ok_or_else(|| format!("{ty} needs a values array"))?
                    .clone();
                Ok(Self::Set {
                    op,
                    term: term(obj)?,
                    values,
                })
            }
            other => Err(format!("unknown expression type {other:?}")),
        }
    }
}

/// Error binding an expression to a schema.
#[derive(Debug, thiserror::Error)]
pub enum BindError {
    /// The referenced column does not exist (under the requested case
    /// sensitivity).
    #[error("unknown column {name:?}")]
    UnknownColumn {
        /// The reference as written.
        name: String,
    },
    /// The referenced column is not a primitive type, or the operator
    /// does not apply to it.
    #[error("cannot filter on {term:?}: {reason}")]
    InvalidTerm {
        /// The reference as written.
        term: String,
        /// Why it cannot be used.
        reason: String,
    },
    /// A literal could not be converted to the column's type.
    #[error("invalid literal for {term:?}: {source}")]
    Literal {
        /// The reference as written.
        term: String,
        /// The conversion failure.
        source: ValueError,
    },
}

/// A term bound to a schema column.
#[derive(Debug, Clone)]
pub struct BoundTerm {
    /// The resolved column's field id.
    pub field_id: i32,
    /// The reference as written in the filter.
    pub name: String,
    /// The column's primitive type.
    pub field_type: PrimitiveType,
    /// Whether the column (and every ancestor on its path) is required.
    pub required: bool,
    /// The transform, for transform terms.
    pub transform: Option<Transform>,
    /// The type literals for this term are parsed as: the transform's
    /// result type for recognized transforms (an `eq` over `bucket[16]`
    /// compares int bucket numbers; over `day` it compares dates), else
    /// the source column type.
    literal_type: PrimitiveType,
}

impl BoundTerm {
    /// The type literals for this term are parsed as.
    #[must_use]
    pub fn literal_type(&self) -> &PrimitiveType {
        &self.literal_type
    }

    fn value_type(&self) -> &PrimitiveType {
        &self.literal_type
    }

    fn bind(term: &Term, schema: &Schema, case_sensitive: bool) -> Result<Self, BindError> {
        let (name, transform) = match term {
            Term::Reference(name) => (name, None),
            Term::Transform {
                transform,
                reference,
            } => (reference, Some(transform.clone())),
        };
        let (field_id, field_type, required) = resolve_path(schema, name, case_sensitive)
            .ok_or_else(|| BindError::UnknownColumn { name: name.clone() })?;
        let literal_type = match &transform {
            None => field_type.clone(),
            Some(t) => crate::manifest::transform_result_type(t, &field_type)
                .unwrap_or_else(|| field_type.clone()),
        };
        Ok(Self {
            field_id,
            name: name.clone(),
            field_type,
            required,
            transform,
            literal_type,
        })
    }
}

/// Resolves a dotted field path to `(field_id, primitive_type,
/// required_all_the_way_down)`.
fn resolve_path(
    schema: &Schema,
    path: &str,
    case_sensitive: bool,
) -> Option<(i32, PrimitiveType, bool)> {
    let mut segments = path.split('.');
    let first = segments.next()?;
    let matches = |field_name: &str, segment: &str| {
        if case_sensitive {
            field_name == segment
        } else {
            field_name.eq_ignore_ascii_case(segment)
        }
    };
    let field = schema.fields.iter().find(|f| matches(&f.name, first))?;
    let mut field_id = field.id;
    let mut required = field.required;
    let mut current: &Type = &field.field_type;
    for segment in segments {
        match current {
            Type::Struct(s) => {
                let f = s.fields.iter().find(|f| matches(&f.name, segment))?;
                field_id = f.id;
                required = required && f.required;
                current = &f.field_type;
            }
            Type::List(l) if segment == "element" => {
                field_id = l.element_id;
                required = required && l.element_required;
                current = &l.element;
            }
            Type::Map(m) if segment == "key" => {
                field_id = m.key_id;
                current = &m.key;
            }
            Type::Map(m) if segment == "value" => {
                field_id = m.value_id;
                required = required && m.value_required;
                current = &m.value;
            }
            _ => return None,
        }
    }
    match current {
        Type::Primitive(p) => Some((field_id, p.clone(), required)),
        _ => None,
    }
}

/// A bound predicate: schema-resolved, literal-typed, `not`-free.
#[derive(Debug, Clone)]
pub enum BoundPredicate {
    /// Always matches.
    True,
    /// Never matches.
    False,
    /// Both must match.
    And(Box<BoundPredicate>, Box<BoundPredicate>),
    /// Either must match.
    Or(Box<BoundPredicate>, Box<BoundPredicate>),
    /// A null/NaN test.
    Unary {
        /// The operator.
        op: UnaryOp,
        /// The bound term.
        term: BoundTerm,
    },
    /// A comparison against one literal.
    Comparison {
        /// The operator.
        op: CompareOp,
        /// The bound term.
        term: BoundTerm,
        /// The typed literal.
        literal: Datum,
    },
    /// A set membership test.
    Set {
        /// The operator.
        op: SetOp,
        /// The bound term.
        term: BoundTerm,
        /// The typed literals.
        literals: Vec<Datum>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::StructField;

    fn test_schema() -> Schema {
        Schema::new(vec![
            StructField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
            StructField::optional(2, "category", Type::Primitive(PrimitiveType::String)),
            StructField::optional(3, "amount", Type::Primitive(PrimitiveType::Double)),
            StructField::optional(
                4,
                "addr",
                Type::Struct(crate::spec::StructType::new(vec![StructField::optional(
                    5,
                    "zip",
                    Type::Primitive(PrimitiveType::Int),
                )])),
            ),
        ])
    }

    #[test]
    fn expression_json_round_trips_exact_yaml_shapes() {
        let json = serde_json::json!({
            "type": "and",
            "left": {
                "type": "not",
                "child": {"type": "eq", "term": "category", "value": "toys"}
            },
            "right": {
                "type": "or",
                "left": {"type": "in", "term": {"type": "transform", "transform": "bucket[16]", "term": "id"}, "values": [1, 2]},
                "right": {"type": "is-null", "term": "amount"}
            }
        });
        let parsed: Expression = serde_json::from_value(json.clone()).expect("parse");
        let back = serde_json::to_value(&parsed).expect("serialize");
        assert_eq!(back, json, "must round-trip the exact OpenAPI field names");
    }

    #[test]
    fn rewrite_not_flips_operators() {
        let expr: Expression = serde_json::from_value(serde_json::json!({
            "type": "not",
            "child": {
                "type": "and",
                "left": {"type": "lt", "term": "id", "value": 5},
                "right": {"type": "not", "child": {"type": "is-null", "term": "category"}}
            }
        }))
        .expect("parse");
        let rewritten = expr.rewrite_not();
        let json = serde_json::to_value(&rewritten).expect("serialize");
        assert_eq!(
            json,
            serde_json::json!({
                "type": "or",
                "left": {"type": "gt-eq", "term": "id", "value": 5},
                "right": {"type": "is-null", "term": "category"}
            })
        );
    }

    #[test]
    fn binding_resolves_types_and_simplifies() {
        let schema = test_schema();
        // is-null on a required column binds to False.
        let expr: Expression =
            serde_json::from_value(serde_json::json!({"type": "is-null", "term": "id"}))
                .expect("parse");
        assert!(matches!(
            expr.bind(&schema, true).expect("bind"),
            BoundPredicate::False
        ));
        // Nested references resolve; case-insensitive when asked.
        let expr: Expression = serde_json::from_value(
            serde_json::json!({"type": "eq", "term": "ADDR.ZIP", "value": 94103}),
        )
        .expect("parse");
        match expr.clone().bind(&schema, false).expect("bind") {
            BoundPredicate::Comparison { term, literal, .. } => {
                assert_eq!(term.field_id, 5);
                assert_eq!(literal, Datum::Int(94103));
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(matches!(
            expr.bind(&schema, true),
            Err(BindError::UnknownColumn { .. })
        ));
        // Wrong-shaped literal.
        let expr: Expression = serde_json::from_value(
            serde_json::json!({"type": "eq", "term": "id", "value": "not-a-number"}),
        )
        .expect("parse");
        assert!(matches!(
            expr.bind(&schema, true),
            Err(BindError::Literal { .. })
        ));
        // is-nan on a non-floating column is rejected.
        let expr: Expression =
            serde_json::from_value(serde_json::json!({"type": "is-nan", "term": "category"}))
                .expect("parse");
        assert!(matches!(
            expr.bind(&schema, true),
            Err(BindError::InvalidTerm { .. })
        ));
        // Empty in() binds to False, empty not-in() to True.
        let expr: Expression =
            serde_json::from_value(serde_json::json!({"type": "in", "term": "id", "values": []}))
                .expect("parse");
        assert!(matches!(
            expr.bind(&schema, true).expect("bind"),
            BoundPredicate::False
        ));
    }
}
