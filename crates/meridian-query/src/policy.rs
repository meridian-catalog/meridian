//! Compiling a resolved [`Enforcement`] into a governed SQL view.
//!
//! This is where `run_sql` becomes governed. The caller resolves row filters
//! and column masks for a `(principal, table)` pair with `meridian-authz`
//! (`resolve_filters_and_masks`) and hands us the [`Enforcement`]. We register
//! the table's raw data under a private name and create a **view** with the
//! caller's chosen name whose projection masks/drops columns and whose `WHERE`
//! folds in the row filter. The user's SQL references the view, so every read is
//! filtered and masked by `DataFusion`'s own planner — the same closed predicate
//! AST the scan-plan seam enforces, just executed here instead of injected into
//! a scan task.
//!
//! Design choices that make this safe:
//!
//! - **Deterministic, no free-form SQL from policy.** Row filters are a closed
//!   predicate AST ([`RowPredicate`]); we render each node to a fixed SQL shape.
//!   Column masks are a closed set of kinds. There is no place a policy string
//!   is spliced into SQL unescaped.
//! - **Dropped columns are absent, not nulled** (H-F2): a `Drop` mask omits the
//!   column from the view's projection entirely, so the restricted column's very
//!   existence cannot be probed through the view.
//! - **Fail closed.** A `Custom` mask (a SQL expression we cannot verify against
//!   this engine) is treated as `Drop`, exactly as the scan-plan path does — we
//!   never execute unverified policy SQL, and we never reveal a column we were
//!   unsure how to mask.
//! - **Literals are escaped.** Even though policy literals are trusted (they
//!   come from resolved policies, not user input), string values are rendered
//!   with SQL-standard single-quote doubling so a stray quote can never break
//!   out of the literal.

use meridian_authz::{ColumnMask, Enforcement, MaskKind, RowPredicate};
use meridian_iceberg::spec::{PrimitiveType, Schema, StructField, Type};
use serde_json::Value;

use crate::error::{QueryError, QueryResult};

/// The SQL that materializes governed rows from a private raw table: the
/// masked/filtered `SELECT` the executor runs in an isolated context to
/// produce the only data the user's query ever sees.
#[derive(Debug, Clone)]
pub(crate) struct GovernedView {
    /// The `SELECT <masked projection> FROM <raw> [WHERE <row filter>]`
    /// statement — the governance transform, without any `CREATE VIEW`
    /// wrapper. Run this against the private context holding the raw table.
    pub select_sql: String,
    /// Column names that were masked or dropped, deduped and sorted (for
    /// provenance/audit).
    pub masked_columns: Vec<String>,
}

/// Builds the governance transform for a table: `SELECT <masked projection>
/// FROM <raw_name> [WHERE <row filter>]`. The executor runs this in a private
/// context over the raw table and materializes the result as the only data
/// the user's query can reach.
///
/// `view_name` is retained for diagnostics; `raw_name` is the private name the
/// raw table was registered under. The projection lists the table's columns in
/// schema order, each either passed through, transformed by its mask, or
/// omitted (dropped). `enforcement` has already been normalized by the resolver
/// (at most one mask per column, strongest wins).
pub(crate) fn build_governed_view(
    view_name: &str,
    raw_name: &str,
    schema: &Schema,
    enforcement: &Enforcement,
) -> QueryResult<GovernedView> {
    let mut projection: Vec<String> = Vec::with_capacity(schema.fields.len());
    let mut masked: Vec<String> = Vec::new();

    for field in &schema.fields {
        let mask = enforcement
            .column_masks
            .iter()
            .find(|m| m.column == field.name);
        match mask {
            None => projection.push(quote_ident(&field.name)),
            Some(mask) => {
                masked.push(field.name.clone());
                match mask_expression(field, mask) {
                    // A dropped column is simply not projected.
                    MaskProjection::Drop => {}
                    MaskProjection::Expr(expr) => {
                        projection.push(format!("{expr} AS {}", quote_ident(&field.name)));
                    }
                }
            }
        }
    }

    if projection.is_empty() {
        // Every column dropped: the view still must be valid SQL and expose the
        // row count, so project a constant. A SELECT * against it yields a
        // single unnamed column of NULLs — no data leaks.
        projection.push("NULL AS __all_columns_restricted".to_owned());
    }

    let _ = view_name; // materialized directly; no named view is created.
    let mut sql = format!(
        "SELECT {} FROM {}",
        projection.join(", "),
        quote_ident(raw_name),
    );

    if let Some(predicate) = enforcement.row_filter_predicate() {
        let where_sql = render_predicate(&predicate, schema)?;
        sql.push_str(" WHERE ");
        sql.push_str(&where_sql);
    }

    masked.sort();
    masked.dedup();
    Ok(GovernedView {
        select_sql: sql,
        masked_columns: masked,
    })
}

/// How a masked column projects into the view.
enum MaskProjection {
    /// Omit the column entirely (absent from results).
    Drop,
    /// Project this SQL expression in place of the raw column.
    Expr(String),
}

/// Renders a column mask to its view projection. Fail-closed: any mask whose SQL
/// we cannot construct safely becomes a `Drop`.
// `Drop` and `Custom` both yield a dropped column, but they are distinct policy
// intents (explicit drop vs. a custom SQL mask we won't execute), so they are
// kept as separate, self-documenting arms rather than merged.
#[allow(clippy::match_same_arms)]
fn mask_expression(field: &StructField, mask: &ColumnMask) -> MaskProjection {
    let col = quote_ident(&field.name);
    match &mask.kind {
        MaskKind::Drop => MaskProjection::Drop,
        // NULL of the column's type, so the result schema keeps the column but
        // every value is null.
        MaskKind::Null => MaskProjection::Expr(format!("CAST(NULL AS {})", sql_type(field))),
        // Stable hash of the string form. Only meaningful for text-like columns;
        // for others we drop (hashing a raw binary/number to hex could leak
        // structure or collide confusingly) — fail closed.
        MaskKind::Hash => {
            if is_stringy(field) {
                MaskProjection::Expr(format!("encode(sha256(CAST({col} AS VARCHAR)), 'hex')"))
            } else {
                MaskProjection::Drop
            }
        }
        // Reveal only the first/last N characters of a string; the middle is
        // fixed asterisks. Only for text columns; otherwise drop.
        MaskKind::Partial {
            show_first,
            show_last,
        } => {
            if is_stringy(field) {
                MaskProjection::Expr(partial_expr(&col, *show_first, *show_last))
            } else {
                MaskProjection::Drop
            }
        }
        // A custom SQL expression we cannot verify against this engine. The
        // scan-plan path treats an unresolvable custom mask as Drop; we do the
        // same — never execute unverified policy SQL.
        MaskKind::Custom { .. } => MaskProjection::Drop,
    }
}

/// Builds a partial-reveal expression: first `show_first` chars, `***`, last
/// `show_last` chars. Uses SUBSTR + string length. When both are zero this is
/// equivalent to a constant mask.
fn partial_expr(col: &str, show_first: u32, show_last: u32) -> String {
    let first = if show_first > 0 {
        format!("SUBSTR({col}, 1, {show_first})")
    } else {
        "''".to_owned()
    };
    let last = if show_last > 0 {
        // Take the last N characters via SUBSTR from an offset computed off the
        // length. `GREATEST(1, ...)` keeps the offset valid for short strings.
        format!("SUBSTR({col}, GREATEST(1, CHARACTER_LENGTH({col}) - {show_last} + 1))")
    } else {
        "''".to_owned()
    };
    // CONCAT is null-tolerant in DataFusion (treats NULL as empty); a fully-null
    // input yields '***', which does not leak the original.
    format!("CONCAT({first}, '***', {last})")
}

/// Renders a closed row predicate to SQL, binding column names against the
/// schema so an unknown column is a clean error rather than invalid SQL.
fn render_predicate(pred: &RowPredicate, schema: &Schema) -> QueryResult<String> {
    Ok(match pred {
        RowPredicate::True => "TRUE".to_owned(),
        RowPredicate::False => "FALSE".to_owned(),
        RowPredicate::And { left, right } => format!(
            "({} AND {})",
            render_predicate(left, schema)?,
            render_predicate(right, schema)?
        ),
        RowPredicate::Or { left, right } => format!(
            "({} OR {})",
            render_predicate(left, schema)?,
            render_predicate(right, schema)?
        ),
        RowPredicate::Not { child } => {
            format!("(NOT {})", render_predicate(child, schema)?)
        }
        RowPredicate::Eq { column, value } => comparison(schema, column, "=", value)?,
        RowPredicate::NotEq { column, value } => comparison(schema, column, "<>", value)?,
        RowPredicate::Lt { column, value } => comparison(schema, column, "<", value)?,
        RowPredicate::LtEq { column, value } => comparison(schema, column, "<=", value)?,
        RowPredicate::Gt { column, value } => comparison(schema, column, ">", value)?,
        RowPredicate::GtEq { column, value } => comparison(schema, column, ">=", value)?,
        RowPredicate::In { column, values } => set_predicate(schema, column, false, values)?,
        RowPredicate::NotIn { column, values } => set_predicate(schema, column, true, values)?,
        RowPredicate::IsNull { column } => {
            bind_column(schema, column)?;
            format!("{} IS NULL", quote_ident(column))
        }
        RowPredicate::NotNull { column } => {
            bind_column(schema, column)?;
            format!("{} IS NOT NULL", quote_ident(column))
        }
    })
}

/// Renders `column <op> literal`, checking the column exists.
fn comparison(schema: &Schema, column: &str, op: &str, value: &Value) -> QueryResult<String> {
    let field = bind_column(schema, column)?;
    Ok(format!(
        "{} {op} {}",
        quote_ident(column),
        sql_literal(value, field)?
    ))
}

/// Renders `column [NOT] IN (literals...)`.
fn set_predicate(
    schema: &Schema,
    column: &str,
    negated: bool,
    values: &[Value],
) -> QueryResult<String> {
    let field = bind_column(schema, column)?;
    if values.is_empty() {
        // `x IN ()` is empty-false; `x NOT IN ()` is empty-true.
        return Ok(if negated { "TRUE" } else { "FALSE" }.to_owned());
    }
    let rendered: Vec<String> = values
        .iter()
        .map(|v| sql_literal(v, field))
        .collect::<QueryResult<_>>()?;
    Ok(format!(
        "{} {}IN ({})",
        quote_ident(column),
        if negated { "NOT " } else { "" },
        rendered.join(", ")
    ))
}

/// Resolves a column name to its schema field, or a clean error. Row filters
/// reference table columns; a policy naming a column that does not exist is a
/// misconfiguration surfaced as invalid SQL to the caller rather than a panic.
fn bind_column<'a>(schema: &'a Schema, column: &str) -> QueryResult<&'a StructField> {
    schema
        .fields
        .iter()
        .find(|f| f.name == column)
        .ok_or_else(|| {
            QueryError::InvalidSql(format!(
                "row-filter policy references column {column:?}, which is not in the table schema"
            ))
        })
}

/// Renders a JSON policy literal as a SQL literal, typed by the target column.
/// Strings are single-quoted with quote-doubling; numbers/bools are inlined;
/// null becomes SQL NULL. A JSON string bound to a temporal/decimal column is
/// wrapped in a CAST so `DataFusion` parses it in the column's type.
fn sql_literal(value: &Value, field: &StructField) -> QueryResult<String> {
    Ok(match value {
        Value::Null => "NULL".to_owned(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_owned(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => {
            let quoted = format!("'{}'", s.replace('\'', "''"));
            // For non-text columns a quoted string must be cast to the column
            // type (e.g. dates, timestamps, decimals arrive as JSON strings).
            match &field.field_type {
                Type::Primitive(p) if !matches!(p, PrimitiveType::String) => {
                    format!("CAST({quoted} AS {})", sql_type(field))
                }
                _ => quoted,
            }
        }
        Value::Array(_) | Value::Object(_) => {
            return Err(QueryError::InvalidSql(format!(
                "row-filter literal for column {:?} is not a scalar",
                field.name
            )));
        }
    })
}

/// Whether a field is a string-like column masks/hashes can operate on directly.
fn is_stringy(field: &StructField) -> bool {
    matches!(&field.field_type, Type::Primitive(PrimitiveType::String))
}

/// A SQL type name for `CAST(... AS ...)` in masks/literals. Falls back to a
/// broad type where the exact one does not matter for the mask.
// Several primitives legitimately map to the same SQL type (e.g. String/Uuid ->
// VARCHAR); the arms stay explicit to document each mapping.
#[allow(clippy::match_same_arms)]
fn sql_type(field: &StructField) -> String {
    match &field.field_type {
        Type::Primitive(p) => match p {
            PrimitiveType::Boolean => "BOOLEAN".to_owned(),
            PrimitiveType::Int => "INT".to_owned(),
            PrimitiveType::Long => "BIGINT".to_owned(),
            PrimitiveType::Float => "REAL".to_owned(),
            PrimitiveType::Double => "DOUBLE".to_owned(),
            PrimitiveType::Decimal { precision, scale } => {
                format!("DECIMAL({precision}, {scale})")
            }
            PrimitiveType::Date => "DATE".to_owned(),
            PrimitiveType::Time => "TIME".to_owned(),
            PrimitiveType::Timestamp | PrimitiveType::TimestampNs => "TIMESTAMP".to_owned(),
            PrimitiveType::Timestamptz | PrimitiveType::TimestamptzNs => {
                "TIMESTAMP WITH TIME ZONE".to_owned()
            }
            PrimitiveType::String | PrimitiveType::Uuid => "VARCHAR".to_owned(),
            PrimitiveType::Fixed(_) | PrimitiveType::Binary => "BYTEA".to_owned(),
            _ => "VARCHAR".to_owned(),
        },
        _ => "VARCHAR".to_owned(),
    }
}

/// Quotes an identifier for SQL: double-quoted with embedded quotes doubled, so
/// arbitrary column/table names (including reserved words or names with spaces)
/// are always safe.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quotes a possibly-qualified name (`schema.table`) by quoting each
/// dot-separated segment: `ns.sales` → `"ns"."sales"`, a bare `t` → `"t"`. The
/// Convenience on [`Enforcement`] to fold row filters into one predicate. Kept
/// here (not in authz) so this crate does not depend on authz's `Expression`
/// re-export; we only need the closed [`RowPredicate`] AST.
trait RowFilterFold {
    fn row_filter_predicate(&self) -> Option<RowPredicate>;
}

impl RowFilterFold for Enforcement {
    fn row_filter_predicate(&self) -> Option<RowPredicate> {
        let mut iter = self.row_filters.iter();
        let first = iter.next()?;
        let mut acc = first.predicate.clone();
        for filter in iter {
            acc = RowPredicate::And {
                left: Box::new(acc),
                right: Box::new(filter.predicate.clone()),
            };
        }
        Some(acc)
    }
}

#[cfg(test)]
mod tests {
    use meridian_authz::{ColumnMask, Enforcement, MaskKind, RowFilter, RowPredicate};
    use meridian_iceberg::spec::{PrimitiveType, Schema, StructField, Type};
    use serde_json::json;

    use super::build_governed_view;

    fn schema() -> Schema {
        Schema::new(vec![
            StructField::optional(1, "id", Type::Primitive(PrimitiveType::Long)),
            StructField::optional(2, "email", Type::Primitive(PrimitiveType::String)),
            StructField::optional(3, "region", Type::Primitive(PrimitiveType::String)),
            StructField::optional(4, "hired", Type::Primitive(PrimitiveType::Date)),
        ])
        .with_schema_id(0)
    }

    #[test]
    fn no_enforcement_projects_all_columns_no_where() {
        let view =
            build_governed_view("t", "__raw", &schema(), &Enforcement::none()).expect("view");
        assert!(view.select_sql.starts_with("SELECT"));
        // Every column present, in order.
        assert!(
            view.select_sql
                .contains("\"id\", \"email\", \"region\", \"hired\"")
        );
        assert!(!view.select_sql.contains("WHERE"));
        assert!(view.masked_columns.is_empty());
    }

    #[test]
    fn string_literal_is_escaped_against_injection() {
        // A policy literal containing a quote and a SQL fragment must be a
        // single, escaped string literal — never break out into executable SQL.
        let enforcement = Enforcement {
            row_filters: vec![RowFilter::new(
                "p",
                RowPredicate::Eq {
                    column: "region".to_owned(),
                    value: json!("EU' OR '1'='1"),
                },
            )],
            column_masks: vec![],
        };
        let view = build_governed_view("t", "__raw", &schema(), &enforcement).expect("view");
        // The dangerous quote is doubled; the whole value stays inside one literal.
        assert!(
            view.select_sql
                .contains("WHERE \"region\" = 'EU'' OR ''1''=''1'")
        );
        // And there is exactly one WHERE — no injected clause.
        assert_eq!(view.select_sql.matches("WHERE").count(), 1);
    }

    #[test]
    fn identifier_with_quotes_is_safely_quoted() {
        // A column named with a double-quote must be escaped in the projection.
        let s = Schema::new(vec![StructField::optional(
            1,
            "od\"d",
            Type::Primitive(PrimitiveType::Long),
        )])
        .with_schema_id(0);
        let view = build_governed_view("t", "__raw", &s, &Enforcement::none()).expect("view");
        assert!(view.select_sql.contains("\"od\"\"d\""));
    }

    #[test]
    fn date_literal_is_cast_to_column_type() {
        let enforcement = Enforcement {
            row_filters: vec![RowFilter::new(
                "p",
                RowPredicate::GtEq {
                    column: "hired".to_owned(),
                    value: json!("2020-01-01"),
                },
            )],
            column_masks: vec![],
        };
        let view = build_governed_view("t", "__raw", &schema(), &enforcement).expect("view");
        assert!(
            view.select_sql
                .contains("\"hired\" >= CAST('2020-01-01' AS DATE)")
        );
    }

    #[test]
    fn in_predicate_renders_value_list() {
        let enforcement = Enforcement {
            row_filters: vec![RowFilter::new(
                "p",
                RowPredicate::In {
                    column: "region".to_owned(),
                    values: vec![json!("EU"), json!("US")],
                },
            )],
            column_masks: vec![],
        };
        let view = build_governed_view("t", "__raw", &schema(), &enforcement).expect("view");
        assert!(view.select_sql.contains("\"region\" IN ('EU', 'US')"));
    }

    #[test]
    fn empty_in_and_not_in_fold_to_constants() {
        let empty_in = Enforcement {
            row_filters: vec![RowFilter::new(
                "p",
                RowPredicate::In {
                    column: "region".to_owned(),
                    values: vec![],
                },
            )],
            column_masks: vec![],
        };
        let v = build_governed_view("t", "__raw", &schema(), &empty_in).expect("view");
        assert!(v.select_sql.ends_with("WHERE FALSE"));

        let empty_not_in = Enforcement {
            row_filters: vec![RowFilter::new(
                "p",
                RowPredicate::NotIn {
                    column: "region".to_owned(),
                    values: vec![],
                },
            )],
            column_masks: vec![],
        };
        let v = build_governed_view("t", "__raw", &schema(), &empty_not_in).expect("view");
        assert!(v.select_sql.ends_with("WHERE TRUE"));
    }

    #[test]
    fn unknown_filter_column_is_invalid_sql_error() {
        let enforcement = Enforcement {
            row_filters: vec![RowFilter::new(
                "p",
                RowPredicate::Eq {
                    column: "ghost".to_owned(),
                    value: json!(1),
                },
            )],
            column_masks: vec![],
        };
        let err = build_governed_view("t", "__raw", &schema(), &enforcement).expect_err("err");
        assert!(matches!(err, crate::error::QueryError::InvalidSql(_)));
    }

    #[test]
    fn drop_mask_omits_column_and_records_it() {
        let enforcement = Enforcement {
            row_filters: vec![],
            column_masks: vec![ColumnMask::new("email", MaskKind::Drop, "p")],
        };
        let view = build_governed_view("t", "__raw", &schema(), &enforcement).expect("view");
        assert!(!view.select_sql.contains("\"email\""));
        assert_eq!(view.masked_columns, vec!["email"]);
    }

    #[test]
    fn hash_and_partial_masks_only_apply_to_strings_else_drop() {
        // Hash on a non-string column (id: Long) fails closed -> dropped.
        let hash_num = Enforcement {
            row_filters: vec![],
            column_masks: vec![ColumnMask::new("id", MaskKind::Hash, "p")],
        };
        let v = build_governed_view("t", "__raw", &schema(), &hash_num).expect("view");
        assert!(!v.select_sql.contains("sha256"));
        assert!(!v.select_sql.contains("\"id\""));
        assert_eq!(v.masked_columns, vec!["id"]);

        // Hash on a string column renders the sha256 expression.
        let hash_str = Enforcement {
            row_filters: vec![],
            column_masks: vec![ColumnMask::new("email", MaskKind::Hash, "p")],
        };
        let v = build_governed_view("t", "__raw", &schema(), &hash_str).expect("view");
        assert!(v.select_sql.contains("sha256"));
        assert!(v.select_sql.contains("AS \"email\""));
    }

    #[test]
    fn custom_mask_fails_closed_to_drop() {
        // A custom SQL mask we cannot verify must be dropped, never executed.
        let enforcement = Enforcement {
            row_filters: vec![],
            column_masks: vec![ColumnMask::new(
                "email",
                MaskKind::Custom {
                    expression: "drop table users".to_owned(),
                },
                "p",
            )],
        };
        let view = build_governed_view("t", "__raw", &schema(), &enforcement).expect("view");
        // The custom expression is nowhere in the SQL; the column is dropped.
        assert!(!view.select_sql.to_lowercase().contains("drop table users"));
        assert!(!view.select_sql.contains("\"email\""));
    }

    #[test]
    fn all_columns_dropped_still_valid_and_leaks_nothing() {
        let enforcement = Enforcement {
            row_filters: vec![],
            column_masks: vec![
                ColumnMask::new("id", MaskKind::Drop, "p"),
                ColumnMask::new("email", MaskKind::Drop, "p"),
                ColumnMask::new("region", MaskKind::Drop, "p"),
                ColumnMask::new("hired", MaskKind::Drop, "p"),
            ],
        };
        let view = build_governed_view("t", "__raw", &schema(), &enforcement).expect("view");
        // No real column names appear; a placeholder keeps the view valid.
        assert!(view.select_sql.contains("__all_columns_restricted"));
        assert!(!view.select_sql.contains("\"email\""));
    }
}
