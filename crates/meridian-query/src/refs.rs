//! Extracting the catalog tables a SQL statement references — the list the
//! **caller** must resolve to [`GovernedTable`](crate::GovernedTable)s before
//! calling [`run`](crate::run).
//!
//! [`run`] is a pure function of *(metadata + bytes + policy + SQL + caps)*: it
//! does not resolve names or load metadata, so the caller (a server route) must
//! hand it exactly the tables the SQL will bind. To do that the caller needs to
//! know which tables the SQL names — and it must agree with what the executor's
//! planner will resolve, or a policy could be resolved for a table the query
//! does not touch (over-broad) or missed for one it does (a leak).
//!
//! This uses the **same `DataFusion` SQL parser** [`run`] uses internally
//! ([`DFParser`]) and `DataFusion`'s own relation visitor
//! ([`resolve_table_references`]), so the set returned here is precisely the set
//! the executor binds — no second parser, no drift. Common-table-expression
//! (`WITH`) names are excluded: they are query-local, not catalog tables (the
//! visitor already separates them, including the shadowing case where a CTE and
//! a real table share a name).

use datafusion::sql::parser::DFParser;
use datafusion::sql::resolve::resolve_table_references;

use crate::error::{QueryError, QueryResult};

/// A catalog table a SQL statement references, decomposed into the optional
/// multi-level namespace and the table name, exactly as it appeared in the SQL.
///
/// Iceberg namespaces are multi-level (`a.b.c.table`); a SQL reference maps its
/// parts to `catalog.schema.table` (at most three by SQL grammar). We surface
/// the parts the caller needs to resolve the table in its catalog: the leading
/// parts (namespace) and the final part (table). The caller decides how to bind
/// them to a warehouse/namespace (a bare `table` uses a caller-supplied default
/// namespace; a qualified `ns.table` uses `ns`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    /// The namespace parts, outermost first (empty for a bare table name).
    pub namespace: Vec<String>,
    /// The table name (the final identifier).
    pub table: String,
}

impl TableRef {
    /// The reference as it would be written in SQL (`ns.sub.table`), for
    /// diagnostics and for use as the executor's per-table registered name.
    #[must_use]
    pub fn qualified_name(&self) -> String {
        if self.namespace.is_empty() {
            self.table.clone()
        } else {
            format!("{}.{}", self.namespace.join("."), self.table)
        }
    }
}

/// Parses `sql` and returns the distinct catalog tables it references, in a
/// stable (sorted) order, with CTE names excluded.
///
/// The SQL must be a single statement; a parse failure or a multi-statement
/// input is a [`QueryError::InvalidSql`] (the same fail-closed shape [`run`]'s
/// read-only gate uses). This does **not** enforce read-only — [`run`] does that
/// on the same parse — it only enumerates relations, so a caller can resolve
/// policy for exactly them.
///
/// [`run`]: crate::run
pub fn referenced_tables(sql: &str) -> QueryResult<Vec<TableRef>> {
    let statements = DFParser::parse_sql(sql).map_err(|e| QueryError::InvalidSql(e.to_string()))?;
    if statements.len() != 1 {
        return Err(QueryError::InvalidSql(format!(
            "expected exactly one statement, found {}",
            statements.len()
        )));
    }
    let statement = statements.front().expect("one statement");

    // Identifier normalization on (lower-casing unquoted identifiers) matches
    // DataFusion's default planner behavior, so the names here are exactly the
    // ones the executor will look up when it binds the query.
    let (relations, _ctes) = resolve_table_references(statement, true)
        .map_err(|e| QueryError::InvalidSql(e.to_string()))?;

    let mut refs: Vec<TableRef> = relations
        .into_iter()
        .map(|r| {
            // A TableReference is Bare{table} | Partial{schema, table} |
            // Full{catalog, schema, table}. We flatten catalog+schema into the
            // namespace parts (outermost first) and keep the table.
            let mut namespace: Vec<String> = Vec::new();
            if let Some(catalog) = r.catalog() {
                namespace.push(catalog.to_owned());
            }
            if let Some(schema) = r.schema() {
                namespace.push(schema.to_owned());
            }
            TableRef {
                namespace,
                table: r.table().to_owned(),
            }
        })
        .collect();

    // `resolve_table_references` already returns a deduplicated, ordered set
    // (it collects from a BTreeSet), but decomposition could in principle
    // collide; sort + dedup on our shape to be certain the caller sees each
    // table once.
    refs.sort_by(|a, b| {
        a.namespace
            .cmp(&b.namespace)
            .then_with(|| a.table.cmp(&b.table))
    });
    refs.dedup();
    Ok(refs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_table_has_empty_namespace() {
        let refs = referenced_tables("SELECT * FROM sales").expect("parse");
        assert_eq!(refs.len(), 1);
        assert!(refs[0].namespace.is_empty());
        assert_eq!(refs[0].table, "sales");
        assert_eq!(refs[0].qualified_name(), "sales");
    }

    #[test]
    fn qualified_table_splits_namespace_and_table() {
        let refs = referenced_tables("SELECT * FROM sales.eu.customers").expect("parse");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].namespace, vec!["sales", "eu"]);
        assert_eq!(refs[0].table, "customers");
        assert_eq!(refs[0].qualified_name(), "sales.eu.customers");
    }

    #[test]
    fn multiple_tables_are_deduped_and_sorted() {
        let refs =
            referenced_tables("SELECT * FROM b JOIN a ON a.id = b.id JOIN a AS a2 ON a2.id = b.id")
                .expect("parse");
        let names: Vec<String> = refs.iter().map(TableRef::qualified_name).collect();
        assert_eq!(names, vec!["a".to_owned(), "b".to_owned()]);
    }

    #[test]
    fn cte_names_are_excluded_even_when_shadowing_a_real_table() {
        // `t` is both a CTE and — inside the CTE body — a real table. The real
        // table must be returned; the CTE name must not add a phantom table.
        let refs = referenced_tables("WITH t AS (SELECT * FROM t) SELECT * FROM t").expect("parse");
        let names: Vec<String> = refs.iter().map(TableRef::qualified_name).collect();
        assert_eq!(names, vec!["t".to_owned()], "only the real table `t`");
    }

    #[test]
    fn pure_cte_query_references_no_catalog_table() {
        let refs = referenced_tables("WITH c AS (SELECT 1 AS x) SELECT x FROM c").expect("parse");
        assert!(refs.is_empty(), "a CTE-only query touches no catalog table");
    }

    #[test]
    fn no_from_clause_references_no_table() {
        let refs = referenced_tables("SELECT 1").expect("parse");
        assert!(refs.is_empty());
    }

    #[test]
    fn unparseable_sql_is_invalid_sql_error() {
        // Genuinely malformed input the parser rejects (a dangling operator).
        // (Note: enumeration does not enforce read-only — `run`'s gate does —
        // so this only asserts the parse-failure path, not statement kind.)
        let err = referenced_tables("SELECT * FROM WHERE)(").expect_err("should fail");
        assert!(matches!(err, QueryError::InvalidSql(_)));
    }

    #[test]
    fn multi_statement_is_refused() {
        let err = referenced_tables("SELECT 1; SELECT 2").expect_err("should fail");
        assert!(matches!(err, QueryError::InvalidSql(_)));
    }
}
