//! The governed MCP tool catalog (Pillar H, H-F2 context tools + H-F3 query
//! tools).
//!
//! Every tool an agent may call is declared here once — its name, display
//! title, human description, JSON-Schema for arguments, and its
//! [`ToolClass`] (a read-only *context* tool vs a *query* tool that executes
//! and is charged against the agent's budget). The server's `/mcp` route
//! renders these into `tools/list` and dispatches `tools/call` by name; the
//! governance wrapper reads the class to decide whether the query budget
//! applies.
//!
//! # Governance is uniform, declared here
//!
//! Context tools are governed too (H-F2): an agent sees only what its grants
//! allow, and masked/denied columns are *absent* from returned schemas. That
//! enforcement lives in the server (it needs the store); this module fixes the
//! *contract* — the tool names, argument shapes, and the read/query split that
//! the wrapper keys off. Keeping the catalog in the pure crate means the tool
//! surface is testable without a database and cannot silently drift from what
//! the wrapper enforces.

use serde_json::{Value, json};

use crate::protocol::Tool;

/// How a tool is governed: a read-only context tool, or a query tool that
/// executes and consumes budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolClass {
    /// A governed **context** read (H-F2): schema/docs/lineage/metrics. Subject
    /// to RBAC + ABAC (masked columns absent), audited, but it does **not**
    /// consume the query budget.
    Context,
    /// A governed **query** (H-F3): compiles/executes SQL. Subject to the full
    /// governance chain *and* the budget (queries/hour, scanned-bytes/day,
    /// dollar cap). Stubbed until the executor is wired (wave 2).
    Query,
}

impl ToolClass {
    /// Whether a call of this class is charged against the query budget.
    #[must_use]
    pub fn consumes_query_budget(self) -> bool {
        matches!(self, Self::Query)
    }
}

/// A catalog entry: the tool's static definition plus its governance class.
#[derive(Debug, Clone)]
pub struct CatalogTool {
    /// Unique tool name (the `tools/call` selector).
    pub name: &'static str,
    /// Human-readable display title.
    pub title: &'static str,
    /// What the tool does (shown to the model).
    pub description: &'static str,
    /// Governance class (context vs query).
    pub class: ToolClass,
    /// JSON-Schema for the tool arguments.
    pub input_schema: fn() -> Value,
}

impl CatalogTool {
    /// Renders this entry into the MCP wire [`Tool`] shape.
    #[must_use]
    pub fn to_wire(&self) -> Tool {
        Tool {
            name: self.name.to_owned(),
            title: self.title.to_owned(),
            description: self.description.to_owned(),
            input_schema: (self.input_schema)(),
        }
    }
}

// ---------------------------------------------------------------------------
// Argument schemas
// ---------------------------------------------------------------------------

/// A JSON-Schema object with the given properties and required list.
fn object_schema(properties: Value, required: &[&str]) -> Value {
    let mut object = serde_json::Map::new();
    object.insert("type".to_owned(), Value::from("object"));
    object.insert("properties".to_owned(), properties);
    object.insert("required".to_owned(), json!(required));
    object.insert("additionalProperties".to_owned(), Value::from(false));
    Value::Object(object)
}

/// A required string property with a description.
fn string_prop(description: &str) -> Value {
    json!({ "type": "string", "description": description })
}

fn search_assets_schema() -> Value {
    object_schema(
        json!({
            "query": string_prop("Full-text query over asset names, columns, docs, tags, and owners."),
            "limit": {
                "type": "integer",
                "description": "Maximum number of results (1-100, default 20).",
                "minimum": 1,
                "maximum": 100,
            },
        }),
        &["query"],
    )
}

fn get_table_context_schema() -> Value {
    object_schema(
        json!({
            "warehouse": string_prop("Warehouse (catalog prefix) name."),
            "namespace": string_prop("Dotted namespace path, e.g. `sales.eu`."),
            "table": string_prop("Table name within the namespace."),
        }),
        &["warehouse", "namespace", "table"],
    )
}

fn get_lineage_schema() -> Value {
    object_schema(
        json!({
            "warehouse": string_prop("Warehouse (catalog prefix) name."),
            "namespace": string_prop("Dotted namespace path, e.g. `sales.eu`."),
            "table": string_prop("Table name within the namespace."),
            "direction": {
                "type": "string",
                "description": "Which way to walk the graph.",
                "enum": ["upstream", "downstream", "both"],
            },
            "depth": {
                "type": "integer",
                "description": "How many hops to traverse (1-5, default 2).",
                "minimum": 1,
                "maximum": 5,
            },
        }),
        &["warehouse", "namespace", "table"],
    )
}

fn get_metric_definition_schema() -> Value {
    object_schema(
        json!({ "name": string_prop("The metric name.") }),
        &["name"],
    )
}

fn empty_schema() -> Value {
    object_schema(json!({}), &[])
}

fn get_glossary_term_schema() -> Value {
    object_schema(
        json!({ "term": string_prop("The glossary term to look up.") }),
        &["term"],
    )
}

fn query_metrics_schema() -> Value {
    object_schema(
        json!({
            "metric": string_prop("The metric to query."),
            "dimensions": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Dimensions to group by.",
            },
            "filters": string_prop("Optional filter expression."),
        }),
        &["metric"],
    )
}

fn run_sql_schema() -> Value {
    object_schema(
        json!({
            "sql": string_prop("The SQL query to run against governed assets."),
            "warehouse": string_prop("Warehouse the query targets (for routing/policy scope)."),
        }),
        &["sql"],
    )
}

fn preview_table_schema() -> Value {
    object_schema(
        json!({
            "warehouse": string_prop("Warehouse (catalog prefix) name."),
            "namespace": string_prop("Dotted namespace path."),
            "table": string_prop("Table name within the namespace."),
            "limit": {
                "type": "integer",
                "description": "Rows to preview (1-100, default 20).",
                "minimum": 1,
                "maximum": 100,
            },
        }),
        &["warehouse", "namespace", "table"],
    )
}

// ---------------------------------------------------------------------------
// The catalog
// ---------------------------------------------------------------------------

/// Every tool the gateway exposes, in a stable order. Context tools (H-F2)
/// first, then the query tools (H-F3).
pub const CATALOG: &[CatalogTool] = &[
    // --- Context tools (H-F2): governed reads, no budget. ---
    CatalogTool {
        name: "search_assets",
        title: "Search assets",
        description: "Search the catalog for tables, views, and namespaces the agent is \
                      permitted to see. Returns matching assets ranked by relevance. Results \
                      are governed: assets outside the agent's grants are not returned.",
        class: ToolClass::Context,
        input_schema: search_assets_schema,
    },
    CatalogTool {
        name: "get_table_context",
        title: "Get table context",
        description: "Return everything an agent needs to reason about a table: its schema, \
                      documentation, owners, quality/trust score, freshness, and contract \
                      status. The schema is governed — columns the agent may not see (masked \
                      or denied by policy) are ABSENT from the returned schema, never nulled, \
                      so restricted structure cannot leak into a prompt.",
        class: ToolClass::Context,
        input_schema: get_table_context_schema,
    },
    CatalogTool {
        name: "get_lineage",
        title: "Get lineage",
        description: "Return the upstream/downstream lineage graph around a table (which \
                      tables feed it and which it feeds), to the requested depth. Governed: \
                      the root table must be visible to the agent.",
        class: ToolClass::Context,
        input_schema: get_lineage_schema,
    },
    CatalogTool {
        name: "list_metrics",
        title: "List metrics",
        description: "List the governed semantic-layer metrics available to the agent \
                      (measures with their definitions and owners).",
        class: ToolClass::Context,
        input_schema: empty_schema,
    },
    CatalogTool {
        name: "get_metric_definition",
        title: "Get metric definition",
        description: "Return the full definition of one semantic-layer metric: its measure, \
                      dimensions, grain, description, and certification status.",
        class: ToolClass::Context,
        input_schema: get_metric_definition_schema,
    },
    CatalogTool {
        name: "list_data_products",
        title: "List data products",
        description: "List the certified data products (curated bundles of tables, views, \
                      metrics, and contracts) the agent may consume.",
        class: ToolClass::Context,
        input_schema: empty_schema,
    },
    CatalogTool {
        name: "get_glossary_term",
        title: "Get glossary term",
        description: "Look up a business-glossary term: its definition, steward, and the \
                      assets it is linked to.",
        class: ToolClass::Context,
        input_schema: get_glossary_term_schema,
    },
    // --- Query tools (H-F3): governed execution, budget-charged. ---
    CatalogTool {
        name: "query_metrics",
        title: "Query metrics",
        description: "Answer a question against the governed semantic layer by compiling a \
                      metric query to SQL and executing it — the high-accuracy path for \
                      covered questions. Row/column policies apply; results are size-capped \
                      and cost-estimated before execution. Counts against the agent's budget.",
        class: ToolClass::Query,
        input_schema: query_metrics_schema,
    },
    CatalogTool {
        name: "run_sql",
        title: "Run SQL",
        description: "Run a validated, policy-rewritten SQL query against governed assets and \
                      return rows with provenance (the tables and snapshots read, so the agent \
                      can cite). Row filters and column masks are applied; results are \
                      size-capped and cost-estimated before execution. Counts against the \
                      agent's budget.",
        class: ToolClass::Query,
        input_schema: run_sql_schema,
    },
    CatalogTool {
        name: "preview_table",
        title: "Preview table",
        description: "Return a small, policy-safe sample of a table's rows (masked columns \
                      absent, row filters applied). Counts against the agent's budget.",
        class: ToolClass::Query,
        input_schema: preview_table_schema,
    },
];

/// Looks a catalog tool up by name.
#[must_use]
pub fn find(name: &str) -> Option<&'static CatalogTool> {
    CATALOG.iter().find(|t| t.name == name)
}

/// Renders the whole catalog into the MCP `tools/list` wire shape.
#[must_use]
pub fn wire_tools() -> Vec<Tool> {
    CATALOG.iter().map(CatalogTool::to_wire).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_has_all_context_and_query_tools() {
        let names: Vec<&str> = CATALOG.iter().map(|t| t.name).collect();
        // H-F2 context tools.
        for expected in [
            "search_assets",
            "get_table_context",
            "get_lineage",
            "list_metrics",
            "get_metric_definition",
            "list_data_products",
            "get_glossary_term",
        ] {
            assert!(names.contains(&expected), "missing context tool {expected}");
            assert_eq!(find(expected).unwrap().class, ToolClass::Context);
        }
        // H-F3 query tools.
        for expected in ["query_metrics", "run_sql", "preview_table"] {
            assert!(names.contains(&expected), "missing query tool {expected}");
            assert_eq!(find(expected).unwrap().class, ToolClass::Query);
        }
    }

    #[test]
    fn tool_names_are_unique() {
        let mut names: Vec<&str> = CATALOG.iter().map(|t| t.name).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "duplicate tool name in the catalog");
    }

    #[test]
    fn only_query_tools_consume_budget() {
        assert!(!ToolClass::Context.consumes_query_budget());
        assert!(ToolClass::Query.consumes_query_budget());
        assert!(find("get_table_context").unwrap().class == ToolClass::Context);
        assert!(find("run_sql").unwrap().class.consumes_query_budget());
    }

    #[test]
    fn every_schema_is_a_valid_object_schema() {
        for tool in CATALOG {
            let schema = (tool.input_schema)();
            assert_eq!(schema["type"], json!("object"), "{}", tool.name);
            assert!(schema["properties"].is_object(), "{}", tool.name);
            assert!(schema["required"].is_array(), "{}", tool.name);
            // Required entries must actually be declared properties.
            for req in schema["required"].as_array().unwrap() {
                let key = req.as_str().unwrap();
                assert!(
                    schema["properties"].get(key).is_some(),
                    "{} requires undeclared property {key}",
                    tool.name
                );
            }
        }
    }

    #[test]
    fn wire_tools_round_trips_names() {
        let wire = wire_tools();
        assert_eq!(wire.len(), CATALOG.len());
        assert_eq!(wire[0].name, "search_assets");
        // Serializes with the spec field name `inputSchema`.
        let v = serde_json::to_value(&wire[0]).unwrap();
        assert!(v.get("inputSchema").is_some());
    }
}
