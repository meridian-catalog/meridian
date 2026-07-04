//! Resolving an external table identifier string to a Meridian [`Endpoint`].
//!
//! Lineage inputs arrive as opaque identifier strings: a declared commit input
//! (`analytics.sales.orders`), or an OpenLineage dataset the sink has
//! normalized to a dotted ident. This module maps such a string to a *native*
//! table endpoint when — and only when — it unambiguously names a table
//! Meridian owns; otherwise it records an *external* endpoint under the
//! original (normalized) string. It never guesses: an unresolved identifier
//! becomes a labeled external node, not a fabricated table id.
//!
//! # Dotted-identifier interpretation
//!
//! A Meridian table is addressed as `warehouse . <namespace levels…> . name`.
//! Given `a.b.c.d`, the first component is the warehouse, the last is the
//! table, and the middle components are the namespace levels
//! (`b.c` → levels `["b","c"]`). A single- or two-component string cannot name
//! a native table (there is always at least warehouse + table, and a table
//! always lives in a namespace of ≥1 level), so it resolves straight to an
//! external endpoint. When the split *could* name a table but no such table
//! exists, the identifier is likewise external — resolution is a lookup, not a
//! constructor.

use meridian_common::Result;
use meridian_common::id::WorkspaceId;
use meridian_store::{table, tenancy, warehouse};
use sqlx::PgPool;

use crate::model::Endpoint;

/// Resolves an input identifier to a native-table endpoint when it names a
/// table Meridian owns, else to an external endpoint carrying the identifier
/// verbatim.
pub async fn resolve_input_endpoint(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    identifier: &str,
) -> Result<Endpoint> {
    if let Some(table_id) = resolve_native(pool, workspace_id, identifier).await? {
        Ok(Endpoint::table(table_id))
    } else {
        Ok(Endpoint::external(identifier))
    }
}

/// Attempts to resolve a dotted identifier to a native `tables.id`. Returns
/// `None` when it does not name an existing Meridian table.
async fn resolve_native(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    identifier: &str,
) -> Result<Option<String>> {
    let parts: Vec<&str> = identifier.split('.').filter(|p| !p.is_empty()).collect();
    // warehouse + ≥1 namespace level + table  ⇒  at least 3 components.
    if parts.len() < 3 {
        return Ok(None);
    }
    let warehouse_name = parts[0];
    let table_name = parts[parts.len() - 1];
    let levels: Vec<String> = parts[1..parts.len() - 1]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();

    let Some(wh) = warehouse::get_by_name(pool, workspace_id, warehouse_name).await? else {
        return Ok(None);
    };
    let record = table::get_by_name(pool, &wh.id, &levels, table_name).await?;
    Ok(record.map(|r| r.id))
}

/// Convenience wrapper defaulting to the single-workspace deployment's
/// workspace, used by paths that do not carry a workspace explicitly.
pub async fn resolve_input_endpoint_default(pool: &PgPool, identifier: &str) -> Result<Endpoint> {
    resolve_input_endpoint(pool, tenancy::default_workspace_id(), identifier).await
}
