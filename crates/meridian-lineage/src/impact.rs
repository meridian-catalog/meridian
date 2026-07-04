//! Impact analysis and the lineage graph (F-F5).
//!
//! - [`lineage_graph`] walks the edge set breadth-first from an asset to a
//!   bounded depth in a direction (upstream, downstream, or both) and returns
//!   the reachable nodes and the edges among them — the graph the console
//!   renders and the API serves.
//! - [`impact_of`] answers "if I make this change to this asset, what breaks?"
//!   — the downstream blast radius, with each affected asset's owner (from its
//!   `owner` table property when set) so the incidents wave can notify them.
//!
//! Traversal is over native-table endpoints only (external endpoints are leaf
//! nodes: Meridian cannot traverse past a dataset it does not own). Confidence
//! is carried through so a caller can threshold weak edges; nothing here
//! invents an edge, so a table with no recorded lineage has an empty blast
//! radius — truthfully empty, not a guess.

use std::collections::{BTreeMap, HashSet, VecDeque};

use meridian_common::Result;
use meridian_common::id::WorkspaceId;
use serde::Serialize;
use sqlx::PgPool;

use crate::model::{Endpoint, LineageEdge, Provenance, downstream_edges, upstream_edges};

/// Traversal direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Toward sources (what this asset is derived from).
    Upstream,
    /// Toward consumers (what is derived from this asset).
    Downstream,
    /// Both directions.
    Both,
}

impl Direction {
    /// Parses the `direction` query parameter. Defaults to `both`.
    #[must_use]
    pub fn parse(raw: Option<&str>) -> Self {
        match raw {
            Some("upstream") => Self::Upstream,
            Some("downstream") => Self::Downstream,
            _ => Self::Both,
        }
    }
}

/// A node in the returned graph: a native Meridian table with a display ident.
#[derive(Debug, Clone, Serialize)]
pub struct GraphNode {
    /// The `tables.id`.
    pub table_id: String,
    /// Human-readable identifier (`warehouse.ns.table`) when resolvable.
    pub ident: Option<String>,
    /// Depth from the root (0 = the queried asset).
    pub depth: u32,
}

/// A serialized edge between two nodes (endpoints rendered as ids/names).
#[derive(Debug, Clone, Serialize)]
pub struct GraphEdge {
    /// Source endpoint identity string (`meridian:<id>` or `ext:<name>`).
    pub src: String,
    /// Destination endpoint identity string.
    pub dst: String,
    /// How the edge was learned.
    pub provenance: Provenance,
    /// Confidence in `[0,1]`.
    pub confidence: f64,
    /// Whether the edge carries column-level detail.
    pub has_column_map: bool,
}

/// The result of [`lineage_graph`].
#[derive(Debug, Clone, Serialize)]
pub struct LineageGraph {
    /// The root asset id the graph was built from.
    pub root: String,
    /// Reachable nodes, including the root.
    pub nodes: Vec<GraphNode>,
    /// Edges among the reachable nodes.
    pub edges: Vec<GraphEdge>,
}

fn endpoint_key(endpoint: &Endpoint) -> String {
    match endpoint {
        Endpoint::Table { id } => format!("meridian:{id}"),
        Endpoint::External { name } => format!("ext:{name}"),
    }
}

fn serialize_edge(edge: &LineageEdge) -> GraphEdge {
    GraphEdge {
        src: endpoint_key(&edge.src),
        dst: endpoint_key(&edge.dst),
        provenance: edge.provenance,
        confidence: edge.confidence,
        has_column_map: edge.column_map.is_some(),
    }
}

/// Builds the up/downstream lineage graph around `root_table_id` to `depth`
/// hops in `direction`. `depth = 0` returns just the root node.
pub async fn lineage_graph(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    root_table_id: &str,
    direction: Direction,
    depth: u32,
) -> Result<LineageGraph> {
    // BFS over native-table nodes. `depth_of` doubles as the visited set.
    let mut depth_of: BTreeMap<String, u32> = BTreeMap::new();
    let mut edges: Vec<GraphEdge> = Vec::new();
    let mut edge_seen: HashSet<(String, String, &'static str)> = HashSet::new();
    let mut queue: VecDeque<(String, u32)> = VecDeque::new();

    depth_of.insert(root_table_id.to_owned(), 0);
    queue.push_back((root_table_id.to_owned(), 0));

    while let Some((table_id, node_depth)) = queue.pop_front() {
        if node_depth >= depth {
            continue;
        }
        let next_depth = node_depth + 1;

        // Downstream: follow edges where this node is the source.
        if matches!(direction, Direction::Downstream | Direction::Both) {
            for edge in downstream_edges(pool, workspace_id, &table_id).await? {
                push_edge(&mut edges, &mut edge_seen, &edge);
                enqueue_native(&edge.dst, next_depth, &mut depth_of, &mut queue);
            }
        }
        // Upstream: follow edges where this node is the target.
        if matches!(direction, Direction::Upstream | Direction::Both) {
            for edge in upstream_edges(pool, workspace_id, &table_id).await? {
                push_edge(&mut edges, &mut edge_seen, &edge);
                enqueue_native(&edge.src, next_depth, &mut depth_of, &mut queue);
            }
        }
    }

    // Resolve display idents for the native nodes.
    let mut nodes = Vec::with_capacity(depth_of.len());
    for (table_id, node_depth) in depth_of {
        let ident = table_ident(pool, &table_id).await?;
        nodes.push(GraphNode {
            table_id,
            ident,
            depth: node_depth,
        });
    }
    nodes.sort_by(|a, b| (a.depth, &a.table_id).cmp(&(b.depth, &b.table_id)));

    Ok(LineageGraph {
        root: root_table_id.to_owned(),
        nodes,
        edges,
    })
}

fn push_edge(
    edges: &mut Vec<GraphEdge>,
    seen: &mut HashSet<(String, String, &'static str)>,
    edge: &LineageEdge,
) {
    let provenance = edge.provenance.as_str();
    let key = (endpoint_key(&edge.src), endpoint_key(&edge.dst), provenance);
    if seen.insert(key) {
        edges.push(serialize_edge(edge));
    }
}

/// Enqueues an endpoint for further traversal only if it is a native table not
/// yet visited (external endpoints are leaves).
fn enqueue_native(
    endpoint: &Endpoint,
    depth: u32,
    depth_of: &mut BTreeMap<String, u32>,
    queue: &mut VecDeque<(String, u32)>,
) {
    if let Endpoint::Table { id } = endpoint
        && !depth_of.contains_key(id)
    {
        depth_of.insert(id.clone(), depth);
        queue.push_back((id.clone(), depth));
    }
}

// ---------------------------------------------------------------------------
// Impact analysis
// ---------------------------------------------------------------------------

/// A requested change whose blast radius is being analyzed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// The whole table is being dropped or rewritten incompatibly.
    DropTable,
    /// A named column is being dropped or renamed.
    DropColumn(String),
}

impl Change {
    /// Parses a `change=` query value: `drop_table`, or `drop_column:<name>`.
    /// Returns `None` for an unrecognized change.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        if raw == "drop_table" {
            return Some(Self::DropTable);
        }
        if let Some(column) = raw.strip_prefix("drop_column:") {
            if column.is_empty() {
                return None;
            }
            return Some(Self::DropColumn(column.to_owned()));
        }
        None
    }
}

/// One downstream asset affected by a change.
#[derive(Debug, Clone, Serialize)]
pub struct AffectedAsset {
    /// The affected table id.
    pub table_id: String,
    /// Human-readable identifier when resolvable.
    pub ident: Option<String>,
    /// Owner, from the table's `owner` property when set (never fabricated).
    pub owner: Option<String>,
    /// Hops from the changed asset (1 = a direct consumer).
    pub depth: u32,
    /// The column (of the changed asset) whose lineage reaches this asset,
    /// when the change is column-scoped and the reaching edge carried a
    /// column map. `None` means the connection is table-level: the asset is
    /// reached, but no column-precise proof that *this* column feeds it.
    pub via_column: Option<String>,
    /// Weakest edge confidence along the path — how sure we are of the link.
    pub path_confidence: f64,
}

/// The result of [`impact_of`].
#[derive(Debug, Clone, Serialize)]
pub struct ImpactReport {
    /// The changed asset id.
    pub asset: String,
    /// The change analyzed, rendered as a string.
    pub change: String,
    /// Distinct owners of affected assets (for notification), sorted.
    pub owners: Vec<String>,
    /// Affected downstream assets, nearest first.
    pub affected: Vec<AffectedAsset>,
    /// True when the change is column-scoped: only downstream reachable via a
    /// column map for that column are column-attributed; table-level-only
    /// links are still reported (a dropped column *may* break them) but
    /// flagged with `via_column = None`, never silently pruned.
    pub column_scoped: bool,
}

/// Computes the downstream blast radius of a change to an asset.
///
/// Traversal is downstream-only and bounded by `max_depth`. For a
/// `DropColumn` change, an edge that carries a `column_map` is followed only
/// when it maps the changed column (precise); an edge with no column map is
/// still followed (a table-level dependency a column drop can break) but the
/// affected asset is marked `via_column = None`, honestly signaling the
/// uncertainty rather than dropping the asset or fabricating a column link.
pub async fn impact_of(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    asset_table_id: &str,
    change: &Change,
    max_depth: u32,
) -> Result<ImpactReport> {
    // BFS carrying, per node, the column (of the root) it traces back to (for
    // column-scoped changes) and the weakest confidence on the path.
    #[derive(Clone)]
    struct Frontier {
        table_id: String,
        depth: u32,
        via_column: Option<String>,
        path_confidence: f64,
    }

    let column_scoped = matches!(change, Change::DropColumn(_));
    let target_column = match change {
        Change::DropColumn(name) => Some(name.clone()),
        Change::DropTable => None,
    };

    let mut best: BTreeMap<String, AffectedAsset> = BTreeMap::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<Frontier> = VecDeque::new();

    visited.insert(asset_table_id.to_owned());
    queue.push_back(Frontier {
        table_id: asset_table_id.to_owned(),
        depth: 0,
        via_column: target_column.clone(),
        path_confidence: 1.0,
    });

    while let Some(node) = queue.pop_front() {
        if node.depth >= max_depth {
            continue;
        }
        for edge in downstream_edges(pool, workspace_id, &node.table_id).await? {
            let Endpoint::Table { id: dst_id } = &edge.dst else {
                continue; // external consumers are leaves we cannot own/notify
            };

            // Column attribution: for a column-scoped change at the *root*,
            // follow a column map only if it carries the tracked column. At
            // depth 0 the tracked column is the changed column; deeper, it is
            // whatever downstream column the previous hop produced.
            let (follow, next_via) =
                column_follow(node.via_column.as_deref(), &edge, column_scoped);
            if !follow {
                continue;
            }

            let path_confidence = node.path_confidence.min(edge.confidence);
            let child_depth = node.depth + 1;

            record_affected(
                pool,
                &mut best,
                dst_id,
                child_depth,
                next_via.clone(),
                path_confidence,
            )
            .await?;

            if visited.insert(dst_id.clone()) {
                queue.push_back(Frontier {
                    table_id: dst_id.clone(),
                    depth: child_depth,
                    via_column: next_via,
                    path_confidence,
                });
            }
        }
    }

    let mut affected: Vec<AffectedAsset> = best.into_values().collect();
    affected.sort_by(|a, b| (a.depth, &a.table_id).cmp(&(b.depth, &b.table_id)));

    let mut owners: Vec<String> = affected
        .iter()
        .filter_map(|a| a.owner.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    owners.sort();

    Ok(ImpactReport {
        asset: asset_table_id.to_owned(),
        change: render_change(change),
        owners,
        affected,
        column_scoped,
    })
}

/// Decides whether to follow an edge for the current tracked column, and what
/// column to track past it.
///
/// - Table-scoped change (`via` is `None` and not column-scoped, or the change
///   is `DropTable`): always follow; nothing to track.
/// - Column-scoped change with a tracked column: if the edge has a column map,
///   follow it only when some entry's `src_column` equals the tracked column,
///   and track that entry's `dst_column` onward (precise). If the edge has no
///   column map, follow it (table-level dependency) but stop tracking the
///   column (`via = None`) — the link is real but not column-precise.
fn column_follow(
    via: Option<&str>,
    edge: &LineageEdge,
    column_scoped: bool,
) -> (bool, Option<String>) {
    if !column_scoped {
        return (true, None);
    }
    let Some(tracked) = via else {
        // Column-scoped, but we already lost column precision upstream: the
        // downstream is still table-reachable, so keep following table-level.
        return (true, None);
    };
    match &edge.column_map {
        Some(map) => {
            let hit = map.iter().find(|e| e.src_column == tracked);
            match hit {
                Some(entry) => (true, Some(entry.dst_column.clone())),
                None => (false, None), // this column does not feed this edge
            }
        }
        // No column detail: a column drop may still break a table-level
        // consumer, so follow, but without column precision.
        None => (true, None),
    }
}

async fn record_affected(
    pool: &PgPool,
    best: &mut BTreeMap<String, AffectedAsset>,
    table_id: &str,
    depth: u32,
    via_column: Option<String>,
    path_confidence: f64,
) -> Result<()> {
    // Keep the nearest, most-confident, most-column-precise record per asset.
    if let Some(existing) = best.get_mut(table_id) {
        if depth < existing.depth
            || (depth == existing.depth && path_confidence > existing.path_confidence)
        {
            existing.depth = depth;
            existing.path_confidence = path_confidence;
        }
        if existing.via_column.is_none() && via_column.is_some() {
            existing.via_column = via_column;
        }
        return Ok(());
    }
    let (ident, owner) = table_ident_and_owner(pool, table_id).await?;
    best.insert(
        table_id.to_owned(),
        AffectedAsset {
            table_id: table_id.to_owned(),
            ident,
            owner,
            depth,
            via_column,
            path_confidence,
        },
    );
    Ok(())
}

fn render_change(change: &Change) -> String {
    match change {
        Change::DropTable => "drop_table".to_owned(),
        Change::DropColumn(c) => format!("drop_column:{c}"),
    }
}

/// Loads a table's display ident (`warehouse.ns.table`) if it still exists.
async fn table_ident(pool: &PgPool, table_id: &str) -> Result<Option<String>> {
    Ok(table_ident_and_owner(pool, table_id).await?.0)
}

/// One `tables` row's display identity + owner, joined to its namespace and
/// warehouse. Kept in this crate (rather than widening the shared `table`
/// store) since the ident string + `owner` property is exactly what the
/// lineage/impact surface needs and nothing else consumes it yet.
#[derive(sqlx::FromRow)]
struct IdentRow {
    warehouse_name: String,
    levels: Vec<String>,
    name: String,
    properties: sqlx::types::Json<BTreeMap<String, String>>,
}

/// Loads a table's display ident (`warehouse.ns.table`) and `owner` property
/// in one lookup. The owner is only ever the value of the table's `owner`
/// property — never inferred — so an unowned table reports `None`, honestly.
async fn table_ident_and_owner(
    pool: &PgPool,
    table_id: &str,
) -> Result<(Option<String>, Option<String>)> {
    let row: Option<IdentRow> = sqlx::query_as(
        "SELECT w.name AS warehouse_name, n.levels AS levels, t.name AS name,
                t.properties AS properties
         FROM tables t
         JOIN namespaces n ON n.id = t.namespace_id
         JOIN warehouses w ON w.id = n.warehouse_id
         WHERE t.id = $1",
    )
    .bind(table_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| meridian_store::map_sqlx_error("failed to load table ident", e))?;

    let Some(row) = row else {
        return Ok((None, None));
    };
    let mut ident = row.warehouse_name;
    for level in &row.levels {
        ident.push('.');
        ident.push_str(level);
    }
    ident.push('.');
    ident.push_str(&row.name);
    let owner = row.properties.0.get("owner").cloned();
    Ok((Some(ident), owner))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_parse_recognizes_forms() {
        assert_eq!(Change::parse("drop_table"), Some(Change::DropTable));
        assert_eq!(
            Change::parse("drop_column:email"),
            Some(Change::DropColumn("email".to_owned())),
        );
        assert_eq!(Change::parse("drop_column:"), None);
        assert_eq!(Change::parse("nonsense"), None);
    }

    #[test]
    fn direction_parse_defaults_to_both() {
        assert_eq!(Direction::parse(Some("upstream")), Direction::Upstream);
        assert_eq!(Direction::parse(Some("downstream")), Direction::Downstream);
        assert_eq!(Direction::parse(Some("both")), Direction::Both);
        assert_eq!(Direction::parse(None), Direction::Both);
        assert_eq!(Direction::parse(Some("garbage")), Direction::Both);
    }
}
