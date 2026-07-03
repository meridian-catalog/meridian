//! Full-text search over catalog assets (search v1: Postgres FTS only —
//! no embeddings, no external search engine).
//!
//! # Index
//!
//! Migration 0010 gives `tables`, `views`, and `namespaces` a
//! trigger-maintained `search_tsv` tsvector (GIN-indexed) over the asset
//! name (weight A), the namespace path (B), column names + docs from the
//! current table schema (C, via the write-through `schema_text` column),
//! and `properties ->> 'comment'` (D). The migration header documents why
//! triggers were chosen over generated columns.
//!
//! # Query semantics
//!
//! The user's query is tokenized (identifier characters only), each token
//! becomes a prefix term (`tok:*`), and the terms are AND-ed. Postgres'
//! parser splits identifiers on underscores in both the document and the
//! query, so `customer_email` becomes the phrase `customer <-> email` —
//! matching the column name exactly — while a bare `email` matches it as a
//! part. Ranking is `ts_rank` over the weighted tsvector plus two boosts:
//! an exact (case-insensitive) asset-name match and an asset-name prefix
//! match. Snippets are `ts_headline` over the reconstructed document text,
//! computed only for the returned page.
//!
//! # Pagination
//!
//! Keyset over `(score DESC, kind:id ASC)`. The page token encodes the last
//! row's exact score (f64 bits, so no decimal round-trip loss) and its
//! `kind:id` tie-break; the next page filters strictly past it. Scores are
//! deterministic for a fixed query and corpus, so tokens stay stable across
//! requests; a mutation between pages can shift results, which is inherent
//! to keyset pagination over a live ranking.
//!
//! # Authorization
//!
//! [`visibility_for`] resolves the caller's visibility in a constant number
//! of queries (2), and [`search`] applies it inside the one search query —
//! there is **no per-result authorization round-trip** (no N+1 on results).
//! Tables and views are visible with a `READ` grant on the asset, any
//! ancestor namespace, or the warehouse (or a built-in `admin` /
//! `catalog_reader` binding); namespaces are visible with `LIST_NAMESPACES`
//! on their warehouse. The one super-linear piece is honest: the
//! namespace-inheritance check runs an EXISTS probe over the caller's
//! *granted namespace set* per candidate row, so its cost is
//! O(matched rows × granted namespaces) primary-key lookups inside one
//! query. TODO(benchmark phase): if that shows up on grant-heavy
//! deployments, precompute a per-principal closure table or cache the
//! visibility snapshot (invalidation must cover grant/binding mutations —
//! same TODO as the rbac decision cache).

use meridian_common::id::WorkspaceId;
use meridian_common::principal::Principal;
use meridian_common::{MeridianError, Result};
use meridian_iceberg::spec::Schema;
use meridian_iceberg::spec::types::Type;
use sqlx::PgPool;

use crate::map_sqlx_error;

/// Hard cap on query tokens; the rest are ignored (bounds tsquery cost).
const MAX_QUERY_TOKENS: usize = 16;

/// Kinds of searchable assets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchAssetKind {
    /// An Iceberg table.
    Table,
    /// An Iceberg view.
    View,
    /// A namespace.
    Namespace,
}

impl SearchAssetKind {
    /// The wire rendering (query parameter and result `type` field).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::View => "view",
            Self::Namespace => "namespace",
        }
    }

    /// Parses the wire rendering.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "table" => Some(Self::Table),
            "view" => Some(Self::View),
            "namespace" => Some(Self::Namespace),
            _ => None,
        }
    }
}

/// One search request against the store.
#[derive(Debug, Clone)]
pub struct SearchRequest<'a> {
    /// The user's query text.
    pub text: &'a str,
    /// Restrict to one warehouse (by id).
    pub warehouse_id: Option<&'a str>,
    /// Restrict to assets at or under this namespace path.
    pub namespace: Option<&'a [String]>,
    /// Restrict to these asset kinds (`None` = all).
    pub kinds: Option<&'a [SearchAssetKind]>,
    /// Page size (the caller clamps; this module trusts it).
    pub limit: i64,
    /// Opaque keyset token from the previous page.
    pub page_token: Option<&'a str>,
}

/// What the calling principal is allowed to see.
///
/// Resolved once per request by [`visibility_for`] and applied inside the
/// search query.
#[derive(Debug, Clone, Default)]
pub struct SearchVisibility {
    /// Everything is visible (anonymous dev mode, `admin`, or
    /// `catalog_reader`).
    pub unrestricted: bool,
    /// Warehouses with a `READ` grant (covers all tables/views inside).
    pub read_warehouse_ids: Vec<String>,
    /// Namespaces with a `READ` grant (covers descendants).
    pub read_namespace_ids: Vec<String>,
    /// Tables with a direct `READ` grant.
    pub read_table_ids: Vec<String>,
    /// Views with a direct `READ` grant.
    pub read_view_ids: Vec<String>,
    /// Warehouses with a `LIST_NAMESPACES` grant (gates namespace results).
    pub list_namespaces_warehouse_ids: Vec<String>,
}

impl SearchVisibility {
    /// Visibility that sees everything.
    #[must_use]
    pub fn all() -> Self {
        Self {
            unrestricted: true,
            ..Self::default()
        }
    }
}

/// One search result.
#[derive(Debug, Clone)]
pub struct SearchHit {
    /// Asset kind.
    pub kind: SearchAssetKind,
    /// ULID of the asset row.
    pub id: String,
    /// Asset name (the last level, for namespaces).
    pub name: String,
    /// Namespace path (for a namespace hit: its own full path).
    pub namespace: Vec<String>,
    /// Name of the containing warehouse.
    pub warehouse: String,
    /// Final score (rank + boosts); higher is better.
    pub rank: f64,
    /// `ts_headline` snippet with `**`-marked matches.
    pub snippet: String,
}

/// One page of results.
#[derive(Debug, Clone)]
pub struct SearchPage {
    /// The hits, best first.
    pub hits: Vec<SearchHit>,
    /// Token for the next page; `None` when this is the last one.
    pub next_page_token: Option<String>,
}

// ---------------------------------------------------------------------------
// Query building
// ---------------------------------------------------------------------------

/// Tokenizes the raw query into identifier-ish tokens (alphanumerics and
/// interior underscores), lower-cased. Everything else separates tokens.
fn query_tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .map(|t| t.trim_matches('_'))
        .filter(|t| t.chars().any(char::is_alphanumeric))
        .take(MAX_QUERY_TOKENS)
        .map(str::to_lowercase)
        .collect()
}

/// Builds the `to_tsquery` input: every token as a prefix term, AND-ed.
/// Tokens only contain `[alphanumeric_]`, so no tsquery syntax can leak in.
fn build_tsquery(tokens: &[String]) -> String {
    tokens
        .iter()
        .map(|t| format!("{t}:*"))
        .collect::<Vec<_>>()
        .join(" & ")
}

/// Escapes LIKE metacharacters (`\`, `%`, `_`) for the name-prefix boost.
fn like_prefix_pattern(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 1);
    for c in text.chars() {
        if matches!(c, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('%');
    out
}

/// Renders a page token: exact score bits, the kind, and the row id.
fn encode_page_token(score: f64, kind: SearchAssetKind, id: &str) -> String {
    format!("{:016x}.{}.{}", score.to_bits(), kind.as_str(), id)
}

/// Parses a page token back into (score, `kind:id` tie-break).
fn decode_page_token(token: &str) -> Result<(f64, String)> {
    let invalid = || MeridianError::Validation("invalid page_token".to_owned());
    let mut parts = token.splitn(3, '.');
    let (Some(bits), Some(kind), Some(id)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(invalid());
    };
    let bits = u64::from_str_radix(bits, 16).map_err(|_| invalid())?;
    let score = f64::from_bits(bits);
    if !score.is_finite() || SearchAssetKind::parse(kind).is_none() || id.is_empty() {
        return Err(invalid());
    }
    Ok((score, format!("{kind}:{id}")))
}

// ---------------------------------------------------------------------------
// Visibility
// ---------------------------------------------------------------------------

/// Resolves the caller's [`SearchVisibility`] in two queries: one for the
/// built-in-role shortcut, one for the applicable `READ` /
/// `LIST_NAMESPACES` grants (direct or via role bindings).
///
/// Anonymous principals (auth disabled) see everything — the same wholesale
/// bypass as [`crate::rbac::authorize`]. An authenticated principal without
/// a `principals` row (impossible in practice: authentication JIT-provisions
/// it) simply has no grants and sees nothing.
pub async fn visibility_for(pool: &PgPool, principal: &Principal) -> Result<SearchVisibility> {
    if principal.is_anonymous() {
        return Ok(SearchVisibility::all());
    }
    let Some(issuer) = principal.issuer.as_deref() else {
        // Contract violation (authenticated principals carry an issuer):
        // fail closed to zero visibility rather than erroring the search.
        return Ok(SearchVisibility::default());
    };

    // Built-in roles short-circuit: admin sees everything; catalog_reader's
    // blanket read-only set (LIST_NAMESPACES, LIST_TABLES, READ) covers
    // everything search exposes.
    let unrestricted: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM roles r
             JOIN role_bindings rb ON rb.role_id = r.id
             JOIN principals p ON p.id = rb.principal_id
             WHERE p.issuer = $1 AND p.subject = $2
               AND r.built_in AND r.name IN ('admin', 'catalog_reader')
         )",
    )
    .bind(issuer)
    .bind(&principal.subject)
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to resolve search visibility", e))?;
    if unrestricted {
        return Ok(SearchVisibility::all());
    }

    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "WITH me AS (
             SELECT id FROM principals WHERE issuer = $1 AND subject = $2
         ),
         my_roles AS (
             SELECT role_id FROM role_bindings
             WHERE principal_id IN (SELECT id FROM me)
         )
         SELECT g.securable_type, g.securable_id, g.privilege
         FROM grants g
         WHERE g.privilege IN ('READ', 'LIST_NAMESPACES')
           AND (g.principal_id IN (SELECT id FROM me)
                OR g.role_id IN (SELECT role_id FROM my_roles))",
    )
    .bind(issuer)
    .bind(&principal.subject)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load search grants", e))?;

    let mut visibility = SearchVisibility::default();
    for (securable_type, securable_id, privilege) in rows {
        match (securable_type.as_str(), privilege.as_str()) {
            ("warehouse", "READ") => visibility.read_warehouse_ids.push(securable_id),
            ("warehouse", "LIST_NAMESPACES") => {
                visibility.list_namespaces_warehouse_ids.push(securable_id);
            }
            ("namespace", "READ") => visibility.read_namespace_ids.push(securable_id),
            ("table", "READ") => visibility.read_table_ids.push(securable_id),
            ("view", "READ") => visibility.read_view_ids.push(securable_id),
            // LIST_NAMESPACES is warehouse-native and not grantable
            // elsewhere; anything else here would be a data bug and is
            // ignored rather than trusted.
            _ => {}
        }
    }
    Ok(visibility)
}

// ---------------------------------------------------------------------------
// The search query
// ---------------------------------------------------------------------------

/// One ranked query across tables, views, and namespaces.
///
/// Boosts (spelled inline in the SQL): +4.0 for an exact (case-insensitive)
/// asset-name match — well above any achievable `ts_rank`, so exact names
/// always sort first — and +1.0 for an asset-name prefix match.
///
/// See the module docs for query semantics, ranking, pagination, and how
/// `visibility` is enforced. Returns an empty page when the query contains
/// no indexable token.
// One statement, one function: the three UNION branches are deliberately
// spelled out next to each other so their column lists and filters cannot
// drift apart silently.
#[allow(clippy::too_many_lines)]
pub async fn search(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    request: &SearchRequest<'_>,
    visibility: &SearchVisibility,
) -> Result<SearchPage> {
    let tokens = query_tokens(request.text);
    if tokens.is_empty() {
        return Ok(SearchPage {
            hits: Vec::new(),
            next_page_token: None,
        });
    }
    let tsquery = build_tsquery(&tokens);
    let exact = request.text.trim().to_lowercase();
    let prefix = like_prefix_pattern(&exact);

    let (cursor_score, cursor_tiebreak) = match request.page_token {
        Some(token) => {
            let (score, tiebreak) = decode_page_token(token)?;
            (Some(score), Some(tiebreak))
        }
        None => (None, None),
    };

    let include = |kind: SearchAssetKind| request.kinds.is_none_or(|kinds| kinds.contains(&kind));

    // Named parameters, in bind order:
    //  $1 tsquery text          $2 exact name       $3 name-prefix LIKE
    //  $4 workspace id          $5 warehouse filter $6 namespace filter
    //  $7/$8/$9 include table/view/namespace
    //  $10 unrestricted         $11 READ warehouses $12 READ namespaces
    //  $13 READ tables          $14 READ views      $15 LIST_NS warehouses
    //  $16 cursor score         $17 cursor tiebreak $18 limit + 1
    let rows: Vec<HitRow> = sqlx::query_as(
        r"WITH q AS (SELECT to_tsquery('simple', $1) AS query),
        hits AS (
            SELECT 'table' AS kind, t.id AS id, t.name AS name,
                   n.levels AS levels, w.name AS warehouse,
                   (ts_rank(t.search_tsv, q.query)::float8
                     + CASE WHEN lower(t.name) = $2 THEN 4.0 ELSE 0.0 END
                     + CASE WHEN lower(t.name) LIKE $3 ESCAPE '\'
                            THEN 1.0 ELSE 0.0 END) AS score,
                   t.name || ' ' || array_to_string(n.levels, '.')
                     || coalesce(' ' || (t.properties ->> 'comment'), '')
                     || coalesce(' ' || t.schema_text, '') AS doc
            FROM tables t
            JOIN namespaces n ON n.id = t.namespace_id
            JOIN warehouses w ON w.id = n.warehouse_id
            CROSS JOIN q
            WHERE $7
              AND t.workspace_id = $4
              AND t.search_tsv @@ q.query
              AND ($5::text IS NULL OR n.warehouse_id = $5)
              AND ($6::text[] IS NULL
                   OR (cardinality(n.levels) >= cardinality($6::text[])
                       AND n.levels[1:cardinality($6::text[])] = $6::text[]))
              AND ($10 OR n.warehouse_id = ANY($11) OR t.id = ANY($13)
                   OR EXISTS (SELECT 1 FROM namespaces g
                              WHERE g.id = ANY($12)
                                AND g.warehouse_id = n.warehouse_id
                                AND cardinality(g.levels) <= cardinality(n.levels)
                                AND n.levels[1:cardinality(g.levels)] = g.levels))
            UNION ALL
            SELECT 'view', v.id, v.name, n.levels, w.name,
                   (ts_rank(v.search_tsv, q.query)::float8
                     + CASE WHEN lower(v.name) = $2 THEN 4.0 ELSE 0.0 END
                     + CASE WHEN lower(v.name) LIKE $3 ESCAPE '\'
                            THEN 1.0 ELSE 0.0 END),
                   v.name || ' ' || array_to_string(n.levels, '.')
                     || coalesce(' ' || (v.properties ->> 'comment'), '')
                     || coalesce(' ' || v.schema_text, '')
            FROM views v
            JOIN namespaces n ON n.id = v.namespace_id
            JOIN warehouses w ON w.id = n.warehouse_id
            CROSS JOIN q
            WHERE $8
              AND v.workspace_id = $4
              AND v.search_tsv @@ q.query
              AND ($5::text IS NULL OR n.warehouse_id = $5)
              AND ($6::text[] IS NULL
                   OR (cardinality(n.levels) >= cardinality($6::text[])
                       AND n.levels[1:cardinality($6::text[])] = $6::text[]))
              AND ($10 OR n.warehouse_id = ANY($11) OR v.id = ANY($14)
                   OR EXISTS (SELECT 1 FROM namespaces g
                              WHERE g.id = ANY($12)
                                AND g.warehouse_id = n.warehouse_id
                                AND cardinality(g.levels) <= cardinality(n.levels)
                                AND n.levels[1:cardinality(g.levels)] = g.levels))
            UNION ALL
            SELECT 'namespace', n.id, n.levels[cardinality(n.levels)],
                   n.levels, w.name,
                   (ts_rank(n.search_tsv, q.query)::float8
                     + CASE WHEN lower(n.levels[cardinality(n.levels)]) = $2
                            THEN 4.0 ELSE 0.0 END
                     + CASE WHEN lower(n.levels[cardinality(n.levels)]) LIKE $3 ESCAPE '\'
                            THEN 1.0 ELSE 0.0 END),
                   array_to_string(n.levels, '.')
                     || coalesce(' ' || (n.properties ->> 'comment'), '')
            FROM namespaces n
            JOIN warehouses w ON w.id = n.warehouse_id
            CROSS JOIN q
            WHERE $9
              AND n.workspace_id = $4
              AND n.search_tsv @@ q.query
              AND ($5::text IS NULL OR n.warehouse_id = $5)
              AND ($6::text[] IS NULL
                   OR (cardinality(n.levels) >= cardinality($6::text[])
                       AND n.levels[1:cardinality($6::text[])] = $6::text[]))
              AND ($10 OR n.warehouse_id = ANY($15))
        ),
        page AS (
            SELECT * FROM hits
            WHERE ($16::float8 IS NULL
                   OR score < $16
                   OR (score = $16 AND (kind || ':' || id) > $17))
            ORDER BY score DESC, kind || ':' || id
            LIMIT $18
        )
        SELECT p.kind, p.id, p.name, p.levels, p.warehouse, p.score,
               ts_headline('simple', p.doc, q.query,
                   'StartSel=**, StopSel=**, MaxWords=16, MinWords=4') AS snippet
        FROM page p CROSS JOIN q
        ORDER BY p.score DESC, p.kind || ':' || p.id",
    )
    .bind(&tsquery)
    .bind(&exact)
    .bind(&prefix)
    .bind(workspace_id.to_string())
    .bind(request.warehouse_id)
    .bind(request.namespace)
    .bind(include(SearchAssetKind::Table))
    .bind(include(SearchAssetKind::View))
    .bind(include(SearchAssetKind::Namespace))
    .bind(visibility.unrestricted)
    .bind(&visibility.read_warehouse_ids)
    .bind(&visibility.read_namespace_ids)
    .bind(&visibility.read_table_ids)
    .bind(&visibility.read_view_ids)
    .bind(&visibility.list_namespaces_warehouse_ids)
    .bind(cursor_score)
    .bind(cursor_tiebreak)
    .bind(request.limit + 1)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to run search query", e))?;

    let page_size = usize::try_from(request.limit).unwrap_or(0);
    let has_more = rows.len() > page_size;
    let mut hits = Vec::with_capacity(rows.len().min(page_size));
    for row in rows.into_iter().take(page_size) {
        let kind = SearchAssetKind::parse(&row.kind).ok_or_else(|| {
            MeridianError::internal_msg("search query returned an unknown asset kind")
        })?;
        hits.push(SearchHit {
            kind,
            id: row.id,
            name: row.name,
            namespace: row.levels,
            warehouse: row.warehouse,
            rank: row.score,
            snippet: row.snippet,
        });
    }
    let next_page_token = if has_more {
        hits.last()
            .map(|last| encode_page_token(last.rank, last.kind, &last.id))
    } else {
        None
    };

    Ok(SearchPage {
        hits,
        next_page_token,
    })
}

/// Raw row shape of the search statement.
#[derive(Debug, sqlx::FromRow)]
struct HitRow {
    kind: String,
    id: String,
    name: String,
    levels: Vec<String>,
    warehouse: String,
    score: f64,
    snippet: String,
}

// ---------------------------------------------------------------------------
// Schema text extraction (the write-through side)
// ---------------------------------------------------------------------------

/// Flattens a schema into the space-joined column names and docs indexed by
/// migration 0010 (`schema_text`), nested struct/list/map fields included.
#[must_use]
pub fn schema_search_text(schema: &Schema) -> String {
    fn walk(field_type: &Type, out: &mut Vec<String>) {
        match field_type {
            Type::Primitive(_) => {}
            Type::Struct(s) => {
                for field in &s.fields {
                    out.push(field.name.clone());
                    if let Some(doc) = &field.doc {
                        out.push(doc.clone());
                    }
                    walk(&field.field_type, out);
                }
            }
            Type::List(l) => walk(&l.element, out),
            Type::Map(m) => {
                walk(&m.key, out);
                walk(&m.value, out);
            }
        }
    }

    let mut out = Vec::new();
    for field in &schema.fields {
        out.push(field.name.clone());
        if let Some(doc) = &field.doc {
            out.push(doc.clone());
        }
        walk(&field.field_type, &mut out);
    }
    out.join(" ")
}

#[cfg(test)]
mod tests {
    use meridian_iceberg::spec::types::{ListType, PrimitiveType, StructField, StructType};

    use super::*;

    #[test]
    fn query_tokens_keep_identifiers_and_drop_syntax() {
        assert_eq!(query_tokens("customer_email"), vec!["customer_email"]);
        assert_eq!(
            query_tokens("Sales & Q4! (daily)"),
            vec!["sales", "q4", "daily"]
        );
        assert_eq!(query_tokens("a.b.c"), vec!["a", "b", "c"]);
        assert_eq!(query_tokens("___"), Vec::<String>::new());
        assert_eq!(query_tokens("  "), Vec::<String>::new());
        assert_eq!(query_tokens("_lead_trail_"), vec!["lead_trail"]);
    }

    #[test]
    fn tsquery_is_prefix_and_conjunctive() {
        assert_eq!(
            build_tsquery(&query_tokens("customer email")),
            "customer:* & email:*"
        );
    }

    #[test]
    fn like_pattern_escapes_metacharacters() {
        assert_eq!(like_prefix_pattern("a_b%c\\d"), "a\\_b\\%c\\\\d%");
    }

    #[test]
    fn page_tokens_round_trip_exactly() {
        let score = 4.000_000_119_209_29_f64; // not representable in decimal
        let token = encode_page_token(score, SearchAssetKind::Table, "01ABC");
        let (parsed, tiebreak) = decode_page_token(&token).expect("valid token");
        assert_eq!(parsed.to_bits(), score.to_bits(), "bit-exact round trip");
        assert_eq!(tiebreak, "table:01ABC");
    }

    #[test]
    fn malformed_page_tokens_are_validation_errors() {
        for token in ["", "nope", "zz.table.01A", "10.rocket.01A", "10.table."] {
            assert!(
                decode_page_token(token).is_err(),
                "token {token:?} must be rejected"
            );
        }
    }

    #[test]
    fn schema_search_text_includes_nested_fields_and_docs() {
        let schema = Schema::new(vec![
            StructField::required(1, "id", Type::primitive(PrimitiveType::Long)),
            StructField {
                doc: Some("primary email".to_owned()),
                ..StructField::optional(2, "customer_email", Type::primitive(PrimitiveType::String))
            },
            StructField::optional(
                3,
                "address",
                Type::Struct(StructType::new(vec![StructField::optional(
                    4,
                    "zip",
                    Type::primitive(PrimitiveType::String),
                )])),
            ),
            StructField::optional(
                5,
                "tags",
                Type::List(ListType::new(
                    6,
                    Type::Struct(StructType::new(vec![StructField::optional(
                        7,
                        "tag_name",
                        Type::primitive(PrimitiveType::String),
                    )])),
                    false,
                )),
            ),
        ]);
        assert_eq!(
            schema_search_text(&schema),
            "id customer_email primary email address zip tags tag_name"
        );
    }
}
