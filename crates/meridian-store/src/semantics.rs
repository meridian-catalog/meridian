//! Semantics-layer persistence (Pillar G, G-F2/G-F3/G-F4): metrics & semantic
//! models, the business glossary, and certified data products.
//!
//! Owns the five tables migration 0021 introduces:
//!
//! - `metrics`: first-class semantic objects (measure + dimensions + filters +
//!   grain + owner + certification). The definition is engine-neutral; the
//!   server compiles it to a chosen engine's SQL deterministically via the
//!   transpilation sidecar (this module holds definitions only, never SQL
//!   generation).
//! - `glossary_terms` + `glossary_links`: stewarded business vocabulary and its
//!   many-to-many links to catalog assets (tables, views, metrics).
//! - `data_products` + `data_product_members`: named certified bundles and
//!   their membership rows — the unit of consumption for humans and agents.
//!
//! # What this module is (and is not)
//!
//! Pure persistence. It does **not** compile metrics to SQL (the sidecar +
//! server route do), resolve governance, or serve HTTP. Every mutation carries
//! its audit row and outbox event on the *same* transaction — the invariant the
//! whole codebase holds: no mutation without its audit row. Reads are plain
//! pooled queries.
//!
//! Certification (`draft` | `certified` | `deprecated`) is a governance signal
//! surfaced verbatim; nothing here asserts a metric is *correct*, exactly as the
//! transpile status machine never asserts a translation is correct beyond what
//! validation proved.

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::types::Json;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// The certification lifecycle of a metric, glossary term, or data product.
///
/// A pure governance signal. `certified` never claims correctness — a steward
/// asserts it, and Meridian surfaces it verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Certification {
    /// Authored, not yet blessed.
    Draft,
    /// A steward has certified it.
    Certified,
    /// Retained for reference but no longer endorsed.
    Deprecated,
}

impl Certification {
    /// The wire/DB string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Certified => "certified",
            Self::Deprecated => "deprecated",
        }
    }

    /// Parses a wire/DB string, if recognized.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "draft" => Some(Self::Draft),
            "certified" => Some(Self::Certified),
            "deprecated" => Some(Self::Deprecated),
            _ => None,
        }
    }
}

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// True when the error is a Postgres foreign-key violation.
fn is_fk_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
}

// ===========================================================================
// Metrics (G-F2)
// ===========================================================================

/// A persisted metric definition.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MetricRecord {
    /// ULID of the metric.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Machine name, unique per workspace (case-insensitively).
    pub name: String,
    /// Optional human label.
    pub display_name: Option<String>,
    /// Source table/view identifier (dotted).
    pub source: String,
    /// Measure expression in the canonical dialect (e.g. `SUM(amount)`).
    pub expression: String,
    /// The dialect `expression`/`dimensions`/`filters` are authored in.
    pub dialect: String,
    /// Default dimensions (group-by columns/expressions).
    pub dimensions: Json<Vec<String>>,
    /// Default filters (boolean SQL fragments, `AND`-ed).
    pub filters: Json<Vec<String>>,
    /// Grain description.
    pub grain: Option<String>,
    /// Description / documentation (markdown).
    pub description: Option<String>,
    /// Accountable owner (audit string).
    pub owner: Option<String>,
    /// Certification status (`draft` | `certified` | `deprecated`).
    pub certification: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

const METRIC_COLUMNS: &str = "id, workspace_id, name, display_name, source, expression, dialect, \
     dimensions, filters, grain, description, owner, certification, created_at, updated_at";

/// A new metric to insert (or the full desired state on upsert-by-name).
#[derive(Debug, Clone)]
pub struct NewMetric<'a> {
    /// Machine name (unique per workspace, case-insensitively).
    pub name: &'a str,
    /// Optional human label.
    pub display_name: Option<&'a str>,
    /// Source table/view identifier.
    pub source: &'a str,
    /// Measure expression in the canonical dialect.
    pub expression: &'a str,
    /// Canonical dialect (defaults applied by the caller).
    pub dialect: &'a str,
    /// Default dimensions.
    pub dimensions: &'a [String],
    /// Default filters.
    pub filters: &'a [String],
    /// Grain description.
    pub grain: Option<&'a str>,
    /// Description.
    pub description: Option<&'a str>,
    /// Owner (audit string).
    pub owner: Option<&'a str>,
    /// Certification status.
    pub certification: Certification,
}

/// A partial metric update: `Some` fields are applied, `None` fields untouched.
/// `certification` overrides the status when set.
#[derive(Debug, Clone, Default)]
pub struct MetricPatch {
    /// New human label.
    pub display_name: Option<String>,
    /// New source identifier.
    pub source: Option<String>,
    /// New measure expression.
    pub expression: Option<String>,
    /// New canonical dialect.
    pub dialect: Option<String>,
    /// New default dimensions.
    pub dimensions: Option<Vec<String>>,
    /// New default filters.
    pub filters: Option<Vec<String>>,
    /// New grain.
    pub grain: Option<String>,
    /// New description.
    pub description: Option<String>,
    /// New owner.
    pub owner: Option<String>,
    /// New certification status.
    pub certification: Option<Certification>,
}

/// Loads a metric by id.
pub async fn get_metric(pool: &PgPool, id: &str) -> Result<Option<MetricRecord>> {
    sqlx::query_as(&format!(
        "SELECT {METRIC_COLUMNS} FROM metrics WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load metric", e))
}

/// Loads a metric by name within a workspace (case-insensitive).
pub async fn get_metric_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
) -> Result<Option<MetricRecord>> {
    sqlx::query_as(&format!(
        "SELECT {METRIC_COLUMNS} FROM metrics WHERE workspace_id = $1 AND lower(name) = lower($2)"
    ))
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load metric by name", e))
}

/// Lists a workspace's metrics in stable id (creation) order, keyset-paginated.
pub async fn list_metrics(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    after_id: Option<&str>,
    limit: Option<i64>,
) -> Result<Vec<MetricRecord>> {
    sqlx::query_as(&format!(
        "SELECT {METRIC_COLUMNS} FROM metrics
         WHERE workspace_id = $1 AND ($2::text IS NULL OR id > $2)
         ORDER BY id LIMIT $3"
    ))
    .bind(workspace_id.to_string())
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list metrics", e))
}

/// Inserts a metric, with its audit row and outbox event, atomically.
///
/// Returns [`MeridianError::Conflict`] when the name is taken in the workspace.
pub async fn create_metric(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    metric: NewMetric<'_>,
    principal: &str,
) -> Result<MetricRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin metric create", e))?;

    let id = Ulid::new().to_string();
    let record: MetricRecord = sqlx::query_as(&format!(
        "INSERT INTO metrics
             (id, workspace_id, name, display_name, source, expression, dialect,
              dimensions, filters, grain, description, owner, certification)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         RETURNING {METRIC_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(metric.name)
    .bind(metric.display_name)
    .bind(metric.source)
    .bind(metric.expression)
    .bind(metric.dialect)
    .bind(Json(metric.dimensions))
    .bind(Json(metric.filters))
    .bind(metric.grain)
    .bind(metric.description)
    .bind(metric.owner)
    .bind(metric.certification.as_str())
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!(
                "a metric named {:?} already exists in this workspace",
                metric.name
            ))
        } else {
            map_sqlx_error("failed to insert metric", e)
        }
    })?;

    let payload = json!({
        "name": record.name,
        "source": record.source,
        "certification": record.certification,
    });
    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "metric.create",
        &format!("metric:{id}"),
        "metric.created",
        payload,
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit metric create", e))?;
    Ok(record)
}

/// Applies a partial update to a metric, with audit + outbox, atomically.
///
/// Returns [`MeridianError::NotFound`] when no such metric exists.
pub async fn update_metric(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    patch: MetricPatch,
    principal: &str,
) -> Result<MetricRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin metric update", e))?;

    // COALESCE keeps the existing value where the patch field is NULL. JSON
    // fields use a sentinel NULL bind (the arrays are never NULL columns).
    let record: Option<MetricRecord> = sqlx::query_as(&format!(
        "UPDATE metrics SET
             display_name  = COALESCE($3, display_name),
             source        = COALESCE($4, source),
             expression    = COALESCE($5, expression),
             dialect       = COALESCE($6, dialect),
             dimensions    = COALESCE($7, dimensions),
             filters       = COALESCE($8, filters),
             grain         = COALESCE($9, grain),
             description   = COALESCE($10, description),
             owner         = COALESCE($11, owner),
             certification = COALESCE($12, certification),
             updated_at    = now()
         WHERE id = $1 AND workspace_id = $2
         RETURNING {METRIC_COLUMNS}"
    ))
    .bind(id)
    .bind(workspace_id.to_string())
    .bind(patch.display_name)
    .bind(patch.source)
    .bind(patch.expression)
    .bind(patch.dialect)
    .bind(patch.dimensions.map(Json))
    .bind(patch.filters.map(Json))
    .bind(patch.grain)
    .bind(patch.description)
    .bind(patch.owner)
    .bind(patch.certification.map(Certification::as_str))
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to update metric", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "metric {id:?} does not exist"
        )));
    };

    let payload = json!({
        "name": record.name,
        "certification": record.certification,
    });
    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "metric.update",
        &format!("metric:{id}"),
        "metric.updated",
        payload,
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit metric update", e))?;
    Ok(record)
}

/// Deletes a metric, with audit + outbox, atomically. Returns the dropped row.
pub async fn delete_metric(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<MetricRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin metric delete", e))?;

    let record: Option<MetricRecord> = sqlx::query_as(&format!(
        "DELETE FROM metrics WHERE id = $1 AND workspace_id = $2 RETURNING {METRIC_COLUMNS}"
    ))
    .bind(id)
    .bind(workspace_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete metric", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "metric {id:?} does not exist"
        )));
    };

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "metric.delete",
        &format!("metric:{id}"),
        "metric.deleted",
        json!({ "name": record.name }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit metric delete", e))?;
    Ok(record)
}

// ===========================================================================
// Glossary (G-F3)
// ===========================================================================

/// A persisted glossary term.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GlossaryTermRecord {
    /// ULID of the term.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Term name, unique per workspace (case-insensitively).
    pub name: String,
    /// Definition (markdown).
    pub definition: String,
    /// Accountable steward (audit string).
    pub steward: Option<String>,
    /// Certification status.
    pub certification: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

const TERM_COLUMNS: &str =
    "id, workspace_id, name, definition, steward, certification, created_at, updated_at";

/// A new glossary term to insert.
#[derive(Debug, Clone)]
pub struct NewGlossaryTerm<'a> {
    /// Term name.
    pub name: &'a str,
    /// Definition.
    pub definition: &'a str,
    /// Steward (audit string).
    pub steward: Option<&'a str>,
    /// Certification status.
    pub certification: Certification,
}

/// A partial glossary-term update.
#[derive(Debug, Clone, Default)]
pub struct GlossaryTermPatch {
    /// New definition.
    pub definition: Option<String>,
    /// New steward.
    pub steward: Option<String>,
    /// New certification status.
    pub certification: Option<Certification>,
}

/// An asset a glossary term is linked to (`kind` + stable `ref`).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GlossaryLinkRecord {
    /// ULID of the link.
    pub id: String,
    /// The linked term id.
    pub term_id: String,
    /// Asset kind (`table` | `view` | `metric`).
    pub asset_kind: String,
    /// Stable asset reference (e.g. `table:<id>`).
    pub asset_ref: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

/// Loads a glossary term by id.
pub async fn get_term(pool: &PgPool, id: &str) -> Result<Option<GlossaryTermRecord>> {
    sqlx::query_as(&format!(
        "SELECT {TERM_COLUMNS} FROM glossary_terms WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load glossary term", e))
}

/// Loads a glossary term by name within a workspace (case-insensitive).
pub async fn get_term_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
) -> Result<Option<GlossaryTermRecord>> {
    sqlx::query_as(&format!(
        "SELECT {TERM_COLUMNS} FROM glossary_terms
         WHERE workspace_id = $1 AND lower(name) = lower($2)"
    ))
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load glossary term by name", e))
}

/// Lists a workspace's glossary terms in stable id order, keyset-paginated.
pub async fn list_terms(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    after_id: Option<&str>,
    limit: Option<i64>,
) -> Result<Vec<GlossaryTermRecord>> {
    sqlx::query_as(&format!(
        "SELECT {TERM_COLUMNS} FROM glossary_terms
         WHERE workspace_id = $1 AND ($2::text IS NULL OR id > $2)
         ORDER BY id LIMIT $3"
    ))
    .bind(workspace_id.to_string())
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list glossary terms", e))
}

/// Lists the links of a glossary term (its linked assets).
pub async fn list_term_links(pool: &PgPool, term_id: &str) -> Result<Vec<GlossaryLinkRecord>> {
    sqlx::query_as(
        "SELECT id, term_id, asset_kind, asset_ref, created_at
         FROM glossary_links WHERE term_id = $1 ORDER BY id",
    )
    .bind(term_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list glossary links", e))
}

/// Lists the glossary terms linked to a given asset (the reverse lookup, for
/// asset pages).
pub async fn list_terms_for_asset(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    asset_kind: &str,
    asset_ref: &str,
) -> Result<Vec<GlossaryTermRecord>> {
    sqlx::query_as(&format!(
        "SELECT {} FROM glossary_terms t
         JOIN glossary_links l ON l.term_id = t.id
         WHERE t.workspace_id = $1 AND l.asset_kind = $2 AND l.asset_ref = $3
         ORDER BY t.id",
        TERM_COLUMNS
            .split(", ")
            .map(|c| format!("t.{c}"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
    .bind(workspace_id.to_string())
    .bind(asset_kind)
    .bind(asset_ref)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list terms for asset", e))
}

/// Inserts a glossary term, with audit + outbox, atomically.
pub async fn create_term(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    term: NewGlossaryTerm<'_>,
    principal: &str,
) -> Result<GlossaryTermRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin term create", e))?;

    let id = Ulid::new().to_string();
    let record: GlossaryTermRecord = sqlx::query_as(&format!(
        "INSERT INTO glossary_terms (id, workspace_id, name, definition, steward, certification)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING {TERM_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(term.name)
    .bind(term.definition)
    .bind(term.steward)
    .bind(term.certification.as_str())
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!(
                "a glossary term named {:?} already exists in this workspace",
                term.name
            ))
        } else {
            map_sqlx_error("failed to insert glossary term", e)
        }
    })?;

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "glossary.create_term",
        &format!("glossary_term:{id}"),
        "glossary.term_created",
        json!({ "name": record.name, "certification": record.certification }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit term create", e))?;
    Ok(record)
}

/// Applies a partial update to a glossary term, with audit + outbox.
pub async fn update_term(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    patch: GlossaryTermPatch,
    principal: &str,
) -> Result<GlossaryTermRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin term update", e))?;

    let record: Option<GlossaryTermRecord> = sqlx::query_as(&format!(
        "UPDATE glossary_terms SET
             definition    = COALESCE($3, definition),
             steward       = COALESCE($4, steward),
             certification = COALESCE($5, certification),
             updated_at    = now()
         WHERE id = $1 AND workspace_id = $2
         RETURNING {TERM_COLUMNS}"
    ))
    .bind(id)
    .bind(workspace_id.to_string())
    .bind(patch.definition)
    .bind(patch.steward)
    .bind(patch.certification.map(Certification::as_str))
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to update glossary term", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "glossary term {id:?} does not exist"
        )));
    };

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "glossary.update_term",
        &format!("glossary_term:{id}"),
        "glossary.term_updated",
        json!({ "name": record.name, "certification": record.certification }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit term update", e))?;
    Ok(record)
}

/// Deletes a glossary term (and its links, by cascade), with audit + outbox.
pub async fn delete_term(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<GlossaryTermRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin term delete", e))?;

    let record: Option<GlossaryTermRecord> = sqlx::query_as(&format!(
        "DELETE FROM glossary_terms WHERE id = $1 AND workspace_id = $2 RETURNING {TERM_COLUMNS}"
    ))
    .bind(id)
    .bind(workspace_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete glossary term", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "glossary term {id:?} does not exist"
        )));
    };

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "glossary.delete_term",
        &format!("glossary_term:{id}"),
        "glossary.term_deleted",
        json!({ "name": record.name }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit term delete", e))?;
    Ok(record)
}

/// Links a glossary term to an asset (idempotent), with audit + outbox.
///
/// Returns the existing link when the (term, asset) pair is already linked, so
/// a re-link is a no-op success. Returns [`MeridianError::NotFound`] when the
/// term does not exist.
pub async fn link_term(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    term_id: &str,
    asset_kind: &str,
    asset_ref: &str,
    principal: &str,
) -> Result<GlossaryLinkRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin glossary link", e))?;

    let id = Ulid::new().to_string();
    // ON CONFLICT DO NOTHING makes the link idempotent; the RETURNING is empty
    // on a conflict, so we fall back to a lookup of the pre-existing row.
    let inserted: Option<GlossaryLinkRecord> = sqlx::query_as(
        "INSERT INTO glossary_links (id, workspace_id, term_id, asset_kind, asset_ref)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (term_id, asset_kind, asset_ref) DO NOTHING
         RETURNING id, term_id, asset_kind, asset_ref, created_at",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(term_id)
    .bind(asset_kind)
    .bind(asset_ref)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| {
        if is_fk_violation(&e) {
            MeridianError::NotFound(format!("glossary term {term_id:?} does not exist"))
        } else {
            map_sqlx_error("failed to link glossary term", e)
        }
    })?;

    let (record, newly_linked) = if let Some(record) = inserted {
        (record, true)
    } else {
        // The pair already existed. Load it so the caller gets the id.
        let existing: GlossaryLinkRecord = sqlx::query_as(
            "SELECT id, term_id, asset_kind, asset_ref, created_at
             FROM glossary_links WHERE term_id = $1 AND asset_kind = $2 AND asset_ref = $3",
        )
        .bind(term_id)
        .bind(asset_kind)
        .bind(asset_ref)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to load existing glossary link", e))?;
        (existing, false)
    };

    // Audit only a real state change (a no-op re-link writes nothing).
    if newly_linked {
        write_audit_and_event(
            &mut tx,
            workspace_id,
            principal,
            "glossary.link",
            &format!("glossary_term:{term_id}"),
            "glossary.linked",
            json!({ "asset_kind": asset_kind, "asset_ref": asset_ref }),
        )
        .await?;
    }

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit glossary link", e))?;
    Ok(record)
}

/// Removes a glossary link by id, with audit + outbox. Returns the removed row.
pub async fn unlink_term(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    link_id: &str,
    principal: &str,
) -> Result<GlossaryLinkRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin glossary unlink", e))?;

    let record: Option<GlossaryLinkRecord> = sqlx::query_as(
        "DELETE FROM glossary_links WHERE id = $1 AND workspace_id = $2
         RETURNING id, term_id, asset_kind, asset_ref, created_at",
    )
    .bind(link_id)
    .bind(workspace_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete glossary link", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "glossary link {link_id:?} does not exist"
        )));
    };

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "glossary.unlink",
        &format!("glossary_term:{}", record.term_id),
        "glossary.unlinked",
        json!({ "asset_kind": record.asset_kind, "asset_ref": record.asset_ref }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit glossary unlink", e))?;
    Ok(record)
}

// ===========================================================================
// Data products (G-F4)
// ===========================================================================

/// A persisted data product (a certified bundle).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DataProductRecord {
    /// ULID of the product.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Machine name, unique per workspace (case-insensitively).
    pub name: String,
    /// Optional human label.
    pub display_name: Option<String>,
    /// Description (markdown).
    pub description: Option<String>,
    /// Accountable owner (audit string).
    pub owner: Option<String>,
    /// Free-text SLA statement.
    pub sla: Option<String>,
    /// Certification status.
    pub certification: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

const PRODUCT_COLUMNS: &str = "id, workspace_id, name, display_name, description, owner, sla, \
     certification, created_at, updated_at";

/// One member of a data product (`kind` + stable `ref`).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DataProductMemberRecord {
    /// ULID of the membership row.
    pub id: String,
    /// The owning product.
    pub product_id: String,
    /// Member kind (`table` | `view` | `metric` | `glossary_term` | `contract`).
    pub member_kind: String,
    /// Stable member reference.
    pub member_ref: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

/// A new data product to insert.
#[derive(Debug, Clone)]
pub struct NewDataProduct<'a> {
    /// Machine name.
    pub name: &'a str,
    /// Optional human label.
    pub display_name: Option<&'a str>,
    /// Description.
    pub description: Option<&'a str>,
    /// Owner (audit string).
    pub owner: Option<&'a str>,
    /// SLA statement.
    pub sla: Option<&'a str>,
    /// Certification status.
    pub certification: Certification,
}

/// A partial data-product update.
#[derive(Debug, Clone, Default)]
pub struct DataProductPatch {
    /// New human label.
    pub display_name: Option<String>,
    /// New description.
    pub description: Option<String>,
    /// New owner.
    pub owner: Option<String>,
    /// New SLA statement.
    pub sla: Option<String>,
    /// New certification status.
    pub certification: Option<Certification>,
}

/// Loads a data product by id.
pub async fn get_product(pool: &PgPool, id: &str) -> Result<Option<DataProductRecord>> {
    sqlx::query_as(&format!(
        "SELECT {PRODUCT_COLUMNS} FROM data_products WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load data product", e))
}

/// Loads a data product by name within a workspace (case-insensitive).
pub async fn get_product_by_name(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    name: &str,
) -> Result<Option<DataProductRecord>> {
    sqlx::query_as(&format!(
        "SELECT {PRODUCT_COLUMNS} FROM data_products
         WHERE workspace_id = $1 AND lower(name) = lower($2)"
    ))
    .bind(workspace_id.to_string())
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load data product by name", e))
}

/// Lists a workspace's data products in stable id order, keyset-paginated.
pub async fn list_products(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    after_id: Option<&str>,
    limit: Option<i64>,
) -> Result<Vec<DataProductRecord>> {
    sqlx::query_as(&format!(
        "SELECT {PRODUCT_COLUMNS} FROM data_products
         WHERE workspace_id = $1 AND ($2::text IS NULL OR id > $2)
         ORDER BY id LIMIT $3"
    ))
    .bind(workspace_id.to_string())
    .bind(after_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list data products", e))
}

/// Lists the members of a data product in stable id order.
pub async fn list_product_members(
    pool: &PgPool,
    product_id: &str,
) -> Result<Vec<DataProductMemberRecord>> {
    sqlx::query_as(
        "SELECT id, product_id, member_kind, member_ref, created_at
         FROM data_product_members WHERE product_id = $1 ORDER BY id",
    )
    .bind(product_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list product members", e))
}

/// Inserts a data product, with audit + outbox, atomically.
pub async fn create_product(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    product: NewDataProduct<'_>,
    principal: &str,
) -> Result<DataProductRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin product create", e))?;

    let id = Ulid::new().to_string();
    let record: DataProductRecord = sqlx::query_as(&format!(
        "INSERT INTO data_products
             (id, workspace_id, name, display_name, description, owner, sla, certification)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING {PRODUCT_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(product.name)
    .bind(product.display_name)
    .bind(product.description)
    .bind(product.owner)
    .bind(product.sla)
    .bind(product.certification.as_str())
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!(
                "a data product named {:?} already exists in this workspace",
                product.name
            ))
        } else {
            map_sqlx_error("failed to insert data product", e)
        }
    })?;

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "product.create",
        &format!("data_product:{id}"),
        "product.created",
        json!({ "name": record.name, "certification": record.certification }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit product create", e))?;
    Ok(record)
}

/// Applies a partial update to a data product, with audit + outbox.
pub async fn update_product(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    patch: DataProductPatch,
    principal: &str,
) -> Result<DataProductRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin product update", e))?;

    let record: Option<DataProductRecord> = sqlx::query_as(&format!(
        "UPDATE data_products SET
             display_name  = COALESCE($3, display_name),
             description   = COALESCE($4, description),
             owner         = COALESCE($5, owner),
             sla           = COALESCE($6, sla),
             certification = COALESCE($7, certification),
             updated_at    = now()
         WHERE id = $1 AND workspace_id = $2
         RETURNING {PRODUCT_COLUMNS}"
    ))
    .bind(id)
    .bind(workspace_id.to_string())
    .bind(patch.display_name)
    .bind(patch.description)
    .bind(patch.owner)
    .bind(patch.sla)
    .bind(patch.certification.map(Certification::as_str))
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to update data product", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "data product {id:?} does not exist"
        )));
    };

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "product.update",
        &format!("data_product:{id}"),
        "product.updated",
        json!({ "name": record.name, "certification": record.certification }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit product update", e))?;
    Ok(record)
}

/// Deletes a data product (and its membership rows, by cascade), with audit +
/// outbox. Returns the dropped row.
pub async fn delete_product(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<DataProductRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin product delete", e))?;

    let record: Option<DataProductRecord> = sqlx::query_as(&format!(
        "DELETE FROM data_products WHERE id = $1 AND workspace_id = $2 RETURNING {PRODUCT_COLUMNS}"
    ))
    .bind(id)
    .bind(workspace_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete data product", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "data product {id:?} does not exist"
        )));
    };

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "product.delete",
        &format!("data_product:{id}"),
        "product.deleted",
        json!({ "name": record.name }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit product delete", e))?;
    Ok(record)
}

/// Adds a member to a data product (idempotent), with audit + outbox.
///
/// Returns the existing membership row when the (product, member) pair already
/// exists. Returns [`MeridianError::NotFound`] when the product does not exist.
pub async fn add_product_member(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    product_id: &str,
    member_kind: &str,
    member_ref: &str,
    principal: &str,
) -> Result<DataProductMemberRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin add product member", e))?;

    let id = Ulid::new().to_string();
    let inserted: Option<DataProductMemberRecord> = sqlx::query_as(
        "INSERT INTO data_product_members (id, workspace_id, product_id, member_kind, member_ref)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (product_id, member_kind, member_ref) DO NOTHING
         RETURNING id, product_id, member_kind, member_ref, created_at",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(product_id)
    .bind(member_kind)
    .bind(member_ref)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| {
        if is_fk_violation(&e) {
            MeridianError::NotFound(format!("data product {product_id:?} does not exist"))
        } else {
            map_sqlx_error("failed to add product member", e)
        }
    })?;

    let (record, newly_added) = if let Some(record) = inserted {
        (record, true)
    } else {
        let existing: DataProductMemberRecord = sqlx::query_as(
            "SELECT id, product_id, member_kind, member_ref, created_at
             FROM data_product_members
             WHERE product_id = $1 AND member_kind = $2 AND member_ref = $3",
        )
        .bind(product_id)
        .bind(member_kind)
        .bind(member_ref)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to load existing product member", e))?;
        (existing, false)
    };

    if newly_added {
        write_audit_and_event(
            &mut tx,
            workspace_id,
            principal,
            "product.add_member",
            &format!("data_product:{product_id}"),
            "product.member_added",
            json!({ "member_kind": member_kind, "member_ref": member_ref }),
        )
        .await?;
    }

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit add product member", e))?;
    Ok(record)
}

/// Removes a member from a data product by membership-row id, with audit +
/// outbox. Returns the removed row.
pub async fn remove_product_member(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    member_id: &str,
    principal: &str,
) -> Result<DataProductMemberRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin remove product member", e))?;

    let record: Option<DataProductMemberRecord> = sqlx::query_as(
        "DELETE FROM data_product_members WHERE id = $1 AND workspace_id = $2
         RETURNING id, product_id, member_kind, member_ref, created_at",
    )
    .bind(member_id)
    .bind(workspace_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to remove product member", e))?;

    let Some(record) = record else {
        return Err(MeridianError::NotFound(format!(
            "data product member {member_id:?} does not exist"
        )));
    };

    write_audit_and_event(
        &mut tx,
        workspace_id,
        principal,
        "product.remove_member",
        &format!("data_product:{}", record.product_id),
        "product.member_removed",
        json!({ "member_kind": record.member_kind, "member_ref": record.member_ref }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit remove product member", e))?;
    Ok(record)
}

// ===========================================================================
// Universal-view translation cache (G-F1, §8.5)
// ===========================================================================

/// A cached universal-view translation.
///
/// The durable side-store for on-demand view transpilations: keyed by the view,
/// the target dialect, and a hash of the canonical source SQL, so a changed view
/// definition never reuses a stale translation. This is a *derived cache*, not a
/// record of user intent, so it carries no audit row — the view-definition
/// mutations it derives from are audited on the view path.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ViewRepresentationCacheRecord {
    /// ULID of the cache row.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// The view this translation belongs to.
    pub view_id: String,
    /// The dialect translated *into* (lowercased).
    pub target_dialect: String,
    /// The dialect the source was authored in (lowercased).
    pub source_dialect: String,
    /// sha256 (hex) of the canonical source SQL that was translated.
    pub source_sql_hash: String,
    /// The translated SQL; `None` when `status` is `unsupported`.
    pub translated_sql: Option<String>,
    /// The honest transpile status (`verified` | `best_effort` | `unsupported`).
    pub status: String,
    /// The sidecar diagnostics (JSONB array), surfaced with the translation.
    pub diagnostics: Json<Vec<Value>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// Looks up a cached translation for a view + target dialect + source hash.
///
/// Returns `None` when nothing is cached for this exact definition, so the
/// caller transpiles fresh. An `unsupported` entry *is* returned (the absence of
/// a good translation is itself cached, to avoid re-hitting the sidecar for a
/// construct it already could not handle).
pub async fn get_cached_translation(
    pool: &PgPool,
    view_id: &str,
    target_dialect: &str,
    source_sql_hash: &str,
) -> Result<Option<ViewRepresentationCacheRecord>> {
    sqlx::query_as(
        "SELECT id, workspace_id, view_id, target_dialect, source_dialect, source_sql_hash,
                translated_sql, status, diagnostics, created_at, updated_at
         FROM view_representation_cache
         WHERE view_id = $1 AND target_dialect = lower($2) AND source_sql_hash = $3",
    )
    .bind(view_id)
    .bind(target_dialect)
    .bind(source_sql_hash)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to read view translation cache", e))
}

/// Upserts a cached translation (idempotent on the natural key).
///
/// A re-translate of the same definition overwrites the prior entry (its status
/// and diagnostics may have changed, e.g. after a sidecar upgrade). Dialects are
/// lowercased for a case-insensitive cache. No audit row: this is a derived
/// cache, not a user mutation.
#[allow(clippy::too_many_arguments)] // a cache key + payload, not a config bag
pub async fn upsert_cached_translation(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    view_id: &str,
    target_dialect: &str,
    source_dialect: &str,
    source_sql_hash: &str,
    translated_sql: Option<&str>,
    status: &str,
    diagnostics: &[Value],
) -> Result<ViewRepresentationCacheRecord> {
    let id = Ulid::new().to_string();
    sqlx::query_as(
        "INSERT INTO view_representation_cache
             (id, workspace_id, view_id, target_dialect, source_dialect,
              source_sql_hash, translated_sql, status, diagnostics)
         VALUES ($1, $2, $3, lower($4), lower($5), $6, $7, $8, $9)
         ON CONFLICT (view_id, target_dialect, source_sql_hash) DO UPDATE SET
             source_dialect = EXCLUDED.source_dialect,
             translated_sql = EXCLUDED.translated_sql,
             status         = EXCLUDED.status,
             diagnostics    = EXCLUDED.diagnostics,
             updated_at     = now()
         RETURNING id, workspace_id, view_id, target_dialect, source_dialect, source_sql_hash,
                   translated_sql, status, diagnostics, created_at, updated_at",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(view_id)
    .bind(target_dialect)
    .bind(source_dialect)
    .bind(source_sql_hash)
    .bind(translated_sql)
    .bind(status)
    .bind(Json(diagnostics))
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to cache view translation", e))
}

/// Lists all cached translations for a view (for the console's per-view dialect
/// status panel).
pub async fn list_cached_translations(
    pool: &PgPool,
    view_id: &str,
) -> Result<Vec<ViewRepresentationCacheRecord>> {
    sqlx::query_as(
        "SELECT id, workspace_id, view_id, target_dialect, source_dialect, source_sql_hash,
                translated_sql, status, diagnostics, created_at, updated_at
         FROM view_representation_cache
         WHERE view_id = $1 ORDER BY target_dialect",
    )
    .bind(view_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list view translation cache", e))
}

// ===========================================================================
// Shared: audit + outbox on the mutation transaction (invariant I6)
// ===========================================================================

/// Writes the outbox event and the audit row for a mutation on the *same*
/// transaction as the state change. Every mutation in this module routes its
/// audit+event through here so the invariant is enforced in one place.
async fn write_audit_and_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace_id: WorkspaceId,
    principal: &str,
    action: &str,
    resource: &str,
    event_type: &str,
    payload: Value,
) -> Result<()> {
    outbox::enqueue(
        &mut **tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: resource.to_owned(),
            event_type: event_type.to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource: resource.to_owned(),
            details: payload,
        },
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certification_round_trips() {
        for c in [
            Certification::Draft,
            Certification::Certified,
            Certification::Deprecated,
        ] {
            assert_eq!(Certification::parse(c.as_str()), Some(c));
        }
        assert_eq!(Certification::parse("bogus"), None);
    }
}
