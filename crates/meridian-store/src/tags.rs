//! Governance **tags** and **tag assignments** (Pillar D / D-F1, D-F3):
//! persistence for the classification unit and its placement on securables.
//!
//! A [`Tag`] is a workspace-scoped `key:value` pair (e.g. `pii:email`) — the
//! pivot the whole ABAC model turns on: policies bind to tags
//! ([`crate::policy`]), and tags carry the sensitivity a catalog knows about
//! a table or a column. A [`TagAssignment`] places a tag on a securable — a
//! table, a namespace, or a single **column** of a table — and records its
//! provenance (`manual` vs `classifier`), a classifier confidence, and an
//! approval bit (a classifier's suggestion is *not in force* until approved).
//!
//! # What this module is and is not
//!
//! This is the **persistence** vocabulary. It does not decide or enforce; it
//! records definitions and answers the resolution question the enforcement
//! layer asks: *what tags does this table carry, and what tags does each of
//! its columns carry* ([`resolve_table_tags`]). The mapping from those tags
//! onto `meridian_authz` inputs (an `AuthzResource` and its `ResolvedColumn`s)
//! lives in the server's governance module, against these public types — the
//! store never depends on `meridian-authz`.
//!
//! # Column-level assignments
//!
//! When `securable_type = column`, `securable_id` is the owning **table's** id
//! and `column_name` names the column (a CHECK in migration 0016 enforces the
//! pairing). Column tags are first-class because column masks and
//! column-scoped policies need per-column classification.
//!
//! Every mutation writes its audit row and outbox event on the same
//! transaction as the state change (the governance audit trail is the product,
//! D-F2).

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::json;
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// The kind of securable a tag can be assigned to.
///
/// `Column` is not a distinct securable in RBAC (grants stop at table/view);
/// it matters here because a tag on one column drives a column mask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagSecurable {
    /// A whole table.
    Table,
    /// A whole namespace (applies to everything it contains, resolved in
    /// code by the policy binder — this module records the raw assignment).
    Namespace,
    /// A single column of a table (`securable_id` is the table id;
    /// `column_name` names the column).
    Column,
}

impl TagSecurable {
    /// The database/wire rendering (matches the 0016 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Namespace => "namespace",
            Self::Column => "column",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "table" => Some(Self::Table),
            "namespace" => Some(Self::Namespace),
            "column" => Some(Self::Column),
            _ => None,
        }
    }
}

impl std::fmt::Display for TagSecurable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a tag assignment came to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentSource {
    /// A human placed it (created already approved).
    Manual,
    /// A classifier job suggested it (created unapproved, with a confidence).
    Classifier,
}

impl AssignmentSource {
    /// The stored string (matches the 0016 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Classifier => "classifier",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "manual" => Ok(Self::Manual),
            "classifier" => Ok(Self::Classifier),
            other => Err(MeridianError::internal_msg(format!(
                "tag assignment row has unknown source {other:?}"
            ))),
        }
    }
}

/// A persisted tag row.
#[derive(Debug, Clone)]
pub struct Tag {
    /// ULID of the tag (the stable handle bindings/assignments point at).
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Tag key, e.g. `pii`.
    pub key: String,
    /// Tag value, e.g. `email`.
    pub value: String,
    /// Optional human description.
    pub description: Option<String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

impl Tag {
    /// The rendered tag string `key:value` (as ABAC policies match it via
    /// `resource.tags.contains("…")`).
    #[must_use]
    pub fn rendered(&self) -> String {
        format!("{}:{}", self.key, self.value)
    }
}

/// A tag row as read from Postgres.
#[derive(sqlx::FromRow)]
struct TagRow {
    id: String,
    workspace_id: String,
    key: String,
    value: String,
    description: Option<String>,
    created_at: DateTime<Utc>,
}

impl From<TagRow> for Tag {
    fn from(r: TagRow) -> Self {
        Self {
            id: r.id,
            workspace_id: r.workspace_id,
            key: r.key,
            value: r.value,
            description: r.description,
            created_at: r.created_at,
        }
    }
}

const TAG_COLUMNS: &str = "id, workspace_id, key, value, description, created_at";

/// A persisted tag assignment.
#[derive(Debug, Clone)]
pub struct TagAssignment {
    /// ULID of the assignment.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// The tag being assigned.
    pub tag_id: String,
    /// The kind of securable.
    pub securable_type: TagSecurable,
    /// Polymorphic id: table id, namespace id, or (for a column) the owning
    /// table's id.
    pub securable_id: String,
    /// The column name, set iff `securable_type == Column`.
    pub column_name: Option<String>,
    /// Provenance of the assignment.
    pub source: AssignmentSource,
    /// Classifier confidence in `[0, 1]`, `None` for manual assignments.
    pub confidence: Option<f64>,
    /// Whether the assignment is in force (classifier suggestions start
    /// unapproved).
    pub approved: bool,
    /// Audit string of the assigning principal (or the classifier job).
    pub assigned_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

/// A tag-assignment row as read from Postgres.
#[derive(sqlx::FromRow)]
struct AssignmentRow {
    id: String,
    workspace_id: String,
    tag_id: String,
    securable_type: String,
    securable_id: String,
    column_name: Option<String>,
    source: String,
    confidence: Option<f64>,
    approved: bool,
    assigned_by: String,
    created_at: DateTime<Utc>,
}

impl TryFrom<AssignmentRow> for TagAssignment {
    type Error = MeridianError;

    fn try_from(r: AssignmentRow) -> Result<Self> {
        Ok(Self {
            id: r.id,
            workspace_id: r.workspace_id,
            tag_id: r.tag_id,
            securable_type: TagSecurable::parse(&r.securable_type).ok_or_else(|| {
                MeridianError::internal_msg(format!(
                    "tag assignment row has unknown securable_type {:?}",
                    r.securable_type
                ))
            })?,
            securable_id: r.securable_id,
            column_name: r.column_name,
            source: AssignmentSource::parse(&r.source)?,
            confidence: r.confidence,
            approved: r.approved,
            assigned_by: r.assigned_by,
            created_at: r.created_at,
        })
    }
}

const ASSIGNMENT_COLUMNS: &str = "id, workspace_id, tag_id, securable_type, securable_id, \
     column_name, source, confidence, approved, assigned_by, created_at";

// ---------------------------------------------------------------------------
// Tag CRUD
// ---------------------------------------------------------------------------

/// Creates a tag `key:value` in a workspace.
///
/// Returns [`MeridianError::Conflict`] if the `(workspace, key, value)`
/// already exists (the tag's identity).
pub async fn create_tag(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    key: &str,
    value: &str,
    description: Option<&str>,
    principal: &str,
) -> Result<Tag> {
    if key.trim().is_empty() || value.trim().is_empty() {
        return Err(MeridianError::Validation(
            "tag key and value must be non-empty".to_owned(),
        ));
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin tag create", e))?;

    let id = Ulid::new().to_string();
    let row: TagRow = sqlx::query_as(&format!(
        "INSERT INTO tags (id, workspace_id, key, value, description)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING {TAG_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(key)
    .bind(value)
    .bind(description)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!("tag {key}:{value} already exists"))
        } else {
            map_sqlx_error("failed to insert tag", e)
        }
    })?;

    let details = json!({ "key": key, "value": value });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("tag:{id}"),
            event_type: "governance.tag.created".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "governance.tag.create".to_owned(),
            resource: format!("tag:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit tag create", e))?;

    Ok(row.into())
}

/// Lists all tags in a workspace, in stable (key, value) order.
pub async fn list_tags(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<Tag>> {
    let rows: Vec<TagRow> = sqlx::query_as(&format!(
        "SELECT {TAG_COLUMNS} FROM tags WHERE workspace_id = $1 ORDER BY key, value"
    ))
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list tags", e))?;
    Ok(rows.into_iter().map(Tag::from).collect())
}

/// Loads a tag by id (workspace-scoped).
pub async fn get_tag(pool: &PgPool, workspace_id: WorkspaceId, id: &str) -> Result<Option<Tag>> {
    let row: Option<TagRow> = sqlx::query_as(&format!(
        "SELECT {TAG_COLUMNS} FROM tags WHERE workspace_id = $1 AND id = $2"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load tag", e))?;
    Ok(row.map(Tag::from))
}

/// Deletes a tag and everything referencing it (assignments and policy
/// bindings CASCADE per migration 0016). Returns [`MeridianError::NotFound`]
/// if the tag does not exist.
pub async fn delete_tag(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin tag delete", e))?;

    let existing: Option<(String, String)> = sqlx::query_as(
        "SELECT key, value FROM tags WHERE workspace_id = $1 AND id = $2 FOR UPDATE",
    )
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load tag for delete", e))?;
    let Some((key, value)) = existing else {
        return Err(MeridianError::NotFound(format!(
            "tag {id:?} does not exist"
        )));
    };

    sqlx::query("DELETE FROM tags WHERE workspace_id = $1 AND id = $2")
        .bind(workspace_id.to_string())
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to delete tag", e))?;

    let details = json!({ "key": key, "value": value });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("tag:{id}"),
            event_type: "governance.tag.deleted".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "governance.tag.delete".to_owned(),
            resource: format!("tag:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit tag delete", e))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tag assignment
// ---------------------------------------------------------------------------

/// A new tag assignment to create.
#[derive(Debug, Clone)]
pub struct NewAssignment<'a> {
    /// The tag to assign.
    pub tag_id: &'a str,
    /// The securable kind.
    pub securable_type: TagSecurable,
    /// The securable id (table/namespace id, or table id for a column).
    pub securable_id: &'a str,
    /// The column name (required iff `securable_type == Column`, forbidden
    /// otherwise — validated here and by a CHECK in migration 0016).
    pub column_name: Option<&'a str>,
    /// Provenance.
    pub source: AssignmentSource,
    /// Classifier confidence, if any.
    pub confidence: Option<f64>,
    /// Whether the assignment is in force. `None` defaults by source:
    /// manual → approved, classifier → unapproved.
    pub approved: Option<bool>,
}

/// Assigns a tag to a securable (optionally one column).
///
/// Returns [`MeridianError::Conflict`] if the same tag is already on the same
/// target, [`MeridianError::NotFound`] if the tag does not exist, and
/// [`MeridianError::Validation`] if the column/type pairing is wrong.
pub async fn assign(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    assignment: NewAssignment<'_>,
    principal: &str,
) -> Result<TagAssignment> {
    let is_column = assignment.securable_type == TagSecurable::Column;
    if is_column != assignment.column_name.is_some() {
        return Err(MeridianError::Validation(
            "column_name must be set exactly for column assignments".to_owned(),
        ));
    }
    if let Some(c) = assignment.confidence
        && !(0.0..=1.0).contains(&c)
    {
        return Err(MeridianError::Validation(
            "confidence must be in [0, 1]".to_owned(),
        ));
    }
    let approved = assignment
        .approved
        .unwrap_or(assignment.source == AssignmentSource::Manual);

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin tag assign", e))?;

    let id = Ulid::new().to_string();
    let row: AssignmentRow = sqlx::query_as(&format!(
        "INSERT INTO tag_assignments
             (id, workspace_id, tag_id, securable_type, securable_id, column_name,
              source, confidence, approved, assigned_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         RETURNING {ASSIGNMENT_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(assignment.tag_id)
    .bind(assignment.securable_type.as_str())
    .bind(assignment.securable_id)
    .bind(assignment.column_name)
    .bind(assignment.source.as_str())
    .bind(assignment.confidence)
    .bind(approved)
    .bind(principal)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(
                "that tag is already assigned to that securable/column".to_owned(),
            )
        } else if e
            .as_database_error()
            .is_some_and(sqlx::error::DatabaseError::is_foreign_key_violation)
        {
            MeridianError::NotFound(format!("tag {:?} does not exist", assignment.tag_id))
        } else {
            map_sqlx_error("failed to insert tag assignment", e)
        }
    })?;

    let details = json!({
        "tag_id": assignment.tag_id,
        "securable_type": assignment.securable_type.as_str(),
        "securable_id": assignment.securable_id,
        "column_name": assignment.column_name,
        "source": assignment.source.as_str(),
        "approved": approved,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("tag_assignment:{id}"),
            event_type: "governance.tag.assigned".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "governance.tag.assign".to_owned(),
            resource: format!("tag_assignment:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit tag assign", e))?;

    TagAssignment::try_from(row)
}

/// Removes a tag assignment by id. Returns [`MeridianError::NotFound`] if
/// absent.
pub async fn unassign(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin tag unassign", e))?;

    let deleted: Option<(String,)> = sqlx::query_as(
        "DELETE FROM tag_assignments WHERE workspace_id = $1 AND id = $2 RETURNING id",
    )
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete tag assignment", e))?;
    if deleted.is_none() {
        return Err(MeridianError::NotFound(format!(
            "tag assignment {id:?} does not exist"
        )));
    }

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "governance.tag.unassign".to_owned(),
            resource: format!("tag_assignment:{id}"),
            details: json!({}),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit tag unassign", e))?;

    Ok(())
}

/// Approves a classifier-suggested assignment (puts it in force). Returns
/// [`MeridianError::NotFound`] if absent.
pub async fn approve_assignment(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<TagAssignment> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin assignment approve", e))?;

    let row: Option<AssignmentRow> = sqlx::query_as(&format!(
        "UPDATE tag_assignments SET approved = TRUE
         WHERE workspace_id = $1 AND id = $2
         RETURNING {ASSIGNMENT_COLUMNS}"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to approve tag assignment", e))?;
    let Some(row) = row else {
        return Err(MeridianError::NotFound(format!(
            "tag assignment {id:?} does not exist"
        )));
    };

    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "governance.tag.approve".to_owned(),
            resource: format!("tag_assignment:{id}"),
            details: json!({ "tag_id": row.tag_id }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit assignment approve", e))?;

    TagAssignment::try_from(row)
}

/// Lists every assignment on a securable (all columns included for a table).
/// Ordered by id (creation order).
pub async fn list_assignments_for_securable(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    securable_type: TagSecurable,
    securable_id: &str,
) -> Result<Vec<TagAssignment>> {
    let rows: Vec<AssignmentRow> = sqlx::query_as(&format!(
        "SELECT {ASSIGNMENT_COLUMNS} FROM tag_assignments
         WHERE workspace_id = $1 AND securable_type = $2 AND securable_id = $3
         ORDER BY id"
    ))
    .bind(workspace_id.to_string())
    .bind(securable_type.as_str())
    .bind(securable_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list assignments for securable", e))?;
    rows.into_iter().map(TagAssignment::try_from).collect()
}

// ---------------------------------------------------------------------------
// Resolution (feeds the enforcement layer)
// ---------------------------------------------------------------------------

/// One resolved tag on a table or one of its columns.
///
/// This is the join of `tag_assignments` and `tags` for a single table,
/// restricted to **approved** assignments (a suggestion not yet approved is
/// not in force and does not affect enforcement).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTag {
    /// The rendered tag string `key:value`.
    pub tag: String,
    /// The column the tag is on, or `None` for a table-level tag.
    pub column_name: Option<String>,
}

/// All approved tags that apply to a table and its columns, for enforcement.
///
/// Includes:
///   * table-level tags directly assigned to the table,
///   * column-level tags assigned to the table's columns, and
///   * tags assigned to any of the table's containing namespaces (passed in
///     via `namespace_ids` — the caller resolves the chain, exactly as RBAC
///     does with [`crate::rbac::namespace_chain`]).
///
/// Namespace tags resolve to **table-level** tags on this table (a namespace
/// classification like `pii:high` covers every table beneath it). Only
/// approved assignments are returned.
pub async fn resolve_table_tags(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
    namespace_ids: &[String],
) -> Result<Vec<ResolvedTag>> {
    // Table + column tags on this table, plus namespace tags on any ancestor
    // namespace (folded to table-level). A single query keeps the read cheap
    // on the enforcement hot path.
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT t.key, t.value, a.column_name
         FROM tag_assignments a
         JOIN tags t ON t.id = a.tag_id
         WHERE a.workspace_id = $1
           AND a.approved = TRUE
           AND (
               (a.securable_type IN ('table', 'column') AND a.securable_id = $2)
               OR
               (a.securable_type = 'namespace' AND a.securable_id = ANY($3))
           )
         ORDER BY t.key, t.value, a.column_name",
    )
    .bind(workspace_id.to_string())
    .bind(table_id)
    .bind(namespace_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to resolve table tags", e))?;

    Ok(rows
        .into_iter()
        .map(|(key, value, column_name)| ResolvedTag {
            tag: format!("{key}:{value}"),
            column_name,
        })
        .collect())
}

/// Classification-coverage input: for each table, the count of its columns
/// and the count of *distinct* columns that carry at least one approved tag.
#[derive(Debug, Clone)]
pub struct TableCoverage {
    /// The table id.
    pub table_id: String,
    /// Distinct columns of this table that carry at least one approved
    /// column-level tag.
    pub tagged_columns: i64,
    /// Whether the table itself (or an ancestor namespace) carries an
    /// approved table-level tag.
    pub table_tagged: bool,
}

/// Distinct approved column-level tag counts per table in a set of tables,
/// plus whether each table carries a *table-level* tag. Used by the
/// classification-coverage report (D-F3, D-F5).
///
/// Note: `table_tagged` reflects only tags assigned directly to the table,
/// not tags inherited from an ancestor namespace. Enforcement folds
/// namespace tags (see `resolve_table_tags`); this coverage report can
/// therefore under-count a table whose only classification is inherited.
/// Tracked as a follow-up; it never causes under-enforcement.
pub async fn column_tag_counts(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_ids: &[String],
) -> Result<Vec<TableCoverage>> {
    if table_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(String, i64, bool)> = sqlx::query_as(
        "SELECT tid AS table_id,
                COALESCE(cols.n, 0) AS tagged_columns,
                COALESCE(tbl.tagged, FALSE) AS table_tagged
         FROM unnest($2::text[]) AS tid
         LEFT JOIN (
             SELECT securable_id, COUNT(DISTINCT column_name) AS n
             FROM tag_assignments
             WHERE workspace_id = $1 AND approved = TRUE AND securable_type = 'column'
             GROUP BY securable_id
         ) cols ON cols.securable_id = tid
         LEFT JOIN (
             SELECT securable_id, TRUE AS tagged
             FROM tag_assignments
             WHERE workspace_id = $1 AND approved = TRUE AND securable_type = 'table'
             GROUP BY securable_id
         ) tbl ON tbl.securable_id = tid
         ORDER BY tid",
    )
    .bind(workspace_id.to_string())
    .bind(table_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to count column tags", e))?;

    Ok(rows
        .into_iter()
        .map(|(table_id, tagged_columns, table_tagged)| TableCoverage {
            table_id,
            tagged_columns,
            table_tagged,
        })
        .collect())
}
