//! Scan-plan state and the cross-pod manifest byte cache (migration 0011).
//!
//! A `scan_plans` row exists for every planning operation — synchronous
//! plans are inserted already `completed` (the spec's completed
//! planTableScan response carries a required plan-id), asynchronous plans
//! start `submitted` and are driven to a terminal status by the worker.
//! Result pages are persisted (see the migration header for the
//! recompute-vs-persist decision) and every status transition is a
//! compare-and-set on the current status, so a cancel racing a completing
//! worker has exactly one winner.
//!
//! Plan creation and cancellation write an audit row (and, for creation,
//! an outbox event) in the same transaction as the state change, like
//! every other mutation path. The expiry sweep audits each batch it
//! deletes — `docs/api-status.md` documents the resulting event stream.

use chrono::{DateTime, Duration, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::{Value, json};
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// Plan lifecycle status, exactly the spec's `PlanStatus` strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanStatus {
    /// Planning is queued or running.
    Submitted,
    /// The result pages are ready.
    Completed,
    /// Planning failed; the row carries the error payload.
    Failed,
    /// The client cancelled the plan; pages are dropped.
    Cancelled,
}

impl PlanStatus {
    /// The spec's status string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "submitted" => Ok(Self::Submitted),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(MeridianError::internal_msg(format!(
                "scan plan row has unknown status {other:?}"
            ))),
        }
    }
}

/// How a completed plan's result is delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultMode {
    /// One page, returned in the response body.
    Inline,
    /// Plan-task tokens; pages are fetched via fetchScanTasks.
    Paged,
}

impl ResultMode {
    /// The stored string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Paged => "paged",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "inline" => Ok(Self::Inline),
            "paged" => Ok(Self::Paged),
            other => Err(MeridianError::internal_msg(format!(
                "scan plan row has unknown result mode {other:?}"
            ))),
        }
    }
}

/// A scan plan as stored.
#[derive(Debug, Clone)]
pub struct ScanPlanRecord {
    /// ULID of the plan (the spec's `plan-id`).
    pub id: String,
    /// Owning warehouse.
    pub warehouse_id: String,
    /// Owning table.
    pub table_id: String,
    /// The snapshot the plan is pinned to.
    pub snapshot_id: i64,
    /// Lifecycle status.
    pub status: PlanStatus,
    /// Result delivery mode.
    pub result_mode: ResultMode,
    /// Audit string of the submitting principal.
    pub created_by: String,
    /// The request body as received (inline plans re-plan from it on
    /// fetch, pinned to `snapshot_id`).
    pub request: Value,
    /// The error payload, for failed plans.
    pub error: Option<Value>,
    /// Planning outcome counters, once completed.
    pub summary: Option<Value>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Expiry deadline; the row (and its pages) is gone after this.
    pub expires_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct ScanPlanRow {
    id: String,
    warehouse_id: String,
    table_id: String,
    snapshot_id: i64,
    status: String,
    result_mode: String,
    created_by: String,
    request: Value,
    error: Option<Value>,
    summary: Option<Value>,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

impl ScanPlanRow {
    fn into_record(self) -> Result<ScanPlanRecord> {
        Ok(ScanPlanRecord {
            status: PlanStatus::parse(&self.status)?,
            result_mode: ResultMode::parse(&self.result_mode)?,
            id: self.id,
            warehouse_id: self.warehouse_id,
            table_id: self.table_id,
            snapshot_id: self.snapshot_id,
            created_by: self.created_by,
            request: self.request,
            error: self.error,
            summary: self.summary,
            created_at: self.created_at,
            expires_at: self.expires_at,
        })
    }
}

const PLAN_COLUMNS: &str = "id, warehouse_id, table_id, snapshot_id, status, result_mode, \
                            created_by, request, error, summary, created_at, expires_at";

/// Inputs for [`create`].
#[derive(Debug)]
pub struct NewScanPlan<'a> {
    /// Owning workspace.
    pub workspace_id: WorkspaceId,
    /// Owning warehouse.
    pub warehouse_id: &'a str,
    /// Owning table.
    pub table_id: &'a str,
    /// The pinned snapshot.
    pub snapshot_id: i64,
    /// `Submitted` for asynchronous plans, `Completed` for synchronous
    /// ones (whose pages are inserted in the same transaction).
    pub status: PlanStatus,
    /// Result delivery mode.
    pub result_mode: ResultMode,
    /// Audit string of the submitting principal.
    pub created_by: &'a str,
    /// The request body as received.
    pub request: Value,
    /// Outcome counters (synchronous plans only; `None` for submitted).
    pub summary: Option<Value>,
    /// Time-to-live from now.
    pub ttl: Duration,
    /// Result pages. Empty for `submitted` plans (the worker adds them
    /// via [`complete`]) and for inline plans (which re-plan on fetch —
    /// see `docs/design/scan-planning.md`).
    pub pages: Vec<NewPlanPage>,
}

/// One result page to persist.
#[derive(Debug)]
pub struct NewPlanPage {
    /// Zero-based page number.
    pub page_index: i32,
    /// The opaque `PlanTask` token handed to clients.
    pub page_token: String,
    /// A complete REST `ScanTasks` object.
    pub payload: Value,
}

/// Creates a plan row (plus any pages), with the audit row and outbox
/// event in the same transaction. Returns the new plan id.
pub async fn create(pool: &PgPool, plan: NewScanPlan<'_>) -> Result<String> {
    let id = Ulid::new().to_string();
    let expires_at = Utc::now() + plan.ttl;

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin scan-plan create", e))?;

    sqlx::query(
        "INSERT INTO scan_plans (id, workspace_id, warehouse_id, table_id, snapshot_id,
                                 status, result_mode, created_by, request, summary, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(&id)
    .bind(plan.workspace_id.to_string())
    .bind(plan.warehouse_id)
    .bind(plan.table_id)
    .bind(plan.snapshot_id)
    .bind(plan.status.as_str())
    .bind(plan.result_mode.as_str())
    .bind(plan.created_by)
    .bind(&plan.request)
    .bind(&plan.summary)
    .bind(expires_at)
    .execute(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to insert scan plan", e))?;

    insert_pages(&mut tx, &id, &plan.pages).await?;

    let details = json!({
        "plan_id": id,
        "table_id": plan.table_id,
        "snapshot_id": plan.snapshot_id,
        "status": plan.status.as_str(),
        "result_mode": plan.result_mode.as_str(),
        "summary": plan.summary,
        "expires_at": expires_at.to_rfc3339(),
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(plan.workspace_id),
            aggregate: format!("table:{}", plan.table_id),
            event_type: "scan.planned".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(plan.workspace_id),
            principal: plan.created_by.to_owned(),
            action: "scan.plan".to_owned(),
            resource: format!("table:{}", plan.table_id),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit scan-plan create", e))?;
    Ok(id)
}

async fn insert_pages(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    plan_id: &str,
    pages: &[NewPlanPage],
) -> Result<()> {
    for page in pages {
        sqlx::query(
            "INSERT INTO scan_plan_pages (plan_id, page_index, page_token, payload)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(plan_id)
        .bind(page.page_index)
        .bind(&page.page_token)
        .bind(&page.payload)
        .execute(&mut **tx)
        .await
        .map_err(|e| map_sqlx_error("failed to insert scan-plan page", e))?;
    }
    Ok(())
}

/// Loads a plan by id. Expired-but-unswept plans are treated as absent —
/// expiry is a deadline, not a sweep schedule.
pub async fn get(pool: &PgPool, plan_id: &str) -> Result<Option<ScanPlanRecord>> {
    let row: Option<ScanPlanRow> = sqlx::query_as(&format!(
        "SELECT {PLAN_COLUMNS} FROM scan_plans WHERE id = $1 AND expires_at > now()"
    ))
    .bind(plan_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load scan plan", e))?;
    row.map(ScanPlanRow::into_record).transpose()
}

/// Marks a submitted plan completed and stores its pages, atomically.
/// Returns `false` without writing pages when the plan is no longer in
/// `submitted` (cancelled or expired-and-swept meanwhile) — the caller
/// discards its result.
pub async fn complete(
    pool: &PgPool,
    plan_id: &str,
    pages: &[NewPlanPage],
    summary: &Value,
) -> Result<bool> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin scan-plan complete", e))?;

    let updated = sqlx::query(
        "UPDATE scan_plans SET status = 'completed', summary = $2, updated_at = now()
         WHERE id = $1 AND status = 'submitted'",
    )
    .bind(plan_id)
    .bind(summary)
    .execute(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to complete scan plan", e))?;
    if updated.rows_affected() == 0 {
        return Ok(false);
    }

    insert_pages(&mut tx, plan_id, pages).await?;
    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit scan-plan complete", e))?;
    Ok(true)
}

/// Marks a submitted plan failed with the given error payload. Returns
/// `false` when the plan already left `submitted`.
pub async fn fail(pool: &PgPool, plan_id: &str, error: &Value) -> Result<bool> {
    let updated = sqlx::query(
        "UPDATE scan_plans SET status = 'failed', error = $2, updated_at = now()
         WHERE id = $1 AND status = 'submitted'",
    )
    .bind(plan_id)
    .bind(error)
    .execute(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to mark scan plan failed", e))?;
    Ok(updated.rows_affected() > 0)
}

/// Cancels a plan: `submitted`/`completed` become `cancelled` and pages
/// are dropped; `failed`/`cancelled` are left as they are (cancellation
/// is idempotent on terminal states). Returns `false` when the plan does
/// not exist or is expired. Audited in the same transaction.
pub async fn cancel(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    plan_id: &str,
    principal: &str,
) -> Result<bool> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin scan-plan cancel", e))?;

    let row: Option<ScanPlanRow> = sqlx::query_as(&format!(
        "SELECT {PLAN_COLUMNS} FROM scan_plans
         WHERE id = $1 AND expires_at > now() FOR UPDATE"
    ))
    .bind(plan_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load scan plan for cancel", e))?;
    let Some(row) = row else {
        return Ok(false);
    };
    let record = row.into_record()?;

    if matches!(record.status, PlanStatus::Submitted | PlanStatus::Completed) {
        sqlx::query("UPDATE scan_plans SET status = 'cancelled', updated_at = now() WHERE id = $1")
            .bind(plan_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error("failed to cancel scan plan", e))?;
        sqlx::query("DELETE FROM scan_plan_pages WHERE plan_id = $1")
            .bind(plan_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error("failed to drop cancelled plan pages", e))?;
        audit::append_in_tx(
            &mut tx,
            NewAuditEntry {
                workspace_id: Some(workspace_id),
                principal: principal.to_owned(),
                action: "scan.plan_cancel".to_owned(),
                resource: format!("table:{}", record.table_id),
                details: json!({
                    "plan_id": plan_id,
                    "previous_status": record.status.as_str(),
                }),
            },
        )
        .await?;
    }

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit scan-plan cancel", e))?;
    Ok(true)
}

/// The page tokens of a plan, in page order.
pub async fn page_tokens(pool: &PgPool, plan_id: &str) -> Result<Vec<String>> {
    let tokens: Vec<(String,)> = sqlx::query_as(
        "SELECT page_token FROM scan_plan_pages WHERE plan_id = $1 ORDER BY page_index",
    )
    .bind(plan_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list scan-plan pages", e))?;
    Ok(tokens.into_iter().map(|(t,)| t).collect())
}

/// Resolves a fetchScanTasks token to its plan and page payload. `None`
/// for unknown tokens and for pages of expired plans.
pub async fn page_by_token(
    pool: &PgPool,
    page_token: &str,
) -> Result<Option<(ScanPlanRecord, Value)>> {
    let page: Option<(String, Value)> =
        sqlx::query_as("SELECT plan_id, payload FROM scan_plan_pages WHERE page_token = $1")
            .bind(page_token)
            .fetch_optional(pool)
            .await
            .map_err(|e| map_sqlx_error("failed to resolve scan-task token", e))?;
    let Some((plan_id, payload)) = page else {
        return Ok(None);
    };
    // Expiry is enforced by the plan lookup: a token whose plan expired
    // between the sweep runs resolves to nothing.
    match get(pool, &plan_id).await? {
        Some(plan) => Ok(Some((plan, payload))),
        None => Ok(None),
    }
}

/// Deletes expired plans (pages cascade). Each non-empty batch writes one
/// audit row attributed to the sweeper. Returns the number deleted.
pub async fn sweep_expired(pool: &PgPool, workspace_id: WorkspaceId) -> Result<u64> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin plan expiry sweep", e))?;

    let expired: Vec<(String,)> =
        sqlx::query_as("DELETE FROM scan_plans WHERE expires_at <= now() RETURNING id")
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error("failed to delete expired scan plans", e))?;
    if expired.is_empty() {
        return Ok(0);
    }

    let count = expired.len() as u64;
    // Cap the id list in the audit detail; the count is the signal.
    let sample: Vec<&str> = expired.iter().take(20).map(|(id,)| id.as_str()).collect();
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: "system:plan-sweeper".to_owned(),
            action: "scan.plans_expired".to_owned(),
            resource: "scan_plans".to_owned(),
            details: json!({ "count": count, "plan_ids_sample": sample }),
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit plan expiry sweep", e))?;
    Ok(count)
}

// ---------------------------------------------------------------------------
// Manifest byte cache
// ---------------------------------------------------------------------------

/// Looks up cached manifest bytes. Bumps `accessed_at` lazily: at most
/// one bookkeeping write per row per five minutes, so a hot manifest does
/// not turn every read into a write.
pub async fn manifest_cache_get(
    pool: &PgPool,
    warehouse_id: &str,
    location: &str,
) -> Result<Option<Vec<u8>>> {
    let row: Option<(Vec<u8>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT content, accessed_at FROM manifest_cache
         WHERE warehouse_id = $1 AND location = $2",
    )
    .bind(warehouse_id)
    .bind(location)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to read manifest cache", e))?;

    let Some((content, accessed_at)) = row else {
        return Ok(None);
    };
    if Utc::now() - accessed_at > Duration::minutes(5) {
        sqlx::query(
            "UPDATE manifest_cache SET accessed_at = now()
             WHERE warehouse_id = $1 AND location = $2",
        )
        .bind(warehouse_id)
        .bind(location)
        .execute(pool)
        .await
        .map_err(|e| map_sqlx_error("failed to touch manifest cache row", e))?;
    }
    Ok(Some(content))
}

/// Inserts manifest bytes. Manifest files are immutable at a path, so a
/// concurrent insert of the same key is a no-op, never an overwrite.
pub async fn manifest_cache_put(
    pool: &PgPool,
    warehouse_id: &str,
    location: &str,
    content: &[u8],
) -> Result<()> {
    sqlx::query(
        "INSERT INTO manifest_cache (warehouse_id, location, content, content_bytes)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (warehouse_id, location) DO NOTHING",
    )
    .bind(warehouse_id)
    .bind(location)
    .bind(content)
    .bind(i64::try_from(content.len()).unwrap_or(i64::MAX))
    .execute(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to write manifest cache", e))?;
    Ok(())
}

/// Evicts least-recently-accessed cache rows until the total stored bytes
/// fit `max_total_bytes`. Returns the number of rows evicted.
pub async fn manifest_cache_evict(pool: &PgPool, max_total_bytes: i64) -> Result<u64> {
    let deleted = sqlx::query(
        "DELETE FROM manifest_cache
         WHERE (warehouse_id, location) IN (
             SELECT warehouse_id, location FROM (
                 SELECT warehouse_id, location,
                        SUM(content_bytes) OVER (
                            ORDER BY accessed_at DESC, warehouse_id, location
                        ) AS running_bytes
                 FROM manifest_cache
             ) ranked
             WHERE running_bytes > $1
         )",
    )
    .bind(max_total_bytes)
    .execute(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to evict manifest cache rows", e))?;
    Ok(deleted.rows_affected())
}
