//! Maintenance policy, job queue, and savings ledger (Pillar C-F3).
//!
//! Three concerns, one module:
//!
//! - **Policies** ([`create_policy`], [`resolve_effective`], …): declarative
//!   per-scope maintenance configuration. A table's *effective* policy is the
//!   most-specific matching scope — table beats namespace beats warehouse.
//! - **Jobs** ([`enqueue_job`], [`claim_next`], [`start_job`], [`complete_job`],
//!   [`fail_job`], [`cancel_job`]): the Postgres work queue. Workers claim
//!   with `FOR UPDATE SKIP LOCKED`, per-tenant fair (oldest queued job across
//!   the least-recently-served workspace first). Every state transition is a
//!   compare-and-set on the prior state, so two workers racing a job have one
//!   winner.
//! - **Savings ledger** ([`append_savings`], [`monthly_rollup`]): the
//!   append-only receipt of what each job saved, and the monthly roll-up the
//!   cost-intelligence surface reports.
//!
//! # Audit + outbox discipline
//!
//! Every *mutation* here — policy create/update/delete, job enqueue and every
//! job transition, ledger append — writes an audit row and an outbox event in
//! the same transaction as the state change, exactly like the commit path.
//! The actual table *maintenance* (the file rewrite) is not performed here; it
//! is an ordinary Iceberg commit through `PostgresCommitBackend`, run by the
//! executor wave. This module is the control plane: what to do, what is being
//! done, and what it saved.

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::{Value, json};
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// C-F3 default target compacted file size: 512 MiB.
pub const DEFAULT_TARGET_FILE_SIZE_BYTES: i64 = 512 * 1024 * 1024;
/// C-F3 default minimum input files for a compaction to run.
pub const DEFAULT_MIN_INPUT_FILES: i32 = 5;
/// Default snapshot retention count.
pub const DEFAULT_SNAPSHOT_RETENTION_COUNT: i32 = 100;
/// Default snapshot retention age: 5 days in millis.
pub const DEFAULT_SNAPSHOT_RETENTION_AGE_MS: i64 = 5 * 24 * 60 * 60 * 1000;

/// The scope a maintenance policy attaches to. Ordered by specificity:
/// [`Scope::Table`] is the most specific, [`Scope::Warehouse`] the least.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Scope {
    /// Applies to every table under a warehouse.
    Warehouse,
    /// Applies to every table under a namespace.
    Namespace,
    /// Applies to a single table.
    Table,
}

impl Scope {
    /// The stored string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Warehouse => "warehouse",
            Self::Namespace => "namespace",
            Self::Table => "table",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "warehouse" => Ok(Self::Warehouse),
            "namespace" => Ok(Self::Namespace),
            "table" => Ok(Self::Table),
            other => Err(MeridianError::internal_msg(format!(
                "maintenance policy has unknown scope {other:?}"
            ))),
        }
    }

    /// Specificity rank: higher wins in effective-policy resolution.
    #[must_use]
    pub fn specificity(self) -> u8 {
        match self {
            Self::Warehouse => 0,
            Self::Namespace => 1,
            Self::Table => 2,
        }
    }
}

/// The maintenance job kinds (`maintenance_jobs.job_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobType {
    /// Bin-pack / sort / z-order rewrite of small files.
    Compaction,
    /// Snapshot expiry per the retention policy.
    ExpireSnapshots,
    /// Orphan-file cleanup (with a safety window).
    RemoveOrphans,
    /// Manifest rewrite/merge.
    RewriteManifests,
}

impl JobType {
    /// The stored string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Compaction => "compaction",
            Self::ExpireSnapshots => "expire_snapshots",
            Self::RemoveOrphans => "remove_orphans",
            Self::RewriteManifests => "rewrite_manifests",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "compaction" => Ok(Self::Compaction),
            "expire_snapshots" => Ok(Self::ExpireSnapshots),
            "remove_orphans" => Ok(Self::RemoveOrphans),
            "rewrite_manifests" => Ok(Self::RewriteManifests),
            other => Err(MeridianError::internal_msg(format!(
                "maintenance job has unknown type {other:?}"
            ))),
        }
    }
}

/// The lifecycle state of a maintenance job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    /// Waiting to be claimed.
    Queued,
    /// Claimed by a worker and executing.
    Running,
    /// Completed; `result` carries before/after metrics.
    Succeeded,
    /// Failed; `error` carries the failure payload.
    Failed,
    /// Cancelled before completion.
    Cancelled,
}

impl JobState {
    /// The stored string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(MeridianError::internal_msg(format!(
                "maintenance job has unknown state {other:?}"
            ))),
        }
    }

    /// Whether this is a terminal state (no further transitions).
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

// ---- policies -------------------------------------------------------------

/// The mutable fields of a maintenance policy (create/update input).
#[derive(Debug, Clone)]
pub struct PolicySpec {
    /// Target compacted file size (small-file threshold + bin-pack target).
    pub target_file_size_bytes: i64,
    /// Minimum input files a compaction must combine.
    pub min_input_files: i32,
    /// Keep at least this many snapshots.
    pub snapshot_retention_count: i32,
    /// Keep any snapshot younger than this many millis.
    pub snapshot_retention_age_ms: i64,
    /// Freshness SLA: act when the newest commit is older than this. `None`
    /// = no SLA.
    pub max_staleness_ms: Option<i64>,
    /// Cron-ish schedule; `None` = reconcile-driven only.
    pub schedule: Option<String>,
    /// Execution-window start (e.g. `"02:00"`).
    pub window_start: Option<String>,
    /// Execution-window end.
    pub window_end: Option<String>,
    /// Monthly spend cap in USD; `None` = uncapped.
    pub cost_cap_usd_month: Option<f64>,
    /// Exclusion rules (name globs, tag predicates, job-type opt-outs).
    pub exclusions: Value,
    /// Whether the policy is active.
    pub enabled: bool,
}

impl Default for PolicySpec {
    /// The C-F3 default policy: 512 MiB target, 5 min input files, keep 100
    /// snapshots / 5 days, no SLA, reconcile-driven, uncapped, enabled.
    fn default() -> Self {
        Self {
            target_file_size_bytes: DEFAULT_TARGET_FILE_SIZE_BYTES,
            min_input_files: DEFAULT_MIN_INPUT_FILES,
            snapshot_retention_count: DEFAULT_SNAPSHOT_RETENTION_COUNT,
            snapshot_retention_age_ms: DEFAULT_SNAPSHOT_RETENTION_AGE_MS,
            max_staleness_ms: None,
            schedule: None,
            window_start: None,
            window_end: None,
            cost_cap_usd_month: None,
            exclusions: json!({}),
            enabled: true,
        }
    }
}

impl PolicySpec {
    /// Validates the field invariants the DB also checks, but with
    /// caller-facing [`MeridianError::Validation`] messages.
    fn validate(&self) -> Result<()> {
        if self.target_file_size_bytes <= 0 {
            return Err(MeridianError::Validation(
                "target_file_size_bytes must be positive".to_owned(),
            ));
        }
        if self.min_input_files < 1 {
            return Err(MeridianError::Validation(
                "min_input_files must be at least 1".to_owned(),
            ));
        }
        if self.snapshot_retention_count < 1 {
            return Err(MeridianError::Validation(
                "snapshot_retention_count must be at least 1".to_owned(),
            ));
        }
        if self.snapshot_retention_age_ms < 0 {
            return Err(MeridianError::Validation(
                "snapshot_retention_age_ms must be non-negative".to_owned(),
            ));
        }
        if self.max_staleness_ms.is_some_and(|v| v < 0) {
            return Err(MeridianError::Validation(
                "max_staleness_ms must be non-negative".to_owned(),
            ));
        }
        if self.cost_cap_usd_month.is_some_and(|v| v < 0.0) {
            return Err(MeridianError::Validation(
                "cost_cap_usd_month must be non-negative".to_owned(),
            ));
        }
        Ok(())
    }
}

/// A persisted maintenance policy.
#[derive(Debug, Clone)]
pub struct PolicyRecord {
    /// ULID of the policy.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// The scope this policy attaches to.
    pub scope: Scope,
    /// Id of the scoped object (warehouse / namespace / table id).
    pub scope_id: String,
    /// The policy configuration.
    pub spec: PolicySpec,
    /// Audit string of the creator.
    pub created_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last-update time.
    pub updated_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct PolicyRow {
    id: String,
    workspace_id: String,
    scope: String,
    scope_id: String,
    target_file_size_bytes: i64,
    min_input_files: i32,
    snapshot_retention_count: i32,
    snapshot_retention_age_ms: i64,
    max_staleness_ms: Option<i64>,
    schedule: Option<String>,
    window_start: Option<String>,
    window_end: Option<String>,
    cost_cap_usd_month: Option<f64>,
    exclusions: Value,
    enabled: bool,
    created_by: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl PolicyRow {
    fn into_record(self) -> Result<PolicyRecord> {
        Ok(PolicyRecord {
            id: self.id,
            workspace_id: self.workspace_id,
            scope: Scope::parse(&self.scope)?,
            scope_id: self.scope_id,
            spec: PolicySpec {
                target_file_size_bytes: self.target_file_size_bytes,
                min_input_files: self.min_input_files,
                snapshot_retention_count: self.snapshot_retention_count,
                snapshot_retention_age_ms: self.snapshot_retention_age_ms,
                max_staleness_ms: self.max_staleness_ms,
                schedule: self.schedule,
                window_start: self.window_start,
                window_end: self.window_end,
                cost_cap_usd_month: self.cost_cap_usd_month,
                exclusions: self.exclusions,
                enabled: self.enabled,
            },
            created_by: self.created_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

const POLICY_COLUMNS: &str = "id, workspace_id, scope, scope_id, target_file_size_bytes, \
     min_input_files, snapshot_retention_count, snapshot_retention_age_ms, max_staleness_ms, \
     schedule, window_start, window_end, cost_cap_usd_month, exclusions, enabled, created_by, \
     created_at, updated_at";

/// Creates a maintenance policy for a scope, or fails with
/// [`MeridianError::Conflict`] if one already exists for that
/// `(workspace, scope, scope_id)` — a scope's policy is singular; edit it
/// with [`update_policy`].
pub async fn create_policy(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    scope: Scope,
    scope_id: &str,
    spec: &PolicySpec,
    created_by: &str,
) -> Result<PolicyRecord> {
    spec.validate()?;
    let id = Ulid::new().to_string();
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin policy create", e))?;

    let row: PolicyRow = sqlx::query_as(&format!(
        "INSERT INTO maintenance_policies
             (id, workspace_id, scope, scope_id, target_file_size_bytes, min_input_files,
              snapshot_retention_count, snapshot_retention_age_ms, max_staleness_ms, schedule,
              window_start, window_end, cost_cap_usd_month, exclusions, enabled, created_by)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
         RETURNING {POLICY_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(scope.as_str())
    .bind(scope_id)
    .bind(spec.target_file_size_bytes)
    .bind(spec.min_input_files)
    .bind(spec.snapshot_retention_count)
    .bind(spec.snapshot_retention_age_ms)
    .bind(spec.max_staleness_ms)
    .bind(&spec.schedule)
    .bind(&spec.window_start)
    .bind(&spec.window_end)
    .bind(spec.cost_cap_usd_month)
    .bind(&spec.exclusions)
    .bind(spec.enabled)
    .bind(created_by)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| conflict_or("a maintenance policy already exists for this scope", e))?;

    audit_and_outbox(
        &mut tx,
        workspace_id,
        created_by,
        "maintenance.policy.create",
        &format!("policy:{id}"),
        "maintenance.policy.created",
        json!({
            "policy_id": id,
            "scope": scope.as_str(),
            "scope_id": scope_id,
        }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit policy create", e))?;
    row.into_record()
}

/// Updates the policy for a scope in place, or fails with
/// [`MeridianError::NotFound`] when none exists.
pub async fn update_policy(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    scope: Scope,
    scope_id: &str,
    spec: &PolicySpec,
    actor: &str,
) -> Result<PolicyRecord> {
    spec.validate()?;
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin policy update", e))?;

    let row: Option<PolicyRow> = sqlx::query_as(&format!(
        "UPDATE maintenance_policies
         SET target_file_size_bytes = $4, min_input_files = $5, snapshot_retention_count = $6,
             snapshot_retention_age_ms = $7, max_staleness_ms = $8, schedule = $9,
             window_start = $10, window_end = $11, cost_cap_usd_month = $12, exclusions = $13,
             enabled = $14, updated_at = now()
         WHERE workspace_id = $1 AND scope = $2 AND scope_id = $3
         RETURNING {POLICY_COLUMNS}"
    ))
    .bind(workspace_id.to_string())
    .bind(scope.as_str())
    .bind(scope_id)
    .bind(spec.target_file_size_bytes)
    .bind(spec.min_input_files)
    .bind(spec.snapshot_retention_count)
    .bind(spec.snapshot_retention_age_ms)
    .bind(spec.max_staleness_ms)
    .bind(&spec.schedule)
    .bind(&spec.window_start)
    .bind(&spec.window_end)
    .bind(spec.cost_cap_usd_month)
    .bind(&spec.exclusions)
    .bind(spec.enabled)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to update maintenance policy", e))?;

    let Some(row) = row else {
        return Err(MeridianError::NotFound(format!(
            "no maintenance policy for {} {scope_id}",
            scope.as_str()
        )));
    };

    audit_and_outbox(
        &mut tx,
        workspace_id,
        actor,
        "maintenance.policy.update",
        &format!("policy:{}", row.id),
        "maintenance.policy.updated",
        json!({ "policy_id": row.id, "scope": scope.as_str(), "scope_id": scope_id }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit policy update", e))?;
    row.into_record()
}

/// Deletes the policy for a scope; returns whether a row was removed.
pub async fn delete_policy(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    scope: Scope,
    scope_id: &str,
    actor: &str,
) -> Result<bool> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin policy delete", e))?;

    let deleted: Option<String> = sqlx::query_scalar(
        "DELETE FROM maintenance_policies
         WHERE workspace_id = $1 AND scope = $2 AND scope_id = $3
         RETURNING id",
    )
    .bind(workspace_id.to_string())
    .bind(scope.as_str())
    .bind(scope_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to delete maintenance policy", e))?;

    let Some(policy_id) = deleted else {
        tx.rollback()
            .await
            .map_err(|e| map_sqlx_error("failed to roll back empty policy delete", e))?;
        return Ok(false);
    };

    audit_and_outbox(
        &mut tx,
        workspace_id,
        actor,
        "maintenance.policy.delete",
        &format!("policy:{policy_id}"),
        "maintenance.policy.deleted",
        json!({ "policy_id": policy_id, "scope": scope.as_str(), "scope_id": scope_id }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit policy delete", e))?;
    Ok(true)
}

/// Fetches the policy at an exact scope, if one exists.
pub async fn get_policy(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    scope: Scope,
    scope_id: &str,
) -> Result<Option<PolicyRecord>> {
    let row: Option<PolicyRow> = sqlx::query_as(&format!(
        "SELECT {POLICY_COLUMNS} FROM maintenance_policies
         WHERE workspace_id = $1 AND scope = $2 AND scope_id = $3"
    ))
    .bind(workspace_id.to_string())
    .bind(scope.as_str())
    .bind(scope_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to fetch maintenance policy", e))?;
    row.map(PolicyRow::into_record).transpose()
}

/// Resolves the *effective* policy for a table: the most-specific enabled
/// policy among the candidate scopes, in precedence order table > namespace >
/// warehouse. Returns `None` when no scope has an enabled policy — callers
/// then fall back to [`PolicySpec::default`].
///
/// The caller passes the ids of the containing scopes (it knows the table's
/// namespace and warehouse); this function does not walk the hierarchy
/// itself, keeping it a pure resolution over the supplied candidates.
pub async fn resolve_effective(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
    namespace_id: &str,
    warehouse_id: &str,
) -> Result<Option<PolicyRecord>> {
    // One query fetches every candidate; resolution picks the winner by
    // specificity so precedence is decided here, deterministically, rather
    // than depending on row order.
    let rows: Vec<PolicyRow> = sqlx::query_as(&format!(
        "SELECT {POLICY_COLUMNS} FROM maintenance_policies
         WHERE workspace_id = $1 AND enabled = TRUE AND (
             (scope = 'table' AND scope_id = $2)
             OR (scope = 'namespace' AND scope_id = $3)
             OR (scope = 'warehouse' AND scope_id = $4)
         )"
    ))
    .bind(workspace_id.to_string())
    .bind(table_id)
    .bind(namespace_id)
    .bind(warehouse_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to resolve effective policy", e))?;

    let mut best: Option<PolicyRecord> = None;
    for row in rows {
        let record = row.into_record()?;
        let more_specific = best
            .as_ref()
            .is_none_or(|b| record.scope.specificity() > b.scope.specificity());
        if more_specific {
            best = Some(record);
        }
    }
    Ok(best)
}

// ---- jobs -----------------------------------------------------------------

/// A persisted maintenance job.
#[derive(Debug, Clone)]
pub struct JobRecord {
    /// ULID of the job.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Target table.
    pub table_id: String,
    /// The operation kind.
    pub job_type: JobType,
    /// Lifecycle state.
    pub state: JobState,
    /// The policy that scheduled it, if policy-driven.
    pub policy_id: Option<String>,
    /// Job parameters.
    pub spec: Value,
    /// Audit string of the creator.
    pub created_by: String,
    /// Worker currently holding the job, if running.
    pub claimed_by: Option<String>,
    /// Claim-cycle count.
    pub attempts: i32,
    /// Failure payload, for failed jobs.
    pub error: Option<Value>,
    /// Success result (before/after metrics).
    pub result: Option<Value>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// When it started running.
    pub started_at: Option<DateTime<Utc>>,
    /// When it reached a terminal state.
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
struct JobRow {
    id: String,
    workspace_id: String,
    table_id: String,
    job_type: String,
    state: String,
    policy_id: Option<String>,
    spec: Value,
    created_by: String,
    claimed_by: Option<String>,
    attempts: i32,
    error: Option<Value>,
    result: Option<Value>,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

impl JobRow {
    fn into_record(self) -> Result<JobRecord> {
        Ok(JobRecord {
            id: self.id,
            workspace_id: self.workspace_id,
            table_id: self.table_id,
            job_type: JobType::parse(&self.job_type)?,
            state: JobState::parse(&self.state)?,
            policy_id: self.policy_id,
            spec: self.spec,
            created_by: self.created_by,
            claimed_by: self.claimed_by,
            attempts: self.attempts,
            error: self.error,
            result: self.result,
            created_at: self.created_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        })
    }
}

const JOB_COLUMNS: &str = "id, workspace_id, table_id, job_type, state, policy_id, spec, \
     created_by, claimed_by, attempts, error, result, created_at, started_at, finished_at";

/// Enqueues a maintenance job in `queued` state.
pub async fn enqueue_job(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
    job_type: JobType,
    policy_id: Option<&str>,
    spec: &Value,
    created_by: &str,
) -> Result<JobRecord> {
    let id = Ulid::new().to_string();
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin job enqueue", e))?;

    let row: JobRow = sqlx::query_as(&format!(
        "INSERT INTO maintenance_jobs
             (id, workspace_id, table_id, job_type, policy_id, spec, created_by)
         VALUES ($1,$2,$3,$4,$5,$6,$7)
         RETURNING {JOB_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(table_id)
    .bind(job_type.as_str())
    .bind(policy_id)
    .bind(spec)
    .bind(created_by)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to enqueue maintenance job", e))?;

    audit_and_outbox(
        &mut tx,
        workspace_id,
        created_by,
        "maintenance.job.enqueue",
        &format!("job:{id}"),
        "maintenance.job.queued",
        json!({ "job_id": id, "table_id": table_id, "job_type": job_type.as_str() }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit job enqueue", e))?;
    row.into_record()
}

/// Claims the next queued job for `worker`, transitioning it to `running`,
/// or returns `None` when the queue is empty.
///
/// Fairness: among all queued jobs, the workspace whose most-recent claim is
/// oldest (or which has never been served) is picked first, then that
/// workspace's oldest queued job. `FOR UPDATE SKIP LOCKED` lets many workers
/// claim concurrently without blocking or double-claiming. The claim,
/// attempt bump, audit, and outbox all land in one transaction.
pub async fn claim_next(pool: &PgPool, worker: &str) -> Result<Option<JobRecord>> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin job claim", e))?;

    // Per-tenant fairness: rank workspaces by the newest `started_at` they
    // already own (NULL — never served — sorts first via NULLS FIRST), then
    // take that workspace's oldest queued job. The correlated aggregate is
    // over a bounded set (running/finished jobs per workspace) and the
    // partial index on queued jobs keeps the candidate scan tight.
    let claimed: Option<String> = sqlx::query_scalar(
        "WITH candidate AS (
             SELECT j.id, j.workspace_id, j.created_at,
                    (SELECT MAX(s.started_at) FROM maintenance_jobs s
                     WHERE s.workspace_id = j.workspace_id AND s.started_at IS NOT NULL)
                        AS last_served
             FROM maintenance_jobs j
             WHERE j.state = 'queued'
             ORDER BY last_served ASC NULLS FIRST, j.created_at ASC
             LIMIT 1
             FOR UPDATE SKIP LOCKED
         )
         UPDATE maintenance_jobs m
         SET state = 'running', claimed_by = $1, attempts = m.attempts + 1,
             started_at = now(), updated_at = now()
         FROM candidate
         WHERE m.id = candidate.id
         RETURNING m.id",
    )
    .bind(worker)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to claim maintenance job", e))?;

    let Some(job_id) = claimed else {
        tx.rollback()
            .await
            .map_err(|e| map_sqlx_error("failed to roll back empty claim", e))?;
        return Ok(None);
    };

    let row: JobRow = sqlx::query_as(&format!(
        "SELECT {JOB_COLUMNS} FROM maintenance_jobs WHERE id = $1"
    ))
    .bind(&job_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load claimed job", e))?;
    let workspace_id = parse_workspace_id(&row.workspace_id)?;

    audit_and_outbox(
        &mut tx,
        workspace_id,
        worker,
        "maintenance.job.claim",
        &format!("job:{job_id}"),
        "maintenance.job.running",
        json!({ "job_id": job_id, "worker": worker, "attempts": row.attempts }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit job claim", e))?;
    row.into_record().map(Some)
}

/// Marks a running job `succeeded`, recording its `result` (before/after
/// metrics). Fails with [`MeridianError::Conflict`] if the job is not
/// currently `running` (a lost race, or a double-complete).
pub async fn complete_job(
    pool: &PgPool,
    job_id: &str,
    worker: &str,
    result: &Value,
) -> Result<JobRecord> {
    transition_terminal(
        pool,
        job_id,
        worker,
        JobState::Succeeded,
        Some(result),
        None,
    )
    .await
}

/// Marks a running job `failed`, recording its `error`. Fails with
/// [`MeridianError::Conflict`] if the job is not currently `running`.
pub async fn fail_job(
    pool: &PgPool,
    job_id: &str,
    worker: &str,
    error: &Value,
) -> Result<JobRecord> {
    transition_terminal(pool, job_id, worker, JobState::Failed, None, Some(error)).await
}

/// Cancels a job that is still `queued` or `running`. Fails with
/// [`MeridianError::Conflict`] if the job already reached a terminal state.
pub async fn cancel_job(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    job_id: &str,
    actor: &str,
) -> Result<JobRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin job cancel", e))?;

    let row: Option<JobRow> = sqlx::query_as(&format!(
        "UPDATE maintenance_jobs
         SET state = 'cancelled', claimed_by = NULL, finished_at = now(), updated_at = now()
         WHERE id = $1 AND workspace_id = $2 AND state IN ('queued', 'running')
         RETURNING {JOB_COLUMNS}"
    ))
    .bind(job_id)
    .bind(workspace_id.to_string())
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to cancel maintenance job", e))?;

    let Some(row) = row else {
        return Err(job_not_cancellable(pool, workspace_id, job_id).await);
    };

    audit_and_outbox(
        &mut tx,
        workspace_id,
        actor,
        "maintenance.job.cancel",
        &format!("job:{job_id}"),
        "maintenance.job.cancelled",
        json!({ "job_id": job_id }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit job cancel", e))?;
    row.into_record()
}

/// Shared terminal transition for `succeeded`/`failed`, guarded on the job
/// being `running` and held by the completing worker.
async fn transition_terminal(
    pool: &PgPool,
    job_id: &str,
    worker: &str,
    to: JobState,
    result: Option<&Value>,
    error: Option<&Value>,
) -> Result<JobRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin job transition", e))?;

    let row: Option<JobRow> = sqlx::query_as(&format!(
        "UPDATE maintenance_jobs
         SET state = $3, result = $4, error = $5, claimed_by = NULL, finished_at = now(),
             updated_at = now()
         WHERE id = $1 AND claimed_by = $2 AND state = 'running'
         RETURNING {JOB_COLUMNS}"
    ))
    .bind(job_id)
    .bind(worker)
    .bind(to.as_str())
    .bind(result)
    .bind(error)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to transition maintenance job", e))?;

    let Some(row) = row else {
        return Err(MeridianError::Conflict(format!(
            "job {job_id} is not running under worker {worker:?}"
        )));
    };
    let workspace_id = parse_workspace_id(&row.workspace_id)?;

    let (action, event) = match to {
        JobState::Succeeded => ("maintenance.job.complete", "maintenance.job.succeeded"),
        JobState::Failed => ("maintenance.job.fail", "maintenance.job.failed"),
        _ => unreachable!("transition_terminal is only called for succeeded/failed"),
    };
    audit_and_outbox(
        &mut tx,
        workspace_id,
        worker,
        action,
        &format!("job:{job_id}"),
        event,
        json!({ "job_id": job_id, "state": to.as_str() }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit job transition", e))?;
    row.into_record()
}

/// Fetches a job by id, scoped to its workspace.
pub async fn get_job(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    job_id: &str,
) -> Result<Option<JobRecord>> {
    let row: Option<JobRow> = sqlx::query_as(&format!(
        "SELECT {JOB_COLUMNS} FROM maintenance_jobs WHERE id = $1 AND workspace_id = $2"
    ))
    .bind(job_id)
    .bind(workspace_id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to fetch maintenance job", e))?;
    row.map(JobRow::into_record).transpose()
}

/// Builds the right conflict error when a cancel found no cancellable row:
/// distinguishes "already terminal" from "not found".
async fn job_not_cancellable(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    job_id: &str,
) -> MeridianError {
    match get_job(pool, workspace_id, job_id).await {
        Ok(Some(job)) => MeridianError::Conflict(format!(
            "job {job_id} is already {} and cannot be cancelled",
            job.state.as_str()
        )),
        Ok(None) => MeridianError::NotFound(format!("job {job_id} not found")),
        Err(e) => e,
    }
}

// ---- savings ledger -------------------------------------------------------

/// Before/after inputs for a savings-ledger row.
#[derive(Debug, Clone)]
pub struct SavingsInput {
    /// Data bytes before the job.
    pub bytes_before: i64,
    /// Data bytes after the job.
    pub bytes_after: i64,
    /// Data-file count before the job.
    pub files_before: i64,
    /// Data-file count after the job.
    pub files_after: i64,
    /// Projected object-store GET requests avoided (small-file cost model).
    pub est_get_requests_saved: i64,
    /// How the numbers were derived (shown in the ledger export).
    pub methodology: String,
}

/// A persisted savings-ledger row.
#[derive(Debug, Clone)]
pub struct SavingsRecord {
    /// ULID of the row.
    pub id: String,
    /// The job that produced these savings.
    pub job_id: String,
    /// Denormalized table id.
    pub table_id: String,
    /// Accounting period (first-of-month, UTC).
    pub period: NaiveDate,
    /// Bytes saved (`before - after`; can be negative if the job grew data).
    pub bytes_saved: i64,
    /// Files removed (`before - after`).
    pub files_removed: i64,
    /// Projected GET requests avoided.
    pub est_get_requests_saved: i64,
}

/// Appends a savings-ledger row for a completed job. `period` is derived from
/// `now` (first of the current UTC month). Fails with
/// [`MeridianError::Conflict`] if the job already has a ledger row (the
/// `UNIQUE(job_id)` guard: a job's savings are counted exactly once).
pub async fn append_savings(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    job_id: &str,
    table_id: &str,
    table_ident: &str,
    input: &SavingsInput,
    actor: &str,
) -> Result<SavingsRecord> {
    let id = Ulid::new().to_string();
    let period = first_of_month(Utc::now().date_naive());
    let bytes_saved = input.bytes_before - input.bytes_after;
    let files_removed = input.files_before - input.files_after;

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin savings append", e))?;

    sqlx::query(
        "INSERT INTO savings_ledger
             (id, workspace_id, job_id, table_id, table_ident, period, bytes_before, bytes_after,
              files_before, files_after, bytes_saved, files_removed, est_get_requests_saved,
              methodology)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(job_id)
    .bind(table_id)
    .bind(table_ident)
    .bind(period)
    .bind(input.bytes_before)
    .bind(input.bytes_after)
    .bind(input.files_before)
    .bind(input.files_after)
    .bind(bytes_saved)
    .bind(files_removed)
    .bind(input.est_get_requests_saved)
    .bind(&input.methodology)
    .execute(&mut *tx)
    .await
    .map_err(|e| conflict_or("savings already recorded for this job", e))?;

    audit_and_outbox(
        &mut tx,
        workspace_id,
        actor,
        "maintenance.savings.append",
        &format!("job:{job_id}"),
        "maintenance.savings.recorded",
        json!({
            "job_id": job_id,
            "table_id": table_id,
            "bytes_saved": bytes_saved,
            "files_removed": files_removed,
        }),
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit savings append", e))?;

    Ok(SavingsRecord {
        id,
        job_id: job_id.to_owned(),
        table_id: table_id.to_owned(),
        period,
        bytes_saved,
        files_removed,
        est_get_requests_saved: input.est_get_requests_saved,
    })
}

/// A monthly savings roll-up row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MonthlyRollup {
    /// Accounting period (first-of-month, UTC).
    pub period: NaiveDate,
    /// Number of ledgered jobs in the period.
    pub job_count: i64,
    /// Total bytes saved.
    pub bytes_saved: i64,
    /// Total files removed.
    pub files_removed: i64,
    /// Total projected GET requests avoided.
    pub est_get_requests_saved: i64,
}

/// Rolls the savings ledger up by month for a workspace, newest period
/// first. This backs the "Meridian saved you $X this month" surface.
pub async fn monthly_rollup(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    limit: i64,
) -> Result<Vec<MonthlyRollup>> {
    sqlx::query_as(
        "SELECT period,
                COUNT(*)::bigint AS job_count,
                COALESCE(SUM(bytes_saved), 0)::bigint AS bytes_saved,
                COALESCE(SUM(files_removed), 0)::bigint AS files_removed,
                COALESCE(SUM(est_get_requests_saved), 0)::bigint AS est_get_requests_saved
         FROM savings_ledger
         WHERE workspace_id = $1
         GROUP BY period
         ORDER BY period DESC
         LIMIT $2",
    )
    .bind(workspace_id.to_string())
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to roll up savings", e))
}

// ---- shared helpers -------------------------------------------------------

/// Writes an audit row and an outbox event on the caller's transaction — the
/// same-transaction discipline every mutation in this module follows.
async fn audit_and_outbox(
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

/// Maps a unique-violation to [`MeridianError::Conflict`] with `message`;
/// anything else stays an internal/unavailable error.
fn conflict_or(message: &str, error: sqlx::Error) -> MeridianError {
    if error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
    {
        MeridianError::Conflict(message.to_owned())
    } else {
        map_sqlx_error(message, error)
    }
}

/// Parses a stored workspace-id string back into a [`WorkspaceId`].
fn parse_workspace_id(raw: &str) -> Result<WorkspaceId> {
    raw.parse()
        .map_err(|_| MeridianError::internal_msg(format!("stored workspace id {raw:?} is invalid")))
}

/// The first day of the month containing `date` (the ledger period grain).
fn first_of_month(date: NaiveDate) -> NaiveDate {
    date.with_day(1).unwrap_or(date) // day 1 always exists in every month
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_specificity_orders_table_over_namespace_over_warehouse() {
        assert!(Scope::Table.specificity() > Scope::Namespace.specificity());
        assert!(Scope::Namespace.specificity() > Scope::Warehouse.specificity());
    }

    #[test]
    fn scope_round_trips() {
        for s in [Scope::Warehouse, Scope::Namespace, Scope::Table] {
            assert_eq!(Scope::parse(s.as_str()).unwrap(), s);
        }
        assert!(Scope::parse("bogus").is_err());
    }

    #[test]
    fn job_type_round_trips() {
        for t in [
            JobType::Compaction,
            JobType::ExpireSnapshots,
            JobType::RemoveOrphans,
            JobType::RewriteManifests,
        ] {
            assert_eq!(JobType::parse(t.as_str()).unwrap(), t);
        }
        assert!(JobType::parse("bogus").is_err());
    }

    #[test]
    fn job_state_terminality() {
        assert!(!JobState::Queued.is_terminal());
        assert!(!JobState::Running.is_terminal());
        assert!(JobState::Succeeded.is_terminal());
        assert!(JobState::Failed.is_terminal());
        assert!(JobState::Cancelled.is_terminal());
    }

    #[test]
    fn default_policy_matches_cf3_defaults() {
        let p = PolicySpec::default();
        assert_eq!(p.target_file_size_bytes, 512 * 1024 * 1024);
        assert_eq!(p.min_input_files, 5);
        assert_eq!(p.snapshot_retention_count, 100);
        assert!(p.enabled);
        p.validate().expect("default policy is valid");
    }

    #[test]
    fn policy_validation_rejects_bad_fields() {
        let bad = PolicySpec {
            target_file_size_bytes: 0,
            ..PolicySpec::default()
        };
        assert!(bad.validate().is_err());
        let bad = PolicySpec {
            min_input_files: 0,
            ..PolicySpec::default()
        };
        assert!(bad.validate().is_err());
        let bad = PolicySpec {
            cost_cap_usd_month: Some(-1.0),
            ..PolicySpec::default()
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn first_of_month_normalizes() {
        let d = NaiveDate::from_ymd_opt(2026, 7, 3).unwrap();
        let p = first_of_month(d);
        assert_eq!(p.day(), 1);
        assert_eq!(p.month(), 7);
        assert_eq!(p.year(), 2026);
    }
}
