//! The incident ledger (Pillar E / E-F5): incident objects with a lifecycle
//! (open → acknowledged → resolved), ownership routing, blast radius, and a
//! per-table/product status roll-up.
//!
//! An incident is opened by two sources, both of which flow through
//! [`open_or_touch`]:
//!
//! - a **monitor** breach ([`crate::monitors`]) — the evaluation worker opens
//!   one when a zero-scan monitor breaches; and
//! - a **contract** violation ([`crate::contracts`]) — the circuit breaker's
//!   post-commit path opens one when a contract is violated (warn / quarantine /
//!   block all record an incident so the ledger is the single pane of glass).
//!
//! # De-duplication
//!
//! A flapping table must not open thousands of incidents. Every incident carries
//! a stable `dedup_key` (`{source}:{table_id}:{kind}`). While an incident for a
//! key is still *live* (open or acknowledged), a fresh breach **re-touches** it
//! (bumps `last_seen_at` + `occurrence_count`) instead of opening a duplicate —
//! enforced by the `incidents_live_dedup_idx` partial unique index. Once the
//! incident is resolved it leaves the index, so the same condition recurring
//! later opens a genuinely new incident.
//!
//! # Ownership + blast radius
//!
//! The owner is captured at open time from the table's `owner` property (never
//! inferred; a table with no owner opens an unowned incident, honestly). The
//! blast radius is the downstream asset set from the lineage impact function,
//! captured as jsonb at open time — the caller (which has the lineage crate)
//! computes it and passes it in, so this store module stays free of a lineage
//! dependency (lineage depends on the store, not the reverse).
//!
//! Every mutation writes its audit row + outbox event on the same transaction
//! as the state change, matching the contract/monitor discipline. The open/
//! resolve events feed the webhook delivery infra (a new `quality.incident.*`
//! event type), so an operator's Slack/pager is driven off the same durable
//! outbox as everything else.

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::monitors::Severity;
use crate::outbox::{self, NewOutboxEvent};

// ===========================================================================
// Enums
// ===========================================================================

/// What opened an incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    /// A monitor breach.
    Monitor,
    /// A data-contract violation (the circuit breaker).
    Contract,
}

impl Source {
    /// The database/wire rendering (matches the 0019 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Monitor => "monitor",
            Self::Contract => "contract",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "monitor" => Some(Self::Monitor),
            "contract" => Some(Self::Contract),
            _ => None,
        }
    }
}

/// The lifecycle status of an incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IncidentStatus {
    /// Newly opened, unacknowledged.
    Open,
    /// A human has acknowledged it (triage in progress).
    Acknowledged,
    /// Closed.
    Resolved,
}

impl IncidentStatus {
    /// The database/wire rendering (matches the 0019 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Acknowledged => "acknowledged",
            Self::Resolved => "resolved",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "open" => Some(Self::Open),
            "acknowledged" => Some(Self::Acknowledged),
            "resolved" => Some(Self::Resolved),
            _ => None,
        }
    }
}

/// The traffic-light status of a table or product: the worst live incident's
/// severity, or green when there are none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrafficLight {
    /// No live incidents.
    Green,
    /// Live incidents, none high severity.
    Yellow,
    /// At least one live high-severity incident.
    Red,
}

impl TrafficLight {
    /// The wire rendering.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Green => "green",
            Self::Yellow => "yellow",
            Self::Red => "red",
        }
    }
}

// ===========================================================================
// The persisted model
// ===========================================================================

/// A persisted incident.
#[derive(Debug, Clone)]
pub struct Incident {
    /// ULID of the incident.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// The table it is about.
    pub table_id: String,
    /// Denormalized human identity (`warehouse.ns.table`) at open time.
    pub table_ident: String,
    /// What opened it.
    pub source: Source,
    /// The monitor kind or contract violation kind.
    pub kind: String,
    /// Lifecycle status.
    pub status: IncidentStatus,
    /// Severity.
    pub severity: Severity,
    /// One-line human summary.
    pub title: String,
    /// Longer human detail.
    pub detail: String,
    /// Owner captured at open time (None when unowned).
    pub owner: Option<String>,
    /// Downstream blast radius (JSON array) at open time.
    pub blast_radius: Value,
    /// Originating monitor id (None for contract incidents / deleted monitors).
    pub monitor_id: Option<String>,
    /// Stable de-duplication key.
    pub dedup_key: String,
    /// Recurrence count while live.
    pub occurrence_count: i32,
    /// Who acknowledged it, if anyone.
    pub acknowledged_by: Option<String>,
    /// When it was acknowledged.
    pub acknowledged_at: Option<DateTime<Utc>>,
    /// Who resolved it, if anyone.
    pub resolved_by: Option<String>,
    /// When it was resolved.
    pub resolved_at: Option<DateTime<Utc>>,
    /// First occurrence.
    pub first_seen_at: DateTime<Utc>,
    /// Most-recent occurrence.
    pub last_seen_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct IncidentRow {
    id: String,
    workspace_id: String,
    table_id: String,
    table_ident: String,
    source: String,
    kind: String,
    status: String,
    severity: String,
    title: String,
    detail: String,
    owner: Option<String>,
    blast_radius: Value,
    monitor_id: Option<String>,
    dedup_key: String,
    occurrence_count: i32,
    acknowledged_by: Option<String>,
    acknowledged_at: Option<DateTime<Utc>>,
    resolved_by: Option<String>,
    resolved_at: Option<DateTime<Utc>>,
    first_seen_at: DateTime<Utc>,
    last_seen_at: DateTime<Utc>,
}

impl TryFrom<IncidentRow> for Incident {
    type Error = MeridianError;

    fn try_from(r: IncidentRow) -> Result<Self> {
        Ok(Self {
            id: r.id,
            workspace_id: r.workspace_id,
            table_id: r.table_id,
            table_ident: r.table_ident,
            source: Source::parse(&r.source).ok_or_else(|| {
                MeridianError::internal_msg(format!(
                    "incident row has unknown source {:?}",
                    r.source
                ))
            })?,
            kind: r.kind,
            status: IncidentStatus::parse(&r.status).ok_or_else(|| {
                MeridianError::internal_msg(format!(
                    "incident row has unknown status {:?}",
                    r.status
                ))
            })?,
            severity: Severity::parse(&r.severity).ok_or_else(|| {
                MeridianError::internal_msg(format!(
                    "incident row has unknown severity {:?}",
                    r.severity
                ))
            })?,
            title: r.title,
            detail: r.detail,
            owner: r.owner,
            blast_radius: r.blast_radius,
            monitor_id: r.monitor_id,
            dedup_key: r.dedup_key,
            occurrence_count: r.occurrence_count,
            acknowledged_by: r.acknowledged_by,
            acknowledged_at: r.acknowledged_at,
            resolved_by: r.resolved_by,
            resolved_at: r.resolved_at,
            first_seen_at: r.first_seen_at,
            last_seen_at: r.last_seen_at,
        })
    }
}

const INCIDENT_COLUMNS: &str = "id, workspace_id, table_id, table_ident, source, kind, status, \
     severity, title, detail, owner, blast_radius, monitor_id, dedup_key, occurrence_count, \
     acknowledged_by, acknowledged_at, resolved_by, resolved_at, first_seen_at, last_seen_at";

/// One status-history row: id, severity, kind, `first_seen_at`, `resolved_at`.
type HistoryRow = (String, String, String, DateTime<Utc>, Option<DateTime<Utc>>);

/// Builds the stable de-duplication key for an ongoing condition.
#[must_use]
pub fn dedup_key(source: Source, table_id: &str, kind: &str) -> String {
    format!("{}:{table_id}:{kind}", source.as_str())
}

// ===========================================================================
// Open / touch
// ===========================================================================

/// Everything needed to open (or re-touch) an incident.
#[derive(Debug, Clone)]
pub struct NewIncident<'a> {
    /// The table it is about.
    pub table_id: &'a str,
    /// Human identity at open time.
    pub table_ident: &'a str,
    /// What opened it.
    pub source: Source,
    /// The monitor kind or contract violation kind.
    pub kind: &'a str,
    /// Severity.
    pub severity: Severity,
    /// One-line summary.
    pub title: &'a str,
    /// Longer detail.
    pub detail: &'a str,
    /// Owner (None when unowned — never fabricate).
    pub owner: Option<&'a str>,
    /// Downstream blast radius (JSON array); pass `json!([])` when none.
    pub blast_radius: Value,
    /// Originating monitor id (None for contract incidents).
    pub monitor_id: Option<&'a str>,
}

/// The outcome of [`open_or_touch`]: whether a brand-new incident was opened
/// (and should notify) or an existing live one was re-touched.
#[derive(Debug, Clone)]
pub struct OpenOutcome {
    /// The incident id (new or existing).
    pub incident_id: String,
    /// True when a new incident was opened (worth a notification); false when an
    /// existing live incident was re-touched (deliberately quiet).
    pub opened: bool,
}

/// Opens a new incident, or re-touches the live one for the same `dedup_key`.
/// Runs on the **caller's** transaction so the incident and the monitor result
/// (or contract violation) that triggered it commit atomically.
///
/// De-duplication is handled by inserting and, on the partial-unique conflict,
/// updating the live row (bump `last_seen_at` + `occurrence_count`, refresh the
/// detail). This is a single round-trip and race-safe: two workers racing to
/// open the same condition serialize on the unique index, and the loser's insert
/// becomes the touch.
pub async fn open_or_touch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace_id: WorkspaceId,
    principal: &str,
    new: &NewIncident<'_>,
) -> Result<OpenOutcome> {
    let key = dedup_key(new.source, new.table_id, new.kind);
    let id = Ulid::new().to_string();

    // Insert-or-touch in one statement: the partial unique index makes a live
    // duplicate conflict; DO UPDATE bumps the existing live row. `xmax = 0`
    // distinguishes a fresh insert from an update in the RETURNING row.
    let row: (String, bool) = sqlx::query_as(
        "INSERT INTO incidents
             (id, workspace_id, table_id, table_ident, source, kind, status, severity,
              title, detail, owner, blast_radius, monitor_id, dedup_key)
         VALUES ($1, $2, $3, $4, $5, $6, 'open', $7, $8, $9, $10, $11, $12, $13)
         ON CONFLICT (workspace_id, dedup_key) WHERE status <> 'resolved'
         DO UPDATE SET
             occurrence_count = incidents.occurrence_count + 1,
             last_seen_at = now(),
             detail = EXCLUDED.detail
         RETURNING id, (xmax = 0) AS inserted",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(new.table_id)
    .bind(new.table_ident)
    .bind(new.source.as_str())
    .bind(new.kind)
    .bind(new.severity.as_str())
    .bind(new.title)
    .bind(new.detail)
    .bind(new.owner)
    .bind(&new.blast_radius)
    .bind(new.monitor_id)
    .bind(&key)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| map_sqlx_error("failed to open or touch incident", e))?;

    let (incident_id, opened) = row;

    if opened {
        let details = json!({
            "incident_id": incident_id,
            "table_id": new.table_id,
            "table_ident": new.table_ident,
            "source": new.source.as_str(),
            "kind": new.kind,
            "severity": new.severity.as_str(),
            "title": new.title,
            "detail": new.detail,
            "owner": new.owner,
            "blast_radius": new.blast_radius,
        });
        outbox::enqueue(
            &mut **tx,
            &NewOutboxEvent {
                workspace_id: Some(workspace_id),
                aggregate: format!("incident:{incident_id}"),
                event_type: "quality.incident.opened".to_owned(),
                payload: details.clone(),
            },
        )
        .await?;
        audit::append_in_tx(
            tx,
            NewAuditEntry {
                workspace_id: Some(workspace_id),
                principal: principal.to_owned(),
                action: "quality.incident.open".to_owned(),
                resource: format!("incident:{incident_id}"),
                details,
            },
        )
        .await?;
    }

    Ok(OpenOutcome {
        incident_id,
        opened,
    })
}

/// Opens or touches an incident in its own transaction (for callers without an
/// ambient transaction).
pub async fn open_or_touch_standalone(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    principal: &str,
    new: &NewIncident<'_>,
) -> Result<OpenOutcome> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin incident open", e))?;
    let outcome = open_or_touch(&mut tx, workspace_id, principal, new).await?;
    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit incident open", e))?;
    Ok(outcome)
}

// ===========================================================================
// Lifecycle transitions (ack / resolve)
// ===========================================================================

/// Acknowledges an open incident. Returns the updated incident, or
/// [`MeridianError::NotFound`] if it does not exist, or
/// [`MeridianError::Conflict`] if it is not `open`.
pub async fn acknowledge(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<Incident> {
    transition(pool, workspace_id, id, principal, Transition::Acknowledge).await
}

/// Resolves an open or acknowledged incident. Returns the updated incident, or
/// [`MeridianError::NotFound`] if it does not exist, or
/// [`MeridianError::Conflict`] if it is already resolved.
pub async fn resolve(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<Incident> {
    transition(pool, workspace_id, id, principal, Transition::Resolve).await
}

#[derive(Debug, Clone, Copy)]
enum Transition {
    Acknowledge,
    Resolve,
}

impl Transition {
    fn event_type(self) -> &'static str {
        match self {
            Self::Acknowledge => "quality.incident.acknowledged",
            Self::Resolve => "quality.incident.resolved",
        }
    }
    fn action(self) -> &'static str {
        match self {
            Self::Acknowledge => "quality.incident.acknowledge",
            Self::Resolve => "quality.incident.resolve",
        }
    }
}

/// Shared ack/resolve driver: loads the incident `FOR UPDATE`, validates the
/// transition, updates the lifecycle columns, and writes the audit row + outbox
/// event on one transaction.
async fn transition(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
    transition: Transition,
) -> Result<Incident> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin incident transition", e))?;

    let current: Option<IncidentRow> = sqlx::query_as(&format!(
        "SELECT {INCIDENT_COLUMNS} FROM incidents WHERE workspace_id = $1 AND id = $2 FOR UPDATE"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load incident for transition", e))?;
    let Some(current) = current else {
        return Err(MeridianError::NotFound(format!(
            "incident {id:?} does not exist"
        )));
    };
    let current = Incident::try_from(current)?;

    // Validate the transition against the current status. The two empty arms
    // (ack-from-open, resolve-from-anything-live) are the valid transitions;
    // they read as a state-machine table and are deliberately not merged.
    #[allow(clippy::match_same_arms)]
    match (transition, current.status) {
        (Transition::Acknowledge, IncidentStatus::Open) => {}
        (Transition::Acknowledge, IncidentStatus::Acknowledged) => {
            return Err(MeridianError::Conflict(
                "incident is already acknowledged".to_owned(),
            ));
        }
        (Transition::Acknowledge, IncidentStatus::Resolved) => {
            return Err(MeridianError::Conflict(
                "a resolved incident cannot be acknowledged".to_owned(),
            ));
        }
        (Transition::Resolve, IncidentStatus::Resolved) => {
            return Err(MeridianError::Conflict(
                "incident is already resolved".to_owned(),
            ));
        }
        (Transition::Resolve, _) => {}
    }

    let sql = match transition {
        Transition::Acknowledge => format!(
            "UPDATE incidents
             SET status = 'acknowledged', acknowledged_by = $3, acknowledged_at = now()
             WHERE workspace_id = $1 AND id = $2
             RETURNING {INCIDENT_COLUMNS}"
        ),
        Transition::Resolve => format!(
            "UPDATE incidents
             SET status = 'resolved', resolved_by = $3, resolved_at = now()
             WHERE workspace_id = $1 AND id = $2
             RETURNING {INCIDENT_COLUMNS}"
        ),
    };
    let updated: IncidentRow = sqlx::query_as(&sql)
        .bind(workspace_id.to_string())
        .bind(id)
        .bind(principal)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to update incident", e))?;

    let details = json!({
        "incident_id": id,
        "table_id": current.table_id,
        "table_ident": current.table_ident,
        "status": if matches!(transition, Transition::Acknowledge) { "acknowledged" } else { "resolved" },
        "severity": current.severity.as_str(),
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("incident:{id}"),
            event_type: transition.event_type().to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: transition.action().to_owned(),
            resource: format!("incident:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit incident transition", e))?;
    Incident::try_from(updated)
}

// ===========================================================================
// Reads
// ===========================================================================

/// Gets one incident by id.
pub async fn get(pool: &PgPool, workspace_id: WorkspaceId, id: &str) -> Result<Option<Incident>> {
    let row: Option<IncidentRow> = sqlx::query_as(&format!(
        "SELECT {INCIDENT_COLUMNS} FROM incidents WHERE workspace_id = $1 AND id = $2"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load incident", e))?;
    row.map(Incident::try_from).transpose()
}

/// Filter for an incidents query.
#[derive(Debug, Clone, Default)]
pub struct IncidentQuery<'a> {
    /// Restrict to one table.
    pub table_id: Option<&'a str>,
    /// Restrict to one status.
    pub status: Option<IncidentStatus>,
    /// Restrict to `open` + `acknowledged` (the live set) when true.
    pub live_only: bool,
}

/// Lists incidents for a workspace, most-recent first, optionally filtered.
/// Bounded by `limit`.
pub async fn list(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    filter: &IncidentQuery<'_>,
    limit: i64,
) -> Result<Vec<Incident>> {
    let status = filter.status.map(IncidentStatus::as_str);
    let rows: Vec<IncidentRow> = sqlx::query_as(&format!(
        "SELECT {INCIDENT_COLUMNS} FROM incidents
         WHERE workspace_id = $1
           AND ($2::text IS NULL OR table_id = $2)
           AND ($3::text IS NULL OR status = $3)
           AND ($4 = FALSE OR status <> 'resolved')
         ORDER BY last_seen_at DESC, id DESC
         LIMIT $5"
    ))
    .bind(workspace_id.to_string())
    .bind(filter.table_id)
    .bind(status)
    .bind(filter.live_only)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list incidents", e))?;
    rows.into_iter().map(Incident::try_from).collect()
}

/// The status roll-up for one table: its traffic light plus live-incident
/// counts by severity.
#[derive(Debug, Clone, Serialize)]
pub struct TableStatus {
    /// The table id.
    pub table_id: String,
    /// The traffic light (worst live severity, or green).
    pub light: TrafficLight,
    /// Count of live (open + acknowledged) incidents.
    pub live_incidents: i64,
    /// Count of live high-severity incidents.
    pub high: i64,
    /// Count of live medium-severity incidents.
    pub medium: i64,
    /// Count of live low-severity incidents.
    pub low: i64,
}

/// Computes the traffic-light + severity counts of the live incidents on a
/// table. A table with no live incidents is green.
pub async fn table_status(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
) -> Result<TableStatus> {
    let row: (i64, i64, i64) = sqlx::query_as(
        "SELECT
             COUNT(*) FILTER (WHERE severity = 'high')   AS high,
             COUNT(*) FILTER (WHERE severity = 'medium') AS medium,
             COUNT(*) FILTER (WHERE severity = 'low')    AS low
         FROM incidents
         WHERE workspace_id = $1 AND table_id = $2 AND status <> 'resolved'",
    )
    .bind(workspace_id.to_string())
    .bind(table_id)
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to compute table status", e))?;
    let (high, medium, low) = row;
    let live = high + medium + low;
    let light = traffic_light(high, live);
    Ok(TableStatus {
        table_id: table_id.to_owned(),
        light,
        live_incidents: live,
        high,
        medium,
        low,
    })
}

/// The traffic light from a live-incident tally: red if any high-severity,
/// yellow if any live at all, else green. Pure.
#[must_use]
pub fn traffic_light(high: i64, live_total: i64) -> TrafficLight {
    if high > 0 {
        TrafficLight::Red
    } else if live_total > 0 {
        TrafficLight::Yellow
    } else {
        TrafficLight::Green
    }
}

/// One point in a table's status history (an incident open or resolve event),
/// derived from the incident ledger. Newest first.
#[derive(Debug, Clone, Serialize)]
pub struct StatusEvent {
    /// The incident id.
    pub incident_id: String,
    /// `opened` or `resolved`.
    pub event: String,
    /// Severity.
    pub severity: String,
    /// The incident kind.
    pub kind: String,
    /// When it happened.
    pub at: DateTime<Utc>,
}

/// Reads a table's recent status history from its incidents: an `opened` point
/// at each `first_seen_at` and a `resolved` point at each `resolved_at`, merged
/// newest-first. Bounded by `limit` incidents.
pub async fn table_status_history(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
    limit: i64,
) -> Result<Vec<StatusEvent>> {
    let rows: Vec<HistoryRow> = sqlx::query_as(
        "SELECT id, severity, kind, first_seen_at, resolved_at
             FROM incidents
             WHERE workspace_id = $1 AND table_id = $2
             ORDER BY last_seen_at DESC
             LIMIT $3",
    )
    .bind(workspace_id.to_string())
    .bind(table_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to read table status history", e))?;

    let mut events: Vec<StatusEvent> = Vec::with_capacity(rows.len());
    for (id, severity, kind, opened_at, resolved_at) in rows {
        events.push(StatusEvent {
            incident_id: id.clone(),
            event: "opened".to_owned(),
            severity: severity.clone(),
            kind: kind.clone(),
            at: opened_at,
        });
        if let Some(resolved_at) = resolved_at {
            events.push(StatusEvent {
                incident_id: id,
                event: "resolved".to_owned(),
                severity,
                kind,
                at: resolved_at,
            });
        }
    }
    events.sort_by_key(|e| std::cmp::Reverse(e.at));
    Ok(events)
}

/// Counts live (open + acknowledged) incidents on a table. Used by the quality
/// score (a table with a live incident loses monitor-health points).
pub async fn count_live_for_table(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
) -> Result<i64> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM incidents
         WHERE workspace_id = $1 AND table_id = $2 AND status <> 'resolved'",
    )
    .bind(workspace_id.to_string())
    .bind(table_id)
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to count live incidents", e))?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_key_is_stable_and_scoped() {
        assert_eq!(
            dedup_key(Source::Monitor, "tbl123", "volume"),
            "monitor:tbl123:volume"
        );
        assert_ne!(
            dedup_key(Source::Monitor, "tbl123", "volume"),
            dedup_key(Source::Contract, "tbl123", "volume"),
        );
        assert_ne!(
            dedup_key(Source::Monitor, "tbl123", "volume"),
            dedup_key(Source::Monitor, "tbl123", "freshness"),
        );
    }

    #[test]
    fn traffic_light_rules() {
        assert_eq!(traffic_light(0, 0), TrafficLight::Green);
        assert_eq!(traffic_light(0, 3), TrafficLight::Yellow);
        assert_eq!(traffic_light(1, 3), TrafficLight::Red);
        assert_eq!(traffic_light(2, 2), TrafficLight::Red);
    }

    #[test]
    fn enum_round_trips() {
        for s in [Source::Monitor, Source::Contract] {
            assert_eq!(Source::parse(s.as_str()), Some(s));
        }
        for s in [
            IncidentStatus::Open,
            IncidentStatus::Acknowledged,
            IncidentStatus::Resolved,
        ] {
            assert_eq!(IncidentStatus::parse(s.as_str()), Some(s));
        }
    }
}
