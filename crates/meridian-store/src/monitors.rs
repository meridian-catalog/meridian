//! Zero-scan data-quality monitors (Pillar E / E-F1) and their append-only
//! result series. This is the *detection* half of the observability pillar: a
//! monitor is evaluated from the commit stream + the `table_snapshots`
//! write-through index + `metrics_reports` — **never** by scanning data files —
//! and a breach opens an incident (see [`crate::incidents`]).
//!
//! This module owns three things, cleanly separated (mirroring
//! [`crate::contracts`]'s discipline):
//!
//! 1. **The model** — [`Monitor`] and its typed [`MonitorConfig`], plus the
//!    [`MonitorResult`] record. Persistence mirrors the contract module:
//!    `monitors` holds the definition, `monitor_results` is the append-only
//!    series, and every mutation writes its audit row + outbox event on the
//!    same transaction as the state change.
//!
//! 2. **The pure evaluation engine** — [`MonitorKind::evaluate`] and the
//!    per-kind scorers ([`score_freshness`], [`score_volume`], …) are pure
//!    functions of a [`CommitObservation`] (the just-committed snapshot's
//!    numbers) and a [`History`] (the recent prior commits, summarized). They do
//!    **no I/O** and are exhaustively unit-tested here. Crucially they read only
//!    metadata the platform already indexes — snapshot summaries, timestamps —
//!    so evaluation is O(history window), never O(rows).
//!
//! 3. **CRUD + result recording** — [`create`]/[`update`]/[`delete`]/… manage
//!    monitor definitions; [`resolve_for_table`] finds the enabled monitors that
//!    bind to a table (directly or via its namespace chain, exactly like
//!    contract resolution); [`record_result`] appends one evaluation row.
//!
//! # Why zero-scan is honest, not a shortcut
//!
//! An Iceberg commit already tells us, in the snapshot summary, how many records
//! and files and bytes the table now holds and how many the commit added — and
//! the commit timestamp tells us *when*. Freshness, volume, file-size, and
//! snapshot-debt anomalies are all functions of those numbers across recent
//! commits. We never need to open a data file to detect "this load wrote 100×
//! the usual rows" or "the table has not been written in 3× its usual cadence".
//! What we cannot see this way (per-column null rates, value distributions) we
//! do not pretend to: those need a scan, and this module does not claim them.
//!
//! The anomaly math converts record/file/byte counts to `f64` for the ratio
//! curves and medians. Precision loss on a commit adding more than 2^52 rows is
//! irrelevant to an anomaly heuristic (and would be a far bigger problem than a
//! rounding error); the cast lints are allowed at module scope with that
//! rationale, matching [`crate::health`], rather than annotated at dozens of
//! arithmetic sites.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
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

// ===========================================================================
// Enums
// ===========================================================================

/// What a monitor binds to (identical semantics to
/// [`crate::contracts::BoundTo`], kept a distinct type so the two surfaces can
/// evolve independently).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BoundTo {
    /// A single table.
    Table,
    /// A namespace (all tables under it, resolved at evaluation time).
    Namespace,
}

impl BoundTo {
    /// The database/wire rendering (matches the 0019 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Namespace => "namespace",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "table" => Some(Self::Table),
            "namespace" => Some(Self::Namespace),
            _ => None,
        }
    }
}

impl std::fmt::Display for BoundTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The zero-scan signal a monitor computes. Each maps to one pure scorer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorKind {
    /// Commit recency vs. the learned/declared cadence: the table has not been
    /// written in longer than expected.
    Freshness,
    /// Rows/files/bytes added by a commit, anomaly-scored against the recent
    /// history (a spike or a collapse).
    Volume,
    /// Any schema evolution on the commit, breaking-change classified (reuses
    /// the contract schema-diff).
    SchemaChange,
    /// Average data-file size of the commit regressed sharply (small-file
    /// regression) vs. recent history.
    FileSize,
    /// The retained snapshot count or delete-file (DV) debt spiked.
    SnapshotDebt,
    /// A burst of failed/retried commits (commit-failure / retry storm).
    CommitFailure,
}

impl MonitorKind {
    /// The database/wire rendering (matches the 0019 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Freshness => "freshness",
            Self::Volume => "volume",
            Self::SchemaChange => "schema_change",
            Self::FileSize => "file_size",
            Self::SnapshotDebt => "snapshot_debt",
            Self::CommitFailure => "commit_failure",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "freshness" => Some(Self::Freshness),
            "volume" => Some(Self::Volume),
            "schema_change" => Some(Self::SchemaChange),
            "file_size" => Some(Self::FileSize),
            "snapshot_debt" => Some(Self::SnapshotDebt),
            "commit_failure" => Some(Self::CommitFailure),
            _ => None,
        }
    }

    /// The monitor kinds enabled by the sane-default opt-in (a table/namespace
    /// with monitoring on, no explicit list): the signals that are cheap and
    /// broadly useful. All six are default-on; the set is a function so the CLI
    /// and API share one source of truth.
    #[must_use]
    pub fn defaults() -> [Self; 6] {
        [
            Self::Freshness,
            Self::Volume,
            Self::SchemaChange,
            Self::FileSize,
            Self::SnapshotDebt,
            Self::CommitFailure,
        ]
    }
}

impl std::fmt::Display for MonitorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The severity an incident opened by a monitor carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// Advisory; visible but not paging.
    Low,
    /// The default.
    Medium,
    /// Operator-urgent.
    High,
}

impl Severity {
    /// The database/wire rendering (matches the 0019 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            _ => None,
        }
    }
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The outcome of one evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResultStatus {
    /// Within the expected band.
    Ok,
    /// Anomalous but below the incident threshold (recorded, not paged).
    Warn,
    /// A breach — opens an incident.
    Breach,
}

impl ResultStatus {
    /// The database/wire rendering (matches the 0019 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warn => "warn",
            Self::Breach => "breach",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "ok" => Some(Self::Ok),
            "warn" => Some(Self::Warn),
            "breach" => Some(Self::Breach),
            _ => None,
        }
    }
}

// ===========================================================================
// The typed config (jsonb)
// ===========================================================================

/// Default sensitivity: a commit-added value this many times the recent median
/// (or its reciprocal, for a collapse) is a breach. 5× is a deliberately loud
/// default — a real pipeline's per-commit volume is usually stable within a
/// small factor, so a 5× jump is a genuine anomaly, not noise.
pub const DEFAULT_VOLUME_FACTOR: f64 = 5.0;

/// Default file-size regression factor: the commit's average data-file size
/// dropping to below `1/factor` of the recent median average is a small-file
/// regression breach.
pub const DEFAULT_FILE_SIZE_FACTOR: f64 = 4.0;

/// Default freshness multiple: a gap since the last commit exceeding this
/// multiple of the learned median inter-commit interval is a staleness breach.
pub const DEFAULT_FRESHNESS_MULTIPLE: f64 = 3.0;

/// Default snapshot-count spike: retained snapshots exceeding the recent median
/// by this factor is a snapshot-debt breach.
pub const DEFAULT_SNAPSHOT_DEBT_FACTOR: f64 = 3.0;

/// Minimum history points before an anomaly scorer will fire. With fewer prior
/// commits than this the baseline is not yet trustworthy, so the scorer returns
/// [`ResultStatus::Ok`] with a "learning" detail rather than a false positive.
pub const MIN_HISTORY: usize = 3;

/// The typed monitor config (stored as jsonb). Every field is optional with a
/// documented default so `{}` is a valid config (use the defaults) and a
/// partial config overrides only what it names.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MonitorConfig {
    /// Freshness: an explicit max-staleness in seconds. When set, the freshness
    /// monitor breaches on a gap exceeding this absolute budget, *instead of*
    /// the learned-cadence multiple (a declared SLA beats a learned one).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_secs: Option<i64>,
    /// Freshness: the multiple of the learned median inter-commit interval that
    /// counts as stale (default [`DEFAULT_FRESHNESS_MULTIPLE`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness_multiple: Option<f64>,
    /// Volume: the spike/collapse factor vs. the recent median
    /// (default [`DEFAULT_VOLUME_FACTOR`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_factor: Option<f64>,
    /// File-size: the small-file regression factor (default
    /// [`DEFAULT_FILE_SIZE_FACTOR`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size_factor: Option<f64>,
    /// Snapshot-debt: the retained-snapshot spike factor (default
    /// [`DEFAULT_SNAPSHOT_DEBT_FACTOR`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_debt_factor: Option<f64>,
    /// Schema-change: when `true`, *any* schema change breaches; when `false`
    /// (the default), only a breaking change (drop/narrow/tighten) breaches and
    /// an additive change is a `warn`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub schema_any_change_breaches: bool,
}

impl MonitorConfig {
    fn freshness_multiple(&self) -> f64 {
        self.freshness_multiple
            .filter(|v| *v > 0.0)
            .unwrap_or(DEFAULT_FRESHNESS_MULTIPLE)
    }
    fn volume_factor(&self) -> f64 {
        self.volume_factor
            .filter(|v| *v > 1.0)
            .unwrap_or(DEFAULT_VOLUME_FACTOR)
    }
    fn file_size_factor(&self) -> f64 {
        self.file_size_factor
            .filter(|v| *v > 1.0)
            .unwrap_or(DEFAULT_FILE_SIZE_FACTOR)
    }
    fn snapshot_debt_factor(&self) -> f64 {
        self.snapshot_debt_factor
            .filter(|v| *v > 1.0)
            .unwrap_or(DEFAULT_SNAPSHOT_DEBT_FACTOR)
    }
}

// ===========================================================================
// The pure evaluation engine
// ===========================================================================

/// The just-committed snapshot, summarized into the numbers the scorers read.
/// Every field is derived from the `table_snapshots` write-through index — no
/// data-file access. `None` fields are signals the summary did not carry (the
/// scorers skip a signal they cannot measure rather than guessing).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CommitObservation {
    /// The committed snapshot id.
    pub snapshot_id: i64,
    /// Commit timestamp (epoch millis).
    pub timestamp_ms: i64,
    /// Records added by this commit (`added-records`).
    pub added_records: Option<i64>,
    /// Total records the table now holds (`total-records`).
    pub total_records: Option<i64>,
    /// Data files added by this commit (`added-data-files`).
    pub added_data_files: Option<i64>,
    /// Bytes added by this commit (`added-files-size`).
    pub added_files_size: Option<i64>,
    /// Total retained snapshots (from the index row count, not the summary).
    pub snapshot_count: i64,
    /// Total delete/DV files the table now holds (`total-delete-files`).
    pub total_delete_files: Option<i64>,
    /// The commit operation (`append`/`overwrite`/`delete`/…), when present.
    pub operation: Option<String>,
}

impl CommitObservation {
    /// The average bytes-per-file this commit wrote, when both are known and
    /// non-zero — the file-size signal.
    #[must_use]
    pub fn avg_added_file_bytes(&self) -> Option<f64> {
        match (self.added_files_size, self.added_data_files) {
            (Some(bytes), Some(files)) if files > 0 && bytes >= 0 => {
                Some(bytes as f64 / files as f64)
            }
            _ => None,
        }
    }
}

/// The recent prior commits, summarized for baselining. Ordered newest-first is
/// not required; the scorers compute order-independent statistics (median,
/// count). Excludes the commit under evaluation.
#[derive(Debug, Clone, Default)]
pub struct History {
    /// Prior commit timestamps (epoch millis), any order.
    pub timestamps_ms: Vec<i64>,
    /// Prior per-commit `added-records`, for the volume baseline.
    pub added_records: Vec<i64>,
    /// Prior per-commit average file bytes, for the file-size baseline.
    pub avg_file_bytes: Vec<f64>,
    /// Prior retained-snapshot counts, for the snapshot-debt baseline.
    pub snapshot_counts: Vec<i64>,
    /// Prior per-commit `total-delete-files`, for the DV-debt baseline.
    pub delete_files: Vec<i64>,
}

/// One monitor finding: the numbers compared + a stable machine token + human
/// detail. Produced by [`MonitorKind::evaluate`]; persisted to
/// `monitor_results` and (on a breach) carried into an incident.
#[derive(Debug, Clone, PartialEq)]
pub struct Evaluation {
    /// The outcome.
    pub status: ResultStatus,
    /// The measured value (None when not measurable this pass).
    pub observed: Option<f64>,
    /// The baseline it was compared against (None when no history).
    pub baseline: Option<f64>,
    /// Stable machine token, e.g. `volume-spike`, `breaking-schema-change`.
    pub result_kind: String,
    /// Human-readable detail.
    pub detail: String,
}

impl Evaluation {
    fn ok(result_kind: &str, detail: impl Into<String>) -> Self {
        Self {
            status: ResultStatus::Ok,
            observed: None,
            baseline: None,
            result_kind: result_kind.to_owned(),
            detail: detail.into(),
        }
    }
}

/// The median of a slice of numbers, or `None` when empty. Sorts a copy;
/// callers pass small history windows so the clone is cheap.
fn median_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted: Vec<f64> = values.iter().copied().filter(|v| v.is_finite()).collect();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        Some(sorted[mid])
    } else {
        Some(f64::midpoint(sorted[mid - 1], sorted[mid]))
    }
}

/// Median of an `i64` slice as `f64`.
fn median_i64(values: &[i64]) -> Option<f64> {
    let as_f: Vec<f64> = values.iter().map(|v| *v as f64).collect();
    median_f64(&as_f)
}

/// The median gap (millis) between consecutive commits, given the prior commit
/// timestamps plus the current one. `None` when there are fewer than two points.
fn median_interval_ms(prior: &[i64], current_ms: i64) -> Option<f64> {
    let mut all: Vec<i64> = prior.to_vec();
    all.push(current_ms);
    all.sort_unstable();
    if all.len() < 2 {
        return None;
    }
    let gaps: Vec<f64> = all.windows(2).map(|w| (w[1] - w[0]) as f64).collect();
    median_f64(&gaps)
}

/// Scores commit recency against the learned cadence (or a declared SLA).
///
/// With a declared `max_staleness_secs`, this compares the gap since the prior
/// commit to that absolute budget. Otherwise it learns the median inter-commit
/// interval from history and breaches when the newest gap exceeds
/// `freshness_multiple ×` that median. Freshness is evaluated *at commit time*
/// against the gap the commit just closed — a table that commits regularly is
/// fresh; the worker also re-checks idle tables (see the worker), but the pure
/// scorer here answers "was this commit late?".
#[must_use]
pub fn score_freshness(
    obs: &CommitObservation,
    hist: &History,
    config: &MonitorConfig,
) -> Evaluation {
    // The gap this commit closed: newest prior timestamp -> this commit.
    let last_prior = hist.timestamps_ms.iter().copied().max();
    let Some(last_prior) = last_prior else {
        return Evaluation::ok(
            "freshness-learning",
            "no prior commit to measure a gap against",
        );
    };
    let gap_ms = (obs.timestamp_ms - last_prior).max(0) as f64;

    // A declared SLA wins over a learned cadence.
    if let Some(budget_secs) = config.max_staleness_secs.filter(|s| *s > 0) {
        let budget_ms = budget_secs as f64 * 1000.0;
        if gap_ms > budget_ms {
            return Evaluation {
                status: ResultStatus::Breach,
                observed: Some(gap_ms / 1000.0),
                baseline: Some(budget_secs as f64),
                result_kind: "stale-sla".to_owned(),
                detail: format!(
                    "commit gap {:.0}s exceeds the declared max-staleness {budget_secs}s",
                    gap_ms / 1000.0
                ),
            };
        }
        return Evaluation {
            status: ResultStatus::Ok,
            observed: Some(gap_ms / 1000.0),
            baseline: Some(budget_secs as f64),
            result_kind: "fresh".to_owned(),
            detail: format!("commit gap {:.0}s within the declared SLA", gap_ms / 1000.0),
        };
    }

    // Learned cadence: need enough history for a trustworthy median.
    if hist.timestamps_ms.len() < MIN_HISTORY {
        return Evaluation::ok(
            "freshness-learning",
            "not enough commit history to learn a cadence yet",
        );
    }
    let Some(median_gap) = median_interval_ms(&hist.timestamps_ms, obs.timestamp_ms) else {
        return Evaluation::ok("freshness-learning", "cadence not measurable");
    };
    if median_gap <= 0.0 {
        return Evaluation::ok("fresh", "commits are effectively simultaneous");
    }
    let multiple = config.freshness_multiple();
    let ratio = gap_ms / median_gap;
    if ratio > multiple {
        Evaluation {
            status: ResultStatus::Breach,
            observed: Some(gap_ms / 1000.0),
            baseline: Some(median_gap / 1000.0),
            result_kind: "stale".to_owned(),
            detail: format!(
                "commit gap {:.0}s is {ratio:.1}× the learned cadence {:.0}s (threshold {multiple:.1}×)",
                gap_ms / 1000.0,
                median_gap / 1000.0
            ),
        }
    } else {
        Evaluation {
            status: ResultStatus::Ok,
            observed: Some(gap_ms / 1000.0),
            baseline: Some(median_gap / 1000.0),
            result_kind: "fresh".to_owned(),
            detail: format!(
                "commit gap {:.0}s within {multiple:.1}× the learned cadence",
                gap_ms / 1000.0
            ),
        }
    }
}

/// Scores the rows added by a commit against the recent median added-rows.
/// A spike (≥ factor× median) or a collapse (≤ median / factor, when the median
/// is non-trivial) is a breach. Skips when the commit carries no `added-records`
/// or when history is too short to baseline.
#[must_use]
pub fn score_volume(obs: &CommitObservation, hist: &History, config: &MonitorConfig) -> Evaluation {
    let Some(added) = obs.added_records else {
        return Evaluation::ok("volume-skip", "commit summary carries no added-records");
    };
    if hist.added_records.len() < MIN_HISTORY {
        return Evaluation::ok(
            "volume-learning",
            "not enough history to baseline volume yet",
        );
    }
    let Some(median) = median_i64(&hist.added_records) else {
        return Evaluation::ok("volume-learning", "volume baseline not measurable");
    };
    let factor = config.volume_factor();
    let observed = added as f64;
    // A near-zero baseline makes ratios meaningless; require a small floor.
    if median < 1.0 {
        // From a ~empty baseline, any sizeable write is expected growth, not an
        // anomaly; only flag if the baseline genuinely had rows.
        return Evaluation {
            status: ResultStatus::Ok,
            observed: Some(observed),
            baseline: Some(median),
            result_kind: "volume-ok".to_owned(),
            detail: format!("added {added} rows; baseline too small to anomaly-score"),
        };
    }
    if observed >= median * factor {
        return Evaluation {
            status: ResultStatus::Breach,
            observed: Some(observed),
            baseline: Some(median),
            result_kind: "volume-spike".to_owned(),
            detail: format!(
                "commit added {added} rows, {:.1}× the recent median {median:.0} (threshold {factor:.1}×)",
                observed / median
            ),
        };
    }
    if observed <= median / factor {
        return Evaluation {
            status: ResultStatus::Breach,
            observed: Some(observed),
            baseline: Some(median),
            result_kind: "volume-drop".to_owned(),
            detail: format!(
                "commit added {added} rows, only 1/{:.1} of the recent median {median:.0}",
                median / observed.max(1.0)
            ),
        };
    }
    Evaluation {
        status: ResultStatus::Ok,
        observed: Some(observed),
        baseline: Some(median),
        result_kind: "volume-ok".to_owned(),
        detail: format!("commit added {added} rows, within band of median {median:.0}"),
    }
}

/// Scores the commit's average data-file size against the recent median average.
/// A regression to below `median / factor` is a small-file breach (the compaction
/// story is Pillar C; here it is a *quality* signal — a producer suddenly writing
/// tiny files). Skips without measurable file sizes or enough history.
#[must_use]
pub fn score_file_size(
    obs: &CommitObservation,
    hist: &History,
    config: &MonitorConfig,
) -> Evaluation {
    let Some(avg) = obs.avg_added_file_bytes() else {
        return Evaluation::ok("file-size-skip", "commit added no measurable data files");
    };
    if hist.avg_file_bytes.len() < MIN_HISTORY {
        return Evaluation::ok(
            "file-size-learning",
            "not enough history to baseline file size",
        );
    }
    let Some(median) = median_f64(&hist.avg_file_bytes) else {
        return Evaluation::ok("file-size-learning", "file-size baseline not measurable");
    };
    if median <= 0.0 {
        return Evaluation::ok("file-size-ok", "file-size baseline is zero; not scored");
    }
    let factor = config.file_size_factor();
    if avg <= median / factor {
        Evaluation {
            status: ResultStatus::Breach,
            observed: Some(avg),
            baseline: Some(median),
            result_kind: "file-size-regression".to_owned(),
            detail: format!(
                "commit wrote {avg:.0}-byte avg files, 1/{:.1} of the recent median {median:.0} bytes (small-file regression)",
                median / avg.max(1.0)
            ),
        }
    } else {
        Evaluation {
            status: ResultStatus::Ok,
            observed: Some(avg),
            baseline: Some(median),
            result_kind: "file-size-ok".to_owned(),
            detail: format!(
                "commit avg file size {avg:.0} bytes within band of median {median:.0}"
            ),
        }
    }
}

/// Scores retained-snapshot count and delete-file (DV) debt against their recent
/// medians. Either exceeding `factor× median` is a debt breach. This detects a
/// runaway snapshot chain (expiry not keeping up) or a DV pile-up, both from the
/// index alone.
#[must_use]
pub fn score_snapshot_debt(
    obs: &CommitObservation,
    hist: &History,
    config: &MonitorConfig,
) -> Evaluation {
    let factor = config.snapshot_debt_factor();

    // Snapshot-count spike.
    if hist.snapshot_counts.len() >= MIN_HISTORY
        && let Some(median) = median_i64(&hist.snapshot_counts)
        && median >= 1.0
        && (obs.snapshot_count as f64) >= median * factor
    {
        return Evaluation {
            status: ResultStatus::Breach,
            observed: Some(obs.snapshot_count as f64),
            baseline: Some(median),
            result_kind: "snapshot-debt".to_owned(),
            detail: format!(
                "retained snapshots {} is {:.1}× the recent median {median:.0} (expiry falling behind)",
                obs.snapshot_count,
                obs.snapshot_count as f64 / median
            ),
        };
    }

    // DV / delete-file pile-up.
    if let Some(deletes) = obs.total_delete_files
        && hist.delete_files.len() >= MIN_HISTORY
        && let Some(median) = median_i64(&hist.delete_files)
        && median >= 1.0
        && (deletes as f64) >= median * factor
    {
        return Evaluation {
            status: ResultStatus::Breach,
            observed: Some(deletes as f64),
            baseline: Some(median),
            result_kind: "delete-debt".to_owned(),
            detail: format!(
                "delete/DV files {deletes} is {:.1}× the recent median {median:.0}",
                deletes as f64 / median
            ),
        };
    }

    Evaluation {
        status: ResultStatus::Ok,
        observed: Some(obs.snapshot_count as f64),
        baseline: median_i64(&hist.snapshot_counts),
        result_kind: "snapshot-debt-ok".to_owned(),
        detail: format!("retained snapshots {} within band", obs.snapshot_count),
    }
}

/// Scores a schema change. `changed` is whether the commit evolved the schema at
/// all; `breaking` is whether it was a breaking change (drop/narrow/tighten),
/// as classified by the contract schema-diff. A breaking change is always a
/// breach; an additive change is a `warn` (or a breach when the config asks for
/// any-change-breaches). No history needed — schema change is a per-commit fact.
#[must_use]
pub fn score_schema_change(changed: bool, breaking: bool, config: &MonitorConfig) -> Evaluation {
    if !changed {
        return Evaluation::ok("schema-stable", "the commit did not change the schema");
    }
    if breaking {
        return Evaluation {
            status: ResultStatus::Breach,
            observed: None,
            baseline: None,
            result_kind: "breaking-schema-change".to_owned(),
            detail: "the commit made a breaking schema change (drop / narrow / tighten)".to_owned(),
        };
    }
    if config.schema_any_change_breaches {
        return Evaluation {
            status: ResultStatus::Breach,
            observed: None,
            baseline: None,
            result_kind: "schema-change".to_owned(),
            detail: "the commit changed the schema (monitor set to flag any change)".to_owned(),
        };
    }
    Evaluation {
        status: ResultStatus::Warn,
        observed: None,
        baseline: None,
        result_kind: "additive-schema-change".to_owned(),
        detail: "the commit made an additive (non-breaking) schema change".to_owned(),
    }
}

/// Scores a commit-failure / retry storm. `recent_failures` is the count of
/// failed/retried commit attempts on this table inside the monitor's window (the
/// worker supplies it from the audit/event trail); `threshold` breaches at or
/// above. This is the one signal not derived from a *successful* commit's
/// summary — it is about commits that did **not** land — so the worker feeds the
/// count in rather than the observation.
#[must_use]
pub fn score_commit_failure(recent_failures: i64, threshold: i64) -> Evaluation {
    if recent_failures >= threshold && threshold > 0 {
        Evaluation {
            status: ResultStatus::Breach,
            observed: Some(recent_failures as f64),
            baseline: Some(threshold as f64),
            result_kind: "commit-failure-storm".to_owned(),
            detail: format!(
                "{recent_failures} failed/retried commit attempts in the window (threshold {threshold})"
            ),
        }
    } else {
        Evaluation {
            status: ResultStatus::Ok,
            observed: Some(recent_failures as f64),
            baseline: Some(threshold as f64),
            result_kind: "commit-failure-ok".to_owned(),
            detail: format!("{recent_failures} failed commit attempts in the window"),
        }
    }
}

impl MonitorKind {
    /// Evaluates this monitor kind against the observation and history. The
    /// commit-failure kind is *not* evaluated here (it needs the failure count,
    /// not a successful-commit observation); the worker scores it directly via
    /// [`score_commit_failure`]. Schema change is likewise scored directly by
    /// the worker (which has the base+staged schemas); a call here for
    /// `SchemaChange` returns a benign skip.
    #[must_use]
    pub fn evaluate(
        self,
        obs: &CommitObservation,
        hist: &History,
        config: &MonitorConfig,
    ) -> Evaluation {
        match self {
            Self::Freshness => score_freshness(obs, hist, config),
            Self::Volume => score_volume(obs, hist, config),
            Self::FileSize => score_file_size(obs, hist, config),
            Self::SnapshotDebt => score_snapshot_debt(obs, hist, config),
            // These two are scored by the worker with inputs the observation
            // does not carry; a direct call is a no-op skip.
            Self::SchemaChange => {
                Evaluation::ok("schema-skip", "schema change scored by the worker")
            }
            Self::CommitFailure => {
                Evaluation::ok("commit-failure-skip", "commit failure scored by the worker")
            }
        }
    }
}

// ===========================================================================
// The persisted model
// ===========================================================================

/// A persisted monitor definition.
#[derive(Debug, Clone)]
pub struct Monitor {
    /// ULID of the monitor.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Human name (unique per workspace).
    pub name: String,
    /// What it binds to.
    pub bound_to: BoundTo,
    /// The bound securable's id.
    pub securable_id: String,
    /// The zero-scan signal.
    pub kind: MonitorKind,
    /// Whether in force.
    pub enabled: bool,
    /// Severity of an incident this monitor opens.
    pub severity: Severity,
    /// The typed config.
    pub config: MonitorConfig,
    /// Creating principal.
    pub created_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last-update time.
    pub updated_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct MonitorRow {
    id: String,
    workspace_id: String,
    name: String,
    bound_to: String,
    securable_id: String,
    kind: String,
    enabled: bool,
    severity: String,
    config: Value,
    created_by: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<MonitorRow> for Monitor {
    type Error = MeridianError;

    fn try_from(r: MonitorRow) -> Result<Self> {
        Ok(Self {
            id: r.id,
            workspace_id: r.workspace_id,
            name: r.name,
            bound_to: BoundTo::parse(&r.bound_to).ok_or_else(|| {
                MeridianError::internal_msg(format!(
                    "monitor row has unknown bound_to {:?}",
                    r.bound_to
                ))
            })?,
            securable_id: r.securable_id,
            kind: MonitorKind::parse(&r.kind).ok_or_else(|| {
                MeridianError::internal_msg(format!("monitor row has unknown kind {:?}", r.kind))
            })?,
            enabled: r.enabled,
            severity: Severity::parse(&r.severity).ok_or_else(|| {
                MeridianError::internal_msg(format!(
                    "monitor row has unknown severity {:?}",
                    r.severity
                ))
            })?,
            config: serde_json::from_value(r.config)
                .map_err(|e| MeridianError::internal("monitor row has an unparseable config", e))?,
            created_by: r.created_by,
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
    }
}

const MONITOR_COLUMNS: &str = "id, workspace_id, name, bound_to, securable_id, kind, enabled, \
     severity, config, created_by, created_at, updated_at";

/// A persisted evaluation result.
#[derive(Debug, Clone)]
pub struct MonitorResult {
    /// ULID of the result row.
    pub id: String,
    /// The monitor.
    pub monitor_id: String,
    /// The table evaluated.
    pub table_id: String,
    /// The monitor kind (denormalized).
    pub kind: String,
    /// The outcome.
    pub status: ResultStatus,
    /// The measured value, when measurable.
    pub observed_value: Option<f64>,
    /// The baseline, when there was one.
    pub baseline_value: Option<f64>,
    /// Stable classification token.
    pub result_kind: String,
    /// Human detail.
    pub detail: String,
    /// The head snapshot the evaluation ran against, when known.
    pub snapshot_id: Option<i64>,
    /// When it ran.
    pub evaluated_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct ResultRow {
    id: String,
    monitor_id: String,
    table_id: String,
    kind: String,
    status: String,
    observed_value: Option<f64>,
    baseline_value: Option<f64>,
    result_kind: String,
    detail: String,
    snapshot_id: Option<i64>,
    evaluated_at: DateTime<Utc>,
}

impl TryFrom<ResultRow> for MonitorResult {
    type Error = MeridianError;

    fn try_from(r: ResultRow) -> Result<Self> {
        Ok(Self {
            id: r.id,
            monitor_id: r.monitor_id,
            table_id: r.table_id,
            kind: r.kind,
            status: ResultStatus::parse(&r.status).ok_or_else(|| {
                MeridianError::internal_msg(format!("result row has unknown status {:?}", r.status))
            })?,
            observed_value: r.observed_value,
            baseline_value: r.baseline_value,
            result_kind: r.result_kind,
            detail: r.detail,
            snapshot_id: r.snapshot_id,
            evaluated_at: r.evaluated_at,
        })
    }
}

const RESULT_COLUMNS: &str = "id, monitor_id, table_id, kind, status, observed_value, \
     baseline_value, result_kind, detail, snapshot_id, evaluated_at";

// ===========================================================================
// CRUD
// ===========================================================================

/// Fields required to create a monitor.
#[derive(Debug, Clone)]
pub struct NewMonitor<'a> {
    /// Human name, unique per workspace.
    pub name: &'a str,
    /// What to bind to.
    pub bound_to: BoundTo,
    /// The bound securable's id.
    pub securable_id: &'a str,
    /// The zero-scan signal.
    pub kind: MonitorKind,
    /// Severity of incidents this monitor opens.
    pub severity: Severity,
    /// The typed config.
    pub config: &'a MonitorConfig,
}

/// Creates a monitor. Returns [`MeridianError::Conflict`] if the name is taken
/// or a monitor of this kind already binds to this securable, and
/// [`MeridianError::Validation`] if the name is empty.
pub async fn create(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    new: NewMonitor<'_>,
    principal: &str,
) -> Result<Monitor> {
    if new.name.trim().is_empty() {
        return Err(MeridianError::Validation(
            "monitor name must be non-empty".to_owned(),
        ));
    }
    let config_json = serde_json::to_value(new.config)
        .map_err(|e| MeridianError::internal("failed to serialize monitor config", e))?;

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin monitor create", e))?;

    let id = Ulid::new().to_string();
    let row: MonitorRow = sqlx::query_as(&format!(
        "INSERT INTO monitors
             (id, workspace_id, name, bound_to, securable_id, kind, enabled, severity, config,
              created_by)
         VALUES ($1, $2, $3, $4, $5, $6, TRUE, $7, $8, $9)
         RETURNING {MONITOR_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(new.name)
    .bind(new.bound_to.as_str())
    .bind(new.securable_id)
    .bind(new.kind.as_str())
    .bind(new.severity.as_str())
    .bind(&config_json)
    .bind(principal)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!(
                "a {} monitor already exists for this securable, or the name {:?} is taken",
                new.kind, new.name
            ))
        } else {
            map_sqlx_error("failed to insert monitor", e)
        }
    })?;

    let details = json!({
        "name": new.name,
        "bound_to": new.bound_to.as_str(),
        "securable_id": new.securable_id,
        "kind": new.kind.as_str(),
        "severity": new.severity.as_str(),
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("monitor:{id}"),
            event_type: "quality.monitor.created".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "quality.monitor.create".to_owned(),
            resource: format!("monitor:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit monitor create", e))?;
    Monitor::try_from(row)
}

/// Fields an update may change. `None` leaves a field unchanged. The binding and
/// kind are fixed at creation (a different binding/kind is a different monitor).
#[derive(Debug, Clone, Default)]
pub struct MonitorUpdate {
    /// New enabled flag.
    pub enabled: Option<bool>,
    /// New severity.
    pub severity: Option<Severity>,
    /// New config.
    pub config: Option<MonitorConfig>,
}

/// Updates a monitor. Returns [`MeridianError::NotFound`] if it does not exist.
pub async fn update(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    change: MonitorUpdate,
    principal: &str,
) -> Result<Monitor> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin monitor update", e))?;

    let current: Option<MonitorRow> = sqlx::query_as(&format!(
        "SELECT {MONITOR_COLUMNS} FROM monitors WHERE workspace_id = $1 AND id = $2 FOR UPDATE"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load monitor for update", e))?;
    let Some(current) = current else {
        return Err(MeridianError::NotFound(format!(
            "monitor {id:?} does not exist"
        )));
    };
    let current = Monitor::try_from(current)?;

    let new_enabled = change.enabled.unwrap_or(current.enabled);
    let new_severity = change.severity.unwrap_or(current.severity);
    let new_config = change.config.unwrap_or(current.config);
    let config_json = serde_json::to_value(&new_config)
        .map_err(|e| MeridianError::internal("failed to serialize monitor config", e))?;

    let row: MonitorRow = sqlx::query_as(&format!(
        "UPDATE monitors
         SET enabled = $3, severity = $4, config = $5, updated_at = now()
         WHERE workspace_id = $1 AND id = $2
         RETURNING {MONITOR_COLUMNS}"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .bind(new_enabled)
    .bind(new_severity.as_str())
    .bind(&config_json)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to update monitor", e))?;

    let details = json!({
        "name": current.name,
        "enabled": new_enabled,
        "severity": new_severity.as_str(),
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("monitor:{id}"),
            event_type: "quality.monitor.updated".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "quality.monitor.update".to_owned(),
            resource: format!("monitor:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit monitor update", e))?;
    Monitor::try_from(row)
}

/// Deletes a monitor (its results cascade; incidents keep the row with a NULL
/// `monitor_id`). Returns [`MeridianError::NotFound`] if it does not exist.
pub async fn delete(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin monitor delete", e))?;

    let deleted = sqlx::query("DELETE FROM monitors WHERE workspace_id = $1 AND id = $2")
        .bind(workspace_id.to_string())
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to delete monitor", e))?;
    if deleted.rows_affected() == 0 {
        return Err(MeridianError::NotFound(format!(
            "monitor {id:?} does not exist"
        )));
    }

    let details = json!({ "monitor_id": id });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("monitor:{id}"),
            event_type: "quality.monitor.deleted".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "quality.monitor.delete".to_owned(),
            resource: format!("monitor:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit monitor delete", e))?;
    Ok(())
}

/// Gets one monitor by id.
pub async fn get(pool: &PgPool, workspace_id: WorkspaceId, id: &str) -> Result<Option<Monitor>> {
    let row: Option<MonitorRow> = sqlx::query_as(&format!(
        "SELECT {MONITOR_COLUMNS} FROM monitors WHERE workspace_id = $1 AND id = $2"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load monitor", e))?;
    row.map(Monitor::try_from).transpose()
}

/// Lists monitors in a workspace, newest first.
pub async fn list(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<Monitor>> {
    let rows: Vec<MonitorRow> = sqlx::query_as(&format!(
        "SELECT {MONITOR_COLUMNS} FROM monitors WHERE workspace_id = $1 ORDER BY id DESC"
    ))
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list monitors", e))?;
    rows.into_iter().map(Monitor::try_from).collect()
}

/// Resolves the **enabled** monitors that bind to a table: those bound directly
/// to the table id, plus those bound to any namespace in the table's
/// self-and-ancestors chain. `namespace_ids` is that chain (the RBAC scope
/// builder already computes it). Excludes disabled monitors.
pub async fn resolve_for_table(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
    namespace_ids: &[String],
) -> Result<Vec<Monitor>> {
    let rows: Vec<MonitorRow> = sqlx::query_as(&format!(
        "SELECT {MONITOR_COLUMNS} FROM monitors
         WHERE workspace_id = $1
           AND enabled = TRUE
           AND (
                (bound_to = 'table' AND securable_id = $2)
             OR (bound_to = 'namespace' AND securable_id = ANY($3))
           )
         ORDER BY id"
    ))
    .bind(workspace_id.to_string())
    .bind(table_id)
    .bind(namespace_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to resolve monitors for table", e))?;
    rows.into_iter().map(Monitor::try_from).collect()
}

// ===========================================================================
// Result recording + reads
// ===========================================================================

/// One evaluation result to append.
#[derive(Debug, Clone)]
pub struct NewResult<'a> {
    /// The monitor.
    pub monitor_id: &'a str,
    /// The table evaluated.
    pub table_id: &'a str,
    /// The monitor kind.
    pub kind: MonitorKind,
    /// The evaluation outcome.
    pub eval: &'a Evaluation,
    /// The head snapshot the evaluation ran against, when known.
    pub snapshot_id: Option<i64>,
}

/// Appends one `monitor_results` row on the caller's transaction. Used by the
/// evaluation worker so the result row and (on a breach) the incident are one
/// atomic write.
pub async fn record_result_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace_id: WorkspaceId,
    new: &NewResult<'_>,
) -> Result<String> {
    let id = Ulid::new().to_string();
    sqlx::query(
        "INSERT INTO monitor_results
             (id, workspace_id, monitor_id, table_id, kind, status, observed_value,
              baseline_value, result_kind, detail, snapshot_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(new.monitor_id)
    .bind(new.table_id)
    .bind(new.kind.as_str())
    .bind(new.eval.status.as_str())
    .bind(new.eval.observed)
    .bind(new.eval.baseline)
    .bind(&new.eval.result_kind)
    .bind(&new.eval.detail)
    .bind(new.snapshot_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| map_sqlx_error("failed to insert monitor result", e))?;
    Ok(id)
}

/// Appends one result row in a dedicated transaction (for non-worker callers).
pub async fn record_result(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    new: &NewResult<'_>,
) -> Result<String> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin result record", e))?;
    let id = record_result_in_tx(&mut tx, workspace_id, new).await?;
    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit result record", e))?;
    Ok(id)
}

/// Filter for a results query.
#[derive(Debug, Clone, Default)]
pub struct ResultQuery<'a> {
    /// Restrict to one monitor.
    pub monitor_id: Option<&'a str>,
    /// Restrict to one table.
    pub table_id: Option<&'a str>,
}

/// Lists monitor results for a workspace, newest first, optionally filtered.
/// Bounded by `limit`.
pub async fn list_results(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    filter: &ResultQuery<'_>,
    limit: i64,
) -> Result<Vec<MonitorResult>> {
    let rows: Vec<ResultRow> = sqlx::query_as(&format!(
        "SELECT {RESULT_COLUMNS} FROM monitor_results
         WHERE workspace_id = $1
           AND ($2::text IS NULL OR monitor_id = $2)
           AND ($3::text IS NULL OR table_id = $3)
         ORDER BY evaluated_at DESC, id DESC
         LIMIT $4"
    ))
    .bind(workspace_id.to_string())
    .bind(filter.monitor_id)
    .bind(filter.table_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list monitor results", e))?;
    rows.into_iter().map(MonitorResult::try_from).collect()
}

// ===========================================================================
// Unit tests — the pure evaluation engine (no database)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn base_obs(ts: i64) -> CommitObservation {
        CommitObservation {
            snapshot_id: ts,
            timestamp_ms: ts,
            snapshot_count: 5,
            ..CommitObservation::default()
        }
    }

    // -- freshness -----------------------------------------------------------

    #[test]
    fn freshness_learning_when_too_little_history() {
        let obs = base_obs(10_000);
        let hist = History {
            timestamps_ms: vec![9_000, 8_000], // only 2 points < MIN_HISTORY
            ..History::default()
        };
        let e = score_freshness(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Ok);
        assert_eq!(e.result_kind, "freshness-learning");
    }

    #[test]
    fn freshness_breaches_on_gap_beyond_learned_cadence() {
        // Regular 1000ms cadence, then a 10_000ms gap.
        let hist = History {
            timestamps_ms: vec![0, 1000, 2000, 3000],
            ..History::default()
        };
        let obs = base_obs(13_000); // 10_000 after the last (3000)
        let e = score_freshness(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Breach, "{e:?}");
        assert_eq!(e.result_kind, "stale");
    }

    #[test]
    fn freshness_ok_on_regular_cadence() {
        let hist = History {
            timestamps_ms: vec![0, 1000, 2000, 3000],
            ..History::default()
        };
        let obs = base_obs(4000); // exactly one cadence later
        let e = score_freshness(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Ok, "{e:?}");
    }

    #[test]
    fn freshness_declared_sla_overrides_and_breaches() {
        let hist = History {
            timestamps_ms: vec![0, 1000, 2000, 3000],
            ..History::default()
        };
        // 5s after the last commit; SLA is 2s.
        let obs = base_obs(8000);
        let config = MonitorConfig {
            max_staleness_secs: Some(2),
            ..MonitorConfig::default()
        };
        let e = score_freshness(&obs, &hist, &config);
        assert_eq!(e.status, ResultStatus::Breach);
        assert_eq!(e.result_kind, "stale-sla");
    }

    // -- volume --------------------------------------------------------------

    #[test]
    fn volume_spike_breaches() {
        let hist = History {
            added_records: vec![100, 110, 90, 105],
            ..History::default()
        };
        let obs = CommitObservation {
            added_records: Some(1000), // ~10× the ~100 median
            ..base_obs(5000)
        };
        let e = score_volume(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Breach, "{e:?}");
        assert_eq!(e.result_kind, "volume-spike");
    }

    #[test]
    fn volume_collapse_breaches() {
        let hist = History {
            added_records: vec![1000, 1100, 900, 1050],
            ..History::default()
        };
        let obs = CommitObservation {
            added_records: Some(10), // ~1/100 the median
            ..base_obs(5000)
        };
        let e = score_volume(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Breach, "{e:?}");
        assert_eq!(e.result_kind, "volume-drop");
    }

    #[test]
    fn volume_in_band_is_ok() {
        let hist = History {
            added_records: vec![100, 110, 90, 105],
            ..History::default()
        };
        let obs = CommitObservation {
            added_records: Some(120),
            ..base_obs(5000)
        };
        let e = score_volume(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Ok, "{e:?}");
    }

    #[test]
    fn volume_skips_without_added_records() {
        let hist = History {
            added_records: vec![100, 110, 90, 105],
            ..History::default()
        };
        let obs = base_obs(5000); // added_records is None
        let e = score_volume(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Ok);
        assert_eq!(e.result_kind, "volume-skip");
    }

    #[test]
    fn volume_tiny_baseline_does_not_false_positive() {
        // A baseline of near-zero rows: the first real load is expected growth.
        let hist = History {
            added_records: vec![0, 0, 0],
            ..History::default()
        };
        let obs = CommitObservation {
            added_records: Some(10_000),
            ..base_obs(5000)
        };
        let e = score_volume(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Ok, "{e:?}");
    }

    // -- file size -----------------------------------------------------------

    #[test]
    fn file_size_regression_breaches() {
        let hist = History {
            avg_file_bytes: vec![128e6, 130e6, 120e6, 125e6],
            ..History::default()
        };
        let obs = CommitObservation {
            added_files_size: Some(10_000_000), // 10MB across
            added_data_files: Some(1000),       // 1000 files -> 10KB avg
            ..base_obs(5000)
        };
        let e = score_file_size(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Breach, "{e:?}");
        assert_eq!(e.result_kind, "file-size-regression");
    }

    #[test]
    fn file_size_ok_when_consistent() {
        let hist = History {
            avg_file_bytes: vec![128e6, 130e6, 120e6, 125e6],
            ..History::default()
        };
        let obs = CommitObservation {
            added_files_size: Some(256_000_000),
            added_data_files: Some(2),
            ..base_obs(5000)
        };
        let e = score_file_size(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Ok, "{e:?}");
    }

    // -- snapshot / delete debt ---------------------------------------------

    #[test]
    fn snapshot_count_spike_breaches() {
        let hist = History {
            snapshot_counts: vec![10, 11, 9, 10],
            ..History::default()
        };
        let obs = CommitObservation {
            snapshot_count: 40, // 4× the ~10 median
            ..base_obs(5000)
        };
        let e = score_snapshot_debt(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Breach, "{e:?}");
        assert_eq!(e.result_kind, "snapshot-debt");
    }

    #[test]
    fn delete_file_pileup_breaches() {
        let hist = History {
            snapshot_counts: vec![10, 10, 10, 10],
            delete_files: vec![2, 3, 2, 3],
            ..History::default()
        };
        let obs = CommitObservation {
            snapshot_count: 10,
            total_delete_files: Some(30), // 10× the ~2.5 median
            ..base_obs(5000)
        };
        let e = score_snapshot_debt(&obs, &hist, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Breach, "{e:?}");
        assert_eq!(e.result_kind, "delete-debt");
    }

    // -- schema change -------------------------------------------------------

    #[test]
    fn schema_breaking_change_breaches() {
        let e = score_schema_change(true, true, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Breach);
        assert_eq!(e.result_kind, "breaking-schema-change");
    }

    #[test]
    fn schema_additive_change_warns_by_default() {
        let e = score_schema_change(true, false, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Warn);
        assert_eq!(e.result_kind, "additive-schema-change");
    }

    #[test]
    fn schema_additive_change_breaches_when_configured() {
        let config = MonitorConfig {
            schema_any_change_breaches: true,
            ..MonitorConfig::default()
        };
        let e = score_schema_change(true, false, &config);
        assert_eq!(e.status, ResultStatus::Breach);
    }

    #[test]
    fn schema_no_change_is_ok() {
        let e = score_schema_change(false, false, &MonitorConfig::default());
        assert_eq!(e.status, ResultStatus::Ok);
    }

    // -- commit failure ------------------------------------------------------

    #[test]
    fn commit_failure_storm_breaches_at_threshold() {
        assert_eq!(score_commit_failure(5, 5).status, ResultStatus::Breach);
        assert_eq!(score_commit_failure(6, 5).status, ResultStatus::Breach);
        assert_eq!(score_commit_failure(4, 5).status, ResultStatus::Ok);
        assert_eq!(score_commit_failure(0, 0).status, ResultStatus::Ok);
    }

    // -- helpers -------------------------------------------------------------

    #[test]
    fn median_handles_even_and_odd() {
        assert_eq!(median_f64(&[]), None);
        assert_eq!(median_f64(&[5.0]), Some(5.0));
        assert_eq!(median_f64(&[1.0, 3.0]), Some(2.0));
        assert_eq!(median_f64(&[3.0, 1.0, 2.0]), Some(2.0));
    }

    #[test]
    fn enum_round_trips() {
        for k in MonitorKind::defaults() {
            assert_eq!(MonitorKind::parse(k.as_str()), Some(k));
        }
        for s in [Severity::Low, Severity::Medium, Severity::High] {
            assert_eq!(Severity::parse(s.as_str()), Some(s));
        }
        for s in [ResultStatus::Ok, ResultStatus::Warn, ResultStatus::Breach] {
            assert_eq!(ResultStatus::parse(s.as_str()), Some(s));
        }
        for b in [BoundTo::Table, BoundTo::Namespace] {
            assert_eq!(BoundTo::parse(b.as_str()), Some(b));
        }
    }

    #[test]
    fn config_defaults_are_serde_stable() {
        // An empty config round-trips and applies the documented defaults.
        let json = serde_json::to_value(MonitorConfig::default()).expect("serialize");
        assert_eq!(
            json,
            json!({}),
            "default config must serialize to empty object"
        );
        let parsed: MonitorConfig = serde_json::from_value(json!({})).expect("parse empty");
        assert!((parsed.volume_factor() - DEFAULT_VOLUME_FACTOR).abs() < f64::EPSILON);
        assert!((parsed.freshness_multiple() - DEFAULT_FRESHNESS_MULTIPLE).abs() < f64::EPSILON);
    }
}
