//! Table health model (Pillar C-F1): a per-table health score and
//! recommended actions, computed from the write-through index and the
//! current snapshot's manifests — **never** from a data scan.
//!
//! # What is measured
//!
//! From the current snapshot's manifest list + manifests (read through
//! [`meridian_iceberg::manifest`], cache-friendly because manifest files are
//! immutable at a path):
//!
//! - file count, total bytes, average and median data-file size;
//! - small-file ratio: data files strictly below the policy's target size
//!   over all data files, plus a full file-size histogram;
//! - delete/DV debt ratio: position + equality delete files and deletion
//!   vectors over data files;
//! - manifest fragmentation: manifest count and average live entries per
//!   manifest;
//! - partition skew: the coefficient of variation of bytes-per-partition.
//!
//! From the write-through index (`table_snapshots`, the `tables` row) and
//! the metadata file itself:
//!
//! - snapshot count and age of the oldest retained snapshot (bloat);
//! - metadata.json size;
//! - commit recency (staleness vs the policy's `max_staleness`).
//!
//! # The score
//!
//! [`compute_score`] combines five component penalties into a composite
//! `0..=100` (100 = healthiest) with fixed weights (see [`ScoreWeights`]).
//! It is **deterministic**: the same [`HealthInputs`] always yield the same
//! score and the same ordered top-3 recommendations. The determinism is the
//! contract the tests pin — the formula lives in pure functions that never
//! touch IO.
//!
//! # Persistence
//!
//! [`compute_health`] appends one immutable `health_snapshots` row per
//! computation so the UI/API can chart a table's health over time and a
//! recommendation can cite the exact inputs that produced it.
//!
//! # A note on numeric casts
//!
//! The scoring math converts file counts, byte totals, and snapshot counts to
//! `f64` for the penalty curves, and rounds the final `[0,100]` score to a
//! `u8`. Precision loss on a table with more than 2^52 files or bytes is
//! irrelevant to a health heuristic, and the final round is clamped to
//! `[0,100]` before the `u8` cast, so the cast is provably in range. The
//! specific cast lints are allowed at module scope with that rationale rather
//! than annotated at dozens of arithmetic sites.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::collections::BTreeMap;
use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use meridian_iceberg::manifest::{
    DataFileContent, ManifestContentType, ManifestEntryStatus, read_manifest, read_manifest_list,
};
use meridian_iceberg::spec::TableMetadata;
use meridian_storage::Storage;
use serde_json::{Value, json};
use sqlx::PgPool;
use ulid::Ulid;

use crate::map_sqlx_error;

/// The default target file size when no policy applies: 512 MiB (C-F3
/// `target_file_size_bytes` default). Files below this count as "small".
pub const DEFAULT_TARGET_FILE_SIZE_BYTES: i64 = 512 * 1024 * 1024;

/// Histogram bucket upper bounds (exclusive) in bytes, plus an implicit
/// final open-ended bucket. Chosen to straddle the small/large boundary at
/// the common 512 MiB target: 1 MiB, 8 MiB, 32 MiB, 128 MiB, 512 MiB, +inf.
const HISTOGRAM_BOUNDS: [i64; 5] = [
    1 << 20,   // 1 MiB
    8 << 20,   // 8 MiB
    32 << 20,  // 32 MiB
    128 << 20, // 128 MiB
    512 << 20, // 512 MiB
];

/// The raw, storage-and-index-derived inputs to the health formula.
///
/// Kept separate from scoring so the deterministic core can be tested with
/// synthetic inputs (and synthetic manifest fixtures) without a database or
/// object store. [`gather_inputs`] builds this from manifests; the formula
/// functions consume it.
#[derive(Debug, Clone)]
pub struct HealthInputs {
    /// Effective target file size for the small-file test (from the policy,
    /// or [`DEFAULT_TARGET_FILE_SIZE_BYTES`]).
    pub target_file_size_bytes: i64,
    /// Sizes in bytes of every live data file, unsorted.
    pub data_file_sizes: Vec<i64>,
    /// Count of live delete files (position + equality) and deletion vectors.
    pub delete_file_count: i64,
    /// Number of manifests in the current snapshot's manifest list.
    pub manifest_count: i64,
    /// Total live entries across all data manifests (for entries/manifest).
    pub live_manifest_entries: i64,
    /// Live data bytes grouped by partition-tuple key (for skew). Empty for
    /// an unpartitioned table.
    pub bytes_by_partition: BTreeMap<String, i64>,
    /// Retained snapshot count (from the index).
    pub snapshot_count: i64,
    /// Epoch-millis timestamp of the oldest retained snapshot, if any.
    pub oldest_snapshot_ms: Option<i64>,
    /// Epoch-millis timestamp of the newest retained snapshot, if any.
    pub newest_snapshot_ms: Option<i64>,
    /// metadata.json size in bytes.
    pub metadata_json_bytes: i64,
    /// Staleness budget from the policy: a table is "stale" if the newest
    /// commit is older than this many millis. `None` = no SLA (never stale).
    pub max_staleness_ms: Option<i64>,
    /// The instant health is being computed at (epoch millis), so staleness
    /// and snapshot age are deterministic given the inputs.
    pub now_ms: i64,
}

/// The derived metric summary (the numbers persisted and charted).
#[derive(Debug, Clone, PartialEq)]
pub struct HealthMetrics {
    /// Total live data bytes.
    pub total_bytes: i64,
    /// Live data-file count.
    pub data_file_count: i64,
    /// Small files / total data files, in `[0,1]`; 0 when no data files.
    pub small_file_ratio: f64,
    /// Mean data-file size (0 when no data files).
    pub avg_file_bytes: i64,
    /// Median data-file size (0 when no data files).
    pub median_file_bytes: i64,
    /// Delete + DV files / data files; 0 when no data files.
    pub delete_debt_ratio: f64,
    /// Live delete/DV file count.
    pub delete_file_count: i64,
    /// Manifest count.
    pub manifest_count: i64,
    /// Average live entries per data manifest (0 when no manifests).
    pub avg_manifest_entries: f64,
    /// Coefficient of variation of bytes-per-partition; 0 when
    /// unpartitioned or perfectly even.
    pub partition_skew: f64,
    /// Retained snapshot count.
    pub snapshot_count: i64,
    /// Oldest retained snapshot timestamp (epoch millis).
    pub oldest_snapshot_ms: Option<i64>,
    /// metadata.json size in bytes.
    pub metadata_json_bytes: i64,
    /// File-size histogram: bucket label -> count.
    pub file_size_histogram: BTreeMap<String, i64>,
}

/// Fixed component weights of the composite score. They sum to 100; each
/// component contributes `weight * (1 - penalty)` where `penalty` is in
/// `[0,1]`. Documented and stable so the score is comparable across
/// versions and reproducible in tests.
#[derive(Debug, Clone, Copy)]
pub struct ScoreWeights {
    /// Small-file health.
    pub small_files: u32,
    /// Delete/DV debt health.
    pub delete_debt: u32,
    /// Snapshot-bloat health.
    pub snapshot_bloat: u32,
    /// Manifest-fragmentation health.
    pub manifest_fragmentation: u32,
    /// Partition-skew health.
    pub partition_skew: u32,
}

impl ScoreWeights {
    /// The default weighting (sums to 100). Small files dominate because
    /// they are the most common and most expensive lakehouse pathology
    /// (the flagship-pillar rationale); skew is the lightest because it is
    /// often intrinsic to the data, not a maintenance failure.
    pub const DEFAULT: Self = Self {
        small_files: 35,
        delete_debt: 25,
        snapshot_bloat: 20,
        manifest_fragmentation: 12,
        partition_skew: 8,
    };

    /// Sum of the weights (always 100 for [`ScoreWeights::DEFAULT`]).
    #[must_use]
    pub const fn total(self) -> u32 {
        self.small_files
            + self.delete_debt
            + self.snapshot_bloat
            + self.manifest_fragmentation
            + self.partition_skew
    }
}

/// The per-component health, each in `[0,1]` (1 = healthy). These are the
/// `1 - penalty` factors the composite score weights.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ComponentHealth {
    /// Small-file health: 1 when no data file is below target.
    pub small_files: f64,
    /// Delete-debt health: 1 when there are no delete/DV files.
    pub delete_debt: f64,
    /// Snapshot-bloat health: 1 at/under the soft snapshot budget.
    pub snapshot_bloat: f64,
    /// Manifest-fragmentation health: 1 when manifests are well-packed.
    pub manifest_fragmentation: f64,
    /// Partition-skew health: 1 when partitions are even (or absent).
    pub partition_skew: f64,
}

/// Soft budgets past which a component starts losing health. These shape the
/// penalty curves; they are deliberately generous (a table at the budget is
/// still "fine", the penalty grows beyond it).
mod budget {
    /// Snapshots beyond this begin to cost snapshot-bloat health; health
    /// reaches 0 at `SNAPSHOT_SOFT * SNAPSHOT_ZERO_MULT`.
    pub(super) const SNAPSHOT_SOFT: f64 = 100.0;
    pub(super) const SNAPSHOT_ZERO_MULT: f64 = 10.0;
    /// Average entries-per-manifest below this begins to cost fragmentation
    /// health (many tiny manifests); health reaches 0 at one entry/manifest.
    pub(super) const MANIFEST_ENTRIES_SOFT: f64 = 100.0;
    /// Delete-debt ratio at which delete health hits 0 (ratio >= 1 means as
    /// many delete files as data files — thoroughly unhealthy).
    pub(super) const DELETE_ZERO_RATIO: f64 = 1.0;
    /// Partition coefficient-of-variation at which skew health hits 0.
    pub(super) const SKEW_ZERO_CV: f64 = 2.0;
}

/// A recommended maintenance action, most impactful first.
#[derive(Debug, Clone, PartialEq)]
pub struct Recommendation {
    /// Machine-readable action, aligned with `maintenance_jobs.job_type`
    /// where one exists (`compaction`, `expire_snapshots`,
    /// `rewrite_manifests`) plus advisory kinds (`repartition`).
    pub action: String,
    /// Human-readable reason citing the metric that triggered it.
    pub reason: String,
    /// The component-health deficit `weight * (1 - health)` this action
    /// addresses — the ranking key (larger = more score to recover).
    pub impact: f64,
}

/// Computes the derived metric summary from raw inputs. Pure and total.
#[must_use]
pub fn compute_metrics(inputs: &HealthInputs) -> HealthMetrics {
    let data_file_count = i64::try_from(inputs.data_file_sizes.len()).unwrap_or(i64::MAX);
    let total_bytes: i64 = inputs.data_file_sizes.iter().copied().sum();

    let small_threshold = inputs.target_file_size_bytes;
    let small_count = inputs
        .data_file_sizes
        .iter()
        .filter(|&&s| s < small_threshold)
        .count();
    let small_file_ratio = ratio(small_count as i64, data_file_count);

    let avg_file_bytes = if data_file_count > 0 {
        total_bytes / data_file_count
    } else {
        0
    };
    let median_file_bytes = median(&inputs.data_file_sizes);

    let delete_debt_ratio = ratio(inputs.delete_file_count, data_file_count);

    let avg_manifest_entries = if inputs.manifest_count > 0 {
        inputs.live_manifest_entries as f64 / inputs.manifest_count as f64
    } else {
        0.0
    };

    let partition_skew = coefficient_of_variation(inputs.bytes_by_partition.values().copied());

    HealthMetrics {
        total_bytes,
        data_file_count,
        small_file_ratio,
        avg_file_bytes,
        median_file_bytes,
        delete_debt_ratio,
        delete_file_count: inputs.delete_file_count,
        manifest_count: inputs.manifest_count,
        avg_manifest_entries,
        partition_skew,
        snapshot_count: inputs.snapshot_count,
        oldest_snapshot_ms: inputs.oldest_snapshot_ms,
        metadata_json_bytes: inputs.metadata_json_bytes,
        file_size_histogram: histogram(&inputs.data_file_sizes),
    }
}

/// Computes per-component health `[0,1]` from the derived metrics. Pure.
#[must_use]
pub fn component_health(metrics: &HealthMetrics) -> ComponentHealth {
    // Small files: health is the fraction of files that are *not* small.
    let small_files = 1.0 - metrics.small_file_ratio;

    // Delete debt: linear from 1 (no deletes) to 0 at DELETE_ZERO_RATIO.
    let delete_debt = 1.0 - clamp01(metrics.delete_debt_ratio / budget::DELETE_ZERO_RATIO);

    // Snapshot bloat: healthy up to the soft budget, then linear to 0 at
    // SNAPSHOT_SOFT * SNAPSHOT_ZERO_MULT.
    let snapshot_bloat = {
        let n = metrics.snapshot_count as f64;
        if n <= budget::SNAPSHOT_SOFT {
            1.0
        } else {
            let span = budget::SNAPSHOT_SOFT * (budget::SNAPSHOT_ZERO_MULT - 1.0);
            1.0 - clamp01((n - budget::SNAPSHOT_SOFT) / span)
        }
    };

    // Manifest fragmentation: a single manifest is never fragmented, however
    // few files it holds — fragmentation is a *many sparse manifests*
    // pathology. So a table with at most one manifest, no live entries, or
    // well-packed manifests (>= soft entries each) is healthy; otherwise the
    // penalty grows as average packing falls toward one entry per manifest.
    let manifest_fragmentation = if metrics.manifest_count <= 1
        || metrics.avg_manifest_entries < f64::EPSILON
        || metrics.avg_manifest_entries >= budget::MANIFEST_ENTRIES_SOFT
    {
        1.0
    } else {
        // 1 entry/manifest -> 0 health; SOFT entries/manifest -> 1 health.
        clamp01((metrics.avg_manifest_entries - 1.0) / (budget::MANIFEST_ENTRIES_SOFT - 1.0))
    };

    // Partition skew: linear from 1 (even) to 0 at SKEW_ZERO_CV.
    let partition_skew = 1.0 - clamp01(metrics.partition_skew / budget::SKEW_ZERO_CV);

    ComponentHealth {
        small_files: clamp01(small_files),
        delete_debt: clamp01(delete_debt),
        snapshot_bloat: clamp01(snapshot_bloat),
        manifest_fragmentation: clamp01(manifest_fragmentation),
        partition_skew: clamp01(partition_skew),
    }
}

/// The composite `0..=100` score from component health and weights. Pure.
///
/// `score = round( sum(weight_i * health_i) / sum(weight_i) * 100 )`. An
/// all-healthy table scores 100; a table failing every component scores 0.
#[must_use]
pub fn compute_score(health: &ComponentHealth, weights: ScoreWeights) -> u8 {
    let total = f64::from(weights.total());
    if total <= 0.0 {
        return 100;
    }
    let weighted = f64::from(weights.small_files) * health.small_files
        + f64::from(weights.delete_debt) * health.delete_debt
        + f64::from(weights.snapshot_bloat) * health.snapshot_bloat
        + f64::from(weights.manifest_fragmentation) * health.manifest_fragmentation
        + f64::from(weights.partition_skew) * health.partition_skew;
    // Guard the cast: weighted/total is in [0,1] by construction.
    let scaled = (weighted / total * 100.0).round();
    scaled.clamp(0.0, 100.0) as u8
}

/// The top-3 recommended actions, ranked by recoverable score. Pure and
/// deterministic: ties break by a fixed action priority so the ordering is
/// stable. Staleness (an SLA breach, not a component of the score) is
/// surfaced as its own recommendation when the policy sets a budget.
#[must_use]
pub fn recommendations(
    inputs: &HealthInputs,
    metrics: &HealthMetrics,
    health: &ComponentHealth,
    weights: ScoreWeights,
) -> Vec<Recommendation> {
    let mut candidates: Vec<Recommendation> = Vec::new();

    let deficit = |weight: u32, h: f64| f64::from(weight) * (1.0 - h);

    if health.small_files < 1.0 && metrics.data_file_count > 0 {
        candidates.push(Recommendation {
            action: "compaction".to_owned(),
            reason: format!(
                "{:.0}% of {} data files are below the {} MiB target size",
                metrics.small_file_ratio * 100.0,
                metrics.data_file_count,
                inputs.target_file_size_bytes / (1 << 20),
            ),
            impact: deficit(weights.small_files, health.small_files),
        });
    }
    if health.delete_debt < 1.0 {
        candidates.push(Recommendation {
            action: "compaction".to_owned(),
            reason: format!(
                "{} delete/DV files against {} data files (ratio {:.2}) — compact to apply deletes",
                metrics.delete_file_count, metrics.data_file_count, metrics.delete_debt_ratio,
            ),
            impact: deficit(weights.delete_debt, health.delete_debt),
        });
    }
    if health.snapshot_bloat < 1.0 {
        candidates.push(Recommendation {
            action: "expire_snapshots".to_owned(),
            reason: format!(
                "{} retained snapshots exceed the soft budget of {:.0}",
                metrics.snapshot_count,
                budget::SNAPSHOT_SOFT,
            ),
            impact: deficit(weights.snapshot_bloat, health.snapshot_bloat),
        });
    }
    if health.manifest_fragmentation < 1.0 {
        candidates.push(Recommendation {
            action: "rewrite_manifests".to_owned(),
            reason: format!(
                "{} manifests averaging {:.1} entries each — merge to reduce planning overhead",
                metrics.manifest_count, metrics.avg_manifest_entries,
            ),
            impact: deficit(
                weights.manifest_fragmentation,
                health.manifest_fragmentation,
            ),
        });
    }
    if health.partition_skew < 1.0 {
        candidates.push(Recommendation {
            action: "repartition".to_owned(),
            reason: format!(
                "bytes-per-partition coefficient of variation is {:.2} — review the partition spec",
                metrics.partition_skew,
            ),
            impact: deficit(weights.partition_skew, health.partition_skew),
        });
    }
    if let Some(budget_ms) = inputs.max_staleness_ms
        && let Some(newest) = inputs.newest_snapshot_ms
    {
        let age = inputs.now_ms - newest;
        if age > budget_ms {
            candidates.push(Recommendation {
                action: "investigate_staleness".to_owned(),
                // Staleness is not part of the composite score, so give it a
                // high impact when breached so it surfaces alongside the
                // score-recovering actions (an SLA breach is operator-urgent).
                reason: format!(
                    "newest commit is {age} ms old, over the {budget_ms} ms freshness budget"
                ),
                impact: f64::from(weights.total()),
            });
        }
    }

    // Stable ranking: impact descending, then a fixed action priority so
    // equal-impact recommendations always order the same way.
    candidates.sort_by(|a, b| {
        b.impact
            .partial_cmp(&a.impact)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| action_priority(&a.action).cmp(&action_priority(&b.action)))
    });
    candidates.truncate(3);
    candidates
}

/// Fixed tie-break priority for recommendation actions (lower sorts first).
fn action_priority(action: &str) -> u8 {
    match action {
        "investigate_staleness" => 0,
        "compaction" => 1,
        "expire_snapshots" => 2,
        "rewrite_manifests" => 3,
        "repartition" => 4,
        _ => 5,
    }
}

/// A computed, ready-to-persist health result: metrics, score, and the
/// ranked recommendations.
#[derive(Debug, Clone)]
pub struct HealthResult {
    /// The derived metric summary.
    pub metrics: HealthMetrics,
    /// Per-component health.
    pub components: ComponentHealth,
    /// Composite 0..=100 score.
    pub score: u8,
    /// Top-3 recommended actions.
    pub recommendations: Vec<Recommendation>,
}

/// Runs the full formula over raw inputs with the default weights. Pure.
#[must_use]
pub fn evaluate(inputs: &HealthInputs) -> HealthResult {
    evaluate_with(inputs, ScoreWeights::DEFAULT)
}

/// Runs the full formula with explicit weights. Pure.
#[must_use]
pub fn evaluate_with(inputs: &HealthInputs, weights: ScoreWeights) -> HealthResult {
    let metrics = compute_metrics(inputs);
    let components = component_health(&metrics);
    let score = compute_score(&components, weights);
    let recs = recommendations(inputs, &metrics, &components, weights);
    HealthResult {
        metrics,
        components,
        score,
        recommendations: recs,
    }
}

/// Where health is being computed for: the table id, its display identity
/// (for events/audit), the current metadata location, and the effective
/// staleness/target from the resolved policy.
#[derive(Debug, Clone)]
pub struct HealthTarget {
    /// The `tables.id`.
    pub table_id: String,
    /// Human-readable identity, e.g. `ns.orders` (for audit/events).
    pub table_ident: String,
    /// Current `metadata.json` location.
    pub metadata_location: String,
    /// Effective target file size (from the resolved policy).
    pub target_file_size_bytes: i64,
    /// Effective staleness budget (from the resolved policy), if any.
    pub max_staleness_ms: Option<i64>,
}

/// Gathers the health inputs for a table by reading its current snapshot's
/// manifests through `storage` and its snapshot index from `pool`. No data
/// scan: only the metadata Avro/JSON layer is touched.
///
/// Returns inputs describing an empty table (no data files, no snapshots)
/// when the metadata has no current snapshot.
pub async fn gather_inputs(
    pool: &PgPool,
    storage: &dyn Storage,
    target: &HealthTarget,
    now_ms: i64,
) -> Result<(HealthInputs, Option<i64>)> {
    let bytes = storage
        .read(&target.metadata_location)
        .await
        .map_err(|e| MeridianError::internal("failed to read table metadata for health", e))?;
    let metadata_json_bytes = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
    let text = std::str::from_utf8(&bytes)
        .map_err(|e| MeridianError::internal("table metadata is not valid UTF-8", e))?;
    let metadata = TableMetadata::from_json(text)
        .map_err(|e| MeridianError::internal("failed to parse table metadata for health", e))?;

    // Snapshot bloat + age come from the index (the write-through source of
    // truth), not from re-parsing the metadata snapshot list — the index is
    // what the rest of the platform trusts and what stays cheap at scale.
    let snapshot_rows: Vec<(i64, i64, bool)> = sqlx::query_as(
        "SELECT snapshot_id, timestamp_ms, is_current
         FROM table_snapshots WHERE table_id = $1",
    )
    .bind(&target.table_id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to read snapshot index for health", e))?;

    let snapshot_count = i64::try_from(snapshot_rows.len()).unwrap_or(i64::MAX);
    let oldest_snapshot_ms = snapshot_rows.iter().map(|(_, ts, _)| *ts).min();
    let newest_snapshot_ms = snapshot_rows.iter().map(|(_, ts, _)| *ts).max();

    let current_snapshot_id = metadata.current_snapshot().map(|s| s.snapshot_id);

    let mut inputs = HealthInputs {
        target_file_size_bytes: target.target_file_size_bytes,
        data_file_sizes: Vec::new(),
        delete_file_count: 0,
        manifest_count: 0,
        live_manifest_entries: 0,
        bytes_by_partition: BTreeMap::new(),
        snapshot_count,
        oldest_snapshot_ms,
        newest_snapshot_ms,
        metadata_json_bytes,
        max_staleness_ms: target.max_staleness_ms,
        now_ms,
    };

    let Some(snapshot) = metadata.current_snapshot() else {
        // Empty table: metadata exists but there is no current snapshot.
        return Ok((inputs, current_snapshot_id));
    };
    let Some(manifest_list_loc) = snapshot.manifest_list.as_deref() else {
        // A v1 inline-manifests snapshot; the Avro manifest-list path is what
        // this model reads. Treat as no measurable file layout rather than
        // failing health for the whole table.
        return Ok((inputs, current_snapshot_id));
    };

    let list_bytes = storage
        .read(manifest_list_loc)
        .await
        .map_err(|e| MeridianError::internal("failed to read manifest list for health", e))?;
    let list = read_manifest_list(&list_bytes)
        .map_err(|e| MeridianError::internal("failed to parse manifest list for health", e))?;
    inputs.manifest_count = i64::try_from(list.manifests.len()).unwrap_or(i64::MAX);

    for manifest_file in &list.manifests {
        // Delete manifests contribute to delete debt at the file level; still
        // read them so equality/position delete files are counted.
        let manifest_bytes = storage
            .read(&manifest_file.manifest_path)
            .await
            .map_err(|e| MeridianError::internal("failed to read manifest for health", e))?;
        let manifest = read_manifest(&manifest_bytes)
            .map_err(|e| MeridianError::internal("failed to parse manifest for health", e))?;
        accumulate_manifest(&mut inputs, manifest_file.content, &manifest.entries);
    }

    Ok((inputs, current_snapshot_id))
}

/// Folds one manifest's live entries into the accumulating health inputs:
/// data files feed the size/partition metrics, delete files feed delete debt,
/// and every live entry counts toward manifest packing.
fn accumulate_manifest(
    inputs: &mut HealthInputs,
    manifest_content: ManifestContentType,
    entries: &[meridian_iceberg::manifest::ManifestEntry],
) {
    for entry in entries {
        // Only live files count toward current health. DELETED entries are
        // tombstones from the snapshot that wrote this manifest.
        if entry.status == ManifestEntryStatus::Deleted {
            continue;
        }
        let file = &entry.data_file;
        match file.content {
            DataFileContent::Data => {
                inputs.data_file_sizes.push(file.file_size_in_bytes);
                inputs.live_manifest_entries += 1;
                let key = partition_key(&file.partition);
                *inputs.bytes_by_partition.entry(key).or_insert(0) += file.file_size_in_bytes;
            }
            DataFileContent::PositionDeletes | DataFileContent::EqualityDeletes => {
                inputs.delete_file_count += 1;
                // Delete-manifest entries also count as live entries for
                // fragmentation accounting.
                if manifest_content == ManifestContentType::Deletes {
                    inputs.live_manifest_entries += 1;
                }
            }
        }
    }
}

/// A persisted health-snapshot row.
#[derive(Debug, Clone)]
pub struct HealthSnapshotRecord {
    /// ULID of the row.
    pub id: String,
    /// Owning table.
    pub table_id: String,
    /// The table snapshot health was computed against.
    pub snapshot_id: Option<i64>,
    /// Composite score.
    pub score: u8,
    /// The derived metrics.
    pub metrics: HealthMetrics,
    /// The ranked recommendations.
    pub recommendations: Vec<Recommendation>,
    /// When it was computed.
    pub computed_at: DateTime<Utc>,
}

/// Computes a table's health and appends a `health_snapshots` row,
/// returning the persisted record.
///
/// This reads only metadata (index + manifest Avro), never data. The write
/// is a plain insert (health history is derived, immutable evidence — not a
/// table-pointer mutation), so it does not go through the commit path and is
/// not itself audited: it changes no catalog state and is reproducible from
/// the inputs it stores.
pub async fn compute_health(
    pool: &PgPool,
    storage: &dyn Storage,
    workspace_id: WorkspaceId,
    target: &HealthTarget,
) -> Result<HealthSnapshotRecord> {
    let now = Utc::now();
    let now_ms = now.timestamp_millis();
    let (inputs, snapshot_id) = gather_inputs(pool, storage, target, now_ms).await?;
    let result = evaluate(&inputs);
    persist(pool, workspace_id, target, snapshot_id, now, &result).await
}

/// Persists a computed [`HealthResult`] as a `health_snapshots` row.
async fn persist(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    target: &HealthTarget,
    snapshot_id: Option<i64>,
    computed_at: DateTime<Utc>,
    result: &HealthResult,
) -> Result<HealthSnapshotRecord> {
    let id = Ulid::new().to_string();
    let m = &result.metrics;
    sqlx::query(
        "INSERT INTO health_snapshots
             (id, workspace_id, table_id, snapshot_id, score, total_bytes, data_file_count,
              small_file_ratio, avg_file_bytes, median_file_bytes, snapshot_count,
              oldest_snapshot_ms, delete_debt_ratio, delete_file_count, manifest_count,
              avg_manifest_entries, partition_skew, metadata_json_bytes, metrics,
              recommendations, computed_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(&target.table_id)
    .bind(snapshot_id)
    .bind(i16::from(result.score))
    .bind(m.total_bytes)
    .bind(m.data_file_count)
    .bind(m.small_file_ratio)
    .bind(m.avg_file_bytes)
    .bind(m.median_file_bytes)
    .bind(i32::try_from(m.snapshot_count).unwrap_or(i32::MAX))
    .bind(m.oldest_snapshot_ms)
    .bind(m.delete_debt_ratio)
    .bind(m.delete_file_count)
    .bind(i32::try_from(m.manifest_count).unwrap_or(i32::MAX))
    .bind(m.avg_manifest_entries)
    .bind(m.partition_skew)
    .bind(m.metadata_json_bytes)
    .bind(metrics_json(m, &result.components))
    .bind(recommendations_json(&result.recommendations))
    .bind(computed_at)
    .execute(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to persist health snapshot", e))?;

    Ok(HealthSnapshotRecord {
        id,
        table_id: target.table_id.clone(),
        snapshot_id,
        score: result.score,
        metrics: result.metrics.clone(),
        recommendations: result.recommendations.clone(),
        computed_at,
    })
}

/// Returns a table's health snapshots, newest first, up to `limit`.
pub async fn history(
    pool: &PgPool,
    table_id: &str,
    limit: i64,
) -> Result<Vec<HealthSnapshotRecord>> {
    let rows: Vec<HealthHistoryRow> = sqlx::query_as(
        "SELECT id, table_id, snapshot_id, score, total_bytes, data_file_count, small_file_ratio,
                avg_file_bytes, median_file_bytes, delete_debt_ratio, delete_file_count,
                manifest_count, avg_manifest_entries, partition_skew, snapshot_count,
                oldest_snapshot_ms, metadata_json_bytes, metrics, recommendations, computed_at
         FROM health_snapshots WHERE table_id = $1
         ORDER BY computed_at DESC LIMIT $2",
    )
    .bind(table_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to read health history", e))?;
    rows.into_iter()
        .map(HealthHistoryRow::into_record)
        .collect()
}

#[derive(sqlx::FromRow)]
struct HealthHistoryRow {
    id: String,
    table_id: String,
    snapshot_id: Option<i64>,
    score: i16,
    total_bytes: i64,
    data_file_count: i64,
    small_file_ratio: f64,
    avg_file_bytes: i64,
    median_file_bytes: i64,
    delete_debt_ratio: f64,
    delete_file_count: i64,
    manifest_count: i32,
    avg_manifest_entries: f64,
    partition_skew: f64,
    snapshot_count: i32,
    oldest_snapshot_ms: Option<i64>,
    metadata_json_bytes: i64,
    metrics: Value,
    recommendations: Value,
    computed_at: DateTime<Utc>,
}

impl HealthHistoryRow {
    fn into_record(self) -> Result<HealthSnapshotRecord> {
        let histogram = self
            .metrics
            .get("file_size_histogram")
            .and_then(Value::as_object)
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_i64().map(|n| (k.clone(), n)))
                    .collect::<BTreeMap<String, i64>>()
            })
            .unwrap_or_default();
        let score = u8::try_from(self.score)
            .map_err(|_| MeridianError::internal_msg("persisted health score out of range"))?;
        let recommendations = parse_recommendations(&self.recommendations);
        Ok(HealthSnapshotRecord {
            id: self.id,
            table_id: self.table_id,
            snapshot_id: self.snapshot_id,
            score,
            metrics: HealthMetrics {
                total_bytes: self.total_bytes,
                data_file_count: self.data_file_count,
                small_file_ratio: self.small_file_ratio,
                avg_file_bytes: self.avg_file_bytes,
                median_file_bytes: self.median_file_bytes,
                delete_debt_ratio: self.delete_debt_ratio,
                delete_file_count: self.delete_file_count,
                manifest_count: i64::from(self.manifest_count),
                avg_manifest_entries: self.avg_manifest_entries,
                partition_skew: self.partition_skew,
                snapshot_count: i64::from(self.snapshot_count),
                oldest_snapshot_ms: self.oldest_snapshot_ms,
                metadata_json_bytes: self.metadata_json_bytes,
                file_size_histogram: histogram,
            },
            recommendations,
            computed_at: self.computed_at,
        })
    }
}

fn parse_recommendations(value: &Value) -> Vec<Recommendation> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|r| {
                    Some(Recommendation {
                        action: r.get("action")?.as_str()?.to_owned(),
                        reason: r.get("reason")?.as_str()?.to_owned(),
                        impact: r.get("impact").and_then(Value::as_f64).unwrap_or(0.0),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The JSON blob stored in `health_snapshots.metrics`: the histogram plus the
/// component sub-scores (so a recommendation can cite them).
fn metrics_json(metrics: &HealthMetrics, components: &ComponentHealth) -> Value {
    json!({
        "file_size_histogram": metrics.file_size_histogram,
        "components": {
            "small_files": components.small_files,
            "delete_debt": components.delete_debt,
            "snapshot_bloat": components.snapshot_bloat,
            "manifest_fragmentation": components.manifest_fragmentation,
            "partition_skew": components.partition_skew,
        },
    })
}

fn recommendations_json(recs: &[Recommendation]) -> Value {
    Value::Array(
        recs.iter()
            .map(|r| {
                json!({
                    "action": r.action,
                    "reason": r.reason,
                    "impact": r.impact,
                })
            })
            .collect(),
    )
}

// ---- pure numeric helpers -------------------------------------------------

/// `numerator / denominator` as an `f64`; 0 when the denominator is 0.
fn ratio(numerator: i64, denominator: i64) -> f64 {
    if denominator <= 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// Clamps to `[0,1]`, mapping NaN to 0 (an undefined ratio is "no signal").
fn clamp01(x: f64) -> f64 {
    if x.is_nan() { 0.0 } else { x.clamp(0.0, 1.0) }
}

/// Median of a slice (0 when empty). Even lengths average the two middle
/// elements with integer (floor) division — deterministic and enough for a
/// health signal.
fn median(sizes: &[i64]) -> i64 {
    if sizes.is_empty() {
        return 0;
    }
    let mut sorted = sizes.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        i64::midpoint(sorted[mid - 1], sorted[mid])
    }
}

/// Coefficient of variation (stddev / mean) of a value set; 0 for fewer than
/// two values or a zero mean. Used as the partition-skew signal — scale-free,
/// so it does not punish large tables for being large.
fn coefficient_of_variation(values: impl Iterator<Item = i64>) -> f64 {
    let vals: Vec<f64> = values.map(|v| v as f64).collect();
    if vals.len() < 2 {
        return 0.0;
    }
    let n = vals.len() as f64;
    let mean = vals.iter().sum::<f64>() / n;
    if mean <= 0.0 {
        return 0.0;
    }
    let variance = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    variance.sqrt() / mean
}

/// Buckets file sizes into [`HISTOGRAM_BOUNDS`] plus a final open bucket.
fn histogram(sizes: &[i64]) -> BTreeMap<String, i64> {
    // Fixed labels, always present (a zero bucket is meaningful signal), and
    // ordered so BTreeMap iteration is smallest-bucket-first via a numeric
    // prefix.
    let labels = [
        "0:<1MiB",
        "1:1-8MiB",
        "2:8-32MiB",
        "3:32-128MiB",
        "4:128-512MiB",
        "5:>=512MiB",
    ];
    let mut out: BTreeMap<String, i64> = labels.iter().map(|l| ((*l).to_owned(), 0)).collect();
    for &size in sizes {
        let idx = HISTOGRAM_BOUNDS
            .iter()
            .position(|&bound| size < bound)
            .unwrap_or(HISTOGRAM_BOUNDS.len());
        *out.get_mut(labels[idx]).expect("label exists") += 1;
    }
    out
}

/// A stable string key for a partition tuple (skew grouping). Null values
/// render as `∅`; the field order is the tuple's spec order.
fn partition_key(tuple: &meridian_iceberg::manifest::PartitionTuple) -> String {
    if tuple.fields.is_empty() {
        return "<unpartitioned>".to_owned();
    }
    let mut key = String::new();
    for (i, field) in tuple.fields.iter().enumerate() {
        if i > 0 {
            key.push('\u{1f}');
        }
        match &field.value {
            // Ignoring the write result: writing into a String never fails.
            Some(datum) => {
                let _ = write!(key, "{datum:?}");
            }
            None => key.push('\u{2205}'),
        }
    }
    key
}

#[cfg(test)]
mod tests {
    // Ratios and scores in these tests are exact by construction (integer
    // inputs chosen so the arithmetic lands on representable values), so exact
    // float equality is the correct, most-legible assertion.
    #![allow(clippy::float_cmp)]

    use super::*;

    /// Inputs for a healthy table: many large files, few snapshots, no
    /// deletes, well-packed manifests, even partitions.
    fn healthy_inputs() -> HealthInputs {
        HealthInputs {
            target_file_size_bytes: DEFAULT_TARGET_FILE_SIZE_BYTES,
            data_file_sizes: vec![DEFAULT_TARGET_FILE_SIZE_BYTES; 20],
            delete_file_count: 0,
            manifest_count: 1,
            live_manifest_entries: 20,
            bytes_by_partition: BTreeMap::new(),
            snapshot_count: 3,
            oldest_snapshot_ms: Some(1_000),
            newest_snapshot_ms: Some(2_000),
            metadata_json_bytes: 4096,
            max_staleness_ms: None,
            now_ms: 3_000,
        }
    }

    #[test]
    fn all_large_files_scores_100() {
        let result = evaluate(&healthy_inputs());
        assert_eq!(result.score, 100, "a pristine table must score 100");
        assert!(
            result.recommendations.is_empty(),
            "nothing to recommend for a healthy table"
        );
        assert_eq!(result.metrics.small_file_ratio, 0.0);
    }

    #[test]
    fn empty_table_scores_100() {
        // No data files, no snapshots: nothing is unhealthy.
        let inputs = HealthInputs {
            data_file_sizes: vec![],
            manifest_count: 0,
            live_manifest_entries: 0,
            snapshot_count: 0,
            oldest_snapshot_ms: None,
            newest_snapshot_ms: None,
            ..healthy_inputs()
        };
        let result = evaluate(&inputs);
        assert_eq!(result.score, 100);
        assert_eq!(result.metrics.total_bytes, 0);
        assert_eq!(result.metrics.median_file_bytes, 0);
        assert!(result.recommendations.is_empty());
    }

    #[test]
    fn all_small_files_scores_low() {
        let inputs = HealthInputs {
            data_file_sizes: vec![1024; 500],
            manifest_count: 1,
            live_manifest_entries: 500,
            ..healthy_inputs()
        };
        let result = evaluate(&inputs);
        assert_eq!(result.metrics.small_file_ratio, 1.0);
        // Small-files weight is 35; failing it fully removes 35 of 100 (and
        // nothing else here is unhealthy), so the score is 65 exactly.
        assert_eq!(result.score, 65);
        assert_eq!(result.recommendations[0].action, "compaction");
    }

    #[test]
    fn many_small_files_plus_deletes_and_bloat_scores_very_low() {
        let inputs = HealthInputs {
            data_file_sizes: vec![1024; 500],
            delete_file_count: 500, // ratio 1.0 -> delete health 0
            manifest_count: 500,
            live_manifest_entries: 500, // 1 entry/manifest -> fragmentation 0
            snapshot_count: 1_000,      // 10x soft budget -> bloat 0
            ..healthy_inputs()
        };
        let result = evaluate(&inputs);
        // small(35)+delete(25)+bloat(20)+manifest(12) all zeroed; only
        // partition-skew(8) survives (no partitions -> healthy). Score = 8.
        assert_eq!(result.score, 8);
        assert_eq!(result.recommendations.len(), 3, "top-3 only");
        // Ranked by recoverable weight: small(35) > delete(25) > bloat(20).
        assert_eq!(result.recommendations[0].action, "compaction");
        assert_eq!(result.recommendations[1].action, "compaction");
        assert_eq!(result.recommendations[2].action, "expire_snapshots");
    }

    #[test]
    fn score_is_deterministic() {
        let inputs = many_small_inputs();
        let first = evaluate(&inputs).score;
        for _ in 0..50 {
            assert_eq!(evaluate(&inputs).score, first);
        }
    }

    fn many_small_inputs() -> HealthInputs {
        HealthInputs {
            data_file_sizes: vec![2048; 137],
            delete_file_count: 12,
            manifest_count: 9,
            live_manifest_entries: 149,
            bytes_by_partition: [
                ("a".to_owned(), 1000),
                ("b".to_owned(), 50),
                ("c".to_owned(), 9000),
            ]
            .into_iter()
            .collect(),
            ..healthy_inputs()
        }
    }

    #[test]
    fn partition_skew_penalizes_uneven_partitions() {
        let even = HealthInputs {
            bytes_by_partition: [("a".to_owned(), 100), ("b".to_owned(), 100)]
                .into_iter()
                .collect(),
            ..healthy_inputs()
        };
        let skewed = HealthInputs {
            bytes_by_partition: [("a".to_owned(), 1), ("b".to_owned(), 100_000)]
                .into_iter()
                .collect(),
            ..healthy_inputs()
        };
        assert_eq!(compute_metrics(&even).partition_skew, 0.0);
        assert!(compute_metrics(&skewed).partition_skew > 0.9);
        assert!(evaluate(&skewed).score < evaluate(&even).score);
    }

    #[test]
    fn staleness_recommendation_fires_over_budget() {
        let inputs = HealthInputs {
            max_staleness_ms: Some(500),
            newest_snapshot_ms: Some(1_000),
            now_ms: 2_000, // age 1000 > budget 500
            ..healthy_inputs()
        };
        let result = evaluate(&inputs);
        assert_eq!(result.recommendations[0].action, "investigate_staleness");
    }

    #[test]
    fn staleness_recommendation_silent_within_budget() {
        let inputs = HealthInputs {
            max_staleness_ms: Some(5_000),
            newest_snapshot_ms: Some(1_000),
            now_ms: 2_000, // age 1000 < budget 5000
            ..healthy_inputs()
        };
        assert!(
            evaluate(&inputs)
                .recommendations
                .iter()
                .all(|r| r.action != "investigate_staleness")
        );
    }

    #[test]
    fn median_handles_even_and_odd() {
        assert_eq!(median(&[]), 0);
        assert_eq!(median(&[5]), 5);
        assert_eq!(median(&[1, 3]), 2);
        assert_eq!(median(&[3, 1, 2]), 2);
        assert_eq!(median(&[10, 2, 8, 4]), 6);
    }

    #[test]
    fn histogram_buckets_boundaries() {
        let sizes = vec![
            0,               // <1MiB
            (1 << 20) - 1,   // <1MiB
            1 << 20,         // 1-8MiB
            (512 << 20) - 1, // 128-512MiB
            512 << 20,       // >=512MiB
            1 << 30,         // >=512MiB
        ];
        let h = histogram(&sizes);
        assert_eq!(h["0:<1MiB"], 2);
        assert_eq!(h["1:1-8MiB"], 1);
        assert_eq!(h["4:128-512MiB"], 1);
        assert_eq!(h["5:>=512MiB"], 2);
    }

    #[test]
    fn score_never_exceeds_bounds() {
        // Degenerate weights and extreme inputs must still clamp.
        let inputs = HealthInputs {
            data_file_sizes: vec![1; 10],
            delete_file_count: 10_000,
            snapshot_count: 1_000_000,
            manifest_count: 10_000,
            live_manifest_entries: 10_000,
            ..healthy_inputs()
        };
        let s = evaluate(&inputs).score;
        assert!(s <= 100);
    }
}
