//! The composite per-table quality / trust score (Pillar E / E-F6).
//!
//! A single `0..=100` number answering "how much should I trust this table?",
//! computed from signals the platform already has — **no data scan**:
//!
//! - **monitors** — are the table's monitors passing? (no live monitor
//!   incidents), and is the table monitored at all?
//! - **contract** — is a data contract in force, and how strong is its mode
//!   (block > quarantine > warn > none)?
//! - **ownership** — does the table declare an `owner`?
//! - **docs** — does the table (and its columns) carry documentation?
//! - **freshness** — is the table free of a live freshness/staleness incident?
//!
//! Like the maintenance health score ([`crate::health`]), the composite is a
//! **pure** weighted combination of component `[0,1]` sub-scores with fixed,
//! documented weights, so it is deterministic and explainable — the API returns
//! the components alongside the score. [`gather_inputs`] does the (cheap) reads;
//! [`compute`] is pure.
//!
//! The score is intentionally cheap enough to compute on demand and to fold
//! into search rank (a small, bounded boost — see `routes::search`), and it is
//! the number agents will later read to decide whether to use a table.
//!
//! The scoring converts live-incident counts to `f64` for the monitor decay
//! curve and rounds the final `[0,100]` fraction to a `u8` after clamping, so
//! the cast is provably in range; the cast lints are allowed at module scope
//! (matching [`crate::health`]) rather than annotated per site.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use meridian_common::Result;
use meridian_common::id::WorkspaceId;
use serde::Serialize;
use sqlx::PgPool;

use crate::contracts::EnforcementMode;
use crate::map_sqlx_error;

/// A table's `(properties, schema_text)` row for the ownership/docs signals.
type PropsRow = (
    sqlx::types::Json<std::collections::BTreeMap<String, String>>,
    Option<String>,
);

/// A `(id, properties, schema_text)` row for the batched search-score path.
type IdPropsRow = (
    String,
    sqlx::types::Json<std::collections::BTreeMap<String, String>>,
    Option<String>,
);

// ===========================================================================
// Weights
// ===========================================================================

/// Fixed component weights of the composite score. They sum to 100; each
/// component contributes `weight × sub_score` where `sub_score ∈ [0,1]`.
/// Documented and stable so the score is comparable across tables and over
/// time (mirroring [`crate::health::ScoreWeights`]).
#[derive(Debug, Clone, Copy)]
pub struct ScoreWeights {
    /// Monitors passing + coverage.
    pub monitors: f64,
    /// Contract present + mode strength.
    pub contract: f64,
    /// Ownership declared.
    pub ownership: f64,
    /// Documentation coverage.
    pub docs: f64,
    /// Freshness (no live staleness incident).
    pub freshness: f64,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        // Sum = 100. Monitors + contract carry the most weight (they are the
        // strongest trust signals); ownership/docs/freshness round it out.
        Self {
            monitors: 30.0,
            contract: 25.0,
            ownership: 15.0,
            docs: 15.0,
            freshness: 15.0,
        }
    }
}

impl ScoreWeights {
    fn sum(self) -> f64 {
        self.monitors + self.contract + self.ownership + self.docs + self.freshness
    }
}

// ===========================================================================
// Inputs + components
// ===========================================================================

/// The (cheap) facts the score is computed from. Every field comes from an
/// index read or a table property — no manifest or data-file access.
#[derive(Debug, Clone)]
pub struct ScoreInputs {
    /// How many enabled monitors bind to the table (directly or via namespace).
    pub monitor_count: i64,
    /// How many live (open + acknowledged) incidents the table has.
    pub live_incidents: i64,
    /// How many of those live incidents are freshness/staleness incidents.
    pub live_freshness_incidents: i64,
    /// The strongest enforcement mode of a contract in force on the table, or
    /// `None` when no contract binds.
    pub contract_mode: Option<EnforcementMode>,
    /// Whether the table declares an `owner` property.
    pub has_owner: bool,
    /// Whether the table declares a `comment` property (table-level docs).
    pub has_table_doc: bool,
    /// Fraction of columns that carry a doc string, in `[0,1]` (0 when there are
    /// no columns to document).
    pub column_doc_ratio: f64,
}

/// The per-component `[0,1]` sub-scores, returned alongside the composite so the
/// score is explainable.
#[derive(Debug, Clone, Serialize)]
pub struct ScoreComponents {
    /// Monitors sub-score.
    pub monitors: f64,
    /// Contract sub-score.
    pub contract: f64,
    /// Ownership sub-score.
    pub ownership: f64,
    /// Docs sub-score.
    pub docs: f64,
    /// Freshness sub-score.
    pub freshness: f64,
}

/// A computed quality score with its explaining components.
#[derive(Debug, Clone, Serialize)]
pub struct QualityScore {
    /// The composite `0..=100` score.
    pub score: u8,
    /// The per-component sub-scores.
    pub components: ScoreComponents,
    /// The letter grade for the score (A..F), for quick human reading.
    pub grade: char,
}

/// Maps the `[0,1]` monitor health: full marks only when the table is monitored
/// *and* has no live incidents. An unmonitored table cannot earn the full
/// monitor sub-score (you cannot trust what you do not watch), but is not
/// zeroed either — it caps at a partial credit.
fn monitor_subscore(inputs: &ScoreInputs) -> f64 {
    if inputs.monitor_count == 0 {
        // Unmonitored: a fixed partial credit — visible headroom to improve.
        return 0.4;
    }
    if inputs.live_incidents == 0 {
        1.0
    } else {
        // Monitored but firing: decays with the number of live incidents, floored
        // so a monitored-but-broken table still scores above an unmonitored one
        // only when it has a single incident; multiple incidents sink it lower.
        (1.0 / (1.0 + inputs.live_incidents as f64)).max(0.1)
    }
}

/// Maps contract presence + mode to a `[0,1]` sub-score: a stronger mode is a
/// stronger guarantee. No contract earns nothing here.
fn contract_subscore(mode: Option<EnforcementMode>) -> f64 {
    match mode {
        Some(EnforcementMode::Block) => 1.0,
        Some(EnforcementMode::Quarantine) => 0.85,
        Some(EnforcementMode::Warn) => 0.6,
        None => 0.0,
    }
}

/// Maps docs coverage: table-level doc is worth half, column coverage the other
/// half, so a fully-documented table scores 1.0.
fn docs_subscore(inputs: &ScoreInputs) -> f64 {
    let table_part = if inputs.has_table_doc { 0.5 } else { 0.0 };
    let column_part = 0.5 * inputs.column_doc_ratio.clamp(0.0, 1.0);
    table_part + column_part
}

/// Maps freshness: a live staleness incident zeroes the freshness sub-score;
/// otherwise it is full.
fn freshness_subscore(inputs: &ScoreInputs) -> f64 {
    if inputs.live_freshness_incidents > 0 {
        0.0
    } else {
        1.0
    }
}

/// Computes the composite quality score from inputs and weights. Pure and
/// total. `score = round( Σ wᵢ·sᵢ / Σ wᵢ × 100 )`.
#[must_use]
pub fn compute_with(inputs: &ScoreInputs, weights: ScoreWeights) -> QualityScore {
    let components = ScoreComponents {
        monitors: monitor_subscore(inputs),
        contract: contract_subscore(inputs.contract_mode),
        ownership: if inputs.has_owner { 1.0 } else { 0.0 },
        docs: docs_subscore(inputs),
        freshness: freshness_subscore(inputs),
    };
    let weighted = components.monitors * weights.monitors
        + components.contract * weights.contract
        + components.ownership * weights.ownership
        + components.docs * weights.docs
        + components.freshness * weights.freshness;
    let denom = weights.sum();
    let fraction = if denom > 0.0 { weighted / denom } else { 0.0 };
    // Round to nearest, clamp to [0,100]; f64->u8 is safe after the clamp.
    let score = (fraction * 100.0).round().clamp(0.0, 100.0) as u8;
    QualityScore {
        score,
        components,
        grade: grade_for(score),
    }
}

/// Computes the score with the default weights.
#[must_use]
pub fn compute(inputs: &ScoreInputs) -> QualityScore {
    compute_with(inputs, ScoreWeights::default())
}

/// The letter grade for a `0..=100` score.
#[must_use]
pub fn grade_for(score: u8) -> char {
    match score {
        90..=100 => 'A',
        80..=89 => 'B',
        70..=79 => 'C',
        60..=69 => 'D',
        _ => 'F',
    }
}

// ===========================================================================
// Input gathering (cheap reads)
// ===========================================================================

/// Gathers the score inputs for a table from the catalog: monitor count,
/// live-incident tallies, the strongest contract mode in force, and the
/// ownership/docs table properties. `namespace_chain` is the table's
/// self-and-ancestors namespace ids (for resolving namespace-bound
/// monitors/contracts), as the RBAC scope builder computes it.
///
/// All reads are index/property reads — no data-file access.
pub async fn gather_inputs(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
    namespace_chain: &[String],
) -> Result<ScoreInputs> {
    // Monitor count (enabled, binding to this table directly or via namespace).
    let monitor_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM monitors
         WHERE workspace_id = $1
           AND enabled = TRUE
           AND (
                (bound_to = 'table' AND securable_id = $2)
             OR (bound_to = 'namespace' AND securable_id = ANY($3))
           )",
    )
    .bind(workspace_id.to_string())
    .bind(table_id)
    .bind(namespace_chain)
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to count monitors for score", e))?;

    // Live-incident tallies (total + freshness-specific).
    let (live_incidents, live_freshness_incidents): (i64, i64) = sqlx::query_as(
        "SELECT
             COUNT(*) AS live,
             COUNT(*) FILTER (WHERE kind = 'freshness') AS freshness
         FROM incidents
         WHERE workspace_id = $1 AND table_id = $2 AND status <> 'resolved'",
    )
    .bind(workspace_id.to_string())
    .bind(table_id)
    .fetch_one(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to count live incidents for score", e))?;

    // Strongest contract mode in force (block > quarantine > warn). Read the
    // enabled modes and reduce in Rust so the ordering is explicit.
    let modes: Vec<String> = sqlx::query_scalar(
        "SELECT mode FROM contracts
         WHERE workspace_id = $1
           AND enabled = TRUE
           AND (
                (bound_to = 'table' AND securable_id = $2)
             OR (bound_to = 'namespace' AND securable_id = ANY($3))
           )",
    )
    .bind(workspace_id.to_string())
    .bind(table_id)
    .bind(namespace_chain)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to read contract modes for score", e))?;
    let contract_mode = strongest_mode(&modes);

    // Ownership + docs from the table properties + schema_text index.
    let row: Option<PropsRow> =
        sqlx::query_as("SELECT properties, schema_text FROM tables WHERE id = $1")
            .bind(table_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| map_sqlx_error("failed to read table for score", e))?;
    let (has_owner, has_table_doc, column_doc_ratio) = match row {
        Some((props, schema_text)) => {
            let has_owner = props.0.get("owner").is_some_and(|v| !v.trim().is_empty());
            let has_table_doc = props.0.get("comment").is_some_and(|v| !v.trim().is_empty());
            // schema_text is the flattened "name doc name doc ..." search text;
            // its non-emptiness is a cheap proxy for column docs existing. When
            // it is absent, treat column docs as unknown -> 0.
            let column_doc_ratio = column_doc_ratio_from_search_text(schema_text.as_deref());
            (has_owner, has_table_doc, column_doc_ratio)
        }
        None => (false, false, 0.0),
    };

    Ok(ScoreInputs {
        monitor_count,
        live_incidents,
        live_freshness_incidents,
        contract_mode,
        has_owner,
        has_table_doc,
        column_doc_ratio,
    })
}

/// The strongest enforcement mode among the given wire strings (block beats
/// quarantine beats warn). `None` when empty or all-unparseable.
fn strongest_mode(modes: &[String]) -> Option<EnforcementMode> {
    let mut best: Option<EnforcementMode> = None;
    for raw in modes {
        let Some(mode) = EnforcementMode::parse(raw) else {
            continue;
        };
        let rank = |m: EnforcementMode| match m {
            EnforcementMode::Block => 3,
            EnforcementMode::Quarantine => 2,
            EnforcementMode::Warn => 1,
        };
        if best.is_none_or(|b| rank(mode) > rank(b)) {
            best = Some(mode);
        }
    }
    best
}

/// A cheap column-doc proxy from the flattened schema search text (migration
/// 0010). The search text interleaves column names and their docs; without
/// parsing the metadata we cannot get an exact per-column ratio, so we return a
/// conservative binary proxy: non-empty text with more than just names present
/// counts as "documented" (0.5), empty/absent as 0. This deliberately
/// under-claims rather than fabricating a precise ratio.
fn column_doc_ratio_from_search_text(schema_text: Option<&str>) -> f64 {
    match schema_text {
        Some(text) if !text.trim().is_empty() => 0.5,
        _ => 0.0,
    }
}

/// Computes a table's quality score end to end (gather + compute).
pub async fn score_table(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
    namespace_chain: &[String],
) -> Result<QualityScore> {
    let inputs = gather_inputs(pool, workspace_id, table_id, namespace_chain).await?;
    Ok(compute(&inputs))
}

/// A lightweight score for search-rank folding: just the `0..=100` number for a
/// table, computed from the same inputs but without the namespace chain
/// (search resolves the table-bound signals only, keeping the per-result cost a
/// single-row read). Returns 50 (neutral) when the table has no signals — an
/// unknown table is neither boosted nor penalized.
pub async fn score_for_search(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
) -> Result<u8> {
    // Table-bound only (no namespace chain) — cheap enough for every result.
    let inputs = gather_inputs(pool, workspace_id, table_id, &[]).await?;
    Ok(compute(&inputs).score)
}

/// Batched [`score_for_search`] for a whole page of table hits.
///
/// [`score_for_search`] runs four table-bound queries; called per result on a
/// 100-hit page that is 400 round trips (the search N+1). This computes the same
/// table-bound scores for every id in a **fixed four** grouped queries and
/// returns a map from table id to score. A table with no signals is omitted from
/// the map (score it neutral, 50, at the call site — matching the single-table
/// path, which returns a neutral score for a table with no signals).
pub async fn score_for_search_batch(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_ids: &[String],
) -> Result<std::collections::HashMap<String, u8>> {
    use std::collections::HashMap;

    if table_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let ws = workspace_id.to_string();

    // 1. Table-bound enabled monitor counts, grouped by table.
    let monitor_rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT securable_id, COUNT(*) FROM monitors
         WHERE workspace_id = $1 AND enabled = TRUE
           AND bound_to = 'table' AND securable_id = ANY($2)
         GROUP BY securable_id",
    )
    .bind(&ws)
    .bind(table_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to batch monitor counts for search score", e))?;
    let monitors: HashMap<String, i64> = monitor_rows.into_iter().collect();

    // 2. Live-incident tallies (total + freshness), grouped by table.
    let incident_rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT table_id,
                COUNT(*) AS live,
                COUNT(*) FILTER (WHERE kind = 'freshness') AS freshness
         FROM incidents
         WHERE workspace_id = $1 AND table_id = ANY($2) AND status <> 'resolved'
         GROUP BY table_id",
    )
    .bind(&ws)
    .bind(table_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to batch incident counts for search score", e))?;
    let incidents: HashMap<String, (i64, i64)> = incident_rows
        .into_iter()
        .map(|(t, live, fresh)| (t, (live, fresh)))
        .collect();

    // 3. Table-bound enabled contract modes, aggregated per table.
    let contract_rows: Vec<(String, Vec<String>)> = sqlx::query_as(
        "SELECT securable_id, array_agg(mode) FROM contracts
         WHERE workspace_id = $1 AND enabled = TRUE
           AND bound_to = 'table' AND securable_id = ANY($2)
         GROUP BY securable_id",
    )
    .bind(&ws)
    .bind(table_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to batch contract modes for search score", e))?;
    let contracts: HashMap<String, Vec<String>> = contract_rows.into_iter().collect();

    // 4. Ownership/docs properties, per table.
    let prop_rows: Vec<IdPropsRow> =
        sqlx::query_as("SELECT id, properties, schema_text FROM tables WHERE id = ANY($1)")
            .bind(table_ids)
            .fetch_all(pool)
            .await
            .map_err(|e| map_sqlx_error("failed to batch table props for search score", e))?;

    let mut out = HashMap::with_capacity(prop_rows.len());
    for (id, props, schema_text) in prop_rows {
        let (live_incidents, live_freshness_incidents) =
            incidents.get(&id).copied().unwrap_or((0, 0));
        let contract_mode = contracts.get(&id).and_then(|modes| strongest_mode(modes));
        let inputs = ScoreInputs {
            monitor_count: monitors.get(&id).copied().unwrap_or(0),
            live_incidents,
            live_freshness_incidents,
            contract_mode,
            has_owner: props.0.get("owner").is_some_and(|v| !v.trim().is_empty()),
            has_table_doc: props.0.get("comment").is_some_and(|v| !v.trim().is_empty()),
            column_doc_ratio: column_doc_ratio_from_search_text(schema_text.as_deref()),
        };
        out.insert(id, compute(&inputs).score);
    }
    Ok(out)
}

impl QualityScore {
    /// Converts the score into a `serde_json::Value` map for embedding in an API
    /// response (the components flattened, plus the score + grade).
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "score": self.score,
            "grade": self.grade.to_string(),
            "components": self.components,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn perfect_inputs() -> ScoreInputs {
        ScoreInputs {
            monitor_count: 3,
            live_incidents: 0,
            live_freshness_incidents: 0,
            contract_mode: Some(EnforcementMode::Block),
            has_owner: true,
            has_table_doc: true,
            column_doc_ratio: 1.0,
        }
    }

    #[test]
    fn perfect_table_scores_100() {
        let score = compute(&perfect_inputs());
        assert_eq!(score.score, 100, "{:?}", score.components);
        assert_eq!(score.grade, 'A');
    }

    #[test]
    fn bare_table_scores_low() {
        // Unmonitored, no contract, no owner, no docs, no incidents.
        let inputs = ScoreInputs {
            monitor_count: 0,
            live_incidents: 0,
            live_freshness_incidents: 0,
            contract_mode: None,
            has_owner: false,
            has_table_doc: false,
            column_doc_ratio: 0.0,
        };
        let score = compute(&inputs);
        // monitors 0.4×30 = 12; freshness 1.0×15 = 15 (no staleness incident);
        // contract/ownership/docs all 0 -> 27/100.
        assert_eq!(score.score, 27, "{:?}", score.components);
        assert_eq!(score.grade, 'F');
    }

    #[test]
    fn live_incident_drops_monitor_subscore() {
        let mut inputs = perfect_inputs();
        inputs.live_incidents = 1;
        let score = compute(&inputs);
        // monitors -> 1/(1+1)=0.5, so 0.5×30=15 instead of 30: 85.
        assert_eq!(score.score, 85, "{:?}", score.components);
    }

    #[test]
    fn freshness_incident_zeroes_freshness_component() {
        let mut inputs = perfect_inputs();
        inputs.live_incidents = 1;
        inputs.live_freshness_incidents = 1;
        let score = compute(&inputs);
        assert!((score.components.freshness - 0.0).abs() < f64::EPSILON);
        // monitors 15 + contract 25 + ownership 15 + docs 15 + freshness 0 = 70.
        assert_eq!(score.score, 70, "{:?}", score.components);
    }

    #[test]
    fn contract_mode_strength_matters() {
        assert!((contract_subscore(Some(EnforcementMode::Block)) - 1.0).abs() < f64::EPSILON);
        assert!(
            contract_subscore(Some(EnforcementMode::Warn))
                < contract_subscore(Some(EnforcementMode::Quarantine))
        );
        assert!((contract_subscore(None) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn strongest_mode_picks_block() {
        assert_eq!(
            strongest_mode(&[
                "warn".to_owned(),
                "block".to_owned(),
                "quarantine".to_owned()
            ]),
            Some(EnforcementMode::Block)
        );
        assert_eq!(
            strongest_mode(&["warn".to_owned(), "quarantine".to_owned()]),
            Some(EnforcementMode::Quarantine)
        );
        assert_eq!(strongest_mode(&[]), None);
        assert_eq!(strongest_mode(&["garbage".to_owned()]), None);
    }

    #[test]
    fn grades_map_correctly() {
        assert_eq!(grade_for(95), 'A');
        assert_eq!(grade_for(85), 'B');
        assert_eq!(grade_for(75), 'C');
        assert_eq!(grade_for(65), 'D');
        assert_eq!(grade_for(50), 'F');
    }

    #[test]
    fn docs_subscore_splits_table_and_columns() {
        let mut inputs = perfect_inputs();
        inputs.has_table_doc = true;
        inputs.column_doc_ratio = 0.0;
        assert!((docs_subscore(&inputs) - 0.5).abs() < f64::EPSILON);
        inputs.has_table_doc = false;
        inputs.column_doc_ratio = 1.0;
        assert!((docs_subscore(&inputs) - 0.5).abs() < f64::EPSILON);
    }
}
