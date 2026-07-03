//! The planning core: manifest pruning, task construction, delete-file
//! attachment, residual filters, and result-page assembly.
//!
//! Correctness sources (in priority order): the Iceberg table spec's
//! "Scan Planning" section (delete-application scope rules quoted at the
//! functions that implement them) and the REST `OpenAPI` document (wire
//! shapes, built in [`super::rest`]). Pruning is *inclusive* end to end —
//! wave-one evaluators in `meridian_iceberg::expr` never prune a
//! container that could hold a matching row — and everything this module
//! adds on top (equality-delete stats restriction, residual folding)
//! preserves that direction: in doubt, keep the file / keep the
//! predicate.
//!
//! ## Residual filters
//!
//! The returned `residual-filter` is the part of the request filter not
//! already guaranteed by pruning. A leaf predicate folds to a constant
//! when its term is *exactly determined* by the file's partition tuple —
//! the term's transform (identity for plain references) matches a
//! partition field over the same source column — evaluated exactly
//! against the stored partition value. Null partition values fold only
//! the unambiguous `is-null`/`not-null` (Iceberg evaluator families
//! disagree with SQL three-valued logic on `not-eq`/`not-in` over null,
//! so those leaves are kept for the client to evaluate). Everything else
//! stays in the residual; keeping a predicate is always sound.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use meridian_iceberg::expr::{
    BoundPredicate, CompareOp, Expression, PartitionPredicate, SetOp, Term, UnaryOp,
    file_might_match, project, summaries_might_match, tuple_might_match,
};
use meridian_iceberg::manifest::{
    DataFile, DataFileContent, Manifest, ManifestContentType, ManifestEntryStatus,
    PartitionFieldType, PartitionTuple, partition_field_types,
};
use meridian_iceberg::spec::{PrimitiveType, Schema, TableMetadata, Transform, Type};
use meridian_iceberg::value::Datum;
use serde_json::{Value, json};

use super::rest;

/// Reserved field id of the `file_path` column in position delete files
/// (spec "Reserved Field IDs").
const FILE_PATH_FIELD_ID: i32 = 2_147_483_546;

/// A planning failure. `Unreadable` covers storage and parse problems —
/// catalog-side issues surfaced loudly, never masked as client errors.
#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    /// A manifest list or manifest could not be fetched or parsed.
    #[error("manifest at {location:?} is unreadable: {reason}")]
    Unreadable {
        /// The storage location.
        location: String,
        /// What went wrong.
        reason: String,
    },
}

/// A matched data file, ready for serialization.
#[derive(Debug)]
pub struct PlannedFile {
    /// The data file.
    pub file: DataFile,
    /// Partition spec id from the manifest-list entry.
    pub spec_id: i32,
    /// Residual filter for this file (`None` when the request had no
    /// filter).
    pub residual: Option<Expression>,
    /// Indices into [`PlanOutcome::deletes`], ascending.
    pub delete_indices: Vec<usize>,
}

/// A delete file that survived pruning (it may or may not end up
/// referenced by a task; pages only carry referenced deletes).
#[derive(Debug)]
pub struct PlannedDelete {
    /// The delete file.
    pub file: DataFile,
    /// Partition spec id from the manifest-list entry.
    pub spec_id: i32,
    /// Inherited data sequence number.
    pub sequence_number: i64,
}

/// Pruning and cache counters for the plan summary.
#[derive(Debug, Default)]
pub struct PlanCounters {
    /// Manifests listed in the manifest list.
    pub manifests_total: u64,
    /// Manifests skipped via partition summaries or zero live counts.
    pub manifests_pruned: u64,
    /// Live data files inspected.
    pub data_files_seen: u64,
    /// Data files pruned by partition tuple.
    pub data_files_pruned_partition: u64,
    /// Data files pruned by column statistics.
    pub data_files_pruned_stats: u64,
    /// Live delete files inspected.
    pub delete_files_seen: u64,
    /// Delete files pruned (partition tuple, or equality-column stats).
    pub delete_files_pruned: u64,
}

impl PlanCounters {
    /// The summary JSON stored on the plan row and logged.
    #[must_use]
    pub fn summary(&self, matched_files: usize, referenced_deletes: usize) -> Value {
        json!({
            "matched_data_files": matched_files,
            "referenced_delete_files": referenced_deletes,
            "manifests": { "total": self.manifests_total, "pruned": self.manifests_pruned },
            "data_files": {
                "seen": self.data_files_seen,
                "pruned_partition": self.data_files_pruned_partition,
                "pruned_stats": self.data_files_pruned_stats,
            },
            "delete_files": {
                "seen": self.delete_files_seen,
                "pruned": self.delete_files_pruned,
            },
        })
    }
}

/// The planning result, pre-serialization.
#[derive(Debug)]
pub struct PlanOutcome {
    /// Matched data files, in manifest-list-then-entry order (stable for
    /// a given snapshot — pagination is deterministic).
    pub files: Vec<PlannedFile>,
    /// Delete files the tasks reference into.
    pub deletes: Vec<PlannedDelete>,
    /// Pruning counters.
    pub counters: PlanCounters,
}

/// A data file that survived pruning, before delete attachment.
struct DataCandidate {
    file: DataFile,
    spec_id: i32,
    sequence_number: i64,
    residual: Option<Expression>,
}

/// Everything the pruning pass needs about one partition spec.
struct SpecPruning {
    types: Vec<PartitionFieldType>,
    projected: Option<PartitionPredicate>,
}

/// Per-spec pruning info, resolved lazily from table metadata. Specs that
/// cannot be resolved against the scan schema (dropped source columns,
/// unknown transforms) get no pruning and no residual folding — files
/// under them are kept with the full filter as residual.
struct SpecTable<'a> {
    metadata: &'a TableMetadata,
    schema: &'a Schema,
    bound: Option<&'a BoundPredicate>,
    by_id: HashMap<i32, Option<Arc<SpecPruning>>>,
}

impl<'a> SpecTable<'a> {
    fn new(
        metadata: &'a TableMetadata,
        schema: &'a Schema,
        bound: Option<&'a BoundPredicate>,
    ) -> Self {
        Self {
            metadata,
            schema,
            bound,
            by_id: HashMap::new(),
        }
    }

    fn get(&mut self, spec_id: i32) -> Option<Arc<SpecPruning>> {
        if let Some(cached) = self.by_id.get(&spec_id) {
            return cached.clone();
        }
        let resolved = self
            .metadata
            .partition_specs
            .iter()
            .find(|s| s.spec_id == Some(spec_id))
            .and_then(|spec| partition_field_types(&spec.fields, self.schema).ok())
            .map(|types| {
                let projected = self.bound.map(|b| project(b, &types));
                Arc::new(SpecPruning { types, projected })
            });
        self.by_id.insert(spec_id, resolved.clone());
        resolved
    }
}

/// Async source of parsed manifests (implemented by the tiered cache in
/// [`super`]; a plain in-memory map in tests).
pub trait ManifestSource {
    /// Fetches and parses the snapshot's manifest list.
    fn manifest_list(
        &self,
        location: &str,
    ) -> impl Future<Output = Result<Arc<meridian_iceberg::manifest::ManifestList>, PlanError>>;
    /// Fetches and parses one manifest.
    fn manifest(&self, location: &str) -> impl Future<Output = Result<Arc<Manifest>, PlanError>>;
}

/// Plans a scan of `snapshot` under `bound` (already bound against
/// `schema`). See the module docs for the correctness rules.
pub async fn plan_scan<S: ManifestSource>(
    source: &S,
    metadata: &TableMetadata,
    schema: &Schema,
    manifest_list_location: &str,
    bound: Option<&BoundPredicate>,
) -> Result<PlanOutcome, PlanError> {
    let list = source.manifest_list(manifest_list_location).await?;
    let mut specs = SpecTable::new(metadata, schema, bound);
    let mut counters = PlanCounters::default();
    let mut data: Vec<DataCandidate> = Vec::new();
    let mut deletes: Vec<PlannedDelete> = Vec::new();

    for manifest_ref in &list.manifests {
        counters.manifests_total += 1;

        // Empty manifests (no ADDED or EXISTING entries) hold nothing live.
        if manifest_ref.added_files_count == Some(0) && manifest_ref.existing_files_count == Some(0)
        {
            counters.manifests_pruned += 1;
            continue;
        }

        let spec = specs.get(manifest_ref.partition_spec_id);
        // Partition-summary pruning applies to data AND delete manifests:
        // a delete constrained to pruned partitions cannot apply to any
        // kept data file (application requires partition equality under
        // the same spec; unpartitioned specs project to `true` and are
        // never pruned here).
        if let (Some(spec), Some(summaries)) = (&spec, &manifest_ref.partitions)
            && let Some(projected) = &spec.projected
            && !summaries_might_match(projected, summaries, &spec.types)
        {
            counters.manifests_pruned += 1;
            continue;
        }

        let manifest = source.manifest(&manifest_ref.manifest_path).await?;
        collect_live_entries(
            &manifest,
            manifest_ref,
            spec.as_deref(),
            bound,
            &mut counters,
            &mut data,
            &mut deletes,
        );
    }

    let index = DeleteIndex::build(&deletes);
    let files = data
        .into_iter()
        .map(|candidate| {
            let delete_indices = index.deletes_for(
                &deletes,
                &candidate.file,
                candidate.spec_id,
                candidate.sequence_number,
            );
            PlannedFile {
                file: candidate.file,
                spec_id: candidate.spec_id,
                residual: candidate.residual,
                delete_indices,
            }
        })
        .collect();

    Ok(PlanOutcome {
        files,
        deletes,
        counters,
    })
}

/// One manifest's live entries: prune, classify, and collect them.
#[allow(clippy::too_many_arguments)] // a plain fan-out of plan_scan locals
fn collect_live_entries(
    manifest: &Manifest,
    manifest_ref: &meridian_iceberg::manifest::ManifestFile,
    spec: Option<&SpecPruning>,
    bound: Option<&BoundPredicate>,
    counters: &mut PlanCounters,
    data: &mut Vec<DataCandidate>,
    deletes: &mut Vec<PlannedDelete>,
) {
    for stored in &manifest.entries {
        if stored.status == ManifestEntryStatus::Deleted {
            continue;
        }
        let mut entry = stored.clone();
        entry.inherit_from(manifest_ref);
        // Malformed files may still lack a sequence number after
        // inheritance; default 0 (v1 semantics).
        let sequence_number = entry.sequence_number.unwrap_or(0);

        let is_data = manifest.metadata.content == ManifestContentType::Data
            && entry.data_file.content == DataFileContent::Data;
        if is_data {
            counters.data_files_seen += 1;
        } else {
            counters.delete_files_seen += 1;
        }

        // Partition-tuple pruning (field-id-based lookups, so it holds
        // regardless of tuple field order).
        if let Some(spec) = spec
            && let Some(projected) = &spec.projected
            && !tuple_might_match(projected, &entry.data_file.partition)
        {
            if is_data {
                counters.data_files_pruned_partition += 1;
            } else {
                counters.delete_files_pruned += 1;
            }
            continue;
        }

        if is_data {
            // Column-statistics pruning.
            if let Some(bound) = bound
                && !file_might_match(bound, &entry.data_file)
            {
                counters.data_files_pruned_stats += 1;
                continue;
            }
            let residual = bound.map(|b| {
                residual_expression(
                    b,
                    &entry.data_file.partition,
                    spec.map_or(&[], |s| s.types.as_slice()),
                )
            });
            data.push(DataCandidate {
                file: entry.data_file,
                spec_id: manifest_ref.partition_spec_id,
                sequence_number,
                residual,
            });
        } else {
            // Equality deletes can additionally be pruned by column
            // stats — but only over their equality columns (stats on
            // other columns describe the deleted rows' payload, not
            // which rows the delete removes).
            if entry.data_file.content == DataFileContent::EqualityDeletes
                && let (Some(bound), Some(eq_ids)) = (bound, &entry.data_file.equality_ids)
            {
                let allowed: BTreeSet<i32> = eq_ids.iter().copied().collect();
                let restricted = restrict_to_fields(bound, &allowed);
                if !file_might_match(&restricted, &entry.data_file) {
                    counters.delete_files_pruned += 1;
                    continue;
                }
            }
            deletes.push(PlannedDelete {
                file: entry.data_file,
                spec_id: manifest_ref.partition_spec_id,
                sequence_number,
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Delete-file attachment (spec "Scan Planning" scope rules)
// ---------------------------------------------------------------------------

/// A canonical, hashable partition identity: spec id plus the tuple's
/// field ids and Appendix-D value bytes. Two partitions are equal exactly
/// when the spec ids match and every field value matches.
fn partition_key(spec_id: i32, tuple: &PartitionTuple) -> Vec<u8> {
    let mut key = spec_id.to_le_bytes().to_vec();
    for field in &tuple.fields {
        key.extend_from_slice(&field.field_id.to_le_bytes());
        match &field.value {
            None => key.push(0),
            Some(datum) => {
                key.push(1);
                let bytes = datum.to_bound_bytes();
                key.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
                key.extend_from_slice(&bytes);
            }
        }
    }
    key
}

/// Decoded `file_path` column bounds of a position delete file, when
/// present (reserved field 2147483546).
fn path_bounds(file: &DataFile) -> (Option<String>, Option<String>) {
    let decode = |bounds: Option<&BTreeMap<i32, Vec<u8>>>| {
        bounds
            .and_then(|m| m.get(&FILE_PATH_FIELD_ID))
            .and_then(
                |b| match Datum::from_bound_bytes(&PrimitiveType::String, b) {
                    Ok(Datum::String(s)) => Some(s),
                    _ => None,
                },
            )
    };
    (
        decode(file.lower_bounds.as_ref()),
        decode(file.upper_bounds.as_ref()),
    )
}

/// Groups delete candidates for near-constant-time attachment per data
/// file.
struct DeleteIndex {
    /// Deletion vectors by `referenced_data_file`.
    dv_by_path: HashMap<String, Vec<usize>>,
    /// Plain position deletes with `referenced_data_file`, by that path.
    pos_by_path: HashMap<String, Vec<usize>>,
    /// Remaining position deletes by partition identity.
    pos_by_partition: HashMap<Vec<u8>, Vec<usize>>,
    /// Equality deletes written under an unpartitioned spec (global).
    eq_global: Vec<usize>,
    /// Equality deletes by partition identity.
    eq_by_partition: HashMap<Vec<u8>, Vec<usize>>,
}

impl DeleteIndex {
    fn build(deletes: &[PlannedDelete]) -> Self {
        let mut index = Self {
            dv_by_path: HashMap::new(),
            pos_by_path: HashMap::new(),
            pos_by_partition: HashMap::new(),
            eq_global: Vec::new(),
            eq_by_partition: HashMap::new(),
        };
        for (i, delete) in deletes.iter().enumerate() {
            match delete.file.content {
                DataFileContent::PositionDeletes => {
                    let is_dv = delete.file.content_offset.is_some();
                    match (&delete.file.referenced_data_file, is_dv) {
                        (Some(path), true) => {
                            index.dv_by_path.entry(path.clone()).or_default().push(i);
                        }
                        (Some(path), false) => {
                            index.pos_by_path.entry(path.clone()).or_default().push(i);
                        }
                        // A DV without referenced_data_file is malformed
                        // (v3 requires it); treat it as a plain
                        // partition-scoped position delete — conservative.
                        (None, _) => {
                            index
                                .pos_by_partition
                                .entry(partition_key(delete.spec_id, &delete.file.partition))
                                .or_default()
                                .push(i);
                        }
                    }
                }
                DataFileContent::EqualityDeletes => {
                    if delete.file.partition.fields.is_empty() {
                        index.eq_global.push(i);
                    } else {
                        index
                            .eq_by_partition
                            .entry(partition_key(delete.spec_id, &delete.file.partition))
                            .or_default()
                            .push(i);
                    }
                }
                // Data content inside a delete manifest is malformed;
                // ignore rather than invent semantics.
                DataFileContent::Data => {}
            }
        }
        index
    }

    /// Applies the spec's scope rules for one data file; returns the
    /// ascending indices of the deletes to attach.
    ///
    /// - Deletion vector: `file_path == referenced_data_file`, data seq
    ///   `<=` DV seq, partition equal. When one applies, plain position
    ///   delete files for this data file are ignored (the DV subsumes
    ///   them).
    /// - Position delete file: `referenced_data_file` (if non-null) equal,
    ///   data seq `<=` delete seq, partition equal; `file_path` column
    ///   bounds, when present, must admit the data file's path.
    /// - Equality delete file: data seq `<` delete seq (strict), partition
    ///   equal or the delete's spec is unpartitioned (global).
    fn deletes_for(
        &self,
        deletes: &[PlannedDelete],
        file: &DataFile,
        spec_id: i32,
        data_sequence: i64,
    ) -> Vec<usize> {
        let key = partition_key(spec_id, &file.partition);
        let partitions_equal = |i: usize| {
            let d = &deletes[i];
            d.spec_id == spec_id && partition_key(d.spec_id, &d.file.partition) == key
        };

        let mut attached: Vec<usize> = Vec::new();

        let dvs: Vec<usize> = self
            .dv_by_path
            .get(&file.file_path)
            .into_iter()
            .flatten()
            .copied()
            .filter(|&i| data_sequence <= deletes[i].sequence_number && partitions_equal(i))
            .collect();
        let has_dv = !dvs.is_empty();
        attached.extend(dvs);

        if !has_dv {
            attached.extend(
                self.pos_by_path
                    .get(&file.file_path)
                    .into_iter()
                    .flatten()
                    .copied()
                    .filter(|&i| {
                        data_sequence <= deletes[i].sequence_number && partitions_equal(i)
                    }),
            );
            attached.extend(
                self.pos_by_partition
                    .get(&key)
                    .into_iter()
                    .flatten()
                    .copied()
                    .filter(|&i| {
                        if data_sequence > deletes[i].sequence_number {
                            return false;
                        }
                        // Path-bounds narrowing: a position delete whose
                        // stored file_path range excludes this path holds
                        // no positions for it.
                        let (lower, upper) = path_bounds(&deletes[i].file);
                        if lower
                            .as_deref()
                            .is_some_and(|l| file.file_path.as_str() < l)
                        {
                            return false;
                        }
                        if upper.as_deref().is_some_and(|u| {
                            // An upper bound may be truncated by the
                            // writer's metrics config; a truncated bound
                            // is a prefix-inclusive ceiling, so only prune
                            // when the path is strictly above AND not an
                            // extension of the bound.
                            file.file_path.as_str() > u && !file.file_path.starts_with(u)
                        }) {
                            return false;
                        }
                        true
                    }),
            );
        }

        attached.extend(
            self.eq_global
                .iter()
                .chain(self.eq_by_partition.get(&key).into_iter().flatten())
                .copied()
                .filter(|&i| data_sequence < deletes[i].sequence_number),
        );

        attached.sort_unstable();
        attached.dedup();
        attached
    }
}

// ---------------------------------------------------------------------------
// Predicate restriction (equality-delete stats pruning)
// ---------------------------------------------------------------------------

/// Weakens a predicate to only constrain the given fields: every leaf
/// over any other field becomes `True`. The result is implied by the
/// original, so "restricted cannot match" still proves "original cannot
/// match".
fn restrict_to_fields(pred: &BoundPredicate, allowed: &BTreeSet<i32>) -> BoundPredicate {
    match pred {
        BoundPredicate::True => BoundPredicate::True,
        BoundPredicate::False => BoundPredicate::False,
        BoundPredicate::And(l, r) => BoundPredicate::And(
            Box::new(restrict_to_fields(l, allowed)),
            Box::new(restrict_to_fields(r, allowed)),
        ),
        BoundPredicate::Or(l, r) => BoundPredicate::Or(
            Box::new(restrict_to_fields(l, allowed)),
            Box::new(restrict_to_fields(r, allowed)),
        ),
        BoundPredicate::Unary { term, .. }
        | BoundPredicate::Comparison { term, .. }
        | BoundPredicate::Set { term, .. } => {
            if allowed.contains(&term.field_id) {
                pred.clone()
            } else {
                BoundPredicate::True
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Residual filters
// ---------------------------------------------------------------------------

/// A partially folded predicate.
enum Folded {
    True,
    False,
    Keep(Expression),
}

/// Computes the residual of `bound` for a file with the given partition
/// tuple under `types` (the file's spec). See the module docs for the
/// folding rules.
pub fn residual_expression(
    bound: &BoundPredicate,
    tuple: &PartitionTuple,
    types: &[PartitionFieldType],
) -> Expression {
    match fold(bound, tuple, types) {
        Folded::True => Expression::True,
        Folded::False => Expression::False,
        Folded::Keep(expr) => expr,
    }
}

/// POLICY INJECTION SEAM (Pillar D, D-F2.1).
///
/// Row-filter policies become an `and` of the pruning residual and the
/// caller's policy expression, applied here — after residual folding,
/// before serialization — so policy predicates are never folded away by
/// partition pruning and every returned task carries them. Column masks
/// will hook the (currently informational) `select` handling in the
/// route. Until policy evaluation lands this is the identity.
#[must_use]
pub fn apply_row_policy_seam(residual: Option<Expression>) -> Option<Expression> {
    residual
}

fn fold(pred: &BoundPredicate, tuple: &PartitionTuple, types: &[PartitionFieldType]) -> Folded {
    match pred {
        BoundPredicate::True => Folded::True,
        BoundPredicate::False => Folded::False,
        BoundPredicate::And(l, r) => match (fold(l, tuple, types), fold(r, tuple, types)) {
            (Folded::False, _) | (_, Folded::False) => Folded::False,
            (Folded::True, other) | (other, Folded::True) => other,
            (Folded::Keep(l), Folded::Keep(r)) => Folded::Keep(Expression::And {
                left: Box::new(l),
                right: Box::new(r),
            }),
        },
        BoundPredicate::Or(l, r) => match (fold(l, tuple, types), fold(r, tuple, types)) {
            (Folded::True, _) | (_, Folded::True) => Folded::True,
            (Folded::False, other) | (other, Folded::False) => other,
            (Folded::Keep(l), Folded::Keep(r)) => Folded::Keep(Expression::Or {
                left: Box::new(l),
                right: Box::new(r),
            }),
        },
        BoundPredicate::Unary { op, term } => match partition_value(term, tuple, types) {
            PartitionLookup::NotDetermined => Folded::Keep(unbind(pred)),
            PartitionLookup::Null => match op {
                UnaryOp::IsNull => Folded::True,
                UnaryOp::NotNull => Folded::False,
                // NaN tests over null differ between evaluator families;
                // let the client decide.
                UnaryOp::IsNan | UnaryOp::NotNan => Folded::Keep(unbind(pred)),
            },
            PartitionLookup::Value(v) => match op {
                UnaryOp::IsNull => Folded::False,
                UnaryOp::NotNull => Folded::True,
                UnaryOp::IsNan => constant(v.is_nan()),
                UnaryOp::NotNan => constant(!v.is_nan()),
            },
        },
        BoundPredicate::Comparison { op, term, literal } => {
            match partition_value(term, tuple, types) {
                PartitionLookup::NotDetermined | PartitionLookup::Null => {
                    Folded::Keep(unbind(pred))
                }
                PartitionLookup::Value(v) => match (op, v, literal) {
                    (CompareOp::StartsWith, Datum::String(v), Datum::String(p)) => {
                        constant(v.starts_with(p.as_str()))
                    }
                    (CompareOp::NotStartsWith, Datum::String(v), Datum::String(p)) => {
                        constant(!v.starts_with(p.as_str()))
                    }
                    (CompareOp::StartsWith | CompareOp::NotStartsWith, ..) => {
                        Folded::Keep(unbind(pred))
                    }
                    (op, v, literal) => match v.partial_cmp_same_type(literal) {
                        // Incomparable (NaN, or promoted-type mismatch):
                        // keep the leaf.
                        None => Folded::Keep(unbind(pred)),
                        Some(ord) => match op {
                            CompareOp::Lt => constant(ord == std::cmp::Ordering::Less),
                            CompareOp::LtEq => constant(ord != std::cmp::Ordering::Greater),
                            CompareOp::Gt => constant(ord == std::cmp::Ordering::Greater),
                            CompareOp::GtEq => constant(ord != std::cmp::Ordering::Less),
                            CompareOp::Eq => constant(ord == std::cmp::Ordering::Equal),
                            CompareOp::NotEq => constant(ord != std::cmp::Ordering::Equal),
                            // Handled by the arms above; keeping the leaf
                            // is the total-function fallback (never wrong,
                            // never a panic path).
                            CompareOp::StartsWith | CompareOp::NotStartsWith => {
                                Folded::Keep(unbind(pred))
                            }
                        },
                    },
                },
            }
        }
        BoundPredicate::Set { op, term, literals } => match partition_value(term, tuple, types) {
            PartitionLookup::NotDetermined | PartitionLookup::Null => Folded::Keep(unbind(pred)),
            PartitionLookup::Value(v) => {
                let mut found = false;
                let mut unknown = false;
                for literal in literals {
                    match v.partial_cmp_same_type(literal) {
                        Some(std::cmp::Ordering::Equal) => found = true,
                        Some(_) => {}
                        None => unknown = true,
                    }
                }
                if found {
                    constant(*op == SetOp::In)
                } else if unknown {
                    Folded::Keep(unbind(pred))
                } else {
                    constant(*op == SetOp::NotIn)
                }
            }
        },
    }
}

fn constant(value: bool) -> Folded {
    if value { Folded::True } else { Folded::False }
}

enum PartitionLookup {
    /// The term is not exactly determined by this file's partition.
    NotDetermined,
    /// Determined, and the stored partition value is null.
    Null,
    /// Determined, with this stored value.
    Value(Datum),
}

/// Looks up a term's exact value in the partition tuple: the term's
/// transform (identity for plain references) must match a partition
/// field over the same source column, and the tuple must carry that
/// field. `void` never determines anything.
fn partition_value(
    term: &meridian_iceberg::expr::BoundTerm,
    tuple: &PartitionTuple,
    types: &[PartitionFieldType],
) -> PartitionLookup {
    let effective = term.transform.clone().unwrap_or(Transform::Identity);
    if effective == Transform::Void || !effective.is_recognized() {
        return PartitionLookup::NotDetermined;
    }
    let Some(pt) = types
        .iter()
        .find(|pt| pt.source_id == term.field_id && pt.transform == effective)
    else {
        return PartitionLookup::NotDetermined;
    };
    match tuple.get(pt.field_id) {
        None => PartitionLookup::NotDetermined,
        Some(None) => PartitionLookup::Null,
        Some(Some(value)) => PartitionLookup::Value(value.clone()),
    }
}

/// Rebuilds the REST expression for a bound leaf (names as written,
/// literals in JSON single-value form). Only called on leaves.
fn unbind(pred: &BoundPredicate) -> Expression {
    let term_of = |term: &meridian_iceberg::expr::BoundTerm| match &term.transform {
        None => Term::Reference(term.name.clone()),
        Some(t) => Term::Transform {
            transform: t.clone(),
            reference: term.name.clone(),
        },
    };
    match pred {
        BoundPredicate::Unary { op, term } => Expression::Unary {
            op: *op,
            term: term_of(term),
        },
        BoundPredicate::Comparison { op, term, literal } => Expression::Comparison {
            op: *op,
            term: term_of(term),
            value: rest::datum_to_rest_json(literal),
        },
        BoundPredicate::Set { op, term, literals } => Expression::Set {
            op: *op,
            term: term_of(term),
            values: literals.iter().map(rest::datum_to_rest_json).collect(),
        },
        // Non-leaves never reach here (fold handles them); expressing them
        // anyway keeps this total.
        BoundPredicate::False => Expression::False,
        BoundPredicate::True | BoundPredicate::And(..) | BoundPredicate::Or(..) => Expression::True,
    }
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

/// Field id → primitive type for every primitive field reachable in the
/// schema (nested structs, list elements, map keys/values included);
/// used to decode bound bytes for the REST `ValueMap`s.
#[must_use]
pub fn schema_primitive_types(schema: &Schema) -> BTreeMap<i32, PrimitiveType> {
    fn walk(
        fields: &[meridian_iceberg::spec::StructField],
        out: &mut BTreeMap<i32, PrimitiveType>,
    ) {
        for field in fields {
            visit(field.id, &field.field_type, out);
        }
    }
    fn visit(id: i32, ty: &Type, out: &mut BTreeMap<i32, PrimitiveType>) {
        match ty {
            Type::Primitive(p) => {
                out.insert(id, p.clone());
            }
            Type::Struct(s) => walk(&s.fields, out),
            Type::List(l) => visit(l.element_id, &l.element, out),
            Type::Map(m) => {
                visit(m.key_id, &m.key, out);
                visit(m.value_id, &m.value, out);
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(&schema.fields, &mut out);
    out
}

/// Resolves a REST `FieldName` (dotted path; `element`/`key`/`value` for
/// lists and maps) to a field id. Unlike filter binding, any terminal
/// type is acceptable — selecting a struct column is legal.
#[must_use]
pub fn resolve_field_name(schema: &Schema, path: &str, case_sensitive: bool) -> Option<i32> {
    let matches = |field_name: &str, segment: &str| {
        if case_sensitive {
            field_name == segment
        } else {
            field_name.eq_ignore_ascii_case(segment)
        }
    };
    let mut segments = path.split('.');
    let first = segments.next()?;
    let field = schema.fields.iter().find(|f| matches(&f.name, first))?;
    let mut field_id = field.id;
    let mut current: &Type = &field.field_type;
    for segment in segments {
        match current {
            Type::Struct(s) => {
                let f = s.fields.iter().find(|f| matches(&f.name, segment))?;
                field_id = f.id;
                current = &f.field_type;
            }
            Type::List(l) if segment == "element" => {
                field_id = l.element_id;
                current = &l.element;
            }
            Type::Map(m) if segment == "key" => {
                field_id = m.key_id;
                current = &m.key;
            }
            Type::Map(m) if segment == "value" => {
                field_id = m.value_id;
                current = &m.value;
            }
            _ => return None,
        }
    }
    Some(field_id)
}

// ---------------------------------------------------------------------------
// Page assembly
// ---------------------------------------------------------------------------

/// Serialization inputs shared by every page of one plan.
#[derive(Debug)]
pub struct SerializeContext<'a> {
    /// Column types for decoding bounds (from the scan schema).
    pub column_types: &'a BTreeMap<i32, PrimitiveType>,
    /// Stats restriction from `stats-fields`; `None` sends everything.
    pub stats_keep: Option<&'a BTreeSet<i32>>,
}

/// Chunks a plan outcome into REST `ScanTasks` pages of at most
/// `page_size` file scan tasks. Each page carries exactly the delete
/// files its tasks reference, with page-local `delete-file-references`
/// indices. Deterministic: same outcome, same pages.
#[must_use]
pub fn build_pages(
    outcome: &PlanOutcome,
    ctx: &SerializeContext<'_>,
    page_size: usize,
) -> Vec<Value> {
    let page_size = page_size.max(1);
    if outcome.files.is_empty() {
        return vec![rest::scan_tasks_json(Vec::new(), Vec::new())];
    }
    outcome
        .files
        .chunks(page_size)
        .map(|chunk| {
            // Page-local delete indices, assigned in ascending global
            // order so the payload is a stable function of the outcome.
            let referenced: BTreeSet<usize> = chunk
                .iter()
                .flat_map(|f| f.delete_indices.iter().copied())
                .collect();
            let local_of: BTreeMap<usize, usize> = referenced
                .iter()
                .enumerate()
                .map(|(local, &global)| (global, local))
                .collect();
            let delete_files: Vec<Value> = referenced
                .iter()
                .map(|&i| {
                    rest::delete_file_json(&outcome.deletes[i].file, outcome.deletes[i].spec_id)
                })
                .collect();
            let tasks: Vec<Value> = chunk
                .iter()
                .map(|file| {
                    let refs: Vec<usize> =
                        file.delete_indices.iter().map(|i| local_of[i]).collect();
                    let residual = apply_row_policy_seam(file.residual.clone());
                    rest::file_scan_task_json(
                        rest::data_file_json(
                            &file.file,
                            file.spec_id,
                            rest::StatsFilter {
                                keep: ctx.stats_keep,
                                types: ctx.column_types,
                            },
                        ),
                        &refs,
                        residual.as_ref(),
                    )
                })
                .collect();
            rest::scan_tasks_json(tasks, delete_files)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use meridian_iceberg::manifest::{
        ManifestEntry, ManifestFile, ManifestMetadata, PartitionValue,
    };
    use meridian_iceberg::spec::{PartitionField, StructField};
    use serde_json::json;

    use super::*;

    fn test_schema() -> Schema {
        let mut schema = Schema::new(vec![
            StructField::required(1, "id", Type::Primitive(PrimitiveType::Long)),
            StructField::optional(2, "region", Type::Primitive(PrimitiveType::String)),
            StructField::optional(3, "name", Type::Primitive(PrimitiveType::String)),
        ]);
        schema.schema_id = Some(0);
        schema
    }

    fn region_spec_types() -> Vec<PartitionFieldType> {
        let fields = vec![PartitionField {
            field_id: Some(1000),
            source_id: 2,
            name: "region".to_owned(),
            transform: Transform::Identity,
            extra: serde_json::Map::new(),
        }];
        partition_field_types(&fields, &test_schema()).expect("resolve partition types")
    }

    fn bucket_spec_types() -> Vec<PartitionFieldType> {
        let fields = vec![PartitionField {
            field_id: Some(1001),
            source_id: 1,
            name: "id_bucket".to_owned(),
            transform: Transform::Bucket(16),
            extra: serde_json::Map::new(),
        }];
        partition_field_types(&fields, &test_schema()).expect("resolve partition types")
    }

    fn region_tuple(value: Option<&str>) -> PartitionTuple {
        PartitionTuple {
            fields: vec![PartitionValue {
                field_id: 1000,
                name: "region".to_owned(),
                value: value.map(|v| Datum::String(v.to_owned())),
            }],
        }
    }

    fn data_file(path: &str, content: DataFileContent, partition: PartitionTuple) -> DataFile {
        DataFile {
            content,
            file_path: path.to_owned(),
            file_format: "PARQUET".to_owned(),
            partition,
            record_count: 10,
            file_size_in_bytes: 1024,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            first_row_id: None,
            referenced_data_file: None,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn planned_delete(file: DataFile, spec_id: i32, sequence_number: i64) -> PlannedDelete {
        PlannedDelete {
            file,
            spec_id,
            sequence_number,
        }
    }

    fn attach(
        deletes: &[PlannedDelete],
        file: &DataFile,
        spec_id: i32,
        data_seq: i64,
    ) -> Vec<usize> {
        DeleteIndex::build(deletes).deletes_for(deletes, file, spec_id, data_seq)
    }

    #[test]
    fn position_deletes_apply_by_seq_le_and_partition_equality() {
        let deletes = vec![
            // Same partition, seq 2.
            planned_delete(
                data_file(
                    "d/pos-a",
                    DataFileContent::PositionDeletes,
                    region_tuple(Some("eu")),
                ),
                0,
                2,
            ),
            // Same partition, seq 1 (older than the data).
            planned_delete(
                data_file(
                    "d/pos-b",
                    DataFileContent::PositionDeletes,
                    region_tuple(Some("eu")),
                ),
                0,
                1,
            ),
            // Different partition value.
            planned_delete(
                data_file(
                    "d/pos-c",
                    DataFileContent::PositionDeletes,
                    region_tuple(Some("us")),
                ),
                0,
                9,
            ),
            // Unpartitioned spec: positional deletes are NOT global (only
            // equality deletes get the unpartitioned special case).
            planned_delete(
                data_file(
                    "d/pos-d",
                    DataFileContent::PositionDeletes,
                    PartitionTuple::default(),
                ),
                1,
                9,
            ),
        ];
        let data = data_file("d/data-1", DataFileContent::Data, region_tuple(Some("eu")));
        // Data seq 2: delete seq 2 applies (same-commit rule, <=), seq 1
        // does not, other partitions do not.
        assert_eq!(attach(&deletes, &data, 0, 2), vec![0]);
    }

    #[test]
    fn equality_deletes_apply_strictly_older_and_globally_when_unpartitioned() {
        let mut eq_partitioned = data_file(
            "d/eq-a",
            DataFileContent::EqualityDeletes,
            region_tuple(Some("eu")),
        );
        eq_partitioned.equality_ids = Some(vec![1]);
        let mut eq_global = data_file(
            "d/eq-b",
            DataFileContent::EqualityDeletes,
            PartitionTuple::default(),
        );
        eq_global.equality_ids = Some(vec![1]);
        let deletes = vec![
            planned_delete(eq_partitioned, 0, 3),
            planned_delete(eq_global, 1, 3),
        ];
        let data = data_file("d/data-1", DataFileContent::Data, region_tuple(Some("eu")));
        // Strictly less: data seq 2 < 3 -> both apply (one via partition
        // equality, one globally).
        assert_eq!(attach(&deletes, &data, 0, 2), vec![0, 1]);
        // Equal sequence numbers: equality deletes do NOT apply.
        assert_eq!(attach(&deletes, &data, 0, 3), Vec::<usize>::new());

        let other_partition =
            data_file("d/data-2", DataFileContent::Data, region_tuple(Some("us")));
        // Only the global delete reaches another partition.
        assert_eq!(attach(&deletes, &other_partition, 0, 2), vec![1]);
    }

    #[test]
    fn deletion_vectors_supersede_position_delete_files() {
        let mut dv = data_file(
            "d/dv-1",
            DataFileContent::PositionDeletes,
            region_tuple(Some("eu")),
        );
        dv.referenced_data_file = Some("d/data-1".to_owned());
        dv.content_offset = Some(4);
        dv.content_size_in_bytes = Some(100);
        let mut referenced_pos = data_file(
            "d/pos-1",
            DataFileContent::PositionDeletes,
            region_tuple(Some("eu")),
        );
        referenced_pos.referenced_data_file = Some("d/data-1".to_owned());
        let partition_pos = data_file(
            "d/pos-2",
            DataFileContent::PositionDeletes,
            region_tuple(Some("eu")),
        );
        let deletes = vec![
            planned_delete(dv, 0, 5),
            planned_delete(referenced_pos, 0, 5),
            planned_delete(partition_pos, 0, 5),
        ];
        let data = data_file("d/data-1", DataFileContent::Data, region_tuple(Some("eu")));
        // The DV wins; both plain position deletes are ignored.
        assert_eq!(attach(&deletes, &data, 0, 2), vec![0]);
        // A different data file in the same partition is untouched by the
        // DV (path-scoped) but still gets the partition-scoped delete.
        let sibling = data_file("d/data-2", DataFileContent::Data, region_tuple(Some("eu")));
        assert_eq!(attach(&deletes, &sibling, 0, 2), vec![2]);
    }

    #[test]
    fn referenced_position_deletes_still_require_partition_equality() {
        let mut referenced = data_file(
            "d/pos-1",
            DataFileContent::PositionDeletes,
            region_tuple(Some("us")),
        );
        referenced.referenced_data_file = Some("d/data-1".to_owned());
        let deletes = vec![planned_delete(referenced, 0, 5)];
        let data = data_file("d/data-1", DataFileContent::Data, region_tuple(Some("eu")));
        assert_eq!(
            attach(&deletes, &data, 0, 2),
            Vec::<usize>::new(),
            "spec: ALL conditions must hold, including partition equality"
        );
    }

    #[test]
    fn position_delete_path_bounds_narrow_attachment() {
        let mut bounded = data_file(
            "d/pos-1",
            DataFileContent::PositionDeletes,
            region_tuple(Some("eu")),
        );
        let mut lower = BTreeMap::new();
        lower.insert(FILE_PATH_FIELD_ID, b"d/data-a".to_vec());
        let mut upper = BTreeMap::new();
        upper.insert(FILE_PATH_FIELD_ID, b"d/data-c".to_vec());
        bounded.lower_bounds = Some(lower);
        bounded.upper_bounds = Some(upper);
        let deletes = vec![planned_delete(bounded, 0, 5)];

        let inside = data_file("d/data-b", DataFileContent::Data, region_tuple(Some("eu")));
        assert_eq!(attach(&deletes, &inside, 0, 2), vec![0]);
        let outside = data_file("d/data-z", DataFileContent::Data, region_tuple(Some("eu")));
        assert_eq!(attach(&deletes, &outside, 0, 2), Vec::<usize>::new());
        // A path extending the (possibly truncated) upper bound is kept.
        let extension = data_file(
            "d/data-c0000",
            DataFileContent::Data,
            region_tuple(Some("eu")),
        );
        assert_eq!(attach(&deletes, &extension, 0, 2), vec![0]);
    }

    fn bind_filter(filter: serde_json::Value) -> BoundPredicate {
        let expr: Expression = serde_json::from_value(filter).expect("parse expression");
        expr.bind(&test_schema(), true).expect("bind expression")
    }

    #[test]
    fn restriction_masks_leaves_on_other_fields() {
        // name = "x" AND id = 3, restricted to {1 (id)}: the name leaf
        // must become True so a delete file whose *payload* stats on name
        // exclude "x" is still considered.
        let bound = bind_filter(json!({
            "type": "and",
            "left": {"type": "eq", "term": "name", "value": "x"},
            "right": {"type": "eq", "term": "id", "value": 3},
        }));
        let restricted = restrict_to_fields(&bound, &BTreeSet::from([1]));

        let mut eq_delete = data_file(
            "d/eq",
            DataFileContent::EqualityDeletes,
            PartitionTuple::default(),
        );
        eq_delete.equality_ids = Some(vec![1]);
        // Stats: id covers 3, name does NOT cover "x".
        let mut lower = BTreeMap::new();
        lower.insert(1, 0_i64.to_le_bytes().to_vec());
        lower.insert(3, b"a".to_vec());
        let mut upper = BTreeMap::new();
        upper.insert(1, 10_i64.to_le_bytes().to_vec());
        upper.insert(3, b"b".to_vec());
        eq_delete.lower_bounds = Some(lower);
        eq_delete.upper_bounds = Some(upper);

        assert!(
            !file_might_match(&bound, &eq_delete),
            "sanity: the unrestricted filter would (wrongly) prune this delete"
        );
        assert!(
            file_might_match(&restricted, &eq_delete),
            "restricted to equality columns, the delete must be kept"
        );
    }

    #[test]
    fn residual_folds_identity_partition_predicates_exactly() {
        let types = region_spec_types();
        let bound = bind_filter(json!({
            "type": "and",
            "left": {"type": "eq", "term": "region", "value": "eu"},
            "right": {"type": "gt", "term": "id", "value": 100},
        }));
        // region = "eu" folds away; id > 100 remains.
        let residual = residual_expression(&bound, &region_tuple(Some("eu")), &types);
        assert_eq!(
            serde_json::to_value(&residual).expect("serialize"),
            json!({"type": "gt", "term": "id", "value": 100})
        );
        // region = "us": the whole filter folds to false.
        let residual = residual_expression(&bound, &region_tuple(Some("us")), &types);
        assert_eq!(
            serde_json::to_value(&residual).expect("serialize"),
            json!({"type": "false"})
        );
    }

    #[test]
    fn residual_on_null_partition_value_keeps_ambiguous_leaves() {
        let types = region_spec_types();
        // is-null folds to True on a null partition value...
        let bound = bind_filter(json!({"type": "is-null", "term": "region"}));
        let residual = residual_expression(&bound, &region_tuple(None), &types);
        assert_eq!(
            serde_json::to_value(&residual).expect("serialize"),
            json!({"type": "true"})
        );
        // ...but not-eq stays for the client (evaluator families disagree
        // on null semantics).
        let bound = bind_filter(json!({"type": "not-eq", "term": "region", "value": "eu"}));
        let residual = residual_expression(&bound, &region_tuple(None), &types);
        assert_eq!(
            serde_json::to_value(&residual).expect("serialize"),
            json!({"type": "not-eq", "term": "region", "value": "eu"})
        );
    }

    #[test]
    fn residual_folds_matching_transform_terms_only() {
        let types = bucket_spec_types();
        let tuple = PartitionTuple {
            fields: vec![PartitionValue {
                field_id: 1001,
                name: "id_bucket".to_owned(),
                value: Some(Datum::Int(7)),
            }],
        };
        // A bucket[16](id) term folds exactly against the stored bucket.
        let bound = bind_filter(json!({
            "type": "eq",
            "term": {"type": "transform", "transform": "bucket[16]", "term": "id"},
            "value": 7,
        }));
        let residual = residual_expression(&bound, &tuple, &types);
        assert_eq!(
            serde_json::to_value(&residual).expect("serialize"),
            json!({"type": "true"})
        );
        // A plain id predicate is NOT determined by the bucket value.
        let bound = bind_filter(json!({"type": "eq", "term": "id", "value": 3}));
        let residual = residual_expression(&bound, &tuple, &types);
        assert_eq!(
            serde_json::to_value(&residual).expect("serialize"),
            json!({"type": "eq", "term": "id", "value": 3})
        );
    }

    #[test]
    fn pages_are_deterministic_and_reindex_deletes_locally() {
        let deletes = vec![
            planned_delete(
                data_file(
                    "d/pos-1",
                    DataFileContent::PositionDeletes,
                    region_tuple(Some("eu")),
                ),
                0,
                5,
            ),
            planned_delete(
                data_file(
                    "d/pos-2",
                    DataFileContent::PositionDeletes,
                    region_tuple(Some("us")),
                ),
                0,
                5,
            ),
        ];
        let outcome = PlanOutcome {
            files: vec![
                PlannedFile {
                    file: data_file("d/data-1", DataFileContent::Data, region_tuple(Some("eu"))),
                    spec_id: 0,
                    residual: None,
                    delete_indices: vec![0],
                },
                PlannedFile {
                    file: data_file("d/data-2", DataFileContent::Data, region_tuple(Some("us"))),
                    spec_id: 0,
                    residual: None,
                    delete_indices: vec![1],
                },
                PlannedFile {
                    file: data_file("d/data-3", DataFileContent::Data, region_tuple(Some("us"))),
                    spec_id: 0,
                    residual: None,
                    delete_indices: vec![],
                },
            ],
            deletes,
            counters: PlanCounters::default(),
        };
        let column_types = schema_primitive_types(&test_schema());
        let ctx = SerializeContext {
            column_types: &column_types,
            stats_keep: None,
        };

        let pages = build_pages(&outcome, &ctx, 2);
        assert_eq!(pages.len(), 2);
        // Page 0 has tasks 1..2 and only the delete they reference,
        // re-indexed to 0.
        let first = &pages[0];
        assert_eq!(first["file-scan-tasks"].as_array().map(Vec::len), Some(2));
        assert_eq!(first["delete-files"].as_array().map(Vec::len), Some(2));
        assert_eq!(
            first["file-scan-tasks"][0]["delete-file-references"],
            json!([0])
        );
        assert_eq!(
            first["file-scan-tasks"][1]["delete-file-references"],
            json!([1])
        );
        // The second page: no deletes at all.
        let second = &pages[1];
        assert_eq!(second["file-scan-tasks"].as_array().map(Vec::len), Some(1));
        assert!(second.get("delete-files").is_none());

        // Determinism: identical inputs, identical pages.
        assert_eq!(pages, build_pages(&outcome, &ctx, 2));

        // Data file shape: spec-id, lowercase format, partition values.
        let file = &first["file-scan-tasks"][0]["data-file"];
        assert_eq!(file["content"], json!("data"));
        assert_eq!(file["file-format"], json!("parquet"));
        assert_eq!(file["spec-id"], json!(0));
        assert_eq!(file["partition"], json!(["eu"]));
        // Delete file shape.
        assert_eq!(
            first["delete-files"][0]["content"],
            json!("position-deletes")
        );
    }

    /// A tiny in-memory [`ManifestSource`] for exercising `plan_scan`
    /// without Avro or storage.
    struct MapSource {
        list: Arc<meridian_iceberg::manifest::ManifestList>,
        manifests: HashMap<String, Arc<Manifest>>,
    }

    impl ManifestSource for MapSource {
        async fn manifest_list(
            &self,
            _location: &str,
        ) -> Result<Arc<meridian_iceberg::manifest::ManifestList>, PlanError> {
            Ok(Arc::clone(&self.list))
        }

        async fn manifest(&self, location: &str) -> Result<Arc<Manifest>, PlanError> {
            self.manifests
                .get(location)
                .cloned()
                .ok_or_else(|| PlanError::Unreadable {
                    location: location.to_owned(),
                    reason: "missing from test map".to_owned(),
                })
        }
    }

    fn manifest_ref(path: &str, content: ManifestContentType, seq: i64) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_owned(),
            manifest_length: 100,
            partition_spec_id: 0,
            content,
            sequence_number: seq,
            min_sequence_number: seq,
            added_snapshot_id: 1,
            added_files_count: Some(2),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(20),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    fn manifest_of(content: ManifestContentType, files: Vec<DataFile>) -> Arc<Manifest> {
        Arc::new(Manifest {
            metadata: ManifestMetadata {
                schema_json: "{}".to_owned(),
                schema_id: Some(0),
                partition_fields: vec![PartitionField {
                    field_id: Some(1000),
                    source_id: 2,
                    name: "region".to_owned(),
                    transform: Transform::Identity,
                    extra: serde_json::Map::new(),
                }],
                partition_spec_id: Some(0),
                format_version: Some(2),
                content,
            },
            entries: files
                .into_iter()
                .map(|data_file| ManifestEntry {
                    status: ManifestEntryStatus::Added,
                    snapshot_id: Some(1),
                    sequence_number: None, // inherited
                    file_sequence_number: None,
                    data_file,
                })
                .collect(),
        })
    }

    fn test_metadata() -> TableMetadata {
        let metadata = json!({
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": "file:///tmp/t",
            "last-sequence-number": 2,
            "last-updated-ms": 1,
            "last-column-id": 3,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "region", "required": false, "type": "string"},
                    {"id": 3, "name": "name", "required": false, "type": "string"},
                ],
            }],
            "default-spec-id": 0,
            "partition-specs": [{
                "spec-id": 0,
                "fields": [{
                    "source-id": 2, "field-id": 1000,
                    "name": "region", "transform": "identity",
                }],
            }],
            "last-partition-id": 1000,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
        });
        serde_json::from_value(metadata).expect("parse test metadata")
    }

    #[tokio::test]
    async fn plan_scan_prunes_by_partition_and_attaches_deletes() {
        let mut data_eu = data_file("d/data-eu", DataFileContent::Data, region_tuple(Some("eu")));
        // Stats so a stats-only predicate could prune: id in [0, 50].
        let mut lower = BTreeMap::new();
        lower.insert(1, 0_i64.to_le_bytes().to_vec());
        let mut upper = BTreeMap::new();
        upper.insert(1, 50_i64.to_le_bytes().to_vec());
        data_eu.lower_bounds = Some(lower);
        data_eu.upper_bounds = Some(upper);
        let data_us = data_file("d/data-us", DataFileContent::Data, region_tuple(Some("us")));
        let pos_eu = data_file(
            "d/pos-eu",
            DataFileContent::PositionDeletes,
            region_tuple(Some("eu")),
        );

        let list = Arc::new(meridian_iceberg::manifest::ManifestList {
            format_version: Some(2),
            snapshot_id: Some(1),
            parent_snapshot_id: None,
            sequence_number: Some(2),
            manifests: vec![
                manifest_ref("m/data.avro", ManifestContentType::Data, 1),
                manifest_ref("m/deletes.avro", ManifestContentType::Deletes, 2),
            ],
        });
        let mut manifests = HashMap::new();
        manifests.insert(
            "m/data.avro".to_owned(),
            manifest_of(ManifestContentType::Data, vec![data_eu, data_us]),
        );
        manifests.insert(
            "m/deletes.avro".to_owned(),
            manifest_of(ManifestContentType::Deletes, vec![pos_eu]),
        );
        let source = MapSource { list, manifests };
        let metadata = test_metadata();
        let schema = metadata.schemas[0].clone();

        // Filter region = "eu": the us file is pruned by its tuple; the
        // eu file keeps the position delete (data seq 1 <= delete seq 2)
        // and gets residual True (identity fold).
        let bound = bind_filter(json!({"type": "eq", "term": "region", "value": "eu"}));
        let outcome = plan_scan(&source, &metadata, &schema, "ml.avro", Some(&bound))
            .await
            .expect("plan");
        assert_eq!(outcome.files.len(), 1);
        assert_eq!(outcome.files[0].file.file_path, "d/data-eu");
        assert_eq!(outcome.files[0].delete_indices, vec![0]);
        assert_eq!(
            outcome.files[0]
                .residual
                .as_ref()
                .map(|r| serde_json::to_value(r).expect("serialize")),
            Some(json!({"type": "true"}))
        );
        assert_eq!(outcome.counters.data_files_pruned_partition, 1);
        assert_eq!(outcome.counters.data_files_seen, 2);

        // A stats predicate prunes the eu file by its id bounds.
        let bound = bind_filter(json!({
            "type": "and",
            "left": {"type": "eq", "term": "region", "value": "eu"},
            "right": {"type": "gt", "term": "id", "value": 1000},
        }));
        let outcome = plan_scan(&source, &metadata, &schema, "ml.avro", Some(&bound))
            .await
            .expect("plan");
        assert_eq!(outcome.files.len(), 0);
        assert_eq!(outcome.counters.data_files_pruned_stats, 1);

        // No filter: everything, in manifest order.
        let outcome = plan_scan(&source, &metadata, &schema, "ml.avro", None)
            .await
            .expect("plan");
        assert_eq!(outcome.files.len(), 2);
        assert!(outcome.files[0].residual.is_none());
    }
}
