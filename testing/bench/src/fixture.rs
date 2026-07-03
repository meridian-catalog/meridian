//! Synthetic Iceberg table generator for scan-planning tests and
//! benchmarks, built on `meridian-iceberg`'s manifest *writers* (the same
//! code path that will back compaction rewrites), so the server's planner
//! is exercised against real Avro manifests — not mocks.
//!
//! Generation is pure: [`SyntheticTable::files`] is a list of
//! `(location, bytes)` pairs (metadata.json, manifest list(s), manifests)
//! for the caller to write — `std::fs` for `file://` warehouses,
//! `meridian-storage` for S3 — followed by an IRC `register` call. Data
//! files are *referenced but never written*: planning reads metadata
//! only, so a 10,000-file table costs a few MB of manifests.
//!
//! Layouts:
//!
//! - [`synthetic_table`] — one append snapshot, `data_files` parquet
//!   entries with realistic per-column stats, identity-partitioned by
//!   `region` into `partitions` values, grouped so each manifest covers
//!   one region (partition summaries prune whole manifests). Ground
//!   truth for filters is computable from the deterministic layout via
//!   [`SyntheticTable::expected`].
//! - [`mor_table`] — a small merge-on-read layout: three data files at
//!   sequence numbers 1 and 2 across two regions, a path-bounded
//!   position delete at sequence 2, and a global (unpartitioned-spec)
//!   equality delete at sequence 3, plus an older snapshot for
//!   time-travel and `use-snapshot-schema` tests. The exact expected
//!   delete attachment is documented on the function.

use std::collections::BTreeMap;

use meridian_iceberg::manifest::{
    DataFile, DataFileContent, FieldSummary, ManifestContentType, ManifestEntry,
    ManifestEntryStatus, ManifestFile, ManifestListWriteParams, ManifestWriteParams,
    PartitionTuple, PartitionValue, partition_field_types, write_manifest, write_manifest_list,
};
use meridian_iceberg::spec::{PartitionField, Schema, TableMetadata, Transform};
use meridian_iceberg::value::Datum;
use serde_json::json;

/// Parameters for [`synthetic_table`].
#[derive(Debug, Clone)]
pub struct SyntheticSpec {
    /// Table root location (no trailing slash), e.g.
    /// `file:///tmp/wh/plan_ns/plan_10k` or `s3://bucket/wh/plan_ns/t`.
    pub table_location: String,
    /// Total data files.
    pub data_files: usize,
    /// Identity partitions over `region` (`region_000`…); data files are
    /// distributed in contiguous blocks.
    pub partitions: usize,
    /// Data files per manifest.
    pub files_per_manifest: usize,
    /// Rows per data file (drives record counts and id ranges).
    pub rows_per_file: i64,
}

impl Default for SyntheticSpec {
    fn default() -> Self {
        Self {
            table_location: String::new(),
            data_files: 10_000,
            partitions: 100,
            files_per_manifest: 100,
            rows_per_file: 1_000,
        }
    }
}

/// One generated (referenced, unwritten) data file, for ground truth.
#[derive(Debug, Clone)]
pub struct SyntheticDataFile {
    /// Full storage path referenced by the manifest.
    pub path: String,
    /// Identity partition value.
    pub region: String,
    /// Inclusive `id` column bounds.
    pub id_min: i64,
    /// Inclusive `id` column bounds.
    pub id_max: i64,
    /// `category` column value (lower == upper bound).
    pub category: String,
}

/// A generated table: files to write plus ground truth.
#[derive(Debug)]
pub struct SyntheticTable {
    /// `(location, bytes)` pairs to write, metadata.json last.
    pub files: Vec<(String, Vec<u8>)>,
    /// The metadata.json location (register the table with this).
    pub metadata_location: String,
    /// The current snapshot id.
    pub snapshot_id: i64,
    /// Ground truth for every referenced data file, in manifest order.
    pub data_files: Vec<SyntheticDataFile>,
}

impl SyntheticTable {
    /// The paths of data files whose region/id ranges satisfy the given
    /// predicate — brute-force ground truth for pruning assertions.
    pub fn expected(&self, matches: impl Fn(&SyntheticDataFile) -> bool) -> Vec<String> {
        self.data_files
            .iter()
            .filter(|f| matches(f))
            .map(|f| f.path.clone())
            .collect()
    }
}

/// The fixture schema: id (long, required), region (string), category
/// (string), amount (double), ts (timestamp).
fn fixture_schema_json() -> serde_json::Value {
    json!({
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "region", "required": false, "type": "string"},
            {"id": 3, "name": "category", "required": false, "type": "string"},
            {"id": 4, "name": "amount", "required": false, "type": "double"},
            {"id": 5, "name": "ts", "required": false, "type": "timestamp"},
        ],
    })
}

fn region_partition_fields() -> Vec<PartitionField> {
    vec![PartitionField {
        field_id: Some(1000),
        source_id: 2,
        name: "region".to_owned(),
        transform: Transform::Identity,
        extra: serde_json::Map::new(),
    }]
}

fn region_tuple(region: &str) -> PartitionTuple {
    PartitionTuple {
        fields: vec![PartitionValue {
            field_id: 1000,
            name: "region".to_owned(),
            value: Some(Datum::String(region.to_owned())),
        }],
    }
}

fn bounds(entries: &[(i32, Datum)]) -> BTreeMap<i32, Vec<u8>> {
    entries
        .iter()
        .map(|(id, datum)| (*id, datum.to_bound_bytes()))
        .collect()
}

fn counts(ids: &[i32], value: i64) -> BTreeMap<i32, i64> {
    ids.iter().map(|id| (*id, value)).collect()
}

#[allow(clippy::too_many_arguments)] // a fixture-row literal, not an API
fn synthetic_data_file(
    path: String,
    region: &str,
    content: DataFileContent,
    rows: i64,
    id_min: i64,
    id_max: i64,
    category: &str,
    amount_min: f64,
) -> DataFile {
    DataFile {
        content,
        file_path: path,
        file_format: "PARQUET".to_owned(),
        partition: region_tuple(region),
        record_count: rows,
        file_size_in_bytes: 64 * 1024 + rows * 100,
        column_sizes: Some(counts(&[1, 2, 3, 4, 5], rows * 8)),
        value_counts: Some(counts(&[1, 2, 3, 4, 5], rows)),
        null_value_counts: Some({
            let mut m = counts(&[1, 2, 4, 5], 0);
            // A realistic touch: some categories are null.
            m.insert(3, rows / 20);
            m
        }),
        nan_value_counts: Some(counts(&[4], 0)),
        lower_bounds: Some(bounds(&[
            (1, Datum::Long(id_min)),
            (2, Datum::String(region.to_owned())),
            (3, Datum::String(category.to_owned())),
            (4, Datum::double(amount_min)),
            (
                5,
                Datum::Timestamp(1_700_000_000_000_000 + id_min * 1_000_000),
            ),
        ])),
        upper_bounds: Some(bounds(&[
            (1, Datum::Long(id_max)),
            (2, Datum::String(region.to_owned())),
            (3, Datum::String(category.to_owned())),
            (4, Datum::double(amount_min + 0.99)),
            (
                5,
                Datum::Timestamp(1_700_000_000_000_000 + id_max * 1_000_000),
            ),
        ])),
        key_metadata: None,
        split_offsets: Some(vec![4]),
        equality_ids: None,
        sort_order_id: Some(0),
        first_row_id: None,
        referenced_data_file: None,
        content_offset: None,
        content_size_in_bytes: None,
    }
}

fn added_entry(data_file: DataFile, snapshot_id: i64, sequence: i64) -> ManifestEntry {
    ManifestEntry {
        status: ManifestEntryStatus::Added,
        snapshot_id: Some(snapshot_id),
        sequence_number: Some(sequence),
        file_sequence_number: Some(sequence),
        data_file,
    }
}

struct BuiltManifest {
    location: String,
    bytes: Vec<u8>,
    entry: ManifestFile,
}

/// Writes one manifest and its manifest-list entry (with partition
/// summaries computed from the entries).
#[allow(clippy::too_many_arguments)] // internal builder
fn build_manifest(
    table_location: &str,
    name: &str,
    schema: &Schema,
    partition_fields: &[PartitionField],
    partition_spec_id: i32,
    content: ManifestContentType,
    snapshot_id: i64,
    sequence: i64,
    entries: &[ManifestEntry],
) -> Result<BuiltManifest, String> {
    let types = partition_field_types(partition_fields, schema).map_err(|e| e.to_string())?;
    let schema_json =
        serde_json::to_string(schema).map_err(|e| format!("serialize schema: {e}"))?;
    let bytes = write_manifest(&ManifestWriteParams {
        format_version: 2,
        content,
        schema_json: &schema_json,
        schema_id: Some(0),
        partition_spec_id,
        partition_fields,
        partition_types: &types,
        entries,
    })
    .map_err(|e| format!("write manifest {name}: {e}"))?;

    // Partition summaries per spec field, from the entries' tuples.
    let summaries: Vec<FieldSummary> = partition_fields
        .iter()
        .map(|field| {
            let field_id = field.field_id.unwrap_or_default();
            let mut summary = FieldSummary::default();
            let mut lower: Option<Vec<u8>> = None;
            let mut upper: Option<Vec<u8>> = None;
            for entry in entries {
                match entry.data_file.partition.get(field_id) {
                    Some(Some(datum)) => {
                        let b = datum.to_bound_bytes();
                        if lower.as_ref().is_none_or(|l| b < *l) {
                            lower = Some(b.clone());
                        }
                        if upper.as_ref().is_none_or(|u| b > *u) {
                            upper = Some(b);
                        }
                    }
                    _ => summary.contains_null = true,
                }
            }
            summary.contains_nan = Some(false);
            summary.lower_bound = lower;
            summary.upper_bound = upper;
            summary
        })
        .collect();

    let (added, rows): (i32, i64) = (
        i32::try_from(entries.len()).unwrap_or(i32::MAX),
        entries.iter().map(|e| e.data_file.record_count).sum(),
    );
    let location = format!("{table_location}/metadata/{name}.avro");
    let entry = ManifestFile {
        manifest_path: location.clone(),
        manifest_length: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
        partition_spec_id,
        content,
        sequence_number: sequence,
        min_sequence_number: sequence,
        added_snapshot_id: snapshot_id,
        added_files_count: Some(added),
        existing_files_count: Some(0),
        deleted_files_count: Some(0),
        added_rows_count: Some(rows),
        existing_rows_count: Some(0),
        deleted_rows_count: Some(0),
        partitions: Some(summaries),
        key_metadata: None,
        first_row_id: None,
    };
    Ok(BuiltManifest {
        location,
        bytes,
        entry,
    })
}

fn build_manifest_list(
    table_location: &str,
    name: &str,
    snapshot_id: i64,
    sequence: i64,
    manifests: &[ManifestFile],
) -> Result<(String, Vec<u8>), String> {
    let bytes = write_manifest_list(&ManifestListWriteParams {
        format_version: 2,
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: Some(sequence),
        manifests,
    })
    .map_err(|e| format!("write manifest list {name}: {e}"))?;
    Ok((format!("{table_location}/metadata/{name}.avro"), bytes))
}

fn parse_schema(value: serde_json::Value) -> Result<Schema, String> {
    serde_json::from_value(value).map_err(|e| format!("fixture schema: {e}"))
}

/// Validates and serializes metadata (round-tripping through the typed
/// model, so a malformed fixture fails at generation time, not in the
/// server).
fn metadata_bytes(metadata: serde_json::Value) -> Result<Vec<u8>, String> {
    let typed: TableMetadata =
        serde_json::from_value(metadata).map_err(|e| format!("fixture metadata: {e}"))?;
    serde_json::to_vec_pretty(&typed).map_err(|e| format!("serialize metadata: {e}"))
}

/// One synthetic data-file entry plus its ground-truth row.
fn synthetic_entry(
    spec: &SyntheticSpec,
    table_location: &str,
    snapshot_id: i64,
    files_per_region: usize,
    i: usize,
) -> (ManifestEntry, SyntheticDataFile) {
    let region_index = i / files_per_region;
    let region = format!("region_{region_index:03}");
    let category = format!("cat_{:02}", i % 10);
    let id_min = i64::try_from(i).unwrap_or(i64::MAX) * spec.rows_per_file;
    let id_max = id_min + spec.rows_per_file - 1;
    let path = format!("{table_location}/data/region={region}/f-{i:06}.parquet");
    let truth = SyntheticDataFile {
        path: path.clone(),
        region: region.clone(),
        id_min,
        id_max,
        category: category.clone(),
    };
    #[allow(clippy::cast_precision_loss)] // fixture stats only
    let amount_min = (i % 100) as f64;
    let entry = added_entry(
        synthetic_data_file(
            path,
            &region,
            DataFileContent::Data,
            spec.rows_per_file,
            id_min,
            id_max,
            &category,
            amount_min,
        ),
        snapshot_id,
        1,
    );
    (entry, truth)
}

/// Generates the single-snapshot planning fixture. See the module docs.
pub fn synthetic_table(spec: &SyntheticSpec) -> Result<SyntheticTable, String> {
    if spec.data_files == 0 || spec.partitions == 0 || spec.files_per_manifest == 0 {
        return Err("data_files, partitions, and files_per_manifest must be positive".to_owned());
    }
    let schema = parse_schema(fixture_schema_json())?;
    let partition_fields = region_partition_fields();
    let table_location = spec.table_location.trim_end_matches('/');
    let snapshot_id: i64 = 3_000_000_001;

    let files_per_region = spec.data_files.div_ceil(spec.partitions);
    let mut ground_truth = Vec::with_capacity(spec.data_files);
    let mut out_files: Vec<(String, Vec<u8>)> = Vec::new();
    let mut manifest_entries: Vec<ManifestFile> = Vec::new();

    let mut file_index = 0_usize;
    let mut manifest_index = 0_usize;
    while file_index < spec.data_files {
        let chunk = (spec.data_files - file_index).min(spec.files_per_manifest);
        let entries: Vec<ManifestEntry> = (file_index..file_index + chunk)
            .map(|i| {
                let (entry, truth) =
                    synthetic_entry(spec, table_location, snapshot_id, files_per_region, i);
                ground_truth.push(truth);
                entry
            })
            .collect();
        let built = build_manifest(
            table_location,
            &format!("mf-{manifest_index:05}"),
            &schema,
            &partition_fields,
            0,
            ManifestContentType::Data,
            snapshot_id,
            1,
            &entries,
        )?;
        out_files.push((built.location, built.bytes));
        manifest_entries.push(built.entry);
        file_index += chunk;
        manifest_index += 1;
    }

    let (list_location, list_bytes) = build_manifest_list(
        table_location,
        &format!("snap-{snapshot_id}"),
        snapshot_id,
        1,
        &manifest_entries,
    )?;
    out_files.push((list_location.clone(), list_bytes));

    let total_records = i64::try_from(spec.data_files).unwrap_or(i64::MAX) * spec.rows_per_file;
    let metadata = json!({
        "format-version": 2,
        "table-uuid": uuid::Uuid::new_v4().to_string(),
        "location": table_location,
        "last-sequence-number": 1,
        "last-updated-ms": 1_700_000_000_000_i64,
        "last-column-id": 5,
        "current-schema-id": 0,
        "schemas": [fixture_schema_json()],
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
        "properties": {"write.parquet.compression-codec": "zstd"},
        "current-snapshot-id": snapshot_id,
        "refs": {"main": {"snapshot-id": snapshot_id, "type": "branch"}},
        "snapshots": [{
            "snapshot-id": snapshot_id,
            "sequence-number": 1,
            "timestamp-ms": 1_700_000_000_000_i64,
            "manifest-list": list_location,
            "schema-id": 0,
            "summary": {
                "operation": "append",
                "added-data-files": spec.data_files.to_string(),
                "added-records": total_records.to_string(),
                "total-data-files": spec.data_files.to_string(),
                "total-records": total_records.to_string(),
            },
        }],
        "snapshot-log": [{"snapshot-id": snapshot_id, "timestamp-ms": 1_700_000_000_000_i64}],
        "metadata-log": [],
    });
    let metadata_location = format!("{table_location}/metadata/00001-plan-fixture.metadata.json");
    out_files.push((metadata_location.clone(), metadata_bytes(metadata)?));

    Ok(SyntheticTable {
        files: out_files,
        metadata_location,
        snapshot_id,
        data_files: ground_truth,
    })
}

/// Ground truth of the [`mor_table`] fixture.
#[derive(Debug)]
pub struct MorTable {
    /// `(location, bytes)` pairs to write, metadata.json last.
    pub files: Vec<(String, Vec<u8>)>,
    /// The metadata.json location.
    pub metadata_location: String,
    /// The current snapshot (sequence 3, both schemas' data visible).
    pub current_snapshot_id: i64,
    /// An older snapshot (sequence 1, only `data_eu_1`/`data_us_1`,
    /// schema without `category`/`amount`/`ts`).
    pub old_snapshot_id: i64,
    /// Data file paths.
    pub data_eu_1: String,
    /// Data file paths.
    pub data_eu_2: String,
    /// Data file paths.
    pub data_us_1: String,
    /// The path-bounded position delete (region eu, sequence 2, bounds
    /// covering only `data_eu_1`).
    pub pos_delete: String,
    /// The global equality delete (unpartitioned spec 1, sequence 3,
    /// equality id 1, deleting id = 150150).
    pub eq_delete: String,
}

/// Generates a small merge-on-read fixture with hand-known delete
/// attachment:
///
/// | data file  | seq | region | expected deletes                       |
/// |------------|-----|--------|----------------------------------------|
/// | `data_eu_1` | 1  | eu     | position (path bounds hit) + equality  |
/// | `data_eu_2` | 2  | eu     | equality only (path bounds miss)       |
/// | `data_us_1` | 1  | us     | equality only (partition mismatch)     |
///
/// The equality delete has sequence 3 (strictly greater than every data
/// sequence) and an unpartitioned spec, so it applies globally; the
/// position delete has sequence 2 (`>=` both eu files' sequences) but its
/// `file_path` bounds cover only `data_eu_1`.
#[allow(clippy::too_many_lines)] // one literal fixture layout, one function
pub fn mor_table(table_location: &str) -> Result<MorTable, String> {
    let table_location = table_location.trim_end_matches('/');
    let schema = parse_schema(fixture_schema_json())?;
    let partition_fields = region_partition_fields();
    let old_snapshot_id: i64 = 3_100_000_001;
    let current_snapshot_id: i64 = 3_100_000_003;

    let data_eu_1 = format!("{table_location}/data/region=eu/f-000001.parquet");
    let data_eu_2 = format!("{table_location}/data/region=eu/f-000002.parquet");
    let data_us_1 = format!("{table_location}/data/region=us/f-000003.parquet");
    let pos_delete_path = format!("{table_location}/data/region=eu/pd-000001.parquet");
    let eq_delete_path = format!("{table_location}/data/ed-000001.parquet");

    // Data manifests: seq-1 files (eu + us) and a seq-2 file (eu).
    let m_data_1 = build_manifest(
        table_location,
        "mf-data-1",
        &schema,
        &partition_fields,
        0,
        ManifestContentType::Data,
        old_snapshot_id,
        1,
        &[
            added_entry(
                synthetic_data_file(
                    data_eu_1.clone(),
                    "eu",
                    DataFileContent::Data,
                    100,
                    0,
                    99,
                    "cat_00",
                    1.0,
                ),
                old_snapshot_id,
                1,
            ),
            added_entry(
                synthetic_data_file(
                    data_us_1.clone(),
                    "us",
                    DataFileContent::Data,
                    100,
                    100_000,
                    100_099,
                    "cat_01",
                    2.0,
                ),
                old_snapshot_id,
                1,
            ),
        ],
    )?;
    let m_data_2 = build_manifest(
        table_location,
        "mf-data-2",
        &schema,
        &partition_fields,
        0,
        ManifestContentType::Data,
        current_snapshot_id - 1,
        2,
        &[added_entry(
            synthetic_data_file(
                data_eu_2.clone(),
                "eu",
                DataFileContent::Data,
                100,
                150_100,
                150_199,
                "cat_02",
                3.0,
            ),
            current_snapshot_id - 1,
            2,
        )],
    )?;

    // Position delete (partition eu, seq 2) with file_path bounds naming
    // data_eu_1 exactly.
    let mut pos_delete = synthetic_data_file(
        pos_delete_path.clone(),
        "eu",
        DataFileContent::PositionDeletes,
        7,
        0,
        0,
        "unused",
        0.0,
    );
    pos_delete.lower_bounds = Some(bounds(&[(2_147_483_546, Datum::String(data_eu_1.clone()))]));
    pos_delete.upper_bounds = Some(bounds(&[(2_147_483_546, Datum::String(data_eu_1.clone()))]));
    pos_delete.value_counts = Some(counts(&[2_147_483_546, 2_147_483_545], 7));
    pos_delete.null_value_counts = Some(counts(&[2_147_483_546, 2_147_483_545], 0));
    pos_delete.nan_value_counts = None;
    pos_delete.column_sizes = None;
    let m_pos = build_manifest(
        table_location,
        "mf-pos-deletes",
        &schema,
        &partition_fields,
        0,
        ManifestContentType::Deletes,
        current_snapshot_id - 1,
        2,
        &[added_entry(pos_delete, current_snapshot_id - 1, 2)],
    )?;

    // Equality delete under the empty spec 1 (global), seq 3, id = 150150.
    let mut eq_delete = synthetic_data_file(
        eq_delete_path.clone(),
        "eu", // tuple content replaced below
        DataFileContent::EqualityDeletes,
        1,
        150_150,
        150_150,
        "unused",
        0.0,
    );
    eq_delete.partition = PartitionTuple::default();
    eq_delete.equality_ids = Some(vec![1]);
    eq_delete.lower_bounds = Some(bounds(&[(1, Datum::Long(150_150))]));
    eq_delete.upper_bounds = Some(bounds(&[(1, Datum::Long(150_150))]));
    eq_delete.value_counts = Some(counts(&[1], 1));
    eq_delete.null_value_counts = Some(counts(&[1], 0));
    eq_delete.nan_value_counts = None;
    eq_delete.column_sizes = None;
    let m_eq = build_manifest(
        table_location,
        "mf-eq-deletes",
        &schema,
        &[], // unpartitioned spec 1
        1,
        ManifestContentType::Deletes,
        current_snapshot_id,
        3,
        &[added_entry(eq_delete, current_snapshot_id, 3)],
    )?;

    let (old_list_location, old_list_bytes) = build_manifest_list(
        table_location,
        &format!("snap-{old_snapshot_id}"),
        old_snapshot_id,
        1,
        std::slice::from_ref(&m_data_1.entry),
    )?;
    let (list_location, list_bytes) = build_manifest_list(
        table_location,
        &format!("snap-{current_snapshot_id}"),
        current_snapshot_id,
        3,
        &[
            m_data_1.entry.clone(),
            m_data_2.entry.clone(),
            m_pos.entry.clone(),
            m_eq.entry.clone(),
        ],
    )?;

    let metadata = json!({
        "format-version": 2,
        "table-uuid": uuid::Uuid::new_v4().to_string(),
        "location": table_location,
        "last-sequence-number": 3,
        "last-updated-ms": 1_700_000_000_000_i64,
        "last-column-id": 5,
        "current-schema-id": 0,
        "schemas": [fixture_schema_json(), {
            "type": "struct",
            "schema-id": 1,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "region", "required": false, "type": "string"},
            ],
        }],
        "default-spec-id": 0,
        "partition-specs": [
            {
                "spec-id": 0,
                "fields": [{
                    "source-id": 2, "field-id": 1000,
                    "name": "region", "transform": "identity",
                }],
            },
            {"spec-id": 1, "fields": []},
        ],
        "last-partition-id": 1000,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "current-snapshot-id": current_snapshot_id,
        "refs": {"main": {"snapshot-id": current_snapshot_id, "type": "branch"}},
        "snapshots": [
            {
                "snapshot-id": old_snapshot_id,
                "sequence-number": 1,
                "timestamp-ms": 1_700_000_000_000_i64,
                "manifest-list": old_list_location,
                "schema-id": 1,
                "summary": {"operation": "append"},
            },
            {
                "snapshot-id": current_snapshot_id,
                "sequence-number": 3,
                "timestamp-ms": 1_700_000_002_000_i64,
                "manifest-list": list_location,
                "schema-id": 0,
                "summary": {"operation": "overwrite"},
            },
        ],
        "snapshot-log": [
            {"snapshot-id": old_snapshot_id, "timestamp-ms": 1_700_000_000_000_i64},
            {"snapshot-id": current_snapshot_id, "timestamp-ms": 1_700_000_002_000_i64},
        ],
        "metadata-log": [],
    });
    let metadata_location = format!("{table_location}/metadata/00003-mor-fixture.metadata.json");

    let files = vec![
        (m_data_1.location, m_data_1.bytes),
        (m_data_2.location, m_data_2.bytes),
        (m_pos.location, m_pos.bytes),
        (m_eq.location, m_eq.bytes),
        (old_list_location.clone(), old_list_bytes),
        (list_location, list_bytes),
        (metadata_location.clone(), metadata_bytes(metadata)?),
    ];

    Ok(MorTable {
        files,
        metadata_location,
        current_snapshot_id,
        old_snapshot_id,
        data_eu_1,
        data_eu_2,
        data_us_1,
        pos_delete: pos_delete_path,
        eq_delete: eq_delete_path,
    })
}

/// Writes generated files under a local filesystem root: locations must
/// start with `file://`.
pub fn write_local(files: &[(String, Vec<u8>)]) -> Result<(), String> {
    for (location, bytes) in files {
        let path = location
            .strip_prefix("file://")
            .ok_or_else(|| format!("not a file:// location: {location}"))?;
        let path = std::path::Path::new(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
        }
        std::fs::write(path, bytes).map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_table_round_trips_through_the_readers() {
        let spec = SyntheticSpec {
            table_location: "file:///tmp/fixture-check".to_owned(),
            data_files: 25,
            partitions: 5,
            files_per_manifest: 10,
            rows_per_file: 100,
        };
        let table = synthetic_table(&spec).expect("generate");
        assert_eq!(table.data_files.len(), 25);
        // 3 manifests + 1 list + 1 metadata.
        assert_eq!(table.files.len(), 5);

        // The manifest list parses and counts match.
        let (_, list_bytes) = table
            .files
            .iter()
            .find(|(loc, _)| loc.contains("/snap-"))
            .expect("manifest list present");
        let list = meridian_iceberg::manifest::read_manifest_list(list_bytes).expect("parse list");
        assert_eq!(list.manifests.len(), 3);
        let total: i32 = list
            .manifests
            .iter()
            .map(|m| m.added_files_count.unwrap_or_default())
            .sum();
        assert_eq!(total, 25);
        assert!(list.manifests.iter().all(|m| m.partitions.is_some()));

        // Each manifest parses; entries carry stats and partition tuples.
        for (loc, bytes) in &table.files {
            if loc.contains("/mf-") {
                let manifest =
                    meridian_iceberg::manifest::read_manifest(bytes).expect("parse manifest");
                assert!(!manifest.entries.is_empty());
                for entry in &manifest.entries {
                    assert!(entry.data_file.lower_bounds.is_some());
                    assert_eq!(entry.data_file.partition.fields.len(), 1);
                }
            }
        }

        // Metadata parses into the typed model with the snapshot wired up.
        let (_, metadata_bytes) = table.files.last().expect("metadata last");
        let metadata: TableMetadata =
            serde_json::from_slice(metadata_bytes).expect("parse metadata");
        assert_eq!(metadata.current_snapshot_id, Some(table.snapshot_id));
    }

    #[test]
    fn mor_table_round_trips_and_marks_delete_manifests() {
        let table = mor_table("file:///tmp/mor-check").expect("generate");
        let (_, list_bytes) = table
            .files
            .iter()
            .find(|(loc, _)| loc.contains(&format!("snap-{}", table.current_snapshot_id)))
            .expect("current manifest list");
        let list = meridian_iceberg::manifest::read_manifest_list(list_bytes).expect("parse list");
        assert_eq!(list.manifests.len(), 4);
        let deletes = list
            .manifests
            .iter()
            .filter(|m| m.content == ManifestContentType::Deletes)
            .count();
        assert_eq!(deletes, 2);
        // The equality-delete manifest is written under the empty spec.
        assert!(list.manifests.iter().any(|m| m.partition_spec_id == 1));
    }
}
