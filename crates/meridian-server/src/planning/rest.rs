//! REST wire shapes for scan planning, exactly as the `OpenAPI` document
//! defines them: `PlanTableScanRequest`, `FetchScanTasksRequest`, and the
//! JSON builders for `FileScanTask` / `DataFile` / `DeleteFile` /
//! `ScanTasks` (JSON single-value serialization for partition values and
//! bounds, kebab-case field names, lowercase `FileFormat`).
//!
//! Everything here is *serialization only* — which files appear, and with
//! which delete references and residuals, is decided in
//! [`super::engine`].

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate};
use meridian_iceberg::expr::Expression;
use meridian_iceberg::manifest::{DataFile, DataFileContent};
use meridian_iceberg::spec::PrimitiveType;
use meridian_iceberg::value::Datum;
use serde::Deserialize;
use serde_json::{Map, Value, json};

/// The spec's `PlanTableScanRequest`.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub struct PlanTableScanRequest {
    /// Snapshot to scan (point-in-time); defaults to the current snapshot.
    pub snapshot_id: Option<i64>,
    /// Selected schema field names (validated; see `docs/api-status.md`
    /// for current projection semantics).
    pub select: Option<Vec<String>>,
    /// Scan filter expression.
    pub filter: Option<Expression>,
    /// Row-count hint; accepted and ignored (the server may return more).
    pub min_rows_requested: Option<i64>,
    /// Case sensitivity for `select`/`filter` name resolution.
    pub case_sensitive: Option<bool>,
    /// Bind names against the snapshot's schema instead of the current one.
    pub use_snapshot_schema: Option<bool>,
    /// Incremental scan start (exclusive) — not yet implemented.
    pub start_snapshot_id: Option<i64>,
    /// Incremental scan end (inclusive) — not yet implemented.
    pub end_snapshot_id: Option<i64>,
    /// Fields to include column stats for; absent means all stats.
    pub stats_fields: Option<Vec<String>>,
}

/// The spec's `FetchScanTasksRequest`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct FetchScanTasksRequest {
    /// The opaque plan-task token from a completed planning result.
    pub plan_task: String,
}

/// Renders a datum as the spec's JSON single-value serialization
/// (`PrimitiveTypeValue`).
#[must_use]
pub fn datum_to_rest_json(datum: &Datum) -> Value {
    match datum {
        Datum::Boolean(b) => json!(b),
        Datum::Int(v) => json!(v),
        Datum::Long(v) => json!(v),
        Datum::Float(v) => float_json(f64::from(*v)),
        Datum::Double(v) => float_json(*v),
        Datum::Date(days) => json!(format_date(*days)),
        Datum::Time(micros) => json!(format_time_micros(*micros)),
        Datum::Timestamp(micros) => json!(format_timestamp_micros(*micros, false)),
        Datum::Timestamptz(micros) => json!(format_timestamp_micros(*micros, true)),
        Datum::TimestampNs(nanos) => json!(format_timestamp_nanos(*nanos, false)),
        Datum::TimestamptzNs(nanos) => json!(format_timestamp_nanos(*nanos, true)),
        Datum::String(s) => json!(s),
        Datum::Uuid(u) => json!(u.to_string()),
        Datum::Fixed(bytes) | Datum::Binary(bytes) => json!(upper_hex(bytes)),
        Datum::Decimal { unscaled, scale } => json!(format_decimal(*unscaled, *scale)),
    }
}

/// Non-finite floats have no JSON number; the reference implementation
/// writes them as the strings `"NaN"` / `"Infinity"` / `"-Infinity"`.
fn float_json(v: f64) -> Value {
    if v.is_nan() {
        json!("NaN")
    } else if v.is_infinite() {
        json!(if v > 0.0 { "Infinity" } else { "-Infinity" })
    } else {
        json!(v)
    }
}

fn format_date(days_from_epoch: i32) -> String {
    // 719_163 = days from 0001-01-01 (CE) to 1970-01-01.
    NaiveDate::from_num_days_from_ce_opt(days_from_epoch.saturating_add(719_163)).map_or_else(
        || format!("out-of-range-date({days_from_epoch})"),
        |d| d.format("%Y-%m-%d").to_string(),
    )
}

fn format_time_micros(micros: i64) -> String {
    let (secs, sub) = (micros.div_euclid(1_000_000), micros.rem_euclid(1_000_000));
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    format!("{h:02}:{m:02}:{s:02}.{sub:06}")
}

fn format_timestamp_micros(micros: i64, utc_offset: bool) -> String {
    DateTime::from_timestamp_micros(micros).map_or_else(
        || format!("out-of-range-timestamp({micros})"),
        |ts| {
            let base = ts.format("%Y-%m-%dT%H:%M:%S%.6f").to_string();
            if utc_offset { base + "+00:00" } else { base }
        },
    )
}

fn format_timestamp_nanos(nanos: i64, utc_offset: bool) -> String {
    let ts = DateTime::from_timestamp_nanos(nanos);
    let base = ts.format("%Y-%m-%dT%H:%M:%S%.9f").to_string();
    if utc_offset { base + "+00:00" } else { base }
}

fn upper_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            // Writing to a String cannot fail.
            let _ = write!(out, "{b:02X}");
            out
        })
}

fn format_decimal(unscaled: i128, scale: u32) -> String {
    let negative = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    let mut body = if scale == 0 {
        digits
    } else if digits.len() > scale {
        let (int_part, frac_part) = digits.split_at(digits.len() - scale);
        format!("{int_part}.{frac_part}")
    } else {
        format!("0.{digits:0>scale$}")
    };
    if negative {
        body.insert(0, '-');
    }
    body
}

/// Manifest file-format strings are stored uppercase; the REST
/// `FileFormat` enum is lowercase.
fn rest_file_format(raw: &str) -> String {
    raw.to_ascii_lowercase()
}

/// A `CountMap`: parallel `keys`/`values` arrays.
fn count_map_json(
    map: &BTreeMap<i32, i64>,
    keep: Option<&std::collections::BTreeSet<i32>>,
) -> Value {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for (k, v) in map {
        if keep.is_none_or(|set| set.contains(k)) {
            keys.push(json!(k));
            values.push(json!(v));
        }
    }
    json!({ "keys": keys, "values": values })
}

/// A `ValueMap`: bounds decoded to typed JSON single values. Bounds whose
/// column type is unknown (dropped columns) or whose bytes do not decode
/// are omitted — the map stays valid, clients just see fewer bounds.
fn value_map_json(
    map: &BTreeMap<i32, Vec<u8>>,
    types: &BTreeMap<i32, PrimitiveType>,
    keep: Option<&std::collections::BTreeSet<i32>>,
) -> Value {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for (k, bytes) in map {
        if !keep.is_none_or(|set| set.contains(k)) {
            continue;
        }
        let Some(ty) = types.get(k) else { continue };
        let Ok(datum) = Datum::from_bound_bytes(ty, bytes) else {
            continue;
        };
        keys.push(json!(k));
        values.push(datum_to_rest_json(&datum));
    }
    json!({ "keys": keys, "values": values })
}

/// Which stats to include on a returned `DataFile`.
#[derive(Debug, Clone, Copy)]
pub struct StatsFilter<'a> {
    /// `None` sends all stats; `Some(ids)` restricts to those columns.
    pub keep: Option<&'a std::collections::BTreeSet<i32>>,
    /// Column types for decoding bounds.
    pub types: &'a BTreeMap<i32, PrimitiveType>,
}

/// Builds the REST `ContentFile` core shared by data and delete files.
fn content_file_core(file: &DataFile, spec_id: i32, content: &str) -> Map<String, Value> {
    let mut obj = Map::new();
    obj.insert("content".to_owned(), json!(content));
    obj.insert("file-path".to_owned(), json!(file.file_path));
    obj.insert(
        "file-format".to_owned(),
        json!(rest_file_format(&file.file_format)),
    );
    obj.insert("spec-id".to_owned(), json!(spec_id));
    let partition: Vec<Value> = file
        .partition
        .fields
        .iter()
        .map(|f| f.value.as_ref().map_or(Value::Null, datum_to_rest_json))
        .collect();
    obj.insert("partition".to_owned(), Value::Array(partition));
    obj.insert(
        "file-size-in-bytes".to_owned(),
        json!(file.file_size_in_bytes),
    );
    obj.insert("record-count".to_owned(), json!(file.record_count));
    if let Some(key_metadata) = &file.key_metadata {
        obj.insert("key-metadata".to_owned(), json!(upper_hex(key_metadata)));
    }
    if let Some(split_offsets) = &file.split_offsets {
        obj.insert("split-offsets".to_owned(), json!(split_offsets));
    }
    if let Some(sort_order_id) = file.sort_order_id {
        obj.insert("sort-order-id".to_owned(), json!(sort_order_id));
    }
    obj
}

/// Builds the REST `DataFile` for a data file (stats included per the
/// filter).
#[must_use]
pub fn data_file_json(file: &DataFile, spec_id: i32, stats: StatsFilter<'_>) -> Value {
    let mut obj = content_file_core(file, spec_id, "data");
    if let Some(first_row_id) = file.first_row_id {
        obj.insert("first-row-id".to_owned(), json!(first_row_id));
    }
    let count_maps = [
        ("column-sizes", &file.column_sizes),
        ("value-counts", &file.value_counts),
        ("null-value-counts", &file.null_value_counts),
        ("nan-value-counts", &file.nan_value_counts),
    ];
    for (name, map) in count_maps {
        if let Some(map) = map {
            obj.insert(name.to_owned(), count_map_json(map, stats.keep));
        }
    }
    if let Some(map) = &file.lower_bounds {
        obj.insert(
            "lower-bounds".to_owned(),
            value_map_json(map, stats.types, stats.keep),
        );
    }
    if let Some(map) = &file.upper_bounds {
        obj.insert(
            "upper-bounds".to_owned(),
            value_map_json(map, stats.types, stats.keep),
        );
    }
    Value::Object(obj)
}

/// Builds the REST `DeleteFile` (position or equality shape by content).
/// The REST delete-file schemas carry no column stats.
#[must_use]
pub fn delete_file_json(file: &DataFile, spec_id: i32) -> Value {
    match file.content {
        DataFileContent::EqualityDeletes => {
            let mut obj = content_file_core(file, spec_id, "equality-deletes");
            if let Some(equality_ids) = &file.equality_ids {
                obj.insert("equality-ids".to_owned(), json!(equality_ids));
            }
            Value::Object(obj)
        }
        // Deletion vectors are position deletes with a content offset.
        DataFileContent::PositionDeletes | DataFileContent::Data => {
            let mut obj = content_file_core(file, spec_id, "position-deletes");
            if let Some(offset) = file.content_offset {
                obj.insert("content-offset".to_owned(), json!(offset));
            }
            if let Some(size) = file.content_size_in_bytes {
                obj.insert("content-size-in-bytes".to_owned(), json!(size));
            }
            Value::Object(obj)
        }
    }
}

/// Builds one REST `FileScanTask`.
#[must_use]
pub fn file_scan_task_json(
    data_file: Value,
    delete_file_references: &[usize],
    residual: Option<&Expression>,
) -> Value {
    let mut obj = Map::new();
    obj.insert("data-file".to_owned(), data_file);
    if !delete_file_references.is_empty() {
        obj.insert(
            "delete-file-references".to_owned(),
            json!(delete_file_references),
        );
    }
    if let Some(residual) = residual {
        match serde_json::to_value(residual) {
            Ok(value) => {
                obj.insert("residual-filter".to_owned(), value);
            }
            Err(error) => {
                // Expression serialization is infallible in practice; if it
                // ever fails, omitting the residual is spec-legal (the
                // client falls back to the original filter).
                tracing::error!(%error, "failed to serialize residual filter; omitting");
            }
        }
    }
    Value::Object(obj)
}

/// Builds a REST `ScanTasks` object (one result page).
#[must_use]
pub fn scan_tasks_json(file_scan_tasks: Vec<Value>, delete_files: Vec<Value>) -> Value {
    let mut obj = Map::new();
    if !delete_files.is_empty() {
        obj.insert("delete-files".to_owned(), Value::Array(delete_files));
    }
    obj.insert("file-scan-tasks".to_owned(), Value::Array(file_scan_tasks));
    Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_value_rendering_matches_the_spec_formats() {
        assert_eq!(datum_to_rest_json(&Datum::Boolean(true)), json!(true));
        assert_eq!(datum_to_rest_json(&Datum::Int(-7)), json!(-7));
        assert_eq!(datum_to_rest_json(&Datum::Long(1)), json!(1));
        assert_eq!(datum_to_rest_json(&Datum::double(1.5)), json!(1.5));
        assert_eq!(datum_to_rest_json(&Datum::double(f64::NAN)), json!("NaN"));
        // 2007-12-03 is 13850 days from the epoch (spec example date).
        assert_eq!(datum_to_rest_json(&Datum::Date(13850)), json!("2007-12-03"));
        assert_eq!(
            datum_to_rest_json(&Datum::Time(81_068_123_456)),
            json!("22:31:08.123456")
        );
        assert_eq!(
            datum_to_rest_json(&Datum::Timestamp(1_196_676_930_123_456)),
            json!("2007-12-03T10:15:30.123456")
        );
        assert_eq!(
            datum_to_rest_json(&Datum::Timestamptz(1_196_676_930_123_456)),
            json!("2007-12-03T10:15:30.123456+00:00")
        );
        assert_eq!(
            datum_to_rest_json(&Datum::TimestampNs(1_196_676_930_123_456_789)),
            json!("2007-12-03T10:15:30.123456789")
        );
        assert_eq!(
            datum_to_rest_json(&Datum::Binary(vec![0x78, 0x79, 0x7A])),
            json!("78797A")
        );
        assert_eq!(
            datum_to_rest_json(&Datum::Decimal {
                unscaled: -1234,
                scale: 2
            }),
            json!("-12.34")
        );
        assert_eq!(
            datum_to_rest_json(&Datum::Decimal {
                unscaled: 5,
                scale: 3
            }),
            json!("0.005")
        );
    }

    #[test]
    fn dates_before_the_epoch_render() {
        assert_eq!(datum_to_rest_json(&Datum::Date(-1)), json!("1969-12-31"));
    }
}
