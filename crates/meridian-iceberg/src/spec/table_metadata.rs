//! The top-level table-metadata model (`metadata.json`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::encryption::EncryptedKey;
use super::partition::PartitionSpec;
use super::schema::Schema;
use super::snapshot::{MetadataLogEntry, Snapshot, SnapshotLogEntry, SnapshotRef};
use super::sort::SortOrder;
use super::statistics::{PartitionStatisticsFile, StatisticsFile};

/// Partition field ids are assigned starting at 1000; the last assigned id
/// of an unpartitioned table is therefore 999.
pub const PARTITION_DATA_ID_START: i32 = 1000;

/// Failure to parse a `metadata.json` document.
#[derive(Debug, thiserror::Error)]
pub enum MetadataParseError {
    /// The document is not valid JSON or does not match the metadata shape.
    #[error("invalid metadata JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// The document is structurally JSON but not valid table metadata.
    #[error("invalid table metadata: {0}")]
    Invalid(String),
}

/// Iceberg table metadata: the unified v1/v2/v3 model.
///
/// v2 is the reference shape. v1 documents are **normalized on read**: the
/// legacy single `schema` / `partition-spec` fields are lifted into
/// `schemas` / `partition-specs` with default ids assigned, and missing
/// v1-optional tracking fields get their spec-mandated defaults (see
/// [`TableMetadata::from_json`]). When serializing a v1 table,
/// [`TableMetadata::to_json`] re-emits the legacy `schema` and
/// `partition-spec` fields alongside the modern lists, as v1 writers are
/// expected to.
///
/// Anything not modelled is preserved untouched in [`TableMetadata::extra`]
/// (and the `extra` maps of every nested struct).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    /// Format version: 1, 2, or 3.
    pub format_version: u8,
    /// Table UUID, stable for the lifetime of the table.
    pub table_uuid: Uuid,
    /// Base location of the table.
    pub location: String,
    /// Highest assigned commit sequence number. Required in v2+; absent in
    /// v1 files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sequence_number: Option<i64>,
    /// v3 row lineage: a value higher than all assigned row ids; the next
    /// snapshot's `first-row-id`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_row_id: Option<i64>,
    /// When the metadata was last updated (epoch millis).
    pub last_updated_ms: i64,
    /// Highest assigned column field id.
    pub last_column_id: i32,
    /// All known schemas.
    pub schemas: Vec<Schema>,
    /// Id of the current schema in [`TableMetadata::schemas`].
    pub current_schema_id: i32,
    /// All known partition specs.
    pub partition_specs: Vec<PartitionSpec>,
    /// Id of the default partition spec.
    pub default_spec_id: i32,
    /// Highest assigned partition field id.
    pub last_partition_id: i32,
    /// All known sort orders.
    pub sort_orders: Vec<SortOrder>,
    /// Id of the default sort order.
    pub default_sort_order_id: i32,
    /// Table properties (string key/value).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, String>>,
    /// Id of the current snapshot, if any. May be `-1`/absent when the table
    /// has no snapshots.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,
    /// All retained snapshots.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshots: Option<Vec<Snapshot>>,
    /// History of current-snapshot changes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_log: Option<Vec<SnapshotLogEntry>>,
    /// History of previous metadata files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_log: Option<Vec<MetadataLogEntry>>,
    /// Named branch/tag references.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refs: Option<BTreeMap<String, SnapshotRef>>,
    /// Statistics files, one per snapshot at most.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<Vec<StatisticsFile>>,
    /// Partition-statistics files, one per snapshot at most.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_statistics: Option<Vec<PartitionStatisticsFile>>,
    /// v3 encryption keys.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_keys: Option<Vec<EncryptedKey>>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl TableMetadata {
    /// Parses table metadata from a JSON string.
    ///
    /// v1 documents are normalized to the unified model: the legacy `schema`
    /// field becomes `schemas` + `current-schema-id` (assigning schema id 0
    /// when absent), the legacy `partition-spec` field list becomes
    /// `partition-specs` + `default-spec-id` (assigning spec id 0 and
    /// partition field ids from 1000 when absent), missing `sort-orders`
    /// default to the unsorted order, and `last-partition-id` is computed
    /// when missing.
    pub fn from_json(json: &str) -> Result<Self, MetadataParseError> {
        let mut value: Value = serde_json::from_str(json)?;
        let format_version = value
            .get("format-version")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                MetadataParseError::Invalid("missing or non-integer format-version".to_owned())
            })?;
        match format_version {
            1 => {
                let root = value.as_object_mut().ok_or_else(|| {
                    MetadataParseError::Invalid("metadata must be a JSON object".to_owned())
                })?;
                normalize_v1(root)?;
            }
            2 | 3 => {}
            other => {
                return Err(MetadataParseError::Invalid(format!(
                    "unsupported format-version {other} (supported: 1, 2, 3)"
                )));
            }
        }
        Ok(serde_json::from_value(value)?)
    }

    /// Serializes table metadata to a JSON string.
    ///
    /// For v1 tables the legacy `schema` and `partition-spec` fields are
    /// re-derived from the current schema and default spec so the output is
    /// readable by v1-only readers.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        let mut value = serde_json::to_value(self)?;
        if self.format_version == 1
            && let Some(root) = value.as_object_mut()
        {
            if let Some(current) = self.current_schema() {
                root.insert("schema".to_owned(), serde_json::to_value(current)?);
            }
            if let Some(spec) = self.default_partition_spec() {
                root.insert(
                    "partition-spec".to_owned(),
                    serde_json::to_value(&spec.fields)?,
                );
            }
        }
        serde_json::to_string(&value)
    }

    /// The current schema, if `current-schema-id` resolves.
    #[must_use]
    pub fn current_schema(&self) -> Option<&Schema> {
        self.schema_by_id(self.current_schema_id)
    }

    /// The schema with the given id, if present.
    #[must_use]
    pub fn schema_by_id(&self, schema_id: i32) -> Option<&Schema> {
        self.schemas.iter().find(|s| s.schema_id == Some(schema_id))
    }

    /// The default partition spec, if `default-spec-id` resolves.
    #[must_use]
    pub fn default_partition_spec(&self) -> Option<&PartitionSpec> {
        self.partition_spec_by_id(self.default_spec_id)
    }

    /// The partition spec with the given id, if present.
    #[must_use]
    pub fn partition_spec_by_id(&self, spec_id: i32) -> Option<&PartitionSpec> {
        self.partition_specs
            .iter()
            .find(|s| s.spec_id == Some(spec_id))
    }

    /// The default sort order, if `default-sort-order-id` resolves.
    #[must_use]
    pub fn default_sort_order(&self) -> Option<&SortOrder> {
        self.sort_order_by_id(self.default_sort_order_id)
    }

    /// The sort order with the given id, if present.
    #[must_use]
    pub fn sort_order_by_id(&self, order_id: i32) -> Option<&SortOrder> {
        self.sort_orders.iter().find(|o| o.order_id == order_id)
    }

    /// The current snapshot, if one is set and present.
    #[must_use]
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        let id = self.current_snapshot_id.filter(|id| *id >= 0)?;
        self.snapshot_by_id(id)
    }

    /// The snapshot with the given id, if present.
    #[must_use]
    pub fn snapshot_by_id(&self, snapshot_id: i64) -> Option<&Snapshot> {
        self.snapshots
            .as_ref()?
            .iter()
            .find(|s| s.snapshot_id == snapshot_id)
    }

    /// The value of a table property, if set.
    #[must_use]
    pub fn property(&self, key: &str) -> Option<&str> {
        self.properties.as_ref()?.get(key).map(String::as_str)
    }
}

/// Lifts a v1 document's legacy fields into the unified (v2-shaped) model.
fn normalize_v1(root: &mut Map<String, Value>) -> Result<(), MetadataParseError> {
    // Schemas: prefer the modern list when present (mirroring the reference
    // implementation's read order); otherwise lift the single `schema`.
    if !root.contains_key("schemas") {
        let mut schema = root.remove("schema").ok_or_else(|| {
            MetadataParseError::Invalid("v1 metadata has neither schemas nor schema".to_owned())
        })?;
        let schema_id = if let Some(id) = schema.get("schema-id").and_then(Value::as_i64) {
            id
        } else {
            if let Some(obj) = schema.as_object_mut() {
                obj.insert("schema-id".to_owned(), json!(0));
            }
            0
        };
        root.insert("schemas".to_owned(), json!([schema]));
        root.entry("current-schema-id".to_owned())
            .or_insert(json!(schema_id));
    }
    // The legacy field is represented by the list now; drop it so it cannot
    // go stale (it is re-derived on write).
    root.remove("schema");
    if !root.contains_key("current-schema-id") {
        return Err(MetadataParseError::Invalid(
            "v1 metadata has schemas but no current-schema-id".to_owned(),
        ));
    }

    // Partition specs: lift the legacy `partition-spec` field list.
    if !root.contains_key("partition-specs") {
        let fields = match root.remove("partition-spec") {
            Some(Value::Array(fields)) => fields,
            Some(_) => {
                return Err(MetadataParseError::Invalid(
                    "v1 partition-spec must be an array of partition fields".to_owned(),
                ));
            }
            None => Vec::new(),
        };
        let fields: Vec<Value> = fields
            .into_iter()
            .enumerate()
            .map(|(index, mut field)| {
                if let Some(obj) = field.as_object_mut()
                    && !obj.contains_key("field-id")
                {
                    obj.insert(
                        "field-id".to_owned(),
                        json!(PARTITION_DATA_ID_START + i32::try_from(index).unwrap_or(0)),
                    );
                }
                field
            })
            .collect();
        root.insert(
            "partition-specs".to_owned(),
            json!([{ "spec-id": 0, "fields": fields }]),
        );
        root.entry("default-spec-id".to_owned()).or_insert(json!(0));
    }
    root.remove("partition-spec");
    root.entry("default-spec-id".to_owned()).or_insert(json!(0));

    // last-partition-id: highest assigned field id, or 999 when nothing was
    // ever assigned.
    if !root.contains_key("last-partition-id") {
        let max_assigned = root
            .get("partition-specs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|s| s.get("fields").and_then(Value::as_array))
            .flatten()
            .filter_map(|f| f.get("field-id").and_then(Value::as_i64))
            .max()
            .unwrap_or(i64::from(PARTITION_DATA_ID_START) - 1);
        root.insert("last-partition-id".to_owned(), json!(max_assigned));
    }

    // Sort orders: default to the unsorted order.
    root.entry("sort-orders".to_owned())
        .or_insert_with(|| json!([{ "order-id": 0, "fields": [] }]));
    root.entry("default-sort-order-id".to_owned())
        .or_insert(json!(0));
    Ok(())
}
