//! The top-level table-metadata model (`metadata.json`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::encryption::EncryptedKey;
use super::partition::PartitionSpec;
use super::schema::Schema;
use super::snapshot::{MetadataLogEntry, RefType, Snapshot, SnapshotLogEntry, SnapshotRef};
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

    /// Retargets this candidate metadata onto a **quarantine branch** so a
    /// contract-violating commit lands *off* `main` (Pillar E, E-F4, managed
    /// WAP). `self` is the candidate the client's updates already built to
    /// advance `main`; `base` is the metadata the commit was based on.
    ///
    /// The transform (pure; see `docs/design/contracts-circuit-breaker.md`
    /// §3.3):
    ///
    /// - every snapshot the candidate added (present in the candidate but not
    ///   the base) is **retained** in `snapshots` — the producer's data and
    ///   manifests are not thrown away;
    /// - `current_snapshot_id` and `refs["main"]` are **reset to the base's
    ///   values**, so from every reader's perspective `main` did not move;
    /// - a branch ref `refs[branch]` is pointed at the candidate's head
    ///   snapshot, so the quarantined work is addressable for publish/discard;
    /// - the `snapshot_log` (the current-snapshot history) is reset to the
    ///   base's, since the current snapshot did not change.
    ///
    /// Returns the id of the quarantined head snapshot (for the violation
    /// record), or `None` when the candidate added no snapshot — in which case
    /// nothing is retargeted and the metadata is left unchanged (a schema-only
    /// violation cannot be quarantined; the caller degrades to block).
    #[must_use = "the quarantined head snapshot id must be recorded"]
    pub fn quarantine_retarget(&mut self, base: &TableMetadata, branch: &str) -> Option<i64> {
        // The head to quarantine is this candidate's current snapshot — the one
        // the client's commit made current. If it equals the base's current,
        // the commit advanced no snapshot: nothing to quarantine.
        let head = self.current_snapshot_id.filter(|id| *id >= 0)?;
        if base.current_snapshot_id.filter(|id| *id >= 0) == Some(head) {
            return None;
        }
        // The head snapshot must actually be retained; a candidate that set the
        // pointer without adding the snapshot is malformed for our purposes.
        self.snapshot_by_id(head)?;

        // Freeze the current-snapshot pointer at the base value.
        self.current_snapshot_id = base.current_snapshot_id;

        // Restore refs to the base set, then point the quarantine branch at the
        // head. This resets `main` (and any other ref the commit moved) to what
        // the base had, so no consumer ref advances past the violation.
        let mut refs = base.refs.clone().unwrap_or_default();
        refs.insert(
            branch.to_owned(),
            SnapshotRef {
                snapshot_id: head,
                ref_type: RefType::Branch,
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
                extra: Map::new(),
            },
        );
        self.refs = Some(refs);

        // The current-snapshot history did not change (main is frozen), so the
        // snapshot log must match the base's, not carry the quarantined head.
        self.snapshot_log.clone_from(&base.snapshot_log);

        Some(head)
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

#[cfg(test)]
mod quarantine_tests {
    use super::*;

    /// A v2 table with one snapshot (id 100) as `main`.
    fn base_with_snapshot() -> TableMetadata {
        let json = json!({
            "format-version": 2,
            "table-uuid": "11111111-1111-1111-1111-111111111111",
            "location": "s3://b/t",
            "last-sequence-number": 1,
            "last-updated-ms": 1_700_000_000_000i64,
            "last-column-id": 1,
            "schemas": [{ "type": "struct", "schema-id": 0,
                "fields": [{ "id": 1, "name": "id", "required": true, "type": "long" }] }],
            "current-schema-id": 0,
            "partition-specs": [{ "spec-id": 0, "fields": [] }],
            "default-spec-id": 0,
            "last-partition-id": 999,
            "sort-orders": [{ "order-id": 0, "fields": [] }],
            "default-sort-order-id": 0,
            "current-snapshot-id": 100,
            "snapshots": [{
                "snapshot-id": 100, "sequence-number": 1,
                "timestamp-ms": 1_700_000_000_000i64,
                "manifest-list": "s3://b/t/snap-100.avro",
                "summary": { "operation": "append" }, "schema-id": 0
            }],
            "snapshot-log": [{ "snapshot-id": 100, "timestamp-ms": 1_700_000_000_000i64 }],
            "refs": { "main": { "snapshot-id": 100, "type": "branch" } }
        });
        TableMetadata::from_json(&json.to_string()).expect("parse base")
    }

    /// The base plus a new snapshot (id 200) as `main` — an ordinary append the
    /// circuit breaker will quarantine.
    fn candidate_advancing_main() -> TableMetadata {
        let mut candidate = base_with_snapshot();
        let snap = json!({
            "snapshot-id": 200, "parent-snapshot-id": 100, "sequence-number": 2,
            "timestamp-ms": 1_700_000_001_000i64,
            "manifest-list": "s3://b/t/snap-200.avro",
            "summary": { "operation": "append", "total-records": "5" }, "schema-id": 0
        });
        let snap: Snapshot = serde_json::from_value(snap).expect("snapshot");
        candidate.snapshots.get_or_insert_with(Vec::new).push(snap);
        candidate.current_snapshot_id = Some(200);
        candidate
            .snapshot_log
            .get_or_insert_with(Vec::new)
            .push(SnapshotLogEntry {
                snapshot_id: 200,
                timestamp_ms: 1_700_000_001_000,
            });
        candidate.refs.as_mut().unwrap().insert(
            "main".to_owned(),
            SnapshotRef {
                snapshot_id: 200,
                ref_type: RefType::Branch,
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
                extra: Map::new(),
            },
        );
        candidate
    }

    #[test]
    fn retarget_freezes_main_and_branches_the_head() {
        let base = base_with_snapshot();
        let mut candidate = candidate_advancing_main();

        let head = candidate.quarantine_retarget(&base, "q").expect("head");
        assert_eq!(head, 200);

        // main is frozen at the base snapshot.
        assert_eq!(candidate.current_snapshot_id, Some(100));
        assert_eq!(candidate.refs.as_ref().unwrap()["main"].snapshot_id, 100);
        // the quarantine branch points at the new head.
        assert_eq!(candidate.refs.as_ref().unwrap()["q"].snapshot_id, 200);
        assert_eq!(
            candidate.refs.as_ref().unwrap()["q"].ref_type,
            RefType::Branch
        );
        // the new snapshot is retained (durable, not thrown away).
        assert!(candidate.snapshot_by_id(200).is_some());
        assert!(candidate.snapshot_by_id(100).is_some());
        // the snapshot log matches the base (current snapshot did not change).
        assert_eq!(
            candidate.snapshot_log.as_ref().map(Vec::len),
            base.snapshot_log.as_ref().map(Vec::len)
        );
    }

    #[test]
    fn retarget_is_noop_when_no_snapshot_added() {
        // A schema-only candidate (main still at 100) cannot be quarantined.
        let base = base_with_snapshot();
        let mut candidate = base_with_snapshot();
        candidate.last_column_id = 2; // pretend a schema change, no new snapshot
        assert_eq!(candidate.quarantine_retarget(&base, "q"), None);
        // untouched: main unchanged, no quarantine ref added.
        assert_eq!(candidate.current_snapshot_id, Some(100));
        assert!(!candidate.refs.as_ref().unwrap().contains_key("q"));
    }

    #[test]
    fn retarget_from_empty_base_freezes_to_no_current() {
        // First-ever commit (base has no snapshot) that adds snapshot 200:
        // quarantine freezes current to None (the table stays empty on main).
        let mut base = base_with_snapshot();
        base.current_snapshot_id = None;
        base.snapshots = Some(vec![]);
        base.snapshot_log = None;
        base.refs = None;

        let mut candidate = candidate_advancing_main();
        // Rebase the candidate onto the empty base: only snapshot 200 present.
        candidate.snapshots = Some(vec![
            candidate.snapshot_by_id(200).cloned().expect("200 present"),
        ]);

        let head = candidate.quarantine_retarget(&base, "q").expect("head");
        assert_eq!(head, 200);
        assert_eq!(candidate.current_snapshot_id, None);
        assert_eq!(candidate.refs.as_ref().unwrap()["q"].snapshot_id, 200);
        assert!(!candidate.refs.as_ref().unwrap().contains_key("main"));
    }
}
