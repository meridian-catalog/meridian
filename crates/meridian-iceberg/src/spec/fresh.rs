//! Fresh field-id assignment for table creation.
//!
//! The field ids in a REST `CreateTableRequest` schema are *provisional*:
//! clients number them however they like (Flink's connector starts at 0,
//! pyiceberg at 1, hand-written requests arbitrarily). The reference
//! implementation (`TableMetadata.newTableMetadata`, via
//! `TypeUtil.assignFreshIds`) discards the incoming ids and assigns fresh
//! ones starting at 1, then remaps everything that referenced the
//! provisional ids: the schema's `identifier-field-ids`, the requested
//! partition spec's source ids, and the requested sort order's source ids.
//! [`assign_fresh_ids`] reproduces that, including the reference's
//! assignment order: all direct fields of a struct receive ids first, then
//! each field's nested type is visited in order (a list assigns its element
//! id before descending; a map assigns key id, then value id, then descends
//! into key and value).
//!
//! This applies **only** to table creation. An `add-schema` update on the
//! commit path carries *real* field ids that must line up with the table's
//! history, so [`super::builder::MetadataBuilder`] keeps strict validation
//! there.

use std::collections::{BTreeMap, BTreeSet};

use super::builder::MetadataBuildError;
use super::partition::PartitionSpec;
use super::schema::Schema;
use super::sort::SortOrder;
use super::types::{ListType, MapType, StructField, StructType, Type};

/// A create request's schema, partition spec, and sort order after fresh
/// field-id assignment.
#[derive(Debug, Clone)]
pub struct FreshCreate {
    /// The schema with server-assigned field ids (1-based, reference
    /// assignment order) and remapped `identifier-field-ids`. `schema-id`
    /// is left unassigned for the metadata builder to assign.
    pub schema: Schema,
    /// The partition spec with source ids remapped to the fresh schema and
    /// partition field ids cleared (the builder assigns those from 1000
    /// up). `spec-id` is left unassigned.
    pub partition_spec: Option<PartitionSpec>,
    /// The sort order with source ids remapped to the fresh schema.
    pub sort_order: Option<SortOrder>,
}

/// Reassigns every field id in a create request and remaps all references
/// to the provisional ids.
///
/// Rejects only genuinely broken requests:
///
/// - two sibling fields with the same name (references by name — the only
///   stable coordinate in a provisional schema — would be ambiguous);
/// - an identifier field or a partition/sort source referencing an id that
///   no schema field carries, or that more than one schema field carries.
///
/// Anything else — 0-based ids, negative ids, duplicate ids on unreferenced
/// fields — is accepted and replaced.
pub fn assign_fresh_ids(
    schema: &Schema,
    partition_spec: Option<&PartitionSpec>,
    sort_order: Option<&SortOrder>,
) -> Result<FreshCreate, MetadataBuildError> {
    let mut assigner = Assigner {
        next_id: 0,
        old_to_new: BTreeMap::new(),
    };

    let mut fresh_schema = schema.clone();
    fresh_schema.schema_id = None;
    fresh_schema.fields = assigner.fresh_fields(&schema.fields)?;
    fresh_schema.identifier_field_ids = match &schema.identifier_field_ids {
        None => None,
        Some(ids) => {
            let mut fresh_ids = Vec::with_capacity(ids.len());
            for id in ids {
                match assigner.old_to_new.get(id) {
                    Some(Mapping::Unique(new_id)) => fresh_ids.push(*new_id),
                    Some(Mapping::Ambiguous) => {
                        return Err(MetadataBuildError::InvalidSchema {
                            reason: format!(
                                "identifier field id {id} is ambiguous: more than one schema field carries it"
                            ),
                        });
                    }
                    None => {
                        return Err(MetadataBuildError::InvalidSchema {
                            reason: format!(
                                "identifier field id {id} is not a field of the schema"
                            ),
                        });
                    }
                }
            }
            Some(fresh_ids)
        }
    };

    let partition_spec = match partition_spec {
        None => None,
        Some(spec) => {
            let mut fresh = spec.clone();
            fresh.spec_id = None;
            for field in &mut fresh.fields {
                // Partition field ids are server-assigned at create, like
                // schema field ids.
                field.field_id = None;
                field.source_id = match assigner.old_to_new.get(&field.source_id) {
                    Some(Mapping::Unique(new_id)) => *new_id,
                    Some(Mapping::Ambiguous) => {
                        return Err(MetadataBuildError::InvalidPartitionSpec {
                            reason: format!(
                                "partition field {:?} references source field id {}, which more than one schema field carries",
                                field.name, field.source_id
                            ),
                        });
                    }
                    None => {
                        return Err(MetadataBuildError::UnknownSourceField {
                            source_id: field.source_id,
                        });
                    }
                };
            }
            Some(fresh)
        }
    };

    let sort_order = match sort_order {
        None => None,
        Some(order) => {
            let mut fresh = order.clone();
            for field in &mut fresh.fields {
                field.source_id = match assigner.old_to_new.get(&field.source_id) {
                    Some(Mapping::Unique(new_id)) => *new_id,
                    Some(Mapping::Ambiguous) => {
                        return Err(MetadataBuildError::InvalidSchema {
                            reason: format!(
                                "sort field references source field id {}, which more than one schema field carries",
                                field.source_id
                            ),
                        });
                    }
                    None => {
                        return Err(MetadataBuildError::UnknownSourceField {
                            source_id: field.source_id,
                        });
                    }
                };
            }
            Some(fresh)
        }
    };

    Ok(FreshCreate {
        schema: fresh_schema,
        partition_spec,
        sort_order,
    })
}

/// Where one provisional id ended up.
enum Mapping {
    /// Exactly one field carried the provisional id.
    Unique(i32),
    /// More than one field carried the provisional id; references to it
    /// cannot be resolved.
    Ambiguous,
}

struct Assigner {
    next_id: i32,
    old_to_new: BTreeMap<i32, Mapping>,
}

impl Assigner {
    fn assign(&mut self, old_id: i32) -> i32 {
        self.next_id += 1;
        let new_id = self.next_id;
        self.old_to_new
            .entry(old_id)
            .and_modify(|mapping| *mapping = Mapping::Ambiguous)
            .or_insert(Mapping::Unique(new_id));
        new_id
    }

    fn fresh_fields(
        &mut self,
        fields: &[StructField],
    ) -> Result<Vec<StructField>, MetadataBuildError> {
        let mut names = BTreeSet::new();
        for field in fields {
            if !names.insert(field.name.as_str()) {
                return Err(MetadataBuildError::InvalidSchema {
                    reason: format!("duplicate field name {:?} within one struct", field.name),
                });
            }
        }
        // Reference order: ids for all direct fields first, then each
        // field's nested type in order.
        let new_ids: Vec<i32> = fields.iter().map(|field| self.assign(field.id)).collect();
        fields
            .iter()
            .zip(new_ids)
            .map(|(field, new_id)| {
                let mut fresh = field.clone();
                fresh.id = new_id;
                fresh.field_type = self.fresh_type(&field.field_type)?;
                Ok(fresh)
            })
            .collect()
    }

    fn fresh_type(&mut self, field_type: &Type) -> Result<Type, MetadataBuildError> {
        match field_type {
            Type::Primitive(_) => Ok(field_type.clone()),
            Type::Struct(nested) => {
                let mut fresh = StructType::new(self.fresh_fields(&nested.fields)?);
                fresh.extra.clone_from(&nested.extra);
                Ok(Type::Struct(fresh))
            }
            Type::List(list) => {
                let element_id = self.assign(list.element_id);
                let mut fresh = ListType::new(
                    element_id,
                    self.fresh_type(&list.element)?,
                    list.element_required,
                );
                fresh.extra.clone_from(&list.extra);
                Ok(Type::List(fresh))
            }
            Type::Map(map) => {
                let key_id = self.assign(map.key_id);
                let value_id = self.assign(map.value_id);
                let key = self.fresh_type(&map.key)?;
                let value = self.fresh_type(&map.value)?;
                let mut fresh = MapType::new(key_id, key, value_id, value, map.value_required);
                fresh.extra.clone_from(&map.extra);
                Ok(Type::Map(fresh))
            }
        }
    }
}
