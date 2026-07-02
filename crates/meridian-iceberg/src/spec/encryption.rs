//! Table encryption-key model (v3).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// An encrypted key-metadata entry stored in table metadata (v3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EncryptedKey {
    /// Unique key id within the table.
    pub key_id: String,
    /// Base64-encoded encrypted key metadata.
    pub encrypted_key_metadata: String,
    /// Id of the key that encrypted this one, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypted_by_id: Option<String>,
    /// Free-form key properties.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, String>>,
    /// Unknown fields, preserved verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
