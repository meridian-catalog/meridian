//! The resource input to an authorization decision.
//!
//! A [`AuthzResource`] is the thing being accessed — a table, namespace,
//! view, or a specific column — carrying the attributes ABAC policies
//! reason about: its **tags** (`pii:high`, `pii:email`, …), its **owner**,
//! and a **classification** (`public`/`internal`/`restricted`). Tags are
//! the pivot of the whole model: policies attach to tags ("`pii:high`
//! denies read unless purpose granted") and tags carry the sensitivity a
//! catalog knows about a column.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What kind of asset is being accessed.
///
/// Becomes the Cedar resource **entity type**. A `Column` is a first-class
/// resource so column-level policies (masks) evaluate against the column's
/// own tags, and so a column entity can be a *child* of its table entity
/// for hierarchy-aware policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// A namespace (database/schema).
    Namespace,
    /// A table.
    Table,
    /// A view.
    View,
    /// A single column of a table or view.
    Column,
}

impl ResourceKind {
    /// The Cedar entity type name for this kind.
    #[must_use]
    pub fn cedar_type(self) -> &'static str {
        match self {
            Self::Namespace => "Namespace",
            Self::Table => "Table",
            Self::View => "View",
            Self::Column => "Column",
        }
    }
}

/// The asset an authorization decision is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzResource {
    /// Stable resource identifier. For a table this is typically the fully
    /// qualified name (`warehouse.namespace.table`) or its ULID — the
    /// caller decides, as long as it is stable and matches policy
    /// references. For a column, include the column so it is unique
    /// (`warehouse.ns.table#col`).
    pub id: String,
    /// Resource kind (drives the Cedar entity type).
    pub kind: ResourceKind,
    /// Tags on this resource (`pii:high`, `pii:email`, `finance`).
    /// Matched by `resource.tags.contains("…")` in policies. Tag
    /// propagation via lineage (Pillar F) happens upstream; by the time a
    /// resource reaches here its effective tag set is already resolved.
    pub tags: Vec<String>,
    /// The owning principal id, if known. Enables owner-allow policies
    /// (`resource.owner == principal.id`).
    pub owner: Option<String>,
    /// A coarse classification label, if assigned.
    pub classification: Option<String>,
    /// For a `Column` resource, the parent table/view's resource id — set
    /// so the column entity is linked to its parent in the Cedar entity
    /// graph (`resource in Table::"…"` style policies). Ignored for
    /// non-column kinds.
    pub parent: Option<String>,
    /// Open bag of extra attributes for org-specific policies.
    pub attributes: BTreeMap<String, Value>,
}

impl AuthzResource {
    /// A minimal resource with just an id and kind and no attributes.
    #[must_use]
    pub fn new(id: impl Into<String>, kind: ResourceKind) -> Self {
        Self {
            id: id.into(),
            kind,
            tags: Vec::new(),
            owner: None,
            classification: None,
            parent: None,
            attributes: BTreeMap::new(),
        }
    }

    /// Adds a tag (builder style).
    #[must_use]
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Sets the owner (builder style).
    #[must_use]
    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    /// Sets the classification (builder style).
    #[must_use]
    pub fn with_classification(mut self, classification: impl Into<String>) -> Self {
        self.classification = Some(classification.into());
        self
    }

    /// Sets the parent resource id (builder style, for columns).
    #[must_use]
    pub fn with_parent(mut self, parent: impl Into<String>) -> Self {
        self.parent = Some(parent.into());
        self
    }

    /// Whether this resource carries the given tag.
    #[must_use]
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }
}
