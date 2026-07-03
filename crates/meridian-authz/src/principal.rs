//! The principal and action inputs to an authorization decision.
//!
//! These are **this crate's own input types**, deliberately independent of
//! the store's persistence rows: the store (a later wave) maps its
//! `principals`/`grants`/`tags` rows onto these, and this crate never
//! depends on the store. The split is the documented type boundary — this
//! crate owns the *enforcement-decision* vocabulary; the store owns the
//! *persistence* vocabulary.
//!
//! A [`AuthzPrincipal`] carries exactly the attributes the ABAC model in
//! the spec (D-F1) needs to reason about: identity, kind, group/role
//! memberships, a declared *purpose*, an *environment* (dev/prod), plus an
//! open bag of extra attributes for org-specific policies. None of it is
//! authorization state — grants become Cedar *policies*, not principal
//! fields.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What kind of actor is requesting access.
///
/// Mirrors `meridian_common::principal::PrincipalKind` by intent but is
/// redeclared here so the crate stands alone; the store maps between them.
/// The kind becomes the Cedar principal **entity type** (`User`,
/// `Service`, `Agent`), so policies can be written against a whole class
/// (e.g. "forbid every `Agent` from reading `pii:high`").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// A human, authenticated through an IdP.
    User,
    /// A workload/service principal (engine, pipeline, CI job).
    Service,
    /// An AI agent principal (governed separately from services).
    Agent,
}

impl PrincipalKind {
    /// The Cedar entity type name for this kind.
    #[must_use]
    pub fn cedar_type(self) -> &'static str {
        match self {
            Self::User => "User",
            Self::Service => "Service",
            Self::Agent => "Agent",
        }
    }
}

/// The actor an authorization decision is about.
///
/// `id` is the stable principal identifier (the store's principal ULID, or
/// the OIDC subject — the caller decides, it only has to be stable and
/// match what policies reference). Everything else is an attribute Cedar
/// policies may read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzPrincipal {
    /// Stable principal identifier.
    pub id: String,
    /// Actor kind (drives the Cedar entity type).
    pub kind: PrincipalKind,
    /// Group memberships (e.g. `analysts`, `finance`). Matched by
    /// `principal.groups.contains("…")` in policies.
    pub groups: Vec<String>,
    /// Role memberships (RBAC roles surfaced to ABAC, e.g. `data_steward`).
    /// Kept distinct from groups so policies can distinguish an IdP group
    /// from a granted Meridian role.
    pub roles: Vec<String>,
    /// The purpose the principal has declared for this session, if any
    /// (purpose-based access, D-F1). Also surfaced through the request
    /// context; kept here too so purpose can be a stable principal
    /// attribute where a deployment binds purpose to the identity.
    pub purpose: Option<String>,
    /// The environment the principal operates in (`dev`, `staging`,
    /// `prod`), if the caller distinguishes them.
    pub environment: Option<String>,
    /// Open bag of extra string/number/bool attributes for org-specific
    /// policies (e.g. `region = "eu"`, `clearance = 3`). Serialized into
    /// the Cedar principal entity verbatim.
    pub attributes: BTreeMap<String, Value>,
}

impl AuthzPrincipal {
    /// A minimal principal with just an id and kind and no attributes.
    #[must_use]
    pub fn new(id: impl Into<String>, kind: PrincipalKind) -> Self {
        Self {
            id: id.into(),
            kind,
            groups: Vec::new(),
            roles: Vec::new(),
            purpose: None,
            environment: None,
            attributes: BTreeMap::new(),
        }
    }

    /// Adds a group membership (builder style, for tests and callers that
    /// assemble a principal inline).
    #[must_use]
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.groups.push(group.into());
        self
    }

    /// Adds a role membership (builder style).
    #[must_use]
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.push(role.into());
        self
    }

    /// Sets the declared purpose (builder style).
    #[must_use]
    pub fn with_purpose(mut self, purpose: impl Into<String>) -> Self {
        self.purpose = Some(purpose.into());
        self
    }

    /// Sets an extra attribute (builder style).
    #[must_use]
    pub fn with_attribute(mut self, key: impl Into<String>, value: Value) -> Self {
        self.attributes.insert(key.into(), value);
        self
    }
}

/// The action a principal is attempting.
///
/// A closed set of catalog verbs (mapped to Cedar `Action::"…"` entities).
/// A closed enum — rather than a free string — keeps policy authoring and
/// evaluation honest: an unknown verb is a type error, not a silent
/// always-deny. New verbs are added here deliberately as the catalog grows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Read table/view data or load metadata (the verb scan-planning and
    /// `loadTable` authorize).
    Read,
    /// Write/append/overwrite table data.
    Write,
    /// Commit new metadata (create/replace snapshot) — the write-path verb.
    Commit,
    /// Create an asset (table/view/namespace).
    Create,
    /// Drop an asset.
    Drop,
    /// Alter an asset's schema/properties.
    Alter,
    /// Manage grants/policies on an asset (delegated administration).
    Manage,
}

impl Action {
    /// The Cedar action id for this verb.
    #[must_use]
    pub fn cedar_id(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Commit => "commit",
            Self::Create => "create",
            Self::Drop => "drop",
            Self::Alter => "alter",
            Self::Manage => "manage",
        }
    }

    /// Every action verb, for schema generation and exhaustive tests.
    pub const ALL: [Self; 7] = [
        Self::Read,
        Self::Write,
        Self::Commit,
        Self::Create,
        Self::Drop,
        Self::Alter,
        Self::Manage,
    ];
}
