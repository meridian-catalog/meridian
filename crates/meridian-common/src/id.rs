//! Typed, ULID-backed identifiers.
//!
//! Every entity gets its own newtype so IDs cannot be mixed up at compile
//! time. IDs serialize as their canonical 26-character Crockford base32 ULID
//! string, which is also how they are stored in Postgres (`TEXT` columns).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Error returned when parsing a typed ID from a string fails.
#[derive(Debug, thiserror::Error)]
#[error("invalid {kind} id {value:?}: {source}")]
pub struct ParseIdError {
    kind: &'static str,
    value: String,
    #[source]
    source: ulid::DecodeError,
}

macro_rules! define_id {
    ($(#[$doc:meta])* $name:ident, $kind:literal) => {
        $(#[$doc])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Ulid);

        impl $name {
            /// Generates a new random, time-ordered ID.
            #[must_use]
            pub fn new() -> Self {
                Self(Ulid::new())
            }

            /// Wraps an existing ULID.
            #[must_use]
            pub const fn from_ulid(ulid: Ulid) -> Self {
                Self(ulid)
            }

            /// Returns the underlying ULID.
            #[must_use]
            pub const fn as_ulid(&self) -> Ulid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl FromStr for $name {
            type Err = ParseIdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ulid::from_str(s).map(Self).map_err(|source| ParseIdError {
                    kind: $kind,
                    value: s.to_owned(),
                    source,
                })
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> Self {
                id.to_string()
            }
        }
    };
}

define_id!(
    /// Identifier of an organization (top-level tenant).
    OrgId,
    "organization"
);
define_id!(
    /// Identifier of a workspace within an organization.
    WorkspaceId,
    "workspace"
);
define_id!(
    /// Identifier of a catalog within a workspace.
    CatalogId,
    "catalog"
);
define_id!(
    /// Identifier of a namespace within a catalog.
    NamespaceId,
    "namespace"
);
define_id!(
    /// Identifier of a table within a namespace.
    TableId,
    "table"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_string() {
        let id = TableId::new();
        let s = id.to_string();
        assert_eq!(s.len(), 26);
        let parsed: TableId = s.parse().expect("valid ULID string");
        assert_eq!(parsed, id);
    }

    #[test]
    fn serializes_as_plain_string() {
        let id = OrgId::new();
        let json = serde_json::to_string(&id).expect("serialize");
        assert_eq!(json, format!("\"{id}\""));
        let back: OrgId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, id);
    }

    #[test]
    fn rejects_invalid_input() {
        let err = "not-a-ulid".parse::<WorkspaceId>().unwrap_err();
        assert!(err.to_string().contains("workspace"));
    }
}
