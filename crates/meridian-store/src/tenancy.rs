//! Single-workspace tenancy constants.
//!
//! The OSS deployment runs with one implicit tenant: organization `default`
//! containing workspace `default`, both seeded by migration 0002 under fixed,
//! well-known ULIDs. Multi-tenant resolution (per-request workspace lookup)
//! replaces these constants when authentication lands.

use std::str::FromStr;

use meridian_common::id::{OrgId, WorkspaceId};

/// Fixed ULID of the seeded `default` organization (`Ulid(0)`).
pub const DEFAULT_ORG_ID: &str = "00000000000000000000000000";

/// Fixed ULID of the seeded `default` workspace (`Ulid(1)`).
pub const DEFAULT_WORKSPACE_ID: &str = "00000000000000000000000001";

/// The seeded default organization ID as a typed [`OrgId`].
#[must_use]
pub fn default_org_id() -> OrgId {
    // The constant is a valid ULID by construction; a parse failure would be
    // a compile-time-style defect caught by the unit test below.
    OrgId::from_str(DEFAULT_ORG_ID).unwrap_or_else(|_| OrgId::from_ulid(ulid::Ulid(0)))
}

/// The seeded default workspace ID as a typed [`WorkspaceId`].
#[must_use]
pub fn default_workspace_id() -> WorkspaceId {
    WorkspaceId::from_str(DEFAULT_WORKSPACE_ID)
        .unwrap_or_else(|_| WorkspaceId::from_ulid(ulid::Ulid(1)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_are_valid_ulids() {
        assert_eq!(default_org_id().to_string(), DEFAULT_ORG_ID);
        assert_eq!(default_workspace_id().to_string(), DEFAULT_WORKSPACE_ID);
        assert_eq!(default_org_id().as_ulid(), ulid::Ulid(0));
        assert_eq!(default_workspace_id().as_ulid(), ulid::Ulid(1));
    }
}
