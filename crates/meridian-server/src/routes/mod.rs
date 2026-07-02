//! Route handlers, grouped by API surface.

pub mod health;
pub mod iceberg;
pub mod namespaces;
pub mod tables;
pub mod warehouses;

/// Principal recorded in audit rows while the API is pre-authentication.
///
/// TODO(M2, authn): replace with the authenticated principal from the
/// request context; anonymous access then becomes a policy decision instead
/// of the only option.
pub(crate) const ANONYMOUS_PRINCIPAL: &str = "anonymous";
