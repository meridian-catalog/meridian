//! Route handlers, grouped by API surface.
//!
//! Every handler that mutates state reads the caller's
//! [`meridian_common::principal::Principal`] from the request extensions
//! (established by `crate::auth`) and records its `audit_string()` in the
//! audit log. In `auth.mode = "disabled"` deployments the middleware
//! inserts the anonymous principal, whose audit string is `"anonymous"` —
//! identical to the pre-authentication behavior.

pub mod events;
pub mod grants;
pub mod health;
pub mod iceberg;
pub mod namespaces;
pub mod principals;
pub mod search;
pub mod tables;
pub mod vending;
pub mod views;
pub mod warehouses;
