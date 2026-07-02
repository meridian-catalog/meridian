//! Route handlers, grouped by API surface.
//!
//! Every handler that mutates state reads the caller's
//! [`meridian_common::principal::Principal`] from the request extensions
//! (established by `crate::auth`) and records its `audit_string()` in the
//! audit log. In `auth.mode = "disabled"` deployments the middleware
//! inserts the anonymous principal, whose audit string is `"anonymous"` —
//! identical to the pre-authentication behavior.

pub mod grants;
pub mod health;
pub mod iceberg;
pub mod namespaces;
pub mod principals;
pub mod tables;
pub mod views;
pub mod warehouses;
