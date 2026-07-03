//! Catalog-as-code: the declarative bundle model behind `meridian plan` and
//! `meridian apply`.
//!
//! A bundle is a versioned YAML document that declares the *desired* state of
//! a Meridian catalog's control-plane objects — warehouses, namespaces, roles,
//! grants, and webhooks. `plan` diffs the desired state against the live server
//! (through the management APIs) and prints a create/update/noop/would-delete
//! report; `apply` reconciles the server toward the bundle with idempotent
//! creates and updates.
//!
//! # What is in scope and what is not
//!
//! The bundle owns *control-plane* objects — the things an administrator
//! provisions and a `GitOps` pipeline should track:
//!
//! - **warehouses** — storage roots and their non-secret storage options,
//! - **namespaces** — the logical containers, with properties,
//! - **roles** — RBAC roles,
//! - **grants** — privilege bindings (role/principal × privilege × securable),
//! - **webhooks** — event delivery endpoints.
//!
//! Tables and views are deliberately **out of scope**. Engines (Spark, Trino,
//! Flink, pyiceberg, dbt, …) own table and view lifecycles through the Iceberg
//! REST protocol: they create them, evolve their schemas, commit snapshots, and
//! drop them as part of data pipelines. A table's authoritative state is its
//! Iceberg metadata, which changes on every write — it is data, not
//! configuration. Trying to declare tables in a bundle would fight the engines
//! for ownership, and any snapshot committed between `plan` and `apply` would
//! make the plan wrong. The bundle stops at the boundary the catalog itself
//! draws: it provisions the *containers and policy*; engines fill them.
//!
//! # Secrets
//!
//! Bundle values support `${ENV_VAR}` interpolation so secrets never live in
//! the file. Webhook signing secrets and storage credentials are referenced by
//! environment variable and resolved at parse time; the file itself stays safe
//! to commit.

pub(crate) mod apply;
mod interpolate;
mod model;
pub(crate) mod plan;

pub(crate) use model::{Bundle, Grant, Namespace, Role, Warehouse, Webhook};

use std::fmt;

/// A bundle-processing failure with a human-readable message.
#[derive(Debug)]
pub(crate) struct BundleError(pub String);

impl fmt::Display for BundleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BundleError {}

impl BundleError {
    /// Builds a bundle error from anything displayable.
    pub(crate) fn msg(message: impl fmt::Display) -> Self {
        Self(message.to_string())
    }
}

/// The `apiVersion` every bundle must carry. Bumped only on a breaking change
/// to the schema; older documents keep working against the version they name.
pub(crate) const API_VERSION: &str = "meridian.dev/v1";

/// The `kind` every bundle must carry.
pub(crate) const KIND: &str = "CatalogBundle";

/// Parses a bundle from YAML text: deserializes, applies `${ENV_VAR}`
/// interpolation to every string value, then validates the header and the
/// cross-references between resources.
///
/// `resolve_env` looks up an environment variable by name; injectable so tests
/// need no real process environment.
pub(crate) fn parse(
    yaml: &str,
    resolve_env: &dyn Fn(&str) -> Option<String>,
) -> Result<Bundle, BundleError> {
    let mut raw: serde_yaml::Value = serde_yaml::from_str(yaml)
        .map_err(|e| BundleError::msg(format!("bundle is not valid YAML: {e}")))?;

    interpolate::interpolate_value(&mut raw, resolve_env)?;

    let bundle: Bundle = serde_yaml::from_value(raw)
        .map_err(|e| BundleError::msg(format!("bundle does not match the schema: {e}")))?;

    bundle.validate()?;
    Ok(bundle)
}

/// Reads and parses a bundle file, resolving `${ENV_VAR}` from the real
/// process environment.
pub(crate) fn load_file(path: &std::path::Path) -> Result<Bundle, BundleError> {
    let yaml = std::fs::read_to_string(path)
        .map_err(|e| BundleError::msg(format!("cannot read bundle {}: {e}", path.display())))?;
    parse(&yaml, &|name| std::env::var(name).ok())
}
