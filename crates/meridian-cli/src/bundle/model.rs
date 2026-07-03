//! The bundle schema: the Rust types a `CatalogBundle` YAML document
//! deserializes into, plus structural validation.
//!
//! # Schema (apiVersion `meridian.dev/v1`, kind `CatalogBundle`)
//!
//! ```yaml
//! apiVersion: meridian.dev/v1
//! kind: CatalogBundle
//!
//! warehouses:
//!   - name: analytics
//!     storage_root: s3://acme-lake/analytics
//!     storage_options:
//!       region: us-east-1
//!       endpoint: https://s3.us-east-1.amazonaws.com
//!
//! namespaces:
//!   - warehouse: analytics
//!     levels: [sales, emea]        # multi-level namespace
//!     properties:
//!       owner: data-platform
//!
//! roles:
//!   - name: analyst
//!     description: Read-only analytics access
//!
//! grants:
//!   - role: analyst               # exactly one of role / principal
//!     privilege: READ
//!     securable:
//!       type: namespace           # warehouse | namespace | table | view
//!       warehouse: analytics
//!       namespace: [sales, emea]
//!
//! webhooks:
//!   - url: https://hooks.acme.example/meridian
//!     event_types: [com.meridian.table.committed]
//!     secret: ${MERIDIAN_WEBHOOK_SECRET}   # env-interpolated
//! ```
//!
//! All top-level resource lists are optional; a bundle may declare any subset.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::{API_VERSION, BundleError, KIND};

/// A parsed and validated catalog bundle.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Bundle {
    /// Schema version; must equal [`API_VERSION`].
    #[serde(rename = "apiVersion")]
    pub(crate) api_version: String,
    /// Document kind; must equal [`KIND`].
    pub(crate) kind: String,

    /// Declared warehouses.
    #[serde(default)]
    pub(crate) warehouses: Vec<Warehouse>,
    /// Declared namespaces.
    #[serde(default)]
    pub(crate) namespaces: Vec<Namespace>,
    /// Declared roles.
    #[serde(default)]
    pub(crate) roles: Vec<Role>,
    /// Declared grants.
    #[serde(default)]
    pub(crate) grants: Vec<Grant>,
    /// Declared webhooks.
    #[serde(default)]
    pub(crate) webhooks: Vec<Webhook>,
}

/// A warehouse: a storage root plus non-secret storage options. The name is
/// the natural key (it doubles as the Iceberg REST prefix).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Warehouse {
    /// Warehouse name (natural key).
    pub(crate) name: String,
    /// Storage root URI, e.g. `s3://bucket/prefix`.
    pub(crate) storage_root: String,
    /// Storage options (region, endpoint, and — via `${ENV}` — credentials).
    #[serde(default)]
    pub(crate) storage_options: BTreeMap<String, String>,
}

/// A namespace within a warehouse. `(warehouse, levels)` is the natural key.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Namespace {
    /// Warehouse this namespace belongs to (must be declared or already exist).
    pub(crate) warehouse: String,
    /// Namespace levels, outermost first (e.g. `[sales, emea]`).
    pub(crate) levels: Vec<String>,
    /// String properties.
    #[serde(default)]
    pub(crate) properties: BTreeMap<String, String>,
}

impl Namespace {
    /// The dotted rendering of the levels, for display and diagnostics.
    pub(crate) fn dotted(&self) -> String {
        self.levels.join(".")
    }
}

/// An RBAC role. The name is the natural key.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Role {
    /// Role name (natural key).
    pub(crate) name: String,
    /// Optional human description.
    #[serde(default)]
    pub(crate) description: Option<String>,
}

/// A privilege binding. The grantee is exactly one of `role` / `principal`.
/// The natural key is the whole tuple (grantee, privilege, securable): a grant
/// either exists or it does not — it has no mutable fields.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Grant {
    /// Grantee role name (mutually exclusive with `principal`).
    #[serde(default)]
    pub(crate) role: Option<String>,
    /// Grantee principal id (mutually exclusive with `role`).
    #[serde(default)]
    pub(crate) principal: Option<String>,
    /// Privilege to grant, e.g. `READ`, `COMMIT`, `CREATE_TABLE`.
    pub(crate) privilege: String,
    /// What the grant attaches to.
    pub(crate) securable: Securable,
}

/// The securable a grant targets.
// `securable_type` repeats the struct name, but `type` is a Rust keyword and
// this field mirrors the server's `SecurableSelector` wire shape verbatim.
#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Securable {
    /// `warehouse` | `namespace` | `table` | `view`.
    #[serde(rename = "type")]
    pub(crate) securable_type: String,
    /// Warehouse name (always required).
    pub(crate) warehouse: String,
    /// Namespace levels (required for namespace/table/view).
    #[serde(default)]
    pub(crate) namespace: Option<Vec<String>>,
    /// Table name (required for `table`).
    #[serde(default)]
    pub(crate) table: Option<String>,
    /// View name (required for `view`).
    #[serde(default)]
    pub(crate) view: Option<String>,
}

/// A webhook delivery endpoint. `(url, sorted event_types)` is the natural key
/// — the server assigns the id, so the bundle identifies an endpoint by what
/// it delivers where.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Webhook {
    /// Destination URL (`http://` or `https://`).
    pub(crate) url: String,
    /// `CloudEvents` type filter; empty = all events.
    #[serde(default)]
    pub(crate) event_types: Vec<String>,
    /// HMAC signing secret (typically `${ENV_VAR}`). Write-only on the server.
    pub(crate) secret: String,
}

impl Bundle {
    /// Validates the header and the structural/cross-reference invariants that
    /// must hold before any server interaction. This is *offline* validation:
    /// it does not check whether referenced objects exist on the server (that
    /// is the planner's job), only that the document is internally coherent.
    pub(crate) fn validate(&self) -> Result<(), BundleError> {
        if self.api_version != API_VERSION {
            return Err(BundleError::msg(format!(
                "unsupported apiVersion {:?}: expected {API_VERSION:?}",
                self.api_version
            )));
        }
        if self.kind != KIND {
            return Err(BundleError::msg(format!(
                "unsupported kind {:?}: expected {KIND:?}",
                self.kind
            )));
        }

        self.validate_warehouses()?;
        self.validate_namespaces()?;
        self.validate_roles()?;
        self.validate_grants()?;
        self.validate_webhooks()?;
        Ok(())
    }

    fn validate_warehouses(&self) -> Result<(), BundleError> {
        let mut seen = std::collections::BTreeSet::new();
        for wh in &self.warehouses {
            if wh.name.trim().is_empty() {
                return Err(BundleError::msg("warehouse name must not be empty"));
            }
            if wh.storage_root.trim().is_empty() {
                return Err(BundleError::msg(format!(
                    "warehouse {:?}: storage_root must not be empty",
                    wh.name
                )));
            }
            if !seen.insert(wh.name.as_str()) {
                return Err(BundleError::msg(format!(
                    "warehouse {:?} is declared more than once",
                    wh.name
                )));
            }
        }
        Ok(())
    }

    fn validate_namespaces(&self) -> Result<(), BundleError> {
        let mut seen = std::collections::BTreeSet::new();
        for ns in &self.namespaces {
            if ns.levels.is_empty() {
                return Err(BundleError::msg("namespace must have at least one level"));
            }
            if ns.levels.iter().any(|l| l.trim().is_empty()) {
                return Err(BundleError::msg(format!(
                    "namespace {:?} in warehouse {:?}: levels must be non-empty",
                    ns.dotted(),
                    ns.warehouse
                )));
            }
            // The warehouse a namespace lives in is checked for existence at
            // plan time (it may be declared in this bundle or already on the
            // server); here we only reject an empty reference.
            if ns.warehouse.trim().is_empty() {
                return Err(BundleError::msg(format!(
                    "namespace {:?}: warehouse must not be empty",
                    ns.dotted()
                )));
            }
            let key = (ns.warehouse.as_str(), ns.levels.clone());
            if !seen.insert(key) {
                return Err(BundleError::msg(format!(
                    "namespace {:?} in warehouse {:?} is declared more than once",
                    ns.dotted(),
                    ns.warehouse
                )));
            }
        }
        Ok(())
    }

    fn validate_roles(&self) -> Result<(), BundleError> {
        let mut seen = std::collections::BTreeSet::new();
        for role in &self.roles {
            if role.name.trim().is_empty() {
                return Err(BundleError::msg("role name must not be empty"));
            }
            if !seen.insert(role.name.as_str()) {
                return Err(BundleError::msg(format!(
                    "role {:?} is declared more than once",
                    role.name
                )));
            }
        }
        Ok(())
    }

    fn validate_grants(&self) -> Result<(), BundleError> {
        for grant in &self.grants {
            match (&grant.role, &grant.principal) {
                (Some(_), None) | (None, Some(_)) => {}
                (Some(_), Some(_)) => {
                    return Err(BundleError::msg(
                        "grant must set exactly one of role / principal, not both",
                    ));
                }
                (None, None) => {
                    return Err(BundleError::msg(
                        "grant must set exactly one of role / principal",
                    ));
                }
            }
            if grant.privilege.trim().is_empty() {
                return Err(BundleError::msg("grant privilege must not be empty"));
            }
            validate_securable(&grant.securable)?;
        }
        Ok(())
    }

    fn validate_webhooks(&self) -> Result<(), BundleError> {
        for hook in &self.webhooks {
            if !(hook.url.starts_with("http://") || hook.url.starts_with("https://")) {
                return Err(BundleError::msg(format!(
                    "webhook url {:?} must start with http:// or https://",
                    hook.url
                )));
            }
            if hook.secret.trim().is_empty() {
                return Err(BundleError::msg(format!(
                    "webhook {:?}: secret must not be empty (use ${{ENV_VAR}} to source it)",
                    hook.url
                )));
            }
        }
        Ok(())
    }
}

/// Validates a securable selector's required fields per type.
fn validate_securable(securable: &Securable) -> Result<(), BundleError> {
    if securable.warehouse.trim().is_empty() {
        return Err(BundleError::msg("securable.warehouse must not be empty"));
    }
    match securable.securable_type.as_str() {
        "warehouse" => {}
        "namespace" => require_namespace(securable)?,
        "table" => {
            require_namespace(securable)?;
            if securable.table.as_deref().unwrap_or("").trim().is_empty() {
                return Err(BundleError::msg(
                    "securable.table is required for type \"table\"",
                ));
            }
        }
        "view" => {
            require_namespace(securable)?;
            if securable.view.as_deref().unwrap_or("").trim().is_empty() {
                return Err(BundleError::msg(
                    "securable.view is required for type \"view\"",
                ));
            }
        }
        other => {
            return Err(BundleError::msg(format!(
                "invalid securable type {other:?}: expected warehouse, namespace, table, or view"
            )));
        }
    }
    Ok(())
}

fn require_namespace(securable: &Securable) -> Result<(), BundleError> {
    match &securable.namespace {
        Some(levels) if !levels.is_empty() && levels.iter().all(|l| !l.trim().is_empty()) => Ok(()),
        _ => Err(BundleError::msg(format!(
            "securable.namespace is required and must be non-empty for type {:?}",
            securable.securable_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::super::parse;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn parses_minimal_bundle() {
        let yaml = "apiVersion: meridian.dev/v1\nkind: CatalogBundle\n";
        let bundle = parse(yaml, &no_env).expect("valid");
        assert!(bundle.warehouses.is_empty());
    }

    #[test]
    fn rejects_wrong_api_version() {
        let yaml = "apiVersion: meridian.dev/v2\nkind: CatalogBundle\n";
        assert!(parse(yaml, &no_env).is_err());
    }

    #[test]
    fn rejects_wrong_kind() {
        let yaml = "apiVersion: meridian.dev/v1\nkind: Nope\n";
        assert!(parse(yaml, &no_env).is_err());
    }

    #[test]
    fn rejects_unknown_field() {
        let yaml = "apiVersion: meridian.dev/v1\nkind: CatalogBundle\nbogus: 1\n";
        assert!(parse(yaml, &no_env).is_err());
    }

    #[test]
    fn parses_full_bundle_and_env() {
        let yaml = r"
apiVersion: meridian.dev/v1
kind: CatalogBundle
warehouses:
  - name: analytics
    storage_root: s3://lake/analytics
    storage_options:
      region: us-east-1
namespaces:
  - warehouse: analytics
    levels: [sales, emea]
    properties:
      owner: data-platform
roles:
  - name: analyst
    description: read only
grants:
  - role: analyst
    privilege: READ
    securable:
      type: namespace
      warehouse: analytics
      namespace: [sales, emea]
webhooks:
  - url: https://hooks.example/mrd
    event_types: [com.meridian.table.committed]
    secret: ${HOOK_SECRET}
";
        let bundle = parse(yaml, &|n| {
            (n == "HOOK_SECRET").then(|| "topsecret".to_owned())
        })
        .expect("valid");
        assert_eq!(bundle.warehouses.len(), 1);
        assert_eq!(bundle.namespaces[0].dotted(), "sales.emea");
        assert_eq!(bundle.webhooks[0].secret, "topsecret");
    }

    #[test]
    fn rejects_grant_with_both_grantees() {
        let yaml = r"
apiVersion: meridian.dev/v1
kind: CatalogBundle
grants:
  - role: a
    principal: p
    privilege: READ
    securable: { type: warehouse, warehouse: w }
";
        assert!(parse(yaml, &no_env).is_err());
    }

    #[test]
    fn rejects_grant_with_no_grantee() {
        let yaml = r"
apiVersion: meridian.dev/v1
kind: CatalogBundle
grants:
  - privilege: READ
    securable: { type: warehouse, warehouse: w }
";
        assert!(parse(yaml, &no_env).is_err());
    }

    #[test]
    fn rejects_namespace_securable_without_levels() {
        let yaml = r"
apiVersion: meridian.dev/v1
kind: CatalogBundle
grants:
  - role: a
    privilege: READ
    securable: { type: namespace, warehouse: w }
";
        assert!(parse(yaml, &no_env).is_err());
    }

    #[test]
    fn rejects_duplicate_warehouse() {
        let yaml = r"
apiVersion: meridian.dev/v1
kind: CatalogBundle
warehouses:
  - name: dup
    storage_root: s3://a
  - name: dup
    storage_root: s3://b
";
        assert!(parse(yaml, &no_env).is_err());
    }

    #[test]
    fn rejects_missing_webhook_secret_env() {
        let yaml = r"
apiVersion: meridian.dev/v1
kind: CatalogBundle
webhooks:
  - url: https://h.example/x
    secret: ${NOT_SET}
";
        assert!(parse(yaml, &no_env).is_err());
    }

    #[test]
    fn rejects_bad_webhook_url() {
        let yaml = r"
apiVersion: meridian.dev/v1
kind: CatalogBundle
webhooks:
  - url: ftp://h.example/x
    secret: ${S}
";
        assert!(parse(yaml, &|_| Some("x".to_owned())).is_err());
    }
}
