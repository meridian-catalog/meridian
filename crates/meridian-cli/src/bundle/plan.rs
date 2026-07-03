//! Diffing a desired bundle against a live server, and rendering the plan.
//!
//! The planner reads current server state through the management/IRC APIs and
//! produces a flat list of [`Action`]s — one per declared resource — each
//! tagged with an [`Op`]. `meridian plan` prints them; `meridian apply`
//! executes the non-noop ones.
//!
//! # Reconciliation model
//!
//! `apply` is *converge-forward only*: it creates what is missing and updates
//! what has drifted, but it never deletes. Resources that exist on the server
//! but are absent from the bundle surface as [`Op::WouldDelete`] warnings and
//! are never acted on — pruning is out of scope for v1 because a bundle is
//! rarely the whole truth (engines create namespaces, operators create ad-hoc
//! grants), and an accidental prune of a production warehouse is unrecoverable.
//!
//! Some resources have no update path in the API:
//!
//! - a **warehouse**'s storage root and options are fixed after creation,
//! - a **role**'s description is fixed after creation.
//!
//! When the bundle asks to change one of these, the planner emits
//! [`Op::WouldUpdateUnsupported`]: it reports the drift honestly but does not
//! fail the apply, because there is no API to reconcile it. Namespace
//! properties *can* be updated, so namespace drift is a real [`Op::Update`].

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::model::{Bundle, Namespace, Role, Warehouse, Webhook};
use super::{BundleError, Grant};
use crate::client::{self, CliError};

/// One planned operation against one resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Action {
    /// The operation to perform (or that would be performed).
    pub(crate) op: Op,
    /// Resource kind, for grouping/display (`warehouse`, `namespace`, ...).
    pub(crate) kind: &'static str,
    /// Human identifier of the resource within its kind.
    pub(crate) identity: String,
    /// Extra human detail (what changes, why unsupported, ...).
    pub(crate) detail: String,
}

/// The disposition of an [`Action`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    /// Resource is absent on the server and will be created.
    Create,
    /// Resource exists but has drifted and will be updated.
    Update,
    /// Resource exists and matches; nothing to do.
    Noop,
    /// Resource has drifted but the API offers no way to update it in place;
    /// reported, never applied.
    WouldUpdateUnsupported,
    /// Resource exists on the server but is not in the bundle; reported as a
    /// warning, never deleted (prune is out of scope for v1).
    WouldDelete,
}

impl Op {
    /// The short tag printed in the plan.
    pub(crate) fn tag(self) -> &'static str {
        match self {
            Op::Create => "create",
            Op::Update => "update",
            Op::Noop => "noop",
            Op::WouldUpdateUnsupported => "would-update",
            Op::WouldDelete => "would-delete",
        }
    }
}

/// The outcome of planning: the ordered actions plus resolved ids the applier
/// reuses (so it need not re-resolve securables/warehouses).
pub(crate) struct Plan {
    /// All actions, in a stable resource order.
    pub(crate) actions: Vec<Action>,
}

/// A read-only snapshot of the server state relevant to a bundle.
///
/// Gathered once up front so the diff is computed against a consistent view and
/// the applier can reuse resolved ids.
pub(crate) struct ServerState {
    /// Warehouse name -> its JSON as returned by the management API.
    pub(crate) warehouses: BTreeMap<String, Value>,
    /// Role name -> its JSON.
    pub(crate) roles: BTreeMap<String, Value>,
    /// All grant tuples present on the server, as `(privilege, grantee,
    /// securable_type, securable_id)`.
    pub(crate) grants: BTreeSet<GrantTuple>,
    /// Webhook natural keys present on the server: `(url, sorted event_types)`.
    pub(crate) webhooks: BTreeSet<(String, Vec<String>)>,
}

/// The identity of a grant on the server, independent of its ULID.
pub(crate) type GrantTuple = (String, Grantee, String, String);

/// A resolved grantee: a role name or a principal id.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Grantee {
    /// A role, by name.
    Role(String),
    /// A principal, by id.
    Principal(String),
}

/// Loads the server-side state a bundle plan needs. `warehouses` is limited to
/// those the bundle references (declared warehouses plus warehouses named by
/// namespaces/grants), so we do not enumerate namespaces of unrelated
/// warehouses.
pub(crate) async fn load_server_state(
    server: &str,
    token: Option<&str>,
) -> Result<ServerState, CliError> {
    let wh_body = client::warehouse_list(server, token).await?;
    let mut warehouses = BTreeMap::new();
    if let Some(list) = wh_body.get("warehouses").and_then(Value::as_array) {
        for wh in list {
            if let Some(name) = wh.get("name").and_then(Value::as_str) {
                warehouses.insert(name.to_owned(), wh.clone());
            }
        }
    }

    let role_body = client::role_list(server, token).await?;
    let mut roles = BTreeMap::new();
    if let Some(list) = role_body.get("roles").and_then(Value::as_array) {
        for role in list {
            if let Some(name) = role.get("name").and_then(Value::as_str) {
                roles.insert(name.to_owned(), role.clone());
            }
        }
    }

    let grant_body = client::grant_list(server, token).await?;
    let mut grants = BTreeSet::new();
    if let Some(list) = grant_body.get("grants").and_then(Value::as_array) {
        for grant in list {
            if let Some(tuple) = grant_tuple(grant) {
                grants.insert(tuple);
            }
        }
    }

    let hook_body = client::webhook_list(server, token).await?;
    let mut webhooks = BTreeSet::new();
    if let Some(list) = hook_body.get("webhooks").and_then(Value::as_array) {
        for hook in list {
            if let Some(url) = hook.get("url").and_then(Value::as_str) {
                let mut types: Vec<String> = hook
                    .get("event_types")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                types.sort();
                webhooks.insert((url.to_owned(), types));
            }
        }
    }

    Ok(ServerState {
        warehouses,
        roles,
        grants,
        webhooks,
    })
}

/// Extracts the identity tuple from a server grant JSON.
fn grant_tuple(grant: &Value) -> Option<GrantTuple> {
    let privilege = grant.get("privilege").and_then(Value::as_str)?.to_owned();
    let securable_type = grant
        .get("securable_type")
        .and_then(Value::as_str)?
        .to_owned();
    let securable_id = grant
        .get("securable_id")
        .and_then(Value::as_str)?
        .to_owned();
    let grantee = if let Some(role) = grant.get("role").and_then(Value::as_str) {
        Grantee::Role(role.to_owned())
    } else if let Some(pid) = grant.get("principal_id").and_then(Value::as_str) {
        Grantee::Principal(pid.to_owned())
    } else {
        return None;
    };
    Some((privilege, grantee, securable_type, securable_id))
}

/// Computes the plan for a bundle against a server. Resolves grant securables
/// against the live catalog (a grant's securable must already exist — either
/// declared earlier in the bundle and created by a prior apply, or present on
/// the server).
pub(crate) async fn compute(
    bundle: &Bundle,
    server: &str,
    token: Option<&str>,
    state: &ServerState,
) -> Result<Plan, BundleError> {
    let mut actions = Vec::new();

    for warehouse in &bundle.warehouses {
        actions.push(diff_warehouse(warehouse, state));
    }

    for namespace in &bundle.namespaces {
        actions.push(diff_namespace(namespace, server, token, state).await?);
    }

    for role in &bundle.roles {
        actions.push(diff_role(role, state));
    }

    for grant in &bundle.grants {
        actions.push(diff_grant(grant, state)?);
    }

    for webhook in &bundle.webhooks {
        actions.push(diff_webhook(webhook, state));
    }

    // Prune warnings: server resources absent from the bundle. Only for the
    // kinds a bundle fully owns by natural key and that we can enumerate
    // cheaply — warehouses, roles, webhooks. Namespaces and grants are left
    // out on purpose: engines and operators legitimately create them outside
    // the bundle, so a would-delete there would be noise, not signal.
    append_prune_warnings(bundle, state, &mut actions);

    Ok(Plan { actions })
}

/// Diffs one warehouse. No update API: drift is reported, not reconciled.
fn diff_warehouse(warehouse: &Warehouse, state: &ServerState) -> Action {
    let kind = "warehouse";
    match state.warehouses.get(&warehouse.name) {
        None => Action {
            op: Op::Create,
            kind,
            identity: warehouse.name.clone(),
            detail: format!("storage_root {}", warehouse.storage_root),
        },
        Some(current) => {
            let current_root = current
                .get("storage_root")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let root_drift = current_root != warehouse.storage_root;
            // Non-secret option drift only: secret values come back redacted
            // (`***`) and cannot be compared, so we ignore any option whose
            // server value is redacted.
            let option_drift = warehouse_option_drift(warehouse, current);
            if root_drift || option_drift {
                let mut reasons = Vec::new();
                if root_drift {
                    reasons.push(format!(
                        "storage_root {current_root:?} -> {:?}",
                        warehouse.storage_root
                    ));
                }
                if option_drift {
                    reasons.push("storage_options differ".to_owned());
                }
                Action {
                    op: Op::WouldUpdateUnsupported,
                    kind,
                    identity: warehouse.name.clone(),
                    detail: format!(
                        "{} (warehouses are immutable after creation; recreate to change)",
                        reasons.join("; ")
                    ),
                }
            } else {
                Action {
                    op: Op::Noop,
                    kind,
                    identity: warehouse.name.clone(),
                    detail: String::new(),
                }
            }
        }
    }
}

/// True if any non-secret declared option differs from the server value.
fn warehouse_option_drift(warehouse: &Warehouse, current: &Value) -> bool {
    let current_opts = current.get("storage_options").and_then(Value::as_object);
    for (key, want) in &warehouse.storage_options {
        let have = current_opts
            .and_then(|o| o.get(key))
            .and_then(Value::as_str);
        match have {
            // Redacted secret: cannot compare, assume in sync.
            Some("***") => {}
            Some(value) if value == want => {}
            _ => return true,
        }
    }
    false
}

/// Diffs one namespace. Namespaces support property updates, so drift here is
/// a real, reconcilable update.
async fn diff_namespace(
    namespace: &Namespace,
    server: &str,
    token: Option<&str>,
    state: &ServerState,
) -> Result<Action, BundleError> {
    let kind = "namespace";
    let identity = format!("{}/{}", namespace.warehouse, namespace.dotted());

    // The warehouse must exist (declared in the bundle or already on the
    // server) for the namespace to be placed.
    if !state.warehouses.contains_key(&namespace.warehouse) {
        // Warehouse will be created earlier in this same apply; treat the
        // namespace as a create against a to-be-created warehouse.
        return Ok(Action {
            op: Op::Create,
            kind,
            identity,
            detail: format!("in warehouse {} (to be created)", namespace.warehouse),
        });
    }

    let current = client::namespace_load(server, token, &namespace.warehouse, &namespace.levels)
        .await
        .map_err(BundleError::msg)?;

    match current {
        None => Ok(Action {
            op: Op::Create,
            kind,
            identity,
            detail: if namespace.properties.is_empty() {
                String::new()
            } else {
                format!("{} properties", namespace.properties.len())
            },
        }),
        Some(body) => {
            let current_props = body
                .get("properties")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let (updates, _removals) = property_delta(&namespace.properties, &current_props);
            if updates.is_empty() {
                Ok(Action {
                    op: Op::Noop,
                    kind,
                    identity,
                    detail: String::new(),
                })
            } else {
                let keys: Vec<&str> = updates.iter().map(|(k, _)| k.as_str()).collect();
                Ok(Action {
                    op: Op::Update,
                    kind,
                    identity,
                    detail: format!("set properties: {}", keys.join(", ")),
                })
            }
        }
    }
}

/// Computes the property updates needed to bring the server toward the desired
/// set. Only *additions and changes* are returned as `updates`; the bundle is
/// additive over properties and never removes keys it does not mention, so
/// `removals` is always empty (returned for symmetry/future use).
///
/// This matches the reconciliation model: the bundle declares the properties
/// it cares about; operator- or engine-set properties it does not mention are
/// left alone, exactly as unmanaged namespaces and grants are.
fn property_delta(
    desired: &BTreeMap<String, String>,
    current: &serde_json::Map<String, Value>,
) -> (Vec<(String, String)>, Vec<String>) {
    let mut updates = Vec::new();
    for (key, want) in desired {
        let have = current.get(key).and_then(Value::as_str);
        if have != Some(want.as_str()) {
            updates.push((key.clone(), want.clone()));
        }
    }
    (updates, Vec::new())
}

/// Diffs one role. No update API: description drift is reported, not
/// reconciled.
fn diff_role(role: &Role, state: &ServerState) -> Action {
    let kind = "role";
    match state.roles.get(&role.name) {
        None => Action {
            op: Op::Create,
            kind,
            identity: role.name.clone(),
            detail: role
                .description
                .clone()
                .map(|d| format!("description: {d}"))
                .unwrap_or_default(),
        },
        Some(current) => {
            let current_desc = current.get("description").and_then(Value::as_str);
            let want_desc = role.description.as_deref();
            // Only report drift when the bundle actually specifies a
            // description; a bundle that omits it does not manage it.
            if want_desc.is_some() && current_desc != want_desc {
                Action {
                    op: Op::WouldUpdateUnsupported,
                    kind,
                    identity: role.name.clone(),
                    detail: format!(
                        "description {:?} -> {:?} (roles have no update API; \
                         recreate to change the description)",
                        current_desc.unwrap_or(""),
                        want_desc.unwrap_or("")
                    ),
                }
            } else {
                Action {
                    op: Op::Noop,
                    kind,
                    identity: role.name.clone(),
                    detail: String::new(),
                }
            }
        }
    }
}

/// Diffs one grant. Compares against server grants precisely for
/// warehouse-scoped securables (the only ones whose id we can pre-resolve);
/// namespace/table/view securables are planned as an idempotent create (see
/// the in-body comment for why).
fn diff_grant(grant: &Grant, state: &ServerState) -> Result<Action, BundleError> {
    let kind = "grant";
    let grantee = match (&grant.role, &grant.principal) {
        (Some(role), None) => Grantee::Role(role.clone()),
        (None, Some(pid)) => Grantee::Principal(pid.clone()),
        _ => {
            return Err(BundleError::msg(
                "grant must set exactly one of role / principal",
            ));
        }
    };
    let identity = format!(
        "{} {} on {}",
        grant.privilege,
        display_grantee(&grantee),
        display_securable(grant),
    );

    // Grant identity on the server is `(privilege, grantee, securable_type,
    // securable_id)`. We can pre-resolve the securable id *precisely* only for
    // warehouse securables — the warehouse list carries the id. Namespace,
    // table, and view ids are not exposed by any read endpoint (the IRC load
    // returns properties, not the ULID), so for those we cannot compare
    // against `grant_list` at plan time. We therefore plan them as an
    // idempotent create: `apply` POSTs the selector, the server resolves and
    // deduplicates, and an identical existing grant comes back as a conflict
    // that the applier treats as a no-op. Re-apply never creates a duplicate
    // and never fails; the only cost is that `plan` shows such a grant as
    // "create" even when it already exists.
    match warehouse_securable_id(grant, state) {
        SecurableId::Warehouse(id) => {
            let tuple: GrantTuple = (
                grant.privilege.clone(),
                grantee,
                grant.securable.securable_type.clone(),
                id,
            );
            let op = if state.grants.contains(&tuple) {
                Op::Noop
            } else {
                Op::Create
            };
            Ok(Action {
                op,
                kind,
                identity,
                detail: String::new(),
            })
        }
        SecurableId::Unresolvable => Ok(Action {
            op: Op::Create,
            kind,
            identity,
            detail: "idempotent: an existing identical grant is a no-op on apply".to_owned(),
        }),
    }
}

/// The precise securable id for a grant, when resolvable.
enum SecurableId {
    /// A warehouse securable, resolved to its id.
    Warehouse(String),
    /// A namespace/table/view securable whose id cannot be pre-resolved.
    Unresolvable,
}

/// Resolves a warehouse-scoped grant to the warehouse id; everything else is
/// [`SecurableId::Unresolvable`] (see [`diff_grant`] for why).
fn warehouse_securable_id(grant: &Grant, state: &ServerState) -> SecurableId {
    let sec = &grant.securable;
    if sec.securable_type != "warehouse" {
        return SecurableId::Unresolvable;
    }
    match state
        .warehouses
        .get(&sec.warehouse)
        .and_then(|w| w.get("id"))
        .and_then(Value::as_str)
    {
        Some(id) => SecurableId::Warehouse(id.to_owned()),
        None => SecurableId::Unresolvable,
    }
}

/// Diffs one webhook by its natural key `(url, sorted event_types)`. The
/// signing secret is write-only server-side, so it is never part of the diff:
/// if an endpoint with the same url and filter exists, it is a noop.
fn diff_webhook(webhook: &Webhook, state: &ServerState) -> Action {
    let kind = "webhook";
    let mut types = webhook.event_types.clone();
    types.sort();
    let key = (webhook.url.clone(), types);
    let identity = if webhook.event_types.is_empty() {
        format!("{} (all events)", webhook.url)
    } else {
        format!("{} [{}]", webhook.url, webhook.event_types.join(", "))
    };
    if state.webhooks.contains(&key) {
        Action {
            op: Op::Noop,
            kind,
            identity,
            detail: String::new(),
        }
    } else {
        Action {
            op: Op::Create,
            kind,
            identity,
            detail: String::new(),
        }
    }
}

/// Appends would-delete warnings for server resources the bundle does not
/// declare, for the kinds a bundle owns by natural key.
fn append_prune_warnings(bundle: &Bundle, state: &ServerState, actions: &mut Vec<Action>) {
    let declared_warehouses: BTreeSet<&str> =
        bundle.warehouses.iter().map(|w| w.name.as_str()).collect();
    for name in state.warehouses.keys() {
        if !declared_warehouses.contains(name.as_str()) {
            actions.push(Action {
                op: Op::WouldDelete,
                kind: "warehouse",
                identity: name.clone(),
                detail: "on server, not in bundle (prune is out of scope; not deleted)".to_owned(),
            });
        }
    }

    let declared_roles: BTreeSet<&str> = bundle.roles.iter().map(|r| r.name.as_str()).collect();
    for (name, role) in &state.roles {
        // Built-in roles are never candidates for deletion.
        if role.get("built_in").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        if !declared_roles.contains(name.as_str()) {
            actions.push(Action {
                op: Op::WouldDelete,
                kind: "role",
                identity: name.clone(),
                detail: "on server, not in bundle (prune is out of scope; not deleted)".to_owned(),
            });
        }
    }

    let declared_hooks: BTreeSet<(String, Vec<String>)> = bundle
        .webhooks
        .iter()
        .map(|h| {
            let mut t = h.event_types.clone();
            t.sort();
            (h.url.clone(), t)
        })
        .collect();
    for (url, types) in &state.webhooks {
        if !declared_hooks.contains(&(url.clone(), types.clone())) {
            actions.push(Action {
                op: Op::WouldDelete,
                kind: "webhook",
                identity: url.clone(),
                detail: "on server, not in bundle (prune is out of scope; not deleted)".to_owned(),
            });
        }
    }
}

/// Human rendering of a grantee.
fn display_grantee(grantee: &Grantee) -> String {
    match grantee {
        Grantee::Role(name) => format!("role:{name}"),
        Grantee::Principal(id) => format!("principal:{id}"),
    }
}

/// Human rendering of a grant's securable.
fn display_securable(grant: &Grant) -> String {
    let sec = &grant.securable;
    match sec.securable_type.as_str() {
        "warehouse" => format!("warehouse {}", sec.warehouse),
        "namespace" => format!(
            "namespace {}/{}",
            sec.warehouse,
            sec.namespace
                .as_ref()
                .map(|l| l.join("."))
                .unwrap_or_default()
        ),
        "table" => format!(
            "table {}/{}.{}",
            sec.warehouse,
            sec.namespace
                .as_ref()
                .map(|l| l.join("."))
                .unwrap_or_default(),
            sec.table.clone().unwrap_or_default()
        ),
        "view" => format!(
            "view {}/{}.{}",
            sec.warehouse,
            sec.namespace
                .as_ref()
                .map(|l| l.join("."))
                .unwrap_or_default(),
            sec.view.clone().unwrap_or_default()
        ),
        other => format!("{other} {}", sec.warehouse),
    }
}

/// Renders a plan as a human-readable report.
pub(crate) fn render(plan: &Plan) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();

    let mut creates = 0;
    let mut updates = 0;
    let mut noops = 0;
    let mut warnings = 0;

    for action in &plan.actions {
        match action.op {
            Op::Create => creates += 1,
            Op::Update => updates += 1,
            Op::Noop => noops += 1,
            Op::WouldUpdateUnsupported | Op::WouldDelete => warnings += 1,
        }
        let detail = if action.detail.is_empty() {
            String::new()
        } else {
            format!("  ({})", action.detail)
        };
        let _ = writeln!(
            out,
            "  {:<12} {:<10} {}{}",
            action.op.tag(),
            action.kind,
            action.identity,
            detail
        );
    }

    if plan.actions.is_empty() {
        out.push_str("  (bundle declares no resources)\n");
    }

    let _ = writeln!(
        out,
        "\nPlan: {creates} to create, {updates} to update, {noops} unchanged, {warnings} warning(s)."
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::parse;
    use serde_json::json;

    fn empty_state() -> ServerState {
        ServerState {
            warehouses: BTreeMap::new(),
            roles: BTreeMap::new(),
            grants: BTreeSet::new(),
            webhooks: BTreeSet::new(),
        }
    }

    #[test]
    fn absent_warehouse_is_create() {
        let bundle = parse(
            "apiVersion: meridian.dev/v1\nkind: CatalogBundle\nwarehouses:\n  - name: w\n    storage_root: s3://a\n",
            &|_| None,
        )
        .unwrap();
        let action = diff_warehouse(&bundle.warehouses[0], &empty_state());
        assert_eq!(action.op, Op::Create);
    }

    #[test]
    fn matching_warehouse_is_noop() {
        let mut state = empty_state();
        state.warehouses.insert(
            "w".to_owned(),
            json!({"id": "01", "name": "w", "storage_root": "s3://a", "storage_options": {}}),
        );
        let bundle = parse(
            "apiVersion: meridian.dev/v1\nkind: CatalogBundle\nwarehouses:\n  - name: w\n    storage_root: s3://a\n",
            &|_| None,
        )
        .unwrap();
        assert_eq!(diff_warehouse(&bundle.warehouses[0], &state).op, Op::Noop);
    }

    #[test]
    fn changed_root_is_would_update_unsupported() {
        let mut state = empty_state();
        state.warehouses.insert(
            "w".to_owned(),
            json!({"id": "01", "name": "w", "storage_root": "s3://OLD", "storage_options": {}}),
        );
        let bundle = parse(
            "apiVersion: meridian.dev/v1\nkind: CatalogBundle\nwarehouses:\n  - name: w\n    storage_root: s3://NEW\n",
            &|_| None,
        )
        .unwrap();
        assert_eq!(
            diff_warehouse(&bundle.warehouses[0], &state).op,
            Op::WouldUpdateUnsupported
        );
    }

    #[test]
    fn redacted_secret_option_does_not_trigger_drift() {
        let mut state = empty_state();
        state.warehouses.insert(
            "w".to_owned(),
            json!({"id": "01", "name": "w", "storage_root": "s3://a",
                   "storage_options": {"secret-access-key": "***", "region": "us-east-1"}}),
        );
        let bundle = parse(
            "apiVersion: meridian.dev/v1\nkind: CatalogBundle\nwarehouses:\n  - name: w\n    storage_root: s3://a\n    storage_options:\n      secret-access-key: whatever\n      region: us-east-1\n",
            &|_| None,
        )
        .unwrap();
        assert_eq!(diff_warehouse(&bundle.warehouses[0], &state).op, Op::Noop);
    }

    #[test]
    fn role_description_drift_is_unsupported() {
        let mut state = empty_state();
        state.roles.insert(
            "analyst".to_owned(),
            json!({"id": "01", "name": "analyst", "description": "old", "built_in": false}),
        );
        let bundle = parse(
            "apiVersion: meridian.dev/v1\nkind: CatalogBundle\nroles:\n  - name: analyst\n    description: new\n",
            &|_| None,
        )
        .unwrap();
        assert_eq!(
            diff_role(&bundle.roles[0], &state).op,
            Op::WouldUpdateUnsupported
        );
    }

    #[test]
    fn role_without_description_does_not_report_drift() {
        let mut state = empty_state();
        state.roles.insert(
            "analyst".to_owned(),
            json!({"id": "01", "name": "analyst", "description": "something", "built_in": false}),
        );
        let bundle = parse(
            "apiVersion: meridian.dev/v1\nkind: CatalogBundle\nroles:\n  - name: analyst\n",
            &|_| None,
        )
        .unwrap();
        assert_eq!(diff_role(&bundle.roles[0], &state).op, Op::Noop);
    }

    #[test]
    fn webhook_present_is_noop_absent_is_create() {
        let mut state = empty_state();
        state.webhooks.insert((
            "https://h/x".to_owned(),
            vec!["com.meridian.table.committed".to_owned()],
        ));
        let bundle = parse(
            "apiVersion: meridian.dev/v1\nkind: CatalogBundle\nwebhooks:\n  - url: https://h/x\n    event_types: [com.meridian.table.committed]\n    secret: ${S}\n",
            &|_| Some("secretvalue0000000".to_owned()),
        )
        .unwrap();
        assert_eq!(diff_webhook(&bundle.webhooks[0], &state).op, Op::Noop);

        let empty = empty_state();
        assert_eq!(diff_webhook(&bundle.webhooks[0], &empty).op, Op::Create);
    }

    #[test]
    fn property_delta_only_reports_changes() {
        let mut desired = BTreeMap::new();
        desired.insert("owner".to_owned(), "team".to_owned());
        desired.insert("tier".to_owned(), "gold".to_owned());
        let mut current = serde_json::Map::new();
        current.insert("owner".to_owned(), json!("team"));
        current.insert("extra".to_owned(), json!("keep")); // unmanaged, untouched
        let (updates, removals) = property_delta(&desired, &current);
        assert_eq!(updates, vec![("tier".to_owned(), "gold".to_owned())]);
        assert!(removals.is_empty());
    }

    #[test]
    fn prune_warnings_for_undeclared_server_resources() {
        let mut state = empty_state();
        state.warehouses.insert(
            "orphan".to_owned(),
            json!({"id": "01", "name": "orphan", "storage_root": "s3://z", "storage_options": {}}),
        );
        state.roles.insert(
            "admin".to_owned(),
            json!({"id": "02", "name": "admin", "built_in": true}),
        );
        state.roles.insert(
            "leftover".to_owned(),
            json!({"id": "03", "name": "leftover", "built_in": false}),
        );
        let bundle = parse(
            "apiVersion: meridian.dev/v1\nkind: CatalogBundle\n",
            &|_| None,
        )
        .unwrap();
        let mut actions = Vec::new();
        append_prune_warnings(&bundle, &state, &mut actions);
        // orphan warehouse + leftover role, but NOT the built-in admin.
        assert_eq!(actions.len(), 2);
        assert!(actions.iter().all(|a| a.op == Op::WouldDelete));
        assert!(actions.iter().any(|a| a.identity == "orphan"));
        assert!(actions.iter().any(|a| a.identity == "leftover"));
        assert!(!actions.iter().any(|a| a.identity == "admin"));
    }
}
