//! Executing a plan: idempotent create/update against the server, with
//! per-resource success/failure reporting.
//!
//! `apply` walks the bundle in dependency order (warehouses, then namespaces,
//! then roles, then grants, then webhooks) and reconciles each resource. Every
//! operation is idempotent: creating a resource that already exists, or setting
//! properties already set, is reported as a success (a no-op), not a failure.
//! An "already exists" conflict from the server is swallowed for exactly this
//! reason — it is the concurrent/re-run case, not an error.
//!
//! The applier never deletes. Warnings from the plan ([`Op::WouldDelete`],
//! [`Op::WouldUpdateUnsupported`]) are surfaced but no destructive or
//! impossible action is taken.

use serde_json::Value;

use super::model::Bundle;
use super::plan::{self, Op, ServerState};
use super::{BundleError, Grant};
use crate::client::{self, CliError};

/// The result of applying one resource.
pub(crate) struct ResourceOutcome {
    /// Resource kind (`warehouse`, `namespace`, ...).
    pub(crate) kind: &'static str,
    /// Human identifier.
    pub(crate) identity: String,
    /// What happened.
    pub(crate) status: Status,
}

/// The disposition of one applied resource.
pub(crate) enum Status {
    /// Resource was created.
    Created,
    /// Resource was updated in place.
    Updated,
    /// Nothing needed doing (already in the desired state).
    Unchanged,
    /// A non-fatal warning (drift with no update path, or a would-delete).
    Warned(String),
    /// The operation failed.
    Failed(String),
}

impl Status {
    /// The short tag printed per resource.
    fn tag(&self) -> &'static str {
        match self {
            Status::Created => "created",
            Status::Updated => "updated",
            Status::Unchanged => "unchanged",
            Status::Warned(_) => "warning",
            Status::Failed(_) => "FAILED",
        }
    }

    /// Whether this outcome represents a failure.
    fn is_failure(&self) -> bool {
        matches!(self, Status::Failed(_))
    }
}

/// The aggregate result of an apply.
pub(crate) struct ApplyReport {
    /// Per-resource outcomes, in apply order.
    pub(crate) outcomes: Vec<ResourceOutcome>,
}

impl ApplyReport {
    /// Number of failed resources.
    pub(crate) fn failures(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|o| o.status.is_failure())
            .count()
    }

    /// Renders the per-resource report.
    pub(crate) fn render(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        let (mut created, mut updated, mut unchanged, mut warned, mut failed) = (0, 0, 0, 0, 0);
        for outcome in &self.outcomes {
            match &outcome.status {
                Status::Created => created += 1,
                Status::Updated => updated += 1,
                Status::Unchanged => unchanged += 1,
                Status::Warned(_) => warned += 1,
                Status::Failed(_) => failed += 1,
            }
            let extra = match &outcome.status {
                Status::Warned(m) | Status::Failed(m) => format!(": {m}"),
                _ => String::new(),
            };
            let _ = writeln!(
                out,
                "  {:<10} {:<10} {}{}",
                outcome.status.tag(),
                outcome.kind,
                outcome.identity,
                extra
            );
        }
        let _ = writeln!(
            out,
            "\nApply: {created} created, {updated} updated, {unchanged} unchanged, \
             {warned} warning(s), {failed} failed."
        );
        out
    }
}

/// Applies a bundle to a server. Loads server state, computes a plan, then
/// reconciles each declared resource idempotently. The returned report drives
/// the CLI's exit code (non-zero if any resource failed).
pub(crate) async fn apply(
    bundle: &Bundle,
    server: &str,
    token: Option<&str>,
) -> Result<ApplyReport, BundleError> {
    let state = plan::load_server_state(server, token)
        .await
        .map_err(BundleError::msg)?;
    let mut outcomes = Vec::new();

    for warehouse in &bundle.warehouses {
        outcomes.push(apply_warehouse(warehouse, server, token, &state).await);
    }
    for namespace in &bundle.namespaces {
        outcomes.push(apply_namespace(namespace, server, token, &state).await);
    }
    for role in &bundle.roles {
        outcomes.push(apply_role(role, server, token, &state).await);
    }
    for grant in &bundle.grants {
        outcomes.push(apply_grant(grant, server, token).await);
    }
    for webhook in &bundle.webhooks {
        outcomes.push(apply_webhook(webhook, server, token, &state).await);
    }

    Ok(ApplyReport { outcomes })
}

/// True when a client error is the server's "already exists" conflict — the
/// idempotent re-run case we treat as success.
fn is_already_exists(err: &CliError) -> bool {
    let msg = err.0.to_lowercase();
    msg.contains("already exists") || msg.contains("alreadyexists") || msg.contains("conflict")
}

async fn apply_warehouse(
    warehouse: &super::Warehouse,
    server: &str,
    token: Option<&str>,
    state: &ServerState,
) -> ResourceOutcome {
    let kind = "warehouse";
    let identity = warehouse.name.clone();
    if let Some(current) = state.warehouses.get(&warehouse.name) {
        // Exists already. If it has drifted, we can only warn (no update API).
        let current_root = current
            .get("storage_root")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if current_root != warehouse.storage_root {
            return ResourceOutcome {
                kind,
                identity,
                status: Status::Warned(format!(
                    "storage_root differs ({current_root:?} on server) but warehouses \
                     are immutable; not changed"
                )),
            };
        }
        return ResourceOutcome {
            kind,
            identity,
            status: Status::Unchanged,
        };
    }
    let options: Vec<(String, String)> = warehouse
        .storage_options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    match client::warehouse_create(
        server,
        token,
        &warehouse.name,
        &warehouse.storage_root,
        &options,
    )
    .await
    {
        Ok(_) => ResourceOutcome {
            kind,
            identity,
            status: Status::Created,
        },
        Err(e) if is_already_exists(&e) => ResourceOutcome {
            kind,
            identity,
            status: Status::Unchanged,
        },
        Err(e) => ResourceOutcome {
            kind,
            identity,
            status: Status::Failed(e.0),
        },
    }
}

async fn apply_namespace(
    namespace: &super::Namespace,
    server: &str,
    token: Option<&str>,
    _state: &ServerState,
) -> ResourceOutcome {
    let kind = "namespace";
    let identity = format!("{}/{}", namespace.warehouse, namespace.dotted());

    // Does it exist? Load it to decide create vs. property update.
    let current =
        client::namespace_load(server, token, &namespace.warehouse, &namespace.levels).await;
    let props: Vec<(String, String)> = namespace
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    match current {
        Ok(None) => {
            // Iceberg requires every ancestor namespace to exist before a
            // child can be created. For a multi-level namespace like
            // `[sales, emea]`, create `[sales]` first (idempotently), then the
            // leaf. Ancestors get no properties — only the declared leaf does.
            if let Err(failure) = ensure_parents(namespace, server, token).await {
                return ResourceOutcome {
                    kind,
                    identity,
                    status: Status::Failed(failure),
                };
            }
            // Create the leaf with its properties.
            match client::namespace_create(
                server,
                token,
                &namespace.warehouse,
                &namespace.levels,
                &props,
            )
            .await
            {
                Ok(_) => ResourceOutcome {
                    kind,
                    identity,
                    status: Status::Created,
                },
                Err(e) if is_already_exists(&e) => {
                    // Raced with another writer; converge the properties.
                    reconcile_namespace_properties(namespace, server, token, kind, identity).await
                }
                Err(e) => ResourceOutcome {
                    kind,
                    identity,
                    status: Status::Failed(e.0),
                },
            }
        }
        Ok(Some(body)) => {
            // Exists: converge properties (additive; unmanaged keys untouched).
            let current_props = body
                .get("properties")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let needs_update = namespace
                .properties
                .iter()
                .any(|(k, v)| current_props.get(k).and_then(Value::as_str) != Some(v.as_str()));
            if needs_update {
                reconcile_namespace_properties(namespace, server, token, kind, identity).await
            } else {
                ResourceOutcome {
                    kind,
                    identity,
                    status: Status::Unchanged,
                }
            }
        }
        Err(e) => ResourceOutcome {
            kind,
            identity,
            status: Status::Failed(e.0),
        },
    }
}

/// Ensures every ancestor of a namespace exists, creating any that are
/// missing (idempotently, without properties). A `[sales, emea, orders]`
/// namespace ensures `[sales]` and `[sales, emea]`. An "already exists"
/// conflict is fine — a concurrent apply or a prior run got there first.
async fn ensure_parents(
    namespace: &super::Namespace,
    server: &str,
    token: Option<&str>,
) -> Result<(), String> {
    for depth in 1..namespace.levels.len() {
        let ancestor = &namespace.levels[..depth];
        match client::namespace_create(server, token, &namespace.warehouse, ancestor, &[]).await {
            Ok(_) => {}
            Err(e) if is_already_exists(&e) => {}
            Err(e) => {
                return Err(format!(
                    "failed to create parent namespace {:?}: {}",
                    ancestor.join("."),
                    e.0
                ));
            }
        }
    }
    Ok(())
}

/// Sets the bundle's namespace properties via the properties endpoint.
async fn reconcile_namespace_properties(
    namespace: &super::Namespace,
    server: &str,
    token: Option<&str>,
    kind: &'static str,
    identity: String,
) -> ResourceOutcome {
    if namespace.properties.is_empty() {
        return ResourceOutcome {
            kind,
            identity,
            status: Status::Unchanged,
        };
    }
    let updates: Vec<(String, String)> = namespace
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    match client::namespace_update_properties(
        server,
        token,
        &namespace.warehouse,
        &namespace.levels,
        &updates,
        &[],
    )
    .await
    {
        Ok(_) => ResourceOutcome {
            kind,
            identity,
            status: Status::Updated,
        },
        Err(e) => ResourceOutcome {
            kind,
            identity,
            status: Status::Failed(e.0),
        },
    }
}

async fn apply_role(
    role: &super::Role,
    server: &str,
    token: Option<&str>,
    state: &ServerState,
) -> ResourceOutcome {
    let kind = "role";
    let identity = role.name.clone();
    if let Some(current) = state.roles.get(&role.name) {
        let current_desc = current.get("description").and_then(Value::as_str);
        if role.description.is_some() && current_desc != role.description.as_deref() {
            return ResourceOutcome {
                kind,
                identity,
                status: Status::Warned(
                    "description differs but roles have no update API; not changed".to_owned(),
                ),
            };
        }
        return ResourceOutcome {
            kind,
            identity,
            status: Status::Unchanged,
        };
    }
    match client::role_create(server, token, &role.name, role.description.as_deref()).await {
        Ok(_) => ResourceOutcome {
            kind,
            identity,
            status: Status::Created,
        },
        Err(e) if is_already_exists(&e) => ResourceOutcome {
            kind,
            identity,
            status: Status::Unchanged,
        },
        Err(e) => ResourceOutcome {
            kind,
            identity,
            status: Status::Failed(e.0),
        },
    }
}

async fn apply_grant(grant: &Grant, server: &str, token: Option<&str>) -> ResourceOutcome {
    let kind = "grant";
    let identity = grant_identity(grant);
    let body = grant_request_body(grant);
    match client::grant_add(server, token, &body).await {
        Ok(_) => ResourceOutcome {
            kind,
            identity,
            status: Status::Created,
        },
        Err(e) if is_already_exists(&e) => ResourceOutcome {
            kind,
            identity,
            status: Status::Unchanged,
        },
        Err(e) => ResourceOutcome {
            kind,
            identity,
            status: Status::Failed(e.0),
        },
    }
}

/// Builds the `POST /api/v2/grants` body from a bundle grant.
fn grant_request_body(grant: &Grant) -> Value {
    let sec = &grant.securable;
    serde_json::json!({
        "privilege": grant.privilege,
        "role": grant.role,
        "principal_id": grant.principal,
        "securable": {
            "type": sec.securable_type,
            "warehouse": sec.warehouse,
            "namespace": sec.namespace,
            "table": sec.table,
            "view": sec.view,
        },
    })
}

/// Human identity of a grant for reporting.
fn grant_identity(grant: &Grant) -> String {
    let grantee = grant
        .role
        .as_ref()
        .map(|r| format!("role:{r}"))
        .or_else(|| grant.principal.as_ref().map(|p| format!("principal:{p}")))
        .unwrap_or_else(|| "?".to_owned());
    format!(
        "{} {} on {} {}",
        grant.privilege, grantee, grant.securable.securable_type, grant.securable.warehouse
    )
}

async fn apply_webhook(
    webhook: &super::Webhook,
    server: &str,
    token: Option<&str>,
    state: &ServerState,
) -> ResourceOutcome {
    let kind = "webhook";
    let identity = webhook.url.clone();
    let mut types = webhook.event_types.clone();
    types.sort();
    if state.webhooks.contains(&(webhook.url.clone(), types)) {
        return ResourceOutcome {
            kind,
            identity,
            status: Status::Unchanged,
        };
    }
    match client::webhook_create(
        server,
        token,
        &webhook.url,
        &webhook.event_types,
        &webhook.secret,
    )
    .await
    {
        Ok(_) => ResourceOutcome {
            kind,
            identity,
            status: Status::Created,
        },
        Err(e) => ResourceOutcome {
            kind,
            identity,
            status: Status::Failed(e.0),
        },
    }
}

/// Surfaces the plan's would-delete (prune) warnings on the apply report.
///
/// Only [`Op::WouldDelete`] is added here: the apply loop already emits a
/// `Warned` outcome for immutable-drift resources (warehouse storage, role
/// description), so re-adding their [`Op::WouldUpdateUnsupported`] warnings
/// would double-report them. Prune candidates, by contrast, are never visited
/// by the apply loop (they are not in the bundle), so they must be added from
/// the plan.
pub(crate) fn warnings_from_plan(plan: &plan::Plan) -> Vec<ResourceOutcome> {
    plan.actions
        .iter()
        .filter(|a| matches!(a.op, Op::WouldDelete))
        .map(|a| ResourceOutcome {
            kind: a.kind,
            identity: a.identity.clone(),
            status: Status::Warned(a.detail.clone()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_detection_matches_server_phrasing() {
        assert!(is_already_exists(&CliError(
            "server returned 409 Conflict (AlreadyExistsException): warehouse \"w\" already exists"
                .to_owned()
        )));
        assert!(is_already_exists(&CliError(
            "an identical grant already exists".to_owned()
        )));
        assert!(!is_already_exists(&CliError(
            "server returned 400 (BadRequest): nope".to_owned()
        )));
    }

    #[test]
    fn grant_body_carries_selector() {
        let grant: Grant = serde_yaml::from_str(
            "role: analyst\nprivilege: READ\nsecurable:\n  type: warehouse\n  warehouse: w\n",
        )
        .unwrap();
        let body = grant_request_body(&grant);
        assert_eq!(body["privilege"], "READ");
        assert_eq!(body["role"], "analyst");
        assert_eq!(body["securable"]["type"], "warehouse");
        assert_eq!(body["securable"]["warehouse"], "w");
    }

    #[test]
    fn warnings_from_plan_only_surfaces_would_delete() {
        use super::super::plan::{Action, Op, Plan};
        let plan = Plan {
            actions: vec![
                Action {
                    op: Op::WouldDelete,
                    kind: "warehouse",
                    identity: "orphan".to_owned(),
                    detail: "prune".to_owned(),
                },
                // Immutable drift is already reported by the apply loop; it
                // must NOT be re-added here.
                Action {
                    op: Op::WouldUpdateUnsupported,
                    kind: "role",
                    identity: "analyst".to_owned(),
                    detail: "desc drift".to_owned(),
                },
                Action {
                    op: Op::Noop,
                    kind: "warehouse",
                    identity: "kept".to_owned(),
                    detail: String::new(),
                },
            ],
        };
        let warnings = warnings_from_plan(&plan);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].identity, "orphan");
    }
}
