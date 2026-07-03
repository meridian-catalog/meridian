//! Credential vending over the IRC surface (spec: `loadCredentials` and the
//! `X-Iceberg-Access-Delegation` header on table loads).
//!
//! What vends, and as what (full mechanics in `meridian-vending`):
//!
//! - The warehouse opts in via storage options: `vending = "sts"` (scoped,
//!   short-lived STS session credentials — AWS or `MinIO`) or
//!   `vending = "static"` (warehouse keys passed through, deliberately
//!   unscoped). The default (`none`) vends nothing and keeps the credential
//!   denylist absolute.
//! - **Access follows RBAC**: a principal holding `WRITE` or `COMMIT` on
//!   the table gets read-write credentials; one holding only `READ` gets
//!   read-only. (With authentication disabled the anonymous principal
//!   passes every check and vends read-write.)
//! - `GET .../tables/{table}/credentials` returns the spec's
//!   `LoadCredentialsResponse`; table loads that carry
//!   `X-Iceberg-Access-Delegation: vended-credentials` get the same
//!   credentials merged into `LoadTableResult.config` **and** mirrored in
//!   its `storage-credentials` field. `remote-signing` (alone) is an
//!   honest 400: not implemented yet.
//! - **Every vend is audited**: an `audit.credential.vend` row and a
//!   `credential.vended` outbox event (principal, table, scope prefix,
//!   access, ttl, mode) are written in one transaction before the
//!   credentials leave the server; if that write fails, nothing is vended.
//!   The audit row is the product.

use std::sync::Arc;
use std::time::Duration;

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_storage::{Storage, read_table_metadata};
use meridian_store::rbac::{self, Privilege, SecurableScope};
use meridian_store::warehouse::WarehouseRecord;
use meridian_store::{audit, outbox, tenancy};
use meridian_vending::{
    AccessMode, CredentialVendor, StaticVendor, StsVendor, TableScope, VendedCredentials,
    VendingConfig, VendingError, Vendor,
};
use serde_json::{Value, json};

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::require;
use crate::routes::namespaces::decode_namespace_param;

/// The access-delegation mechanisms a client asked for, parsed from
/// `X-Iceberg-Access-Delegation` (a comma-separated preference list).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestedDelegation {
    /// No header: plain config passthrough only.
    None,
    /// `vended-credentials` (possibly alongside `remote-signing`; vended
    /// credentials are the mechanism this server implements, so they win).
    VendedCredentials,
}

/// Parses the `X-Iceberg-Access-Delegation` header.
///
/// `remote-signing` *alone* is an honest 400 (not implemented yet), as is
/// a header carrying only unknown mechanisms; clients that list
/// `vended-credentials` anywhere get vended credentials.
pub(crate) fn requested_delegation(headers: &HeaderMap) -> Result<RequestedDelegation, ApiError> {
    let Some(raw) = headers.get("x-iceberg-access-delegation") else {
        return Ok(RequestedDelegation::None);
    };
    let raw = raw
        .to_str()
        .map_err(|_| ApiError::bad_request("X-Iceberg-Access-Delegation must be visible ASCII"))?;
    let mechanisms: Vec<&str> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if mechanisms.is_empty() {
        return Ok(RequestedDelegation::None);
    }
    if mechanisms
        .iter()
        .any(|m| m.eq_ignore_ascii_case("vended-credentials"))
    {
        return Ok(RequestedDelegation::VendedCredentials);
    }
    if mechanisms
        .iter()
        .any(|m| m.eq_ignore_ascii_case("remote-signing"))
    {
        return Err(ApiError::bad_request(
            "remote-signing access delegation is not implemented yet; \
             request vended-credentials instead",
        ));
    }
    Err(ApiError::bad_request(format!(
        "unknown access-delegation mechanism(s) {raw:?}: \
         this server supports \"vended-credentials\""
    )))
}

/// The highest access the principal may hold on the table: `WRITE` or
/// `COMMIT` vends read-write; otherwise `READ` is required and vends
/// read-only. (Resolution order matters: the cheap upgrade checks run
/// first, and a principal without even `READ` is rejected by `require`.)
pub(crate) async fn vend_access_mode(
    state: &AppState,
    principal: &Principal,
    scope: &SecurableScope,
) -> Result<AccessMode, ApiError> {
    for privilege in [Privilege::Write, Privilege::Commit] {
        match rbac::authorize(&state.pool, principal, privilege, scope).await {
            Ok(()) => return Ok(AccessMode::ReadWrite),
            Err(rbac::AuthzError::Forbidden(_)) => {}
            Err(rbac::AuthzError::Store(error)) => return Err(error.into()),
        }
    }
    require(&state.pool, principal, Privilege::Read, scope).await?;
    Ok(AccessMode::Read)
}

/// Everything a recorded vend names: who, what, how much, for how long.
pub(crate) struct VendContext<'a> {
    /// The warehouse being vended from.
    pub warehouse: &'a WarehouseRecord,
    /// Table id (ULID) — the audit resource.
    pub table_id: &'a str,
    /// Human-readable `ns.table` identifier.
    pub table_ident: &'a str,
    /// Table location whose prefix the credentials are scoped to.
    pub table_location: &'a str,
    /// Read or read-write.
    pub access: AccessMode,
}

/// Vends credentials for one table and records the vend (audit row +
/// outbox event, one transaction) before returning them.
///
/// Returns `Ok(None)` when the warehouse has vending disabled — the caller
/// decides whether that is a silent no-op (header path) or an error
/// (`loadCredentials`).
pub(crate) async fn vend_for_table(
    state: &AppState,
    principal: &Principal,
    ctx: &VendContext<'_>,
) -> Result<Option<VendedCredentials>, ApiError> {
    let options = &ctx.warehouse.storage_config.0;
    let config = VendingConfig::parse(options).map_err(|e| vending_config_error(&e))?;

    let (vendor, ttl) = match &config {
        VendingConfig::None => return Ok(None),
        VendingConfig::Static => {
            let vendor = StaticVendor::new(
                options.get("access-key-id").map(String::as_str),
                options.get("secret-access-key").map(String::as_str),
                options.get("session-token").map(String::as_str),
            )
            .map_err(|e| vending_config_error(&e))?;
            // Static keys do not expire; the ttl is nominal.
            (Vendor::Static(vendor), Duration::from_secs(0))
        }
        VendingConfig::Sts { role_arn, ttl } => {
            let credentials = match (
                options.get("access-key-id"),
                options.get("secret-access-key"),
            ) {
                (Some(access_key_id), Some(secret_access_key)) => {
                    Some((access_key_id.clone(), secret_access_key.clone()))
                }
                _ => None,
            };
            let vendor = StsVendor::new(
                role_arn.clone(),
                options
                    .get("region")
                    .cloned()
                    .unwrap_or_else(|| "us-east-1".to_owned()),
                // The server calls STS on the *internal* endpoint; only
                // what clients see is rewritten to `endpoint.external`.
                options.get("endpoint").cloned(),
                credentials,
                &principal.audit_string(),
            );
            (Vendor::Sts(vendor), *ttl)
        }
    };

    let scope =
        TableScope::from_s3_location(ctx.table_location).map_err(|e| vending_error_to_api(&e))?;
    let mut vended = vendor
        .vend(&scope, ctx.access, ttl)
        .await
        .map_err(|e| vending_error_to_api(&e))?;

    // Client-facing endpoint advertisement rides along with the vended
    // config so engines that only read `storage-credentials` still resolve
    // the right endpoint.
    if let Some(endpoint) = options
        .get("endpoint.external")
        .or_else(|| options.get("endpoint"))
    {
        vended
            .config
            .insert("s3.endpoint".to_owned(), endpoint.clone());
    }

    record_vend(state, principal, ctx, &config, ttl, &vended).await?;
    Ok(Some(vended))
}

/// Writes the vend's audit row and outbox event in one transaction. A vend
/// that cannot be recorded does not happen.
async fn record_vend(
    state: &AppState,
    principal: &Principal,
    ctx: &VendContext<'_>,
    config: &VendingConfig,
    ttl: Duration,
    vended: &VendedCredentials,
) -> Result<(), ApiError> {
    let workspace_id = tenancy::default_workspace_id();
    let details = json!({
        "warehouse": ctx.warehouse.name,
        "table": ctx.table_ident,
        "table_id": ctx.table_id,
        "prefix": vended.prefix,
        "access": ctx.access.as_str(),
        "mode": config.mode_str(),
        "ttl_secs": ttl.as_secs(),
        "expires_at": vended.expires_at.map(|t| t.to_rfc3339()),
    });

    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|e| MeridianError::internal("failed to begin vend audit", e))?;
    outbox::enqueue(
        &mut *tx,
        &meridian_store::outbox::NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("table:{}", ctx.table_id),
            event_type: "credential.vended".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        audit::NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.audit_string(),
            action: "credential.vend".to_owned(),
            resource: format!("table:{}", ctx.table_id),
            details,
        },
    )
    .await?;
    tx.commit()
        .await
        .map_err(|e| MeridianError::internal("failed to commit vend audit", e))?;
    Ok(())
}

/// The spec's `StorageCredential` shape.
pub(crate) fn storage_credential_json(vended: &VendedCredentials) -> Value {
    json!({
        "prefix": vended.prefix,
        "config": vended.config,
    })
}

/// A vending *configuration* problem is the operator's to fix, but it must
/// be visible: 400 with the parse error (it can only arise on warehouses
/// that opted into vending).
fn vending_config_error(error: &VendingError) -> ApiError {
    ApiError::bad_request(error.to_string())
}

/// Maps vend failures onto the IRC error surface.
fn vending_error_to_api(error: &VendingError) -> ApiError {
    match error {
        VendingError::UnsupportedCloud { .. } | VendingError::UnsupportedLocation(_) => {
            ApiError::bad_request(error.to_string())
        }
        VendingError::Config(_) => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
            error.to_string(),
        ),
        VendingError::Provider(_) => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailableException",
            error.to_string(),
        ),
    }
}

/// `GET /{prefix}/namespaces/{namespace}/tables/{table}/credentials` —
/// the spec's `loadCredentials`: vend for an already-loaded table.
pub async fn load_credentials(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    use crate::routes::grants::namespace_scope_chain;
    use crate::routes::namespaces::resolve_warehouse;

    let warehouse = resolve_warehouse(&state.pool, &prefix).await?;
    let levels = decode_namespace_param(&raw_namespace)?;
    let record = meridian_store::table::get_by_name(&state.pool, &warehouse.id, &levels, &name)
        .await?
        .ok_or_else(|| {
            ApiError::no_such_table(format!(
                "table {:?} does not exist",
                if levels.is_empty() {
                    name.clone()
                } else {
                    format!("{}.{name}", levels.join("."))
                }
            ))
        })?;
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    let scope = SecurableScope::table(&warehouse.id, chain, Some(&record.id));
    let access = vend_access_mode(&state, &principal, &scope).await?;

    // Refuse loudly rather than answer with an empty credentials list a
    // client would misread as "no credentials needed".
    let vending =
        VendingConfig::parse(&warehouse.storage_config.0).map_err(|e| vending_config_error(&e))?;
    if !vending.is_enabled() {
        return Err(ApiError::bad_request(format!(
            "credential vending is not enabled for warehouse {:?}; \
             set storage option vending = \"sts\" or \"static\" to enable it",
            warehouse.name
        )));
    }

    // The scope prefix is the table location, read from current metadata
    // (the pointer row only stores the metadata file location).
    let metadata_location = record.metadata_location.clone().ok_or_else(|| {
        ApiError::no_such_table(format!("table {name:?} has no committed metadata"))
    })?;
    let storage = connect_storage(&warehouse)?;
    let metadata = read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("current metadata at {metadata_location:?} is unreadable: {e}"),
            )
        })?;

    let ident = if levels.is_empty() {
        name.clone()
    } else {
        format!("{}.{name}", levels.join("."))
    };
    let vended = vend_for_table(
        &state,
        &principal,
        &VendContext {
            warehouse: &warehouse,
            table_id: &record.id,
            table_ident: &ident,
            table_location: &metadata.location,
            access,
        },
    )
    .await?
    .ok_or_else(|| {
        // Unreachable (checked above); belt and braces for the race where
        // vending was disabled between the check and the vend.
        ApiError::bad_request("credential vending is not enabled for this warehouse")
    })?;

    let body = json!({ "storage-credentials": [storage_credential_json(&vended)] });
    Ok((StatusCode::OK, axum::Json(body)).into_response())
}

/// Connects the warehouse's storage profile (local twin of the table
/// module's private helper).
fn connect_storage(warehouse: &WarehouseRecord) -> Result<Arc<dyn Storage>, ApiError> {
    let profile =
        meridian_storage::StorageProfile::parse(&warehouse.storage_root, &warehouse.storage_config)
            .map_err(|e| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalServerError",
                    format!(
                        "warehouse {:?} storage configuration is unusable: {e}",
                        warehouse.name
                    ),
                )
            })?;
    profile.connect().map_err(|e| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
            format!(
                "warehouse {:?} storage configuration is unusable: {e}",
                warehouse.name
            ),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-iceberg-access-delegation",
            value.parse().expect("header value"),
        );
        headers
    }

    #[test]
    fn no_header_means_no_delegation() {
        assert_eq!(
            requested_delegation(&HeaderMap::new()).expect("parse"),
            RequestedDelegation::None
        );
        assert_eq!(
            requested_delegation(&headers("")).expect("parse"),
            RequestedDelegation::None
        );
    }

    #[test]
    fn vended_credentials_win_in_any_position_and_case() {
        for value in [
            "vended-credentials",
            "VENDED-CREDENTIALS",
            "vended-credentials,remote-signing",
            "remote-signing, vended-credentials",
            " vended-credentials ",
        ] {
            assert_eq!(
                requested_delegation(&headers(value)).expect("parse"),
                RequestedDelegation::VendedCredentials,
                "value: {value:?}"
            );
        }
    }

    #[test]
    fn remote_signing_alone_is_an_honest_400() {
        let error = requested_delegation(&headers("remote-signing")).expect_err("must reject");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn unknown_mechanisms_alone_are_rejected() {
        let error = requested_delegation(&headers("carrier-pigeon")).expect_err("must reject");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }
}
