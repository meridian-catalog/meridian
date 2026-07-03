//! Remote signing over the IRC surface (ADR 005): the spec's
//! `POST /{prefix}/namespaces/{namespace}/tables/{table}/sign` endpoint
//! (`RemoteSignRequest` → `RemoteSignResult`) plus the config
//! advertisement that turns it on for clients.
//!
//! Division of labor:
//!
//! - `meridian_vending::signing` owns the decision
//!   ([`authorize_sign_request`]) and the `SigV4` mechanics. **The decision
//!   is the security boundary** — signatures are computed with warehouse
//!   credentials, so this handler refuses anything the policy cannot prove
//!   stays inside the table's location prefix.
//! - This module owns HTTP framing, RBAC resolution (same rules as
//!   credential vending: `WRITE`/`COMMIT` unlock write methods, `READ`
//!   alone signs `GET`/`HEAD` only), the audit trail, and the response's
//!   `Cache-Control` (per spec: `private` when the client may reuse the
//!   signature — immutable-read methods — else `no-cache`).
//!
//! **Every signing decision is audited** — allow *and* deny: an
//! `audit_log` row (`credential.sign`, with principal, table, method,
//! decoded object keys, decision, and the deny reason) and an outbox event
//! (`credential.signed` / `credential.sign-denied`) are written in one
//! transaction before the response leaves the server. An allow that cannot
//! be recorded is not signed.
//!
//! Signing rides the vending opt-in (`vending = "sts" | "static"`) and
//! requires the warehouse to hold static keys (`access-key-id` /
//! `secret-access-key`) in its storage options; warehouses relying on
//! ambient AWS credentials get an honest 400 here.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::http::header::CACHE_CONTROL;
use axum::response::{IntoResponse, Response};
use meridian_common::MeridianError;
use meridian_common::principal::Principal;
use meridian_storage::read_table_metadata;
use meridian_store::warehouse::WarehouseRecord;
use meridian_store::{audit, outbox, table, tenancy};
use meridian_vending::signing::{RemoteSigner, SignContext, authorize_sign_request};
use meridian_vending::{AccessMode, TableScope, VendingConfig};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::json;

use crate::AppState;
use crate::error::ApiError;
use crate::routes::grants::{forbidden, namespace_scope_chain};
use crate::routes::namespaces::{decode_namespace_param, resolve_warehouse};
use crate::routes::vending::{connect_storage, vend_access_mode};

/// The spec's `RemoteSignRequest`. `properties` is accepted and ignored;
/// unknown future fields do not fail deserialization.
#[derive(Debug, serde::Deserialize)]
pub struct RemoteSignRequestBody {
    /// Signing region (`SigV4` credential scope).
    region: String,
    /// Full object-storage URI the client will send.
    uri: String,
    /// HTTP method.
    method: String,
    /// The headers the client will send (multi-valued map).
    headers: BTreeMap<String, Vec<String>>,
    /// Optional body (`DeleteObjects` XML) — validated, then hashed into
    /// the signature.
    #[serde(default)]
    body: Option<String>,
    /// Storage provider; only `s3` (the default) is supported.
    #[serde(default)]
    provider: Option<String>,
}

/// `POST /{prefix}/namespaces/{namespace}/tables/{table}/sign`.
pub async fn sign_request(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((prefix, raw_namespace, name)): Path<(String, String, String)>,
    axum::Json(request): axum::Json<RemoteSignRequestBody>,
) -> Result<Response, ApiError> {
    let warehouse = resolve_warehouse(&state.pool, &prefix).await?;
    let levels = decode_namespace_param(&raw_namespace)?;
    let record = table::get_by_name(&state.pool, &warehouse.id, &levels, &name)
        .await?
        .ok_or_else(|| {
            ApiError::no_such_table(format!("table {:?} does not exist", ident(&levels, &name)))
        })?;

    let provider = request.provider.as_deref().unwrap_or("s3");
    if !provider.eq_ignore_ascii_case("s3") {
        return Err(ApiError::bad_request(format!(
            "remote signing supports provider \"s3\" only, got {provider:?}"
        )));
    }
    if request.region.trim().is_empty() {
        return Err(ApiError::bad_request("region must not be empty"));
    }

    // RBAC first (cheap, deny-fast): READ signs GET/HEAD; WRITE/COMMIT
    // additionally sign PUT/POST/DELETE — enforced by the policy below.
    let chain = namespace_scope_chain(&state.pool, &warehouse.id, &levels).await?;
    let scope = meridian_store::rbac::SecurableScope::table(&warehouse.id, chain, Some(&record.id));
    let access = vend_access_mode(&state, &principal, &scope).await?;

    // Signing rides the vending opt-in and needs the warehouse keys.
    let vending = VendingConfig::parse(&warehouse.storage_config.0)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    if !vending.is_enabled() {
        return Err(ApiError::bad_request(format!(
            "remote signing is not enabled for warehouse {:?}; \
             set storage option vending = \"sts\" or \"static\" to enable it",
            warehouse.name
        )));
    }
    let signer = signer_from_warehouse(&warehouse)?;

    let location = table_location(&warehouse, &record).await?;
    let table_scope = TableScope::from_s3_location(&location)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let endpoints = endpoint_authorities(&warehouse.storage_config.0);

    let ctx = SignContext {
        method: &request.method,
        uri: &request.uri,
        headers: &request.headers,
        body: request.body.as_deref(),
    };
    let audit_ctx = SignAuditContext {
        warehouse: &warehouse,
        table_id: &record.id,
        table_ident: &ident(&levels, &name),
        method: &request.method,
        uri: &request.uri,
        access,
    };
    match authorize_sign_request(&table_scope, access, &endpoints, &ctx) {
        Err(denial) => {
            record_sign_decision(
                &state,
                &principal,
                &audit_ctx,
                &Decision::Deny {
                    reason: denial.reason(),
                },
            )
            .await?;
            Err(forbidden(format!("request will not be signed: {denial}")))
        }
        Ok(authorized) => {
            sign_and_respond(
                &state,
                &principal,
                &signer,
                &request,
                &audit_ctx,
                &authorized,
            )
            .await
        }
    }
}

/// The allow path: sign, record (audit + outbox, one transaction — an
/// allow that cannot be recorded is not returned), and frame the spec's
/// `RemoteSignResult` with the right `Cache-Control`.
async fn sign_and_respond(
    state: &AppState,
    principal: &Principal,
    signer: &RemoteSigner,
    request: &RemoteSignRequestBody,
    audit_ctx: &SignAuditContext<'_>,
    authorized: &meridian_vending::signing::AuthorizedSign,
) -> Result<Response, ApiError> {
    let headers = signer
        .sign_request(
            &request.method,
            &request.uri,
            &request.region,
            &request.headers,
            request.body.as_deref(),
        )
        .map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                e.to_string(),
            )
        })?;
    record_sign_decision(
        state,
        principal,
        audit_ctx,
        &Decision::Allow {
            action: authorized.action,
            keys: &authorized.keys,
        },
    )
    .await?;

    // Only the headers signing *added* (see `RemoteSigner::sign_request`
    // for why the input headers are not echoed), as the spec's
    // multi-valued map.
    let mut header_map: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (name, value) in &headers {
        header_map.entry(name).or_default().push(value);
    }
    let body = json!({ "uri": request.uri, "headers": header_map });
    let cache_control = if authorized.cacheable {
        "private"
    } else {
        "no-cache"
    };
    Ok((
        StatusCode::OK,
        [(CACHE_CONTROL, cache_control)],
        axum::Json(body),
    )
        .into_response())
}

/// What one signing decision names in the audit trail.
struct SignAuditContext<'a> {
    warehouse: &'a WarehouseRecord,
    table_id: &'a str,
    table_ident: &'a str,
    method: &'a str,
    uri: &'a str,
    access: AccessMode,
}

/// The decision being recorded.
enum Decision<'a> {
    Allow {
        action: &'static str,
        keys: &'a [String],
    },
    Deny {
        reason: &'a str,
    },
}

/// Keys recorded verbatim in one audit row before truncation to a count
/// (`DeleteObjects` bodies carry up to 1000).
const AUDITED_KEYS_MAX: usize = 25;

/// Writes the decision's audit row and outbox event in one transaction.
/// An allow that cannot be recorded is not signed (the caller propagates
/// the error before returning headers).
async fn record_sign_decision(
    state: &AppState,
    principal: &Principal,
    ctx: &SignAuditContext<'_>,
    decision: &Decision<'_>,
) -> Result<(), ApiError> {
    let workspace_id = tenancy::default_workspace_id();
    let mut details = json!({
        "warehouse": ctx.warehouse.name,
        "table": ctx.table_ident,
        "table_id": ctx.table_id,
        "method": ctx.method,
        "uri": ctx.uri,
        "access": ctx.access.as_str(),
    });
    let event_type = match decision {
        Decision::Allow { action, keys } => {
            details["decision"] = json!("allow");
            details["action"] = json!(action);
            details["keys"] = json!(keys.iter().take(AUDITED_KEYS_MAX).collect::<Vec<_>>());
            details["key_count"] = json!(keys.len());
            "credential.signed"
        }
        Decision::Deny { reason } => {
            details["decision"] = json!("deny");
            details["reason"] = json!(reason);
            "credential.sign-denied"
        }
    };

    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|e| MeridianError::internal("failed to begin sign audit", e))?;
    outbox::enqueue(
        &mut *tx,
        &meridian_store::outbox::NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("table:{}", ctx.table_id),
            event_type: event_type.to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        audit::NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.audit_string(),
            action: "credential.sign".to_owned(),
            resource: format!("table:{}", ctx.table_id),
            details,
        },
    )
    .await?;
    tx.commit()
        .await
        .map_err(|e| MeridianError::internal("failed to commit sign audit", e))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Config advertisement (LoadTableResult.config)
// ---------------------------------------------------------------------------

/// Percent-encoding set for one URL path segment.
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// The client properties that switch a table onto remote signing, or
/// `None` when this warehouse/table cannot sign (vending disabled, no
/// static keys, non-S3 location) — the delegation header is then a
/// silent no-op, mirroring vended credentials.
///
/// - `s3.remote-signing-enabled` — the spec's switch.
/// - `s3.signer.endpoint` — this table's sign path, **relative** to the
///   catalog base URI. `s3.signer.uri` is deliberately not set: the spec
///   defaults it to the catalog URI on the client side, which is the one
///   value the client always has right (this server may sit behind port
///   maps or `host.docker.internal` and would only guess).
/// - `s3.signer = S3V4RestSigner` — the property pyiceberg's fsspec
///   `FileIO` keys its signer activation on; inert for other clients.
pub(crate) fn remote_signing_config(
    warehouse: &WarehouseRecord,
    prefix: &str,
    levels: &[String],
    table: &str,
    table_location: &str,
) -> Option<BTreeMap<String, String>> {
    let vending = VendingConfig::parse(&warehouse.storage_config.0).ok()?;
    if !vending.is_enabled() || signer_from_warehouse(warehouse).is_err() {
        return None;
    }
    TableScope::from_s3_location(table_location).ok()?;

    let namespace = levels
        .iter()
        .map(|level| utf8_percent_encode(level, SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("%1F");
    let endpoint = format!(
        "v1/{}/namespaces/{namespace}/tables/{}/sign",
        utf8_percent_encode(prefix, SEGMENT),
        utf8_percent_encode(table, SEGMENT),
    );
    Some(BTreeMap::from([
        ("s3.remote-signing-enabled".to_owned(), "true".to_owned()),
        ("s3.signer.endpoint".to_owned(), endpoint),
        ("s3.signer".to_owned(), "S3V4RestSigner".to_owned()),
    ]))
}

// ---------------------------------------------------------------------------
// Warehouse plumbing
// ---------------------------------------------------------------------------

/// A signer over the warehouse's static keys, or an honest 400.
fn signer_from_warehouse(warehouse: &WarehouseRecord) -> Result<RemoteSigner, ApiError> {
    let options = &warehouse.storage_config.0;
    match (
        options.get("access-key-id"),
        options.get("secret-access-key"),
    ) {
        (Some(access_key_id), Some(secret_access_key)) => RemoteSigner::new(
            access_key_id,
            secret_access_key,
            options.get("session-token").map(String::as_str),
        )
        .map_err(|e| ApiError::bad_request(e.to_string())),
        _ => Err(ApiError::bad_request(format!(
            "remote signing requires warehouse {:?} to configure the \
             access-key-id and secret-access-key storage options",
            warehouse.name
        ))),
    }
}

/// The `host[:port]` authorities of the warehouse's storage endpoints
/// (internal and external), for the policy's host check. Empty for
/// warehouses without an explicit endpoint (real AWS).
fn endpoint_authorities(options: &BTreeMap<String, String>) -> Vec<String> {
    ["endpoint", "endpoint.external"]
        .iter()
        .filter_map(|key| options.get(*key))
        .filter_map(|url| authority_of(url))
        .collect()
}

/// Extracts the lower-cased authority of an endpoint URL, dropping default
/// ports so it compares equal to the policy's normalized request
/// authority.
fn authority_of(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    let authority = rest.split(['/', '?']).next()?.to_ascii_lowercase();
    let stripped = match scheme.to_ascii_lowercase().as_str() {
        "http" => authority.strip_suffix(":80"),
        "https" => authority.strip_suffix(":443"),
        _ => None,
    };
    Some(stripped.map_or(authority.clone(), str::to_owned))
}

// ---------------------------------------------------------------------------
// Table-location cache
// ---------------------------------------------------------------------------

/// Upper bound on cached locations; the cache is cleared (not evicted)
/// beyond it — crude, but the next requests simply re-read metadata.
const LOCATION_CACHE_MAX: usize = 16_384;

/// table id → (pointer version, table location). The pointer version keys
/// staleness: every commit bumps it, so a cached location can never
/// outlive the metadata it came from.
static LOCATION_CACHE: OnceLock<Mutex<HashMap<String, (i64, String)>>> = OnceLock::new();

/// The table's location (the prefix signing is scoped to), from the
/// in-process cache or from current metadata. Signing sits on the hot data
/// path — every object request from a remote-signing client lands here —
/// so steady-state must not re-read `metadata.json` per request.
async fn table_location(
    warehouse: &WarehouseRecord,
    record: &table::TableRecord,
) -> Result<String, ApiError> {
    let metadata_location = record.metadata_location.clone().ok_or_else(|| {
        ApiError::no_such_table(format!("table {:?} has no committed metadata", record.name))
    })?;

    let cache = LOCATION_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(guard) = cache.lock()
        && let Some((version, location)) = guard.get(&record.id)
        && *version == record.pointer_version
    {
        return Ok(location.clone());
    }

    let storage = connect_storage(warehouse)?;
    let metadata = read_table_metadata(storage.as_ref(), &metadata_location)
        .await
        .map_err(|e| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                format!("current metadata at {metadata_location:?} is unreadable: {e}"),
            )
        })?;
    if let Ok(mut guard) = cache.lock() {
        if guard.len() >= LOCATION_CACHE_MAX {
            guard.clear();
        }
        guard.insert(
            record.id.clone(),
            (record.pointer_version, metadata.location.clone()),
        );
    }
    Ok(metadata.location)
}

/// Human-readable `ns.table` identifier.
fn ident(levels: &[String], name: &str) -> String {
    if levels.is_empty() {
        name.to_owned()
    } else {
        format!("{}.{name}", levels.join("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_authorities_normalize_default_ports() {
        let options = BTreeMap::from([
            ("endpoint".to_owned(), "http://localhost:9000".to_owned()),
            (
                "endpoint.external".to_owned(),
                "https://minio.example:443/console".to_owned(),
            ),
        ]);
        assert_eq!(
            endpoint_authorities(&options),
            vec!["localhost:9000".to_owned(), "minio.example".to_owned()]
        );
        assert!(endpoint_authorities(&BTreeMap::new()).is_empty());
        assert!(
            endpoint_authorities(&BTreeMap::from([(
                "endpoint".to_owned(),
                "not a url".to_owned()
            )]))
            .is_empty()
        );
    }

    #[test]
    fn signer_endpoint_is_relative_and_encoded() {
        let warehouse = test_warehouse(&[
            ("vending", "static"),
            ("access-key-id", "k"),
            ("secret-access-key", "s"),
        ]);
        let config = remote_signing_config(
            &warehouse,
            "wh1",
            &["a b".to_owned(), "c".to_owned()],
            "orders",
            "s3://bucket/wh1/ab/orders-x",
        )
        .expect("signing config");
        assert_eq!(config["s3.remote-signing-enabled"], "true");
        assert_eq!(
            config["s3.signer.endpoint"],
            "v1/wh1/namespaces/a%20b%1Fc/tables/orders/sign"
        );
        assert_eq!(config["s3.signer"], "S3V4RestSigner");
    }

    #[test]
    fn signing_config_is_none_without_opt_in_keys_or_s3() {
        // No vending opt-in.
        let warehouse = test_warehouse(&[("access-key-id", "k"), ("secret-access-key", "s")]);
        assert!(remote_signing_config(&warehouse, "w", &[], "t", "s3://bucket/p/t").is_none());
        // No keys.
        let warehouse = test_warehouse(&[("vending", "static")]);
        assert!(remote_signing_config(&warehouse, "w", &[], "t", "s3://bucket/p/t").is_none());
        // Non-S3 location.
        let warehouse = test_warehouse(&[
            ("vending", "static"),
            ("access-key-id", "k"),
            ("secret-access-key", "s"),
        ]);
        assert!(remote_signing_config(&warehouse, "w", &[], "t", "file:///tmp/wh/t").is_none());
    }

    fn test_warehouse(options: &[(&str, &str)]) -> WarehouseRecord {
        WarehouseRecord {
            id: "01TEST".to_owned(),
            workspace_id: "ws".to_owned(),
            name: "wh1".to_owned(),
            storage_root: "s3://bucket/wh1".to_owned(),
            storage_config: sqlx::types::Json(
                options
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
            ),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }
}
