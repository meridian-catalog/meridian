//! The authorization policy for remote signing — the security boundary.
//!
//! A signing endpoint computes signatures with *warehouse* credentials, so
//! the only thing standing between "engine may read this table" and "engine
//! may read the whole bucket" is this module. [`authorize_sign_request`] is
//! a pure function from (table scope, caller access, endpoint allowlist,
//! request) to an allow-with-resolved-keys or a deny-with-reason; it never
//! signs anything itself and is exhaustively unit-tested below.
//!
//! Policy summary (deny unless every point holds):
//!
//! - The request host is one of the warehouse's storage endpoints (or an
//!   `*.amazonaws.com` host when the warehouse has no explicit endpoint),
//!   addressed path-style or virtual-host style — never a signing oracle
//!   for arbitrary hosts.
//! - The bucket is the table's bucket and the percent-decoded object key is
//!   the table prefix or strictly under it. `.`/`..` path segments are
//!   rejected before comparison (S3 keys never need them; proxies may
//!   normalize them into escapes).
//! - The method is within the caller's access: `GET`/`HEAD` for `READ`,
//!   plus `PUT`/`POST`/`DELETE` for `READ_WRITE`. `PATCH`/`OPTIONS` are
//!   never signed.
//! - No denied subresource (`?acl`, `?policy`, `?tagging`, ...) is
//!   addressed.
//! - `x-amz-copy-source`, when present, also resolves inside the table
//!   prefix (otherwise `CopyObject` reads any warehouse object).
//! - Bucket-root requests are only ListObjects(V1/V2/Versions) with a
//!   `prefix` parameter inside the table prefix (read), or `DeleteObjects`
//!   (`POST ?delete`) whose XML body keys **all** resolve inside the table
//!   prefix (write).

use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::{AccessMode, TableScope};

/// A signing request as the policy sees it (already deserialized; the HTTP
/// shape lives in the server).
#[derive(Debug)]
pub struct SignContext<'a> {
    /// HTTP method, per the spec's `RemoteSignRequest` enum (upper-case).
    pub method: &'a str,
    /// The full request URI the client will send to object storage.
    pub uri: &'a str,
    /// The headers the client will send (multi-valued).
    pub headers: &'a BTreeMap<String, Vec<String>>,
    /// Optional request body (`DeleteObjects` XML).
    pub body: Option<&'a str>,
}

/// An allowed signing request, resolved to what it touches (for the audit
/// row).
#[derive(Debug)]
pub struct AuthorizedSign {
    /// Stable action name (`get-object`, `list-objects`, ...).
    pub action: &'static str,
    /// Decoded object key(s) — or the list prefix — the request addresses.
    pub keys: Vec<String>,
    /// Whether the signed response may be reused by the client
    /// (`Cache-Control: private`): true only for immutable-read methods.
    pub cacheable: bool,
}

/// A denied signing request, with the operator-readable reason.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SignDenial(String);

impl SignDenial {
    fn new(reason: impl Into<String>) -> Self {
        Self(reason.into())
    }

    /// The denial reason (also the audit-row detail).
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.0
    }
}

/// Query keys that address S3 subresources no Iceberg engine needs signed —
/// ACLs, policies, tagging and friends change object/bucket *governance*,
/// not table data, and are denied outright.
const DENIED_SUBRESOURCES: &[&str] = &[
    "accelerate",
    "acl",
    "analytics",
    "cors",
    "encryption",
    "intelligent-tiering",
    "inventory",
    "legal-hold",
    "lifecycle",
    "logging",
    "metrics",
    "notification",
    "object-lock",
    "ownershipControls",
    "policy",
    "publicAccessBlock",
    "replication",
    "requestPayment",
    "restore",
    "retention",
    "tagging",
    "torrent",
    "versioning",
    "website",
];

/// Query keys permitted on a bucket-root listing (`ListObjects` V1/V2 and
/// `ListObjectVersions`). Anything else on the bucket root is denied.
const LIST_QUERY_KEYS: &[&str] = &[
    "continuation-token",
    "delimiter",
    "encoding-type",
    "fetch-owner",
    "key-marker",
    "list-type",
    "marker",
    "max-keys",
    "prefix",
    "start-after",
    "version-id-marker",
    "versions",
];

/// Authorizes one signing request against a table scope. See the module
/// docs for the policy; errors carry the reason for the audit row.
///
/// `allowed_endpoints` is the warehouse's storage-endpoint authority list
/// (`host[:port]`, from its `endpoint` / `endpoint.external` options).
/// When empty (no explicit endpoint: real AWS), only `*.amazonaws.com`
/// hosts are signable.
///
/// # Errors
///
/// Returns [`SignDenial`] when any policy point fails.
pub fn authorize_sign_request(
    scope: &TableScope,
    access: AccessMode,
    allowed_endpoints: &[String],
    request: &SignContext<'_>,
) -> Result<AuthorizedSign, SignDenial> {
    let method = request.method.to_ascii_uppercase();
    match (method.as_str(), access) {
        ("GET" | "HEAD", _) | ("PUT" | "POST" | "DELETE", AccessMode::ReadWrite) => {}
        ("PUT" | "POST" | "DELETE", AccessMode::Read) => {
            return Err(SignDenial::new(format!(
                "method {method} requires WRITE or COMMIT on the table; caller has READ only"
            )));
        }
        _ => {
            return Err(SignDenial::new(format!(
                "method {method:?} is never remotely signed"
            )));
        }
    }

    let uri: http::Uri = request.uri.parse().map_err(|_| {
        SignDenial::new(format!("request uri {:?} is not a valid URI", request.uri))
    })?;
    let key = resolve_object_key(scope, allowed_endpoints, &uri)?;
    let query = parse_query(uri.query().unwrap_or(""));
    for (name, _) in &query {
        if DENIED_SUBRESOURCES.contains(&name.as_str()) {
            return Err(SignDenial::new(format!(
                "subresource {name:?} is never remotely signed"
            )));
        }
    }

    if key.is_empty() {
        return authorize_bucket_root(scope, &method, &query, request.body);
    }

    if !key_in_scope(&key, &scope.key_prefix) {
        return Err(SignDenial::new(format!(
            "object key {key:?} is outside the table prefix {:?}",
            scope.key_prefix
        )));
    }

    // CopyObject / UploadPartCopy read their source with the *signing*
    // credentials, so the source must be inside the table too.
    check_copy_source(scope, request.headers)?;

    let action = match method.as_str() {
        "GET" => "get-object",
        "HEAD" => "head-object",
        "PUT" => "put-object",
        "DELETE" => "delete-object",
        "POST" => {
            // Only the multipart-upload lifecycle POSTs to an object key.
            if !query
                .iter()
                .any(|(name, _)| name == "uploads" || name == "uploadId")
            {
                return Err(SignDenial::new(
                    "POST to an object is only signed for multipart uploads \
                     (?uploads / ?uploadId)",
                ));
            }
            "post-object"
        }
        // Unreachable (methods are vetted first); deny rather than panic.
        other => {
            return Err(SignDenial::new(format!(
                "method {other:?} is never remotely signed"
            )));
        }
    };
    Ok(AuthorizedSign {
        action,
        keys: vec![key],
        cacheable: matches!(method.as_str(), "GET" | "HEAD"),
    })
}

/// Resolves a request URI to the percent-decoded object key it addresses,
/// verifying scheme, endpoint host (no signing oracle for arbitrary
/// hosts), bucket (path-style or virtual-host), and traversal-freeness.
fn resolve_object_key(
    scope: &TableScope,
    allowed_endpoints: &[String],
    uri: &http::Uri,
) -> Result<String, SignDenial> {
    let scheme = uri.scheme_str().unwrap_or_default().to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return Err(SignDenial::new(format!(
            "request uri scheme {scheme:?} is not http(s)"
        )));
    }
    let authority = normalized_authority(uri, &scheme)
        .ok_or_else(|| SignDenial::new("request uri has no host"))?;

    // Which addressing style is this, and is the host one of ours?
    let bucket_host = format!("{}.", scope.bucket.to_ascii_lowercase());
    let (virtual_host, base_authority) = match authority.strip_prefix(&bucket_host) {
        Some(rest) => (true, rest.to_owned()),
        None => (false, authority.clone()),
    };
    let host_allowed = if allowed_endpoints.is_empty() {
        host_of(&base_authority).ends_with(".amazonaws.com")
    } else {
        allowed_endpoints
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(&base_authority))
    };
    if !host_allowed {
        return Err(SignDenial::new(format!(
            "request host {authority:?} is not a storage endpoint of this warehouse"
        )));
    }

    // Path-style requests carry the bucket as the first path segment;
    // virtual-host requests carry it in the host.
    let path = uri.path();
    let raw_key = if virtual_host {
        path.strip_prefix('/').unwrap_or(path)
    } else {
        let rest = path
            .strip_prefix('/')
            .and_then(|rest| {
                rest.strip_prefix(&scope.bucket).filter(|_| {
                    rest.len() == scope.bucket.len()
                        || rest.as_bytes().get(scope.bucket.len()) == Some(&b'/')
                })
            })
            .ok_or_else(|| {
                SignDenial::new(format!(
                    "request path {path:?} does not address bucket {:?}",
                    scope.bucket
                ))
            })?;
        rest.strip_prefix('/').unwrap_or(rest)
    };
    let key = decode_component(raw_key);
    reject_traversal(&key)?;
    Ok(key)
}

/// Bucket-root requests: listings under the table prefix, and
/// `DeleteObjects` with every body key under the table prefix.
fn authorize_bucket_root(
    scope: &TableScope,
    method: &str,
    query: &[(String, String)],
    body: Option<&str>,
) -> Result<AuthorizedSign, SignDenial> {
    match method {
        "GET"
            if query
                .iter()
                .all(|(name, _)| LIST_QUERY_KEYS.contains(&name.as_str())) =>
        {
            let prefix = query
                .iter()
                .find(|(name, _)| name == "prefix")
                .map(|(_, value)| value.as_str())
                .ok_or_else(|| {
                    SignDenial::new(
                        "bucket listing without a prefix parameter would expose the whole bucket",
                    )
                })?;
            reject_traversal(prefix)?;
            if !key_in_scope(prefix.trim_end_matches('/'), &scope.key_prefix) {
                return Err(SignDenial::new(format!(
                    "list prefix {prefix:?} is outside the table prefix {:?}",
                    scope.key_prefix
                )));
            }
            Ok(AuthorizedSign {
                action: "list-objects",
                keys: vec![prefix.to_owned()],
                cacheable: false,
            })
        }
        "POST" if query.len() == 1 && query[0].0 == "delete" => {
            let body = body.ok_or_else(|| {
                SignDenial::new("DeleteObjects requires the request body for key validation")
            })?;
            let keys = delete_objects_keys(body)?;
            for key in &keys {
                reject_traversal(key)?;
                if !key_in_scope(key, &scope.key_prefix) {
                    return Err(SignDenial::new(format!(
                        "DeleteObjects key {key:?} is outside the table prefix {:?}",
                        scope.key_prefix
                    )));
                }
            }
            Ok(AuthorizedSign {
                action: "delete-objects",
                keys,
                cacheable: false,
            })
        }
        _ => Err(SignDenial::new(format!(
            "bucket-level {method} request is not signed: only scoped listings \
             and DeleteObjects are supported"
        ))),
    }
}

/// Whether `key` is the table prefix itself or strictly under it.
fn key_in_scope(key: &str, prefix: &str) -> bool {
    key == prefix
        || key
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// Denies keys containing `.`/`..` path segments: S3 keys never need them,
/// and any path-normalizing hop between engine and store would turn them
/// into an escape from the prefix.
fn reject_traversal(key: &str) -> Result<(), SignDenial> {
    if key
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return Err(SignDenial::new(format!(
            "object key {key:?} contains path-traversal segments"
        )));
    }
    Ok(())
}

/// Validates `x-amz-copy-source` (`CopyObject` / `UploadPartCopy`): the source
/// object is read with the signing credentials, so it must live inside the
/// table prefix like everything else.
fn check_copy_source(
    scope: &TableScope,
    headers: &BTreeMap<String, Vec<String>>,
) -> Result<(), SignDenial> {
    let values: Vec<&String> = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("x-amz-copy-source"))
        .flat_map(|(_, values)| values)
        .collect();
    let [value] = values.as_slice() else {
        return if values.is_empty() {
            Ok(())
        } else {
            Err(SignDenial::new("multiple x-amz-copy-source headers"))
        };
    };
    // Forms: "bucket/key", "/bucket/key", optionally "?versionId=...".
    let path = value.split_once('?').map_or(value.as_str(), |(p, _)| p);
    let path = path.strip_prefix('/').unwrap_or(path);
    let (bucket, raw_key) = path
        .split_once('/')
        .ok_or_else(|| SignDenial::new(format!("malformed x-amz-copy-source {value:?}")))?;
    let key = decode_component(raw_key);
    reject_traversal(&key)?;
    if !bucket.eq_ignore_ascii_case(&scope.bucket) || !key_in_scope(&key, &scope.key_prefix) {
        return Err(SignDenial::new(format!(
            "x-amz-copy-source {value:?} is outside the table prefix {:?}",
            scope.key_prefix
        )));
    }
    Ok(())
}

/// The URI authority with a default port stripped, lower-cased.
fn normalized_authority(uri: &http::Uri, scheme: &str) -> Option<String> {
    let host = uri.host()?.to_ascii_lowercase();
    match (uri.port_u16(), scheme) {
        (None, _) | (Some(80), "http") | (Some(443), "https") => Some(host),
        (Some(port), _) => Some(format!("{host}:{port}")),
    }
}

/// The host part of a `host[:port]` authority.
fn host_of(authority: &str) -> &str {
    authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host)
}

/// Splits and percent-decodes a query string into (key, value) pairs.
fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode_component(name), decode_component(value))
        })
        .collect()
}

/// Percent-decodes one URI component the way S3 will see it. Lenient on
/// malformed escapes (kept literal — S3 does the same), strict on UTF-8
/// (non-UTF-8 keys are not comparable to the prefix and are denied by the
/// scope check via lossy replacement).
fn decode_component(raw: &str) -> String {
    match percent_encoding::percent_decode_str(raw).decode_utf8() {
        Ok(decoded) => decoded.into_owned(),
        Err(_) => Cow::Borrowed("\u{FFFD}").into_owned(),
    }
}

/// Extracts every `<Key>` from a `DeleteObjects` XML body. Deliberately
/// minimal: AWS SDKs serialize exactly `<Key>...</Key>` with the five XML
/// entities; anything the extractor cannot account for is a deny (the
/// caller treats an error as such), never a silent skip.
fn delete_objects_keys(body: &str) -> Result<Vec<String>, SignDenial> {
    let mut keys = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find("<Key>") {
        rest = &rest[start + "<Key>".len()..];
        let end = rest
            .find("</Key>")
            .ok_or_else(|| SignDenial::new("malformed DeleteObjects body: unterminated <Key>"))?;
        keys.push(xml_unescape(&rest[..end])?);
        rest = &rest[end + "</Key>".len()..];
    }
    if keys.is_empty() {
        return Err(SignDenial::new(
            "DeleteObjects body contains no <Key> entries",
        ));
    }
    Ok(keys)
}

/// Resolves the standard XML entities (named and numeric). Unknown
/// entities are an error: guessing at a key would defeat the scope check.
fn xml_unescape(raw: &str) -> Result<String, SignDenial> {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(start) = rest.find('&') {
        out.push_str(&rest[..start]);
        rest = &rest[start..];
        let end = rest
            .find(';')
            .ok_or_else(|| SignDenial::new("malformed XML entity in DeleteObjects key"))?;
        let entity = &rest[1..end];
        let resolved = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => entity
                .strip_prefix("#x")
                .or_else(|| entity.strip_prefix("#X"))
                .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                .or_else(|| {
                    entity
                        .strip_prefix('#')
                        .and_then(|dec| dec.parse::<u32>().ok())
                })
                .and_then(char::from_u32),
        };
        match resolved {
            Some(c) => out.push(c),
            None => {
                return Err(SignDenial::new(format!(
                    "unsupported XML entity &{entity}; in DeleteObjects key"
                )));
            }
        }
        rest = &rest[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> TableScope {
        TableScope::from_s3_location("s3://warehouse/wh1/ns/orders-0195e").expect("scope")
    }

    fn endpoints() -> Vec<String> {
        vec!["localhost:9000".to_owned()]
    }

    fn ctx<'a>(
        method: &'a str,
        uri: &'a str,
        headers: &'a BTreeMap<String, Vec<String>>,
    ) -> SignContext<'a> {
        SignContext {
            method,
            uri,
            headers,
            body: None,
        }
    }

    fn allow(method: &str, uri: &str, access: AccessMode) -> AuthorizedSign {
        let headers = BTreeMap::new();
        authorize_sign_request(&scope(), access, &endpoints(), &ctx(method, uri, &headers))
            .expect("must allow")
    }

    fn deny(method: &str, uri: &str, access: AccessMode) -> SignDenial {
        let headers = BTreeMap::new();
        authorize_sign_request(&scope(), access, &endpoints(), &ctx(method, uri, &headers))
            .expect_err("must deny")
    }

    #[test]
    fn path_style_object_reads_allow() {
        let signed = allow(
            "GET",
            "http://localhost:9000/warehouse/wh1/ns/orders-0195e/data/f1.parquet",
            AccessMode::Read,
        );
        assert_eq!(signed.action, "get-object");
        assert_eq!(signed.keys, ["wh1/ns/orders-0195e/data/f1.parquet"]);
        assert!(signed.cacheable);
        assert!(!allow(
            "HEAD",
            "http://localhost:9000/warehouse/wh1/ns/orders-0195e/metadata/v1.metadata.json?versionId=3",
            AccessMode::Read,
        ).keys.is_empty());
    }

    #[test]
    fn virtual_host_addressing_allows_and_checks_base_host() {
        let signed = allow(
            "GET",
            "http://warehouse.localhost:9000/wh1/ns/orders-0195e/data/f1.parquet",
            AccessMode::Read,
        );
        assert_eq!(signed.keys, ["wh1/ns/orders-0195e/data/f1.parquet"]);
        // Same shape against a host that is not our endpoint: no signing
        // oracle for arbitrary hosts.
        let denial = deny(
            "GET",
            "http://warehouse.evil.example/wh1/ns/orders-0195e/data/f1.parquet",
            AccessMode::Read,
        );
        assert!(
            denial.reason().contains("not a storage endpoint"),
            "{denial}"
        );
    }

    #[test]
    fn aws_hosts_allowed_only_without_explicit_endpoint() {
        let headers = BTreeMap::new();
        let request = ctx(
            "GET",
            "https://warehouse.s3.us-east-1.amazonaws.com/wh1/ns/orders-0195e/data/f1.parquet",
            &headers,
        );
        assert!(authorize_sign_request(&scope(), AccessMode::Read, &[], &request).is_ok());
        assert!(
            authorize_sign_request(&scope(), AccessMode::Read, &endpoints(), &request).is_err()
        );
        // Bare "...amazonaws.com.evil.example" must not pass the suffix check.
        let request = ctx(
            "GET",
            "https://warehouse.s3.amazonaws.com.evil.example/wh1/ns/orders-0195e/data/f1.parquet",
            &headers,
        );
        assert!(authorize_sign_request(&scope(), AccessMode::Read, &[], &request).is_err());
    }

    #[test]
    fn default_ports_normalize() {
        let headers = BTreeMap::new();
        let request = ctx(
            "GET",
            "http://minio.internal:80/warehouse/wh1/ns/orders-0195e/data/f1.parquet",
            &headers,
        );
        let allowed = vec!["minio.internal".to_owned()];
        assert!(authorize_sign_request(&scope(), AccessMode::Read, &allowed, &request).is_ok());
    }

    #[test]
    fn sibling_tables_bucket_root_and_other_buckets_deny() {
        for uri in [
            // Sibling table.
            "http://localhost:9000/warehouse/wh1/ns/other-0196f/data/f1.parquet",
            // Prefix is a string prefix but not a path prefix.
            "http://localhost:9000/warehouse/wh1/ns/orders-0195e2/data/f1.parquet",
            // Bucket root / bare bucket.
            "http://localhost:9000/warehouse",
            "http://localhost:9000/warehouse/",
            // The prefix itself as a bucket in another bucket.
            "http://localhost:9000/other-bucket/wh1/ns/orders-0195e/data/f1.parquet",
        ] {
            let denial = deny("GET", uri, AccessMode::Read);
            assert!(!denial.reason().is_empty(), "{uri}");
        }
    }

    #[test]
    fn traversal_and_encoded_traversal_deny() {
        for uri in [
            "http://localhost:9000/warehouse/wh1/ns/orders-0195e/../secrets/f",
            "http://localhost:9000/warehouse/wh1/ns/orders-0195e/%2e%2e/secrets/f",
            "http://localhost:9000/warehouse/wh1/ns/orders-0195e/data/%2E%2E%2Fescape",
            "http://localhost:9000/warehouse/wh1/ns/orders-0195e/./f",
        ] {
            let denial = deny("GET", uri, AccessMode::Read);
            assert!(denial.reason().contains("traversal"), "{uri}: {denial}");
        }
    }

    #[test]
    fn encoded_slashes_decode_before_the_scope_check() {
        // %2F decodes to a real slash: same object S3-side, so in-scope.
        let signed = allow(
            "GET",
            "http://localhost:9000/warehouse/wh1%2Fns%2Forders-0195e%2Fdata%2Ff1.parquet",
            AccessMode::Read,
        );
        assert_eq!(signed.keys, ["wh1/ns/orders-0195e/data/f1.parquet"]);
        // ...and an encoded escape out of the prefix is still an escape.
        deny(
            "GET",
            "http://localhost:9000/warehouse/wh1/ns/orders-0195e%2F..%2F..%2Fother",
            AccessMode::Read,
        );
    }

    #[test]
    fn method_policy_follows_access() {
        let uri = "http://localhost:9000/warehouse/wh1/ns/orders-0195e/data/f1.parquet";
        for method in ["PUT", "DELETE"] {
            let denial = deny(method, uri, AccessMode::Read);
            assert!(denial.reason().contains("READ only"), "{denial}");
            let signed = allow(method, uri, AccessMode::ReadWrite);
            assert!(!signed.cacheable);
        }
        deny("PATCH", uri, AccessMode::ReadWrite);
        deny("OPTIONS", uri, AccessMode::ReadWrite);
    }

    #[test]
    fn multipart_posts_allow_other_object_posts_deny() {
        let base = "http://localhost:9000/warehouse/wh1/ns/orders-0195e/data/f1.parquet";
        assert_eq!(
            allow("POST", &format!("{base}?uploads"), AccessMode::ReadWrite).action,
            "post-object"
        );
        allow(
            "POST",
            &format!("{base}?uploadId=abc"),
            AccessMode::ReadWrite,
        );
        deny("POST", base, AccessMode::ReadWrite);
    }

    #[test]
    fn governance_subresources_deny() {
        let base = "http://localhost:9000/warehouse/wh1/ns/orders-0195e/data/f1.parquet";
        for sub in ["acl", "tagging", "retention", "legal-hold"] {
            let denial = deny("PUT", &format!("{base}?{sub}"), AccessMode::ReadWrite);
            assert!(denial.reason().contains("subresource"), "{denial}");
        }
        deny("GET", &format!("{base}?acl"), AccessMode::Read);
    }

    #[test]
    fn listings_require_an_in_scope_prefix() {
        let signed = allow(
            "GET",
            "http://localhost:9000/warehouse?list-type=2&prefix=wh1%2Fns%2Forders-0195e%2Fdata%2F",
            AccessMode::Read,
        );
        assert_eq!(signed.action, "list-objects");
        assert_eq!(signed.keys, ["wh1/ns/orders-0195e/data/"]);
        // Exactly the prefix (no trailing slash) is fine too.
        allow(
            "GET",
            "http://localhost:9000/warehouse?prefix=wh1/ns/orders-0195e",
            AccessMode::Read,
        );
        // Missing, sibling, or short prefixes are not.
        deny(
            "GET",
            "http://localhost:9000/warehouse?list-type=2",
            AccessMode::Read,
        );
        deny(
            "GET",
            "http://localhost:9000/warehouse?prefix=wh1/ns/",
            AccessMode::Read,
        );
        deny(
            "GET",
            "http://localhost:9000/warehouse?prefix=wh1/ns/other-0196f/",
            AccessMode::Read,
        );
        // A listing riding along unknown query keys is denied.
        deny(
            "GET",
            "http://localhost:9000/warehouse?prefix=wh1/ns/orders-0195e/&uploads",
            AccessMode::Read,
        );
    }

    #[test]
    fn delete_objects_validates_every_body_key() {
        let headers = BTreeMap::new();
        let uri = "http://localhost:9000/warehouse?delete";
        let ok_body = "<Delete><Object><Key>wh1/ns/orders-0195e/data/f1.parquet</Key></Object>\
                       <Object><Key>wh1/ns/orders-0195e/data/f&amp;2.parquet</Key></Object></Delete>";
        let request = SignContext {
            method: "POST",
            uri,
            headers: &headers,
            body: Some(ok_body),
        };
        let signed =
            authorize_sign_request(&scope(), AccessMode::ReadWrite, &endpoints(), &request)
                .expect("in-scope delete");
        assert_eq!(signed.action, "delete-objects");
        assert_eq!(signed.keys[1], "wh1/ns/orders-0195e/data/f&2.parquet");

        // One key outside the prefix poisons the whole request.
        let bad_body = "<Delete><Object><Key>wh1/ns/orders-0195e/data/f1.parquet</Key></Object>\
                        <Object><Key>wh1/ns/other-0196f/data/f1.parquet</Key></Object></Delete>";
        for (body, access) in [
            (Some(bad_body), AccessMode::ReadWrite),
            (None, AccessMode::ReadWrite),
            (Some(ok_body), AccessMode::Read),
            (Some("<Delete></Delete>"), AccessMode::ReadWrite),
            (
                Some("<Delete><Object><Key>wh1/ns/orders-0195e/../x</Key></Object></Delete>"),
                AccessMode::ReadWrite,
            ),
        ] {
            let request = SignContext {
                method: "POST",
                uri,
                headers: &headers,
                body,
            };
            assert!(
                authorize_sign_request(&scope(), access, &endpoints(), &request).is_err(),
                "body {body:?} access {access:?}"
            );
        }
    }

    #[test]
    fn copy_source_must_stay_inside_the_table() {
        let uri = "http://localhost:9000/warehouse/wh1/ns/orders-0195e/data/copy.parquet";
        let mut headers = BTreeMap::new();
        headers.insert(
            "x-amz-copy-source".to_owned(),
            vec!["/warehouse/wh1/ns/orders-0195e/data/f1.parquet".to_owned()],
        );
        let request = SignContext {
            method: "PUT",
            uri,
            headers: &headers,
            body: None,
        };
        assert!(
            authorize_sign_request(&scope(), AccessMode::ReadWrite, &endpoints(), &request).is_ok()
        );

        for source in [
            "warehouse/wh1/ns/other-0196f/data/f1.parquet",
            "other-bucket/wh1/ns/orders-0195e/data/f1.parquet",
            "/warehouse/wh1/ns/orders-0195e/%2E%2E/x",
        ] {
            let mut headers = BTreeMap::new();
            headers.insert("x-amz-copy-source".to_owned(), vec![source.to_owned()]);
            let request = SignContext {
                method: "PUT",
                uri,
                headers: &headers,
                body: None,
            };
            assert!(
                authorize_sign_request(&scope(), AccessMode::ReadWrite, &endpoints(), &request)
                    .is_err(),
                "source {source:?}"
            );
        }
    }

    #[test]
    fn non_http_schemes_and_hostless_uris_deny() {
        deny(
            "GET",
            "ftp://localhost:9000/warehouse/wh1/ns/orders-0195e/f",
            AccessMode::Read,
        );
        deny("GET", "/warehouse/wh1/ns/orders-0195e/f", AccessMode::Read);
        deny("GET", "not a uri", AccessMode::Read);
    }
}
