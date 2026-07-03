//! Thin HTTP client for the admin subcommands (`warehouse`, `namespace`).
//!
//! Talks plain JSON to a running Meridian server: the management API under
//! `/api/v2` and the Iceberg REST surface under `/v1`. Deliberately
//! dependency-light — reqwest plus `serde_json`, no generated SDK.

use std::fmt;
use std::time::Duration;

use serde_json::{Value, json};

/// The multi-level namespace separator used on the wire (`%1F` in URLs).
const UNIT_SEPARATOR: char = '\u{1f}';

/// A CLI-facing failure: a human-readable message, printed as-is.
#[derive(Debug)]
pub(crate) struct CliError(pub String);

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CliError {}

impl From<reqwest::Error> for CliError {
    fn from(error: reqwest::Error) -> Self {
        Self(format!("request failed: {error}"))
    }
}

/// Builds the shared HTTP client.
fn http_client() -> Result<reqwest::Client, CliError> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| CliError(format!("failed to build HTTP client: {e}")))
}

/// Attaches the optional bearer token (servers in `auth.mode = "oidc"`
/// require one).
fn with_token(request: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
    match token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

/// Normalizes the server base URL: strips a trailing slash and defaults the
/// scheme to `http://` when none is given, so `--server localhost:8181`
/// works instead of failing with an opaque URL-builder error.
fn base(server: &str) -> String {
    let trimmed = server.trim_end_matches('/');
    if trimmed.contains("://") {
        trimmed.to_owned()
    } else {
        format!("http://{trimmed}")
    }
}

/// Turns a non-success response into a readable error, using the server's
/// IRC error envelope when present.
async fn check(response: reqwest::Response) -> Result<reqwest::Response, CliError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body: Value = response.json().await.unwrap_or(Value::Null);
    let message = body
        .pointer("/error/message")
        .and_then(Value::as_str)
        .unwrap_or("no error message");
    let error_type = body
        .pointer("/error/type")
        .and_then(Value::as_str)
        .unwrap_or("unknown error type");
    Err(CliError(format!(
        "server returned {status} ({error_type}): {message}"
    )))
}

/// `POST /api/v2/warehouses`.
pub(crate) async fn warehouse_create(
    server: &str,
    token: Option<&str>,
    name: &str,
    storage_root: &str,
    storage_options: &[(String, String)],
) -> Result<Value, CliError> {
    let options: serde_json::Map<String, Value> = storage_options
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    let request = http_client()?
        .post(format!("{}/api/v2/warehouses", base(server)))
        .json(&json!({
            "name": name,
            "storage_root": storage_root,
            "storage_options": options,
        }));
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `GET /api/v2/warehouses`.
pub(crate) async fn warehouse_list(server: &str, token: Option<&str>) -> Result<Value, CliError> {
    let request = http_client()?.get(format!("{}/api/v2/warehouses", base(server)));
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `POST /v1/{prefix}/namespaces`.
pub(crate) async fn namespace_create(
    server: &str,
    token: Option<&str>,
    warehouse: &str,
    levels: &[String],
    properties: &[(String, String)],
) -> Result<Value, CliError> {
    let props: serde_json::Map<String, Value> = properties
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    let request = http_client()?
        .post(format!("{}/v1/{warehouse}/namespaces", base(server)))
        .json(&json!({ "namespace": levels, "properties": props }));
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `GET /v1/{prefix}/namespaces[?parent=...]`.
pub(crate) async fn namespace_list(
    server: &str,
    token: Option<&str>,
    warehouse: &str,
    parent: Option<&[String]>,
) -> Result<Value, CliError> {
    let mut request = http_client()?.get(format!("{}/v1/{warehouse}/namespaces", base(server)));
    if let Some(parent) = parent {
        let encoded: String = parent.join(&UNIT_SEPARATOR.to_string());
        request = request.query(&[("parent", encoded)]);
    }
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `GET /v1/{prefix}/namespaces/{namespace}/tables`.
pub(crate) async fn table_list(
    server: &str,
    warehouse: &str,
    namespace: &[String],
) -> Result<Value, CliError> {
    let ns = encode_namespace(namespace);
    let response = http_client()?
        .get(format!(
            "{}/v1/{warehouse}/namespaces/{ns}/tables",
            base(server)
        ))
        .send()
        .await?;
    Ok(check(response).await?.json().await?)
}

/// `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`.
pub(crate) async fn table_load(
    server: &str,
    warehouse: &str,
    namespace: &[String],
    table: &str,
) -> Result<Value, CliError> {
    let ns = encode_namespace(namespace);
    let response = http_client()?
        .get(format!(
            "{}/v1/{warehouse}/namespaces/{ns}/tables/{table}",
            base(server)
        ))
        .send()
        .await?;
    Ok(check(response).await?.json().await?)
}

/// Joins namespace levels with the URL-encoded unit separator.
fn encode_namespace(levels: &[String]) -> String {
    levels.join("%1F")
}

/// `GET /api/v2/search`.
pub(crate) async fn search(
    server: &str,
    token: Option<&str>,
    query: &str,
    warehouse: Option<&str>,
    kinds: Option<&str>,
    limit: i64,
) -> Result<Value, CliError> {
    let mut params: Vec<(&str, String)> =
        vec![("q", query.to_owned()), ("limit", limit.to_string())];
    if let Some(warehouse) = warehouse {
        params.push(("warehouse", warehouse.to_owned()));
    }
    if let Some(kinds) = kinds {
        params.push(("type", kinds.to_owned()));
    }
    let request = http_client()?
        .get(format!("{}/api/v2/search", base(server)))
        .query(&params);
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `GET /api/v2/roles`.
pub(crate) async fn role_list(server: &str, token: Option<&str>) -> Result<Value, CliError> {
    let request = http_client()?.get(format!("{}/api/v2/roles", base(server)));
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `POST /api/v2/roles`.
pub(crate) async fn role_create(
    server: &str,
    token: Option<&str>,
    name: &str,
    description: Option<&str>,
) -> Result<Value, CliError> {
    let request = http_client()?
        .post(format!("{}/api/v2/roles", base(server)))
        .json(&json!({ "name": name, "description": description }));
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `POST /api/v2/grants`.
pub(crate) async fn grant_add(
    server: &str,
    token: Option<&str>,
    body: &Value,
) -> Result<Value, CliError> {
    let request = http_client()?
        .post(format!("{}/api/v2/grants", base(server)))
        .json(body);
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `GET /api/v2/grants`.
pub(crate) async fn grant_list(server: &str, token: Option<&str>) -> Result<Value, CliError> {
    let request = http_client()?.get(format!("{}/api/v2/grants", base(server)));
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `GET /v1/{prefix}/namespaces/{namespace}` — load one namespace's
/// properties. Returns `Ok(None)` on 404 so callers can treat "absent" as a
/// normal planning outcome rather than an error.
pub(crate) async fn namespace_load(
    server: &str,
    token: Option<&str>,
    warehouse: &str,
    levels: &[String],
) -> Result<Option<Value>, CliError> {
    let ns = encode_namespace(levels);
    let request = http_client()?.get(format!("{}/v1/{warehouse}/namespaces/{ns}", base(server)));
    let response = with_token(request, token).send().await?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    Ok(Some(check(response).await?.json().await?))
}

/// `POST /v1/{prefix}/namespaces/{namespace}/properties` — set and/or remove
/// namespace properties atomically.
pub(crate) async fn namespace_update_properties(
    server: &str,
    token: Option<&str>,
    warehouse: &str,
    levels: &[String],
    updates: &[(String, String)],
    removals: &[String],
) -> Result<Value, CliError> {
    let updates_map: serde_json::Map<String, Value> = updates
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    let ns = encode_namespace(levels);
    let request = http_client()?
        .post(format!(
            "{}/v1/{warehouse}/namespaces/{ns}/properties",
            base(server)
        ))
        .json(&json!({ "updates": updates_map, "removals": removals }));
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `GET /api/v2/webhooks`.
pub(crate) async fn webhook_list(server: &str, token: Option<&str>) -> Result<Value, CliError> {
    let request = http_client()?.get(format!("{}/api/v2/webhooks", base(server)));
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `POST /api/v2/webhooks`.
pub(crate) async fn webhook_create(
    server: &str,
    token: Option<&str>,
    url: &str,
    event_types: &[String],
    secret: &str,
) -> Result<Value, CliError> {
    let request = http_client()?
        .post(format!("{}/api/v2/webhooks", base(server)))
        .json(&json!({
            "url": url,
            "event_types": event_types,
            "secret": secret,
        }));
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `GET /api/v2/events` — one page of the published event feed.
pub(crate) async fn events_list(
    server: &str,
    token: Option<&str>,
    after: &str,
    types: Option<&str>,
    limit: i64,
) -> Result<Value, CliError> {
    let mut query: Vec<(&str, String)> = vec![("limit", limit.to_string())];
    if !after.is_empty() {
        query.push(("after", after.to_owned()));
    }
    if let Some(types) = types {
        query.push(("types", types.to_owned()));
    }
    let request = http_client()?
        .get(format!("{}/api/v2/events", base(server)))
        .query(&query);
    let response = with_token(request, token).send().await?;
    Ok(check(response).await?.json().await?)
}

/// `DELETE /api/v2/grants/{id}`.
pub(crate) async fn grant_remove(
    server: &str,
    token: Option<&str>,
    id: &str,
) -> Result<(), CliError> {
    let request = http_client()?.delete(format!("{}/api/v2/grants/{id}", base(server)));
    let response = with_token(request, token).send().await?;
    check(response).await?;
    Ok(())
}

/// Renders rows as a plain aligned text table.
#[must_use]
pub(crate) fn render_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    let mut out = String::new();
    let render_row = |cells: &[String], widths: &[usize], out: &mut String| {
        for (i, cell) in cells.iter().enumerate() {
            if i > 0 {
                out.push_str("  ");
            }
            out.push_str(cell);
            if i + 1 < cells.len() {
                for _ in cell.len()..widths[i] {
                    out.push(' ');
                }
            }
        }
        out.push('\n');
    };

    let header_cells: Vec<String> = headers.iter().map(|h| (*h).to_owned()).collect();
    render_row(&header_cells, &widths, &mut out);
    let separators: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    render_row(&separators, &widths, &mut out);
    for row in rows {
        render_row(row, &widths, &mut out);
    }
    out
}

/// Parses a `key=value` argument.
pub(crate) fn parse_key_value(raw: &str) -> Result<(String, String), CliError> {
    raw.split_once('=')
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .filter(|(k, _)| !k.is_empty())
        .ok_or_else(|| CliError(format!("expected key=value, got {raw:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_key_value_accepts_and_rejects() {
        assert_eq!(
            parse_key_value("region=us-east-1").expect("valid pair"),
            ("region".to_owned(), "us-east-1".to_owned())
        );
        assert_eq!(
            parse_key_value("k=a=b").expect("valid pair"),
            ("k".to_owned(), "a=b".to_owned())
        );
        assert!(parse_key_value("novalue").is_err());
        assert!(parse_key_value("=v").is_err());
    }

    #[test]
    fn render_table_aligns_columns() {
        let out = render_table(
            &["NAME", "ROOT"],
            &[
                vec!["wh".to_owned(), "s3://b/x".to_owned()],
                vec!["warehouse-2".to_owned(), "s3://b".to_owned()],
            ],
        );
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 4);
        assert!(lines[0].starts_with("NAME"));
        assert!(lines[2].starts_with("wh "));
        assert!(lines[3].starts_with("warehouse-2  "));
    }
}
