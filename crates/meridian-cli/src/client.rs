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

/// Normalizes the server base URL (strips a trailing slash).
fn base(server: &str) -> &str {
    server.trim_end_matches('/')
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
    name: &str,
    storage_root: &str,
    storage_options: &[(String, String)],
) -> Result<Value, CliError> {
    let options: serde_json::Map<String, Value> = storage_options
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    let response = http_client()?
        .post(format!("{}/api/v2/warehouses", base(server)))
        .json(&json!({
            "name": name,
            "storage_root": storage_root,
            "storage_options": options,
        }))
        .send()
        .await?;
    Ok(check(response).await?.json().await?)
}

/// `GET /api/v2/warehouses`.
pub(crate) async fn warehouse_list(server: &str) -> Result<Value, CliError> {
    let response = http_client()?
        .get(format!("{}/api/v2/warehouses", base(server)))
        .send()
        .await?;
    Ok(check(response).await?.json().await?)
}

/// `POST /v1/{prefix}/namespaces`.
pub(crate) async fn namespace_create(
    server: &str,
    warehouse: &str,
    levels: &[String],
    properties: &[(String, String)],
) -> Result<Value, CliError> {
    let props: serde_json::Map<String, Value> = properties
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    let response = http_client()?
        .post(format!("{}/v1/{warehouse}/namespaces", base(server)))
        .json(&json!({ "namespace": levels, "properties": props }))
        .send()
        .await?;
    Ok(check(response).await?.json().await?)
}

/// `GET /v1/{prefix}/namespaces[?parent=...]`.
pub(crate) async fn namespace_list(
    server: &str,
    warehouse: &str,
    parent: Option<&[String]>,
) -> Result<Value, CliError> {
    let mut request = http_client()?.get(format!("{}/v1/{warehouse}/namespaces", base(server)));
    if let Some(parent) = parent {
        let encoded: String = parent.join(&UNIT_SEPARATOR.to_string());
        request = request.query(&[("parent", encoded)]);
    }
    let response = request.send().await?;
    Ok(check(response).await?.json().await?)
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
