//! Minimal Iceberg REST catalog client for the benchmark scenarios.
//!
//! Deliberately spec-shaped and vendor-neutral: the prefix used in IRC
//! paths is resolved from `GET /v1/config` (`overrides.prefix`, falling
//! back to `defaults.prefix`, falling back to the warehouse name), which
//! is how conformant clients behave. Some catalogs use the warehouse name
//! as the prefix, others return an opaque identifier.

use serde_json::{Value, json};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

/// Fetches an `OAuth2` access token via the client-credentials grant.
///
/// Token acquisition happens once, before any timed request; the returned
/// bearer is reused for the whole run so auth round-trips never appear in
/// the measured path.
pub(crate) async fn fetch_oauth2_token(
    http: &reqwest::Client,
    token_url: &str,
    client_id: &str,
    client_secret: &str,
    scope: &str,
) -> Result<String> {
    let resp = http
        .post(token_url)
        .basic_auth(client_id, Some(client_secret))
        .form(&[("grant_type", "client_credentials"), ("scope", scope)])
        .send()
        .await?;
    let status = resp.status();
    let body: Value = resp.json().await?;
    if !status.is_success() {
        return Err(format!("token endpoint returned {status}: {body}").into());
    }
    body.get("access_token")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "token response had no access_token".into())
}

/// One IRC endpoint under test.
#[derive(Debug, Clone)]
pub(crate) struct Catalog {
    http: reqwest::Client,
    /// IRC base, e.g. `http://localhost:8181/iceberg` (no trailing `/v1`).
    base: String,
    /// Warehouse name passed to `GET /v1/config`.
    warehouse: String,
    /// Path prefix resolved from the config endpoint.
    prefix: String,
    /// Pre-fetched bearer token, if the catalog requires auth.
    bearer: Option<String>,
}

impl Catalog {
    /// Connects and resolves the IRC path prefix from `/v1/config`.
    pub(crate) async fn connect(
        http: reqwest::Client,
        base: &str,
        warehouse: &str,
        bearer: Option<String>,
    ) -> Result<Self> {
        let mut catalog = Self {
            http,
            base: base.trim_end_matches('/').to_owned(),
            warehouse: warehouse.to_owned(),
            prefix: warehouse.to_owned(),
            bearer,
        };
        let config: Value = catalog
            .expect_2xx(catalog.get(&catalog.config_url()), "GET /v1/config")
            .await?;
        if let Some(p) = config
            .pointer("/overrides/prefix")
            .or_else(|| config.pointer("/defaults/prefix"))
            .and_then(Value::as_str)
        {
            p.clone_into(&mut catalog.prefix);
        }
        Ok(catalog)
    }

    /// The `GET /v1/config?warehouse=` URL for this catalog.
    pub(crate) fn config_url(&self) -> String {
        format!("{}/v1/config?warehouse={}", self.base, self.warehouse)
    }

    /// The load/commit URL for a table.
    pub(crate) fn table_url(&self, namespace: &str, table: &str) -> String {
        format!(
            "{}/v1/{}/namespaces/{namespace}/tables/{table}",
            self.base, self.prefix
        )
    }

    /// Resolved IRC path prefix.
    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    /// A GET request builder with auth applied.
    pub(crate) fn get(&self, url: &str) -> reqwest::RequestBuilder {
        self.authed(self.http.get(url))
    }

    /// A POST request builder with auth and a JSON body applied.
    pub(crate) fn post_json(&self, url: &str, body: &Value) -> reqwest::RequestBuilder {
        self.authed(self.http.post(url).json(body))
    }

    fn authed(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.bearer {
            Some(token) => rb.bearer_auth(token),
            None => rb,
        }
    }

    async fn expect_2xx(&self, rb: reqwest::RequestBuilder, what: &str) -> Result<Value> {
        let resp = rb.send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("{what} returned {status}: {text}").into());
        }
        if text.is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_str(&text)?)
    }

    /// Creates the benchmark namespace + table and layers `snapshots`
    /// fabricated append snapshots on top via sequential commits, so
    /// `loadTable` serves realistically sized metadata. Drops any previous
    /// table of the same name first.
    pub(crate) async fn setup_fixture(
        &self,
        namespace: &str,
        table: &str,
        columns: u32,
        snapshots: u64,
    ) -> Result<()> {
        // Namespace: tolerate AlreadyExists.
        let resp = self
            .post_json(
                &format!("{}/v1/{}/namespaces", self.base, self.prefix),
                &json!({"namespace": [namespace]}),
            )
            .send()
            .await?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::CONFLICT {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("create namespace returned {status}: {text}").into());
        }

        // Drop a stale fixture table so every run starts from identical state.
        let resp = self
            .authed(self.http.delete(format!(
                "{}?purgeRequested=false",
                self.table_url(namespace, table)
            )))
            .send()
            .await?;
        if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("drop table returned {status}: {text}").into());
        }

        let created: Value = self
            .expect_2xx(
                self.post_json(
                    &format!(
                        "{}/v1/{}/namespaces/{namespace}/tables",
                        self.base, self.prefix
                    ),
                    &json!({"name": table, "schema": wide_schema(columns)}),
                ),
                "create table",
            )
            .await?;
        let location = created
            .pointer("/metadata/location")
            .and_then(Value::as_str)
            .ok_or("create table response had no metadata.location")?
            .to_owned();

        // Fabricated append snapshots: metadata-level commits only. The
        // manifest-list files are never read by the catalog on the commit
        // or load path, so paths under the table location are sufficient
        // to grow the metadata realistically.
        let base_ts = chrono::Utc::now().timestamp_millis();
        let mut parent: Option<u64> = None;
        for i in 1..=snapshots {
            let snapshot_id = 3_000_000_000 + i;
            let mut snapshot = json!({
                "snapshot-id": snapshot_id,
                "sequence-number": i,
                "timestamp-ms": base_ts + i64::try_from(i)?,
                "manifest-list": format!("{location}/metadata/snap-{i}.avro"),
                "summary": {
                    "operation": "append",
                    "added-data-files": "4",
                    "added-records": "100000",
                    "added-files-size": "13421772"
                },
                "schema-id": 0
            });
            if let Some(p) = parent {
                snapshot["parent-snapshot-id"] = json!(p);
            }
            self.expect_2xx(
                self.post_json(
                    &self.table_url(namespace, table),
                    &json!({
                        "requirements": [],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": snapshot},
                            {
                                "action": "set-snapshot-ref",
                                "ref-name": "main",
                                "type": "branch",
                                "snapshot-id": snapshot_id
                            }
                        ]
                    }),
                ),
                &format!("add-snapshot commit {i}"),
            )
            .await?;
            parent = Some(snapshot_id);
        }
        Ok(())
    }
}

/// A flat schema with `columns` fields of mixed primitive types.
fn wide_schema(columns: u32) -> Value {
    const TYPES: [&str; 10] = [
        "long",
        "string",
        "double",
        "timestamptz",
        "boolean",
        "date",
        "int",
        "float",
        "decimal(18, 2)",
        "uuid",
    ];
    let fields: Vec<Value> = (1..=columns)
        .map(|id| {
            json!({
                "id": id,
                "name": format!("col_{id:02}"),
                "required": id == 1,
                "type": TYPES[(id as usize - 1) % TYPES.len()],
            })
        })
        .collect();
    json!({"type": "struct", "fields": fields})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wide_schema_has_requested_arity() {
        let schema = wide_schema(40);
        let fields = schema["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 40);
        // Field ids are unique and 1-based.
        assert_eq!(fields[0]["id"], 1);
        assert_eq!(fields[39]["id"], 40);
        // Only the first column is required.
        assert_eq!(fields[0]["required"], true);
        assert_eq!(fields[1]["required"], false);
    }
}
