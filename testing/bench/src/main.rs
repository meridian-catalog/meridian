//! `meridian-bench`: a small, vendor-neutral benchmark harness for Iceberg
//! REST catalog (IRC) servers.
//!
//! Scenarios (all closed-loop, per-request latency into an HDR histogram):
//!
//! - `get-config`   — `GET /v1/config?warehouse=…`
//! - `load-table`   — `GET …/tables/{table}` against a wide, multi-snapshot
//!   fixture table, swept over several concurrency levels
//! - `commit`       — sequential `set-properties` commits (`POST …/tables/{table}`)
//!
//! Auth is pluggable: `--auth none` or `--auth oauth2` (client-credentials
//! token fetched once, before any timed request).

mod catalog;
mod runner;
mod stats;

use std::process::ExitCode;

use clap::Parser;
use serde::Serialize;
use serde_json::json;

use catalog::Catalog;
use stats::ScenarioResult;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, clap::ValueEnum)]
enum AuthMode {
    /// No Authorization header (catalogs running with auth disabled).
    None,
    /// `OAuth2` client-credentials: fetch a bearer token once, up front.
    Oauth2,
}

#[derive(Debug, Parser)]
#[command(
    name = "meridian-bench",
    about = "Benchmark harness for Iceberg REST catalogs"
)]
struct Args {
    /// Label for this catalog in reports (e.g. "meridian", "polaris").
    #[arg(long)]
    catalog_name: String,

    /// IRC base URL without the /v1 suffix, e.g. <http://localhost:8181/iceberg>
    #[arg(long)]
    base_url: String,

    /// Warehouse name passed to GET /v1/config (the IRC path prefix is
    /// resolved from the response).
    #[arg(long)]
    warehouse: String,

    #[arg(long, value_enum, default_value = "none")]
    auth: AuthMode,

    /// `OAuth2` token endpoint (required with --auth oauth2).
    #[arg(long)]
    token_url: Option<String>,

    #[arg(long)]
    client_id: Option<String>,

    #[arg(long)]
    client_secret: Option<String>,

    #[arg(long, default_value = "PRINCIPAL_ROLE:ALL")]
    scope: String,

    /// Create the fixture (namespace + wide table + snapshot history),
    /// dropping any previous fixture table first.
    #[arg(long)]
    setup: bool,

    /// Comma-separated scenario list.
    #[arg(long, value_delimiter = ',', default_values_t = [
        "get-config".to_owned(), "load-table".to_owned(), "commit".to_owned()
    ])]
    scenarios: Vec<String>,

    #[arg(long, default_value = "bench_ns")]
    namespace: String,

    #[arg(long, default_value = "bench_wide")]
    table: String,

    /// Fixture width (columns) created by --setup.
    #[arg(long, default_value_t = 40)]
    columns: u32,

    /// Fixture snapshot count created by --setup.
    #[arg(long, default_value_t = 20)]
    snapshots: u64,

    /// Measured loadTable requests per concurrency level.
    #[arg(long, default_value_t = 2000)]
    load_n: u64,

    /// Warm-up loadTable requests (excluded) per concurrency level.
    #[arg(long, default_value_t = 100)]
    load_warmup: u64,

    /// Concurrency sweep for loadTable.
    #[arg(long, value_delimiter = ',', default_values_t = [1, 8, 32])]
    load_concurrency: Vec<usize>,

    /// Measured sequential set-properties commits.
    #[arg(long, default_value_t = 200)]
    commit_n: u64,

    /// Warm-up commits (excluded).
    #[arg(long, default_value_t = 20)]
    commit_warmup: u64,

    /// Measured getConfig requests.
    #[arg(long, default_value_t = 2000)]
    config_n: u64,

    /// Warm-up getConfig requests (excluded).
    #[arg(long, default_value_t = 100)]
    config_warmup: u64,

    /// Write the JSON report here.
    #[arg(long)]
    out: Option<std::path::PathBuf>,

    /// Write the markdown table here.
    #[arg(long)]
    markdown: Option<std::path::PathBuf>,
}

#[derive(Debug, Serialize)]
struct Report {
    catalog: String,
    base_url: String,
    warehouse: String,
    prefix: String,
    auth: String,
    timestamp: String,
    harness_version: String,
    results: Vec<ScenarioResult>,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime construction");
    match runtime.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn run(args: Args) -> Result<()> {
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .pool_max_idle_per_host(64)
        .build()?;

    let bearer = match args.auth {
        AuthMode::None => None,
        AuthMode::Oauth2 => {
            let token_url = args
                .token_url
                .as_deref()
                .ok_or("--token-url is required with --auth oauth2")?;
            let client_id = args
                .client_id
                .as_deref()
                .ok_or("--client-id is required with --auth oauth2")?;
            let client_secret = args
                .client_secret
                .as_deref()
                .ok_or("--client-secret is required with --auth oauth2")?;
            // Fetched once, outside every timed window.
            Some(
                catalog::fetch_oauth2_token(
                    &http,
                    token_url,
                    client_id,
                    client_secret,
                    &args.scope,
                )
                .await?,
            )
        }
    };

    let cat = Catalog::connect(http, &args.base_url, &args.warehouse, bearer).await?;
    eprintln!(
        "connected to {} (warehouse={}, prefix={})",
        args.base_url,
        args.warehouse,
        cat.prefix()
    );

    if args.setup {
        eprintln!(
            "setting up fixture {}.{} ({} columns, {} snapshots)…",
            args.namespace, args.table, args.columns, args.snapshots
        );
        cat.setup_fixture(&args.namespace, &args.table, args.columns, args.snapshots)
            .await?;
    }

    let mut results: Vec<ScenarioResult> = Vec::new();
    for scenario in &args.scenarios {
        match scenario.as_str() {
            "get-config" => {
                let raw = {
                    let cat = cat.clone();
                    runner::run(1, args.config_warmup, args.config_n, move |_| {
                        let cat = cat.clone();
                        async move { send_expect_2xx(cat.get(&cat.config_url())).await }
                    })
                    .await?
                };
                report_raw(&mut results, "get-config", 1, args.config_warmup, &raw);
            }
            "load-table" => {
                let url = cat.table_url(&args.namespace, &args.table);
                for &c in &args.load_concurrency {
                    let raw = {
                        let cat = cat.clone();
                        let url = url.clone();
                        runner::run(c, args.load_warmup, args.load_n, move |_| {
                            let cat = cat.clone();
                            let url = url.clone();
                            async move { send_expect_2xx(cat.get(&url)).await }
                        })
                        .await?
                    };
                    report_raw(&mut results, "load-table", c, args.load_warmup, &raw);
                }
            }
            "commit" => {
                let url = cat.table_url(&args.namespace, &args.table);
                let raw = {
                    let cat = cat.clone();
                    runner::run(1, args.commit_warmup, args.commit_n, move |i| {
                        let cat = cat.clone();
                        let url = url.clone();
                        async move {
                            let body = json!({
                                "requirements": [],
                                "updates": [{
                                    "action": "set-properties",
                                    "updates": {"bench.iter": i.to_string()}
                                }]
                            });
                            send_expect_2xx(cat.post_json(&url, &body)).await
                        }
                    })
                    .await?
                };
                report_raw(&mut results, "commit", 1, args.commit_warmup, &raw);
            }
            other => return Err(format!("unknown scenario: {other}").into()),
        }
    }

    let report = Report {
        catalog: args.catalog_name.clone(),
        base_url: args.base_url.clone(),
        warehouse: args.warehouse.clone(),
        prefix: cat.prefix().to_owned(),
        auth: format!("{:?}", args.auth).to_lowercase(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        harness_version: env!("CARGO_PKG_VERSION").to_owned(),
        results,
    };

    let json_text = serde_json::to_string_pretty(&report)?;
    let md_text = stats::markdown_table(&report.catalog, &report.results);
    if let Some(path) = &args.out {
        std::fs::write(path, &json_text)?;
        eprintln!("wrote {}", path.display());
    }
    if let Some(path) = &args.markdown {
        std::fs::write(path, &md_text)?;
        eprintln!("wrote {}", path.display());
    }
    println!("{md_text}");
    Ok(())
}

/// Sends a prepared request and maps the response to the runner's outcome.
async fn send_expect_2xx(rb: reqwest::RequestBuilder) -> std::result::Result<(), String> {
    match rb.send().await {
        Ok(resp) => {
            let status = resp.status();
            // Drain the body so the connection returns to the pool and the
            // full response transfer is inside the timed window.
            let body = resp.bytes().await;
            if status.is_success() {
                body.map(|_| ())
                    .map_err(|e| format!("body read failed: {e}"))
            } else {
                Err(format!("HTTP {status}"))
            }
        }
        Err(e) => Err(format!("transport: {e}")),
    }
}

fn report_raw(
    results: &mut Vec<ScenarioResult>,
    scenario: &str,
    concurrency: usize,
    warmup: u64,
    raw: &runner::RawRun,
) {
    let summary = stats::summarize(
        scenario,
        concurrency,
        &raw.hist,
        warmup,
        raw.errors,
        raw.measured_wall_secs,
    );
    eprintln!(
        "{scenario} c={concurrency}: n={} errors={} p50={:.2}ms p99={:.2}ms rps={:.0}",
        summary.measured_requests, summary.errors, summary.p50_ms, summary.p99_ms, summary.rps
    );
    for msg in &raw.error_samples {
        eprintln!("  error sample: {msg}");
    }
    results.push(summary);
}
