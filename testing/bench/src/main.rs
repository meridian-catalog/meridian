//! `meridian-bench`: a small, vendor-neutral benchmark harness for Iceberg
//! REST catalog (IRC) servers.
//!
//! Scenarios (all closed-loop, per-request latency into an HDR histogram):
//!
//! - `get-config`   — `GET /v1/config?warehouse=…`
//! - `load-table`   — `GET …/tables/{table}` against a wide, multi-snapshot
//!   fixture table, swept over several concurrency levels
//! - `commit`       — sequential `set-properties` commits (`POST …/tables/{table}`)
//! - `plan` / `plan-full` — server-side scan planning (`POST …/plan`)
//!   against a synthetic many-file table (see `--setup-plan`): `plan`
//!   sends a partition-selective filter, `plan-full` an unfiltered scan.
//!   Submissions answered `submitted` are polled to completion inside the
//!   timed window and then cancelled (one extra request), so async-mode
//!   numbers are end-to-end plan latency, not just the submit call.
//!
//! Auth is pluggable: `--auth none` or `--auth oauth2` (client-credentials
//! token fetched once, before any timed request).

mod catalog;
mod runner;
mod stats;

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

    /// Create the scan-planning fixture table (real manifests written to
    /// --plan-storage-root, register via IRC). Requires the harness to
    /// have write access to the warehouse's storage.
    #[arg(long)]
    setup_plan: bool,

    #[arg(long, default_value = "plan_ns")]
    plan_namespace: String,

    #[arg(long, default_value = "plan_10k")]
    plan_table: String,

    /// Data files in the planning fixture.
    #[arg(long, default_value_t = 10_000)]
    plan_files: usize,

    /// Identity partitions in the planning fixture.
    #[arg(long, default_value_t = 100)]
    plan_partitions: usize,

    /// Data files per manifest in the planning fixture.
    #[arg(long, default_value_t = 100)]
    plan_files_per_manifest: usize,

    /// Warehouse storage root for --setup-plan (e.g.
    /// `s3://bench-meridian/warehouse` or `file:///tmp/wh`); the fixture
    /// lands under `{root}/{plan-namespace}/{plan-table}`.
    #[arg(long)]
    plan_storage_root: Option<String>,

    /// Storage options for --setup-plan as key=value (repeatable), e.g.
    /// endpoint=http://localhost:9000 access-key-id=… — the same options
    /// the warehouse was created with.
    #[arg(long)]
    plan_storage_option: Vec<String>,

    /// Measured plan requests per plan scenario.
    #[arg(long, default_value_t = 500)]
    plan_n: u64,

    /// Warm-up plan requests (excluded).
    #[arg(long, default_value_t = 50)]
    plan_warmup: u64,

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

    if args.setup_plan {
        let storage_root = args
            .plan_storage_root
            .as_deref()
            .ok_or("--plan-storage-root is required with --setup-plan")?;
        let mut storage_options = std::collections::BTreeMap::new();
        for option in &args.plan_storage_option {
            let (key, value) = option
                .split_once('=')
                .ok_or_else(|| format!("--plan-storage-option {option:?} is not key=value"))?;
            storage_options.insert(key.to_owned(), value.to_owned());
        }
        eprintln!(
            "setting up planning fixture {}.{} ({} files, {} partitions)…",
            args.plan_namespace, args.plan_table, args.plan_files, args.plan_partitions
        );
        cat.setup_plan_fixture(
            &args.plan_namespace,
            &args.plan_table,
            storage_root,
            &storage_options,
            &meridian_bench::fixture::SyntheticSpec {
                table_location: String::new(), // derived inside
                data_files: args.plan_files,
                partitions: args.plan_partitions,
                files_per_manifest: args.plan_files_per_manifest,
                rows_per_file: 1_000,
            },
        )
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
            "plan" | "plan-full" => {
                let table_url = cat.table_url(&args.plan_namespace, &args.plan_table);
                let selective = scenario == "plan";
                let partitions = args.plan_partitions.max(1);
                let async_completions = Arc::new(AtomicU64::new(0));
                let raw = {
                    let cat = cat.clone();
                    let table_url = table_url.clone();
                    let async_completions = Arc::clone(&async_completions);
                    runner::run(1, args.plan_warmup, args.plan_n, move |i| {
                        let cat = cat.clone();
                        let table_url = table_url.clone();
                        let async_completions = Arc::clone(&async_completions);
                        async move {
                            let body = if selective {
                                let region = format!(
                                    "region_{:03}",
                                    usize::try_from(i).unwrap_or(0) % partitions
                                );
                                json!({
                                    "filter": {"type": "eq", "term": "region", "value": region},
                                    "stats-fields": ["id"],
                                })
                            } else {
                                json!({})
                            };
                            plan_once(&cat, &table_url, &body, &async_completions).await
                        }
                    })
                    .await?
                };
                let async_count = async_completions.load(Ordering::Relaxed);
                if async_count > 0 {
                    eprintln!(
                        "{scenario}: {async_count} of {} plans took the asynchronous \
                         (submitted/poll/cancel) path",
                        args.plan_n + args.plan_warmup
                    );
                }
                report_raw(&mut results, scenario, 1, args.plan_warmup, &raw);
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

/// One end-to-end plan: submit; if `submitted`, poll fetchPlanningResult
/// to a terminal status and cancel (releasing the server-held pages).
/// Everything happens inside the timed window.
async fn plan_once(
    cat: &Catalog,
    table_url: &str,
    body: &serde_json::Value,
    async_completions: &AtomicU64,
) -> std::result::Result<(), String> {
    let plan_url = format!("{table_url}/plan");
    let response = cat
        .post_json(&plan_url, body)
        .send()
        .await
        .map_err(|e| format!("transport: {e}"))?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!("HTTP {status}: {text}"));
    }
    let parsed: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("bad plan response: {e}"))?;
    match parsed["status"].as_str() {
        Some("completed") => Ok(()),
        Some("submitted") => {
            let plan_id = parsed["plan-id"]
                .as_str()
                .ok_or("submitted response without plan-id")?
                .to_owned();
            async_completions.fetch_add(1, Ordering::Relaxed);
            let result_url = format!("{plan_url}/{plan_id}");
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            loop {
                let response = cat
                    .get(&result_url)
                    .send()
                    .await
                    .map_err(|e| format!("poll transport: {e}"))?;
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                if !status.is_success() {
                    return Err(format!("poll HTTP {status}: {text}"));
                }
                let parsed: serde_json::Value =
                    serde_json::from_str(&text).map_err(|e| format!("bad poll response: {e}"))?;
                match parsed["status"].as_str() {
                    Some("completed") => break,
                    Some("submitted") => {
                        if std::time::Instant::now() > deadline {
                            return Err("plan did not complete within 60s".to_owned());
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    other => return Err(format!("plan ended {other:?}: {text}")),
                }
            }
            // Well-behaved client: release the held result pages.
            let _ = cat.delete(&result_url).send().await;
            Ok(())
        }
        other => Err(format!("unexpected plan status {other:?}: {text}")),
    }
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
