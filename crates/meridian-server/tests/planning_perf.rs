//! Perf smoke for scan planning: the performance target — warm p95 <
//! 150 ms for a 10,000-file table — asserted against the synthetic
//! fixture through the full in-process router (no network, no TLS).
//!
//! `#[ignore]` because it builds a 10K-file fixture (~seconds) and is
//! meaningless in debug builds; run it in release via
//! `testing/bench/scripts/plan-perf.sh` (requires `DATABASE_URL`), which
//! is how the numbers in the planning design doc were produced. QA
//! re-measures independently; nothing here writes to docs/benchmarks.
//!
//! Measured operations, all synchronous (threshold raised above 10K):
//!
//! - `plan/point`: partition-selective filter (1% of files match) with
//!   `stats-fields: ["id"]` — the thin-engine hot call.
//! - `plan/full`: no filter, every task serialized inline with full
//!   stats — the worst case the sync path can be asked for.
//!
//! Only `plan/point` carries the hard assertion; `plan/full` is reported
//! for the record (it is dominated by serializing 10,000 tasks and is
//! the number the async threshold protects in production defaults).

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use meridian_bench::fixture::{self, SyntheticSpec};
use meridian_common::AppConfig;
use meridian_server::{AppState, build_router};
use serde_json::{Value, json};
use tower::ServiceExt;
use ulid::Ulid;

const WARMUP: usize = 5;
const MEASURED: usize = 50;

async fn post_json(router: &Router, uri: &str, body: &Value) -> (StatusCode, usize) {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("build request");
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("infallible router call");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    (status, bytes.len())
}

fn percentile(sorted: &[Duration], hundredths: usize) -> Duration {
    // Nearest-rank percentile over integer hundredths (50, 95, 99).
    let rank = (sorted.len() * hundredths).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

async fn measure(router: &Router, uri: &str, body: &Value, label: &str) -> Duration {
    for _ in 0..WARMUP {
        let (status, _) = post_json(router, uri, body).await;
        assert_eq!(status, StatusCode::OK, "{label} warm-up");
    }
    let mut samples = Vec::with_capacity(MEASURED);
    let mut last_bytes = 0;
    for _ in 0..MEASURED {
        let started = Instant::now();
        let (status, bytes) = post_json(router, uri, body).await;
        samples.push(started.elapsed());
        last_bytes = bytes;
        assert_eq!(status, StatusCode::OK, "{label} measured request");
    }
    samples.sort_unstable();
    let p50 = percentile(&samples, 50);
    let p95 = percentile(&samples, 95);
    let p99 = percentile(&samples, 99);
    println!(
        "{label}: n={MEASURED} p50={:.1}ms p95={:.1}ms p99={:.1}ms response={last_bytes}B",
        p50.as_secs_f64() * 1e3,
        p95.as_secs_f64() * 1e3,
        p99.as_secs_f64() * 1e3,
    );
    p95
}

#[tokio::test]
#[ignore = "perf smoke: run in release via testing/bench/scripts/plan-perf.sh"]
async fn warm_plan_p95_meets_spec_target_on_10k_files() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping planning perf smoke: DATABASE_URL is not set");
        return;
    };
    let mut config = AppConfig::default();
    config.database.url = url;
    // Measure the synchronous path on the 10K table (production default
    // sends tables this large down the async path).
    config.planning.sync_max_data_files = 20_000;

    let pool = meridian_store::connect(&config.database)
        .await
        .expect("connect to test database");
    meridian_store::MIGRATOR
        .run(&pool)
        .await
        .expect("run migrations");
    let router = build_router(AppState {
        pool,
        config: Arc::new(config),
    });

    let root = tempfile::tempdir().expect("create tempdir");
    let warehouse = format!("wh-perf-{}", Ulid::new().to_string().to_lowercase());
    let storage_root = format!("file://{}", root.path().join("warehouse").display());
    let (status, _) = post_json(
        &router,
        "/api/v2/warehouses",
        &json!({"name": warehouse, "storage_root": storage_root}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    eprintln!("generating the 10,000-file fixture…");
    let spec = SyntheticSpec {
        table_location: format!(
            "file://{}",
            root.path().join("warehouse/perf_ns/plan_10k").display()
        ),
        data_files: 10_000,
        partitions: 100,
        files_per_manifest: 100,
        rows_per_file: 1_000,
    };
    let table = fixture::synthetic_table(&spec).expect("generate fixture");
    fixture::write_local(&table.files).expect("write fixture");
    let (status, _) = post_json(
        &router,
        &format!("/v1/{warehouse}/namespaces"),
        &json!({"namespace": ["perf_ns"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = post_json(
        &router,
        &format!("/v1/{warehouse}/namespaces/perf_ns/register"),
        &json!({"name": "plan_10k", "metadata-location": table.metadata_location}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let plan_url = format!("/v1/{warehouse}/namespaces/perf_ns/tables/plan_10k/plan");

    let point = json!({
        "filter": {"type": "eq", "term": "region", "value": "region_042"},
        "stats-fields": ["id"],
    });
    let point_p95 = measure(&router, &plan_url, &point, "plan/point (1% match)").await;

    let full = json!({});
    let _full_p95 = measure(&router, &plan_url, &full, "plan/full (10k tasks inline)").await;

    assert!(
        point_p95 < Duration::from_millis(150),
        "planning target: warm plan p95 < 150 ms, measured {:.1} ms",
        point_p95.as_secs_f64() * 1e3
    );
}
