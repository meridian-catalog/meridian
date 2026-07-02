//! Latency summarization and report rendering.
//!
//! Latencies are recorded in microseconds into an HDR histogram
//! (3 significant digits, values up to 10 minutes) and reported in
//! milliseconds.

use hdrhistogram::Histogram;
use serde::Serialize;

/// Upper bound for recorded latencies: 10 minutes, in microseconds.
pub(crate) const MAX_LATENCY_US: u64 = 600_000_000;

/// Summary of one scenario run against one catalog.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ScenarioResult {
    /// Scenario name (`get-config`, `load-table`, `commit`).
    pub(crate) scenario: String,
    /// Number of concurrent workers.
    pub(crate) concurrency: usize,
    /// Requests measured (excludes warm-up).
    pub(crate) measured_requests: u64,
    /// Warm-up requests issued before measurement started (excluded).
    pub(crate) warmup_requests: u64,
    /// Non-2xx responses or transport errors during the measured window.
    pub(crate) errors: u64,
    /// Median latency, milliseconds.
    pub(crate) p50_ms: f64,
    /// 95th percentile latency, milliseconds.
    pub(crate) p95_ms: f64,
    /// 99th percentile latency, milliseconds.
    pub(crate) p99_ms: f64,
    /// Maximum observed latency, milliseconds.
    pub(crate) max_ms: f64,
    /// Successful requests per second over the measured wall-clock window.
    pub(crate) rps: f64,
}

/// Builds a [`ScenarioResult`] from a histogram of microsecond latencies.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn summarize(
    scenario: &str,
    concurrency: usize,
    hist: &Histogram<u64>,
    warmup_requests: u64,
    errors: u64,
    measured_wall_secs: f64,
) -> ScenarioResult {
    let to_ms = |us: u64| us as f64 / 1_000.0;
    let successes = hist.len();
    ScenarioResult {
        scenario: scenario.to_owned(),
        concurrency,
        measured_requests: successes + errors,
        warmup_requests,
        errors,
        p50_ms: to_ms(hist.value_at_quantile(0.50)),
        p95_ms: to_ms(hist.value_at_quantile(0.95)),
        p99_ms: to_ms(hist.value_at_quantile(0.99)),
        max_ms: to_ms(hist.max()),
        rps: if measured_wall_secs > 0.0 {
            successes as f64 / measured_wall_secs
        } else {
            0.0
        },
    }
}

/// Renders results as a GitHub-flavored markdown table.
pub(crate) fn markdown_table(catalog: &str, results: &[ScenarioResult]) -> String {
    use std::fmt::Write as _;

    let mut out = format!(
        "### {catalog}\n\n\
         | scenario | concurrency | requests | errors | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | req/s |\n\
         |---|---:|---:|---:|---:|---:|---:|---:|---:|\n"
    );
    for r in results {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.0} |",
            r.scenario,
            r.concurrency,
            r.measured_requests,
            r.errors,
            r.p50_ms,
            r.p95_ms,
            r.p99_ms,
            r.max_ms,
            r.rps
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hist_of(values_us: &[u64]) -> Histogram<u64> {
        let mut h = Histogram::new_with_bounds(1, MAX_LATENCY_US, 3).unwrap();
        for v in values_us {
            h.record(*v).unwrap();
        }
        h
    }

    #[test]
    fn percentiles_from_known_distribution() {
        // 1..=1000 microseconds -> p50 ~ 500us, p99 ~ 990us, max = 1000us.
        let values: Vec<u64> = (1..=1000).collect();
        let h = hist_of(&values);
        let s = summarize("load-table", 8, &h, 100, 0, 2.0);
        assert_eq!(s.scenario, "load-table");
        assert_eq!(s.concurrency, 8);
        assert_eq!(s.measured_requests, 1000);
        assert_eq!(s.warmup_requests, 100);
        assert_eq!(s.errors, 0);
        // HDR histograms with 3 significant digits are exact at this scale.
        assert!((s.p50_ms - 0.5).abs() < 0.01, "p50 was {}", s.p50_ms);
        assert!((s.p99_ms - 0.99).abs() < 0.01, "p99 was {}", s.p99_ms);
        assert!((s.max_ms - 1.0).abs() < 0.01, "max was {}", s.max_ms);
        // 1000 successes over 2 seconds.
        assert!((s.rps - 500.0).abs() < 0.5, "rps was {}", s.rps);
    }

    #[test]
    fn errors_counted_in_measured_requests_not_rps() {
        let h = hist_of(&[1_000, 2_000, 3_000, 4_000]);
        let s = summarize("commit", 1, &h, 0, 6, 1.0);
        assert_eq!(s.measured_requests, 10);
        assert_eq!(s.errors, 6);
        // Only the 4 successes count toward throughput.
        assert!((s.rps - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn zero_wall_time_yields_zero_rps() {
        let h = hist_of(&[1_000]);
        let s = summarize("get-config", 1, &h, 0, 0, 0.0);
        assert!(s.rps.abs() < f64::EPSILON);
    }

    #[test]
    fn markdown_table_shape() {
        let h = hist_of(&[1_500]);
        let s = summarize("get-config", 1, &h, 100, 0, 1.0);
        let md = markdown_table("meridian", &[s]);
        let lines: Vec<&str> = md.lines().collect();
        assert_eq!(lines[0], "### meridian");
        assert!(lines[2].starts_with("| scenario |"));
        assert!(lines[3].starts_with("|---|"));
        // Data row: latency 1.50 ms, 1 req/s.
        assert!(lines[4].contains("| get-config | 1 | 1 | 0 | 1.50 |"));
        assert!(lines[4].ends_with("| 1 |"));
    }
}
