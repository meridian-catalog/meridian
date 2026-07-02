//! Generic closed-loop request runner.
//!
//! `concurrency` workers pull request indices from a shared counter until
//! `warmup + measured` requests have been issued. Latencies for the first
//! `warmup` indices are discarded; the measured wall clock starts when the
//! first measured request is dequeued.

use std::future::Future;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use hdrhistogram::Histogram;

use crate::stats::MAX_LATENCY_US;

/// Outcome of one scenario execution before summarization.
#[derive(Debug)]
pub(crate) struct RawRun {
    /// Microsecond latencies of successful measured requests.
    pub(crate) hist: Histogram<u64>,
    /// Failed measured requests (non-2xx or transport error).
    pub(crate) errors: u64,
    /// Wall-clock seconds spanning the measured window.
    pub(crate) measured_wall_secs: f64,
    /// First few error messages, for diagnostics.
    pub(crate) error_samples: Vec<String>,
}

/// Runs `warmup + measured` requests through `concurrency` workers.
///
/// `request` receives the global request index (0-based, warm-up included)
/// and resolves to `Ok(())` on a 2xx response.
pub(crate) async fn run<F, Fut>(
    concurrency: usize,
    warmup: u64,
    measured: u64,
    request: F,
) -> Result<RawRun, Box<dyn std::error::Error + Send + Sync>>
where
    F: Fn(u64) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send,
{
    let total = warmup + measured;
    let counter = Arc::new(AtomicU64::new(0));
    let measure_start: Arc<OnceLock<Instant>> = Arc::new(OnceLock::new());

    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let counter = Arc::clone(&counter);
        let measure_start = Arc::clone(&measure_start);
        let request = request.clone();
        handles.push(tokio::spawn(async move {
            let mut hist = Histogram::<u64>::new_with_bounds(1, MAX_LATENCY_US, 3)
                .expect("static histogram bounds are valid");
            let mut errors = 0_u64;
            let mut error_samples = Vec::new();
            loop {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                if i >= total {
                    break;
                }
                if i == warmup {
                    let _ = measure_start.set(Instant::now());
                }
                let started = Instant::now();
                let outcome = request(i).await;
                let elapsed_us =
                    u64::try_from(started.elapsed().as_micros()).unwrap_or(MAX_LATENCY_US);
                if i < warmup {
                    continue;
                }
                match outcome {
                    Ok(()) => {
                        hist.record(elapsed_us.clamp(1, MAX_LATENCY_US))
                            .expect("value clamped into histogram bounds");
                    }
                    Err(msg) => {
                        errors += 1;
                        if error_samples.len() < 3 {
                            error_samples.push(msg);
                        }
                    }
                }
            }
            (hist, errors, error_samples)
        }));
    }

    let mut hist = Histogram::<u64>::new_with_bounds(1, MAX_LATENCY_US, 3)
        .expect("static histogram bounds are valid");
    let mut errors = 0_u64;
    let mut error_samples = Vec::new();
    for handle in handles {
        let (worker_hist, worker_errors, worker_samples) = handle.await?;
        hist.add(&worker_hist)?;
        errors += worker_errors;
        for s in worker_samples {
            if error_samples.len() < 5 {
                error_samples.push(s);
            }
        }
    }

    let measured_wall_secs = measure_start
        .get()
        .map_or(0.0, |t| t.elapsed().as_secs_f64());

    Ok(RawRun {
        hist,
        errors,
        measured_wall_secs,
        error_samples,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn warmup_requests_are_excluded() {
        // Warm-up requests "fail"; measured ones succeed. If warm-up leaked
        // into measurement the error count would be nonzero.
        let raw = run(4, 10, 50, |i| async move {
            if i < 10 { Err("warmup".into()) } else { Ok(()) }
        })
        .await
        .unwrap();
        assert_eq!(raw.errors, 0);
        assert_eq!(raw.hist.len(), 50);
    }

    #[tokio::test]
    async fn errors_are_counted_and_sampled() {
        let raw = run(2, 0, 20, |i| async move {
            if i % 2 == 0 {
                Ok(())
            } else {
                Err(format!("boom {i}"))
            }
        })
        .await
        .unwrap();
        assert_eq!(raw.errors, 10);
        assert_eq!(raw.hist.len(), 10);
        assert!(!raw.error_samples.is_empty());
    }

    #[tokio::test]
    async fn issues_exactly_total_requests() {
        let issued = Arc::new(AtomicU64::new(0));
        let issued_clone = Arc::clone(&issued);
        let raw = run(8, 5, 100, move |_| {
            let issued = Arc::clone(&issued_clone);
            async move {
                issued.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(issued.load(Ordering::Relaxed), 105);
        assert_eq!(raw.hist.len(), 100);
    }
}
