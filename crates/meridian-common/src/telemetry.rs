//! Tracing/logging initialization.
//!
//! TODO(M1+): OpenTelemetry trace/metric export; for M0 we only initialize a
//! `tracing-subscriber` stack (pretty for dev, JSON for production pipelines).

use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::{LogFormat, TelemetryConfig};
use crate::error::MeridianError;

/// Initializes the global tracing subscriber.
///
/// The filter is taken from `RUST_LOG` when set, otherwise from
/// [`TelemetryConfig::filter`]. Calling this twice returns an error rather
/// than silently replacing the subscriber.
pub fn init(config: &TelemetryConfig) -> Result<(), MeridianError> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.filter))
        .map_err(|e| {
            MeridianError::Validation(format!("invalid telemetry filter {:?}: {e}", config.filter))
        })?;

    let registry = tracing_subscriber::registry().with(filter);

    let result = match config.format {
        LogFormat::Pretty => registry.with(tracing_subscriber::fmt::layer()).try_init(),
        LogFormat::Json => registry
            .with(tracing_subscriber::fmt::layer().json().flatten_event(true))
            .try_init(),
    };

    result.map_err(|e| MeridianError::internal("failed to initialize tracing subscriber", e))
}
