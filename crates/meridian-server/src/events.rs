//! Event delivery: the outbox relay and the webhook dispatcher.
//!
//! Both run as background tasks inside `meridian serve` (spawned by
//! [`crate::serve`]); neither holds HTTP-request state. The heavy lifting —
//! claiming with `FOR UPDATE SKIP LOCKED`, per-aggregate ordering, durable
//! delivery rows — lives in `meridian_store::{outbox, webhook}`; this
//! module owns the loops, the `CloudEvents` 1.0 rendering, and the webhook
//! HTTP requests with their HMAC signatures.
//!
//! Design: [`docs/design/events.md`](../../../docs/design/events.md).

use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use hmac::{Hmac, Mac};
use meridian_common::config::EventsConfig;
use meridian_store::outbox::{self, OutboxRecord};
use meridian_store::webhook::{self, DueDelivery};
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::PgPool;

/// Prefix turning a stored event type (`table.committed`) into its
/// `CloudEvents` `type` (`com.meridian.table.committed`).
pub const EVENT_TYPE_PREFIX: &str = "com.meridian.";

/// Longest webhook retry delay (the exponential backoff cap).
const MAX_RETRY_DELAY: Duration = Duration::from_secs(15 * 60);

/// Longest relay/dispatcher pause after repeated infrastructure errors.
const MAX_ERROR_DELAY: Duration = Duration::from_secs(30);

/// Renders an outbox row as a `CloudEvents` 1.0 JSON object.
///
/// - `id`: the outbox row ULID (doubles as the feed cursor),
/// - `source`: `meridian/<workspace>` (`meridian` for org-level events),
/// - `type`: `com.meridian.` + the stored event type,
/// - `subject`: the aggregate, e.g. `table:01J...`,
/// - `time`: when the mutation committed (RFC 3339, UTC),
/// - `data`: the payload written by the emitting module.
#[must_use]
pub fn cloud_event(record: &OutboxRecord) -> Value {
    cloud_event_parts(
        &record.id,
        record.workspace_id.as_deref(),
        &record.aggregate,
        &record.event_type,
        record.created_at,
        &record.payload,
    )
}

fn cloud_event_parts(
    id: &str,
    workspace_id: Option<&str>,
    aggregate: &str,
    event_type: &str,
    created_at: chrono::DateTime<Utc>,
    payload: &Value,
) -> Value {
    let source = match workspace_id {
        Some(workspace) => format!("meridian/{workspace}"),
        None => "meridian".to_owned(),
    };
    json!({
        "specversion": "1.0",
        "id": id,
        "source": source,
        "type": format!("{EVENT_TYPE_PREFIX}{event_type}"),
        "subject": aggregate,
        "time": created_at.to_rfc3339_opts(SecondsFormat::Micros, true),
        "datacontenttype": "application/json",
        "data": payload,
    })
}

/// Computes the webhook signature header value for a delivery:
/// `v1=` + hex(HMAC-SHA256(secret, `<timestamp>.<body>`)).
///
/// The signed string couples the payload to the `x-meridian-timestamp`
/// header so receivers can reject replays outside their tolerance window.
/// Verification is documented in `docs/design/events.md`.
#[must_use]
pub fn webhook_signature(secret: &str, timestamp: i64, body: &str) -> String {
    // HMAC-SHA256 accepts keys of any length, so this cannot fail; the
    // empty-string fallback (never a valid signature) keeps the function
    // total without a panic path.
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
        return String::new();
    };
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body.as_bytes());
    format!("v1={}", hex::encode(mac.finalize().into_bytes()))
}

/// The outbox relay loop: publish batches until the backlog is drained,
/// then poll. Never returns; run it under `tokio::spawn`.
///
/// A full batch means more backlog is likely waiting, so the loop continues
/// immediately — a large first-boot backlog drains in bounded batches
/// without a thundering herd — and sleeps only once a partial batch signals
/// the drain is done. Database errors back off exponentially (1s → 30s).
pub async fn run_relay(pool: PgPool, config: EventsConfig) {
    let idle_sleep = Duration::from_millis(config.relay_poll_ms);
    let mut error_delay = Duration::from_secs(1);
    tracing::info!(batch_size = config.relay_batch_size, "outbox relay started");
    loop {
        match outbox::relay_once(&pool, config.relay_batch_size).await {
            Ok(published) => {
                error_delay = Duration::from_secs(1);
                if published > 0 {
                    tracing::debug!(published, "outbox relay published events");
                }
                // relay_once returns the claimed count; a full batch means
                // "keep going", anything less means "caught up".
                if i64::try_from(published).unwrap_or(i64::MAX) < config.relay_batch_size {
                    tokio::time::sleep(idle_sleep).await;
                }
            }
            Err(error) => {
                tracing::warn!(%error, "outbox relay iteration failed; backing off");
                tokio::time::sleep(error_delay).await;
                error_delay = (error_delay * 2).min(MAX_ERROR_DELAY);
            }
        }
    }
}

/// The webhook dispatcher loop. Never returns; run it under `tokio::spawn`.
pub async fn run_dispatcher(pool: PgPool, config: EventsConfig) {
    let idle_sleep = Duration::from_millis(config.webhook_poll_ms);
    let mut error_delay = Duration::from_secs(1);
    let client = match webhook_client(&config) {
        Ok(client) => client,
        Err(error) => {
            // Without an HTTP client there is nothing to dispatch; leave
            // deliveries pending (durable) rather than crashing the server.
            tracing::error!(%error, "webhook dispatcher failed to start; deliveries stay pending");
            return;
        }
    };
    tracing::info!("webhook dispatcher started");
    loop {
        match dispatch_once(&pool, &client, &config).await {
            Ok(dispatched) => {
                error_delay = Duration::from_secs(1);
                if dispatched == 0 {
                    tokio::time::sleep(idle_sleep).await;
                }
            }
            Err(error) => {
                tracing::warn!(%error, "webhook dispatch iteration failed; backing off");
                tokio::time::sleep(error_delay).await;
                error_delay = (error_delay * 2).min(MAX_ERROR_DELAY);
            }
        }
    }
}

/// Builds the dispatcher's HTTP client (bounded per-request timeout).
pub fn webhook_client(
    config: &EventsConfig,
) -> Result<reqwest::Client, meridian_common::MeridianError> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(config.webhook_timeout_secs))
        .user_agent(concat!("meridian-webhook/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| meridian_common::MeridianError::internal("failed to build webhook client", e))
}

/// One dispatcher iteration: claim due deliveries (leased, `SKIP LOCKED`),
/// attempt each, record success / retry-with-backoff / dead-letter.
/// Returns how many deliveries were attempted.
///
/// The HTTP attempts happen outside any database transaction — the lease
/// taken at claim time makes a crash mid-attempt just a delayed retry.
pub async fn dispatch_once(
    pool: &PgPool,
    client: &reqwest::Client,
    config: &EventsConfig,
) -> Result<usize, meridian_common::MeridianError> {
    // The lease must outlive the slowest possible attempt of the batch.
    let lease_secs = (config.webhook_timeout_secs * 2).max(60);
    let lease_until =
        Utc::now() + chrono::Duration::seconds(i64::try_from(lease_secs).unwrap_or(i64::MAX));

    let batch = webhook::claim_due_deliveries(pool, 50, lease_until).await?;
    let attempted = batch.len();
    for delivery in batch {
        attempt_delivery(pool, client, config, &delivery).await?;
    }
    Ok(attempted)
}

/// Attempts one delivery and records the outcome.
async fn attempt_delivery(
    pool: &PgPool,
    client: &reqwest::Client,
    config: &EventsConfig,
    delivery: &DueDelivery,
) -> Result<(), meridian_common::MeridianError> {
    let event = cloud_event_parts(
        &delivery.event_id,
        delivery.workspace_id.as_deref(),
        &delivery.aggregate,
        &delivery.event_type,
        delivery.created_at,
        &delivery.payload,
    );
    let body = event.to_string();
    let timestamp = Utc::now().timestamp();
    let signature = webhook_signature(&delivery.secret, timestamp, &body);
    let event_type = format!("{EVENT_TYPE_PREFIX}{}", delivery.event_type);

    let outcome = client
        .post(&delivery.url)
        .header("content-type", "application/cloudevents+json")
        .header("x-meridian-event-id", &delivery.event_id)
        .header("x-meridian-event-type", &event_type)
        .header("x-meridian-timestamp", timestamp.to_string())
        .header("x-meridian-signature", &signature)
        .body(body)
        .send()
        .await;

    let (status, error) = match outcome {
        Ok(response) => {
            let code = i16::try_from(response.status().as_u16()).unwrap_or(i16::MAX);
            if response.status().is_success() {
                webhook::record_delivery_success(
                    pool,
                    &delivery.endpoint_id,
                    &delivery.event_id,
                    code,
                )
                .await?;
                return Ok(());
            }
            (Some(code), format!("endpoint returned HTTP {code}"))
        }
        // The reqwest error chain can embed the URL; the stored message is
        // returned by the management API, which is management-only, so
        // that is fine — but keep it bounded.
        Err(error) => (None, truncate(&error.to_string(), 500)),
    };

    let next_attempt_at = if delivery.attempts >= config.webhook_max_attempts {
        None // Dead-letter.
    } else {
        let delay = retry_delay(delivery.attempts, config.webhook_retry_base_secs);
        Some(
            Utc::now()
                + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::seconds(900)),
        )
    };
    if next_attempt_at.is_none() {
        tracing::warn!(
            endpoint_id = %delivery.endpoint_id,
            event_id = %delivery.event_id,
            attempts = delivery.attempts,
            "webhook delivery dead-lettered"
        );
    }
    webhook::record_delivery_failure(
        pool,
        &delivery.endpoint_id,
        &delivery.event_id,
        status,
        &error,
        next_attempt_at,
    )
    .await
}

/// Exponential backoff for attempt `attempts` (1-based): `base * 2^(n-1)`,
/// capped at 15 minutes.
fn retry_delay(attempts: i32, base_secs: u64) -> Duration {
    let exponent = u32::try_from(attempts.saturating_sub(1))
        .unwrap_or(0)
        .min(20);
    Duration::from_secs(base_secs.saturating_mul(1_u64 << exponent)).min(MAX_RETRY_DELAY)
}

/// Bounds a stored error message.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_owned()
    } else {
        let mut end = max;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &s[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_is_stable_and_key_dependent() {
        let sig = webhook_signature("secret", 1_700_000_000, "{\"a\":1}");
        assert!(sig.starts_with("v1="));
        assert_eq!(sig, webhook_signature("secret", 1_700_000_000, "{\"a\":1}"));
        assert_ne!(sig, webhook_signature("other", 1_700_000_000, "{\"a\":1}"));
        assert_ne!(sig, webhook_signature("secret", 1_700_000_001, "{\"a\":1}"));
    }

    #[test]
    fn retry_delay_grows_and_caps() {
        assert_eq!(retry_delay(1, 10), Duration::from_secs(10));
        assert_eq!(retry_delay(2, 10), Duration::from_secs(20));
        assert_eq!(retry_delay(4, 10), Duration::from_secs(80));
        assert_eq!(retry_delay(30, 10), Duration::from_secs(900));
    }

    #[test]
    fn cloud_event_has_the_required_fields() {
        let record = OutboxRecord {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            workspace_id: Some("00000000000000000000000001".to_owned()),
            aggregate: "table:01X".to_owned(),
            event_type: "table.committed".to_owned(),
            payload: serde_json::json!({"snapshot_id": 7}),
            created_at: chrono::Utc::now(),
        };
        let event = cloud_event(&record);
        assert_eq!(event["specversion"], "1.0");
        assert_eq!(event["id"], "01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(event["source"], "meridian/00000000000000000000000001");
        assert_eq!(event["type"], "com.meridian.table.committed");
        assert_eq!(event["subject"], "table:01X");
        assert_eq!(event["data"]["snapshot_id"], 7);
        assert!(event["time"].as_str().is_some_and(|t| t.ends_with('Z')));
    }
}
