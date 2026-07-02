//! Append-only, hash-chained audit log.
//!
//! Every entry's `hash` is `sha256(prev_hash || canonical_json(entry))`,
//! where `prev_hash` is the hex hash of the previous entry (empty for the
//! genesis entry) and `canonical_json` is a deterministic rendering of the
//! entry content (sorted object keys, no whitespace). Any retroactive edit
//! to a row breaks every hash after it, so the chain can be verified end to
//! end with [`verify_chain`].
//!
//! Appends serialize on a Postgres advisory lock: the chain needs a total
//! order, and an advisory transaction lock is the simplest correct way to
//! get one. Audit writes are not on the hot read path; if append throughput
//! ever matters we revisit with batched appends, not by weakening the chain.

use chrono::{DateTime, DurationRound, SecondsFormat, TimeDelta, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use ulid::Ulid;

use crate::map_sqlx_error;

/// Advisory lock key serializing audit appends. Arbitrary but stable
/// (ASCII "MERIDIAN" packed into an i64).
const AUDIT_CHAIN_LOCK_KEY: i64 = 0x4D45_5249_4449_414E;

/// A new audit entry to append.
#[derive(Debug, Clone)]
pub struct NewAuditEntry {
    /// Workspace scope; `None` for org-level actions.
    pub workspace_id: Option<WorkspaceId>,
    /// Acting principal, e.g. `user:alice@example.com` or `service:relay`.
    pub principal: String,
    /// Action performed, e.g. `table.commit`.
    pub action: String,
    /// Resource acted on, e.g. `table:01J...`.
    pub resource: String,
    /// Structured detail payload.
    pub details: Value,
}

/// A persisted audit entry.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AuditRecord {
    /// Position in the chain (monotonic, assigned by Postgres).
    pub seq: i64,
    /// ULID of the entry.
    pub id: String,
    /// Workspace scope, if any.
    pub workspace_id: Option<String>,
    /// When the action occurred (UTC, microsecond precision).
    pub occurred_at: DateTime<Utc>,
    /// Acting principal.
    pub principal: String,
    /// Action performed.
    pub action: String,
    /// Resource acted on.
    pub resource: String,
    /// Structured detail payload.
    pub details: Value,
    /// Hash of the previous entry; `None` for the genesis entry.
    pub prev_hash: Option<String>,
    /// This entry's hash.
    pub hash: String,
}

/// Renders a JSON value deterministically: object keys sorted
/// lexicographically (byte order), no insignificant whitespace, standard
/// JSON string escaping. Array order is preserved (it is significant).
///
/// This is intentionally independent of `serde_json`'s map ordering so the
/// hash chain does not depend on crate feature flags (`preserve_order`).
#[must_use]
pub fn canonical_json(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        // `serde_json::Number`'s `Display` renders exactly its JSON form.
        Value::Number(number) => out.push_str(&number.to_string()),
        Value::String(s) => write_escaped_string(s, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            out.push('{');
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_escaped_string(key, out);
                out.push(':');
                write_canonical(&map[*key], out);
            }
            out.push('}');
        }
    }
}

/// JSON string escaping per RFC 8259 §7, byte-identical to `serde_json`'s
/// default output: `"` and `\` escaped, control characters below U+0020
/// escaped (short forms for backspace/tab/newline/form-feed/carriage-return,
/// lowercase `\u00xx` otherwise), everything else — including non-ASCII —
/// verbatim. Hashing must be a total function, so this replaces a fallible
/// serializer call on the commit path; equivalence with `serde_json` is
/// locked by a property test in this module.
fn write_escaped_string(s: &str, out: &mut String) {
    use std::fmt::Write as _;

    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{0C}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => {
                // Infallible: `write!` into a String cannot fail.
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Computes an entry hash: `sha256(prev_hash || canonical)` hex-encoded.
#[must_use]
pub fn compute_hash(prev_hash: Option<&str>, canonical: &str) -> String {
    let mut hasher = Sha256::new();
    if let Some(prev) = prev_hash {
        hasher.update(prev.as_bytes());
    }
    hasher.update(canonical.as_bytes());
    hex::encode(hasher.finalize())
}

/// The canonical content rendering of one entry (the hashed material).
fn entry_content(
    id: &str,
    workspace_id: Option<&str>,
    occurred_at: DateTime<Utc>,
    principal: &str,
    action: &str,
    resource: &str,
    details: &Value,
) -> String {
    canonical_json(&json!({
        "id": id,
        "workspace_id": workspace_id,
        "occurred_at": occurred_at.to_rfc3339_opts(SecondsFormat::Micros, true),
        "principal": principal,
        "action": action,
        "resource": resource,
        "details": details,
    }))
}

/// Appends an entry to the audit chain in its own transaction and returns
/// the persisted record.
///
/// For mutations that must be atomic with their audit row (every API
/// mutation), use [`append_in_tx`] on the mutation's transaction instead.
pub async fn append(pool: &PgPool, entry: NewAuditEntry) -> Result<AuditRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin audit transaction", e))?;

    let record = append_in_tx(&mut tx, entry).await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit audit entry", e))?;

    Ok(record)
}

/// Appends an entry to the audit chain on the caller's transaction.
///
/// The entry becomes durable if and only if the caller's transaction
/// commits, which is exactly the atomicity the commit protocol requires
/// (state change + audit row + outbox event, all or nothing).
///
/// Takes the audit-chain advisory lock (`pg_advisory_xact_lock`), which is
/// held until the caller's transaction ends — so keep transactions that
/// audit short, and take this lock last (it is a serialization point).
pub async fn append_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    entry: NewAuditEntry,
) -> Result<AuditRecord> {
    // Serialize appends: the chain requires a total order over hashes.
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(AUDIT_CHAIN_LOCK_KEY)
        .execute(&mut **tx)
        .await
        .map_err(|e| map_sqlx_error("failed to acquire audit chain lock", e))?;

    let prev_hash: Option<String> =
        sqlx::query_scalar("SELECT hash FROM audit_log ORDER BY seq DESC LIMIT 1")
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| map_sqlx_error("failed to read audit chain head", e))?;

    let id = Ulid::new().to_string();
    // Truncate to microseconds so the hashed timestamp is exactly what
    // Postgres stores (timestamptz has microsecond precision) and
    // verification after a round-trip cannot drift.
    let occurred_at = Utc::now()
        .duration_trunc(TimeDelta::microseconds(1))
        .map_err(|e| MeridianError::internal("failed to truncate audit timestamp", e))?;

    let workspace_id = entry.workspace_id.map(|w| w.to_string());
    let content = entry_content(
        &id,
        workspace_id.as_deref(),
        occurred_at,
        &entry.principal,
        &entry.action,
        &entry.resource,
        &entry.details,
    );
    let hash = compute_hash(prev_hash.as_deref(), &content);

    let record: AuditRecord = sqlx::query_as(
        "INSERT INTO audit_log
             (id, workspace_id, occurred_at, principal, action, resource, details, prev_hash, hash)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         RETURNING seq, id, workspace_id, occurred_at, principal, action, resource, details,
                   prev_hash, hash",
    )
    .bind(&id)
    .bind(&workspace_id)
    .bind(occurred_at)
    .bind(&entry.principal)
    .bind(&entry.action)
    .bind(&entry.resource)
    .bind(&entry.details)
    .bind(&prev_hash)
    .bind(&hash)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| map_sqlx_error("failed to append audit entry", e))?;

    Ok(record)
}

/// Verifies the entire audit chain, returning the number of entries checked.
///
/// Fails with [`MeridianError::Internal`] at the first entry whose linkage or
/// hash does not recompute.
pub async fn verify_chain(pool: &PgPool) -> Result<u64> {
    let records: Vec<AuditRecord> = sqlx::query_as(
        "SELECT seq, id, workspace_id, occurred_at, principal, action, resource, details,
                prev_hash, hash
         FROM audit_log
         ORDER BY seq ASC",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load audit chain", e))?;

    let mut expected_prev: Option<String> = None;
    let mut checked: u64 = 0;

    for record in records {
        if record.prev_hash != expected_prev {
            return Err(MeridianError::internal_msg(format!(
                "audit chain broken at seq {}: prev_hash linkage mismatch",
                record.seq
            )));
        }

        let content = entry_content(
            &record.id,
            record.workspace_id.as_deref(),
            record.occurred_at,
            &record.principal,
            &record.action,
            &record.resource,
            &record.details,
        );
        let recomputed = compute_hash(record.prev_hash.as_deref(), &content);
        if recomputed != record.hash {
            return Err(MeridianError::internal_msg(format!(
                "audit chain broken at seq {}: hash does not recompute",
                record.seq
            )));
        }

        expected_prev = Some(record.hash);
        checked += 1;
    }

    Ok(checked)
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest::proptest! {
        /// The hand-rolled leaf rendering must stay byte-identical to
        /// serde_json's default output — strings (escaping), integers,
        /// floats, and keys all go through it.
        #[test]
        fn escaping_matches_serde_json_for_arbitrary_strings(s in "\\PC*|[\\x00-\\x1f\"\\\\]{0,64}") {
            let ours = canonical_json(&Value::String(s.clone()));
            let serde = serde_json::to_string(&Value::String(s)).unwrap();
            proptest::prop_assert_eq!(ours, serde);
        }

        #[test]
        fn numbers_match_serde_json(i in proptest::num::i64::ANY, f in proptest::num::f64::NORMAL) {
            let ours_i = canonical_json(&json!(i));
            proptest::prop_assert_eq!(ours_i, serde_json::to_string(&json!(i)).unwrap());
            let ours_f = canonical_json(&json!(f));
            proptest::prop_assert_eq!(ours_f, serde_json::to_string(&json!(f)).unwrap());
        }
    }

    #[test]
    fn canonical_json_escapes_control_chars_like_serde_json() {
        // Every control char plus DEL (which must NOT be escaped).
        let nasty: String = (0u8..0x21)
            .map(char::from)
            .chain(['\u{7f}', 'é', '🦀'])
            .collect();
        let value = Value::String(nasty);
        assert_eq!(
            canonical_json(&value),
            serde_json::to_string(&value).unwrap()
        );
    }

    #[test]
    fn canonical_json_sorts_keys_recursively() {
        let value = json!({
            "zebra": 1,
            "alpha": { "y": [3, 1, 2], "x": null },
            "mid": "text"
        });
        assert_eq!(
            canonical_json(&value),
            r#"{"alpha":{"x":null,"y":[3,1,2]},"mid":"text","zebra":1}"#
        );
    }

    #[test]
    fn canonical_json_preserves_array_order_and_escapes() {
        let value = json!(["b", "a", {"k": "line\nbreak \"quoted\""}]);
        assert_eq!(
            canonical_json(&value),
            r#"["b","a",{"k":"line\nbreak \"quoted\""}]"#
        );
    }

    #[test]
    fn canonical_json_is_stable_for_equal_values() {
        // Same logical object built in different key orders.
        let a: Value = serde_json::from_str(r#"{"b":1,"a":2}"#).expect("valid JSON");
        let b: Value = serde_json::from_str(r#"{"a":2,"b":1}"#).expect("valid JSON");
        assert_eq!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn compute_hash_is_deterministic_and_chained() {
        let content = r#"{"action":"table.commit"}"#;
        let genesis = compute_hash(None, content);
        assert_eq!(genesis.len(), 64);
        assert!(genesis.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(genesis, compute_hash(None, content));

        // Chaining changes the digest; different content changes the digest.
        let chained = compute_hash(Some(&genesis), content);
        assert_ne!(genesis, chained);
        assert_ne!(genesis, compute_hash(None, r#"{"action":"table.drop"}"#));
        assert_ne!(chained, compute_hash(Some(&chained), content));
    }

    #[test]
    fn compute_hash_matches_reference_construction() {
        // Reference: sha256 over the exact byte concatenation.
        let prev = "ab".repeat(32);
        let content = r#"{"k":"v"}"#;
        let mut hasher = Sha256::new();
        hasher.update(prev.as_bytes());
        hasher.update(content.as_bytes());
        let expected = hex::encode(hasher.finalize());
        assert_eq!(compute_hash(Some(&prev), content), expected);
    }

    #[test]
    fn entry_content_includes_all_hashed_fields() {
        let ts = DateTime::parse_from_rfc3339("2026-07-02T10:00:00.123456Z")
            .expect("valid timestamp")
            .with_timezone(&Utc);
        let content = entry_content(
            "01J0000000000000000000000",
            Some("01J0000000000000000000001"),
            ts,
            "user:alice",
            "table.commit",
            "table:t1",
            &json!({"snapshot_id": 42}),
        );
        assert_eq!(
            content,
            concat!(
                r#"{"action":"table.commit","details":{"snapshot_id":42},"#,
                r#""id":"01J0000000000000000000000","#,
                r#""occurred_at":"2026-07-02T10:00:00.123456Z","#,
                r#""principal":"user:alice","resource":"table:t1","#,
                r#""workspace_id":"01J0000000000000000000001"}"#
            )
        );
    }
}
