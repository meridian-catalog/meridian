//! Pure gateway decision helpers: the stable argument digest for the activity
//! ledger, and the graceful-refusal message rendering (H-F4).
//!
//! These are database-free so they can be unit-tested directly and reused by
//! the server wrapper. The wrapper reads persisted budget/agent state (in the
//! store) and turns a refusal into one of these messages; the *shape* of the
//! refusal — and the digest that pins what was asked — lives here.

use sha2::{Digest, Sha256};

/// The reason a tool call was refused, with everything a relayable message
/// needs. This is the pure companion to the store's decision enums: the server
/// maps a store outcome onto one of these, then renders it for the agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefusalReason {
    /// The agent's kill switch is engaged (disabled).
    Killed,
    /// The agent has passed its lifecycle expiry.
    Expired,
    /// A budget cap would be exceeded.
    Budget {
        /// Which cap (a stable label, e.g. `queries_per_hour`).
        dimension: String,
        /// The cap value.
        limit: i64,
        /// Usage in the current window before this call.
        used: i64,
        /// What this call would have added.
        requested: i64,
    },
    /// Policy denied the read/query (RBAC or ABAC).
    PolicyDenied {
        /// The policy engine's human-readable reason.
        reason: String,
    },
}

impl RefusalReason {
    /// A graceful, agent-relayable message. Deliberately actionable and free of
    /// internal detail — the agent can surface this to a human verbatim.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::Killed => "This agent is suspended (kill switch engaged). No tools can be \
                             called until an owner re-enables it."
                .to_owned(),
            Self::Expired => "This agent has passed its expiry date and is no longer permitted \
                             to call tools. Ask an owner to renew it."
                .to_owned(),
            Self::Budget {
                dimension,
                limit,
                used,
                requested,
            } => format!(
                "Budget exceeded: this call would use {requested} against the {dimension} cap \
                 of {limit}, but {used} is already used in the current window. The call was \
                 refused; it will be allowed again once the window resets or the cap is raised."
            ),
            Self::PolicyDenied { reason } => {
                format!("Access denied by policy: {reason}")
            }
        }
    }

    /// The activity-ledger decision label for this refusal.
    #[must_use]
    pub fn activity_label(&self) -> &'static str {
        match self {
            Self::Killed => "refused_killed",
            Self::Expired => "refused_expired",
            Self::Budget { .. } => "refused_budget",
            Self::PolicyDenied { .. } => "denied",
        }
    }
}

/// Redaction rule for computing a stable argument digest.
///
/// The digest must let an auditor prove *what shape of thing* was asked and
/// correlate repeated calls, without persisting raw argument values (which may
/// be sensitive — a `run_sql` body, a search string). We hash the *canonical
/// key structure with values redacted to their type*, so
/// `{"sql":"SELECT ..."}` and `{"sql":"SELECT other"}` share a digest (same
/// shape) while `{"table":"x"}` differs — enough to group calls by kind. The
/// raw SQL/text still lands in the tamper-evident audit chain's details if the
/// operator wants it; the ledger keeps only the digest.
#[must_use]
pub fn args_digest(arguments: &serde_json::Value) -> String {
    let canonical = redact_to_shape(arguments);
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    hex::encode(hasher.finalize())
}

/// Renders a JSON value to a canonical *shape* string: object keys sorted, each
/// value replaced by a type token (`"s"`, `"n"`, `"b"`, `"null"`), arrays by
/// their element shapes. Deterministic and value-free.
fn redact_to_shape(value: &serde_json::Value) -> String {
    use serde_json::Value;
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(_) => "b".to_owned(),
        Value::Number(_) => "n".to_owned(),
        Value::String(_) => "s".to_owned(),
        Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(redact_to_shape).collect();
            format!("[{}]", inner.join(","))
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            let inner: Vec<String> = keys
                .iter()
                .map(|k| format!("{k}:{}", redact_to_shape(&map[*k])))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn digest_is_stable_and_value_free() {
        // Same shape, different values -> same digest.
        let a = args_digest(&json!({ "sql": "SELECT 1" }));
        let b = args_digest(&json!({ "sql": "SELECT other FROM t" }));
        assert_eq!(a, b);
        // Different shape -> different digest.
        let c = args_digest(&json!({ "table": "x" }));
        assert_ne!(a, c);
        // Deterministic.
        assert_eq!(a, args_digest(&json!({ "sql": "anything" })));
        // 64 hex chars (sha256).
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn digest_ignores_key_order() {
        let a = args_digest(&json!({ "a": "x", "b": 1 }));
        let b = args_digest(&json!({ "b": 2, "a": "y" }));
        assert_eq!(a, b);
    }

    #[test]
    fn budget_refusal_message_is_actionable() {
        let r = RefusalReason::Budget {
            dimension: "queries_per_hour".into(),
            limit: 10,
            used: 10,
            requested: 1,
        };
        let msg = r.message();
        assert!(msg.contains("queries_per_hour"));
        assert!(msg.contains("10"));
        assert!(msg.contains("refused") || msg.contains("Budget exceeded"));
        assert_eq!(r.activity_label(), "refused_budget");
    }

    #[test]
    fn killed_and_expired_messages_and_labels() {
        assert_eq!(RefusalReason::Killed.activity_label(), "refused_killed");
        assert!(RefusalReason::Killed.message().contains("suspended"));
        assert_eq!(RefusalReason::Expired.activity_label(), "refused_expired");
        assert!(RefusalReason::Expired.message().contains("expiry"));
    }

    #[test]
    fn policy_denied_carries_reason() {
        let r = RefusalReason::PolicyDenied {
            reason: "pii:high requires a granted purpose".into(),
        };
        assert!(r.message().contains("pii:high"));
        assert_eq!(r.activity_label(), "denied");
    }
}
