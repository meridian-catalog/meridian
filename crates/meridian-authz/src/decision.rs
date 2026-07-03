//! The authorization decision — and the audit-grade *reason* that comes
//! with it.
//!
//! Per the spec (D-F2, §8.9), **every governance decision is audited, and
//! the audit trail is the product**. So a [`Decision`] is not a bare
//! allow/deny bit: it carries the exact set of Cedar policies that
//! *determined* the outcome and a human-readable [`reason`](Decision::reason)
//! string suitable for writing straight into the audit log. The store (a
//! later wave) persists `determining_policies` + `reason` alongside the
//! decision on the same transaction as the access it authorizes.

use serde::{Deserialize, Serialize};

/// Whether access is allowed or denied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// Access is permitted.
    Allow,
    /// Access is denied.
    Deny,
}

impl Effect {
    /// Whether this effect permits access.
    #[must_use]
    pub fn is_allow(self) -> bool {
        matches!(self, Self::Allow)
    }
}

/// One policy that contributed to the decision.
///
/// Cedar reports the ids of the policies that determined the outcome (the
/// `forbid`s that fired for a `Deny`, or the `permit`s that fired for an
/// `Allow`). We enrich each with the policy's `@id`/`@description`
/// annotations where present, so the audit reason is legible to a human
/// reviewer, not just a machine id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeterminingPolicy {
    /// The policy's stable id. Cedar assigns `policy0`, `policy1`, … when
    /// a policy set is parsed without explicit ids; a stored policy should
    /// carry an explicit `@id` annotation (which we surface as
    /// [`annotation_id`](Self::annotation_id)).
    pub policy_id: String,
    /// The policy's `@id(...)` annotation, if it declared one — a stable
    /// business-meaningful name independent of parse order.
    pub annotation_id: Option<String>,
    /// The policy's `@description(...)` annotation, if any — the
    /// human-readable reason to show in an audit timeline.
    pub description: Option<String>,
    /// The effect of this policy (`Allow` for a `permit`, `Deny` for a
    /// `forbid`).
    pub effect: Effect,
}

/// The result of an authorization decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    /// The outcome.
    pub effect: Effect,
    /// The policies that determined the outcome, most-authoritative first
    /// (for a `Deny` these are the `forbid`s that fired; for an `Allow`,
    /// the `permit`s). Empty when nothing matched — a default deny.
    pub determining_policies: Vec<DeterminingPolicy>,
    /// A human-readable explanation, built from the determining policies
    /// (and any evaluation errors). This is what a person reads in the
    /// audit log: *"denied by policy `pii-high-deny`: pii:high denies read
    /// unless a matching purpose is granted"*.
    pub reason: String,
    /// Any evaluation errors Cedar reported (e.g. a policy read an
    /// attribute the entity did not carry). Errors never *grant* access —
    /// a policy that errors simply does not contribute a permit — but they
    /// are recorded so a misauthored policy is visible rather than silent.
    pub errors: Vec<String>,
}

impl Decision {
    /// Whether the decision permits access.
    #[must_use]
    pub fn is_allow(&self) -> bool {
        self.effect.is_allow()
    }

    /// Whether the decision denies access.
    #[must_use]
    pub fn is_deny(&self) -> bool {
        !self.is_allow()
    }
}
