//! The request context: attributes of *this call*, not of the principal or
//! resource.
//!
//! Cedar's `context` is where time-bound and purpose-based policies read
//! their inputs. A [`RequestContext`] carries the wall-clock time (so
//! time-window policies can compare against it), the purpose declared *for
//! this request* (which may differ from a principal's standing purpose),
//! and an open bag of session attributes.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::Value;

/// Attributes of a single authorization request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestContext {
    /// The moment the request is evaluated. Time-bound policies compare
    /// `context.now` against a window. Defaults to [`Utc::now`] via
    /// [`RequestContext::now`].
    pub now: DateTime<Utc>,
    /// The purpose declared for this request (purpose-based access,
    /// D-F1). Matched by `context.purpose == "…"`.
    pub purpose: Option<String>,
    /// Open bag of extra session attributes (e.g. `mfa == true`,
    /// `source_ip`, `ticket == "INC-123"` for break-glass).
    pub session: BTreeMap<String, Value>,
}

impl RequestContext {
    /// A context stamped at the current instant with no purpose or session
    /// attributes.
    #[must_use]
    pub fn now() -> Self {
        Self {
            now: Utc::now(),
            purpose: None,
            session: BTreeMap::new(),
        }
    }

    /// A context stamped at an explicit instant (deterministic tests, and
    /// replaying a decision for audit).
    #[must_use]
    pub fn at(now: DateTime<Utc>) -> Self {
        Self {
            now,
            purpose: None,
            session: BTreeMap::new(),
        }
    }

    /// Sets the request purpose (builder style).
    #[must_use]
    pub fn with_purpose(mut self, purpose: impl Into<String>) -> Self {
        self.purpose = Some(purpose.into());
        self
    }

    /// Sets a session attribute (builder style).
    #[must_use]
    pub fn with_session(mut self, key: impl Into<String>, value: Value) -> Self {
        self.session.insert(key.into(), value);
        self
    }
}

impl Default for RequestContext {
    fn default() -> Self {
        Self::now()
    }
}
