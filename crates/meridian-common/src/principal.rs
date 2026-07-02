//! The authenticated (or anonymous) caller identity.
//!
//! This is the contract between the authentication layer (which produces a
//! [`Principal`] from a validated credential and stores it in request
//! extensions) and everything downstream that consumes it: authorization
//! checks, audit rows, ownership records. It deliberately carries no
//! authorization state — grants and roles are resolved against the store at
//! decision time, never cached on the identity.

use std::fmt;

/// What kind of actor is calling.
///
/// Agents become a first-class kind when the agent gateway lands; modeling
/// them as services until then would poison audit history, so the variant
/// exists from day one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// A human, authenticated through an IdP.
    User,
    /// A workload/service principal (engine, pipeline, CI job).
    Service,
    /// An AI agent principal (governed separately from services).
    Agent,
    /// No credential presented and the deployment allows it.
    Anonymous,
}

/// The authenticated caller, as established by the authn middleware.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Principal {
    /// Actor kind.
    pub kind: PrincipalKind,
    /// Stable subject identifier. For OIDC callers this is `sub`; for the
    /// anonymous principal it is the literal `"anonymous"`.
    pub subject: String,
    /// Token issuer URL for OIDC callers; `None` for anonymous.
    pub issuer: Option<String>,
    /// Preferred display name (`preferred_username`/`email`/`client_id`
    /// in that order of preference), if the credential carried one.
    pub display_name: Option<String>,
}

impl Principal {
    /// The anonymous principal used when authentication is disabled.
    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            kind: PrincipalKind::Anonymous,
            subject: "anonymous".to_owned(),
            issuer: None,
            display_name: None,
        }
    }

    /// Whether this is the anonymous principal.
    #[must_use]
    pub fn is_anonymous(&self) -> bool {
        self.kind == PrincipalKind::Anonymous
    }

    /// The audit-log rendering, e.g. `user:auth0|abc123` or `anonymous`.
    /// Stable format: audit rows outlive refactors, so change this only
    /// with a documented migration story.
    #[must_use]
    pub fn audit_string(&self) -> String {
        match self.kind {
            PrincipalKind::Anonymous => "anonymous".to_owned(),
            PrincipalKind::User => format!("user:{}", self.subject),
            PrincipalKind::Service => format!("service:{}", self.subject),
            PrincipalKind::Agent => format!("agent:{}", self.subject),
        }
    }
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.audit_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_strings_are_stable() {
        assert_eq!(Principal::anonymous().audit_string(), "anonymous");
        let p = Principal {
            kind: PrincipalKind::Service,
            subject: "spark-etl".to_owned(),
            issuer: Some("https://idp.example.com".to_owned()),
            display_name: None,
        };
        assert_eq!(p.audit_string(), "service:spark-etl");
        assert_eq!(p.to_string(), "service:spark-etl");
    }

    #[test]
    fn anonymous_roundtrip() {
        let p = Principal::anonymous();
        assert!(p.is_anonymous());
        let json = serde_json::to_string(&p).unwrap();
        let back: Principal = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }
}
