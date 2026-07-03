//! Meridian attribute-based access control (Pillar D / D-F1): a Cedar
//! policy engine plus row-filter/column-mask resolution for cross-engine
//! access governance.
//!
//! # What this crate is
//!
//! A **pure, database-free** decision library. It wraps AWS's
//! [`cedar-policy`](https://crates.io/crates/cedar-policy) crate and adds
//! the two things Meridian needs on top of a policy evaluator:
//!
//! 1. **A fixed catalog policy model** — Meridian principals (human,
//!    service, agent, with groups/roles/purpose/environment), catalog
//!    resources (namespace/table/view/column, with tags/owner/
//!    classification), catalog actions (read/write/commit/…), and a request
//!    context (time, purpose, session) — mapped to Cedar entities and a
//!    request, evaluated to a [`Decision`] whose *reason* is captured for
//!    the audit trail (D-F2: the audit trail is the product).
//! 2. **Enforcement resolution** — given a `(principal, table)` and the
//!    tags on the table and its columns, which [`RowFilter`]s and
//!    [`ColumnMask`]s apply. A `RowFilter` compiles to the exact
//!    [`meridian_iceberg::expr::Expression`] that the scan-plan enforcement
//!    seam (D-F2.1) folds into every returned `FileScanTask` residual, so
//!    "what the policy says" and "what the planner enforces" cannot drift.
//!
//! It also ships a **tag → policy convenience layer** ([`AbacRule`]) that
//! compiles the common D-F1 rule shapes ("`pii:high` denies read unless a
//! purpose is granted", owner-allow, group-based, time-bound, tag→filter,
//! tag→mask) to Cedar, and **validation/dry-run** ([`validate`]) so a
//! malformed policy is rejected before it is saved.
//!
//! # The type boundary with the store
//!
//! This crate owns the **enforcement-decision** vocabulary
//! ([`AuthzPrincipal`], [`AuthzResource`], [`Decision`], [`RowFilter`],
//! [`ColumnMask`], [`Enforcement`], [`AbacRule`]). The store (a later wave)
//! owns the **persistence** vocabulary (its `principals`/`grants`/`tags`/
//! `policies` rows) and maps those rows onto these input types. This crate
//! depends on nothing but `cedar-policy`, `meridian-iceberg` (for the
//! `Expression` output type), and small utility crates — never on the store
//! or server. See `docs/adr/009-cedar-abac.md`.
//!
//! # Determinism
//!
//! Every decision is a pure function of `(policies, principal, action,
//! resource, context)`. The only ambient input is wall-clock time, which is
//! passed in explicitly via [`RequestContext`] (defaulting to
//! [`RequestContext::now`]) so a decision can be replayed exactly for audit.
//!
//! # Example
//!
//! ```
//! use meridian_authz::{
//!     Action, AuthzPrincipal, AuthzResource, PolicyEngine, PrincipalKind,
//!     RequestContext, ResourceKind, engine::BaseEffect,
//! };
//!
//! // "pii:high denies read unless the fraud_investigation purpose is set."
//! let policy = r#"
//!     @id("pii-high-deny")
//!     @description("pii:high denies read unless a matching purpose is granted")
//!     forbid(principal, action == Action::"read", resource)
//!       when { resource.tags.contains("pii:high") }
//!       unless { context has purpose && context.purpose == "fraud_investigation" };
//! "#;
//! let engine = PolicyEngine::new(policy, BaseEffect::AllowUnlessForbidden).unwrap();
//!
//! let alice = AuthzPrincipal::new("alice", PrincipalKind::User);
//! let orders = AuthzResource::new("sales.orders", ResourceKind::Table).with_tag("pii:high");
//!
//! // No purpose -> denied by the forbid.
//! let d = engine
//!     .authorize(&alice, Action::Read, &orders, &RequestContext::now())
//!     .unwrap();
//! assert!(d.is_deny());
//!
//! // With the granted purpose -> the forbid is lifted, baseline allows.
//! let ctx = RequestContext::now().with_purpose("fraud_investigation");
//! let d = engine.authorize(&alice, Action::Read, &orders, &ctx).unwrap();
//! assert!(d.is_allow());
//! ```

pub mod context;
pub mod decision;
pub mod enforcement;
pub mod engine;
pub mod error;
pub mod principal;
pub mod resolve;
pub mod resource;
pub mod rules;
pub mod validate;

pub use context::RequestContext;
pub use decision::{Decision, DeterminingPolicy, Effect};
pub use enforcement::{ColumnMask, Enforcement, MaskKind, RowFilter, RowPredicate};
pub use engine::{BaseEffect, PolicyEngine};
pub use error::AuthzError;
pub use principal::{Action, AuthzPrincipal, PrincipalKind};
pub use resolve::{ResolvedColumn, resolve_filters_and_masks};
pub use resource::{AuthzResource, ResourceKind};
pub use rules::{AbacRule, compile_ruleset};
pub use validate::{
    dry_run, meridian_schema, meridian_schema_source, validate_against_schema,
    validate_against_schema_mode, validate_syntax,
};
