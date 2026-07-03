//! Scan planning.
//!
//! The building blocks live in this crate ([`crate::manifest`],
//! [`crate::expr`], [`crate::value`]); the planning *service* — endpoint
//! handlers, pruning orchestration, delete-file attachment, residuals,
//! caching, async execution — lives in `meridian-server::planning` (see
//! `docs/design/scan-planning.md`). Still open here (M2+): serving plans
//! from the Postgres write-through index instead of manifest reads.
