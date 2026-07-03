//! Errors surfaced by the ABAC engine.
//!
//! A decision itself is never an error — a *deny* is a valid [`Decision`]
//! ([`crate::decision`]). These errors are for the surrounding machinery:
//! malformed policy text (rejected before save), and entity/request
//! assembly problems (a reserved attribute name, an unbuildable entity).

use thiserror::Error;

/// An error from policy parsing or request assembly.
#[derive(Debug, Error)]
pub enum AuthzError {
    /// The Cedar policy text did not parse. The message carries Cedar's
    /// own diagnostic (location + reason) so a policy author sees exactly
    /// what is wrong. This is what the validation/dry-run path returns to
    /// reject a bad policy before it is stored.
    #[error("policy parse error: {message}")]
    PolicyParse {
        /// Cedar's diagnostic message.
        message: String,
    },

    /// Policy validation against the Cedar schema failed. Carries the
    /// concatenated validation errors.
    #[error("policy validation error: {message}")]
    Validation {
        /// The validation diagnostic(s).
        message: String,
    },

    /// A principal/resource/context could not be turned into a Cedar
    /// entity (e.g. a reserved attribute name, a malformed entity type).
    #[error("entity assembly error: {message}")]
    Entity {
        /// What went wrong.
        message: String,
    },

    /// The Cedar request could not be constructed.
    #[error("request assembly error: {message}")]
    Request {
        /// What went wrong.
        message: String,
    },
}
