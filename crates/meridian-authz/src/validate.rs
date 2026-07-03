//! Policy validation and dry-run — catch a bad policy *before* it is saved,
//! and preview what a policy set would decide.
//!
//! Two levels:
//!
//! - [`validate_syntax`] parses the Cedar text and reports parse errors
//!   (the cheap, always-run check the API calls on every policy write).
//! - [`validate_against_schema`] additionally type-checks the policies
//!   against the fixed Meridian Cedar schema ([`meridian_schema`]) — it
//!   catches a policy that reads a misspelled attribute
//!   (`resource.onwer`), compares a string to a number, or names an action
//!   that does not exist. This is the "detect errors before save" the spec
//!   (D-F1) asks for.
//!
//! Dry-run ("who would lose access", "what would this decide") is just
//! [`crate::PolicyEngine::authorize`] with no persistence — the engine has
//! no side effects — so a caller previews a change by building an engine
//! from the *proposed* policy text and authorizing sample requests against
//! it. [`dry_run`] is a thin convenience wrapper that does exactly that.

use std::str::FromStr;

use cedar_policy::{PolicySet, Schema, ValidationMode, Validator};

use crate::context::RequestContext;
use crate::decision::Decision;
use crate::engine::{BaseEffect, PolicyEngine};
use crate::error::AuthzError;
use crate::principal::{Action, AuthzPrincipal};
use crate::resource::AuthzResource;

/// The Meridian Cedar **schema** in human-readable `cedarschema` form: the
/// principal/resource entity types and their attributes, and the action
/// verbs and what they apply to. Validation checks policies against this so
/// authoring mistakes are caught before save.
///
/// Kept in lockstep with the entity assembly in [`crate::engine`]: every
/// attribute the engine sets must appear here, and vice versa. The
/// `context` shape is declared on the actions.
#[must_use]
pub fn meridian_schema_source() -> String {
    // Attributes are optional where the engine only sets them conditionally
    // (`purpose`, `environment`, `owner`, `classification`), required where
    // always set (`id`, `groups`, `roles`, `tags`).
    //
    // `now` uses the datetime extension type; `now_millis` a Long. Session
    // extras are open, so context is not sealed — but Cedar requires a
    // closed context record per action. We declare the known fields and
    // leave validation permissive for extras by not referencing them in
    // shipped rules (org rules that use session extras validate with
    // `ValidationMode::permissive`).
    r"
entity User {
  id: String,
  groups: Set<String>,
  roles: Set<String>,
  purpose?: String,
  environment?: String,
};
entity Service {
  id: String,
  groups: Set<String>,
  roles: Set<String>,
  purpose?: String,
  environment?: String,
};
entity Agent {
  id: String,
  groups: Set<String>,
  roles: Set<String>,
  purpose?: String,
  environment?: String,
};

entity Namespace {
  tags: Set<String>,
  owner?: String,
  classification?: String,
};
entity View in [Namespace] {
  tags: Set<String>,
  owner?: String,
  classification?: String,
};
entity Table in [Namespace] {
  tags: Set<String>,
  owner?: String,
  classification?: String,
};
entity Column in [Table, View] {
  tags: Set<String>,
  owner?: String,
  classification?: String,
};

action read, write, commit, create, drop, alter, manage
  appliesTo {
    principal: [User, Service, Agent],
    resource: [Namespace, Table, View, Column],
    context: {
      now?: datetime,
      now_millis?: Long,
      purpose?: String,
    },
  };
"
    .to_owned()
}

/// Parses the Meridian Cedar schema. Infallible in practice (the source is
/// a constant), but returns a `Result` so a future edit that breaks it
/// surfaces loudly.
///
/// # Errors
///
/// [`AuthzError::Validation`] if the schema source does not parse.
pub fn meridian_schema() -> Result<Schema, AuthzError> {
    Schema::from_str(&meridian_schema_source()).map_err(|e| AuthzError::Validation {
        message: format!("meridian schema failed to parse: {e}"),
    })
}

/// Parses policy text, returning the number of policies on success. This is
/// the cheap syntax gate.
///
/// # Errors
///
/// [`AuthzError::PolicyParse`] with Cedar's diagnostic if the text is
/// malformed.
pub fn validate_syntax(policy_text: &str) -> Result<usize, AuthzError> {
    let set = PolicySet::from_str(policy_text).map_err(|e| AuthzError::PolicyParse {
        message: e.to_string(),
    })?;
    Ok(set.policies().count())
}

/// Type-checks policy text against the Meridian Cedar schema.
///
/// Returns `Ok(())` when the policies are valid. Uses
/// [`ValidationMode::Strict`] by default; org policies that legitimately
/// use open session-context attributes should be validated with
/// [`validate_against_schema_mode`] in permissive mode.
///
/// # Errors
///
/// [`AuthzError::PolicyParse`] if the text does not parse, or
/// [`AuthzError::Validation`] listing every validation error found.
pub fn validate_against_schema(policy_text: &str) -> Result<(), AuthzError> {
    validate_against_schema_mode(policy_text, ValidationMode::Strict)
}

/// Like [`validate_against_schema`] but with an explicit validation mode.
///
/// # Errors
///
/// As [`validate_against_schema`].
pub fn validate_against_schema_mode(
    policy_text: &str,
    mode: ValidationMode,
) -> Result<(), AuthzError> {
    let set = PolicySet::from_str(policy_text).map_err(|e| AuthzError::PolicyParse {
        message: e.to_string(),
    })?;
    let schema = meridian_schema()?;
    let validator = Validator::new(schema);
    let result = validator.validate(&set, mode);
    if result.validation_passed() {
        Ok(())
    } else {
        let messages: Vec<String> = result
            .validation_errors()
            .map(std::string::ToString::to_string)
            .collect();
        Err(AuthzError::Validation {
            message: messages.join("; "),
        })
    }
}

/// Dry-runs a proposed policy text against one sample request, returning
/// the [`Decision`] it *would* produce — without saving anything. A caller
/// evaluating a policy change loops this over the principals/resources it
/// cares about to answer "who would lose access".
///
/// # Errors
///
/// [`AuthzError::PolicyParse`] if the policy text is malformed, or an
/// entity/request assembly error.
pub fn dry_run(
    policy_text: &str,
    base: BaseEffect,
    principal: &AuthzPrincipal,
    action: Action,
    resource: &AuthzResource,
    context: &RequestContext,
) -> Result<Decision, AuthzError> {
    let engine = PolicyEngine::new(policy_text, base)?;
    engine.authorize(principal, action, resource, context)
}
