//! The REST commit `requirements` list for views.
//!
//! The `OpenAPI` spec defines a single view requirement,
//! `assert-view-uuid`: view commits are last-writer-wins over the whole
//! metadata except for identity, which must never change silently. A failed
//! check maps to `409 CommitFailedException` at the API boundary, exactly
//! like table requirements ([`super::requirement::RequirementFailed`] is
//! shared).

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::requirement::RequirementFailed;
use super::view::ViewMetadata;

/// One view commit requirement, tagged by `type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "kebab-case",
    rename_all_fields = "kebab-case"
)]
pub enum ViewRequirement {
    /// The view UUID must match.
    AssertViewUuid {
        /// The expected UUID.
        uuid: Uuid,
    },
}

impl ViewRequirement {
    /// Checks this requirement against the current view state (`None` when
    /// the view does not exist).
    pub fn check(&self, metadata: Option<&ViewMetadata>) -> Result<(), RequirementFailed> {
        match self {
            Self::AssertViewUuid { uuid } => {
                let Some(metadata) = metadata else {
                    return Err(RequirementFailed::new(
                        "view does not exist, so the requirement cannot hold",
                    ));
                };
                if metadata.view_uuid == *uuid {
                    Ok(())
                } else {
                    Err(RequirementFailed::new(format!(
                        "view UUID must be {uuid}, found {}",
                        metadata.view_uuid
                    )))
                }
            }
        }
    }
}
