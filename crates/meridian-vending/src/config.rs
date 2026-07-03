//! Parsing and validation of the vending-related warehouse storage options.
//!
//! A warehouse's `storage_options` map carries both storage-connection keys
//! (owned by `meridian-storage`) and catalog-layer keys (owned here):
//!
//! | Key | Meaning |
//! |---|---|
//! | `vending` | `none` (default) \| `static` \| `sts` |
//! | `vending.role-arn` | STS role to assume (required for `sts`) |
//! | `vending.duration-secs` | Vended-credential TTL (900–43200, default 3600) |
//! | `endpoint.external` | Endpoint advertised to clients instead of `endpoint` |
//!
//! Parsing is strict: an unknown `vending.*` key or an out-of-range value is
//! an error, surfaced at warehouse create time.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::VendingError;

/// Minimum vended-credential TTL (the STS `DurationSeconds` floor).
pub const MIN_TTL_SECS: u64 = 900;
/// Maximum vended-credential TTL (the STS chained-role ceiling; roles with a
/// smaller max-session cap will reject larger values at vend time).
pub const MAX_TTL_SECS: u64 = 43_200;
/// Default vended-credential TTL: one hour.
pub const DEFAULT_TTL_SECS: u64 = 3_600;

/// A warehouse's parsed vending configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VendingConfig {
    /// No vending: credential-shaped material never leaves the server.
    None,
    /// Pass the warehouse's static keys through (explicit opt-in).
    Static,
    /// STS `AssumeRole` with a session policy scoped to the table prefix.
    Sts {
        /// The role to assume.
        role_arn: String,
        /// Vended-credential TTL.
        ttl: Duration,
    },
}

impl VendingConfig {
    /// Parses the vending keys out of a warehouse `storage_options` map.
    ///
    /// # Errors
    ///
    /// Returns [`VendingError::Config`] for unknown modes, unknown
    /// `vending.*` keys, a missing/blank `role-arn` in `sts` mode,
    /// out-of-range durations, or vending keys present with `vending`
    /// absent or `none`.
    pub fn parse(options: &BTreeMap<String, String>) -> Result<Self, VendingError> {
        for key in options.keys() {
            if key.starts_with("vending.")
                && !matches!(key.as_str(), "vending.role-arn" | "vending.duration-secs")
            {
                return Err(VendingError::Config(format!(
                    "unknown vending option {key:?} (supported: \
                     \"vending\", \"vending.role-arn\", \"vending.duration-secs\")"
                )));
            }
        }

        let mode = options.get("vending").map_or("none", String::as_str);
        let role_arn = options.get("vending.role-arn");
        let duration = options.get("vending.duration-secs");

        match mode {
            "none" => {
                if role_arn.is_some() || duration.is_some() {
                    return Err(VendingError::Config(
                        "vending.* options require vending = \"static\" or \"sts\"".to_owned(),
                    ));
                }
                Ok(Self::None)
            }
            "static" => {
                if role_arn.is_some() || duration.is_some() {
                    return Err(VendingError::Config(
                        "vending.role-arn / vending.duration-secs apply to vending = \"sts\" only"
                            .to_owned(),
                    ));
                }
                Ok(Self::Static)
            }
            "sts" => {
                let role_arn = role_arn
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        VendingError::Config(
                            "vending = \"sts\" requires vending.role-arn".to_owned(),
                        )
                    })?;
                let ttl_secs = match duration {
                    None => DEFAULT_TTL_SECS,
                    Some(raw) => raw
                        .parse::<u64>()
                        .ok()
                        .filter(|v| (MIN_TTL_SECS..=MAX_TTL_SECS).contains(v))
                        .ok_or_else(|| {
                            VendingError::Config(format!(
                                "vending.duration-secs must be an integer between \
                                 {MIN_TTL_SECS} and {MAX_TTL_SECS}, got {raw:?}"
                            ))
                        })?,
                };
                Ok(Self::Sts {
                    role_arn: role_arn.to_owned(),
                    ttl: Duration::from_secs(ttl_secs),
                })
            }
            other => Err(VendingError::Config(format!(
                "unknown vending mode {other:?} (supported: \"none\", \"static\", \"sts\")"
            ))),
        }
    }

    /// Whether any vending mode is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Stable mode string, used in audit rows and events.
    #[must_use]
    pub fn mode_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Static => "static",
            Self::Sts { .. } => "sts",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn default_is_none() {
        assert_eq!(
            VendingConfig::parse(&BTreeMap::new()).expect("parse"),
            VendingConfig::None
        );
        assert_eq!(
            VendingConfig::parse(&opts(&[("vending", "none")])).expect("parse"),
            VendingConfig::None
        );
    }

    #[test]
    fn sts_requires_role_arn_and_bounds_duration() {
        assert!(VendingConfig::parse(&opts(&[("vending", "sts")])).is_err());
        assert!(
            VendingConfig::parse(&opts(&[("vending", "sts"), ("vending.role-arn", "  ")])).is_err()
        );

        let parsed = VendingConfig::parse(&opts(&[
            ("vending", "sts"),
            ("vending.role-arn", "arn:minio:iam:::role/vend"),
        ]))
        .expect("parse");
        assert_eq!(
            parsed,
            VendingConfig::Sts {
                role_arn: "arn:minio:iam:::role/vend".to_owned(),
                ttl: Duration::from_secs(DEFAULT_TTL_SECS),
            }
        );

        for bad in ["899", "43201", "-1", "x"] {
            assert!(
                VendingConfig::parse(&opts(&[
                    ("vending", "sts"),
                    ("vending.role-arn", "arn:aws:iam::1:role/r"),
                    ("vending.duration-secs", bad),
                ]))
                .is_err(),
                "duration {bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn static_rejects_sts_only_options() {
        assert_eq!(
            VendingConfig::parse(&opts(&[("vending", "static")])).expect("parse"),
            VendingConfig::Static
        );
        assert!(
            VendingConfig::parse(&opts(&[
                ("vending", "static"),
                ("vending.role-arn", "arn:aws:iam::1:role/r"),
            ]))
            .is_err()
        );
    }

    #[test]
    fn rejects_unknown_modes_orphan_options_and_unknown_keys() {
        assert!(VendingConfig::parse(&opts(&[("vending", "magic")])).is_err());
        assert!(VendingConfig::parse(&opts(&[("vending.role-arn", "arn")])).is_err());
        assert!(
            VendingConfig::parse(&opts(&[("vending", "sts"), ("vending.roel-arn", "arn")]))
                .is_err()
        );
    }
}
