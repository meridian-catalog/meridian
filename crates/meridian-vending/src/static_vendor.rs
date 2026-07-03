//! Static-key passthrough vendor.
//!
//! Hands the warehouse's own configured keys to the client, unchanged and
//! unscoped. This is what many self-hosted `MinIO` deployments actually want
//! (no STS story, one bucket per warehouse) — but it means every vend
//! grants warehouse-wide access, so it is **opt-in only**: the warehouse
//! must set `vending = "static"` in its storage options. Without that
//! opt-in the server's credential denylist keeps key material out of every
//! response, exactly as before.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::{AccessMode, CredentialVendor, TableScope, VendedCredentials, VendingError};

/// Passes warehouse-configured static keys through (explicit opt-in).
#[derive(Clone)]
pub struct StaticVendor {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

/// Manual `Debug` so key material never reaches logs.
impl std::fmt::Debug for StaticVendor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticVendor")
            .field("access_key_id", &"...")
            .field("secret_access_key", &"...")
            .field("session_token", &self.session_token.as_ref().map(|_| "..."))
            .finish()
    }
}

impl StaticVendor {
    /// Builds the vendor from the warehouse's configured keys.
    ///
    /// # Errors
    ///
    /// Returns [`VendingError::Config`] when the warehouse has no static
    /// keys to pass through.
    pub fn new(
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
        session_token: Option<&str>,
    ) -> Result<Self, VendingError> {
        match (access_key_id, secret_access_key) {
            (Some(access_key_id), Some(secret_access_key))
                if !access_key_id.is_empty() && !secret_access_key.is_empty() =>
            {
                Ok(Self {
                    access_key_id: access_key_id.to_owned(),
                    secret_access_key: secret_access_key.to_owned(),
                    session_token: session_token.map(str::to_owned),
                })
            }
            _ => Err(VendingError::Config(
                "vending = \"static\" requires access-key-id and secret-access-key \
                 in the warehouse storage options"
                    .to_owned(),
            )),
        }
    }
}

impl CredentialVendor for StaticVendor {
    async fn vend(
        &self,
        scope: &TableScope,
        _access: AccessMode,
        _ttl: Duration,
    ) -> Result<VendedCredentials, VendingError> {
        // Static keys cannot be narrowed: access mode and ttl do not apply,
        // which is exactly why this mode is opt-in (see module docs).
        let mut config = BTreeMap::new();
        config.insert("s3.access-key-id".to_owned(), self.access_key_id.clone());
        config.insert(
            "s3.secret-access-key".to_owned(),
            self.secret_access_key.clone(),
        );
        if let Some(token) = &self.session_token {
            config.insert("s3.session-token".to_owned(), token.clone());
        }
        Ok(VendedCredentials {
            prefix: scope.location.clone(),
            config,
            expires_at: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn passes_configured_keys_through_unchanged() {
        let vendor = StaticVendor::new(Some("AK"), Some("SK"), None).expect("vendor");
        let scope = TableScope::from_s3_location("s3://b/wh/t").expect("scope");
        let vended = vendor
            .vend(&scope, AccessMode::Read, Duration::from_secs(900))
            .await
            .expect("vend");
        assert_eq!(vended.prefix, "s3://b/wh/t");
        assert_eq!(vended.config["s3.access-key-id"], "AK");
        assert_eq!(vended.config["s3.secret-access-key"], "SK");
        assert!(!vended.config.contains_key("s3.session-token"));
        assert!(vended.expires_at.is_none());
    }

    #[test]
    fn requires_both_keys() {
        assert!(StaticVendor::new(Some("AK"), None, None).is_err());
        assert!(StaticVendor::new(None, Some("SK"), None).is_err());
        assert!(StaticVendor::new(Some(""), Some("SK"), None).is_err());
    }

    #[test]
    fn debug_never_renders_key_material() {
        let vendor = StaticVendor::new(Some("SECRET-AK"), Some("SECRET-SK"), Some("SECRET-TOKEN"))
            .expect("vendor");
        let rendered = format!("{vendor:?}");
        assert!(!rendered.contains("SECRET"), "leaked: {rendered}");
    }
}
