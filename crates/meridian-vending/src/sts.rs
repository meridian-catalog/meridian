//! AWS STS `AssumeRole` vendor (AWS S3 and `MinIO`).
//!
//! Every vend is one `AssumeRole` call carrying an inline session policy
//! from [`crate::policy`], so the returned keys can touch nothing beyond
//! the one table prefix (the session policy intersects with the role's own
//! policy — it can only narrow, never widen).
//!
//! Endpoint notes:
//!
//! - **AWS**: leave `endpoint` unset; the SDK resolves the regional STS
//!   endpoint. The role must trust the caller (`sts:AssumeRole`), and
//!   `role_arn` is a real IAM role.
//! - **`MinIO`**: STS is served on the *same* endpoint as S3. `MinIO` accepts
//!   `AssumeRole` signed with regular (even root) credentials and treats
//!   `role_arn` as an opaque required parameter — the session policy is
//!   what scopes the result. Verified against a real local `MinIO`.
//!
//! The signing credentials come from the warehouse's storage options when
//! present, otherwise from the ambient AWS credential chain (env,
//! profile, IMDS) — the same resolution order the storage layer uses.

use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_sdk_sts::config::{Credentials, Region, SharedCredentialsProvider};
use chrono::{TimeZone, Utc};

use crate::policy::s3_session_policy;
use crate::{AccessMode, CredentialVendor, TableScope, VendedCredentials, VendingError};

/// Longest role-session-name STS accepts.
const MAX_SESSION_NAME_LEN: usize = 64;

/// Vends per-table credentials via STS `AssumeRole`.
#[derive(Debug, Clone)]
pub struct StsVendor {
    /// Role to assume (opaque to `MinIO`, real IAM role on AWS).
    role_arn: String,
    /// Signing region (STS requires one even when `MinIO` ignores it).
    region: String,
    /// Endpoint override (`MinIO`); `None` resolves the AWS regional endpoint.
    endpoint: Option<String>,
    /// Explicit signing credentials; `None` uses the ambient AWS chain.
    credentials: Option<(String, String)>,
    /// Recorded as the role session name (sanitized), tying the vended
    /// session to the requesting principal in provider-side logs
    /// (`CloudTrail` on AWS).
    session_name: String,
}

impl StsVendor {
    /// Creates a vendor for one warehouse + requesting principal.
    #[must_use]
    pub fn new(
        role_arn: impl Into<String>,
        region: impl Into<String>,
        endpoint: Option<String>,
        credentials: Option<(String, String)>,
        principal: &str,
    ) -> Self {
        Self {
            role_arn: role_arn.into(),
            region: region.into(),
            endpoint,
            credentials,
            session_name: session_name(principal),
        }
    }

    fn client(&self) -> aws_sdk_sts::Client {
        let mut builder = aws_sdk_sts::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(self.region.clone()));
        if let Some((access_key_id, secret_access_key)) = &self.credentials {
            builder =
                builder.credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    access_key_id,
                    secret_access_key,
                    None,
                    None,
                    "meridian-warehouse-options",
                )));
        }
        if let Some(endpoint) = &self.endpoint {
            builder = builder.endpoint_url(endpoint.clone());
        }
        aws_sdk_sts::Client::from_conf(builder.build())
    }
}

impl CredentialVendor for StsVendor {
    async fn vend(
        &self,
        scope: &TableScope,
        access: AccessMode,
        ttl: Duration,
    ) -> Result<VendedCredentials, VendingError> {
        let policy = s3_session_policy(&scope.bucket, &scope.key_prefix, access);
        let duration_secs = i32::try_from(ttl.as_secs())
            .map_err(|_| VendingError::Config(format!("ttl {ttl:?} is out of range")))?;

        let output = self
            .client()
            .assume_role()
            .role_arn(&self.role_arn)
            .role_session_name(&self.session_name)
            .policy(policy)
            .duration_seconds(duration_secs)
            .send()
            .await
            .map_err(|e| {
                // DisplayErrorContext renders the full source chain (the
                // service error text lives below the top-level variant).
                VendingError::Provider(format!(
                    "AssumeRole failed: {}",
                    aws_sdk_sts::error::DisplayErrorContext(e)
                ))
            })?;

        let creds = output.credentials().ok_or_else(|| {
            VendingError::Provider("AssumeRole returned no credentials".to_owned())
        })?;

        let expires_at = Utc
            .timestamp_millis_opt(
                creds.expiration().to_millis().map_err(|e| {
                    VendingError::Provider(format!("unrepresentable expiration: {e}"))
                })?,
            )
            .single()
            .ok_or_else(|| {
                VendingError::Provider("unrepresentable expiration timestamp".to_owned())
            })?;

        let mut config = std::collections::BTreeMap::new();
        config.insert(
            "s3.access-key-id".to_owned(),
            creds.access_key_id().to_owned(),
        );
        config.insert(
            "s3.secret-access-key".to_owned(),
            creds.secret_access_key().to_owned(),
        );
        config.insert(
            "s3.session-token".to_owned(),
            creds.session_token().to_owned(),
        );
        config.insert(
            "s3.session-token-expires-at-ms".to_owned(),
            expires_at.timestamp_millis().to_string(),
        );

        Ok(VendedCredentials {
            prefix: scope.location.clone(),
            config,
            expires_at: Some(expires_at),
        })
    }
}

/// Maps a principal audit string onto a valid role session name:
/// STS allows `[\w+=,.@-]{2,64}`.
fn session_name(principal: &str) -> String {
    let mut name: String = format!("meridian-{principal}")
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '+' | '=' | ',' | '.' | '@' | '-' | '_') {
                c
            } else {
                '-'
            }
        })
        .take(MAX_SESSION_NAME_LEN)
        .collect();
    if name.len() < 2 {
        "meridian".clone_into(&mut name);
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_names_are_sanitized_and_bounded() {
        assert_eq!(session_name("anonymous"), "meridian-anonymous");
        assert_eq!(
            session_name("user:auth0|abc def"),
            "meridian-user-auth0-abc-def"
        );
        let long = session_name(&"x".repeat(200));
        assert_eq!(long.len(), MAX_SESSION_NAME_LEN);
        assert!(
            session_name("")
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "+=,.@-_".contains(c))
        );
    }
}
