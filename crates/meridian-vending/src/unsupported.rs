//! Honest stubs for clouds without a vending implementation yet.
//!
//! GCS credential-access-boundary (downscoped) tokens and Azure
//! user-delegation SAS are on the roadmap; until they exist these types
//! return [`VendingError::UnsupportedCloud`] with a message that says so —
//! nothing is faked. They also pin down the dispatch surface so adding the
//! real implementations changes no caller.

use std::time::Duration;

use crate::{AccessMode, CredentialVendor, TableScope, VendedCredentials, VendingError};

/// GCS downscoped-token vending — not implemented yet.
#[derive(Debug, Clone, Copy, Default)]
pub struct GcsVendor;

impl CredentialVendor for GcsVendor {
    async fn vend(
        &self,
        _scope: &TableScope,
        _access: AccessMode,
        _ttl: Duration,
    ) -> Result<VendedCredentials, VendingError> {
        Err(VendingError::UnsupportedCloud {
            cloud: "gcs",
            detail: "GCS downscoped access-boundary tokens are not implemented yet; \
                     configure client-side credentials for GCS warehouses"
                .to_owned(),
        })
    }
}

/// Azure user-delegation SAS vending — not implemented yet.
#[derive(Debug, Clone, Copy, Default)]
pub struct AzureVendor;

impl CredentialVendor for AzureVendor {
    async fn vend(
        &self,
        _scope: &TableScope,
        _access: AccessMode,
        _ttl: Duration,
    ) -> Result<VendedCredentials, VendingError> {
        Err(VendingError::UnsupportedCloud {
            cloud: "azure",
            detail: "Azure user-delegation SAS vending is not implemented yet; \
                     configure client-side credentials for Azure warehouses"
                .to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stubs_refuse_with_a_clear_error() {
        let scope = TableScope::from_s3_location("s3://b/p").expect("scope");
        for (cloud, result) in [
            (
                "gcs",
                GcsVendor
                    .vend(&scope, AccessMode::Read, Duration::from_secs(900))
                    .await,
            ),
            (
                "azure",
                AzureVendor
                    .vend(&scope, AccessMode::Read, Duration::from_secs(900))
                    .await,
            ),
        ] {
            match result {
                Err(VendingError::UnsupportedCloud { cloud: c, detail }) => {
                    assert_eq!(c, cloud);
                    assert!(detail.contains("not implemented yet"));
                }
                other => panic!("expected UnsupportedCloud for {cloud}, got {other:?}"),
            }
        }
    }
}
