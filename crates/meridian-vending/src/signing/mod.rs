//! Remote signing (ADR 005): `SigV4` signature headers for client-built S3
//! requests, computed with warehouse credentials that never leave the
//! server.
//!
//! Two halves, deliberately separated:
//!
//! - [`authorize_sign_request`] (in [`policy`]) — the pure authorization
//!   decision. **This is the security boundary**: the signature below is
//!   computed with warehouse-wide credentials, so scope enforcement lives
//!   entirely in the policy, not in the signature.
//! - [`RemoteSigner`] — the signing mechanics via `aws-sigv4`, configured
//!   the way S3 requires (single percent-encoding pass, no path
//!   normalization, `x-amz-content-sha256` always present).
//!
//! The HTTP endpoint (`POST .../tables/{table}/sign`, the spec's
//! `RemoteSignRequest`/`RemoteSignResult`) lives in `meridian-server`;
//! this module knows nothing about HTTP framing or the catalog.

use std::collections::BTreeMap;
use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings,
    UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;

use crate::VendingError;

mod policy;

pub use policy::{AuthorizedSign, SignContext, SignDenial, authorize_sign_request};

/// Headers never included in the signature. `authorization`/`x-amz-date`
/// are replaced by the signing output; the rest are hop-by-hop or known to
/// be rewritten between engine and store (botocore refuses to sign the
/// same set).
const UNSIGNABLE_HEADERS: &[&str] = &[
    "authorization",
    "connection",
    "expect",
    "transfer-encoding",
    "user-agent",
    "x-amz-date",
    "x-amz-user-agent",
    "x-amzn-trace-id",
];

/// Signs S3 requests with one set of (warehouse) credentials.
#[derive(Debug)]
pub struct RemoteSigner {
    credentials: Credentials,
}

impl RemoteSigner {
    /// Builds a signer from static credentials.
    ///
    /// # Errors
    ///
    /// Returns [`VendingError::Config`] when either key is blank.
    pub fn new(
        access_key_id: &str,
        secret_access_key: &str,
        session_token: Option<&str>,
    ) -> Result<Self, VendingError> {
        if access_key_id.trim().is_empty() || secret_access_key.trim().is_empty() {
            return Err(VendingError::Config(
                "remote signing requires the warehouse's access-key-id and \
                 secret-access-key storage options"
                    .to_owned(),
            ));
        }
        Ok(Self {
            credentials: Credentials::new(
                access_key_id,
                secret_access_key,
                session_token.map(str::to_owned),
                None,
                "meridian-remote-signing",
            ),
        })
    }

    /// Computes the `SigV4` headers for one request and returns **only the
    /// headers signing added** (`authorization`, `x-amz-date`,
    /// `x-amz-content-sha256`, `x-amz-security-token`). Returning the
    /// added set — not an echo of the input — keeps clients that *append*
    /// response headers (pyiceberg) from duplicating `Host` and friends,
    /// while clients that *merge* (the Java SDK signer) behave identically
    /// either way.
    ///
    /// The payload hash: a client-supplied `x-amz-content-sha256` wins
    /// (signed as-is, not re-emitted); otherwise a supplied body is hashed;
    /// otherwise the payload is signed as `UNSIGNED-PAYLOAD`.
    ///
    /// # Errors
    ///
    /// Returns [`VendingError::Provider`] when `aws-sigv4` rejects the
    /// request (unparseable URI, header values it cannot canonicalize).
    pub fn sign_request(
        &self,
        method: &str,
        uri: &str,
        region: &str,
        headers: &BTreeMap<String, Vec<String>>,
        body: Option<&str>,
    ) -> Result<Vec<(String, String)>, VendingError> {
        let provided_sha = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-amz-content-sha256"))
            .and_then(|(_, values)| values.first());
        let (signable_body, checksum_kind) = match (provided_sha, body) {
            // The client already carries the header: sign it in place and
            // do not emit a duplicate.
            (Some(sha), _) => (
                SignableBody::Precomputed(sha.clone()),
                PayloadChecksumKind::NoHeader,
            ),
            (None, Some(body)) => (
                SignableBody::Bytes(body.as_bytes()),
                PayloadChecksumKind::XAmzSha256,
            ),
            (None, None) => (
                SignableBody::UnsignedPayload,
                PayloadChecksumKind::XAmzSha256,
            ),
        };

        let signable_headers: Vec<(&str, &str)> = headers
            .iter()
            .filter(|(name, _)| {
                !UNSIGNABLE_HEADERS
                    .iter()
                    .any(|skip| name.eq_ignore_ascii_case(skip))
            })
            .flat_map(|(name, values)| {
                values
                    .iter()
                    .map(move |value| (name.as_str(), value.as_str()))
            })
            .collect();

        let provider_error = |e: &dyn std::fmt::Display| {
            VendingError::Provider(format!("SigV4 signing failed: {e}"))
        };

        // S3 signing settings: single percent-encoding pass and no path
        // normalization (S3 compares the canonical path byte-for-byte),
        // exactly what the AWS SDK's own S3 signer configures.
        let mut settings = SigningSettings::default();
        settings.percent_encoding_mode = PercentEncodingMode::Single;
        settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
        settings.payload_checksum_kind = checksum_kind;

        let identity: Identity = self.credentials.clone().into();
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(region)
            .name("s3")
            .time(SystemTime::now())
            .settings(settings)
            .build()
            .map_err(|e| provider_error(&e))?;

        let signable =
            SignableRequest::new(method, uri, signable_headers.into_iter(), signable_body)
                .map_err(|e| provider_error(&e))?;
        let (instructions, _signature) = sign(signable, &params.into())
            .map_err(|e| provider_error(&e))?
            .into_parts();

        Ok(instructions
            .headers()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> BTreeMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), vec![(*v).to_owned()]))
            .collect()
    }

    #[test]
    fn blank_credentials_are_rejected() {
        assert!(RemoteSigner::new("", "secret", None).is_err());
        assert!(RemoteSigner::new("key", "  ", None).is_err());
    }

    #[test]
    fn signing_returns_only_added_headers() {
        let signer = RemoteSigner::new("AKIDEXAMPLE", "secret", None).expect("signer");
        let added = signer
            .sign_request(
                "GET",
                "http://localhost:9000/bucket/prefix/data/f1.parquet",
                "us-east-1",
                &headers(&[("Host", "localhost:9000")]),
                None,
            )
            .expect("sign");
        let names: Vec<&str> = added.iter().map(|(name, _)| name.as_str()).collect();
        assert!(names.contains(&"authorization"), "{names:?}");
        assert!(names.contains(&"x-amz-date"), "{names:?}");
        assert!(names.contains(&"x-amz-content-sha256"), "{names:?}");
        assert!(
            !names.contains(&"host"),
            "input headers must not echo: {names:?}"
        );
        let sha = added
            .iter()
            .find(|(name, _)| name == "x-amz-content-sha256")
            .expect("payload hash header");
        assert_eq!(sha.1, "UNSIGNED-PAYLOAD");
    }

    #[test]
    fn session_tokens_join_the_signature() {
        let signer = RemoteSigner::new("AKIDEXAMPLE", "secret", Some("token")).expect("signer");
        let added = signer
            .sign_request(
                "GET",
                "http://localhost:9000/bucket/prefix/f",
                "us-east-1",
                &BTreeMap::new(),
                None,
            )
            .expect("sign");
        assert!(
            added
                .iter()
                .any(|(name, value)| name == "x-amz-security-token" && value == "token"),
            "{added:?}"
        );
    }

    #[test]
    fn client_supplied_payload_hash_is_not_duplicated() {
        let signer = RemoteSigner::new("AKIDEXAMPLE", "secret", None).expect("signer");
        let added = signer
            .sign_request(
                "PUT",
                "http://localhost:9000/bucket/prefix/f",
                "us-east-1",
                &headers(&[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")]),
                None,
            )
            .expect("sign");
        assert!(
            !added.iter().any(|(name, _)| name == "x-amz-content-sha256"),
            "must not re-emit the client's own header: {added:?}"
        );
        // ...but it is signed: the signed-headers list names it.
        let authorization = &added
            .iter()
            .find(|(name, _)| name == "authorization")
            .expect("authorization header")
            .1;
        assert!(
            authorization.contains("x-amz-content-sha256"),
            "{authorization}"
        );
    }

    #[test]
    fn a_body_is_hashed_into_the_signature() {
        let signer = RemoteSigner::new("AKIDEXAMPLE", "secret", None).expect("signer");
        let added = signer
            .sign_request(
                "POST",
                "http://localhost:9000/bucket?delete",
                "us-east-1",
                &BTreeMap::new(),
                Some("<Delete><Object><Key>prefix/f</Key></Object></Delete>"),
            )
            .expect("sign");
        let sha = &added
            .iter()
            .find(|(name, _)| name == "x-amz-content-sha256")
            .expect("payload hash header")
            .1;
        assert_eq!(sha.len(), 64, "hex sha256, got {sha}");
    }
}
