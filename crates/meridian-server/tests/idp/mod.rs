//! An in-process OIDC issuer for authentication tests.
//!
//! Serves a discovery document and a JWKS (with a fetch counter and
//! rotatable keys) on an ephemeral port, and mints RS256 tokens with
//! arbitrary claims. The embedded RSA keys are TEST FIXTURES ONLY —
//! generated for this test suite, never used anywhere else, and worthless
//! outside it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};

/// Audience the test server is configured to require.
pub(crate) const AUDIENCE: &str = "meridian-catalog-tests";

/// `kid` of test key 1 (served from boot).
pub(crate) const KID1: &str = "test-key-1";

/// `kid` of test key 2 (introduced by rotation).
pub(crate) const KID2: &str = "test-key-2";

/// Test-only RSA-2048 private key backing [`KID1`].
pub(crate) const KEY1_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDElzZv658n5qEY
V3dirdvPWJREMcJ0MBm6NvmceKsdUEw6B7ugzsLiBR5mZKIJETQHGW2kHVdlGAE2
EvPjDrGX1B9jAyobZHEKBR5aHiVvGmnKC4vDEPpN5YMAR1p1SQQmPXsOjriZ27b4
hOaolwI8mttRb3M7bWOAgN2p5F5eYkuOCcRjfKEDK9t7bGxjuANJO4j2Ggi4HiuO
AzyN4cWyyUqivQqq+ABSvNp5zTuTsISu3bqc43lutNmq48ecEIGTPvKakg5XpArt
S8Hw69sjGhsukdPTL2vG43YpYPMjTukUkvE0Ca8DtKm+TMD75QZEFRrC8zSL9EV5
uPUsmlL/AgMBAAECggEABNw+pAWdqoxf/HOwQInPElvgOMFlEoLcXxWcFh751kQV
kQl/IN/vUPbg4LIwkHpErtwHIxSF9gGa3oFurBF6NYvpohL7O3eVhsDWMuD0CMCM
z8pAgkZ+5xDmD9iJGQxyeaX/zyGbLjeCdZs/jU4LWZzJZagw5qMr34q6vnLRAASq
dO01lWnw5e9Q2AULJArNtWmlNl7g86vLvs5PUe5hpJVAytHu6RiBXLMm+EB24TSh
64rQcZynVc+fQRI7b0lohwjcNP2DlvJH5M86ZxHAw/sFms5s/PHdkz9e4NNM7BBW
rNsNeYKodc0RDt1/BPCsz6nkT5yetRxAIQ707j/9MQKBgQDol7n+zJ//rIutITMk
koYJxHGaDdGgj4jA8JZjjQ4eHkVxRIqOnM2eWRcDlnnhqh5s9k71Cy8eswLSmMsM
0jLhCLX99pEptTc4caOAILivJrMAlSEI26vwhWHKOGk5WPcc3FZ15nEkumuF0Uz5
q9y0xOnnJlH3Tbtm6czUQef1EQKBgQDYX/fcsu3I/TNLU7zAyTClDWjZq/UOWvja
ov/BuxC5/hLox1pVclltxCnR0iY9W7IX+WVwpfloYRTuvl5qF7jO0276wYXcnjJ5
4oDSGxntVE8wXCZ4SwnYs88NlEPb6bnJ6COtL2616Kp694YTxGbyDnwA+1e/CWwn
iNxAZVeHDwKBgDl5FGqToY2J01HKfFqzIg/TzMZmV8A293HFgUPEHRLwI/SjHSG0
OVLBbOBkFGXgpXgDPOtsAg6x6SakvrfCUPQuNNo2TRRjROvbmK0WaMxO4bhpISqR
LWFXdByF5+pVw2oMQAkOEjMjJWKBn2WqQ/UfGzUU0Pgs6vu5FfX0+x4hAoGAHsRu
s1RCGa1faNusYGF7aEzi1ujrvLHU5wn8gii+dSQavjzyrGnJK6GULMDMLTdnuJ7+
/KitMVl0p4osVLuwzMAl7MQt67QXC1vY44d1BVlStVa1Ja/N46GV1KF7kL7Ia1x2
Dj9LZ9SAwWGHEDKCTPMgUOdsj76gQXLllsaSTqMCgYA1IV6xzDjZlb6MxjvKE0Kb
IcpP8cUyzXklKe9q9X0c93zahxfcdCGwMlU1GfobIHAw/tY7o9D2f6ogsnk1Ag8H
wBpjiKTy11v1mmvoFQAqLqxXWIyVFa73u3NCPInzKarSUAZw3J93cMOVUO8AiUX0
XyRsfPFTs1IQRXWVq2Gu6A==
-----END PRIVATE KEY-----";

/// Test-only RSA-2048 private key backing [`KID2`].
pub(crate) const KEY2_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCgybKnwCRuZJDd
5Ibez2bDqmwOSMGIFHTtMbsPcBdvRR+F/3I6TDXPp3E+9k3AOmz19zpdWXAEPEwc
nFjvzoLTFnJLgl3ME89Y1/KH+5AJpP27RvscnGBwax911KxSeS5S/ExcQQexvVfY
Rpxy+zXmMh9jA79yYPd4KRECN931NRvbrwpwKbv/cU1G0hzy/xZkMAy1uDSfSExd
bJapCVPqQezBUOFRPh9e/mxuuE4VLIwlLxfFhHDsze/QVBvqix6fO4F8LVJZCj7a
R/I99EQyk7F/N2qBmdjSELdjBRPe6RJRNDeipvZc62uMevanbP5v4rdNhdDUJcUK
R3k6RhANAgMBAAECggEAATtKSQqpvMa8bVawycgg62LEzR81jEtL0f1Nh4K+SzXm
Veps/5FR4DNSJL9SPSS1bPTl14011HJ5ysZP0BZu3hsP6RAok8WbEv0keHIu2kQP
RX33snJoMrQ/W3GzmDYharVQCGzfGDdxEtEHpcHS2d6Mav++WavweVUnMjMnWhd4
8XJYgxWfJKxORr0oAVZqZSstaAtXItK8ytfNsL2C494f+xrChEI89LjtV6q5VPN0
pIFnKQgJE1om2bgF187RqyvhbQGDR4UifDQuLfOjnHG/dFi+WFKMwkD2wDgOkuLw
RRkezgx8ClooO0JqBgIS6+cd/ZI+ffynVnGB6fCWQQKBgQDMguU6WzrrgbN1Y/tJ
zO/xLRyhHOsqmt+mhDse8UzwQKaACinkSQAT+mPnzkMkdSdhijegp68Ib9RGZiln
jG6dFRkw5cxSjxf3YQJl8Xj54ko4RK2gZqlOlRrKo6RQ+FYfvXCveDGWv55E5K9P
0KXm0y0lI6nStq63HM3yqyR39QKBgQDJRMCnk1W10q0KWXB0C1OdxxpEN4cVJyaM
kHmcKFWQ2xDK8Oj3acUTEAKEUujsmHqqHX5D5iXtY98KX4WzbxzF4Q6hup4MhWaM
JUx989dOWcsOFr1z42hW1RH4Yic91HbR68mzbR2dZyUTkxFfOq4j3PHM98N7co3W
SuBa/AHguQKBgBLatnts0b/Ik1ztPMuPA0f+2rbXza593MSjSDgQEwHLVA5V4YrU
WBd/bBqA35vK2Tia34oGK5LhjHZ5ELQlNEVzHoFtjirGWnVKEkiHvJl9DU6mtkMl
c9J02KV59LoqSvZeJrdmo1u8isDbPHZlTAY9zdmwsgVlJjJni20l7hTJAoGAYzOd
+XqnLj0uyQEYajoC9qtiCOmNjSGE4Jd9OTiwI/u1pTFkwj3BwwmLFAmBgMwO+bYb
u/++BenJz2URk0Va2zV4bsJ6kBVYXA8uSo5bOuULLmCK9InLrbDLcK+AQ/tqrUEY
Y3WOuTxTi/hbAaL8nfSSwcIE+d2Wh17UgkPf8RECgYBH2r/58Z9BsEfVwyA+fliW
ovIFe2bY+yG+Y2znSwsaO8KiHkH8R6OhdQqik3NrXok66TjOQ7jsZTYnjCN5Qf9x
dPJz/axKz74vUAOmnTiEk3FUhpBoonP+Jee3C23iNaIjrzPd1k8gDUDgMIK3fLl6
6Ut+qvpatdSnuLuys9PpcQ==
-----END PRIVATE KEY-----";

/// base64url modulus of key 1 (precomputed from the PEM above).
const KEY1_N: &str = "xJc2b-ufJ-ahGFd3Yq3bz1iURDHCdDAZujb5nHirHVBMOge7oM7C4gUeZmSiCRE0BxltpB1XZRgBNhLz4w6xl9QfYwMqG2RxCgUeWh4lbxppyguLwxD6TeWDAEdadUkEJj17Do64mdu2-ITmqJcCPJrbUW9zO21jgIDdqeReXmJLjgnEY3yhAyvbe2xsY7gDSTuI9hoIuB4rjgM8jeHFsslKor0KqvgAUrzaec07k7CErt26nON5brTZquPHnBCBkz7ympIOV6QK7UvB8OvbIxobLpHT0y9rxuN2KWDzI07pFJLxNAmvA7SpvkzA--UGRBUawvM0i_RFebj1LJpS_w";

/// base64url modulus of key 2 (precomputed from the PEM above).
const KEY2_N: &str = "oMmyp8AkbmSQ3eSG3s9mw6psDkjBiBR07TG7D3AXb0Ufhf9yOkw1z6dxPvZNwDps9fc6XVlwBDxMHJxY786C0xZyS4JdzBPPWNfyh_uQCaT9u0b7HJxgcGsfddSsUnkuUvxMXEEHsb1X2Eaccvs15jIfYwO_cmD3eCkRAjfd9TUb268KcCm7_3FNRtIc8v8WZDAMtbg0n0hMXWyWqQlT6kHswVDhUT4fXv5sbrhOFSyMJS8XxYRw7M3v0FQb6osenzuBfC1SWQo-2kfyPfREMpOxfzdqgZnY0hC3YwUT3ukSUTQ3oqb2XOtrjHr2p2z-b-K3TYXQ1CXFCkd5OkYQDQ";

/// The JWKS entry for a test kid.
fn jwk_for(kid: &str) -> Value {
    let n = match kid {
        KID1 => KEY1_N,
        KID2 => KEY2_N,
        other => panic!("no test JWK for kid {other:?}"),
    };
    json!({ "kty": "RSA", "use": "sig", "alg": "RS256", "kid": kid, "n": n, "e": "AQAB" })
}

/// Mints an RS256 token signed with the key matching `kid`.
pub(crate) fn mint(kid: &str, claims: &Value) -> String {
    let pem = match kid {
        KID1 => KEY1_PEM,
        KID2 => KEY2_PEM,
        other => panic!("no test key for kid {other:?}"),
    };
    mint_with_key(kid, pem, claims)
}

/// Mints an RS256 token with an explicit signing key — the header `kid`
/// and the actual key may deliberately disagree (bad-signature cases).
pub(crate) fn mint_with_key(kid: &str, pem: &str, claims: &Value) -> String {
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some(kid.to_owned());
    let key = jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).expect("valid test RSA key");
    jsonwebtoken::encode(&header, claims, &key).expect("mint test token")
}

/// The in-process issuer.
// Shared by several test binaries; not every binary uses every helper.
#[allow(dead_code)]
pub(crate) struct TestIdp {
    /// Issuer URL (`http://127.0.0.1:<port>`).
    pub(crate) issuer: String,
    keys: Arc<std::sync::RwLock<Vec<Value>>>,
    hits: Arc<AtomicUsize>,
}

#[allow(dead_code)] // shared test-support: unused helpers vary per binary
impl TestIdp {
    /// Binds an ephemeral port and serves discovery + JWKS with the given
    /// initial key ids.
    pub(crate) async fn start(kids: &[&str]) -> Self {
        let keys = Arc::new(std::sync::RwLock::new(
            kids.iter().map(|kid| jwk_for(kid)).collect::<Vec<_>>(),
        ));
        let hits = Arc::new(AtomicUsize::new(0));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test IdP");
        let issuer = format!("http://{}", listener.local_addr().expect("IdP address"));

        let discovery = {
            let issuer = issuer.clone();
            move || {
                let issuer = issuer.clone();
                async move {
                    axum::Json(json!({
                        "issuer": issuer,
                        "jwks_uri": format!("{issuer}/jwks.json"),
                    }))
                }
            }
        };
        let jwks = {
            let keys = Arc::clone(&keys);
            let hits = Arc::clone(&hits);
            move || {
                let keys = Arc::clone(&keys);
                let hits = Arc::clone(&hits);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    let keys = keys.read().expect("keys lock").clone();
                    axum::Json(json!({ "keys": keys }))
                }
            }
        };
        let app = axum::Router::new()
            .route(
                "/.well-known/openid-configuration",
                axum::routing::get(discovery),
            )
            .route("/jwks.json", axum::routing::get(jwks));

        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve test IdP");
        });

        Self { issuer, keys, hits }
    }

    /// Replaces the served key set (simulates key rotation).
    pub(crate) fn set_keys(&self, kids: &[&str]) {
        *self.keys.write().expect("keys lock") = kids.iter().map(|kid| jwk_for(kid)).collect();
    }

    /// How many times the JWKS endpoint has been fetched.
    pub(crate) fn jwks_hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    /// Waits until the JWKS endpoint has served at least `at_least`
    /// fetches (plus a grace period for the client to store the keys).
    pub(crate) async fn wait_for_jwks_hits(&self, at_least: usize) {
        for _ in 0..100 {
            if self.jwks_hits() >= at_least {
                // The counter increments before the response is written;
                // give the client a moment to finish processing it.
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("test IdP never served {at_least} JWKS fetch(es)");
    }

    /// Base claims for a valid token from this issuer, with `extra`
    /// object entries merged in (overwriting on key collision).
    pub(crate) fn claims(&self, sub: &str, extra: Value) -> Value {
        let mut claims = json!({
            "iss": self.issuer,
            "aud": AUDIENCE,
            "sub": sub,
            "exp": chrono::Utc::now().timestamp() + 3600,
        });
        if let Value::Object(extra) = extra {
            claims.as_object_mut().expect("claims object").extend(extra);
        }
        claims
    }
}
