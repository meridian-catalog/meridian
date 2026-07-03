# 005. Remote signing lives in `meridian-vending`

## Status

Accepted — 2026-07-03

## Context

The Iceberg REST spec's second access-delegation mechanism, `remote-signing`,
lets an engine read and write a table's objects **without ever holding
storage credentials**: the client builds each S3 request, sends its method,
URI, and headers to the catalog, and the catalog returns SigV4 signature
headers computed with credentials only the catalog holds. The current spec
defines the endpoint as part of the catalog surface itself —
`POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign` with
`RemoteSignRequest` / `RemoteSignResult` bodies (the older standalone
`/v1/aws/s3/sign` API from `aws/s3-signer-open-api.yaml` is deprecated in
favor of it).

Until now Meridian answered `remote-signing` with an honest 400. The
question for the implementation was where the mechanics belong: a new
`meridian-signing` crate, or the existing `meridian-vending` crate.

The security question is the interesting one. A signing endpoint is a
*confused-deputy factory* unless every request is checked against the
table the endpoint is mounted on: the signature is computed with
warehouse-level credentials, so the **authorization decision — not the
signature — is the entire security boundary.** That decision needs the same
inputs credential vending already resolves: the table's storage prefix
(`TableScope`), the caller's effective access (`AccessMode` from RBAC), and
the warehouse's vending opt-in (`VendingConfig`).

## Decision

**Extend `meridian-vending`** with a `signing` module rather than adding a
crate. Remote signing and credential vending are the same trust boundary —
"turn warehouse credentials into table-scoped access" — differing only in
what crosses the wire (signature headers vs. short-lived keys). A separate
crate would re-export or duplicate `TableScope`, `AccessMode`, and
`VendingConfig`, and give the workspace two crates whose docs would each
have to explain the other.

Concretely:

- `meridian_vending::signing::RemoteSigner` — SigV4 header signing via the
  `aws-sigv4` crate (already in the dependency tree transitively through
  `aws-sdk-sts`; S3 settings: single percent-encoding, no path
  normalization). Credentials come from the warehouse's storage options and
  never leave the server.
- `meridian_vending::signing::authorize_sign_request` — the pure,
  exhaustively unit-tested policy function: given a `TableScope`, an
  `AccessMode`, and the request's method/URI/body, it either resolves the
  request to an in-scope object action or denies with a reason.
- The HTTP handler (`meridian-server::routes::signing`) does RBAC
  resolution, policy, audit, and signing — in that order, deny-fast.

### Endpoint and advertisement

- `POST /{prefix}/namespaces/{namespace}/tables/{table}/sign` (both the
  `/iceberg/v1` and `/v1` mounts), request/response exactly the spec's
  `RemoteSignRequest` / `RemoteSignResult` (multi-valued header maps).
- A table load/create carrying `X-Iceberg-Access-Delegation` with
  `remote-signing` (and not `vended-credentials`, which keeps precedence)
  on a warehouse with `vending = "sts" | "static"` gets, in
  `LoadTableResult.config`:
  - `s3.remote-signing-enabled = true`,
  - `s3.signer.endpoint = v1/{prefix}/namespaces/{ns}/tables/{t}/sign`
    (relative, per the spec's `signer.endpoint`),
  - `s3.signer = S3V4RestSigner` (the property pyiceberg's fsspec FileIO
    keys its signer activation on; inert for other clients).
- `s3.signer.uri` is deliberately **not** set: the spec defaults it to the
  catalog's base URI on the client side, which is the one value the client
  always knows correctly and the server (behind proxies, port maps,
  `host.docker.internal`) often does not.

### Authorization policy (the point of the feature)

Signing requests are resolved to bucket + decoded object key (path-style and
virtual-host addressing both supported) and checked against the table's
location prefix. Deny unless:

- the bucket is the table's bucket and the decoded key is the table prefix
  or strictly under it (traversal segments `.`/`..` and undecodable escapes
  are denied before comparison);
- the method is allowed for the caller's RBAC access: `GET`/`HEAD` with
  `READ`; `PUT`/`POST`/`DELETE` additionally with `WRITE`/`COMMIT`;
- no denied subresource is addressed (`?acl`, `?policy`, `?tagging`, ...);
- `x-amz-copy-source`, when present, also resolves inside the table prefix
  (otherwise CopyObject would read other tables with warehouse credentials);
- bucket-root requests are only ListObjects(V1/V2/Versions) with a `prefix`
  query parameter inside the table prefix (READ), or DeleteObjects
  (`POST ?delete`, WRITE) whose XML body keys **all** resolve inside the
  table prefix.

Every decision — allow and deny — writes an `audit_log` row
(`credential.sign`) and an outbox event (`credential.signed`) in one
transaction before the response leaves the server, mirroring the vending
audit contract.

## Consequences

- One crate owns the storage trust boundary; policy logic is pure and
  testable without a server or MinIO.
- `aws-sigv4` becomes a direct dependency (no new code in the lockfile — it
  was already pulled in by `aws-sdk-sts`).
- Signing requires the warehouse to hold static keys in its storage options
  (`access-key-id`/`secret-access-key`); warehouses that rely on ambient
  AWS credentials get an honest 400 from the sign endpoint until a
  credentials-provider path is added.
- The signed request executes under warehouse credentials at the storage
  layer; object stores see the catalog's identity, not the engine's. The
  per-principal record lives in Meridian's audit log, which is why the
  audit write is transactional with the decision.
- Every object-storage request from a remote-signing client costs one
  catalog round trip (plus one audit insert). `Cache-Control: private` on
  signed GET/HEAD responses lets spec-following clients reuse signatures
  within the SigV4 validity window; writes return `no-cache`. Table
  locations are cached in-process keyed by pointer version, so steady-state
  signing does not re-read `metadata.json`.
