# Credential vending

Status: implemented for S3-compatible storage (AWS STS semantics; automated
tests cover MinIO, and the real-AWS path has been run on a cloud deployment by
the maintainer, though it is not yet in the automated suite); GCS and Azure are
honest stubs. This document records the design and its decisions;
[`docs/api-status.md`](../api-status.md) is the authoritative statement of
endpoint behavior.

## What it is

Engines should not need warehouse-wide storage keys to read one table.
Credential vending makes the catalog the credential boundary: a client asks
for a table, and the catalog — after its own RBAC check — hands back
credentials scoped to **that table's storage prefix**, with a bounded
lifetime, and writes an audit row for the exchange.

Two client surfaces, per the Iceberg REST spec:

- `GET /v1/{prefix}/namespaces/{ns}/tables/{table}/credentials`
  (`loadCredentials`) → `{"storage-credentials": [{"prefix", "config"}]}`.
- The `X-Iceberg-Access-Delegation: vended-credentials` header on
  `loadTable`/`createTable` → credentials merged into
  `LoadTableResult.config` (what engines read today) and mirrored in its
  `storage-credentials` field (what newer clients prefer). pyiceberg sends
  this header by default, which is why a warehouse without vending enabled
  ignores it rather than erroring.
- `remote-signing` (alone) switches the table onto the per-table sign
  endpoint (`POST .../tables/{table}/sign`) instead of shipping
  credentials — see [ADR 005](../adr/005-remote-signing.md) and the
  [remote signing section of the API status page](../api-status.md#remote-signing).
  When both mechanisms are listed, vended credentials win.

## Warehouse opt-in (storage options)

| Key | Meaning |
|---|---|
| `vending` | `none` (default) \| `static` \| `sts` |
| `vending.role-arn` | Role to assume (required for `sts`) |
| `vending.duration-secs` | Credential TTL, 900–43200 (default 3600) |
| `endpoint.external` | Endpoint advertised to clients instead of `endpoint` |

All of these are validated at `POST /api/v2/warehouses` — a broken vending
setup fails at create time, not on the first table load. Vending requires
an `s3://` storage root (there is nothing to scope on a filesystem root).

## Modes

### `sts` — scoped, short-lived (the real thing)

Every vend is one STS `AssumeRole` call carrying an **inline session
policy** generated from the table's location: object actions
(`s3:GetObject`, plus `s3:PutObject`/`s3:DeleteObject` for read-write) on
`arn:aws:s3:::{bucket}/{table-prefix}/*`, and `s3:ListBucket` under an
`s3:prefix` condition pinned to the table prefix. No statement carries a
wildcard broader than the table prefix (unit-tested against the exact
JSON). Session policies intersect with the role's own policy — they can
only narrow it.

- **AWS**: `vending.role-arn` is a real IAM role that trusts the server's
  credentials; STS resolves regionally (no endpoint override). The role
  session name encodes the requesting principal (sanitized), so vends
  correlate in CloudTrail. MinIO is what CI and the dev loop exercise; the AWS
  path has been **run on a real cloud deployment by the maintainer** but is not
  yet covered by an automated test in this repo. GCS and Azure remain
  unimplemented.
- **MinIO**: STS is served on the same endpoint as S3, `AssumeRole` works
  with regular (even root) credentials, and the role ARN is an opaque
  required parameter — the session policy does the scoping. Verified
  against a real local MinIO: read-only creds for table A can read A,
  cannot write A, cannot read table B; expiry follows the requested TTL
  (`crates/meridian-vending/tests/minio_sts.rs`, plus a pyiceberg e2e
  where the client holds zero s3 configuration).

The signing credentials for the `AssumeRole` call come from the
warehouse's `access-key-id`/`secret-access-key` options when present,
otherwise the ambient AWS chain (env/profile/IMDS).

### `static` — passthrough (deliberate, explicit)

Many self-hosted MinIO deployments have no STS story and simply want the
catalog to hand engines the keys it already holds. `vending = "static"`
does exactly that: the warehouse's own keys, unscoped, no expiry. This is
the **only** path by which stored credential material reaches a response
body, and it exists precisely so the default posture can stay absolute:
without the opt-in, the passthrough denylist
(`access-key-id`/`secret-access-key`/`session-token`) holds everywhere.
Access mode and TTL do not apply — a static key cannot be narrowed — which
is documented rather than pretended otherwise.

### `gcs` / `azure` — not implemented

GCS downscoped access-boundary tokens and Azure user-delegation SAS are
planned; today `GcsVendor`/`AzureVendor` return an `UnsupportedCloud`
error with that exact message. (Meridian's storage layer currently
supports `s3://` and `file://` roots only, so these stubs are ahead of
their storage backends.) Nothing is faked.

## Access follows RBAC

The vend never grants more than the caller's catalog rights: a principal
holding `WRITE` or `COMMIT` on the table (directly or by inheritance) gets
read-write credentials; one holding only `READ` gets read-only; one
holding neither gets 403 and no vend. With `auth.mode = "disabled"` the
anonymous principal passes every check and vends read-write — the same
open-door posture as the rest of the dev mode, warned loudly at startup.

## Every vend is audited

A vend is a security-relevant event even though it mutates no catalog
state. Each one writes an `audit_log` row (`credential.vend`) and an
`events_outbox` event (`credential.vended`) **in one transaction, before
the credentials leave the server** — principal, warehouse, table, scope
prefix, access mode, vending mode, TTL, expiry. If that write fails, the
client gets an error and no credentials. The audit row is the product.

## External endpoint advertisement

`endpoint.external` solves the container-networking split (documented in
the engine conformance READMEs): the server reaches MinIO at one address
(say `localhost:9000`) while engines in containers need another
(`host.docker.internal:9000`). When set, **every** client-facing config —
`LoadTableResult.config`, `LoadViewResult.config`, vended credential
config — advertises the external endpoint, while the server keeps using
`endpoint` internally, including for its own STS calls.

## Decisions worth recording

- **Vend on `loadTable`/`createTable`, not on `stage-create` or
  `registerTable`.** A staged create has no table row (nothing to audit
  against); a registered table's first load vends normally. `createTable`
  vends read-write because the caller is about to write the table's first
  data files and pyiceberg uses the create response's config directly.
- **`loadCredentials` on a vending-disabled warehouse is a 400**, not an
  empty list a client would misread as "no credentials needed". The header
  path, by contrast, ignores the header on such warehouses — pyiceberg
  sends it unconditionally.
- **TTL is warehouse configuration, not client input.** A client-supplied
  TTL adds a negotiation surface with no consumer today; the audit row
  records what was granted.
- **`storage-credentials` and `config` both carry the credentials** in
  load results: `config` is what shipping engines actually consume,
  `storage-credentials` is the spec's forward-looking shape. Redundant by
  design; remove the `config` copy only when the ecosystem moves.
