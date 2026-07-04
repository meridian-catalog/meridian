# Sharing & the data-products exchange (the neutral Delta-Sharing alternative)

Status: **Cross-org shares (J-F1) and the internal marketplace (J-F2)
implemented and tested.** A share is a scoped, read-only projection of catalog
assets to an external recipient org, served over a per-share Iceberg REST
endpoint with vended read-only credentials and an optional row/column policy
per grant. The internal marketplace is the certified-data-product gallery
(Pillar G) with a request-access flow (reusing the Pillar-D `access_requests`
object). This document says exactly what is prevented versus surfaced; where
another document disagrees on a guarantee, this one wins.

The pitch is neutrality. Delta Sharing carries Databricks gravity and Snowflake
shares only work Snowflake-to-Snowflake; Meridian's share endpoint speaks plain
Iceberg REST, so **any IRC-capable engine on the recipient side** — Spark,
Trino, DuckDB, PyIceberg, StarRocks — reads a share directly. It is built
entirely from primitives the catalog already has: **credential vending**
(`meridian-vending`), **row/column policy** (the same filter/mask primitives
Pillar D applies in the scan plan), and the **audit log**. No new trust
machinery.

## 1. The model

Two tables (migration `0024_sharing.sql`):

- **`shares`** — one row per (recipient, projection). A share has a name
  (unique per workspace), a free-text `recipient` identifier (the external org;
  an audit string like `org:acme` — Meridian does not manage the recipient's
  identity, only the token), an opaque high-entropy `token`, optional `terms`
  text plus the `terms_accepted_at` timestamp, the `created_by` audit string,
  and a `revoked` flag with `revoked_at`.

- **`share_grants`** — the projection contents. Each row adds one securable
  (a `table`, a `view`, or a certified `data_product`) to the share by stable
  `(kind, ref)`, with an optional `row_filter` (a boolean SQL predicate) and an
  optional `column_mask` (a JSONB array of column names to hide). A
  `data_product` grant expands to its member tables at serve time — the product
  is the unit a human reasons about; the endpoint still serves individual
  Iceberg tables. `(share_id, kind, ref)` is unique, and grants cascade-delete
  with the share.

The `token` is the recipient's bearer secret **and** its catalog path prefix.
It is 256 bits of `uuid`-v4 randomness rendered as hex, unique across all
shares. It is returned exactly once — on the create response — and never again
(not in `GET /api/v2/shares/{id}`, not in any audit payload). Treat it like a
password.

## 2. The two surfaces

### 2.1 Management API (`/api/v2/shares`, `/api/v2/marketplace`)

Workspace-side, authenticated by the normal OIDC middleware and
management-gated (`require_management`). A data owner:

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/api/v2/shares` | `GET`/`POST` | list shares (tokens omitted) / create a share (token returned once) |
| `/api/v2/shares/{id}` | `GET`/`DELETE` | one share with its grants / delete it |
| `/api/v2/shares/{id}/revoke` | `POST` | revoke (idempotent) |
| `/api/v2/shares/{id}/grants` | `POST` | add a securable + optional row/column policy |
| `/api/v2/shares/grants/{grant_id}` | `DELETE` | remove a grant |
| `/api/v2/marketplace/products` | `GET` | the certified-product gallery (certified first) |
| `/api/v2/marketplace/requests` | `GET`/`POST` | the request queue / request access to an asset |
| `/api/v2/marketplace/requests/{id}/decide` | `POST` | approve or deny (management-gated) |

Every management mutation writes its `audit_log` row and outbox event on the
same transaction as the state change — the invariant the whole codebase holds.

### 2.2 Recipient IRC endpoint (`/share/{token}/v1/...`)

A distinct read-only Iceberg REST catalog **per share**, addressed by the
token. It is exempted from the OIDC middleware (the `/share/` prefix; see
`crate::auth`) because an external recipient holds no Meridian OIDC identity —
the **token itself is the credential**. The handler resolves the share by token
and does its own authentication:

| Endpoint | Behavior |
| --- | --- |
| `GET /share/{token}/v1/config` | the recipient `ConfigResponse`; advertises only read endpoints; flags `terms-required` when terms are outstanding |
| `GET /share/{token}/terms` · `POST /share/{token}/terms/accept` | read / accept the terms of use |
| `GET /share/{token}/v1/namespaces` | only namespaces that hold a shared table |
| `GET /share/{token}/v1/namespaces/{ns}/tables` | only the shared tables in `ns` |
| `GET /share/{token}/v1/namespaces/{ns}/tables/{table}` | load a shared table, read-only, column-masked, with vended read-only credentials |
| `POST`/`DELETE` on any table path | `403` — a share is read-only by construction |

A requested table that is not granted returns **404**, not 403: a recipient
must not learn a table exists unless it is shared.

Every recipient access — config, list, load, terms — writes a
`share.recipient.*` audit row attributed to `recipient:<id>`, so the full
recipient trail is on record (the point of the whole feature: the court-grade
answer to "who read what, when").

## 3. What is prevented vs. surfaced (the honest part)

- **Read-only: prevented.** The recipient catalog advertises no write endpoints
  and answers every write verb with 403. The vended credentials are
  `AccessMode::Read` — scoped by the STS session policy to read-only on the
  table prefix.

- **Revocation: instant in effect.** The recipient only ever holds short-lived
  vended credentials. The moment `revoked` is set, the endpoint returns 403 and
  vends nothing new; any already-vended credentials expire on their TTL. There
  is no long-lived key to claw back. (With `vending = static`, keys do not
  expire — so static-vending warehouses trade instant revocation for
  simplicity; use STS vending where instant revocation matters.)

- **Column masking: prevented at the catalog layer.** Masked columns are
  dropped from the served current schema, so a recipient engine never learns
  they exist and cannot select them.

- **Row filtering: surfaced, not prevented.** A vended-credential engine reads
  Parquet directly from object storage; a pure IRC catalog cannot interpose a
  WHERE clause on that read. Meridian therefore *surfaces* the grant's row
  filter to the recipient (as the `meridian.share.row-filter` config property)
  and audits it, but does **not** claim to prevent a recipient whose engine
  ignores it from reading filtered-out rows. Full row-level *prevention*
  requires a query-mediated path (the workbench / scan-plan surface, where
  Meridian executes the query and folds the predicate into the plan), which is
  out of scope for the neutral IRC endpoint. This is the same prevent-vs-detect
  honesty the enforcement matrix holds for engines that bypass scan planning.

- **Terms acceptance: gated.** A share with `terms` serves no data until the
  recipient accepts (`config` still resolves and flags `terms-required`; every
  data endpoint returns 403 pointing at the accept endpoint).

## 4. The internal marketplace (J-F2)

The `GET /api/v2/marketplace/products` gallery is the Pillar-G certified data
products, certified-first — the "shopping" catalog for a workspace's own
consumers. A consumer clicks "request access", which creates a `pending` row in
the Pillar-D `access_requests` table (`POST /api/v2/marketplace/requests`); an
approver decides it (`.../decide`). Provisioning the actual grant on approval
is the D-F4 workflow wave; this records the decision on the request object.

## 5. Out of scope (explicit)

- **External / public marketplace.** The marketplace here is *internal* — for a
  workspace's own consumers. A public exchange (discovery across orgs,
  billing, listings) is a different product with a different buyer.
- **Clean-room compute.** Joining two parties' data under a compute enclave is a
  heavy compliance motion with a different buyer and is not built.
- **Row-level *prevention* over the neutral IRC endpoint** (see §3).

## 6. Where it is verified

- `crates/meridian-store/tests/shares_db.rs` — the store model: create, grant
  (with row filter + column mask), idempotent re-grant, revoke (idempotent),
  terms acceptance, token lookup, and the audit+outbox invariant (including
  that the token never leaks into the audit log).
- `crates/meridian-server/tests/shares_api.rs` — the end-to-end recipient walk
  against the dev MinIO: a share of a table lists only the shared asset,
  read-only, with vended read-only credentials; the column mask is applied to
  the served schema; a non-shared table is invisible (404); a write is 403;
  revoke denies instantly; recipient access is audited; the terms gate blocks
  then serves; and the marketplace lists certified-first and runs the
  request→decide flow.
