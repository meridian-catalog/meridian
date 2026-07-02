# Iceberg REST Catalog API status

This is the authoritative statement of which Iceberg REST Catalog (IRC)
operations Meridian implements, what is partial, and where behavior
deliberately deviates from the specification. If another document disagrees
with this one, this one wins.

The endpoint list below follows the upstream OpenAPI definition,
[`rest-catalog-open-api.yaml`](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml),
as of the `main` branch in July 2026 (which includes endpoints newer than any
released spec version: scan planning, functions, `unregister`,
`register-view`). Every operation was checked against the running server when
this page was written; the page is updated whenever the surface changes.

Beyond endpoint-level checks, the surface is exercised by real clients:
the [e2e suite](../conformance/e2e/) (pyiceberg, DuckDB) and per-engine
smoke tests (Flink) — see the
[engine conformance matrix](../conformance/engines/README.md).

> [!WARNING]
> **AUTHENTICATION — AND WITH IT AUTHORIZATION — IS OFF BY DEFAULT.**
> With the default `auth.mode = "disabled"`, every endpoint — including the
> warehouse and RBAC management APIs — is open: anyone who can reach the
> port owns the catalog, all operations are audited as `anonymous`, and
> **authorization is bypassed entirely**. The server logs a loud warning at
> startup in this mode; it is a dev loop, not a deployment posture.
>
> With `auth.mode = "oidc"` (see [Authentication](#authentication)), the
> namespace, table, and view surfaces are **deny-by-default RBAC**: an
> authenticated principal can do nothing until it holds a grant or a role
> (see [Authorization (RBAC)](#authorization-rbac)). Bootstrap the first
> administrator with `auth.bootstrap_admin`.

## How the API is mounted

- The IRC surface is served at **both** `/iceberg/v1/...` and the bare
  `/v1/...` alias that many clients default to. The two mounts are identical.
- The IRC `{prefix}` is a **warehouse name**. Warehouses are created through
  the (non-IRC) management API: `POST /api/v2/warehouses`. Clients that send
  `GET /v1/config?warehouse=<name>` receive that name back as a `prefix`
  override, per the spec.
- Multi-level namespaces use the spec's `0x1F` unit-separator encoding
  (`%1F` in URLs).
- `GET /v1/config` advertises the exact implemented endpoint set in the
  `endpoints` field, so spec-aware clients do not have to guess.

## Endpoint matrix

Status legend: **Implemented** — works as specified (notes may record
documented divergences) · **Partial** — the operation works but a documented
piece is missing · **Not yet** — returns 404/405.

### Configuration and OAuth

| Operation | Endpoint | Status | Notes |
|---|---|---|---|
| `getConfig` | `GET /v1/config` | Implemented | `warehouse` param resolves a registered warehouse into a `prefix` override (unknown warehouse → 404). Returns the implemented `endpoints` list and `idempotency-key-lifetime: PT24H`. |
| `getToken` | `POST /v1/oauth/tokens` | Not yet | And not planned: the endpoint is deprecated for removal in the spec itself. Meridian's authentication is OIDC-native (see [Authentication](#authentication)): it validates tokens from external identity providers and never issues its own. |

### Namespaces

| Operation | Endpoint | Status | Notes |
|---|---|---|---|
| `listNamespaces` | `GET /v1/{prefix}/namespaces` | Implemented | `parent` supported (missing parent → 404). Pagination: see divergence (a). |
| `createNamespace` | `POST /v1/{prefix}/namespaces` | Implemented | Multi-level namespaces and initial properties. |
| `loadNamespaceMetadata` | `GET /v1/{prefix}/namespaces/{ns}` | Implemented | |
| `namespaceExists` | `HEAD /v1/{prefix}/namespaces/{ns}` | Implemented | 204 / 404. |
| `dropNamespace` | `DELETE /v1/{prefix}/namespaces/{ns}` | Implemented | Only empty namespaces (no child namespaces or tables); otherwise 409 `NamespaceNotEmptyError`. |
| `updateProperties` | `POST /v1/{prefix}/namespaces/{ns}/properties` | Implemented | Atomic set + remove; a key in both `updates` and `removals` → 422. |

### Tables

| Operation | Endpoint | Status | Notes |
|---|---|---|---|
| `listTables` | `GET .../namespaces/{ns}/tables` | Implemented | Pagination: see divergence (a). |
| `createTable` | `POST .../namespaces/{ns}/tables` | Partial | `stage-create` supported (metadata returned, nothing persisted until the create transaction commits with `assert-create`). `format-version` property selects format 1–3 (default 2). No credential vending: the `X-Iceberg-Access-Delegation` header is ignored; `config` carries the warehouse's non-secret storage options only (see [Storage config passthrough](#storage-config-passthrough)). Partition-spec numbering: see divergence (d). Name collisions with views: see divergence (g). **Missing:** incoming schema field ids are validated as-is instead of being treated as provisional and reassigned server-side (the Java reference runs `AssignFreshIds`); Flink's connector sends 0-based provisional ids, so Flink `CREATE TABLE` is rejected with 400 `field id 0 is not positive`. Found by the [Flink smoke](../conformance/engines/flink/README.md#known-issues), which documents a workaround. |
| `loadTable` | `GET .../tables/{table}` | Implemented | `snapshots=all\|refs`; strong `ETag` and `If-None-Match` → 304 (see [ETags](#etags)). No credential vending; `config` carries non-secret storage options only (see [Storage config passthrough](#storage-config-passthrough)). |
| `updateTable` (commit) | `POST .../tables/{table}` | Implemented | The single-table commit path: requirements checked against the current metadata, unknown update/requirement types → 400 (as the spec requires), bounded compare-and-swap retry (409 `CommitFailedException` after 3 lost races), `assert-create` finalizes a stage-create transaction. `Idempotency-Key` honored (see [Idempotency keys](#idempotency-keys)). Exercised end-to-end by pyiceberg (appends, schema evolution, two concurrent writers) and by Flink's checkpoint-driven streaming commits — see the [engine matrix](../conformance/engines/README.md). |
| `dropTable` | `DELETE .../tables/{table}` | Implemented | `purgeRequested=true` semantics: see divergence (e). |
| `tableExists` | `HEAD .../tables/{table}` | Implemented | 204 / 404. |
| `registerTable` | `POST .../namespaces/{ns}/register` | Partial | Adopts an existing metadata file as-is (it must parse and live under the warehouse root). **Missing:** `overwrite: true` is rejected with 400. Adopting a UUID that belongs to a live table is rejected: see divergence (c). |
| `renameTable` | `POST /v1/{prefix}/tables/rename` | Implemented | Rename or move across namespaces within one warehouse (prefix); 204. |
| `reportMetrics` | `POST .../tables/{table}/metrics` | Implemented | The report is validated as a JSON object and stored verbatim (it feeds the planned observability layer); 204. |
| `commitTransaction` | `POST /v1/{prefix}/transactions/commit` | Partial | Atomic multi-table commit: all requirements evaluated before anything is staged, **every** violation reported (not just the first), all pointers move in one database transaction or none do. `Idempotency-Key` honored. **Missing:** `assert-create` (staged creates) inside a transaction is rejected with 400. |
| `unregisterTable` | `POST .../tables/{table}/unregister` | Not yet | |
| `loadCredentials` | `GET .../tables/{table}/credentials` | Not yet | No credential vending anywhere in the catalog yet. |
| `signRequest` | `POST .../tables/{table}/sign` | Not yet | No remote request signing. |

### Scan planning

| Operation | Endpoint | Status |
|---|---|---|
| `planTableScan` | `POST .../tables/{table}/plan` | Not yet |
| `fetchPlanningResult` | `GET .../tables/{table}/plan/{plan-id}` | Not yet |
| `cancelPlanning` | `DELETE .../tables/{table}/plan/{plan-id}` | Not yet |
| `fetchScanTasks` | `POST .../tables/{table}/tasks` | Not yet |

### Views

| Operation | Endpoint | Status | Notes |
|---|---|---|---|
| `listViews` | `GET .../namespaces/{ns}/views` | Implemented | RBAC: `LIST_TABLES` on the namespace. Pagination: see divergence (a). |
| `createView` | `POST .../namespaces/{ns}/views` | Implemented | RBAC: `CREATE_VIEW` on the namespace. Multiple SQL representations per version (at most one per dialect, case-insensitive). 409 when the name exists as a view **or a table**: see divergence (g). Default location is uuid-suffixed under the namespace path, like tables. |
| `loadView` | `GET .../views/{view}` | Implemented | RBAC: `READ` on the view. `config` carries the warehouse's non-secret storage options (see [Storage config passthrough](#storage-config-passthrough)). No `ETag`: the spec defines the `ETag`/`If-None-Match` mechanism for table responses only (`LoadViewResponse` has no `etag` header). The `referenced-by` parameter is accepted and ignored (the caller's own `READ` on the view decides access; chain-based decisions are not implemented). |
| `replaceView` | `POST .../views/{view}` | Implemented | RBAC: `COMMIT` on the view, checked before anything is staged. The view commit path: `assert-view-uuid` checked against current metadata, unknown update/requirement types → 400, updates applied through the validating view-metadata builder (version log grows per current-version change, versions expire per `version.history.num-entries`), bounded compare-and-swap retry (409 `CommitFailedException` after 3 lost races). **Missing:** `Idempotency-Key` is not honored on view endpoints (see [Idempotency keys](#idempotency-keys)); dialect-drop protection (`replace.drop-dialect.allowed`) is not enforced yet (builder TODO). |
| `dropView` | `DELETE .../views/{view}` | Implemented | RBAC: `DROP` on the view. 204; the spec defines no purge for views, so metadata files always remain in object storage. |
| `viewExists` | `HEAD .../views/{view}` | Implemented | RBAC: `READ` on the view. 204 / 404. |
| `renameView` | `POST /v1/{prefix}/views/rename` | Implemented | RBAC: `WRITE` on the source view **and** `CREATE_VIEW` on the destination namespace. Rename or move across namespaces within one warehouse; 409 when the destination exists as a view **or a table** (divergence (g)); 204. |
| `registerView` | `POST .../namespaces/{ns}/register-view` | Not yet | |

### Functions

| Operation | Endpoint | Status |
|---|---|---|
| `listFunctions` | `GET .../namespaces/{ns}/functions` | Not yet |
| `loadFunction` | `GET .../namespaces/{ns}/functions/{function}` | Not yet |

## Documented divergences

These are deliberate, tested behaviors that differ from a strict reading of
the spec (or fill gaps the spec leaves open). Each records the rationale so
the decision can be revisited with its context intact.

### (a) `pageSize` is honored on the first request, without a `pageToken`

The spec's strict wording says a paginating server "must return all results
in a single response ... if the query parameter `pageToken` is not set".
Meridian instead engages pagination when the client signals it with
**either** parameter: a request carrying only `pageSize=N` gets at most `N`
results plus a `next-page-token`. This matches how clients commonly probe for
pagination in practice. A request with neither parameter returns the full
listing with a `null` `next-page-token`, exactly as the spec requires.
Details: tokens are opaque keyset cursors; default page size 100, hard cap
1000; `pageSize < 1` → 400; a malformed token → 400.

### (b) Table names may contain `/`

The spec is silent on the allowed character set for table names, and no
mainstream engine produces names containing `/`. Meridian accepts them
(rejecting only empty names and names containing the `0x1F` separator, which
could never be addressed in a URL). A name like `a/b` must be addressed with
the percent-encoded segment `.../tables/a%2Fb`. If engine compatibility ever
demands it, this may tighten to a rejection — do not depend on it.

### (c) `registerTable` refuses to adopt a UUID that belongs to a live table

One catalog keeps **one live table per `table-uuid`**, enforced by a unique
index. Registering a metadata file whose UUID is already registered in the
warehouse is a 409 `AlreadyExistsException` naming the conflicting UUID. The
reference JDBC catalog permits such aliasing (two catalog entries pointing at
one metadata lineage); Meridian rejects it because two pointers to one
lineage make ownership of maintenance, statistics, and purge ambiguous. To
adopt the file, drop the owning table first (or register into a different
warehouse). The full conventions are documented in
[`crates/meridian-server/src/routes/tables.rs`](../crates/meridian-server/src/routes/tables.rs)
(module docs).

### (d) `createTable` numbers a requested partition spec as id 1

A table created with a partition spec carries **two** specs in its metadata:
the empty (unpartitioned) spec as `spec-id: 0` and the requested spec as
`spec-id: 1`, with `default-spec-id: 1`. The Java reference implementation
numbers the requested spec 0. This is benign: engines resolve the active spec
through `default-spec-id`, never by assuming spec 0. (A requested spec that
is itself unpartitioned is structurally identical to spec 0 and reuses id 0.)

### (e) `purgeRequested=true` deletes metadata now; data files wait for the maintenance worker

On `DELETE .../tables/{table}?purgeRequested=true`, Meridian atomically
deletes the catalog entry and enqueues a `table.purge_requested` outbox event
in the same database transaction, then best-effort deletes the table's
`metadata/` prefix immediately. **Data files are not deleted yet** — that is
the job of the maintenance worker consuming the purge event, which does not
exist yet. Until it lands, purge removes the catalog entry and metadata
files; data files remain in object storage.

### (f) Authentication — and authorization — are off by default

The spec assumes OAuth2/OIDC bearer tokens. Meridian implements exactly
that when `auth.mode = "oidc"` (see [Authentication](#authentication)),
but the out-of-the-box default is `disabled`: every request is accepted,
audited as `anonymous`, and **exempt from authorization**. In `oidc` mode
access is deny-by-default RBAC (see
[Authorization (RBAC)](#authorization-rbac)); denials are 403 with the
IRC envelope and type `ForbiddenException`. The spec's own 403 example
uses `NotAuthorizedException` but marks the field non-prescriptive;
Meridian uses `ForbiddenException` (matching the reference Java client's
403 mapping) so 401 `NotAuthorizedException` stays unambiguous.

### (g) Tables and views share a namespace — enforced from the views side; two table-side gaps remain

The spec's `createView`, `createTable`, `registerTable`, `renameView`, and
`renameTable` all describe their 409 as "the identifier already exists as a
**table or view**": one namespace has one name space for both. Meridian's
decision is to implement that shared name space. Current enforcement:

- **Views side (complete):** `createView` and `renameView` return 409
  `AlreadyExistsException` when the requested identifier exists as a view
  *or as a table* (checked inside the create/rename transaction).
- **Tables side (known gap, tracked):** `createTable`, `registerTable`, and
  `renameTable` do not yet check the `views` table, so a table can currently
  be created with the same name as an existing view (each remains loadable
  through its own endpoint, but the identifier is ambiguous to engines).
  Postgres cannot express a cross-table unique constraint, so this needs the
  same application-level check on the table paths.
- **Related known gap:** `dropNamespace` counts child namespaces and tables
  but not views. A namespace whose only content is views is refused (the
  foreign key blocks the delete — nothing is corrupted), but the failure
  surfaces as a 500 instead of the correct 409 `NamespaceNotEmptyError`.

## Storage config passthrough

`LoadTableResult.config` and `LoadViewResult.config` forward the owning
warehouse's **non-secret** storage options under the Iceberg client
property names, so engines pointed at S3-compatible stores (MinIO, R2, ...)
resolve the endpoint and addressing style from the catalog:

| Warehouse option | Client properties |
|---|---|
| `endpoint` | `s3.endpoint` |
| `region` | `client.region`, `s3.region` |
| `path-style` | `s3.path-style-access` |

Credential material — `access-key-id`, `secret-access-key`,
`session-token` — is **never** forwarded (an explicit denylist, verified by
tests that sweep response bodies for planted credential values). Credential
delivery is the credential-vending milestone (`loadCredentials`, the
`X-Iceberg-Access-Delegation` header), not a side effect of config
passthrough. Server-side options (`retry.*`, `anonymous`) have no client
property and are not forwarded; filesystem-rooted warehouses have no
client-facing options, so their `config` is empty.

## Idempotency keys

The current spec draft attaches an optional `Idempotency-Key` header to most
mutating operations. Meridian implements it on the **two table commit
endpoints only** (`updateTable` and `commitTransaction`) — the operations
where a retried, half-applied request is genuinely dangerous. The view
endpoints the draft also annotates (`replaceView`, `dropView`, `renameView`)
do **not** honor it yet: a retried `replaceView` re-runs the commit (its
`assert-view-uuid` requirement still holds, so the usual outcome is a
second, identical version being detected as a re-add and reusing its id). Semantics
(specified in [docs/design/commit-protocol.md](design/commit-protocol.md) §8):

- The recorded fingerprint is a SHA-256 over the canonical request identity
  (endpoint + prefix + identifiers + body). Same key + same fingerprint →
  the recorded receipt is replayed (200/204 with the originally committed
  metadata). Same key + **different** fingerprint → 422, surfaced loudly
  rather than guessed at.
- Only **successful** commits are recorded; a failed commit does not burn
  the key, so the client can retry with it.
- If the server cannot determine a commit's outcome (e.g. a crash at the
  point of no return), the error instructs the client to retry with the same
  key — the retry either replays the recorded success or re-runs cleanly.
- Receipts are retained for 24 hours, advertised as
  `idempotency-key-lifetime: PT24H` in the config response.
- Divergences from the spec draft's header description: Meridian accepts any
  opaque ASCII key of 1–255 characters (the draft prescribes a 36-character
  UUIDv7), and does not replay finalized 4xx outcomes (only successes are
  recorded).

## ETags

`createTable`, `registerTable`, `loadTable`, and single-table commit
responses carry a strong `ETag` identifying the exact metadata version.
`loadTable` honors `If-None-Match` (weak comparison, lists, and `*`) and
answers 304 with no body when the client's version is current. As the spec
requires, the `snapshots=refs` and `snapshots=all` representations of the
same version carry **distinct** tags. Tags are opaque; do not parse them.

## Authentication

Meridian is OIDC-native: it validates bearer tokens issued by external
identity providers and never issues tokens of its own. Configuration
(`[auth]` in `meridian.toml`, or `MERIDIAN__AUTH__*` environment
variables):

```toml
[auth]
mode = "oidc"                 # "disabled" (default) | "oidc"

# Grants the built-in admin role to this identity at startup (idempotent).
# This is how the first administrator gets in: oidc mode is deny-by-default.
[auth.bootstrap_admin]
issuer  = "https://idp.example.com"
subject = "auth0|abc123"

[auth.oidc]
clock_skew_secs = 60          # leeway for exp/nbf validation
require_https_issuers = true  # false only for tests; logs a warning
# service_claim = "..."       # optional extra claim marking service tokens

[[auth.oidc.issuers]]
issuer_url = "https://idp.example.com"
audience   = "meridian"
# jwks_uri = "https://..."    # optional; discovered via
                              # /.well-known/openid-configuration when absent
```

Behavior in `oidc` mode:

- Every route except the health probes (`/healthz`, `/readyz`) requires
  `Authorization: Bearer <token>`; liveness never depends on IdP
  availability.
- Tokens must be RS256/RS384/RS512/ES256/ES384, signed by a key in the
  issuer's JWKS, and carry valid `exp`/`nbf` (with the configured skew),
  the exact configured `iss`, and the configured `aud`. Failures are
  `401` with the IRC envelope (`NotAuthorizedException`) and a
  `WWW-Authenticate` challenge; an unreachable IdP during a needed JWKS
  fetch is `503`, not a token error.
- JWKS are fetched at startup and refreshed on an unknown `kid`
  (single-flight, rate-limited), so IdP key rotation needs no restart.
- The caller becomes a service principal when the token carries
  client-credentials-style identity (`gty = "client-credentials"`, the
  configured `service_claim`, or neither `email` nor
  `preferred_username`); otherwise a user principal. Audit rows record
  `user:<sub>` / `service:<sub>`.
- On an identity's first authenticated request a `principals` row is
  provisioned (race-safe, audited once) so audit history and future
  grants reference a stable local identity. `GET /api/v2/principals`
  lists the provisioned principals (management access required — listing
  identities is identity enumeration).

In `disabled` mode every request runs as the anonymous principal (audit
string `anonymous`, exactly the pre-authentication behavior), and the
server logs a loud warning at startup and on `GET /v1/config` calls.

## Authorization (RBAC)

Authorization is role-based (RBAC only; attribute-based policies are a
later milestone). In `oidc` mode it is **deny by default**: an
authenticated principal holds nothing until granted. In `disabled` mode
the anonymous principal bypasses authorization entirely (see the warning
at the top).

### Model

- A **grant** gives one privilege on one securable — a warehouse, a
  namespace, a table, or a view — to exactly one grantee: a **role** or a
  **principal**.
- **Hierarchy inheritance:** a grant on a warehouse covers every
  namespace, table, and view inside it; a grant on a namespace covers its
  child namespaces, tables, and views. A privilege may be granted at its
  native level or any level above it (e.g. `READ` on a whole warehouse).
- **Built-in roles** (seeded, undeletable): `admin` (every privilege on
  everything) and `catalog_reader` (`LIST_NAMESPACES`, `LIST_TABLES`,
  `READ` on everything — views included: `LIST_TABLES` also gates
  `listViews` and `READ` gates `loadView`).
- Privileges: `MANAGE_WAREHOUSE`, `CREATE_NAMESPACE`, `LIST_NAMESPACES`
  (warehouse-native); `MANAGE_NAMESPACE`, `CREATE_TABLE`, `LIST_TABLES`,
  `CREATE_VIEW` (namespace-native); `READ`, `WRITE`, `COMMIT`, `DROP`
  (leaf-native: grantable on a table or a view — the two sit at the same
  hierarchy rank and share the privilege vocabulary).

### Privilege → endpoint mapping

The authoritative table (kept in sync with the code) lives in the module
docs of
[`crates/meridian-server/src/routes/grants.rs`](../crates/meridian-server/src/routes/grants.rs).
Summary: `GET /v1/config` is exempt; namespace list/read →
`LIST_NAMESPACES`; namespace create → `CREATE_NAMESPACE`; namespace
drop/properties → `MANAGE_NAMESPACE`; table list → `LIST_TABLES`; table
create/register → `CREATE_TABLE`; table load/exists → `READ`; table commit
→ `COMMIT` (the assert-create finalization → `CREATE_TABLE`); table drop →
`DROP`; metrics and rename-source → `WRITE` (rename also needs
`CREATE_TABLE` at the destination); multi-table transactions → `COMMIT` on
every table; view list → `LIST_TABLES`; view create → `CREATE_VIEW`; view
load/exists → `READ` (view); view replace → `COMMIT` (view); view drop →
`DROP` (view); view rename → `WRITE` on the source view plus `CREATE_VIEW`
on the destination namespace. Denials are `403 ForbiddenException`
(divergence (f)).

### Management API and CLI

`/api/v2/roles` (list/create, `DELETE /api/v2/roles/{name}`),
`/api/v2/roles/{name}/bindings` (bind/unbind principals),
`/api/v2/grants` (list/create, `DELETE /api/v2/grants/{id}`), and
`GET /api/v2/permissions?principal=<id>` (effective permissions). All of
them — plus warehouse create/list/delete — require **management access**:
a binding to the built-in `admin` role or any `MANAGE_WAREHOUSE` grant.
CLI: `meridian role list|create`, `meridian grant add|list|rm` (with
`--token` for oidc-mode servers).

The first administrator is bootstrapped from configuration
(`auth.bootstrap_admin = { issuer, subject }`, see
[Authentication](#authentication)): `meridian serve` idempotently
provisions that identity and binds it to `admin` on startup.

Every grant/role/binding mutation writes an audit row and an outbox event
in the same transaction as the change (`grant.create`/`grant.delete`,
`role.create`/`role.delete`, `role.bind`/`role.unbind`), recorded under
the authenticated principal that performed it.

### Known gaps (tracked, honest)

- Dropping a securable leaves its grants behind as inert rows (ids are
  never reused); a cleanup sweep comes with the maintenance worker.
- Decisions are uncached — one Postgres round-trip per check. Correctness
  first; a cache is a benchmark-phase TODO recorded in
  `meridian_store::rbac`.

## Outside the IRC spec

Warehouse management is a Meridian API, not part of the IRC surface:
`GET`/`POST /api/v2/warehouses`, `DELETE /api/v2/warehouses/{name}`, plus
`GET /api/v2/principals` for principal visibility and the RBAC management
API (`/api/v2/roles`, `/api/v2/grants`, `/api/v2/permissions` — see
[Authorization (RBAC)](#authorization-rbac)). These sit behind the same
authentication middleware as the IRC surface (see the warning at the top
about the disabled-by-default posture), and all of them — principal
listing included — require management access in `oidc` mode.
