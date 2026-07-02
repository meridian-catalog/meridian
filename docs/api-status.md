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

> [!WARNING]
> **NO AUTHENTICATION, NO AUTHORIZATION — YET. EVERY ENDPOINT, INCLUDING THE
> WAREHOUSE MANAGEMENT API, IS OPEN. ANYONE WHO CAN REACH THE PORT OWNS THE
> CATALOG: THEY CAN READ, COMMIT TO, DROP, AND PURGE EVERY TABLE. DO NOT
> EXPOSE MERIDIAN TO ANY NETWORK YOU DO NOT FULLY TRUST.**
>
> OIDC-based authentication is the next milestone of work (M1b). Until it
> lands, all operations are recorded in the audit log under the principal
> `anonymous`.

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
| `getToken` | `POST /v1/oauth/tokens` | Not yet | And not planned: the endpoint is deprecated for removal in the spec itself. Meridian's authentication will be OIDC (see warning above), not a catalog-hosted token endpoint. |

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
| `createTable` | `POST .../namespaces/{ns}/tables` | Implemented | `stage-create` supported (metadata returned, nothing persisted until the create transaction commits with `assert-create`). `format-version` property selects format 1–3 (default 2). No credential vending: the `X-Iceberg-Access-Delegation` header is ignored and `config` in the response is always empty. Partition-spec numbering: see divergence (d). |
| `loadTable` | `GET .../tables/{table}` | Implemented | `snapshots=all\|refs`; strong `ETag` and `If-None-Match` → 304 (see [ETags](#etags)). No credential vending (`config` always empty). |
| `updateTable` (commit) | `POST .../tables/{table}` | Implemented | The single-table commit path: requirements checked against the current metadata, unknown update/requirement types → 400 (as the spec requires), bounded compare-and-swap retry (409 `CommitFailedException` after 3 lost races), `assert-create` finalizes a stage-create transaction. `Idempotency-Key` honored (see [Idempotency keys](#idempotency-keys)). |
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

| Operation | Endpoint | Status |
|---|---|---|
| `listViews` | `GET .../namespaces/{ns}/views` | Not yet |
| `createView` | `POST .../namespaces/{ns}/views` | Not yet |
| `loadView` | `GET .../views/{view}` | Not yet |
| `replaceView` | `POST .../views/{view}` | Not yet |
| `dropView` | `DELETE .../views/{view}` | Not yet |
| `viewExists` | `HEAD .../views/{view}` | Not yet |
| `renameView` | `POST /v1/{prefix}/views/rename` | Not yet |
| `registerView` | `POST .../namespaces/{ns}/register-view` | Not yet |

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

### (f) No authentication

See the warning at the top of this page. This is the single most important
divergence: the spec assumes OAuth2/OIDC bearer tokens; Meridian currently
accepts every request. OIDC authentication is the next milestone (M1b).

## Idempotency keys

The current spec draft attaches an optional `Idempotency-Key` header to most
mutating operations. Meridian implements it on the **two commit endpoints
only** (`updateTable` and `commitTransaction`) — the operations where a
retried, half-applied request is genuinely dangerous. Semantics
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

## Outside the IRC spec

Warehouse management is a Meridian API, not part of the IRC surface:
`GET`/`POST /api/v2/warehouses`, `DELETE /api/v2/warehouses/{name}`. Like
everything else it is currently unauthenticated (see warning).
