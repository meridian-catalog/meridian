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
smoke tests (Flink, Spark, Trino) — see the
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
| `createTable` | `POST .../namespaces/{ns}/tables` | Implemented | `stage-create` supported (metadata returned, nothing persisted until the create transaction commits with `assert-create`). `format-version` property selects format 1–3 (default 2). Request field ids are treated as provisional, like the Java reference (`AssignFreshIds`): the server assigns fresh 1-based ids (nested types included) and remaps `identifier-field-ids` and partition-spec/sort-order source ids; a requested partition spec becomes the table's only spec, `spec-id: 0` (divergence (d), resolved). Flink's 0-based connector ids, which used to be rejected, are covered by the [Flink smoke](../conformance/engines/flink/README.md). `X-Iceberg-Access-Delegation: vended-credentials` vends read-write credentials on warehouses that opted in (see [Credential vending](#credential-vending)); `remote-signing` (alone) advertises the sign endpoint instead (see [Remote signing](#remote-signing)); otherwise `config` carries the warehouse's non-secret storage options only (see [Storage config passthrough](#storage-config-passthrough)). `stage-create` responses never vend or advertise signing (no table exists yet). Name collisions with views: see divergence (g). |
| `loadTable` | `GET .../tables/{table}` | Implemented | `snapshots=all\|refs`; strong `ETag` and `If-None-Match` → 304 (see [ETags](#etags)). `X-Iceberg-Access-Delegation: vended-credentials` vends per-table credentials on opted-in warehouses — read-write for `WRITE`/`COMMIT` holders, read-only for `READ`-only holders (see [Credential vending](#credential-vending)); `remote-signing` (alone) advertises the per-table sign endpoint instead (see [Remote signing](#remote-signing); when both are listed, vended credentials win). Otherwise `config` carries non-secret storage options only (see [Storage config passthrough](#storage-config-passthrough)). |
| `updateTable` (commit) | `POST .../tables/{table}` | Implemented | The single-table commit path: requirements checked against the current metadata, unknown update/requirement types → 400 (as the spec requires), bounded compare-and-swap retry (409 `CommitFailedException` after 3 lost races), `assert-create` finalizes a stage-create transaction. `Idempotency-Key` honored (see [Idempotency keys](#idempotency-keys)). Exercised end-to-end by pyiceberg (appends, schema evolution, two concurrent writers), by Flink's checkpoint-driven streaming commits, and by Spark's merge-on-read row-level operations — `MERGE INTO` and `DELETE FROM` commit position-delete files through this path, and Trino reads the resulting table back exactly (cross-engine, verified) — see the [engine matrix](../conformance/engines/README.md). |
| `dropTable` | `DELETE .../tables/{table}` | Implemented | `purgeRequested=true` semantics: see divergence (e). |
| `tableExists` | `HEAD .../tables/{table}` | Implemented | 204 / 404. |
| `registerTable` | `POST .../namespaces/{ns}/register` | Partial | Adopts an existing metadata file as-is (it must parse and live under the warehouse root). **Missing:** `overwrite: true` is rejected with 400. Adopting a UUID that belongs to a live table is rejected: see divergence (c). |
| `renameTable` | `POST /v1/{prefix}/tables/rename` | Implemented | Rename or move across namespaces within one warehouse (prefix); 204. |
| `reportMetrics` | `POST .../tables/{table}/metrics` | Implemented | The report is validated as a JSON object and stored verbatim (it feeds the planned observability layer); 204. |
| `commitTransaction` | `POST /v1/{prefix}/transactions/commit` | Partial | Atomic multi-table commit: all requirements evaluated before anything is staged, **every** violation reported (not just the first), all pointers move in one database transaction or none do. `Idempotency-Key` honored. **Missing:** `assert-create` (staged creates) inside a transaction is rejected with 400. |
| `unregisterTable` | `POST .../tables/{table}/unregister` | Not yet | |
| `loadCredentials` | `GET .../tables/{table}/credentials` | Implemented | Per-table scoped credentials on warehouses that opted in via the `vending = "sts"` or `"static"` storage option; 400 on warehouses that did not (see [Credential vending](#credential-vending)). RBAC decides read vs read-write; every vend is audited. |
| `signRequest` | `POST .../tables/{table}/sign` | Implemented | S3 only (`provider` other than `s3` → 400). Requires the warehouse vending opt-in plus static keys in its storage options. The request must resolve inside the table's location prefix and within the caller's RBAC access (`GET`/`HEAD` with `READ`; `PUT`/`POST`/`DELETE` with `WRITE`/`COMMIT`); every decision is audited, denies included. See [Remote signing](#remote-signing). |

### Scan planning

Server-side scan planning per the 1.11+ REST surface (design:
[docs/design/scan-planning.md](design/scan-planning.md)). Every endpoint
requires `READ` on the table (the loadTable rule), re-checked on each
call. Tables whose snapshot tracks at most `planning.sync_max_data_files`
live data files (default 2000) are planned synchronously — `completed`
with all file scan tasks inline; larger tables answer `submitted` and are
planned on a bounded worker pool with results fetched page by page via
opaque `plan-task` tokens. Plans expire after `planning.plan_ttl_secs`
(default one hour); expired plan-ids are 404 `NoSuchPlanIdException`.
Plan submission and cancellation are audited (`scan.plan`,
`scan.plan_cancel`), submission also emits a `scan.planned` catalog
event, and the expiry sweep audits each batch (`scan.plans_expired`).
Disable the whole surface with `planning.enabled = false` (endpoints then
answer 406 and are not advertised in `GET /v1/config`).

| Operation | Endpoint | Status | Notes |
|---|---|---|---|
| `planTableScan` | `POST .../tables/{table}/plan` | Partial | Point-in-time scans: `snapshot-id` (default: current), `filter` (full expression pushdown: manifest summaries → partition tuples → column stats), `case-sensitive`, `use-snapshot-schema`, `stats-fields` (trims returned column stats), `select` (validated; does not change the payload yet — the column-mask hook). Tasks carry `delete-file-references` (position/equality deletes attached by the spec's sequence-number and partition scope rules, deletion vectors supersede position delete files) and a per-file `residual-filter` (exact partition folding; the row-policy injection point). Verified against the conformance suite's real Spark merge-on-read table. **Missing:** incremental scans (`start-snapshot-id`/`end-snapshot-id`) → 406; `min-rows-requested` accepted and ignored; no `storage-credentials` in planning responses (use loadTable delegation); `Idempotency-Key` accepted but not deduplicated. |
| `fetchPlanningResult` | `GET .../tables/{table}/plan/{plan-id}` | Implemented | `submitted`/`cancelled`/`failed`/`completed` per the spec's discriminated result. Completed synchronous plans re-plan from the stored request pinned to the plan's snapshot (deterministic on immutable metadata); completed asynchronous plans return `plan-tasks` page tokens. |
| `cancelPlanning` | `DELETE .../tables/{table}/plan/{plan-id}` | Implemented | 204; drops persisted result pages, flips `submitted`/`completed` plans to `cancelled` (a racing worker's result is discarded), idempotent on terminal states. |
| `fetchScanTasks` | `POST .../tables/{table}/tasks` | Implemented | One persisted page per `plan-task` token (single primary-key read); repeatable; unknown or expired tokens → 404 `NoSuchPlanTaskException`. Page-local `delete-file-references` indices; each page carries exactly the delete files its tasks reference. |

### Views

| Operation | Endpoint | Status | Notes |
|---|---|---|---|
| `listViews` | `GET .../namespaces/{ns}/views` | Implemented | RBAC: `LIST_TABLES` on the namespace. Pagination: see divergence (a). |
| `createView` | `POST .../namespaces/{ns}/views` | Implemented | RBAC: `CREATE_VIEW` on the namespace. Multiple SQL representations per version (at most one per dialect, case-insensitive). 409 when the name exists as a view **or a table**: see divergence (g). Default location is uuid-suffixed under the namespace path, like tables. Request field ids are treated as provisional, exactly as on `createTable`: fresh 1-based ids are assigned server-side. Spark 3.5's `CREATE VIEW` numbers the output schema from 0 and used to be rejected with `field id 0 is not positive`; covered by the [Spark smoke](../conformance/engines/spark/README.md). |
| `loadView` | `GET .../views/{view}` | Implemented | RBAC: `READ` on the view. `config` carries the warehouse's non-secret storage options (see [Storage config passthrough](#storage-config-passthrough)). No `ETag`: the spec defines the `ETag`/`If-None-Match` mechanism for table responses only (`LoadViewResponse` has no `etag` header). The `referenced-by` parameter is accepted and ignored (the caller's own `READ` on the view decides access; chain-based decisions are not implemented). |
| `replaceView` | `POST .../views/{view}` | Implemented | RBAC: `COMMIT` on the view, checked before anything is staged. The view commit path: `assert-view-uuid` checked against current metadata, unknown update/requirement types → 400, updates applied through the validating view-metadata builder (version log grows per current-version change, versions expire per `version.history.num-entries`), bounded compare-and-swap retry (409 `CommitFailedException` after 3 lost races). **Missing:** `Idempotency-Key` is not honored on view endpoints (see [Idempotency keys](#idempotency-keys)); dialect-drop protection (`replace.drop-dialect.allowed`) is not enforced yet (builder TODO). **Known gap:** `add-schema` updates on the replace path validate field ids strictly (must be positive), but view schemas have no cross-version field-id continuity to protect and the Java reference accepts whatever ids the client sends — so Spark 3.5's `CREATE OR REPLACE VIEW` on an *existing* view fails with `field id 0 is not positive` (0-based ids, same shape as the resolved create-path bug). Reproduced by the [Spark smoke](../conformance/engines/spark/README.md#known-gap-create-or-replace-view); initial `CREATE VIEW` is unaffected. |
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

### (d) Resolved: `createTable` used to number a requested partition spec as id 1

Historical — fixed alongside provisional field-id assignment; the letter is
kept so older references stay meaningful. A table created with a partition
spec used to carry **two** specs in its metadata: the empty (unpartitioned)
spec as `spec-id: 0` and the requested spec as `spec-id: 1`, with
`default-spec-id: 1`. Meridian now matches the Java reference
implementation: the requested spec is the table's only spec, numbered 0.
Spec **evolution** on an existing table is unchanged (added specs append
with the next id; existing specs are never renumbered).

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

When the warehouse sets `endpoint.external`, that value wins over
`endpoint` for `s3.endpoint` in **all** client-facing config (table and
view loads, vended credentials) while the server keeps using `endpoint`
internally — for containerized engines that reach object storage on a
different address than the server does (e.g. `host.docker.internal`).

Credential material — `access-key-id`, `secret-access-key`,
`session-token` — is **never** forwarded by passthrough (an explicit
denylist, verified by tests that sweep response bodies for planted
credential values). Credential delivery happens only through
[credential vending](#credential-vending) — an explicit client request
against a warehouse that explicitly opted in — never as a side effect of
config passthrough. Server-side options (`retry.*`, `anonymous`,
`vending*`) have no client property and are not forwarded;
filesystem-rooted warehouses have no client-facing options, so their
`config` is empty.

## Credential vending

Per-table, RBAC-scoped storage credentials, delivered through
`loadCredentials` and the `X-Iceberg-Access-Delegation: vended-credentials`
header on `createTable`/`loadTable` (design and decisions:
[docs/design/vending.md](design/vending.md)). Off by default; a warehouse
opts in through storage options:

```jsonc
// POST /api/v2/warehouses
{
  "name": "prod",
  "storage_root": "s3://bucket/prefix",
  "storage_options": {
    "endpoint": "http://minio:9000",          // internal (server-side)
    "endpoint.external": "http://host.docker.internal:9000", // advertised
    "access-key-id": "…", "secret-access-key": "…",
    "vending": "sts",                          // none | static | sts
    "vending.role-arn": "arn:…:role/…",        // sts only
    "vending.duration-secs": "3600"            // sts only, 900–43200
  }
}
```

- **`sts`** — one STS `AssumeRole` per vend with an inline session policy
  scoped to the table's location prefix (`GetObject` + `ListBucket` under
  a prefix condition; `PutObject`/`DeleteObject` added for read-write).
  Verified against MinIO (STS on the S3 endpoint; prefix isolation and
  TTL covered by integration + e2e tests, including a pyiceberg client
  configured with only the catalog URI). Standard AWS STS semantics, but
  **not yet cloud-verified against real AWS**.
- **`static`** — the warehouse's own keys passed through, unscoped and
  without expiry, for self-hosted setups with no STS: an explicit,
  documented trade-off, which is why it is a separate opt-in value.
- **GCS / Azure** — not implemented; vends fail with a clear
  "not implemented yet" error (no fake credentials, ever).
- **Access follows RBAC**: `WRITE`/`COMMIT` on the table → read-write
  credentials; `READ` only → read-only; neither → 403. In
  `auth.mode = "disabled"` the anonymous principal vends read-write.
- **Auditing**: every vend writes an `audit_log` row (`credential.vend`)
  and an outbox event (`credential.vended`) — principal, table, prefix,
  access, mode, TTL — in one transaction *before* credentials are
  returned.
- Misconfigured vending options are rejected at warehouse create time.
  The vending header is ignored on warehouses with `vending = "none"`
  (pyiceberg sends it by default).

## Remote signing

The spec's second delegation mechanism (`X-Iceberg-Access-Delegation:
remote-signing`), implemented per ADR
[005](adr/005-remote-signing.md): instead of shipping credentials, the
catalog signs each client-built S3 request at
`POST .../tables/{table}/sign` (the spec's
`RemoteSignRequest`/`RemoteSignResult`) with the warehouse's keys, which
never leave the server.

- **Opt-in and keys**: rides the same `vending = "sts" | "static"` opt-in;
  additionally requires `access-key-id`/`secret-access-key` in the
  warehouse storage options (warehouses on ambient AWS credentials get a
  400 from the sign endpoint — no credentials-provider path yet).
- **Advertisement**: a table load/create carrying `remote-signing` (and
  not `vended-credentials`, which wins when both are listed) gets
  `s3.remote-signing-enabled=true`, a **relative** `s3.signer.endpoint`
  (per-table sign path; `s3.signer.uri` is left to its spec default, the
  catalog base URI), and `s3.signer=S3V4RestSigner` (pyiceberg's fsspec
  activation property; inert elsewhere) in `LoadTableResult.config`.
- **Authorization is the boundary** (signatures use warehouse-wide keys):
  the request URI must resolve inside the table's location prefix —
  path-style or virtual-host, percent-decoded, `.`/`..` segments denied,
  host restricted to the warehouse's endpoints; `GET`/`HEAD` need `READ`,
  `PUT`/`POST`/`DELETE` need `WRITE`/`COMMIT`; governance subresources
  (`?acl`, `?policy`, `?tagging`, ...) are never signed;
  `x-amz-copy-source` must also stay inside the table; bucket-root
  requests are limited to listings with an in-prefix `prefix` parameter
  and `DeleteObjects` with every body key validated.
- **Auditing**: every decision — allow *and* deny — writes an `audit_log`
  row (`credential.sign`: principal, table, method, decoded keys,
  decision, deny reason) and an outbox event (`credential.signed` /
  `credential.sign-denied`) in one transaction before the response leaves.
- **Caching**: signed `GET`/`HEAD` responses carry `Cache-Control:
  private` (spec-following clients may reuse them within the SigV4
  validity window); writes carry `no-cache`.
- **Cost, honestly**: every uncached object request from a remote-signing
  client is one catalog round trip plus one audit transaction. Table
  locations are cached in-process (keyed by the commit pointer version),
  so steady-state signing does not re-read `metadata.json`.
- Verified end to end against MinIO (sign → execute → 200; sibling-table
  and read-only-PUT attempts → 403 + audit row) and with a real pyiceberg
  0.11 client holding zero S3 configuration
  ([e2e](../conformance/e2e/tests/test_remote_signing.py) — requires the
  fsspec FileIO; pyiceberg's pyarrow FileIO has no remote-signing
  support). **Not yet cloud-verified against real AWS.**

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
`GET /api/v2/principals` for principal visibility, the RBAC management
API (`/api/v2/roles`, `/api/v2/grants`, `/api/v2/permissions` — see
[Authorization (RBAC)](#authorization-rbac)), and the federation API
(`/api/v2/mirrors`, `/api/v2/federation/sprawl` — see
[Federation](#federation-mirrors--sprawl-apiv2mirrors-apiv2federationsprawl)). These sit behind the same
authentication middleware as the IRC surface (see the warning at the top
about the disabled-by-default posture), and all of them — principal
listing included — require management access in `oidc` mode.

### Events (`/api/v2/events`, `/api/v2/webhooks`)

Every catalog mutation emits an event (transactional outbox, published by
a background relay inside `meridian serve`) rendered as CloudEvents 1.0
JSON. Full design, event-type catalog, ordering/at-least-once guarantees,
and the webhook signature-verification recipe:
[docs/design/events.md](design/events.md).

- **Queryable feed**: `GET /api/v2/events?after=<cursor>&types=<t,..>&limit=`
  — keyset-paginated over published events, cursor = event id,
  `after=latest` starts at the current end. Gap-free and totally ordered
  (publication-frontier bounded).
- **Durable consumers**: `POST /api/v2/events/consumers {name}`,
  `GET .../consumers/{name}/next`, `POST .../consumers/{name}/commit
  {cursor}`, `DELETE .../consumers/{name}` — persistent offsets,
  at-least-once (`next` re-serves until committed; backward commits → 409).
- **Webhooks**: `POST`/`GET /api/v2/webhooks`, `GET`/`DELETE
  /api/v2/webhooks/{id}`, `GET /api/v2/webhooks/{id}/deliveries?status=` —
  HMAC-SHA256-signed CloudEvents deliveries with per-endpoint exponential
  retry and dead-letter visibility. Secrets are write-only.
- **Authorization** (`oidc` mode): all events endpoints require management
  access (admin or any `MANAGE_WAREHOUSE` grant). The feed spans every
  resource in the workspace, so the existing resource-scoped privileges
  cannot express "may read events"; a dedicated `READ_EVENTS` privilege is
  deliberately deferred (documented in the design doc).
- **CLI**: `meridian events tail [--from-start | --after <cursor>]
  [--types ...]` follows the feed as JSON lines.

### Search (`GET /api/v2/search`)

Ranked full-text search over tables, views, and namespaces (Postgres FTS;
no external search engine). CLI: `meridian search <query>`.

- **Query**: `q` (required), `type` (comma-separated `table,view,namespace`),
  `warehouse` (name; unknown → 404), `namespace` (dot-separated path
  prefix), `limit` (1–100, default 20), `page_token` (keyset cursor from
  the previous response).
- **Matches**: asset name, namespace path, table **column names and docs**
  (extracted from the current schema and re-indexed on every create,
  register, and commit, in the same transaction as the pointer write), and
  `properties.comment`. Identifiers split on underscores, so `email` finds
  a `customer_email` column and `customer_email` matches it exactly; every
  query token is also a prefix match.
- **Ranking**: weighted `ts_rank` (name > path > columns > comment) plus
  exact-name and name-prefix boosts. Results carry the asset type,
  identifiers, rank, and a `ts_headline` snippet.
- **Authorization** (`oidc` mode): no endpoint-level gate — results are
  filtered to what the caller can see, inside the search query itself
  (constant number of authorization queries per request, no per-result
  round-trips). Tables/views require `READ` (direct, inherited from a
  namespace, or from the warehouse); namespaces require `LIST_NAMESPACES`
  on their warehouse; `admin` and `catalog_reader` see everything. An
  ungranted caller gets an empty result list, not a 403.
- **Known gaps (tracked, honest)**: view schemas are not column-indexed yet
  (views match by name/path/comment only); the namespace-inheritance
  visibility check probes the caller's granted-namespace set per matched
  row inside the query — fine at small grant counts, a benchmark-phase
  TODO recorded in `meridian_store::search`; no usage-based ranking, no
  semantic search (both are later slices of the search feature).

### Federation: mirrors + sprawl (`/api/v2/mirrors`, `/api/v2/federation/sprawl`)

Catalog federation (Pillar B): register *mirrors* — external catalogs
(another Iceberg REST endpoint, or an AWS Glue Data Catalog) that Meridian
tracks without owning their storage — and roll up a cross-catalog *sprawl*
summary across everything Meridian knows (its own warehouses plus mirrors).
CLI: `meridian mirror create|list|sync`, `meridian sprawl`.

- **Mirror CRUD**: `GET`/`POST /api/v2/mirrors`, `GET`/`PATCH`/`DELETE
  /api/v2/mirrors/{name}`. A mirror carries a `kind` (`iceberg-rest` |
  `glue`), an `endpoint`, an optional `remote_catalog`, non-secret `config`
  (secret-looking keys are redacted on read), an `enabled` flag, and a
  `sync_interval_s` cadence. Mutations are audited and emit outbox events on
  the same transaction, exactly like warehouse CRUD.
- **Sync status + sync-now**: `GET /api/v2/mirrors/{name}/sync` returns the
  mirror plus its recent sync-run history; `POST /api/v2/mirrors/{name}/sync`
  runs the sync engine now (404 if unknown, 409 if disabled) and returns a
  summary of what changed (inserted/updated/unchanged/removed). The scheduled
  worker also pulls enabled mirrors on their own cadence. The sync engine
  (`meridian-federation`, ADR 008) connects to the source over a read-only IRC
  client (`GET /v1/config`, list namespaces/tables, `loadTable`; none /
  static-bearer / OAuth2-client-credentials auth) and materializes each
  mirrored table as an ordinary — but read-only — row in the native `tables`
  table under a dedicated `mirror__<name>` warehouse, so search and health work
  on foreign assets with no read-path changes. Sync is incremental (unchanged
  `metadata_location` is skipped) and reflects source deletions.
- **Foreign assets are read-only** (conflict-free federation): a foreign table
  (`mirror_id` set) is the source catalog's to write. A `commit`, `create`,
  `register`, `drop`, or `rename` targeting a foreign table — or any write
  under a `mirror__<name>` warehouse — is rejected with a `409
  CommitFailedException` that names the source as the write authority.
- **Sprawl summary**: `GET /api/v2/federation/sprawl[?stale_threshold_s=]`
  computes, across all sources: per-source asset counts (native warehouses
  vs. mirrors; a mirror's private foreign warehouse is not double-counted),
  duplicate/overlap detection (the same storage location registered in more
  than one source — the zero-copy-register signal), staleness (mirrors not
  synced within the threshold, default 24h), ownership gaps (mirror assets
  with no known owner), and a health roll-up over the indexed native assets
  reusing the maintenance health model.
- **Authorization** (`oidc` mode): every federation endpoint requires
  management access (admin or any `MANAGE_WAREHOUSE` grant) — federation
  spans the whole workspace, the same bar as warehouse CRUD and the fleet
  health summary.
- **Console**: the **Federation** page lists mirrors with their sync status,
  a create-mirror form, and the sprawl dashboard (per-source counts,
  duplicates, stale mirrors, ownership, native health).
