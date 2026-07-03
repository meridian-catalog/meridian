# Server-side scan planning

Status: implemented for point-in-time scans (snapshot pinning, filter and
stats pushdown, position/equality delete attachment, residual filters,
sync + async execution, tiered manifest caching). Incremental scans
(`start-snapshot-id`/`end-snapshot-id`) are not implemented and are
refused with 406. This document records the design and its decisions;
[`docs/api-status.md`](../api-status.md) is the authoritative statement of
endpoint behavior.

## What it is

Server-side planning moves manifest I/O from every engine to the catalog.
A thin client (DuckDB, PyIceberg, Daft) sends a filter; the catalog —
which already caches the immutable manifests and already knows the
caller's permissions — answers with the exact files to read, the delete
files to apply to each, and a per-file **residual filter**. Four
endpoints, per the Iceberg REST spec (1.11+ planning surface):

- `POST .../tables/{table}/plan` (planTableScan) — submit a scan.
- `GET .../tables/{table}/plan/{plan-id}` (fetchPlanningResult) — poll or
  re-fetch a result.
- `DELETE .../tables/{table}/plan/{plan-id}` (cancelPlanning) — release
  server-held results.
- `POST .../tables/{table}/tasks` (fetchScanTasks) — fetch one result
  page by its opaque `plan-task` token.

Everything is authorized like `loadTable`: `READ` on the table,
re-checked on every call. Plan-ids and plan-task tokens are unguessable
handles, not capabilities — possession never substitutes for a grant.
This surface is also the enforcement seam for row/column policies
(pillar D): residuals pass through one marked injection point
(`apply_row_policy_seam` in `meridian-server/src/planning/engine.rs`),
and `select` is validated where column masks will hook.

## Sync or async

The manifest list (one small, cached read) tells us how many live data
files the snapshot tracks:

- **≤ `planning.sync_max_data_files`** (default 2000): plan inline and
  answer `completed` with every `FileScanTask` in the body. The spec
  requires a `plan-id` even here, so a plan row is still written (with
  audit + outbox event in the same transaction).
- **Larger**: answer `submitted` immediately; a worker on a bounded pool
  (`planning.max_concurrent_plans`, default 4; saturation answers 503
  rather than queueing without bound) plans and persists result pages of
  `planning.page_size_files` (default 500) tasks. `fetchPlanningResult`
  then lists opaque page tokens; each `fetchScanTasks` call is one
  primary-key read.
- Manifests whose v1 lists omit file counts make the total unknowable
  cheaply: those tables plan asynchronously.

Status transitions are compare-and-set (`submitted → completed | failed |
cancelled`; `completed → cancelled`), so a cancel racing a completing
worker has exactly one winner and the loser's work is discarded.

### Results: persisted for async, recomputed for sync

Asynchronous plans **persist** their pages once, in Postgres: fetches are
O(1), results survive pod crashes between submit and fetch, pagination is
trivially deterministic, and cancellation/TTL deletes them. Synchronous
plans persist **no** pages — the result already went out in the response
body, and re-persisting it would put a multi-megabyte Postgres write on
the hot path for a result that is usually never re-fetched. A later
`fetchPlanningResult` on an inline plan **re-plans from the stored
request pinned to the stored snapshot id**: deterministic (manifests are
immutable and the snapshot cannot drift), warm in the cache, and — once
policy residuals land — the semantically right choice, because the
*fetcher's* policies get injected. Plans expire after
`planning.plan_ttl_secs` (default one hour): expired ids answer 404 on
read, and a background sweep deletes the rows (crash-orphaned `submitted`
rows age out the same way).

## Task construction (correctness rules)

The pruning pipeline per manifest, using the spec that *wrote* the
manifest (never the current one blindly):

1. **Manifest list level** — skip manifests with zero live entries, and
   manifests whose partition field summaries cannot satisfy the filter
   (inclusive projection, spec "Scan Planning"). Delete manifests prune
   the same way: a delete constrained to pruned partitions cannot apply
   to any kept data file, because application requires partition equality
   under the same spec, and unpartitioned (global) specs project to
   `true` and are never pruned.
2. **Entry level** — drop `DELETED` entries; prune by partition tuple
   (exact, field-id addressed), then by column statistics
   (value/null/NaN counts, lower/upper bounds; three-valued, "unknown
   keeps the file").
3. **Equality-delete stats pruning** — a delete file's column stats
   describe the *deleted rows' payload*; only the stats of its
   **equality columns** say which rows it removes. The filter is first
   weakened to those columns (every other leaf becomes `true`) and only
   then evaluated. Position delete files are never stats-pruned (their
   row-payload stats are optional and deprecated); they prune by
   partition, sequence number, and `file_path` bounds only.

Delete attachment implements the spec's scope rules verbatim
(`crates/meridian-server/src/planning/engine.rs`, unit-tested rule by
rule and integration-tested against a hand-built merge-on-read fixture
and the conformance suite's real Spark table):

- **Deletion vector** (position delete with `content-offset`): data path
  equals `referenced_data_file`, data seq ≤ DV seq, partitions equal —
  and when one applies, plain position delete files for that data file
  are ignored (the DV subsumes them).
- **Position delete file**: `referenced_data_file` (when set) equals the
  data path; data seq ≤ delete seq (same-commit deletes apply);
  partitions equal (spec id + values — position deletes have **no**
  unpartitioned-global special case); `file_path` column bounds, when
  present, must admit the data path (a truncated upper bound is treated
  as prefix-inclusive).
- **Equality delete file**: data seq **strictly less** than delete seq;
  partitions equal, or the delete's spec is unpartitioned (global).

Each page carries exactly the delete files its tasks reference, with
page-local `delete-file-references` indices, and task order is manifest
list order then entry order — deterministic for a snapshot.

### Residual filters

`residual-filter` is the part of the request filter pruning has not
already guaranteed. A leaf folds to a constant only when the file's
partition tuple *exactly determines* its term — the term's transform
(identity for plain references) matches a partition field over the same
source column — evaluated exactly against the stored partition value. A
`bucket[16](id)` term folds against a `bucket[16]` partition; a plain
`id` predicate on the same file does not. Null partition values fold only
`is-null`/`not-null`: evaluator families disagree with SQL three-valued
logic on `not-eq`/`not-in` over null, so those leaves are kept for the
client. Keeping a predicate is always sound; dropping one never happens
unless the fold is exact.

## Manifest caching

Manifests are immutable at a path, so the cache needs bounds, not
invalidation. Three tiers, hit-counters logged with every plan summary:

1. **In-process LRU of parsed manifests** (`planning.cache_max_bytes`,
   default 256 MiB, weighted by estimated parsed size; exact LRU; values
   are `Arc`-shared). A warm plan does zero manifest I/O and zero
   parsing.
2. **Postgres byte cache** (`manifest_cache`, migration 0011;
   `planning.pg_cache_max_bytes`, default 1 GiB, `0` disables): raw file
   bytes shared across pods, so a cold pod skips the object-storage round
   trip but still parses. Raw bytes, not a parsed form — no
   struct-versioning in the database. `accessed_at` is bumped lazily (at
   most once per 5 minutes per row); the sweep evicts
   least-recently-accessed rows beyond the budget.
3. **Object storage** — source of truth; reads write through to both
   tiers. Byte-cache failures degrade to storage reads with a warning,
   never fail a plan.

Not yet served from the Postgres write-through metadata index: planning
reads real manifests today; index-served planning with manifest fallback
is future work and slots in behind the same `ManifestSource` seam.

## Observability and audit

Plan creation writes `scan.plan` to the audit log and a `scan.planned`
event to the outbox (same transaction as the plan row) — every scan
becomes a visible catalog event. Cancellation audits `scan.plan_cancel`;
the expiry sweep audits `scan.plans_expired` per non-empty batch. Every
completion logs and stores a summary: matched files, manifests/files
pruned at each stage, delete files seen/pruned, page count, duration, and
cache tier counters.

## Measured performance

Target: warm plan p95 < 150 ms on a 10,000-file table. Numbers
from the synthetic fixture (10,000 files, 100 identity partitions, 100
files/manifest, realistic stats; `testing/bench` `plan` scenario over
HTTP against a release build on localhost with a MinIO warehouse — see
the bench README for caveats; re-measure before quoting):

| scenario | mode | p50 | p95 | p99 |
|---|---|---:|---:|---:|
| selective filter (1% match, `stats-fields: ["id"]`) | sync | 5.1 ms | 6.9 ms | 7.9 ms |
| selective filter | async (poll 10 ms + cancel) | 26 ms | 30 ms | 31 ms |
| unfiltered (all 10,000 tasks, full stats, ~8 MB body) | sync | 181 ms | 188 ms | 192 ms |
| unfiltered | async | 327 ms | 356 ms | 423 ms |

The in-process perf smoke (`testing/bench/scripts/plan-perf.sh`, which is
what CI-adjacent machines should run) asserts the target on the
selective-filter path and prints both: measured there at p95 6.7 ms
(selective) and 120 ms (unfiltered inline). The unfiltered-inline case is
dominated by serializing 10,000 tasks; the default 2000-file sync
threshold routes such scans to the async path in production
configurations.

## Known gaps (also in api-status)

- Incremental scans: 406 `UnsupportedOperationException`.
- `min-rows-requested`: accepted and ignored (the spec allows returning
  more rows; early-stopping would make pagination non-deterministic).
- `select`: validated against the scan schema (unknown fields are 400)
  but does not yet change the returned tasks; it is the column-mask hook.
- No `storage-credentials` in planning responses; clients obtain access
  via `loadTable` delegation (vending/remote signing).
- Idempotency keys on planTableScan are accepted but not deduplicated
  (each submission creates a new plan).
- Type-promoted partition columns (e.g. `int`→`long`) compare as unknown
  during tuple pruning and residual folding — files are kept and the
  full filter stays in the residual (sound, mildly conservative).
