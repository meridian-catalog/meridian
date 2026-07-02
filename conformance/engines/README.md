# Engine conformance matrix

Results of running real Iceberg clients and query engines against a local
Meridian server. Two sources feed this table:

- the [e2e suite](../e2e/) (pyiceberg + DuckDB, run on every change), and
- per-engine smoke tests in this directory (currently [`flink/`](flink/)),
  each with its own README documenting exactly what was run and what
  failed.

Every cell is backed by a script or test in this repository — nothing here
is aspirational. A ❌ is a real, reproducible failure and links to its
analysis.

## Matrix

Legend: ✅ pass · ❌ fail (documented) · — not exercised yet.

| Engine | Version | Create (DDL) | Write | Schema evolution | Read | Row-level ops | Views |
|---|---|---|---|---|---|---|---|
| pyiceberg | 0.11.1 | ✅ namespaces + tables | ✅ appends, incl. two concurrent writers | ✅ add column, old rows read back as `NULL` | ✅ scans, snapshots, time travel | — | ✅ * |
| DuckDB (iceberg extension) | 1.5.4 | — | — | — | ✅ REST `ATTACH` + scan of a Meridian-committed table | — | — |
| Flink | 1.20 (iceberg-flink-runtime 1.11.0) | ❌ table DDL — see note † (namespace DDL ✅) | ✅ batch `INSERT` + streaming insert committing on checkpoints | — | ✅ batch scans / `COUNT(*)` | — | — |
| Spark | not yet run | — | — | — | — | — | — |
| Trino | not yet run | — | — | — | — | — | — |

\* View lifecycle (create with multiple SQL dialects, load, replace,
rename, drop, collision 409s) is verified end-to-end, but pyiceberg's
RestCatalog implements only `list_views` / `view_exists` / `drop_view` as
of 0.11.x, so the remaining operations are exercised through raw REST
calls in the same test module. See
[`../e2e/tests/test_views.py`](../e2e/tests/test_views.py).

† Flink's `CREATE TABLE` is rejected by Meridian with
`invalid schema: field id 0 is not positive`: the Flink connector sends
provisional 0-based field ids and expects the server to assign fresh ids
(as the Java reference implementation does); Meridian currently validates
the incoming ids as-is. This is a Meridian bug, tracked in
[docs/api-status.md](../../docs/api-status.md) (`createTable`), with a
workaround (pre-create the table via REST) scripted in
[`flink/setup.sh`](flink/setup.sh). Full analysis:
[`flink/README.md`](flink/README.md#known-issues).

Row-level operations (position/equality deletes, `UPDATE`/`DELETE`/
`MERGE`) have not been exercised by any engine yet — pyiceberg's suite is
append-based and the Flink smoke only inserts. They will be covered by the
Spark run.

## Environment notes

All runs used a Meridian server on `localhost:8181` (auth disabled) with a
MinIO-backed `s3://` warehouse where noted; see each directory's README
for exact setup. Engines running in containers need care with the vended
`s3.endpoint` — see
[`flink/README.md`](flink/README.md#s3-endpoint-vending-vs-containerized-engines)
for the endpoint-vending limitation and why the Flink containers use host
networking.
