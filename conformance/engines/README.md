# Engine conformance matrix

Results of running real Iceberg clients and query engines against a local
Meridian server. Two sources feed this table:

- the [e2e suite](../e2e/) (pyiceberg + DuckDB, run on every change), and
- per-engine smoke tests in this directory (currently [`flink/`](flink/),
  [`spark/`](spark/) and [`trino/`](trino/)), each with its own README
  documenting exactly what was run and what failed.

Every cell is backed by a script or test in this repository — nothing here
is aspirational. A ❌ is a real, reproducible failure and links to its
analysis.

## Matrix

Legend: ✅ pass · ❌ fail (documented) · — not exercised yet.

| Engine | Version | Create (DDL) | Write | Schema evolution | Read | Row-level ops | Views |
|---|---|---|---|---|---|---|---|
| pyiceberg | 0.11.1 | ✅ namespaces + tables | ✅ appends, incl. two concurrent writers | ✅ add column, old rows read back as `NULL` | ✅ scans, snapshots, time travel | — | ✅ * |
| DuckDB (iceberg extension) | 1.5.4 | — | — | — | ✅ REST `ATTACH` + scan of a Meridian-committed table | — | — |
| Flink | 1.20 (iceberg-flink-runtime 1.11.0) | ✅ namespace + table DDL — table DDL required a Meridian fix, see note † | ✅ batch `INSERT` + streaming insert committing on checkpoints | — | ✅ batch scans / `COUNT(*)` | — | — |
| Spark | 3.5.6 (iceberg-spark-runtime 1.11.0) | ✅ namespace + partitioned table DDL | ✅ batched `INSERT`s | ✅ add column, old rows read back as `NULL` | ✅ scans, aggregates, `VERSION AS OF` time travel | ✅ merge-on-read `MERGE INTO` + `DELETE FROM`; position-delete files and snapshot operations verified over REST | ✅ `CREATE VIEW` + `SELECT` and `CREATE OR REPLACE VIEW` of an existing view — both required a Meridian fix, see note ‡ |
| Trino | 482 | ✅ schema + partitioned table DDL — the schema needs an explicit `location`, see note § | ✅ `INSERT` via `stage-create` → `assert-create` commit | ✅ add column, old rows read back as `NULL` | ✅ scans, aggregates; **cross-engine**: reads Spark's post-`MERGE`/`DELETE` table (960 rows, all aggregates and per-category counts exact — position-delete files written by Spark applied correctly) | — (writes not exercised; reads of Spark's merge-on-read deletes verified) | ✅ `CREATE VIEW` + read back (`trino` dialect stored in Meridian); reading Spark's `spark`-dialect view is cleanly rejected by Trino, see note § |

\* View lifecycle (create with multiple SQL dialects, load, replace,
rename, drop, collision 409s) is verified end-to-end, but pyiceberg's
RestCatalog implements only `list_views` / `view_exists` / `drop_view` as
of 0.11.x, so the remaining operations are exercised through raw REST
calls in the same test module. See
[`../e2e/tests/test_views.py`](../e2e/tests/test_views.py).

† Flink's `CREATE TABLE` used to be rejected by Meridian with
`invalid schema: field id 0 is not positive`: the Flink connector sends
provisional 0-based field ids and expects the server to assign fresh ids
(as the Java reference implementation does), while Meridian validated the
incoming ids as-is. Meridian now assigns fresh field ids server-side on
`createTable` (see [docs/api-status.md](../../docs/api-status.md)), the
pre-create workaround in [`flink/setup.sh`](flink/setup.sh) is gone, and
the smoke's table DDL passes end to end. Full history:
[`flink/README.md`](flink/README.md#resolved-issues).

‡ Spark's `CREATE VIEW` sends 0-based provisional field ids, exactly like
Flink's `CREATE TABLE` (†), and was rejected with the same
`invalid schema: field id 0 is not positive` until Meridian applied the
same fresh-id treatment to `createView`. `CREATE OR REPLACE VIEW` of an
*existing* view goes through `replaceView`, whose `add-schema` update
sends the same 0-based ids and hit the same error, until Meridian extended
the fresh-id treatment to the replace path — both now pass. Details and
reproduction:
[`spark/README.md`](spark/README.md#create-or-replace-view--provisional-field-ids-fixed-in-meridian)
and [docs/api-status.md](../../docs/api-status.md#views).

§ Two Trino notes, both documented with reproductions in
[`trino/README.md`](trino/README.md): (1) *DDL* — when a namespace has no
`location` property, Trino's REST catalog falls back to a direct,
non-staged table create (an S3-Tables workaround in Trino 482); Meridian
writes the initial metadata file on such a create, Trino's subsequent
"location must be empty" check trips over that very file, and the failed
query leaves the table registered. The smoke's `CREATE SCHEMA … WITH
(location = …)` keeps Trino on the staged-create path, which works end to
end. Vending a default namespace location from Meridian (so vanilla
`CREATE SCHEMA` works) is worth tracking as a conformance item.
(2) *Views* — Trino executes only `trino`-dialect view representations
and rejects Spark's view with `Cannot read unsupported dialect 'spark'`;
cross-engine view portability needs multi-dialect representations.

Row-level operations are covered by the Spark smoke (merge-on-read
`MERGE INTO` and `DELETE FROM`, with the resulting position-delete files
and snapshot summaries asserted over REST). Copy-on-write row-level ops
and equality deletes have not been exercised yet — pyiceberg's suite is
append-based and the Flink smoke only inserts.

## Environment notes

All runs used a Meridian server on `localhost:8181` (auth disabled) with a
MinIO-backed `s3://` warehouse where noted; see each directory's README
for exact setup. Engines running in containers need care with the vended
`s3.endpoint` — see
[`flink/README.md`](flink/README.md#s3-endpoint-vending-vs-containerized-engines)
for the endpoint-vending limitation and why the Flink and Spark
containers use host networking.
