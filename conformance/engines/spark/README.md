# Spark smoke test

Runs Apache Spark 3.5.6 (image
`apache/spark:3.5.6-scala2.12-java17-python3-ubuntu`, multi-arch —
amd64 and arm64) with `iceberg-spark-runtime-3.5_2.12` 1.11.0 and
`iceberg-aws-bundle` 1.11.0 against a Meridian server on
`localhost:8181` with a MinIO-backed `s3://` warehouse.

This is the widest engine smoke so far: it exercises row-level
operations (`MERGE INTO`, `DELETE FROM`, merge-on-read) and Iceberg
views through the REST catalog, which no other engine run covered.

## What it runs

`suite/suite.py` (PySpark, submitted with `spark-submit` inside the
container) executes and *verifies* every step — a step without a passing
assertion does not count:

| # | Step | Verification |
|---|---|---|
| 1 | `CREATE NAMESPACE spark_ns` | listed by `SHOW NAMESPACES` |
| 2 | `CREATE TABLE spark_ns.orders` — 5 columns, `PARTITIONED BY (category)`, format-version 2, merge-on-read for delete/update/merge | column list; REST metadata has exactly one partition spec (identity on `category`, `spec-id: 0`) |
| 3 | Two `INSERT INTO … SELECT … FROM range(…)` batches of 500 rows | `count(*)` = 1000 |
| 4 | Aggregates | `sum(amount)` = 74250.0, `sum(quantity)` = 5500, 4 categories at 250 rows each |
| 5 | `ALTER TABLE … ADD COLUMN note STRING`, then a 10-row insert with `note` set | count = 1010; the 1000 pre-evolution rows read back `note IS NULL`; the 10 new rows read `'late'` |
| 6 | Time travel `VERSION AS OF <first snapshot>` | count = 500 and the `note` column is absent at that snapshot |
| 7 | `MERGE INTO` updating rows with `id < 100` | 100 rows read back `note = 'merged'`; total count unchanged |
| 8 | `DELETE FROM … WHERE id >= 950 AND id < 1000` | count = 960; the deleted id range reads back empty |
| 9 | REST metadata check (`GET …/tables/orders` straight from Meridian) | exactly 5 snapshots with operations `append, append, append, overwrite, delete`; the MERGE and DELETE snapshots each report `added-delete-files ≥ 1` (merge-on-read position deletes); final `total-delete-files ≥ 2` |
| 10 | `CREATE VIEW spark_ns.orders_by_category` + `SELECT` from it | view aggregates sum back to 960 rows across 4 categories; `GET …/views/orders_by_category` confirms the view lives in Meridian with a `spark` SQL representation |

The script's last line of output is a single JSON object
(`SUITE_RESULT: {status, table_identifier, warehouse, row_count,
snapshot_count, view_name_or_null, failures}`), and `run.sh` exits 0
only if every step verified. The final table — post-MERGE, post-DELETE,
with its delete files and 5-snapshot history — is deliberately left in
place for inspection.

Observed passing result:

```
SUITE_RESULT: {"status": "pass", "table_identifier": "spark_ns.orders",
"warehouse": "spark_smoke", "row_count": 960, "snapshot_count": 5,
"view_name_or_null": "spark_ns.orders_by_category", "failures": []}
```

## Prerequisites

- Meridian on `localhost:8181` (auth disabled), e.g.
  `cargo build -p meridian-cli && DATABASE_URL=… ./target/debug/meridian serve`
- MinIO on `localhost:9000` with the local-dev credentials
  (`meridian` / `meridian123`)
- Docker (the image supports amd64 and arm64; on Docker Desktop,
  host networking must be enabled — see below)

## Running

```sh
./run.sh
```

`run.sh` fetches the pinned jars (`fetch-jars.sh`), provisions the
`spark-smoke` bucket and `spark_smoke` warehouse and tears down
leftovers from a previous run (`setup.sh`), then submits
`suite/suite.py` in a `--rm` container. Nothing engine-side outlives the
run.

## S3 endpoint vending vs. containerized engines

Same situation as the [Flink smoke](../flink/README.md): Meridian vends
the warehouse's `s3.endpoint` (`http://localhost:9000`) in
`LoadTableResult.config`, and the Iceberg Java REST client merges the
vended config **over** catalog-level client properties, so a client-side
`s3.endpoint` override does not stick. The container therefore runs with
`--network host`, which makes `localhost:9000` inside the container
reach MinIO. The catalog URI is not vended and points at
`host.docker.internal:8181`.

Internal vs. external endpoint advertisement now exists: setting the
`endpoint.external` storage option on the warehouse (e.g.
`http://host.docker.internal:9000`) makes every client-facing config
advertise that address while the server keeps using `endpoint`
internally — see [Storage config passthrough](../../../docs/api-status.md#storage-config-passthrough).
This smoke still uses host networking (predates the option); switching it
over is a cleanup TODO.

## Resolved issues

### `CREATE VIEW` — provisional field ids (fixed in Meridian)

On the first run, steps 1–9 passed and step 10 failed:

```
org.apache.iceberg.exceptions.BadRequestException:
Malformed request: invalid schema: field id 0 is not positive
```

Spark's `CREATE VIEW` (`SparkCatalog.createView` →
`RESTSessionCatalog$RESTViewBuilder.create`) numbers the view's output
schema from 0 — the same provisional-id convention Flink's `CREATE
TABLE` used, which Meridian's `createTable` already handles by assigning
fresh ids server-side. `createView` was missing the same treatment and
validated the incoming ids as-is. Meridian now assigns fresh 1-based
field ids on `createView` too (see
[docs/api-status.md](../../../docs/api-status.md#views)), and the view
step passes end to end.

## Known gap: `CREATE OR REPLACE VIEW`

Replacing an *existing* view from Spark still fails with the same error:

```
org.apache.iceberg.exceptions.BadRequestException:
Malformed request: invalid schema: field id 0 is not positive
```

`replaceView` goes through the view commit path, whose `add-schema`
update still validates field ids strictly — but Spark sends 0-based ids
there as well, and view schemas have no cross-version field-id
continuity that strict validation would protect (the Java reference
accepts client-sent ids on replace). Initial `CREATE OR REPLACE VIEW` of
a view that does not exist yet works (it is a create). Tracked in
[docs/api-status.md](../../../docs/api-status.md#views); not part of the
suite's pass criteria, so the smoke stays green while the gap is open.

## Version pinning

| Component | Version | Why |
|---|---|---|
| Spark | 3.5.6 (`apache/spark:3.5.6-scala2.12-java17-python3-ubuntu`) | latest 3.5.x image published for both amd64 and arm64 with Python (the suite is PySpark) |
| iceberg-spark-runtime-3.5_2.12 | 1.11.0 | matches the Flink smoke's Iceberg version |
| iceberg-aws-bundle | 1.11.0 | S3FileIO for the MinIO-backed warehouse |
