# Trino smoke test (with cross-engine verification)

Runs Trino 482 (image `trinodb/trino:482`, multi-arch — amd64 and
arm64) against a Meridian server on `localhost:8181` with the
MinIO-backed `spark_smoke` warehouse — deliberately the **same
warehouse the [Spark smoke](../spark/README.md) uses**, so this suite
can read the table the Spark run left behind and prove that data
written by one engine (including merge-on-read position deletes) reads
back exactly through another.

## Prerequisites

- Meridian on `localhost:8181` (auth disabled), e.g.
  `cargo build -p meridian-cli && DATABASE_URL=… ./target/debug/meridian serve`
- MinIO on `localhost:9000` with the local-dev credentials
  (`meridian` / `meridian123`)
- **The Spark smoke must have run first** (`../spark/run.sh`): the
  cross-engine steps read `spark_ns.orders` and
  `spark_ns.orders_by_category` in their final post-MERGE/DELETE state
  (960 rows, 5 snapshots).
- Docker (the image supports amd64 and arm64)

## Running

```sh
./run.sh
```

`run.sh` provisions the bucket/warehouse if missing and tears down
`trino_ns` leftovers (`setup.sh` — it never touches `spark_ns`), starts
a Trino container with the catalog file in `etc/mrd.properties`, and
drives `suite/suite.py` from the host (stdlib Python, statements via
`docker exec … trino --execute`). The container is always removed on
exit; the suite drops everything it created (`trino_ns` only). Exits 0
only if every step verified.

## What it runs

| # | Step | Verification |
|---|---|---|
| 1 | wait for Trino + `SHOW SCHEMAS` | `spark_ns` (created by the Spark smoke) is visible |
| 2 | `CREATE SCHEMA trino_ns WITH (location = …)` | in `SHOW SCHEMAS`; REST `GET …/namespaces/trino_ns` = 200 (see "Schema location" below for why the explicit location) |
| 3 | `CREATE TABLE trino_ns.items` (5 cols, `partitioning = ARRAY['category']`, format v2) + `INSERT` 500 rows | `count(*)` = 500, `sum(amount)` = 37125.0, `sum(quantity)` = 2750, 4 categories; REST metadata has exactly one partition spec (identity) |
| 4 | `ALTER TABLE … ADD COLUMN note VARCHAR` + 10-row insert | count = 510; the 500 pre-evolution rows read `note IS NULL`; the 10 new read `'late'` |
| 5 | **cross-engine table read**: `spark_ns.orders` | must equal Spark's reported final state *exactly*: 960 rows, `sum(amount)` = 68730.0, `sum(quantity)` = 5280, per-category counts {cat_0: 241, cat_1: 241, cat_2: 239, cat_3: 239}, 100 × `note='merged'` (MERGE), 10 × `'late'`, 850 NULL, zero rows in the DELETEd id range 950–999, 5 snapshots over REST. Matching proves Trino applies Spark's merge-on-read **position delete files** through the catalog |
| 6 | **cross-engine view read**: `spark_ns.orders_by_category` | either correct results or a *clean dialect-level rejection* (observed — see below); anything else fails |
| 7 | `CREATE VIEW trino_ns.items_by_category` + read back | 4 categories summing to 510 rows; REST `GET …/views/items_by_category` shows the view lives in Meridian with a `trino` SQL representation |
| 8 | cleanup | `DROP VIEW` / `DROP TABLE` / `DROP SCHEMA` for `trino_ns` only; REST confirms `trino_ns` = 404 and both Spark objects still = 200 |

The expected cross-engine numbers in step 5 are derived from the Spark
suite's deterministic dataset (ids 0–1009 minus the DELETEd 950–999;
`amount = (id % 100) * 1.5`, `quantity = (id % 10) + 1`,
`category = 'cat_' || (id % 4)`), and the row/snapshot totals match the
Spark run's reported `SUITE_RESULT` (row_count 960, snapshot_count 5).

The script's last line of output is a single JSON object
(`SUITE_RESULT: {status, cross_engine_match, details, failures}`), and
`run.sh` exits 0 only if every step verified.

## Reading Spark's view: dialect handling

The Spark suite's view carries only a `spark` SQL representation.
Trino's Iceberg connector executes only representations whose dialect
is `trino` and rejects everything else at analysis time:

```
Query … failed: Cannot read unsupported dialect 'spark' for view 'spark_ns.orders_by_category'
```

The suite treats exactly this rejection (or a correct read, should
Trino ever gain spark-dialect support) as a pass — a silent wrong
answer or a server-side error would fail. The reverse direction is the
same story: the `trino_ns.items_by_category` view this suite creates
carries only a `trino` representation, so Spark would refuse it
analogously. Cross-engine view portability needs multi-dialect
representations (e.g. via Meridian's SQLGlot transpilation sidecar) —
not exercised here.

## Schema location and Trino's staged creates

`CREATE SCHEMA` uses an explicit `WITH (location = …)`, and that is
load-bearing. Trino's REST catalog (`TrinoRestCatalog`, Trino 482)
computes table locations client-side from the namespace's `location`
property; when the namespace has none, `newCreateTableTransaction`
falls back to a **direct, non-staged create** (a workaround for REST
catalogs without `stage-create` support, per the TODO in Trino's
source). Against Meridian that fallback misfires:

1. the direct create registers the table and writes the first metadata
   file (Meridian generates the `<table>-<uuid>` location);
2. Trino's `beginCreateTable` then checks that the table location is
   empty and finds the metadata file its own create just wrote:
   `Cannot create a table on a non-empty location: …`;
3. the query fails but the table stays registered in the catalog.

With a namespace `location` property, Trino derives the table location
itself and uses the proper `stage-create` → `assert-create` commit
path (which Meridian implements), and creates work end to end. A
Meridian-side option — vending a default namespace location so vanilla
`CREATE SCHEMA` from Trino works — is a catalog conformance item worth
tracking.

## S3 endpoint vending vs. containerized engines

Unlike the Spark/Flink smokes, this container does **not** need host
networking. Trino's native S3 file system (`fs.native-s3.enabled`) is
configured purely from the catalog properties when
`iceberg.rest-catalog.vended-credentials-enabled=false`, so the
`s3.endpoint` vended by Meridian in `LoadTableResult.config`
(`http://localhost:9000`) never reaches the S3 client and the
client-side `s3.endpoint=http://host.docker.internal:9000` override in
`etc/mrd.properties` sticks. The container runs on the default bridge
network with `--add-host host.docker.internal:host-gateway`.

## Version pinning

| Component | Version | Why |
|---|---|---|
| Trino | 482 (`trinodb/trino:482`) | current release at the time of writing; published for amd64 and arm64 (verified with `docker manifest inspect`) |
