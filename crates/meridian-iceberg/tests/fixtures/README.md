# Test fixtures

## Table metadata JSON (`table_metadata_*.json`, `view_metadata_v1.json`)

Hand-maintained metadata documents exercising the v1/v2/v3 table and v1
view shapes; used by the metadata round-trip tests.

## Manifest fixtures (`pyiceberg_v1/`, `pyiceberg_v2/`, `spark_orders/`)

Real Avro manifest lists and manifests, each directory paired with an
`expected.json` — a dump of **pyiceberg's own view** of the same files
(manifest-list fields, entries with sequence-number inheritance applied,
partition tuples, bound bytes as hex). The `manifest_fixtures` test
parses the Avro with this crate and compares field-for-field against
that dump, so the reader is checked against an independent
implementation rather than against itself.

- `pyiceberg_v1/`, `pyiceberg_v2/`: written locally by pyiceberg 0.11.1
  (`SqlCatalog`, file warehouse). Wide primitive schema (boolean, int,
  long, float with NaN, double with NaN and nulls, decimal(9,2),
  decimal(28,10), date, time, timestamp, timestamptz, string, uuid,
  binary, fixed[4]). The v2 table is partitioned by
  `identity(category), day(ts)` and has two appends plus a partition
  delete (ADDED and DELETED entry statuses); the v1 table uses an
  identity partition with two appends.
- `spark_orders/`: the metadata layer of the conformance suite's real
  Spark-written merge-on-read table (`spark_ns.orders`, format v2,
  five snapshots: three appends, an update, a delete — with
  position-delete manifests). Copied from the dev MinIO after running
  `conformance/engines/spark/`; `expected.json` was dumped by pyiceberg
  reading straight from that warehouse. `table.metadata.json` is the
  table's current metadata document.

### Regenerating

`generate.py` rebuilds everything. It needs a Python environment with
`pyiceberg[sql-sqlite,pyarrow]` (e.g. `uv venv && uv pip install
'pyiceberg[sql-sqlite,pyarrow]'`) and, for the `spark_orders` dump, the
dev MinIO from `docker-compose.dev.yml` with the Spark smoke suite's
warehouse in place (set `SKIP_SPARK=1` to regenerate only the pyiceberg
tables):

```sh
python crates/meridian-iceberg/tests/fixtures/generate.py
```

Paths inside `expected.json` (`file://.../pyiceberg_wh/...`,
`s3://spark-smoke/...`) are the paths at generation time; tests resolve
manifest files by basename within each fixture directory, so the
absolute prefixes are irrelevant — but regeneration rewrites
`expected.json` and the `.avro` files together, keeping them
consistent.
