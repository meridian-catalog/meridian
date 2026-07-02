# Meridian end-to-end engine suite

Runs real Iceberg clients (pyiceberg, DuckDB) against a running Meridian
server and verifies full table lifecycles: create, append, scan, schema
evolution, time travel, rename, drop, and concurrent commits.

## Prerequisites

- A running Meridian server on `localhost:8181`:

  ```sh
  docker start meridian-dev-pg
  DATABASE_URL=postgres://meridian:meridian@localhost:5433/meridian \
      cargo run -p meridian-cli -- serve
  ```

- MinIO on `localhost:9000` (console `:9001`), credentials
  `meridian` / `meridian123`, for the S3 tests. If MinIO is not reachable
  the S3 tests are skipped, not failed.

- [`uv`](https://docs.astral.sh/uv/) with Python >= 3.12 available.

## Run

```sh
./run.sh
```

`run.sh` generates a unique run id, so every invocation creates fresh
warehouses, namespaces, buckets, and `/tmp` directories — no state from a
previous run is reused, and no cleanup is required between runs.

To run a single file:

```sh
E2E_RUN_ID=$(date +%s) uv run pytest tests/test_pyiceberg_fs.py -v
```

## What is covered

| File | Engine | Storage | Scope |
| --- | --- | --- | --- |
| `test_pyiceberg_fs.py` | pyiceberg | `file:///tmp` | full lifecycle: namespaces, create table, appends, scans, snapshots, schema evolution, time travel, rename, drop |
| `test_pyiceberg_minio.py` | pyiceberg | `s3://` (MinIO) | same lifecycle against object storage |
| `test_duckdb_read.py` | DuckDB iceberg extension | `file:///tmp` | read a pyiceberg-written, Meridian-committed table (REST `ATTACH` first, `iceberg_scan` fallback) |
| `test_concurrent_writers.py` | pyiceberg x2 | `file:///tmp` | two catalog instances appending concurrently to one table |

Every pyiceberg HTTP interaction is watched through a response hook: any
5xx from the server fails the test that triggered it.

Known, deliberately-noted gap: Meridian's `LoadTableResult.config` is
always empty, so S3 credentials/endpoint are not vended to clients and the
S3 tests configure `s3.*` properties client-side
(`test_pyiceberg_minio.py::test_server_does_not_vend_storage_config`
records this).
