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
| `test_views.py` | pyiceberg + raw REST | `file:///tmp` | view lifecycle: create (two SQL dialects) / load / replace / rename / collision 409s via raw REST; `list_views`, `view_exists`, `drop_view` via pyiceberg (its RestCatalog implements only those three view operations as of 0.11.x — no `create_view`/`load_view` yet, hence the raw-requests half) |
| `test_remote_signing.py` | pyiceberg (fsspec FileIO) | `s3://` (MinIO) | write + scan with **zero client-side keys** via `X-Iceberg-Access-Delegation: remote-signing`: every object request signed through `POST .../tables/{table}/sign`; also asserts the advertisement carries no credential-shaped keys and that the signer refuses foreign objects (403). pyiceberg's pyarrow FileIO has no remote-signing support as of 0.11.x, hence the `py-io-impl` pin |

Every pyiceberg HTTP interaction is watched through a response hook: any
5xx from the server fails the test that triggered it.

Storage config: Meridian vends the warehouse's **non-secret** storage
options (`s3.endpoint`, `s3.region`/`client.region`,
`s3.path-style-access`) in `LoadTableResult.config` /
`LoadViewResult.config`
(`test_pyiceberg_minio.py::test_server_vends_non_secret_storage_config`
verifies this). Credentials are never passed through as config — the
plain S3 tests still configure
`s3.access-key-id`/`s3.secret-access-key` client-side, and the same test
asserts the server never leaks them. Credential **vending** is separate
and opt-in: `test_vended_credentials.py` runs a warehouse with
`vending = "sts"` against MinIO's STS endpoint, and its pyiceberg client
holds zero S3 configuration — scoped, short-lived session credentials
arrive from the catalog, and a boto3 check proves they cannot cross into
a sibling table's prefix. Remote **signing** (`test_remote_signing.py`)
is the credential-free alternative: the client asks for the
`remote-signing` delegation and the catalog signs each S3 request at the
per-table sign endpoint — no keys of any kind reach the client.
