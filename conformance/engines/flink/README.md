# Flink smoke test

Runs Apache Flink 1.20 (SQL Client) against a Meridian server on
`localhost:8181` with a MinIO-backed (`s3://`) warehouse, exercising the
Iceberg REST catalog from Flink's connector: catalog + namespace DDL,
a batch `INSERT`/`SELECT` round trip, and a streaming insert that
commits through Flink checkpoints.

## Status: pass

| Scenario | Result |
| --- | --- |
| `CREATE CATALOG` (REST, S3FileIO) | pass |
| `CREATE DATABASE` (namespace DDL) | pass |
| `CREATE TABLE` (table DDL) | pass — required a Meridian fix, see [Resolved issues](#resolved-issues) |
| Batch `INSERT INTO ... VALUES` + `SELECT COUNT(*)` / full scan | pass |
| Streaming insert (datagen source, 2s checkpoints) | pass |
| Batch read-back after streaming | pass (53 rows: 3 batch + 50 streamed) |

Verified with: `flink:1.20-scala_2.12-java17`,
`iceberg-flink-runtime-1.20` **1.11.0**, `iceberg-aws-bundle` **1.11.0**,
`flink-shaded-hadoop-2-uber` **2.8.3-10.0**.

## Prerequisites

- A running Meridian server on `localhost:8181` (see
  [`../../e2e/README.md`](../../e2e/README.md)).
- MinIO on `localhost:9000`, credentials `meridian` / `meridian123`.
- Docker Desktop **with host networking enabled** (Settings → Resources →
  Network on macOS/Windows; native on Linux). Check with:

  ```sh
  docker run --rm --network host curlimages/curl -s http://localhost:9000/minio/health/live -o /dev/null -w "%{http_code}\n"
  # must print 200
  ```

  Host networking is load-bearing here — see
  [Networking](#networking-why-host-mode) below.

## Run

```sh
./run.sh                # fetch jars, provision, start cluster, run both smokes
docker compose down     # tear down the Flink containers afterwards
```

`run.sh` is idempotent: `setup.sh` drops the `events` table so the batch
smoke's `CREATE TABLE` re-creates it fresh, and the expected row counts
(3 after the batch smoke, 53 after the streaming smoke) hold on every
run. The Flink web UI is on `:8081` while
the cluster is up.

## Layout

| File | Purpose |
| --- | --- |
| `fetch-jars.sh` | downloads the three pinned jars into `jars/` (gitignored) |
| `setup.sh` | creates the MinIO bucket, Meridian warehouse `flink_smoke`, and namespace `flink_ns`, and drops any leftover `events` table so the smoke's `CREATE TABLE` runs real DDL |
| `docker-compose.yml` | one jobmanager + one taskmanager, host networking, jars mounted into `/opt/flink/lib/iceberg` |
| `sql/00_catalog.sql` | shared `CREATE CATALOG` init script (`sql-client.sh -i`) |
| `sql/10_batch_smoke.sql` | batch DDL + insert + read-back |
| `sql/20_streaming_smoke.sql` | streaming datagen → Iceberg sink with 2s checkpoints, then a batch count |

## Resolved issues

### Flink `CREATE TABLE` was rejected (Meridian bug, fixed)

Earlier runs failed table DDL with:

```
[ERROR] Could not execute SQL statement. Reason:
org.apache.iceberg.exceptions.BadRequestException: Malformed request:
invalid schema: field id 0 is not positive
```

The Flink connector (`FlinkSchemaUtil`) assigns *provisional* field ids
starting at 0 in its create-table request. The Iceberg REST spec treats
incoming ids as provisional — the server is expected to assign fresh ids
(the Java reference implementation runs `AssignFreshIds`, starting at 1).
Meridian used to validate the incoming ids as-is and reject id 0;
pyiceberg assigns 1-based ids, which is why the e2e suite never hit this.
Meridian now assigns fresh field ids server-side on `createTable`
(`assign_fresh_ids` in `crates/meridian-iceberg/src/spec/fresh.rs`), and
the smoke's `CREATE TABLE` passes without the pre-create workaround that
`setup.sh` used to carry.

## Known issues

### S3 endpoint vending vs. containerized engines

Meridian vends the warehouse's `s3.endpoint`
(here `http://localhost:9000`) in `LoadTableResult.config`, and the
Iceberg Java REST client merges that vended config **over** catalog-level
client properties — so an engine container cannot override the endpoint
client-side; a `host.docker.internal` override in `CREATE CATALOG` is
silently clobbered back on every table load. Vending separate
internal/external endpoints is a known limitation on the roadmap.

### Hadoop is still required

`iceberg-flink-runtime` needs `org.apache.hadoop.conf.Configuration` on
the classpath for its catalog factory — without the
`flink-shaded-hadoop-2-uber` jar, `CREATE CATALOG` fails with
`ClassNotFoundException: org.apache.hadoop.conf.Configuration`
(verified against 1.11.0). `fetch-jars.sh` includes it.

## Networking (why host mode)

The Flink containers run with `network_mode: host` so that the vended
`s3.endpoint=http://localhost:9000` resolves to MinIO from inside the
containers (Docker publishes the MinIO port into the VM/host namespace).
The catalog `uri` is **not** vended, so it can point at
`http://host.docker.internal:8181` where Meridian runs on the host.
Note that with Docker Desktop host networking, `localhost` inside the
container reaches the Docker VM, not the macOS/Windows host — published
container ports (MinIO's 9000) work, host processes (Meridian's 8181) do
not, which is why the two URLs differ.
