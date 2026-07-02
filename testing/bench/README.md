# meridian-bench

A small, vendor-neutral benchmark harness for Iceberg REST catalog (IRC)
servers. It measures catalog-plane HTTP latency only — no query engines, no
data files, no object-store I/O in the timed path.

## Scenarios

| Scenario | What it does | Defaults |
|---|---|---|
| `get-config` | `GET /v1/config?warehouse=…` | 2000 measured, 100 warm-up, sequential |
| `load-table` | `GET …/namespaces/{ns}/tables/{t}` against the fixture table | 2000 measured + 100 warm-up **per concurrency level**, sweep c ∈ {1, 8, 32} |
| `commit` | sequential `set-properties` commits (`POST …/tables/{t}`) | 200 measured, 20 warm-up |

Warm-up requests are issued but excluded from every statistic. Latency is
recorded per request into an HDR histogram (microsecond resolution, 3
significant digits); throughput is successful requests over the measured
wall-clock window. Non-2xx responses are counted as errors and reported —
never silently dropped.

The fixture (`--setup`) creates a namespace and a table with 40 columns of
mixed primitive types, then layers 20 append snapshots on top via sequential
IRC commits, so `loadTable` serves realistically sized metadata rather than a
toy two-column table. Setup runs before each benchmark run and drops the
previous fixture table first, so repeated runs start from identical state.
The snapshots are metadata-level only (fabricated manifest-list paths under
the table location); the catalogs never read those files on the load or
commit path.

## Auth

- `--auth none` — no Authorization header (Meridian and Lakekeeper with auth
  disabled).
- `--auth oauth2` — OAuth2 client-credentials. The token is fetched **once,
  before any timed request**, so token acquisition never appears in the
  measured path (Polaris requires this mode; it cannot run without auth).

## Usage

```bash
cargo build --release -p meridian-bench

# Meridian (auth disabled)
./target/release/meridian-bench \
  --catalog-name meridian \
  --base-url http://localhost:8181/iceberg \
  --warehouse bench_meridian \
  --setup --out meridian.json --markdown meridian.md

# Apache Polaris (OAuth2 client-credentials)
./target/release/meridian-bench \
  --catalog-name polaris \
  --base-url http://localhost:8183/api/catalog \
  --warehouse bench_s3 \
  --auth oauth2 \
  --token-url http://localhost:8183/api/catalog/v1/oauth/tokens \
  --client-id root --client-secret s3cr3t --scope PRINCIPAL_ROLE:ALL \
  --setup --out polaris.json --markdown polaris.md

# Lakekeeper (auth disabled)
./target/release/meridian-bench \
  --catalog-name lakekeeper \
  --base-url http://localhost:8184/catalog \
  --warehouse bench \
  --setup --out lakekeeper.json --markdown lakekeeper.md
```

The IRC path prefix is resolved from `GET /v1/config` (`overrides.prefix`,
then `defaults.prefix`, then the warehouse name) the way spec-conformant
clients do — this matters for Lakekeeper, whose prefix is a warehouse UUID
rather than the warehouse name.

All counts, warm-ups, the concurrency sweep, fixture shape, namespace and
table names are tunable; see `--help`.

## Environment scripts

`scripts/` boots each catalog against the same local Postgres container
(one database per catalog) and the same MinIO instance (one bucket per
catalog):

- `scripts/meridian-up.sh` — release build of Meridian, auth disabled, on :8181
- `scripts/polaris-up.sh [--reset]` — Apache Polaris 1.5.0 on :8183 (bootstraps
  realm + catalog + grants)
- `scripts/lakekeeper-up.sh [--reset]` — Lakekeeper v0.13.1 on :8184 (migrates +
  bootstraps + warehouse)
- `scripts/bench-down.sh` — stops everything the scripts started

Competitor containers run with identical resource caps (`FAIR_LIMITS`,
default `-m 4g --cpus 4`). See `docs/benchmarks/` for methodology, published
results, and the honest list of caveats (including the native-vs-Docker
asymmetry for Meridian itself).
