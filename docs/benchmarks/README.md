# Catalog benchmarks

Latency benchmarks of the Iceberg REST catalog plane: Meridian compared
against other open-source IRC servers, using the harness in
[`testing/bench/`](../../testing/bench/).

These are **local development benchmarks** run on a laptop. They are useful
for tracking Meridian's own regressions and for a rough sense of where the
implementations stand relative to each other; they are not cloud or
production performance claims. Read the caveats section of each result
document before quoting any number.

## Results

- [2026-07-03 — initial local benchmark](2026-07-03-initial-local.md):
  Meridian `0.1.0` vs Apache Polaris `1.5.0` vs Lakekeeper `v0.13.1`.

## Reproducing

Everything runs against the standard local dev dependencies (one Postgres
container, one MinIO container). One catalog is benchmarked at a time; the
others are stopped.

```bash
# 0. Shared infra: the Meridian dev Postgres (:5433) and MinIO (:9000)
#    containers must be running (see docs/dev.md).

cargo build --release -p meridian-cli -p meridian-bench
cd testing/bench/scripts

# 1. Meridian (release build, auth disabled, native process)
./meridian-up.sh
../../../target/release/meridian-bench \
  --catalog-name meridian --base-url http://localhost:8181/iceberg \
  --warehouse bench_meridian --setup --out meridian.json --markdown meridian.md
./bench-down.sh

# 2. Apache Polaris (Docker, OAuth2 client-credentials)
./polaris-up.sh --reset
../../../target/release/meridian-bench \
  --catalog-name polaris --base-url http://localhost:8183/api/catalog \
  --warehouse bench_s3 --auth oauth2 \
  --token-url http://localhost:8183/api/catalog/v1/oauth/tokens \
  --client-id root --client-secret s3cr3t \
  --setup --out polaris.json --markdown polaris.md
./bench-down.sh

# 3. Lakekeeper (Docker, auth disabled)
./lakekeeper-up.sh --reset
../../../target/release/meridian-bench \
  --catalog-name lakekeeper --base-url http://localhost:8184/catalog \
  --warehouse bench --setup --out lakekeeper.json --markdown lakekeeper.md
./bench-down.sh
```

Protocol for published numbers: run each catalog twice back-to-back
(`--setup` re-creates the fixture before each run so both start from
identical state) and publish the better run of the two, for every catalog
alike. Record versions, image digests, and hardware alongside the tables.
