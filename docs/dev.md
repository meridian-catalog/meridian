# Developing Meridian

## Prerequisites

- Rust 1.88+ (the workspace uses edition 2024; developed against 1.96)
- Docker (for Postgres and container builds)
- No local Postgres needed — the dev Postgres for running from source lives in
  Docker on host port **5433** (avoids clashing with a locally installed
  Postgres on 5432). The compose stack's Postgres is not host-published at all;
  reach it with `docker compose -f docker-compose.dev.yml exec postgres psql -U meridian`.

## Quick start (everything in Docker)

```sh
docker compose -f docker-compose.dev.yml up --build
```

This starts Postgres and the Meridian server (migrations run automatically on
startup). Then:

```sh
curl -s localhost:8181/healthz
# {"status":"ok","checks":{"database":"ok"}}

curl -s localhost:8181/v1/config
# {"defaults":{},"overrides":{},"endpoints":[...],"idempotency-key-lifetime":"PT24H"}

curl -s "localhost:8181/iceberg/v1/config?warehouse=<name>"
# {"defaults":{},"overrides":{"prefix":"<name>"},"endpoints":[...],"idempotency-key-lifetime":"PT24H"}
```

## Running from source

Start just the dev database:

```sh
docker run -d --name meridian-dev-pg -p 5433:5432 \
  -e POSTGRES_USER=meridian \
  -e POSTGRES_PASSWORD=meridian \
  -e POSTGRES_DB=meridian \
  postgres:16-alpine
```

Then run the server (applies migrations, then serves on `:8181`):

```sh
export DATABASE_URL=postgres://meridian:meridian@localhost:5433/meridian
cargo run -p meridian-cli -- serve
```

Configuration is layered: defaults < `meridian.toml` (or `--config <path>`)
< `DATABASE_URL` < `MERIDIAN__*` environment variables (double-underscore
nesting, e.g. `MERIDIAN__SERVER__PORT=8282`,
`MERIDIAN__TELEMETRY__FORMAT=json`).

## Tests

Unit tests run with no external services. Database and HTTP integration tests
need `DATABASE_URL`; when it is unset they skip with a note on stderr.

```sh
# Full suite, including DB-backed tests (start meridian-dev-pg first, above):
export DATABASE_URL=postgres://meridian:meridian@localhost:5433/meridian
cargo test --workspace
```

## Lint and format

Both are enforced in CI; run before pushing:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
```

## Container image

```sh
docker build -t meridian:dev .
docker run --rm -p 8181:8181 \
  -e DATABASE_URL=postgres://meridian:meridian@host.docker.internal:5433/meridian \
  meridian:dev
```

The image is a multi-stage build (Rust builder, `debian:bookworm-slim`
runtime, non-root user). The full-workspace release build is slow from a cold
cache; dependency layer caching is a known TODO.
