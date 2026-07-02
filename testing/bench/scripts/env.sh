#!/usr/bin/env bash
# Shared settings for the local benchmark environment.
#
# Assumes the standard Meridian dev containers are running:
#   - Postgres:  container meridian-dev-pg, host port 5433, user meridian/meridian
#   - MinIO:     container meridian-minio, host port 9000, root meridian/meridian123
#
# Every catalog gets its own Postgres database inside the same container and
# its own MinIO bucket. Competitor containers get identical resource caps so
# neither JVM nor Rust runtimes can grab the whole Docker VM.

export PG_CONTAINER="${PG_CONTAINER:-meridian-dev-pg}"
export PG_HOST_PORT="${PG_HOST_PORT:-5433}"
export PG_USER="${PG_USER:-meridian}"
export PG_PASSWORD="${PG_PASSWORD:-meridian}"

export MINIO_CONTAINER="${MINIO_CONTAINER:-meridian-minio}"
export MINIO_ENDPOINT_HOST="${MINIO_ENDPOINT_HOST:-http://localhost:9000}"
export MINIO_ENDPOINT_DOCKER="${MINIO_ENDPOINT_DOCKER:-http://host.docker.internal:9000}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-meridian}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-meridian123}"

# Resource caps applied to every competitor container (docker run flags).
export FAIR_LIMITS="${FAIR_LIMITS:--m 4g --cpus 4}"

# Image pins. Record the digests alongside any published numbers.
export POLARIS_IMAGE="${POLARIS_IMAGE:-apache/polaris:1.5.0}"
export POLARIS_ADMIN_IMAGE="${POLARIS_ADMIN_IMAGE:-apache/polaris-admin-tool:1.5.0}"
export LAKEKEEPER_IMAGE="${LAKEKEEPER_IMAGE:-quay.io/lakekeeper/catalog:v0.13.1}"

# Host ports (Meridian's own server uses 8181; do not collide).
export POLARIS_PORT="${POLARIS_PORT:-8183}"
export POLARIS_HEALTH_PORT="${POLARIS_HEALTH_PORT:-8193}"
export LAKEKEEPER_PORT="${LAKEKEEPER_PORT:-8184}"

pg_sql() {
  docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d postgres -c "$1"
}

ensure_bucket() {
  docker exec "$MINIO_CONTAINER" mc alias set local http://localhost:9000 \
    "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null
  docker exec "$MINIO_CONTAINER" mc mb --ignore-existing "local/$1"
}
