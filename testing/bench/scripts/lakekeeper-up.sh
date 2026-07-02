#!/usr/bin/env bash
# Boot Lakekeeper against the shared dev Postgres + MinIO, ready to bench.
#
# Creates (idempotently): database lakekeeper_bench, bucket bench-lakekeeper,
# default project, warehouse "bench" backed by MinIO. Lakekeeper runs with
# auth disabled (its default when no OpenID provider is configured).
#
# Usage: lakekeeper-up.sh [--reset]   (--reset drops and recreates the database)
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=env.sh
source ./env.sh

if [[ "${1:-}" == "--reset" ]]; then
  docker rm -f bench-lakekeeper >/dev/null 2>&1 || true
  pg_sql "DROP DATABASE IF EXISTS lakekeeper_bench;"
fi

pg_sql "CREATE DATABASE lakekeeper_bench;" 2>/dev/null || true
ensure_bucket bench-lakekeeper

PG_URL="postgresql://${PG_USER}:${PG_PASSWORD}@host.docker.internal:${PG_HOST_PORT}/lakekeeper_bench"

# One-time, idempotent migrations. The encryption key protects stored
# warehouse credentials; it must stay identical across migrate/serve.
docker run --rm \
  -e LAKEKEEPER__PG_ENCRYPTION_KEY="bench-insecure-key" \
  -e LAKEKEEPER__PG_DATABASE_URL_READ="$PG_URL" \
  -e LAKEKEEPER__PG_DATABASE_URL_WRITE="$PG_URL" \
  "$LAKEKEEPER_IMAGE" migrate

docker rm -f bench-lakekeeper >/dev/null 2>&1 || true
# shellcheck disable=SC2086  # FAIR_LIMITS is intentionally word-split
docker run -d --name bench-lakekeeper $FAIR_LIMITS \
  -p "${LAKEKEEPER_PORT}:8181" \
  -e LAKEKEEPER__PG_ENCRYPTION_KEY="bench-insecure-key" \
  -e LAKEKEEPER__PG_DATABASE_URL_READ="$PG_URL" \
  -e LAKEKEEPER__PG_DATABASE_URL_WRITE="$PG_URL" \
  "$LAKEKEEPER_IMAGE" serve

echo "waiting for Lakekeeper…"
for _ in $(seq 1 60); do
  if curl -sf "http://localhost:${LAKEKEEPER_PORT}/health" >/dev/null 2>&1 \
     || curl -s "http://localhost:${LAKEKEEPER_PORT}/catalog/v1/config?warehouse=x" \
        | grep -q .; then
    break
  fi
  sleep 1
done

# One-time; 204 on first run, error text afterwards (harmless).
curl -s -X POST "http://localhost:${LAKEKEEPER_PORT}/management/v1/bootstrap" \
  -H 'Content-Type: application/json' -d '{"accept-terms-of-use": true}' || true
echo

# Warehouse backed by MinIO. NOTE: the storage endpoint below is what
# Lakekeeper vends to clients in s3.endpoint; from the macOS host that
# hostname does not resolve, so engine clients must override it. The bench
# harness never touches storage, so this does not affect catalog timings.
curl -s -X POST "http://localhost:${LAKEKEEPER_PORT}/management/v1/warehouse" \
  -H 'Content-Type: application/json' -d '{
  "warehouse-name": "bench",
  "storage-profile": {
    "type": "s3",
    "bucket": "bench-lakekeeper",
    "key-prefix": "warehouse",
    "endpoint": "'"$MINIO_ENDPOINT_DOCKER"'",
    "region": "local-01",
    "path-style-access": true,
    "flavor": "s3-compat",
    "sts-enabled": true
  },
  "storage-credential": {
    "type": "s3",
    "credential-type": "access-key",
    "aws-access-key-id": "'"$MINIO_ACCESS_KEY"'",
    "aws-secret-access-key": "'"$MINIO_SECRET_KEY"'"
  }
}'
echo
echo "Lakekeeper ready: http://localhost:${LAKEKEEPER_PORT}/catalog (warehouse bench)"
