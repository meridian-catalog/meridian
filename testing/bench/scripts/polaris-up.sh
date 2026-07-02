#!/usr/bin/env bash
# Boot Apache Polaris against the shared dev Postgres + MinIO, ready to bench.
#
# Creates (idempotently): database polaris_bench, bucket bench-polaris,
# realm POLARIS with root credentials, catalog bench_s3 backed by MinIO.
#
# Usage: polaris-up.sh [--reset]   (--reset drops and recreates the database)
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=env.sh
source ./env.sh

if [[ "${1:-}" == "--reset" ]]; then
  docker rm -f bench-polaris >/dev/null 2>&1 || true
  pg_sql "DROP DATABASE IF EXISTS polaris_bench;"
fi

pg_sql "CREATE DATABASE polaris_bench;" 2>/dev/null || true
ensure_bucket bench-polaris

JDBC_URL="jdbc:postgresql://host.docker.internal:${PG_HOST_PORT}/polaris_bench"

# One-time, idempotent: writes the root principal into the metastore.
docker run --rm \
  -e POLARIS_PERSISTENCE_TYPE=relational-jdbc \
  -e QUARKUS_DATASOURCE_JDBC_URL="$JDBC_URL" \
  -e QUARKUS_DATASOURCE_USERNAME="$PG_USER" \
  -e QUARKUS_DATASOURCE_PASSWORD="$PG_PASSWORD" \
  "$POLARIS_ADMIN_IMAGE" bootstrap --realm=POLARIS --credential=POLARIS,root,s3cr3t

docker rm -f bench-polaris >/dev/null 2>&1 || true
# shellcheck disable=SC2086  # FAIR_LIMITS is intentionally word-split
docker run -d --name bench-polaris $FAIR_LIMITS \
  -p "${POLARIS_PORT}:8181" -p "${POLARIS_HEALTH_PORT}:8182" \
  -e POLARIS_PERSISTENCE_TYPE=relational-jdbc \
  -e QUARKUS_DATASOURCE_JDBC_URL="$JDBC_URL" \
  -e QUARKUS_DATASOURCE_USERNAME="$PG_USER" \
  -e QUARKUS_DATASOURCE_PASSWORD="$PG_PASSWORD" \
  -e POLARIS_REALM_CONTEXT_REALMS=POLARIS \
  -e QUARKUS_OTEL_SDK_DISABLED=true \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
  -e AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
  -e JAVA_MAX_MEM_RATIO=50 \
  "$POLARIS_IMAGE"

echo "waiting for Polaris health…"
for _ in $(seq 1 60); do
  if curl -sf "http://localhost:${POLARIS_HEALTH_PORT}/q/health" >/dev/null; then
    break
  fi
  sleep 2
done
curl -sf "http://localhost:${POLARIS_HEALTH_PORT}/q/health" >/dev/null \
  || { echo "Polaris did not become healthy"; docker logs --tail 50 bench-polaris; exit 1; }

TOKEN=$(curl -s "http://localhost:${POLARIS_PORT}/api/catalog/v1/oauth/tokens" \
  --user root:s3cr3t \
  -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
[[ -n "$TOKEN" ]] || { echo "failed to obtain Polaris token"; exit 1; }

# Catalog backed by MinIO. endpointInternal is what the Polaris container
# uses; endpoint is what gets vended to host-side clients.
curl -s -o /dev/null -w "create catalog: %{http_code}\n" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  "http://localhost:${POLARIS_PORT}/api/management/v1/catalogs" -d '{
  "catalog": {
    "name": "bench_s3", "type": "INTERNAL", "readOnly": false,
    "properties": {"default-base-location": "s3://bench-polaris/warehouse"},
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://bench-polaris/warehouse"],
      "endpoint": "http://localhost:9000",
      "endpointInternal": "http://host.docker.internal:9000",
      "pathStyleAccess": true,
      "region": "us-east-1"
    }
  }}'

# Content privileges for root on the new catalog (table DDL 403s without it).
curl -s -o /dev/null -w "grant: %{http_code}\n" -X PUT \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  "http://localhost:${POLARIS_PORT}/api/management/v1/catalogs/bench_s3/catalog-roles/catalog_admin/grants" \
  -d '{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}'

echo "Polaris ready: http://localhost:${POLARIS_PORT}/api/catalog (warehouse bench_s3)"
echo "token endpoint: http://localhost:${POLARIS_PORT}/api/catalog/v1/oauth/tokens (root/s3cr3t)"
