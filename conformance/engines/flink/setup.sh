#!/usr/bin/env sh
# Provisions everything the Flink smoke needs on the Meridian side:
#   - MinIO bucket        flink-smoke
#   - Meridian warehouse  flink_smoke
#   - namespace           flink_ns
#
# The flink_ns.events table is dropped (if present) so the smoke's
# CREATE TABLE runs real Flink DDL and the row counts stay deterministic.
# Historical note: this script used to pre-create the table via REST as a
# workaround for a Meridian bug — create requests carrying Flink's 0-based
# provisional field ids were rejected ("invalid schema: field id 0 is not
# positive") instead of getting fresh server-assigned ids. Meridian now
# treats create-request field ids as provisional and assigns fresh ones
# (like the Java reference implementation), so Flink DDL works directly.
set -eu

MERIDIAN_URL="${MERIDIAN_URL:-http://localhost:8181}"
MINIO_URL="${MINIO_URL:-http://localhost:9000}"

# Bucket (409/conflict output from MinIO is fine on re-runs).
curl -sf --aws-sigv4 "aws:amz:us-east-1:s3" --user meridian:meridian123 \
    -X PUT "$MINIO_URL/flink-smoke" > /dev/null || echo "bucket flink-smoke already exists (ok)"

# Warehouse (409 on re-runs is fine).
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$MERIDIAN_URL/api/v2/warehouses" \
    -H 'Content-Type: application/json' -d '{
      "name": "flink_smoke",
      "storage_root": "s3://flink-smoke/warehouse",
      "storage_options": {
        "endpoint": "http://localhost:9000",
        "path-style": "true",
        "region": "us-east-1",
        "access-key-id": "meridian",
        "secret-access-key": "meridian123"
      }
    }')
case "$code" in
  2??) echo "warehouse flink_smoke created" ;;
  409) echo "warehouse flink_smoke already exists (ok)" ;;
  *) echo "warehouse create failed: HTTP $code" >&2; exit 1 ;;
esac

# Namespace (409 on re-runs is fine).
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$MERIDIAN_URL/iceberg/v1/flink_smoke/namespaces" \
    -H 'Content-Type: application/json' -d '{"namespace": ["flink_ns"]}')
case "$code" in
  2??) echo "namespace flink_ns created" ;;
  409) echo "namespace flink_ns already exists (ok)" ;;
  *) echo "namespace create failed: HTTP $code" >&2; exit 1 ;;
esac

# Drop the table so the smoke's CREATE TABLE IF NOT EXISTS actually runs
# Flink DDL and the expected row counts (3, then 53) hold on every run.
code=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
    "$MERIDIAN_URL/iceberg/v1/flink_smoke/namespaces/flink_ns/tables/events")
case "$code" in
  2??) echo "table flink_ns.events dropped" ;;
  404) echo "table flink_ns.events absent (ok)" ;;
  *) echo "table drop failed: HTTP $code" >&2; exit 1 ;;
esac
