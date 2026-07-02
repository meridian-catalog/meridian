#!/usr/bin/env sh
# Provisions everything the Flink smoke needs on the Meridian side:
#   - MinIO bucket        flink-smoke
#   - Meridian warehouse  flink_smoke
#   - namespace           flink_ns
#   - table               flink_ns.events  (dropped and re-created, so the
#                         row counts in the smoke scripts are deterministic)
#
# The table is created via the REST API instead of Flink DDL because
# Meridian currently rejects the create-table request Flink sends: the
# Flink connector assigns provisional field ids starting at 0, and
# Meridian validates ids as positive instead of reassigning fresh ones
# ("Malformed request: invalid schema: field id 0 is not positive").
# See README.md ("Known issues").
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

# Reset the table so the smoke's expected row counts (3, then 53) hold.
curl -s -o /dev/null -X DELETE \
    "$MERIDIAN_URL/iceberg/v1/flink_smoke/namespaces/flink_ns/tables/events"
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$MERIDIAN_URL/iceberg/v1/flink_smoke/namespaces/flink_ns/tables" \
    -H 'Content-Type: application/json' -d '{
      "name": "events",
      "schema": {
        "type": "struct",
        "schema-id": 0,
        "fields": [
          {"id": 1, "name": "id",    "required": false, "type": "long"},
          {"id": 2, "name": "name",  "required": false, "type": "string"},
          {"id": 3, "name": "value", "required": false, "type": "double"},
          {"id": 4, "name": "ts",    "required": false, "type": "timestamp"}
        ]
      },
      "stage-create": false,
      "properties": {}
    }')
case "$code" in
  2??) echo "table flink_ns.events created" ;;
  *) echo "table create failed: HTTP $code" >&2; exit 1 ;;
esac
