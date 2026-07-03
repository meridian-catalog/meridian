#!/usr/bin/env sh
# Provisions everything the Spark smoke needs on the Meridian side:
#   - MinIO bucket        spark-smoke
#   - Meridian warehouse  spark_smoke
#
# The suite itself creates the namespace, table and view through Spark
# DDL, so this script tears down any leftovers from a previous run
# (view, table, then namespace) to keep the suite's expected counts
# deterministic. 404s on the deletes are fine on a first run.
set -eu

MERIDIAN_URL="${MERIDIAN_URL:-http://localhost:8181}"
MINIO_URL="${MINIO_URL:-http://localhost:9000}"

# Bucket (conflict output from MinIO is fine on re-runs).
curl -sf --aws-sigv4 "aws:amz:us-east-1:s3" --user meridian:meridian123 \
    -X PUT "$MINIO_URL/spark-smoke" > /dev/null || echo "bucket spark-smoke already exists (ok)"

# Warehouse (409 on re-runs is fine).
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$MERIDIAN_URL/api/v2/warehouses" \
    -H 'Content-Type: application/json' -d '{
      "name": "spark_smoke",
      "storage_root": "s3://spark-smoke/warehouse",
      "storage_options": {
        "endpoint": "http://localhost:9000",
        "path-style": "true",
        "region": "us-east-1",
        "access-key-id": "meridian",
        "secret-access-key": "meridian123"
      }
    }')
case "$code" in
  2??) echo "warehouse spark_smoke created" ;;
  409) echo "warehouse spark_smoke already exists (ok)" ;;
  *) echo "warehouse create failed: HTTP $code" >&2; exit 1 ;;
esac

drop() {
    what="$1"; url="$2"
    code=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "$url")
    case "$code" in
      2??) echo "$what dropped" ;;
      404) echo "$what absent (ok)" ;;
      *) echo "$what drop failed: HTTP $code" >&2; exit 1 ;;
    esac
}

drop "view spark_ns.orders_by_category" \
    "$MERIDIAN_URL/iceberg/v1/spark_smoke/namespaces/spark_ns/views/orders_by_category"
drop "table spark_ns.orders" \
    "$MERIDIAN_URL/iceberg/v1/spark_smoke/namespaces/spark_ns/tables/orders?purgeRequested=true"
drop "namespace spark_ns" \
    "$MERIDIAN_URL/iceberg/v1/spark_smoke/namespaces/spark_ns"
