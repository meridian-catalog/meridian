#!/usr/bin/env sh
# Provisions what the Trino smoke needs on the Meridian side and tears
# down leftovers from a previous run.
#
# The suite deliberately shares the Spark smoke's warehouse
# (spark_smoke, bucket spark-smoke) so it can read the table the Spark
# suite left behind (cross-engine verification). This script therefore:
#   - ensures the bucket and warehouse exist (idempotent, same
#     definitions as ../spark/setup.sh);
#   - deletes ONLY trino_ns leftovers (view, table, namespace). It never
#     touches spark_ns — that is the cross-engine fixture.
# 404s on the deletes are fine on a first run.
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

drop "view trino_ns.items_by_category" \
    "$MERIDIAN_URL/iceberg/v1/spark_smoke/namespaces/trino_ns/views/items_by_category"
drop "table trino_ns.items" \
    "$MERIDIAN_URL/iceberg/v1/spark_smoke/namespaces/trino_ns/tables/items?purgeRequested=true"
drop "namespace trino_ns" \
    "$MERIDIAN_URL/iceberg/v1/spark_smoke/namespaces/trino_ns"
