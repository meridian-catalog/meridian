#!/usr/bin/env sh
# Spark smoke test against a Meridian server on localhost:8181 with MinIO
# on localhost:9000. See README.md for prerequisites (server + MinIO).
#
# The container runs with host networking for the same reason the Flink
# smoke does: Meridian vends the warehouse's s3.endpoint
# (http://localhost:9000) in LoadTableResult.config, and the Iceberg Java
# REST client merges the vended config OVER catalog-level client
# properties — a client-side s3.endpoint override does not stick. With
# host networking, localhost:9000 inside the container reaches MinIO.
# Splitting internal vs. external endpoint vending is a known M2 item.
#
# Exits 0 only if every step of suite/suite.py verified. The final table
# (spark_ns.orders, post-MERGE/DELETE) is left in place for inspection.
set -eu

cd "$(dirname "$0")"

SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.6-scala2.12-java17-python3-ubuntu}"

./fetch-jars.sh
./setup.sh

echo "=== spark smoke (image: $SPARK_IMAGE) ==="
docker run --rm --network host \
    --name meridian-spark-smoke \
    -v "$(pwd)/jars:/opt/iceberg:ro" \
    -v "$(pwd)/suite:/opt/suite:ro" \
    "$SPARK_IMAGE" \
    /opt/spark/bin/spark-submit \
    --master 'local[2]' \
    --jars /opt/iceberg/iceberg-spark-runtime-3.5_2.12-1.11.0.jar,/opt/iceberg/iceberg-aws-bundle-1.11.0.jar \
    --conf spark.ui.enabled=false \
    /opt/suite/suite.py

echo "spark smoke passed."
