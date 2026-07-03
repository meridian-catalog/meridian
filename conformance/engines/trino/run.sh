#!/usr/bin/env sh
# Trino smoke test against a Meridian server on localhost:8181 with
# MinIO on localhost:9000. See README.md for prerequisites — notably,
# the Spark smoke (../spark/run.sh) must have run first so that
# spark_ns.orders exists for the cross-engine steps.
#
# Unlike the Spark/Flink smokes, this container does NOT need host
# networking: Trino's native S3 file system takes its endpoint from the
# catalog properties (etc/mrd.properties) when
# iceberg.rest-catalog.vended-credentials-enabled=false, so the vended
# s3.endpoint (http://localhost:9000) never reaches the S3 client and
# host.docker.internal overrides work from the bridge network.
#
# Exits 0 only if every step of suite/suite.py verified. The container
# is always removed on exit; trino_ns is dropped by the suite itself.
set -eu

cd "$(dirname "$0")"

TRINO_IMAGE="${TRINO_IMAGE:-trinodb/trino:482}"
CONTAINER=meridian-trino-smoke

./setup.sh

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
trap 'docker rm -f "$CONTAINER" >/dev/null 2>&1 || true' EXIT INT TERM

echo "=== trino smoke (image: $TRINO_IMAGE) ==="
docker run -d --name "$CONTAINER" \
    --add-host host.docker.internal:host-gateway \
    -v "$(pwd)/etc/mrd.properties:/etc/trino/catalog/mrd.properties:ro" \
    "$TRINO_IMAGE" >/dev/null

python3 suite/suite.py

echo "trino smoke passed."
