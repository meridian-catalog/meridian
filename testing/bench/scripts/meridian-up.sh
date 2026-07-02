#!/usr/bin/env bash
# Boot Meridian (release build, auth disabled) against the shared dev
# Postgres + MinIO, ready to bench.
#
# Creates (idempotently): bucket bench-meridian and warehouse bench_meridian.
# The server runs natively on the host (see the benchmark caveats in
# docs/benchmarks/ — the competitors run inside Docker Desktop).
#
# Usage: meridian-up.sh          # builds, starts, creates warehouse
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=env.sh
source ./env.sh
REPO_ROOT="$(cd ../../.. && pwd)"

DATABASE_URL="${DATABASE_URL:-postgres://${PG_USER}:${PG_PASSWORD}@localhost:${PG_HOST_PORT}/meridian}"
export DATABASE_URL

ensure_bucket bench-meridian

(cd "$REPO_ROOT" && cargo build --release -q -p meridian-cli)

if curl -sf http://localhost:8181/healthz >/dev/null 2>&1; then
  echo "a server is already listening on 8181; stop it first" >&2
  exit 1
fi

nohup "$REPO_ROOT/target/release/meridian" serve \
  > "${MERIDIAN_LOG:-/tmp/meridian-bench-server.log}" 2>&1 &
echo $! > /tmp/meridian-bench-server.pid
echo "meridian serve started (pid $(cat /tmp/meridian-bench-server.pid))"

for _ in $(seq 1 30); do
  curl -sf http://localhost:8181/healthz >/dev/null 2>&1 && break
  sleep 1
done
curl -sf http://localhost:8181/healthz >/dev/null \
  || { echo "Meridian did not become healthy"; tail -20 /tmp/meridian-bench-server.log; exit 1; }

curl -s -o /dev/null -w "create warehouse: %{http_code}\n" \
  -H 'Content-Type: application/json' \
  http://localhost:8181/api/v2/warehouses -d '{
  "name": "bench_meridian",
  "storage_root": "s3://bench-meridian/warehouse",
  "storage_options": {
    "endpoint": "'"$MINIO_ENDPOINT_HOST"'",
    "path-style": "true",
    "region": "us-east-1",
    "access-key-id": "'"$MINIO_ACCESS_KEY"'",
    "secret-access-key": "'"$MINIO_SECRET_KEY"'"
  }
}'

echo "Meridian ready: http://localhost:8181/iceberg (warehouse bench_meridian)"
