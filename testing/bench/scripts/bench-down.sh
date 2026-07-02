#!/usr/bin/env bash
# Stop everything the bench scripts started. The shared dev Postgres and
# MinIO containers are left running.
set -uo pipefail

docker rm -f bench-polaris bench-lakekeeper 2>/dev/null

if [[ -f /tmp/meridian-bench-server.pid ]]; then
  kill "$(cat /tmp/meridian-bench-server.pid)" 2>/dev/null
  rm -f /tmp/meridian-bench-server.pid
  echo "stopped meridian serve"
fi
echo "done"
