#!/usr/bin/env bash
# Runs the full e2e suite against a Meridian server on localhost:8181.
# Each run uses a fresh run id, so warehouses/namespaces/buckets never
# collide with a previous run.
set -euo pipefail

cd "$(dirname "$0")"

BASE_URL="${MERIDIAN_URL:-http://localhost:8181}"

if ! curl -sf -o /dev/null "$BASE_URL/healthz"; then
    echo "error: no Meridian server at $BASE_URL" >&2
    echo "start one with:" >&2
    echo "  docker start meridian-dev-pg" >&2
    echo "  DATABASE_URL=postgres://meridian:meridian@localhost:5433/meridian \\" >&2
    echo "      cargo run -p meridian-cli -- serve" >&2
    exit 1
fi

export E2E_RUN_ID="${E2E_RUN_ID:-$(date +%s)$RANDOM}"
export MERIDIAN_URL="$BASE_URL"
echo "run id: $E2E_RUN_ID"

uv sync --quiet
uv run pytest tests/ -v "$@"
