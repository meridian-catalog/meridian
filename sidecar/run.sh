#!/usr/bin/env bash
# Starts the Meridian transpilation sidecar (FastAPI + uvicorn).
#
# The Rust server spawns or connects to this over localhost. Port is
# configurable via MERIDIAN_SIDECAR_PORT (default 8200); host via
# MERIDIAN_SIDECAR_HOST (default 127.0.0.1 — localhost only by design).
#
# LLM-assist is OFF unless a BYO key is configured (see docs/design/
# transpilation.md). With no key, the fallback is a no-op and never touches a
# network.
set -euo pipefail

cd "$(dirname "$0")"

HOST="${MERIDIAN_SIDECAR_HOST:-127.0.0.1}"
PORT="${MERIDIAN_SIDECAR_PORT:-8200}"

uv sync --quiet
exec uv run uvicorn meridian_sidecar.app:app --host "$HOST" --port "$PORT" "$@"
