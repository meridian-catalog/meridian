#!/usr/bin/env bash
# Scan-planning perf smoke: warm plan p95 < 150 ms on a 10,000-file
# table, asserted by the in-process test
# crates/meridian-server/tests/planning_perf.rs.
#
# Runs in release mode (the smoke is meaningless in debug) against the
# shared dev Postgres. Requires DATABASE_URL (defaults to the dev
# database used across the repo).
#
# Usage: plan-perf.sh
set -euo pipefail
cd "$(dirname "$0")/../../.."

export DATABASE_URL="${DATABASE_URL:-postgres://meridian:meridian@localhost:5433/meridian}"

cargo test --release -p meridian-server --test planning_perf -- --ignored --nocapture
