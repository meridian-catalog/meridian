# 2026-07-03 — initial local benchmark

First catalog-plane latency comparison: **Meridian** (unreleased `0.1.0`
dev build) vs **Apache Polaris 1.5.0** vs **Lakekeeper v0.13.1**, run with
the harness in [`testing/bench/`](../../testing/bench/).

> **These are local development numbers from a laptop.** They are not
> cloud or production performance claims. Read the [caveats](#caveats)
> before quoting anything.

## Setup

| | |
|---|---|
| Hardware | Apple M3 Pro, 12 cores, 36 GB RAM (macOS 15.5) |
| Meridian | release build, run as a native process, auth disabled |
| Polaris | `apache/polaris:1.5.0` (`sha256:03a04f04…`), Docker, OAuth2 client-credentials (Polaris cannot run without auth; the token is fetched once, outside the timed path) |
| Lakekeeper | `quay.io/lakekeeper/catalog:v0.13.1` (`sha256:33094292…`), Docker, auth disabled |
| Shared infra | one Postgres 16 container (a database per catalog), one MinIO container (a bucket per catalog) |
| Container caps | competitor containers capped at 4 CPUs / 4 GB (`FAIR_LIMITS`) |
| Fixture | one namespace, one 40-column table with 20 append snapshots (see [`testing/bench/`](../../testing/bench/)) |

One catalog ran at a time. Per the protocol in
[README.md](README.md#reproducing), each catalog was run twice
back-to-back from identical fixture state and the better run (lower p50 on
the majority of scenarios) is published below; run-to-run deltas were
small (Meridian and Polaris: run 2 better on every scenario; Lakekeeper:
run 2 better on 3 of 5).

## Results

Latencies in milliseconds; `req/s` is successful requests over the
measured window. All runs completed with **zero errors**.

### Meridian (run 2 of 2)

| scenario | concurrency | requests | p50 | p95 | p99 | max | req/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| get-config | 1 | 2000 | 0.59 | 1.39 | 2.13 | 5.41 | 1452 |
| load-table | 1 | 2000 | 2.76 | 3.94 | 6.05 | 10.10 | 352 |
| load-table | 8 | 2000 | 4.71 | 6.56 | 8.10 | 11.73 | 1647 |
| load-table | 32 | 2000 | 17.58 | 20.34 | 22.30 | 23.38 | 1787 |
| commit | 1 | 200 | 10.28 | 12.54 | 15.23 | 15.80 | 95 |

### Apache Polaris 1.5.0 (run 2 of 2)

| scenario | concurrency | requests | p50 | p95 | p99 | max | req/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| get-config | 1 | 2000 | 2.83 | 3.26 | 4.15 | 10.19 | 350 |
| load-table | 1 | 2000 | 4.88 | 6.81 | 9.90 | 107.58 | 190 |
| load-table | 8 | 2000 | 9.18 | 12.19 | 14.41 | 72.51 | 825 |
| load-table | 32 | 2000 | 22.34 | 31.31 | 36.48 | 46.27 | 1375 |
| commit | 1 | 200 | 10.50 | 12.54 | 16.80 | 17.31 | 93 |

### Lakekeeper v0.13.1 (run 2 of 2)

| scenario | concurrency | requests | p50 | p95 | p99 | max | req/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| get-config | 1 | 2000 | 0.29 | 0.41 | 0.51 | 25.42 | 3040 |
| load-table | 1 | 2000 | 2.99 | 3.66 | 4.28 | 9.42 | 328 |
| load-table | 8 | 2000 | 6.00 | 7.74 | 8.94 | 12.04 | 1302 |
| load-table | 32 | 2000 | 23.68 | 26.78 | 28.89 | 32.26 | 1332 |
| commit | 1 | 200 | 8.05 | 9.30 | 10.62 | 13.75 | 129 |

### Side by side (p50, ms)

| scenario | Meridian | Polaris 1.5.0 | Lakekeeper v0.13.1 |
|---|---:|---:|---:|
| get-config (c=1) | 0.59 | 2.83 | **0.29** |
| load-table (c=1) | **2.76** | 4.88 | 2.99 |
| load-table (c=8) | **4.71** | 9.18 | 6.00 |
| load-table (c=32) | **17.58** | 22.34 | 23.68 |
| commit (c=1) | 10.28 | 10.50 | **8.05** |

## Reading the numbers

- **Meridian and Lakekeeper are in the same class** on this workload;
  Lakekeeper is faster on `get-config` and `commit`, Meridian is faster
  on `load-table` at every concurrency level.
- **Meridian's commit path is its slowest scenario relative to
  Lakekeeper** (10.28 ms vs 8.05 ms p50) — a known area to profile
  (RBAC/audit/outbox work happens inside the commit transaction).
- **Polaris trails on every scenario here**, but see the caveats: it is
  the only catalog paying per-request bearer-token processing, and it runs
  a JVM in Docker against natively-run Meridian.

## Caveats

Do not quote these numbers without this list.

1. **Laptop, loopback, one machine.** Client, catalogs, Postgres, and
   MinIO all shared one machine. No network hop, no cloud storage, thermal
   and background-process noise apply.
2. **Native-vs-Docker asymmetry.** Meridian ran as a native release
   binary; Polaris and Lakekeeper ran in Docker Desktop's VM (with 4
   CPU / 4 GB caps). This favors Meridian by some margin that was not
   measured.
3. **Auth asymmetry.** Meridian and Lakekeeper ran with auth disabled;
   Polaris requires OAuth2 and validates a bearer token on every request.
   Token *acquisition* is excluded from the timed path, per-request
   validation is not.
4. **Catalog plane only.** No query engines, no data files, no
   object-store I/O in the timed path; the fixture's snapshots are
   metadata-level. Real workloads are dominated by other costs.
5. **Short runs.** 2000 measured requests per scenario (200 for commits)
   after warm-up; JVM-based Polaris in particular may profile differently
   over longer windows.
6. **One JVM warm-up nuance:** the published Polaris run is the second of
   two back-to-back runs, which was faster across the board — consistent
   with JIT warm-up. The same better-of-two rule was applied to all three
   catalogs.

Raw harness output (JSON + markdown, both runs, all catalogs) is not
checked in; re-run per [README.md](README.md#reproducing) to reproduce.
