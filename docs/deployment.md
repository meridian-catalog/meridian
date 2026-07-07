# Deploying Meridian in production

This guide covers running Meridian as a real service: turning authentication on,
sizing and backing up PostgreSQL, terminating TLS, health probes, upgrades, and
the configuration reference. For the local dev loop see [dev.md](dev.md); for the
honest per-feature status see [status.md](status.md).

Meridian is alpha. There are no tagged releases yet and APIs may change. Nothing
here is a substitute for testing against your own workload first.

## What you need

- **PostgreSQL 14+** — the only required dependency. All catalog state lives
  here. Size and back it up like the system of record it is (below).
- **Object storage** — S3 (or S3-compatible, e.g. MinIO). Your tables' data and
  metadata live in your bucket; Meridian never copies it. GCS and Azure work for
  table IO but credential *vending* for them is not implemented yet (they return
  a clear unsupported error) — see [status.md](status.md).
- **A reverse proxy / ingress that terminates TLS** (below).
- Optional: the **Python transpilation sidecar** (only if you use universal
  views / cross-dialect SQL); the server degrades gracefully without it.

## Running the server

The server is a single binary (`meridian serve`), also published as a container
image built from the repo `Dockerfile`. Configuration layers, lowest priority
first: built-in defaults < `meridian.toml` (or `--config <path>`) < `DATABASE_URL`
< `MERIDIAN__*` environment variables (double-underscore nests, e.g.
`MERIDIAN__SERVER__PORT=8181`, `MERIDIAN__AUTH__MODE=oidc`).

Migrations run automatically on startup (embedded in the binary), so a fresh
database is initialized on first boot and an upgraded binary applies any new
migrations when it starts (see **Upgrades**).

```sh
export DATABASE_URL=postgres://meridian:...@your-pg-host:5432/meridian
meridian serve            # binds MERIDIAN__SERVER__HOST:PORT (default 0.0.0.0:8181)
```

## Turn authentication ON (do this before exposing the port)

**Authentication is `disabled` by default** so the dev loop needs no identity
provider. In that mode there is no authentication and authorization is bypassed:
anyone who can reach the port has full access. **Never expose a `disabled`-mode
server.** Switch to OIDC before binding a reachable address:

```toml
[auth]
mode = "oidc"

# One or more trusted token issuers. A bearer token is accepted only if its
# `iss` matches an issuer_url here and it validates against that issuer's JWKS.
[[auth.oidc.issuers]]
issuer_url = "https://login.example.com/"
audience   = "meridian"          # required `aud` claim

# Authorization is deny-by-default, so bootstrap the first admin by identity.
# This grant is idempotent and re-applied on every startup.
[auth.bootstrap_admin]
issuer  = "https://login.example.com/"
subject = "the-oidc-sub-of-your-first-admin"
```

Everything else — roles, grants, service principals — is then managed through
the API/console by that admin. Issuers must be `https://` unless you explicitly
set `auth.oidc.require_https_issuers = false` (which logs a warning; only for a
local issuer in tests).

## TLS

The server speaks plain HTTP; it has **no built-in TLS listener**. Terminate TLS
at a reverse proxy or ingress (nginx, Envoy, an ALB, a Kubernetes ingress) and
forward to the server's HTTP port on a trusted network. This is the standard,
recommended topology and keeps certificate management out of the catalog.

If the console (a browser app on a different origin) calls the API, set
`server.cors_allowed_origins` to the console's origin(s).

## PostgreSQL: sizing, backup, restore

Meridian's durability *is* Postgres's durability. Treat the database as the
system of record.

- **Sizing.** The connection pool defaults to 20 connections
  (`database.max_connections`), shared by request handlers and the background
  workers. Keep Postgres's own `max_connections` comfortably above
  `max_connections × (number of Meridian replicas)`. Append-only tables (the
  audit log, the events outbox) grow with write volume — provision storage and
  monitor growth. (Idempotency receipts are swept automatically; a retention
  policy for the event-plane tables is tracked.)
- **Backup.** Use standard Postgres backup: continuous archiving / PITR
  (`pg_basebackup` + WAL archiving) for point-in-time recovery, or scheduled
  `pg_dump` for smaller estates. Because table data lives in object storage and
  is referenced by immutable metadata files, a catalog restored to an earlier
  point is internally consistent as long as the referenced metadata files still
  exist in the bucket — so pair the Postgres backup with object-storage
  lifecycle rules that do not delete metadata your backups may still reference.
- **Restore.** Restore Postgres by your normal procedure, then start the server
  against it; migrations reconcile the schema version on boot. No Meridian-side
  restore step is required.
- **The audit log is append-only** and protected by a database trigger that
  rejects UPDATE/DELETE. Do not disable it; `GET /api/v2/audit/verify`
  re-checks the hash chain.

## Health probes

- **`GET /healthz` — liveness.** Returns `200` whenever the process is serving.
  It reports database reachability in the body but never fails on it, because
  restarting the pod does not fix a database outage. Wire your orchestrator's
  **liveness** probe here.
- **`GET /readyz` — readiness.** Returns `503` when Postgres is unreachable, so
  the load balancer stops routing to a replica that cannot serve — without
  restarting it. Wire your **readiness** probe here.

## Upgrades

1. Read the [CHANGELOG](../CHANGELOG.md) for the target version.
2. **Back up Postgres first** (migrations are not automatically reversible).
3. Roll out the new binary/image. Each replica runs embedded migrations on
   startup; run migrations from a single replica first (or scale to one) if a
   release note flags a long or exclusive migration, then scale back up.
4. Migrations are forward-only. To roll back a release, restore the pre-upgrade
   Postgres backup and redeploy the previous binary. There is no automatic
   down-migration.

Until 1.0, treat every upgrade as potentially breaking and test it against a
copy of your data.

## Observability

- **Logs** are structured; set `telemetry.format = "json"` for machine-readable
  output and `telemetry.filter` (or the `RUST_LOG` env var) for levels. Secrets
  (storage credentials, share tokens) are redacted from logs.
- A Prometheus `/metrics` endpoint is **not yet implemented** — process metrics
  are a tracked gap. For now, scrape Postgres and your proxy, and alert on the
  `/readyz` probe and log error rates.

## Configuration reference

Every key is settable in `meridian.toml` or via `MERIDIAN__<SECTION>__<KEY>`
(uppercase, `__`-nested). Selected production-relevant keys:

| Key | Default | Purpose |
|---|---|---|
| `server.host` / `server.port` | `0.0.0.0` / `8181` | Bind address. |
| `server.request_timeout_secs` | 30 | Per-request timeout. |
| `server.max_body_bytes` | 16 MiB | Max request body size. |
| `server.cors_allowed_origins` | localhost dev ports | Browser origins for the console. `["*"]` to allow any; `[]` to disable CORS. |
| `database.url` | — | Postgres URL (or `DATABASE_URL`). |
| `database.max_connections` | 20 | Pool size (handlers + workers). |
| `database.min_connections` | 2 | Warm connections kept open. |
| `database.acquire_timeout_secs` | 5 | Fail-fast bound when the pool is saturated. |
| `database.idle_timeout_secs` | 600 | Recycle idle connections. |
| `database.max_lifetime_secs` | 1800 | Hard connection lifetime (clean recovery after a PG failover). |
| `auth.mode` | `disabled` | `disabled` or `oidc`. **Set `oidc` in production.** |
| `auth.oidc.issuers` | — | Trusted token issuers (`issuer_url`, `audience`). |
| `auth.bootstrap_admin` | — | Identity granted `admin` at startup. |
| `telemetry.format` | `pretty` | `pretty` or `json`. |
| `maintenance.enabled` | true | Run the autonomous maintenance workers. |
| `maintenance.job_lease_secs` | 1800 | Reclaim a maintenance job whose worker died past this lease. |
| `planning.enabled` | true | Server-side scan planning (else the endpoints 406). |

Per-warehouse **storage options** (set at warehouse creation) include object-store
retry and timeout tuning: `retry.timeout-ms` (default 30000, per non-streaming
op) and `retry.io-timeout-ms` (default 60000, per streaming chunk), so a hung
object store cannot stall commits indefinitely.

See [config.rs](../crates/meridian-common/src/config.rs) for the exhaustive,
always-current set with per-field documentation.
