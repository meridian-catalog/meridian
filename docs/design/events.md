# Events: outbox relay, webhooks, and the queryable feed

Every catalog mutation in Meridian already writes an event into the
`events_outbox` table **in the same database transaction** as the state
change and its audit row (the transactional-outbox pattern; see
`meridian-store/src/outbox.rs`). This document describes the delivery side:
how those rows become consumable events, and the guarantees consumers can
rely on.

Status: implemented and tested against a local Postgres. Aligned in spirit
with the in-review upstream Iceberg REST events proposal (CloudEvents
envelope, catalog-mutation event types); Meridian will track the upstream
schema as it stabilizes.

## Model

```
mutation tx:   [ state change | audit row | outbox row ]   -- atomic
                                              │
relay (loop):  claim batch (SKIP LOCKED, per-aggregate ordered)
               ├─ fan out webhook_deliveries rows (filtered per endpoint)
               └─ set published_at                          -- one tx
                                              │
              ┌───────────────────────────────┴───────────────┐
webhook       │                                    queryable feed
dispatcher:   POST CloudEvents JSON,               GET /api/v2/events
              HMAC-signed, retries with            keyset cursor = event id;
              backoff, dead-letters                named durable consumers
```

### Event envelope

Events are rendered as [CloudEvents 1.0](https://cloudevents.io/) JSON
(structured content mode, `application/cloudevents+json`):

```json
{
  "specversion": "1.0",
  "id": "01JZBTZ4Q0V7WQF2M6E3T1H9RD",
  "source": "meridian/00000000000000000000000001",
  "type": "com.meridian.table.committed",
  "subject": "table:01JZBTZ36GJRD0PVV5XVWD2R4B",
  "time": "2026-07-03T10:15:23.412345Z",
  "datacontenttype": "application/json",
  "data": { "...": "payload written by the emitting module" }
}
```

- `id` — the outbox row's ULID. Time-ordered, unique, and **doubles as the
  feed cursor**.
- `source` — `meridian/<workspace-id>`, or `meridian` for org-level events.
- `type` — `com.meridian.` + the internal event type. Filters (webhook
  `event_types`, the feed's `types` parameter) always use this full form.
- `subject` — the aggregate the event is about (`table:<id>`,
  `namespace:<id>`, ...). Per-aggregate ordering guarantees are scoped to
  this value.
- `time` — when the mutation committed.

### Event type catalog

| Type (`com.meridian.` +) | Emitted on |
|---|---|
| `warehouse.created` / `warehouse.deleted` | warehouse management |
| `namespace.created` / `namespace.deleted` / `namespace.properties_updated` | namespace lifecycle |
| `table.created` / `table.committed` / `table.renamed` / `table.dropped` / `table.purge_requested` | table lifecycle and commits |
| `view.created` / `view.committed` / `view.renamed` / `view.dropped` | view lifecycle |
| `role.created` / `role.deleted` / `role.binding.created` / `role.binding.deleted` | RBAC roles |
| `grant.created` / `grant.deleted` | RBAC grants |
| `principal.provisioned` | OIDC JIT provisioning |
| `webhook.created` / `webhook.deleted` | webhook endpoint management |
| `event_consumer.created` / `event_consumer.deleted` | durable consumer management |

New mutations must enqueue an outbox event (and an audit row) on their own
transaction; the type list above grows with the API surface.

## The relay

A background task inside `meridian serve` (no extra process; see
`meridian-server/src/events.rs` and `meridian_store::outbox::relay_once`).
One iteration, all in a single transaction:

1. **Claim** up to `events.relay_batch_size` unpublished rows in id order
   with `FOR UPDATE SKIP LOCKED` — concurrent relays (several server
   replicas) never double-publish and never block each other.
2. **Ordering guard:** drop any claimed row whose aggregate still has an
   *earlier* unpublished row outside this claim (typically held by another
   relay's open transaction). Publication order per aggregate is therefore
   strict even with concurrent relays; dropped rows are picked up by a
   later batch.
3. **Fan out**: insert one `webhook_deliveries` row per (matching endpoint,
   event). `ON CONFLICT DO NOTHING` keeps crash-replays idempotent.
4. **Mark published** (`published_at = now()`) and commit.

A crash anywhere before the commit re-publishes the whole batch later:
delivery is **at-least-once** end to end, and outbox rows are never lost
(they were durable before the relay ever saw them).

**Backlog / first boot:** the relay loops without sleeping while full
batches keep coming, so a large pre-existing backlog (the dev database had
~16,700 unpublished rows when this feature landed) drains in bounded
batches — no giant transaction, no thundering herd. It sleeps
`events.relay_poll_ms` once a partial batch signals it has caught up, and
backs off exponentially (1s → 30s) on database errors.

### The publication frontier (why the feed never skips events)

Marking rows published in claim order is not enough for a keyset-paginated
feed: with two concurrent relays, a batch with *higher* ids can commit
before a batch with *lower* ids, and a reader that already saw the higher
ids would then miss the lower ones forever.

The feed therefore only serves events **below the frontier**: `MIN(id)`
over unpublished rows, evaluated in the same statement as the page read.
Rows claimed by an in-flight relay are still unpublished in every other
snapshot, so nothing above them is served until they commit. The result:
the feed is gap-free and totally ordered by id, at the cost of a small
publication lag while a batch is in flight. (A permanently unpublishable
row would stall the feed — but publish failures are database errors, which
the relay retries forever; there is no poison-row path.)

## Webhooks

Management API (management access required — see [Authorization](#authorization-rbac-posture)):

| Endpoint | Semantics |
|---|---|
| `POST /api/v2/webhooks` `{url, event_types?, secret}` | Register an endpoint. `event_types` = full CloudEvents types, empty/omitted = all events; `secret` (min 16 chars) is write-only. 409 on duplicate URL. |
| `GET /api/v2/webhooks` / `GET /api/v2/webhooks/{id}` | List / load (never returns the secret). |
| `DELETE /api/v2/webhooks/{id}` | Delete endpoint + its delivery history. |
| `GET /api/v2/webhooks/{id}/deliveries?status=pending\|delivered\|dead&limit=` | Delivery history: per delivery `event_id`, `event_type`, `status`, `attempts`, `last_status` (HTTP), `last_error`, `next_attempt_at`. Dead-lettered deliveries are `status=dead`. |

The dispatcher (second background task) claims due deliveries with a lease
(`SKIP LOCKED`, `attempts + 1`, `next_attempt_at` pushed out), performs the
HTTP POST *outside* any transaction, and records the outcome:

- **2xx** → `delivered`.
- Anything else (non-2xx, connect error, timeout — per-request timeout
  `events.webhook_timeout_secs`, default 10s) → retry with exponential
  backoff: `webhook_retry_base_secs × 2^(attempt-1)` (default 10s base),
  capped at 15 minutes, until `events.webhook_max_attempts` (default 10,
  ≈ a day of retries), then **dead-letter** (`status=dead`, visible via the
  deliveries endpoint above; the delivery is kept, never silently dropped).
- A dispatcher crash mid-attempt leaves the row pending; the lease expiry
  makes it due again. At-least-once — receivers must de-duplicate by event
  `id`.

**Ordering:** delivery rows are *created* in strict per-aggregate order,
and dispatched oldest-event-first, but retries mean a failing event does
not block newer events to the same endpoint — so cross-event delivery
order is best-effort (standard webhook semantics). Consumers that need
strict order should read the feed.

### Verifying webhook signatures

Every delivery carries:

```
content-type:          application/cloudevents+json
x-meridian-event-id:   01JZBTZ4Q0V7WQF2M6E3T1H9RD
x-meridian-event-type: com.meridian.table.committed
x-meridian-timestamp:  1783765382            (unix seconds, at send time)
x-meridian-signature:  v1=hex(HMAC-SHA256(secret, "<timestamp>.<raw body>"))
```

To verify: concatenate the `x-meridian-timestamp` value, a literal `.`, and
the **raw request body bytes**; compute HMAC-SHA256 with your endpoint's
secret; hex-encode; constant-time-compare against the header after `v1=`.
Reject requests whose timestamp is outside your tolerance window (e.g.
5 minutes) to bound replays. Python reference:

```python
import hashlib, hmac

def verify(secret: str, timestamp: str, body: bytes, header: str) -> bool:
    digest = hmac.new(secret.encode(), f"{timestamp}.".encode() + body,
                      hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"v1={digest}", header)
```

The secret is stored as-is in Postgres (the catalog's trust root, same
posture as warehouse storage options); it never appears in API responses,
audit entries, or events.

## The queryable feed and durable consumers

| Endpoint | Semantics |
|---|---|
| `GET /api/v2/events?after=<cursor>&types=<t1,t2>&limit=` | Keyset page of published events (CloudEvents JSON), id order. `after` omitted = from the beginning; `after=latest` = only events published after this request. Response: `{events, next_cursor}`; poll with `after=next_cursor`. |
| `POST /api/v2/events/consumers` `{name}` | Create a named durable consumer (cursor starts at the beginning of the feed). |
| `GET /api/v2/events/consumers` | List consumers with their committed cursors. |
| `GET /api/v2/events/consumers/{name}/next?types=&limit=` | The batch after the consumer's committed cursor. **Does not advance the cursor** — repeated calls return the same batch. |
| `POST /api/v2/events/consumers/{name}/commit` `{cursor}` | Persist the cursor (use `next_cursor` from `next`). Idempotent for the same cursor; moving backwards → 409. |
| `DELETE /api/v2/events/consumers/{name}` | Delete the consumer. |

Consumer processing is at-least-once by construction: read `next`, process,
`commit`, repeat; a crash between processing and commit re-serves the batch.
One cursor per name — run one worker per consumer, or create several
consumers with distinct type filters for parallelism.

Cursor commits deliberately do **not** write audit or outbox rows: an
offset advance is consumption bookkeeping, not a catalog mutation — and a
commit that emitted an event would make any subscribed-to-everything
consumer feed itself forever. Consumer/webhook create & delete are catalog
mutations and are audited (with outbox events) as usual.

`meridian events tail` follows the feed from the CLI:

```
meridian events tail                          # follow new events (like tail -f)
meridian events tail --from-start             # replay the whole feed first
meridian events tail --after <cursor>         # resume from a cursor
meridian events tail --types com.meridian.table.committed
```

One compact CloudEvents JSON object per line; stop with ctrl-c.

## Authorization (RBAC posture)

All events endpoints require **management access** (the built-in `admin`
role or any `MANAGE_WAREHOUSE` grant), like the rest of `/api/v2`. The
feed spans events about every resource in the workspace, so none of the
existing resource-scoped privileges can express "may read events" without
over- or under-granting; rather than mint a `READ_EVENTS` privilege
prematurely, reads are management-level for now. Revisit when there is a
concrete consumer persona that must not hold management rights. (In
`auth.mode = "disabled"` everything is open — see the warning in
[api-status.md](../api-status.md).)

Webhook URLs are operator-configured (management-only) and fetched from
inside the deployment, which is the usual webhook SSRF posture; deployers
who let untrusted principals manage webhooks should put egress policy in
front.

## Configuration

```toml
[events]
relay_batch_size        = 500    # outbox rows per relay transaction
relay_poll_ms           = 1000   # relay sleep once caught up
webhook_poll_ms         = 1000   # dispatcher sleep when nothing is due
webhook_timeout_secs    = 10     # per-delivery HTTP timeout
webhook_max_attempts    = 10     # then dead-letter
webhook_retry_base_secs = 10     # backoff base (exponential, 15 min cap)
```

## Limitations (tracked, honest)

- **No broker sinks yet.** The spec calls for NATS/Kafka sinks; today the
  sinks are webhooks and the queryable feed. The relay's fan-out point
  (`enqueue_deliveries`) is where a broker sink plugs in.
- **No webhook endpoint update.** Create/delete only; rotate a secret by
  creating a replacement endpoint and deleting the old one.
- **No outbox retention.** Published rows are kept indefinitely (they are
  the feed's history). A retention/compaction job is future work; the
  feed's frontier design does not depend on retention.
- **Webhook cross-event ordering is best-effort** (see above); the feed is
  the strictly-ordered surface.
- **`purge_requested` data-file deletion** remains the maintenance
  worker's job (divergence (e) in api-status.md); the event only signals
  the request.
