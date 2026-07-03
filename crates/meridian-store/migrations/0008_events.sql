-- Events (A-F6): outbox relay support, webhook endpoints + delivery
-- tracking, and named durable feed consumers.

-- Relay claim scans unpublished rows in id order (ULIDs are time-ordered,
-- so lexicographic id order is creation order), and the feed's publication
-- frontier is MIN(id) over unpublished rows.
CREATE INDEX events_outbox_unpublished_id_idx
    ON events_outbox (id)
    WHERE published_at IS NULL;

-- Per-aggregate ordering guard: "does this aggregate have an earlier
-- unpublished event?" must be cheap for every claim candidate.
CREATE INDEX events_outbox_unpublished_aggregate_idx
    ON events_outbox (aggregate, id)
    WHERE published_at IS NULL;

-- The queryable feed pages over published rows by id.
CREATE INDEX events_outbox_published_id_idx
    ON events_outbox (id)
    WHERE published_at IS NOT NULL;

-- A webhook endpoint: a URL that receives published events as CloudEvents
-- 1.0 JSON, HMAC-signed with the endpoint's secret. event_types is a list
-- of full CloudEvents type strings (e.g. 'com.meridian.table.committed');
-- an empty list subscribes to everything.
CREATE TABLE webhook_endpoints (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    url          TEXT NOT NULL,
    event_types  TEXT[] NOT NULL DEFAULT '{}',
    -- HMAC-SHA256 signing key. Stored as-is: Postgres is the trust root of
    -- the catalog; at-rest encryption is the database's/disk's job. Never
    -- returned by the API.
    secret       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, url)
);

-- One delivery per (endpoint, event): created by the relay when it
-- publishes an event matching the endpoint's filter, then driven to
-- 'delivered' or 'dead' by the webhook dispatcher with exponential
-- backoff. Durable, so deliveries survive restarts (at-least-once).
CREATE TABLE webhook_deliveries (
    endpoint_id     TEXT NOT NULL REFERENCES webhook_endpoints (id) ON DELETE CASCADE,
    event_id        TEXT NOT NULL REFERENCES events_outbox (id) ON DELETE RESTRICT,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivered', 'dead')),
    attempts        INT  NOT NULL DEFAULT 0,
    -- HTTP status of the most recent attempt; NULL when the attempt never
    -- got a response (connect error, timeout).
    last_status     SMALLINT,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (endpoint_id, event_id)
);

-- Dispatcher claim: due pending deliveries, oldest event first.
CREATE INDEX webhook_deliveries_due_idx
    ON webhook_deliveries (next_attempt_at, event_id)
    WHERE status = 'pending';

-- Dead-letter visibility: list an endpoint's deliveries by status.
CREATE INDEX webhook_deliveries_endpoint_status_idx
    ON webhook_deliveries (endpoint_id, status, event_id);

-- A named durable consumer of the event feed: a persistent cursor
-- (exclusive lower bound, an events_outbox id) advanced explicitly via
-- commit. NULL cursor = start of the feed.
CREATE TABLE event_consumers (
    workspace_id TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    name         TEXT NOT NULL,
    cursor       TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, name)
);
