-- 0020_agents: the MCP agent gateway data model (Pillar H, H-F1/H-F2/H-F4 —
-- the agent firewall). Agents are first-class principals (kind='agent', already
-- in migration 0004's principals CHECK) with a governance envelope of their
-- own: an owner, a purpose statement, an environment, a lifecycle (expiry +
-- review date), a kill switch, per-agent budgets with rolling window counters,
-- and a per-tool-call activity ledger.
--
-- Append-only: this file only adds to the 0001-0019 schema; nothing is
-- rewritten. It does NOT build the query executor (the sibling DataFusion
-- executor is wave 2) — it owns the *definitions* (agent registration, budgets)
-- and the *evidence* (the activity ledger). The MCP endpoint + governance
-- wrapper (crates/meridian-server routes/mcp + the meridian-agents crate)
-- consume these; the full audit chain still lives in audit_log (0001) — every
-- MCP tool call writes an agent_activity row AND an audit_log row on the same
-- transaction, so the tamper-evident chain covers agent actions (H-F4).
--
-- Model:
--
--  * agent_principals: the agent's governance envelope, 1:1 with a
--    principals row of kind='agent'. principal_id is a real FK (agents ARE
--    principals; deleting the principal removes the envelope). owner is the
--    audit string of the human/service accountable for the agent. purpose is
--    the free-text purpose statement (H-F1) that purpose-conditioned policies
--    consult. environment is dev | prod. expires_at / review_at are the
--    lifecycle dates (an expired agent is refused; review_at is advisory, for
--    the recertification campaign). enabled is the KILL SWITCH: a disabled
--    agent has every tool refused (H-F4), independent of grants.
--
--  * agent_budgets: per-agent caps + rolling-window counters, 1:1 with an
--    agent (agent_id FKs agent_principals.principal_id). Limits: queries per
--    hour, scanned bytes per day, and a dollar-estimate cap per day. NULL on a
--    limit means "no cap for this dimension". The counters are maintained by
--    the gateway: each window has a start timestamp and an accumulator; when
--    the current time rolls past (window_start + window), the accumulator
--    resets. Keeping counters in the row (rather than deriving from
--    agent_activity every call) makes enforcement one indexed read + one
--    conditional update, and the ledger remains the audit-grade source of
--    truth if the counters ever need rebuilding.
--
--  * agent_activity: the append-only per-tool-call ledger — the "which agent
--    read which columns for which purpose" answer CISOs currently cannot
--    produce (the 16% stat). One row per MCP tool call: the tool name, a
--    stable digest of the (redacted) arguments, the governance decision
--    (allowed | denied | refused_budget | refused_killed | error), the rows /
--    bytes / cost the call touched (0 for a refusal or a read tool), the
--    resolved purpose, and the audit_log seq this call's chain entry landed at
--    (so the two are cross-referenceable). This table is queried for the
--    per-agent activity view and the anomaly hooks (novel-table access,
--    off-hours); it is never mutated after insert.

-- ----------------------------------------------------------------------------
-- agent_principals: the per-agent governance envelope (1:1 with a principal).

CREATE TABLE agent_principals (
    -- The agent's principal id (kind='agent'). PK and FK: the envelope IS the
    -- agent, keyed by its principal identity. ON DELETE CASCADE so removing
    -- the principal removes its envelope, budgets, and (below) leaves the
    -- append-only activity ledger intact by its own FK choice.
    principal_id  TEXT PRIMARY KEY REFERENCES principals (id) ON DELETE CASCADE,
    workspace_id  TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- Audit string of the human/service accountable for this agent
    -- (e.g. user:alice@example.com). Not an FK: owners are external identities
    -- rendered as audit strings, exactly like audit_log.principal.
    owner         TEXT NOT NULL,
    -- The agent's declared purpose statement (H-F1). Purpose-conditioned
    -- policies (D-F1) consult this when the caller does not override it per
    -- request.
    purpose       TEXT NOT NULL,
    -- Deployment environment: dev or prod. Governs nothing on its own here but
    -- is a first-class lifecycle attribute (H-F1) and an anomaly signal.
    environment   TEXT NOT NULL DEFAULT 'dev'
        CHECK (environment IN ('dev', 'prod')),
    -- Lifecycle (H-F1). expires_at: after this instant the agent is refused
    -- (a hard stop). review_at: advisory recertification date (a soft nudge;
    -- surfaced in analytics, not enforced). Both nullable (no deadline set).
    expires_at    TIMESTAMPTZ,
    review_at     TIMESTAMPTZ,
    -- The KILL SWITCH (H-F4): FALSE refuses every tool call for this agent,
    -- independent of grants/budgets. Default TRUE (a freshly registered agent
    -- is live). Auto-suspend (anomaly hooks) flips this to FALSE.
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX agent_principals_workspace_idx ON agent_principals (workspace_id);
CREATE INDEX agent_principals_owner_idx ON agent_principals (owner);

-- ----------------------------------------------------------------------------
-- agent_budgets: per-agent caps + rolling-window counters (1:1 with an agent).
--
-- A NULL limit means that dimension is uncapped. The window counters roll:
-- queries_window_start anchors the per-hour queries window; scanned/cost share
-- the per-day window anchored by day_window_start. The gateway resets an
-- accumulator when now() passes the window end, then applies the increment.

CREATE TABLE agent_budgets (
    agent_id             TEXT PRIMARY KEY
        REFERENCES agent_principals (principal_id) ON DELETE CASCADE,
    -- Caps (NULL = uncapped for that dimension).
    queries_per_hour     BIGINT
        CHECK (queries_per_hour IS NULL OR queries_per_hour >= 0),
    scanned_bytes_per_day BIGINT
        CHECK (scanned_bytes_per_day IS NULL OR scanned_bytes_per_day >= 0),
    -- Dollar-estimate cap per day, in micro-dollars (1e-6 USD) to stay integer
    -- and exact — a cost estimate is never a float we compare for equality.
    dollar_cap_micros    BIGINT
        CHECK (dollar_cap_micros IS NULL OR dollar_cap_micros >= 0),
    -- Rolling per-hour queries window.
    queries_window_start TIMESTAMPTZ NOT NULL DEFAULT now(),
    queries_in_window    BIGINT NOT NULL DEFAULT 0 CHECK (queries_in_window >= 0),
    -- Rolling per-day window shared by scanned-bytes and dollar-estimate.
    day_window_start     TIMESTAMPTZ NOT NULL DEFAULT now(),
    scanned_bytes_in_day BIGINT NOT NULL DEFAULT 0 CHECK (scanned_bytes_in_day >= 0),
    cost_micros_in_day   BIGINT NOT NULL DEFAULT 0 CHECK (cost_micros_in_day >= 0),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- agent_activity: the append-only per-tool-call ledger (the audit-chain
-- companion). Never mutated after insert.
--
-- agent_id references the principal, but ON DELETE SET NULL (not CASCADE):
-- deleting an agent must NOT erase its history — evidence outlives the agent.
-- The row keeps enough identity (its own workspace + the audit seq) to remain
-- meaningful after the agent row is gone.

CREATE TABLE agent_activity (
    id            TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- The acting agent's principal id; SET NULL on agent deletion (keep the
    -- evidence). Also carry the audit string so a null'd row is still readable.
    agent_id      TEXT REFERENCES agent_principals (principal_id) ON DELETE SET NULL,
    agent_audit   TEXT NOT NULL,
    -- The MCP tool invoked (e.g. get_table_context, run_sql).
    tool          TEXT NOT NULL,
    -- A stable digest (sha256 hex) of the redacted call arguments — enough to
    -- correlate repeated calls and prove what was asked without storing raw,
    -- possibly-sensitive argument values.
    args_digest   TEXT NOT NULL,
    -- The governance decision for this call.
    decision      TEXT NOT NULL
        CHECK (decision IN ('allowed', 'denied', 'refused_budget',
                            'refused_killed', 'refused_expired', 'error')),
    -- The resolved purpose for this call (per-call override or the agent's
    -- registered purpose); NULL when none applied.
    purpose       TEXT,
    -- What the call touched. 0 for a refusal or a pure metadata read. rows is
    -- NULL when not applicable (a context tool); bytes/cost default 0.
    rows_touched  BIGINT CHECK (rows_touched IS NULL OR rows_touched >= 0),
    bytes_scanned BIGINT NOT NULL DEFAULT 0 CHECK (bytes_scanned >= 0),
    cost_micros   BIGINT NOT NULL DEFAULT 0 CHECK (cost_micros >= 0),
    -- The audit_log.seq of the tamper-evident chain entry written for this same
    -- call (cross-reference between the ledger and the hash chain). NULL only
    -- if the chain write is deferred; the gateway always writes both together.
    audit_seq     BIGINT,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The per-agent activity view reads newest-first by agent; the anomaly hooks
-- scan a recent window per agent. Index both the agent timeline and the
-- workspace timeline.
CREATE INDEX agent_activity_agent_time_idx
    ON agent_activity (agent_id, occurred_at DESC);
CREATE INDEX agent_activity_workspace_time_idx
    ON agent_activity (workspace_id, occurred_at DESC);
CREATE INDEX agent_activity_tool_idx ON agent_activity (tool);
