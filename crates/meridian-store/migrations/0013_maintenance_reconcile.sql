-- 0013_maintenance_reconcile: per-table reconciliation state for the
-- desired-state loop (Pillar C-F3) and streaming-aware coalescing.
--
-- Append-only: this file only adds to the 0001-0012 schema. The
-- reconciliation loop (in meridian-server's maintenance worker) evaluates
-- enabled policies against computed health and enqueues maintenance jobs for
-- tables that violate their targets. It must NOT re-enqueue a table it just
-- acted on (debounce), and it must NOT compact a table that is committing
-- every few seconds (streaming-aware coalescing: yield to the writer, let the
-- commit storm settle first). Both need a small amount of durable per-table
-- state that survives restarts and is shared across worker pods; that is what
-- this table holds. It is pure control-plane bookkeeping — it never touches a
-- table pointer and is not itself a maintenance write path.
--
-- One row per (workspace, table). table_id is a FK with CASCADE: the debounce
-- state is meaningless once the table is gone.

CREATE TABLE maintenance_reconcile_state (
    table_id             TEXT PRIMARY KEY REFERENCES tables (id) ON DELETE CASCADE,
    workspace_id         TEXT NOT NULL REFERENCES workspaces (id) ON DELETE RESTRICT,
    -- The last time the reconciliation loop evaluated this table (whether or
    -- not it enqueued anything). Lets the loop skip tables it looked at very
    -- recently and spread evaluation cost.
    last_evaluated_at    TIMESTAMPTZ,
    -- The last time the loop enqueued a maintenance job for this table. The
    -- debounce window is measured from here: the loop will not enqueue again
    -- until this is older than the configured debounce interval, so one
    -- unhealthy table cannot flood the queue between health recomputations.
    last_enqueued_at     TIMESTAMPTZ,
    -- The table's newest snapshot timestamp (epoch millis) observed at the
    -- previous evaluation, and the wall-clock instant of that observation.
    -- Together they let the loop estimate a table's commit rate WITHOUT a
    -- second write path: (current newest-snapshot age) vs (last observed) over
    -- the elapsed wall time approximates commits/sec. A table whose newest
    -- snapshot advanced within the coalescing window since we last looked is
    -- "actively committing" and is skipped (compacting it would immediately
    -- lose the optimistic-commit race to the writer anyway — spec C-F3
    -- streaming-aware mode / commit-storm coalescing).
    last_snapshot_ms     BIGINT,
    last_snapshot_seen_at TIMESTAMPTZ,
    -- The job the loop most recently enqueued for this table, if any (audit
    -- convenience; SET NULL semantics are unnecessary because it is not a FK —
    -- a job id whose row was pruned simply reads as historical).
    last_job_id          TEXT,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX maintenance_reconcile_state_workspace_idx
    ON maintenance_reconcile_state (workspace_id);
