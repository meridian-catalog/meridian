-- Composite indexes for the audit query API (GET /api/v2/audit).
--
-- The query filters audit_log by (workspace_id + one of principal / action /
-- resource), optionally by an occurred_at range, and always returns the newest
-- first (ORDER BY seq DESC LIMIT n). With only the single-column workspace_id
-- and occurred_at indexes (0001_init), a filtered query on a large, append-only
-- audit log degrades to a scan + sort. These composites serve the filter and
-- the seq-DESC ordering from one index, so an auditor's "what did principal X
-- do" / "history of resource Z" query stays fast as the log grows.
--
-- seq is BIGINT GENERATED ALWAYS AS IDENTITY (monotonic), so DESC on it is the
-- newest-first order the API returns. The write cost (three more index updates
-- per append) is secondary to the existing per-append audit advisory lock.

CREATE INDEX audit_log_workspace_principal_seq_idx
    ON audit_log (workspace_id, principal, seq DESC);

CREATE INDEX audit_log_workspace_action_seq_idx
    ON audit_log (workspace_id, action, seq DESC);

CREATE INDEX audit_log_workspace_resource_seq_idx
    ON audit_log (workspace_id, resource, seq DESC);
