-- A visibility delay for re-queued maintenance jobs (fixes a requeue hot loop).
--
-- When a maintenance job yields to a concurrent writer commit (optimistic-CAS
-- loss) or errors with retries left, the server re-queues it. Before this, the
-- re-queue kept the job's original created_at, so it went straight back to the
-- head of the queue and was re-claimed immediately — a table that is
-- perpetually busy would spin the worker (claim -> yield -> requeue -> claim)
-- with no progress and starve other tenants' jobs.
--
-- run_after gives a re-queued job a not-before time: the claim skips a job whose
-- run_after is still in the future, so the worker backs off (and sleeps when the
-- rest of the queue is empty) instead of hot-looping. NULL = immediately
-- claimable (the default for freshly enqueued jobs).

ALTER TABLE maintenance_jobs ADD COLUMN run_after TIMESTAMPTZ;

-- Re-create the queued-claim partial index to include run_after, so the claim's
-- "queued AND due" scan stays tight even with delayed jobs present.
DROP INDEX IF EXISTS maintenance_jobs_queued_idx;
CREATE INDEX maintenance_jobs_queued_idx
    ON maintenance_jobs (workspace_id, run_after, created_at)
    WHERE state = 'queued';
