-- $1 id. Gives a dead job a fresh set of attempts.
UPDATE jobs
SET status = 'queued', attempts = 0, run_at = now(), finished_at = NULL, last_error = '', updated_at = now()
WHERE id = $1 AND status = 'dead'
