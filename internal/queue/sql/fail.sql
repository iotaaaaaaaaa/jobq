-- $1 id, $2 worker id, $3 attempt, $4 error text, $5 retry delay in seconds
UPDATE jobs
SET status      = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
    run_at      = CASE WHEN attempts >= max_attempts THEN run_at ELSE now() + make_interval(secs => $5) END,
    finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
    last_error = $4, locked_by = '', locked_until = NULL, updated_at = now()
WHERE id = $1 AND status = 'running' AND locked_by = $2 AND attempts = $3
RETURNING status
