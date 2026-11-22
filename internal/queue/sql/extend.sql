-- $1 id, $2 worker id, $3 attempt, $4 lease in seconds
UPDATE jobs
SET locked_until = now() + make_interval(secs => $4), updated_at = now()
WHERE id = $1 AND status = 'running' AND locked_by = $2 AND attempts = $3
