-- $1 id, $2 worker id, $3 attempt
-- The worker and attempt must still match. If the lease expired and another
-- worker took the job, this matches no row and the late result is ignored.
UPDATE jobs
SET status = 'succeeded', finished_at = now(), updated_at = now(), locked_by = '', locked_until = NULL
WHERE id = $1 AND status = 'running' AND locked_by = $2 AND attempts = $3
