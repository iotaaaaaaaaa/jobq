-- $1 queue
-- A job whose worker vanished on its final attempt has no retries left.
UPDATE jobs
SET status = 'dead', finished_at = now(), updated_at = now(),
    last_error = 'worker lease expired on the final attempt',
    locked_by = '', locked_until = NULL
WHERE queue = $1 AND status = 'running' AND locked_until < now() AND attempts >= max_attempts
