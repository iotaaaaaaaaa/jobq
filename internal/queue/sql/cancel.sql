-- $1 id. Only a job that has not started can be cancelled.
UPDATE jobs SET status = 'cancelled', finished_at = now(), updated_at = now()
WHERE id = $1 AND status = 'queued'
