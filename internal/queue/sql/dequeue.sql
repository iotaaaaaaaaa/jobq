-- $1 queue, $2 worker id, $3 max jobs, $4 lease in seconds
--
-- The heart of the queue. Many workers run this at the same time.
--
-- FOR UPDATE locks the rows the subquery picks. SKIP LOCKED makes a worker
-- pass over rows another worker has already locked instead of waiting for
-- them. Together they hand each ready job to exactly one worker, with no
-- blocking between workers.
--
-- A job is ready if it is queued and due, or if it is running but its lease
-- has expired, which means the worker that took it has died or hung.
WITH picked AS (
    SELECT id
    FROM jobs
    WHERE queue = $1
      AND (   (status = 'queued'  AND run_at <= now())
           OR (status = 'running' AND locked_until < now() AND attempts < max_attempts))
    ORDER BY priority DESC, run_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
SET status = 'running',
    attempts = j.attempts + 1,
    locked_by = $2,
    locked_until = now() + make_interval(secs => $4),
    updated_at = now()
FROM picked
WHERE j.id = picked.id
RETURNING j.*
