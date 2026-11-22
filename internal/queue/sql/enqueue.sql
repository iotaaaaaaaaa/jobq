-- $1 queue, $2 type, $3 payload, $4 priority, $5 run_at ('' = now),
-- $6 max_attempts, $7 idempotency key ('' = none)
--
-- If a job with the same idempotency key already exists, the conflict clause
-- turns the insert into a no-op update so RETURNING still yields that row.
-- xmax is 0 only for a freshly inserted row, which tells us which case ran.
INSERT INTO jobs (queue, type, payload, priority, run_at, max_attempts, idempotency_key)
VALUES ($1, $2, $3::jsonb, $4, COALESCE(NULLIF($5, '')::timestamptz, now()), $6, NULLIF($7, ''))
ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
RETURNING *, (xmax = 0) AS created
