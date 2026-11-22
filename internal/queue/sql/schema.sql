CREATE TABLE IF NOT EXISTS jobs (
    id              bigserial   PRIMARY KEY,
    queue           text        NOT NULL,
    type            text        NOT NULL,
    payload         jsonb       NOT NULL DEFAULT '{}',
    status          text        NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued', 'running', 'succeeded', 'dead', 'cancelled')),
    priority        int         NOT NULL DEFAULT 0,
    run_at          timestamptz NOT NULL DEFAULT now(),
    attempts        int         NOT NULL DEFAULT 0,
    max_attempts    int         NOT NULL DEFAULT 5,
    last_error      text        NOT NULL DEFAULT '',
    locked_by       text        NOT NULL DEFAULT '',
    locked_until    timestamptz,
    idempotency_key text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz
);

-- Partial indexes: each covers only the rows one query cares about, so they
-- stay small even when the table holds millions of finished jobs.
CREATE INDEX IF NOT EXISTS jobs_ready_idx
    ON jobs (queue, priority DESC, run_at, id) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS jobs_running_idx
    ON jobs (queue, locked_until) WHERE status = 'running';
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_idx
    ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
