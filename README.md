# jobq

A background job queue in Go, backed by Postgres. Applications submit jobs over HTTP; a pool of workers runs them with retries, backoff, scheduling and crash recovery. It is the same kind of system as Sidekiq or Celery, small enough to read in an afternoon.

```sh
curl -X POST localhost:8080/jobs -d '{"type":"webhook","payload":{"url":"https://example.com/hook"},"max_attempts":5}'
# {"id":1,"status":"queued",...}
curl localhost:8080/jobs/1
# {"id":1,"status":"succeeded","attempts":1,...}
```

## Run it

With Docker (starts Postgres and the service):

```sh
docker compose up --build
```

Without Docker:

```sh
go mod tidy                      # downloads the Postgres driver, once
go build -o bin/jobq ./cmd/jobq
./bin/jobq -database-url "postgres://user:pass@localhost:5432/jobq?sslmode=disable"
```

With no `-database-url` it uses an in-memory store, which is handy for trying it out but loses jobs on exit.

Scale out by running more worker processes against the same database:

```sh
./bin/jobq -api=false -workers 16 -database-url ...
```

## API

| Request | Purpose |
|---|---|
| `POST /jobs` | Enqueue. 201 if created, 200 if the idempotency key already existed |
| `GET /jobs/{id}` | Job details and status |
| `DELETE /jobs/{id}` | Cancel a job that has not started |
| `POST /jobs/{id}/retry` | Requeue a dead job |
| `GET /stats` | Counts per queue and status |
| `GET /healthz` | Liveness probe |

`POST /jobs` body:

| Field | Meaning |
|---|---|
| `type` (required) | Which handler runs the job |
| `payload` | Any JSON, passed to the handler |
| `queue` | Defaults to `default` |
| `priority` | Higher runs first |
| `delay_seconds` or `run_at` | Schedule for later |
| `max_attempts` | Defaults to 5 |
| `idempotency_key` | Makes a repeated submit return the first job |

Built-in job types for experimenting: `echo`, `sleep`, `fail`, `webhook`. Real handlers are registered in `cmd/jobq/main.go`.

## Job lifecycle

```
queued --dequeue--> running --ok-----------------> succeeded
   ^                   |
   |                   +--error, attempts left---> queued   (after backoff)
   |                   +--error, none left-------> dead     (manual retry possible)
   +--lease expired----+                           (worker crashed; another takes over)
```

## Design decisions

**Postgres as the queue, with `FOR UPDATE SKIP LOCKED`.** Workers claim jobs with one statement (`internal/queue/sql/dequeue.sql`). The row lock stops two workers from taking the same job, and `SKIP LOCKED` makes a worker pass over rows someone else is claiming instead of waiting behind them. No separate broker to run, and enqueueing a job can share a transaction with the application's own writes.

**Leases instead of "worker is alive" tracking.** A claimed job is held until a deadline. A worker that crashes simply stops renewing, the deadline passes, and the job becomes available again. Running jobs renew their lease every third of its length, so a long job is not mistaken for a dead worker.

**Fencing on every state change.** Completing, failing or extending a job requires the worker ID and attempt number it was handed out with. A worker that froze, lost its lease and woke up later cannot overwrite the result of the worker that replaced it.

**At-least-once delivery.** A job is never lost, but one whose worker dies mid-run is run again. Handlers must be safe to repeat. Exactly-once is not possible when the handler has outside effects, so the design is honest about it.

**Idempotency keys for submitters.** If a client times out and resubmits, the unique index on `(queue, idempotency_key)` returns the original job instead of creating a duplicate.

**Exponential backoff with jitter.** Retries wait 1s, 2s, 4s and so on up to 5 minutes, plus up to 20% random extra, so a burst of failures does not retry in one synchronized wave.

**Dead jobs are kept.** A job that exhausts its attempts stays in the table with its last error, where it can be inspected and retried by hand.

**Graceful shutdown.** On SIGTERM a worker stops claiming, waits up to 20 seconds for running jobs, then cancels the rest. Cancelled jobs are requeued, not lost.

**Partial indexes.** The index used to find ready jobs covers only `queued` rows, so it stays small however many finished jobs pile up.

## Layout

| Path | Contents |
|---|---|
| `internal/queue/queue.go` | Job type, states, the `Store` interface |
| `internal/queue/sql/*.sql` | Schema and every query, one per file, commented |
| `internal/queue/postgres.go` | Postgres store (plain `database/sql`) |
| `internal/queue/memory.go` | In-memory store with identical rules |
| `internal/queue/worker.go` | Worker pool: polling, heartbeats, retries, shutdown |
| `internal/queue/api.go` | HTTP API |

## Tests

```sh
make test                                  # Go tests on the in-memory store, with -race
DATABASE_URL=... make test-sql             # the SQL files against a real Postgres, via psql
DATABASE_URL=... make test-postgres        # the Go store against a real Postgres
```

- `store_test.go` is a conformance suite that any `Store` must pass: ordering, scheduling, retries, lease takeover, fencing, idempotency, and 16 goroutines draining 400 jobs with no job delivered twice.
- `worker_test.go` covers the pool: three competing pools run 300 jobs exactly once each, the concurrency limit holds, panics become failures, a crashed worker's job is recovered, and shutdown waits for running jobs.
- `tests/sql_test.sh` runs 30 checks on the real queries in Postgres, including 8 concurrent sessions draining 2,000 jobs with no duplicates.

## Limits

- **Polling.** Idle workers check every 500 ms. `LISTEN`/`NOTIFY` would wake them instantly.
- **No recurring jobs.** There is `run_at` but no cron schedule.
- **No per-queue rate limits or unique-job rules** beyond the idempotency key.
- **Finished rows are never deleted.** A real deployment needs a cleanup job or table partitioning.
- **No authentication on the API.**

## Questions to check your understanding

1. What happens in `dequeue.sql` if you remove `SKIP LOCKED`? What if you remove `FOR UPDATE` as well?
2. A worker claims a job, pauses for a minute (say, a long GC or a frozen VM), then finishes it. Walk through what the database does with its `Complete` call.
3. Why is delivery at-least-once and not exactly-once? What must a handler that charges a credit card do about that?
4. Why does `Pool.Run` only request as many jobs as it has free slots?
5. Why do retries add random jitter to the delay?
6. The API returns 200 instead of 201 for a repeated idempotency key. Why does that distinction help a client?
7. Why is the "ready jobs" index a partial index on `status = 'queued'`?
