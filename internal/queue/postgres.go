package queue

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"sort"
	"time"
)

// The SQL lives in separate files so it can be read, and run by hand in
// psql, without wading through Go string literals.
var (
	//go:embed sql/schema.sql
	schemaSQL string
	//go:embed sql/enqueue.sql
	enqueueSQL string
	//go:embed sql/reap.sql
	reapSQL string
	//go:embed sql/dequeue.sql
	dequeueSQL string
	//go:embed sql/complete.sql
	completeSQL string
	//go:embed sql/fail.sql
	failSQL string
	//go:embed sql/extend.sql
	extendSQL string
	//go:embed sql/cancel.sql
	cancelSQL string
	//go:embed sql/retry.sql
	retrySQL string
)

// PostgresStore stores jobs in one table. It uses only database/sql, so it
// works with any Postgres driver the program registers.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore creates the table and indexes if they are missing.
func NewPostgresStore(ctx context.Context, db *sql.DB) (*PostgresStore, error) {
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return nil, err
	}
	return &PostgresStore{db: db}, nil
}

type scanner interface{ Scan(dest ...any) error }

// scanJob reads the columns of the jobs table, in table order, plus any
// extra trailing columns the query returns.
func scanJob(row scanner, extra ...any) (Job, error) {
	var (
		j           Job
		payload     []byte
		status      string
		lockedUntil sql.NullTime
		idemKey     sql.NullString
		finishedAt  sql.NullTime
	)
	dest := []any{
		&j.ID, &j.Queue, &j.Type, &payload, &status, &j.Priority, &j.RunAt, &j.Attempts, &j.MaxAttempts,
		&j.LastError, &j.LockedBy, &lockedUntil, &idemKey, &j.CreatedAt, &j.UpdatedAt, &finishedAt,
	}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return Job{}, err
	}
	j.Payload, j.Status, j.IdempotencyKey = payload, Status(status), idemKey.String
	if lockedUntil.Valid {
		j.LockedUntil = &lockedUntil.Time
	}
	if finishedAt.Valid {
		j.FinishedAt = &finishedAt.Time
	}
	return j, nil
}

func (s *PostgresStore) Enqueue(ctx context.Context, p EnqueueParams) (Job, bool, error) {
	if err := p.normalize(); err != nil {
		return Job{}, false, err
	}
	runAt := "" // empty lets the database use its own clock
	if !p.RunAt.IsZero() {
		runAt = p.RunAt.UTC().Format(time.RFC3339Nano)
	}
	var created bool
	row := s.db.QueryRowContext(ctx, enqueueSQL, p.Queue, p.Type, string(p.Payload), p.Priority, runAt, p.MaxAttempts, p.IdempotencyKey)
	j, err := scanJob(row, &created)
	return j, created, err
}

func (s *PostgresStore) Dequeue(ctx context.Context, queue, workerID string, limit int, lease time.Duration) ([]Job, error) {
	if _, err := s.db.ExecContext(ctx, reapSQL, queue); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, dequeueSQL, queue, workerID, limit, lease.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	// UPDATE ... RETURNING does not promise any row order.
	sort.Slice(out, func(a, b int) bool {
		if out[a].Priority != out[b].Priority {
			return out[a].Priority > out[b].Priority
		}
		if !out[a].RunAt.Equal(out[b].RunAt) {
			return out[a].RunAt.Before(out[b].RunAt)
		}
		return out[a].ID < out[b].ID
	})
	return out, rows.Err()
}

func (s *PostgresStore) changedOne(ctx context.Context, query string, args ...any) (bool, error) {
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *PostgresStore) Complete(ctx context.Context, id int64, workerID string, attempt int) (bool, error) {
	return s.changedOne(ctx, completeSQL, id, workerID, attempt)
}

func (s *PostgresStore) Fail(ctx context.Context, id int64, workerID string, attempt int, errMsg string, retryIn time.Duration) (Status, error) {
	var status string
	err := s.db.QueryRowContext(ctx, failSQL, id, workerID, attempt, errMsg, retryIn.Seconds()).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil // the lease was lost; someone else owns the job now
	}
	return Status(status), err
}

func (s *PostgresStore) ExtendLease(ctx context.Context, id int64, workerID string, attempt int, lease time.Duration) (bool, error) {
	return s.changedOne(ctx, extendSQL, id, workerID, attempt, lease.Seconds())
}

func (s *PostgresStore) Get(ctx context.Context, id int64) (Job, error) {
	j, err := scanJob(s.db.QueryRowContext(ctx, `SELECT * FROM jobs WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}

func (s *PostgresStore) Cancel(ctx context.Context, id int64) (bool, error) {
	return s.changedOne(ctx, cancelSQL, id)
}

func (s *PostgresStore) Retry(ctx context.Context, id int64) (bool, error) {
	return s.changedOne(ctx, retrySQL, id)
}

func (s *PostgresStore) Stats(ctx context.Context) (map[string]map[Status]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT queue, status, count(*) FROM jobs GROUP BY queue, status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]map[Status]int{}
	for rows.Next() {
		var queue, status string
		var n int
		if err := rows.Scan(&queue, &status, &n); err != nil {
			return nil, err
		}
		if out[queue] == nil {
			out[queue] = map[Status]int{}
		}
		out[queue][Status(status)] = n
	}
	return out, rows.Err()
}
