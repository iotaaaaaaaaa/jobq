// Package queue is a durable background job queue.
//
// A job moves through these states:
//
//	queued --dequeue--> running --complete--> succeeded
//	   ^                   |
//	   |                   +--fail, attempts left--> queued (after a backoff delay)
//	   |                   +--fail, none left------> dead
//	   +--lease expired----+   (the worker crashed or hung; another worker takes over)
//
// The guarantee is at-least-once delivery: a job is never lost, but one whose
// worker dies mid-run is run again, so handlers must be safe to repeat.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusDead      Status = "dead" // failed on every attempt; kept for inspection and manual retry
	StatusCancelled Status = "cancelled"
)

type Job struct {
	ID             int64           `json:"id"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         Status          `json:"status"`
	Priority       int             `json:"priority"`
	RunAt          time.Time       `json:"run_at"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	LastError      string          `json:"last_error,omitempty"`
	LockedBy       string          `json:"locked_by,omitempty"`
	LockedUntil    *time.Time      `json:"locked_until,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

type EnqueueParams struct {
	Queue       string
	Type        string
	Payload     json.RawMessage
	Priority    int       // higher runs first
	RunAt       time.Time // zero means now
	MaxAttempts int       // zero means DefaultMaxAttempts
	// If set, enqueuing the same key twice on a queue returns the first job
	// instead of creating a second one. This makes client retries safe.
	IdempotencyKey string
}

const (
	DefaultQueue       = "default"
	DefaultMaxAttempts = 5
)

var ErrNotFound = errors.New("job not found")

// Store is the persistence layer. There are two implementations: Postgres
// for real use and an in-memory one for tests and local runs.
//
// Complete, Fail and ExtendLease take the worker ID and attempt number the
// job was handed out with. They only succeed if the job is still held by
// that worker on that attempt. This is a fencing check: a worker that lost
// its lease cannot overwrite the result of the worker that replaced it.
type Store interface {
	Enqueue(ctx context.Context, p EnqueueParams) (job Job, created bool, err error)
	// Dequeue claims up to limit ready jobs for workerID and leases them for
	// the given duration.
	Dequeue(ctx context.Context, queue, workerID string, limit int, lease time.Duration) ([]Job, error)
	Complete(ctx context.Context, id int64, workerID string, attempt int) (ok bool, err error)
	// Fail records an error. The job is retried after retryIn, or marked
	// dead if it has used all its attempts. It returns the resulting status,
	// or "" if the caller no longer holds the job.
	Fail(ctx context.Context, id int64, workerID string, attempt int, errMsg string, retryIn time.Duration) (Status, error)
	ExtendLease(ctx context.Context, id int64, workerID string, attempt int, lease time.Duration) (ok bool, err error)

	Get(ctx context.Context, id int64) (Job, error)
	Cancel(ctx context.Context, id int64) (ok bool, err error) // queued jobs only
	Retry(ctx context.Context, id int64) (ok bool, err error)  // dead jobs only
	Stats(ctx context.Context) (map[string]map[Status]int, error)
}

func (p *EnqueueParams) normalize() error {
	if p.Type == "" {
		return errors.New("job type is required")
	}
	if p.Queue == "" {
		p.Queue = DefaultQueue
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = DefaultMaxAttempts
	}
	if len(p.Payload) == 0 {
		p.Payload = json.RawMessage(`{}`)
	}
	if !json.Valid(p.Payload) {
		return errors.New("payload is not valid JSON")
	}
	return nil
}
