package queue

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryStore keeps jobs in a map behind one mutex. It follows the same rules
// as the Postgres store, which makes it a readable reference for what the SQL
// does, but nothing survives a restart.
type MemoryStore struct {
	mu     sync.Mutex
	jobs   map[int64]*Job
	keys   map[string]int64 // queue + "\x00" + idempotency key -> job id
	nextID int64
	now    func() time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: map[int64]*Job{}, keys: map[string]int64{}, now: time.Now}
}

func (m *MemoryStore) Enqueue(_ context.Context, p EnqueueParams) (Job, bool, error) {
	if err := p.normalize(); err != nil {
		return Job{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if p.IdempotencyKey != "" {
		if id, ok := m.keys[p.Queue+"\x00"+p.IdempotencyKey]; ok {
			return *m.jobs[id], false, nil
		}
	}
	now := m.now()
	if p.RunAt.IsZero() {
		p.RunAt = now
	}
	m.nextID++
	j := &Job{
		ID: m.nextID, Queue: p.Queue, Type: p.Type, Payload: p.Payload, Status: StatusQueued,
		Priority: p.Priority, RunAt: p.RunAt, MaxAttempts: p.MaxAttempts,
		IdempotencyKey: p.IdempotencyKey, CreatedAt: now, UpdatedAt: now,
	}
	m.jobs[j.ID] = j
	if p.IdempotencyKey != "" {
		m.keys[p.Queue+"\x00"+p.IdempotencyKey] = j.ID
	}
	return *j, true, nil
}

func (m *MemoryStore) Dequeue(_ context.Context, queue, workerID string, limit int, lease time.Duration) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()

	var ready []*Job
	for _, j := range m.jobs {
		if j.Queue != queue {
			continue
		}
		expired := j.Status == StatusRunning && j.LockedUntil.Before(now)
		switch {
		case expired && j.Attempts >= j.MaxAttempts:
			// The worker vanished on the final attempt: no retries left.
			j.Status, j.FinishedAt, j.UpdatedAt = StatusDead, &now, now
			j.LastError, j.LockedBy, j.LockedUntil = "worker lease expired on the final attempt", "", nil
		case expired, j.Status == StatusQueued && !j.RunAt.After(now):
			ready = append(ready, j)
		}
	}
	sort.Slice(ready, func(a, b int) bool {
		x, y := ready[a], ready[b]
		if x.Priority != y.Priority {
			return x.Priority > y.Priority
		}
		if !x.RunAt.Equal(y.RunAt) {
			return x.RunAt.Before(y.RunAt)
		}
		return x.ID < y.ID
	})
	if len(ready) > limit {
		ready = ready[:limit]
	}

	out := make([]Job, 0, len(ready))
	for _, j := range ready {
		until := now.Add(lease)
		j.Status, j.LockedBy, j.LockedUntil, j.UpdatedAt = StatusRunning, workerID, &until, now
		j.Attempts++
		out = append(out, *j)
	}
	return out, nil
}

// held returns the job if workerID still holds it on the given attempt.
func (m *MemoryStore) held(id int64, workerID string, attempt int) *Job {
	j := m.jobs[id]
	if j == nil || j.Status != StatusRunning || j.LockedBy != workerID || j.Attempts != attempt {
		return nil
	}
	return j
}

func (m *MemoryStore) Complete(_ context.Context, id int64, workerID string, attempt int) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j := m.held(id, workerID, attempt)
	if j == nil {
		return false, nil
	}
	now := m.now()
	j.Status, j.FinishedAt, j.UpdatedAt, j.LockedBy, j.LockedUntil = StatusSucceeded, &now, now, "", nil
	return true, nil
}

func (m *MemoryStore) Fail(_ context.Context, id int64, workerID string, attempt int, errMsg string, retryIn time.Duration) (Status, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j := m.held(id, workerID, attempt)
	if j == nil {
		return "", nil
	}
	now := m.now()
	j.LastError, j.LockedBy, j.LockedUntil, j.UpdatedAt = errMsg, "", nil, now
	if j.Attempts >= j.MaxAttempts {
		j.Status, j.FinishedAt = StatusDead, &now
	} else {
		j.Status, j.RunAt = StatusQueued, now.Add(retryIn)
	}
	return j.Status, nil
}

func (m *MemoryStore) ExtendLease(_ context.Context, id int64, workerID string, attempt int, lease time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j := m.held(id, workerID, attempt)
	if j == nil {
		return false, nil
	}
	until := m.now().Add(lease)
	j.LockedUntil, j.UpdatedAt = &until, m.now()
	return true, nil
}

func (m *MemoryStore) Get(_ context.Context, id int64) (Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j := m.jobs[id]; j != nil {
		return *j, nil
	}
	return Job{}, ErrNotFound
}

func (m *MemoryStore) Cancel(_ context.Context, id int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j := m.jobs[id]
	if j == nil || j.Status != StatusQueued {
		return false, nil
	}
	now := m.now()
	j.Status, j.FinishedAt, j.UpdatedAt = StatusCancelled, &now, now
	return true, nil
}

func (m *MemoryStore) Retry(_ context.Context, id int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j := m.jobs[id]
	if j == nil || j.Status != StatusDead {
		return false, nil
	}
	now := m.now()
	j.Status, j.Attempts, j.RunAt, j.FinishedAt, j.LastError, j.UpdatedAt = StatusQueued, 0, now, nil, "", now
	return true, nil
}

func (m *MemoryStore) Stats(context.Context) (map[string]map[Status]int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]map[Status]int{}
	for _, j := range m.jobs {
		if out[j.Queue] == nil {
			out[j.Queue] = map[Status]int{}
		}
		out[j.Queue][j.Status]++
	}
	return out, nil
}
