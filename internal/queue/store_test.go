package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

var ctx = context.Background()

// runStoreTests is a conformance suite: every Store implementation must pass
// it. It runs against MemoryStore here, and against Postgres in
// postgres_test.go (go test -tags postgres).
func runStoreTests(t *testing.T, newStore func(t *testing.T) Store) {
	enqueue := func(t *testing.T, s Store, p EnqueueParams) Job {
		t.Helper()
		if p.Type == "" {
			p.Type = "test"
		}
		j, _, err := s.Enqueue(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		return j
	}
	dequeue := func(t *testing.T, s Store, worker string, n int, lease time.Duration) []Job {
		t.Helper()
		jobs, err := s.Dequeue(ctx, DefaultQueue, worker, n, lease)
		if err != nil {
			t.Fatal(err)
		}
		return jobs
	}
	get := func(t *testing.T, s Store, id int64) Job {
		t.Helper()
		j, err := s.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return j
	}

	t.Run("enqueue applies defaults", func(t *testing.T) {
		s := newStore(t)
		j := enqueue(t, s, EnqueueParams{Type: "email", Payload: json.RawMessage(`{"to":"a@b.c"}`)})
		if j.ID == 0 || j.Queue != DefaultQueue || j.Status != StatusQueued || j.MaxAttempts != DefaultMaxAttempts || j.Attempts != 0 {
			t.Fatalf("unexpected job %+v", j)
		}
		var p map[string]string
		if json.Unmarshal(get(t, s, j.ID).Payload, &p); p["to"] != "a@b.c" {
			t.Fatalf("payload round trip: %s", j.Payload)
		}
		if _, _, err := s.Enqueue(ctx, EnqueueParams{}); err == nil {
			t.Fatal("a job without a type must be rejected")
		}
		if _, err := s.Get(ctx, 999999); err != ErrNotFound {
			t.Fatalf("Get of missing job: %v", err)
		}
	})

	t.Run("dequeue order is priority then age", func(t *testing.T) {
		s := newStore(t)
		low1 := enqueue(t, s, EnqueueParams{})
		high := enqueue(t, s, EnqueueParams{Priority: 10})
		low2 := enqueue(t, s, EnqueueParams{})
		jobs := dequeue(t, s, "w", 10, time.Minute)
		if len(jobs) != 3 || jobs[0].ID != high.ID || jobs[1].ID != low1.ID || jobs[2].ID != low2.ID {
			t.Fatalf("order: %v", ids(jobs))
		}
		if jobs[0].Status != StatusRunning || jobs[0].Attempts != 1 || jobs[0].LockedBy != "w" || jobs[0].LockedUntil == nil {
			t.Fatalf("claimed job not marked: %+v", jobs[0])
		}
		if more := dequeue(t, s, "w2", 10, time.Minute); len(more) != 0 {
			t.Fatalf("running jobs handed out again: %v", ids(more))
		}
	})

	t.Run("queues are separate", func(t *testing.T) {
		s := newStore(t)
		enqueue(t, s, EnqueueParams{Queue: "other"})
		if jobs := dequeue(t, s, "w", 10, time.Minute); len(jobs) != 0 {
			t.Fatal("got a job from a different queue")
		}
	})

	t.Run("scheduled job waits for its time", func(t *testing.T) {
		s := newStore(t)
		j := enqueue(t, s, EnqueueParams{RunAt: time.Now().Add(300 * time.Millisecond)})
		if jobs := dequeue(t, s, "w", 10, time.Minute); len(jobs) != 0 {
			t.Fatal("job ran before run_at")
		}
		time.Sleep(400 * time.Millisecond)
		if jobs := dequeue(t, s, "w", 10, time.Minute); len(jobs) != 1 || jobs[0].ID != j.ID {
			t.Fatal("job not available after run_at")
		}
	})

	t.Run("complete", func(t *testing.T) {
		s := newStore(t)
		j := enqueue(t, s, EnqueueParams{})
		got := dequeue(t, s, "w", 1, time.Minute)[0]
		if ok, _ := s.Complete(ctx, j.ID, "w", got.Attempts); !ok {
			t.Fatal("complete refused")
		}
		done := get(t, s, j.ID)
		if done.Status != StatusSucceeded || done.FinishedAt == nil || done.LockedBy != "" {
			t.Fatalf("after complete: %+v", done)
		}
		if ok, _ := s.Complete(ctx, j.ID, "w", got.Attempts); ok {
			t.Fatal("completing twice must be refused")
		}
	})

	t.Run("fail retries then goes dead", func(t *testing.T) {
		s := newStore(t)
		j := enqueue(t, s, EnqueueParams{MaxAttempts: 2})

		a1 := dequeue(t, s, "w", 1, time.Minute)[0]
		if st, _ := s.Fail(ctx, j.ID, "w", a1.Attempts, "boom 1", 200*time.Millisecond); st != StatusQueued {
			t.Fatalf("first failure -> %q, want queued", st)
		}
		if jobs := dequeue(t, s, "w", 1, time.Minute); len(jobs) != 0 {
			t.Fatal("retry ran before its backoff delay")
		}
		time.Sleep(300 * time.Millisecond)

		a2 := dequeue(t, s, "w", 1, time.Minute)
		if len(a2) != 1 || a2[0].Attempts != 2 || a2[0].LastError != "boom 1" {
			t.Fatalf("second attempt: %+v", a2)
		}
		if st, _ := s.Fail(ctx, j.ID, "w", 2, "boom 2", time.Millisecond); st != StatusDead {
			t.Fatalf("final failure -> %q, want dead", st)
		}
		dead := get(t, s, j.ID)
		if dead.Status != StatusDead || dead.LastError != "boom 2" || dead.FinishedAt == nil {
			t.Fatalf("dead job: %+v", dead)
		}

		if ok, _ := s.Retry(ctx, j.ID); !ok {
			t.Fatal("retry of dead job refused")
		}
		if again := dequeue(t, s, "w", 1, time.Minute); len(again) != 1 || again[0].Attempts != 1 {
			t.Fatalf("after manual retry: %+v", again)
		}
		if ok, _ := s.Retry(ctx, j.ID); ok {
			t.Fatal("retry of a running job must be refused")
		}
	})

	t.Run("expired lease lets another worker take over", func(t *testing.T) {
		s := newStore(t)
		j := enqueue(t, s, EnqueueParams{})
		first := dequeue(t, s, "crashed", 1, 150*time.Millisecond)[0]
		if jobs := dequeue(t, s, "other", 1, time.Minute); len(jobs) != 0 {
			t.Fatal("job taken while its lease was still valid")
		}
		time.Sleep(250 * time.Millisecond)

		second := dequeue(t, s, "other", 1, time.Minute)
		if len(second) != 1 || second[0].ID != j.ID || second[0].Attempts != 2 || second[0].LockedBy != "other" {
			t.Fatalf("takeover: %+v", second)
		}
		// Fencing: the first worker wakes up late and must not be able to
		// change a job it no longer holds.
		if ok, _ := s.Complete(ctx, j.ID, "crashed", first.Attempts); ok {
			t.Fatal("stale worker completed a job it had lost")
		}
		if st, _ := s.Fail(ctx, j.ID, "crashed", first.Attempts, "late", 0); st != "" {
			t.Fatal("stale worker failed a job it had lost")
		}
		if ok, _ := s.ExtendLease(ctx, j.ID, "crashed", first.Attempts, time.Minute); ok {
			t.Fatal("stale worker extended a lease it had lost")
		}
		if ok, _ := s.Complete(ctx, j.ID, "other", 2); !ok {
			t.Fatal("current holder could not complete")
		}
	})

	t.Run("extending a lease keeps the job", func(t *testing.T) {
		s := newStore(t)
		enqueue(t, s, EnqueueParams{})
		j := dequeue(t, s, "w", 1, 150*time.Millisecond)[0]
		time.Sleep(100 * time.Millisecond)
		if ok, _ := s.ExtendLease(ctx, j.ID, "w", j.Attempts, 500*time.Millisecond); !ok {
			t.Fatal("extend refused")
		}
		time.Sleep(150 * time.Millisecond) // past the original lease
		if jobs := dequeue(t, s, "other", 1, time.Minute); len(jobs) != 0 {
			t.Fatal("job taken despite extended lease")
		}
	})

	t.Run("crash on the final attempt goes dead", func(t *testing.T) {
		s := newStore(t)
		j := enqueue(t, s, EnqueueParams{MaxAttempts: 1})
		dequeue(t, s, "crashed", 1, 100*time.Millisecond)
		time.Sleep(200 * time.Millisecond)
		if jobs := dequeue(t, s, "other", 1, time.Minute); len(jobs) != 0 {
			t.Fatal("job with no attempts left was run again")
		}
		if got := get(t, s, j.ID); got.Status != StatusDead {
			t.Fatalf("status %q, want dead", got.Status)
		}
	})

	t.Run("idempotency key", func(t *testing.T) {
		s := newStore(t)
		a, created1, _ := s.Enqueue(ctx, EnqueueParams{Type: "pay", IdempotencyKey: "order-42"})
		b, created2, _ := s.Enqueue(ctx, EnqueueParams{Type: "pay", IdempotencyKey: "order-42"})
		if !created1 || created2 || a.ID != b.ID {
			t.Fatalf("same key gave jobs %d (created=%v) and %d (created=%v)", a.ID, created1, b.ID, created2)
		}
		c, created3, _ := s.Enqueue(ctx, EnqueueParams{Type: "pay", Queue: "other", IdempotencyKey: "order-42"})
		if !created3 || c.ID == a.ID {
			t.Fatal("keys must be scoped to a queue")
		}
		d := enqueue(t, s, EnqueueParams{})
		e := enqueue(t, s, EnqueueParams{})
		if d.ID == e.ID {
			t.Fatal("jobs without a key must never be merged")
		}
	})

	t.Run("cancel", func(t *testing.T) {
		s := newStore(t)
		j := enqueue(t, s, EnqueueParams{})
		if ok, _ := s.Cancel(ctx, j.ID); !ok {
			t.Fatal("cancel of queued job refused")
		}
		if jobs := dequeue(t, s, "w", 1, time.Minute); len(jobs) != 0 {
			t.Fatal("cancelled job was run")
		}
		running := enqueue(t, s, EnqueueParams{})
		dequeue(t, s, "w", 1, time.Minute)
		if ok, _ := s.Cancel(ctx, running.ID); ok {
			t.Fatal("a running job must not be cancellable")
		}
	})

	t.Run("stats", func(t *testing.T) {
		s := newStore(t)
		enqueue(t, s, EnqueueParams{})
		enqueue(t, s, EnqueueParams{})
		enqueue(t, s, EnqueueParams{Queue: "mail"})
		dequeue(t, s, "w", 1, time.Minute)
		stats, err := s.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if stats[DefaultQueue][StatusQueued] != 1 || stats[DefaultQueue][StatusRunning] != 1 || stats["mail"][StatusQueued] != 1 {
			t.Fatalf("stats: %v", stats)
		}
	})

	// The property the whole design exists for: many workers pulling at the
	// same time, and every job is handed out exactly once.
	t.Run("concurrent dequeue never hands a job to two workers", func(t *testing.T) {
		s := newStore(t)
		const total, workers = 400, 16
		for i := 0; i < total; i++ {
			enqueue(t, s, EnqueueParams{})
		}
		var mu sync.Mutex
		seen := map[int64]string{}
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(worker string) {
				defer wg.Done()
				for {
					jobs, err := s.Dequeue(ctx, DefaultQueue, worker, 7, time.Minute)
					if err != nil {
						t.Error(err)
						return
					}
					if len(jobs) == 0 {
						return
					}
					mu.Lock()
					for _, j := range jobs {
						if prev, dup := seen[j.ID]; dup {
							t.Errorf("job %d given to both %s and %s", j.ID, prev, worker)
						}
						seen[j.ID] = worker
					}
					mu.Unlock()
				}
			}(fmt.Sprintf("w%d", w))
		}
		wg.Wait()
		if len(seen) != total {
			t.Fatalf("%d jobs handed out, want %d", len(seen), total)
		}
	})
}

func ids(jobs []Job) []int64 {
	out := make([]int64, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}

func TestMemoryStore(t *testing.T) {
	runStoreTests(t, func(*testing.T) Store { return NewMemoryStore() })
}
