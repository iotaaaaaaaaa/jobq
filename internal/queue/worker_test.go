package queue

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fastPool(s Store, id string, concurrency int) *Pool {
	return NewPool(s, PoolConfig{
		WorkerID: id, Concurrency: concurrency,
		Lease: 300 * time.Millisecond, PollEvery: 10 * time.Millisecond, ShutdownGrace: 2 * time.Second,
		Backoff: func(int) time.Duration { return 20 * time.Millisecond },
	})
}

// start runs the pool in the background and returns a function that shuts it
// down and waits for it.
func start(p *Pool) (stop func()) {
	c, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(c); close(done) }()
	return func() { cancel(); <-done }
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func statusOf(t *testing.T, s Store, id int64) Status {
	t.Helper()
	j, err := s.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return j.Status
}

func TestPoolRunsEveryJobOnce(t *testing.T) {
	s := NewMemoryStore()
	const total = 300
	var mu sync.Mutex
	runs := map[int64]int{}
	handler := func(_ context.Context, j Job) error {
		mu.Lock()
		runs[j.ID]++
		mu.Unlock()
		return nil
	}
	// Three worker processes competing for the same queue.
	var stops []func()
	for i := 0; i < 3; i++ {
		p := fastPool(s, fmt.Sprintf("w%d", i), 8)
		p.Handle("work", handler)
		stops = append(stops, start(p))
	}
	for i := 0; i < total; i++ {
		s.Enqueue(ctx, EnqueueParams{Type: "work"})
	}
	waitFor(t, "all jobs to succeed", func() bool {
		st, _ := s.Stats(ctx)
		return st[DefaultQueue][StatusSucceeded] == total
	})
	for _, stop := range stops {
		stop()
	}
	if len(runs) != total {
		t.Fatalf("%d distinct jobs ran, want %d", len(runs), total)
	}
	for id, n := range runs {
		if n != 1 {
			t.Fatalf("job %d ran %d times", id, n)
		}
	}
}

func TestPoolRespectsConcurrencyLimit(t *testing.T) {
	s := NewMemoryStore()
	p := fastPool(s, "w", 3)
	var running, peak atomic.Int32
	p.Handle("slow", func(context.Context, Job) error {
		n := running.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		running.Add(-1)
		return nil
	})
	for i := 0; i < 20; i++ {
		s.Enqueue(ctx, EnqueueParams{Type: "slow"})
	}
	stop := start(p)
	waitFor(t, "jobs to finish", func() bool {
		st, _ := s.Stats(ctx)
		return st[DefaultQueue][StatusSucceeded] == 20
	})
	stop()
	if peak.Load() != 3 {
		t.Fatalf("peak concurrency %d, want exactly 3", peak.Load())
	}
}

func TestPoolRetriesThenSucceeds(t *testing.T) {
	s := NewMemoryStore()
	p := fastPool(s, "w", 2)
	var calls atomic.Int32
	p.Handle("flaky", func(context.Context, Job) error {
		if calls.Add(1) < 3 {
			return errors.New("temporary failure")
		}
		return nil
	})
	j, _, _ := s.Enqueue(ctx, EnqueueParams{Type: "flaky", MaxAttempts: 5})
	stop := start(p)
	defer stop()
	waitFor(t, "job to succeed", func() bool { return statusOf(t, s, j.ID) == StatusSucceeded })
	if got, _ := s.Get(ctx, j.ID); got.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("attempts=%d calls=%d, want 3 and 3", got.Attempts, calls.Load())
	}
}

func TestPoolGivesUpAfterMaxAttempts(t *testing.T) {
	s := NewMemoryStore()
	p := fastPool(s, "w", 2)
	p.Handle("broken", func(context.Context, Job) error { return errors.New("always fails") })
	p.Handle("panics", func(context.Context, Job) error { panic("handler bug") })

	broken, _, _ := s.Enqueue(ctx, EnqueueParams{Type: "broken", MaxAttempts: 3})
	panics, _, _ := s.Enqueue(ctx, EnqueueParams{Type: "panics", MaxAttempts: 1})
	unknown, _, _ := s.Enqueue(ctx, EnqueueParams{Type: "no-such-type", MaxAttempts: 1})
	stop := start(p)
	defer stop()

	for _, id := range []int64{broken.ID, panics.ID, unknown.ID} {
		waitFor(t, "job to go dead", func() bool { return statusOf(t, s, id) == StatusDead })
	}
	if j, _ := s.Get(ctx, broken.ID); j.Attempts != 3 || j.LastError != "always fails" {
		t.Fatalf("broken: %+v", j)
	}
	if j, _ := s.Get(ctx, panics.ID); !strings.Contains(j.LastError, "panicked") {
		t.Fatalf("a panic must be recorded as an error, got %q", j.LastError)
	}
	if j, _ := s.Get(ctx, unknown.ID); !strings.Contains(j.LastError, "no handler") {
		t.Fatalf("unknown type: %q", j.LastError)
	}
}

// A job that runs longer than its lease must not be taken by another worker,
// because the heartbeat keeps extending the lease.
func TestHeartbeatProtectsLongJobs(t *testing.T) {
	s := NewMemoryStore()
	var calls atomic.Int32
	handler := func(context.Context, Job) error {
		calls.Add(1)
		time.Sleep(900 * time.Millisecond) // three times the 300ms lease
		return nil
	}
	a, b := fastPool(s, "a", 1), fastPool(s, "b", 1)
	a.Handle("long", handler)
	b.Handle("long", handler)
	j, _, _ := s.Enqueue(ctx, EnqueueParams{Type: "long"})
	stopA, stopB := start(a), start(b)
	defer stopA()
	defer stopB()
	waitFor(t, "long job", func() bool { return statusOf(t, s, j.ID) == StatusSucceeded })
	if calls.Load() != 1 {
		t.Fatalf("long job ran %d times, want 1", calls.Load())
	}
}

// Simulates a worker process dying: it claims a job and never reports back.
func TestCrashedWorkerJobIsRecovered(t *testing.T) {
	s := NewMemoryStore()
	j, _, _ := s.Enqueue(ctx, EnqueueParams{Type: "work"})
	if jobs, _ := s.Dequeue(ctx, DefaultQueue, "crashed", 1, 200*time.Millisecond); len(jobs) != 1 {
		t.Fatal("setup: could not claim job")
	}

	p := fastPool(s, "survivor", 1)
	p.Handle("work", func(context.Context, Job) error { return nil })
	stop := start(p)
	defer stop()
	waitFor(t, "recovery", func() bool { return statusOf(t, s, j.ID) == StatusSucceeded })
	if got, _ := s.Get(ctx, j.ID); got.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (one lost, one successful)", got.Attempts)
	}
}

func TestShutdownWaitsForRunningJobs(t *testing.T) {
	s := NewMemoryStore()
	p := fastPool(s, "w", 2)
	started := make(chan struct{})
	p.Handle("slow", func(context.Context, Job) error {
		close(started)
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	j, _, _ := s.Enqueue(ctx, EnqueueParams{Type: "slow"})
	stop := start(p)
	<-started
	stop() // returns only after the handler finished
	if st := statusOf(t, s, j.ID); st != StatusSucceeded {
		t.Fatalf("status after shutdown %q, want succeeded", st)
	}
}

func TestShutdownAbandonsJobsAfterGracePeriod(t *testing.T) {
	s := NewMemoryStore()
	p := NewPool(s, PoolConfig{
		WorkerID: "w", Concurrency: 1, Lease: 300 * time.Millisecond,
		PollEvery: 10 * time.Millisecond, ShutdownGrace: 100 * time.Millisecond,
	})
	started := make(chan struct{})
	p.Handle("stuck", func(c context.Context, _ Job) error {
		close(started)
		<-c.Done() // a well-behaved handler stops when told to
		return c.Err()
	})
	j, _, _ := s.Enqueue(ctx, EnqueueParams{Type: "stuck"})
	stop := start(p)
	<-started
	began := time.Now()
	stop()
	if d := time.Since(began); d > time.Second {
		t.Fatalf("shutdown took %s despite a 100ms grace period", d)
	}
	// The job was not lost: it is queued for another attempt.
	if st := statusOf(t, s, j.ID); st != StatusQueued {
		t.Fatalf("status %q, want queued", st)
	}
}

func TestDefaultBackoffGrowsAndIsCapped(t *testing.T) {
	prev := time.Duration(0)
	for attempt := 1; attempt <= 8; attempt++ {
		d := DefaultBackoff(attempt)
		base := time.Second << (attempt - 1)
		if d < base || d > base+base/5 {
			t.Fatalf("attempt %d: %s outside [%s, %s]", attempt, d, base, base+base/5)
		}
		if d <= prev {
			t.Fatalf("backoff did not grow at attempt %d", attempt)
		}
		prev = d
	}
	if d := DefaultBackoff(50); d > 6*time.Minute {
		t.Fatalf("backoff not capped: %s", d)
	}
}

func TestAPI(t *testing.T) {
	s := NewMemoryStore()
	srv := httptest.NewServer(NewAPI(s))
	defer srv.Close()

	call := func(method, path, body string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var sb strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			sb.Write(buf[:n])
			if err != nil {
				break
			}
		}
		return resp.StatusCode, sb.String()
	}
	expect := func(code int, body string, wantCode int, wantSub string) {
		t.Helper()
		if code != wantCode || !strings.Contains(body, wantSub) {
			t.Fatalf("got %d %s; want %d containing %q", code, body, wantCode, wantSub)
		}
	}

	code, body := call("POST", "/jobs", `{"type":"email","payload":{"to":"x"},"idempotency_key":"k1"}`)
	expect(code, body, 201, `"status":"queued"`)
	code, body = call("POST", "/jobs", `{"type":"email","idempotency_key":"k1"}`)
	expect(code, body, 200, `"id":1`)

	code, body = call("POST", "/jobs", `{"payload":{}}`)
	expect(code, body, 400, "type is required")
	code, body = call("POST", "/jobs", `{"type":"x","prioritty":5}`)
	expect(code, body, 400, "unknown field")
	code, body = call("POST", "/jobs", `not json`)
	expect(code, body, 400, "invalid JSON")

	code, body = call("GET", "/jobs/1", "")
	expect(code, body, 200, `"type":"email"`)
	code, body = call("GET", "/jobs/999", "")
	expect(code, body, 404, "not found")
	code, body = call("GET", "/jobs/abc", "")
	expect(code, body, 400, "must be a number")

	code, body = call("POST", "/jobs", `{"type":"later","delay_seconds":3600}`)
	expect(code, body, 201, `"id":2`)
	if jobs, _ := s.Dequeue(ctx, DefaultQueue, "w", 10, time.Minute); len(jobs) != 1 || jobs[0].ID != 1 {
		t.Fatalf("delayed job should not be ready: %v", ids(jobs))
	}

	code, body = call("DELETE", "/jobs/2", "")
	expect(code, body, 200, `"status":"cancelled"`)
	code, body = call("DELETE", "/jobs/1", "")
	expect(code, body, 409, "only a queued job")
	code, body = call("POST", "/jobs/1/retry", "")
	expect(code, body, 409, "only a dead job")
	code, body = call("DELETE", "/jobs/999", "")
	expect(code, body, 404, "not found")

	code, body = call("GET", "/stats", "")
	expect(code, body, 200, `"running":1`)
	code, body = call("GET", "/healthz", "")
	expect(code, body, 200, "ok")
}
