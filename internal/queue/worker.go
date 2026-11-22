package queue

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

// Handler does the work for one job type. Returning an error (or panicking)
// counts as a failed attempt. The context is cancelled if the worker loses
// its lease on the job or the pool is shutting down.
type Handler func(ctx context.Context, job Job) error

type PoolConfig struct {
	Queue       string
	WorkerID    string        // unique per process
	Concurrency int           // jobs run at once
	Lease       time.Duration // how long a claimed job is ours before others may take it
	PollEvery   time.Duration // how often to look for work when idle
	// How long Run waits for in-flight jobs after its context is cancelled.
	ShutdownGrace time.Duration
	// Backoff returns the delay before retrying after the given failed
	// attempt (1 = first failure). Nil means DefaultBackoff.
	Backoff func(attempt int) time.Duration
	Logger  *slog.Logger
}

// DefaultBackoff doubles from 1s up to 5 minutes, with up to 20% random
// jitter. Jitter stops a batch of jobs that failed together from all
// retrying at the same instant and failing together again.
func DefaultBackoff(attempt int) time.Duration {
	d := time.Second << min(attempt-1, 20)
	if d > 5*time.Minute {
		d = 5 * time.Minute
	}
	return d + time.Duration(rand.Int63n(int64(d)/5+1))
}

// Pool pulls jobs from a store and runs them on a fixed number of goroutines.
type Pool struct {
	store    Store
	cfg      PoolConfig
	handlers map[string]Handler
	log      *slog.Logger
}

func NewPool(store Store, cfg PoolConfig) *Pool {
	if cfg.Queue == "" {
		cfg.Queue = DefaultQueue
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.PollEvery <= 0 {
		cfg.PollEvery = 500 * time.Millisecond
	}
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = 20 * time.Second
	}
	if cfg.Backoff == nil {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Pool{store: store, cfg: cfg, handlers: map[string]Handler{}, log: cfg.Logger.With("worker", cfg.WorkerID)}
}

// Handle registers the handler for a job type. Call it before Run.
func (p *Pool) Handle(jobType string, h Handler) { p.handlers[jobType] = h }

// Run processes jobs until ctx is cancelled. It then stops taking new jobs
// and waits up to ShutdownGrace for running ones. Any job still running
// after that is abandoned; its lease expires and another worker picks it up.
func (p *Pool) Run(ctx context.Context) {
	// Handlers get their own context so that cancelling ctx does not kill
	// them at once; they are only cancelled when the grace period runs out.
	jobCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()

	slots := make(chan struct{}, p.cfg.Concurrency) // one token per running job
	var wg sync.WaitGroup

loop:
	for {
		// Only ask for as many jobs as we can start right now. Claiming more
		// would hold leases on jobs that are just sitting in memory.
		free := cap(slots) - len(slots)
		var jobs []Job
		if free > 0 {
			var err error
			jobs, err = p.store.Dequeue(ctx, p.cfg.Queue, p.cfg.WorkerID, free, p.cfg.Lease)
			if err != nil && ctx.Err() == nil {
				p.log.Error("dequeue failed", "err", err)
			}
		}
		for _, job := range jobs {
			slots <- struct{}{}
			wg.Add(1)
			go func(job Job) {
				defer wg.Done()
				defer func() { <-slots }()
				p.runJob(jobCtx, job)
			}(job)
		}

		// If the batch was full there is probably more work: go straight back.
		if free > 0 && len(jobs) == free {
			select {
			case <-ctx.Done():
				break loop
			default:
				continue
			}
		}
		select {
		case <-ctx.Done():
			break loop
		case <-time.After(p.cfg.PollEvery):
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(p.cfg.ShutdownGrace):
		p.log.Warn("shutdown grace period over; abandoning running jobs")
		cancelJobs()
		<-done
	}
}

func (p *Pool) runJob(ctx context.Context, job Job) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Heartbeat: keep extending the lease while the handler runs, so a long
	// job is not mistaken for a dead worker. If the extension is refused we
	// have lost the job, and the handler is told to stop.
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(p.cfg.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				ok, err := p.store.ExtendLease(ctx, job.ID, p.cfg.WorkerID, job.Attempts, p.cfg.Lease)
				if err == nil && !ok {
					p.log.Warn("lost lease", "job", job.ID)
					cancel()
					return
				}
			}
		}
	}()

	err := p.invoke(ctx, job)
	cancel()
	<-hbDone

	// Record the outcome even if the pool is shutting down.
	save, cancelSave := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelSave()
	if err == nil {
		if ok, serr := p.store.Complete(save, job.ID, p.cfg.WorkerID, job.Attempts); serr != nil {
			p.log.Error("could not record success", "job", job.ID, "err", serr)
		} else if !ok {
			p.log.Warn("finished a job we no longer held; result discarded", "job", job.ID)
		}
		return
	}
	status, serr := p.store.Fail(save, job.ID, p.cfg.WorkerID, job.Attempts, err.Error(), p.cfg.Backoff(job.Attempts))
	if serr != nil {
		p.log.Error("could not record failure", "job", job.ID, "err", serr)
		return
	}
	p.log.Info("job failed", "job", job.ID, "type", job.Type, "attempt", job.Attempts, "now", status, "err", err)
}

// invoke calls the handler and converts a panic into an error, so one bad
// job cannot take down the whole worker process.
func (p *Pool) invoke(ctx context.Context, job Job) (err error) {
	h, ok := p.handlers[job.Type]
	if !ok {
		return fmt.Errorf("no handler registered for job type %q", job.Type)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h(ctx, job)
}
