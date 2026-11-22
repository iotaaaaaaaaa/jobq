// jobq runs the job queue: the HTTP API, a pool of workers, or both.
//
//	jobq -database-url postgres://...     API and workers, backed by Postgres
//	jobq                                  API and workers, in memory (lost on exit)
//	jobq -workers 0                       API only
//	jobq -api=false -workers 16           workers only; run several of these to scale out
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/iotaaaaaaaaa/jobq/internal/queue"
)

func main() {
	var (
		addr    = flag.String("addr", ":8080", "HTTP listen address")
		dbURL   = flag.String("database-url", os.Getenv("DATABASE_URL"), "Postgres URL; empty means in-memory")
		qname   = flag.String("queue", queue.DefaultQueue, "queue the workers consume")
		workers = flag.Int("workers", 4, "concurrent jobs in this process (0 = no workers)")
		api     = flag.Bool("api", true, "serve the HTTP API")
		lease   = flag.Duration("lease", 30*time.Second, "how long a claimed job is held before others may take it")
	)
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log, *addr, *dbURL, *qname, *workers, *api, *lease); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr, dbURL, qname string, workers int, api bool, lease time.Duration) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var store queue.Store
	if dbURL == "" {
		log.Warn("no -database-url: using the in-memory store; jobs are lost on exit")
		store = queue.NewMemoryStore()
	} else {
		if driverName == "" {
			return errors.New("this binary was built without a Postgres driver")
		}
		db, err := sql.Open(driverName, dbURL)
		if err != nil {
			return err
		}
		defer db.Close()
		db.SetMaxOpenConns(workers + 8)
		pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("cannot reach database: %w", err)
		}
		if store, err = queue.NewPostgresStore(ctx, db); err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	if workers > 0 {
		host, _ := os.Hostname()
		pool := queue.NewPool(store, queue.PoolConfig{
			Queue: qname, WorkerID: fmt.Sprintf("%s-%d", host, os.Getpid()),
			Concurrency: workers, Lease: lease, Logger: log,
		})
		registerHandlers(pool, log)
		wg.Add(1)
		go func() { defer wg.Done(); pool.Run(ctx) }()
		log.Info("workers started", "queue", qname, "concurrency", workers)
	}

	if api {
		srv := &http.Server{Addr: addr, Handler: queue.NewAPI(store), ReadHeaderTimeout: 5 * time.Second}
		errCh := make(chan error, 1)
		go func() { errCh <- srv.ListenAndServe() }()
		log.Info("api listening", "addr", addr)
		select {
		case err := <-errCh:
			stop()
			wg.Wait()
			return err
		case <-ctx.Done():
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	} else {
		<-ctx.Done()
	}
	log.Info("shutting down; waiting for running jobs")
	wg.Wait()
	return nil
}

// registerHandlers wires up the built-in job types. A real deployment would
// register its own here: send an email, resize an image, call a webhook.
func registerHandlers(p *queue.Pool, log *slog.Logger) {
	// {"message": "..."}: logs the message.
	p.Handle("echo", func(_ context.Context, j queue.Job) error {
		var in struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(j.Payload, &in); err != nil {
			return err
		}
		log.Info("echo", "job", j.ID, "message", in.Message)
		return nil
	})

	// {"ms": 500}: simulates slow work, and stops early if cancelled.
	p.Handle("sleep", func(ctx context.Context, j queue.Job) error {
		var in struct {
			MS int `json:"ms"`
		}
		if err := json.Unmarshal(j.Payload, &in); err != nil {
			return err
		}
		select {
		case <-time.After(time.Duration(in.MS) * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// Always fails: useful for watching retries, backoff and the dead state.
	p.Handle("fail", func(context.Context, queue.Job) error {
		return errors.New("this job type always fails")
	})

	// {"url": "https://..."}: POSTs the payload to a URL; any non-2xx reply is a failure.
	p.Handle("webhook", func(ctx context.Context, j queue.Job) error {
		var in struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(j.Payload, &in); err != nil || in.URL == "" {
			return errors.New("webhook payload needs a url")
		}
		reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, in.URL, bytesReader(j.Payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("webhook returned %s", resp.Status)
		}
		return nil
	})
}
