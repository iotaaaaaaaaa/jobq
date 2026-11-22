package queue

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

// enqueueRequest is the JSON body of POST /jobs.
type enqueueRequest struct {
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	RunAt          *time.Time      `json:"run_at"`        // absolute time, or
	DelaySeconds   float64         `json:"delay_seconds"` // relative delay
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey string          `json:"idempotency_key"`
}

// NewAPI returns the HTTP interface to a store:
//
//	POST   /jobs             enqueue; 201 if created, 200 if the idempotency key already existed
//	GET    /jobs/{id}        job details
//	DELETE /jobs/{id}        cancel a job that has not started
//	POST   /jobs/{id}/retry  requeue a dead job
//	GET    /stats            job counts per queue and status
//	GET    /healthz          liveness probe
func NewAPI(store Store) http.Handler {
	mux := http.NewServeMux()

	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(v)
	}
	fail := func(w http.ResponseWriter, code int, msg string) {
		writeJSON(w, code, map[string]string{"error": msg})
	}
	jobID := func(w http.ResponseWriter, r *http.Request) (int64, bool) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			fail(w, http.StatusBadRequest, "job id must be a number")
			return 0, false
		}
		return id, true
	}

	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var req enqueueRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		dec.DisallowUnknownFields() // a typo in a field name should be an error, not silently ignored
		if err := dec.Decode(&req); err != nil {
			fail(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
			return
		}
		p := EnqueueParams{
			Queue: req.Queue, Type: req.Type, Payload: req.Payload, Priority: req.Priority,
			MaxAttempts: req.MaxAttempts, IdempotencyKey: req.IdempotencyKey,
		}
		if req.RunAt != nil {
			p.RunAt = *req.RunAt
		} else if req.DelaySeconds > 0 {
			p.RunAt = time.Now().Add(time.Duration(req.DelaySeconds * float64(time.Second)))
		}
		if p.Type == "" {
			fail(w, http.StatusBadRequest, "type is required")
			return
		}
		job, created, err := store.Enqueue(r.Context(), p)
		if err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		code := http.StatusCreated
		if !created {
			code = http.StatusOK
		}
		writeJSON(w, code, job)
	})

	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := jobID(w, r)
		if !ok {
			return
		}
		job, err := store.Get(r.Context(), id)
		switch {
		case errors.Is(err, ErrNotFound):
			fail(w, http.StatusNotFound, "job not found")
		case err != nil:
			fail(w, http.StatusInternalServerError, err.Error())
		default:
			writeJSON(w, http.StatusOK, job)
		}
	})

	// change handles the two state-changing actions, which share their shape.
	change := func(action func(r *http.Request, id int64) (bool, error), refused string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			id, ok := jobID(w, r)
			if !ok {
				return
			}
			if _, err := store.Get(r.Context(), id); errors.Is(err, ErrNotFound) {
				fail(w, http.StatusNotFound, "job not found")
				return
			}
			done, err := action(r, id)
			switch {
			case err != nil:
				fail(w, http.StatusInternalServerError, err.Error())
			case !done:
				fail(w, http.StatusConflict, refused)
			default:
				job, _ := store.Get(r.Context(), id)
				writeJSON(w, http.StatusOK, job)
			}
		}
	}
	mux.HandleFunc("DELETE /jobs/{id}", change(
		func(r *http.Request, id int64) (bool, error) { return store.Cancel(r.Context(), id) },
		"only a queued job can be cancelled"))
	mux.HandleFunc("POST /jobs/{id}/retry", change(
		func(r *http.Request, id int64) (bool, error) { return store.Retry(r.Context(), id) },
		"only a dead job can be retried"))

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := store.Stats(r.Context())
		if err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, stats)
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}
