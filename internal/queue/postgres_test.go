//go:build postgres

// Runs the same conformance suite against a real Postgres:
//
//	DATABASE_URL=postgres://user:pass@localhost:5432/jobq?sslmode=disable \
//	    go test -tags postgres ./internal/queue/
package queue

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

func TestPostgresStore(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	runStoreTests(t, func(t *testing.T) Store {
		// Each subtest starts from an empty table.
		if _, err := db.Exec(`DROP TABLE IF EXISTS jobs`); err != nil {
			t.Fatal(err)
		}
		s, err := NewPostgresStore(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}
