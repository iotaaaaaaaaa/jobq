.PHONY: build test test-sql test-postgres vet clean

# The Postgres driver is the one outside dependency; `go mod tidy` fetches it.
build:
	go mod tidy
	go build -o bin/jobq ./cmd/jobq

# Unit and integration tests on the in-memory store. No database needed.
test:
	go test -race -count=1 -tags nodriver ./...

# The SQL files against a real Postgres, using psql (version 16+).
test-sql:
	./tests/sql_test.sh

# The Go Postgres store against a real Postgres.
test-postgres:
	go mod tidy
	go test -race -count=1 -tags postgres ./internal/queue/

vet:
	go vet -tags nodriver ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

clean:
	rm -rf bin
