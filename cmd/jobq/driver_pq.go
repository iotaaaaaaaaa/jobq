//go:build !nodriver

package main

// The Postgres driver registers itself with database/sql on import.
import _ "github.com/lib/pq"

const driverName = "postgres"
