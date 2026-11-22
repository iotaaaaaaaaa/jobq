//go:build nodriver

package main

// Built with -tags nodriver: no Postgres driver is linked in, so only the
// in-memory store is available. Useful where the driver cannot be downloaded.
const driverName = ""
