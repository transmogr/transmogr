package models

import "time"

// Lease is a distributed ownership record persisted in the shared state store.
type Lease struct {
	Kind       string
	Region     string
	PeerRegion string
	Table      string
	OwnerID    string
	Generation int64
	ExpiresAt  time.Time
	UpdatedAt  time.Time
}
