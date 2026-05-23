package crypto

import (
	"context"
	"time"
)

// Snapshot contains the full in-memory crypto keyring state.
type Snapshot struct {
	ActiveKeyID string
	KeysByID    map[string][]byte
	LoadedAt    time.Time
	Version     string
}

// Provider loads the complete crypto keyring from one source.
type Provider interface {
	Load(ctx context.Context) (Snapshot, error)
}
