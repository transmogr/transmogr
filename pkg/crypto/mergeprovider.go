package crypto

import (
	"context"
	"fmt"
	"time"
)

// NamedProvider labels one key provider for diagnostics and merge order.
type NamedProvider struct {
	Name     string
	Provider Provider
}

// MergeProvider merges snapshots from multiple providers in declaration order.
type MergeProvider struct {
	Providers []NamedProvider
}

// Load merges all configured providers into one snapshot.
func (p *MergeProvider) Load(ctx context.Context) (Snapshot, error) {
	merged := Snapshot{
		KeysByID: make(map[string][]byte),
		LoadedAt: time.Now().UTC(),
	}

	loaded := false
	for _, item := range p.Providers {
		if item.Provider == nil {
			continue
		}

		snapshot, err := item.Provider.Load(ctx)
		if err != nil {
			return Snapshot{}, fmt.Errorf("%s: %w", item.Name, err)
		}

		for keyID, key := range snapshot.KeysByID {
			merged.KeysByID[keyID] = key
		}
		if snapshot.ActiveKeyID != "" {
			merged.ActiveKeyID = snapshot.ActiveKeyID
		}
		if snapshot.Version != "" {
			merged.Version = snapshot.Version
		}
		loaded = true
	}

	if !loaded {
		return Snapshot{}, fmt.Errorf("no key providers configured")
	}

	return merged, nil
}
