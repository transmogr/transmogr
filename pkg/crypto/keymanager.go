package crypto

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// KeyManagerConfig controls background key refresh behavior.
type KeyManagerConfig struct {
	RefreshInterval time.Duration
	RetryAttempts   int
	RetryDelay      time.Duration
	RequiredKeyIDs  []string
}

// KeyManager serves keys from memory and refreshes them in the background.
type KeyManager struct {
	provider Provider
	cfg      KeyManagerConfig

	mu             sync.RWMutex
	snapshot       Snapshot
	lastRefreshErr error
}

// NewCachedManager performs an initial load and returns a memory-backed key manager.
func NewCachedManager(
	ctx context.Context,
	provider Provider,
	cfg KeyManagerConfig,
) (*KeyManager, error) {
	if provider == nil {
		return nil, fmt.Errorf("key provider is required")
	}

	snapshot, err := loadWithRetry(ctx, provider, cfg)
	if err != nil {
		return nil, fmt.Errorf("initial key load: %w", err)
	}
	if err := validateSnapshot(snapshot, cfg.RequiredKeyIDs); err != nil {
		return nil, fmt.Errorf("initial key load: %w", err)
	}

	return &KeyManager{
		provider:       provider,
		cfg:            cfg,
		snapshot:       cloneSnapshot(snapshot),
		lastRefreshErr: nil,
	}, nil
}

// GetKey returns a cloned key from the current in-memory snapshot.
func (m *KeyManager) GetKey(_ context.Context, keyID string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key, ok := m.snapshot.KeysByID[keyID]
	if !ok {
		return nil, fmt.Errorf("crypto key %q is not configured", keyID)
	}

	return append([]byte(nil), key...), nil
}

// ActiveKeyID returns the active encryption key id from the current snapshot.
func (m *KeyManager) ActiveKeyID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.snapshot.ActiveKeyID
}

// Snapshot returns a defensive copy of the current key snapshot.
func (m *KeyManager) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return cloneSnapshot(m.snapshot)
}

// Ready reports whether the key manager has a valid current key snapshot.
func (m *KeyManager) Ready(context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.lastRefreshErr
}

// Run periodically refreshes the in-memory key snapshot.
func (m *KeyManager) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := m.refresh(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("crypto key refresh failed", "error", err)
			}
		}
	}
}

func (m *KeyManager) refresh(ctx context.Context) error {
	snapshot, err := loadWithRetry(ctx, m.provider, m.cfg)
	if err != nil {
		m.mu.Lock()
		m.lastRefreshErr = err
		m.mu.Unlock()
		return err
	}
	if err := validateSnapshot(snapshot, m.cfg.RequiredKeyIDs); err != nil {
		m.mu.Lock()
		m.lastRefreshErr = err
		m.mu.Unlock()
		return err
	}

	m.mu.Lock()
	m.snapshot = cloneSnapshot(snapshot)
	m.lastRefreshErr = nil
	m.mu.Unlock()

	return nil
}

func validateSnapshot(snapshot Snapshot, requiredKeyIDs []string) error {
	if snapshot.ActiveKeyID == "" {
		return fmt.Errorf("active key id is required")
	}
	if len(snapshot.KeysByID) == 0 {
		return fmt.Errorf("crypto keyring is empty")
	}
	if _, ok := snapshot.KeysByID[snapshot.ActiveKeyID]; !ok {
		return fmt.Errorf("active crypto key %q is not present in keyring", snapshot.ActiveKeyID)
	}
	for _, keyID := range requiredKeyIDs {
		if _, ok := snapshot.KeysByID[keyID]; !ok {
			return fmt.Errorf("required crypto key %q is not present in keyring", keyID)
		}
	}

	return nil
}

func loadWithRetry(ctx context.Context, provider Provider, cfg KeyManagerConfig) (Snapshot, error) {
	var lastErr error
	for attempt := 1; attempt <= cfg.RetryAttempts; attempt++ {
		snapshot, err := provider.Load(ctx)
		if err == nil {
			return snapshot, nil
		}
		lastErr = err
		if attempt == cfg.RetryAttempts {
			break
		}

		timer := time.NewTimer(cfg.RetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Snapshot{}, ctx.Err()
		case <-timer.C:
		}
	}

	return Snapshot{}, lastErr
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	keys := make(map[string][]byte, len(snapshot.KeysByID))
	for keyID, key := range snapshot.KeysByID {
		keys[keyID] = key
	}

	return Snapshot{
		ActiveKeyID: snapshot.ActiveKeyID,
		KeysByID:    keys,
		LoadedAt:    snapshot.LoadedAt,
		Version:     snapshot.Version,
	}
}
